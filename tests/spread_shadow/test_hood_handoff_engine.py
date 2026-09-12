from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import pytest

from risex_spread_shadow.hood_handoff import (
    AccountSnapshot,
    Direction,
    HandoffConfig,
    HistoryPage,
    MarketMetadata,
    MutationReceipt,
    OrderSnapshot,
    Outcome,
    TradeReceipt,
    run_handoff,
)


NOW = 1_000.0


def make_config(path: Path, **overrides) -> HandoffConfig:
    value = {
        "market_id": 7,
        "direction": Direction.LONG,
        "quantity": Decimal("0.125"),
        "source_limit_price": Decimal("100.25"),
        "receiver_worst_price": Decimal("101.25"),
        "receiver_price_cap": Decimal("101.25"),
        "max_gross_notional": Decimal("200"),
        "source_fee_budget": Decimal("1"),
        "receiver_fee_budget": Decimal("1"),
        "freshness_seconds": 10,
        "request_timeout_seconds": 1,
        "order_timeout_seconds": 1,
        "reconcile_timeout_seconds": 1,
        "poll_interval_seconds": 0.001,
        "max_poll_count": 2,
        "source_order_lifetime_seconds": 300,
        "client_order_prefix": "test-hcr",
        "journal_path": str(path),
        "operator_execution_opt_in": True,
        "operator_plan_reviewed": True,
        "api_base_url": "https://mainnet.zklighter.elliot.ai",
        "api_key_index": 4,
        "chain_id": 304,
    }
    value.update(overrides)
    return HandoffConfig(**value)


def account(index: int, position: str, active_orders=()):
    return AccountSnapshot.from_mapping(
        {
            "account_index": index,
            "market_id": 7,
            "signed_position": position,
            "active_orders": list(active_orders),
            "observed_at": NOW,
            "authorized": True,
            "ready": True,
            "margin_available": "1000",
            "margin_required": "1",
            "incremental_margin_required": "1",
            "incremental_margin_evidence": "fixture planned delta",
            "fee_rate": "0.001",
            "source_identity": f"account-{index}",
        }
    )


def order(*, account_index, order_id, side, status="open", filled="0", remaining="0.125", reduce_only, client_order_index=1, price="100.25"):
    return OrderSnapshot(
        account_index=account_index,
        market_id=7,
        order_id=order_id,
        client_order_index=client_order_index,
        status=status,
        side=side,
        order_type="LIMIT" if reduce_only else "MARKET",
        time_in_force="POST_ONLY" if reduce_only else "IOC",
        reduce_only=reduce_only,
        initial_quantity=Decimal("0.125"),
        remaining_quantity=Decimal(remaining),
        filled_quantity=Decimal(filled),
        price=Decimal(price),
        observed_at=NOW,
    )


class FakeClock:
    def now(self):
        return NOW

    async def sleep(self, seconds):
        return None


class FakeClient:
    source_account_index = 11
    receiver_account_index = 22

    def __init__(self, *, source_fills=False, ambiguous_source=False, foreign_receipt=False):
        self.source_fills = source_fills
        self.ambiguous_source = ambiguous_source
        self.foreign_receipt = foreign_receipt
        self.submissions = []
        self.cancellations = []
        self.source_order = None
        self.receiver_order = None
        self.source_position = Decimal("1")
        self.receiver_position = Decimal("0")

    async def market_metadata(self, market_id):
        return MarketMetadata(
            market_id=7,
            symbol="HOOD",
            status="active",
            price_decimals=2,
            size_decimals=3,
            minimum_base_amount=Decimal("0.001"),
            minimum_quote_amount=Decimal("1"),
            source_fee_rate=Decimal("0.001"),
            receiver_fee_rate=Decimal("0.001"),
            observed_at=NOW,
            margin_evidence="fixture",
        )

    async def account_snapshot(self, account_index, market_id):
        if account_index == self.source_account_index:
            active = (self.source_order,) if self.source_order and self.source_order.active else ()
            return account(account_index, str(self.source_position), active)
        active = (self.receiver_order,) if self.receiver_order and self.receiver_order.active else ()
        return account(account_index, str(self.receiver_position), active)

    async def lookup_order(self, account_index, market_id, *, order_id=None, client_order_index=None):
        if account_index == self.source_account_index:
            return self.source_order
        return self.receiver_order

    async def submit_order(self, plan):
        self.submissions.append(plan)
        if plan.reduce_only:
            if self.ambiguous_source:
                raise TimeoutError("transport timed out after dispatch")
            self.source_order = order(
                account_index=self.source_account_index,
                order_id="source-1",
                side=plan.side,
                reduce_only=True,
                client_order_index=plan.client_order_index,
                price=str(plan.price),
            )
            return MutationReceipt(True, "source-1", "tx-source")
        self.receiver_order = order(
            account_index=self.receiver_account_index,
            order_id="receiver-1",
            side=plan.side,
            status="filled" if self.source_fills else "canceled",
            filled="0.125" if self.source_fills else "0",
            remaining="0" if self.source_fills else "0.125",
            reduce_only=False,
            client_order_index=plan.client_order_index,
            price=str(plan.price),
        )
        if self.source_fills:
            self.source_position = Decimal("0.875") if self.source_order.side == "SELL" else Decimal("-0.875")
            self.source_order = order(
                account_index=self.source_account_index,
                order_id="source-1",
                side=self.source_order.side,
                status="filled",
                filled="0.125",
                remaining="0",
                reduce_only=True,
                client_order_index=self.source_order.client_order_index,
                price=str(self.source_order.price),
            )
            self.receiver_position = Decimal("0.125") if plan.side == "BUY" else Decimal("-0.125")
        return MutationReceipt(True, "receiver-1", "tx-receiver")

    async def cancel_order(self, account_index, market_id, order_id):
        self.cancellations.append(order_id)
        if self.source_order and self.source_order.order_id == order_id and self.source_order.active:
            self.source_order = order(
                account_index=self.source_account_index,
                order_id="source-1",
                side=self.source_order.side,
                status="canceled",
                filled=str(self.source_order.filled_quantity),
                remaining="0",
                reduce_only=True,
                client_order_index=self.source_order.client_order_index,
                price=str(self.source_order.price),
            )
        return MutationReceipt(True, order_id, "tx-cancel")

    async def list_trades(self, account_index, market_id, *, order_id=None, cursor=None, limit=100):
        if self.source_fills and order_id == "source-1" and account_index == self.source_account_index:
            return HistoryPage(
                trades=(TradeReceipt("t-source", account_index, 7, "source-1", self.source_order.side, Decimal("0.125"), self.source_order.price, Decimal("0.0125"), self.receiver_account_index, NOW),),
            )
        if self.source_fills and order_id == "receiver-1" and account_index == self.receiver_account_index:
            trades = [TradeReceipt("t-receiver", account_index, 7, "receiver-1", self.receiver_order.side, Decimal("0.125"), self.receiver_order.price, Decimal("0.0126"), self.source_account_index, NOW)]
            if self.foreign_receipt:
                trades.append(TradeReceipt("t-foreign", 999, 7, "receiver-1", "BUY", Decimal("0.125"), self.receiver_order.price, Decimal("0.0126"), None, NOW))
            return HistoryPage(trades=tuple(reversed(trades)))
        return HistoryPage()


class RejectReceiverClient(FakeClient):
    async def submit_order(self, plan):
        if not plan.reduce_only:
            self.submissions.append(plan)
            return MutationReceipt(False, None, None, "sequencer rejected IOC")
        return await super().submit_order(plan)


class CancellationRaceClient(FakeClient):
    async def cancel_order(self, account_index, market_id, order_id):
        self.cancellations.append(order_id)
        return MutationReceipt(True, order_id, "tx-cancel")


@pytest.mark.asyncio
async def test_long_source_then_receiver_success_and_exact_receipts(tmp_path):
    client = FakeClient(source_fills=True)
    result = await run_handoff(make_config(tmp_path / "success.jsonl"), client, clock=FakeClock())
    assert result.outcome is Outcome.SUCCESS
    assert [plan.side for plan in client.submissions] == ["SELL", "BUY"]
    assert result.source.filled_quantity == Decimal("0.125")
    assert result.receiver.filled_quantity == Decimal("0.125")
    assert result.receiver.quantity_against(client.source_account_index) == Decimal("0.125")
    assert not client.cancellations


@pytest.mark.asyncio
async def test_ambiguous_source_dispatch_is_unknown_and_never_dispatches_receiver(tmp_path):
    client = FakeClient(ambiguous_source=True)
    result = await run_handoff(make_config(tmp_path / "ambiguous.jsonl"), client, clock=FakeClock())
    assert result.outcome is Outcome.UNKNOWN
    assert [plan.side for plan in client.submissions] == ["SELL"]
    assert not [plan for plan in client.submissions if not plan.reduce_only]
    journal = (tmp_path / "ambiguous.jsonl").read_text()
    assert "SOURCE_DISPATCH_INTENT" in journal
    assert "receiver" not in journal.lower() or "RECEIVER_DISPATCH_INTENT" not in journal


@pytest.mark.asyncio
async def test_partial_source_fill_blocks_receiver_and_cancels_identified_order(tmp_path):
    client = FakeClient()
    original_submit = client.submit_order

    async def partial(plan):
        receipt = await original_submit(plan)
        if plan.reduce_only:
            client.source_order = order(
                account_index=client.source_account_index,
                order_id="source-1",
                side="SELL",
                filled="0.025",
                remaining="0.100",
                reduce_only=True,
                client_order_index=client.source_order.client_order_index,
                price=str(client.source_order.price),
            )
        return receipt

    client.submit_order = partial
    result = await run_handoff(make_config(tmp_path / "partial.jsonl"), client, clock=FakeClock())
    assert result.outcome is Outcome.UNKNOWN
    assert not [plan for plan in client.submissions if not plan.reduce_only]
    assert client.cancellations == ["source-1"]


@pytest.mark.asyncio
async def test_restart_reconciles_without_replaying_intents(tmp_path):
    path = tmp_path / "restart.jsonl"
    first = FakeClient(ambiguous_source=True)
    result = await run_handoff(make_config(path), first, clock=FakeClock())
    assert result.outcome is Outcome.UNKNOWN
    second = FakeClient()
    resumed = await run_handoff(make_config(path), second, clock=FakeClock())
    assert resumed.outcome is Outcome.UNKNOWN
    assert second.submissions == []
    assert "RESTART_RECONCILIATION_ONLY" in path.read_text()


@pytest.mark.asyncio
async def test_short_direction_maps_to_buy_then_sell_and_live_cap_conflict_blocks(tmp_path):
    client = FakeClient(source_fills=True)
    client.source_position = Decimal("-1")
    client.receiver_position = Decimal("0")
    preview = await run_handoff(
        make_config(
            tmp_path / "short-preview.jsonl",
            direction=Direction.SHORT,
            operator_execution_opt_in=False,
        ),
        client,
        clock=FakeClock(),
    )
    assert preview.outcome is Outcome.PREVIEW
    assert preview.plan.source.side == "BUY"
    assert preview.plan.receiver.side == "SELL"
    result = await run_handoff(
        make_config(tmp_path / "short-live.jsonl", direction=Direction.SHORT), client, clock=FakeClock()
    )
    assert result.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED
    assert client.submissions == []
    assert result.reason == "protocol_conflict_receiver_sell_market_price_floor"


@pytest.mark.asyncio
async def test_receiver_rejection_is_observed_and_identified_source_is_cancelled(tmp_path):
    client = RejectReceiverClient()
    result = await run_handoff(make_config(tmp_path / "reject.jsonl"), client, clock=FakeClock())
    assert result.outcome is Outcome.UNKNOWN
    assert [plan.side for plan in client.submissions] == ["SELL", "BUY"]
    assert client.cancellations == ["source-1"]


@pytest.mark.asyncio
async def test_terminal_receiver_no_fill_is_partial_and_not_success(tmp_path):
    client = FakeClient(source_fills=False)
    result = await run_handoff(make_config(tmp_path / "nofill.jsonl"), client, clock=FakeClock())
    assert result.outcome is Outcome.PARTIAL
    assert result.source.filled_quantity == Decimal("0")
    assert result.receiver.filled_quantity == Decimal("0")
    assert client.cancellations == ["source-1"]


@pytest.mark.asyncio
async def test_cancel_ack_without_terminal_order_remains_unknown(tmp_path):
    client = CancellationRaceClient()
    result = await run_handoff(make_config(tmp_path / "cancel-race.jsonl"), client, clock=FakeClock())
    assert result.outcome is Outcome.UNKNOWN
    assert "final order state is not terminal" in result.unknown_reasons


@pytest.mark.asyncio
async def test_foreign_receipt_is_not_counted_as_a_fill(tmp_path):
    client = FakeClient(source_fills=True, foreign_receipt=True)
    result = await run_handoff(make_config(tmp_path / "foreign.jsonl"), client, clock=FakeClock())
    assert result.outcome is Outcome.UNKNOWN
    assert result.receiver.filled_quantity == Decimal("0.125")
    assert all(trade.account_index == client.receiver_account_index for trade in result.receiver.trades)
