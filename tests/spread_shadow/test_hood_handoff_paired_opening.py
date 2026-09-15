from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from risex_spread_shadow.hood_handoff import (
    AccountSnapshot,
    DepthLevel,
    Direction,
    HandoffConfig,
    HistoryPage,
    MarketMetadata,
    MutationReceipt,
    OperationMode,
    OrderBookSnapshot,
    OrderSnapshot,
    Outcome,
    TradeReceipt,
    run_handoff,
    run_series,
    RobinhoodSeriesConfig,
)


NOW = 1_000.0


class Clock:
    def now(self) -> float:
        return NOW

    async def sleep(self, seconds: float) -> None:
        return None


def metadata() -> MarketMetadata:
    return MarketMetadata(
        market_id=1,
        symbol="BTC",
        status="active",
        price_decimals=1,
        size_decimals=2,
        minimum_base_amount=Decimal("0.10"),
        minimum_quote_amount=Decimal("10"),
        source_fee_rate=None,
        receiver_fee_rate=None,
        observed_at=NOW,
        margin_evidence="synthetic account limit fixture",
        market_type="perp",
        venue="robinhood",
    )


def account(index: int, position: Decimal, *, margin: Decimal = Decimal("100")) -> AccountSnapshot:
    return AccountSnapshot(
        account_index=index,
        market_id=1,
        signed_position=position,
        active_orders=(),
        observed_at=NOW,
        authorized=True,
        ready=True,
        margin_available=margin,
        margin_required=Decimal("1"),
        fee_rate=None,
        source_identity=f"synthetic-{index}",
        incremental_margin_required=Decimal("2"),
        incremental_margin_evidence="synthetic opening margin proof",
    )


def config(path: Path, **overrides) -> HandoffConfig:
    values = {
        "market_id": 1,
        "market_symbol": "BTC",
        "environment": "robinhood",
        "direction": Direction.LONG,
        "quantity": Decimal("0.20"),
        "source_limit_price": Decimal("100.0"),
        "receiver_worst_price": Decimal("101.0"),
        "freshness_seconds": 10,
        "request_timeout_seconds": 1,
        "order_timeout_seconds": 1,
        "reconcile_timeout_seconds": 1,
        "poll_interval_seconds": 0.001,
        "max_poll_count": 2,
        "source_order_lifetime_seconds": 300,
        "client_order_prefix": "paired-open",
        "journal_path": str(path),
        "api_base_url": "https://api.rh.lighter.xyz",
        "chain_id": 466324,
        "api_key_index": 4,
        "operator_execution_opt_in": True,
        "operator_plan_reviewed": True,
        "operation_mode": OperationMode.PAIRED_OPENING,
    }
    values.update(overrides)
    return HandoffConfig(**values)


class PairedClient:
    source_account_index = 11
    receiver_account_index = 22

    def __init__(self, *, source_fills_before_receiver: bool = False, receiver_margin: Decimal = Decimal("100")) -> None:
        self.source_position = Decimal("0")
        self.receiver_position = Decimal("0")
        self.receiver_margin = receiver_margin
        self.source_order: OrderSnapshot | None = None
        self.receiver_order: OrderSnapshot | None = None
        self.submissions = []
        self.cancellations = []
        self.source_fills_before_receiver = source_fills_before_receiver
        self.child_number = 0

    async def market_metadata(self, market_id: int) -> MarketMetadata:
        assert market_id == 1
        return metadata()

    async def account_snapshot(self, account_index: int, market_id: int) -> AccountSnapshot:
        assert market_id == 1
        position = self.source_position if account_index == self.source_account_index else self.receiver_position
        margin = Decimal("100") if account_index == self.source_account_index else self.receiver_margin
        order = self.source_order if account_index == self.source_account_index else self.receiver_order
        active = () if order is None or not order.active else (order,)
        return _account_snapshot(account_index, position, active, margin=margin)

    async def lookup_order(self, account_index: int, market_id: int, *, order_id=None, client_order_index=None):
        order = self.source_order if account_index == self.source_account_index else self.receiver_order
        if order is None:
            return None
        if order_id is not None and order.order_id != str(order_id):
            return None
        if client_order_index is not None and str(order.client_order_index) != str(client_order_index):
            return None
        return order

    async def submit_order(self, plan):
        self.submissions.append(plan)
        if plan.account_index == self.source_account_index:
            self.source_order = OrderSnapshot(
                account_index=11,
                market_id=1,
                order_id=f"source-{self.child_number}",
                client_order_index=plan.client_order_index,
                status="filled" if self.source_fills_before_receiver else "open",
                side=plan.side,
                order_type="LIMIT",
                time_in_force="POST_ONLY",
                reduce_only=False,
                initial_quantity=plan.quantity,
                remaining_quantity=Decimal("0") if self.source_fills_before_receiver else plan.quantity,
                filled_quantity=plan.quantity if self.source_fills_before_receiver else Decimal("0"),
                price=plan.price,
                observed_at=NOW,
            )
            if self.source_fills_before_receiver:
                self.source_position -= plan.quantity
            return MutationReceipt(True, self.source_order.order_id, "0xsource")
        assert plan.account_index == self.receiver_account_index
        self.receiver_order = OrderSnapshot(
            account_index=22,
            market_id=1,
            order_id=f"receiver-{self.child_number}",
            client_order_index=plan.client_order_index,
            status="filled",
            side=plan.side,
            order_type="MARKET",
            time_in_force="IOC",
            reduce_only=False,
            initial_quantity=plan.quantity,
            remaining_quantity=Decimal("0"),
            filled_quantity=plan.quantity,
            price=plan.price,
            observed_at=NOW,
        )
        assert self.source_order is not None
        self.source_order = OrderSnapshot(
            **{
                "account_index": self.source_order.account_index,
                "market_id": self.source_order.market_id,
                "order_id": self.source_order.order_id,
                "client_order_index": self.source_order.client_order_index,
                "status": "filled",
                "side": self.source_order.side,
                "order_type": self.source_order.order_type,
                "time_in_force": self.source_order.time_in_force,
                "reduce_only": self.source_order.reduce_only,
                "initial_quantity": self.source_order.initial_quantity,
                "remaining_quantity": Decimal("0"),
                "filled_quantity": plan.quantity,
                "price": self.source_order.price,
                "observed_at": NOW,
            }
        )
        self.source_position -= plan.quantity
        self.receiver_position += plan.quantity
        self.child_number += 1
        return MutationReceipt(True, self.receiver_order.order_id, "0xreceiver")

    async def cancel_order(self, account_index: int, market_id: int, order_id: str) -> MutationReceipt:
        self.cancellations.append(order_id)
        return MutationReceipt(True, order_id, "0xcancel")

    async def list_trades(self, account_index: int, market_id: int, *, order_id=None, cursor=None, limit=100):
        if self.source_order is None or self.receiver_order is None:
            return HistoryPage()
        if account_index == self.source_account_index and order_id == self.source_order.order_id:
            return HistoryPage(
                trades=(
                    TradeReceipt(
                        f"trade-{self.child_number}",
                        11,
                        1,
                        self.source_order.order_id,
                        "SELL",
                        self.source_order.filled_quantity,
                        self.source_order.price,
                        None,
                        22,
                        NOW,
                        counterparty_order_id=self.receiver_order.order_id,
                    ),
                )
            )
        if account_index == self.receiver_account_index and order_id == self.receiver_order.order_id:
            return HistoryPage(
                trades=(
                    TradeReceipt(
                        f"trade-{self.child_number}",
                        22,
                        1,
                        self.receiver_order.order_id,
                        "BUY",
                        self.receiver_order.filled_quantity,
                        self.receiver_order.price,
                        None,
                        11,
                        NOW,
                        counterparty_order_id=self.source_order.order_id,
                    ),
                )
            )
        return HistoryPage()


def _account_snapshot(index: int, position: Decimal, active_orders=(), *, margin=Decimal("100")) -> AccountSnapshot:
    base = account(index, position, margin=margin)
    return AccountSnapshot(
        account_index=base.account_index,
        market_id=base.market_id,
        signed_position=base.signed_position,
        active_orders=tuple(active_orders),
        observed_at=base.observed_at,
        authorized=base.authorized,
        ready=base.ready,
        margin_available=base.margin_available,
        margin_required=base.margin_required,
        fee_rate=base.fee_rate,
        source_identity=base.source_identity,
        incremental_margin_required=base.incremental_margin_required,
        incremental_margin_evidence=base.incremental_margin_evidence,
    )


class SeriesPairedClient(PairedClient):
    def __init__(self):
        super().__init__()
        self.books = (
            OrderBookSnapshot(
                market_id=1,
                symbol="BTC",
                asks=(DepthLevel(Decimal("100.0"), Decimal("0.30"), "ask-0"),),
                bids=(),
                observed_at=NOW,
                market_type="perp",
                venue="robinhood",
            ),
            OrderBookSnapshot(
                market_id=1,
                symbol="BTC",
                asks=(DepthLevel(Decimal("100.0"), Decimal("0.30"), "ask-1"),),
                bids=(),
                observed_at=NOW,
                market_type="perp",
                venue="robinhood",
            ),
        )
    async def resolve_market(self, symbol: str) -> MarketMetadata:
        assert symbol == "BTC"
        return metadata()

    async def order_book(self, market_id: int) -> OrderBookSnapshot:
        return self.books[min(self.child_number, len(self.books) - 1)]


def series_config(path: Path, **overrides) -> RobinhoodSeriesConfig:
    values = {
        "market_symbol": "BTC",
        "direction": Direction.LONG,
        "total_quantity": Decimal("0.50"),
        "desired_slice_quantity": Decimal("0.25"),
        "allowed_price_deviation": Decimal("0.02"),
        "source_limit_price": Decimal("100.0"),
        "receiver_worst_price": Decimal("101.0"),
        "freshness_seconds": 10,
        "request_timeout_seconds": 1,
        "order_timeout_seconds": 1,
        "reconcile_timeout_seconds": 1,
        "poll_interval_seconds": 0.001,
        "max_poll_count": 2,
        "source_order_lifetime_seconds": 300,
        "client_order_prefix": "paired-series",
        "journal_path": str(path),
        "market_id": 1,
        "operator_execution_opt_in": True,
        "operator_plan_reviewed": True,
        "operation_mode": OperationMode.PAIRED_OPENING,
    }
    values.update(overrides)
    return RobinhoodSeriesConfig(**values)


@pytest.mark.asyncio
async def test_paired_opening_plan_is_explicit_and_builds_opposite_positions(tmp_path):
    client = PairedClient()
    result = await run_handoff(config(tmp_path / "paired.jsonl"), client, clock=Clock())
    assert result.outcome is Outcome.SUCCESS
    assert result.operation_mode is OperationMode.PAIRED_OPENING
    assert result.plan is not None
    assert result.plan.operation_mode is OperationMode.PAIRED_OPENING
    assert [(item.account_index, item.side, item.reduce_only) for item in client.submissions] == [
        (11, "SELL", False),
        (22, "BUY", False),
    ]
    assert client.source_position == Decimal("-0.20")
    assert client.receiver_position == Decimal("0.20")
    text = (tmp_path / "paired.jsonl").read_text()
    assert '"operation_mode":"PAIRED_OPENING"' in text
    assert result.as_dict()["operation_mode"] == "PAIRED_OPENING"


@pytest.mark.asyncio
async def test_paired_opening_requires_flat_accounts_and_both_leg_margin(tmp_path):
    client = PairedClient(receiver_margin=Decimal("1"))
    result = await run_handoff(config(tmp_path / "margin.jsonl"), client, clock=Clock())
    assert result.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED
    assert result.reason == "contract_error"
    assert client.submissions == []

    flat_client = PairedClient()
    flat_client.source_position = Decimal("0.01")
    result = await run_handoff(config(tmp_path / "nonflat.jsonl"), flat_client, clock=Clock())
    assert result.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED
    assert result.reason == "contract_error"
    assert flat_client.submissions == []


@pytest.mark.asyncio
async def test_source_fill_before_receiver_stops_second_leg(tmp_path):
    client = PairedClient(source_fills_before_receiver=True)
    result = await run_handoff(config(tmp_path / "race.jsonl"), client, clock=Clock())
    assert result.outcome is Outcome.UNKNOWN
    assert [item.account_index for item in client.submissions] == [11]
    assert any("before receiver dispatch" in reason for reason in result.unknown_reasons)


@pytest.mark.asyncio
async def test_paired_series_binds_expected_cumulative_positions(tmp_path):
    client = SeriesPairedClient()
    result = await run_series(series_config(tmp_path / "series.jsonl"), client, clock=Clock())
    assert result.outcome is Outcome.SUCCESS
    assert result.operation_mode is OperationMode.PAIRED_OPENING
    assert result.completed_quantity == Decimal("0.50")
    assert client.source_position == Decimal("-0.50")
    assert client.receiver_position == Decimal("0.50")
    assert [item.reduce_only for item in client.submissions] == [False, False, False, False]
    text = (tmp_path / "series.jsonl").read_text()
    assert text.count("PAIRED_OPENING") >= 4
