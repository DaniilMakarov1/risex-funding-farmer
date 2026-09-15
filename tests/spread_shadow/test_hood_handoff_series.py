from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest
import risex_spread_shadow.hood_handoff.series as series_module

from risex_spread_shadow.hood_handoff import (
    AccountSnapshot,
    ContractError,
    DepthLevel,
    Direction,
    DurableJournal,
    HandoffConfig,
    HandoffResult,
    HistoryPage,
    LegReconciliation,
    MarketMetadata,
    MutationReceipt,
    OrderBookSnapshot,
    OrderSnapshot,
    Outcome,
    Phase,
    PreflightBlocked,
    RobinhoodSeriesEngine,
    LighterSdkClient,
    StaticSecretProvider,
    ROBINHOOD_ORDER_BOOK_LIMIT,
    RobinhoodSeriesConfig,
    TradeReceipt,
    run_series,
    size_next_slice,
)


NOW = 1_000.0


def series_config(path: Path, **overrides) -> RobinhoodSeriesConfig:
    values = {
        "market_symbol": "BTC",
        "direction": Direction.LONG,
        "total_quantity": Decimal("1.0"),
        "desired_slice_quantity": Decimal("0.5"),
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
        "client_order_prefix": "rh-series",
        "journal_path": str(path),
        "market_id": 1,
        "api_base_url": "https://api.rh.lighter.xyz",
        "chain_id": 466324,
        "operator_execution_opt_in": True,
        "operator_plan_reviewed": True,
    }
    values.update(overrides)
    return RobinhoodSeriesConfig(**values)


def metadata(symbol: str = "BTC") -> MarketMetadata:
    return MarketMetadata(
        market_id=1 if symbol == "BTC" else 0,
        symbol=symbol,
        status="active",
        price_decimals=1,
        size_decimals=2,
        minimum_base_amount=Decimal("0.10"),
        minimum_quote_amount=Decimal("10"),
        source_fee_rate=None,
        receiver_fee_rate=None,
        observed_at=NOW,
        margin_evidence="current account limit fixture",
        market_type="perp",
        venue="robinhood",
    )


def account(index: int, position: Decimal) -> AccountSnapshot:
    return AccountSnapshot(
        account_index=index,
        market_id=1,
        signed_position=position,
        active_orders=(),
        observed_at=NOW,
        authorized=True,
        ready=True,
        margin_available=Decimal("10000"),
        margin_required=Decimal("1"),
        fee_rate=None,
        source_identity=f"identity-{index}",
        incremental_margin_required=Decimal("1"),
        incremental_margin_evidence="slice evidence bound to account",
    )


def book(*levels: tuple[str, str], observed_at: float = NOW) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        market_id=1,
        symbol="BTC",
        asks=tuple(
            # IDs are required here so duplicate order identity is checked in
            # the same way as the official orderBookOrders response.
            DepthLevel(
                Decimal(price), Decimal(quantity), f"ask-{index}"
            )
            for index, (price, quantity) in enumerate(levels)
        ),
        bids=(),
        observed_at=observed_at,
        market_type="perp",
        venue="robinhood",
    )


class Clock:
    def now(self) -> float:
        return NOW

    async def sleep(self, seconds: float) -> None:
        return None


class SeriesClient:
    source_account_index = 11
    receiver_account_index = 22

    def __init__(self, books: list[OrderBookSnapshot], *, reject_receiver: bool = False, drift: bool = False):
        self.books = books
        self.book_calls = 0
        self.reject_receiver = reject_receiver
        self.drift = drift
        self.submissions = []
        self.source_position = Decimal("1.0")
        self.receiver_position = Decimal("0")
        self.source_order: OrderSnapshot | None = None
        self.receiver_order: OrderSnapshot | None = None
        self.child_number = 0

    async def resolve_market(self, symbol: str) -> MarketMetadata:
        assert symbol == "BTC"
        return metadata()

    async def market_metadata(self, market_id: int) -> MarketMetadata:
        assert market_id == 1
        return metadata()

    async def order_book(self, market_id: int) -> OrderBookSnapshot:
        assert market_id == 1
        item = self.books[min(self.book_calls, len(self.books) - 1)]
        self.book_calls += 1
        return item

    async def account_snapshot(self, account_index: int, market_id: int) -> AccountSnapshot:
        assert market_id == 1
        position = self.source_position if account_index == self.source_account_index else self.receiver_position
        if self.drift and self.source_position < Decimal("1.0") and account_index == self.source_account_index:
            position += Decimal("0.01")
        snapshot = account(account_index, position)
        if account_index == self.source_account_index and self.source_order is not None and self.source_order.active:
            snapshot = replace(snapshot, active_orders=(self.source_order,))
        return snapshot

    async def lookup_order(self, account_index, market_id, *, order_id=None, client_order_index=None):
        return self.source_order if account_index == self.source_account_index else self.receiver_order

    async def submit_order(self, plan):
        self.submissions.append(plan)
        child = self.child_number
        if plan.reduce_only:
            self.source_order = OrderSnapshot(
                account_index=self.source_account_index,
                market_id=1,
                order_id=f"source-{child}",
                client_order_index=plan.client_order_index,
                status="open",
                side=plan.side,
                order_type="LIMIT",
                time_in_force="POST_ONLY",
                reduce_only=True,
                initial_quantity=plan.quantity,
                remaining_quantity=plan.quantity,
                filled_quantity=Decimal("0"),
                price=plan.price,
                observed_at=NOW,
            )
            return MutationReceipt(True, self.source_order.order_id, "0xsource")
        if self.reject_receiver:
            return MutationReceipt(False, None, None, "sequencer rejected")
        self.receiver_order = OrderSnapshot(
            account_index=self.receiver_account_index,
            market_id=1,
            order_id=f"receiver-{child}",
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
        self.source_order = replace(
            self.source_order,
            status="filled",
            remaining_quantity=Decimal("0"),
            filled_quantity=plan.quantity,
        )
        self.source_position -= plan.quantity
        self.receiver_position += plan.quantity
        self.child_number += 1
        return MutationReceipt(True, self.receiver_order.order_id, "0xreceiver")

    async def cancel_order(self, account_index, market_id, order_id):
        return MutationReceipt(True, order_id, "0xcancel")

    async def list_trades(self, account_index, market_id, *, order_id=None, cursor=None, limit=100):
        if self.source_order is None or self.receiver_order is None:
            return HistoryPage()
        if account_index == self.source_account_index and order_id == self.source_order.order_id:
            return HistoryPage(
                trades=(
                    TradeReceipt(
                        f"trade-source-{self.child_number}",
                        self.source_account_index,
                        1,
                        self.source_order.order_id,
                        "SELL",
                        self.source_order.filled_quantity,
                        self.source_order.price,
                        None,
                        self.receiver_account_index,
                        NOW,
                        counterparty_order_id=self.receiver_order.order_id,
                    ),
                )
            )
        if account_index == self.receiver_account_index and order_id == self.receiver_order.order_id:
            return HistoryPage(
                trades=(
                    TradeReceipt(
                        f"trade-receiver-{self.child_number}",
                        self.receiver_account_index,
                        1,
                        self.receiver_order.order_id,
                        "BUY",
                        self.receiver_order.filled_quantity,
                        self.receiver_order.price,
                        None,
                        self.source_account_index,
                        NOW,
                        counterparty_order_id=self.source_order.order_id,
                    ),
                )
            )
        return HistoryPage()


def test_robinhood_contract_rejects_wrong_domain_network_and_spot():
    with pytest.raises(ContractError, match="Robinhood Chain Lighter"):
        series_config(Path("/tmp/rh-series.jsonl"), api_base_url="https://mainnet.zklighter.elliot.ai")
    with pytest.raises(ContractError, match="466324"):
        series_config(Path("/tmp/rh-series.jsonl"), chain_id=304)
    assert series_config(Path("/tmp/rh-series.jsonl"), market_symbol="XRP").market_symbol == "XRP"
    xrp = replace(metadata(), market_id=99, symbol="XRP")
    RobinhoodSeriesEngine._validate_market(
        series_config(Path("/tmp/rh-series.jsonl"), market_symbol="XRP", market_id=99), xrp
    )
    with pytest.raises(PreflightBlocked, match="wrong symbol"):
        RobinhoodSeriesEngine._validate_market(series_config(Path("/tmp/rh-series.jsonl"), market_symbol="SOL"), xrp)
    with pytest.raises(ContractError, match="market_type must be perp"):
        replace(metadata(), market_type="spot")
    with pytest.raises(ContractError, match="only HOOD"):
        HandoffConfig(
            market_id=1,
            direction="LONG",
            quantity=Decimal("0.1"),
            source_limit_price=Decimal("100"),
            receiver_worst_price=Decimal("101"),
            freshness_seconds=10,
            request_timeout_seconds=1,
            order_timeout_seconds=1,
            reconcile_timeout_seconds=1,
            poll_interval_seconds=0.01,
            max_poll_count=1,
            source_order_lifetime_seconds=300,
            client_order_prefix="old",
            journal_path="/tmp/old.jsonl",
            market_symbol="BTC",
            api_base_url="https://mainnet.zklighter.elliot.ai",
            chain_id=304,
        )


def test_sizing_downsizes_depth_rounds_down_and_marks_minimum_remainder(tmp_path):
    config = series_config(tmp_path / "sizing.jsonl")
    item = book(("100.5", "0.37"), ("100.7", "0.18"))
    selected = size_next_slice(config, metadata(), item, remaining_quantity=Decimal("1"), now=NOW)
    assert selected.executable_depth == Decimal("0.55")
    assert selected.rounded_quantity == Decimal("0.5")
    assert selected.reason == ""
    remainder = size_next_slice(config, metadata(), book(("100.5", "0.04")), remaining_quantity=Decimal("0.04"), now=NOW)
    assert remainder.rounded_quantity == Decimal("0")
    assert "minimum" in remainder.reason or "size step" in remainder.reason
    with pytest.raises(Exception, match="duplicate"):
        OrderBookSnapshot(
            market_id=1,
            symbol="BTC",
            asks=(
                DepthLevel(Decimal("100"), Decimal("0.2"), "same"),
                DepthLevel(Decimal("101"), Decimal("0.2"), "same"),
            ),
            bids=(),
            observed_at=NOW,
        )


@pytest.mark.asyncio
async def test_series_advances_only_full_children_and_records_variable_sizes(tmp_path):
    client = SeriesClient([
        book(("100.5", "0.62")),
        book(("100.6", "0.30")),
        book(("100.7", "0.20")),
    ])
    result = await run_series(series_config(tmp_path / "series.jsonl"), client, clock=Clock())
    assert result.outcome is Outcome.SUCCESS
    assert result.completed_quantity == Decimal("1.0")
    assert result.remaining_quantity == Decimal("0")
    assert [plan.quantity for plan in client.submissions if plan.reduce_only] == [
        Decimal("0.5"), Decimal("0.3"), Decimal("0.2")
    ]
    assert sum((plan.quantity for plan in client.submissions if plan.reduce_only), Decimal("0")) <= Decimal("1.0")
    journal = (tmp_path / "series.jsonl").read_text()
    assert journal.count("CHILD_INTENT") == 3
    assert journal.count("CHILD_COMPLETE") == 3
    assert "SERIES_COMPLETE" in journal


@pytest.mark.asyncio
async def test_partial_child_stops_without_advancing_series(tmp_path):
    client = SeriesClient([book(("100.5", "1.0")), book(("100.5", "1.0"))], reject_receiver=True)
    result = await run_series(series_config(tmp_path / "partial.jsonl"), client, clock=Clock())
    assert result.outcome is Outcome.UNKNOWN
    assert result.completed_quantity == Decimal("0")
    assert len([plan for plan in client.submissions if plan.reduce_only]) == 1
    assert len([plan for plan in client.submissions if not plan.reduce_only]) == 1


@pytest.mark.asyncio
async def test_continuity_drift_stops_before_next_child(tmp_path):
    client = SeriesClient([book(("100.5", "0.5")), book(("100.5", "0.5"))], drift=True)
    result = await run_series(series_config(tmp_path / "drift.jsonl"), client, clock=Clock())
    assert result.outcome is Outcome.UNKNOWN
    assert result.completed_quantity == Decimal("0")
    assert len([plan for plan in client.submissions if plan.reduce_only]) == 1


@pytest.mark.asyncio
async def test_restart_child_intent_without_child_journal_never_starts_new_attempt(tmp_path):
    path = tmp_path / "restart.jsonl"
    config = series_config(path)
    client = SeriesClient([book(("100.5", "1.0"))])
    journal = DurableJournal(path, run_id="prior", clock=lambda: NOW)
    journal.acquire_attempt()
    try:
        journal.append("SERIES_STARTED", {"binding": config.binding(source_account_index=11, receiver_account_index=22)})
        journal.append(
            "CHILD_INTENT",
            {
                "child_index": 0,
                "child_journal_path": str(path) + ".child-0000",
                "quantity": "0.5",
            },
        )
    finally:
        journal.release_attempt()
    result = await run_series(config, client, clock=Clock())
    assert result.outcome is Outcome.UNKNOWN
    assert "no child journal" in result.reason
    assert client.submissions == []


@pytest.mark.asyncio
async def test_restart_preserves_completed_quantity_before_missing_next_child(tmp_path):
    path = tmp_path / "restart-after-child.jsonl"
    config = series_config(path)
    client = SeriesClient([book(("100.5", "1.0"))])
    journal = DurableJournal(path, run_id="prior-complete", clock=lambda: NOW)
    journal.acquire_attempt()
    try:
        journal.append(
            "SERIES_STARTED",
            {"binding": config.binding(source_account_index=11, receiver_account_index=22, resolved_market_id=1)},
        )
        journal.append(
            "CHILD_INTENT",
            {
                "child_index": 0,
                "child_journal_path": str(path) + ".child-0000",
                "quantity": "0.5",
            },
        )
        journal.append("CHILD_RESULT", {"child_index": 0, "outcome": "SUCCESS", "quantity": "0.5"})
        journal.append("CHILD_COMPLETE", {"child_index": 0, "quantity": "0.5"})
        journal.append(
            "CHILD_INTENT",
            {
                "child_index": 1,
                "child_journal_path": str(path) + ".child-0001",
                "quantity": "0.5",
            },
        )
    finally:
        journal.release_attempt()
    result = await run_series(config, client, clock=Clock())
    assert result.outcome is Outcome.UNKNOWN
    assert result.completed_quantity == Decimal("0.5")
    assert result.remaining_quantity == Decimal("0.5")
    assert "no child journal" in result.reason
    assert client.submissions == []


def test_stale_and_future_books_are_explicitly_rejected(tmp_path):
    config = series_config(tmp_path / "freshness.jsonl", freshness_seconds=1)
    with pytest.raises(Exception, match="stale"):
        size_next_slice(config, metadata(), book(("100.5", "0.5"), observed_at=NOW - 2), remaining_quantity=Decimal("1"), now=NOW)
    with pytest.raises(Exception, match="future"):
        size_next_slice(config, metadata(), book(("100.5", "0.5"), observed_at=NOW + 1), remaining_quantity=Decimal("1"), now=NOW)


@pytest.mark.asyncio
async def test_sdk_order_book_uses_required_bounded_limit(monkeypatch):
    class ExactOrderApi:
        calls = []

        def __init__(self, api_client):
            self.api_client = api_client

        async def order_book_orders(self, *, market_id, limit, _request_timeout):
            self.__class__.calls.append((market_id, limit, _request_timeout))
            return {
                "code": 200,
                "asks": [{"price": "100.5", "remaining_base_amount": "0.5", "order_id": "a1"}],
                "bids": [{"price": "100.0", "remaining_base_amount": "0.5", "order_id": "b1"}],
            }

    class Module:
        OrderApi = ExactOrderApi

        class Configuration:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        class ApiClient:
            def __init__(self, configuration):
                self.configuration = configuration

    config = HandoffConfig(
        market_id=1,
        direction=Direction.LONG,
        quantity=Decimal("0.1"),
        source_limit_price=Decimal("100.0"),
        receiver_worst_price=Decimal("101.0"),
        freshness_seconds=10,
        request_timeout_seconds=1,
        order_timeout_seconds=1,
        reconcile_timeout_seconds=1,
        poll_interval_seconds=0.01,
        max_poll_count=1,
        source_order_lifetime_seconds=300,
        client_order_prefix="sdk-rh",
        journal_path="/tmp/sdk-rh.jsonl",
        market_symbol="BTC",
        environment="robinhood",
        api_base_url="https://api.rh.lighter.xyz",
        chain_id=466324,
        api_key_index=4,
    )
    client = LighterSdkClient(
        config,
        source_account_index=11,
        receiver_account_index=22,
        secrets=StaticSecretProvider({11: "a", 22: "b"}),
        market_evidence={},
    )
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client._lighter = lambda: Module
    snapshot = await client.order_book(1)
    assert snapshot.symbol == "BTC"
    assert ExactOrderApi.calls == [(1, ROBINHOOD_ORDER_BOOK_LIMIT, 1)]


@pytest.mark.asyncio
async def test_restart_reconciles_persisted_effective_bound_without_new_mutation(tmp_path):
    class Crash(BaseException):
        pass

    class CrashAfterReceiver(SeriesClient):
        def __init__(self):
            super().__init__([book(("100.5", "1.0"))])
            self.crash = True
            self.cancel_calls = 0

        async def submit_order(self, plan):
            receipt = await super().submit_order(plan)
            if not plan.reduce_only and self.crash:
                self.crash = False
                raise Crash()
            return receipt

        async def cancel_order(self, account_index, market_id, order_id):
            self.cancel_calls += 1
            return await super().cancel_order(account_index, market_id, order_id)

    config = series_config(
        tmp_path / "restart-effective-bound.jsonl",
        allowed_price_deviation=Decimal("0.01"),
        receiver_worst_price=Decimal("105"),
    )
    client = CrashAfterReceiver()
    with pytest.raises(Crash):
        await run_series(config, client, clock=Clock())
    before_posts = len(client.submissions)
    before_cancels = client.cancel_calls
    assert client.submissions[1].price == Decimal("101.0")

    recovered = await run_series(config, client, clock=Clock())
    assert recovered.outcome is Outcome.UNKNOWN
    assert recovered.actual_filled_quantity == Decimal("0.5")
    assert recovered.actual_source_filled_quantity == Decimal("0.5")
    assert recovered.actual_receiver_filled_quantity == Decimal("0.5")
    assert recovered.children and recovered.children[0].source is not None
    assert recovered.children[0].receiver is not None
    assert len(client.submissions) == before_posts
    assert client.cancel_calls == before_cancels

    repeated = await run_series(config, client, clock=Clock())
    assert repeated.outcome is Outcome.UNKNOWN
    assert repeated.actual_filled_quantity == Decimal("0.5")
    assert repeated.actual_source_filled_quantity == Decimal("0.5")
    assert repeated.actual_receiver_filled_quantity == Decimal("0.5")
    assert len(client.submissions) == before_posts
    assert client.cancel_calls == before_cancels


@pytest.mark.asyncio
async def test_completed_restart_preserves_series_identity_and_totals(tmp_path):
    config = series_config(tmp_path / "completed-restart.jsonl")
    client = SeriesClient([book(("100.5", "1.0"))])
    first = await run_series(config, client, clock=Clock())
    before_posts = len(client.submissions)
    second = await run_series(config, client, clock=Clock())

    assert first.outcome is Outcome.SUCCESS
    assert second.outcome is Outcome.UNKNOWN
    assert second.series_id == first.series_id
    assert second.market_id == 1
    assert second.completed_quantity == Decimal("1.0")
    assert second.remaining_quantity == Decimal("0")
    assert second.actual_filled_quantity == Decimal("1.0")
    assert second.actual_source_filled_quantity == Decimal("1.0")
    assert second.actual_receiver_filled_quantity == Decimal("1.0")
    assert len(client.submissions) == before_posts


@pytest.mark.asyncio
async def test_repeated_reconciliation_preserves_prior_completed_slice(tmp_path):
    class Crash(BaseException):
        pass

    class CrashOnSecondChild(SeriesClient):
        def __init__(self):
            super().__init__([book(("100.5", "1.0"))])
            self.crash = True

        async def submit_order(self, plan):
            receipt = await super().submit_order(plan)
            if not plan.reduce_only and self.crash and self.child_number == 2:
                self.crash = False
                raise Crash()
            return receipt

    config = series_config(tmp_path / "repeat-prior-complete.jsonl")
    client = CrashOnSecondChild()
    with pytest.raises(Crash):
        await run_series(config, client, clock=Clock())
    before_posts = len(client.submissions)

    reconciled = await run_series(config, client, clock=Clock())
    assert reconciled.completed_quantity == Decimal("0.5")
    assert reconciled.actual_filled_quantity == Decimal("1.0")
    assert reconciled.actual_source_filled_quantity == Decimal("1.0")
    assert reconciled.actual_receiver_filled_quantity == Decimal("1.0")
    assert len(client.submissions) == before_posts

    repeated = await run_series(config, client, clock=Clock())
    assert repeated.completed_quantity == Decimal("0.5")
    assert repeated.actual_filled_quantity == Decimal("1.0")
    assert repeated.actual_source_filled_quantity == Decimal("1.0")
    assert repeated.actual_receiver_filled_quantity == Decimal("1.0")
    assert len(client.submissions) == before_posts


@pytest.mark.asyncio
async def test_partial_child_reports_cumulative_proven_leg_quantities(tmp_path, monkeypatch):
    config = series_config(tmp_path / "partial-cumulative.jsonl")
    client = SeriesClient([book(("100.5", "1.0")), book(("100.5", "1.0"))])
    original_run_handoff = series_module.run_handoff
    calls = 0

    def leg(account_index, quantity, order_id):
        return LegReconciliation(
            account_index=account_index,
            order_id=order_id,
            trades=(
                TradeReceipt(
                    trade_id=f"trade-{order_id}",
                    account_index=account_index,
                    market_id=1,
                    order_id=order_id,
                    side="SELL" if account_index == 11 else "BUY",
                    quantity=quantity,
                    price=Decimal("100"),
                    fee=None,
                    counterparty_account_index=22 if account_index == 11 else 11,
                    observed_at=NOW,
                ),
            ),
            position_before=Decimal("0"),
            position_after=Decimal("0"),
            order=None,
            history_complete=True,
        )

    async def fake_run_handoff(child_config, child_client, *, clock):
        nonlocal calls
        calls += 1
        if calls == 1:
            return await original_run_handoff(child_config, child_client, clock=clock)
        return HandoffResult(
            outcome=Outcome.PARTIAL,
            phase=Phase.RECONCILIATION,
            run_id="partial-child",
            plan=None,
            source=leg(11, Decimal("0.2"), "partial-source"),
            receiver=leg(22, Decimal("0.1"), "partial-receiver"),
            reason="receiver partial",
            unknown_reasons=("receiver partial",),
        )

    monkeypatch.setattr(series_module, "run_handoff", fake_run_handoff)
    result = await run_series(config, client, clock=Clock())

    assert result.outcome is Outcome.PARTIAL
    assert result.completed_quantity == Decimal("0.5")
    assert result.actual_source_filled_quantity == Decimal("0.7")
    assert result.actual_receiver_filled_quantity == Decimal("0.6")
    assert result.actual_filled_quantity == Decimal("0.6")


@pytest.mark.asyncio
async def test_mapping_book_preserves_observation_and_identity(tmp_path):
    class MappingClient(SeriesClient):
        def __init__(self, payload):
            super().__init__([book(("100.5", "1.0"))])
            self.payload = payload

        async def order_book(self, market_id):
            return self.payload

    stale = book(("100.5", "1.0"), observed_at=NOW - 100).as_dict()
    stale_client = MappingClient(stale)
    stale_result = await run_series(
        series_config(tmp_path / "stale-mapping.jsonl", freshness_seconds=10),
        stale_client,
        clock=Clock(),
    )
    assert stale_result.outcome is Outcome.UNKNOWN
    assert stale_result.reason == "contract_error"
    assert stale_client.submissions == []

    future = dict(book(("100.5", "1.0"), observed_at=NOW + 1).as_dict())
    future_client = MappingClient(future)
    future_result = await run_series(
        series_config(tmp_path / "future-mapping.jsonl"),
        future_client,
        clock=Clock(),
    )
    assert future_result.outcome is Outcome.UNKNOWN
    assert future_client.submissions == []

    contradictory = dict(book(("100.5", "1.0")).as_dict())
    contradictory["symbol"] = "ETH"
    contradictory_client = MappingClient(contradictory)
    contradictory_result = await run_series(
        series_config(tmp_path / "contradictory-mapping.jsonl"),
        contradictory_client,
        clock=Clock(),
    )
    assert contradictory_result.outcome is Outcome.UNKNOWN
    assert contradictory_client.submissions == []

    missing_timestamp = dict(book(("100.5", "1.0")).as_dict())
    del missing_timestamp["observed_at"]
    missing_client = MappingClient(missing_timestamp)
    missing_result = await run_series(
        series_config(tmp_path / "missing-timestamp.jsonl"),
        missing_client,
        clock=Clock(),
    )
    assert missing_result.outcome is Outcome.UNKNOWN
    assert missing_client.submissions == []
