from __future__ import annotations

import asyncio
from dataclasses import replace
from decimal import Decimal
import json
from pathlib import Path
import time
from time import perf_counter
from types import SimpleNamespace

import pytest

from risex_spread_shadow.hood_handoff import (
    AccountSnapshot,
    DepthLevel,
    Direction,
    HandoffConfig,
    HandoffEngine,
    HistoryPage,
    LighterSdkClient,
    MarketMetadata,
    MutationReceipt,
    OperationMode,
    OrderBookSnapshot,
    OrderSnapshot,
    Outcome,
    OrderPlan,
    StaticSecretProvider,
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

    def __init__(
        self,
        *,
        source_fills_before_receiver: bool = False,
        source_margin: Decimal = Decimal("100"),
        receiver_margin: Decimal = Decimal("100"),
        receiver_fill_quantity: Decimal | None = None,
    ) -> None:
        self.source_position = Decimal("0")
        self.receiver_position = Decimal("0")
        self.source_margin = source_margin
        self.receiver_margin = receiver_margin
        self.receiver_fill_quantity = receiver_fill_quantity
        self.source_order: OrderSnapshot | None = None
        self.receiver_order: OrderSnapshot | None = None
        self.submissions = []
        self.cancellations = []
        self.source_fills_before_receiver = source_fills_before_receiver
        self.child_number = 0

    async def market_metadata(self, market_id: int) -> MarketMetadata:
        assert market_id == 1
        return metadata()

    async def order_book(self, market_id: int) -> OrderBookSnapshot:
        assert market_id == 1
        source = self.source_order
        if source is None or not source.active:
            source_level = DepthLevel(Decimal("100.0"), Decimal("0.20"), "source-preview", self.source_account_index)
            source_side = source.side if source is not None else "SELL"
        else:
            source_level = DepthLevel(
                source.price,
                source.remaining_quantity,
                source.order_id,
                source.account_index,
            )
            source_side = source.side
        if source_side == "SELL":
            asks = (source_level,)
            bids = (DepthLevel(Decimal("99.0"), Decimal("1"), "bid-guard", 999),)
        else:
            bids = (source_level,)
            asks = (DepthLevel(Decimal("101.0"), Decimal("1"), "ask-guard", 999),)
        return OrderBookSnapshot(
            market_id=1,
            symbol="BTC",
            bids=bids,
            asks=asks,
            observed_at=NOW,
            market_type="perp",
            venue="robinhood",
        )

    async def account_snapshot(self, account_index: int, market_id: int) -> AccountSnapshot:
        assert market_id == 1
        position = self.source_position if account_index == self.source_account_index else self.receiver_position
        margin = self.source_margin if account_index == self.source_account_index else self.receiver_margin
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
                reduce_only=plan.reduce_only,
                initial_quantity=plan.quantity,
                remaining_quantity=Decimal("0") if self.source_fills_before_receiver else plan.quantity,
                filled_quantity=plan.quantity if self.source_fills_before_receiver else Decimal("0"),
                price=plan.price,
                observed_at=NOW,
            )
            if self.source_fills_before_receiver:
                self.source_position += plan.quantity if plan.side == "BUY" else -plan.quantity
            return MutationReceipt(True, self.source_order.order_id, "0xsource")
        assert plan.account_index == self.receiver_account_index
        fill_quantity = (
            plan.quantity
            if self.receiver_fill_quantity is None
            else self.receiver_fill_quantity
        )
        assert Decimal("0") < fill_quantity <= plan.quantity
        self.receiver_order = OrderSnapshot(
            account_index=22,
            market_id=1,
            order_id=f"receiver-{self.child_number}",
            client_order_index=plan.client_order_index,
            status="filled",
            side=plan.side,
            order_type="MARKET",
            time_in_force="IOC",
            reduce_only=plan.reduce_only,
            initial_quantity=plan.quantity,
            remaining_quantity=plan.quantity - fill_quantity,
            filled_quantity=fill_quantity,
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
                "status": "filled" if fill_quantity == plan.quantity else "open",
                "side": self.source_order.side,
                "order_type": self.source_order.order_type,
                "time_in_force": self.source_order.time_in_force,
                "reduce_only": self.source_order.reduce_only,
                "initial_quantity": self.source_order.initial_quantity,
                "remaining_quantity": plan.quantity - fill_quantity,
                "filled_quantity": fill_quantity,
                "price": self.source_order.price,
                "observed_at": NOW,
            }
        )
        self.source_position += fill_quantity if self.source_order.side == "BUY" else -fill_quantity
        self.receiver_position += fill_quantity if plan.side == "BUY" else -fill_quantity
        self.child_number += 1
        return MutationReceipt(True, self.receiver_order.order_id, "0xreceiver")

    async def cancel_order(self, account_index: int, market_id: int, order_id: str) -> MutationReceipt:
        self.cancellations.append(order_id)
        if self.source_order is not None and self.source_order.order_id == order_id and self.source_order.active:
            self.source_order = OrderSnapshot(
                account_index=self.source_order.account_index,
                market_id=self.source_order.market_id,
                order_id=self.source_order.order_id,
                client_order_index=self.source_order.client_order_index,
                status="canceled",
                side=self.source_order.side,
                order_type=self.source_order.order_type,
                time_in_force=self.source_order.time_in_force,
                reduce_only=self.source_order.reduce_only,
                initial_quantity=self.source_order.initial_quantity,
                remaining_quantity=Decimal("0"),
                filled_quantity=self.source_order.filled_quantity,
                price=self.source_order.price,
                observed_at=NOW,
            )
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
                        self.source_order.side,
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
                        self.receiver_order.side,
                        self.receiver_order.filled_quantity,
                        self.source_order.price,
                        None,
                        11,
                        NOW,
                        counterparty_order_id=self.source_order.order_id,
                    ),
                )
            )
        return HistoryPage()


class PreparedPairedClient(PairedClient):
    """Synthetic prepared-dispatch client for fast-path ordering barriers."""

    def __init__(self, *, ambiguous_source: bool = False) -> None:
        super().__init__()
        self.ambiguous_source = ambiguous_source
        self.preparation_events: list[tuple[str, int]] = []
        self.prepared: list[dict[str, object]] = []
        self.dispatch_deadlines: list[tuple[int, float | None, float | None]] = []

    async def prepare_order(self, plan):
        self.preparation_events.append(("prepare", plan.account_index))
        token = {"plan": plan, "state": "READY"}
        self.prepared.append(token)
        return token

    async def submit_prepared_order(self, plan, prepared, *, deadline=None):
        self.preparation_events.append(("dispatch", plan.account_index))
        assert prepared["plan"] == plan
        self.dispatch_deadlines.append(
            (plan.account_index, plan.mutation_deadline_monotonic, deadline)
        )
        if prepared["state"] != "READY":
            return MutationReceipt(False, None, None, "prepared token was already consumed")
        prepared["state"] = "CONSUMED"
        if self.ambiguous_source and plan.account_index == self.source_account_index:
            raise TimeoutError("synthetic send ambiguity")
        return await super().submit_order(plan)

    async def invalidate_prepared_order(self, prepared) -> None:
        if prepared["state"] == "READY":
            prepared["state"] = "INVALIDATED"


class DelayedPreparedPairedClient(PreparedPairedClient):
    """Add independent preparation delay so overlap and quote age are measured."""

    def __init__(self, *, delay: float) -> None:
        super().__init__()
        self.delay = delay
        self.prepare_started: list[tuple[int, float]] = []

    async def prepare_order(self, plan):
        self.prepare_started.append((plan.account_index, perf_counter()))
        await asyncio.sleep(self.delay)
        return await super().prepare_order(plan)


class LegacyPreparedPairedClient(PreparedPairedClient):
    """Keep the generic two-argument prepared submission surface working."""

    async def submit_prepared_order(self, plan, prepared):
        return await super().submit_prepared_order(plan, prepared)


class DelayedPairedClient(PairedClient):
    """Apply the same synthetic read delay to fast and serial controls."""

    def __init__(self, *, delay: float) -> None:
        super().__init__()
        self.delay = delay
        self.read_counts = {"account": 0, "book": 0, "lookup": 0}

    async def account_snapshot(self, account_index: int, market_id: int) -> AccountSnapshot:
        self.read_counts["account"] += 1
        await asyncio.sleep(self.delay)
        return await super().account_snapshot(account_index, market_id)

    async def order_book(self, market_id: int) -> OrderBookSnapshot:
        self.read_counts["book"] += 1
        await asyncio.sleep(self.delay)
        return await super().order_book(market_id)

    async def lookup_order(self, account_index: int, market_id: int, *, order_id=None, client_order_index=None):
        self.read_counts["lookup"] += 1
        await asyncio.sleep(self.delay)
        return await super().lookup_order(
            account_index,
            market_id,
            order_id=order_id,
            client_order_index=client_order_index,
        )


class PropagatingPreparedPairedClient(PreparedPairedClient):
    """Delay source visibility while early account/book reads remain causal."""

    def __init__(
        self,
        *,
        closing: bool = False,
        foreign_conflict: bool = False,
        adverse: str | None = None,
    ) -> None:
        super().__init__()
        if closing:
            self.source_position = Decimal("0.20")
            self.receiver_position = Decimal("-0.20")
        self.closing = closing
        self.foreign_conflict = foreign_conflict
        self.adverse = adverse
        self.source_visible = False
        self.source_dispatch_started = False
        self.source_lookup_calls = 0
        self.read_counts = {"account": 0, "book": 0, "lookup": 0}

    def _foreign_order(self) -> OrderSnapshot:
        return OrderSnapshot(
            account_index=self.source_account_index,
            market_id=1,
            order_id="foreign-active-order",
            client_order_index=999,
            status="open",
            side="SELL",
            order_type="LIMIT",
            time_in_force="POST_ONLY",
            reduce_only=self.closing,
            initial_quantity=Decimal("0.10"),
            remaining_quantity=Decimal("0.10"),
            filled_quantity=Decimal("0"),
            price=Decimal("99.0"),
            observed_at=NOW,
        )

    async def account_snapshot(self, account_index: int, market_id: int) -> AccountSnapshot:
        self.read_counts["account"] += 1
        if not self.source_visible and self.source_dispatch_started:
            if account_index == self.source_account_index:
                active = (self._foreign_order(),) if self.foreign_conflict else ()
                snapshot = _account_snapshot(account_index, self.source_position, active)
                if self.adverse == "source_identity":
                    return replace(
                        snapshot,
                        account_index=999,
                        source_identity="foreign-source",
                    )
                if self.adverse == "source_position":
                    return replace(
                        snapshot,
                        signed_position=self.source_position + Decimal("0.10"),
                    )
                if self.adverse == "complete":
                    return await super().account_snapshot(account_index, market_id)
                return snapshot
            if account_index == self.receiver_account_index:
                snapshot = _account_snapshot(account_index, self.receiver_position)
                if self.adverse == "receiver_identity":
                    return replace(
                        snapshot,
                        account_index=998,
                        source_identity="foreign-receiver",
                    )
                return snapshot
        return await super().account_snapshot(account_index, market_id)

    async def order_book(self, market_id: int) -> OrderBookSnapshot:
        self.read_counts["book"] += 1
        value = await super().order_book(market_id)
        if self.source_visible or self.adverse == "complete":
            return value
        # A source order accepted by the sequencer may be absent from the
        # first public snapshot.  Leave only a neutral guard level here; the
        # post-visibility refresh must recover the exact owner-bound level.
        return replace(
            value,
            asks=(DepthLevel(Decimal("101.0"), Decimal("1"), "ask-guard", 999),),
            bids=(DepthLevel(Decimal("99.0"), Decimal("1"), "bid-guard", 999),),
        )

    async def lookup_order(
        self,
        account_index: int,
        market_id: int,
        *,
        order_id=None,
        client_order_index=None,
    ):
        self.read_counts["lookup"] += 1
        if account_index == self.source_account_index and order_id is not None:
            self.source_lookup_calls += 1
            if self.source_lookup_calls == 1:
                await asyncio.sleep(0.02)
                self.source_visible = True
        return await super().lookup_order(
            account_index,
            market_id,
            order_id=order_id,
            client_order_index=client_order_index,
        )

    async def submit_prepared_order(self, plan, prepared, *, deadline=None):
        if plan.account_index == self.source_account_index:
            self.source_dispatch_started = True
        return await super().submit_prepared_order(plan, prepared, deadline=deadline)


class DelayedLegacyPreparedPairedClient(LegacyPreparedPairedClient):
    """Two-argument adapter whose mutation crosses the final deadline."""

    def __init__(self, *, delay: float) -> None:
        super().__init__()
        self.delay = delay

    async def submit_prepared_order(self, plan, prepared):
        if plan.account_index == self.receiver_account_index:
            await asyncio.sleep(self.delay)
        return await super().submit_prepared_order(plan, prepared)


class NoBookPairedClient(PairedClient):
    """Model a paired adapter that cannot provide the mandatory fresh book."""

    order_book = None


class AdverseBookPairedClient(PairedClient):
    """Keep the source visible while varying only the public guard evidence."""

    def __init__(self, mode: str) -> None:
        super().__init__()
        self.mode = mode

    def _replace_source_level(self, book: OrderBookSnapshot, level: DepthLevel) -> OrderBookSnapshot:
        assert self.source_order is not None
        if self.source_order.side == "SELL":
            return replace(book, asks=(level, *book.asks[1:]))
        return replace(book, bids=(level, *book.bids[1:]))

    async def order_book(self, market_id: int) -> OrderBookSnapshot:
        book = await super().order_book(market_id)
        if self.mode == "stale":
            return replace(book, observed_at=NOW - 100)
        if self.mode == "future":
            return replace(book, observed_at=NOW + 1)
        assert self.source_order is not None
        source = self.source_order
        if self.mode == "exact_absent":
            return self._replace_source_level(
                book,
                DepthLevel(source.price, source.remaining_quantity, "foreign-source", 999),
            )
        if self.mode == "anonymous_same_price":
            level = DepthLevel(source.price, Decimal("0.05"), "anonymous-same-price", None)
            if source.side == "SELL":
                return replace(book, asks=(*book.asks, level))
            return replace(book, bids=(*book.bids, level))
        if self.mode == "external_same_price":
            level = DepthLevel(source.price, Decimal("0.05"), "external-same-price", 999)
            if source.side == "SELL":
                return replace(book, asks=(*book.asks, level))
            return replace(book, bids=(*book.bids, level))
        if self.mode == "wrong_owner":
            return self._replace_source_level(
                book,
                DepthLevel(source.price, source.remaining_quantity, source.order_id, 999),
            )
        if self.mode == "wrong_order_id":
            return self._replace_source_level(
                book,
                DepthLevel(source.price, source.remaining_quantity, "wrong-source-id", source.account_index),
            )
        if self.mode == "wrong_quantity":
            return self._replace_source_level(
                book,
                DepthLevel(source.price, source.remaining_quantity - Decimal("0.01"), source.order_id, source.account_index),
            )
        raise AssertionError(f"unknown adverse book mode: {self.mode}")


class WrongExactLookupPairedClient(PairedClient):
    """Return a conflicting identity only on the exact pre-receiver lookup."""

    def __init__(self) -> None:
        super().__init__()
        self.source_lookup_calls = 0

    async def lookup_order(self, account_index: int, market_id: int, *, order_id=None, client_order_index=None):
        value = await super().lookup_order(
            account_index,
            market_id,
            order_id=order_id,
            client_order_index=client_order_index,
        )
        if account_index == self.source_account_index and order_id is not None and value is not None:
            self.source_lookup_calls += 1
            if self.source_lookup_calls == 2:
                return replace(value, order_id="conflicting-exact-order-id")
        return value


class ExpiringBookClock:
    """Advance only across the exact source lookup boundary."""

    def __init__(self) -> None:
        self.value = NOW + 9.9

    def now(self) -> float:
        return self.value

    async def sleep(self, seconds: float) -> None:
        return None


class ExpiringBookPairedClient(PairedClient):
    """Return a book fresh at receipt but stale after the exact lookup."""

    def __init__(self, clock: ExpiringBookClock) -> None:
        super().__init__()
        self.clock = clock
        self.source_lookup_calls = 0

    async def order_book(self, market_id: int) -> OrderBookSnapshot:
        value = await super().order_book(market_id)
        return replace(value, observed_at=self.clock.now() - 9.9)

    async def lookup_order(self, account_index: int, market_id: int, *, order_id=None, client_order_index=None):
        value = await super().lookup_order(
            account_index,
            market_id,
            order_id=order_id,
            client_order_index=client_order_index,
        )
        if account_index == self.source_account_index and order_id is not None and value is not None:
            self.source_lookup_calls += 1
            if self.source_lookup_calls == 2:
                self.clock.value += 0.2
        return value


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
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
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
        if self.source_order is not None and self.source_order.active:
            return await super().order_book(market_id)
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
async def test_partial_receiver_cancels_after_one_fresh_exact_source_read(tmp_path):
    class CountingClient(PairedClient):
        def __init__(self):
            super().__init__(receiver_fill_quantity=Decimal("0.10"))
            self.post_receiver_source_reads = 0
            self.reads_at_cancel = None

        async def lookup_order(self, account_index, market_id, *, order_id=None, client_order_index=None):
            if self.receiver_order is not None and account_index == self.source_account_index and self.reads_at_cancel is None:
                self.post_receiver_source_reads += 1
            return await super().lookup_order(account_index, market_id, order_id=order_id, client_order_index=client_order_index)

        async def cancel_order(self, account_index, market_id, order_id):
            self.reads_at_cancel = self.post_receiver_source_reads
            return await super().cancel_order(account_index, market_id, order_id)

    client = CountingClient()
    result = await run_handoff(config(tmp_path / "one-source-read.jsonl"), client, clock=Clock())
    assert client.cancellations == ["source-0"]
    assert client.reads_at_cancel == 1
    assert result.source.filled_quantity == Decimal("0.10")
    assert result.receiver.filled_quantity == Decimal("0.10")


@pytest.mark.asyncio
async def test_prepared_pair_is_bound_before_source_exposure_and_receiver_dispatch_is_later(tmp_path):
    client = PreparedPairedClient()
    result = await run_handoff(config(tmp_path / "prepared-pair.jsonl"), client, clock=Clock())

    assert result.outcome is Outcome.SUCCESS
    assert client.preparation_events[:3] == [
        ("prepare", client.source_account_index),
        ("prepare", client.receiver_account_index),
        ("dispatch", client.source_account_index),
    ]
    assert client.preparation_events[-1] == ("dispatch", client.receiver_account_index)
    assert [token["state"] for token in client.prepared] == ["CONSUMED", "CONSUMED"]
    receiver_deadline = client.dispatch_deadlines[-1]
    assert receiver_deadline[0] == client.receiver_account_index
    assert receiver_deadline[2] <= receiver_deadline[1]
    assert not [
        event
        for event in client.preparation_events
        if event == ("prepare", client.receiver_account_index)
    ][1:]


@pytest.mark.asyncio
async def test_prepared_pair_keeps_two_argument_generic_client_compatibility(tmp_path):
    client = LegacyPreparedPairedClient()
    result = await run_handoff(config(tmp_path / "prepared-legacy.jsonl"), client, clock=Clock())

    assert result.outcome is Outcome.SUCCESS
    assert client.dispatch_deadlines[-1][2] is None


@pytest.mark.asyncio
async def test_prepared_pair_overlaps_independent_work_and_records_source_quote_age(tmp_path):
    delay = 0.03
    client = DelayedPreparedPairedClient(delay=delay)
    result = await run_handoff(
        config(
            tmp_path / "prepared-overlap.jsonl",
            source_quote_observed_at=NOW - 0.25,
        ),
        client,
        clock=Clock(),
    )

    assert result.outcome is Outcome.SUCCESS, result.as_dict()
    assert len(client.prepare_started) == 2
    assert max(at for _, at in client.prepare_started) - min(at for _, at in client.prepare_started) < delay * 0.5
    assert result.latency["paired_preparation_seconds"] < delay * 1.8
    assert result.latency["source_quote_age_seconds"] == pytest.approx(0.25)
    assert result.latency["quote_age_to_source_dispatch_seconds"] == pytest.approx(0.25)
    assert result.latency["source_to_receiver_intent_seconds"] >= 0
    assert result.latency["source_ack_to_receiver_intent_seconds"] >= 0
    assert result.latency["receiver_admission_seconds"] >= 0
    assert result.latency["receiver_dispatch_ack_at"] >= result.latency["receiver_dispatch_intent_at"]
    assert result.latency["receiver_fill_observed_at"] >= 0
    assert result.latency["receiver_visibility_seconds"] >= 0


@pytest.mark.asyncio
async def test_ambiguous_prepared_source_send_consumes_source_and_invalidates_receiver(tmp_path):
    client = PreparedPairedClient(ambiguous_source=True)
    result = await run_handoff(config(tmp_path / "prepared-ambiguous.jsonl"), client, clock=Clock())

    assert result.outcome is Outcome.UNKNOWN
    assert client.preparation_events == [
        ("prepare", client.source_account_index),
        ("prepare", client.receiver_account_index),
        ("dispatch", client.source_account_index),
    ]
    assert [token["state"] for token in client.prepared] == ["CONSUMED", "INVALIDATED"]
    assert not [plan for plan in client.submissions if plan.account_index == client.receiver_account_index]


@pytest.mark.asyncio
async def test_coalesced_source_visibility_reduces_window_without_reducing_reads(monkeypatch, tmp_path):
    delay = 0.03
    fast_client = DelayedPairedClient(delay=delay)
    fast_result = await run_handoff(
        config(tmp_path / "coalesced.jsonl"),
        fast_client,
        clock=Clock(),
    )

    async def serial_source_visibility_and_checks(self, config, plan, receipt, journal, run_id):
        started = perf_counter()
        source_order = await self._poll_order(
            plan,
            receipt.order_id,
            journal,
            run_id,
            require_terminal=False,
        )
        checks = await self._parallel_pre_receiver_checks(config)
        return source_order, checks, max(0.0, perf_counter() - started)

    monkeypatch.setattr(
        HandoffEngine,
        "_parallel_source_visibility_and_checks",
        serial_source_visibility_and_checks,
    )
    serial_client = DelayedPairedClient(delay=delay)
    serial_result = await run_handoff(
        config(tmp_path / "serial.jsonl"),
        serial_client,
        clock=Clock(),
    )

    assert fast_result.outcome is Outcome.SUCCESS
    assert serial_result.outcome is Outcome.SUCCESS
    assert fast_client.read_counts == serial_client.read_counts
    fast_window = fast_result.latency["coalesced_pre_receiver_window_seconds"]
    serial_window = serial_result.latency["coalesced_pre_receiver_window_seconds"]
    assert serial_window > fast_window + delay * 0.5


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("closing", "foreign_conflict"),
    [(False, False), (True, False), (False, True)],
)
async def test_source_propagation_refreshes_only_causal_absence_for_open_and_close(
    tmp_path,
    closing,
    foreign_conflict,
):
    client = PropagatingPreparedPairedClient(
        closing=closing,
        foreign_conflict=foreign_conflict,
    )
    operation_mode = OperationMode.PAIRED_CLOSING if closing else OperationMode.PAIRED_OPENING
    result = await run_handoff(
        config(
            tmp_path / f"propagation-{closing}-{foreign_conflict}.jsonl",
            operation_mode=operation_mode,
        ),
        client,
        clock=Clock(),
    )

    if foreign_conflict:
        assert result.outcome is Outcome.UNKNOWN, result.as_dict()
        assert any("additional or conflicting active order" in reason for reason in result.unknown_reasons)
        assert [plan.account_index for plan in client.submissions] == [client.source_account_index]
        assert result.latency.get("pre_visibility_refresh_seconds") == 0.0
    else:
        assert result.outcome is Outcome.SUCCESS, result.as_dict()
        assert [plan.account_index for plan in client.submissions] == [
            client.source_account_index,
            client.receiver_account_index,
        ]
        assert result.latency["pre_visibility_refresh_seconds"] > 0
        assert client.read_counts["account"] >= 6
        assert client.read_counts["book"] >= 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("adverse", "reason_fragment"),
    [
        ("source_identity", "source recheck account/market identity conflicts"),
        ("receiver_identity", "receiver recheck account/market identity conflicts"),
        ("source_position", "source position changed"),
    ],
)
async def test_pre_visibility_adverse_account_evidence_is_sticky(
    tmp_path,
    adverse,
    reason_fragment,
):
    client = PropagatingPreparedPairedClient(adverse=adverse)
    path = tmp_path / f"adverse-{adverse}.jsonl"
    result = await run_handoff(config(path), client, clock=Clock())

    assert result.outcome is Outcome.UNKNOWN, result.as_dict()
    assert any(reason_fragment in reason for reason in result.unknown_reasons)
    assert [plan.account_index for plan in client.submissions] == [client.source_account_index]
    assert result.latency["pre_visibility_refresh_seconds"] == 0.0
    guard = _guard_payload(path)
    first = guard["causal_pre_visibility_observations"]
    if adverse == "source_identity":
        assert first["source"]["account_index"] == 999
        assert first["source"]["source_identity"] == "foreign-source"
    elif adverse == "receiver_identity":
        assert first["receiver"]["account_index"] == 998
        assert first["receiver"]["source_identity"] == "foreign-receiver"
    else:
        assert first["source"]["signed_position"] == "0.10"


@pytest.mark.asyncio
async def test_pre_visibility_complete_owner_book_evidence_keeps_fast_path(tmp_path):
    client = PropagatingPreparedPairedClient(adverse="complete")
    result = await run_handoff(
        config(tmp_path / "complete-evidence.jsonl"),
        client,
        clock=Clock(),
    )

    assert result.outcome is Outcome.SUCCESS, result.as_dict()
    assert result.latency["pre_visibility_refresh_seconds"] == 0.0
    assert client.read_counts["book"] == 1
    assert [plan.account_index for plan in client.submissions] == [11, 22]


def _prepared_test_plan(*, account_index: int, deadline: float) -> OrderPlan:
    return OrderPlan(
        account_index=account_index,
        market_id=1,
        side="SELL" if account_index == 11 else "BUY",
        quantity=Decimal("0.20"),
        quantity_int=20,
        price=Decimal("100.0"),
        price_int=1000,
        order_type="LIMIT",
        time_in_force="POST_ONLY",
        reduce_only=False,
        order_expiry_ms=1_500_000,
        client_order_index=account_index,
        mutation_deadline_monotonic=deadline,
    )


@pytest.mark.asyncio
async def test_two_argument_prepared_submission_checks_expired_deadline_before_invocation():
    client = LegacyPreparedPairedClient()
    engine = HandoffEngine(client, clock=Clock())
    plan = _prepared_test_plan(account_index=11, deadline=time.monotonic() - 1)
    prepared = await client.prepare_order(plan)

    with pytest.raises(TimeoutError, match="final mutation barrier"):
        await engine._submit_order(
            plan,
            prepared=prepared,
            final_deadline=plan.mutation_deadline_monotonic,
        )
    assert client.preparation_events == [("prepare", client.source_account_index)]


@pytest.mark.asyncio
async def test_two_argument_prepared_submission_is_bounded_across_await():
    client = DelayedLegacyPreparedPairedClient(delay=0.05)
    engine = HandoffEngine(client, clock=Clock())
    plan = _prepared_test_plan(account_index=22, deadline=time.monotonic() + 0.01)
    prepared = await client.prepare_order(plan)

    with pytest.raises(TimeoutError, match="final mutation barrier"):
        await engine._submit_order(
            plan,
            prepared=prepared,
            final_deadline=plan.mutation_deadline_monotonic,
        )
    assert client.preparation_events == [("prepare", client.receiver_account_index)]


def _guard_payload(path: Path) -> dict:
    rows = [
        json.loads(line)
        for line in path.read_text().splitlines()
    ]
    guards = [row["payload"] for row in rows if row["event"] == "PRE_RECEIVER_GUARD"]
    assert len(guards) == 1
    return guards[0]


@pytest.mark.asyncio
async def test_paired_guard_requires_a_fresh_reader_and_never_dispatches_receiver(tmp_path):
    client = NoBookPairedClient()
    result = await run_handoff(config(tmp_path / "no-book.jsonl"), client, clock=Clock())

    assert result.outcome is Outcome.UNKNOWN
    assert [item.account_index for item in client.submissions] == [11]
    assert client.cancellations == ["source-0"]
    guard = _guard_payload(tmp_path / "no-book.jsonl")
    assert guard["status"] == "UNKNOWN"
    assert guard["book_observed_at"] is None
    assert guard["source_public_level"] is None
    assert any("unresolved" in reason for reason in result.unknown_reasons)
    assert not [item for item in client.submissions if item.account_index == client.receiver_account_index]


@pytest.mark.asyncio
async def test_paired_guard_rechecks_book_freshness_after_exact_source_lookup(tmp_path):
    clock = ExpiringBookClock()
    client = ExpiringBookPairedClient(clock)
    path = tmp_path / "book-expires-during-lookup.jsonl"

    result = await run_handoff(config(path), client, clock=clock)

    assert result.outcome is Outcome.UNKNOWN
    assert client.source_lookup_calls >= 2
    assert [item.account_index for item in client.submissions] == [11]
    assert not [item for item in client.submissions if item.account_index == client.receiver_account_index]
    assert any("public book recheck is stale" in reason for reason in result.unknown_reasons)
    guard = _guard_payload(path)
    assert guard["book_observed_at"] == pytest.approx(NOW)
    assert guard["status"] == "UNKNOWN"
    assert any("public book recheck is stale" in reason for reason in guard["admission_reasons"])


@pytest.mark.asyncio
async def test_receiver_deadline_uses_wall_clock_at_plan_construction(monkeypatch, tmp_path):
    import risex_spread_shadow.hood_handoff.engine as engine_module

    class MutableClock:
        def __init__(self) -> None:
            self.value = NOW

        def now(self) -> float:
            return self.value

        async def sleep(self, seconds: float) -> None:
            return None

    clock = MutableClock()
    client = PairedClient()
    ticks = [500.0]
    original_time = engine_module.time
    original_mutation_plan = engine_module.HandoffEngine._mutation_plan
    calls: list[tuple[object, float]] = []
    monkeypatch.setattr(
        engine_module,
        "time",
        SimpleNamespace(
            monotonic=lambda: ticks[0],
            perf_counter=original_time.perf_counter,
            time=original_time.time,
        ),
    )

    def delayed_mutation_plan(self, plan, config, *, observations, observation_now=None):
        if plan.account_index == client.receiver_account_index:
            # This is after the guard and immediately before the real plan
            # construction.  The method must not reuse an earlier guard time.
            clock.value += 0.2
            calls.append((observation_now, clock.now()))
        return original_mutation_plan(
            self,
            plan,
            config,
            observations=observations,
            observation_now=observation_now,
        )

    monkeypatch.setattr(engine_module.HandoffEngine, "_mutation_plan", delayed_mutation_plan)
    result = await run_handoff(
        config(
            tmp_path / "deadline-at-plan.jsonl",
            request_timeout_seconds=20,
        ),
        client,
        clock=clock,
    )

    assert result.outcome is Outcome.SUCCESS
    assert calls == [(None, NOW + 0.2)]
    receiver_plan = client.submissions[1]
    assert receiver_plan.mutation_deadline_monotonic == pytest.approx(500.0 + 9.8)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode",
    [
        "exact_absent",
        "stale",
        "future",
        "anonymous_same_price",
        "external_same_price",
        "wrong_owner",
        "wrong_order_id",
        "wrong_quantity",
    ],
)
async def test_paired_guard_rejects_incomplete_or_ambiguous_source_public_evidence(tmp_path, mode):
    client = AdverseBookPairedClient(mode)
    path = tmp_path / f"{mode}.jsonl"
    result = await run_handoff(config(path), client, clock=Clock())

    if mode in {"stale", "future"}:
        assert result.outcome is Outcome.UNKNOWN
        assert not result.retryable_pair
    else:
        assert result.outcome is Outcome.PARTIAL
        assert result.retryable_pair
    assert [item.account_index for item in client.submissions] == [11]
    assert client.cancellations == ["source-0"]
    assert not [item for item in client.submissions if item.account_index == client.receiver_account_index]
    guard = _guard_payload(path)
    assert guard["status"] == "UNKNOWN"
    assert guard["request_finished_at"] >= guard["request_started_at"]
    if mode in {"exact_absent", "wrong_owner", "wrong_order_id", "wrong_quantity"}:
        assert guard["best_bid"] is not None
        assert guard["best_ask"] is not None
        assert guard["source_public_level"] is None
    if mode == "anonymous_same_price":
        assert guard["source_public_level"]["order_id"] == "source-0"
        assert guard["same_price_evidence"][0]["owner_account_index"] is None
    if mode == "external_same_price":
        assert guard["source_public_level"]["order_id"] == "source-0"
        assert guard["same_price_evidence"][0]["owner_account_index"] == 999
    if mode in {"stale", "future"}:
        assert guard["source_public_level"] is None


@pytest.mark.asyncio
async def test_paired_guard_rejects_conflicting_exact_lookup_identity_without_cancellation(tmp_path):
    client = WrongExactLookupPairedClient()
    path = tmp_path / "wrong-exact-lookup.jsonl"
    result = await run_handoff(config(path), client, clock=Clock())

    assert result.outcome is Outcome.UNKNOWN
    # Reconciliation retains the original ID and reads it once more, while
    # the prior conflicting exact lookup still blocks every dependent write.
    assert client.source_lookup_calls == 3
    assert result.source.order_id == "source-0"
    assert not result.retryable_pair
    assert client.cancellations == []
    assert [item.account_index for item in client.submissions] == [11]
    assert not [item for item in client.submissions if item.account_index == client.receiver_account_index]
    assert any("exact order identity" in reason for reason in result.unknown_reasons)
    guard = _guard_payload(path)
    assert guard["status"] == "UNKNOWN"
    assert guard["source_public_level"]["order_id"] == "source-0"


@pytest.mark.asyncio
async def test_paired_opening_requires_flat_accounts_and_both_leg_margin(tmp_path):
    for margin_kwargs, journal_name in (
        ({"source_margin": Decimal("1")}, "source-margin.jsonl"),
        ({"receiver_margin": Decimal("1")}, "receiver-margin.jsonl"),
    ):
        client = PairedClient(**margin_kwargs)
        result = await run_handoff(config(tmp_path / journal_name), client, clock=Clock())
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
async def test_reversed_direction_opens_the_opposite_signed_positions(tmp_path):
    client = PairedClient()
    result = await run_handoff(
        config(tmp_path / "short.jsonl", direction=Direction.SHORT, receiver_worst_price=Decimal("100.0")),
        client,
        clock=Clock(),
    )
    assert result.outcome is Outcome.SUCCESS
    assert [(item.account_index, item.side, item.reduce_only) for item in client.submissions] == [
        (11, "BUY", False),
        (22, "SELL", False),
    ]
    assert client.source_position == Decimal("0.20")
    assert client.receiver_position == Decimal("-0.20")


class CrashAfterSourceDispatchClient(PairedClient):
    async def submit_order(self, plan):
        if plan.account_index == self.source_account_index:
            self.submissions.append(plan)
            raise TimeoutError("synthetic source dispatch crash after mutation boundary")
        return await super().submit_order(plan)


@pytest.mark.asyncio
async def test_paired_restart_and_mode_mismatch_never_replay_or_cancel(tmp_path):
    path = tmp_path / "restart.jsonl"
    first_client = CrashAfterSourceDispatchClient()
    first = await run_handoff(config(path), first_client, clock=Clock())
    assert first.outcome is Outcome.UNKNOWN
    assert len(first_client.submissions) == 1

    resumed_client = PairedClient()
    resumed = await run_handoff(config(path), resumed_client, clock=Clock())
    assert resumed.outcome is Outcome.UNKNOWN
    assert resumed_client.submissions == []
    assert resumed_client.cancellations == []
    assert "RESTART_RECONCILIATION_ONLY" in path.read_text()

    mismatch_client = PairedClient()
    mismatch = await run_handoff(
        config(path, operation_mode=OperationMode.CLOSE_REOPEN),
        mismatch_client,
        clock=Clock(),
    )
    assert mismatch.outcome is Outcome.UNKNOWN
    assert mismatch_client.submissions == []
    assert mismatch_client.cancellations == []
    assert "RESTART_BINDING_MISMATCH" in path.read_text()


@pytest.mark.asyncio
async def test_partial_receiver_fill_reports_truthful_positions_and_stops_series(tmp_path):
    client = SeriesPairedClient(receiver_fill_quantity=Decimal("0.10"))
    result = await run_series(
        series_config(
            tmp_path / "partial-series.jsonl",
            total_quantity=Decimal("0.40"),
            desired_slice_quantity=Decimal("0.20"),
        ),
        client,
        clock=Clock(),
    )
    assert result.outcome in {Outcome.PARTIAL, Outcome.UNKNOWN}
    assert result.completed_quantity == Decimal("0")
    assert result.actual_source_filled_quantity == Decimal("0.10")
    assert result.actual_receiver_filled_quantity == Decimal("0.10")
    assert client.source_position == Decimal("-0.10")
    assert client.receiver_position == Decimal("0.10")
    assert len(client.submissions) == 2
    assert client.cancellations == ["source-0"]
    assert not (tmp_path / "partial-series.jsonl.child-0001").exists()


def _operator_config_payload() -> dict[str, object]:
    return {
        "market_id": 1,
        "direction": "LONG",
        "quantity": "0.20",
        "source_limit_price": "100.0",
        "receiver_worst_price": "101.0",
        "freshness_seconds": 10,
        "request_timeout_seconds": 1,
        "order_timeout_seconds": 1,
        "reconcile_timeout_seconds": 1,
        "poll_interval_seconds": 0.001,
        "max_poll_count": 2,
        "source_order_lifetime_seconds": 300,
        "client_order_prefix": "operator",
        "journal_path": "/tmp/operator.jsonl",
        "api_base_url": "https://api.rh.lighter.xyz",
        "api_key_index": 4,
        "chain_id": 466324,
        "operation_mode": "PAIRED_OPENING",
    }


def _operator_series_payload() -> dict[str, object]:
    return {
        "market_symbol": "BTC",
        "direction": "LONG",
        "total_quantity": "0.40",
        "desired_slice_quantity": "0.20",
        "allowed_price_deviation": "0.02",
        "source_limit_price": "100.0",
        "receiver_worst_price": "101.0",
        "freshness_seconds": 10,
        "request_timeout_seconds": 1,
        "order_timeout_seconds": 1,
        "reconcile_timeout_seconds": 1,
        "poll_interval_seconds": 0.001,
        "max_poll_count": 2,
        "source_order_lifetime_seconds": 300,
        "client_order_prefix": "operator-series",
        "journal_path": "/tmp/operator-series.jsonl",
        "market_id": 1,
        "operation_mode": "PAIRED_OPENING",
        "mode": "series",
    }


def test_cli_rejects_operator_position_overrides_but_keeps_series_continuity_internal():
    from risex_spread_shadow.hood_handoff.cli import _config, _series_config

    direct = _operator_config_payload()
    direct["expected_source_position"] = "-0.10"
    direct["expected_receiver_position"] = "0.10"
    with pytest.raises(SystemExit, match="reserved"):
        _config(direct, execute=False)

    series = _operator_series_payload()
    series["expectedSourcePosition"] = "-0.10"
    with pytest.raises(SystemExit, match="reserved"):
        _series_config(series, execute=False)


class OpeningSigner:
    ORDER_TYPE_LIMIT = 0
    ORDER_TYPE_MARKET = 1
    ORDER_TIME_IN_FORCE_POST_ONLY = 2
    ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 0
    SKIP_NONCE_OFF = 0

    def __init__(self, **kwargs):
        self.factory_kwargs = kwargs
        self.nonce_manager = OpeningNonceManager()
        self.sign_calls = []

    async def sign_create_order(self, **kwargs):
        self.sign_calls.append(kwargs)
        return (14, "synthetic-create-info", "0x" + "1" * 64, None)


class OpeningNonceManager:
    def __init__(self):
        self.next_value = 40

    async def async_next_nonce(self, api_key_index):
        self.next_value += 1
        return api_key_index, self.next_value


class OpeningHttp:
    def __init__(self, *args, **kwargs):
        self.calls = []

    async def post_form(self, path, *, form):
        self.calls.append((path, dict(form)))
        return {"code": 200, "tx_hash": "0x" + "2" * 64}


class OpeningModule:
    pass


@pytest.mark.asyncio
async def test_lighter_adapter_dispatches_both_paired_opening_legs_to_exact_accounts(monkeypatch, tmp_path):
    signers = {}

    def signer_factory(**kwargs):
        signer = OpeningSigner(**kwargs)
        signers[kwargs["account_index"]] = signer
        return signer

    client = LighterSdkClient(
        config(tmp_path / "adapter.json"),
        source_account_index=11,
        receiver_account_index=22,
        secrets=StaticSecretProvider({11: "synthetic-source", 22: "synthetic-receiver"}),
        market_evidence={},
        signer_factory=signer_factory,
        http_factory=OpeningHttp,
    )
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client._lighter = lambda: OpeningModule

    source = OrderPlan(
        account_index=11,
        market_id=1,
        side="SELL",
        quantity=Decimal("0.20"),
        quantity_int=20,
        price=Decimal("100.0"),
        price_int=1000,
        order_type="LIMIT",
        time_in_force="POST_ONLY",
        reduce_only=False,
        order_expiry_ms=1_500_000,
        client_order_index=11,
    )
    receiver = OrderPlan(
        account_index=22,
        market_id=1,
        side="BUY",
        quantity=Decimal("0.20"),
        quantity_int=20,
        price=Decimal("101.0"),
        price_int=1010,
        order_type="MARKET",
        time_in_force="IOC",
        reduce_only=False,
        order_expiry_ms=0,
        client_order_index=22,
    )
    assert (await client.submit_order(source)).accepted
    assert (await client.submit_order(receiver)).accepted
    assert set(signers) == {11, 22}
    assert all(signer.factory_kwargs["chain_id"] == 466324 for signer in signers.values())
    source_call = signers[11].sign_calls[0]
    receiver_call = signers[22].sign_calls[0]
    assert {
        key: source_call[key]
        for key in ("market_index", "client_order_index", "base_amount", "price", "is_ask", "order_type", "time_in_force", "reduce_only")
    } == {
        "market_index": 1,
        "client_order_index": 11,
        "base_amount": 20,
        "price": 1000,
        "is_ask": True,
        "order_type": 0,
        "time_in_force": 2,
        "reduce_only": False,
    }
    assert {
        key: receiver_call[key]
        for key in ("market_index", "client_order_index", "base_amount", "price", "is_ask", "order_type", "time_in_force", "reduce_only")
    } == {
        "market_index": 1,
        "client_order_index": 22,
        "base_amount": 20,
        "price": 1010,
        "is_ask": False,
        "order_type": 1,
        "time_in_force": 0,
        "reduce_only": False,
    }
    assert len(client._http.calls) == 2
    assert all(path == "api/v1/sendTx" for path, _ in client._http.calls)


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


@pytest.mark.asyncio
async def test_engine_persists_only_allowlisted_numeric_preparation_timings(tmp_path):
    class Token(dict):
        pass
    class TimedClient(PreparedPairedClient):
        async def prepare_order(self, plan):
            token = Token(await super().prepare_order(plan))
            token.diagnostic_timings = {"nonce_acquisition_seconds": 0.2, "signing_call_seconds": 0.3,
                "preparation_lock_wait_seconds": float("nan"), "tx_info": "SYNTHETIC_SIGNED_CANARY"}
            return token
        async def submit_prepared_order(self, plan, prepared, *, deadline=None):
            result = await super().submit_prepared_order(plan, prepared, deadline=deadline)
            prepared.diagnostic_timings["transport_roundtrip_seconds"] = 0.5
            return result
    path = tmp_path / "timings.jsonl"
    result = await run_handoff(config(path), TimedClient(), clock=Clock())
    assert result.outcome is Outcome.SUCCESS
    for leg in ("source", "receiver"):
        assert result.latency[f"{leg}_nonce_acquisition_seconds"] == 0.2
        assert result.latency[f"{leg}_signing_call_seconds"] == 0.3
        assert result.latency[f"{leg}_transport_roundtrip_seconds"] == 0.5
        assert f"{leg}_preparation_lock_wait_seconds" not in result.latency
    assert "SYNTHETIC_SIGNED_CANARY" not in path.read_text()
