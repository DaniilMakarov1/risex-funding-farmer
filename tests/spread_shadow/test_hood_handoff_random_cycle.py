from __future__ import annotations

import asyncio
from dataclasses import asdict, replace
from decimal import Decimal
import json
from pathlib import Path

import pytest

from risex_spread_shadow.hood_handoff import cli as cli_module
from risex_spread_shadow.hood_handoff import (
    AccountSnapshot,
    ContractError,
    DepthLevel,
    Direction,
    HistoryPage,
    MarketMetadata,
    MutationReceipt,
    OperationMode,
    OrderBookSnapshot,
    OrderSnapshot,
    Outcome,
    PreflightBlocked,
    RandomCycleConfig,
    TradeReceipt,
    compute_quantity_bounds,
    run_random_cycle,
)


NOW = 1_000.0


class AdvancingClock:
    def __init__(self) -> None:
        self.value = NOW
        self.sleeps: list[float] = []

    def now(self) -> float:
        return self.value

    def monotonic(self) -> float:
        return self.value

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.value += seconds


class FixedRng:
    def __init__(self, *values: int) -> None:
        self.values = list(values)
        self.bounds: list[tuple[int, int]] = []

    def randint(self, lower: int, upper: int) -> int:
        self.bounds.append((lower, upper))
        if not self.values:
            raise AssertionError("unexpected random draw")
        value = self.values.pop(0)
        assert lower <= value <= upper
        return value


def metadata(observed_at: float = NOW, *, minimum_base: str = "0.10", minimum_quote: str = "10") -> MarketMetadata:
    return MarketMetadata(
        market_id=7,
        symbol="BTC",
        status="active",
        price_decimals=1,
        size_decimals=2,
        minimum_base_amount=Decimal(minimum_base),
        minimum_quote_amount=Decimal(minimum_quote),
        source_fee_rate=None,
        receiver_fee_rate=None,
        observed_at=observed_at,
        margin_evidence="synthetic account limits",
        market_type="perp",
        venue="robinhood",
    )


def account(
    index: int,
    position: Decimal,
    *,
    observed_at: float = NOW,
    available_balance: Decimal | None = Decimal("1000"),
    active_orders: tuple[OrderSnapshot, ...] = (),
) -> AccountSnapshot:
    return AccountSnapshot(
        account_index=index,
        market_id=7,
        signed_position=position,
        active_orders=active_orders,
        observed_at=observed_at,
        authorized=True,
        ready=True,
        margin_available=available_balance,
        margin_required=Decimal("1"),
        fee_rate=None,
        source_identity=f"cycle-account-{index}",
        incremental_margin_required=Decimal("1"),
        incremental_margin_evidence="synthetic opening margin proof",
        available_balance=available_balance,
    )


def book(observed_at: float = NOW) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        market_id=7,
        symbol="BTC",
        bids=(DepthLevel(Decimal("100.0"), Decimal("100"), f"bid-{observed_at}"),),
        asks=(DepthLevel(Decimal("100.2"), Decimal("100"), f"ask-{observed_at}"),),
        observed_at=observed_at,
        market_type="perp",
        venue="robinhood",
    )


class CycleClient:
    source_account_index = 11
    receiver_account_index = 22

    def __init__(
        self,
        clock: AdvancingClock,
        *,
        partial_open: bool = False,
        partial_close: bool = False,
        partial_fallback: bool = False,
        fallback_fill_fractions: list[Decimal] | tuple[Decimal, ...] | None = None,
    ) -> None:
        self.clock = clock
        self.partial_open = partial_open
        self.partial_close = partial_close
        self.partial_fallback = partial_fallback
        self.fallback_fill_fractions = None if fallback_fill_fractions is None else list(fallback_fill_fractions)
        self.fallback_fill_calls = 0
        self.fallback_plans = []
        self.source_position = Decimal("0")
        self.receiver_position = Decimal("0")
        self.orders: dict[tuple[int, str], OrderSnapshot] = {}
        self.latest_order: dict[int, str] = {}
        self.trades: dict[str, tuple] = {}
        self.submissions: list = []
        self.cancellations: list[str] = []
        self._pair_number = 0

    async def market_metadata(self, market_id: int) -> MarketMetadata:
        assert market_id == 7
        return metadata(self.clock.now())

    async def order_book(self, market_id: int) -> OrderBookSnapshot:
        assert market_id == 7
        return book(self.clock.now())

    async def account_snapshot(self, account_index: int, market_id: int) -> AccountSnapshot:
        assert market_id == 7
        assert account_index in {self.source_account_index, self.receiver_account_index}
        position = self.source_position if account_index == self.source_account_index else self.receiver_position
        active_id = self.latest_order.get(account_index)
        active = ()
        if active_id is not None:
            current = self.orders[(account_index, active_id)]
            if current.active:
                active = (current,)
        return account(account_index, position, observed_at=self.clock.now(), active_orders=active)

    async def lookup_order(self, account_index: int, market_id: int, *, order_id=None, client_order_index=None):
        assert market_id == 7
        for (owner, candidate_id), value in self.orders.items():
            if owner != account_index:
                continue
            if order_id is not None and candidate_id != str(order_id):
                continue
            if client_order_index is not None and str(value.client_order_index) != str(client_order_index):
                continue
            return value
        return None

    def _save_order(self, order: OrderSnapshot) -> None:
        self.orders[(order.account_index, order.order_id)] = order
        self.latest_order[order.account_index] = order.order_id

    def _replace_order(self, old: OrderSnapshot, **changes) -> OrderSnapshot:
        values = {
            "account_index": old.account_index,
            "market_id": old.market_id,
            "order_id": old.order_id,
            "client_order_index": old.client_order_index,
            "status": old.status,
            "side": old.side,
            "order_type": old.order_type,
            "time_in_force": old.time_in_force,
            "reduce_only": old.reduce_only,
            "initial_quantity": old.initial_quantity,
            "remaining_quantity": old.remaining_quantity,
            "filled_quantity": old.filled_quantity,
            "price": old.price,
            "observed_at": self.clock.now(),
        }
        values.update(changes)
        result = OrderSnapshot(**values)
        self._save_order(result)
        return result

    async def submit_order(self, plan):
        self.submissions.append(plan)
        if plan.order_type == "LIMIT":
            order = OrderSnapshot(
                account_index=plan.account_index,
                market_id=plan.market_id,
                order_id=f"order-{len(self.orders) + 1}",
                client_order_index=plan.client_order_index,
                status="open",
                side=plan.side,
                order_type="LIMIT",
                time_in_force="POST_ONLY",
                reduce_only=plan.reduce_only,
                initial_quantity=plan.quantity,
                remaining_quantity=plan.quantity,
                filled_quantity=Decimal("0"),
                price=plan.price,
                observed_at=self.clock.now(),
            )
            self._save_order(order)
            return MutationReceipt(True, order.order_id, f"tx-{order.order_id}")

        assert plan.order_type == "MARKET"
        source_order = self.orders[(self.source_account_index, self.latest_order[self.source_account_index])]
        paired_receiver = plan.account_index == self.receiver_account_index and source_order.status == "open"
        pair_fill = plan.quantity
        if plan.reduce_only and self.partial_close and paired_receiver:
            pair_fill = plan.quantity / Decimal("2")
        elif plan.reduce_only and not paired_receiver:
            self.fallback_plans.append(plan)
            if self.fallback_fill_fractions is not None and self.fallback_fill_calls < len(self.fallback_fill_fractions):
                pair_fill = plan.quantity * self.fallback_fill_fractions[self.fallback_fill_calls]
                self.fallback_fill_calls += 1
            elif self.partial_fallback:
                pair_fill = plan.quantity / Decimal("2")
        elif not plan.reduce_only and self.partial_open:
            pair_fill = plan.quantity / Decimal("2")
        receiver_order = OrderSnapshot(
            account_index=plan.account_index,
            market_id=plan.market_id,
            order_id=f"order-{len(self.orders) + 1}",
            client_order_index=plan.client_order_index,
            status="filled" if pair_fill == plan.quantity else "canceled-not-enough-liquidity",
            side=plan.side,
            order_type="MARKET",
            time_in_force="IOC",
            reduce_only=plan.reduce_only,
            initial_quantity=plan.quantity,
            remaining_quantity=plan.quantity - pair_fill,
            filled_quantity=pair_fill,
            price=plan.price,
            observed_at=self.clock.now(),
        )
        self._save_order(receiver_order)
        if paired_receiver or not plan.reduce_only:
            self._replace_order(
                source_order,
                status="filled" if pair_fill == plan.quantity else "open",
                remaining_quantity=plan.quantity - pair_fill,
                filled_quantity=pair_fill,
            )
            if source_order.side == "BUY":
                self.source_position += pair_fill
            else:
                self.source_position -= pair_fill
            if plan.side == "BUY":
                self.receiver_position += pair_fill
            else:
                self.receiver_position -= pair_fill
            source_order = self.orders[(self.source_account_index, self.latest_order[self.source_account_index])]
        else:
            if plan.account_index == self.source_account_index:
                self.source_position += pair_fill if plan.side == "BUY" else -pair_fill
            else:
                self.receiver_position += pair_fill if plan.side == "BUY" else -pair_fill
        if pair_fill > 0:
            trade_id = f"pair-trade-{self._pair_number}"
            self._pair_number += 1
            from risex_spread_shadow.hood_handoff import TradeReceipt

            market_trade = TradeReceipt(
                trade_id,
                plan.account_index,
                7,
                receiver_order.order_id,
                plan.side,
                pair_fill,
                receiver_order.price,
                None,
                self.receiver_account_index if plan.account_index == self.source_account_index else self.source_account_index,
                self.clock.now(),
                counterparty_order_id=(receiver_order.order_id if paired_receiver and plan.account_index == self.source_account_index else source_order.order_id),
                counterparty_client_order_index=(receiver_order.client_order_index if paired_receiver and plan.account_index == self.source_account_index else source_order.client_order_index),
                client_order_index=receiver_order.client_order_index,
            )
            self.trades[receiver_order.order_id] = (market_trade,)
            if paired_receiver:
                source_trade = TradeReceipt(
                    trade_id,
                    self.source_account_index,
                    7,
                    source_order.order_id,
                    source_order.side,
                    pair_fill,
                    source_order.price,
                    None,
                    self.receiver_account_index,
                    self.clock.now(),
                    counterparty_order_id=receiver_order.order_id,
                    counterparty_client_order_index=receiver_order.client_order_index,
                    client_order_index=source_order.client_order_index,
                )
                self.trades[source_order.order_id] = (source_trade,)
            else:
                self.trades.setdefault(receiver_order.order_id, (market_trade,))
        else:
            self.trades[receiver_order.order_id] = ()
        return MutationReceipt(True, receiver_order.order_id, f"tx-{receiver_order.order_id}")

    async def cancel_order(self, account_index: int, market_id: int, order_id: str) -> MutationReceipt:
        self.cancellations.append(order_id)
        current = self.orders.get((account_index, str(order_id)))
        if current is not None and current.active:
            self._replace_order(current, status="canceled", remaining_quantity=Decimal("0"))
        return MutationReceipt(True, str(order_id), f"tx-cancel-{order_id}")

    async def list_trades(self, account_index: int, market_id: int, *, order_id=None, cursor=None, limit=100):
        assert cursor is None
        return HistoryPage(trades=self.trades.get(str(order_id), ()))


class UnknownFirstFallbackClient(CycleClient):
    def __init__(self, clock: AdvancingClock) -> None:
        super().__init__(clock, partial_close=True)
        self.fallback_started = False

    async def submit_order(self, plan):
        if plan.order_type == "MARKET" and plan.reduce_only:
            source_order = self.orders.get((self.source_account_index, self.latest_order.get(self.source_account_index, "")))
            if source_order is None or source_order.status != "open":
                self.fallback_started = True
        return await super().submit_order(plan)

    async def list_trades(self, account_index: int, market_id: int, *, order_id=None, cursor=None, limit=100):
        if self.fallback_started:
            raise RuntimeError("synthetic ambiguous fallback history")
        return await super().list_trades(account_index, market_id, order_id=order_id, cursor=cursor, limit=limit)


class ExternalOpeningClient(CycleClient):
    """Fill the source maker externally after the required recheck barrier."""

    def __init__(self, clock: AdvancingClock, *, direction: Direction, fill_fraction: Decimal):
        super().__init__(clock)
        self.direction = direction
        self.fill_fraction = fill_fraction
        self.source_lookup_count = 0
        self.injected = False

    async def lookup_order(self, account_index, market_id, *, order_id=None, client_order_index=None):
        value = await super().lookup_order(
            account_index,
            market_id,
            order_id=order_id,
            client_order_index=client_order_index,
        )
        if account_index == self.source_account_index and order_id is not None and value is not None:
            self.source_lookup_count += 1
            if self.source_lookup_count == 2 and not self.injected:
                self.injected = True
                filled = value.initial_quantity * self.fill_fraction
                value = self._replace_order(
                    value,
                    status="filled" if filled == value.initial_quantity else "canceled",
                    filled_quantity=filled,
                    remaining_quantity=value.initial_quantity - filled,
                )
                self.source_position += -filled if value.side == "SELL" else filled
                self.trades[value.order_id] = (
                    TradeReceipt(
                        f"external-source-{value.order_id}",
                        self.source_account_index,
                        value.market_id,
                        value.order_id,
                        value.side,
                        filled,
                        value.price,
                        None,
                        999,
                        self.clock.now(),
                        counterparty_order_id="external-maker",
                        counterparty_client_order_index="external-client",
                        client_order_index=value.client_order_index,
                    ),
                )
                return None
        return value


class UnknownLaterFallbackClient(CycleClient):
    def __init__(self, clock: AdvancingClock) -> None:
        super().__init__(
            clock,
            partial_close=True,
            fallback_fill_fractions=[Decimal("0.5"), Decimal("0.5"), Decimal("1")],
        )
        self.fallback_order_ids: list[str] = []
        self.unknown_order_id: str | None = None

    async def submit_order(self, plan):
        source_order = self.orders.get((self.source_account_index, self.latest_order.get(self.source_account_index, "")))
        paired_receiver = plan.order_type == "MARKET" and plan.reduce_only and plan.account_index == self.receiver_account_index and source_order is not None and source_order.status == "open"
        receipt = await super().submit_order(plan)
        if plan.order_type == "MARKET" and plan.reduce_only and not paired_receiver:
            assert receipt.order_id is not None
            self.fallback_order_ids.append(receipt.order_id)
            if len(self.fallback_order_ids) == 2:
                self.unknown_order_id = receipt.order_id
        return receipt

    async def list_trades(self, account_index: int, market_id: int, *, order_id=None, cursor=None, limit=100):
        if self.unknown_order_id is not None and str(order_id) == self.unknown_order_id:
            raise RuntimeError("synthetic ambiguous later fallback history")
        return await super().list_trades(account_index, market_id, order_id=order_id, cursor=cursor, limit=limit)


class IdentityChangingFallbackClient(CycleClient):
    def __init__(self, clock: AdvancingClock) -> None:
        super().__init__(clock, partial_close=True, fallback_fill_fractions=[Decimal("0.5"), Decimal("1")])
        self.change_identity = False

    async def submit_order(self, plan):
        source_order = self.orders.get((self.source_account_index, self.latest_order.get(self.source_account_index, "")))
        paired_receiver = plan.order_type == "MARKET" and plan.reduce_only and plan.account_index == self.receiver_account_index and source_order is not None and source_order.status == "open"
        receipt = await super().submit_order(plan)
        if plan.order_type == "MARKET" and plan.reduce_only and not paired_receiver:
            self.change_identity = True
        return receipt

    async def account_snapshot(self, account_index: int, market_id: int) -> AccountSnapshot:
        snapshot = await super().account_snapshot(account_index, market_id)
        if self.change_identity and account_index == self.source_account_index:
            return replace(snapshot, source_identity="foreign-account")
        return snapshot


class ExternalCloseClient(CycleClient):
    """Close the receiver against an external counterparty.

    The paired close still admits our source maker first.  The receiver market
    fill is deliberately independent, so the engine must cancel the source
    order and then use only the confirmed source residual.
    """

    def __init__(
        self,
        clock: AdvancingClock,
        *,
        cancel_mode: str | None = None,
        cancel_fill_fraction: Decimal | None = None,
        partial_fallback: bool = False,
    ) -> None:
        super().__init__(clock, partial_fallback=partial_fallback)
        self.cancel_mode = cancel_mode
        self.cancel_fill_fraction = cancel_fill_fraction

    async def submit_order(self, plan):
        if plan.order_type == "MARKET" and plan.reduce_only and plan.account_index == self.receiver_account_index:
            order = OrderSnapshot(
                account_index=plan.account_index,
                market_id=plan.market_id,
                order_id=f"order-{len(self.orders) + 1}",
                client_order_index=plan.client_order_index,
                status="filled",
                side=plan.side,
                order_type="MARKET",
                time_in_force="IOC",
                reduce_only=True,
                initial_quantity=plan.quantity,
                remaining_quantity=Decimal("0"),
                filled_quantity=plan.quantity,
                price=plan.price,
                observed_at=self.clock.now(),
            )
            self._save_order(order)
            self.receiver_position += plan.quantity if plan.side == "BUY" else -plan.quantity
            trade = TradeReceipt(
                f"external-trade-{self._pair_number}",
                self.receiver_account_index,
                7,
                order.order_id,
                plan.side,
                plan.quantity,
                plan.price,
                None,
                999,
                self.clock.now(),
                counterparty_order_id=f"external-order-{self._pair_number}",
                counterparty_client_order_index=99000 + self._pair_number,
                client_order_index=order.client_order_index,
            )
            self._pair_number += 1
            self.trades[order.order_id] = (trade,)
            return MutationReceipt(True, order.order_id, f"tx-{order.order_id}")
        return await super().submit_order(plan)

    async def cancel_order(self, account_index: int, market_id: int, order_id: str) -> MutationReceipt:
        self.cancellations.append(order_id)
        current = self.orders.get((account_index, str(order_id)))
        if (
            current is not None
            and current.active
            and current.reduce_only
            and current.order_type == "LIMIT"
            and self.cancel_mode is not None
        ):
            if self.cancel_mode == "unknown":
                raise RuntimeError("synthetic cancellation transport failure")
            if self.cancel_mode == "rejected":
                return MutationReceipt(False, current.order_id, None, "synthetic cancellation rejection")
        if (
            current is not None
            and current.active
            and current.reduce_only
            and current.order_type == "LIMIT"
            and self.cancel_fill_fraction is not None
        ):
            fill = current.initial_quantity * self.cancel_fill_fraction
            status = "filled" if fill == current.initial_quantity else "canceled"
            self._replace_order(
                current,
                status=status,
                remaining_quantity=current.initial_quantity - fill,
                filled_quantity=fill,
            )
            self.source_position += fill if current.side == "BUY" else -fill
            if fill > 0:
                trade = TradeReceipt(
                    f"cancel-race-trade-{self._pair_number}",
                    self.source_account_index,
                    7,
                    current.order_id,
                    current.side,
                    fill,
                    current.price,
                    None,
                    999,
                    self.clock.now(),
                    counterparty_order_id=f"cancel-race-external-{self._pair_number}",
                    counterparty_client_order_index=99100 + self._pair_number,
                    client_order_index=current.client_order_index,
                )
                self._pair_number += 1
                self.trades[current.order_id] = (trade,)
            else:
                self.trades[current.order_id] = ()
            return MutationReceipt(True, current.order_id, f"tx-cancel-race-{current.order_id}")
        return await super().cancel_order(account_index, market_id, order_id)


class ChildIdentityChangeClient(CycleClient):
    """Change the source account identity at a selected child-close boundary."""

    def __init__(self, clock: AdvancingClock, *, change_phase: str) -> None:
        super().__init__(clock)
        self.change_phase = change_phase
        self.close_reads = 0
        self.identity_changed = False
        self.opening_complete = False

    async def submit_order(self, plan):
        receipt = await super().submit_order(plan)
        if (
            plan.order_type == "MARKET"
            and not plan.reduce_only
            and plan.account_index == self.receiver_account_index
        ):
            self.opening_complete = True
        return receipt

    async def account_snapshot(self, account_index: int, market_id: int) -> AccountSnapshot:
        snapshot = await super().account_snapshot(account_index, market_id)
        # The opening reconciliation happens at the initial clock value.  At
        # the later hold deadline, the parent close reads both accounts first
        # (relative reads 1-2), then the nested child preflight reads them
        # (3-4), and its account recheck starts at read 5.  Stage the identity
        # change at those child-relative boundaries so parent close admission
        # remains valid and the child guard is the code under test.
        if self.opening_complete and self.clock.now() > NOW and not self.identity_changed:
            self.close_reads += 1
            if (
                account_index == self.source_account_index
                and (
                    (self.change_phase == "child-preflight" and self.close_reads == 3)
                    or (self.change_phase == "child-recheck" and self.close_reads == 5)
                )
            ):
                self.identity_changed = True
        if self.identity_changed and account_index == self.source_account_index:
            return replace(snapshot, source_identity="foreign-account")
        return snapshot


class TransientIdentityChangeClient(CycleClient):
    """Return one foreign identity at a chosen post-hold account read."""

    def __init__(self, clock: AdvancingClock, *, target_account: int, target_read: int) -> None:
        super().__init__(clock)
        self.target_account = target_account
        self.target_read = target_read
        self.reads = 0
        self.opening_complete = False
        self.identity_mismatch_count = 0

    async def submit_order(self, plan):
        receipt = await super().submit_order(plan)
        if (
            plan.order_type == "MARKET"
            and not plan.reduce_only
            and plan.account_index == self.receiver_account_index
        ):
            self.opening_complete = True
        return receipt

    async def account_snapshot(self, account_index: int, market_id: int) -> AccountSnapshot:
        snapshot = await super().account_snapshot(account_index, market_id)
        if (
            self.opening_complete
            and self.clock.now() >= NOW + 20
            and account_index == self.target_account
        ):
            self.reads += 1
            if self.reads == self.target_read:
                self.identity_mismatch_count += 1
                return replace(snapshot, source_identity="one-response-foreign-identity")
        return snapshot


class TransientFallbackIdentityClient(CycleClient):
    """Return one foreign identity during fallback reconciliation."""

    def __init__(self, clock: AdvancingClock, *, target_account: int) -> None:
        super().__init__(clock, partial_close=True)
        self.target_account = target_account
        self.reads = 0
        self.identity_mismatch_count = 0

    async def account_snapshot(self, account_index: int, market_id: int) -> AccountSnapshot:
        snapshot = await super().account_snapshot(account_index, market_id)
        if self.fallback_plans and account_index == self.target_account:
            self.reads += 1
            if self.reads == 1:
                self.identity_mismatch_count += 1
                return replace(snapshot, source_identity="one-response-foreign-identity")
        return snapshot


class AccountBindingFaultClient(CycleClient):
    """Inject one malformed account response at a close or fallback boundary."""

    def __init__(
        self,
        clock: AdvancingClock,
        *,
        fault_kind: str,
        target_account: int,
        target_read: int = 1,
        boundary: str = "close",
    ) -> None:
        super().__init__(clock, partial_close=boundary == "fallback")
        self.fault_kind = fault_kind
        self.target_account = target_account
        self.target_read = target_read
        self.boundary = boundary
        self.boundary_reads = 0
        self.fault_injected = False

    def _fault(self, snapshot: AccountSnapshot):
        if self.fault_kind == "mapping_missing":
            value = asdict(snapshot)
            value.pop("source_identity")
            return value
        if self.fault_kind == "mapping_empty":
            value = asdict(snapshot)
            value["source_identity"] = ""
            return value
        if self.fault_kind == "mapping_invalid":
            value = asdict(snapshot)
            value["source_identity"] = 99
            return value
        if self.fault_kind == "wrong_account":
            return replace(snapshot, account_index=99)
        if self.fault_kind == "wrong_market":
            return replace(snapshot, market_id=99)
        if self.fault_kind == "decoder":
            raise ContractError("synthetic account decoder failure")
        raise AssertionError(f"unknown account fault kind: {self.fault_kind}")

    async def account_snapshot(self, account_index: int, market_id: int):
        snapshot = await super().account_snapshot(account_index, market_id)
        if (
            not self.fault_injected
            and self.clock.now() >= NOW + 20
            and account_index == self.target_account
            and (self.boundary == "close" or self.fallback_plans)
            and (self.boundary != "close" or not self.fallback_plans)
        ):
            self.boundary_reads += 1
            if self.boundary_reads == self.target_read:
                self.fault_injected = True
                return self._fault(snapshot)
        return snapshot


class FallbackReceiptTimingClient(CycleClient):
    """Inject historical/future trade receipts without aging order snapshots."""

    def __init__(
        self,
        clock: AdvancingClock,
        *,
        trade_age: float = 0.0,
        order_age: float = 0.0,
    ) -> None:
        super().__init__(clock, partial_close=True)
        self.trade_age = trade_age
        self.order_age = order_age
        self.fallback_order_ids: set[str] = set()

    async def submit_order(self, plan):
        receipt = await super().submit_order(plan)
        if any(candidate is plan for candidate in self.fallback_plans) and receipt.order_id is not None:
            self.fallback_order_ids.add(str(receipt.order_id))
        return receipt

    async def lookup_order(self, *args, **kwargs):
        value = await super().lookup_order(*args, **kwargs)
        if value is not None and value.order_id in self.fallback_order_ids and self.order_age:
            return replace(value, observed_at=self.clock.now() - self.order_age)
        return value

    async def list_trades(self, account_index: int, market_id: int, *, order_id=None, cursor=None, limit=100):
        page = await super().list_trades(
            account_index,
            market_id,
            order_id=order_id,
            cursor=cursor,
            limit=limit,
        )
        if str(order_id) not in self.fallback_order_ids or not page.trades or not self.trade_age:
            return page
        trades = tuple(
            replace(trade, observed_at=self.clock.now() - self.trade_age)
            for trade in page.trades
        )
        return HistoryPage(
            trades=trades,
            orders=page.orders,
            next_cursor=page.next_cursor,
            complete=page.complete,
        )


def cycle_config(path: Path, **overrides) -> RandomCycleConfig:
    values = {
        "market_id": 7,
        "market_symbol": "BTC",
        "direction": Direction.LONG,
        "source_account_index": 11,
        "receiver_account_index": 22,
        "cycle_dir": path,
        "freshness_seconds": 10,
        "request_timeout_seconds": 1,
        "order_timeout_seconds": 1,
        "reconcile_timeout_seconds": 1,
        "poll_interval_seconds": 0.001,
        "max_poll_count": 2,
        "source_order_lifetime_seconds": 300,
        "environment": "robinhood",
        "api_base_url": "https://api.rh.lighter.xyz",
        "chain_id": 466324,
        "operator_execution_opt_in": True,
        "operator_plan_reviewed": True,
    }
    values.update(overrides)
    return RandomCycleConfig(**values)


def test_quantity_bounds_use_independent_integer_ceil_floor_and_documented_balance():
    source = account(11, Decimal("0"), available_balance=Decimal("100.19"))
    receiver = account(22, Decimal("0"), available_balance=Decimal("251.01"))
    bounds = compute_quantity_bounds(metadata(), source, receiver, Decimal("100.1"))
    assert bounds.lower_tick == 10
    assert bounds.upper_tick == 100
    assert bounds.lower_quantity == Decimal("0.10")
    assert bounds.upper_quantity == Decimal("1.00")

    with pytest.raises(PreflightBlocked, match="available_balance evidence is missing"):
        compute_quantity_bounds(metadata(), account(11, Decimal("0"), available_balance=None), receiver, Decimal("100.1"))


def test_quantity_bounds_do_not_turn_missing_or_undersized_balance_into_leverage():
    source = account(11, Decimal("0"), available_balance=Decimal("9.99"))
    receiver = account(22, Decimal("0"), available_balance=Decimal("1000"))
    with pytest.raises(PreflightBlocked, match="cannot fund"):
        compute_quantity_bounds(metadata(), source, receiver, Decimal("100.1"))


@pytest.mark.asyncio
async def test_preview_has_no_reads_or_cycle_directory_consumption(tmp_path):
    clock = AdvancingClock()
    client = CycleClient(clock)
    config = cycle_config(tmp_path / "preview")
    preview = await run_random_cycle(
        RandomCycleConfig(
            **{
                "market_id": config.market_id,
                "market_symbol": config.market_symbol,
                "direction": config.direction,
                "source_account_index": config.source_account_index,
                "receiver_account_index": config.receiver_account_index,
                "cycle_dir": config.cycle_dir,
                "operator_execution_opt_in": False,
                "operator_plan_reviewed": False,
            }
        ),
        client,
        clock=clock,
        rng=FixedRng(10, 20),
    )
    assert preview.outcome is Outcome.PREVIEW
    assert not client.submissions
    assert not config.cycle_dir.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("direction, expected_open, expected_close", [
    (Direction.LONG, [("SELL", False), ("BUY", False)], [("BUY", True), ("SELL", True)]),
    (Direction.SHORT, [("BUY", False), ("SELL", False)], [("SELL", True), ("BUY", True)]),
])
async def test_one_cycle_opens_holds_and_closes_actual_positions_once(tmp_path, direction, expected_open, expected_close):
    clock = AdvancingClock()
    client = CycleClient(clock)
    rng = FixedRng(25, 20)
    result = await run_random_cycle(
        cycle_config(tmp_path / direction.value.lower(), direction=direction),
        client,
        clock=clock,
        rng=rng,
    )
    assert result.outcome is Outcome.SUCCESS
    assert result.selection is not None
    assert result.selection.quantity == Decimal("0.25")
    assert result.selection.quantity_tick == 25
    assert result.selection.hold_seconds == 20
    assert client.source_position == Decimal("0")
    assert client.receiver_position == Decimal("0")
    assert [(plan.side, plan.reduce_only) for plan in client.submissions] == [*expected_open, *expected_close]
    assert all(plan.quantity == Decimal("0.25") for plan in client.submissions)
    assert clock.sleeps == [20]
    journal = (tmp_path / direction.value.lower() / "cycle.jsonl").read_text()
    assert "SELECTION_PROVED" in journal
    assert "HOLD_ANCHORED" in journal
    assert "CLOSING_PLAN_READY" in journal
    assert "CYCLE_COMPLETE" in journal


@pytest.mark.asyncio
async def test_partial_paired_close_uses_confirmed_reduce_only_fallback_per_account(tmp_path):
    clock = AdvancingClock()
    client = CycleClient(clock, partial_close=True)
    result = await run_random_cycle(
        cycle_config(tmp_path / "partial-close"),
        client,
        clock=clock,
        rng=FixedRng(20, 20),
    )
    assert result.outcome is Outcome.SUCCESS
    assert len(result.fallbacks) == 2
    assert all(item.outcome is Outcome.SUCCESS for item in result.fallbacks)
    assert [plan.reduce_only for plan in client.submissions] == [False, False, True, True, True, True]
    assert [plan.order_type for plan in client.submissions[-2:]] == ["MARKET", "MARKET"]
    assert all(plan.quantity == Decimal("0.10") for plan in client.submissions[-2:])
    assert client.source_position == Decimal("0")
    assert client.receiver_position == Decimal("0")


@pytest.mark.asyncio
async def test_partial_market_fallback_repeats_fairly_with_unique_ids_and_exact_residuals(tmp_path):
    clock = AdvancingClock()
    client = CycleClient(
        clock,
        partial_close=True,
        fallback_fill_fractions=[
            Decimal("0.5"),
            Decimal("0.5"),
            Decimal("1"),
            Decimal("1"),
        ],
    )
    result = await run_random_cycle(
        cycle_config(tmp_path / "repeated-fallback"),
        client,
        clock=clock,
        rng=FixedRng(40, 20),
    )
    assert result.outcome is Outcome.SUCCESS
    assert [item.attempt for item in result.fallbacks] == [1, 2, 3, 4]
    assert [item.account_index for item in result.fallbacks] == [11, 22, 11, 22]
    assert [item.requested_quantity for item in result.fallbacks] == [
        Decimal("0.20"),
        Decimal("0.20"),
        Decimal("0.10"),
        Decimal("0.10"),
    ]
    assert [item.filled_quantity for item in result.fallbacks] == [
        Decimal("0.10"),
        Decimal("0.10"),
        Decimal("0.10"),
        Decimal("0.10"),
    ]
    ids = [item.order_id for item in result.fallbacks]
    assert len(ids) == len(set(ids)) == 4
    client_ids = [plan.client_order_index for plan in client.fallback_plans]
    assert len(client_ids) == len(set(client_ids)) == 4
    assert all(plan.order_type == "MARKET" and plan.time_in_force == "IOC" and plan.reduce_only for plan in client.fallback_plans)
    assert client.source_position == Decimal("0")
    assert client.receiver_position == Decimal("0")
    assert clock.sleeps == [20]


@pytest.mark.asyncio
async def test_unknown_fallback_state_blocks_the_other_account_mutation(tmp_path):
    clock = AdvancingClock()
    client = UnknownFirstFallbackClient(clock)
    result = await run_random_cycle(
        cycle_config(tmp_path / "unknown-fallback"),
        client,
        clock=clock,
        rng=FixedRng(20, 20),
    )
    assert result.outcome is Outcome.UNKNOWN
    assert len(result.fallbacks) == 1
    assert result.fallbacks[0].outcome is Outcome.UNKNOWN
    assert len(client.submissions) == 5
    assert client.source_position == Decimal("0")
    assert client.receiver_position == Decimal("0.10")


@pytest.mark.asyncio
async def test_later_unknown_fallback_stops_all_following_attempts(tmp_path):
    clock = AdvancingClock()
    client = UnknownLaterFallbackClient(clock)
    result = await run_random_cycle(
        cycle_config(tmp_path / "unknown-later"),
        client,
        clock=clock,
        rng=FixedRng(40, 20),
    )
    assert result.outcome is Outcome.UNKNOWN
    assert [(item.attempt, item.account_index, item.outcome) for item in result.fallbacks] == [
        (1, 11, Outcome.PARTIAL),
        (2, 22, Outcome.UNKNOWN),
    ]
    assert len(client.fallback_plans) == 2
    assert client.source_position == Decimal("-0.10")
    assert client.receiver_position == Decimal("0.10")


@pytest.mark.asyncio
async def test_identity_change_after_known_partial_stops_following_mutations(tmp_path):
    clock = AdvancingClock()
    client = IdentityChangingFallbackClient(clock)
    result = await run_random_cycle(
        cycle_config(tmp_path / "identity-change"),
        client,
        clock=clock,
        rng=FixedRng(40, 20),
    )
    assert result.outcome is Outcome.UNKNOWN
    assert len(client.fallback_plans) == 1
    assert client.source_position == Decimal("-0.10")
    assert client.receiver_position == Decimal("0.20")


@pytest.mark.asyncio
async def test_zero_fill_is_paced_and_does_not_starve_the_other_account(tmp_path):
    clock = AdvancingClock()
    client = CycleClient(
        clock,
        partial_close=True,
        fallback_fill_fractions=[
            Decimal("0"),
            Decimal("0.5"),
            Decimal("1"),
            Decimal("1"),
        ],
    )
    result = await run_random_cycle(
        cycle_config(tmp_path / "zero-fill", poll_interval_seconds=Decimal("0.01")),
        client,
        clock=clock,
        rng=FixedRng(40, 20),
    )
    assert result.outcome is Outcome.SUCCESS
    assert [item.account_index for item in result.fallbacks] == [11, 22, 22, 11]
    assert result.fallbacks[0].filled_quantity == Decimal("0")
    assert clock.sleeps == pytest.approx([20, 0.01])
    assert client.source_position == Decimal("0")
    assert client.receiver_position == Decimal("0")


@pytest.mark.asyncio
async def test_interruption_after_fallback_intent_preserves_unknown_and_consumes_cycle(tmp_path):
    class InterruptingFallbackClient(CycleClient):
        def __init__(self, clock: AdvancingClock) -> None:
            super().__init__(clock, partial_close=True, partial_fallback=True)
            self.fallback_count = 0

        async def submit_order(self, plan):
            source_order = self.orders.get((self.source_account_index, self.latest_order.get(self.source_account_index, "")))
            paired_receiver = plan.order_type == "MARKET" and plan.reduce_only and plan.account_index == self.receiver_account_index and source_order is not None and source_order.status == "open"
            receipt = await super().submit_order(plan)
            if plan.order_type == "MARKET" and plan.reduce_only and not paired_receiver:
                self.fallback_count += 1
                if self.fallback_count == 1:
                    raise asyncio.CancelledError()
            return receipt

    clock = AdvancingClock()
    client = InterruptingFallbackClient(clock)
    config = cycle_config(tmp_path / "interrupted-fallback")
    with pytest.raises(asyncio.CancelledError):
        await run_random_cycle(config, client, clock=clock, rng=FixedRng(40, 20))
    journal = config.journal_path.read_text()
    assert "FALLBACK_DISPATCH_INTENT" in journal
    assert "FALLBACK_DISPATCH_UNKNOWN" in journal
    assert "CYCLE_INTERRUPTED" in journal
    replay_client = CycleClient(clock)
    replay = await run_random_cycle(config, replay_client, clock=clock, rng=FixedRng(40, 20))
    assert replay.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED
    assert "consumed" in (replay.reason or "")
    assert not replay_client.submissions


@pytest.mark.asyncio
async def test_external_receiver_fill_cancels_source_then_closes_own_residual(tmp_path):
    clock = AdvancingClock()
    client = ExternalCloseClient(clock, cancel_fill_fraction=Decimal("0"))
    result = await run_random_cycle(
        cycle_config(tmp_path / "external-receiver"),
        client,
        clock=clock,
        rng=FixedRng(40, 20),
    )
    assert result.outcome is Outcome.SUCCESS
    assert len(client.fallback_plans) == 1
    assert client.fallback_plans[0].account_index == client.source_account_index
    assert client.fallback_plans[0].quantity == Decimal("0.40")
    assert client.cancellations == ["order-3"]
    assert client.orders[(client.source_account_index, "order-3")].status == "canceled"
    assert client.source_position == Decimal("0")
    assert client.receiver_position == Decimal("0")


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_fill_fraction, expected_residual", [
    (Decimal("0.50"), Decimal("0.20")),
    (Decimal("0.75"), Decimal("0.10")),
])
async def test_source_fill_during_cancel_sizes_fallback_from_confirmed_residual(
    tmp_path,
    cancel_fill_fraction,
    expected_residual,
):
    clock = AdvancingClock()
    client = ExternalCloseClient(clock, cancel_fill_fraction=cancel_fill_fraction)
    result = await run_random_cycle(
        cycle_config(tmp_path / f"cancel-race-{cancel_fill_fraction}"),
        client,
        clock=clock,
        rng=FixedRng(40, 20),
    )
    assert result.outcome is Outcome.SUCCESS
    assert len(client.fallback_plans) == 1
    assert client.fallback_plans[0].quantity == expected_residual
    assert result.fallbacks[0].requested_quantity == expected_residual
    assert client.source_position == Decimal("0")
    assert client.receiver_position == Decimal("0")


@pytest.mark.asyncio
async def test_source_fill_during_cancel_can_eliminate_fallback(tmp_path):
    clock = AdvancingClock()
    client = ExternalCloseClient(clock, cancel_fill_fraction=Decimal("1"))
    result = await run_random_cycle(
        cycle_config(tmp_path / "cancel-race-full"),
        client,
        clock=clock,
        rng=FixedRng(20, 20),
    )
    assert result.outcome is Outcome.SUCCESS
    assert not client.fallback_plans
    assert result.remaining_source_position == Decimal("0.00")
    assert result.remaining_receiver_position == Decimal("0")
    assert client.source_position == Decimal("0.00")
    assert client.receiver_position == Decimal("0")


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_mode", ["unknown", "rejected"])
async def test_unknown_or_rejected_source_cancel_never_dispatches_residual_fallback(tmp_path, cancel_mode):
    clock = AdvancingClock()
    client = ExternalCloseClient(clock, cancel_mode=cancel_mode)
    result = await run_random_cycle(
        cycle_config(tmp_path / f"cancel-{cancel_mode}"),
        client,
        clock=clock,
        rng=FixedRng(20, 20),
    )
    assert result.outcome is Outcome.UNKNOWN
    assert not client.fallback_plans
    assert client.source_position == Decimal("-0.20")
    assert client.receiver_position == Decimal("0")


@pytest.mark.asyncio
async def test_partial_opening_never_starts_hold_and_closes_confirmed_residuals(tmp_path):
    clock = AdvancingClock()
    client = CycleClient(clock, partial_open=True)
    result = await run_random_cycle(
        cycle_config(tmp_path / "partial-open"),
        client,
        clock=clock,
        rng=FixedRng(20, 20),
    )
    assert result.outcome is Outcome.PARTIAL
    assert result.closing is None
    assert len(result.fallbacks) == 2
    assert clock.sleeps == []
    assert client.source_position == Decimal("0")
    assert client.receiver_position == Decimal("0")
    journal = (tmp_path / "partial-open" / "cycle.jsonl").read_text()
    assert "HOLD_ANCHORED" not in journal


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "direction, fill_fraction",
    [
        (Direction.LONG, Decimal("1")),
        (Direction.SHORT, Decimal("0.5")),
    ],
)
async def test_known_external_source_opening_fill_closes_residual_without_receiver_or_hold(
    tmp_path,
    direction,
    fill_fraction,
):
    clock = AdvancingClock()
    client = ExternalOpeningClient(
        clock,
        direction=direction,
        fill_fraction=fill_fraction,
    )
    result = await run_random_cycle(
        cycle_config(
            tmp_path / f"external-opening-{direction.value.lower()}",
            direction=direction,
        ),
        client,
        clock=clock,
        rng=FixedRng(20, 20),
    )

    assert result.outcome is Outcome.PARTIAL
    assert result.opening is not None
    assert result.opening.outcome is Outcome.PARTIAL
    assert result.closing is None
    assert result.fallbacks and all(item.outcome is Outcome.SUCCESS for item in result.fallbacks)
    assert [(plan.order_type, plan.reduce_only) for plan in client.submissions] == [
        ("LIMIT", False),
        ("MARKET", True),
    ]
    assert client.source_position == Decimal("0")
    assert client.receiver_position == Decimal("0")
    assert clock.sleeps == []
    journal = (tmp_path / f"external-opening-{direction.value.lower()}" / "cycle.jsonl").read_text()
    assert "HOLD_ANCHORED" not in journal


@pytest.mark.asyncio
async def test_cycle_interruption_leaves_consumed_journal_and_never_replays(tmp_path):
    class SyntheticProcessInterruption(BaseException):
        pass

    class InterruptingClient(CycleClient):
        async def submit_order(self, plan):
            if plan.order_type == "LIMIT":
                await super().submit_order(plan)
                raise SyntheticProcessInterruption("synthetic interruption after source admission")
            return await super().submit_order(plan)

    clock = AdvancingClock()
    client = InterruptingClient(clock)
    config = cycle_config(tmp_path / "interrupted")
    result = await run_random_cycle(config, client, clock=clock, rng=FixedRng(25, 20))
    assert result.outcome is Outcome.UNKNOWN
    assert len(client.submissions) == 1
    journal = (tmp_path / "interrupted" / "cycle.jsonl").read_text()
    assert "CYCLE_EXECUTION_UNKNOWN" in journal
    replay = await run_random_cycle(config, CycleClient(clock), clock=clock, rng=FixedRng(25, 20))
    assert replay.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED
    assert "consumed" in (replay.reason or "")


def test_paired_closing_contract_requires_reduce_only_both_legs():
    with pytest.raises(ContractError):
        OperationMode.parse("not-a-mode")


def test_cli_random_cycle_preview_is_explicit_and_does_not_consume_cycle(tmp_path, capsys):
    config_path = tmp_path / "random-cycle.json"
    cycle_path = tmp_path / "cycle"
    config_path.write_text(
        json.dumps(
            {
                "market_id": "7",
                "market_symbol": "BTC",
                "direction": "SHORT",
                "source_account_index": "11",
                "receiver_account_index": "22",
                "cycle_dir": str(cycle_path),
            }
        ),
        encoding="utf-8",
    )
    assert cli_module.main(["random-cycle", "--config", str(config_path)]) == 0
    preview = json.loads(capsys.readouterr().out)
    assert preview["outcome"] == "PREVIEW"
    assert preview["direction"] == "SHORT"
    assert preview["source_account_index"] == 11
    assert preview["receiver_account_index"] == 22
    assert preview["hold_policy"].endswith("[20,300] after both opening legs are fully reconciled")
    assert preview["close_source_reduce_only"] is True
    assert not cycle_path.exists()


@pytest.mark.asyncio
async def test_close_refuses_position_drift_from_the_opening_lineage(tmp_path):
    class HoldDriftClock(AdvancingClock):
        async def sleep(self, seconds: float) -> None:
            await super().sleep(seconds)
            if seconds >= 20:
                self.client.source_position = Decimal("-0.20")

    clock = HoldDriftClock()
    client = CycleClient(clock)
    clock.client = client
    result = await run_random_cycle(
        cycle_config(tmp_path / "lineage-drift"),
        client,
        clock=clock,
        rng=FixedRng(40, 20),
    )
    assert result.outcome is Outcome.UNKNOWN
    assert "source position changed" in (result.reason or "")
    assert [(plan.account_index, plan.reduce_only) for plan in client.submissions] == [
        (11, False),
        (22, False),
    ]
    assert "CLOSING_BLOCKED" in (tmp_path / "lineage-drift" / "cycle.jsonl").read_text()


@pytest.mark.asyncio
async def test_close_refuses_identity_drift_from_the_opening_lineage(tmp_path):
    class HoldIdentityClock(AdvancingClock):
        async def sleep(self, seconds: float) -> None:
            await super().sleep(seconds)
            if seconds >= 20:
                self.client.identity_changed = True

    class HoldIdentityClient(CycleClient):
        def __init__(self, clock: AdvancingClock) -> None:
            super().__init__(clock)
            self.identity_changed = False

        async def account_snapshot(self, account_index: int, market_id: int) -> AccountSnapshot:
            snapshot = await super().account_snapshot(account_index, market_id)
            if self.identity_changed and account_index == self.source_account_index:
                return replace(snapshot, source_identity="manual-account")
            return snapshot

    clock = HoldIdentityClock()
    client = HoldIdentityClient(clock)
    clock.client = client
    result = await run_random_cycle(
        cycle_config(tmp_path / "lineage-identity-drift"),
        client,
        clock=clock,
        rng=FixedRng(40, 20),
    )
    assert result.outcome is Outcome.UNKNOWN
    assert "identity changed" in (result.reason or "")
    assert [(plan.account_index, plan.reduce_only) for plan in client.submissions] == [
        (11, False),
        (22, False),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("direction", [Direction.LONG, Direction.SHORT])
async def test_close_child_preflight_cannot_rebind_opening_account_identity(tmp_path, direction):
    clock = AdvancingClock()
    client = ChildIdentityChangeClient(clock, change_phase="child-preflight")
    result = await run_random_cycle(
        cycle_config(tmp_path / f"child-preflight-{direction.value.lower()}", direction=direction),
        client,
        clock=clock,
        rng=FixedRng(20, 20),
    )

    assert result.outcome is Outcome.UNKNOWN
    assert result.closing is not None
    assert result.closing.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED
    assert result.closing.reason == "contract_error"
    assert client.identity_changed
    assert not [plan for plan in client.submissions if plan.reduce_only]
    assert not client.fallback_plans
    expected_source = Decimal("-0.20") if direction is Direction.LONG else Decimal("0.20")
    expected_receiver = Decimal("0.20") if direction is Direction.LONG else Decimal("-0.20")
    assert client.source_position == expected_source
    assert client.receiver_position == expected_receiver


@pytest.mark.asyncio
@pytest.mark.parametrize("direction", [Direction.LONG, Direction.SHORT])
async def test_close_child_recheck_cannot_rebind_identity_before_receiver_mutation(tmp_path, direction):
    clock = AdvancingClock()
    client = ChildIdentityChangeClient(clock, change_phase="child-recheck")
    result = await run_random_cycle(
        cycle_config(tmp_path / f"child-recheck-{direction.value.lower()}", direction=direction),
        client,
        clock=clock,
        rng=FixedRng(20, 20),
    )

    assert result.outcome is Outcome.UNKNOWN
    assert result.closing is not None
    assert result.closing.outcome is Outcome.UNKNOWN
    assert client.identity_changed
    assert result.closing.unknown_reasons
    assert any(plan.order_type == "LIMIT" and plan.reduce_only for plan in client.submissions)
    assert not any(plan.order_type == "MARKET" and plan.reduce_only for plan in client.submissions)
    assert not client.fallback_plans
    assert client.cancellations
    expected_source = Decimal("-0.20") if direction is Direction.LONG else Decimal("0.20")
    expected_receiver = Decimal("0.20") if direction is Direction.LONG else Decimal("-0.20")
    assert client.source_position == expected_source
    assert client.receiver_position == expected_receiver


@pytest.mark.asyncio
@pytest.mark.parametrize("target_account", [11, 22])
@pytest.mark.parametrize("target_read", [1, 2, 3])
async def test_transient_identity_failure_remains_cycle_barrier(
    tmp_path,
    target_account,
    target_read,
):
    clock = AdvancingClock()
    client = TransientIdentityChangeClient(
        clock,
        target_account=target_account,
        target_read=target_read,
    )
    result = await run_random_cycle(
        cycle_config(tmp_path / f"transient-{target_account}-{target_read}"),
        client,
        clock=clock,
        rng=FixedRng(40, 20),
    )

    assert result.outcome is Outcome.UNKNOWN
    assert "identity failure barrier" in (result.reason or "")
    assert client.identity_mismatch_count == 1
    assert not any(plan.order_type == "MARKET" and plan.reduce_only for plan in client.submissions)
    assert not client.fallback_plans
    assert client.source_position == Decimal("-0.40")
    assert client.receiver_position == Decimal("0.40")
    if target_read == 3:
        assert any(plan.order_type == "LIMIT" and plan.reduce_only for plan in client.submissions)
        assert client.cancellations
    else:
        assert not any(plan.order_type == "LIMIT" and plan.reduce_only for plan in client.submissions)


@pytest.mark.asyncio
@pytest.mark.parametrize("target_account", [11, 22])
async def test_transient_fallback_identity_failure_blocks_later_account_mutation(
    tmp_path,
    target_account,
):
    clock = AdvancingClock()
    client = TransientFallbackIdentityClient(clock, target_account=target_account)
    result = await run_random_cycle(
        cycle_config(tmp_path / f"fallback-transient-{target_account}"),
        client,
        clock=clock,
        rng=FixedRng(40, 20),
    )

    assert result.outcome is Outcome.UNKNOWN
    assert "identity failure barrier" in (result.reason or "")
    assert client.identity_mismatch_count == 1
    assert len(client.fallback_plans) == 1
    assert not any(plan.account_index == 22 and plan.order_type == "MARKET" for plan in client.fallback_plans)
    assert client.source_position == Decimal("0")
    assert client.receiver_position == Decimal("0.20")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault_kind",
    ["mapping_missing", "mapping_empty", "mapping_invalid", "wrong_account", "wrong_market", "decoder"],
)
@pytest.mark.parametrize("target_account", [11, 22])
@pytest.mark.parametrize("target_read", [1, 2, 3])
async def test_account_identity_binding_fault_is_terminal_before_dependent_writes(
    tmp_path,
    fault_kind,
    target_account,
    target_read,
):
    clock = AdvancingClock()
    client = AccountBindingFaultClient(
        clock,
        fault_kind=fault_kind,
        target_account=target_account,
        target_read=target_read,
    )
    result = await run_random_cycle(
        cycle_config(tmp_path / f"account-fault-{fault_kind}-{target_account}-{target_read}"),
        client,
        clock=clock,
        rng=FixedRng(40, 20),
    )

    assert result.outcome is Outcome.UNKNOWN
    assert "identity failure barrier" in (result.reason or "")
    assert client.fault_injected
    assert not client.fallback_plans
    assert client.source_position == Decimal("-0.40")
    assert client.receiver_position == Decimal("0.40")
    if target_read < 3:
        assert len(client.submissions) == 2
        assert not any(plan.reduce_only for plan in client.submissions)
    else:
        assert len(client.submissions) == 3
        assert [plan.reduce_only for plan in client.submissions] == [False, False, True]
        assert client.cancellations


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault_kind",
    ["mapping_missing", "mapping_empty", "mapping_invalid", "wrong_account", "wrong_market", "decoder"],
)
@pytest.mark.parametrize("target_account", [11, 22])
async def test_account_identity_binding_fault_stops_fallback_rechecks_and_later_account(
    tmp_path,
    fault_kind,
    target_account,
):
    clock = AdvancingClock()
    client = AccountBindingFaultClient(
        clock,
        fault_kind=fault_kind,
        target_account=target_account,
        boundary="fallback",
    )
    result = await run_random_cycle(
        cycle_config(tmp_path / f"fallback-account-fault-{fault_kind}-{target_account}"),
        client,
        clock=clock,
        rng=FixedRng(40, 20),
    )

    assert result.outcome is Outcome.UNKNOWN
    assert "identity failure barrier" in (result.reason or "")
    assert client.fault_injected
    assert len(client.fallback_plans) == 1
    assert not any(plan.account_index == 22 for plan in client.fallback_plans)


@pytest.mark.asyncio
async def test_fallback_polls_fresh_terminal_order_and_keeps_failure_reason(tmp_path):
    class StaleFallbackClient(CycleClient):
        async def lookup_order(self, *args, **kwargs):
            value = await super().lookup_order(*args, **kwargs)
            fallback_ids = {plan.client_order_index for plan in self.fallback_plans}
            if value is not None and value.client_order_index in fallback_ids:
                return replace(value, observed_at=self.clock.now() - 100)
            return value

    clock = AdvancingClock()
    client = StaleFallbackClient(clock, partial_close=True)
    result = await run_random_cycle(
        cycle_config(tmp_path / "stale-fallback"),
        client,
        clock=clock,
        rng=FixedRng(40, 20),
    )
    assert result.outcome is Outcome.UNKNOWN
    assert "fallback order" in (result.reason or "")
    assert "stale" in (result.reason or "")
    assert len(client.fallback_plans) == 1
    assert client.receiver_position == Decimal("0.20")

    class DelayedFallbackClient(CycleClient):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.delayed_order_ids: set[str] = set()

        async def lookup_order(self, *args, **kwargs):
            value = await super().lookup_order(*args, **kwargs)
            fallback_ids = {plan.client_order_index for plan in self.fallback_plans}
            if (
                value is not None
                and value.client_order_index in fallback_ids
                and value.order_id not in self.delayed_order_ids
            ):
                self.delayed_order_ids.add(value.order_id)
                return None
            return value

    delayed_clock = AdvancingClock()
    delayed_client = DelayedFallbackClient(delayed_clock, partial_close=True)
    delayed_result = await run_random_cycle(
        cycle_config(tmp_path / "delayed-fallback"),
        delayed_client,
        clock=delayed_clock,
        rng=FixedRng(40, 20),
    )
    assert delayed_result.outcome is Outcome.SUCCESS
    assert len(delayed_client.fallback_plans) == 2
    assert delayed_client.source_position == Decimal("0")
    assert delayed_client.receiver_position == Decimal("0")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "trade_age, order_age, expected_outcome, expected_reason",
    [
        (11.0, 0.0, Outcome.SUCCESS, None),
        (-1.0, 0.0, Outcome.UNKNOWN, "future"),
        (11.0, 11.0, Outcome.UNKNOWN, "stale"),
    ],
)
async def test_fallback_history_separates_trade_time_from_order_snapshot_freshness(
    tmp_path,
    trade_age,
    order_age,
    expected_outcome,
    expected_reason,
):
    clock = AdvancingClock()
    client = FallbackReceiptTimingClient(
        clock,
        trade_age=trade_age,
        order_age=order_age,
    )
    result = await run_random_cycle(
        cycle_config(tmp_path / f"fallback-receipt-{trade_age}-{order_age}"),
        client,
        clock=clock,
        rng=FixedRng(40, 20),
    )

    assert result.outcome is expected_outcome
    if expected_reason is None:
        assert len(result.fallbacks) == 2
        assert all(item.outcome is Outcome.SUCCESS for item in result.fallbacks)
        assert client.source_position == Decimal("0")
        assert client.receiver_position == Decimal("0")
    else:
        assert expected_reason in (result.reason or "")
        assert len(result.fallbacks) == 1
        assert result.fallbacks[0].outcome is Outcome.UNKNOWN


@pytest.mark.asyncio
async def test_fallback_journal_retains_receipts_and_account_state(tmp_path):
    clock = AdvancingClock()
    client = CycleClient(clock, partial_close=True)
    cycle_path = tmp_path / "fallback-evidence"
    result = await run_random_cycle(
        cycle_config(cycle_path),
        client,
        clock=clock,
        rng=FixedRng(40, 20),
    )
    assert result.outcome is Outcome.SUCCESS
    rows = [json.loads(line) for line in (cycle_path / "cycle.jsonl").read_text().splitlines()]
    reconciled = [row for row in rows if row["event"] == "FALLBACK_RECONCILED"]
    assert len(reconciled) == 2
    for row in reconciled:
        payload = row["payload"]
        assert payload["receipt"]["accepted"] is True
        assert payload["before"]["source_identity"]
        assert payload["after"]["signed_position"] == "0.00"
        assert payload["history_complete"] is True
        assert payload["history_pages"]
        assert payload["trades"]
        assert payload["trades"][0]["trade_id"].startswith("pair-trade-")


def test_cli_random_cycle_requires_interactive_launch_before_client_or_keys(tmp_path, monkeypatch, capsys):
    config_path = tmp_path / "random-cycle.json"
    evidence_path = tmp_path / "market-evidence.json"
    cycle_path = tmp_path / "cancelled-cycle"
    config_path.write_text(
        json.dumps(
            {
                "market_id": 7,
                "market_symbol": "BTC",
                "direction": "LONG",
                "source_account_index": 11,
                "receiver_account_index": 22,
                "api_key_index": 4,
                "cycle_dir": str(cycle_path),
            }
        ),
        encoding="utf-8",
    )
    evidence_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr("builtins.input", lambda _prompt: "CANCEL")

    def fail_client(*args, **kwargs):
        raise AssertionError("SDK client was constructed before LAUNCH")

    monkeypatch.setattr(cli_module, "LighterSdkClient", fail_client)
    assert cli_module.main(
        [
            "random-cycle",
            "--config",
            str(config_path),
            "--market-evidence",
            str(evidence_path),
            "--execute",
            "--i-understand-one-attempt-live-operation",
            "--confirm-plan",
        ]
    ) == 0
    output = capsys.readouterr().out
    assert "pending LAUNCH" in output
    assert "cancelled before LAUNCH" in output
    assert not cycle_path.exists()


@pytest.mark.asyncio
async def test_fallback_interruption_after_receipt_persists_attempt_evidence(tmp_path):
    class InterruptingFallbackLookupClient(CycleClient):
        def __init__(self, clock: AdvancingClock) -> None:
            super().__init__(clock, partial_close=True)
            self.interrupted = False

        async def lookup_order(self, *args, **kwargs):
            value = await super().lookup_order(*args, **kwargs)
            fallback_ids = {plan.client_order_index for plan in self.fallback_plans}
            if (
                value is not None
                and value.client_order_index in fallback_ids
                and not self.interrupted
            ):
                self.interrupted = True
                raise asyncio.CancelledError()
            return value

    clock = AdvancingClock()
    client = InterruptingFallbackLookupClient(clock)
    config = cycle_config(tmp_path / "interrupted-fallback-lookup")
    with pytest.raises(asyncio.CancelledError):
        await run_random_cycle(config, client, clock=clock, rng=FixedRng(40, 20))
    rows = [json.loads(line) for line in config.journal_path.read_text().splitlines()]
    evidence = [row for row in rows if row["event"] == "FALLBACK_ATTEMPT_EVIDENCE"]
    assert len(evidence) == 1
    assert evidence[0]["payload"]["receipt"]["order_id"]
    assert evidence[0]["payload"]["plan"]["reduce_only"] is True
    assert any(row["event"] == "CYCLE_INTERRUPTED" for row in rows)
