"""The bounded HCR-1 state machine.

The engine depends on a small read/mutation interface.  The production Lighter
implementation in :mod:`sdk` supplies that interface; tests use an in-memory
fake and therefore never instantiate an SDK client or make a request.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from decimal import Decimal
import hashlib
import time
from typing import Any, Mapping, Protocol, Sequence, runtime_checkable

from .contracts import (
    AccountSnapshot,
    ContractError,
    Direction,
    HandoffConfig,
    HandoffPlan,
    HandoffResult,
    HistoryPage,
    LegReconciliation,
    MarketMetadata,
    MutationReceipt,
    OrderPlan,
    OrderSnapshot,
    Outcome,
    Phase,
    PreflightBlocked,
    TradeReceipt,
    _bool,
)
from .journal import DurableJournal, sanitize_exception


@runtime_checkable
class HandoffClient(Protocol):
    async def market_metadata(self, market_id: int) -> MarketMetadata | Mapping[str, Any]: ...

    async def account_snapshot(self, account_index: int, market_id: int) -> AccountSnapshot | Mapping[str, Any]: ...

    async def lookup_order(
        self,
        account_index: int,
        market_id: int,
        *,
        order_id: str | None = None,
        client_order_index: int | None = None,
    ) -> OrderSnapshot | Mapping[str, Any] | None: ...

    async def list_trades(
        self,
        account_index: int,
        market_id: int,
        *,
        order_id: str | None = None,
        cursor: str | None = None,
        limit: int = 100,
    ) -> HistoryPage | Mapping[str, Any]: ...

    async def submit_order(self, plan: OrderPlan) -> MutationReceipt | Mapping[str, Any]: ...

    async def cancel_order(self, account_index: int, market_id: int, order_id: str) -> MutationReceipt | Mapping[str, Any]: ...


class Clock(Protocol):
    def now(self) -> float: ...

    async def sleep(self, seconds: float) -> None: ...


@dataclass(slots=True)
class SystemClock:
    def now(self) -> float:
        return time.time()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


def _as_market(value: MarketMetadata | Mapping[str, Any]) -> MarketMetadata:
    return value if isinstance(value, MarketMetadata) else MarketMetadata.from_mapping(value)


def _as_account(value: AccountSnapshot | Mapping[str, Any]) -> AccountSnapshot:
    return value if isinstance(value, AccountSnapshot) else AccountSnapshot.from_mapping(value)


def _as_order(value: OrderSnapshot | Mapping[str, Any] | None) -> OrderSnapshot | None:
    if value is None:
        return None
    return value if isinstance(value, OrderSnapshot) else OrderSnapshot.from_mapping(value)


def _as_receipt(value: MutationReceipt | Mapping[str, Any]) -> MutationReceipt:
    if isinstance(value, MutationReceipt):
        return value
    return MutationReceipt(
        accepted=_bool(value.get("accepted", False), "accepted"),
        order_id=None if value.get("order_id") is None else str(value["order_id"]),
        tx_hash=None if value.get("tx_hash") is None else str(value["tx_hash"]),
        error=None if value.get("error") is None else sanitize_exception(ValueError(str(value["error"]))),
        response_code=value.get("response_code"),
    )


def _as_page(value: HistoryPage | Mapping[str, Any]) -> HistoryPage:
    if isinstance(value, HistoryPage):
        return value
    raw_trades = value.get("trades", ())
    raw_orders = value.get("orders", ())
    return HistoryPage(
        trades=tuple(
            item if isinstance(item, TradeReceipt) else TradeReceipt.from_mapping(item)
            for item in raw_trades
        ),
        orders=tuple(
            item if isinstance(item, OrderSnapshot) else OrderSnapshot.from_mapping(item)
            for item in raw_orders
        ),
        next_cursor=value.get("next_cursor"),
        complete=_bool(value.get("complete", value.get("history_complete", True)), "complete"),
    )


class HandoffEngine:
    """Execute or reconcile exactly one close/reopen attempt."""

    def __init__(self, client: HandoffClient, *, clock: Clock | None = None) -> None:
        self.client = client
        self.clock = clock or SystemClock()

    async def execute(self, config: HandoffConfig) -> HandoffResult:
        self._configured_poll_limit = config.max_poll_count
        self._configured_poll_interval = config.poll_interval_seconds
        self._configured_order_timeout = config.order_timeout_seconds
        self._configured_reconcile_timeout = config.reconcile_timeout_seconds
        journal = DurableJournal(config.journal_path, clock=self.clock.now)
        if journal.has_unresolved_mutation():
            return await self._resume_reconciliation(config, journal)
        run_id = journal.run_id
        now = self.clock.now()
        journal.append("ATTEMPT_STARTED", {"run_id": run_id, "market_id": config.market_id})
        try:
            plan, source, receiver = await self._preflight(config, journal, run_id, now)
        except (ContractError, PreflightBlocked) as exc:
            reason = sanitize_exception(exc)
            journal.append("PREFLIGHT_BLOCKED", {"reason": reason}, run_id=run_id)
            return HandoffResult(
                outcome=Outcome.FAILED_PREFLIGHT_BLOCKED,
                phase=Phase.PREFLIGHT,
                run_id=run_id,
                plan=None,
                source=None,
                receiver=None,
                reason=reason,
            )
        journal.append("PLAN_REVIEWED", {"plan": plan.as_dict()}, run_id=run_id)
        if not config.operator_execution_opt_in:
            journal.append("PREVIEW", {"plan": plan.as_dict()}, run_id=run_id)
            return HandoffResult(
                outcome=Outcome.PREVIEW,
                phase=Phase.PREFLIGHT,
                run_id=run_id,
                plan=plan,
                source=LegReconciliation(
                    account_index=source.account_index,
                    order_id=None,
                    trades=(),
                    position_before=source.signed_position,
                    position_after=source.signed_position,
                    order=None,
                    history_complete=True,
                ),
                receiver=LegReconciliation(
                    account_index=receiver.account_index,
                    order_id=None,
                    trades=(),
                    position_before=receiver.signed_position,
                    position_after=receiver.signed_position,
                    order=None,
                    history_complete=True,
                ),
                reason="execution opt-in is false; no signing or mutation was attempted",
            )

        source_receipt: MutationReceipt | None = None
        source_order: OrderSnapshot | None = None
        receiver_receipt: MutationReceipt | None = None
        receiver_order: OrderSnapshot | None = None
        unknown_reasons: list[str] = []
        try:
            journal.append(
                "SOURCE_DISPATCH_INTENT",
                {"plan": plan.source.as_dict()},
                run_id=run_id,
            )
            source_receipt = _as_receipt(await self.client.submit_order(plan.source))
            journal.append(
                "SOURCE_DISPATCH_RESULT",
                {
                    "accepted": source_receipt.accepted,
                    "order_id": source_receipt.order_id,
                    "tx_hash": source_receipt.tx_hash,
                    "response_code": source_receipt.response_code,
                    "error": source_receipt.error,
                },
                run_id=run_id,
            )
        except Exception as exc:  # an exception after intent is dispatch-unknown
            unknown_reasons.append(f"source dispatch outcome unknown: {sanitize_exception(exc)}")
            journal.append("SOURCE_DISPATCH_UNKNOWN", {"reason": unknown_reasons[-1]}, run_id=run_id)

        if source_receipt is not None:
            if not source_receipt.accepted:
                unknown_reasons.append("source dispatch was rejected")
            source_order = await self._poll_order(
                plan.source,
                source_receipt.order_id,
                journal,
                run_id,
                require_terminal=False,
            )
            if source_order is None:
                unknown_reasons.append("source order identity or status is unresolved")
            elif not self._order_matches(source_order, plan.source):
                unknown_reasons.append("source order identity/parameters conflict with plan")
            elif source_order.filled_quantity > 0:
                unknown_reasons.append("source fill observed before receiver dispatch")
                journal.append(
                    "SOURCE_FILLED_BEFORE_RECEIVER",
                    {"order_id": source_order.order_id, "filled_quantity": str(source_order.filled_quantity)},
                    run_id=run_id,
                )
                await self._cancel_if_safe(plan.source, source_order, journal, run_id, unknown_reasons)
            elif not source_order.active or source_order.remaining_quantity != plan.quantity:
                unknown_reasons.append("source order did not prove exact resting quantity")
                await self._cancel_if_safe(plan.source, source_order, journal, run_id, unknown_reasons)

        # A missing/rejected/partially-filled source can never authorize B.
        if not unknown_reasons and source_order is not None and self._source_is_resting(source_order, plan.source):
            try:
                source_recheck = _as_account(
                    await self.client.account_snapshot(plan.source.account_index, plan.source.market_id)
                )
                receiver_recheck = _as_account(
                    await self.client.account_snapshot(plan.receiver.account_index, plan.receiver.market_id)
                )
                source_order = await self._lookup_order(plan.source, source_order.order_id)
                if source_order is None:
                    unknown_reasons.append("source order disappeared during pre-receiver recheck")
                elif source_recheck.signed_position != source.signed_position:
                    unknown_reasons.append("source position changed before receiver dispatch")
                elif self.clock.now() - source_recheck.observed_at > config.freshness_seconds:
                    unknown_reasons.append("source recheck is stale before receiver dispatch")
                elif source_recheck.observed_at > self.clock.now():
                    unknown_reasons.append("source recheck is from the future")
                elif not source_recheck.authorized or not source_recheck.ready:
                    unknown_reasons.append("source recheck authorization/readiness is unproven")
                elif (
                    source_recheck.margin_available is None
                    or source_recheck.margin_required is None
                    or source_recheck.margin_required > source_recheck.margin_available
                ):
                    unknown_reasons.append("source margin recheck is insufficient or missing")
                elif source_recheck.fee_rate is None:
                    unknown_reasons.append("source fee recheck is missing")
                elif receiver_recheck.signed_position != receiver.signed_position:
                    unknown_reasons.append("receiver position changed before receiver dispatch")
                elif self.clock.now() - receiver_recheck.observed_at > config.freshness_seconds:
                    unknown_reasons.append("receiver recheck is stale before receiver dispatch")
                elif receiver_recheck.observed_at > self.clock.now():
                    unknown_reasons.append("receiver recheck is from the future")
                elif not receiver_recheck.authorized or not receiver_recheck.ready:
                    unknown_reasons.append("receiver recheck authorization/readiness is unproven")
                elif receiver_recheck.active_orders:
                    unknown_reasons.append("receiver active HOOD order appeared before receiver dispatch")
                elif (
                    receiver_recheck.margin_available is None
                    or receiver_recheck.margin_required is None
                    or receiver_recheck.margin_required > receiver_recheck.margin_available
                ):
                    unknown_reasons.append("receiver margin recheck is insufficient or missing")
                elif receiver_recheck.fee_rate is None:
                    unknown_reasons.append("receiver fee recheck is missing")
                elif not self._source_is_resting(source_order, plan.source):
                    unknown_reasons.append("source fill or quantity change before receiver dispatch")
                else:
                    journal.append(
                        "SOURCE_RECHECK_PASS",
                        {"order_id": source_order.order_id, "remaining_quantity": str(source_order.remaining_quantity)},
                        run_id=run_id,
                    )
            except Exception as exc:
                unknown_reasons.append(f"pre-receiver state is unresolved: {sanitize_exception(exc)}")

        if unknown_reasons:
            if source_order is not None:
                await self._cancel_if_safe(plan.source, source_order, journal, run_id, unknown_reasons)
            source_result, receiver_result = await self._reconcile(
                config,
                plan,
                source,
                receiver,
                source_order_id=source_order.order_id if source_order else (source_receipt.order_id if source_receipt else None),
                receiver_order_id=None,
                run_id=run_id,
                journal=journal,
                unknown_reasons=unknown_reasons,
            )
            return await self._finish(
                journal,
                run_id,
                plan,
                source_result,
                receiver_result,
                unknown_reasons,
                forced_outcome=Outcome.UNKNOWN if any("unknown" in item.lower() or "unresolved" in item.lower() for item in unknown_reasons) else None,
            )

        try:
            journal.append("RECEIVER_DISPATCH_INTENT", {"plan": plan.receiver.as_dict()}, run_id=run_id)
            receiver_receipt = _as_receipt(await self.client.submit_order(plan.receiver))
            journal.append(
                "RECEIVER_DISPATCH_RESULT",
                {
                    "accepted": receiver_receipt.accepted,
                    "order_id": receiver_receipt.order_id,
                    "tx_hash": receiver_receipt.tx_hash,
                    "response_code": receiver_receipt.response_code,
                    "error": receiver_receipt.error,
                },
                run_id=run_id,
            )
        except Exception as exc:
            unknown_reasons.append(f"receiver dispatch outcome unknown: {sanitize_exception(exc)}")
            journal.append("RECEIVER_DISPATCH_UNKNOWN", {"reason": unknown_reasons[-1]}, run_id=run_id)

        if receiver_receipt is not None:
            receiver_order = await self._poll_order(
                plan.receiver,
                receiver_receipt.order_id,
                journal,
                run_id,
                require_terminal=True,
            )
            if receiver_order is None:
                unknown_reasons.append("receiver order identity or terminal status is unresolved")
            elif not self._order_matches(receiver_order, plan.receiver):
                unknown_reasons.append("receiver order identity/parameters conflict with plan")
            elif not receiver_order.terminal:
                unknown_reasons.append("receiver IOC did not prove terminal status")

        # Once B is terminal or uncertain, cancel only this identified A order.
        if source_order is not None:
            current_source = await self._lookup_order(plan.source, source_order.order_id)
            if current_source is not None:
                await self._cancel_if_safe(plan.source, current_source, journal, run_id, unknown_reasons)
            else:
                unknown_reasons.append("source order disappeared before cancellation reconciliation")

        source_result, receiver_result = await self._reconcile(
            config,
            plan,
            source,
            receiver,
            source_order_id=source_order.order_id if source_order else (source_receipt.order_id if source_receipt else None),
            receiver_order_id=receiver_order.order_id if receiver_order else (receiver_receipt.order_id if receiver_receipt else None),
            run_id=run_id,
            journal=journal,
            unknown_reasons=unknown_reasons,
        )
        return await self._finish(journal, run_id, plan, source_result, receiver_result, unknown_reasons)

    async def _preflight(
        self,
        config: HandoffConfig,
        journal: DurableJournal,
        run_id: str,
        now: float,
    ) -> tuple[HandoffPlan, AccountSnapshot, AccountSnapshot]:
        metadata = _as_market(await self.client.market_metadata(config.market_id))
        source = _as_account(await self.client.account_snapshot(_account_from_client(self.client, "source"), config.market_id))
        receiver = _as_account(await self.client.account_snapshot(_account_from_client(self.client, "receiver"), config.market_id))
        # The client must expose account identities explicitly; this prevents a
        # fallback to one shared account or a hidden account discovery call.
        if source.account_index == receiver.account_index:
            raise PreflightBlocked("source and receiver accounts must differ")
        config.validate_against(metadata, source, receiver, now)
        quantity_int, source_price_int, receiver_price_int = config.integer_order_values(metadata)
        expiry_ms = int(now * 1000) + config.source_order_lifetime_seconds * 1000
        if expiry_ms <= int(now * 1000):
            raise PreflightBlocked("source order expiry is not in the future")
        source_client_index = _client_order_index(run_id, "source")
        receiver_client_index = _client_order_index(run_id, "receiver")
        plan = HandoffPlan(
            run_id=run_id,
            source=OrderPlan(
                account_index=source.account_index,
                market_id=config.market_id,
                side=config.direction.source_side,
                quantity=config.quantity,
                quantity_int=quantity_int,
                price=config.source_limit_price,
                price_int=source_price_int,
                order_type="LIMIT",
                time_in_force="POST_ONLY",
                reduce_only=True,
                order_expiry_ms=expiry_ms,
                client_order_index=source_client_index,
            ),
            receiver=OrderPlan(
                account_index=receiver.account_index,
                market_id=config.market_id,
                side=config.direction.receiver_side,
                quantity=config.quantity,
                quantity_int=quantity_int,
                price=config.receiver_worst_price,
                price_int=receiver_price_int,
                order_type="MARKET",
                time_in_force="IOC",
                reduce_only=False,
                order_expiry_ms=0,
                client_order_index=receiver_client_index,
            ),
            source_position_before=source.signed_position,
            receiver_position_before=receiver.signed_position,
            direction=config.direction,
            quantity=config.quantity,
            created_at=now,
        )
        journal.append(
            "PREFLIGHT_PROVED",
            {
                "market_id": metadata.market_id,
                "symbol": metadata.symbol,
                "source_account_index": source.account_index,
                "receiver_account_index": receiver.account_index,
                "source_position": str(source.signed_position),
                "receiver_position": str(receiver.signed_position),
            },
            run_id=run_id,
        )
        return plan, source, receiver

    async def _poll_order(
        self,
        plan: OrderPlan,
        order_id: str | None,
        journal: DurableJournal,
        run_id: str,
        *,
        require_terminal: bool,
    ) -> OrderSnapshot | None:
        deadline = self.clock.now() + self._order_timeout
        for poll in range(1, self._poll_limit + 1):
            if self.clock.now() > deadline:
                break
            try:
                order = await self._lookup_order(plan, order_id, client_order_index=plan.client_order_index)
            except Exception as exc:
                journal.append("ORDER_OBSERVATION_ERROR", {"leg": plan.side, "poll": poll, "reason": sanitize_exception(exc)}, run_id=run_id)
                order = None
            if order is not None:
                journal.append(
                    "ORDER_OBSERVED",
                    {
                        "account_index": order.account_index,
                        "market_id": order.market_id,
                        "order_id": order.order_id,
                        "status": order.status,
                        "filled_quantity": str(order.filled_quantity),
                        "remaining_quantity": str(order.remaining_quantity),
                        "poll": poll,
                    },
                    run_id=run_id,
                )
                if require_terminal and order.terminal:
                    return order
                if not require_terminal and (order.terminal or order.active):
                    return order
            if poll < self._poll_limit:
                remaining = deadline - self.clock.now()
                if remaining <= 0:
                    break
                await self._sleep(min(self._poll_interval, remaining))
        return None

    async def _lookup_order(
        self,
        plan: OrderPlan,
        order_id: str | None,
        *,
        client_order_index: int | None = None,
    ) -> OrderSnapshot | None:
        if client_order_index is None:
            client_order_index = plan.client_order_index
        value = await self.client.lookup_order(
            plan.account_index,
            plan.market_id,
            order_id=order_id,
            client_order_index=client_order_index,
        )
        return _as_order(value)

    def _order_matches(self, order: OrderSnapshot, plan: OrderPlan) -> bool:
        return (
            order.account_index == plan.account_index
            and order.market_id == plan.market_id
            and order.side.upper() == plan.side
            and (
                order.client_order_index is None
                or str(order.client_order_index) == str(plan.client_order_index)
            )
            and order.reduce_only == plan.reduce_only
            and order.initial_quantity == plan.quantity
            and order.order_type.upper() in {plan.order_type, plan.order_type.lower()}
            and order.time_in_force.upper() in {plan.time_in_force, plan.time_in_force.lower()}
        )

    def _source_is_resting(self, order: OrderSnapshot, plan: OrderPlan) -> bool:
        return (
            self._order_matches(order, plan)
            and order.status.lower() in {"open", "active"}
            and order.filled_quantity == 0
            and order.remaining_quantity == plan.quantity
        )

    async def _cancel_if_safe(
        self,
        plan: OrderPlan,
        order: OrderSnapshot,
        journal: DurableJournal,
        run_id: str,
        unknown_reasons: list[str],
    ) -> None:
        if (
            order.account_index != plan.account_index
            or order.market_id != plan.market_id
            or order.order_id == ""
            or not order.active
            or order.remaining_quantity <= 0
        ):
            return
        try:
            # Recheck the exact owner/market/order immediately before the only
            # cancellation mutation.  Never cancel by a bare client index.
            current = await self._lookup_order(plan, order.order_id)
            if current is None or not self._order_matches(current, plan) or not current.active:
                return
            journal.append(
                "CANCEL_DISPATCH_INTENT",
                {
                    "account_index": plan.account_index,
                    "market_id": plan.market_id,
                    "order_id": current.order_id,
                },
                run_id=run_id,
            )
            receipt = _as_receipt(
                await self.client.cancel_order(plan.account_index, plan.market_id, current.order_id)
            )
            journal.append(
                "CANCEL_DISPATCH_RESULT",
                {
                    "accepted": receipt.accepted,
                    "order_id": current.order_id,
                    "tx_hash": receipt.tx_hash,
                    "response_code": receipt.response_code,
                    "error": receipt.error,
                },
                run_id=run_id,
            )
        except Exception as exc:
            reason = f"source cancellation outcome unknown: {sanitize_exception(exc)}"
            unknown_reasons.append(reason)
            journal.append("CANCEL_DISPATCH_UNKNOWN", {"reason": reason}, run_id=run_id)

    async def _reconcile(
        self,
        config: HandoffConfig,
        plan: HandoffPlan,
        source: AccountSnapshot,
        receiver: AccountSnapshot,
        *,
        source_order_id: str | None,
        receiver_order_id: str | None,
        run_id: str,
        journal: DurableJournal,
        unknown_reasons: list[str],
    ) -> tuple[LegReconciliation, LegReconciliation]:
        journal.append("RECONCILIATION_STARTED", {"source_order_id": source_order_id, "receiver_order_id": receiver_order_id}, run_id=run_id)
        source_leg = await self._reconcile_leg(
            config,
            plan.source,
            source,
            source_order_id,
            run_id,
            journal,
            unknown_reasons,
        )
        receiver_leg = await self._reconcile_leg(
            config,
            plan.receiver,
            receiver,
            receiver_order_id,
            run_id,
            journal,
            unknown_reasons,
        )
        return source_leg, receiver_leg

    async def _reconcile_leg(
        self,
        config: HandoffConfig,
        plan: OrderPlan,
        before: AccountSnapshot,
        order_id: str | None,
        run_id: str,
        journal: DurableJournal,
        unknown_reasons: list[str],
    ) -> LegReconciliation:
        local_unknown: list[str] = []
        if order_id is None:
            try:
                discovered = await self._lookup_order(plan, None, client_order_index=plan.client_order_index)
            except Exception as exc:
                discovered = None
                local_unknown.append(f"{plan.side.lower()} order lookup by client id failed: {sanitize_exception(exc)}")
            if discovered is not None:
                order_id = discovered.order_id
            else:
                local_unknown.append(f"{plan.side.lower()} order id is missing")
        trades: list[TradeReceipt] = []
        seen: set[str] = set()
        complete = True
        cursor: str | None = None
        deadline = self.clock.now() + config.reconcile_timeout_seconds
        for _ in range(max(1, config.max_poll_count)):
            if self.clock.now() > deadline:
                local_unknown.append("reconciliation deadline exceeded")
                complete = False
                break
            try:
                page = _as_page(
                    await self.client.list_trades(
                        plan.account_index,
                        plan.market_id,
                        order_id=order_id,
                        cursor=cursor,
                        limit=100,
                    )
                )
            except Exception as exc:
                local_unknown.append(f"trade history read failed: {sanitize_exception(exc)}")
                complete = False
                break
            complete = complete and page.complete
            for trade in page.trades:
                if trade.account_index != plan.account_index or trade.market_id != plan.market_id:
                    local_unknown.append("trade history contains foreign account/market")
                    continue
                if trade.observed_at > self.clock.now():
                    local_unknown.append("trade receipt is from the future")
                    continue
                if order_id is not None and trade.order_id != order_id:
                    continue
                if trade.side.upper() != plan.side:
                    local_unknown.append("trade side conflicts with planned leg")
                    continue
                if trade.trade_id in seen:
                    previous = next(item for item in trades if item.trade_id == trade.trade_id)
                    if previous != trade:
                        local_unknown.append("duplicate trade id has conflicting receipt fields")
                    continue
                seen.add(trade.trade_id)
                trades.append(trade)
            if not page.next_cursor:
                break
            if page.next_cursor == cursor:
                local_unknown.append("trade history cursor repeated")
                complete = False
                break
            cursor = page.next_cursor
        else:
            local_unknown.append("trade history pagination exceeded configured bound")
            complete = False
        try:
            after = _as_account(await self.client.account_snapshot(plan.account_index, plan.market_id))
        except Exception as exc:
            after = None
            local_unknown.append(f"final account read failed: {sanitize_exception(exc)}")
        try:
            order = await self._lookup_order(plan, order_id, client_order_index=plan.client_order_index)
        except Exception as exc:
            order = None
            local_unknown.append(f"final order read failed: {sanitize_exception(exc)}")
        if order is None:
            local_unknown.append("final order state is missing")
        elif not self._order_matches(order, plan):
            local_unknown.append("final order identity/parameters conflict with plan")
        elif not order.terminal:
            local_unknown.append("final order state is not terminal")
        if not complete:
            local_unknown.append("trade history is incomplete")
        expected_delta = (Decimal("-1") if plan.side == "SELL" else Decimal("1")) * sum(
            (trade.quantity for trade in trades), Decimal(0)
        )
        if after is not None and after.signed_position != before.signed_position + expected_delta:
            local_unknown.append("final position does not equal independently reconciled trade quantity")
        for reason in local_unknown:
            if reason not in unknown_reasons:
                unknown_reasons.append(reason)
        leg = LegReconciliation(
            account_index=plan.account_index,
            order_id=order_id,
            trades=tuple(trades),
            position_before=before.signed_position,
            position_after=None if after is None else after.signed_position,
            order=order,
            history_complete=complete and not local_unknown,
            unknown_reasons=tuple(local_unknown),
        )
        journal.append(
            "LEG_RECONCILED",
            {
                "account_index": plan.account_index,
                "order_id": order_id,
                "trade_ids": [trade.trade_id for trade in trades],
                "filled_quantity": str(leg.filled_quantity),
                "history_complete": leg.history_complete,
                "unknown_reasons": list(local_unknown),
            },
            run_id=run_id,
        )
        return leg

    async def _finish(
        self,
        journal: DurableJournal,
        run_id: str,
        plan: HandoffPlan,
        source: LegReconciliation,
        receiver: LegReconciliation,
        unknown_reasons: list[str],
        *,
        forced_outcome: Outcome | None = None,
    ) -> HandoffResult:
        outcome = forced_outcome or self._classify(plan, source, receiver, unknown_reasons)
        phase = Phase.COMPLETE
        reason = None if outcome is Outcome.SUCCESS else (unknown_reasons[0] if unknown_reasons else "one-attempt handoff did not prove full completion")
        journal.append(
            "COMPLETE",
            {
                "outcome": outcome.value,
                "reason": reason,
                "source_filled_quantity": str(source.filled_quantity),
                "receiver_filled_quantity": str(receiver.filled_quantity),
                "unknown_reasons": list(dict.fromkeys(unknown_reasons)),
            },
            run_id=run_id,
        )
        return HandoffResult(
            outcome=outcome,
            phase=phase,
            run_id=run_id,
            plan=plan,
            source=source,
            receiver=receiver,
            reason=reason,
            unknown_reasons=tuple(dict.fromkeys(unknown_reasons)),
        )

    def _classify(
        self,
        plan: HandoffPlan,
        source: LegReconciliation,
        receiver: LegReconciliation,
        unknown_reasons: Sequence[str],
    ) -> Outcome:
        if unknown_reasons or source.unknown_reasons or receiver.unknown_reasons:
            return Outcome.UNKNOWN
        if source.filled_quantity == plan.quantity and receiver.filled_quantity == plan.quantity:
            expected_source = plan.source_position_before - plan.quantity * plan.direction.sign
            expected_receiver = plan.receiver_position_before + plan.quantity * plan.direction.sign
            if (
                source.order is not None
                and receiver.order is not None
                and source.order.terminal
                and receiver.order.terminal
                and source.position_after == expected_source
                and receiver.position_after == expected_receiver
            ):
                return Outcome.SUCCESS
        return Outcome.PARTIAL

    async def _resume_reconciliation(self, config: HandoffConfig, journal: DurableJournal) -> HandoffResult:
        events = journal.events
        run_event = next((event for event in reversed(events) if event.event == "SOURCE_DISPATCH_INTENT"), None)
        plan_event = next((event for event in reversed(events) if event.event == "PLAN_REVIEWED"), None)
        if run_event is None or plan_event is None:
            reason = "unresolved journal lacks a complete plan; reconciliation cannot be bound safely"
            return HandoffResult(Outcome.UNKNOWN, Phase.RECONCILIATION, journal.run_id, None, None, None, reason, (reason,))
        run_id = run_event.run_id
        plan = _plan_from_dict(plan_event.payload.get("plan"))
        journal.append("RESTART_RECONCILIATION_ONLY", {"plan": plan.as_dict()}, run_id=run_id)
        try:
            source = _as_account(await self.client.account_snapshot(plan.source.account_index, plan.source.market_id))
            receiver = _as_account(await self.client.account_snapshot(plan.receiver.account_index, plan.receiver.market_id))
        except Exception as exc:
            reason = f"restart reconciliation account read failed: {sanitize_exception(exc)}"
            journal.append("COMPLETE", {"outcome": Outcome.UNKNOWN.value, "reason": reason}, run_id=run_id)
            return HandoffResult(Outcome.UNKNOWN, Phase.RECONCILIATION, run_id, plan, None, None, reason, (reason,))
        unknown: list[str] = []
        source_before = replace(source, signed_position=plan.source_position_before)
        receiver_before = replace(receiver, signed_position=plan.receiver_position_before)
        source_leg, receiver_leg = await self._reconcile(
            config,
            plan,
            source_before,
            receiver_before,
            source_order_id=_event_order_id(events, run_id, "SOURCE_DISPATCH_RESULT"),
            receiver_order_id=_event_order_id(events, run_id, "RECEIVER_DISPATCH_RESULT"),
            run_id=run_id,
            journal=journal,
            unknown_reasons=unknown,
        )
        return await self._finish(journal, run_id, plan, source_leg, receiver_leg, unknown, forced_outcome=Outcome.UNKNOWN if unknown else None)

    @property
    def _poll_limit(self) -> int:
        return getattr(self, "_configured_poll_limit", 20)

    @property
    def _poll_interval(self) -> float:
        return getattr(self, "_configured_poll_interval", 0.25)

    @property
    def _order_timeout(self) -> float:
        return getattr(self, "_configured_order_timeout", 30.0)

    async def _sleep(self, seconds: float) -> None:
        await self.clock.sleep(min(seconds, 60.0))


def _account_from_client(client: HandoffClient, role: str) -> int:
    value = getattr(client, f"{role}_account_index", None)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise PreflightBlocked(f"{role} account identity is not explicitly configured")
    return value


def _client_order_index(run_id: str, leg: str) -> int:
    digest = hashlib.sha256(f"{run_id}:{leg}".encode("utf-8")).digest()
    value = int.from_bytes(digest[:6], "big") & ((1 << 47) - 1)
    return value or 1


def _event_order_id(events: Sequence[Any], run_id: str, event_name: str) -> str | None:
    for event in reversed(events):
        if event.run_id == run_id and event.event == event_name:
            value = event.payload.get("order_id")
            return None if value is None else str(value)
    return None


def _plan_from_dict(value: Any) -> HandoffPlan:
    if not isinstance(value, Mapping):
        raise RuntimeError("journal plan is malformed")

    def order(item: Any) -> OrderPlan:
        if not isinstance(item, Mapping):
            raise RuntimeError("journal order plan is malformed")
        return OrderPlan(
            account_index=int(item["account_index"]),
            market_id=int(item["market_id"]),
            side=str(item["side"]),
            quantity=Decimal(str(item["quantity"])),
            quantity_int=int(item["quantity_int"]),
            price=Decimal(str(item["price"])),
            price_int=int(item["price_int"]),
            order_type=str(item["order_type"]),
            time_in_force=str(item["time_in_force"]),
            reduce_only=bool(item["reduce_only"]),
            order_expiry_ms=int(item["order_expiry_ms"]),
            client_order_index=int(item["client_order_index"]),
        )

    return HandoffPlan(
        run_id=str(value["run_id"]),
        source=order(value["source"]),
        receiver=order(value["receiver"]),
        source_position_before=Decimal(str(value["source_position_before"])),
        receiver_position_before=Decimal(str(value["receiver_position_before"])),
        direction=Direction(value["direction"]),
        quantity=Decimal(str(value["quantity"])),
        created_at=float(value["created_at"]),
    )


async def run_handoff(config: HandoffConfig, client: HandoffClient, *, clock: Clock | None = None) -> HandoffResult:
    engine = HandoffEngine(client, clock=clock)
    engine._configured_poll_limit = config.max_poll_count
    engine._configured_poll_interval = config.poll_interval_seconds
    return await engine.execute(config)


__all__ = ["Clock", "HandoffClient", "HandoffEngine", "SystemClock", "run_handoff"]
