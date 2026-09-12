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
from pathlib import Path
import re
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
        return MutationReceipt(
            accepted=value.accepted,
            order_id=value.order_id,
            tx_hash=_safe_tx_hash(value.tx_hash),
            error=(
                None
                if value.error is None
                else sanitize_exception(ValueError(value.error))
            ),
            response_code=value.response_code,
        )
    return MutationReceipt(
        accepted=_bool(value.get("accepted", False), "accepted"),
        order_id=None if value.get("order_id") is None else str(value["order_id"]),
        tx_hash=_safe_tx_hash(value.get("tx_hash")),
        error=None if value.get("error") is None else sanitize_exception(ValueError(str(value["error"]))),
        response_code=value.get("response_code"),
    )


def _safe_tx_hash(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if re.fullmatch(r"0x[0-9a-fA-F]{8,128}", text) else None


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
        try:
            journal = DurableJournal(config.journal_path, clock=self.clock.now)
        except Exception as exc:
            reason = f"journal unavailable: {sanitize_exception(exc)}"
            return HandoffResult(
                outcome=Outcome.UNKNOWN,
                phase=Phase.RECONCILIATION,
                run_id="",
                plan=None,
                source=None,
                receiver=None,
                reason=reason,
                unknown_reasons=(reason,),
            )
        try:
            journal.acquire_attempt()
        except Exception as exc:
            reason = f"attempt ownership unavailable: {sanitize_exception(exc)}"
            return HandoffResult(
                outcome=Outcome.UNKNOWN,
                phase=Phase.RECONCILIATION,
                run_id=journal.run_id,
                plan=None,
                source=None,
                receiver=None,
                reason=reason,
                unknown_reasons=(reason,),
            )
        try:
            return await self._execute_locked(config, journal)
        finally:
            journal.release_attempt()

    async def _execute_locked(self, config: HandoffConfig, journal: DurableJournal) -> HandoffResult:
        self._configured_poll_limit = config.max_poll_count
        self._configured_poll_interval = config.poll_interval_seconds
        self._configured_order_timeout = config.order_timeout_seconds
        self._configured_reconcile_timeout = config.reconcile_timeout_seconds
        self._configured_freshness = config.freshness_seconds
        self._configured_request_timeout = config.request_timeout_seconds
        if journal.has_unresolved_mutation():
            return await self._resume_reconciliation(config, journal)
        if journal.has_completed_mutation():
            reason = "journal already contains a completed attempt; use a new owner-only journal path"
            return HandoffResult(
                outcome=Outcome.UNKNOWN,
                phase=Phase.RECONCILIATION,
                run_id=journal.run_id,
                plan=None,
                source=None,
                receiver=None,
                reason=reason,
                unknown_reasons=(reason,),
            )
        run_id = journal.run_id
        try:
            binding = _config_binding(config, self.client)
        except Exception as exc:
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
                unknown_reasons=(reason,),
            )
        journal.append("ATTEMPT_STARTED", {"run_id": run_id, "binding": binding})
        try:
            plan, source, receiver = await self._preflight(config, journal, run_id)
        except Exception as exc:
            reason = _safe_preflight_reason(exc)
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
        plan_payload = {"plan": plan.as_dict(), "config": binding, "review_required": True}
        journal.append("PLAN_READY", plan_payload, run_id=run_id)
        if not config.operator_execution_opt_in or not config.operator_plan_reviewed:
            journal.append("PREVIEW", plan_payload, run_id=run_id)
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
                    dispatched=False,
                ),
                receiver=LegReconciliation(
                    account_index=receiver.account_index,
                    order_id=None,
                    trades=(),
                    position_before=receiver.signed_position,
                    position_after=receiver.signed_position,
                    order=None,
                    history_complete=True,
                    dispatched=False,
                ),
                reason=(
                    "explicit plan review and execution opt-in are required; no signing or mutation was attempted"
                ),
            )

        source_receipt: MutationReceipt | None = None
        source_order: OrderSnapshot | None = None
        receiver_receipt: MutationReceipt | None = None
        receiver_order: OrderSnapshot | None = None
        unknown_reasons: list[str] = []
        source_dispatch_attempted = False
        receiver_dispatch_attempted = False
        try:
            source_dispatch_attempted = True
            source_dispatch_plan = self._mutation_plan(plan.source, config)
            journal.append(
                "SOURCE_DISPATCH_INTENT",
                {"plan": source_dispatch_plan.as_dict()},
                run_id=run_id,
            )
            source_receipt = _as_receipt(
                await self._bounded(self.client.submit_order(source_dispatch_plan), "source mutation")
            )
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
                    await self._bounded(
                        self.client.account_snapshot(plan.source.account_index, plan.source.market_id),
                        "source recheck account read",
                    )
                )
                receiver_recheck = _as_account(
                    await self._bounded(
                        self.client.account_snapshot(plan.receiver.account_index, plan.receiver.market_id),
                        "receiver recheck account read",
                    )
                )
                source_order = await self._lookup_order(plan.source, source_order.order_id)
                decision_now = self.clock.now()
                if source_order is None:
                    unknown_reasons.append("source order disappeared during pre-receiver recheck")
                elif not self._snapshot_matches(
                    source_recheck, plan.source, expected_identity=plan.source_identity
                ):
                    unknown_reasons.append("source recheck account/market identity conflicts with plan")
                elif not self._snapshot_matches(
                    receiver_recheck, plan.receiver, expected_identity=plan.receiver_identity
                ):
                    unknown_reasons.append("receiver recheck account/market identity conflicts with plan")
                elif not self._snapshot_fresh(source_recheck, decision_now, config.freshness_seconds):
                    unknown_reasons.append("source recheck is stale or from the future before receiver dispatch")
                elif not self._snapshot_fresh(receiver_recheck, decision_now, config.freshness_seconds):
                    unknown_reasons.append("receiver recheck is stale or from the future before receiver dispatch")
                elif not self._time_fresh(source_order.observed_at, decision_now, config.freshness_seconds):
                    unknown_reasons.append("source order recheck is stale or from the future before receiver dispatch")
                elif source_recheck.signed_position != source.signed_position:
                    unknown_reasons.append("source position changed before receiver dispatch")
                elif not source_recheck.authorized or not source_recheck.ready:
                    unknown_reasons.append("source recheck authorization/readiness is unproven")
                elif (
                    source_recheck.margin_available is None
                    or source_recheck.margin_required is None
                    or source_recheck.margin_required > source_recheck.margin_available
                    or source_recheck.incremental_margin_required is None
                    or source_recheck.incremental_margin_required > source_recheck.margin_available
                    or not source_recheck.incremental_margin_evidence
                ):
                    unknown_reasons.append("source margin recheck is insufficient or missing")
                elif source_recheck.fee_rate is None:
                    unknown_reasons.append("source fee recheck is missing")
                elif plan.source_fee_rate is None or source_recheck.fee_rate != plan.source_fee_rate:
                    unknown_reasons.append("source fee evidence changed before receiver dispatch")
                elif not self._fee_budget_proven(config, plan.source, source_recheck.fee_rate, config.source_fee_budget):
                    unknown_reasons.append("source fee budget is no longer proven before receiver dispatch")
                elif not self._active_orders_match(source_recheck, plan.source, source_order.order_id):
                    unknown_reasons.append("source recheck contains an additional or conflicting active order")
                elif receiver_recheck.signed_position != receiver.signed_position:
                    unknown_reasons.append("receiver position changed before receiver dispatch")
                elif not receiver_recheck.authorized or not receiver_recheck.ready:
                    unknown_reasons.append("receiver recheck authorization/readiness is unproven")
                elif receiver_recheck.active_orders:
                    unknown_reasons.append("receiver active HOOD order appeared before receiver dispatch")
                elif (
                    receiver_recheck.margin_available is None
                    or receiver_recheck.margin_required is None
                    or receiver_recheck.margin_required > receiver_recheck.margin_available
                    or receiver_recheck.incremental_margin_required is None
                    or receiver_recheck.incremental_margin_required > receiver_recheck.margin_available
                    or not receiver_recheck.incremental_margin_evidence
                ):
                    unknown_reasons.append("receiver margin recheck is insufficient or missing")
                elif receiver_recheck.fee_rate is None:
                    unknown_reasons.append("receiver fee recheck is missing")
                elif plan.receiver_fee_rate is None or receiver_recheck.fee_rate != plan.receiver_fee_rate:
                    unknown_reasons.append("receiver fee evidence changed before receiver dispatch")
                elif not self._fee_budget_proven(
                    config, plan.receiver, receiver_recheck.fee_rate, config.receiver_fee_budget
                ):
                    unknown_reasons.append("receiver fee budget is no longer proven before receiver dispatch")
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
            # A dispatch can be ambiguous before the first order observation.
            # Resolve the exact client identity once more so an identified
            # remaining maker can be cancelled safely; a missing identity is
            # never guessed and remains UNKNOWN.
            if source_order is None and source_dispatch_attempted:
                try:
                    source_order = await self._lookup_order(
                        plan.source,
                        source_receipt.order_id if source_receipt is not None else None,
                        client_order_index=plan.source.client_order_index,
                    )
                except Exception as exc:
                    unknown_reasons.append(f"source cancellation lookup unresolved: {sanitize_exception(exc)}")
            if source_order is not None:
                await self._cancel_if_safe(plan.source, source_order, journal, run_id, unknown_reasons)
            source_result, receiver_result = await self._reconcile(
                config,
                plan,
                source,
                receiver,
                source_order_id=source_order.order_id if source_order else (source_receipt.order_id if source_receipt else None),
                receiver_order_id=None,
                source_dispatched=source_dispatch_attempted,
                receiver_dispatched=receiver_dispatch_attempted,
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
                config=config,
                binding=binding,
                forced_outcome=Outcome.UNKNOWN if any("unknown" in item.lower() or "unresolved" in item.lower() for item in unknown_reasons) else None,
            )

        try:
            receiver_dispatch_attempted = True
            receiver_dispatch_plan = self._mutation_plan(plan.receiver, config)
            journal.append("RECEIVER_DISPATCH_INTENT", {"plan": receiver_dispatch_plan.as_dict()}, run_id=run_id)
            receiver_receipt = _as_receipt(
                await self._bounded(self.client.submit_order(receiver_dispatch_plan), "receiver mutation")
            )
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
            try:
                current_source = await self._lookup_order(plan.source, source_order.order_id)
            except Exception as exc:
                current_source = None
                unknown_reasons.append(f"source cancellation lookup unresolved: {sanitize_exception(exc)}")
            if current_source is not None:
                await self._cancel_if_safe(plan.source, current_source, journal, run_id, unknown_reasons)
            elif not any("source cancellation lookup unresolved" in item for item in unknown_reasons):
                unknown_reasons.append("source order disappeared before cancellation reconciliation")

        source_result, receiver_result = await self._reconcile(
            config,
            plan,
            source,
            receiver,
            source_order_id=source_order.order_id if source_order else (source_receipt.order_id if source_receipt else None),
            receiver_order_id=receiver_order.order_id if receiver_order else (receiver_receipt.order_id if receiver_receipt else None),
            source_dispatched=source_dispatch_attempted,
            receiver_dispatched=receiver_dispatch_attempted,
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
            config=config,
            binding=binding,
        )

    async def _preflight(
        self,
        config: HandoffConfig,
        journal: DurableJournal,
        run_id: str,
    ) -> tuple[HandoffPlan, AccountSnapshot, AccountSnapshot]:
        metadata = _as_market(
            await self._bounded(self.client.market_metadata(config.market_id), "market metadata read")
        )
        source = _as_account(
            await self._bounded(
                self.client.account_snapshot(_account_from_client(self.client, "source"), config.market_id),
                "source account read",
            )
        )
        receiver = _as_account(
            await self._bounded(
                self.client.account_snapshot(_account_from_client(self.client, "receiver"), config.market_id),
                "receiver account read",
            )
        )
        now = self.clock.now()
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
            source_fee_rate=metadata.source_fee_rate,
            receiver_fee_rate=metadata.receiver_fee_rate,
            source_identity=source.source_identity,
            receiver_identity=receiver.source_identity,
        )
        journal.append(
            "PREFLIGHT_PROVED",
            {
                "market_id": metadata.market_id,
                "symbol": metadata.symbol,
                "source_account_index": source.account_index,
                "receiver_account_index": receiver.account_index,
                "source_identity": source.source_identity,
                "receiver_identity": receiver.source_identity,
                "source_position": str(source.signed_position),
                "receiver_position": str(receiver.signed_position),
                "binding": _config_binding(config, self.client),
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
                if order_id is not None and order.order_id != str(order_id):
                    journal.append(
                        "ORDER_OBSERVATION_ERROR",
                        {"leg": plan.side, "poll": poll, "reason": "order_id_conflict"},
                        run_id=run_id,
                    )
                    order = None
            if order is not None:
                observed_now = self.clock.now()
                if not self._time_fresh(order.observed_at, observed_now, self._configured_freshness):
                    journal.append("ORDER_OBSERVATION_ERROR", {"leg": plan.side, "poll": poll, "reason": "stale_or_future_order"}, run_id=run_id)
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
        value = await self._bounded(
            self.client.lookup_order(
                plan.account_index,
                plan.market_id,
                order_id=order_id,
                client_order_index=client_order_index,
            ),
            "order read",
        )
        return _as_order(value)

    def _order_matches(self, order: OrderSnapshot, plan: OrderPlan) -> bool:
        if order.client_order_index is None:
            return False
        identity = (
            order.account_index == plan.account_index
            and order.market_id == plan.market_id
            and order.side.upper() == plan.side
            and str(order.client_order_index) == str(plan.client_order_index)
            and order.reduce_only == plan.reduce_only
            and order.initial_quantity == plan.quantity
            and order.order_type == plan.order_type
            and order.time_in_force == plan.time_in_force
        )
        if not identity or order.price is None:
            return False
        if plan.order_type == "LIMIT":
            return order.price == plan.price
        # A market order's reported price may be its actual average.  The
        # submitted bound remains directional: BUY must not exceed it, SELL
        # must not fall below it.
        return order.price <= plan.price if plan.side == "BUY" else order.price >= plan.price

    @staticmethod
    def _snapshot_matches(
        snapshot: AccountSnapshot,
        plan: OrderPlan,
        *,
        expected_identity: str | None = None,
    ) -> bool:
        return (
            snapshot.account_index == plan.account_index
            and snapshot.market_id == plan.market_id
            and (expected_identity is None or snapshot.source_identity == expected_identity)
        )

    @staticmethod
    def _snapshot_fresh(snapshot: AccountSnapshot, now: float, freshness: float) -> bool:
        return HandoffEngine._time_fresh(snapshot.observed_at, now, freshness)

    @staticmethod
    def _time_fresh(observed_at: float, now: float, freshness: float) -> bool:
        return observed_at <= now and now - observed_at <= freshness

    def _active_orders_match(self, snapshot: AccountSnapshot, plan: OrderPlan, order_id: str) -> bool:
        active = tuple(order for order in snapshot.active_orders if order.active)
        if len(active) != 1:
            return False
        order = active[0]
        return order.order_id == order_id and self._order_matches(order, plan)

    def _source_is_resting(self, order: OrderSnapshot, plan: OrderPlan) -> bool:
        return (
            self._order_matches(order, plan)
            and order.status.lower() == "open"
            and order.filled_quantity == 0
            and order.remaining_quantity == plan.quantity
        )

    @staticmethod
    def _fee_budget_proven(
        config: HandoffConfig,
        plan: OrderPlan,
        fee_rate: Decimal,
        budget: Decimal,
    ) -> bool:
        """Check a fee ceiling only where the submitted price is a ceiling."""

        if plan.order_type == "MARKET" and plan.side == "SELL":
            # Lighter's SELL taker price is a floor.  It cannot prove a fee
            # ceiling or gross-notional ceiling before mutation.
            return False
        return plan.quantity * plan.price * fee_rate <= budget

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
        for event in journal.events:
            if event.run_id != run_id or event.event != "CANCEL_DISPATCH_INTENT":
                continue
            if (
                str(event.payload.get("account_index")) == str(plan.account_index)
                and str(event.payload.get("market_id")) == str(plan.market_id)
                and str(event.payload.get("order_id")) == str(order.order_id)
            ):
                return
        try:
            # Recheck the exact owner/market/order immediately before the only
            # cancellation mutation.  Never cancel by a bare client index.
            current = await self._lookup_order(plan, order.order_id)
            if (
                current is None
                or not self._order_matches(current, plan)
                or current.order_id != order.order_id
                or not current.active
                or not self._time_fresh(current.observed_at, self.clock.now(), self._configured_freshness)
            ):
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
                await self._bounded(
                    self.client.cancel_order(plan.account_index, plan.market_id, current.order_id),
                    "source cancellation mutation",
                )
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
        source_dispatched: bool,
        receiver_dispatched: bool,
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
            source_dispatched,
            plan.source_identity,
            run_id,
            journal,
            unknown_reasons,
        )
        receiver_leg = await self._reconcile_leg(
            config,
            plan.receiver,
            receiver,
            receiver_order_id,
            receiver_dispatched,
            plan.receiver_identity,
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
        dispatched: bool,
        expected_identity: str,
        run_id: str,
        journal: DurableJournal,
        unknown_reasons: list[str],
    ) -> LegReconciliation:
        local_unknown: list[str] = []
        if expected_identity and before.source_identity != expected_identity:
            local_unknown.append("initial account identity does not match the bound plan")
        if dispatched and order_id is None:
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
        seen: dict[tuple[Any, ...], TradeReceipt] = {}
        complete = True
        cursor: str | None = None
        deadline = self.clock.now() + config.reconcile_timeout_seconds
        if dispatched and order_id is None:
            complete = False
        for _ in range(max(1, config.max_poll_count)) if dispatched and order_id is not None else ():
            if self.clock.now() > deadline:
                local_unknown.append("reconciliation deadline exceeded")
                complete = False
                break
            try:
                page = _as_page(
                    await self._bounded(
                        self.client.list_trades(
                            plan.account_index,
                            plan.market_id,
                            order_id=order_id,
                            cursor=cursor,
                            limit=100,
                        ),
                        "trade history read",
                    )
                )
            except Exception as exc:
                local_unknown.append(f"trade history read failed: {sanitize_exception(exc)}")
                complete = False
                break
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
                if plan.order_type == "LIMIT" and trade.price != plan.price:
                    local_unknown.append("limit trade price conflicts with planned price")
                    continue
                if plan.order_type == "MARKET":
                    within_bound = (
                        trade.price <= plan.price if plan.side == "BUY" else trade.price >= plan.price
                    )
                    if not within_bound:
                        local_unknown.append("market trade price violates the directional worst-price bound")
                        continue
                economic_key = _trade_economic_key(trade)
                previous = seen.get(economic_key)
                if previous is not None:
                    # Observation timestamps can change between pages; the
                    # economic receipt identity must remain stable.
                    if previous.trade_id != trade.trade_id:
                        local_unknown.append("economic receipt has conflicting trade identity")
                    continue
                if any(item.trade_id == trade.trade_id and _trade_economic_key(item) != economic_key for item in trades):
                    local_unknown.append("duplicate trade id has conflicting receipt fields")
                    continue
                seen[economic_key] = trade
                trades.append(trade)
            if not page.next_cursor:
                complete = page.complete
                break
            if page.next_cursor == cursor:
                local_unknown.append("trade history cursor repeated")
                complete = False
                break
            cursor = page.next_cursor
        else:
            if dispatched:
                local_unknown.append("trade history pagination exceeded configured bound")
                complete = False
        try:
            after = _as_account(
                await self._bounded(
                    self.client.account_snapshot(plan.account_index, plan.market_id),
                    "final account read",
                )
            )
            after_now = self.clock.now()
            if not self._snapshot_matches(
                after, plan, expected_identity=expected_identity
            ) or not self._snapshot_fresh(
                after, after_now, config.freshness_seconds
            ):
                local_unknown.append("final account identity or freshness is not proven")
        except Exception as exc:
            after = None
            local_unknown.append(f"final account read failed: {sanitize_exception(exc)}")
        order: OrderSnapshot | None = None
        if dispatched:
            try:
                order = await self._lookup_order(plan, order_id, client_order_index=plan.client_order_index)
                order_now = self.clock.now()
            except Exception as exc:
                order = None
                order_now = self.clock.now()
                local_unknown.append(f"final order read failed: {sanitize_exception(exc)}")
            if order is None:
                local_unknown.append("final order state is missing")
            elif not self._order_matches(order, plan):
                local_unknown.append("final order identity/parameters conflict with plan")
            elif order_id is not None and order.order_id != str(order_id):
                local_unknown.append("final order identifier conflicts with dispatched order")
            elif not self._time_fresh(order.observed_at, order_now, config.freshness_seconds):
                local_unknown.append("final order state is stale or from the future")
            elif not order.terminal:
                local_unknown.append("final order state is not terminal")
            if not complete:
                local_unknown.append("trade history is incomplete")
            trade_total = sum((trade.quantity for trade in trades), Decimal(0))
            if order is not None and order.filled_quantity != trade_total:
                local_unknown.append("terminal order filled quantity conflicts with trade receipt sum")
        elif after is not None and after.signed_position != before.signed_position:
            local_unknown.append("non-dispatched leg position changed unexpectedly")
        expected_delta = (Decimal("-1") if plan.side == "SELL" else Decimal("1")) * sum(
            (trade.quantity for trade in trades), Decimal(0)
        )
        if after is not None and dispatched and after.signed_position != before.signed_position + expected_delta:
            local_unknown.append("final position does not equal independently reconciled trade quantity")
        if after is not None and any(item.active for item in after.active_orders):
            local_unknown.append("final account still has an active HOOD order")
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
            dispatched=dispatched,
        )
        journal.append(
            "LEG_RECONCILED",
            {
                "account_index": plan.account_index,
                "order_id": order_id,
                "trade_ids": [trade.trade_id for trade in trades],
                "filled_quantity": str(leg.filled_quantity),
                "fee_total": None if leg.fee_total is None else str(leg.fee_total),
                "position_before": str(leg.position_before),
                "position_after": None if leg.position_after is None else str(leg.position_after),
                "trades": [
                    {
                        "trade_id": trade.trade_id,
                        "account_index": trade.account_index,
                        "market_id": trade.market_id,
                        "order_id": trade.order_id,
                        "side": trade.side,
                        "quantity": str(trade.quantity),
                        "price": str(trade.price),
                        "fee": None if trade.fee is None else str(trade.fee),
                        "counterparty_account_index": trade.counterparty_account_index,
                        "counterparty_order_id": trade.counterparty_order_id,
                        "counterparty_client_order_index": trade.counterparty_client_order_index,
                        "observed_at": trade.observed_at,
                    }
                    for trade in trades
                ],
                "order": None
                if leg.order is None
                else {
                    "order_id": leg.order.order_id,
                    "client_order_index": leg.order.client_order_index,
                    "account_index": leg.order.account_index,
                    "market_id": leg.order.market_id,
                    "side": leg.order.side,
                    "order_type": leg.order.order_type,
                    "time_in_force": leg.order.time_in_force,
                    "reduce_only": leg.order.reduce_only,
                    "price": None if leg.order.price is None else str(leg.order.price),
                    "status": leg.order.status,
                    "initial_quantity": str(leg.order.initial_quantity),
                    "remaining_quantity": str(leg.order.remaining_quantity),
                    "filled_quantity": str(leg.order.filled_quantity),
                    "observed_at": leg.order.observed_at,
                },
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
        config: HandoffConfig,
        binding: Mapping[str, Any] | None = None,
        forced_outcome: Outcome | None = None,
    ) -> HandoffResult:
        joint_status, joint_quantity, joint_reasons = _joint_trade_match(source, receiver, plan.quantity)
        economic_status, economic_findings = _economic_findings(config, source, receiver)
        findings = tuple(dict.fromkeys((*joint_reasons, *economic_findings)))
        outcome = forced_outcome or self._classify(plan, source, receiver, unknown_reasons)
        # Exposure completion remains independently observable, but a missing
        # fee or an observed cap breach cannot be reported as an unqualified
        # bounded SUCCESS.
        if outcome is Outcome.SUCCESS and economic_status != "PROVEN":
            outcome = Outcome.UNKNOWN
        phase = Phase.COMPLETE
        reason = (
            None
            if outcome is Outcome.SUCCESS
            else (
                unknown_reasons[0]
                if unknown_reasons
                else (economic_findings[0] if economic_findings else "one-attempt handoff did not prove full completion")
            )
        )
        result = HandoffResult(
            outcome=outcome,
            phase=phase,
            run_id=run_id,
            plan=plan,
            source=source,
            receiver=receiver,
            reason=reason,
            unknown_reasons=tuple(dict.fromkeys(unknown_reasons)),
            joint_match_status=joint_status,
            joint_match_quantity=joint_quantity,
            economic_status=economic_status,
            economic_findings=economic_findings,
            findings=findings,
        )
        dispatch_evidence = [
            {
                "event": event.event,
                "at": event.at,
                "payload": event.payload,
            }
            for event in journal.events
            if event.run_id == run_id
            and event.event
            in {
                "SOURCE_DISPATCH_INTENT",
                "SOURCE_DISPATCH_RESULT",
                "SOURCE_DISPATCH_UNKNOWN",
                "RECEIVER_DISPATCH_INTENT",
                "RECEIVER_DISPATCH_RESULT",
                "RECEIVER_DISPATCH_UNKNOWN",
                "CANCEL_DISPATCH_INTENT",
                "CANCEL_DISPATCH_RESULT",
                "CANCEL_DISPATCH_UNKNOWN",
            }
        ]
        journal.append(
            "COMPLETE",
            {
                "outcome": outcome.value,
                "reason": reason,
                "binding": dict(binding or {}),
                "source_filled_quantity": str(source.filled_quantity),
                "receiver_filled_quantity": str(receiver.filled_quantity),
                "unknown_reasons": list(dict.fromkeys(unknown_reasons)),
                "joint_trade_match": {
                    "status": joint_status,
                    "quantity": str(joint_quantity),
                    "reasons": list(joint_reasons),
                },
                "economic_status": economic_status,
                "economic_findings": list(economic_findings),
                "findings": list(findings),
                "receipt": result.as_dict(),
                "dispatch_evidence": dispatch_evidence,
            },
            run_id=run_id,
        )
        return result

    def _classify(
        self,
        plan: HandoffPlan,
        source: LegReconciliation,
        receiver: LegReconciliation,
        unknown_reasons: Sequence[str],
    ) -> Outcome:
        known_partial_reasons = {
            "source fill observed before receiver dispatch",
            "source order did not prove exact resting quantity",
        }
        unresolved = [
            reason
            for reason in (*unknown_reasons, *source.unknown_reasons, *receiver.unknown_reasons)
            if reason not in known_partial_reasons
        ]
        if unresolved:
            return Outcome.UNKNOWN
        if source.filled_quantity < plan.quantity and not receiver.dispatched and not receiver.trades:
            return Outcome.PARTIAL
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
            return Outcome.UNKNOWN
        if receiver.order is None and not receiver.trades and source.filled_quantity < plan.quantity:
            return Outcome.PARTIAL
        return Outcome.PARTIAL

    async def _resume_reconciliation(self, config: HandoffConfig, journal: DurableJournal) -> HandoffResult:
        events = journal.events
        run_id = _unresolved_run_id(events)
        run_event = (
            next(
                (
                    event
                    for event in reversed(events)
                    if event.run_id == run_id and event.event == "SOURCE_DISPATCH_INTENT"
                ),
                None,
            )
            if run_id is not None
            else None
        )
        plan_event = (
            next(
                (
                    event
                    for event in reversed(events)
                    if event.run_id == run_id and event.event in {"PLAN_READY", "PLAN_REVIEWED"}
                ),
                None,
            )
            if run_id is not None
            else None
        )
        attempt_event = (
            next(
                (
                    event
                    for event in reversed(events)
                    if event.run_id == run_id and event.event == "ATTEMPT_STARTED"
                ),
                None,
            )
            if run_id is not None
            else None
        )
        try:
            binding = _config_binding(config, self.client)
        except Exception as exc:
            reason = f"restart binding cannot be established: {sanitize_exception(exc)}"
            journal.append("RESTART_BINDING_MISMATCH", {"reason": reason}, run_id=journal.run_id)
            return HandoffResult(
                Outcome.UNKNOWN,
                Phase.RECONCILIATION,
                journal.run_id,
                None,
                None,
                None,
                reason,
                (reason,),
            )
        if run_event is None or plan_event is None or attempt_event is None or run_id is None:
            reason = "unresolved journal lacks a complete plan; reconciliation cannot be bound safely"
            return HandoffResult(Outcome.UNKNOWN, Phase.RECONCILIATION, journal.run_id, None, None, None, reason, (reason,))
        if attempt_event.payload.get("binding") != binding:
            reason = "restart configuration/account/environment binding conflicts with the original attempt"
            journal.append("RESTART_BINDING_MISMATCH", {"reason": reason}, run_id=run_event.run_id)
            return HandoffResult(Outcome.UNKNOWN, Phase.RECONCILIATION, run_event.run_id, None, None, None, reason, (reason,))
        try:
            plan = _plan_from_dict(plan_event.payload.get("plan"))
        except Exception as exc:
            reason = f"journal plan cannot be reconstructed safely: {sanitize_exception(exc)}"
            journal.append("RESTART_RECONCILIATION_BLOCKED", {"reason": reason}, run_id=run_id)
            return HandoffResult(Outcome.UNKNOWN, Phase.RECONCILIATION, run_id, None, None, None, reason, (reason,))
        if plan.run_id != run_id:
            reason = "journal plan run identity conflicts with the unresolved mutation run"
            journal.append("RESTART_RECONCILIATION_BLOCKED", {"reason": reason}, run_id=run_id)
            return HandoffResult(Outcome.UNKNOWN, Phase.RECONCILIATION, run_id, None, None, None, reason, (reason,))
        if not _plan_matches_binding(plan, binding):
            reason = "journal plan identity/parameters conflict with the original configuration"
            journal.append("RESTART_RECONCILIATION_BLOCKED", {"reason": reason}, run_id=run_id)
            return HandoffResult(Outcome.UNKNOWN, Phase.RECONCILIATION, run_id, None, None, None, reason, (reason,))
        if not plan.source_identity or not plan.receiver_identity:
            reason = "journal plan lacks immutable source/receiver identities"
            journal.append("RESTART_RECONCILIATION_BLOCKED", {"reason": reason}, run_id=run_id)
            return HandoffResult(Outcome.UNKNOWN, Phase.RECONCILIATION, run_id, plan, None, None, reason, (reason,))
        preflight_event = next(
            (
                event
                for event in reversed(events)
                if event.run_id == run_id and event.event == "PREFLIGHT_PROVED"
            ),
            None,
        )
        if (
            preflight_event is None
            or preflight_event.payload.get("source_identity") != plan.source_identity
            or preflight_event.payload.get("receiver_identity") != plan.receiver_identity
        ):
            reason = "journal plan account identities are not bound to the original preflight evidence"
            journal.append("RESTART_RECONCILIATION_BLOCKED", {"reason": reason}, run_id=run_id)
            return HandoffResult(Outcome.UNKNOWN, Phase.RECONCILIATION, run_id, plan, None, None, reason, (reason,))
        journal.append("RESTART_RECONCILIATION_ONLY", {"plan": plan.as_dict()}, run_id=run_id)
        try:
            source = _as_account(
                await self._bounded(
                    self.client.account_snapshot(plan.source.account_index, plan.source.market_id),
                    "restart source account read",
                )
            )
            receiver = _as_account(
                await self._bounded(
                    self.client.account_snapshot(plan.receiver.account_index, plan.receiver.market_id),
                    "restart receiver account read",
                )
            )
        except Exception as exc:
            reason = f"restart reconciliation account read failed: {sanitize_exception(exc)}"
            journal.append(
                "RESTART_RECONCILIATION_BLOCKED",
                {"reason": reason, "binding": dict(binding), "plan": plan.as_dict()},
                run_id=run_id,
            )
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
            source_dispatched=True,
            receiver_dispatched=any(event.run_id == run_id and event.event == "RECEIVER_DISPATCH_INTENT" for event in events),
            run_id=run_id,
            journal=journal,
            unknown_reasons=unknown,
        )
        return await self._finish(
            journal,
            run_id,
            plan,
            source_leg,
            receiver_leg,
            unknown,
            config=config,
            binding=binding,
            forced_outcome=Outcome.UNKNOWN if unknown else None,
        )

    @property
    def _poll_limit(self) -> int:
        return getattr(self, "_configured_poll_limit", 20)

    @property
    def _poll_interval(self) -> float:
        return getattr(self, "_configured_poll_interval", 0.25)

    @property
    def _order_timeout(self) -> float:
        return getattr(self, "_configured_order_timeout", 30.0)

    @property
    def _request_timeout(self) -> float:
        return getattr(self, "_configured_request_timeout", 30.0)

    async def _bounded(self, awaitable: Any, label: str) -> Any:
        """Bound every SDK/read boundary without ever retrying a mutation."""

        try:
            return await asyncio.wait_for(awaitable, timeout=self._request_timeout)
        except asyncio.TimeoutError as exc:
            raise TimeoutError(f"{label} exceeded configured request timeout") from exc

    @staticmethod
    def _mutation_plan(plan: OrderPlan, config: HandoffConfig) -> OrderPlan:
        # The SDK uses this monotonic barrier for nonce reads, signing and the
        # single sendTx call.  It is set only after the latest state recheck.
        deadline = time.monotonic() + min(config.request_timeout_seconds, config.freshness_seconds)
        return replace(plan, mutation_deadline_monotonic=deadline)

    async def _sleep(self, seconds: float) -> None:
        await self.clock.sleep(min(seconds, 60.0))


def _account_from_client(client: HandoffClient, role: str) -> int:
    value = getattr(client, f"{role}_account_index", None)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise PreflightBlocked(f"{role} account identity is not explicitly configured")
    return value


def _safe_preflight_reason(exc: BaseException) -> str:
    """Expose only named, non-sensitive contract conflicts to the operator."""

    if isinstance(exc, PreflightBlocked) and str(exc) == (
        "Lighter MARKET SELL exposes only a minimum price; the configured gross/fee ceiling cannot be enforced before dispatch"
    ):
        return "protocol_conflict_receiver_sell_market_price_floor"
    return sanitize_exception(exc)


def _client_order_index(run_id: str, leg: str) -> int:
    digest = hashlib.sha256(f"{run_id}:{leg}".encode("utf-8")).digest()
    value = int.from_bytes(digest[:6], "big") & ((1 << 47) - 1)
    return value or 1


def _unresolved_run_id(events: Sequence[Any]) -> str | None:
    """Select the newest run that still needs read-only reconciliation."""

    mutation_events = {"SOURCE_DISPATCH_INTENT", "RECEIVER_DISPATCH_INTENT", "CANCEL_DISPATCH_INTENT"}
    candidates: list[tuple[int, str]] = []
    for run_id in {event.run_id for event in events}:
        run_events = [event for event in events if event.run_id == run_id]
        if not any(event.event in mutation_events for event in run_events):
            continue
        last = run_events[-1]
        if last.event == "COMPLETE" and last.payload.get("outcome") != Outcome.UNKNOWN.value:
            continue
        candidates.append((last.sequence, run_id))
    return max(candidates)[1] if candidates else None


def _event_order_id(events: Sequence[Any], run_id: str, event_name: str) -> str | None:
    for event in reversed(events):
        if event.run_id == run_id and event.event == event_name:
            value = event.payload.get("order_id")
            return None if value is None else str(value)
    return None


def _trade_economic_key(trade: TradeReceipt) -> tuple[Any, ...]:
    return (
        trade.trade_id,
        trade.account_index,
        trade.market_id,
        trade.order_id,
        trade.side.upper(),
        trade.quantity,
        trade.price,
        trade.fee,
        trade.counterparty_account_index,
        trade.counterparty_order_id,
        trade.counterparty_client_order_index,
        trade.client_order_index,
    )


def _joint_trade_match(
    source: LegReconciliation,
    receiver: LegReconciliation,
    expected_quantity: Decimal,
) -> tuple[str, Decimal, tuple[str, ...]]:
    """Report optional direct pairing without gating independently proven exposure."""

    if not source.trades or not receiver.trades:
        return "UNKNOWN", Decimal(0), ("joint trade matching is UNKNOWN because one leg has no trade receipt",)
    if any(
        trade.counterparty_account_index is None
        for trade in (*source.trades, *receiver.trades)
    ):
        return "UNKNOWN", Decimal(0), ("joint trade matching is UNKNOWN because a counterparty account is missing",)

    used_receiver: set[int] = set()
    matched = Decimal(0)
    conflicts = False
    for source_trade in source.trades:
        candidates = [
            (index, receiver_trade)
            for index, receiver_trade in enumerate(receiver.trades)
            if index not in used_receiver
            and source_trade.counterparty_account_index == receiver.account_index
            and receiver_trade.counterparty_account_index == source.account_index
            and source_trade.side != receiver_trade.side
        ]
        if not candidates:
            if source_trade.counterparty_account_index == receiver.account_index:
                conflicts = True
            continue
        compatible: list[tuple[int, TradeReceipt]] = []
        for index, receiver_trade in candidates:
            order_ids_compatible = (
                source_trade.counterparty_order_id is None
                or source_trade.counterparty_order_id == receiver_trade.order_id
            ) and (
                receiver_trade.counterparty_order_id is None
                or receiver_trade.counterparty_order_id == source_trade.order_id
            )
            client_ids_compatible = (
                source_trade.counterparty_client_order_index is None
                or receiver_trade.client_order_index is None
                or str(source_trade.counterparty_client_order_index)
                == str(receiver_trade.client_order_index)
            ) and (
                receiver_trade.counterparty_client_order_index is None
                or source_trade.client_order_index is None
                or str(receiver_trade.counterparty_client_order_index)
                == str(source_trade.client_order_index)
            )
            if (
                source_trade.trade_id == receiver_trade.trade_id
                and source_trade.quantity == receiver_trade.quantity
                and source_trade.price == receiver_trade.price
                and source_trade.order_id != receiver_trade.order_id
                and order_ids_compatible
                and client_ids_compatible
            ):
                compatible.append((index, receiver_trade))
        if compatible:
            index, receiver_trade = compatible[0]
            used_receiver.add(index)
            matched += min(source_trade.quantity, receiver_trade.quantity)
        else:
            conflicts = True

    if any(
        index not in used_receiver
        and trade.counterparty_account_index == source.account_index
        for index, trade in enumerate(receiver.trades)
    ):
        conflicts = True

    if matched >= expected_quantity:
        status = "MATCHED"
    elif matched > 0:
        status = "PARTIAL"
    elif conflicts:
        status = "CONFLICTING"
    else:
        status = "KNOWN_ZERO"
    reasons: list[str] = []
    if status == "MATCHED":
        reasons.append("joint trade matching is independently proven from compatible trade/account/order identities")
    elif status == "PARTIAL":
        reasons.append("joint trade matching proves only part of the exposure quantity")
    elif status == "CONFLICTING":
        reasons.append("joint trade matching is CONFLICTING: trade/order IDs, prices or quantities are incompatible")
    else:
        reasons.append("joint trade matching is KNOWN_ZERO: receipts name no direct source/receiver pair")
    return status, matched, tuple(reasons)


def _economic_findings(
    config: HandoffConfig,
    source: LegReconciliation,
    receiver: LegReconciliation,
) -> tuple[str, tuple[str, ...]]:
    """Reconcile observed economics without converting undocumented fee units."""

    findings: list[str] = []
    has_unknown = False
    has_violation = False
    for label, leg, budget in (
        ("source", source, config.source_fee_budget),
        ("receiver", receiver, config.receiver_fee_budget),
    ):
        if not leg.trades:
            continue
        gross = leg.gross_notional
        if gross > config.max_gross_notional:
            has_violation = True
            findings.append(
                f"{label} observed gross {gross} exceeds configured max_gross_notional {config.max_gross_notional}"
            )
        fee_total = leg.fee_total
        if fee_total is None:
            has_unknown = True
            findings.append(f"{label} fee economics UNKNOWN: at least one official receipt has no fee")
        elif fee_total > budget:
            has_violation = True
            findings.append(
                f"{label} observed fee {fee_total} exceeds configured fee budget {budget}"
            )
    if has_violation:
        return "VIOLATION", tuple(findings)
    if has_unknown:
        return "UNKNOWN", tuple(findings)
    return ("PROVEN" if (source.trades or receiver.trades) else "NOT_OBSERVED"), tuple(findings)


def _config_binding(config: HandoffConfig, client: HandoffClient) -> dict[str, Any]:
    return {
        "market_id": config.market_id,
        "market_symbol": config.market_symbol.upper(),
        "environment": config.environment.lower(),
        "journal_path": config.journal_path,
        "api_base_url": config.api_base_url,
        "chain_id": config.chain_id,
        "api_key_index": config.api_key_index,
        "source_account_index": _account_from_client(client, "source"),
        "receiver_account_index": _account_from_client(client, "receiver"),
        "direction": config.direction.value,
        "quantity": str(config.quantity),
        "source_limit_price": str(config.source_limit_price),
        "receiver_worst_price": str(config.receiver_worst_price),
        "receiver_price_cap": None if config.receiver_price_cap is None else str(config.receiver_price_cap),
        "operator_execution_opt_in": config.operator_execution_opt_in,
        "operator_plan_reviewed": config.operator_plan_reviewed,
        "max_gross_notional": str(config.max_gross_notional),
        "source_fee_budget": str(config.source_fee_budget),
        "receiver_fee_budget": str(config.receiver_fee_budget),
        "freshness_seconds": config.freshness_seconds,
        "request_timeout_seconds": config.request_timeout_seconds,
        "order_timeout_seconds": config.order_timeout_seconds,
        "reconcile_timeout_seconds": config.reconcile_timeout_seconds,
        "poll_interval_seconds": config.poll_interval_seconds,
        "max_poll_count": config.max_poll_count,
        "client_order_prefix": config.client_order_prefix,
        "source_order_lifetime_seconds": config.source_order_lifetime_seconds,
        "auth_token_lifetime_seconds": config.auth_token_lifetime_seconds,
        "sdk_version": getattr(client, "sdk_version", None),
        "implementation_fingerprint": _implementation_fingerprint(),
    }


def _implementation_fingerprint() -> str:
    """Bind a journaled attempt to the exact HCR-1 source files that ran."""

    package_dir = Path(__file__).resolve().parent
    digest = hashlib.sha256()
    for name in ("contracts.py", "engine.py", "journal.py", "sdk.py", "cli.py"):
        path = package_dir / name
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return f"sha256:{digest.hexdigest()}"


def _plan_from_dict(value: Any) -> HandoffPlan:
    if not isinstance(value, Mapping):
        raise RuntimeError("journal plan is malformed")

    def exact_int(item: Mapping[str, Any], key: str, *, minimum: int = 0) -> int:
        value = item.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            raise RuntimeError(f"journal order field {key} is malformed")
        return value

    def exact_bool(item: Mapping[str, Any], key: str) -> bool:
        value = item.get(key)
        if not isinstance(value, bool):
            raise RuntimeError(f"journal order field {key} is malformed")
        return value

    def order(item: Any) -> OrderPlan:
        if not isinstance(item, Mapping):
            raise RuntimeError("journal order plan is malformed")
        return OrderPlan(
            account_index=exact_int(item, "account_index"),
            market_id=exact_int(item, "market_id"),
            side=item.get("side"),
            quantity=Decimal(str(item["quantity"])),
            quantity_int=exact_int(item, "quantity_int", minimum=1),
            price=Decimal(str(item["price"])),
            price_int=exact_int(item, "price_int", minimum=1),
            order_type=item.get("order_type"),
            time_in_force=item.get("time_in_force"),
            reduce_only=exact_bool(item, "reduce_only"),
            order_expiry_ms=exact_int(item, "order_expiry_ms"),
            client_order_index=exact_int(item, "client_order_index"),
        )

    return HandoffPlan(
        run_id=value.get("run_id"),
        source=order(value["source"]),
        receiver=order(value["receiver"]),
        source_position_before=Decimal(str(value["source_position_before"])),
        receiver_position_before=Decimal(str(value["receiver_position_before"])),
        direction=Direction(value["direction"]),
        quantity=Decimal(str(value["quantity"])),
        created_at=float(value["created_at"]),
        source_fee_rate=(
            None if value.get("source_fee_rate") is None else Decimal(str(value["source_fee_rate"]))
        ),
        receiver_fee_rate=(
            None if value.get("receiver_fee_rate") is None else Decimal(str(value["receiver_fee_rate"]))
        ),
        source_identity=("" if value.get("source_identity") is None else str(value["source_identity"])),
        receiver_identity=("" if value.get("receiver_identity") is None else str(value["receiver_identity"])),
    )


def _plan_matches_binding(plan: HandoffPlan, binding: Mapping[str, Any]) -> bool:
    """Keep read-only restart tied to the original configured operation."""

    try:
        return (
            plan.source.account_index == int(binding["source_account_index"])
            and plan.receiver.account_index == int(binding["receiver_account_index"])
            and plan.source.market_id == int(binding["market_id"])
            and plan.receiver.market_id == int(binding["market_id"])
            and plan.direction.value == str(binding["direction"])
            and plan.quantity == Decimal(str(binding["quantity"]))
            and plan.source.quantity == plan.quantity
            and plan.receiver.quantity == plan.quantity
            and plan.source.price == Decimal(str(binding["source_limit_price"]))
            and plan.receiver.price == Decimal(str(binding["receiver_worst_price"]))
        )
    except (KeyError, TypeError, ValueError, ArithmeticError):
        return False


async def run_handoff(config: HandoffConfig, client: HandoffClient, *, clock: Clock | None = None) -> HandoffResult:
    engine = HandoffEngine(client, clock=clock)
    engine._configured_poll_limit = config.max_poll_count
    engine._configured_poll_interval = config.poll_interval_seconds
    return await engine.execute(config)


__all__ = ["Clock", "HandoffClient", "HandoffEngine", "SystemClock", "run_handoff"]
