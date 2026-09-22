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
import inspect
import math
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
    OperationMode,
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

    def monotonic(self) -> float:
        return time.monotonic()

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


def _book_decimal(value: Any, name: str) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise ContractError(f"{name} is missing or malformed")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except Exception as exc:
        raise ContractError(f"{name} is missing or malformed") from exc
    if not parsed.is_finite() or parsed <= 0:
        raise ContractError(f"{name} is missing or malformed")
    return parsed


def _book_level(value: Any, label: str) -> dict[str, Any]:
    if isinstance(value, Mapping):
        price = value.get("price")
        quantity = None
        for name in ("remaining_base_amount", "quantity", "size", "base_amount"):
            if name in value:
                quantity = value[name]
                break
        raw_order_id = value.get("order_id", value.get("order_index"))
        raw_owner = value.get("owner_account_index", value.get("owner_account_id"))
    else:
        price = getattr(value, "price", None)
        quantity = None
        for name in ("remaining_base_amount", "quantity", "size", "base_amount"):
            candidate = getattr(value, name, None)
            if candidate is not None:
                quantity = candidate
                break
        raw_order_id = getattr(value, "order_id", None)
        if raw_order_id is None:
            raw_order_id = getattr(value, "order_index", None)
        raw_owner = getattr(value, "owner_account_index", None)
        if raw_owner is None:
            raw_owner = getattr(value, "owner_account_id", None)
    if quantity is None:
        raise ContractError(f"{label} quantity is missing")
    order_id = None if raw_order_id is None else str(raw_order_id).strip()
    if order_id == "":
        raise ContractError(f"{label} order identity is empty")
    owner = None
    if raw_owner is not None:
        if isinstance(raw_owner, bool):
            raise ContractError(f"{label} owner identity is malformed")
        try:
            owner = int(raw_owner)
        except (TypeError, ValueError) as exc:
            raise ContractError(f"{label} owner identity is malformed") from exc
        if owner < 0:
            raise ContractError(f"{label} owner identity is malformed")
    return {
        "price": _book_decimal(price, f"{label} price"),
        "quantity": _book_decimal(quantity, f"{label} quantity"),
        "order_id": order_id,
        "owner_account_index": owner,
    }


def _book_optional_metadata(value: Any, name: str) -> str | int | float | None:
    """Retain only bounded venue metadata that was actually supplied."""

    if value is None:
        return None
    if isinstance(value, bool):
        raise ContractError(f"{name} is malformed")
    if isinstance(value, str):
        text = value.strip()
        if not text or len(text) > 128:
            raise ContractError(f"{name} is malformed")
        return text
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ContractError(f"{name} is malformed")
        return value
    raise ContractError(f"{name} is malformed")


def _coerce_public_book(value: Any, config: HandoffConfig, now: float) -> dict[str, Any]:
    """Decode one fresh book without trusting SDK object identity implicitly."""

    if isinstance(value, Mapping):
        market_id = value.get("market_id")
        symbol = value.get("symbol")
        market_type = value.get("market_type")
        venue = value.get("venue")
        observed_at = value.get("observed_at")
        bids = value.get("bids")
        asks = value.get("asks")
        venue_timestamp = value.get("venue_timestamp", value.get("exchange_timestamp"))
        version = value.get("version", value.get("book_version", value.get("sequence", value.get("seq"))))
    else:
        market_id = getattr(value, "market_id", None)
        symbol = getattr(value, "symbol", None)
        market_type = getattr(value, "market_type", None)
        venue = getattr(value, "venue", None)
        observed_at = getattr(value, "observed_at", None)
        bids = getattr(value, "bids", None)
        asks = getattr(value, "asks", None)
        venue_timestamp = getattr(value, "venue_timestamp", None)
        if venue_timestamp is None:
            venue_timestamp = getattr(value, "exchange_timestamp", None)
        version = getattr(value, "version", None)
        if version is None:
            version = getattr(value, "book_version", None)
        if version is None:
            version = getattr(value, "sequence", None)
        if version is None:
            version = getattr(value, "seq", None)
    if isinstance(market_id, bool) or not isinstance(market_id, int) or market_id != config.market_id:
        raise PreflightBlocked("pre-receiver public book market identity conflicts with plan")
    if not isinstance(symbol, str) or symbol.strip().upper() != config.market_symbol.upper():
        raise PreflightBlocked("pre-receiver public book symbol conflicts with plan")
    if not isinstance(market_type, str) or market_type.strip().lower() != "perp":
        raise PreflightBlocked("pre-receiver public book is not a perpetual market")
    if not isinstance(venue, str) or venue.strip().lower() not in {"robinhood", "robinhood-chain"}:
        raise PreflightBlocked("pre-receiver public book is from the wrong venue")
    if isinstance(observed_at, bool) or observed_at is None:
        raise PreflightBlocked("pre-receiver public book timestamp is missing")
    try:
        observed = float(observed_at)
    except (TypeError, ValueError) as exc:
        raise PreflightBlocked("pre-receiver public book timestamp is malformed") from exc
    if not math.isfinite(observed) or observed < 0:
        raise PreflightBlocked("pre-receiver public book timestamp is malformed")
    if observed > now:
        raise PreflightBlocked("pre-receiver public book is from the future")
    if now - observed > config.freshness_seconds:
        raise PreflightBlocked("pre-receiver public book is stale")
    if not isinstance(bids, (tuple, list)) or not isinstance(asks, (tuple, list)):
        raise PreflightBlocked("pre-receiver public book lacks bids/asks arrays")
    if not bids or not asks:
        raise PreflightBlocked("pre-receiver public book is not two-sided")
    parsed_bids = tuple(_book_level(item, f"bid[{index}]") for index, item in enumerate(bids))
    parsed_asks = tuple(_book_level(item, f"ask[{index}]") for index, item in enumerate(asks))
    for levels, descending, label in ((parsed_bids, True, "bids"), (parsed_asks, False, "asks")):
        previous: Decimal | None = None
        for level in levels:
            if previous is not None and (
                (descending and level["price"] > previous)
                or (not descending and level["price"] < previous)
            ):
                raise PreflightBlocked(f"pre-receiver public book {label} are not in price order")
            previous = level["price"]
    if parsed_bids[0]["price"] >= parsed_asks[0]["price"]:
        raise PreflightBlocked("pre-receiver public book is crossed")
    identities = [
        level["order_id"]
        for level in (*parsed_bids, *parsed_asks)
        if level["order_id"] is not None
    ]
    if len(identities) != len(set(identities)):
        raise PreflightBlocked("pre-receiver public book contains duplicate order identity")
    payload = {
        "market_id": market_id,
        "symbol": symbol.strip().upper(),
        "market_type": market_type.strip().lower(),
        "venue": venue.strip().lower(),
        "observed_at": observed,
        "bids": parsed_bids,
        "asks": parsed_asks,
    }
    if venue_timestamp is not None:
        payload["venue_timestamp"] = _book_optional_metadata(venue_timestamp, "venue timestamp")
    if version is not None:
        payload["version"] = _book_optional_metadata(version, "public book version")
    return payload


def _prepared_timing_values(prepared: Any, leg: str) -> dict[str, float]:
    """Export only finite numeric diagnostics, never the prepared token itself."""
    values = getattr(prepared, "diagnostic_timings", None)
    if not isinstance(values, dict):
        return {}
    return {
        f"{leg}_{name}": float(value)
        for name in ("preparation_lock_wait_seconds", "nonce_acquisition_seconds", "signing_call_seconds", "transport_roundtrip_seconds", "nonce_reserved_before_quote")
        if isinstance((value := values.get(name)), (int, float))
        and not isinstance(value, bool) and math.isfinite(value) and value >= 0
    }


@dataclass(slots=True, repr=False)
class HandoffPreflightContext:
    """One child plan's original observations, carried across the final quote."""

    config: HandoffConfig
    metadata: MarketMetadata
    source: AccountSnapshot
    receiver: AccountSnapshot
    reserved_nonces: Mapping[int, Any]
    nonce_deadline: float | None = None
    used: bool = False

    def claim(self, config: HandoffConfig) -> tuple[MarketMetadata, AccountSnapshot, AccountSnapshot]:
        if self.used or config != self.config:
            raise PreflightBlocked("prepared preflight context is consumed or bound to another plan")
        self.used = True
        return self.metadata, self.source, self.receiver


class HandoffEngine:
    """Execute or reconcile exactly one close/reopen attempt."""

    def __init__(self, client: HandoffClient, *, clock: Clock | None = None) -> None:
        self.client = client
        self.clock = clock or SystemClock()
        self._last_pre_visibility_refresh = False
        self._last_pre_visibility_refresh_seconds = 0.0
        self._last_pre_visibility_original_checks: tuple[
            AccountSnapshot,
            AccountSnapshot,
            dict[str, Any],
            float,
            float,
        ] | None = None
        self._visibility_source_baseline: AccountSnapshot | None = None
        self._visibility_receiver_baseline: AccountSnapshot | None = None
        self._visibility_requires_incremental_margin = False
        self._preflight_context: HandoffPreflightContext | None = None
        self._source_dispatch_monotonic: float | None = None

    async def _read_public_book(
        self,
        config: HandoffConfig,
    ) -> tuple[dict[str, Any], float]:
        """Read and validate the public book used by the paired guard."""

        method = getattr(self.client, "order_book", None)
        if not callable(method):
            method = getattr(self.client, "order_book_snapshot", None)
        if not callable(method):
            method = getattr(self.client, "public_order_book", None)
        if not callable(method):
            raise ContractError("paired operation client does not expose a public order-book reader")
        started = time.perf_counter()
        value = await self._bounded(method(config.market_id), "pre-receiver public order-book read")
        duration = max(0.0, time.perf_counter() - started)
        return _coerce_public_book(value, config, self.clock.now()), duration

    async def _parallel_pre_receiver_checks(
        self,
        config: HandoffConfig,
        *,
        visibility_state: dict[str, float | bool | None] | None = None,
    ) -> tuple[AccountSnapshot, AccountSnapshot, dict[str, Any], float, float]:
        """Run independent account and public-book reads in one bounded window."""

        started = time.perf_counter()
        book_reader = getattr(self.client, "order_book", None)
        if not callable(book_reader):
            book_reader = getattr(self.client, "order_book_snapshot", None)
        if not callable(book_reader):
            book_reader = getattr(self.client, "public_order_book", None)
        if not callable(book_reader):
            raise ContractError("paired operation requires a public order-book reader")
        async def read_accounts() -> tuple[AccountSnapshot, AccountSnapshot]:
            value = await self._parallel_account_rechecks(
                _account_from_client(self.client, "source"),
                _account_from_client(self.client, "receiver"),
                config.market_id,
            )
            if visibility_state is not None:
                visibility_state["accounts_finished_at"] = time.perf_counter()
            return value

        async def read_book() -> tuple[dict[str, Any], float]:
            value = await self._read_public_book(config)
            if visibility_state is not None:
                visibility_state["book_finished_at"] = time.perf_counter()
            return value

        account_task = asyncio.create_task(read_accounts())
        book_task = asyncio.create_task(read_book())
        try:
            (source, receiver), (book, book_duration) = await asyncio.gather(account_task, book_task)
        except BaseException:
            tasks = (account_task, book_task)
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        if visibility_state is not None:
            visible_at = visibility_state.get("visible_finished_at")
            component_finished = tuple(
                value
                for value in (
                    visibility_state.get("accounts_finished_at"),
                    visibility_state.get("book_finished_at"),
                )
                if isinstance(value, (int, float))
            )
            visibility_state["component_pre_visibility"] = bool(
                isinstance(visible_at, (int, float))
                and any(value < visible_at for value in component_finished)
            )
        return source, receiver, book, book_duration, max(0.0, time.perf_counter() - started)

    def _visibility_account_is_eligible(
        self,
        snapshot: AccountSnapshot,
        baseline: AccountSnapshot | None,
        *,
        account_index: int,
        market_id: int,
    ) -> bool:
        """Check that an early account read contains no contradictory evidence."""

        if baseline is None:
            return False
        if (
            snapshot.account_index != account_index
            or snapshot.market_id != market_id
            or snapshot.source_identity != baseline.source_identity
            or snapshot.signed_position != baseline.signed_position
            or not snapshot.authorized
            or not snapshot.ready
            or not self._snapshot_fresh(
                snapshot,
                self.clock.now(),
                self._configured_freshness,
            )
        ):
            return False
        if (
            snapshot.margin_available is None
            or snapshot.margin_required is None
            or snapshot.margin_required > snapshot.margin_available
        ):
            return False
        if self._visibility_requires_incremental_margin and (
            snapshot.incremental_margin_required is None
            or not snapshot.incremental_margin_evidence
        ):
            return False
        if (
            snapshot.incremental_margin_required is not None
            and snapshot.incremental_margin_evidence
            and snapshot.incremental_margin_required > snapshot.margin_available
        ):
            return False
        return True

    def _visibility_source_order_status(
        self,
        snapshot: AccountSnapshot,
        plan: OrderPlan,
        source_order: OrderSnapshot,
    ) -> str:
        """Classify source account evidence without treating contradictions as absence."""

        for order in snapshot.active_orders:
            if order.filled_quantity > 0 or order.remaining_quantity != order.initial_quantity:
                return "CONFLICT"
        active = tuple(order for order in snapshot.active_orders if order.active)
        if not active:
            return "ABSENT"
        if len(active) == 1 and (
            active[0].order_id == source_order.order_id
            and self._order_matches(active[0], plan)
            and active[0].filled_quantity == 0
            and active[0].remaining_quantity == plan.quantity
        ):
            return "EXACT"
        return "CONFLICT"

    @staticmethod
    def _visibility_receiver_order_status(snapshot: AccountSnapshot) -> str:
        """Receiver active/fill evidence is always a conflict for this window."""

        if any(
            order.active or order.filled_quantity > 0
            for order in snapshot.active_orders
        ):
            return "CONFLICT"
        return "CLEAR"

    def _visibility_book_status(
        self,
        public_book: Mapping[str, Any],
        plan: OrderPlan,
        source_order: OrderSnapshot,
    ) -> str:
        """Classify exact source-book proof while retaining foreign priority evidence."""

        levels = public_book.get("asks" if plan.side == "SELL" else "bids", ())
        exact_source = False
        foreign_priority = False
        source_price = plan.price
        for level in levels:
            exact = (
                level.get("order_id") is not None
                and str(level.get("order_id")) == str(source_order.order_id)
                and level.get("owner_account_index") == plan.account_index
                and level.get("price") == source_price
                and level.get("quantity") == plan.quantity
            )
            if exact:
                exact_source = True
                continue
            price = level.get("price")
            if price == source_price or (
                plan.side == "SELL" and price < source_price
            ) or (
                plan.side == "BUY" and price > source_price
            ):
                foreign_priority = True
        if foreign_priority:
            return "CONFLICT"
        return "EXACT" if exact_source else "ABSENT"

    def _visibility_refresh_is_eligible(
        self,
        config: HandoffConfig,
        plan: OrderPlan,
        source_order: OrderSnapshot | None,
        checks: tuple[AccountSnapshot, AccountSnapshot, dict[str, Any], float, float],
        *,
        pre_visibility: bool,
    ) -> bool:
        """Permit one refresh only when the early window proves causal absence."""

        if (
            not pre_visibility
            or source_order is None
            or not self._source_is_resting(source_order, plan)
            or not self._time_fresh(
                source_order.observed_at,
                self.clock.now(),
                self._configured_freshness,
            )
        ):
            return False
        original_source, original_receiver, original_book, _, _ = checks
        if not self._time_fresh(
            original_book["observed_at"],
            self.clock.now(),
            self._configured_freshness,
        ):
            return False
        if not self._visibility_account_is_eligible(
            original_source,
            self._visibility_source_baseline,
            account_index=plan.account_index,
            market_id=plan.market_id,
        ) or not self._visibility_account_is_eligible(
            original_receiver,
            self._visibility_receiver_baseline,
            account_index=(
                self._visibility_receiver_baseline.account_index
                if self._visibility_receiver_baseline is not None
                else -1
            ),
            market_id=plan.market_id,
        ):
            return False
        source_status = self._visibility_source_order_status(
            original_source,
            plan,
            source_order,
        )
        receiver_status = self._visibility_receiver_order_status(original_receiver)
        book_status = self._visibility_book_status(original_book, plan, source_order)
        if source_status == "CONFLICT" or receiver_status == "CONFLICT" or book_status == "CONFLICT":
            return False
        # A refresh is useful only when the first window lacks source proof.
        # Complete owner-bound account and book evidence stays on the fast path.
        return source_status == "ABSENT" or book_status == "ABSENT"

    async def _parallel_source_visibility_and_checks(
        self,
        config: HandoffConfig,
        plan: OrderPlan,
        receipt: MutationReceipt,
        journal: DurableJournal,
        run_id: str,
    ) -> tuple[
        OrderSnapshot | None,
        tuple[AccountSnapshot, AccountSnapshot, dict[str, Any], float, float],
        float,
    ]:
        """Overlap independent source visibility with the guarded read window.

        Source order visibility, the two account snapshots, and the public
        book are all read-only after the source mutation intent is durable.
        They are drained together before the caller can cancel or dispatch the
        receiver.  The exact source lookup remains in the caller after this
        window and therefore remains the final owner-bound admission boundary.
        """

        started = time.perf_counter()
        visibility_finished_at: float | None = None
        visibility_state: dict[str, float | bool | None] = {}
        checks_finished_at: float | None = None
        self._last_visibility_timings = {}

        async def observe_source() -> OrderSnapshot | None:
            nonlocal visibility_finished_at
            value = await self._poll_order(
                plan,
                receipt.order_id,
                journal,
                run_id,
                require_terminal=False,
            )
            visibility_finished_at = time.perf_counter()
            self._last_visibility_timings["source_visibility_lookup_seconds"] = max(0.0, visibility_finished_at - started)
            visibility_state["visible_finished_at"] = visibility_finished_at
            return value

        async def read_checks() -> tuple[AccountSnapshot, AccountSnapshot, dict[str, Any], float, float]:
            nonlocal checks_finished_at
            value = await self._parallel_pre_receiver_checks(
                config,
                visibility_state=visibility_state,
            )
            checks_finished_at = time.perf_counter()
            return value

        visibility_task = asyncio.create_task(observe_source())
        checks_task = asyncio.create_task(read_checks())
        try:
            source_order, checks = await asyncio.gather(visibility_task, checks_task)
        except BaseException:
            for task in (visibility_task, checks_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(visibility_task, checks_task, return_exceptions=True)
            raise
        # Account/book snapshots that completed before source visibility may
        # legitimately omit the newly accepted order.  Refresh only a
        # validated causal absence: identity, baseline position, readiness,
        # margin, fill and conflicting-order evidence remain authoritative.
        self._last_pre_visibility_refresh = False
        self._last_pre_visibility_refresh_seconds = 0.0
        self._last_visibility_timings["initial_account_checks_seconds"] = max(
            0.0, float(visibility_state.get("accounts_finished_at") or started) - started)
        self._last_visibility_timings["initial_public_book_seconds"] = checks[3]
        pre_visibility = (
            visibility_finished_at is not None
            and checks_finished_at is not None
            and (
                checks_finished_at < visibility_finished_at
                or visibility_state.get("component_pre_visibility") is True
            )
        )
        if pre_visibility:
            self._last_pre_visibility_original_checks = checks
        if self._visibility_refresh_is_eligible(
            config,
            plan,
            source_order,
            checks,
            pre_visibility=pre_visibility,
        ):
            refresh_started = time.perf_counter()
            # When only the public book has not caught up, the independently
            # validated account snapshots already prove the exact resting
            # source and unchanged receiver. Keep their original timestamps;
            # the normal final admission revalidates freshness and exact order.
            book_only = (
                self._visibility_source_order_status(checks[0], plan, source_order) == "EXACT"
                and self._visibility_book_status(checks[2], plan, source_order) == "ABSENT"
            )
            if book_only:
                book, duration = await self._read_public_book(config)
                checks = (checks[0], checks[1], book, duration, max(0.0, time.perf_counter() - refresh_started))
            else:
                checks = await self._parallel_pre_receiver_checks(config)
            self._last_visibility_timings["pre_visibility_refresh_book_only"] = float(book_only)
            self._last_pre_visibility_refresh = True
            self._last_pre_visibility_refresh_seconds = max(
                0.0,
                time.perf_counter() - refresh_started,
            )
        return source_order, checks, max(0.0, time.perf_counter() - started)

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
                operation_mode=config.operation_mode,
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
                operation_mode=config.operation_mode,
            )
        try:
            return await self._execute_locked(config, journal)
        finally:
            journal.release_attempt()

    async def _execute_locked(self, config: HandoffConfig, journal: DurableJournal) -> HandoffResult:
        self._source_dispatch_monotonic = None
        self._preflight_context = None
        self._configured_poll_limit = config.max_poll_count
        self._configured_poll_interval = config.poll_interval_seconds
        self._configured_order_timeout = config.order_timeout_seconds
        self._configured_reconcile_timeout = config.reconcile_timeout_seconds
        self._configured_freshness = config.freshness_seconds
        self._configured_request_timeout = config.request_timeout_seconds
        requires_incremental_margin = (
            config.operation_mode is not OperationMode.PAIRED_CLOSING
            and not config.defer_incremental_margin_calculation
        )
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
                operation_mode=config.operation_mode,
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
                operation_mode=config.operation_mode,
            )
        from .provenance import capture_provenance
        journal.append("ATTEMPT_STARTED", {"run_id": run_id, "binding": binding,
                                            "runtime_provenance": capture_provenance(binding)})
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
                operation_mode=config.operation_mode,
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
                operation_mode=config.operation_mode,
            )

        source_receipt: MutationReceipt | None = None
        source_order: OrderSnapshot | None = None
        receiver_receipt: MutationReceipt | None = None
        receiver_order: OrderSnapshot | None = None
        unknown_reasons: list[str] = []
        source_dispatch_attempted = False
        receiver_dispatch_attempted = False
        priority_guard: dict[str, Any] | None = None
        latency: dict[str, float] = {}
        paired_mode = plan.operation_mode in {OperationMode.PAIRED_OPENING, OperationMode.PAIRED_CLOSING}
        public_book: dict[str, Any] | None = None
        source_order_id: str | None = None
        guard_event_recorded = False
        guard_request_started_at = self.clock.now() if paired_mode else None
        source_recheck: AccountSnapshot | None = None
        receiver_recheck: AccountSnapshot | None = None
        source_recheck_transition = "UNAVAILABLE"
        coalesced_pre_receiver_checks: tuple[
            AccountSnapshot,
            AccountSnapshot,
            dict[str, Any],
            float,
            float,
        ] | None = None
        coalesced_pre_receiver_window_seconds = 0.0
        self._last_pre_visibility_original_checks = None
        receiver_mutation_observations: tuple[Any, ...] = (source, receiver, plan.metadata_observed_at)
        receiver_mutation_observation_now: float | None = None
        prepared_source: Any | None = None
        prepared_receiver: Any | None = None
        prepared_source_plan: OrderPlan | None = None
        prepared_receiver_plan: OrderPlan | None = None
        source_dispatch_intent_at: float | None = None
        source_dispatch_ack_at: float | None = None
        receiver_dispatch_intent_at: float | None = None
        self._visibility_source_baseline = source
        self._visibility_receiver_baseline = receiver
        self._visibility_requires_incremental_margin = requires_incremental_margin
        prepared_dispatch_enabled = self._supports_prepared_dispatch()
        if prepared_dispatch_enabled:
            preparation_started = time.perf_counter()
            try:
                # Nonces and signatures are acquired before source exposure.
                # The SDK owns the in-memory single-use state; only the plain
                # order plans are journaled below.
                prepared_source_plan = self._mutation_plan(
                    plan.source,
                    config,
                    observations=(
                        source,
                        receiver,
                        plan.metadata_observed_at,
                        config.source_quote_observed_at,
                    ),
                )
                prepared_receiver_plan = self._mutation_plan(
                    plan.receiver,
                    config,
                    observations=(
                        source,
                        receiver,
                        plan.metadata_observed_at,
                        config.source_quote_observed_at,
                    ),
                )
                prepared_source, prepared_receiver = await self._prepare_pair(
                    prepared_source_plan,
                    prepared_receiver_plan,
                )
            except Exception as exc:
                if prepared_source is not None:
                    await self._invalidate_prepared(prepared_source)
                if prepared_receiver is not None:
                    await self._invalidate_prepared(prepared_receiver)
                unknown_reasons.append(f"order preparation failed before source exposure: {sanitize_exception(exc)}")
            finally:
                latency["paired_preparation_seconds"] = max(
                    0.0,
                    time.perf_counter() - preparation_started,
                )
        latency.update(_prepared_timing_values(prepared_source, "source"))
        latency.update(_prepared_timing_values(prepared_receiver, "receiver"))
        if config.max_quote_age_seconds is not None and self.clock.now() - config.source_quote_observed_at > config.max_quote_age_seconds:
            unknown_reasons.append("source quote latency budget expired before dispatch")
        try:
            if not unknown_reasons:
                source_dispatch_attempted = True
                source_submit_started = time.perf_counter()
                source_dispatch_intent_at = self.clock.now()
                self._source_dispatch_monotonic = time.monotonic()
                latency["source_dispatch_intent_at"] = source_dispatch_intent_at
                if config.source_quote_observed_at is not None:
                    quote_age = max(
                        0.0,
                        source_dispatch_intent_at - config.source_quote_observed_at,
                    )
                    latency["quote_age_to_source_dispatch_seconds"] = quote_age
                    latency["source_quote_age_seconds"] = quote_age
                source_dispatch_plan = prepared_source_plan or self._mutation_plan(
                    plan.source,
                    config,
                    observations=(
                        source,
                        receiver,
                        plan.metadata_observed_at,
                        config.source_quote_observed_at,
                    ),
                )
                journal.append(
                    "SOURCE_DISPATCH_INTENT",
                    {"plan": source_dispatch_plan.as_dict()},
                    run_id=run_id,
                )
                source_receipt = _as_receipt(
                    await self._submit_order(
                        source_dispatch_plan,
                        prepared=prepared_source,
                    )
                )
                latency["source_submit_ack_seconds"] = max(0.0, time.perf_counter() - source_submit_started)
                source_dispatch_ack_at = self.clock.now()
                latency["source_dispatch_ack_at"] = source_dispatch_ack_at
                journal.append(
                    "SOURCE_DISPATCH_RESULT",
                    {
                        "operation_mode": plan.operation_mode.value,
                        "accepted": source_receipt.accepted,
                        "order_id": source_receipt.order_id,
                        "tx_hash": source_receipt.tx_hash,
                        "response_code": source_receipt.response_code,
                        "error": source_receipt.error,
                    },
                    run_id=run_id,
                )
        except Exception as exc:  # an exception after intent is dispatch-unknown
            if "source_submit_started" in locals():
                latency["source_submit_ack_seconds"] = max(0.0, time.perf_counter() - source_submit_started)
            unknown_reasons.append(f"source dispatch outcome unknown: {sanitize_exception(exc)}")
            journal.append(
                "SOURCE_DISPATCH_UNKNOWN",
                {"operation_mode": plan.operation_mode.value, "reason": unknown_reasons[-1]},
                run_id=run_id,
            )
            await self._invalidate_prepared(prepared_receiver)

        latency.update(_prepared_timing_values(prepared_source, "source"))
        if source_receipt is not None:
            if not source_receipt.accepted:
                unknown_reasons.append("source dispatch was rejected")
            source_visibility_started = time.perf_counter()
            try:
                if paired_mode and source_receipt.accepted:
                    (
                        source_order,
                        coalesced_pre_receiver_checks,
                        coalesced_pre_receiver_window_seconds,
                    ) = await self._parallel_source_visibility_and_checks(
                        config,
                        plan.source,
                        source_receipt,
                        journal,
                        run_id,
                    )
                    latency["pre_visibility_refresh_seconds"] = self._last_pre_visibility_refresh_seconds
                    latency.update(getattr(self, "_last_visibility_timings", {}))
                else:
                    source_order = await self._poll_order(
                        plan.source,
                        source_receipt.order_id,
                        journal,
                        run_id,
                        require_terminal=False,
                    )
            except Exception as exc:
                unknown_reasons.append(
                    f"pre-receiver state is unresolved: {sanitize_exception(exc)}"
                )
            latency["source_visibility_seconds"] = max(0.0, time.perf_counter() - source_visibility_started)
            if coalesced_pre_receiver_checks is not None:
                # These observations were already fetched while source
                # visibility was settling.  Keep them available even when a
                # terminal source state skips normal receiver admission.
                source_recheck, receiver_recheck, public_book = coalesced_pre_receiver_checks[:3]
                latency["public_book_read_seconds"] = coalesced_pre_receiver_checks[3]
                latency["concurrent_pre_receiver_checks_seconds"] = coalesced_pre_receiver_checks[4]
                latency["coalesced_pre_receiver_window_seconds"] = coalesced_pre_receiver_window_seconds
            if source_order is not None:
                source_order_id = source_order.order_id
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
                await self._cancel_if_safe(
                    plan.source,
                    source_order,
                    journal,
                    run_id,
                    unknown_reasons,
                    expected_order_id=source_order_id,
                )
            elif not source_order.active or source_order.remaining_quantity != plan.quantity:
                if self._is_exact_canceled_post_only_zero_fill(source_order, plan.source):
                    reason = "source canceled-post-only zero-fill; receiver was not dispatched"
                    if reason not in unknown_reasons:
                        unknown_reasons.append(reason)
                    journal.append(
                        "SOURCE_CANCELED_POST_ONLY_ZERO_FILL",
                        {
                            "order_id": source_order.order_id,
                            "status": source_order.status,
                            "filled_quantity": str(source_order.filled_quantity),
                            "remaining_quantity": str(source_order.remaining_quantity),
                            "terminal_reason": "canceled-post-only",
                        },
                        run_id=run_id,
                    )
                else:
                    unknown_reasons.append("source order did not prove exact resting quantity")
                await self._cancel_if_safe(
                    plan.source,
                    source_order,
                    journal,
                    run_id,
                    unknown_reasons,
                    expected_order_id=source_order_id,
                )

        # A missing/rejected/partially-filled source can never authorize B.
        # Account and public-book reads are independent, so they share one
        # bounded pre-receiver window.  The exact source lookup remains after
        # both reads and immediately before the guard decision.
        if (
            not unknown_reasons
            and source_order is not None
            and self._source_is_resting(source_order, plan.source)
        ):
            pre_receiver_started = time.perf_counter()
            if paired_mode:
                guard_request_started_at = self.clock.now()
            try:
                if paired_mode:
                    if coalesced_pre_receiver_checks is None:
                        (
                            source_recheck,
                            receiver_recheck,
                            public_book,
                            book_duration,
                            parallel_duration,
                        ) = await self._parallel_pre_receiver_checks(config)
                    else:
                        (
                            source_recheck,
                            receiver_recheck,
                            public_book,
                            book_duration,
                            parallel_duration,
                        ) = coalesced_pre_receiver_checks
                else:
                    source_recheck, receiver_recheck = await self._parallel_account_rechecks(
                        _account_from_client(self.client, "source"),
                        _account_from_client(self.client, "receiver"),
                        config.market_id,
                    )
                    book_duration = 0.0
                    parallel_duration = 0.0
                latency["public_book_read_seconds"] = book_duration
                latency["concurrent_pre_receiver_checks_seconds"] = parallel_duration
                source_order_id = source_order.order_id
                final_lookup_started = time.perf_counter()
                source_order = await self._lookup_order(plan.source, source_order.order_id)
                latency["final_source_lookup_seconds"] = max(0.0, time.perf_counter() - final_lookup_started)
                decision_now = self.clock.now()
                if source_dispatch_intent_at is not None:
                    latency["source_to_admission_seconds"] = max(0.0, decision_now - source_dispatch_intent_at)
                if config.max_source_to_receiver_seconds is not None and self._source_dispatch_monotonic is not None and (
                    time.monotonic() - self._source_dispatch_monotonic > config.max_source_to_receiver_seconds
                    or decision_now - source_dispatch_intent_at > config.max_source_to_receiver_seconds
                ):
                    unknown_reasons.append("source-to-receiver latency budget expired before dispatch")
                if paired_mode and public_book is not None and not self._time_fresh(
                    public_book["observed_at"], decision_now, config.freshness_seconds
                ):
                    # The book was validated at receipt, but the exact source
                    # lookup is a causal boundary of receiver admission.  A
                    # fresh-on-receipt book that expires while that lookup is
                    # in flight cannot authorize a receiver mutation.
                    unknown_reasons.append(
                        "public book recheck is stale or from the future before receiver dispatch"
                    )
                if source_order is None:
                    unknown_reasons.append("source order disappeared during pre-receiver recheck")
                if not self._snapshot_matches(
                    source_recheck, plan.source, expected_identity=plan.source_identity
                ):
                    unknown_reasons.append("source recheck account/market identity conflicts with plan")
                if not self._snapshot_matches(
                    receiver_recheck, plan.receiver, expected_identity=plan.receiver_identity
                ):
                    unknown_reasons.append("receiver recheck account/market identity conflicts with plan")
                if not self._snapshot_fresh(source_recheck, decision_now, config.freshness_seconds):
                    unknown_reasons.append("source recheck is stale or from the future before receiver dispatch")
                if not self._snapshot_fresh(receiver_recheck, decision_now, config.freshness_seconds):
                    unknown_reasons.append("receiver recheck is stale or from the future before receiver dispatch")
                if source_order is not None and not self._time_fresh(
                    source_order.observed_at, decision_now, config.freshness_seconds
                ):
                    unknown_reasons.append("source order recheck is stale or from the future before receiver dispatch")
                source_fill_visible_in_recheck = (
                    source_order is not None
                    and source_order.terminal
                    and source_order.filled_quantity > 0
                    and source_recheck.signed_position
                    == source.signed_position - source_order.filled_quantity * plan.direction.sign
                )
                if (
                    source_recheck.signed_position != source.signed_position
                    and not source_fill_visible_in_recheck
                ):
                    unknown_reasons.append("source position changed before receiver dispatch")
                if not source_recheck.authorized or not source_recheck.ready:
                    unknown_reasons.append("source recheck authorization/readiness is unproven")
                if (
                    source_recheck.margin_available is None
                    or source_recheck.margin_required is None
                    or source_recheck.margin_required > source_recheck.margin_available
                    or (
                        requires_incremental_margin
                        and (
                            source_recheck.incremental_margin_required is None
                            or not source_recheck.incremental_margin_evidence
                        )
                    )
                    or (
                        source_recheck.incremental_margin_required is not None
                        and source_recheck.incremental_margin_evidence
                        and source_recheck.incremental_margin_required > source_recheck.margin_available
                    )
                ):
                    unknown_reasons.append("source margin recheck is insufficient or missing")
                source_active_order_matches = self._active_orders_match(
                    source_recheck, plan.source, source_order_id
                )
                source_active_order_was_consumed_by_fill = (
                    source_fill_visible_in_recheck and not source_recheck.active_orders
                )
                source_exact_terminal_fill = (
                    source_order is not None
                    and self._order_matches(source_order, plan.source)
                    and source_order.terminal
                    and source_order.filled_quantity > 0
                )
                source_recheck_transition = (
                    "EXACT_SOURCE_CONSUMED_BY_FILL"
                    if source_active_order_was_consumed_by_fill
                    else (
                        "TEMPORALLY_SKEWED_EXACT_SOURCE_FILL"
                        if (
                            source_exact_terminal_fill
                            and not source_recheck.active_orders
                            and source_recheck.signed_position == source.signed_position
                            and source_order is not None
                            and source_order.observed_at >= source_recheck.observed_at
                        )
                        else (
                            "EXACT_SOURCE_ACTIVE"
                            if source_active_order_matches
                            else (
                                "CONFLICTING_ACTIVE_ORDER"
                                if source_recheck.active_orders
                                else "NO_ACTIVE_ORDER"
                            )
                        )
                    )
                )
                if (
                    not source_active_order_matches
                    and not source_active_order_was_consumed_by_fill
                    and source_recheck_transition != "TEMPORALLY_SKEWED_EXACT_SOURCE_FILL"
                ):
                    unknown_reasons.append("source recheck contains an additional or conflicting active order")
                if receiver_recheck.signed_position != receiver.signed_position:
                    unknown_reasons.append("receiver position changed before receiver dispatch")
                if not receiver_recheck.authorized or not receiver_recheck.ready:
                    unknown_reasons.append("receiver recheck authorization/readiness is unproven")
                if receiver_recheck.active_orders:
                    unknown_reasons.append("receiver active HOOD order appeared before receiver dispatch")
                if (
                    receiver_recheck.margin_available is None
                    or receiver_recheck.margin_required is None
                    or receiver_recheck.margin_required > receiver_recheck.margin_available
                    or (
                        requires_incremental_margin
                        and (
                            receiver_recheck.incremental_margin_required is None
                            or not receiver_recheck.incremental_margin_evidence
                        )
                    )
                    or (
                        receiver_recheck.incremental_margin_required is not None
                        and receiver_recheck.incremental_margin_evidence
                        and receiver_recheck.incremental_margin_required > receiver_recheck.margin_available
                    )
                ):
                    unknown_reasons.append("receiver margin recheck is insufficient or missing")
                if source_order is None:
                    unknown_reasons.append("source exact order is missing before receiver dispatch")
                elif not self._source_exact_resting(source_order, plan.source, source_order_id):
                    if source_order.filled_quantity > 0:
                        unknown_reasons.append("source fill or quantity change before receiver dispatch")
                    elif not source_order.active or source_order.remaining_quantity != plan.quantity:
                        unknown_reasons.append("source order did not prove exact resting quantity")
                    else:
                        unknown_reasons.append("source exact order identity/parameters/remaining mismatch before receiver dispatch")
                if (
                    paired_mode
                    and not unknown_reasons
                    and source_order is not None
                    and public_book is not None
                ):
                    priority_guard = self._priority_guard(public_book, plan, source_order_id)
                    if priority_guard["status"] != "PROVED":
                        unknown_reasons.append(
                            f"PAIR_GUARD_{priority_guard['status']}: {priority_guard['priority_reason']}"
                        )
                if not unknown_reasons and source_order is not None:
                    receiver_mutation_observations = (
                        source_recheck,
                        receiver_recheck,
                        source_order,
                        public_book,
                        plan.metadata_observed_at,
                    )
                    journal.append(
                        "SOURCE_RECHECK_PASS",
                        {"order_id": source_order.order_id, "remaining_quantity": str(source_order.remaining_quantity)},
                        run_id=run_id,
                    )
            except Exception as exc:
                unknown_reasons.append(f"pre-receiver state is unresolved: {sanitize_exception(exc)}")
            finally:
                latency["pre_receiver_checks_seconds"] = max(
                    0.0, time.perf_counter() - pre_receiver_started
                )
                if paired_mode and not guard_event_recorded:
                    guard_finished_at = self.clock.now()
                    if guard_request_started_at is not None:
                        latency["receiver_admission_seconds"] = max(
                            0.0,
                            time.perf_counter() - pre_receiver_started,
                        )
                        latency["receiver_admission_at"] = guard_finished_at
                    if priority_guard is None:
                        if public_book is not None and source_order_id is not None:
                            try:
                                priority_guard = self._priority_guard(
                                    public_book,
                                    plan,
                                    source_order_id,
                                )
                            except Exception as exc:
                                priority_guard = self._priority_guard_unavailable(
                                    plan,
                                    source_order_id,
                                    f"public priority evidence is malformed: {sanitize_exception(exc)}",
                                    public_book,
                                )
                        else:
                            priority_guard = self._priority_guard_unavailable(
                                plan,
                                source_order_id,
                                "fresh public-book priority evidence is unavailable",
                                public_book,
                            )
                    if unknown_reasons and priority_guard["status"] == "PROVED":
                        priority_guard["status"] = "UNKNOWN"
                        priority_guard["priority_status"] = "UNKNOWN"
                        priority_guard["priority_reason"] = (
                            "paired admission checks failed: " + "; ".join(unknown_reasons[:8])
                        )
                    if self._is_exact_canceled_post_only_zero_fill(source_order, plan.source):
                        # The book/account window is retained as evidence, but
                        # no priority proof was admitted because the source
                        # was already terminal before the receiver boundary.
                        priority_guard["status"] = "UNKNOWN"
                        priority_guard["priority_status"] = "UNKNOWN"
                        priority_guard["priority_proof_admitted"] = False
                        priority_guard["priority_reason"] = (
                            "source canceled-post-only before receiver admission; "
                            "priority proof was not admitted"
                        )
                        priority_guard["terminal_reason"] = "canceled-post-only"
                    if source_order is not None:
                        priority_guard["source_order"] = {
                            "order_id": source_order.order_id,
                            "owner_account_index": source_order.account_index,
                            "market_id": source_order.market_id,
                            "client_order_index": source_order.client_order_index,
                            "side": source_order.side,
                            "order_type": source_order.order_type,
                            "time_in_force": source_order.time_in_force,
                            "price": None if source_order.price is None else str(source_order.price),
                            "initial_quantity": str(source_order.initial_quantity),
                            "filled_quantity": str(source_order.filled_quantity),
                            "remaining_quantity": str(source_order.remaining_quantity),
                            "status": source_order.status,
                            "observed_at": source_order.observed_at,
                        }
                    priority_guard["source_recheck"] = self._account_observation_payload(source_recheck)
                    priority_guard["receiver_recheck"] = self._account_observation_payload(receiver_recheck)
                    priority_guard["source_recheck_transition"] = source_recheck_transition
                    if self._last_pre_visibility_original_checks is not None:
                        first_source, first_receiver, first_book, _, _ = (
                            self._last_pre_visibility_original_checks
                        )
                        priority_guard["causal_pre_visibility_observations"] = {
                            "source": self._account_observation_payload(first_source),
                            "receiver": self._account_observation_payload(first_receiver),
                            "book_observed_at": first_book.get("observed_at"),
                        }
                    priority_guard["admission_reasons"] = list(unknown_reasons[:8])
                    journal.append(
                        "PRE_RECEIVER_GUARD",
                        {
                            **priority_guard,
                            "request_started_at": guard_request_started_at,
                            "request_finished_at": self.clock.now(),
                            "local_observed_at": self.clock.now(),
                            "latency_seconds": latency.get("concurrent_pre_receiver_checks_seconds", 0.0),
                        },
                        run_id=run_id,
                    )
                    guard_event_recorded = True

        if paired_mode and not guard_event_recorded:
            priority_guard = self._priority_guard_unavailable(
                plan,
                source_order_id,
                "paired pre-receiver admission did not obtain a complete guard window",
                public_book,
            )
            if self._is_exact_canceled_post_only_zero_fill(source_order, plan.source):
                # Keep the coalesced book/account evidence while explicitly
                # recording that no priority proof was admitted after the
                # source had already reached its terminal post-only outcome.
                priority_guard["priority_reason"] = (
                    "source canceled-post-only before receiver admission; "
                    "priority proof was not admitted"
                )
                priority_guard["terminal_reason"] = "canceled-post-only"
                priority_guard["priority_proof_admitted"] = False
            if source_order is not None:
                priority_guard["source_order"] = {
                    "order_id": source_order.order_id,
                    "owner_account_index": source_order.account_index,
                    "market_id": source_order.market_id,
                    "client_order_index": source_order.client_order_index,
                    "side": source_order.side,
                    "order_type": source_order.order_type,
                    "time_in_force": source_order.time_in_force,
                    "price": None if source_order.price is None else str(source_order.price),
                    "initial_quantity": str(source_order.initial_quantity),
                    "filled_quantity": str(source_order.filled_quantity),
                    "remaining_quantity": str(source_order.remaining_quantity),
                    "status": source_order.status,
                    "observed_at": source_order.observed_at,
                }
            priority_guard["source_recheck"] = self._account_observation_payload(source_recheck)
            priority_guard["receiver_recheck"] = self._account_observation_payload(receiver_recheck)
            priority_guard["source_recheck_transition"] = source_recheck_transition
            if self._last_pre_visibility_original_checks is not None:
                first_source, first_receiver, first_book, _, _ = (
                    self._last_pre_visibility_original_checks
                )
                priority_guard["causal_pre_visibility_observations"] = {
                    "source": self._account_observation_payload(first_source),
                    "receiver": self._account_observation_payload(first_receiver),
                    "book_observed_at": first_book.get("observed_at"),
                }
            priority_guard["admission_reasons"] = list(unknown_reasons[:8])
            journal.append(
                "PRE_RECEIVER_GUARD",
                {
                    **priority_guard,
                    "request_started_at": guard_request_started_at,
                    "request_finished_at": self.clock.now(),
                    "local_observed_at": self.clock.now(),
                    "latency_seconds": latency.get("concurrent_pre_receiver_checks_seconds", 0.0),
                },
                run_id=run_id,
            )
            guard_event_recorded = True

        if unknown_reasons:
            await self._invalidate_prepared(prepared_receiver)
            # A dispatch can be ambiguous before the first order observation.
            # Resolve the exact client identity once more so an identified
            # remaining maker can be cancelled safely; a missing identity is
            # never guessed and remains UNKNOWN.
            if source_order is None and source_dispatch_attempted:
                try:
                    source_order = await self._lookup_order(
                        plan.source,
                        source_order_id if source_order_id is not None else (
                            source_receipt.order_id if source_receipt is not None else None
                        ),
                        client_order_index=plan.source.client_order_index,
                    )
                except Exception as exc:
                    unknown_reasons.append(f"source cancellation lookup unresolved: {sanitize_exception(exc)}")
            if source_order is not None:
                await self._cancel_if_safe(
                    plan.source,
                    source_order,
                    journal,
                    run_id,
                    unknown_reasons,
                    expected_order_id=(
                        source_order_id
                        if source_order_id is not None
                        else (source_receipt.order_id if source_receipt is not None else None)
                    ),
                )
            reconciliation_started = time.perf_counter()
            source_result, receiver_result = await self._reconcile(
                config,
                plan,
                source,
                receiver,
                source_order_id=source_order_id if source_order_id is not None else (
                    source_order.order_id if source_order else (source_receipt.order_id if source_receipt else None)
                ),
                receiver_order_id=None,
                source_dispatched=source_dispatch_attempted,
                receiver_dispatched=receiver_dispatch_attempted,
                run_id=run_id,
                journal=journal,
                unknown_reasons=unknown_reasons,
            )
            latency["reconciliation_seconds"] = max(
                0.0, time.perf_counter() - reconciliation_started
            )
            retryable_pair = self._retryable_pair_after_guard(
                plan,
                source_result,
                receiver_result,
                unknown_reasons,
                priority_guard,
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
                forced_outcome=(
                    Outcome.PARTIAL
                    if retryable_pair
                    else None
                ),
                retryable_pair=retryable_pair,
                latency=latency,
                priority_guard=priority_guard,
            )

        try:
            receiver_dispatch_attempted = True
            receiver_submit_started = time.perf_counter()
            receiver_dispatch_intent_at = self.clock.now()
            latency["receiver_dispatch_intent_at"] = receiver_dispatch_intent_at
            if source_dispatch_intent_at is not None:
                latency["source_to_receiver_intent_seconds"] = max(
                    0.0,
                    receiver_dispatch_intent_at - source_dispatch_intent_at,
                )
            if source_dispatch_ack_at is not None:
                latency["source_ack_to_receiver_intent_seconds"] = max(
                    0.0,
                    receiver_dispatch_intent_at - source_dispatch_ack_at,
                )
            receiver_admission_plan = self._mutation_plan(
                plan.receiver,
                config,
                observations=receiver_mutation_observations,
                observation_now=receiver_mutation_observation_now,
            )
            receiver_dispatch_plan = prepared_receiver_plan or receiver_admission_plan
            receiver_final_deadline = receiver_admission_plan.mutation_deadline_monotonic
            if prepared_receiver_plan is not None:
                prepared_deadline = prepared_receiver_plan.mutation_deadline_monotonic
                if (
                    receiver_final_deadline is not None
                    and prepared_deadline is not None
                ):
                    # Preparation cannot renew the evidence it was bound to.
                    # The send barrier is the stricter of the prepared and
                    # final-admission deadlines, so a later final read never
                    # widens a pre-signed mutation's lifetime.
                    receiver_final_deadline = min(receiver_final_deadline, prepared_deadline)
            journal.append("RECEIVER_DISPATCH_INTENT", {"plan": receiver_dispatch_plan.as_dict()}, run_id=run_id)
            receiver_receipt = _as_receipt(
                await self._submit_order(
                    receiver_dispatch_plan,
                    prepared=prepared_receiver,
                    final_deadline=receiver_final_deadline,
                )
            )
            latency["receiver_submit_ack_seconds"] = max(0.0, time.perf_counter() - receiver_submit_started)
            # Persist the wall-clock response boundary separately from the
            # monotonic duration so an offline report can distinguish an ack
            # from later order visibility.
            latency["receiver_dispatch_ack_at"] = self.clock.now()
            journal.append(
                "RECEIVER_DISPATCH_RESULT",
                {
                    "operation_mode": plan.operation_mode.value,
                    "accepted": receiver_receipt.accepted,
                    "order_id": receiver_receipt.order_id,
                    "tx_hash": receiver_receipt.tx_hash,
                    "response_code": receiver_receipt.response_code,
                    "error": receiver_receipt.error,
                },
                run_id=run_id,
            )
        except Exception as exc:
            if "receiver_submit_started" in locals():
                latency["receiver_submit_ack_seconds"] = max(0.0, time.perf_counter() - receiver_submit_started)
            latency["receiver_dispatch_outcome_at"] = self.clock.now()
            unknown_reasons.append(f"receiver dispatch outcome unknown: {sanitize_exception(exc)}")
            journal.append(
                "RECEIVER_DISPATCH_UNKNOWN",
                {"operation_mode": plan.operation_mode.value, "reason": unknown_reasons[-1]},
                run_id=run_id,
            )

        latency.update(_prepared_timing_values(prepared_receiver, "receiver"))
        if receiver_receipt is not None:
            receiver_visibility_started = time.perf_counter()
            receiver_order = await self._poll_order(
                plan.receiver,
                receiver_receipt.order_id,
                journal,
                run_id,
                require_terminal=True,
            )
            latency["receiver_fill_observation_seconds"] = max(
                0.0, time.perf_counter() - receiver_visibility_started
            )
            # Terminal visibility and a positive fill are separate observations.
            if receiver_order is not None:
                latency["receiver_terminal_observed_at"] = receiver_order.observed_at
                if receiver_order.filled_quantity > 0:
                    latency["receiver_fill_observed_at"] = receiver_order.observed_at
            latency["receiver_visibility_seconds"] = latency["receiver_fill_observation_seconds"]
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
                await self._cancel_if_safe(
                    plan.source,
                    current_source,
                    journal,
                    run_id,
                    unknown_reasons,
                    expected_order_id=source_order.order_id,
                )
            elif not any("source cancellation lookup unresolved" in item for item in unknown_reasons):
                unknown_reasons.append("source order disappeared before cancellation reconciliation")

        reconciliation_started = time.perf_counter()
        source_result, receiver_result = await self._reconcile(
            config,
            plan,
            source,
            receiver,
            source_order_id=source_order_id if source_order_id is not None else (
                source_order.order_id if source_order else (source_receipt.order_id if source_receipt else None)
            ),
            receiver_order_id=receiver_order.order_id if receiver_order else (receiver_receipt.order_id if receiver_receipt else None),
            source_dispatched=source_dispatch_attempted,
            receiver_dispatched=receiver_dispatch_attempted,
            run_id=run_id,
            journal=journal,
            unknown_reasons=unknown_reasons,
        )
        latency["reconciliation_seconds"] = max(0.0, time.perf_counter() - reconciliation_started)
        return await self._finish(
            journal,
            run_id,
            plan,
            source_result,
            receiver_result,
            unknown_reasons,
            config=config,
            binding=binding,
            latency=latency,
            priority_guard=priority_guard,
        )

    async def _parallel_account_rechecks(
        self,
        source_account_index: int,
        receiver_account_index: int,
        market_id: int,
    ) -> tuple[AccountSnapshot, AccountSnapshot]:
        """Read the two independent pre-receiver account snapshots together.

        The exact source order is checked by the caller only after both reads
        have completed.  If either bounded read fails or this task is
        cancelled, cancel and drain the sibling before propagating the same
        error so the caller cannot enter its mutation or cleanup path with a
        live account read.
        """

        tasks: list[asyncio.Task[Any]] = []

        async def read(account_index: int, label: str) -> AccountSnapshot:
            value = await self._bounded(
                self.client.account_snapshot(account_index, market_id),
                label,
            )
            return _as_account(value)

        try:
            tasks.append(asyncio.create_task(read(source_account_index, "source recheck account read")))
            tasks.append(asyncio.create_task(read(receiver_account_index, "receiver recheck account read")))
            source_value, receiver_value = await asyncio.gather(*tasks)
        except BaseException as exc:
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            if not isinstance(exc, asyncio.CancelledError):
                # Gather wakes on the first failure.  Report the first task in
                # source/receiver order after both tasks are drained so two
                # simultaneous read failures cannot change the diagnostic.
                for task in tasks:
                    if task.cancelled():
                        continue
                    task_error = task.exception()
                    if task_error is not None:
                        raise task_error
            raise
        return source_value, receiver_value

    def _supports_prepared_dispatch(self) -> bool:
        """Use the fast path only when the client exposes both halves."""

        if not (
            callable(getattr(self.client, "prepare_order", None))
            and callable(getattr(self.client, "submit_prepared_order", None))
        ):
            return False
        # A test or adapter may intentionally replace the legacy generic
        # submit_order surface on one instance.  Keep that explicit override
        # authoritative instead of silently bypassing it with inherited SDK
        # preparation methods.
        if "submit_order" in getattr(self.client, "__dict__", {}):
            return False
        client_type = type(self.client)

        def owner(name: str) -> type | None:
            return next((candidate for candidate in client_type.__mro__ if name in candidate.__dict__), None)

        submit_owner = owner("submit_order")
        prepared_owner = owner("prepare_order")
        if (
            submit_owner is not None
            and prepared_owner is not None
            and submit_owner is not prepared_owner
            and issubclass(submit_owner, prepared_owner)
        ):
            return False
        return True

    async def _prepare_order(self, plan: OrderPlan) -> Any:
        method = getattr(self.client, "prepare_order", None)
        if not callable(method):
            raise ContractError("prepared dispatch client lacks order preparation")
        token = None if self._preflight_context is None else self._preflight_context.reserved_nonces.get(plan.account_index)
        operation = method(plan) if token is None else method(plan, reserved_nonce=token)
        return await self._bounded(operation, "order preparation")

    async def _prepare_pair(
        self,
        source_plan: OrderPlan,
        receiver_plan: OrderPlan,
    ) -> tuple[Any, Any]:
        """Prepare independent account/key legs concurrently and drain failures.

        The SDK keeps nonce reservation and signing state behind its own
        account/key lock.  Scheduling both preparations together therefore
        preserves that reservation contract while allowing adapters with
        independent accounts or keys to overlap their network/signing work.
        A failed pair invalidates every successful unsent preparation before
        the caller can expose the source order.
        """

        tasks = [
            asyncio.create_task(self._prepare_order(source_plan)),
            asyncio.create_task(self._prepare_order(receiver_plan)),
        ]
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        except BaseException:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            results = []
            for task in tasks:
                if task.cancelled():
                    continue
                try:
                    results.append(task.result())
                except BaseException:
                    continue
            for prepared in results:
                await self._invalidate_prepared(prepared)
            raise

        prepared_values: list[Any] = []
        failure: BaseException | None = None
        for result in results:
            if isinstance(result, BaseException):
                if failure is None:
                    failure = result
            else:
                prepared_values.append(result)
        if failure is not None:
            for prepared in prepared_values:
                await self._invalidate_prepared(prepared)
            raise failure
        if len(prepared_values) != 2:
            raise ContractError("prepared pair returned an unsupported result")
        return prepared_values[0], prepared_values[1]

    async def _submit_order(
        self,
        plan: OrderPlan,
        *,
        prepared: Any | None = None,
        final_deadline: float | None = None,
    ) -> MutationReceipt | Mapping[str, Any]:
        if prepared is None:
            return await self._bounded(self.client.submit_order(plan), "order mutation")
        method = getattr(self.client, "submit_prepared_order", None)
        if not callable(method):
            raise ContractError("prepared dispatch client lacks prepared submission")
        effective_deadline = plan.mutation_deadline_monotonic
        if final_deadline is not None:
            effective_deadline = (
                final_deadline
                if effective_deadline is None
                else min(effective_deadline, final_deadline)
            )
        if effective_deadline is not None and effective_deadline - time.monotonic() <= 0:
            raise TimeoutError("prepared order mutation crossed the final mutation barrier")
        try:
            parameters = inspect.signature(method).parameters.values()
            accepts_deadline = any(
                parameter.name == "deadline"
                or parameter.kind is inspect.Parameter.VAR_KEYWORD
                for parameter in parameters
            )
        except (TypeError, ValueError):
            # A callable without inspectable metadata may still be a generic
            # adapter.  Its two positional arguments are the compatibility
            # surface; the SDK implementation exposes the optional keyword.
            accepts_deadline = False
        invocation = (
            method(plan, prepared, deadline=effective_deadline)
            if accepts_deadline
            else method(plan, prepared)
        )
        if effective_deadline is None:
            return await self._bounded(invocation, "prepared order mutation")
        return await self._bounded(invocation, "prepared order mutation", deadline=effective_deadline)

    async def _invalidate_prepared(self, prepared: Any | None) -> None:
        if prepared is None:
            return
        method = getattr(self.client, "invalidate_prepared_order", None)
        if not callable(method):
            method = getattr(prepared, "invalidate", None)
            if callable(method):
                result = method()
                if inspect.isawaitable(result):
                    await result
            return
        result = method(prepared)
        if inspect.isawaitable(result):
            await result

    async def _preflight(
        self,
        config: HandoffConfig,
        journal: DurableJournal,
        run_id: str,
    ) -> tuple[HandoffPlan, AccountSnapshot, AccountSnapshot]:
        source_account_index = _account_from_client(self.client, "source")
        receiver_account_index = _account_from_client(self.client, "receiver")

        async def read_metadata() -> MarketMetadata:
            return _as_market(
                await self._bounded(
                    self.client.market_metadata(config.market_id),
                    "market metadata read",
                )
            )

        async def read_account(account_index: int, label: str) -> AccountSnapshot:
            return _as_account(
                await self._bounded(
                    self.client.account_snapshot(account_index, config.market_id),
                    label,
                )
            )

        # Metadata and the two account snapshots are independent read-only
        # observations.  Fetch them in one bounded window, then run the same
        # complete validation below.  If one read fails, drain every sibling
        # before returning so no account task remains live across the mutation
        # boundary.
        context = getattr(self.client, "handoff_preflight_context", None)
        if context is not None:
            if not isinstance(context, HandoffPreflightContext):
                raise PreflightBlocked("prepared preflight context has an unsupported type")
            metadata, source, receiver = context.claim(config)
            self._preflight_context = context
            if source.account_index != source_account_index or receiver.account_index != receiver_account_index:
                raise PreflightBlocked("prepared preflight accounts do not match the execution client")
        else:
            tasks = [
                asyncio.create_task(read_metadata()),
                asyncio.create_task(read_account(source_account_index, "source account read")),
                asyncio.create_task(read_account(receiver_account_index, "receiver account read")),
            ]
            try:
                metadata, source, receiver = await asyncio.gather(*tasks)
            except BaseException:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                raise
        now = self.clock.now()
        # The client must expose account identities explicitly; this prevents a
        # fallback to one shared account or a hidden account discovery call.
        if source.account_index == receiver.account_index:
            raise PreflightBlocked("source and receiver accounts must differ")
        config.validate_against(metadata, source, receiver, now)
        if config.operation_mode in {OperationMode.PAIRED_OPENING, OperationMode.PAIRED_CLOSING} and (
            (config.direction.source_side == "BUY" and config.receiver_worst_price > config.source_limit_price)
            or (config.direction.source_side == "SELL" and config.receiver_worst_price < config.source_limit_price)
        ):
            raise PreflightBlocked("receiver price bound cannot execute the source limit")
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
                reduce_only=config.operation_mode
                in {OperationMode.CLOSE_REOPEN, OperationMode.PAIRED_CLOSING},
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
                reduce_only=config.operation_mode is OperationMode.PAIRED_CLOSING,
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
            metadata_observed_at=metadata.observed_at,
            operation_mode=config.operation_mode,
            defer_incremental_margin_calculation=config.defer_incremental_margin_calculation,
        )
        journal.append(
            "PREFLIGHT_PROVED",
            {
                "market_id": metadata.market_id,
                "symbol": metadata.symbol,
                "source_account_index": source.account_index,
                "receiver_account_index": receiver.account_index,
                "operation_mode": config.operation_mode.value,
                "source_identity": source.source_identity,
                "receiver_identity": receiver.source_identity,
                "source_position": str(source.signed_position),
                "receiver_position": str(receiver.signed_position),
                "context_reused_before_quote": context is not None,
                "source_observed_at": source.observed_at,
                "receiver_observed_at": receiver.observed_at,
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
                        "observed_at": order.observed_at,
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

    @staticmethod
    def _order_observation_payload(order: OrderSnapshot) -> dict[str, Any]:
        """Keep bounded order identity/state evidence without SDK payloads."""

        return {
            "order_id": order.order_id,
            "client_order_index": order.client_order_index,
            "account_index": order.account_index,
            "market_id": order.market_id,
            "side": order.side,
            "order_type": order.order_type,
            "time_in_force": order.time_in_force,
            "reduce_only": order.reduce_only,
            "price": None if order.price is None else str(order.price),
            "status": order.status,
            "initial_quantity": str(order.initial_quantity),
            "remaining_quantity": str(order.remaining_quantity),
            "filled_quantity": str(order.filled_quantity),
            "observed_at": order.observed_at,
        }

    @classmethod
    def _account_observation_payload(
        cls,
        snapshot: AccountSnapshot | None,
    ) -> dict[str, Any] | None:
        """Preserve the account/active-order observations used by the guard."""

        if snapshot is None:
            return None
        return {
            "account_index": snapshot.account_index,
            "market_id": snapshot.market_id,
            "source_identity": snapshot.source_identity,
            "signed_position": str(snapshot.signed_position),
            "observed_at": snapshot.observed_at,
            "authorized": snapshot.authorized,
            "ready": snapshot.ready,
            "active_orders": [
                cls._order_observation_payload(order)
                for order in snapshot.active_orders
            ],
        }

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
    def _source_exact_resting(order: OrderSnapshot, plan: OrderPlan, order_id: str) -> bool:
        """Require every immutable source field and the exact remaining amount."""

        return (
            order.order_id == str(order_id)
            and order.account_index == plan.account_index
            and order.market_id == plan.market_id
            and order.client_order_index is not None
            and str(order.client_order_index) == str(plan.client_order_index)
            and order.side == plan.side
            and order.order_type == "LIMIT"
            and order.time_in_force == "POST_ONLY"
            and order.reduce_only == plan.reduce_only
            and order.price == plan.price
            and order.initial_quantity == plan.quantity
            and order.filled_quantity == 0
            and order.remaining_quantity == plan.quantity
            and order.status.lower() == "open"
        )

    @staticmethod
    def _is_exact_canceled_post_only_zero_fill(
        order: OrderSnapshot | None,
        plan: OrderPlan,
    ) -> bool:
        """Recognize only the terminal source form eligible for a retry."""

        return bool(
            order is not None
            and order.account_index == plan.account_index
            and order.market_id == plan.market_id
            and bool(order.order_id)
            and order.client_order_index is not None
            and str(order.client_order_index) == str(plan.client_order_index)
            and order.side == plan.side
            and order.order_type == "LIMIT"
            and order.time_in_force == "POST_ONLY"
            and order.reduce_only == plan.reduce_only
            and order.initial_quantity == plan.quantity
            and order.price == plan.price
            and order.status.lower() == "canceled-post-only"
            and order.terminal
            and order.filled_quantity == 0
            and order.remaining_quantity == 0
        )

    @staticmethod
    def _priority_guard_unavailable(
        plan: HandoffPlan,
        source_order_id: str | None,
        reason: str,
        public_book: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build a bounded UNKNOWN guard record when admission evidence is incomplete."""

        payload: dict[str, Any] = {
            "status": "UNKNOWN",
            "priority_status": "UNKNOWN",
            "priority_reason": reason,
            "book_observation_available": public_book is not None,
            "priority_proof_admitted": False,
            "source_side": plan.source.side,
            "source_price": format(plan.source.price, "f"),
            "source_order_id": None if source_order_id is None else str(source_order_id),
            "book_observed_at": None if public_book is None else public_book.get("observed_at"),
            "external_better_price_volume": "0",
            "better_price_evidence": [],
            "same_price_evidence": [],
            "source_public_level": None,
            "best_bid": None,
            "best_ask": None,
            "counterparty_not_guaranteed": True,
        }
        if public_book is not None:
            bids = public_book.get("bids", ())
            asks = public_book.get("asks", ())

            def level_payload(level: Mapping[str, Any]) -> dict[str, Any]:
                return {
                    "price": format(level["price"], "f"),
                    "quantity": format(level["quantity"], "f"),
                    "order_id": level["order_id"],
                    "owner_account_index": level["owner_account_index"],
                }

            if bids:
                payload["best_bid"] = level_payload(bids[0])
            if asks:
                payload["best_ask"] = level_payload(asks[0])
            for key in ("venue_timestamp", "version"):
                if key in public_book:
                    payload[key] = public_book[key]
        return payload

    @staticmethod
    def _priority_guard(
        public_book: Mapping[str, Any],
        plan: HandoffPlan,
        source_order_id: str,
    ) -> dict[str, Any]:
        """Classify only public price/queue evidence; never infer FIFO."""

        levels = public_book["asks"] if plan.source.side == "SELL" else public_book["bids"]
        source_price = plan.source.price
        same_price: list[dict[str, Any]] = []
        better_price: list[dict[str, Any]] = []
        better_quantity = Decimal(0)
        source_public_level: dict[str, Any] | None = None

        def level_payload(level: Mapping[str, Any]) -> dict[str, Any]:
            return {
                "price": format(level["price"], "f"),
                "quantity": format(level["quantity"], "f"),
                "order_id": level["order_id"],
                "owner_account_index": level["owner_account_index"],
            }

        for level in levels:
            price = level["price"]
            payload = level_payload(level)
            exact_source_level = (
                level["order_id"] is not None
                and level["order_id"] == str(source_order_id)
                and level["owner_account_index"] == plan.source.account_index
                and price == source_price
                and level["quantity"] == plan.quantity
            )
            if exact_source_level:
                source_public_level = payload
                continue
            is_better = price < source_price if plan.source.side == "SELL" else price > source_price
            if is_better:
                better_quantity += level["quantity"]
                if len(better_price) < 8:
                    better_price.append(payload)
            elif price == source_price:
                if len(same_price) < 8:
                    same_price.append(payload)

        reasons: list[str] = []
        if source_public_level is None:
            reasons.append(
                "exact source public level is absent or does not prove owner, order id, price and remaining quantity"
            )
        if better_quantity > 0:
            reasons.append("fresh public book shows better-priced volume")
        if same_price:
            reasons.append("fresh public book shows same-price volume; queue/FIFO priority is unproved")
        if better_quantity > 0:
            status = "LOST"
        elif reasons:
            status = "UNKNOWN"
        else:
            status = "PROVED"
        reason = "; ".join(reasons) if reasons else "exact source level is present and no better/same-price external volume is observed"
        source_levels = public_book["bids"]
        ask_levels = public_book["asks"]
        payload = {
            "status": status,
            "priority_status": status,
            "priority_reason": reason,
            "book_observation_available": True,
            "priority_proof_admitted": status == "PROVED",
            "source_side": plan.source.side,
            "source_price": format(source_price, "f"),
            "source_order_id": str(source_order_id),
            "book_observed_at": public_book["observed_at"],
            "external_better_price_volume": format(better_quantity, "f"),
            "better_price_evidence": better_price,
            "same_price_evidence": same_price,
            "source_public_level": source_public_level,
            "best_bid": None if not source_levels else level_payload(source_levels[0]),
            "best_ask": None if not ask_levels else level_payload(ask_levels[0]),
            # Public price/level observations do not prove who will fill an IOC.
            "counterparty_not_guaranteed": True,
        }
        for key in ("venue_timestamp", "version"):
            if key in public_book:
                payload[key] = public_book[key]
        return payload

    @staticmethod
    def _retryable_pair_after_guard(
        plan: HandoffPlan,
        source: LegReconciliation,
        receiver: LegReconciliation,
        unknown_reasons: Sequence[str],
        priority_guard: Mapping[str, Any] | None,
    ) -> bool:
        if priority_guard is None or priority_guard.get("status") not in {"LOST", "UNKNOWN"}:
            return False
        terminal_source = HandoffEngine._is_exact_canceled_post_only_zero_fill(
            source.order,
            plan.source,
        )
        if terminal_source:
            # The terminal source form is checked structurally below.  Its
            # only permitted top-level barrier is the exact terminal reason;
            # arbitrary scalar strings cannot opt into retry.
            if any(
                str(reason) != "source canceled-post-only zero-fill; receiver was not dispatched"
                for reason in unknown_reasons
            ):
                return False
        else:
            status = str(priority_guard.get("status"))
            expected_reason = (
                f"PAIR_GUARD_{status}: {priority_guard.get('priority_reason')}"
            )
            if any(str(reason) != expected_reason for reason in unknown_reasons):
                return False
        if (
            not source.dispatched
            or receiver.dispatched
            or source.filled_quantity != 0
            or receiver.filled_quantity != 0
            or source.trades
            or receiver.trades
            or not source.history_complete
            or not receiver.history_complete
            or source.unknown_reasons
            or receiver.unknown_reasons
            or source.order is None
            or not source.order.terminal
            or source.order.filled_quantity != 0
            or source.order.remaining_quantity != 0
            or source.position_after != source.position_before
            or receiver.position_after != receiver.position_before
        ):
            return False
        return True

    async def _cancel_if_safe(
        self,
        plan: OrderPlan,
        order: OrderSnapshot,
        journal: DurableJournal,
        run_id: str,
        unknown_reasons: list[str],
        *,
        expected_order_id: str | None = None,
    ) -> None:
        operation_mode = (
            OperationMode.CLOSE_REOPEN
            if plan.reduce_only
            else OperationMode.PAIRED_OPENING
        )
        if (
            order.account_index != plan.account_index
            or order.market_id != plan.market_id
            or order.order_id == ""
            or not order.active
            or order.remaining_quantity <= 0
        ):
            return
        if expected_order_id is not None and order.order_id != str(expected_order_id):
            reason = "source cancellation lookup returned a different order identity"
            if reason not in unknown_reasons:
                unknown_reasons.append(reason)
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
                or (
                    expected_order_id is not None
                    and current.order_id != str(expected_order_id)
                )
                or not current.active
                or not self._time_fresh(current.observed_at, self.clock.now(), self._configured_freshness)
            ):
                if (
                    current is not None
                    and expected_order_id is not None
                    and current.order_id != str(expected_order_id)
                ):
                    reason = "source cancellation lookup returned a different order identity"
                    if reason not in unknown_reasons:
                        unknown_reasons.append(reason)
                return
            current_now = self.clock.now()
            mutation_deadline = self._evidence_deadline(
                observations=(current,),
                observation_now=current_now,
                freshness_seconds=self._configured_freshness,
                request_timeout_seconds=self._configured_request_timeout,
            )
            if mutation_deadline <= time.monotonic():
                reason = "source cancellation freshness budget expired before mutation"
                unknown_reasons.append(reason)
                journal.append(
                    "CANCEL_DISPATCH_BLOCKED",
                    {"operation_mode": operation_mode.value, "reason": reason},
                    run_id=run_id,
                )
                return
            set_deadline = getattr(self.client, "set_mutation_deadline", None)
            if callable(set_deadline):
                set_deadline(mutation_deadline)
            journal.append(
                "CANCEL_DISPATCH_INTENT",
                {
                    "operation_mode": operation_mode.value,
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
                    "operation_mode": operation_mode.value,
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
            journal.append(
                "CANCEL_DISPATCH_UNKNOWN",
                {"operation_mode": operation_mode.value, "reason": reason},
                run_id=run_id,
            )

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
        complete = not (dispatched and order_id is not None)
        deadline = self.clock.now() + config.reconcile_timeout_seconds
        history_requested = dispatched and order_id is not None
        if dispatched and order_id is None:
            complete = False
        history_page_count = 0
        reconcile_round_count = 0
        pagination_exhausted = False
        deadline_exceeded = False
        last_history_error: str | None = None
        last_account_error: str | None = None
        last_order_error: str | None = None
        read_validation_barrier = False
        after: AccountSnapshot | None = None
        order: OrderSnapshot | None = None

        def add_local_unknown(reason: str) -> None:
            if reason not in local_unknown:
                local_unknown.append(reason)

        def process_history_trade(trade: TradeReceipt) -> None:
            if trade.account_index != plan.account_index or trade.market_id != plan.market_id:
                add_local_unknown("trade history contains foreign account/market")
                return
            if trade.observed_at > self.clock.now():
                add_local_unknown("trade receipt is from the future")
                return
            if order_id is not None and trade.order_id != order_id:
                add_local_unknown("trade receipt order identity conflicts with requested order")
                return
            if trade.side.upper() != plan.side:
                add_local_unknown("trade side conflicts with planned leg")
                return
            if plan.order_type == "LIMIT" and trade.price != plan.price:
                add_local_unknown("limit trade price conflicts with planned price")
                return
            if plan.order_type == "MARKET":
                within_bound = (
                    trade.price <= plan.price if plan.side == "BUY" else trade.price >= plan.price
                )
                if not within_bound:
                    add_local_unknown("market trade price violates the directional worst-price bound")
                    return
            economic_key = _trade_economic_key(trade)
            previous = seen.get(economic_key)
            if previous is not None:
                # Observation timestamps can change between pages; the
                # economic receipt identity must remain stable.
                if previous.trade_id != trade.trade_id:
                    add_local_unknown("economic receipt has conflicting trade identity")
                return
            if any(item.trade_id == trade.trade_id and _trade_economic_key(item) != economic_key for item in trades):
                add_local_unknown("duplicate trade id has conflicting receipt fields")
                return
            seen[economic_key] = trade
            trades.append(trade)

        # A terminal order can become visible before its trade history and
        # account position propagate.  Repeat only bounded read rounds; every
        # mutation was already settled before this function was entered.
        max_rounds = max(1, config.max_poll_count)
        while True:
            reconcile_round_count += 1
            round_complete = True
            if history_requested:
                cursor: str | None = None
                while True:
                    if self.clock.now() > deadline:
                        deadline_exceeded = True
                        round_complete = False
                        break
                    if history_page_count >= max(1, config.max_poll_count):
                        pagination_exhausted = bool(cursor)
                        round_complete = False
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
                        last_history_error = None
                    except ContractError as exc:
                        reason = f"trade history read failed: {sanitize_exception(exc)}"
                        add_local_unknown(reason)
                        read_validation_barrier = True
                        last_history_error = None
                        journal.append(
                            "RECONCILIATION_READ_ERROR",
                            {
                                "leg": plan.side,
                                "kind": "trade_history",
                                "round": reconcile_round_count,
                                "reason": reason,
                                "classification": "CONTRACT_ERROR",
                            },
                            run_id=run_id,
                        )
                        round_complete = False
                        break
                    except Exception as exc:
                        last_history_error = f"trade history read failed: {sanitize_exception(exc)}"
                        journal.append(
                            "RECONCILIATION_READ_ERROR",
                            {
                                "leg": plan.side,
                                "kind": "trade_history",
                                "round": reconcile_round_count,
                                "reason": last_history_error,
                            },
                            run_id=run_id,
                        )
                        round_complete = False
                        break
                    history_page_count += 1
                    for trade in page.trades:
                        process_history_trade(trade)
                    if not page.next_cursor:
                        round_complete = page.complete
                        break
                    if page.next_cursor == cursor:
                        add_local_unknown("trade history cursor repeated")
                        round_complete = False
                        break
                    cursor = page.next_cursor
                complete = round_complete
            else:
                complete = not dispatched

            try:
                after = _as_account(
                    await self._bounded(
                        self.client.account_snapshot(plan.account_index, plan.market_id),
                        "final account read",
                    )
                )
                last_account_error = None
            except ContractError as exc:
                after = None
                last_account_error = f"final account read failed: {sanitize_exception(exc)}"
                read_validation_barrier = True
                journal.append(
                    "RECONCILIATION_READ_ERROR",
                    {
                        "leg": plan.side,
                        "kind": "account",
                        "round": reconcile_round_count,
                        "reason": last_account_error,
                        "classification": "CONTRACT_ERROR",
                    },
                    run_id=run_id,
                )
            except Exception as exc:
                after = None
                last_account_error = f"final account read failed: {sanitize_exception(exc)}"
                journal.append(
                    "RECONCILIATION_READ_ERROR",
                    {
                        "leg": plan.side,
                        "kind": "account",
                        "round": reconcile_round_count,
                        "reason": last_account_error,
                    },
                    run_id=run_id,
                )

            if dispatched:
                try:
                    order = await self._lookup_order(
                        plan,
                        order_id,
                        client_order_index=plan.client_order_index,
                    )
                    last_order_error = None
                except ContractError as exc:
                    order = None
                    last_order_error = f"final order read failed: {sanitize_exception(exc)}"
                    read_validation_barrier = True
                    journal.append(
                        "RECONCILIATION_READ_ERROR",
                        {
                            "leg": plan.side,
                            "kind": "order",
                            "round": reconcile_round_count,
                            "reason": last_order_error,
                            "classification": "CONTRACT_ERROR",
                        },
                        run_id=run_id,
                    )
                except Exception as exc:
                    order = None
                    last_order_error = f"final order read failed: {sanitize_exception(exc)}"
                    journal.append(
                        "RECONCILIATION_READ_ERROR",
                        {
                            "leg": plan.side,
                            "kind": "order",
                            "round": reconcile_round_count,
                            "reason": last_order_error,
                        },
                        run_id=run_id,
                    )

            trade_total = sum((trade.quantity for trade in trades), Decimal(0))
            order_identity_conflict = False
            order_not_terminal = False
            order_fill_mismatch = False
            if dispatched and order is not None:
                order_now = self.clock.now()
                if not self._order_matches(order, plan):
                    add_local_unknown("final order identity/parameters conflict with plan")
                    order_identity_conflict = True
                elif order_id is not None and order.order_id != str(order_id):
                    add_local_unknown("final order identifier conflicts with dispatched order")
                    order_identity_conflict = True
                elif not self._time_fresh(order.observed_at, order_now, config.freshness_seconds):
                    # A stale observation may be replaced by a fresh read in
                    # the bounded reconciliation window.
                    order_not_terminal = True
                elif not order.terminal:
                    order_not_terminal = True
                else:
                    order_fill_mismatch = order.filled_quantity != trade_total
            elif dispatched and order is None:
                order_not_terminal = True

            account_identity_conflict = False
            account_needs_retry = False
            if after is not None:
                after_now = self.clock.now()
                if not self._snapshot_matches(after, plan, expected_identity=expected_identity):
                    add_local_unknown("final account identity or freshness is not proven")
                    account_identity_conflict = True
                elif not self._snapshot_fresh(after, after_now, config.freshness_seconds):
                    account_needs_retry = True
                if any(item.active for item in after.active_orders):
                    reason = "final account still has an active HOOD order"
                    add_local_unknown(reason)
                    read_validation_barrier = True
                    journal.append(
                        "RECONCILIATION_ACCOUNT_CONFLICT",
                        {
                            "leg": plan.side,
                            "round": reconcile_round_count,
                            "reason": reason,
                            "account": self._account_observation_payload(after),
                        },
                        run_id=run_id,
                    )

            expected_delta = (Decimal("-1") if plan.side == "SELL" else Decimal("1")) * trade_total
            position_mismatch = (
                dispatched
                and after is not None
                and not account_identity_conflict
                and after.signed_position != before.signed_position + expected_delta
            )
            if position_mismatch:
                account_needs_retry = True

            retry_allowed = (
                reconcile_round_count < max_rounds
                and history_page_count < max(1, config.max_poll_count)
                and self.clock.now() <= deadline
                and not pagination_exhausted
                and not deadline_exceeded
                and not order_identity_conflict
                and not account_identity_conflict
                and not read_validation_barrier
                and not any(reason == "trade history cursor repeated" for reason in local_unknown)
            )
            needs_retry = bool(
                history_requested
                and (
                    not complete
                    or order_not_terminal
                    or order_fill_mismatch
                    or after is None
                    or bool(last_account_error)
                    or bool(last_order_error)
                    or account_needs_retry
                )
            )
            if not (retry_allowed and needs_retry):
                break
            remaining = deadline - self.clock.now()
            if remaining <= 0:
                deadline_exceeded = True
                break
            await self._sleep(min(self._poll_interval, remaining))

        if deadline_exceeded:
            add_local_unknown("reconciliation deadline exceeded")
            complete = False
        if pagination_exhausted:
            add_local_unknown("trade history pagination exceeded configured bound")
            complete = False
        if last_history_error is not None:
            add_local_unknown(last_history_error)
            complete = False
        if last_account_error is not None:
            add_local_unknown(last_account_error)
        if last_order_error is not None:
            add_local_unknown(last_order_error)
        if dispatched and order is None:
            add_local_unknown("final order state is missing")
        elif dispatched and order is not None:
            order_now = self.clock.now()
            if not self._order_matches(order, plan):
                add_local_unknown("final order identity/parameters conflict with plan")
            elif order_id is not None and order.order_id != str(order_id):
                add_local_unknown("final order identifier conflicts with dispatched order")
            elif not self._time_fresh(order.observed_at, order_now, config.freshness_seconds):
                add_local_unknown("final order state is stale or from the future")
            elif not order.terminal:
                add_local_unknown("final order state is not terminal")
            if not complete:
                add_local_unknown("trade history is incomplete")
            trade_total = sum((trade.quantity for trade in trades), Decimal(0))
            if order.filled_quantity != trade_total:
                add_local_unknown("terminal order filled quantity conflicts with trade receipt sum")
        if after is not None and (
            not self._snapshot_matches(after, plan, expected_identity=expected_identity)
            or not self._snapshot_fresh(after, self.clock.now(), config.freshness_seconds)
        ):
            add_local_unknown("final account identity or freshness is not proven")
        if after is not None and not dispatched and after.signed_position != before.signed_position:
            add_local_unknown("non-dispatched leg position changed unexpectedly")
        expected_delta = (Decimal("-1") if plan.side == "SELL" else Decimal("1")) * sum(
            (trade.quantity for trade in trades), Decimal(0)
        )
        if after is not None and dispatched and after.signed_position != before.signed_position + expected_delta:
            add_local_unknown("final position does not equal independently reconciled trade quantity")
        if after is not None and any(item.active for item in after.active_orders):
            add_local_unknown("final account still has an active HOOD order")
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
                "operation_mode": config.operation_mode.value,
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
                        "fee_role": trade.fee_role,
                        "venue_fee_raw": trade.venue_fee_raw,
                        "integrator_fee_raw": trade.integrator_fee_raw,
                        "fee_evidence": trade.fee_evidence,
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
        retryable_pair: bool = False,
        latency: Mapping[str, Any] | None = None,
        priority_guard: Mapping[str, Any] | None = None,
    ) -> HandoffResult:
        joint_status, joint_quantity, joint_reasons = _joint_trade_match(source, receiver, plan.quantity)
        economic_status, economic_findings = _economic_findings(source, receiver)
        findings = tuple(dict.fromkeys((*joint_reasons, *economic_findings)))
        outcome = forced_outcome or self._classify(plan, source, receiver, unknown_reasons)
        mutual_failure = bool(
            outcome is Outcome.SUCCESS
            and plan.operation_mode in {OperationMode.PAIRED_OPENING, OperationMode.PAIRED_CLOSING}
            and (joint_status != "MATCHED" or joint_quantity != plan.quantity)
        )
        if mutual_failure:
            outcome = Outcome.PARTIAL
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
        if mutual_failure:
            reason = "full own-account execution was not proven: " + joint_reasons[0]
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
            operation_mode=plan.operation_mode,
            retryable_pair=retryable_pair,
            attempt_index=config.attempt_index,
            latency=None if latency is None else dict(latency),
            priority_guard=None if priority_guard is None else dict(priority_guard),
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
                "operation_mode": plan.operation_mode.value,
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
                "retryable_pair": retryable_pair,
                "latency": None if latency is None else dict(latency),
                "priority_guard": None if priority_guard is None else dict(priority_guard),
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
        if self._known_source_only_partial(plan, source, receiver, unknown_reasons):
            # The pre-receiver source observation is retained in the durable
            # evidence, but a later complete source reconciliation proves a
            # known source-only residual.  That residual is eligible only for
            # the caller's existing reduce-only cleanup path; it never makes
            # the receiver dispatch or hold barrier pass.
            known_partial_reasons.update(
                {
                    "source order disappeared during pre-receiver recheck",
                    "source fill or quantity change before receiver dispatch",
                    "source exact order is missing before receiver dispatch",
                }
            )
            # Admission uncertainty is distinct from execution uncertainty.
            # A guard may fail first and the maker fill during cancellation;
            # there need not be an earlier private "source disappeared" event.
            known_partial_reasons.update(
                reason for reason in unknown_reasons
                if reason.startswith(("PAIR_GUARD_LOST: ", "PAIR_GUARD_UNKNOWN: "))
            )
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

    @staticmethod
    def _known_source_only_partial(
        plan: HandoffPlan,
        source: LegReconciliation,
        receiver: LegReconciliation,
        unknown_reasons: Sequence[str],
    ) -> bool:
        """Prove a narrow pre-receiver source observation is a known residual.

        A source order can disappear, or become filled/canceled, between the
        two bounded account reads and the required final exact-order lookup.
        The observation remains a stop reason, while a later terminal order,
        complete trade history and agreeing final position can prove the
        source leg independently.  All other unknowns remain barriers,
        including a missing/conflicting final order, identity drift,
        incomplete history or a dispatched receiver.
        """

        provisional_reasons = {
            "source fill observed before receiver dispatch",
            "source order did not prove exact resting quantity",
            "source order disappeared during pre-receiver recheck",
            "source fill or quantity change before receiver dispatch",
            "source exact order is missing before receiver dispatch",
        }
        def provisional(reason: str) -> bool:
            return reason in provisional_reasons or reason.startswith(
                ("PAIR_GUARD_LOST: ", "PAIR_GUARD_UNKNOWN: ")
            )

        if not unknown_reasons or not all(provisional(reason) for reason in unknown_reasons):
            return False
        if source.unknown_reasons or receiver.unknown_reasons:
            return False
        if not source.dispatched or receiver.dispatched:
            return False
        if not source.history_complete or not receiver.history_complete:
            return False
        if source.order is None or not source.order.terminal:
            return False
        if receiver.order is not None or receiver.trades:
            return False
        if receiver.position_after != receiver.position_before:
            return False
        if source.filled_quantity <= 0 or source.filled_quantity > plan.quantity:
            return False
        if source.order.filled_quantity != source.filled_quantity:
            return False
        expected_source = plan.source_position_before - plan.quantity * plan.direction.sign
        expected_source += (plan.quantity - source.filled_quantity) * plan.direction.sign
        return source.position_after == expected_source

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
                operation_mode=config.operation_mode,
            )
        if run_event is None or plan_event is None or attempt_event is None or run_id is None:
            reason = "unresolved journal lacks a complete plan; reconciliation cannot be bound safely"
            return HandoffResult(Outcome.UNKNOWN, Phase.RECONCILIATION, journal.run_id, None, None, None, reason, (reason,), operation_mode=config.operation_mode)
        if attempt_event.payload.get("binding") != binding:
            reason = "restart configuration/account/environment binding conflicts with the original attempt"
            journal.append("RESTART_BINDING_MISMATCH", {"reason": reason}, run_id=run_event.run_id)
            return HandoffResult(Outcome.UNKNOWN, Phase.RECONCILIATION, run_event.run_id, None, None, None, reason, (reason,), operation_mode=config.operation_mode)
        try:
            plan = _plan_from_dict(plan_event.payload.get("plan"))
        except Exception as exc:
            reason = f"journal plan cannot be reconstructed safely: {sanitize_exception(exc)}"
            journal.append("RESTART_RECONCILIATION_BLOCKED", {"reason": reason}, run_id=run_id)
            return HandoffResult(Outcome.UNKNOWN, Phase.RECONCILIATION, run_id, None, None, None, reason, (reason,), operation_mode=config.operation_mode)
        if plan.run_id != run_id:
            reason = "journal plan run identity conflicts with the unresolved mutation run"
            journal.append("RESTART_RECONCILIATION_BLOCKED", {"reason": reason}, run_id=run_id)
            return HandoffResult(Outcome.UNKNOWN, Phase.RECONCILIATION, run_id, None, None, None, reason, (reason,), operation_mode=config.operation_mode)
        if not _plan_matches_binding(plan, binding):
            reason = "journal plan identity/parameters conflict with the original configuration"
            journal.append("RESTART_RECONCILIATION_BLOCKED", {"reason": reason}, run_id=run_id)
            return HandoffResult(Outcome.UNKNOWN, Phase.RECONCILIATION, run_id, None, None, None, reason, (reason,), operation_mode=config.operation_mode)
        if not plan.source_identity or not plan.receiver_identity:
            reason = "journal plan lacks immutable source/receiver identities"
            journal.append("RESTART_RECONCILIATION_BLOCKED", {"reason": reason}, run_id=run_id)
            return HandoffResult(Outcome.UNKNOWN, Phase.RECONCILIATION, run_id, plan, None, None, reason, (reason,), operation_mode=config.operation_mode)
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
            return HandoffResult(Outcome.UNKNOWN, Phase.RECONCILIATION, run_id, plan, None, None, reason, (reason,), operation_mode=config.operation_mode)
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
            return HandoffResult(Outcome.UNKNOWN, Phase.RECONCILIATION, run_id, plan, None, None, reason, (reason,), operation_mode=config.operation_mode)
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

    async def _bounded(
        self,
        awaitable: Any,
        label: str,
        *,
        deadline: float | None = None,
    ) -> Any:
        """Bound every SDK/read boundary without ever retrying a mutation."""

        timeout = self._request_timeout
        if deadline is not None:
            timeout = min(timeout, deadline - time.monotonic())
            if timeout <= 0:
                close = getattr(awaitable, "close", None)
                if callable(close):
                    close()
                raise TimeoutError(f"{label} crossed the final mutation barrier")
        try:
            return await asyncio.wait_for(awaitable, timeout=timeout)
        except asyncio.TimeoutError as exc:
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"{label} crossed the final mutation barrier") from exc
            raise TimeoutError(f"{label} exceeded configured request timeout") from exc

    @staticmethod
    def _evidence_deadline(
        *,
        observations: Sequence[Any],
        observation_now: float,
        freshness_seconds: float,
        request_timeout_seconds: float,
    ) -> float:
        """Return one monotonic barrier for all evidence used by a mutation."""

        remaining_freshness = freshness_seconds
        for observation in observations:
            if observation is None:
                continue
            if isinstance(observation, Mapping):
                observed_at = observation.get("observed_at")
            else:
                observed_at = getattr(observation, "observed_at", observation)
            try:
                observed_at = float(observed_at)
            except (TypeError, ValueError):
                return time.monotonic()
            if not math.isfinite(observed_at) or observed_at > observation_now:
                return time.monotonic()
            remaining_freshness = min(
                remaining_freshness,
                max(0.0, freshness_seconds - (observation_now - observed_at)),
            )
        budget = min(request_timeout_seconds, remaining_freshness)
        return time.monotonic() + max(0.0, budget)

    def _mutation_plan(
        self,
        plan: OrderPlan,
        config: HandoffConfig,
        *,
        observations: Sequence[Any],
        observation_now: float | None = None,
    ) -> OrderPlan:
        # The SDK uses this monotonic barrier for nonce reads, signing and the
        # single sendTx call.  It preserves the remaining lifetime of every
        # observation used for this phase; it never renews an old observation.
        now = self.clock.now() if observation_now is None else observation_now
        deadline = self._evidence_deadline(
            observations=observations,
            observation_now=now,
            freshness_seconds=config.freshness_seconds,
            request_timeout_seconds=config.request_timeout_seconds,
        )
        if self._preflight_context is not None and self._preflight_context.nonce_deadline is not None:
            deadline = min(deadline, self._preflight_context.nonce_deadline)
        if plan.order_type == "LIMIT" and config.max_quote_age_seconds is not None:
            remaining = config.max_quote_age_seconds - (now - config.source_quote_observed_at)
            deadline = min(deadline, time.monotonic() + max(0.0, remaining))
        if plan.order_type == "MARKET" and config.max_source_to_receiver_seconds is not None and self._source_dispatch_monotonic is not None:
            deadline = min(deadline, self._source_dispatch_monotonic + config.max_source_to_receiver_seconds)
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
    """Prove reciprocal trade/order identities without reusing any quantity."""

    if not source.trades or not receiver.trades:
        return "UNKNOWN", Decimal(0), ("joint trade matching is UNKNOWN because one leg has no trade receipt",)
    if any(
        trade.counterparty_account_index is None
        for trade in (*source.trades, *receiver.trades)
    ):
        return "UNKNOWN", Decimal(0), ("joint trade matching is UNKNOWN because a counterparty account is missing",)
    if any(
        trade.counterparty_order_id is None
        for leg, peer in ((source, receiver), (receiver, source))
        for trade in leg.trades if trade.counterparty_account_index == peer.account_index
    ):
        return "UNKNOWN", Decimal(0), ("joint trade matching is UNKNOWN because a reciprocal order identity is missing",)
    if any(len({t.trade_id for t in leg.trades}) != len(leg.trades) for leg in (source, receiver)):
        return "CONFLICTING", Decimal(0), ("joint trade matching is CONFLICTING: duplicate trade identities",)

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
                source_trade.counterparty_order_id == receiver_trade.order_id
            ) and (
                receiver_trade.counterparty_order_id == source_trade.order_id
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

    if conflicts and matched > 0:
        status = "CONFLICTING"
    elif matched == expected_quantity:
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
        if matched > 0:
            reasons.append(
                "joint trade matching is CONFLICTING:"
                f" {matched} quantity is compatible but another receipt has incompatible"
                " trade/order IDs, prices or quantities"
            )
        else:
            reasons.append("joint trade matching is CONFLICTING: trade/order IDs, prices or quantities are incompatible")
    else:
        reasons.append("joint trade matching is KNOWN_ZERO: receipts name no direct source/receiver pair")
    return status, matched, tuple(reasons)


def _leg_proves_no_execution(leg: LegReconciliation) -> bool:
    """Prove that a reconciled leg incurred no execution fee."""

    if (
        leg.unknown_reasons
        or not leg.history_complete
        or not leg.trades
        and leg.position_after != leg.position_before
    ):
        return False
    if leg.trades:
        return False
    if not leg.dispatched:
        return leg.order is None and leg.position_after == leg.position_before
    order = leg.order
    return bool(
        order is not None
        and order.terminal
        and not order.active
        and order.filled_quantity == 0
        and order.remaining_quantity == 0
        and leg.position_after == leg.position_before
    )


def _economic_findings(
    source: LegReconciliation,
    receiver: LegReconciliation,
) -> tuple[str, tuple[str, ...]]:
    """Report observed economics without turning them into admission gates."""

    findings: list[str] = []
    has_unknown = False
    has_evidence = False
    for label, leg in (("source", source), ("receiver", receiver)):
        if not leg.trades:
            if _leg_proves_no_execution(leg):
                findings.append(f"{label} no execution proven; no execution fees were due")
                has_evidence = True
            else:
                has_unknown = True
            continue
        has_evidence = True
        gross = leg.gross_notional
        fee_total = leg.fee_total
        if fee_total is None:
            has_unknown = True
            findings.append(f"{label} fee economics UNKNOWN: at least one official receipt has no fee")
        else:
            findings.append(f"{label} observed gross {gross} and fee {fee_total}")
    if has_unknown:
        return "UNKNOWN", tuple(findings)
    return ("PROVEN" if has_evidence else "UNKNOWN"), tuple(findings)


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
        "operation_mode": config.operation_mode.value,
        "mode": config.operation_mode.value,
        "attempt_index": config.attempt_index,
        "defer_incremental_margin_calculation": config.defer_incremental_margin_calculation,
        "quantity": str(config.quantity),
        "source_limit_price": str(config.source_limit_price),
        "receiver_worst_price": str(config.receiver_worst_price),
        "expected_source_position": (
            None
            if config.expected_source_position is None
            else str(config.expected_source_position)
        ),
        "expected_receiver_position": (
            None
            if config.expected_receiver_position is None
            else str(config.expected_receiver_position)
        ),
        "operator_execution_opt_in": config.operator_execution_opt_in,
        "operator_plan_reviewed": config.operator_plan_reviewed,
        "freshness_seconds": config.freshness_seconds,
        "request_timeout_seconds": config.request_timeout_seconds,
        "order_timeout_seconds": config.order_timeout_seconds,
        "reconcile_timeout_seconds": config.reconcile_timeout_seconds,
        "poll_interval_seconds": config.poll_interval_seconds,
        "max_poll_count": config.max_poll_count,
        "client_order_prefix": config.client_order_prefix,
        "source_order_lifetime_seconds": config.source_order_lifetime_seconds,
        "source_quote_observed_at": config.source_quote_observed_at,
        **{name: getattr(config, name) for name in ("max_quote_age_seconds", "max_source_to_receiver_seconds")
           if getattr(config, name) is not None},
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
        metadata_observed_at=(
            None if value.get("metadata_observed_at") is None else float(value["metadata_observed_at"])
        ),
        operation_mode=value.get("operation_mode", value.get("mode", OperationMode.CLOSE_REOPEN.value)),
        defer_incremental_margin_calculation=value.get("defer_incremental_margin_calculation", False),
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
            and plan.operation_mode.value
            == str(binding.get("operation_mode", binding.get("mode", OperationMode.CLOSE_REOPEN.value)))
            and isinstance(binding.get("defer_incremental_margin_calculation", False), bool)
            and plan.defer_incremental_margin_calculation
            == binding.get("defer_incremental_margin_calculation", False)
            and plan.quantity == Decimal(str(binding["quantity"]))
            and plan.source.quantity == plan.quantity
            and plan.receiver.quantity == plan.quantity
            and plan.source.price == Decimal(str(binding["source_limit_price"]))
            and plan.receiver.price == Decimal(str(binding["receiver_worst_price"]))
            and (
                binding.get("expected_source_position") is None
                or plan.source_position_before == Decimal(str(binding["expected_source_position"]))
            )
            and (
                binding.get("expected_receiver_position") is None
                or plan.receiver_position_before == Decimal(str(binding["expected_receiver_position"]))
            )
        )
    except (KeyError, TypeError, ValueError, ArithmeticError):
        return False


async def run_handoff(config: HandoffConfig, client: HandoffClient, *, clock: Clock | None = None) -> HandoffResult:
    engine = HandoffEngine(client, clock=clock)
    engine._configured_poll_limit = config.max_poll_count
    engine._configured_poll_interval = config.poll_interval_seconds
    return await engine.execute(config)


__all__ = ["Clock", "HandoffClient", "HandoffEngine", "SystemClock", "run_handoff"]
