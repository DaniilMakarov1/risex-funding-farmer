from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
import io
import json
import logging
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from risex_farmer import lighter_testnet_lifecycle as lifecycle
from risex_farmer import lighter_testnet_readiness as readiness


ADDRESS = readiness.EXPECTED_L1_ADDRESS
ACCOUNT_INDEX = 202
API_KEY_INDEX = 4
PUBLIC_KEY = "0x" + "11" * 40
MARKET_ID = 987
HOUR = lifecycle.HOURLY_FUNDING_INTERVAL_MS
LIGHTER_TRANSACTION_HASH = "ab" * 40


def identity() -> lifecycle.IdentityMetadata:
    return lifecycle.IdentityMetadata(
        l1_address=ADDRESS,
        account_index=ACCOUNT_INDEX,
        api_key_index=API_KEY_INDEX,
        api_key_public_key=PUBLIC_KEY,
        sdk_commit=lifecycle.LIGHTER_SDK_COMMIT,
    )


def ready_readiness() -> readiness.ReadinessResult:
    return readiness.ReadinessResult(
        status="READY",
        failure_class="NONE",
        reason="FIXTURE_READY",
        wallet_address=ADDRESS,
        account_index=ACCOUNT_INDEX,
        api_key_index=API_KEY_INDEX,
        requests=7,
        retries=0,
        identity_verified=True,
        authorization_identity_verified=True,
        api_key_verified=True,
        collateral_positive=True,
        fees_verified=True,
        active_orders_zero=True,
        positions_flat=True,
        unrelated_state_clear=True,
        trades_read=True,
        funding_history_read=True,
        active_order_count=0,
        regular_order_count=0,
        trigger_order_count=0,
        trade_count=0,
        funding_count=0,
        asset_count=3,
        position_count=0,
        api_key_public_key_verified=True,
        bootstrap_asset_baseline_verified=True,
    )


def market_at(now_ms: int) -> lifecycle.MarketObservation:
    boundary = HOUR if now_ms < HOUR else 2 * HOUR
    return lifecycle.MarketObservation(
        contract=readiness.MarketContract(
            market_id=MARKET_ID,
            symbol="LIT",
            market_type="perp",
            min_base_amount=Decimal("1"),
            min_quote_amount=Decimal("10"),
            size_decimals=0,
            price_decimals=2,
            quote_decimals=6,
            status="active",
            force_reduce_only=False,
        ),
        bids=(lifecycle.BookLevel(Decimal("10.00"), Decimal("2")),),
        asks=(lifecycle.BookLevel(Decimal("10.10"), Decimal("2")),),
        funding=lifecycle.FundingSchedule(
            market_id=MARKET_ID,
            symbol="LIT",
            next_boundary_ms=boundary,
            interval_ms=HOUR,
            rate=Decimal("0.001"),
            source="synthetic-authoritative-schedule",
            observed_at_ms=now_ms,
        ),
        observed_at_ms=now_ms,
    )


def close_market_at(
    now_ms: int,
    *,
    best_bid: str = "3.7285",
    best_ask: str = "3.8000",
    bid_quantity: str = "3.00",
    ask_quantity: str = "3.00",
    min_base_amount: str = "3.00",
    price_decimals: int = 4,
) -> lifecycle.MarketObservation:
    base = market_at(now_ms)
    return replace(
        base,
        contract=replace(
            base.contract,
            min_base_amount=Decimal(min_base_amount),
            size_decimals=2,
            price_decimals=price_decimals,
        ),
        bids=(lifecycle.BookLevel(Decimal(best_bid), Decimal(bid_quantity)),),
        asks=(lifecycle.BookLevel(Decimal(best_ask), Decimal(ask_quantity)),),
    )


class SyntheticGateway:
    def __init__(
        self,
        clock: list[int],
        *,
        funding_mode: str = "good",
        disagree_terminal: bool = False,
        ambiguous_kind: lifecycle.IntentKind | None = None,
        response_mismatch_kind: lifecycle.IntentKind | None = None,
        drop_after_accept_kind: lifecycle.IntentKind | None = None,
        public_order_nonces: dict[lifecycle.IntentKind, int] | None = None,
        maker_cancel_status: str = "CANCELLED",
        maker_cancel_status_sequence: tuple[str, ...] | None = None,
        maker_cancel_unrelated_order: bool = False,
        maker_cancel_mismatch: bool = False,
        maker_cancel_fill: bool = False,
    ) -> None:
        self.clock = clock
        self.funding_mode = funding_mode
        self.disagree_terminal = disagree_terminal
        self.ambiguous_kind = ambiguous_kind
        self.response_mismatch_kind = response_mismatch_kind
        self.drop_after_accept_kind = drop_after_accept_kind
        self.public_order_nonces = dict(public_order_nonces or {})
        self.maker_cancel_status = maker_cancel_status
        self.maker_cancel_status_sequence = (
            None
            if maker_cancel_status_sequence is None
            else tuple(maker_cancel_status_sequence)
        )
        self.maker_cancel_status_index: int | None = None
        self.maker_cancel_order_index: int | None = None
        self.maker_cancel_unrelated_order = maker_cancel_unrelated_order
        self.maker_cancel_mismatch = maker_cancel_mismatch
        self.maker_cancel_fill = maker_cancel_fill
        self.store: lifecycle.LifecycleStore | None = None
        self.discover_calls = 0
        self.market_calls: list[int] = []
        self.snapshot_calls = 0
        self.nonce_calls: list[tuple[int, int]] = []
        self.dispatches: list[tuple[str, int, int, bool, bool]] = []
        self.orders: dict[int, lifecycle.OrderSnapshot] = {}
        self.fills: list[lifecycle.FillSnapshot] = []
        self.position = Decimal("0")
        self.next_order_index = 500
        self.terminal_calls = 0
        self.close_calls = 0
        self.close_error: BaseException | None = None
        self.cancel_calls = 0
        self.cancel_snapshot_statuses: list[str] = []
        self.sleep_calls: list[float] = []

    async def close(self) -> None:
        self.close_calls += 1
        if self.close_error is not None:
            raise self.close_error

    async def discover_market(self) -> lifecycle.MarketObservation:
        self.discover_calls += 1
        return market_at(self.clock[0])

    async def market(self, market_id: int) -> lifecycle.MarketObservation:
        if market_id == 120:
            raise AssertionError("the tentative market id must never be used")
        self.market_calls.append(market_id)
        return market_at(self.clock[0])

    def _account(self) -> lifecycle.AccountSnapshot:
        return lifecycle.AccountSnapshot(
            account_index=ACCOUNT_INDEX,
            l1_address=ADDRESS,
            collateral=Decimal("1000"),
            maker_fee_tick=0,
            taker_fee_tick=0,
            orders=tuple(self.orders.values()),
            positions=(
                lifecycle.PositionSnapshot(ACCOUNT_INDEX, MARKET_ID, self.position),
            ),
            fills=tuple(self.fills),
            funding_high_water=0,
            unrelated_state_clear=True,
            observed_at_ms=self.clock[0],
        )

    async def snapshot(
        self,
        market_id: int,
        *,
        client_order_index: int | None = None,
        post_cancel_target: tuple[int, int] | None = None,
    ) -> lifecycle.AccountSnapshot:
        assert market_id == MARKET_ID
        if post_cancel_target is not None:
            assert post_cancel_target == (
                self.maker_cancel_order_index,
                client_order_index,
            )
        self.snapshot_calls += 1
        if self.maker_cancel_status_index is not None:
            assert self.maker_cancel_order_index is not None
            order = self.orders[self.maker_cancel_order_index]
            if self.maker_cancel_status_sequence is not None:
                if self.maker_cancel_status_index < len(self.maker_cancel_status_sequence):
                    order = self._with_cancel_status(
                        order,
                        self.maker_cancel_status_sequence[self.maker_cancel_status_index],
                    )
                    self.orders[self.maker_cancel_order_index] = order
                    self.maker_cancel_status_index += 1
                self.cancel_snapshot_statuses.append(order.status)
        return self._account()

    async def reconcile_nonce(self, *, account_index: int, api_key_index: int) -> int:
        self.nonce_calls.append((account_index, api_key_index))
        return 100 + len(self.nonce_calls) - 1

    def _assert_intent_durable(self) -> None:
        assert self.store is not None
        assert self.store.intent_count() == len(self.dispatches) + 1
        assert self.store.dispatch_count() == len(self.dispatches) + 1

    @staticmethod
    def _with_cancel_status(
        order: lifecycle.OrderSnapshot, status: str
    ) -> lifecycle.OrderSnapshot:
        active_statuses = {
            "OPEN",
            "ACTIVE",
            "PENDING",
            "IN-PROGRESS",
            "IN_PROGRESS",
        }
        if status.upper() in active_statuses:
            return replace(
                order,
                remaining_quantity=order.quantity,
                filled_quantity=Decimal("0"),
                filled_quote=Decimal("0"),
                status=status,
            )
        if status.upper() == "FILLED":
            return replace(
                order,
                remaining_quantity=Decimal("0"),
                filled_quantity=order.quantity,
                filled_quote=order.quantity * order.price,
                status=status,
            )
        return replace(
            order,
            remaining_quantity=Decimal("0"),
            filled_quantity=Decimal("0"),
            filled_quote=Decimal("0"),
            status=status,
        )

    def _new_order(
        self,
        request: lifecycle.OrderRequest,
        *,
        nonce: int,
        status: str,
        order_index: int,
        fill_price: Decimal | None = None,
    ) -> lifecycle.OrderSnapshot:
        filled = request.quantity if status == "FILLED" else Decimal("0")
        price = request.price if fill_price is None else fill_price
        kind = (
            lifecycle.IntentKind.CLOSE
            if request.reduce_only
            else (
                lifecycle.IntentKind.OPEN
                if request.order_type == "market"
                else lifecycle.IntentKind.MAKER_PLACE
            )
        )
        return lifecycle.OrderSnapshot(
            order_index=order_index,
            client_order_index=request.client_order_index,
            account_index=ACCOUNT_INDEX,
            market_id=MARKET_ID,
            quantity=request.quantity,
            remaining_quantity=Decimal("0") if filled else request.quantity,
            filled_quantity=filled,
            filled_quote=filled * price,
            price=request.price,
            is_ask=request.is_ask,
            order_type=request.order_type,
            time_in_force=request.time_in_force,
            reduce_only=request.reduce_only,
            nonce=self.public_order_nonces.get(kind, nonce),
            status=status,
        )

    async def create_order(
        self, request: lifecycle.OrderRequest, *, nonce: int, api_key_index: int
    ) -> lifecycle.DispatchOutcome:
        self._assert_intent_durable()
        assert api_key_index == API_KEY_INDEX
        kind = "CLOSE" if request.reduce_only else ("OPEN" if request.order_type == "market" else "MAKER_PLACE")
        kind_enum = lifecycle.IntentKind[kind]
        if self.ambiguous_kind == kind_enum:
            raise lifecycle.AmbiguousDispatch("SYNTHETIC_AMBIGUOUS_SEND")
        order_index = self.next_order_index
        self.next_order_index += 1
        if request.order_type == "limit":
            order = self._new_order(
                request, nonce=nonce, status="OPEN", order_index=order_index
            )
        elif request.reduce_only:
            order = self._new_order(
                request,
                nonce=nonce,
                status="FILLED",
                order_index=order_index,
                fill_price=Decimal("10.10"),
            )
            self.position = Decimal("0")
            self.fills.append(
                lifecycle.FillSnapshot(
                    trade_id="close-trade",
                    order_index=order_index,
                    client_order_index=request.client_order_index,
                    account_index=ACCOUNT_INDEX,
                    market_id=MARKET_ID,
                    quantity=request.quantity,
                    price=Decimal("10.10"),
                    is_ask=request.is_ask,
                    timestamp_ms=self.clock[0],
                )
            )
        else:
            order = self._new_order(
                request,
                nonce=nonce,
                status="FILLED",
                order_index=order_index,
                fill_price=Decimal("10.00"),
            )
            self.position = -request.quantity if request.is_ask else request.quantity
            self.fills.append(
                lifecycle.FillSnapshot(
                    trade_id="open-trade",
                    order_index=order_index,
                    client_order_index=request.client_order_index,
                    account_index=ACCOUNT_INDEX,
                    market_id=MARKET_ID,
                    quantity=request.quantity,
                    price=Decimal("10.00"),
                    is_ask=request.is_ask,
                    timestamp_ms=self.clock[0],
                )
            )
        self.orders[order_index] = order
        self.dispatches.append((kind, nonce, api_key_index, request.reduce_only, request.is_ask))
        if self.response_mismatch_kind == kind_enum:
            raise lifecycle.LifecycleHalt("SDK_ORDER_RESPONSE_MISMATCH", failure_class="SAFETY")
        if self.drop_after_accept_kind == kind_enum:
            del self.orders[order_index]
        return lifecycle.DispatchOutcome(
            accepted=True,
            order_index=order_index,
            tx_hash="12" * 40,
        )

    async def cancel_order(
        self,
        market_id: int,
        order_index: int,
        *,
        nonce: int,
        api_key_index: int,
    ) -> lifecycle.DispatchOutcome:
        self._assert_intent_durable()
        assert market_id == MARKET_ID
        assert api_key_index == API_KEY_INDEX
        self.cancel_calls += 1
        order = self.orders[order_index]
        self.maker_cancel_order_index = order_index
        if self.maker_cancel_status_sequence is None:
            updated = self._with_cancel_status(order, self.maker_cancel_status)
        else:
            if not self.maker_cancel_status_sequence:
                raise AssertionError("the synthetic status sequence must not be empty")
            updated = order
            self.maker_cancel_status_index = 0
        if self.maker_cancel_mismatch:
            updated = replace(updated, price=updated.price + Decimal("0.01"))
        self.orders[order_index] = updated
        if self.maker_cancel_fill:
            self.fills.append(
                lifecycle.FillSnapshot(
                    trade_id="maker-cancel-fill",
                    order_index=order_index,
                    client_order_index=order.client_order_index,
                    account_index=ACCOUNT_INDEX,
                    market_id=MARKET_ID,
                    quantity=order.quantity,
                    price=order.price,
                    is_ask=order.is_ask,
                    timestamp_ms=self.clock[0],
                )
            )
        if self.maker_cancel_unrelated_order:
            self.orders[order_index + 1] = replace(
                order,
                order_index=order_index + 1,
                client_order_index=order.client_order_index + 1,
                remaining_quantity=order.quantity,
                filled_quantity=Decimal("0"),
                filled_quote=Decimal("0"),
                status="OPEN",
            )
        self.dispatches.append(("MAKER_CANCEL", nonce, api_key_index, False, order.is_ask))
        return lifecycle.DispatchOutcome(
            accepted=True,
            tx_hash="13" * 40,
        )

    async def funding_history(
        self,
        market_id: int,
        *,
        account_index: int,
        baseline_high_water: int,
        boundary_ms: int | None = None,
    ) -> lifecycle.FundingHistory:
        assert market_id == MARKET_ID
        assert account_index == ACCOUNT_INDEX
        quantity = abs(self.position)
        change = {
            "good": Decimal("0.25"),
            "negative": Decimal("-0.25"),
            "zero": Decimal("0"),
        }.get(self.funding_mode, Decimal("0.25"))
        records: tuple[lifecycle.FundingRecord, ...]
        if self.funding_mode == "missing":
            records = ()
        elif self.funding_mode == "contradictory":
            records = (
                lifecycle.FundingRecord(1, MARKET_ID, HOUR, change, Decimal("0.001"), quantity, "short"),
                lifecycle.FundingRecord(2, MARKET_ID, HOUR, change, Decimal("0.001"), quantity, "short"),
            )
        else:
            records = (
                lifecycle.FundingRecord(1, MARKET_ID, HOUR, change, Decimal("0.001"), quantity, "short"),
            )
        return lifecycle.FundingHistory(
            complete=self.funding_mode != "incomplete",
            baseline_high_water=baseline_high_water,
            high_water=1,
            records=records,
        )

    async def terminal_round(self, market_id: int) -> lifecycle.TerminalRound:
        assert market_id == MARKET_ID
        self.terminal_calls += 1
        self.clock[0] += 1
        digest = "0x" + ("ab" if not self.disagree_terminal or self.terminal_calls == 1 else "cd") * 32
        return lifecycle.TerminalRound(
            round_id=f"terminal-{self.terminal_calls}",
            digest=digest,
            account_index=ACCOUNT_INDEX,
            market_id=MARKET_ID,
            observed_at_ms=self.clock[0],
            active_regular_orders=0,
            active_trigger_orders=0,
            signed_position=Decimal("0"),
            unrelated_state_clear=True,
        )

    async def sleep_until_boundary(self, delay: float) -> None:
        assert delay >= 0
        self.sleep_calls.append(delay)
        self.clock[0] += int(delay * 1000)


def make_runner(
    tmp_path: Path,
    gateway: SyntheticGateway,
    *,
    mode: lifecycle.RunMode = lifecycle.RunMode.TESTNET_WRITE,
) -> tuple[lifecycle.LighterLevelCRunner, lifecycle.LifecycleStore]:
    store = lifecycle.LifecycleStore(tmp_path / "lighter-level-c.sqlite")
    gateway.store = store
    runner = lifecycle.LighterLevelCRunner(
        gateway,
        store,
        readiness=ready_readiness(),
        identity=identity(),
        mode=mode,
        clock_ms=lambda: gateway.clock[0],
        sleep=gateway.sleep_until_boundary,
    )
    return runner, store


@pytest.mark.asyncio
async def test_preflight_validates_each_read_against_post_read_clock(tmp_path):
    clock = [0]
    gateway = SyntheticGateway(clock)

    async def discover_after_clock_advance():
        clock[0] += 1
        return market_at(clock[0])

    async def snapshot_after_clock_advance(
        market_id: int, *, client_order_index: int | None = None
    ):
        assert market_id == MARKET_ID
        assert client_order_index is None
        clock[0] += 1
        return gateway._account()

    gateway.discover_market = discover_after_clock_advance
    gateway.snapshot = snapshot_after_clock_advance
    runner, store = make_runner(tmp_path, gateway)

    market, account, quantity, _, _ = await runner._preflight()

    assert market.observed_at_ms == 1
    assert account.observed_at_ms == 2
    assert quantity > 0
    store.close()


@pytest.mark.asyncio
async def test_fresh_prewrite_rejects_market_stale_after_market_read(tmp_path):
    clock = [0]
    gateway = SyntheticGateway(clock)
    runner, store = make_runner(tmp_path, gateway)

    async def market_after_clock_advance(market_id):
        observed = market_at(clock[0])
        clock[0] += 10_001
        return observed

    gateway.market = market_after_clock_advance

    report = await runner.run()

    assert report.result is lifecycle.RunnerResult.BLOCKED
    assert report.reason == "MARKET_OBSERVATION_STALE"
    assert report.intent_count == 0
    assert report.dispatch_count == 0
    assert not gateway.dispatches
    store.close()


@pytest.mark.asyncio
async def test_fresh_prewrite_uses_post_read_clock_for_account(tmp_path):
    clock = [0]
    gateway = SyntheticGateway(clock)
    runner, store = make_runner(tmp_path, gateway)
    runner._market = market_at(clock[0])

    async def market_read_advances_clock(market_id):
        observed = market_at(clock[0])
        clock[0] += 1
        return observed

    gateway.market = market_read_advances_clock

    market, account = await runner._fresh_prewrite()

    assert market.observed_at_ms == 0
    assert account.observed_at_ms == 1
    store.close()


@pytest.mark.asyncio
async def test_close_pre_read_rejects_market_stale_after_market_read(tmp_path):
    clock = [0]
    gateway = SyntheticGateway(clock)
    runner, store = make_runner(tmp_path, gateway)
    runner._quantity = Decimal("1")
    runner._open_is_ask = True
    gateway.position = Decimal("-1")
    runner_market = market_at(clock[0])

    async def market_after_clock_advance(market_id):
        observed = market_at(clock[0])
        clock[0] += 10_001
        return observed

    gateway.market = market_after_clock_advance

    with pytest.raises(lifecycle.LifecycleHalt, match="MARKET_OBSERVATION_STALE"):
        await runner._close_phase(runner_market)

    assert not gateway.dispatches
    store.close()


@pytest.mark.asyncio
async def test_close_pre_read_uses_post_read_clock_for_account(tmp_path):
    clock = [0]
    gateway = SyntheticGateway(clock)
    runner, store = make_runner(tmp_path, gateway)
    runner._quantity = Decimal("1")
    runner._open_is_ask = True
    gateway.position = Decimal("-1")
    runner_market = market_at(clock[0])
    store.begin()

    async def market_read_advances_clock(market_id):
        observed = market_at(clock[0])
        clock[0] += 1
        return observed

    gateway.market = market_read_advances_clock

    await runner._close_phase(runner_market)

    assert [row[0] for row in gateway.dispatches] == ["CLOSE"]
    store.close()


@pytest.mark.asyncio
async def test_long_close_uses_exact_position_and_20_percent_sell_guard(tmp_path):
    clock = [0]
    gateway = SyntheticGateway(clock)
    observed = close_market_at(0, min_base_amount="3.00")
    gateway.market = lambda market_id: _async_market(observed, market_id)
    gateway.position = Decimal("2.69")
    runner, store = make_runner(tmp_path, gateway)
    runner._market = observed
    runner._quantity = Decimal("2.69")
    runner._open_is_ask = False
    store.begin()

    await runner._close_phase(observed)

    close_order = next(order for order in gateway.orders.values() if order.reduce_only)
    expected_bound = observed.best_bid * (Decimal(1) - lifecycle.LIGHTER_CLOSE_MAX_SLIPPAGE)
    assert close_order.quantity == Decimal("2.69")
    assert close_order.is_ask is True
    assert close_order.order_type == "market"
    assert close_order.time_in_force == "ioc"
    assert close_order.reduce_only is True
    assert close_order.price == expected_bound == Decimal("2.9828")
    assert close_order.price < observed.best_bid
    assert lifecycle._grid(close_order.price, observed.price_tick)
    store.close()


@pytest.mark.asyncio
async def test_short_close_uses_exact_position_and_20_percent_buy_guard(tmp_path):
    clock = [0]
    gateway = SyntheticGateway(clock)
    observed = close_market_at(0, min_base_amount="3.00")
    gateway.market = lambda market_id: _async_market(observed, market_id)
    gateway.position = Decimal("-2.69")
    runner, store = make_runner(tmp_path, gateway)
    runner._market = observed
    runner._quantity = Decimal("2.69")
    runner._open_is_ask = True
    store.begin()

    await runner._close_phase(observed)

    close_order = next(order for order in gateway.orders.values() if order.reduce_only)
    expected_bound = observed.best_ask * (Decimal(1) + lifecycle.LIGHTER_CLOSE_MAX_SLIPPAGE)
    assert close_order.quantity == Decimal("2.69")
    assert close_order.is_ask is False
    assert close_order.order_type == "market"
    assert close_order.time_in_force == "ioc"
    assert close_order.reduce_only is True
    assert close_order.price == expected_bound == Decimal("4.5600")
    assert close_order.price > observed.best_ask
    assert lifecycle._grid(close_order.price, observed.price_tick)
    store.close()


@pytest.mark.parametrize(
    ("is_ask", "best_bid", "best_ask", "expected"),
    [
        (True, "3.7284", "3.8000", Decimal("2.9827")),
        (False, "3.7285", "3.8001", Decimal("4.5602")),
    ],
)
def test_close_price_bound_rounds_directionally_to_price_tick(
    is_ask, best_bid, best_ask, expected
):
    market = close_market_at(0, best_bid=best_bid, best_ask=best_ask)
    reference = market.best_bid if is_ask else market.best_ask
    raw_bound = reference * (
        Decimal(1) - lifecycle.LIGHTER_CLOSE_MAX_SLIPPAGE
        if is_ask
        else Decimal(1) + lifecycle.LIGHTER_CLOSE_MAX_SLIPPAGE
    )

    bound = lifecycle.close_price_bound(
        market,
        quantity=Decimal("2.69"),
        is_ask=is_ask,
    )

    assert bound == expected
    assert lifecycle._grid(bound, market.price_tick)
    assert bound <= raw_bound if is_ask else bound >= raw_bound


async def _async_market(observed, market_id):
    assert market_id == observed.market_id
    return observed


@pytest.mark.parametrize(
    ("mutation", "is_ask", "reason"),
    [
        ("empty", True, "EXTERNAL_LIQUIDITY_INSUFFICIENT"),
        ("insufficient", True, "EXTERNAL_LIQUIDITY_INSUFFICIENT"),
        ("off_grid", True, "CLOSE_BOOK_LEVEL_OFF_GRID"),
        ("overflow", False, "CLOSE_PRICE_BOUND_INVALID"),
    ],
)
def test_close_price_bound_fails_closed_on_book_or_arithmetic_defects(
    mutation, is_ask, reason
):
    if mutation == "empty":
        market = replace(close_market_at(0), bids=())
    elif mutation == "insufficient":
        market = close_market_at(0, bid_quantity="2.68")
    elif mutation == "off_grid":
        market = close_market_at(0, best_bid="3.72845")
    else:
        market = close_market_at(0, best_ask="9e999999", price_decimals=0)

    with pytest.raises(lifecycle.LifecycleHalt, match=reason):
        lifecycle.close_price_bound(
            market,
            quantity=Decimal("2.69"),
            is_ask=is_ask,
        )


def test_close_price_bound_rejects_nonfinite_policy():
    market = close_market_at(0)
    original = lifecycle.LIGHTER_CLOSE_MAX_SLIPPAGE
    lifecycle.LIGHTER_CLOSE_MAX_SLIPPAGE = Decimal("NaN")
    try:
        with pytest.raises(lifecycle.LifecycleHalt, match="CLOSE_SLIPPAGE_POLICY_INVALID"):
            lifecycle.close_price_bound(
                market,
                quantity=Decimal("2.69"),
                is_ask=True,
            )
    finally:
        lifecycle.LIGHTER_CLOSE_MAX_SLIPPAGE = original


def test_smallest_executable_quantity_remains_the_open_path_contract():
    market = market_at(0)

    assert lifecycle.smallest_executable_quantity(market, is_ask=False) == (
        Decimal("1"),
        Decimal("10.10"),
    )
    assert lifecycle.smallest_executable_quantity(market, is_ask=True) == (
        Decimal("1"),
        Decimal("10.00"),
    )


@pytest.mark.parametrize(
    ("quantity", "is_ask", "reason"),
    [
        (Decimal("0"), True, "CLOSE_QUANTITY_INVALID"),
        (Decimal("NaN"), True, "CLOSE_QUANTITY_INVALID"),
        (Decimal("2.691"), True, "CLOSE_QUANTITY_OFF_GRID"),
        (Decimal("2.69"), 1, "CLOSE_SIDE_INVALID"),
    ],
)
def test_close_price_bound_rejects_invalid_quantity_or_side(quantity, is_ask, reason):
    with pytest.raises(lifecycle.LifecycleHalt, match=reason):
        lifecycle.close_price_bound(
            close_market_at(0),
            quantity=quantity,
            is_ask=is_ask,
        )


async def run_synthetic(
    tmp_path: Path,
    *,
    funding_mode: str = "good",
    disagree_terminal: bool = False,
    mode: lifecycle.RunMode = lifecycle.RunMode.TESTNET_WRITE,
    ambiguous_kind: lifecycle.IntentKind | None = None,
    response_mismatch_kind: lifecycle.IntentKind | None = None,
    drop_after_accept_kind: lifecycle.IntentKind | None = None,
    public_order_nonces: dict[lifecycle.IntentKind, int] | None = None,
    maker_cancel_status: str = "CANCELLED",
    maker_cancel_status_sequence: tuple[str, ...] | None = None,
    maker_cancel_unrelated_order: bool = False,
    maker_cancel_mismatch: bool = False,
    maker_cancel_fill: bool = False,
) -> tuple[lifecycle.RunnerReport, SyntheticGateway, lifecycle.LifecycleStore]:
    clock = [0]
    gateway = SyntheticGateway(
        clock,
        funding_mode=funding_mode,
        disagree_terminal=disagree_terminal,
        ambiguous_kind=ambiguous_kind,
        response_mismatch_kind=response_mismatch_kind,
        drop_after_accept_kind=drop_after_accept_kind,
        public_order_nonces=public_order_nonces,
        maker_cancel_status=maker_cancel_status,
        maker_cancel_status_sequence=maker_cancel_status_sequence,
        maker_cancel_unrelated_order=maker_cancel_unrelated_order,
        maker_cancel_mismatch=maker_cancel_mismatch,
        maker_cancel_fill=maker_cancel_fill,
    )
    runner, store = make_runner(tmp_path, gateway, mode=mode)
    report = await runner.run()
    return report, gateway, store


def write_protected(path: Path, content: str) -> None:
    path.parent.mkdir(mode=0o700)
    path.parent.chmod(0o700)
    path.write_text(content, encoding="utf-8")
    path.chmod(0o600)


def test_identity_schema_tolerates_additive_public_fields(tmp_path, monkeypatch):
    path = tmp_path / "lighter-testnet" / "identity.json"
    payload = {
        "l1_address": ADDRESS,
        "account_index": ACCOUNT_INDEX,
        "api_key_index": API_KEY_INDEX,
        "api_key_public_key": PUBLIC_KEY,
        "sdk_commit": lifecycle.LIGHTER_SDK_COMMIT,
        "future_read_only_metadata": {"observed": True},
    }
    write_protected(path, json.dumps(payload))
    monkeypatch.setattr(lifecycle, "LIGHTER_IDENTITY_PATH", path)
    loaded = lifecycle.load_identity_metadata(path)
    assert loaded == identity()


def test_identity_schema_accepts_actual_api_key_public_field_and_additive_paths(tmp_path, monkeypatch):
    path = tmp_path / "lighter-testnet" / "identity.json"
    payload = {
        "l1_address": ADDRESS,
        "account_index": ACCOUNT_INDEX,
        "api_key_index": API_KEY_INDEX,
        "api_key_public": PUBLIC_KEY,
        "sdk_commit": lifecycle.LIGHTER_SDK_COMMIT,
        "api_key_path": "/protected/lighter-testnet/api-key-4-private",
        "identity_path": "/protected/lighter-testnet/identity.json",
        "future_read_only_metadata": {"observed": True},
    }
    write_protected(path, json.dumps(payload))
    monkeypatch.setattr(lifecycle, "LIGHTER_IDENTITY_PATH", path)

    loaded = lifecycle.load_identity_metadata(path)

    assert loaded == identity()


def test_identity_schema_rejects_contradictory_public_key_aliases(tmp_path, monkeypatch):
    path = tmp_path / "lighter-testnet" / "identity.json"
    payload = {
        "l1_address": ADDRESS,
        "account_index": ACCOUNT_INDEX,
        "api_key_index": API_KEY_INDEX,
        "api_key_public": PUBLIC_KEY,
        "api_key_public_key": "0x" + "2" * 80,
        "sdk_commit": lifecycle.LIGHTER_SDK_COMMIT,
    }
    write_protected(path, json.dumps(payload))
    monkeypatch.setattr(lifecycle, "LIGHTER_IDENTITY_PATH", path)

    with pytest.raises(lifecycle.LifecycleHalt, match="IDENTITY_FILE_API_PUBLIC_CONTRADICTORY"):
        lifecycle.load_identity_metadata(path)


@pytest.mark.parametrize(
    "change",
    [
        {"l1_address": "0x" + "22" * 20},
        {"account_index": 203},
        {"api_key_index": 255},
        {"api_key_public_key": "0x11"},
    ],
)
def test_identity_and_api_key_width_fail_closed(change):
    values = {
        "l1_address": ADDRESS,
        "account_index": ACCOUNT_INDEX,
        "api_key_index": API_KEY_INDEX,
        "api_key_public_key": PUBLIC_KEY,
        "sdk_commit": lifecycle.LIGHTER_SDK_COMMIT,
    }
    values.update(change)
    with pytest.raises(lifecycle.LifecycleHalt):
        lifecycle.IdentityMetadata(**values)


def test_secret_material_is_not_persistable(tmp_path):
    store = lifecycle.LifecycleStore(tmp_path / "journal.sqlite")
    with pytest.raises(lifecycle.LifecycleHalt, match="SENSITIVE_FIELD_NOT_PERSISTABLE"):
        store.record_evidence("TEST", {"private_key": "synthetic-secret"}, now_ms=0)
    store.close()
    assert b"synthetic-secret" not in (tmp_path / "journal.sqlite").read_bytes()


@pytest.mark.parametrize("mode", [lifecycle.RunMode.DRY_RUN, lifecycle.RunMode.PREPARE_ONLY])
async def test_prepare_controls_never_dispatch(mode, tmp_path):
    report, gateway, store = await run_synthetic(tmp_path, mode=mode)
    assert report.result is lifecycle.RunnerResult.PREPARED
    assert report.intent_count == 0
    assert report.dispatch_count == 0
    assert not gateway.dispatches
    assert gateway.terminal_calls == 0
    store.close()


async def test_complete_ordered_lifecycle_rediscovery_intents_and_terminal_rounds(tmp_path):
    report, gateway, store = await run_synthetic(tmp_path, funding_mode="good")
    assert report.result is lifecycle.RunnerResult.COMPLETE
    assert report.market_id == MARKET_ID
    assert report.quantity == Decimal("1")
    assert report.open_is_ask is True
    assert report.funding_status == "AUTHORITATIVE"
    assert report.funding_change == Decimal("0.25")
    assert report.intent_count == 4
    assert report.dispatch_count == 4
    assert report.terminal_rounds == 2
    assert report.sqlite_integrity == "ok"
    assert gateway.discover_calls == 1
    assert gateway.market_calls and set(gateway.market_calls) == {MARKET_ID}
    assert all(account == ACCOUNT_INDEX and key == API_KEY_INDEX for account, key in gateway.nonce_calls)
    assert [row[0] for row in gateway.dispatches] == ["MAKER_PLACE", "MAKER_CANCEL", "OPEN", "CLOSE"]
    assert all(row[2] == API_KEY_INDEX for row in gateway.dispatches)
    assert all(row[3] is False for row in gateway.dispatches[:3])
    assert gateway.dispatches[-1][3] is True
    assert store.all_intents_reconciled()
    store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("maker_cancel_status", ["canceled", "CANCELLED", "EXPIRED"])
async def test_maker_cancel_accepts_official_and_legacy_terminal_statuses(
    tmp_path, maker_cancel_status
):
    report, gateway, store = await run_synthetic(
        tmp_path,
        maker_cancel_status=maker_cancel_status,
    )

    assert report.result is lifecycle.RunnerResult.COMPLETE
    assert report.intent_count == 4
    assert report.dispatch_count == 4
    assert [row[0] for row in gateway.dispatches] == [
        "MAKER_PLACE",
        "MAKER_CANCEL",
        "OPEN",
        "CLOSE",
    ]
    maker_order = next(row for row in gateway.orders.values() if row.order_type == "limit")
    assert maker_order.status == maker_cancel_status
    assert maker_order.remaining_quantity == 0
    assert maker_order.filled_quantity == 0
    store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("maker_cancel_status", ["OPEN", "FILLED", "UNKNOWN"])
async def test_maker_cancel_rejects_active_filled_and_unknown_statuses(
    tmp_path, maker_cancel_status
):
    report, gateway, store = await run_synthetic(
        tmp_path,
        maker_cancel_status=maker_cancel_status,
    )

    assert report.result is lifecycle.RunnerResult.HALTED_MANUAL_RECOVERY
    assert report.failure_class == "TRANSPORT"
    assert report.reason == "POST_SEND_RECONCILIATION_UNRESOLVED"
    assert [row[0] for row in gateway.dispatches] == ["MAKER_PLACE", "MAKER_CANCEL"]
    assert report.intent_count == 2
    assert report.dispatch_count == 2
    store.close()


async def test_maker_cancel_polls_observed_active_then_canceled_once(tmp_path):
    report, gateway, store = await run_synthetic(
        tmp_path,
        maker_cancel_status_sequence=("OPEN", "OPEN", "CANCELLED"),
    )

    assert report.result is lifecycle.RunnerResult.COMPLETE
    assert report.intent_count == 4
    assert report.dispatch_count == 4
    assert gateway.cancel_calls == 1
    assert [status.upper() for status in gateway.cancel_snapshot_statuses[:3]] == [
        "OPEN",
        "OPEN",
        "CANCELLED",
    ]
    assert gateway.sleep_calls[:2] == [1.0, 1.0]
    assert store.all_intents_reconciled()
    store.close()


async def test_maker_cancel_perpetual_active_exhausts_without_replay(tmp_path):
    report, gateway, store = await run_synthetic(tmp_path, maker_cancel_status="OPEN")

    assert report.result is lifecycle.RunnerResult.HALTED_MANUAL_RECOVERY
    assert report.failure_class == "TRANSPORT"
    assert report.reason == "POST_SEND_RECONCILIATION_UNRESOLVED"
    assert report.intent_count == 2
    assert report.dispatch_count == 2
    assert gateway.cancel_calls == 1
    assert gateway.sleep_calls == [1.0] * lifecycle.MAKER_CANCEL_RECONCILIATION_MAX_POLLS

    restarted = lifecycle.LighterLevelCRunner(
        gateway,
        store,
        readiness=ready_readiness(),
        identity=identity(),
        mode=lifecycle.RunMode.TESTNET_WRITE,
        clock_ms=lambda: gateway.clock[0],
        sleep=gateway.sleep_until_boundary,
    )
    second = await restarted.run()

    assert second.result is lifecycle.RunnerResult.BLOCKED
    assert second.reason == "RESTART_REQUIRES_FRESH_DATABASE"
    assert gateway.cancel_calls == 1
    assert [row[0] for row in gateway.dispatches] == [
        "MAKER_PLACE",
        "MAKER_CANCEL",
    ]
    store.close()


async def test_maker_cancel_rejects_fill_evidence_even_with_zero_terminal_order(tmp_path):
    report, gateway, store = await run_synthetic(tmp_path, maker_cancel_fill=True)

    assert report.result is lifecycle.RunnerResult.HALTED_MANUAL_RECOVERY
    assert report.failure_class == "TRANSPORT"
    assert report.reason == "POST_SEND_RECONCILIATION_UNRESOLVED"
    assert gateway.cancel_calls == 1
    assert [row[0] for row in gateway.dispatches] == ["MAKER_PLACE", "MAKER_CANCEL"]
    assert gateway.sleep_calls == []
    store.close()


@pytest.mark.parametrize(
    ("failure", "gateway_kwargs"),
    [
        ("filled", {"maker_cancel_status": "FILLED"}),
        ("unrelated", {"maker_cancel_unrelated_order": True}),
        ("mismatch", {"maker_cancel_mismatch": True}),
        ("unknown", {"maker_cancel_status": "UNKNOWN"}),
    ],
)
async def test_maker_cancel_contradictions_fail_without_poll(
    tmp_path, failure, gateway_kwargs
):
    report, gateway, store = await run_synthetic(tmp_path, **gateway_kwargs)

    assert report.result is lifecycle.RunnerResult.HALTED_MANUAL_RECOVERY, failure
    assert report.failure_class == "TRANSPORT"
    assert report.reason == "POST_SEND_RECONCILIATION_UNRESOLVED"
    assert report.intent_count == 2
    assert report.dispatch_count == 2
    assert gateway.cancel_calls == 1
    assert gateway.sleep_calls == []
    store.close()


async def test_generated_maker_open_close_client_indexes_fit_lighter_uint48(tmp_path):
    report, _, store = await run_synthetic(tmp_path, funding_mode="good")

    assert report.result is lifecycle.RunnerResult.COMPLETE
    rows = store._connection.execute(
        "SELECT kind, client_order_index FROM intents "
        "WHERE kind IN ('MAKER_PLACE', 'OPEN', 'CLOSE') ORDER BY rowid"
    ).fetchall()
    assert [row[0] for row in rows] == ["MAKER_PLACE", "OPEN", "CLOSE"]
    assert all(
        0 < row[1] <= lifecycle.LIGHTER_MAX_CLIENT_ORDER_INDEX for row in rows
    )
    store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind",
    [
        lifecycle.IntentKind.MAKER_PLACE,
        lifecycle.IntentKind.OPEN,
        lifecycle.IntentKind.CLOSE,
    ],
)
async def test_public_order_nonce_is_distinct_from_transaction_nonce_for_reconciliation(
    tmp_path, kind
):
    public_order_nonce = 3_629_417
    report, gateway, store = await run_synthetic(
        tmp_path,
        public_order_nonces={kind: public_order_nonce},
    )

    assert report.result is lifecycle.RunnerResult.COMPLETE
    assert report.intent_count == 4
    assert report.dispatch_count == 4
    persisted = store._connection.execute(
        "SELECT kind, nonce FROM intents ORDER BY rowid"
    ).fetchall()
    assert [(row[0], row[1]) for row in persisted] == [
        ("MAKER_PLACE", 100),
        ("MAKER_CANCEL", 101),
        ("OPEN", 102),
        ("CLOSE", 103),
    ]
    assert [row[1] for row in gateway.dispatches] == [100, 101, 102, 103]
    if kind is lifecycle.IntentKind.MAKER_PLACE:
        order = next(row for row in gateway.orders.values() if row.order_type == "limit")
    elif kind is lifecycle.IntentKind.OPEN:
        order = next(
            row
            for row in gateway.orders.values()
            if row.order_type == "market" and not row.reduce_only
        )
    else:
        order = next(row for row in gateway.orders.values() if row.reduce_only)
    assert order.nonce == public_order_nonce
    assert order.nonce not in {100, 101, 102, 103}
    store.close()


def _order_reconciliation_fixture():
    request = lifecycle.OrderRequest(
        market_id=MARKET_ID,
        client_order_index=123,
        quantity=Decimal("1"),
        price=Decimal("10"),
        is_ask=True,
        order_type="market",
        time_in_force="ioc",
        reduce_only=False,
        order_expiry=0,
        size_decimals=0,
        price_decimals=2,
    )
    intent = lifecycle.IntentSpec(
        "open-reconciliation",
        lifecycle.IntentKind.OPEN,
        request,
        None,
        5,
    )
    order = lifecycle.OrderSnapshot(
        order_index=700,
        client_order_index=request.client_order_index,
        account_index=ACCOUNT_INDEX,
        market_id=MARKET_ID,
        quantity=request.quantity,
        remaining_quantity=Decimal("0"),
        filled_quantity=request.quantity,
        filled_quote=Decimal("10"),
        price=request.price,
        is_ask=request.is_ask,
        order_type=request.order_type,
        time_in_force=request.time_in_force,
        reduce_only=request.reduce_only,
        nonce=3_629_417,
        status="FILLED",
    )
    snapshot = lifecycle.AccountSnapshot(
        account_index=ACCOUNT_INDEX,
        l1_address=ADDRESS,
        collateral=Decimal("1000"),
        maker_fee_tick=0,
        taker_fee_tick=0,
        orders=(order,),
        positions=(),
        fills=(),
        funding_high_water=0,
        unrelated_state_clear=True,
        observed_at_ms=0,
    )
    return request, intent, order, snapshot


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("client_order_index", 124),
        ("account_index", ACCOUNT_INDEX + 1),
        ("market_id", MARKET_ID + 1),
        ("quantity", Decimal("2")),
        ("price", Decimal("10.1")),
        ("is_ask", False),
        ("order_type", "limit"),
        ("time_in_force", "post_only"),
        ("reduce_only", True),
    ],
)
def test_find_order_rejects_every_non_nonce_order_field_mismatch(field, value):
    _, intent, order, snapshot = _order_reconciliation_fixture()
    mismatched = replace(order, **{field: value})

    with pytest.raises(lifecycle.LifecycleHalt) as exc_info:
        lifecycle._find_order(replace(snapshot, orders=(mismatched,)), intent)

    assert exc_info.value.reason in {
        "EXACT_ORDER_RECONCILIATION_FAILED",
        "EXACT_ORDER_FIELDS_MISMATCH",
    }


def test_find_order_requires_a_unique_client_order_index_match():
    _, intent, order, snapshot = _order_reconciliation_fixture()
    duplicate = replace(order, order_index=701)

    with pytest.raises(
        lifecycle.LifecycleHalt, match="EXACT_ORDER_RECONCILIATION_FAILED"
    ):
        lifecycle._find_order(replace(snapshot, orders=(order, duplicate)), intent)


@pytest.mark.parametrize(
    ("funding_mode", "change"),
    [("negative", Decimal("-0.25")), ("zero", Decimal("0"))],
)
async def test_authoritative_negative_and_zero_funding_are_not_rewritten(tmp_path, funding_mode, change):
    report, _, store = await run_synthetic(tmp_path, funding_mode=funding_mode)
    assert report.result is lifecycle.RunnerResult.COMPLETE
    assert report.funding_status == "AUTHORITATIVE"
    assert report.funding_change == change
    assert report.terminal_rounds == 2
    store.close()


@pytest.mark.parametrize("funding_mode", ["missing", "incomplete", "contradictory"])
async def test_missing_or_contradictory_funding_blocks_but_close_remains_available(tmp_path, funding_mode):
    report, gateway, store = await run_synthetic(tmp_path, funding_mode=funding_mode)
    assert report.result is lifecycle.RunnerResult.BLOCKED
    assert report.funding_status == "BLOCKED"
    assert report.failure_class == "SAFETY"
    assert report.intent_count == 4
    assert report.dispatch_count == 4
    assert gateway.dispatches[-1][0] == "CLOSE"
    assert report.terminal_rounds == 2
    store.close()


async def test_unrelated_state_blocks_before_first_send(tmp_path):
    clock = [0]
    gateway = SyntheticGateway(clock)
    runner, store = make_runner(tmp_path, gateway)
    original = gateway._account
    gateway._account = lambda: replace(original(), unrelated_state_clear=False)
    report = await runner.run()
    assert report.result is lifecycle.RunnerResult.BLOCKED
    assert report.reason == "UNRELATED_ACCOUNT_STATE"
    assert not gateway.dispatches
    assert report.dispatch_count == 0
    store.close()


async def test_trigger_order_is_part_of_zero_order_gate(tmp_path):
    clock = [0]
    gateway = SyntheticGateway(clock)
    gateway.orders[700] = lifecycle.OrderSnapshot(
        order_index=700,
        client_order_index=700,
        account_index=ACCOUNT_INDEX,
        market_id=MARKET_ID,
        quantity=Decimal("1"),
        remaining_quantity=Decimal("1"),
        filled_quantity=Decimal("0"),
        filled_quote=Decimal("0"),
        price=Decimal("10"),
        is_ask=True,
        order_type="limit",
        time_in_force="post_only",
        reduce_only=False,
        nonce=4,
        status="OPEN",
        trigger=True,
    )
    runner, store = make_runner(tmp_path, gateway)
    report = await runner.run()
    assert report.result is lifecycle.RunnerResult.BLOCKED
    assert report.reason == "ACTIVE_ORDERS_PRESENT"
    assert not gateway.dispatches
    store.close()


async def test_ambiguous_send_is_one_shot_and_restart_does_not_replay(tmp_path):
    clock = [0]
    gateway = SyntheticGateway(clock, ambiguous_kind=lifecycle.IntentKind.MAKER_PLACE)
    runner, store = make_runner(tmp_path, gateway)
    first = await runner.run()
    assert first.result is lifecycle.RunnerResult.HALTED_MANUAL_RECOVERY
    assert first.reason == "SYNTHETIC_AMBIGUOUS_SEND"
    assert first.intent_count == 1
    assert first.dispatch_count == 1
    assert not gateway.dispatches

    gateway.store = store
    restarted = lifecycle.LighterLevelCRunner(
        gateway,
        store,
        readiness=ready_readiness(),
        identity=identity(),
        mode=lifecycle.RunMode.TESTNET_WRITE,
        clock_ms=lambda: clock[0],
        sleep=gateway.sleep_until_boundary,
    )
    second = await restarted.run()
    assert second.result is lifecycle.RunnerResult.BLOCKED
    assert second.reason == "RESTART_REQUIRES_FRESH_DATABASE"
    assert not gateway.dispatches
    store.close()
    with pytest.raises(lifecycle.LifecycleHalt, match="FRESH_DATABASE_PATH_MUST_NOT_EXIST"):
        lifecycle.LifecycleStore(tmp_path / "lighter-level-c.sqlite")


async def test_ambiguous_close_is_one_shot_and_restart_does_not_replay(tmp_path):
    clock = [0]
    gateway = SyntheticGateway(clock, ambiguous_kind=lifecycle.IntentKind.CLOSE)
    runner, store = make_runner(tmp_path, gateway)
    report = await runner.run()

    assert report.result is lifecycle.RunnerResult.HALTED_MANUAL_RECOVERY
    assert report.reason == "SYNTHETIC_AMBIGUOUS_SEND"
    assert report.intent_count == 4
    assert report.dispatch_count == 4
    assert [row[0] for row in gateway.dispatches] == [
        "MAKER_PLACE",
        "MAKER_CANCEL",
        "OPEN",
    ]
    row = store._connection.execute(
        "SELECT state FROM intents WHERE kind = 'CLOSE'"
    ).fetchone()
    assert row[0] == lifecycle.IntentState.AMBIGUOUS.value

    restarted = lifecycle.LighterLevelCRunner(
        gateway,
        store,
        readiness=ready_readiness(),
        identity=identity(),
        mode=lifecycle.RunMode.TESTNET_WRITE,
        clock_ms=lambda: clock[0],
        sleep=gateway.sleep_until_boundary,
    )
    second = await restarted.run()
    assert second.result is lifecycle.RunnerResult.BLOCKED
    assert second.reason == "RESTART_REQUIRES_FRESH_DATABASE"
    assert [row[0] for row in gateway.dispatches] == [
        "MAKER_PLACE",
        "MAKER_CANCEL",
        "OPEN",
    ]
    store.close()


async def test_terminal_round_disagreement_blocks_after_close(tmp_path):
    report, gateway, store = await run_synthetic(tmp_path, disagree_terminal=True)
    assert report.result is lifecycle.RunnerResult.BLOCKED
    assert report.reason == "TERMINAL_ROUNDS_DISAGREE"
    assert gateway.dispatches[-1][0] == "CLOSE"
    assert report.terminal_rounds == 0
    store.close()


@pytest.mark.asyncio
async def test_sdk_dispatch_accepts_observed_stale_parsed_transaction_fields():
    class FakeSigner:
        def __init__(self):
            self.create_kwargs = None
            self.cancel_kwargs = None

        async def create_order(self, **kwargs):
            self.create_kwargs = kwargs
            return (
                SimpleNamespace(
                    account_index=None,
                    order_book_index=None,
                    base_amount=None,
                    price=None,
                    is_ask=None,
                    order_type=None,
                    nonce=None,
                ),
                SimpleNamespace(code=200, tx_hash="21" * 40),
                None,
            )

        async def cancel_order(self, **kwargs):
            self.cancel_kwargs = kwargs
            return (
                SimpleNamespace(
                    account_index=None,
                    order_book_index=None,
                    order_nonce=None,
                    nonce=None,
                ),
                SimpleNamespace(code=200, tx_hash="22" * 40),
                None,
            )

    async def no_market():
        raise AssertionError("not a dispatch read")

    async def no_market_by_id(_market_id):
        raise AssertionError("not a dispatch read")

    async def no_snapshot(_market_id):
        raise AssertionError("not a dispatch read")

    async def no_funding(_market_id, _account_index, _baseline):
        raise AssertionError("not a dispatch read")

    signer = FakeSigner()
    gateway = lifecycle.SdkLighterGateway(
        signer,
        market_discoverer=no_market,
        market_reader=no_market_by_id,
        snapshot_reader=no_snapshot,
        funding_reader=no_funding,
    )
    request = lifecycle.OrderRequest(
        market_id=MARKET_ID,
        client_order_index=123,
        quantity=Decimal("1"),
        price=Decimal("10.10"),
        is_ask=True,
        order_type="limit",
        time_in_force="post_only",
        reduce_only=False,
        order_expiry=-1,
        size_decimals=0,
        price_decimals=2,
    )
    created = await gateway.create_order(request, nonce=33, api_key_index=API_KEY_INDEX)
    cancelled = await gateway.cancel_order(
        MARKET_ID,
        777,
        nonce=34,
        api_key_index=API_KEY_INDEX,
    )
    assert created.accepted and cancelled.accepted
    assert created.tx_hash == "21" * 40
    assert cancelled.tx_hash == "22" * 40
    assert signer.create_kwargs["api_key_index"] == API_KEY_INDEX
    assert signer.create_kwargs["nonce"] == 33
    assert signer.create_kwargs["skip_nonce"] == 0
    assert signer.cancel_kwargs["api_key_index"] == API_KEY_INDEX
    assert signer.cancel_kwargs["nonce"] == 34
    assert signer.cancel_kwargs["skip_nonce"] == 0
    with pytest.raises(lifecycle.LifecycleHalt, match="EXPLICIT_KEY_AND_NONCE_REQUIRED"):
        await gateway.cancel_order(MARKET_ID, 777, nonce=35, api_key_index=255)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["create", "cancel"])
async def test_sdk_none_parsed_transaction_is_rejected(operation):
    class FakeSigner:
        def __init__(self):
            self.calls = 0

        async def create_order(self, **_kwargs):
            self.calls += 1
            return None, SimpleNamespace(code=200, tx_hash="23" * 40), None

        async def cancel_order(self, **_kwargs):
            self.calls += 1
            return None, SimpleNamespace(code=200, tx_hash="24" * 40), None

    async def unused(*_args, **_kwargs):
        raise AssertionError("not a dispatch read")

    signer = FakeSigner()
    gateway = lifecycle.SdkLighterGateway(
        signer,
        market_discoverer=unused,
        market_reader=unused,
        snapshot_reader=unused,
        funding_reader=unused,
    )
    if operation == "create":
        outcome = await gateway.create_order(
            lifecycle.OrderRequest(
                market_id=MARKET_ID,
                client_order_index=123,
                quantity=Decimal("1"),
                price=Decimal("10.10"),
                is_ask=True,
                order_type="limit",
                time_in_force="post_only",
                reduce_only=False,
                order_expiry=-1,
                size_decimals=0,
                price_decimals=2,
            ),
            nonce=35,
            api_key_index=API_KEY_INDEX,
        )
    else:
        outcome = await gateway.cancel_order(
            MARKET_ID,
            777,
            nonce=36,
            api_key_index=API_KEY_INDEX,
        )

    assert not outcome.accepted
    assert outcome.rejected
    assert outcome.error_class == "SDK_REJECTED"
    assert signer.calls == 1


@pytest.mark.asyncio
async def test_sdk_stale_parsed_fields_then_authoritative_create_and_cancel_reconcile(tmp_path):
    clock = [0]
    store = lifecycle.LifecycleStore(tmp_path / "lighter-sdk-authoritative.sqlite")
    state: dict[str, lifecycle.OrderSnapshot | None] = {"order": None}

    def account() -> lifecycle.AccountSnapshot:
        order = state["order"]
        return lifecycle.AccountSnapshot(
            account_index=ACCOUNT_INDEX,
            l1_address=ADDRESS,
            collateral=Decimal("1000"),
            maker_fee_tick=0,
            taker_fee_tick=0,
            orders=() if order is None else (order,),
            positions=(lifecycle.PositionSnapshot(ACCOUNT_INDEX, MARKET_ID, Decimal("0")),),
            fills=(),
            funding_high_water=0,
            unrelated_state_clear=True,
            observed_at_ms=clock[0],
        )

    class FakeSigner:
        def __init__(self):
            self.create_calls = 0
            self.cancel_calls = 0

        async def create_order(self, **kwargs):
            self.create_calls += 1
            assert store.intent_count() == 1
            assert store.dispatch_count() == 1
            quantity = Decimal(kwargs["base_amount"])
            price = Decimal(kwargs["price"]).scaleb(-2)
            state["order"] = lifecycle.OrderSnapshot(
                order_index=700,
                client_order_index=kwargs["client_order_index"],
                account_index=ACCOUNT_INDEX,
                market_id=kwargs["market_index"],
                quantity=quantity,
                remaining_quantity=quantity,
                filled_quantity=Decimal("0"),
                filled_quote=Decimal("0"),
                price=price,
                is_ask=kwargs["is_ask"],
                order_type="limit",
                time_in_force="post_only",
                reduce_only=False,
                nonce=kwargs["nonce"],
                status="OPEN",
            )
            return (
                SimpleNamespace(
                    account_index=None,
                    order_book_index=None,
                    base_amount=None,
                    price=None,
                    is_ask=None,
                    order_type=None,
                    nonce=None,
                ),
                SimpleNamespace(code=200, tx_hash="31" * 40),
                None,
            )

        async def cancel_order(self, **kwargs):
            self.cancel_calls += 1
            assert store.intent_count() == 2
            assert store.dispatch_count() == 2
            order = state["order"]
            assert order is not None
            state["order"] = replace(
                order,
                remaining_quantity=Decimal("0"),
                status="CANCELLED",
            )
            return (
                SimpleNamespace(
                    account_index=None,
                    order_book_index=None,
                    order_nonce=None,
                    nonce=None,
                ),
                SimpleNamespace(code=200, tx_hash="32" * 40),
                None,
            )

    async def discover_market():
        raise AssertionError("maker reconciliation does not rediscover a market")

    async def read_market(market_id):
        assert market_id == MARKET_ID
        return market_at(clock[0])

    async def read_snapshot(
        market_id, *, client_order_index=None, post_cancel_target=None
    ):
        assert market_id == MARKET_ID
        return account()

    async def read_funding(*_args, **_kwargs):
        raise AssertionError("funding is outside this regression")

    next_nonce = [100]

    async def read_nonce(*_args):
        value = next_nonce[0]
        next_nonce[0] += 1
        return value

    signer = FakeSigner()
    gateway = lifecycle.SdkLighterGateway(
        signer,
        market_discoverer=discover_market,
        market_reader=read_market,
        snapshot_reader=read_snapshot,
        funding_reader=read_funding,
        nonce_reader=read_nonce,
    )
    runner = lifecycle.LighterLevelCRunner(
        gateway,
        store,
        readiness=ready_readiness(),
        identity=identity(),
        mode=lifecycle.RunMode.TESTNET_WRITE,
        clock_ms=lambda: clock[0],
    )
    runner._market = market_at(clock[0])
    runner._quantity = Decimal("1")
    runner._open_is_ask = True
    store.begin()

    await runner._maker_phase(runner._market, account())

    assert signer.create_calls == 1
    assert signer.cancel_calls == 1
    assert store.dispatch_count() == 2
    assert store.all_intents_reconciled()
    assert state["order"] is not None
    assert state["order"].status == "CANCELLED"
    store.close()


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("0" * 80, "0" * 80),
        ("A" * 80, "a" * 80),
        ("0123456789abcdef" * 5, "0123456789abcdef" * 5),
    ],
)
def test_lighter_transaction_hash_validator_accepts_exact_80_hex(value, expected):
    assert lifecycle._safe_lighter_transaction_hash(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        None,
        123,
        b"a" * 80,
        "",
        "a" * 79,
        "a" * 81,
        "0x" + "a" * 64,
        "0x" + "a" * 80,
        "0X" + "a" * 78,
        "a" * 64,
        "a" * 40 + "g" + "a" * 39,
        "a" * 40 + "0x" + "a" * 38,
        "a" * 79 + "\n",
    ],
)
def test_lighter_transaction_hash_validator_rejects_previous_and_invalid_shapes(value):
    assert lifecycle._safe_lighter_transaction_hash(value) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response_code", "error", "tx_hash"),
    [
        (400, None, LIGHTER_TRANSACTION_HASH),
        (200, "sdk-error", LIGHTER_TRANSACTION_HASH),
        (200, None, ""),
        (200, None, "0x" + "41" * 32),
        (200, None, "41" * 32),
        (200, None, "0x" + "41" * 40),
        (200, None, "41" * 39),
        (200, None, "41" * 41),
        (200, None, "41" * 40 + "g"),
        (200, None, "41" * 40 + "0x"),
        (200, None, None),
    ],
)
async def test_sdk_rejection_or_unsafe_hash_blocks_without_replay(
    tmp_path, response_code, error, tx_hash
):
    clock = [0]
    store = lifecycle.LifecycleStore(tmp_path / "lighter-sdk-rejection.sqlite")

    class FakeSigner:
        def __init__(self):
            self.create_calls = 0

        async def create_order(self, **_kwargs):
            self.create_calls += 1
            return (
                SimpleNamespace(
                    account_index=None,
                    order_book_index=None,
                    base_amount=None,
                    price=None,
                    is_ask=None,
                    order_type=None,
                    nonce=None,
                ),
                SimpleNamespace(code=response_code, tx_hash=tx_hash),
                error,
            )

    def empty_account() -> lifecycle.AccountSnapshot:
        return lifecycle.AccountSnapshot(
            account_index=ACCOUNT_INDEX,
            l1_address=ADDRESS,
            collateral=Decimal("1000"),
            maker_fee_tick=0,
            taker_fee_tick=0,
            orders=(),
            positions=(lifecycle.PositionSnapshot(ACCOUNT_INDEX, MARKET_ID, Decimal("0")),),
            fills=(),
            funding_high_water=0,
            unrelated_state_clear=True,
            observed_at_ms=clock[0],
        )

    async def discover_market():
        return market_at(clock[0])

    async def read_market(market_id):
        assert market_id == MARKET_ID
        return market_at(clock[0])

    async def read_snapshot(
        market_id, *, client_order_index=None, post_cancel_target=None
    ):
        assert market_id == MARKET_ID
        assert client_order_index is None
        return empty_account()

    async def read_funding(*_args, **_kwargs):
        raise AssertionError("rejected dispatch must not enter funding")

    async def read_nonce(*_args):
        return 200

    signer = FakeSigner()
    gateway = lifecycle.SdkLighterGateway(
        signer,
        market_discoverer=discover_market,
        market_reader=read_market,
        snapshot_reader=read_snapshot,
        funding_reader=read_funding,
        nonce_reader=read_nonce,
    )

    def new_runner() -> lifecycle.LighterLevelCRunner:
        return lifecycle.LighterLevelCRunner(
            gateway,
            store,
            readiness=ready_readiness(),
            identity=identity(),
            mode=lifecycle.RunMode.TESTNET_WRITE,
            clock_ms=lambda: clock[0],
        )

    first = await new_runner().run()

    assert first.result is lifecycle.RunnerResult.BLOCKED
    assert first.failure_class == "HTTP"
    assert first.reason == "SDK_REJECTED"
    assert first.intent_count == 1
    assert first.dispatch_count == 1
    assert signer.create_calls == 1

    second = await new_runner().run()

    assert second.result is lifecycle.RunnerResult.BLOCKED
    assert second.reason == "RESTART_REQUIRES_FRESH_DATABASE"
    assert signer.create_calls == 1
    assert store.intent_state(
        store._connection.execute("SELECT intent_id FROM intents").fetchone()[0]
    ) is lifecycle.IntentState.REJECTED
    store.close()


@pytest.mark.asyncio
async def test_sdk_gateway_close_is_awaited_once():
    class FakeSigner:
        def __init__(self):
            self.close_calls = 0

        async def close(self):
            self.close_calls += 1

    async def unused(*_args, **_kwargs):
        return None

    signer = FakeSigner()
    gateway = lifecycle.SdkLighterGateway(
        signer,
        market_discoverer=unused,
        market_reader=unused,
        snapshot_reader=unused,
        funding_reader=unused,
    )

    await gateway.close()
    await gateway.close()

    assert signer.close_calls == 1


@pytest.mark.asyncio
async def test_sdk_factory_closes_client_when_reader_setup_fails(monkeypatch):
    class FakeSigner:
        def __init__(self):
            self.api_client = object()
            self.close_calls = 0

        async def close(self):
            self.close_calls += 1

    signer = FakeSigner()

    def failing_account_api(_api_client):
        raise RuntimeError("synthetic reader setup failure")

    fake_lighter = SimpleNamespace(
        nonce_manager=SimpleNamespace(
            NonceManagerType=SimpleNamespace(NONE=object())
        ),
        SignerClient=lambda *_args, **_kwargs: signer,
        AccountApi=failing_account_api,
    )
    monkeypatch.setattr(lifecycle, "load_identity_metadata", lambda _path: identity())
    monkeypatch.setattr(
        lifecycle,
        "load_api_key_private",
        lambda _path: "synthetic-only-private",
    )
    monkeypatch.setattr(lifecycle.importlib, "import_module", lambda _name: fake_lighter)

    with pytest.raises(lifecycle.LifecycleHalt, match="LIGHTER_SDK_READ_API_UNAVAILABLE"):
        await lifecycle.SdkLighterGateway.from_protected_files()

    assert signer.close_calls == 1


def test_sdk_debug_records_with_transaction_data_are_suppressed():
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    root = logging.getLogger()
    old_level = root.level
    root.setLevel(logging.DEBUG)
    root.addHandler(handler)
    try:
        with lifecycle.suppress_sdk_secret_debug():
            logging.getLogger("lighter").debug("Create Order TxInfo: PRIVATE_MARKER")
            logging.getLogger("lighter").debug("safe market observation")
    finally:
        root.removeHandler(handler)
        root.setLevel(old_level)
    output = stream.getvalue()
    assert "PRIVATE_MARKER" not in output
    assert "safe market observation" in output


def test_post_only_and_reduce_only_vectors_are_closed_world():
    with pytest.raises(lifecycle.LifecycleHalt, match="MAKER_MUST_BE_POST_ONLY"):
        lifecycle.OrderRequest(
            MARKET_ID, 1, Decimal("1"), Decimal("10"), False, "limit", "ioc", False, -1, 0, 0
        )
    with pytest.raises(lifecycle.LifecycleHalt, match="REDUCE_ONLY_CLOSE_MUST_BE_MARKET"):
        lifecycle.OrderRequest(
            MARKET_ID, 1, Decimal("1"), Decimal("10"), False, "limit", "post_only", True, -1, 0, 0
        )


@pytest.mark.parametrize(
    "client_order_index",
    [1, lifecycle.LIGHTER_MAX_CLIENT_ORDER_INDEX],
)
def test_client_order_index_accepts_positive_uint48_boundary(client_order_index):
    request = lifecycle.OrderRequest(
        MARKET_ID,
        client_order_index,
        Decimal("1"),
        Decimal("10"),
        True,
        "market",
        "ioc",
        False,
        0,
        0,
        0,
    )
    assert request.client_order_index == client_order_index


@pytest.mark.asyncio
async def test_oversized_client_order_index_is_rejected_before_intent_dispatch(
    tmp_path, monkeypatch
):
    gateway = SyntheticGateway([0])
    runner, store = make_runner(tmp_path, gateway)
    runner._market = market_at(0)
    runner._quantity = Decimal("1")
    runner._open_is_ask = True
    monkeypatch.setattr(
        lifecycle,
        "_new_client_order_index",
        lambda: lifecycle.LIGHTER_MAX_CLIENT_ORDER_INDEX + 1,
    )

    with pytest.raises(lifecycle.LifecycleHalt, match="ORDER_ID_INVALID"):
        await runner._maker_phase(runner._market, gateway._account())

    assert store.intent_count() == 0
    assert store.dispatch_count() == 0
    assert gateway.dispatches == []
    store.close()


@pytest.mark.parametrize(
    ("quantity", "price", "size_decimals", "price_decimals", "base_wire", "price_wire"),
    [
        (Decimal("2.70"), Decimal("3.7169"), 2, 4, 270, 37169),
        (Decimal("0.1"), Decimal("4050"), 4, 2, 1000, 405000),
    ],
)
def test_order_wire_units_use_observed_market_precision(
    quantity, price, size_decimals, price_decimals, base_wire, price_wire
):
    request = lifecycle.OrderRequest(
        MARKET_ID,
        1,
        quantity,
        price,
        True,
        "market",
        "ioc",
        False,
        0,
        size_decimals,
        price_decimals,
    )
    assert lifecycle._wire_units(request.quantity, request.size_decimals, "BASE_AMOUNT") == base_wire
    assert lifecycle._wire_units(request.price, request.price_decimals, "PRICE") == price_wire


@pytest.mark.parametrize(
    ("value", "decimals"),
    [
        (True, 2),
        (Decimal("NaN"), 2),
        (Decimal("2.701"), 2),
        (Decimal("92233720368.55"), 8),
    ],
)
def test_order_wire_units_reject_bool_nonfinite_off_grid_and_overflow(value, decimals):
    with pytest.raises(lifecycle.LifecycleHalt):
        lifecycle._wire_units(value, decimals, "BASE_AMOUNT")


@pytest.mark.parametrize("kind", [lifecycle.IntentKind.MAKER_PLACE, lifecycle.IntentKind.OPEN])
async def test_post_claim_sdk_mismatch_is_durable_manual_recovery(tmp_path, kind):
    report, gateway, store = await run_synthetic(
        tmp_path,
        response_mismatch_kind=kind,
    )
    assert report.result is lifecycle.RunnerResult.HALTED_MANUAL_RECOVERY
    assert report.reason == "POST_CLAIM_WRITE_UNRESOLVED"
    expected_dispatches = {lifecycle.IntentKind.MAKER_PLACE: 1, lifecycle.IntentKind.OPEN: 3}[kind]
    assert report.dispatch_count == expected_dispatches
    assert gateway.dispatches[-1][0] == kind.value
    row = store._connection.execute(
        "SELECT intent_id, state FROM intents ORDER BY rowid DESC LIMIT 1"
    ).fetchone()
    assert row[1] == lifecycle.IntentState.AMBIGUOUS.value
    assert store.intent_state(row[0]) is lifecycle.IntentState.AMBIGUOUS
    store.close()


async def test_accepted_send_order_not_found_is_manual_recovery(tmp_path):
    report, gateway, store = await run_synthetic(
        tmp_path,
        drop_after_accept_kind=lifecycle.IntentKind.MAKER_PLACE,
    )
    assert report.result is lifecycle.RunnerResult.HALTED_MANUAL_RECOVERY
    assert report.reason == "POST_SEND_RECONCILIATION_UNRESOLVED"
    assert report.dispatch_count == 1
    assert gateway.dispatches[0][0] == "MAKER_PLACE"
    row = store._connection.execute("SELECT intent_id, state FROM intents").fetchone()
    assert row[1] == lifecycle.IntentState.AMBIGUOUS.value
    store.close()


async def test_terminal_rounds_must_be_fresh_and_ordered(tmp_path):
    clock = [0]
    gateway = SyntheticGateway(clock)
    runner, store = make_runner(tmp_path, gateway)
    original = gateway.terminal_round

    async def same_timestamp(market_id):
        value = await original(market_id)
        clock[0] -= 1
        return replace(value, observed_at_ms=value.observed_at_ms - 1)

    gateway.terminal_round = same_timestamp
    report = await runner.run()
    assert report.result is lifecycle.RunnerResult.BLOCKED
    assert report.reason == "TERMINAL_ROUNDS_DISAGREE"
    assert report.terminal_rounds == 0
    store.close()


def test_lifecycle_store_requires_fresh_secure_database(tmp_path, monkeypatch):
    parent = tmp_path / "private"
    parent.mkdir(mode=0o700)
    parent.chmod(0o700)
    existing = parent / "existing.sqlite"
    existing.write_bytes(b"not-a-database")
    existing.chmod(0o600)
    with pytest.raises(lifecycle.LifecycleHalt, match="FRESH_DATABASE_PATH_MUST_NOT_EXIST"):
        lifecycle.LifecycleStore(existing)

    linked = parent / "linked.sqlite"
    os.link(existing, linked)
    with pytest.raises(lifecycle.LifecycleHalt, match="FRESH_DATABASE_PATH_MUST_NOT_EXIST"):
        lifecycle.LifecycleStore(linked)

    symlink = parent / "symlink.sqlite"
    symlink.symlink_to(existing)
    with pytest.raises(lifecycle.LifecycleHalt, match="FRESH_DATABASE_PATH_MUST_NOT_EXIST"):
        lifecycle.LifecycleStore(symlink)

    chmod_failure = parent / "chmod-failure.sqlite"
    original_fchmod = lifecycle.os.fchmod

    def fail_fchmod(*args):
        raise OSError("synthetic chmod failure")

    monkeypatch.setattr(lifecycle.os, "fchmod", fail_fchmod)
    with pytest.raises(lifecycle.LifecycleHalt, match="FRESH_DATABASE_PERMISSION_FAILURE"):
        lifecycle.LifecycleStore(chmod_failure)
    monkeypatch.setattr(lifecycle.os, "fchmod", original_fchmod)
    assert not chmod_failure.exists()


def test_lifecycle_store_rejects_non_owner_only_parent(tmp_path):
    parent = tmp_path / "shared"
    parent.mkdir(mode=0o755)
    parent.chmod(0o755)
    with pytest.raises(lifecycle.LifecycleHalt, match="FRESH_DATABASE_PARENT_INVALID"):
        lifecycle.LifecycleStore(parent / "journal.sqlite")


def _sdk_ok(data):
    return data, SimpleNamespace(status=200), None


def official_order_row(
    *,
    order_index: int = 700,
    client_order_index: int = 701,
    status: str = "open",
    remaining_base_amount: str = "1",
    filled_base_amount: str = "0",
    filled_quote_amount: str = "0",
    price: str = "3.8000",
    nonce: int = 7,
    trigger_price: str = "0",
    trigger_status: str = "na",
):
    return SimpleNamespace(
        order_index=order_index,
        client_order_index=client_order_index,
        owner_account_index=ACCOUNT_INDEX,
        market_index=MARKET_ID,
        initial_base_amount="1",
        remaining_base_amount=remaining_base_amount,
        filled_base_amount=filled_base_amount,
        filled_quote_amount=filled_quote_amount,
        price=price,
        is_ask=True,
        type="limit",
        time_in_force="post-only",
        reduce_only=False,
        nonce=nonce,
        status=status,
        trigger_price=trigger_price,
        trigger_status=trigger_status,
    )


class FakeOfficialApis:
    def __init__(self):
        self.detail = SimpleNamespace(
            symbol="LIT",
            market_id=MARKET_ID,
            market_type="perp",
            min_base_amount="1",
            min_quote_amount="10",
            supported_quote_decimals=6,
            size_decimals=2,
            price_decimals=4,
            status="active",
            market_config=SimpleNamespace(force_reduce_only=False),
        )
        self.book = SimpleNamespace(
            total_bids=1,
            bids=[SimpleNamespace(price="3.7000", remaining_base_amount="10")],
            total_asks=1,
            asks=[SimpleNamespace(price="3.8000", remaining_base_amount="10")],
        )
        self.account_value = SimpleNamespace(
            account_index=ACCOUNT_INDEX,
            index=ACCOUNT_INDEX,
            l1_address=ADDRESS,
            collateral="1000",
            positions=[],
            assets=[
                SimpleNamespace(
                    symbol="ETH",
                    asset_id=1,
                    balance="3",
                    locked_balance="0",
                    margin_balance="0",
                    margin_mode="disabled",
                    multiplier="1",
                ),
                SimpleNamespace(
                    symbol="LIT",
                    asset_id=2,
                    balance="1000000",
                    locked_balance="0",
                    margin_balance="0",
                    margin_mode="disabled",
                    multiplier="1",
                ),
                SimpleNamespace(
                    symbol="USDC",
                    asset_id=3,
                    balance="0",
                    locked_balance="0",
                    margin_balance="10000",
                    margin_mode="enabled",
                    multiplier="1",
                ),
            ],
            pool_info=None,
            shares=[],
            pending_unlocks=[],
            approved_integrators=[],
        )
        self.limits = SimpleNamespace(current_maker_fee_tick=0, current_taker_fee_tick=0)
        self.active_orders = []
        self.target_orders = []
        self.account_pages = {}
        self.trade_pages = {}
        self.funding_pages = {}
        self.position_funding_rows = []
        self.loop_accounts = False
        self.loop_funding = False
        self.loop_trades = False

    async def order_book_details(self, **kwargs):
        return _sdk_ok(SimpleNamespace(order_book_details=[self.detail]))

    async def order_book_orders(self, **kwargs):
        return _sdk_ok(self.book)

    async def funding_rates(self, **kwargs):
        return _sdk_ok(
            SimpleNamespace(
                funding_rates=[
                    SimpleNamespace(market_id=MARKET_ID, exchange="lighter", symbol="LIT", rate="0.001")
                ]
            )
        )

    async def account(self, **kwargs):
        cursor = kwargs.get("cursor")
        if self.loop_accounts:
            return _sdk_ok(SimpleNamespace(accounts=[self.account_value], next_cursor="loop"))
        return _sdk_ok(SimpleNamespace(accounts=[self.account_value], next_cursor=""))

    async def account_limits(self, **kwargs):
        return _sdk_ok(self.limits)

    async def account_active_orders(self, **kwargs):
        return _sdk_ok(SimpleNamespace(orders=self.active_orders))

    async def account_orders(self, **kwargs):
        return _sdk_ok(SimpleNamespace(orders=self.target_orders))

    async def trades(self, **kwargs):
        if self.loop_trades:
            return _sdk_ok(SimpleNamespace(trades=[], next_cursor="loop"))
        return _sdk_ok(SimpleNamespace(trades=[], next_cursor=""))

    async def position_funding(self, **kwargs):
        if self.loop_funding:
            return _sdk_ok(SimpleNamespace(position_fundings=[], next_cursor="loop"))
        return _sdk_ok(
            SimpleNamespace(position_fundings=self.position_funding_rows, next_cursor="")
        )

    async def apikeys(self, **kwargs):
        return _sdk_ok(
            SimpleNamespace(
                api_keys=[
                    SimpleNamespace(
                        account_index=ACCOUNT_INDEX,
                        api_key_index=API_KEY_INDEX,
                        public_key=PUBLIC_KEY,
                    )
                ]
            )
        )

    async def next_nonce(self, **kwargs):
        return _sdk_ok(SimpleNamespace(nonce=55))


def make_read_adapter(apis=None, *, clock=None, authorization_provider=None):
    apis = apis or FakeOfficialApis()
    clock = [0] if clock is None else clock
    return lifecycle.OfficialSdkReadAdapter(
        identity=identity(),
        authorization="synthetic-auth-token",
        authorization_provider=authorization_provider,
        account_api=apis,
        order_api=apis,
        funding_api=apis,
        transaction_api=apis,
        clock_ms=lambda: clock[0],
        sleep=lambda _delay: _advance_clock(clock),
        monotonic=lambda: 0.0,
    )


async def _advance_clock(clock):
    clock[0] += 1


async def test_official_sdk_reader_builds_complete_public_snapshots_and_terminal_round():
    apis = FakeOfficialApis()
    clock = [0]
    adapter = make_read_adapter(apis, clock=clock)
    market = await adapter.discover_market()
    snapshot = await adapter.snapshot(MARKET_ID)
    result = await adapter.readiness()
    terminal = await adapter.terminal_round(MARKET_ID)
    assert market.contract.size_decimals == 2
    assert market.contract.price_decimals == 4
    assert snapshot.collateral == Decimal("1000")
    assert snapshot.asset_count == 3
    assert result.status == "READY"
    assert result.api_key_public_key_verified is True
    assert terminal.active_regular_orders == 0
    assert terminal.active_trigger_orders == 0
    assert terminal.signed_position == 0
    assert "synthetic-auth-token" not in json.dumps(result.evidence())


async def test_official_reader_allows_only_bound_post_cancel_open_to_terminal_transition():
    apis = FakeOfficialApis()
    apis.active_orders = [official_order_row(status="open")]
    apis.target_orders = [
        official_order_row(
            status="canceled",
            remaining_base_amount="0",
            filled_base_amount="0",
            filled_quote_amount="0",
        )
    ]
    adapter = make_read_adapter(apis)

    snapshot = await adapter.snapshot(
        MARKET_ID,
        client_order_index=701,
        post_cancel_target=(700, 701),
    )

    assert len(snapshot.orders) == 1
    assert snapshot.orders[0].status == "canceled"
    assert snapshot.orders[0].remaining_quantity == 0
    assert snapshot.orders[0].filled_quantity == 0
    assert snapshot.orders[0].filled_quote == 0
    assert snapshot.active_regular_orders == ()


async def test_official_reader_rejects_contradictory_duplicate_outside_bound_transition():
    apis = FakeOfficialApis()
    apis.active_orders = [official_order_row(price="3.8000")]
    apis.target_orders = [official_order_row(price="3.8100")]
    adapter = make_read_adapter(apis)

    with pytest.raises(
        lifecycle.LifecycleHalt,
        match="DUPLICATE_ORDER_CONTRADICTION",
    ) as exc_info:
        await adapter.snapshot(
            MARKET_ID,
            client_order_index=701,
            post_cancel_target=(999, 701),
        )

    assert exc_info.value.failure_class == "SAFETY"


async def test_official_reader_rejects_bound_post_cancel_target_field_mismatch():
    apis = FakeOfficialApis()
    apis.active_orders = [official_order_row(price="3.8000")]
    apis.target_orders = [
        official_order_row(
            status="canceled",
            remaining_base_amount="0",
            filled_base_amount="0",
            filled_quote_amount="0",
            price="3.8100",
        )
    ]
    adapter = make_read_adapter(apis)

    with pytest.raises(
        lifecycle.LifecycleHalt,
        match="DUPLICATE_ORDER_CONTRADICTION",
    ):
        await adapter.snapshot(
            MARKET_ID,
            client_order_index=701,
            post_cancel_target=(700, 701),
        )


@pytest.mark.parametrize(
    "target",
    [
        (700, 702),
        (701, 701),
    ],
)
async def test_official_reader_rejects_post_cancel_target_binding_mismatch(target):
    apis = FakeOfficialApis()
    apis.active_orders = [official_order_row(status="open")]
    apis.target_orders = [
        official_order_row(
            status="canceled",
            remaining_base_amount="0",
            filled_base_amount="0",
            filled_quote_amount="0",
        )
    ]
    adapter = make_read_adapter(apis)

    with pytest.raises(
        lifecycle.LifecycleHalt,
        match="POST_CANCEL_TARGET_BINDING_INVALID|DUPLICATE_ORDER_CONTRADICTION",
    ):
        await adapter.snapshot(
            MARKET_ID,
            client_order_index=701,
            post_cancel_target=target,
        )


async def test_official_reader_parses_pinned_plural_position_fundings_field():
    apis = FakeOfficialApis()
    apis.position_funding_rows = [
        SimpleNamespace(
            timestamp=HOUR,
            market_id=MARKET_ID,
            funding_id=77,
            change="-0.25",
            discount="0",
            rate="0.001",
            position_size="1",
            position_side="short",
        )
    ]
    adapter = make_read_adapter(apis)

    history = await adapter.funding_history(
        MARKET_ID,
        account_index=ACCOUNT_INDEX,
        baseline_high_water=0,
    )

    assert history.complete is True
    assert history.high_water == 77
    assert history.records == (
        lifecycle.FundingRecord(
            77,
            MARKET_ID,
            HOUR,
            Decimal("-0.25"),
            Decimal("0.001"),
            Decimal("1"),
            "short",
        ),
    )


async def test_official_reader_trigger_orders_fail_the_zero_order_gate():
    apis = FakeOfficialApis()
    apis.active_orders = [
        SimpleNamespace(
            order_index=700,
            client_order_index=701,
            owner_account_index=ACCOUNT_INDEX,
            market_index=MARKET_ID,
            initial_base_amount="1",
            remaining_base_amount="1",
            filled_base_amount="0",
            filled_quote_amount="0",
            price="3.8000",
            is_ask=True,
            type="limit",
            time_in_force="post-only",
            reduce_only=False,
            nonce=7,
            status="open",
            trigger_price="3.9000",
            trigger_status="na",
        )
    ]
    adapter = make_read_adapter(apis)
    snapshot = await adapter.snapshot(MARKET_ID)
    assert len(snapshot.active_trigger_orders) == 1
    result = await adapter.readiness()
    assert result.status == "BLOCKED"
    assert result.reason == "ACTIVE_ORDERS_PRESENT"


@pytest.mark.parametrize("nonce", [True, -1, "3"])
async def test_official_reader_keeps_public_order_nonce_schema_validation(nonce):
    apis = FakeOfficialApis()
    apis.active_orders = [
        SimpleNamespace(
            order_index=700,
            client_order_index=701,
            owner_account_index=ACCOUNT_INDEX,
            market_index=MARKET_ID,
            initial_base_amount="1",
            remaining_base_amount="1",
            filled_base_amount="0",
            filled_quote_amount="0",
            price="3.8000",
            is_ask=True,
            type="limit",
            time_in_force="post-only",
            reduce_only=False,
            nonce=nonce,
            status="open",
            trigger_price="0",
            trigger_status="na",
        )
    ]
    adapter = make_read_adapter(apis)

    with pytest.raises(lifecycle.LifecycleHalt, match="ORDER_NONCE_INVALID"):
        await adapter.snapshot(MARKET_ID)


@pytest.mark.parametrize("kind", ["accounts", "trades", "funding"])
async def test_official_reader_incomplete_pagination_fails_closed(kind):
    apis = FakeOfficialApis()
    setattr(apis, {"accounts": "loop_accounts", "trades": "loop_trades", "funding": "loop_funding"}[kind], True)
    adapter = make_read_adapter(apis)
    with pytest.raises(lifecycle.LifecycleHalt, match="PAGINATION"):
        if kind == "accounts":
            await adapter._account()
        elif kind == "trades":
            await adapter._fills()
        else:
            await adapter.funding_history(
                MARKET_ID,
                account_index=ACCOUNT_INDEX,
                baseline_high_water=0,
            )


async def test_official_reader_missing_order_fields_fail_closed():
    apis = FakeOfficialApis()
    apis.active_orders = [SimpleNamespace(order_index=1)]
    adapter = make_read_adapter(apis)
    with pytest.raises(lifecycle.LifecycleHalt, match="ORDER_.*MISSING"):
        await adapter.snapshot(MARKET_ID)


async def test_official_reader_uses_pinned_account_limits_fee_tick_fields():
    apis = FakeOfficialApis()
    apis.limits = SimpleNamespace(current_maker_fee=0, current_taker_fee=0)
    adapter = make_read_adapter(apis)
    with pytest.raises(lifecycle.LifecycleHalt, match="MAKER_FEE_TICK_MISSING"):
        await adapter.snapshot(MARKET_ID)


async def test_official_reader_accepts_legal_post_trade_asset_balance_change():
    apis = FakeOfficialApis()
    apis.account_value.assets[2].margin_balance = "10000.016409"
    adapter = make_read_adapter(apis)

    snapshot = await adapter.snapshot(MARKET_ID)

    assert snapshot.asset_count == 3


async def test_official_reader_accepts_exact_original_asset_baseline():
    adapter = make_read_adapter(FakeOfficialApis())

    snapshot = await adapter.snapshot(MARKET_ID)

    assert snapshot.asset_count == 3


@pytest.mark.parametrize(
    ("mutation", "reason", "failure_class"),
    [
        ("missing", "BOOTSTRAP_ASSET_BASELINE_MISMATCH", "SAFETY"),
        ("unknown", "UNKNOWN_BOOTSTRAP_ASSET", "SAFETY"),
        ("duplicate", "DUPLICATE_ACCOUNT_ASSET", "SAFETY"),
        ("wrong_id", "ACCOUNT_ASSET_ID_MISMATCH", "IDENTITY"),
        ("wrong_mode", "ACCOUNT_ASSET_MARGIN_MODE_MISMATCH", "SAFETY"),
        ("locked", "ASSET_LOCKED_BALANCE_UNEXPECTED", "SAFETY"),
        ("margin_liability", "ASSET_MARGIN_LIABILITY_PRESENT", "SAFETY"),
        ("negative_balance", "ASSET_BALANCE_NEGATIVE", "SAFETY"),
        ("nonfinite_balance", "ASSET_BALANCE_INVALID", "SCHEMA"),
    ],
)
async def test_official_reader_rejects_structurally_unsafe_asset_state(
    mutation, reason, failure_class
):
    apis = FakeOfficialApis()
    if mutation == "missing":
        apis.account_value.assets.pop(1)
    elif mutation == "unknown":
        apis.account_value.assets.append(
            SimpleNamespace(
                symbol="OTHER",
                asset_id=99,
                balance="0",
                locked_balance="0",
                margin_balance="0",
                margin_mode="disabled",
                multiplier="1",
            )
        )
    elif mutation == "duplicate":
        apis.account_value.assets.append(apis.account_value.assets[0])
    elif mutation == "wrong_id":
        apis.account_value.assets[0].asset_id = 99
    elif mutation == "wrong_mode":
        apis.account_value.assets[1].margin_mode = "enabled"
    elif mutation == "locked":
        apis.account_value.assets[0].locked_balance = "1"
    elif mutation == "margin_liability":
        apis.account_value.assets[0].margin_balance = "1"
    elif mutation == "negative_balance":
        apis.account_value.assets[1].balance = "-1"
    else:
        apis.account_value.assets[1].balance = "NaN"
    adapter = make_read_adapter(apis)
    with pytest.raises(lifecycle.LifecycleHalt, match=reason) as exc_info:
        await adapter.snapshot(MARKET_ID)
    assert exc_info.value.failure_class == failure_class


@pytest.mark.parametrize(
    ("field_name", "value", "reason", "failure_class"),
    [
        ("balance", "-1", "ASSET_BALANCE_NEGATIVE", "SAFETY"),
        ("locked_balance", "-1", "ASSET_LOCKED_BALANCE_NEGATIVE", "SAFETY"),
        ("margin_balance", "-1", "ASSET_MARGIN_BALANCE_NEGATIVE", "SAFETY"),
        ("locked_balance", "NaN", "ASSET_LOCKED_BALANCE_INVALID", "SCHEMA"),
        ("margin_balance", "Infinity", "ASSET_MARGIN_BALANCE_INVALID", "SCHEMA"),
        ("multiplier", "0", "ASSET_MULTIPLIER_INVALID", "SCHEMA"),
    ],
)
async def test_official_reader_rejects_nonfinite_negative_or_unsafe_asset_fields(
    field_name, value, reason, failure_class
):
    apis = FakeOfficialApis()
    setattr(apis.account_value.assets[1], field_name, value)
    adapter = make_read_adapter(apis)

    with pytest.raises(lifecycle.LifecycleHalt, match=reason) as exc_info:
        await adapter.snapshot(MARKET_ID)

    assert exc_info.value.failure_class == failure_class


async def test_official_reader_type_checks_account_l1_before_lower():
    apis = FakeOfficialApis()
    apis.account_value.l1_address = 202
    adapter = make_read_adapter(apis)
    with pytest.raises(lifecycle.LifecycleHalt, match="ACCOUNT_L1_INVALID"):
        await adapter._account()


async def test_authenticated_private_reads_refresh_in_memory_auth_token_after_expiry():
    apis = FakeOfficialApis()
    seen_tokens = []
    clock = [0]

    def refresh():
        token = f"synthetic-auth-{clock[0]}"
        seen_tokens.append(token)
        return token

    for method_name in ("account_limits", "account_active_orders", "trades", "position_funding"):
        original = getattr(apis, method_name)

        async def capture(*args, _original=original, **kwargs):
            apis.seen_authorizations.append(kwargs.get("authorization"))
            return await _original(*args, **kwargs)

        setattr(apis, method_name, capture)
    apis.seen_authorizations = []
    adapter = make_read_adapter(apis, clock=clock, authorization_provider=refresh)
    await adapter._active_orders()
    clock[0] = 601
    await adapter._active_orders()
    assert seen_tokens == ["synthetic-auth-0", "synthetic-auth-601"]
    assert apis.seen_authorizations == seen_tokens


@pytest.mark.parametrize("mode", [lifecycle.RunMode.DRY_RUN, lifecycle.RunMode.PREPARE_ONLY])
async def test_isolated_entry_modes_never_dispatch(tmp_path, monkeypatch, mode):
    gateway = SyntheticGateway([0])
    monkeypatch.setattr(lifecycle.time, "time", lambda: 0)

    async def fake_readiness():
        return ready_readiness()

    gateway.readiness = fake_readiness
    monkeypatch.setattr(lifecycle, "load_identity_metadata", lambda _path: identity())
    monkeypatch.setattr(
        lifecycle.SdkLighterGateway,
        "from_protected_files",
        lambda **_kwargs: gateway,
    )
    report = await lifecycle.run_isolated_lighter_testnet_level_c(
        db_path=tmp_path / f"{mode.value}.sqlite",
        mode=mode,
    )
    assert report.result is lifecycle.RunnerResult.PREPARED
    assert gateway.dispatches == []
    assert gateway.close_calls == 1


@pytest.mark.asyncio
async def test_isolated_entry_closes_gateway_and_preserves_primary_failure(tmp_path, monkeypatch):
    gateway = SyntheticGateway([0])
    gateway.close_error = RuntimeError("synthetic close failure")

    async def failing_readiness():
        raise RuntimeError("synthetic readiness failure")

    gateway.readiness = failing_readiness
    monkeypatch.setattr(lifecycle, "load_identity_metadata", lambda _path: identity())
    monkeypatch.setattr(
        lifecycle.SdkLighterGateway,
        "from_protected_files",
        lambda **_kwargs: gateway,
    )

    with pytest.raises(RuntimeError, match="synthetic readiness failure"):
        await lifecycle.run_isolated_lighter_testnet_level_c(
            db_path=tmp_path / "primary-failure.sqlite",
        )

    assert gateway.close_calls == 1
