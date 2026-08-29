from __future__ import annotations

import asyncio
import copy
from dataclasses import replace
from decimal import Decimal
import json
import os
from pathlib import Path
import ssl

import aiohttp
import pytest

from risex_farmer.testnet_risex_two_account_coordinator import (
    AccountRole,
    AccountSnapshot,
    BookLevel,
    BookObservation,
    CANCEL_PATH,
    COUNTERPARTY_ACCOUNT as FIXED_COUNTERPARTY_ACCOUNT,
    COUNTERPARTY_REGISTRATION_EXPIRATION,
    COUNTERPARTY_SIGNER as FIXED_COUNTERPARTY_SIGNER,
    COUNTERPARTY_SIGNER_MARKER,
    CoordinatorSafetyError,
    CoordinatorResult,
    DurableIntent,
    DurableCancel,
    FixedRisexTwoAccountTransport,
    FixedRisexTwoAccountVenue,
    HISTORY_PATH,
    MAX_AGE_SECONDS,
    MARKET_STEP,
    MarketObservation,
    NonceState,
    OrderHistoryPropagationMismatch,
    Phase,
    PortfolioState,
    ORDER_LOOKUP_PATH_TEMPLATE,
    PAGE_LIMIT,
    PORTFOLIO_PATH,
    PLACE_PATH,
    PROPAGATION_SETTLE_SECONDS,
    PrivateEventEvidence,
    RestOrder,
    RestTrade,
    RoleIdentity,
    TRADES_PATH,
    TwoAccountCoordinator,
    VenueObservation,
    WriteResult,
    _HTTPObservation,
    _paged_rows,
    _parse_open_orders,
    _parse_order_row,
    _parse_portfolio,
    _parse_trade_row,
    _require_recent,
    _all_orders,
    _atomic_position_size,
    _decode_counterparty_secret,
    _normalize_position_map,
    _order_for,
    _point_position,
    _position_maps_agree,
    _position_rows,
    _portfolio_position_rows,
    _response_data,
    _is_monotonic_order_visibility,
    _MAX_PORTFOLIO_POSITION_SIZE,
    _validate_order,
    _validate_unsigned_cancel,
    _validate_unsigned_place,
    unsigned_cancel_request,
    unsigned_place_request,
)
from risex_farmer.testnet_risex_private_read_preflight import (
    ACCOUNT,
    AUTHORIZATION,
    ROUTER,
    SIGNER,
)


NOW = 1_800_000_000
COUNTERPARTY = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
COUNTERPARTY_SIGNER = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


def private_events(account: str, position: str = "0", order_count: int = 0):
    return PrivateEventEvidence(
        account=account,
        auth_status="success",
        orders_snapshot=(),
        positions_snapshot=((2, Decimal(position)),) if position != "0" else (),
        orders_updates=(),
        positions_updates=(),
        observed_at=NOW,
    )


def account_snapshot(
    role: AccountRole,
    *,
    position: str = "0",
    orders: tuple[RestOrder, ...] = (),
    trades: tuple[RestTrade, ...] = (),
    source: str = "REST",
):
    account, signer = (
        (ACCOUNT, SIGNER)
        if role is AccountRole.PRIMARY
        else (COUNTERPARTY, COUNTERPARTY_SIGNER)
    )
    return AccountSnapshot(
        role=role,
        account=account,
        signer=signer,
        signer_status="ACTIVE",
        position=Decimal(position),
        open_orders=orders,
        trades=trades,
        private=private_events(account, position, len(orders)),
        portfolio=PortfolioState(
            account=account, usdc_balance=Decimal("1000"),
            free_collateral=Decimal("1000"), total_account_value=Decimal("1000"),
            in_liquidation=False, risk_level="NORMAL", observed_at=NOW,
        ),
        observed_at=NOW,
        source=source,
    )


def market(book: BookObservation | None = None):
    if book is None:
        book = BookObservation(
            bid=Decimal("2999.00"),
            ask=Decimal("3000.00"),
            bids=(BookLevel(Decimal("2999.00"), Decimal("1"), 1),),
            asks=(BookLevel(Decimal("3000.00"), Decimal("1"), 1),),
            observed_at=NOW,
        )
    return MarketObservation(
        host="api.testnet.rise.trade",
        chain_id=11155931,
        domain_name="RISEx",
        domain_version="1",
        router=ROUTER,
        authorization=AUTHORIZATION,
        market_id=2,
        symbol="ETH/USDC",
        active=True,
        unlocked=True,
        tick=Decimal("0.01"),
        step=Decimal("0.001"),
        minimum=Decimal("0.1"),
        observed_at=NOW,
        book=book,
    )


def observation(
    primary: AccountSnapshot | None = None,
    counterparty: AccountSnapshot | None = None,
    *,
    book: BookObservation | None = None,
    rest_round: int = 0,
):
    return VenueObservation(
        market=market(book),
        accounts={
            AccountRole.PRIMARY: primary or account_snapshot(AccountRole.PRIMARY),
            AccountRole.COUNTERPARTY: counterparty or account_snapshot(AccountRole.COUNTERPARTY),
        },
        rest_round=rest_round,
    )


class FakeVenue:
    def __init__(self):
        self.place_calls = []
        self.cancel_calls = []
        self.observations = [observation()]
        self.rest_rounds = []

    async def observe(self):
        return self.observations.pop(0)

    async def rest_round(self):
        return self.rest_rounds.pop(0)

    async def place(self, role, request):
        self.place_calls.append((role, request))
        return WriteResult.accepted(order_id=f"0x{len(self.place_calls):048x}")

    async def cancel(self, role, request):
        self.cancel_calls.append((role, request))
        return WriteResult.accepted()


def test_fixed_market_and_unique_inside_prices_are_tick_aligned():
    from risex_farmer.testnet_risex_two_account_coordinator import (
        maker_price,
    )

    current = market()
    assert maker_price(current, "SELL") == Decimal("2999.01")
    assert maker_price(current, "BUY") == Decimal("2999.99")


def test_coordinator_rejects_missing_private_event_evidence(tmp_path: Path):
    venue = FakeVenue()
    broken = account_snapshot(AccountRole.PRIMARY)
    broken = replace(broken, private=None)
    venue.observations = [observation(primary=broken)]
    coordinator = TwoAccountCoordinator._fixture(
        venue=venue,
        primary_journal=tmp_path / "primary.sqlite3",
        counterparty_journal=tmp_path / "counterparty.sqlite3",
        now=lambda: NOW,
    )
    report = asyncio.run(coordinator.run())
    assert report.result is CoordinatorResult.BLOCKED_BEFORE_WRITE
    assert not venue.place_calls


def order(
    *,
    wide: int,
    client: int,
    account: str,
    side: str,
    order_type: str,
    tif: str,
    status: str,
    price: str,
    filled: str = "0",
    post_only: bool = False,
    reduce_only: bool = False,
):
    order_id = f"0x{wide:016x}{1:016x}{1:016x}"
    return RestOrder(
        order_id=order_id,
        wide_order_id=wide,
        resting_order_id=wide >> 1,
        client_order_id=client,
        market_id=2,
        account=account,
        side=side,
        order_type=order_type,
        time_in_force=tif,
        status=status,
        size=Decimal("0.1"),
        filled_size=Decimal(filled),
        price=Decimal("0") if order_type == "MARKET" else Decimal(price),
        post_only=post_only,
        reduce_only=reduce_only,
        observed_at=NOW,
    )


def trade(*, trade_id: str, item: RestOrder, side: str, price: str):
    return RestTrade(
        trade_id=trade_id,
        order_id=item.order_id,
        client_order_id=item.client_order_id,
        market_id=2,
        account=item.account,
        side=side,
        size=Decimal("0.1"),
        price=Decimal(price),
        observed_at=NOW,
    )


def _unsigned_place_fixture():
    identity = _primary_identity()
    intent = DurableIntent(
        intent_id="intent-place",
        ordinal=1,
        step="ENTRY_TAKER",
        client_order_id=4013,
        nonce_anchor=1,
        nonce_bitmap=0,
        payload_digest="payload",
        bbo_digest="b" * 64,
        state="PREPARED",
        side="BUY",
        order_type="MARKET",
        time_in_force="IOC",
        reduce_only=False,
        post_only=False,
        market_id=2,
        size=Decimal("0.1"),
        price=Decimal("3008.01"),
        source_position=Decimal("0"),
        expires_at=NOW + 30,
        dispatch_count=0,
        order_id=None,
        filled_size=None,
        reconciled=False,
    )
    return identity, unsigned_place_request(intent, identity=identity, market=market())


def _unsigned_cancel_fixture():
    identity = _primary_identity()
    resting = order(
        wide=413, client=4013, account=ACCOUNT, side="SELL",
        order_type="LIMIT", tif="GTC", status="OPEN", price="2999.01",
        post_only=True,
    )
    cancel = DurableCancel(
        cancel_id="cancel-resting",
        intent_id="intent-maker",
        order_id=resting.order_id,
        market_id=2,
        resting_order_id=resting.resting_order_id,
        nonce_anchor=1,
        nonce_bitmap=0,
        payload_digest="payload-cancel",
        expires_at=NOW + 30,
        state="PREPARED",
        dispatch_count=0,
    )
    return identity, unsigned_cancel_request(cancel, identity=identity, market=market())


def _flip_first_byte(value: bytes) -> bytes:
    return bytes((value[0] ^ 1,)) + value[1:]


def test_unsigned_place_and_cancel_requests_bind_the_canonical_contract():
    place_identity, place = _unsigned_place_fixture()
    cancel_identity, cancel = _unsigned_cancel_fixture()
    _validate_unsigned_place(place, place_identity)
    _validate_unsigned_cancel(cancel, cancel_identity)


@pytest.mark.parametrize(
    "label, mutate",
    [
        pytest.param("side", lambda request: request["body"].__setitem__("side", 1), id="side"),
        pytest.param("order_type", lambda request: request["body"].__setitem__("order_type", 1), id="order-type"),
        pytest.param("time_in_force", lambda request: request["body"].__setitem__("time_in_force", 0), id="time-in-force"),
        pytest.param("post_only", lambda request: request["body"].__setitem__("post_only", True), id="post-only"),
        pytest.param("reduce_only", lambda request: request["body"].__setitem__("reduce_only", True), id="reduce-only"),
        pytest.param("size_steps", lambda request: request["body"].__setitem__("size_steps", 101), id="size-steps"),
        pytest.param("price_ticks", lambda request: request["body"].__setitem__("price_ticks", 300802), id="price-ticks"),
        pytest.param("client_order_id", lambda request: request["body"].__setitem__("client_order_id", 4014), id="client-order-id"),
        pytest.param("market_id", lambda request: request["body"].__setitem__("market_id", 3), id="market-id"),
        pytest.param("nonce_anchor", lambda request: request["body"].__setitem__("nonce_anchor", "2"), id="nonce-anchor"),
        pytest.param("nonce_bitmap", lambda request: request["body"].__setitem__("nonce_bitmap_index", 1), id="nonce-bitmap"),
        pytest.param("deadline", lambda request: request["body"].__setitem__("deadline", NOW + 31), id="deadline"),
        pytest.param("account", lambda request: request["body"].__setitem__("account", COUNTERPARTY), id="account"),
        pytest.param("signer", lambda request: request["body"].__setitem__("signer", COUNTERPARTY_SIGNER), id="signer"),
        pytest.param("header_flags", lambda request: request.__setitem__("header_flags", 0), id="header-flags"),
        pytest.param("order_data", lambda request: request.__setitem__("order_data", request["order_data"] + 1), id="order-data"),
        pytest.param("abi_encoded", lambda request: request.__setitem__("abi_encoded", _flip_first_byte(request["abi_encoded"])), id="abi-encoded"),
        pytest.param("action_hash", lambda request: request.__setitem__("action_hash", b"\\x00" * 32), id="action-hash"),
        pytest.param("action_digest", lambda request: request.__setitem__("action_digest", "0" * 64), id="action-digest"),
        pytest.param("permit_hash", lambda request: request["permit"]["message"].__setitem__("hash", "0x" + "00" * 32), id="permit-hash"),
        pytest.param("permit_nonce_anchor", lambda request: request["permit"]["message"].__setitem__("nonceAnchor", 2), id="permit-nonce-anchor"),
        pytest.param("permit_deadline", lambda request: request["permit"]["message"].__setitem__("deadline", NOW + 31), id="permit-deadline"),
    ],
)
def test_unsigned_place_rejects_any_canonical_mutation(label, mutate):
    identity, request = _unsigned_place_fixture()
    mutated = copy.deepcopy(request)
    mutate(mutated)
    with pytest.raises(CoordinatorSafetyError):
        _validate_unsigned_place(mutated, identity)


@pytest.mark.parametrize(
    "label, mutate",
    [
        pytest.param("action", lambda request: request.__setitem__("action", "other"), id="action"),
        pytest.param("market_id", lambda request: request.__setitem__("market_id", 3), id="market-id"),
        pytest.param("resting_order_id", lambda request: request.__setitem__("resting_order_id", request["resting_order_id"] + 1), id="resting-order-id"),
        pytest.param("abi_encoded", lambda request: request.__setitem__("abi_encoded", _flip_first_byte(request["abi_encoded"])), id="abi-encoded"),
        pytest.param("action_hash", lambda request: request.__setitem__("action_hash", b"\\x00" * 32), id="action-hash"),
        pytest.param("body_market_id", lambda request: request["body"].__setitem__("market_id", 3), id="body-market-id"),
        pytest.param("body_order_id", lambda request: request["body"].__setitem__("order_id", order(
            wide=415, client=4015, account=ACCOUNT, side="SELL", order_type="LIMIT",
            tif="GTC", status="OPEN", price="2999.01", post_only=True,
        ).order_id), id="body-order-id"),
        pytest.param("account", lambda request: request["body"]["permit"].__setitem__("account", COUNTERPARTY), id="account"),
        pytest.param("signer", lambda request: request["body"]["permit"].__setitem__("signer", COUNTERPARTY_SIGNER), id="signer"),
        pytest.param("nonce_anchor", lambda request: request["body"]["permit"].__setitem__("nonce_anchor", "2"), id="nonce-anchor"),
        pytest.param("nonce_bitmap", lambda request: request["body"]["permit"].__setitem__("nonce_bitmap_index", 1), id="nonce-bitmap"),
        pytest.param("deadline", lambda request: request["body"]["permit"].__setitem__("deadline", NOW + 31), id="deadline"),
        pytest.param("permit_hash", lambda request: request["permit"]["message"].__setitem__("hash", "0x" + "00" * 32), id="permit-hash"),
        pytest.param("permit_nonce_anchor", lambda request: request["permit"]["message"].__setitem__("nonceAnchor", 2), id="permit-nonce-anchor"),
        pytest.param("permit_deadline", lambda request: request["permit"]["message"].__setitem__("deadline", NOW + 31), id="permit-deadline"),
    ],
)
def test_unsigned_cancel_rejects_any_canonical_mutation(label, mutate):
    identity, request = _unsigned_cancel_fixture()
    mutated = copy.deepcopy(request)
    mutate(mutated)
    with pytest.raises(CoordinatorSafetyError):
        _validate_unsigned_cancel(mutated, identity)


def test_adapter_rejects_unsigned_mutations_before_credential_load():
    counterparty = RoleIdentity(
        AccountRole.COUNTERPARTY, FIXED_COUNTERPARTY_ACCOUNT,
        FIXED_COUNTERPARTY_SIGNER, "key", "marker", "journal",
    )
    loaded: list[bool] = []

    def load_credential():
        loaded.append(True)
        raise AssertionError("credential must not be loaded")

    venue = FixedRisexTwoAccountVenue(
        identities={
            AccountRole.PRIMARY: _primary_identity(),
            AccountRole.COUNTERPARTY: counterparty,
        },
        credential_loaders={
            AccountRole.PRIMARY: load_credential,
            AccountRole.COUNTERPARTY: load_credential,
        },
        transport=object(),
        now=lambda: NOW,
    )
    place_identity, place = _unsigned_place_fixture()
    place["body"]["side"] = 1
    with pytest.raises(CoordinatorSafetyError):
        asyncio.run(venue.place(place_identity.role, place))
    cancel_identity, cancel = _unsigned_cancel_fixture()
    cancel["resting_order_id"] += 1
    with pytest.raises(CoordinatorSafetyError):
        asyncio.run(venue.cancel(cancel_identity.role, cancel))
    assert loaded == []


def with_private(value: AccountSnapshot) -> AccountSnapshot:
    return replace(
        value,
        private=PrivateEventEvidence(
            account=value.account,
            auth_status="success",
            orders_snapshot=value.open_orders,
            positions_snapshot=((2, value.position),) if value.position else (),
            orders_updates=value.open_orders,
            positions_updates=((2, value.position),) if value.position else (),
            observed_at=NOW,
        ),
    )


def with_private_positions(
    value: AccountSnapshot, snapshot: tuple[tuple[int, Decimal], ...],
    updates: tuple[tuple[int, Decimal], ...] = (),
) -> AccountSnapshot:
    return replace(
        value,
        private=PrivateEventEvidence(
            account=value.account,
            auth_status="success",
            orders_snapshot=value.open_orders,
            positions_snapshot=snapshot,
            orders_updates=value.open_orders,
            positions_updates=updates,
            observed_at=NOW,
        ),
    )


def pair_observation(
    primary: AccountSnapshot,
    counterparty: AccountSnapshot,
    *,
    book: BookObservation,
    round_id: int = 0,
):
    return VenueObservation(
        market=market(book),
        accounts={
            AccountRole.PRIMARY: with_private(primary),
            AccountRole.COUNTERPARTY: with_private(counterparty),
        },
        nonces={
            AccountRole.PRIMARY: NonceState(10, 0),
            AccountRole.COUNTERPARTY: NonceState(20, 0),
        },
        rest_round=round_id,
    )


class LifecycleVenue(FakeVenue):
    def __init__(self, observations, rounds, *, fail_on_place=None, accepted_order_ids=None):
        super().__init__()
        self.observations = list(observations)
        self.rest_rounds = list(rounds)
        self.fail_on_place = fail_on_place
        self.accepted_order_ids = tuple(accepted_order_ids or ())
        if not self.accepted_order_ids and len(observations) >= 5:
            def history_id(index, role, client):
                return next(
                    item.order_id for item in observations[index].accounts[role].history_orders
                    if item.client_order_id == client
                )

            self.accepted_order_ids = (
                observations[1].accounts[AccountRole.COUNTERPARTY].open_orders[0].order_id,
                history_id(2, AccountRole.PRIMARY, 2001),
                observations[3].accounts[AccountRole.COUNTERPARTY].open_orders[0].order_id,
                history_id(4, AccountRole.PRIMARY, 2002),
            )

    async def place(self, role, request):
        self.place_calls.append((role, request))
        if self.fail_on_place == len(self.place_calls):
            raise KeyboardInterrupt
        if len(self.accepted_order_ids) < len(self.place_calls):
            return WriteResult.accepted(order_id=f"0x{len(self.place_calls):048x}")
        return WriteResult.accepted(order_id=self.accepted_order_ids[len(self.place_calls) - 1])


class PropagatingLifecycleVenue(LifecycleVenue):
    def __init__(
        self, observations, rounds, *, propagation_failures=0,
        propagation_order_id=None, propagation_after_place=None,
        accepted_order_ids=None,
    ):
        super().__init__(
            observations, rounds, accepted_order_ids=accepted_order_ids,
        )
        self.propagation_failures = propagation_failures
        self.propagation_order_id = propagation_order_id
        self.propagation_after_place = propagation_after_place
        self.observe_calls = 0

    async def observe(self):
        self.observe_calls += 1
        if (
            self.propagation_failures
            and self.place_calls
            and (
                self.propagation_after_place is None
                or len(self.place_calls) == self.propagation_after_place
            )
        ):
            self.propagation_failures -= 1
            order_id = self.propagation_order_id or self.accepted_order_ids[
                len(self.place_calls) - 1
            ]
            raise OrderHistoryPropagationMismatch(order_id)
        return await super().observe()


def lifecycle_observations(
    *, second_final_book: BookObservation | None = None,
    independent_external_fills: bool = False,
):
    maker_entry = order(
        wide=201, client=1001, account=COUNTERPARTY, side="SELL",
        order_type="LIMIT", tif="GTC", status="OPEN", price="2999.01",
        post_only=True,
    )
    entry = order(
        wide=101, client=2001, account=ACCOUNT, side="BUY",
        order_type="MARKET", tif="IOC", status="FILLED", price="3008.01",
        filled="0.1",
    )
    maker_entry_filled = replace(maker_entry, status="FILLED", filled_size=Decimal("0.1"))
    maker_exit = order(
        wide=203, client=1002, account=COUNTERPARTY, side="BUY",
        order_type="LIMIT", tif="GTC", status="OPEN", price="2999.00",
        post_only=True, reduce_only=True,
    )
    maker_exit_filled = replace(maker_exit, status="FILLED", filled_size=Decimal("0.1"))
    exit_order = order(
        wide=103, client=2002, account=ACCOUNT, side="SELL",
        order_type="MARKET", tif="IOC", status="FILLED", price="2990.00",
        filled="0.1", reduce_only=True,
    )
    initial_book = BookObservation(
        bid=Decimal("2999.00"), ask=Decimal("3000.00"),
        bids=(BookLevel(Decimal("2999.00"), Decimal("1"), 1),),
        asks=(BookLevel(Decimal("3000.00"), Decimal("1"), 1),), observed_at=NOW,
    )
    entry_book = BookObservation(
        bid=Decimal("2998.99"), ask=Decimal("2999.01"),
        bids=(BookLevel(Decimal("2998.99"), Decimal("1"), 1),),
        asks=(BookLevel(Decimal("2999.01"), Decimal("0.1"), 1),), observed_at=NOW,
    )
    exit_rest_book = BookObservation(
        bid=Decimal("2999.00"), ask=Decimal("2999.01"),
        bids=(BookLevel(Decimal("2999.00"), Decimal("0.1"), 1),),
        asks=(BookLevel(Decimal("2999.01"), Decimal("1"), 1),), observed_at=NOW,
    )
    entry_primary_trade_id = f"{maker_entry_filled.order_id}-{entry.order_id}"
    entry_counterparty_trade_id = entry_primary_trade_id
    entry_primary_price = Decimal("2999.01")
    entry_counterparty_price = entry_primary_price
    exit_primary_trade_id = f"{maker_exit_filled.order_id}-{exit_order.order_id}"
    exit_counterparty_trade_id = exit_primary_trade_id
    exit_primary_price = Decimal("2999.00")
    exit_counterparty_price = exit_primary_price
    if independent_external_fills:
        def external_order_id(wide: int) -> str:
            return f"0x{wide:016x}{1:016x}{1:016x}"

        entry_primary_trade_id = f"{external_order_id(301)}-{entry.order_id}"
        entry_counterparty_trade_id = f"{external_order_id(302)}-{maker_entry_filled.order_id}"
        entry_primary_price = Decimal("2999.02")
        exit_primary_trade_id = f"{external_order_id(303)}-{exit_order.order_id}"
        exit_counterparty_trade_id = f"{external_order_id(304)}-{maker_exit_filled.order_id}"
        exit_primary_price = Decimal("2998.99")
    entry_primary = account_snapshot(AccountRole.PRIMARY, position="0.1", orders=(), trades=(
        trade(trade_id=entry_primary_trade_id, item=entry, side="BUY", price=str(entry_primary_price)),
    ))
    entry_primary = replace(entry_primary, history_orders=(entry,))
    entry_counter = account_snapshot(AccountRole.COUNTERPARTY, position="-0.1", orders=(), trades=(
        trade(trade_id=entry_counterparty_trade_id, item=maker_entry_filled, side="SELL", price=str(entry_counterparty_price)),
    ))
    entry_counter = replace(entry_counter, history_orders=(maker_entry_filled,))
    exit_primary = account_snapshot(AccountRole.PRIMARY, position="0", orders=(), trades=(
        trade(trade_id=entry_primary_trade_id, item=entry, side="BUY", price=str(entry_primary_price)),
        trade(trade_id=exit_primary_trade_id, item=exit_order, side="SELL", price=str(exit_primary_price)),
    ))
    exit_primary = replace(exit_primary, history_orders=(entry, exit_order))
    exit_counter = account_snapshot(AccountRole.COUNTERPARTY, position="0", orders=(), trades=(
        trade(trade_id=entry_counterparty_trade_id, item=maker_entry_filled, side="SELL", price=str(entry_counterparty_price)),
        trade(trade_id=exit_counterparty_trade_id, item=maker_exit_filled, side="BUY", price=str(exit_counterparty_price)),
    ))
    exit_counter = replace(exit_counter, history_orders=(maker_entry_filled, maker_exit_filled))
    rounds_book = second_final_book or exit_rest_book
    observations = [
        pair_observation(account_snapshot(AccountRole.PRIMARY), account_snapshot(AccountRole.COUNTERPARTY), book=initial_book),
        pair_observation(account_snapshot(AccountRole.PRIMARY), account_snapshot(AccountRole.COUNTERPARTY, orders=(maker_entry,)), book=entry_book),
        pair_observation(entry_primary, entry_counter, book=entry_book),
    pair_observation(entry_primary, replace(entry_counter, position=Decimal("-0.1"), open_orders=(maker_exit,)), book=exit_rest_book),
        pair_observation(exit_primary, exit_counter, book=exit_rest_book),
    ]
    final = pair_observation(exit_primary, exit_counter, book=exit_rest_book, round_id=1)
    final_two = pair_observation(exit_primary, exit_counter, book=rounds_book, round_id=2)
    return observations, [final, final_two]


def identity_factory(role, step, _observation):
    values = {
        (AccountRole.COUNTERPARTY, "ENTRY_MAKER"): (1001, 20, 0, NOW + 60),
        (AccountRole.PRIMARY, "ENTRY_TAKER"): (2001, 10, 0, NOW + 60),
        (AccountRole.COUNTERPARTY, "EXIT_MAKER"): (1002, 20, 1, NOW + 60),
        (AccountRole.PRIMARY, "EXIT_TAKER"): (2002, 10, 1, NOW + 60),
    }
    return values[(role, step)]


def make_lifecycle(tmp_path: Path, venue: LifecycleVenue):
    return TwoAccountCoordinator._fixture(
        venue=venue,
        primary_journal=tmp_path / "primary.sqlite3",
        counterparty_journal=tmp_path / "counterparty.sqlite3",
        now=lambda: NOW,
        identity_factory=identity_factory,
    )


def install_external_entry_maker_fill(observations, rounds):
    maker = observations[1].accounts[AccountRole.COUNTERPARTY].open_orders[0]
    filled = replace(maker, status="FILLED", filled_size=Decimal("0.1"))
    external_trade = trade(
        trade_id=f"{maker.order_id}-{maker.order_id}",
        item=filled, side="SELL", price="2999.01",
    )
    external_counterparty = account_snapshot(
        AccountRole.COUNTERPARTY, position="-0.1", trades=(external_trade,),
    )
    external_counterparty = replace(
        external_counterparty, history_orders=(filled,),
    )
    observations[1] = pair_observation(
        account_snapshot(AccountRole.PRIMARY), external_counterparty,
        book=observations[1].market.book,
    )
    for collection, start in ((observations, 2), (rounds, 0)):
        for index in range(start, len(collection)):
            current = collection[index]
            counterparty = current.accounts[AccountRole.COUNTERPARTY]
            counterparty = replace(
                counterparty,
                history_orders=tuple(
                    filled if item.order_id == maker.order_id else item
                    for item in counterparty.history_orders
                ),
                trades=tuple(
                    external_trade if item.order_id == maker.order_id else item
                    for item in counterparty.trades
                ),
            )
            accounts = dict(current.accounts)
            accounts[AccountRole.COUNTERPARTY] = counterparty
            collection[index] = replace(current, accounts=accounts)
    accepted_order_ids = (
        maker.order_id,
        next(
            item.order_id
            for item in observations[2].accounts[AccountRole.PRIMARY].history_orders
            if item.client_order_id == 2001
        ),
        observations[3].accounts[AccountRole.COUNTERPARTY].open_orders[0].order_id,
        next(
            item.order_id
            for item in observations[4].accounts[AccountRole.PRIMARY].history_orders
            if item.client_order_id == 2002
        ),
    )
    return maker, external_trade, accepted_order_ids


def install_external_exit_maker_fill(observations, rounds):
    maker = observations[3].accounts[AccountRole.COUNTERPARTY].open_orders[0]
    filled = replace(maker, status="FILLED", filled_size=Decimal("0.1"))
    external_trade = trade(
        trade_id=f"{maker.order_id}-{maker.order_id}",
        item=filled, side="BUY", price="2999.00",
    )
    current_counterparty = observations[3].accounts[AccountRole.COUNTERPARTY]
    external_counterparty = with_private(replace(
        current_counterparty,
        position=Decimal("0"),
        open_orders=(),
        history_orders=(*current_counterparty.history_orders, filled),
        trades=(*current_counterparty.trades, external_trade),
    ))
    observations[3] = replace(observations[3], accounts={
        AccountRole.PRIMARY: observations[3].accounts[AccountRole.PRIMARY],
        AccountRole.COUNTERPARTY: external_counterparty,
    })
    for collection, start in ((observations, 4), (rounds, 0)):
        for index in range(start, len(collection)):
            current = collection[index]
            counterparty = current.accounts[AccountRole.COUNTERPARTY]
            counterparty = with_private(replace(
                counterparty,
                history_orders=tuple(
                    filled if item.order_id == maker.order_id else item
                    for item in counterparty.history_orders
                ),
                trades=tuple(
                    external_trade if item.order_id == maker.order_id else item
                    for item in counterparty.trades
                ),
            ))
            accounts = dict(current.accounts)
            accounts[AccountRole.COUNTERPARTY] = counterparty
            collection[index] = replace(current, accounts=accounts)
    accepted_order_ids = (
        observations[1].accounts[AccountRole.COUNTERPARTY].open_orders[0].order_id,
        next(
            item.order_id
            for item in observations[2].accounts[AccountRole.PRIMARY].history_orders
            if item.client_order_id == 2001
        ),
        maker.order_id,
        next(
            item.order_id
            for item in observations[4].accounts[AccountRole.PRIMARY].history_orders
            if item.client_order_id == 2002
        ),
    )
    return maker, external_trade, accepted_order_ids


@pytest.fixture
def propagation_sleep(monkeypatch):
    calls = []

    async def instant_sleep(delay):
        calls.append(delay)

    monkeypatch.setattr(asyncio, "sleep", instant_sleep)
    return calls


def _halted_entry_coordinator(
    tmp_path: Path, *, independent_external_fills: bool = False,
):
    valid_observations, rounds = lifecycle_observations(
        independent_external_fills=independent_external_fills,
    )
    bad_observations = list(valid_observations)
    bad = bad_observations[2]
    primary = bad.accounts[AccountRole.PRIMARY]
    bad_primary = replace(
        primary,
        history_orders=(replace(primary.history_orders[0], filled_size=Decimal("0.05")),),
        trades=(replace(primary.trades[0], size=Decimal("0.05")),),
    )
    bad_observations[2] = replace(bad, accounts={
        AccountRole.PRIMARY: with_private(bad_primary),
        AccountRole.COUNTERPARTY: bad.accounts[AccountRole.COUNTERPARTY],
    })
    venue = LifecycleVenue(bad_observations, rounds)
    coordinator = make_lifecycle(tmp_path, venue)
    report = asyncio.run(coordinator.run())
    assert report.result is CoordinatorResult.FAILED_HALTED_MANUAL_RECOVERY
    return coordinator, venue, valid_observations, rounds


def test_propagation_settlement_bound_is_fixed_sixty_seconds():
    assert PROPAGATION_SETTLE_SECONDS == 60


def test_complete_two_account_lifecycle_is_sequential_and_reduce_only(
    tmp_path: Path, propagation_sleep,
):
    observations, rounds = lifecycle_observations()
    venue = LifecycleVenue(observations, rounds)
    coordinator = make_lifecycle(tmp_path, venue)
    report = asyncio.run(coordinator.run())
    assert report.result is CoordinatorResult.COMPLETE
    assert report.primary_intents == 2
    assert report.counterparty_intents == 2
    assert report.primary_dispatches == report.counterparty_dispatches == 2
    assert [role for role, _ in venue.place_calls] == [
        AccountRole.COUNTERPARTY, AccountRole.PRIMARY,
        AccountRole.COUNTERPARTY, AccountRole.PRIMARY,
    ]
    maker_request = venue.place_calls[0][1]["body"]
    assert maker_request["side"] == 1 and maker_request["order_type"] == 1
    assert maker_request["time_in_force"] == 0
    assert maker_request["post_only"] is True and maker_request["reduce_only"] is False
    exit_maker_request = venue.place_calls[2][1]["body"]
    exit_request = venue.place_calls[3][1]["body"]
    assert exit_maker_request["side"] == 0 and exit_maker_request["post_only"] is True
    assert exit_maker_request["reduce_only"] is True
    assert exit_request["side"] == 1 and exit_request["reduce_only"] is True
    assert report.final_rounds == 2
    assert propagation_sleep == []


def test_independent_external_fills_complete_with_each_journal_binding_its_own_evidence(
    tmp_path: Path,
):
    observations, rounds = lifecycle_observations(independent_external_fills=True)
    coordinator = make_lifecycle(tmp_path, LifecycleVenue(observations, rounds))

    report = asyncio.run(coordinator.run())

    assert report.result is CoordinatorResult.COMPLETE
    primary = coordinator._journals[AccountRole.PRIMARY]
    counterparty = coordinator._journals[AccountRole.COUNTERPARTY]
    assert primary.terminal("trade:ENTRY") != counterparty.terminal("trade:ENTRY")
    assert primary.terminal("trade:EXIT") != counterparty.terminal("trade:EXIT")
    assert primary.terminal("price:ENTRY") == "2999.02"
    assert counterparty.terminal("price:ENTRY") == "2999.01"
    assert primary.terminal("price:EXIT") == "2998.99"
    assert counterparty.terminal("price:EXIT") == "2999.00"
    assert primary.by_step("ENTRY_TAKER").dispatch_count == 1
    assert counterparty.by_step("ENTRY_MAKER").dispatch_count == 1
    assert primary.by_step("EXIT_TAKER").dispatch_count == 1
    assert counterparty.by_step("EXIT_MAKER").dispatch_count == 1
    assert report.final_rounds == 2


@pytest.mark.parametrize(
    "mutation",
    [
        "same_direction",
        "unequal_quantity",
        "duplicate",
        "wrong_account",
        "wrong_order",
        "wrong_client",
        "wrong_market",
        "partial",
        "ambiguous",
        "unrelated_order",
        "unrelated_fill",
        "nonterminal_order",
    ],
)
def test_independent_fill_contract_rejects_adverse_entry_evidence(
    tmp_path: Path, mutation: str,
):
    observations, rounds = lifecycle_observations(independent_external_fills=True)
    current = observations[2]
    primary = current.accounts[AccountRole.PRIMARY]
    counterparty = current.accounts[AccountRole.COUNTERPARTY]
    primary_trade = primary.trades[0]
    counterparty_trade = counterparty.trades[0]

    if mutation == "same_direction":
        counterparty = with_private(replace(
            counterparty, trades=(replace(counterparty_trade, side="BUY"),),
        ))
    elif mutation == "unequal_quantity":
        primary = with_private(replace(
            primary, trades=(replace(primary_trade, size=Decimal("0.099")),),
        ))
    elif mutation == "duplicate":
        primary = with_private(replace(
            primary, trades=(primary_trade, primary_trade),
        ))
    elif mutation == "wrong_account":
        primary = with_private(replace(
            primary, trades=(replace(primary_trade, account=COUNTERPARTY),),
        ))
    elif mutation == "wrong_order":
        wrong_order_id = counterparty.history_orders[0].order_id
        primary = with_private(replace(
            primary, trades=(replace(primary_trade, order_id=wrong_order_id),),
        ))
    elif mutation == "wrong_client":
        primary = with_private(replace(
            primary,
            trades=(replace(primary_trade, client_order_id=counterparty_trade.client_order_id),),
        ))
    elif mutation == "wrong_market":
        primary = with_private(replace(
            primary, trades=(replace(primary_trade, market_id=3),),
        ))
    elif mutation == "partial":
        primary = with_private(replace(
            primary,
            history_orders=(replace(primary.history_orders[0], filled_size=Decimal("0.05")),),
            trades=(replace(primary_trade, size=Decimal("0.05")),),
        ))
    elif mutation == "ambiguous":
        ambiguous = replace(
            primary_trade,
            trade_id=f"0x{499:016x}{1:016x}{1:016x}-{primary_trade.order_id}",
        )
        primary = with_private(replace(
            primary, trades=(primary_trade, ambiguous),
        ))
    elif mutation == "unrelated_order":
        unrelated = order(
            wide=499, client=4499, account=ACCOUNT, side="BUY",
            order_type="MARKET", tif="IOC", status="FILLED", price="0",
        )
        primary = with_private(replace(
            primary, history_orders=(*primary.history_orders, unrelated),
        ))
    elif mutation == "unrelated_fill":
        unrelated = order(
            wide=499, client=4499, account=ACCOUNT, side="BUY",
            order_type="MARKET", tif="IOC", status="FILLED", price="0",
            filled="0.1",
        )
        unrelated_trade = trade(
            trade_id=f"{unrelated.order_id}-{unrelated.order_id}",
            item=unrelated, side="BUY", price="2999.02",
        )
        primary = with_private(replace(
            primary, trades=(*primary.trades, unrelated_trade),
        ))
    else:
        counterparty = with_private(replace(
            counterparty,
            history_orders=(replace(counterparty.history_orders[0], status="OPEN"),),
        ))

    observations[2] = replace(current, accounts={
        AccountRole.PRIMARY: primary,
        AccountRole.COUNTERPARTY: counterparty,
    })
    report = asyncio.run(make_lifecycle(
        tmp_path, LifecycleVenue(observations, rounds),
    ).run())

    assert report.result is CoordinatorResult.FAILED_HALTED_MANUAL_RECOVERY
    assert report.primary_dispatches == 1
    assert report.counterparty_dispatches == 1


def test_exact_order_history_propagation_mismatch_settles_before_one_fresh_resample(
    tmp_path: Path, propagation_sleep,
):
    observations, rounds = lifecycle_observations()
    venue = PropagatingLifecycleVenue(
        observations, rounds, propagation_failures=1,
    )
    coordinator = make_lifecycle(tmp_path, venue)
    report = asyncio.run(coordinator.run())
    assert report.result is CoordinatorResult.COMPLETE
    assert venue.observe_calls == 6
    assert len(venue.place_calls) == 4
    entry_maker = coordinator._journals[AccountRole.COUNTERPARTY].by_step("ENTRY_MAKER")
    assert entry_maker is not None
    assert coordinator._journals[AccountRole.PRIMARY].terminal(
        f"place_resample:{entry_maker.intent_id}"
    ) == "USED"
    assert coordinator._journals[AccountRole.COUNTERPARTY].terminal(
        f"place_resample:{entry_maker.intent_id}"
    ) == "USED"
    assert propagation_sleep == [PROPAGATION_SETTLE_SECONDS]


def test_propagation_settlement_cancellation_consumes_allowance_without_resample(
    tmp_path: Path, monkeypatch,
):
    observations, rounds = lifecycle_observations()
    venue = PropagatingLifecycleVenue(
        observations, rounds, propagation_failures=1,
    )
    coordinator = make_lifecycle(tmp_path, venue)
    sleep_calls = []
    sleep_started = asyncio.Event()
    durable_markers = []

    async def cancellable_sleep(delay):
        intent = coordinator._journals[AccountRole.COUNTERPARTY].by_step("ENTRY_MAKER")
        assert intent is not None
        key = f"place_resample:{intent.intent_id}"
        sleep_calls.append(delay)
        durable_markers.append(tuple(
            coordinator._journals[role].terminal(key)
            for role in (AccountRole.PRIMARY, AccountRole.COUNTERPARTY)
        ))
        sleep_started.set()
        await asyncio.Future()

    monkeypatch.setattr(asyncio, "sleep", cancellable_sleep)

    async def cancel_during_settlement():
        task = asyncio.create_task(coordinator.run())
        await sleep_started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(cancel_during_settlement())
    assert sleep_calls == [PROPAGATION_SETTLE_SECONDS]
    assert durable_markers == [("USED", "USED")]
    assert venue.observe_calls == 2
    assert [role for role, _ in venue.place_calls] == [AccountRole.COUNTERPARTY]


def test_taker_propagation_mismatch_allows_current_taker_or_paired_maker(
    tmp_path: Path, propagation_sleep,
):
    observations, rounds = lifecycle_observations()
    paired_maker_id = observations[2].accounts[
        AccountRole.COUNTERPARTY
    ].history_orders[0].order_id
    venue = PropagatingLifecycleVenue(
        observations, rounds, propagation_failures=1,
        propagation_order_id=paired_maker_id, propagation_after_place=2,
    )
    coordinator = make_lifecycle(tmp_path, venue)
    report = asyncio.run(coordinator.run())
    assert report.result is CoordinatorResult.COMPLETE
    assert venue.observe_calls == 6
    entry_taker = coordinator._journals[AccountRole.PRIMARY].by_step("ENTRY_TAKER")
    assert entry_taker is not None
    assert coordinator._journals[AccountRole.PRIMARY].terminal(
        f"place_resample:{entry_taker.intent_id}"
    ) == "USED"
    assert propagation_sleep == [PROPAGATION_SETTLE_SECONDS]


def test_exact_order_history_propagation_mismatch_halts_after_one_resample(
    tmp_path: Path, propagation_sleep,
):
    observations, rounds = lifecycle_observations()
    venue = PropagatingLifecycleVenue(
        observations, rounds, propagation_failures=2,
    )
    coordinator = make_lifecycle(tmp_path, venue)
    report = asyncio.run(coordinator.run())
    assert report.result is CoordinatorResult.FAILED_HALTED_MANUAL_RECOVERY
    assert report.failure_code == "RISEx exact order/history propagation mismatch"
    assert venue.observe_calls == 3
    assert [role for role, _ in venue.place_calls] == [AccountRole.COUNTERPARTY]
    assert coordinator.phase is Phase.HALTED
    assert propagation_sleep == [PROPAGATION_SETTLE_SECONDS]


def test_older_stage_propagation_mismatch_is_terminal_without_resample(
    tmp_path: Path, propagation_sleep,
):
    observations, rounds = lifecycle_observations()
    older_entry_order_id = observations[2].accounts[
        AccountRole.COUNTERPARTY
    ].history_orders[0].order_id
    venue = PropagatingLifecycleVenue(
        observations, rounds, propagation_failures=1,
        propagation_order_id=older_entry_order_id, propagation_after_place=3,
    )
    coordinator = make_lifecycle(tmp_path, venue)
    report = asyncio.run(coordinator.run())
    assert report.result is CoordinatorResult.FAILED_HALTED_MANUAL_RECOVERY
    assert report.failure_code == "RISEx unrelated order/history propagation mismatch"
    assert venue.observe_calls == 4
    assert [role for role, _ in venue.place_calls] == [
        AccountRole.COUNTERPARTY, AccountRole.PRIMARY, AccountRole.COUNTERPARTY,
    ]
    exit_maker = coordinator._journals[AccountRole.COUNTERPARTY].by_step("EXIT_MAKER")
    assert exit_maker is not None
    assert coordinator._journals[AccountRole.PRIMARY].terminal(
        f"place_resample:{exit_maker.intent_id}"
    ) is None
    assert coordinator._journals[AccountRole.COUNTERPARTY].terminal(
        f"place_resample:{exit_maker.intent_id}"
    ) is None
    assert propagation_sleep == []


def test_order_history_propagation_classifier_is_monotonic_and_immutable():
    history = order(
        wide=430, client=4030, account=ACCOUNT, side="BUY",
        order_type="LIMIT", tif="GTC", status="OPEN", price="2999.01",
    )
    assert _is_monotonic_order_visibility(
        history, replace(history, filled_size=Decimal("0.1")),
    )
    assert _is_monotonic_order_visibility(
        history, replace(history, status="FILLED", filled_size=Decimal("0.1")),
    )
    assert _is_monotonic_order_visibility(
        history, replace(history, status="CANCELLED"),
    )
    assert not _is_monotonic_order_visibility(
        replace(history, status="FILLED", filled_size=Decimal("0.1")), history,
    )
    assert not _is_monotonic_order_visibility(
        replace(history, filled_size=Decimal("0.1")),
        replace(history, filled_size=Decimal("0.05")),
    )
    assert not _is_monotonic_order_visibility(
        history, replace(history, price=Decimal("2999.02")),
    )
    assert not _is_monotonic_order_visibility(
        replace(history, status="CANCELLED"),
        replace(history, status="FILLED", filled_size=Decimal("0.1")),
    )


def test_fixed_account_adapter_classifies_only_history_status_visibility_lag():
    history = order(
        wide=431, client=4031, account=ACCOUNT, side="BUY",
        order_type="LIMIT", tif="GTC", status="OPEN", price="2999.01",
    )
    exact = replace(history, status="FILLED", filled_size=Decimal("0.1"))
    transport = _FlatAccountReads(
        history_orders=(history,), exact_order=exact,
    )
    with pytest.raises(OrderHistoryPropagationMismatch) as captured:
        asyncio.run(
            _flat_read_venue(
                transport, known_order_ids=(history.order_id,)
            )._account(AccountRole.PRIMARY, include_private=False),
        )
    assert captured.value.order_id == history.order_id

    regression = _FlatAccountReads(
        history_orders=(replace(history, status="FILLED", filled_size=Decimal("0.1")),),
        exact_order=history,
    )
    with pytest.raises(CoordinatorSafetyError) as captured:
        asyncio.run(
            _flat_read_venue(
                regression, known_order_ids=(history.order_id,)
            )._account(AccountRole.PRIMARY, include_private=False),
        )
    assert not isinstance(captured.value, OrderHistoryPropagationMismatch)


def test_fixed_account_adapter_classifies_missing_known_order_until_list_catches_up():
    history = order(
        wide=432, client=4032, account=ACCOUNT, side="BUY",
        order_type="LIMIT", tif="GTC", status="OPEN", price="2999.01",
    )
    exact = replace(history, status="FILLED", filled_size=Decimal("0.1"))
    trade_row = {
        "id": f"{exact.order_id}-{exact.order_id}",
        "order_id": exact.order_id,
        "client_order_id": exact.client_order_id,
        "market_id": 2,
        "side": "BUY",
        "size": "0.1",
        "price": "2999.01",
    }
    transport = _FlatAccountReads(
        history_orders=(), exact_order=exact, trade_rows=(trade_row,),
    )
    venue = _flat_read_venue(
        transport, known_order_ids=(history.order_id,),
    )
    lookup_path = ORDER_LOOKUP_PATH_TEMPLATE.format(order_id=history.order_id)
    with pytest.raises(OrderHistoryPropagationMismatch) as captured:
        asyncio.run(
            venue._account(AccountRole.PRIMARY, include_private=False),
        )
    assert captured.value.order_id == history.order_id
    assert transport.calls.count((lookup_path, ())) == 1

    transport.history_orders = (exact,)
    snapshot, _, _ = asyncio.run(
        venue._account(AccountRole.PRIMARY, include_private=False),
    )
    assert snapshot.history_orders == (exact,)
    assert snapshot.trades[0].order_id == exact.order_id
    assert transport.calls.count((lookup_path, ())) == 2


def test_entry_recovery_accepts_exact_fill_and_resumed_run_only_writes_exits(tmp_path: Path):
    coordinator, initial_venue, valid_observations, rounds = _halted_entry_coordinator(tmp_path)
    coordinator.recover_entry_fill(valid_observations[2])
    assert coordinator.phase is Phase.ENTRY_RECONCILED
    assert all(
        coordinator._journals[role].outcome == "ACTIVE"
        for role in (AccountRole.PRIMARY, AccountRole.COUNTERPARTY)
    )
    assert all(
        coordinator._journals[role].by_step(
            "ENTRY_TAKER" if role is AccountRole.PRIMARY else "ENTRY_MAKER"
        ).state == "TERMINAL"
        for role in (AccountRole.PRIMARY, AccountRole.COUNTERPARTY)
    )
    exit_maker_id = valid_observations[3].accounts[
        AccountRole.COUNTERPARTY
    ].open_orders[0].order_id
    exit_taker_id = next(
        item.order_id for item in valid_observations[4].accounts[AccountRole.PRIMARY].history_orders
        if item.client_order_id == 2002
    )
    resumed_venue = LifecycleVenue(
        [valid_observations[2], valid_observations[3], valid_observations[4]], rounds,
        accepted_order_ids=(exit_maker_id, exit_taker_id),
    )
    coordinator._venue = resumed_venue
    report = asyncio.run(coordinator.run())
    assert report.result is CoordinatorResult.COMPLETE
    assert [role for role, _ in initial_venue.place_calls] == [
        AccountRole.COUNTERPARTY, AccountRole.PRIMARY,
    ]
    assert [role for role, _ in resumed_venue.place_calls] == [
        AccountRole.COUNTERPARTY, AccountRole.PRIMARY,
    ]
    assert coordinator._journals[AccountRole.PRIMARY].by_step("ENTRY_TAKER").dispatch_count == 1
    assert coordinator._journals[AccountRole.COUNTERPARTY].by_step("ENTRY_MAKER").dispatch_count == 1
    assert coordinator._journals[AccountRole.PRIMARY].by_step("EXIT_TAKER").reduce_only is True
    assert coordinator._journals[AccountRole.COUNTERPARTY].by_step("EXIT_MAKER").reduce_only is True


def test_entry_recovery_preserves_independent_trade_ids_and_prices(tmp_path: Path):
    coordinator, _, valid_observations, _ = _halted_entry_coordinator(
        tmp_path, independent_external_fills=True,
    )

    coordinator.recover_entry_fill(valid_observations[2])

    primary = coordinator._journals[AccountRole.PRIMARY]
    counterparty = coordinator._journals[AccountRole.COUNTERPARTY]
    assert primary.terminal("trade:ENTRY") != counterparty.terminal("trade:ENTRY")
    assert primary.terminal("price:ENTRY") == "2999.02"
    assert counterparty.terminal("price:ENTRY") == "2999.01"


def test_entry_recovery_rejects_mismatch_without_writes_and_cannot_replay(tmp_path: Path):
    coordinator, venue, valid_observations, _ = _halted_entry_coordinator(tmp_path)
    valid = valid_observations[2]
    primary = valid.accounts[AccountRole.PRIMARY]
    mismatched = replace(primary, position=Decimal("0"))
    bad = replace(valid, accounts={
        AccountRole.PRIMARY: with_private(mismatched),
        AccountRole.COUNTERPARTY: valid.accounts[AccountRole.COUNTERPARTY],
    })
    with pytest.raises(CoordinatorSafetyError):
        coordinator.recover_entry_fill(bad)
    assert coordinator.phase is Phase.HALTED
    assert [role for role, _ in venue.place_calls] == [
        AccountRole.COUNTERPARTY, AccountRole.PRIMARY,
    ]
    assert coordinator._journals[AccountRole.PRIMARY].by_step("ENTRY_TAKER").state == "DISPATCHED"
    assert coordinator._journals[AccountRole.COUNTERPARTY].by_step("ENTRY_MAKER").state == "RESTING"
    coordinator.recover_entry_fill(valid)
    with pytest.raises(CoordinatorSafetyError):
        coordinator.recover_entry_fill(valid)
    assert [role for role, _ in venue.place_calls] == [
        AccountRole.COUNTERPARTY, AccountRole.PRIMARY,
    ]


def test_post_baseline_unknown_open_order_halts_before_lifecycle_progress(tmp_path: Path):
    observations, rounds = lifecycle_observations()
    unknown = order(
        wide=409, client=4009, account=COUNTERPARTY, side="BUY",
        order_type="LIMIT", tif="GTC", status="OPEN", price="2999.01",
        post_only=True,
    )
    original = observations[1].accounts[AccountRole.COUNTERPARTY]
    observations[1] = replace(observations[1], accounts={
        AccountRole.PRIMARY: observations[1].accounts[AccountRole.PRIMARY],
        AccountRole.COUNTERPARTY: replace(
            original, open_orders=(*original.open_orders, unknown),
        ),
    })
    venue = LifecycleVenue(observations, rounds)
    report = asyncio.run(make_lifecycle(tmp_path, venue).run())
    assert report.result is CoordinatorResult.FAILED_HALTED_MANUAL_RECOVERY
    assert [role for role, _ in venue.place_calls] == [AccountRole.COUNTERPARTY]


def test_partial_mutual_fill_halts_before_second_maker(tmp_path: Path):
    observations, rounds = lifecycle_observations()
    bad = observations[2]
    primary = bad.accounts[AccountRole.PRIMARY]
    trade_row = primary.trades[0]
    bad_primary = replace(
        primary,
        history_orders=(replace(primary.history_orders[0], filled_size=Decimal("0.05")),),
        trades=(replace(trade_row, size=Decimal("0.05")),),
    )
    observations[2] = replace(bad, accounts={
        AccountRole.PRIMARY: with_private(bad_primary),
        AccountRole.COUNTERPARTY: bad.accounts[AccountRole.COUNTERPARTY],
    })
    venue = LifecycleVenue(observations, rounds)
    report = asyncio.run(make_lifecycle(tmp_path, venue).run())
    assert report.result is CoordinatorResult.FAILED_HALTED_MANUAL_RECOVERY
    assert [role for role, _ in venue.place_calls] == [AccountRole.COUNTERPARTY, AccountRole.PRIMARY]


def test_external_entry_maker_fill_is_reconciled_before_primary_dispatch(
    tmp_path: Path, propagation_sleep, monkeypatch,
):
    observations, rounds = lifecycle_observations()
    maker, external_trade, accepted_order_ids = install_external_entry_maker_fill(
        observations, rounds,
    )
    venue = PropagatingLifecycleVenue(
        observations, rounds, propagation_failures=1,
        propagation_order_id=maker.order_id,
        accepted_order_ids=accepted_order_ids,
    )
    coordinator = make_lifecycle(tmp_path, venue)
    preparation_states = []
    original_prepare = coordinator._prepare

    def capture_primary_preparation(role, observation, **kwargs):
        if role is AccountRole.PRIMARY and kwargs["step"] == "ENTRY_TAKER":
            maker_intent = coordinator._journals[AccountRole.COUNTERPARTY].by_step(
                "ENTRY_MAKER"
            )
            assert maker_intent is not None
            journal = coordinator._journals[AccountRole.COUNTERPARTY]
            preparation_states.append(
                (
                    maker_intent.state, maker_intent.filled_size,
                    maker_intent.reconciled, journal.terminal("trade:ENTRY"),
                    journal.terminal("price:ENTRY"),
                )
            )
        return original_prepare(role, observation, **kwargs)

    monkeypatch.setattr(coordinator, "_prepare", capture_primary_preparation)
    report = asyncio.run(coordinator.run())
    assert report.result is CoordinatorResult.COMPLETE
    assert report.primary_dispatches == 2
    assert report.counterparty_dispatches == 2
    assert [role for role, _ in venue.place_calls] == [
        AccountRole.COUNTERPARTY, AccountRole.PRIMARY,
        AccountRole.COUNTERPARTY, AccountRole.PRIMARY,
    ]
    assert preparation_states == [
        (
            "TERMINAL", Decimal("0.1"), True,
            external_trade.trade_id, str(external_trade.price),
        )
    ]
    maker_intent = coordinator._journals[AccountRole.COUNTERPARTY].by_step("ENTRY_MAKER")
    assert maker_intent is not None
    assert maker_intent.state == "TERMINAL"
    assert maker_intent.filled_size == Decimal("0.1")
    assert maker_intent.reconciled is True
    assert coordinator._journals[AccountRole.COUNTERPARTY].terminal(
        "trade:ENTRY"
    ) == external_trade.trade_id
    assert coordinator._journals[AccountRole.COUNTERPARTY].terminal(
        "price:ENTRY"
    ) == str(external_trade.price)
    assert venue.observe_calls == 6
    assert propagation_sleep == [PROPAGATION_SETTLE_SECONDS]


@pytest.mark.parametrize(
    "mutation",
    [
        "cancelled",
        "partial",
        "missing_trade",
        "duplicate_trade",
        "wrong_position",
        "open_order",
        "open_contradiction",
        "wrong_order_price",
        "wrong_order_client",
        "wrong_order_direction",
        "wrong_trade_order",
        "wrong_trade_client",
        "wrong_trade_market",
        "wrong_trade_side",
        "wrong_trade_price",
        "extra_trade",
        "stale",
    ],
)
def test_external_entry_maker_fill_rejects_non_exact_evidence(
    tmp_path: Path, propagation_sleep, mutation: str,
):
    observations, rounds = lifecycle_observations()
    maker, matching_trade, accepted_order_ids = install_external_entry_maker_fill(
        observations, rounds,
    )
    filled = observations[1].accounts[AccountRole.COUNTERPARTY].history_orders[0]
    external_counterparty = observations[1].accounts[AccountRole.COUNTERPARTY]

    if mutation == "cancelled":
        cancelled = replace(filled, status="CANCELLED", filled_size=Decimal("0"))
        external_counterparty = replace(external_counterparty, history_orders=(cancelled,))
    elif mutation == "partial":
        partial = replace(filled, filled_size=Decimal("0.05"))
        partial_trade = replace(matching_trade, size=Decimal("0.05"))
        external_counterparty = replace(
            external_counterparty, history_orders=(partial,), trades=(partial_trade,),
        )
    elif mutation == "missing_trade":
        external_counterparty = replace(external_counterparty, trades=())
    elif mutation == "duplicate_trade":
        external_counterparty = replace(
            external_counterparty, trades=(matching_trade, matching_trade),
        )
    elif mutation == "wrong_position":
        external_counterparty = with_private(
            replace(external_counterparty, position=Decimal("-0.05")),
        )
    elif mutation == "open_order":
        external_counterparty = with_private(
            replace(external_counterparty, open_orders=(filled,)),
        )
    elif mutation == "open_contradiction":
        external_counterparty = with_private(
            replace(
                external_counterparty, open_orders=(maker,), history_orders=(),
            ),
        )
    elif mutation == "wrong_order_price":
        external_counterparty = replace(
            external_counterparty,
            history_orders=(replace(filled, price=Decimal("2999.02")),),
        )
    elif mutation == "wrong_order_client":
        external_counterparty = replace(
            external_counterparty,
            history_orders=(replace(filled, client_order_id=1009),),
        )
    elif mutation == "wrong_order_direction":
        external_counterparty = replace(
            external_counterparty,
            history_orders=(replace(filled, side="BUY"),),
        )
    elif mutation == "wrong_trade_order":
        wrong_order = order(
            wide=499, client=4499, account=COUNTERPARTY, side="SELL",
            order_type="LIMIT", tif="GTC", status="FILLED", price="2999.01",
            filled="0.1", post_only=True,
        )
        external_counterparty = replace(
            external_counterparty,
            trades=(replace(matching_trade, order_id=wrong_order.order_id),),
        )
    elif mutation == "wrong_trade_client":
        external_counterparty = replace(
            external_counterparty,
            trades=(replace(matching_trade, client_order_id=1009),),
        )
    elif mutation == "wrong_trade_market":
        external_counterparty = replace(
            external_counterparty,
            trades=(replace(matching_trade, market_id=3),),
        )
    elif mutation == "wrong_trade_side":
        external_counterparty = replace(
            external_counterparty,
            trades=(replace(matching_trade, side="BUY"),),
        )
    elif mutation == "wrong_trade_price":
        external_counterparty = replace(
            external_counterparty,
            trades=(replace(matching_trade, price=Decimal("2999.02")),),
        )
    elif mutation == "extra_trade":
        extra_order = order(
            wide=499, client=4499, account=COUNTERPARTY, side="SELL",
            order_type="LIMIT", tif="GTC", status="FILLED", price="2999.01",
            filled="0.1", post_only=True,
        )
        extra_trade = replace(
            matching_trade,
            trade_id=f"{maker.order_id}-{extra_order.order_id}",
        )
        external_counterparty = replace(
            external_counterparty, trades=(matching_trade, extra_trade),
        )
    else:
        external_counterparty = replace(
            external_counterparty, observed_at=NOW - MAX_AGE_SECONDS - 1,
        )

    venue = PropagatingLifecycleVenue(
        observations, rounds, propagation_failures=1,
        propagation_order_id=maker.order_id,
        accepted_order_ids=accepted_order_ids,
    )
    venue.observations[1] = pair_observation(
        account_snapshot(AccountRole.PRIMARY), external_counterparty,
        book=observations[1].market.book,
    )
    report = asyncio.run(make_lifecycle(tmp_path, venue).run())

    assert report.result is CoordinatorResult.FAILED_HALTED_MANUAL_RECOVERY
    assert report.primary_dispatches == 0
    assert report.counterparty_dispatches == 1
    assert [role for role, _ in venue.place_calls] == [AccountRole.COUNTERPARTY]
    assert propagation_sleep == [PROPAGATION_SETTLE_SECONDS]


def test_external_entry_maker_fill_survives_restart_before_primary_preparation(
    tmp_path: Path, propagation_sleep, monkeypatch,
):
    observations, rounds = lifecycle_observations()
    maker, external_trade, accepted_order_ids = install_external_entry_maker_fill(
        observations, rounds,
    )
    venue = PropagatingLifecycleVenue(
        observations, rounds, propagation_failures=1,
        propagation_order_id=maker.order_id,
        accepted_order_ids=accepted_order_ids,
    )
    coordinator = make_lifecycle(tmp_path, venue)
    primary_preparations = []
    original_prepare = coordinator._prepare

    def capture_primary_preparation(role, observation, **kwargs):
        if role is AccountRole.PRIMARY and kwargs["step"] == "ENTRY_TAKER":
            primary_preparations.append(True)
        return original_prepare(role, observation, **kwargs)

    monkeypatch.setattr(coordinator, "_prepare", capture_primary_preparation)
    original_set_phase = coordinator._set_phase

    def crash_after_external_reconciliation(phase):
        original_set_phase(phase)
        if phase is Phase.ENTRY_MAKER_RESTING:
            raise KeyboardInterrupt

    monkeypatch.setattr(coordinator, "_set_phase", crash_after_external_reconciliation)
    with pytest.raises(KeyboardInterrupt):
        asyncio.run(coordinator.run())

    counterparty = coordinator._journals[AccountRole.COUNTERPARTY]
    maker_intent = counterparty.by_step("ENTRY_MAKER")
    assert maker_intent is not None
    assert maker_intent.state == "TERMINAL"
    assert maker_intent.filled_size == Decimal("0.1")
    assert maker_intent.reconciled is True
    assert counterparty.terminal("trade:ENTRY") == external_trade.trade_id
    assert counterparty.terminal("price:ENTRY") == str(external_trade.price)
    assert primary_preparations == []
    assert coordinator.phase is Phase.ENTRY_MAKER_RESTING
    for journal in coordinator._journals.values():
        journal.close()

    entry_taker_id = next(
        item.order_id for item in observations[2].accounts[AccountRole.PRIMARY].history_orders
        if item.client_order_id == 2001
    )
    exit_maker_id = observations[3].accounts[AccountRole.COUNTERPARTY].open_orders[0].order_id
    exit_taker_id = next(
        item.order_id for item in observations[4].accounts[AccountRole.PRIMARY].history_orders
        if item.client_order_id == 2002
    )
    recovery_venue = LifecycleVenue(
        observations[1:5], rounds,
        accepted_order_ids=(entry_taker_id, exit_maker_id, exit_taker_id),
    )
    recovered = make_lifecycle(tmp_path, recovery_venue)
    recovery_preparations = []
    recovered_original_prepare = recovered._prepare

    def capture_recovery_preparation(role, observation, **kwargs):
        if role is AccountRole.PRIMARY and kwargs["step"] == "ENTRY_TAKER":
            journal = recovered._journals[AccountRole.COUNTERPARTY]
            recovery_preparations.append(
                (journal.terminal("trade:ENTRY"), journal.terminal("price:ENTRY"))
            )
        return recovered_original_prepare(role, observation, **kwargs)

    monkeypatch.setattr(recovered, "_prepare", capture_recovery_preparation)
    report = asyncio.run(recovered.run())
    assert report.result is CoordinatorResult.COMPLETE
    assert recovery_preparations == [(external_trade.trade_id, str(external_trade.price))]
    assert recovery_venue.place_calls[0][0] is AccountRole.PRIMARY
    assert [role for role, _ in recovery_venue.place_calls] == [
        AccountRole.PRIMARY, AccountRole.COUNTERPARTY, AccountRole.PRIMARY,
    ]
    assert recovered._journals[AccountRole.COUNTERPARTY].terminal(
        "trade:ENTRY"
    ) == external_trade.trade_id
    assert recovered._journals[AccountRole.COUNTERPARTY].terminal(
        "price:ENTRY"
    ) == str(external_trade.price)
    assert propagation_sleep == [PROPAGATION_SETTLE_SECONDS]


@pytest.mark.parametrize("mutation", ["trade_id", "trade_price"])
def test_external_entry_maker_fill_restart_rejects_changed_identity(
    tmp_path: Path, propagation_sleep, monkeypatch, mutation: str,
):
    observations, rounds = lifecycle_observations()
    maker, external_trade, accepted_order_ids = install_external_entry_maker_fill(
        observations, rounds,
    )
    venue = PropagatingLifecycleVenue(
        observations, rounds, propagation_failures=1,
        propagation_order_id=maker.order_id,
        accepted_order_ids=accepted_order_ids,
    )
    coordinator = make_lifecycle(tmp_path, venue)
    original_set_phase = coordinator._set_phase

    def crash_after_external_reconciliation(phase):
        original_set_phase(phase)
        if phase is Phase.ENTRY_MAKER_RESTING:
            raise KeyboardInterrupt

    monkeypatch.setattr(coordinator, "_set_phase", crash_after_external_reconciliation)
    with pytest.raises(KeyboardInterrupt):
        asyncio.run(coordinator.run())
    counterparty = coordinator._journals[AccountRole.COUNTERPARTY]
    assert counterparty.terminal("trade:ENTRY") == external_trade.trade_id
    assert counterparty.terminal("price:ENTRY") == str(external_trade.price)
    for journal in coordinator._journals.values():
        journal.close()

    current = observations[1]
    current_counterparty = current.accounts[AccountRole.COUNTERPARTY]
    if mutation == "trade_id":
        changed_trade = replace(
            external_trade,
            trade_id=f"0x{302:016x}{1:016x}{1:016x}-{maker.order_id}",
        )
    else:
        changed_trade = replace(external_trade, price=Decimal("2999.02"))
    changed_counterparty = replace(
        current_counterparty, trades=(changed_trade,),
    )
    bad = replace(current, accounts={
        AccountRole.PRIMARY: current.accounts[AccountRole.PRIMARY],
        AccountRole.COUNTERPARTY: changed_counterparty,
    })
    recovery_venue = LifecycleVenue([bad], [], accepted_order_ids=())
    recovered = make_lifecycle(tmp_path, recovery_venue)
    report = asyncio.run(recovered.run())
    assert report.result is CoordinatorResult.FAILED_HALTED_MANUAL_RECOVERY
    assert report.failure_code == "RISEx external maker fill identity contradiction"
    assert report.primary_dispatches == 0
    assert report.counterparty_dispatches == 1
    assert recovery_venue.place_calls == []
    assert recovered._journals[AccountRole.COUNTERPARTY].terminal(
        "trade:ENTRY"
    ) == external_trade.trade_id
    assert recovered._journals[AccountRole.COUNTERPARTY].terminal(
        "price:ENTRY"
    ) == str(external_trade.price)
    assert propagation_sleep == [PROPAGATION_SETTLE_SECONDS]


def test_external_exit_maker_fill_is_reconciled_before_primary_dispatch(
    tmp_path: Path, propagation_sleep, monkeypatch,
):
    observations, rounds = lifecycle_observations()
    maker, external_trade, accepted_order_ids = install_external_exit_maker_fill(
        observations, rounds,
    )
    venue = PropagatingLifecycleVenue(
        observations, rounds, propagation_failures=1,
        propagation_order_id=maker.order_id, propagation_after_place=3,
        accepted_order_ids=accepted_order_ids,
    )
    coordinator = make_lifecycle(tmp_path, venue)
    preparation_states = []
    original_prepare = coordinator._prepare

    def capture_primary_preparation(role, observation, **kwargs):
        if role is AccountRole.PRIMARY and kwargs["step"] == "EXIT_TAKER":
            maker_intent = coordinator._journals[AccountRole.COUNTERPARTY].by_step(
                "EXIT_MAKER"
            )
            assert maker_intent is not None
            journal = coordinator._journals[AccountRole.COUNTERPARTY]
            preparation_states.append(
                (
                    maker_intent.state, maker_intent.filled_size,
                    maker_intent.reconciled,
                    observation.accounts[AccountRole.COUNTERPARTY].position,
                    observation.accounts[AccountRole.COUNTERPARTY].open_orders,
                    journal.terminal("trade:EXIT"),
                    journal.terminal("price:EXIT"),
                )
            )
        return original_prepare(role, observation, **kwargs)

    monkeypatch.setattr(coordinator, "_prepare", capture_primary_preparation)
    report = asyncio.run(coordinator.run())

    assert report.result is CoordinatorResult.COMPLETE
    assert [role for role, _ in venue.place_calls] == [
        AccountRole.COUNTERPARTY, AccountRole.PRIMARY,
        AccountRole.COUNTERPARTY, AccountRole.PRIMARY,
    ]
    assert preparation_states == [
        (
            "TERMINAL", Decimal("0.1"), True, Decimal("0"), (),
            external_trade.trade_id, str(external_trade.price),
        )
    ]
    maker_intent = coordinator._journals[AccountRole.COUNTERPARTY].by_step(
        "EXIT_MAKER"
    )
    assert maker_intent is not None
    assert maker_intent.state == "TERMINAL"
    assert maker_intent.filled_size == Decimal("0.1")
    assert maker_intent.reconciled is True
    assert coordinator._journals[AccountRole.COUNTERPARTY].terminal(
        "trade:EXIT"
    ) == external_trade.trade_id
    assert coordinator._journals[AccountRole.COUNTERPARTY].terminal(
        "price:EXIT"
    ) == str(external_trade.price)
    assert venue.observe_calls == 6
    assert propagation_sleep == [PROPAGATION_SETTLE_SECONDS]


@pytest.mark.parametrize(
    "mutation",
    [
        "cancelled",
        "partial",
        "missing_order",
        "missing_trade",
        "duplicate_trade",
        "wrong_position",
        "open_order",
        "open_contradiction",
        "wrong_order_market",
        "wrong_order_price",
        "wrong_order_client",
        "wrong_order_direction",
        "non_reduce_only",
        "wrong_trade_order",
        "wrong_trade_client",
        "wrong_trade_market",
        "wrong_trade_side",
        "wrong_trade_price",
        "wrong_trade_account",
        "extra_trade",
        "unrelated_order",
        "stale",
    ],
)
def test_external_exit_maker_fill_rejects_non_exact_evidence(
    tmp_path: Path, propagation_sleep, mutation: str,
):
    observations, rounds = lifecycle_observations()
    maker, matching_trade, accepted_order_ids = install_external_exit_maker_fill(
        observations, rounds,
    )
    venue = PropagatingLifecycleVenue(
        observations, rounds, propagation_failures=1,
        propagation_order_id=maker.order_id, propagation_after_place=3,
        accepted_order_ids=accepted_order_ids,
    )
    current = venue.observations[3]
    counterparty = current.accounts[AccountRole.COUNTERPARTY]
    filled = next(
        item for item in counterparty.history_orders
        if item.order_id == maker.order_id
    )

    if mutation == "cancelled":
        counterparty = replace(
            counterparty,
            history_orders=(replace(filled, status="CANCELLED", filled_size=Decimal("0")),),
        )
    elif mutation == "partial":
        partial = replace(filled, filled_size=Decimal("0.05"))
        counterparty = with_private(replace(
            counterparty,
            position=Decimal("-0.05"),
            history_orders=tuple(
                partial if item.order_id == maker.order_id else item
                for item in counterparty.history_orders
            ),
            trades=tuple(
                replace(matching_trade, size=Decimal("0.05"))
                if item.order_id == maker.order_id else item
                for item in counterparty.trades
            ),
        ))
    elif mutation == "missing_order":
        counterparty = replace(
            counterparty,
            history_orders=tuple(
                item for item in counterparty.history_orders
                if item.order_id != maker.order_id
            ),
        )
    elif mutation == "missing_trade":
        counterparty = replace(
            counterparty,
            trades=tuple(
                item for item in counterparty.trades
                if item.order_id != maker.order_id
            ),
        )
    elif mutation == "duplicate_trade":
        counterparty = replace(
            counterparty, trades=(*counterparty.trades, matching_trade),
        )
    elif mutation == "wrong_position":
        counterparty = with_private(replace(counterparty, position=Decimal("-0.1")))
    elif mutation == "open_order":
        counterparty = with_private(replace(counterparty, open_orders=(filled,)))
    elif mutation == "open_contradiction":
        counterparty = replace(
            counterparty, open_orders=(replace(filled, status="OPEN", filled_size=Decimal("0")),),
        )
    elif mutation == "wrong_order_market":
        counterparty = replace(
            counterparty,
            history_orders=(replace(filled, market_id=3),),
        )
    elif mutation == "wrong_order_price":
        counterparty = replace(
            counterparty,
            history_orders=(replace(filled, price=Decimal("2999.01")),),
        )
    elif mutation == "wrong_order_client":
        counterparty = replace(
            counterparty,
            history_orders=(replace(filled, client_order_id=1009),),
        )
    elif mutation == "wrong_order_direction":
        counterparty = replace(
            counterparty,
            history_orders=(replace(filled, side="SELL"),),
        )
    elif mutation == "non_reduce_only":
        counterparty = replace(
            counterparty,
            history_orders=(replace(filled, reduce_only=False),),
        )
    elif mutation == "wrong_trade_order":
        counterparty = replace(
            counterparty,
            trades=(replace(matching_trade, order_id=order(
                wide=499, client=4499, account=COUNTERPARTY, side="BUY",
                order_type="LIMIT", tif="GTC", status="FILLED", price="2999.00",
                filled="0.1", post_only=True, reduce_only=True,
            ).order_id),),
        )
    elif mutation == "wrong_trade_client":
        counterparty = replace(
            counterparty,
            trades=(replace(matching_trade, client_order_id=1009),),
        )
    elif mutation == "wrong_trade_market":
        counterparty = replace(
            counterparty,
            trades=(replace(matching_trade, market_id=3),),
        )
    elif mutation == "wrong_trade_side":
        counterparty = replace(
            counterparty,
            trades=(replace(matching_trade, side="SELL"),),
        )
    elif mutation == "wrong_trade_price":
        counterparty = replace(
            counterparty,
            trades=(replace(matching_trade, price=Decimal("2999.01")),),
        )
    elif mutation == "wrong_trade_account":
        counterparty = replace(
            counterparty,
            trades=(replace(matching_trade, account=ACCOUNT),),
        )
    elif mutation == "extra_trade":
        extra_order = order(
            wide=499, client=4499, account=COUNTERPARTY, side="BUY",
            order_type="LIMIT", tif="GTC", status="FILLED", price="2999.00",
            filled="0.1", post_only=True, reduce_only=True,
        )
        extra_trade = replace(
            matching_trade,
            trade_id=f"{maker.order_id}-{extra_order.order_id}",
        )
        counterparty = replace(
            counterparty, trades=(*counterparty.trades, extra_trade),
        )
    elif mutation == "unrelated_order":
        unrelated = order(
            wide=499, client=4499, account=COUNTERPARTY, side="BUY",
            order_type="LIMIT", tif="GTC", status="FILLED", price="2999.00",
            filled="0.1", post_only=True, reduce_only=True,
        )
        counterparty = replace(
            counterparty, history_orders=(*counterparty.history_orders, unrelated),
        )
    else:
        counterparty = replace(
            counterparty, observed_at=NOW - MAX_AGE_SECONDS - 1,
        )

    venue.observations[3] = pair_observation(
        current.accounts[AccountRole.PRIMARY], counterparty,
        book=current.market.book,
    )
    report = asyncio.run(make_lifecycle(tmp_path, venue).run())

    assert report.result is CoordinatorResult.FAILED_HALTED_MANUAL_RECOVERY
    assert report.primary_dispatches == 1
    assert report.counterparty_dispatches == 2
    assert [role for role, _ in venue.place_calls] == [
        AccountRole.COUNTERPARTY, AccountRole.PRIMARY, AccountRole.COUNTERPARTY,
    ]
    assert propagation_sleep == [PROPAGATION_SETTLE_SECONDS]


def test_external_exit_maker_fill_survives_restart_before_primary_preparation(
    tmp_path: Path, propagation_sleep, monkeypatch,
):
    observations, rounds = lifecycle_observations()
    maker, external_trade, accepted_order_ids = install_external_exit_maker_fill(
        observations, rounds,
    )
    venue = PropagatingLifecycleVenue(
        observations, rounds, propagation_failures=1,
        propagation_order_id=maker.order_id, propagation_after_place=3,
        accepted_order_ids=accepted_order_ids,
    )
    coordinator = make_lifecycle(tmp_path, venue)
    primary_preparations = []
    original_prepare = coordinator._prepare

    def capture_primary_preparation(role, observation, **kwargs):
        if role is AccountRole.PRIMARY and kwargs["step"] == "EXIT_TAKER":
            primary_preparations.append(True)
        return original_prepare(role, observation, **kwargs)

    monkeypatch.setattr(coordinator, "_prepare", capture_primary_preparation)
    original_set_phase = coordinator._set_phase

    def crash_after_external_reconciliation(phase):
        original_set_phase(phase)
        if phase is Phase.EXIT_MAKER_RESTING:
            raise KeyboardInterrupt

    monkeypatch.setattr(coordinator, "_set_phase", crash_after_external_reconciliation)
    with pytest.raises(KeyboardInterrupt):
        asyncio.run(coordinator.run())

    counterparty = coordinator._journals[AccountRole.COUNTERPARTY]
    maker_intent = counterparty.by_step("EXIT_MAKER")
    assert maker_intent is not None
    assert maker_intent.state == "TERMINAL"
    assert maker_intent.filled_size == Decimal("0.1")
    assert maker_intent.reconciled is True
    assert counterparty.terminal("trade:EXIT") == external_trade.trade_id
    assert counterparty.terminal("price:EXIT") == str(external_trade.price)
    assert primary_preparations == []
    assert coordinator.phase is Phase.EXIT_MAKER_RESTING
    assert [role for role, _ in venue.place_calls] == [
        AccountRole.COUNTERPARTY, AccountRole.PRIMARY, AccountRole.COUNTERPARTY,
    ]
    for journal in coordinator._journals.values():
        journal.close()

    exit_taker_id = next(
        item.order_id
        for item in observations[4].accounts[AccountRole.PRIMARY].history_orders
        if item.client_order_id == 2002
    )
    recovery_venue = LifecycleVenue(
        [observations[3], observations[4]], rounds,
        accepted_order_ids=(exit_taker_id,),
    )
    recovered = make_lifecycle(tmp_path, recovery_venue)
    report = asyncio.run(recovered.run())

    assert report.result is CoordinatorResult.COMPLETE
    assert recovery_venue.place_calls[0][0] is AccountRole.PRIMARY
    assert [role for role, _ in recovery_venue.place_calls] == [AccountRole.PRIMARY]
    assert recovered._journals[AccountRole.COUNTERPARTY].by_step(
        "EXIT_MAKER"
    ).dispatch_count == 1
    assert recovered._journals[AccountRole.PRIMARY].by_step(
        "EXIT_TAKER"
    ).dispatch_count == 1
    assert recovered._journals[AccountRole.COUNTERPARTY].terminal(
        "trade:EXIT"
    ) == external_trade.trade_id
    assert propagation_sleep == [PROPAGATION_SETTLE_SECONDS]


@pytest.mark.parametrize("mutation", ["trade_id", "trade_price"])
def test_external_exit_maker_fill_restart_rejects_changed_identity(
    tmp_path: Path, propagation_sleep, monkeypatch, mutation: str,
):
    observations, rounds = lifecycle_observations()
    maker, external_trade, accepted_order_ids = install_external_exit_maker_fill(
        observations, rounds,
    )
    venue = PropagatingLifecycleVenue(
        observations, rounds, propagation_failures=1,
        propagation_order_id=maker.order_id, propagation_after_place=3,
        accepted_order_ids=accepted_order_ids,
    )
    coordinator = make_lifecycle(tmp_path, venue)
    original_set_phase = coordinator._set_phase

    def crash_after_external_reconciliation(phase):
        original_set_phase(phase)
        if phase is Phase.EXIT_MAKER_RESTING:
            raise KeyboardInterrupt

    monkeypatch.setattr(coordinator, "_set_phase", crash_after_external_reconciliation)
    with pytest.raises(KeyboardInterrupt):
        asyncio.run(coordinator.run())
    counterparty = coordinator._journals[AccountRole.COUNTERPARTY]
    assert counterparty.terminal("trade:EXIT") == external_trade.trade_id
    assert counterparty.terminal("price:EXIT") == str(external_trade.price)
    for journal in coordinator._journals.values():
        journal.close()

    current = observations[3]
    current_counterparty = current.accounts[AccountRole.COUNTERPARTY]
    if mutation == "trade_id":
        changed_trade = replace(
            external_trade,
            trade_id=f"0x{302:016x}{1:016x}{1:016x}-{maker.order_id}",
        )
    else:
        changed_trade = replace(external_trade, price=Decimal("2999.01"))
    changed_counterparty = replace(
        current_counterparty, trades=(
            *tuple(
                item for item in current_counterparty.trades
                if item.order_id != maker.order_id
            ),
            changed_trade,
        ),
    )
    bad = replace(current, accounts={
        AccountRole.PRIMARY: current.accounts[AccountRole.PRIMARY],
        AccountRole.COUNTERPARTY: changed_counterparty,
    })
    recovery_venue = LifecycleVenue([bad], [], accepted_order_ids=())
    recovered = make_lifecycle(tmp_path, recovery_venue)
    report = asyncio.run(recovered.run())

    assert report.result is CoordinatorResult.FAILED_HALTED_MANUAL_RECOVERY
    assert report.primary_dispatches == 1
    assert report.counterparty_dispatches == 2
    expected_failure = (
        "RISEx external maker fill identity contradiction"
        if mutation == "trade_id"
        else "RISEx external exit maker fill trade binding rejected"
    )
    assert report.failure_code == expected_failure
    assert recovery_venue.place_calls == []
    assert recovered._journals[AccountRole.COUNTERPARTY].terminal(
        "trade:EXIT"
    ) == external_trade.trade_id
    assert recovered._journals[AccountRole.COUNTERPARTY].terminal(
        "price:EXIT"
    ) == str(external_trade.price)
    assert propagation_sleep == [PROPAGATION_SETTLE_SECONDS]


def test_process_death_after_durable_dispatch_never_replays(tmp_path: Path):
    observations, rounds = lifecycle_observations()
    venue = LifecycleVenue(observations, rounds, fail_on_place=2)
    coordinator = make_lifecycle(tmp_path, venue)
    with pytest.raises(KeyboardInterrupt):
        asyncio.run(coordinator.run())
    coordinator._journals[AccountRole.PRIMARY].close()
    coordinator._journals[AccountRole.COUNTERPARTY].close()
    recovery_venue = LifecycleVenue([], [])
    recovered = make_lifecycle(tmp_path, recovery_venue)
    report = asyncio.run(recovered.run())
    assert report.result is CoordinatorResult.FAILED_HALTED_MANUAL_RECOVERY
    assert recovery_venue.place_calls == []


def test_final_rest_round_allows_fresh_bbo_change(tmp_path: Path):
    observations, rounds = lifecycle_observations(
        second_final_book=BookObservation(
            bid=Decimal("2998.99"), ask=Decimal("2999.01"),
            bids=(BookLevel(Decimal("2998.99"), Decimal("1"), 1),),
            asks=(BookLevel(Decimal("2999.01"), Decimal("1"), 1),), observed_at=NOW,
        ),
    )
    report = asyncio.run(make_lifecycle(tmp_path, LifecycleVenue(observations, rounds)).run())
    assert report.result is CoordinatorResult.COMPLETE
    assert report.final_rounds == 2


def test_final_rest_round_allows_positive_balance_settlement(tmp_path: Path):
    observations, rounds = lifecycle_observations()
    final = rounds[1]
    primary = final.accounts[AccountRole.PRIMARY]
    assert primary.portfolio is not None
    settled = replace(
        primary.portfolio,
        usdc_balance=Decimal("999.50"),
        free_collateral=Decimal("998.50"),
        total_account_value=Decimal("999.75"),
    )
    rounds[1] = replace(final, accounts={
        AccountRole.PRIMARY: replace(primary, portfolio=settled),
        AccountRole.COUNTERPARTY: final.accounts[AccountRole.COUNTERPARTY],
    })
    report = asyncio.run(make_lifecycle(tmp_path, LifecycleVenue(observations, rounds)).run())
    assert report.result is CoordinatorResult.COMPLETE
    assert report.final_rounds == 2


@pytest.mark.parametrize("kind", ["position", "order", "trade"])
def test_final_rest_round_account_discrepancy_halts(tmp_path: Path, kind: str):
    observations, rounds = lifecycle_observations()
    final = rounds[1]
    primary = final.accounts[AccountRole.PRIMARY]
    if kind == "position":
        changed = replace(primary, position=Decimal("0.001"))
    elif kind == "order":
        extra = order(
            wide=415, client=4015, account=ACCOUNT, side="BUY",
            order_type="MARKET", tif="IOC", status="FILLED", price="0",
            filled="0.1",
        )
        changed = replace(primary, history_orders=(*primary.history_orders, extra))
    else:
        extra = trade(
            trade_id=f"{primary.history_orders[0].order_id}-{primary.history_orders[0].order_id}",
            item=primary.history_orders[0], side="BUY", price="2999.01",
        )
        changed = replace(primary, trades=(*primary.trades, extra))
    rounds[1] = replace(final, accounts={
        AccountRole.PRIMARY: changed,
        AccountRole.COUNTERPARTY: final.accounts[AccountRole.COUNTERPARTY],
    })

    report = asyncio.run(make_lifecycle(tmp_path, LifecycleVenue(observations, rounds)).run())
    assert report.result is CoordinatorResult.FAILED_HALTED_MANUAL_RECOVERY
    assert report.final_rounds == 1


def _primary_identity() -> RoleIdentity:
    return RoleIdentity(AccountRole.PRIMARY, ACCOUNT, SIGNER, "key", "marker", "journal")


def _raw_order(item: RestOrder) -> dict[str, object]:
    return {
        "id": item.order_id,
        "wide_order_id": item.wide_order_id,
        "resting_order_id": item.resting_order_id,
        "client_order_id": item.client_order_id,
        "market_id": item.market_id,
        "sender": item.account,
        "side": item.side,
        "type": item.order_type,
        "time_in_force": item.time_in_force,
        "status": item.status,
        "size": str(item.size),
        "filled_size": str(item.filled_size),
        "price": str(item.price),
        "post_only": item.post_only,
        "reduce_only": item.reduce_only,
        "is_liquidation": False,
    }


class _FlatAccountReads:
    """Fixture-only REST reads for the fixed account adapter."""

    def __init__(
        self, *, open_orders: tuple[RestOrder, ...] = (),
        history_orders: tuple[RestOrder, ...] = (),
        exact_order: RestOrder | None = None,
        trade_rows: tuple[dict[str, object], ...] = (),
        point_market_id: str = "0", point_size: str = "0",
        point_account: str | None = None,
        positions: tuple[tuple[int, str], ...] = (),
        positions_account: str | None = None,
        portfolio_positions: tuple[tuple[int, str], ...] | None = None,
    ):
        self.calls: list[tuple[str, tuple[tuple[str, str], ...]]] = []
        self.open_orders = open_orders
        self.history_orders = history_orders
        self.exact_order = exact_order
        self.trade_rows = trade_rows
        self.point_market_id = point_market_id
        self.point_size = point_size
        self.point_account = point_account
        self.positions = positions
        self.positions_account = positions_account
        self.portfolio_positions = portfolio_positions

    async def get(self, path, query=()):
        query = tuple(query)
        self.calls.append((path, query))
        params = dict(query)
        account = params.get("account")
        if account is None and path.startswith("/v1/nonce-state/"):
            account = path.rsplit("/", 1)[-1]
        signer = SIGNER if account == ACCOUNT else FIXED_COUNTERPARTY_SIGNER
        if path == "/v1/auth/session-key-status":
            data = {"status": 1, "status_description": "Active"}
        elif path == "/v1/auth/signers":
            data = {"signers": [{"signer": signer, "status": "Active"}]}
        elif path == "/v1/orders/open":
            data = {
                "account": account, "market_id": "0",
                "orders": [_raw_compact_open_order(item) for item in self.open_orders],
                "total_orders": str(len(self.open_orders)),
            }
        elif path.startswith("/v1/orders/by-id/"):
            if self.exact_order is None:
                raise AssertionError(path)
            data = {"order": _raw_order(self.exact_order)}
        elif path == HISTORY_PATH:
            data = {
                "orders": [_raw_order(item) for item in self.history_orders],
                "page": 1, "has_next_page": False,
            }
        elif path == TRADES_PATH:
            data = {
                "trades": list(self.trade_rows),
                "page": 1, "has_next_page": False,
            }
        elif path == "/v1/account/position":
            point = {
                "market_id": self.point_market_id, "size": self.point_size,
            }
            if self.point_account is not None:
                point["account"] = self.point_account
            data = {"position": point}
        elif path == "/v1/positions":
            position_account = self.positions_account or account
            data = {"positions": [
                {
                    "account": position_account,
                    "market_id": market_id,
                    "size": size,
                }
                for market_id, size in self.positions
            ]}
        elif path == PORTFOLIO_PATH:
            portfolio_positions = self.portfolio_positions
            if portfolio_positions is None:
                portfolio_positions = tuple(
                    (market_id, "0") for market_id in range(1, 20)
                )
            data = {
                "account": account,
                "positions": [
                    {"market_id": market_id, "size": size}
                    for market_id, size in portfolio_positions
                ],
                "summary": {
                    "usdc_balance": "1000", "free_collateral": "1000",
                    "total_account_value": "1000", "in_liquidation": False,
                    "risk_level": "NORMAL",
                },
            }
        elif path.startswith("/v1/nonce-state/"):
            data = {
                "nonce_anchor": "1", "current_bitmap_index": 0,
                "bitmap": "0x0",
            }
        else:
            raise AssertionError(path)
        return _HTTPObservation(200, "", {"data": data, "request_id": path}, NOW)


def _flat_read_venue(transport: _FlatAccountReads, *, known_order_ids=()):
    counterparty = RoleIdentity(
        AccountRole.COUNTERPARTY, FIXED_COUNTERPARTY_ACCOUNT,
        FIXED_COUNTERPARTY_SIGNER, "key", "marker", "journal",
    )
    return FixedRisexTwoAccountVenue(
        identities={
            AccountRole.PRIMARY: _primary_identity(),
            AccountRole.COUNTERPARTY: counterparty,
        },
        credential_loaders={
            AccountRole.PRIMARY: lambda: None,
            AccountRole.COUNTERPARTY: lambda: None,
        },
        transport=transport,
        now=lambda: NOW,
        known_order_ids={AccountRole.PRIMARY: tuple(known_order_ids)},
    )


def _raw_compact_open_order(item: RestOrder, *, strings: bool = False) -> dict[str, object]:
    values: dict[str, object] = {
        "account": item.account,
        "client_order_id": item.client_order_id,
        "market_id": item.market_id,
        "order_id": item.order_id,
        "order_type": {"MARKET": 0, "LIMIT": 1}[item.order_type],
        "post_only": item.post_only,
        "price_ticks": int(item.price / Decimal("0.01")),
        "reduce_only": item.reduce_only,
        "resting_order_id": item.resting_order_id,
        "side": {"BUY": 0, "SELL": 1}[item.side],
        "size_steps": int(item.size / Decimal("0.001")),
        "time_in_force": {"GTC": 0, "IOC": 3}[item.time_in_force],
        "wide_order_id": item.wide_order_id,
    }
    if strings:
        for key in (
            "client_order_id", "market_id", "order_type", "price_ticks",
            "resting_order_id", "side", "size_steps", "time_in_force",
            "wide_order_id",
        ):
            values[key] = str(values[key])
    return values


def _open_orders_response(
    account: str, orders: tuple[dict[str, object], ...] = (), *,
    market_id: object = "0", total_orders: object = "0",
) -> _HTTPObservation:
    return _HTTPObservation(
        200, "", {"data": {
            "account": account,
            "market_id": market_id,
            "orders": list(orders),
            "total_orders": total_orders,
        }, "request_id": "/v1/orders/open"}, NOW,
    )


def test_compact_open_order_fixture_binds_observed_contract_and_synthesizes_state():
    item = order(
        wide=0x339EF, client=11160469709073598394, account=ACCOUNT,
        side="SELL", order_type="LIMIT", tif="GTC", status="OPEN",
        price="2363.04", post_only=True,
    )
    parsed = _parse_open_orders(
        _open_orders_response(ACCOUNT, (_raw_compact_open_order(item),)),
        _primary_identity(),
    )
    assert len(parsed) == 1
    assert parsed[0].order_id == item.order_id
    assert parsed[0].status == "OPEN"
    assert parsed[0].filled_size == Decimal("0")
    assert parsed[0].size == Decimal("0.1")
    assert parsed[0].price == Decimal("2363.04")


@pytest.mark.parametrize("strings", [False, True])
@pytest.mark.parametrize("market_id", [0, "0"])
def test_compact_open_order_accepts_canonical_uint_string_forms(strings, market_id):
    item = order(
        wide=205, client=4205, account=ACCOUNT, side="SELL",
        order_type="LIMIT", tif="GTC", status="OPEN", price="2363.04",
        post_only=True,
    )
    parsed = _parse_open_orders(
        _open_orders_response(
            ACCOUNT, (_raw_compact_open_order(item, strings=strings),),
            market_id=market_id, total_orders=0,
        ),
        _primary_identity(),
    )
    assert parsed[0].client_order_id == 4205


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda data: data.pop("account"), id="missing-account"),
        pytest.param(lambda data: data.__setitem__("account", "not-an-address"), id="malformed-account"),
        pytest.param(lambda data: data.__setitem__("account", COUNTERPARTY), id="unrelated-account"),
        pytest.param(lambda data: data.__setitem__("market_id", "2"), id="filtered-market"),
        pytest.param(lambda data: data.__setitem__("market_id", "00"), id="noncanonical-market"),
        pytest.param(lambda data: data.__setitem__("total_orders", "00"), id="noncanonical-count"),
        pytest.param(lambda data: data.__setitem__("total_orders", -1), id="negative-count"),
        pytest.param(lambda data: data.__setitem__("total_orders", 257), id="oversized-count"),
        pytest.param(lambda data: data.__setitem__("orders", {}), id="orders-not-list"),
        pytest.param(lambda data: data.pop("total_orders"), id="missing-count"),
    ],
)
def test_compact_open_order_wrapper_rejects_identity_market_and_count_contradictions(mutate):
    item = order(
        wide=206, client=4206, account=ACCOUNT, side="SELL",
        order_type="LIMIT", tif="GTC", status="OPEN", price="2363.04",
        post_only=True,
    )
    response = _open_orders_response(ACCOUNT, (_raw_compact_open_order(item),))
    data = copy.deepcopy(response.body["data"])
    mutate(data)
    broken = replace(response, body={"data": data, "request_id": "/v1/orders/open"})
    with pytest.raises(CoordinatorSafetyError):
        _parse_open_orders(broken, _primary_identity())


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda row: row.pop("price_ticks"), id="missing-field"),
        pytest.param(lambda row: row.__setitem__("side", 2), id="side-enum"),
        pytest.param(lambda row: row.__setitem__("side", "SELL"), id="named-side-enum"),
        pytest.param(lambda row: row.__setitem__("order_type", 2), id="type-enum"),
        pytest.param(lambda row: row.__setitem__("time_in_force", 4), id="tif-enum"),
        pytest.param(lambda row: row.__setitem__("size_steps", "100.0"), id="size-grid"),
        pytest.param(lambda row: row.__setitem__("price_ticks", "236304.0"), id="price-grid"),
        pytest.param(lambda row: row.__setitem__("post_only", "true"), id="post-only-flag"),
        pytest.param(lambda row: row.__setitem__("reduce_only", 0), id="reduce-only-flag"),
        pytest.param(lambda row: row.__setitem__("wide_order_id", "208"), id="wide-identity"),
        pytest.param(lambda row: row.__setitem__("resting_order_id", "102"), id="resting-identity"),
        pytest.param(lambda row: row.__setitem__("order_id", "0x" + "00" * 24), id="composite-identity"),
    ],
)
def test_compact_open_order_row_rejects_malformed_missing_enum_grid_and_composite_fields(mutate):
    item = order(
        wide=207, client=4207, account=ACCOUNT, side="SELL",
        order_type="LIMIT", tif="GTC", status="OPEN", price="2363.04",
        post_only=True,
    )
    row = _raw_compact_open_order(item)
    mutate(row)
    with pytest.raises(CoordinatorSafetyError):
        _parse_open_orders(
            _open_orders_response(ACCOUNT, (row,)), _primary_identity(),
        )


def test_compact_open_order_row_accepts_additive_irrelevant_fields():
    item = order(
        wide=208, client=4208, account=ACCOUNT, side="SELL",
        order_type="LIMIT", tif="GTC", status="OPEN", price="2363.04",
        post_only=True,
    )
    row = _raw_compact_open_order(item)
    row["unexpected"] = {"venue": "ignored"}
    parsed = _parse_open_orders(
        _open_orders_response(ACCOUNT, (row,)), _primary_identity(),
    )
    assert parsed[0].order_id == item.order_id


def test_official_rest_trade_and_market_order_contracts_are_sealed():
    market_order = order(
        wide=401, client=4001, account=ACCOUNT, side="BUY",
        order_type="MARKET", tif="IOC", status="FILLED", price="3000",
        filled="0.1",
    )
    parsed = _parse_order_row(_raw_order(market_order), _primary_identity(), NOW)
    assert parsed.price == Decimal("0")
    bounded_raw = _raw_order(market_order)
    bounded_raw["price"] = "2607.66"
    bounded = _parse_order_row(bounded_raw, _primary_identity(), NOW)
    assert bounded.price == Decimal("2607.66")
    official_id = f"{market_order.order_id}-{market_order.order_id}"
    parsed_trade = _parse_trade_row({
        "id": official_id,
        "order_id": market_order.order_id,
        "client_order_id": market_order.client_order_id,
        "market_id": 2,
        "wallet_address": ACCOUNT,
        "side": "BUY",
        "size": "0.1",
        "price": "2999.01",
    }, _primary_identity(), NOW)
    assert parsed_trade.trade_id == official_id
    with pytest.raises(CoordinatorSafetyError):
        _parse_trade_row({
            "id": "trade-entry", "match_id": "invented",
            "order_id": market_order.order_id,
            "client_order_id": market_order.client_order_id,
            "market_id": 2, "wallet_address": ACCOUNT,
            "side": "BUY", "size": "0.1", "price": "2999.01",
        }, _primary_identity(), NOW)


def test_account_scoped_trade_binds_omitted_identity_and_tolerates_additive_fields():
    item = order(
        wide=420, client=4020, account=ACCOUNT, side="BUY",
        order_type="MARKET", tif="IOC", status="FILLED", price="0",
        filled="0.1",
    )
    row = {
        "id": f"{item.order_id}-{item.order_id}",
        "order_id": item.order_id,
        "client_order_id": item.client_order_id,
        "market_id": 2,
        "side": "BUY",
        "size": "0.1",
        "price": "2999.01",
        "ignored": {"additive": True},
    }
    with pytest.raises(CoordinatorSafetyError):
        _parse_trade_row(row, _primary_identity(), NOW)
    parsed = _parse_trade_row(
        row, _primary_identity(), NOW, account_scoped=True,
    )
    assert parsed.account == ACCOUNT
    for field in ("wallet_address", "sender", "account"):
        with_identity = dict(row, **{field: ACCOUNT})
        assert _parse_trade_row(
            with_identity, _primary_identity(), NOW, account_scoped=True,
        ).account == ACCOUNT


@pytest.mark.parametrize("field", ["wallet_address", "sender", "account"])
def test_account_scoped_trade_rejects_conflicting_or_malformed_identity(field):
    item = order(
        wide=421, client=4021, account=ACCOUNT, side="BUY",
        order_type="MARKET", tif="IOC", status="FILLED", price="0",
        filled="0.1",
    )
    row = {
        "id": f"{item.order_id}-{item.order_id}",
        "order_id": item.order_id,
        "client_order_id": item.client_order_id,
        "market_id": 2,
        "side": "BUY",
        "size": "0.1",
        "price": "2999.01",
        field: COUNTERPARTY if field != "sender" else "not-an-address",
    }
    with pytest.raises(CoordinatorSafetyError):
        _parse_trade_row(row, _primary_identity(), NOW, account_scoped=True)


def test_account_scoped_trade_rejects_conflicting_multiple_identity_fields():
    item = order(
        wide=422, client=4022, account=ACCOUNT, side="BUY",
        order_type="MARKET", tif="IOC", status="FILLED", price="0",
        filled="0.1",
    )
    row = {
        "id": f"{item.order_id}-{item.order_id}",
        "order_id": item.order_id,
        "client_order_id": item.client_order_id,
        "market_id": 2,
        "wallet_address": ACCOUNT,
        "sender": COUNTERPARTY,
        "side": "BUY",
        "size": "0.1",
        "price": "2999.01",
    }
    with pytest.raises(CoordinatorSafetyError):
        _parse_trade_row(row, _primary_identity(), NOW, account_scoped=True)


@pytest.mark.parametrize(
    "field",
    ["id", "order_id", "client_order_id", "market_id", "side", "size", "price"],
)
def test_account_scoped_trade_still_requires_every_identity_and_economic_field(field):
    item = order(
        wide=423, client=4023, account=ACCOUNT, side="BUY",
        order_type="MARKET", tif="IOC", status="FILLED", price="0",
        filled="0.1",
    )
    row = {
        "id": f"{item.order_id}-{item.order_id}",
        "order_id": item.order_id,
        "client_order_id": item.client_order_id,
        "market_id": 2,
        "side": "BUY",
        "size": "0.1",
        "price": "2999.01",
    }
    row.pop(field)
    with pytest.raises(CoordinatorSafetyError):
        _parse_trade_row(row, _primary_identity(), NOW, account_scoped=True)


def test_fixed_account_adapter_binds_omitted_trade_identity_on_sealed_read():
    item = order(
        wide=424, client=4024, account=ACCOUNT, side="BUY",
        order_type="MARKET", tif="IOC", status="FILLED", price="0",
        filled="0.1",
    )
    transport = _FlatAccountReads(trade_rows=(
        {
            "id": f"{item.order_id}-{item.order_id}",
            "order_id": item.order_id,
            "client_order_id": item.client_order_id,
            "market_id": 2,
            "side": "BUY", "size": "0.1", "price": "2999.01",
            "venue_additive": "ignored",
        },
    ))
    snapshot, _, _ = asyncio.run(
        _flat_read_venue(transport)._account(AccountRole.PRIMARY, include_private=False),
    )
    assert snapshot.trades[0].account == ACCOUNT


def test_market_order_binding_accepts_zero_or_signed_bound_only(tmp_path: Path):
    market_order = order(
        wide=412, client=4012, account=ACCOUNT, side="BUY",
        order_type="MARKET", tif="IOC", status="FILLED", price="0",
        filled="0.1",
    )
    bounded = replace(market_order, price=Decimal("2607.66"))
    coordinator = TwoAccountCoordinator._fixture(
        venue=FakeVenue(),
        primary_journal=tmp_path / "primary.sqlite3",
        counterparty_journal=tmp_path / "counterparty.sqlite3",
        now=lambda: NOW,
    )
    intent = DurableIntent(
        intent_id="intent", ordinal=1, step="ENTRY_TAKER", client_order_id=4012,
        nonce_anchor=1, nonce_bitmap=0, payload_digest="payload", bbo_digest="bbo",
        state="DISPATCHED", side="BUY", order_type="MARKET", time_in_force="IOC",
        reduce_only=False, post_only=False, market_id=2, size=Decimal("0.1"),
        price=Decimal("2607.66"), source_position=Decimal("0"), expires_at=NOW + 30,
        dispatch_count=1, order_id=market_order.order_id, filled_size=Decimal("0.1"),
        reconciled=False,
    )
    coordinator._ensure_order_matches(intent, market_order, AccountRole.PRIMARY)
    coordinator._ensure_order_matches(intent, bounded, AccountRole.PRIMARY)
    with pytest.raises(CoordinatorSafetyError):
        coordinator._ensure_order_matches(
            intent, replace(market_order, price=Decimal("2607.67")),
            AccountRole.PRIMARY,
        )


def test_stale_rest_order_and_response_are_rejected():
    item = order(
        wide=402, client=4002, account=ACCOUNT, side="BUY",
        order_type="LIMIT", tif="GTC", status="OPEN", price="2999.00",
    )
    stale = replace(item, observed_at=NOW - MAX_AGE_SECONDS - 1)
    with pytest.raises(CoordinatorSafetyError):
        _validate_order(stale, _primary_identity(), NOW)
    with pytest.raises(CoordinatorSafetyError):
        _require_recent(_HTTPObservation(200, "", {}, NOW - MAX_AGE_SECONDS - 1), NOW, "test")


def test_private_snapshot_must_match_rest_current_state(tmp_path: Path):
    venue = FakeVenue()
    coordinator = TwoAccountCoordinator._fixture(
        venue=venue,
        primary_journal=tmp_path / "primary.sqlite3",
        counterparty_journal=tmp_path / "counterparty.sqlite3",
        now=lambda: NOW,
    )
    base = account_snapshot(AccountRole.PRIMARY)
    bad_private = replace(
        base.private,
        positions_snapshot=((2, Decimal("0.1")),),
    )
    bad = replace(base, private=bad_private)
    with pytest.raises(CoordinatorSafetyError):
        coordinator._validate_observation(observation(primary=bad))


def test_private_position_snapshots_allow_observed_additive_zero_rows_and_absence(
    tmp_path: Path,
):
    primary = with_private_positions(
        account_snapshot(AccountRole.PRIMARY),
        ((1, Decimal("0")), (29, Decimal("0")), (2, Decimal("0"))),
    )
    counterparty = with_private_positions(account_snapshot(AccountRole.COUNTERPARTY), ())
    coordinator = TwoAccountCoordinator._fixture(
        venue=FakeVenue(),
        primary_journal=tmp_path / "primary.sqlite3",
        counterparty_journal=tmp_path / "counterparty.sqlite3",
        now=lambda: NOW,
    )
    coordinator._validate_observation(
        observation(primary=primary, counterparty=counterparty),
    )


def test_private_position_snapshot_reconciles_fixed_market_to_rest_position(
    tmp_path: Path,
):
    primary = with_private_positions(
        account_snapshot(AccountRole.PRIMARY, position="0.1"),
        ((1, Decimal("0")), (29, Decimal("0")), (2, Decimal("0.1"))),
    )
    coordinator = TwoAccountCoordinator._fixture(
        venue=FakeVenue(),
        primary_journal=tmp_path / "primary.sqlite3",
        counterparty_journal=tmp_path / "counterparty.sqlite3",
        now=lambda: NOW,
    )
    coordinator._validate_observation(observation(primary=primary))


def test_private_position_snapshot_absence_means_zero_against_rest_position(
    tmp_path: Path,
):
    primary = with_private_positions(
        account_snapshot(AccountRole.PRIMARY, position="0.1"), (),
    )
    coordinator = TwoAccountCoordinator._fixture(
        venue=FakeVenue(),
        primary_journal=tmp_path / "primary.sqlite3",
        counterparty_journal=tmp_path / "counterparty.sqlite3",
        now=lambda: NOW,
    )
    with pytest.raises(CoordinatorSafetyError):
        coordinator._validate_observation(observation(primary=primary))


@pytest.mark.parametrize(
    "snapshot",
    [
        ((1, Decimal("0.001")), (2, Decimal("0"))),
        ((1, Decimal("0")), (1, Decimal("0")), (2, Decimal("0"))),
        ((2, Decimal("0.1")),),
        ((29, Decimal("0.0005")), (2, Decimal("0"))),
        ((0, Decimal("0")), (2, Decimal("0"))),
    ],
    ids=["unrelated-nonzero", "duplicate-market", "fixed-mismatch", "off-grid", "nonpositive-market"],
)
def test_private_position_snapshots_reject_untrusted_rows(
    tmp_path: Path, snapshot: tuple[tuple[int, Decimal], ...],
):
    primary = with_private_positions(account_snapshot(AccountRole.PRIMARY), snapshot)
    coordinator = TwoAccountCoordinator._fixture(
        venue=FakeVenue(),
        primary_journal=tmp_path / "primary.sqlite3",
        counterparty_journal=tmp_path / "counterparty.sqlite3",
        now=lambda: NOW,
    )
    with pytest.raises(CoordinatorSafetyError):
        coordinator._validate_observation(observation(primary=primary))


def test_private_position_updates_reject_contradictory_fixed_market_state(
    tmp_path: Path,
):
    primary = with_private_positions(
        account_snapshot(AccountRole.PRIMARY), (),
        updates=((2, Decimal("0")), (2, Decimal("0.1"))),
    )
    coordinator = TwoAccountCoordinator._fixture(
        venue=FakeVenue(),
        primary_journal=tmp_path / "primary.sqlite3",
        counterparty_journal=tmp_path / "counterparty.sqlite3",
        now=lambda: NOW,
    )
    with pytest.raises(CoordinatorSafetyError):
        coordinator._validate_observation(observation(primary=primary))


def test_rest_open_history_overlap_is_allowed_only_when_semantically_equal(tmp_path: Path):
    maker = order(
        wide=403, client=4003, account=COUNTERPARTY, side="SELL",
        order_type="LIMIT", tif="GTC", status="OPEN", price="2999.01",
        post_only=True,
    )
    equal = with_private(replace(
        account_snapshot(AccountRole.COUNTERPARTY, orders=(maker,)),
        history_orders=(maker,),
    ))
    coordinator = make_lifecycle(tmp_path, FakeVenue())
    coordinator._validate_observation(observation(counterparty=equal))
    disagreement = replace(equal, history_orders=(replace(maker, status="CANCELLED"),))
    with pytest.raises(CoordinatorSafetyError):
        coordinator._validate_observation(observation(counterparty=disagreement))


def test_rest_overlap_selector_deduplicates_agreeing_order_for_residue_lookup():
    maker = order(
        wide=404, client=4004, account=COUNTERPARTY, side="SELL",
        order_type="LIMIT", tif="GTC", status="OPEN", price="2999.01",
        post_only=True,
    )
    value = replace(
        account_snapshot(AccountRole.COUNTERPARTY, orders=(maker,)),
        history_orders=(replace(maker),),
    )

    assert _all_orders(value) == (maker,)
    assert _order_for(value, maker.client_order_id) == maker


def test_equal_but_wrong_mutual_trade_sizes_halt(tmp_path: Path):
    observations, rounds = lifecycle_observations()
    current = observations[2]
    primary = current.accounts[AccountRole.PRIMARY]
    counterparty = current.accounts[AccountRole.COUNTERPARTY]
    primary = replace(primary, trades=(replace(primary.trades[0], size=Decimal("0.05")),))
    counterparty = replace(counterparty, trades=(replace(counterparty.trades[0], size=Decimal("0.05")),))
    observations[2] = replace(current, accounts={
        AccountRole.PRIMARY: with_private(primary),
        AccountRole.COUNTERPARTY: with_private(counterparty),
    })
    report = asyncio.run(make_lifecycle(tmp_path, LifecycleVenue(observations, rounds)).run())
    assert report.result is CoordinatorResult.FAILED_HALTED_MANUAL_RECOVERY
    assert report.failure_code == "RISEx mutual trade identity rejected"


def test_prior_history_is_bound_as_baseline_and_new_unrelated_history_halts(tmp_path: Path):
    observations, rounds = lifecycle_observations()
    old_order = order(
        wide=405, client=4005, account=ACCOUNT, side="BUY",
        order_type="MARKET", tif="IOC", status="FILLED", price="0", filled="0",
    )
    old_trade = trade(
        trade_id=f"{old_order.order_id}-{old_order.order_id}",
        item=old_order, side="BUY", price="2998.00",
    )
    def add_baseline(value: AccountSnapshot) -> AccountSnapshot:
        if value.role is not AccountRole.PRIMARY:
            return value
        return with_private(replace(
            value,
            history_orders=(old_order, *value.history_orders),
            trades=(old_trade, *value.trades),
        ))
    observations = [
        replace(item, accounts={
            AccountRole.PRIMARY: add_baseline(item.accounts[AccountRole.PRIMARY]),
            AccountRole.COUNTERPARTY: item.accounts[AccountRole.COUNTERPARTY],
        }) for item in observations
    ]
    rounds = [
        replace(item, accounts={
            AccountRole.PRIMARY: add_baseline(item.accounts[AccountRole.PRIMARY]),
            AccountRole.COUNTERPARTY: item.accounts[AccountRole.COUNTERPARTY],
        }) for item in rounds
    ]
    report = asyncio.run(make_lifecycle(tmp_path, LifecycleVenue(observations, rounds)).run())
    assert report.result is CoordinatorResult.COMPLETE
    bad_observations, bad_rounds = lifecycle_observations()
    unrelated = order(
        wide=407, client=4007, account=ACCOUNT, side="BUY",
        order_type="MARKET", tif="IOC", status="FILLED", price="0", filled="0",
    )
    for index, item in enumerate(bad_rounds):
        primary = item.accounts[AccountRole.PRIMARY]
        bad_rounds[index] = replace(item, accounts={
            AccountRole.PRIMARY: with_private(replace(
                primary, history_orders=(*primary.history_orders, unrelated),
            )),
            AccountRole.COUNTERPARTY: item.accounts[AccountRole.COUNTERPARTY],
        })
    unrelated_path = tmp_path / "unrelated"
    unrelated_path.mkdir()
    report = asyncio.run(make_lifecycle(unrelated_path, LifecycleVenue(bad_observations, bad_rounds)).run())
    assert report.result is CoordinatorResult.FAILED_HALTED_MANUAL_RECOVERY


def test_official_pagination_is_complete_bounded_and_monotonic():
    identity = _primary_identity()

    class Paged:
        def __init__(self, bad_page: int | None = None, stale: bool = False):
            self.calls = []
            self.bad_page = bad_page
            self.stale = stale

        async def get(self, path, query):
            self.calls.append((path, query))
            requested = int(dict(query)["page"])
            page = self.bad_page if self.bad_page is not None else requested
            body = {
                "data": {
                    "orders": [], "page": page,
                    "has_next_page": requested == 1,
                },
                "request_id": f"r-{requested}",
            }
            timestamp = NOW - MAX_AGE_SECONDS - 1 if self.stale else NOW
            return _HTTPObservation(200, "", body, timestamp)

    transport = Paged()
    rows, observed_at = asyncio.run(
        _paged_rows(transport, HISTORY_PATH, identity, "orders", lambda: NOW),
    )
    assert rows == () and observed_at == NOW
    assert transport.calls == [
        (HISTORY_PATH, (
            ("account", ACCOUNT), ("market_id", "2"),
            ("page", "1"), ("limit", str(PAGE_LIMIT)),
        )),
        (HISTORY_PATH, (
            ("account", ACCOUNT), ("market_id", "2"),
            ("page", "2"), ("limit", str(PAGE_LIMIT)),
        )),
    ]
    with pytest.raises(CoordinatorSafetyError):
        asyncio.run(_paged_rows(Paged(bad_page=3), HISTORY_PATH, identity, "orders", lambda: NOW))
    with pytest.raises(CoordinatorSafetyError):
        asyncio.run(_paged_rows(Paged(stale=True), HISTORY_PATH, identity, "orders", lambda: NOW))


def test_portfolio_details_require_positive_normal_state_and_position_binding():
    identity = _primary_identity()
    position_rows = [{"market_id": market_id, "size": "0"} for market_id in range(1, 20)]
    value = {
        "account": ACCOUNT,
        "positions": position_rows,
        "summary": {
            "usdc_balance": "1000", "free_collateral": "1000",
            "total_account_value": "1000", "in_liquidation": False,
            "risk_level": "NORMAL",
        },
    }
    portfolio, positions = _parse_portfolio(value, identity, NOW)
    assert portfolio.free_collateral == Decimal("1000")
    assert len(positions) == 19
    for field, bad in (
        ("usdc_balance", "0"),
        ("free_collateral", "-1"),
        ("total_account_value", "0"),
    ):
        broken = json.loads(json.dumps(value))
        broken["summary"][field] = bad
        with pytest.raises(CoordinatorSafetyError):
            _parse_portfolio(broken, identity, NOW)
    broken = json.loads(json.dumps(value))
    broken["summary"]["in_liquidation"] = True
    with pytest.raises(CoordinatorSafetyError):
        _parse_portfolio(broken, identity, NOW)
    broken = json.loads(json.dumps(value))
    broken["positions"][2]["size"] = "0.001"
    portfolio, broken_positions = _parse_portfolio(broken, identity, NOW)
    assert portfolio.account == ACCOUNT
    assert broken_positions[2] == (3, Decimal("0.001"))


@pytest.mark.parametrize(
    ("raw", "expected"),
    (
        ("0", Decimal("0")),
        ("1000000000000000", Decimal("0.001")),
        ("100000000000000000", Decimal("0.1")),
        ("-100000000000000000", Decimal("-0.1")),
    ),
)
def test_full_position_sources_decode_canonical_18_decimal_atomic_sizes(raw, expected):
    identity = _primary_identity()
    assert _position_rows({
        "positions": [{"account": ACCOUNT, "market_id": "2", "size": raw}],
    }, identity) == ((2, expected),)


@pytest.mark.parametrize(
    ("raw", "expected"),
    (
        ("0", Decimal("0")),
        ("0.1", Decimal("0.1")),
        ("-0.1", Decimal("-0.1")),
        ("0.001", Decimal("0.001")),
    ),
)
def test_full_position_sources_and_point_route_agree_on_endpoint_units(raw, expected):
    identity = _primary_identity()
    atomic = {
        "0": "0",
        "0.1": "100000000000000000",
        "-0.1": "-100000000000000000",
        "0.001": "1000000000000000",
    }[raw]
    assert _position_rows({
        "positions": [{"account": ACCOUNT, "market_id": "2", "size": atomic}],
    }, identity) == ((2, expected),)
    assert _portfolio_position_rows([
        {"market_id": "2", "size": raw},
    ]) == ((2, expected),)
    assert _point_position(
        {"market_id": "2", "size": raw}, identity,
    ) == (2, expected)


@pytest.mark.parametrize(
    "raw",
    (
        "100000000000000000", "-100000000000000000", "1e17", "-1e17",
        "0.1005", "NaN", "Infinity", "-Infinity",
    ),
)
def test_portfolio_position_rows_reject_atomic_ambiguous_off_grid_and_nonfinite_sizes(raw):
    with pytest.raises(CoordinatorSafetyError):
        _portfolio_position_rows([{"market_id": 2, "size": raw}])


def test_portfolio_position_rows_use_the_strict_uint32_size_bound():
    maximum = _MAX_PORTFOLIO_POSITION_SIZE
    assert _portfolio_position_rows([
        {"market_id": 2, "size": str(maximum)},
        {"market_id": 3, "size": str(-maximum)},
    ]) == ((2, maximum), (3, -maximum))
    with pytest.raises(CoordinatorSafetyError):
        _portfolio_position_rows([
            {"market_id": 2, "size": str(maximum + MARKET_STEP)},
        ])


@pytest.mark.parametrize(
    "raw",
    (
        "0.1", "-0.1", "+100000000000000000", "01",
        "-0", "1e17", "100000000000000001", str(2**255),
    ),
)
def test_full_position_sources_reject_decimal_ambiguous_off_grid_and_bounded_sizes(raw):
    identity = _primary_identity()
    with pytest.raises(CoordinatorSafetyError):
        _position_rows({
            "positions": [{"account": ACCOUNT, "market_id": 2, "size": raw}],
        }, identity)


def test_atomic_position_size_accepts_only_the_signed_int256_grid_bounds():
    step = 10**15
    maximum = (2**255 - 1) // step * step
    assert _atomic_position_size(str(maximum))
    assert _atomic_position_size(str(-maximum))
    with pytest.raises(CoordinatorSafetyError):
        _atomic_position_size(str(maximum + step))
    with pytest.raises(CoordinatorSafetyError):
        _atomic_position_size(str(-maximum - step))


def test_point_position_remains_human_decimal_and_route_alias_is_not_global():
    identity = _primary_identity()
    assert _point_position({"market_id": "2", "size": "0.1"}, identity) == (
        2, Decimal("0.1"),
    )
    assert _point_position(
        {"market_id": "0", "size": "0.1"}, identity, allow_route_alias=True,
    ) == (0, Decimal("0.1"))
    assert _point_position(
        {"market_id": "2", "size": "100000000000000000"}, identity,
    ) == (2, Decimal("100000000000000000"))


def test_fixed_account_adapter_accepts_market_zero_alias_only_after_atomic_cross_read():
    atomic = "100000000000000000"
    transport = _FlatAccountReads(
        point_market_id="0", point_size="0.1",
        positions=((2, atomic),),
        portfolio_positions=((2, "0.1"),),
    )
    snapshot, _, _ = asyncio.run(
        _flat_read_venue(transport)._account(AccountRole.PRIMARY, include_private=False),
    )
    assert snapshot.position == Decimal("0.1")


@pytest.mark.parametrize(
    "kwargs",
    (
        pytest.param(
            {}, id="missing-corroboration",
        ),
        pytest.param(
            {
                "positions": ((2, "100000000000000000"),),
                "portfolio_positions": ((2, "0.2"),),
            }, id="full-read-disagreement",
        ),
        pytest.param(
            {
                "positions": ((2, "100000000000000000"), (3, "1000000000000000")),
                "portfolio_positions": ((2, "0.1"), (3, "0.001")),
            }, id="unrelated-nonzero-state",
        ),
        pytest.param(
            {
                "point_size": "0.1005",
                "positions": ((2, "100000000000000000"),),
                "portfolio_positions": ((2, "0.1"),),
            }, id="point-off-grid",
        ),
        pytest.param(
            {
                "point_account": COUNTERPARTY,
                "positions": ((2, "100000000000000000"),),
                "portfolio_positions": ((2, "0.1"),),
            }, id="point-account-contradiction",
        ),
        pytest.param(
            {
                "positions": ((2, "100000000000000000"),),
                "positions_account": COUNTERPARTY,
                "portfolio_positions": ((2, "0.1"),),
            }, id="full-account-contradiction",
        ),
        pytest.param(
            {
                "positions": ((2, "0.1"),),
                "portfolio_positions": ((2, "0.1"),),
            }, id="decimal-full-unit",
        ),
        pytest.param(
            {
                "positions": ((2, "100000000000000000"),),
                "portfolio_positions": ((2, "100000000000000000"),),
            }, id="portfolio-atomic-unit",
        ),
        pytest.param(
            {
                "positions": ((2, "100000000000000000"),),
                "portfolio_positions": ((2, "1e17"),),
            }, id="portfolio-ambiguous-unit",
        ),
        pytest.param(
            {
                "positions": ((2, "100000000000000001"),),
                "portfolio_positions": ((2, "0.1"),),
            }, id="off-grid-full-unit",
        ),
        pytest.param(
            {
                "positions": ((0, "100000000000000000"),),
                "portfolio_positions": ((0, "0.1"),),
            }, id="full-market-zero-is-not-an-alias",
        ),
        pytest.param(
            {
                "positions": ((2, "100000000000000000"), (2, "0")),
                "portfolio_positions": ((2, "0.1"),),
            }, id="duplicate-full-market",
        ),
    ),
)
def test_fixed_account_adapter_rejects_unproven_or_ambiguous_market_zero_alias(kwargs):
    params = {"point_market_id": "0", "point_size": "0.1", **kwargs}
    transport = _FlatAccountReads(**params)
    with pytest.raises(CoordinatorSafetyError):
        asyncio.run(
            _flat_read_venue(transport)._account(
                AccountRole.PRIMARY, include_private=False,
            ),
        )


def test_fixed_account_adapter_normalizes_flat_position_variants():
    primary = _primary_identity()
    counterparty = RoleIdentity(
        AccountRole.COUNTERPARTY, FIXED_COUNTERPARTY_ACCOUNT,
        FIXED_COUNTERPARTY_SIGNER, "key", "marker", "journal",
    )

    class FlatReads:
        def __init__(
            self, *, mismatch: bool = False, open_order: RestOrder | None = None,
            exact_order: RestOrder | None = None,
            history_order: RestOrder | None = None,
            point_market_id: str = "0", point_size: str = "0",
            point_account: str | None = None,
            positions: tuple[tuple[int, str], ...] = (),
            portfolio_positions: tuple[tuple[int, str], ...] | None = None,
            trade_rows: tuple[dict[str, object], ...] = (),
        ):
            self.calls: list[tuple[str, tuple[tuple[str, str], ...]]] = []
            self.mismatch = mismatch
            self.open_order = open_order
            self.exact_order = exact_order if exact_order is not None else open_order
            self.history_order = history_order
            self.point_market_id = point_market_id
            self.point_size = point_size
            self.point_account = point_account
            self.positions = positions
            self.portfolio_positions = portfolio_positions
            self.trade_rows = trade_rows

        async def get(self, path, query=()):
            query = tuple(query)
            self.calls.append((path, query))
            params = dict(query)
            account = params.get("account")
            if account is None and path.startswith("/v1/nonce-state/"):
                account = path.rsplit("/", 1)[-1]
            signer = (
                SIGNER if account == ACCOUNT else FIXED_COUNTERPARTY_SIGNER
            )
            if path == "/v1/auth/session-key-status":
                data = {"status": 1, "status_description": "Active"}
            elif path == "/v1/auth/signers":
                data = {"signers": [{"signer": signer, "status": "Active"}]}
            elif path == "/v1/orders/open":
                data = {
                    "account": account, "market_id": "0",
                    "orders": [] if self.open_order is None else [
                        _raw_compact_open_order(self.open_order),
                    ],
                    "total_orders": "0",
                }
            elif path.startswith("/v1/orders/by-id/"):
                if self.exact_order is None:
                    raise AssertionError(path)
                data = {"order": _raw_order(self.exact_order)}
            elif path == HISTORY_PATH:
                data = {
                    "orders": [] if self.history_order is None else [
                        _raw_order(self.history_order),
                    ],
                    "page": 1, "has_next_page": False,
                }
            elif path == TRADES_PATH:
                data = {
                    "trades": list(self.trade_rows),
                    "page": 1, "has_next_page": False,
                }
            elif path == "/v1/account/position":
                point = {
                    "market_id": self.point_market_id, "size": self.point_size,
                }
                if self.point_account is not None:
                    point["account"] = self.point_account
                data = {"position": point}
            elif path == "/v1/positions":
                data = {"positions": [
                    {"account": account, "market_id": market_id, "size": size}
                    for market_id, size in self.positions
                ]}
            elif path == PORTFOLIO_PATH:
                portfolio_positions = self.portfolio_positions
                if portfolio_positions is None:
                    portfolio_positions = tuple((market_id, "0") for market_id in range(1, 20))
                positions = [
                    {"market_id": market_id, "size": size}
                    for market_id, size in portfolio_positions
                ]
                if self.mismatch:
                    positions[2]["size"] = "0.001"
                data = {
                    "account": account,
                    "positions": positions,
                    "summary": {
                        "usdc_balance": "1000", "free_collateral": "1000",
                        "total_account_value": "1000", "in_liquidation": False,
                        "risk_level": "NORMAL",
                    },
                }
            elif path.startswith("/v1/nonce-state/"):
                data = {
                    "nonce_anchor": "1", "current_bitmap_index": 0,
                    "bitmap": "0x0",
                }
            else:
                raise AssertionError(path)
            return _HTTPObservation(
                200, "", {"data": data, "request_id": path}, NOW,
            )

    transport = FlatReads()
    venue = FixedRisexTwoAccountVenue(
        identities={
            AccountRole.PRIMARY: primary,
            AccountRole.COUNTERPARTY: counterparty,
        },
        credential_loaders={
            AccountRole.PRIMARY: lambda: None,
            AccountRole.COUNTERPARTY: lambda: None,
        },
        transport=transport,
        now=lambda: NOW,
    )
    snapshot, nonce, nonce_at = asyncio.run(
        venue._account(AccountRole.PRIMARY, include_private=False),
    )
    assert snapshot.position == Decimal("0")
    assert snapshot.portfolio is not None
    assert nonce == NonceState(1, 0) and nonce_at == NOW
    assert ("/v1/positions", (("account", ACCOUNT),)) in transport.calls
    assert (PORTFOLIO_PATH, (("account", ACCOUNT),)) in transport.calls

    mismatch_transport = FlatReads(mismatch=True)
    mismatch_venue = FixedRisexTwoAccountVenue(
        identities={
            AccountRole.PRIMARY: primary,
            AccountRole.COUNTERPARTY: counterparty,
        },
        credential_loaders={
            AccountRole.PRIMARY: lambda: None,
            AccountRole.COUNTERPARTY: lambda: None,
        },
        transport=mismatch_transport,
        now=lambda: NOW,
    )
    with pytest.raises(CoordinatorSafetyError):
        asyncio.run(mismatch_venue._account(AccountRole.PRIMARY, include_private=False))

    open_order = order(
        wide=408, client=4008, account=ACCOUNT, side="SELL",
        order_type="LIMIT", tif="GTC", status="OPEN", price="2363.04",
        post_only=True,
    )
    open_transport = FlatReads(open_order=open_order)
    open_venue = FixedRisexTwoAccountVenue(
        identities={
            AccountRole.PRIMARY: primary,
            AccountRole.COUNTERPARTY: counterparty,
        },
        credential_loaders={
            AccountRole.PRIMARY: lambda: None,
            AccountRole.COUNTERPARTY: lambda: None,
        },
        transport=open_transport,
        now=lambda: NOW,
        known_order_ids={AccountRole.PRIMARY: (open_order.order_id,)},
    )
    snapshot, _, _ = asyncio.run(
        open_venue._account(AccountRole.PRIMARY, include_private=False),
    )
    assert snapshot.open_orders[0].order_id == open_order.order_id
    assert (
        ORDER_LOOKUP_PATH_TEMPLATE.format(order_id=open_order.order_id), ()
    ) in open_transport.calls

    disagreement = replace(open_order, price=Decimal("2363.05"))
    disagreement_transport = FlatReads(
        open_order=open_order, exact_order=disagreement,
    )
    disagreement_venue = FixedRisexTwoAccountVenue(
        identities={
            AccountRole.PRIMARY: primary,
            AccountRole.COUNTERPARTY: counterparty,
        },
        credential_loaders={
            AccountRole.PRIMARY: lambda: None,
            AccountRole.COUNTERPARTY: lambda: None,
        },
        transport=disagreement_transport,
        now=lambda: NOW,
        known_order_ids={AccountRole.PRIMARY: (open_order.order_id,)},
    )
    with pytest.raises(CoordinatorSafetyError):
        asyncio.run(disagreement_venue._account(AccountRole.PRIMARY, include_private=False))

    assert _point_position(
        {"position": {"market_id": "0", "size": "0"}}, primary,
    ) == (2, Decimal("0"))
    with pytest.raises(CoordinatorSafetyError):
        _point_position({"market_id": "3", "size": "0"}, primary)
    with pytest.raises(CoordinatorSafetyError):
        _point_position({"market_id": "0", "size": "0.1"}, primary)
    assert _position_maps_agree(
        _normalize_position_map(()),
        _normalize_position_map(tuple((i, Decimal("0")) for i in range(1, 20))),
    )


def test_fixed_rest_paths_and_counterparty_marker_are_sealed(tmp_path: Path, monkeypatch):
    transport = object.__new__(FixedRisexTwoAccountTransport)
    transport._accounts = frozenset({ACCOUNT, FIXED_COUNTERPARTY_ACCOUNT})
    history_query = (
        ("account", ACCOUNT), ("market_id", "2"),
        ("page", "1"), ("limit", str(PAGE_LIMIT)),
    )
    assert transport._target(HISTORY_PATH, history_query).endswith(
        "account=" + ACCOUNT + "&market_id=2&page=1&limit=" + str(PAGE_LIMIT)
    )
    assert transport._target(TRADES_PATH, history_query).endswith(
        "account=" + ACCOUNT + "&market_id=2&page=1&limit=" + str(PAGE_LIMIT)
    )
    assert transport._target(PORTFOLIO_PATH, (("account", ACCOUNT),)).endswith(
        "account=" + ACCOUNT
    )
    assert transport._target(ORDER_LOOKUP_PATH_TEMPLATE.format(
        order_id=order(wide=409, client=4009, account=ACCOUNT, side="BUY",
                       order_type="MARKET", tif="IOC", status="FILLED", price="0").order_id,
    ), ())
    with pytest.raises(CoordinatorSafetyError):
        transport._target("/v1/orders/history", ("account", ACCOUNT))
    assert all("/" not in item for item in (COUNTERPARTY_SIGNER_MARKER,))

    marker = {
        "account": FIXED_COUNTERPARTY_ACCOUNT,
        "chain_id": 11155931,
        "expiration": COUNTERPARTY_REGISTRATION_EXPIRATION,
        "host": "api.testnet.rise.trade",
        "operation": "COUNTERPARTY_REGISTER_SIGNER",
        "schema_version": 1,
        "signer": FIXED_COUNTERPARTY_SIGNER,
        "state": "ACTIVE",
        "venue": "RISEx",
    }
    import risex_farmer.testnet_risex_two_account_coordinator as module
    monkeypatch.setattr(module._signer, "_open_home", lambda: os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY))
    monkeypatch.setattr(module._signer, "_read_file", lambda _fd, name: (
        json.dumps(marker).encode() if name == COUNTERPARTY_SIGNER_MARKER else None
    ))
    module._load_counterparty_registration_marker()
    marker["state"] = "REVOKED"
    with pytest.raises(CoordinatorSafetyError):
        module._load_counterparty_registration_marker()


class _RetryContent:
    def __init__(self, outcome):
        self.outcome = outcome
        self.read_calls = []
        self._read = False

    async def read(self, limit):
        self.read_calls.append(limit)
        if self._read:
            return b""
        self._read = True
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return self.outcome[:limit]


class _StreamProtocol:
    def __init__(self):
        self.connected = True
        self._reading_paused = False

    def pause_reading(self):
        self._reading_paused = True

    def resume_reading(self):
        self._reading_paused = False


def _live_stream_response(transport, chunks, *, terminal=None):
    reader = aiohttp.StreamReader(_StreamProtocol(), limit=transport.MAX_BYTES)
    reader.feed_data(chunks[0])

    async def produce():
        for chunk in chunks[1:]:
            await asyncio.sleep(0)
            reader.feed_data(chunk)
        if terminal is None:
            reader.feed_eof()
        else:
            await asyncio.sleep(0)
            reader.set_exception(terminal)

    producer = asyncio.create_task(produce())
    response = _RetryResponse(
        transport.REST_ORIGIN + "/v1/system/config",
        body=b"",
        content=reader,
    )
    return response, reader, producer


class _RetryResponse:
    def __init__(self, url, *, body, status=200, content_length=None, content=None):
        self.url = url
        self.status = status
        self.history = ()
        self.content = content if content is not None else _RetryContent(body)
        self.content_length = (
            len(body) if content_length is None and content is None and isinstance(body, bytes)
            else content_length
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None


class _RetrySession:
    def __init__(self, *, get_outcomes=(), post_outcomes=()):
        self.get_outcomes = list(get_outcomes)
        self.post_outcomes = list(post_outcomes)
        self.get_calls = []
        self.post_calls = []

    def get(self, url, **kwargs):
        self.get_calls.append((url, kwargs))
        outcome = self.get_outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def post(self, url, **kwargs):
        self.post_calls.append((url, kwargs))
        outcome = self.post_outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def _retry_transport(session):
    transport = object.__new__(FixedRisexTwoAccountTransport)
    transport._accounts = frozenset({ACCOUNT, FIXED_COUNTERPARTY_ACCOUNT})
    transport._session = session
    return transport


def _retry_response(
    transport, body, *, status=200, path="/v1/system/config", content=None,
):
    return _RetryResponse(
        transport.REST_ORIGIN + path, body=body, status=status, content=content,
    )


VALID_RETRY_BODY = b'{"data":{},"request_id":"ok"}'


def test_fixed_transport_get_success_uses_one_attempt():
    session = _RetrySession()
    transport = _retry_transport(session)
    response_fixture = _retry_response(transport, VALID_RETRY_BODY)
    session.get_outcomes = [response_fixture]

    response = asyncio.run(transport.get("/v1/system/config"))

    assert response.body == {"data": {}, "request_id": "ok"}
    assert len(session.get_calls) == 1
    assert response_fixture.content.read_calls == [
        transport.MAX_BYTES + 1,
        transport.MAX_BYTES - len(VALID_RETRY_BODY) + 1,
    ]
    assert session.get_calls[0][1] == {
        "allow_redirects": False, "proxy": None,
    }


def test_fixed_transport_get_drains_live_shaped_multichunk_stream_to_eof():
    async def exercise():
        session = _RetrySession()
        transport = _retry_transport(session)
        response, reader, producer = _live_stream_response(
            transport,
            (b'{"data":', b'{},"request_id":', b'"ok"}'),
        )
        session.get_outcomes = [response]
        try:
            observed = await transport.get("/v1/system/config")
            return observed, reader.at_eof(), len(session.get_calls)
        finally:
            if not producer.done():
                producer.cancel()
            await asyncio.gather(producer, return_exceptions=True)

    response, at_eof, get_call_count = asyncio.run(exercise())

    assert response.body == {"data": {}, "request_id": "ok"}
    assert at_eof is True
    assert get_call_count == 1


def test_fixed_transport_get_rejects_oversized_decoded_stream_without_retry():
    async def exercise():
        session = _RetrySession()
        transport = _retry_transport(session)
        transport.MAX_BYTES = 4
        response, _reader, producer = _live_stream_response(
            transport, (b"{}", b"xxx"),
        )
        session.get_outcomes = [response]
        try:
            with pytest.raises(CoordinatorSafetyError, match="response bound rejected"):
                await transport.get("/v1/system/config")
            return len(session.get_calls)
        finally:
            if not producer.done():
                producer.cancel()
            await asyncio.gather(producer, return_exceptions=True)

    assert asyncio.run(exercise()) == 1


def test_fixed_transport_get_accepts_exact_decoded_limit_and_checks_eof():
    session = _RetrySession()
    transport = _retry_transport(session)
    transport.MAX_BYTES = 4
    response_fixture = _retry_response(transport, b"{}  ")
    session.get_outcomes = [response_fixture]

    response = asyncio.run(transport.get("/v1/system/config"))

    assert response.body == {}
    assert len(session.get_calls) == 1
    assert response_fixture.content.read_calls == [5, 1]


@pytest.mark.parametrize(
    "failure",
    (
        TimeoutError("timeout"),
        aiohttp.ClientConnectionError("disconnect"),
        ConnectionResetError("reset"),
    ),
)
def test_fixed_transport_get_retries_initial_pre_response_connect_or_timeout(failure):
    session = _RetrySession()
    transport = _retry_transport(session)
    session.get_outcomes = [
        failure, _retry_response(transport, VALID_RETRY_BODY),
    ]

    response = asyncio.run(transport.get("/v1/system/config"))

    assert response.body == {"data": {}, "request_id": "ok"}
    assert len(session.get_calls) == 2


def test_fixed_transport_get_retries_aiohttp_partial_body_then_succeeds():
    session = _RetrySession()
    transport = _retry_transport(session)
    session.get_outcomes = [
        _retry_response(
            transport, aiohttp.ClientPayloadError("premature EOF"),
        ),
        _retry_response(transport, VALID_RETRY_BODY),
    ]

    response = asyncio.run(transport.get("/v1/system/config"))

    assert response.body == {"data": {}, "request_id": "ok"}
    assert len(session.get_calls) == 2


@pytest.mark.parametrize(
    "failure",
    (
        aiohttp.ClientPayloadError("premature EOF"),
        ConnectionResetError("reset"),
        TimeoutError("timeout"),
    ),
)
def test_fixed_transport_get_retries_stream_failure_after_partial_body(failure):
    async def exercise():
        session = _RetrySession()
        transport = _retry_transport(session)
        first, _reader, producer = _live_stream_response(
            transport, (b'{"data":',), terminal=failure,
        )
        session.get_outcomes = [
            first, _retry_response(transport, VALID_RETRY_BODY),
        ]
        try:
            observed = await transport.get("/v1/system/config")
            return observed, len(session.get_calls)
        finally:
            if not producer.done():
                producer.cancel()
            await asyncio.gather(producer, return_exceptions=True)

    response, get_call_count = asyncio.run(exercise())

    assert response.body == {"data": {}, "request_id": "ok"}
    assert get_call_count == 2


def test_fixed_transport_get_stops_after_two_stream_failures():
    async def exercise():
        session = _RetrySession()
        transport = _retry_transport(session)
        first, _reader_one, producer_one = _live_stream_response(
            transport,
            (b'{"data":',),
            terminal=aiohttp.ClientPayloadError("first premature EOF"),
        )
        second, _reader_two, producer_two = _live_stream_response(
            transport,
            (b'{"data":',),
            terminal=ConnectionResetError("second reset"),
        )
        session.get_outcomes = [first, second]
        try:
            with pytest.raises(CoordinatorSafetyError, match="transport failed"):
                await transport.get("/v1/system/config")
            return len(session.get_calls)
        finally:
            for producer in (producer_one, producer_two):
                if not producer.done():
                    producer.cancel()
            await asyncio.gather(producer_one, producer_two, return_exceptions=True)

    assert asyncio.run(exercise()) == 2


@pytest.mark.parametrize("raw", (b"", b" \r\n", b'{"data":'))
def test_fixed_transport_get_retries_one_incomplete_json_document(raw):
    session = _RetrySession()
    transport = _retry_transport(session)
    session.get_outcomes = [
        _retry_response(transport, raw),
        _retry_response(transport, VALID_RETRY_BODY),
    ]

    response = asyncio.run(transport.get("/v1/system/config"))

    assert response.body == {"data": {}, "request_id": "ok"}
    assert len(session.get_calls) == 2


def test_fixed_transport_get_stops_after_two_pre_response_transport_failures():
    session = _RetrySession()
    transport = _retry_transport(session)
    session.get_outcomes = [
        TimeoutError("first"), aiohttp.ClientConnectionError("second"),
    ]

    with pytest.raises(CoordinatorSafetyError, match="transport failed"):
        asyncio.run(transport.get("/v1/system/config"))

    assert len(session.get_calls) == 2


def test_fixed_transport_get_does_not_retry_tls_failure():
    session = _RetrySession()
    transport = _retry_transport(session)
    session.get_outcomes = [ssl.SSLError("certificate verification failed")]

    with pytest.raises(CoordinatorSafetyError, match="transport failed"):
        asyncio.run(transport.get("/v1/system/config"))

    assert len(session.get_calls) == 1


def test_fixed_transport_get_stops_after_two_incomplete_json_documents():
    session = _RetrySession()
    transport = _retry_transport(session)
    session.get_outcomes = [
        _retry_response(transport, b'{"data":'),
        _retry_response(transport, b'{"data":'),
    ]

    with pytest.raises(CoordinatorSafetyError, match="response JSON rejected"):
        asyncio.run(transport.get("/v1/system/config"))

    assert len(session.get_calls) == 2


@pytest.mark.parametrize(
    "raw",
    (
        b"not-json",
        b'{"data":{},"data":{}}',
        b'{"data":NaN}',
        b'{"data":Infinity}',
        b'{"data":-Infinity}',
        b'{"data":1e999}',
    ),
)
def test_fixed_transport_get_complete_invalid_json_does_not_retry(raw):
    session = _RetrySession()
    transport = _retry_transport(session)
    session.get_outcomes = [_retry_response(transport, raw)]

    with pytest.raises(CoordinatorSafetyError, match="response JSON rejected"):
        asyncio.run(transport.get("/v1/system/config"))

    assert len(session.get_calls) == 1


@pytest.mark.parametrize(
    ("status", "raw"),
    (
        (503, VALID_RETRY_BODY),
        (200, b'{"data":{}}'),
    ),
)
def test_fixed_transport_get_http_and_schema_semantics_do_not_retry(status, raw):
    session = _RetrySession()
    transport = _retry_transport(session)
    session.get_outcomes = [_retry_response(transport, raw, status=status)]

    response = asyncio.run(transport.get("/v1/system/config"))
    with pytest.raises(CoordinatorSafetyError, match="rejected"):
        _response_data(response)

    assert len(session.get_calls) == 1


def test_fixed_transport_get_redirect_does_not_retry():
    session = _RetrySession()
    transport = _retry_transport(session)
    response = _retry_response(transport, VALID_RETRY_BODY)
    response.url = transport.REST_ORIGIN + "/v1/system/config?redirected=true"
    session.get_outcomes = [response]

    with pytest.raises(CoordinatorSafetyError, match="redirect rejected"):
        asyncio.run(transport.get("/v1/system/config"))

    assert len(session.get_calls) == 1


@pytest.mark.parametrize("path", (PLACE_PATH, CANCEL_PATH))
@pytest.mark.parametrize("outcome", ("transport", "json", "status"))
def test_fixed_transport_post_paths_remain_single_attempt(path, outcome):
    session = _RetrySession()
    transport = _retry_transport(session)
    if outcome == "transport":
        session.post_outcomes = [ConnectionResetError("reset")]
        expected = "write transport ambiguous"
    elif outcome == "json":
        session.post_outcomes = [
            _retry_response(transport, b"not-json", path=path),
        ]
        expected = "response JSON rejected"
    else:
        session.post_outcomes = [
            _retry_response(
                transport, VALID_RETRY_BODY, status=503, path=path,
            ),
        ]
        expected = None

    if expected is None:
        response = asyncio.run(transport.post(path, {"value": 1}))
        assert response.status == 503
    else:
        with pytest.raises(CoordinatorSafetyError, match=expected):
            asyncio.run(transport.post(path, {"value": 1}))
    assert len(session.post_calls) == 1


def test_counterparty_key_decoder_accepts_only_canonical_lower_hex(tmp_path: Path):
    secret = bytes.fromhex("01" * 32)
    encoded = b"0x" + secret.hex().encode("ascii") + b"\n"
    assert _decode_counterparty_secret(encoded) == secret

    malformed = (
        encoded[:-1],
        b"0X" + encoded[2:],
        b"0x" + b"A" + encoded[3:],
        b"0x" + b"g" + encoded[3:],
        encoded + b"trailing",
    )
    for value in malformed:
        with pytest.raises(CoordinatorSafetyError) as captured:
            _decode_counterparty_secret(value)
        assert secret.hex() not in repr(captured.value)


def test_place_binds_accepted_order_before_return(monkeypatch):
    primary = _primary_identity()
    counterparty = RoleIdentity(
        AccountRole.COUNTERPARTY, FIXED_COUNTERPARTY_ACCOUNT,
        FIXED_COUNTERPARTY_SIGNER, "key", "marker", "journal",
    )
    accepted = order(
        wide=411, client=4011, account=FIXED_COUNTERPARTY_ACCOUNT,
        side="SELL", order_type="LIMIT", tif="GTC", status="OPEN",
        price="2999.01", post_only=True,
    ).order_id

    class Credential:
        def sign_permit(self, _typed, _identity):
            return "0x" + ("00" * 64) + "1b"

        def close(self):
            pass

    class Transport:
        async def post(self, path, body):
            assert path == PLACE_PATH
            return _HTTPObservation(200, "", {
                "data": {"order_id": accepted}, "request_id": "place",
            }, NOW)

        async def close(self):
            pass

    venue = FixedRisexTwoAccountVenue(
        identities={AccountRole.PRIMARY: primary, AccountRole.COUNTERPARTY: counterparty},
        credential_loaders={
            AccountRole.PRIMARY: lambda: Credential(),
            AccountRole.COUNTERPARTY: lambda: Credential(),
        },
        transport=Transport(), now=lambda: NOW,
    )
    monkeypatch.setattr(
        "risex_farmer.testnet_risex_two_account_coordinator._validate_unsigned_place",
        lambda _request, _identity: None,
    )
    request = {
        "body": {
            "market_id": 2, "size_steps": 100, "price_ticks": 299901,
            "side": 1, "post_only": True, "reduce_only": False,
            "stp_mode": 0, "order_type": 1, "time_in_force": 0,
            "client_order_id": 4011,
        },
        "permit": {"message": {"nonceAnchor": 1, "nonceBitmap": 0, "deadline": NOW + 10}},
    }
    result = asyncio.run(venue.place(AccountRole.COUNTERPARTY, request))
    assert result.order_id == accepted
    assert accepted in venue._known_order_ids[AccountRole.COUNTERPARTY]


def test_incomplete_durable_resume_cannot_be_reported_complete(tmp_path: Path):
    coordinator = make_lifecycle(tmp_path, FakeVenue())
    coordinator._journals[AccountRole.PRIMARY].set_phase(Phase.COMPLETE)
    coordinator._journals[AccountRole.COUNTERPARTY].set_phase(Phase.COMPLETE)
    coordinator._journals[AccountRole.PRIMARY].set_outcome("COMPLETE")
    coordinator._journals[AccountRole.COUNTERPARTY].set_outcome("COMPLETE")
    report = asyncio.run(coordinator.run())
    assert report.result is not CoordinatorResult.COMPLETE
