from __future__ import annotations

import asyncio
import copy
from dataclasses import replace
from decimal import Decimal
import json
import os
from pathlib import Path

import pytest

from risex_farmer.testnet_risex_two_account_coordinator import (
    AccountRole,
    AccountSnapshot,
    BookLevel,
    BookObservation,
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
    MarketObservation,
    NonceState,
    Phase,
    PortfolioState,
    ORDER_LOOKUP_PATH_TEMPLATE,
    PAGE_LIMIT,
    PORTFOLIO_PATH,
    PLACE_PATH,
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
    _parse_order_row,
    _parse_portfolio,
    _parse_trade_row,
    _require_recent,
    _all_orders,
    _decode_counterparty_secret,
    _normalize_position_map,
    _order_for,
    _point_position,
    _position_maps_agree,
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


def lifecycle_observations(*, second_final_book: BookObservation | None = None):
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
    entry_primary = account_snapshot(AccountRole.PRIMARY, position="0.1", orders=(), trades=(
        trade(trade_id=f"{maker_entry_filled.order_id}-{entry.order_id}", item=entry, side="BUY", price="2999.01"),
    ))
    entry_primary = replace(entry_primary, history_orders=(entry,))
    entry_counter = account_snapshot(AccountRole.COUNTERPARTY, position="-0.1", orders=(), trades=(
        trade(trade_id=f"{maker_entry_filled.order_id}-{entry.order_id}", item=maker_entry_filled, side="SELL", price="2999.01"),
    ))
    entry_counter = replace(entry_counter, history_orders=(maker_entry_filled,))
    exit_primary = account_snapshot(AccountRole.PRIMARY, position="0", orders=(), trades=(
        trade(trade_id=f"{maker_entry_filled.order_id}-{entry.order_id}", item=entry, side="BUY", price="2999.01"),
        trade(trade_id=f"{maker_exit_filled.order_id}-{exit_order.order_id}", item=exit_order, side="SELL", price="2999.00"),
    ))
    exit_primary = replace(exit_primary, history_orders=(entry, exit_order))
    exit_counter = account_snapshot(AccountRole.COUNTERPARTY, position="0", orders=(), trades=(
        trade(trade_id=f"{maker_entry_filled.order_id}-{entry.order_id}", item=maker_entry_filled, side="SELL", price="2999.01"),
        trade(trade_id=f"{maker_exit_filled.order_id}-{exit_order.order_id}", item=maker_exit_filled, side="BUY", price="2999.00"),
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


def test_complete_two_account_lifecycle_is_sequential_and_reduce_only(tmp_path: Path):
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


def test_fixed_account_adapter_normalizes_flat_position_variants():
    primary = _primary_identity()
    counterparty = RoleIdentity(
        AccountRole.COUNTERPARTY, FIXED_COUNTERPARTY_ACCOUNT,
        FIXED_COUNTERPARTY_SIGNER, "key", "marker", "journal",
    )

    class FlatReads:
        def __init__(self, *, mismatch: bool = False):
            self.calls: list[tuple[str, tuple[tuple[str, str], ...]]] = []
            self.mismatch = mismatch

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
                data = {"orders": []}
            elif path == HISTORY_PATH:
                data = {"orders": [], "page": 1, "has_next_page": False}
            elif path == TRADES_PATH:
                data = {"trades": [], "page": 1, "has_next_page": False}
            elif path == "/v1/account/position":
                data = {"position": {"market_id": "0", "size": "0"}}
            elif path == "/v1/positions":
                data = {"positions": []}
            elif path == PORTFOLIO_PATH:
                positions = [
                    {"market_id": market_id, "size": "0"}
                    for market_id in range(1, 20)
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
