from __future__ import annotations

import copy
import importlib
import inspect
import json
from pathlib import Path
from decimal import Decimal

import pytest

from risex_farmer.extended_testnet_lifecycle_operational import (
    ACTIVE,
    CANCELLED,
    STARK_PRIVATE_KEY_BASENAME,
    ExtendedCredentialCapability,
    ExtendedIdentity,
    OperationalIntentStore,
    OperationalReport,
    OperationalSafetyError,
    OperationalVenueIO,
    RestObservation,
    RestFailure,
    RuntimeRunJournal,
    SealedLifecycleRunner,
    SignedOrder,
    TARGET_MARKET,
    WriteReceipt,
    _PasswdHomeCredentialSource,
    canonical_digest,
    _fixture_run,
    _list_data,
    _validate_page_meta,
    _validate_signed_payload,
    run,
)


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "extended_testnet_001" / "official_lifecycle.json"
IDENTITY = ExtendedIdentity(7001, 3, "0x12345", 7001003)


def _base_fixture() -> dict:
    return json.loads(FIXTURE_PATH.read_text())


def _account(wire: dict) -> dict:
    return copy.deepcopy(wire["account"]["info"])


class FakeSigner:
    def __init__(self, identity: ExtendedIdentity = IDENTITY) -> None:
        self.identity = identity
        self.calls = []
        self.closed = False

    def sign_order(self, intent, market):
        self.calls.append((intent.id, intent.state, intent.external_id, intent.nonce, intent.expiry_ms))
        payload = {
            "id": intent.external_id,
            "market": intent.market,
            "type": "LIMIT",
            "side": intent.side,
            "qty": str(intent.qty),
            "price": str(intent.price),
            "reduceOnly": intent.reduce_only,
            "postOnly": False,
            "timeInForce": "IOC",
            "expiryEpochMillis": intent.expiry_ms,
            "fee": str(intent.fee),
            "nonce": intent.nonce,
            "selfTradeProtectionLevel": "ACCOUNT",
            "settlement": {
                "signature": {"r": "0x111", "s": "0x222"},
                "starkKey": self.identity.l2_key,
                "collateralPosition": str(self.identity.l2_vault),
            },
        }
        return SignedOrder(payload, intent.external_id, None)

    def close(self) -> None:
        self.closed = True


class FixtureIO:
    def __init__(self, *, entry_outcome: str = "FILLED") -> None:
        self.wire = _base_fixture()
        self.entry_outcome = entry_outcome
        self.phase = "FLAT"
        self.clock = 1_770_000_000_000
        self.place_calls = []
        self.cancel_calls = []
        self.stream_calls = 0

    def now_ms(self) -> int:
        self.clock += 1_000
        return self.clock

    def _order(self, intent, order_id: int, status: str) -> dict:
        value = copy.deepcopy(
            self.wire["filledOrder"] if intent.kind == "ENTRY" else self.wire["filledCloseOrder"]
        )
        value.update(
            id=order_id,
            accountId=IDENTITY.account_id,
            externalId=intent.external_id,
            market=TARGET_MARKET,
            side=intent.side,
            status=status,
            price=str(intent.price),
            qty=str(intent.qty),
            filledQty=str(intent.qty if status == "FILLED" else Decimal(0)),
            cancelledQty=str(intent.qty if status == CANCELLED else Decimal(0)),
            reduceOnly=intent.reduce_only,
            postOnly=False,
            expiryTime=intent.expiry_ms,
            timeInForce="IOC",
        )
        if status != "FILLED":
            value["averagePrice"] = None
            value["payedFee"] = "0"
        return value

    def _trade(self, intent, trade_id: int, order_id: int) -> dict:
        value = copy.deepcopy(self.wire["fill"] if intent.kind == "ENTRY" else self.wire["closeFill"])
        value.update(
            id=trade_id,
            accountId=IDENTITY.account_id,
            externalId=intent.external_id,
            orderId=order_id,
            market=TARGET_MARKET,
            side=intent.side,
            price=str(intent.price),
            qty=str(intent.qty),
            value=str(intent.qty * intent.price),
            fee=str(intent.qty * intent.price * intent.fee),
            isTaker=True,
            tradeType="TRADE",
        )
        return value

    def observe(self, intents):
        observed = self.now_ms()
        entry = next((item for item in intents if item.kind == "ENTRY"), None)
        close = next((item for item in intents if item.kind == "CLOSE"), None)
        history = []
        trades = []
        positions = []
        open_orders = []
        exact_by_id = {}
        exact_by_external = {}
        if entry is not None:
            status = "FILLED" if self.phase in {"ENTRY_FILLED", "CLOSED"} else (
                CANCELLED if self.phase == "ENTRY_CANCELLED" else "NEW"
            )
            entry_order = self._order(entry, 90001, status)
            history.append(entry_order)
            exact_by_id[str(entry_order["id"])] = entry_order
            exact_by_external[entry.external_id] = (entry_order,)
            if status == "NEW":
                open_orders.append(entry_order)
            elif status == "FILLED":
                trades.append(self._trade(entry, 80001, 90001))
                if self.phase != "CLOSED":
                    position = copy.deepcopy(self.wire["position"])
                    position.update(
                        accountId=IDENTITY.account_id,
                        market=TARGET_MARKET,
                        side="LONG",
                        size=str(entry.qty),
                        value=str(entry.qty * Decimal("40005")),
                    )
                    positions.append(position)
        if close is not None and self.phase == "CLOSED":
            close_order = self._order(close, 90002, "FILLED")
            history.append(close_order)
            trades.append(self._trade(close, 80002, 90002))
            exact_by_id[str(close_order["id"])] = close_order
            exact_by_external[close.external_id] = (close_order,)
        return RestObservation(
            observed_at_ms=observed,
            server_time_ms=observed,
            account=_account(self.wire),
            market=copy.deepcopy(self.wire["market"]),
            book={
                "market": TARGET_MARKET,
                "bids": copy.deepcopy(self.wire["book"]["bids"]),
                "asks": copy.deepcopy(self.wire["book"]["asks"]),
            },
            balance=copy.deepcopy(self.wire["account"]["balance"]),
            fees=tuple(copy.deepcopy(self.wire["account"]["fees"])),
            leverage=tuple(copy.deepcopy(self.wire["account"]["leverage"])),
            open_orders=tuple(open_orders),
            positions=tuple(positions),
            order_history=tuple(history),
            trades=tuple(trades),
            exact_by_id=exact_by_id,
            exact_by_external=exact_by_external,
            stream_frames=self.stream_calls,
        )

    def place_order(self, intent, payload) -> WriteReceipt:
        self.place_calls.append((intent.kind, intent.state, intent.external_id, intent.nonce))
        if intent.kind == "ENTRY":
            self.phase = "ENTRY_FILLED" if self.entry_outcome == "FILLED" else "ENTRY_OPEN"
            return WriteReceipt(True, "90001", intent.external_id)
        self.phase = "CLOSED"
        return WriteReceipt(True, "90002", intent.external_id)

    def cancel_order(self, intent, order_id) -> WriteReceipt:
        self.cancel_calls.append((intent.state, order_id, intent.target_external_id))
        self.phase = "ENTRY_CANCELLED"
        return WriteReceipt(True)


@pytest.fixture
def no_sleep(monkeypatch):
    module = importlib.import_module("risex_farmer.extended_testnet_lifecycle_operational")
    monkeypatch.setattr(module.time, "sleep", lambda _seconds: None)


def test_zero_argument_surface_and_normal_startup_isolation():
    assert tuple(inspect.signature(run).parameters) == ()
    package = importlib.import_module("risex_farmer")
    assert "extended_testnet_lifecycle_operational" not in Path(package.__file__).read_text()


def test_fixture_filled_lifecycle_has_exact_order_trade_and_close_identities(tmp_path, no_sleep):
    io = FixtureIO()
    signer = FakeSigner()
    report = _fixture_run(
        path=tmp_path / "level-c.sqlite3", io=io, capability=signer, identity=IDENTITY,
    )
    assert report.status == "SUCCESS_CLOSED_FLAT"
    assert report.entry_order_id == "90001" and report.close_order_id == "90002"
    assert report.writes == 2 and report.stream_frames == 0
    assert len(io.place_calls) == 2
    assert all(item[1] == "CLAIMED" for item in io.place_calls)
    assert signer.closed
    store = OperationalIntentStore(tmp_path / "level-c.sqlite3")
    intents = store.all()
    assert [item.kind for item in intents] == ["ENTRY", "CLOSE"]
    assert [item.venue_order_id for item in intents] == ["90001", "90002"]
    assert all(item.dispatch_count == 1 for item in intents)
    assert intents[0].external_id != intents[1].external_id
    assert intents[0].nonce != intents[1].nonce
    assert store.lifecycle_state() == "COMPLETE"
    runtime = RuntimeRunJournal(tmp_path / "level-c.sqlite3").snapshot()
    assert runtime is not None and runtime["state"] == "COMPLETE"


def test_fixture_open_ioc_uses_one_known_id_cancel_and_finishes_flat(tmp_path, no_sleep):
    io = FixtureIO(entry_outcome="OPEN")
    signer = FakeSigner()
    report = _fixture_run(
        path=tmp_path / "cancel.sqlite3", io=io, capability=signer, identity=IDENTITY,
    )
    assert report.status == "COMPLETED_NO_FILL_FLAT"
    assert report.writes == 2
    assert io.cancel_calls and io.cancel_calls[0][1] == "90001"
    store = OperationalIntentStore(tmp_path / "cancel.sqlite3")
    assert [item.kind for item in store.all()] == ["ENTRY", "CANCEL"]
    assert store.lifecycle_state() == "COMPLETE"


def test_ambiguous_place_is_durable_and_never_replayed(tmp_path, no_sleep):
    class AmbiguousIO(FixtureIO):
        def place_order(self, intent, payload):
            self.place_calls.append((intent.kind, intent.state, intent.external_id, intent.nonce))
            from risex_farmer.extended_testnet_lifecycle_operational import AmbiguousWrite
            raise AmbiguousWrite()

        def observe(self, intents):
            value = super().observe(intents)
            value = copy.copy(value)
            object.__setattr__(value, "open_orders", ())
            object.__setattr__(value, "order_history", ())
            object.__setattr__(value, "exact_by_id", {})
            object.__setattr__(value, "exact_by_external", {})
            return value

    io = AmbiguousIO()
    signer = FakeSigner()
    with pytest.raises(OperationalSafetyError, match="WRITE_OUTCOME_AMBIGUOUS"):
        _fixture_run(path=tmp_path / "ambiguous.sqlite3", io=io, capability=signer, identity=IDENTITY)
    assert len(io.place_calls) == 1
    store = OperationalIntentStore(tmp_path / "ambiguous.sqlite3")
    intent = store.all()[0]
    assert intent.state == "AMBIGUOUS" and intent.dispatch_count == 1
    assert intent.payload is not None
    assert "apiKey" not in json.dumps(intent.payload)
    with pytest.raises(OperationalSafetyError, match="RUNTIME_ALREADY_TERMINAL"):
        _fixture_run(path=tmp_path / "ambiguous.sqlite3", io=io, capability=FakeSigner(), identity=IDENTITY)
    assert len(io.place_calls) == 1


def test_ambiguous_place_reconciles_by_external_then_exact_id_without_replay(tmp_path, no_sleep):
    class LandedAmbiguous(FixtureIO):
        def place_order(self, intent, payload):
            if intent.kind != "ENTRY":
                return super().place_order(intent, payload)
            self.place_calls.append((intent.kind, intent.state, intent.external_id, intent.nonce))
            self.phase = "ENTRY_FILLED"
            from risex_farmer.extended_testnet_lifecycle_operational import AmbiguousWrite
            raise AmbiguousWrite()

        def observe(self, intents):
            value = super().observe(intents)
            if any(item.kind == "ENTRY" and item.venue_order_id is None for item in intents):
                value = copy.copy(value)
                object.__setattr__(value, "exact_by_id", {})
            return value

    io = LandedAmbiguous()
    report = _fixture_run(
        path=tmp_path / "ambiguous-landed.sqlite3", io=io,
        capability=FakeSigner(), identity=IDENTITY,
    )
    assert report.status == "SUCCESS_CLOSED_FLAT"
    assert report.writes == 2
    assert len(io.place_calls) == 2


def test_signer_observes_claimed_after_durable_preparation(tmp_path, no_sleep):
    io = FixtureIO()
    signer = FakeSigner()
    _fixture_run(path=tmp_path / "order.sqlite3", io=io, capability=signer, identity=IDENTITY)
    assert signer.calls
    assert all(state == "CLAIMED" for _, state, *_ in signer.calls)
    assert all(call[2] and call[3] and call[4] for call in signer.calls)


def test_pinned_official_sdk_signing_is_offline_and_exact():
    pytest.importorskip("x10")
    intent = type("Intent", (), {
        "external_id": "123456789012345678901234567891", "nonce": 7,
        "market": TARGET_MARKET, "side": "BUY", "qty": Decimal("0.001"),
        "price": Decimal("40010"), "fee": Decimal("0.00025"),
        "expiry_ms": 1770000015000, "reduce_only": False,
    })()
    fixture = _base_fixture()
    capability = ExtendedCredentialCapability(
        bytearray(b"fixture-api-key"), bytearray(b"0x" + b"1" * 64), IDENTITY,
    )
    try:
        signed = capability.sign_order(intent, fixture["market"])
        _validate_signed_payload(intent, signed.payload, IDENTITY)
        assert signed.external_id == intent.external_id
        assert signed.settlement_hash
    finally:
        capability.close()


def test_signed_payload_identity_and_secret_rejection():
    intent = type("Intent", (), {
        "external_id": "123", "market": TARGET_MARKET, "side": "BUY", "qty": Decimal("0.001"),
        "price": Decimal("40010"), "reduce_only": False, "expiry_ms": 1000,
        "fee": Decimal("0.00025"), "nonce": 7,
    })()
    payload = {
        "id": "123", "market": TARGET_MARKET, "type": "LIMIT", "side": "BUY",
        "qty": "0.001", "price": "40010", "reduceOnly": False, "postOnly": False,
        "timeInForce": "IOC", "expiryEpochMillis": 1000, "fee": "0.00025", "nonce": 7,
        "selfTradeProtectionLevel": "ACCOUNT",
        "settlement": {
            "signature": {"r": "0x1", "s": "0x2"},
            "starkKey": IDENTITY.l2_key, "collateralPosition": str(IDENTITY.l2_vault),
        },
    }
    _validate_signed_payload(intent, payload, IDENTITY)
    poisoned = copy.deepcopy(payload)
    poisoned["apiKey"] = "fixture-secret"
    with pytest.raises(OperationalSafetyError, match="SECRET_PERSISTENCE_FORBIDDEN"):
        _validate_signed_payload(intent, poisoned, IDENTITY)


def test_exact_order_requires_both_returned_id_and_external_lookup():
    wire = _base_fixture()
    order = copy.deepcopy(wire["filledOrder"])
    intent = type("Intent", (), {
        "venue_order_id": "90001", "external_id": order["externalId"], "account_id": 7001,
        "l2_key": "0x12345", "market": TARGET_MARKET, "side": "BUY", "qty": Decimal("0.001"),
        "price": Decimal("40010"), "fee": Decimal("0.00025"), "nonce": 7,
        "expiry_ms": order["expiryTime"],
        "reduce_only": False,
    })()
    observation = RestObservation(
        1770000000000, 1770000000000, _account(wire), wire["market"],
        {"bids": wire["book"]["bids"], "asks": wire["book"]["asks"]}, wire["account"]["balance"],
        tuple(wire["account"]["fees"]), tuple(wire["account"]["leverage"]),
        (), (), (order,), (), {"90001": order}, {order["externalId"]: (order,)},
    )
    from risex_farmer.extended_testnet_lifecycle_operational import _matching_order
    assert _matching_order(intent, observation, IDENTITY) == order
    missing_id = copy.copy(observation)
    object.__setattr__(missing_id, "exact_by_id", {})
    with pytest.raises(OperationalSafetyError, match="EXACT_ORDER_RESOLUTION_INCOMPLETE"):
        _matching_order(intent, missing_id, IDENTITY)


@pytest.mark.parametrize(
    "body",
    [
        {"status": "OK", "data": [{"id": 1}]},
        {"status": "OK", "data": [{"id": 1}], "pagination": {"cursor": None, "count": 2}},
        {"status": "OK", "data": [{"id": 1}], "pagination": {"cursor": 0, "count": 1}},
    ],
)
def test_nonempty_lists_require_complete_bounded_pagination(body):
    with pytest.raises(OperationalSafetyError):
        _list_data(body, "LIST_SCHEMA")


def test_empty_lists_allow_absent_or_null_pagination_only():
    assert _list_data({"status": "OK", "data": []}, "LIST_SCHEMA") == ([], None)
    assert _list_data({"status": "OK", "data": [], "pagination": None}, "LIST_SCHEMA") == ([], None)
    with pytest.raises(OperationalSafetyError, match="PAGINATION_EMPTY_CONTRADICTION"):
        _list_data({"status": "OK", "data": [], "pagination": {"cursor": 1, "count": 0}}, "LIST_SCHEMA")


def test_pagination_count_and_cursor_bounds_are_strict():
    with pytest.raises(OperationalSafetyError):
        _validate_page_meta({"pagination": {"cursor": None, "count": 0}}, 1, nonempty=True)
    with pytest.raises(OperationalSafetyError):
        _validate_page_meta({"pagination": {"cursor": None, "count": 257}}, 257, nonempty=True)


def test_close_rejects_stale_or_wrong_position_before_signing(tmp_path):
    io = FixtureIO()
    signer = FakeSigner()
    store = OperationalIntentStore(tmp_path / "close.sqlite3")
    journal = RuntimeRunJournal(tmp_path / "close.sqlite3")
    runner = SealedLifecycleRunner(
        store=store, journal=journal, io=io, capability=signer, identity=IDENTITY,
    )
    runner._last_observation = io.observe(())
    entry = runner._prepare_entry(runner._last_observation)
    store.claim(entry.id, expected_lifecycle="ENTRY_PREPARED", next_lifecycle="ENTRY_AMBIGUOUS")
    signed = signer.sign_order(store.get(entry.id), runner._last_observation.market)
    store.bind_signed(entry.id, payload=signed.payload, payload_digest=canonical_digest(signed.payload))
    store.mark_accepted(entry.id, "90001", entry.external_id)
    store.mark_reconciled(entry.id)
    bad = runner._last_observation
    object.__setattr__(bad, "positions", ())
    with pytest.raises(OperationalSafetyError, match="AUTHORITATIVE_POSITION_REQUIRED"):
        runner._prepare_close(entry, bad)


def test_transport_does_not_open_stream_and_uses_fixed_sdk_headers():
    capability = type("Capability", (), {
        "api_key": lambda self: "fixture-api-key",
        "identity": IDENTITY,
    })()
    io = OperationalVenueIO(capability, transport=object(), clock=lambda: 1770000000000)
    with pytest.raises(OperationalSafetyError, match="STREAM_UNAVAILABLE"):
        io.open_stream()
    from risex_farmer.extended_testnet_lifecycle_operational import ExtendedRestTransport
    transport = ExtendedRestTransport("fixture-api-key")
    assert list(transport._headers("fixture-api-key")) == ["Accept", "Content-Type", "User-Agent", "X-Api-Key"]
    assert transport._headers("fixture-api-key")["User-Agent"] == "X10PythonTradingClient/2.5.0"


def test_rest_http_failure_class_is_preserved_without_payload_leakage():
    class HttpFailureTransport:
        def request(self, method, path, *, query=(), body=None, allow_404=False):
            raise RestFailure("REST_HTTP_STATUS", failure_class="HTTP")

    capability = type("Capability", (), {"identity": IDENTITY})()
    io = OperationalVenueIO(capability, transport=HttpFailureTransport(), clock=lambda: 1770000000000)
    with pytest.raises(OperationalSafetyError) as caught:
        io._request("GET", "/user/orders")
    assert caught.value.code == "REST_HTTP_STATUS"
    assert caught.value.failure_class == "HTTP"


def test_production_credential_source_has_fixed_private_key_path_and_no_live_invocation():
    module = importlib.import_module("risex_farmer.extended_testnet_lifecycle_operational")
    assert module.__dict__["STARK_PRIVATE_KEY_BASENAME"] == STARK_PRIVATE_KEY_BASENAME
    assert tuple(inspect.signature(_PasswdHomeCredentialSource.open).parameters) == ("self",)
    assert run.__module__ == "risex_farmer.extended_testnet_lifecycle_operational"


def test_rest_list_follows_one_bounded_cursor_without_retrying_pages():
    class Pages:
        def __init__(self):
            self.calls = []

        def request(self, method, path, *, query=(), body=None, allow_404=False):
            self.calls.append((method, path, tuple(query)))
            cursor = dict(query).get("cursor")
            if cursor is None:
                return {"status": "OK", "data": [{"id": 1}], "pagination": {"cursor": 9, "count": 1}}
            assert cursor == 9
            return {"status": "OK", "data": [{"id": 2}], "pagination": {"cursor": None, "count": 1}}

    pages = Pages()
    capability = type("Capability", (), {"api_key": lambda self: "key", "identity": IDENTITY})()
    io = OperationalVenueIO(capability, transport=pages, clock=lambda: 1770000000000)
    assert io._list("/user/orders", code="ORDER_LIST_SCHEMA") == ({"id": 1}, {"id": 2})
    assert len(pages.calls) == 2
    assert all(call[0] == "GET" for call in pages.calls)
    assert all(dict(call[2])["limit"] == 256 for call in pages.calls)


def test_rest_list_rejects_cursor_replay_and_missing_pagination():
    class Repeating:
        def request(self, method, path, *, query=(), body=None, allow_404=False):
            return {"status": "OK", "data": [{"id": 1}], "pagination": {"cursor": 4, "count": 1}}

    capability = type("Capability", (), {"api_key": lambda self: "key", "identity": IDENTITY})()
    io = OperationalVenueIO(capability, transport=Repeating(), clock=lambda: 1770000000000)
    with pytest.raises(OperationalSafetyError, match="PAGINATION_CURSOR_REPLAY"):
        io._list("/user/orders", code="ORDER_LIST_SCHEMA")


def test_final_barrier_validates_exact_endpoints_in_both_rounds(tmp_path, no_sleep):
    class MissingSecondExact(FixtureIO):
        def __init__(self):
            super().__init__()
            self.closed_observations = 0

        def observe(self, intents):
            value = super().observe(intents)
            if self.phase == "CLOSED":
                self.closed_observations += 1
                if self.closed_observations == 3:
                    value = copy.copy(value)
                    object.__setattr__(value, "exact_by_id", {"90001": value.exact_by_id["90001"]})
            return value

    with pytest.raises(OperationalSafetyError, match="EXACT_ORDER_RESOLUTION_INCOMPLETE"):
        _fixture_run(
            path=tmp_path / "barrier.sqlite3", io=MissingSecondExact(),
            capability=FakeSigner(), identity=IDENTITY,
        )


def test_write_response_external_mismatch_is_terminal_and_not_replayed(tmp_path, no_sleep):
    class WrongExternal(FixtureIO):
        def place_order(self, intent, payload):
            self.place_calls.append((intent.kind, intent.state, intent.external_id, intent.nonce))
            self.phase = "ENTRY_FILLED"
            return WriteReceipt(True, "90001", "different-external-id")

    io = WrongExternal()
    with pytest.raises(OperationalSafetyError, match="WRITE_RESPONSE_IDENTITY_MISMATCH"):
        _fixture_run(path=tmp_path / "identity.sqlite3", io=io, capability=FakeSigner(), identity=IDENTITY)
    assert len(io.place_calls) == 1
    assert OperationalIntentStore(tmp_path / "identity.sqlite3").all()[0].state == "AMBIGUOUS"
