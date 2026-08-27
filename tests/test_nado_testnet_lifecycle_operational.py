from __future__ import annotations

from dataclasses import replace
import brotli
import gzip
import importlib
import inspect
import json
from pathlib import Path
import sqlite3
import zlib

import pytest

import risex_farmer.nado_testnet_lifecycle_operational as nado_operational
from risex_farmer.nado_testnet_lifecycle import (
    ACTIVE_PERP, COMPLETE, EXECUTE_RESPONSE_AMBIGUITY,
    EXECUTE_TRANSPORT_AMBIGUITY, EXECUTE_VENUE_REJECTION, HALTED, IOC_APPENDIX,
    AccountSnapshot, CatalogSnapshot, EngineEvidence, ExecuteFailure, FillEvidence,
    FixedEnvironment, IntentStore, OrderEvidence, Product, OrderIntent,
    SyntheticOrderVector, TriggerSnapshot, build_order_nonce, canonical_payload,
    order_digest, smallest_executable_amount,
)
from risex_farmer.nado_testnet_lifecycle_operational import (
    DurableExecuteFailure, DurableOperationalFailure, OperationalSafetyError,
    OperationalVenueIO,
    OwnerOrderCapability, REDACTED_STORE_PATH, RUN_STORE_BASENAME,
    RECV_WINDOW_MS, SealedLifecycleRunner, TARGET_PRODUCT_ID, TARGET_TICKER_ID,
    _fixture_run, run,
)
from risex_farmer.nado_private_read_preflight import MAX_FRESHNESS_MS


X18 = 10**18
SECRET = bytes.fromhex("00" * 31 + "01")
OWNER = "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf"
SENDER = OWNER + "64656661756c740000000000"
PRIVATE_FIXTURE = (
    Path(__file__).parent / "fixtures/nado_private_read_preflight/official_contract.json"
)


def capability(owner: str) -> OwnerOrderCapability:
    return OwnerOrderCapability(SECRET, owner)


class FixtureIO:
    def __init__(self, entry: str = "RESTING", close: tuple[str, ...] = ("FILLED",)) -> None:
        self.clock = 1_700_000_000_000
        self.entry_result = entry
        self.close_results = list(close)
        self.phase = "FLAT"
        self.intents = []
        self.terminals: dict[str, str] = {}
        self.dispatch_states: list[str] = []
        self.product = Product(
            TARGET_PRODUCT_ID, TARGET_TICKER_ID, ACTIVE_PERP, True,
            10**12, 50 * X18, 50 * X18, 100 * X18,
        )

    def now_ms(self) -> int:
        self.clock += RECV_WINDOW_MS + 1
        return self.clock

    def terminal_status(self, digest: str):
        return self.terminals.get(digest.lower())

    def validate_order(self, order, signature) -> bool:
        assert order.product_id == TARGET_PRODUCT_ID
        assert len(bytes.fromhex(signature[2:])) == 65
        return True

    def dispatch(self, intent, signature) -> str:
        self.dispatch_states.append(self.store.state(intent.digest))
        self.intents.append(intent)
        if intent.kind == "ENTRY":
            self.phase = self.entry_result
        elif intent.kind == "CANCEL_ALL":
            self.phase = "CANCELLED"
            self.terminals[intent.digest.lower()] = "CANCELLED"
            self.terminals[self.intents[0].digest.lower()] = "CANCELLED"
        else:
            result = self.close_results.pop(0)
            self.phase = result
            if result != "FILLED":
                self.terminals[intent.digest.lower()] = result
        return intent.digest

    def observe(self, digests):
        self.clock += 101
        product = self.product
        catalog = CatalogSnapshot((product,), True, self.clock, True, "engine")
        regular = {TARGET_PRODUCT_ID: ()}
        position = 0
        orders = ()
        fills = []
        if self.intents:
            entry = self.intents[0]
            if self.phase == "RESTING":
                regular = {TARGET_PRODUCT_ID: (entry.digest,)}
                orders = (OrderEvidence(entry.digest, TARGET_PRODUCT_ID, entry.nonce,
                                        entry.amount_x18, "OPEN"),)
            elif self.phase == "PARTIAL":
                filled = entry.amount_x18 - self.product.step_x18
                regular = {TARGET_PRODUCT_ID: (entry.digest,)}
                orders = (OrderEvidence(entry.digest, TARGET_PRODUCT_ID, entry.nonce,
                                        entry.amount_x18 - filled, "OPEN"),)
                fills.append(FillEvidence(entry.digest, TARGET_PRODUCT_ID, filled, 1))
                position = filled
            elif self.entry_result in {"FILLED", "PARTIAL"} or self.phase in {
                "FILLED", "CANCELLED", "EXPIRED"
            }:
                if self.entry_result in {"FILLED", "PARTIAL"}:
                    filled = (
                        entry.amount_x18 if self.entry_result == "FILLED"
                        else entry.amount_x18 - self.product.step_x18
                    )
                    fills.append(FillEvidence(entry.digest, TARGET_PRODUCT_ID, filled, 1))
                    position = filled
                if any(item.kind == "CLOSE" for item in self.intents):
                    for index, item in enumerate(
                        (value for value in self.intents if value.kind == "CLOSE"), 2
                    ):
                        if item.digest in self.terminals:
                            continue
                        fills.append(FillEvidence(
                            item.digest, TARGET_PRODUCT_ID, -position, index
                        ))
                        position = 0
        account = AccountSnapshot(
            763373, "Nado", "0.0.1", FixedEnvironment.endpoint,
            FixedEnvironment.gateway, FixedEnvironment.gateway_ws,
            FixedEnvironment.archive, FixedEnvironment.trigger, OWNER, "default",
            self.clock, True, "engine", regular, {TARGET_PRODUCT_ID: position}, (),
            snapshot_id=f"snapshot-{self.clock}",
        )
        trigger = TriggerSnapshot(
            OWNER, "default", self.clock, True, "trigger", (),
            snapshot_id=f"trigger-{self.clock}",
        )
        evidence = EngineEvidence(account, trigger, orders, tuple(fills), self.clock)
        return importlib.import_module(
            "risex_farmer.nado_testnet_lifecycle_operational"
        ).LiveObservation(
            catalog, evidence, product,
            7_765_000_000_000_000, 7_766_000_000_000_000,
        )


class ReceiveWindowFixtureIO(FixtureIO):
    """Keep sealed-runner receive-window tests fast while recording fencing."""

    def __init__(self, entry: str = "RESTING", close: tuple[str, ...] = ("FILLED",)) -> None:
        super().__init__(entry, close)
        self.last_now_ms: int | None = None
        self.write_timings: list[tuple[str, int, int]] = []

    def now_ms(self) -> int:
        self.clock += RECV_WINDOW_MS + 1
        self.last_now_ms = self.clock
        return self.clock

    def dispatch(self, intent, signature) -> str:
        assert self.last_now_ms is not None
        self.write_timings.append((intent.kind, self.last_now_ms, intent.recv_time))
        return super().dispatch(intent, signature)


def assert_nonce_and_digest_binding(intent: OrderIntent) -> None:
    salt = intent.nonce & ((1 << 20) - 1)
    assert intent.nonce == build_order_nonce(intent.recv_time, salt)
    payload = json.loads(intent.payload)
    if intent.kind == "CANCEL_ALL":
        transaction = payload["cancel_product_orders"]["tx"]
        assert int(transaction["nonce"]) == intent.nonce
        return
    operation = payload["place_order"]
    wire_order = operation["order"]
    assert int(operation["product_id"]) == intent.product_id
    assert int(wire_order["nonce"]) == intent.nonce
    order = SyntheticOrderVector(
        intent.owner, intent.subaccount_name, wire_order["sender"],
        int(operation["product_id"]), int(wire_order["priceX18"]),
        int(wire_order["amount"]), int(wire_order["expiration"]),
        intent.recv_time, salt, int(wire_order["nonce"]),
        int(wire_order["appendix"]),
    )
    assert order_digest(order) == intent.digest


def run_fixture(tmp_path: Path, io: FixtureIO):
    path = tmp_path / "nado.sqlite"
    result = _fixture_run(
        path=path, io=io, capability_loader=capability, owner=OWNER, sender=SENDER,
    )
    # inspect through a fresh connection after the runner closes its owner
    io.store = IntentStore(path)
    return result, io.store


def runtime_terminal(path: Path) -> tuple[tuple[object, ...], tuple[object, ...]]:
    connection = sqlite3.connect(path)
    try:
        runtime = connection.execute(
            "SELECT state, failure_class, stage FROM nado_runtime_runs "
            "ORDER BY rowid"
        ).fetchall()
        lifecycle = connection.execute(
            "SELECT status FROM nado_lifecycle_state WHERE singleton = 1"
        ).fetchall()
        assert connection.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        return tuple(runtime), tuple(lifecycle)
    finally:
        connection.close()


def test_zero_argument_surface_and_normal_startup_isolation() -> None:
    assert tuple(inspect.signature(run).parameters) == ()
    assert RUN_STORE_BASENAME.startswith(".risex-funding-farmer-nado-level-c")
    assert REDACTED_STORE_PATH.startswith("<passwd-home>/")
    package = importlib.import_module("risex_farmer")
    assert "nado_testnet_lifecycle_operational" not in Path(package.__file__).read_text()


def test_operational_parser_reuses_accepted_aggregate_account_contract() -> None:
    fixture = json.loads(PRIVATE_FIXTURE.read_text())
    io = OperationalVenueIO(str(fixture["owner"]), str(fixture["sender"]))
    raw_products = fixture["wire"]["all_products"]["data"]
    pairs = {
        item["product_id"]: f"PRODUCT-{item['product_id']}_USDT0"
        for field in ("spot_products", "perp_products")
        for item in raw_products[field]
        if item["product_id"] != 0
    }
    products = io._products(raw_products, pairs)
    account = fixture["wire"]["subaccount_info"]["data"]
    assert not any(io._positions(account, products).values())
    regular, orders = io._orders(fixture["wire"]["orders"]["data"], products)
    assert set(regular) == set(products)
    assert not any(regular.values()) and orders == []


def test_current_min_size_binds_to_quote_notional_and_one_base_step() -> None:
    io = OperationalVenueIO(OWNER, SENDER)
    products = io._products(
        {
            "spot_products": [],
            "perp_products": [{
                "product_id": TARGET_PRODUCT_ID,
                "book_info": {
                    "price_increment_x18": str(10**12),
                    "size_increment": str(50 * X18),
                    "min_size": str(100 * X18),
                },
            }],
        },
        {TARGET_PRODUCT_ID: TARGET_TICKER_ID},
    )
    product = products[TARGET_PRODUCT_ID]
    assert product.minimum_amount_x18 == product.step_x18 == 50 * X18
    assert product.minimum_notional_x18 == 100 * X18
    assert smallest_executable_amount(
        product,
        prices_x18=(7_765_000_000_000_000, 7_766_000_000_000_000),
    ) == 12_900 * X18


def test_v2_pair_identity_binds_exact_selected_regular_perpetual() -> None:
    io = OperationalVenueIO(OWNER, SENDER)
    pairs = io._pairs([{
        "product_id": TARGET_PRODUCT_ID,
        "ticker_id": TARGET_TICKER_ID,
        "base": "SKR-PERP",
        "quote": "USDT0",
        "additive_irrelevant": {"ignored": True},
    }])
    assert pairs == {TARGET_PRODUCT_ID: TARGET_TICKER_ID}
    for changed in (
        {"ticker_id": "PUMP-PERP_USDT0"},
        {"base": "PUMP-PERP"},
        {"quote": "USDC"},
    ):
        invalid = {
            "product_id": TARGET_PRODUCT_ID,
            "ticker_id": TARGET_TICKER_ID,
            "base": "SKR-PERP",
            "quote": "USDT0",
        }
        invalid.update(changed)
        with pytest.raises(OperationalSafetyError, match="V2 pair identity"):
            io._pairs([invalid])


@pytest.mark.parametrize("missing", ["product_id", "ticker_id", "base", "quote"])
def test_v2_pair_identity_rejects_missing_required_field(missing: str) -> None:
    pair = {
        "product_id": TARGET_PRODUCT_ID,
        "ticker_id": TARGET_TICKER_ID,
        "base": "SKR-PERP",
        "quote": "USDT0",
    }
    pair.pop(missing)
    with pytest.raises(OperationalSafetyError, match="V2 pair identity"):
        OperationalVenueIO(OWNER, SENDER)._pairs([pair])


def test_v2_pair_identity_rejects_duplicate_product_id() -> None:
    pair = {
        "product_id": TARGET_PRODUCT_ID,
        "ticker_id": TARGET_TICKER_ID,
        "base": "SKR-PERP",
        "quote": "USDT0",
    }
    with pytest.raises(OperationalSafetyError, match="V2 pair identity"):
        OperationalVenueIO(OWNER, SENDER)._pairs([pair, dict(pair)])


def test_entry_rejects_same_product_id_with_wrong_v2_ticker() -> None:
    io = FixtureIO()
    observed = io.observe(())
    observed = replace(
        observed, product=replace(observed.product, symbol="PUMP-PERP_USDT0")
    )
    module = importlib.import_module("risex_farmer.nado_testnet_lifecycle_operational")
    with pytest.raises(OperationalSafetyError, match="product identity"):
        module._entry_order(observed, OWNER, SENDER, io.now_ms() + RECV_WINDOW_MS)


def test_entry_is_smallest_tick_aligned_ten_percent_buffered_ioc_buy() -> None:
    io = FixtureIO()
    observed = io.observe(())
    order = importlib.import_module(
        "risex_farmer.nado_testnet_lifecycle_operational"
    )._entry_order(observed, OWNER, SENDER, io.now_ms() + RECV_WINDOW_MS)
    assert order.product_id == TARGET_PRODUCT_ID
    assert order.price_x18 == 8_543_000_000_000_000
    assert order.price_x18 % observed.product.tick_x18 == 0
    assert order.price_x18 * 100 >= observed.ask_x18 * 110
    assert (order.price_x18 - observed.product.tick_x18) * 100 < observed.ask_x18 * 110
    assert order.appendix == IOC_APPENDIX
    assert order.amount_x18 == 12_900 * X18


def test_entry_buffer_rounds_exact_ten_percent_bound_without_extra_tick() -> None:
    io = FixtureIO()
    observed = replace(io.observe(()), ask_x18=8_000_000_000_000_000)
    order = importlib.import_module(
        "risex_farmer.nado_testnet_lifecycle_operational"
    )._entry_order(observed, OWNER, SENDER, io.now_ms() + RECV_WINDOW_MS)
    assert order.price_x18 == 8_800_000_000_000_000
    assert order.amount_x18 == 12_900 * X18


def test_sealed_order_receive_window_matches_sdk_and_forbids_legacy_100_ms() -> None:
    assert RECV_WINDOW_MS == 90_000
    assert RECV_WINDOW_MS != 100


def test_entry_cancel_close_use_sdk_window_and_bind_nonce_digest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module(
        "risex_farmer.nado_testnet_lifecycle_operational"
    )
    monkeypatch.setattr(module, "_salt", lambda: 7)

    def run_at(path: Path, initial_clock: int):
        io = ReceiveWindowFixtureIO("PARTIAL")
        io.clock = initial_clock
        _fixture_run(
            path=path, io=io, capability_loader=capability,
            owner=OWNER, sender=SENDER,
        )
        store = IntentStore(path)
        try:
            intents = tuple(intent for intent, _state in store.intents())
        finally:
            store.close()
        return io, intents

    first_io, first_intents = run_at(
        tmp_path / "first.sqlite", 1_700_000_000_000
    )
    second_io, second_intents = run_at(
        tmp_path / "second.sqlite", 1_700_000_000_001
    )
    expected_kinds = ("ENTRY", "CANCEL_ALL", "CLOSE")
    assert tuple(intent.kind for intent in first_intents) == expected_kinds
    assert tuple(intent.kind for intent in second_intents) == expected_kinds

    for io in (first_io, second_io):
        assert tuple(kind for kind, _issued, _recv in io.write_timings) == expected_kinds
        assert tuple(
            recv - issued for _kind, issued, recv in io.write_timings
        ) == (90_000, 90_000, 90_000)
        assert all(
            recv - issued != 100 for _kind, issued, recv in io.write_timings
        )

    for first, second in zip(first_intents, second_intents):
        assert_nonce_and_digest_binding(first)
        assert_nonce_and_digest_binding(second)
        assert first.recv_time != second.recv_time
        assert first.nonce != second.nonce
        # With the same salt and all other fixture inputs fixed, moving only
        # receive time must move every durable intent digest, including cancel.
        assert first.digest != second.digest


def test_trigger_read_reuses_fresh_server_time_envelope(monkeypatch) -> None:
    module = importlib.import_module("risex_farmer.nado_testnet_lifecycle_operational")
    now_ms = 1_700_000_000_500
    server_ms = now_ms - 200
    stale_recv = server_ms + MAX_FRESHNESS_MS - 1
    requests = []

    class TriggerCapability:
        def sign_list_trigger_orders(self, typed):
            return "0x" + "11" * 65

        def close(self):
            return None

    monkeypatch.setattr(module, "_load_owner_capability", lambda _sender: TriggerCapability())
    monkeypatch.setattr(module, "_recover_owner", lambda _typed, _signature: OWNER)
    io = OperationalVenueIO(OWNER, SENDER)
    io.now_ms = lambda: now_ms

    def trigger_response(recv: int):
        if recv == stale_recv:
            return {
                "status": "failure", "request_type": "query_list_trigger_orders",
                "error_code": 1000, "error": "expired",
            }
        assert recv == server_ms + MAX_FRESHNESS_MS
        return {
            "status": "success", "request_type": "query_list_trigger_orders",
            "data": {"orders": []},
        }

    assert trigger_response(stale_recv) == {
        "status": "failure", "request_type": "query_list_trigger_orders",
        "error_code": 1000, "error": "expired",
    }

    def post(host, path, body):
        requests.append((host, path, body))
        if path == "/v1/edge/query":
            return {
                "status": "success", "method": "time", "id": None,
                "server_time": str(server_ms),
            }
        return trigger_response(int(body["tx"]["recvTime"]))

    io._post = post
    assert io._triggers() == ()
    assert requests == [
        ("gateway.test.nado.xyz", "/v1/edge/query", {"type": "time"}),
        (
            "trigger.test.nado.xyz", "/v1/query",
            {
                "type": "list_trigger_orders",
                "tx": {
                    "sender": SENDER,
                    "recvTime": str(server_ms + MAX_FRESHNESS_MS),
                },
                "signature": "0x" + "11" * 65,
                "limit": 500,
            },
        ),
    ]


def test_place_write_binding_emits_exact_full_official_payload() -> None:
    io = OperationalVenueIO(OWNER, SENDER)
    recv, salt = 1_700_000_000_100, 7
    order = SyntheticOrderVector(
        OWNER, "default", SENDER, TARGET_PRODUCT_ID, 7_765_000_000_000_000,
        650 * X18, 2**32 - 1,
        recv, salt, build_order_nonce(recv, salt), 1537,
    )
    intent = OrderIntent(
        "ENTRY", TARGET_PRODUCT_ID, order.nonce, recv, order_digest(order),
        canonical_payload(order.as_payload()), order.amount_x18, order.appendix,
        sender=SENDER, owner=OWNER, subaccount_name="default",
    )
    captured = {}
    def post(host, path, body):
        captured.update(host=host, path=path, body=body)
        return {
            "status": "success", "request_type": "execute_place_order",
            "data": {"digest": intent.digest},
        }
    io._post = post
    signature = "0x" + "11" * 65
    assert io.dispatch(intent, signature) == intent.digest
    assert captured["host"] == "gateway.test.nado.xyz"
    assert captured["path"] == "/v1/execute"
    operation = captured["body"]["place_order"]
    assert captured["body"] == {
        "place_order": {
            "product_id": TARGET_PRODUCT_ID,
            "order": order.as_payload()["place_order"]["order"],
            "signature": signature,
            "digest": intent.digest,
            "id": int.from_bytes(bytes.fromhex(intent.digest[2:10]), "big"),
        }
    }


def _cancel_intent() -> OrderIntent:
    nonce = build_order_nonce(1_700_000_000_100, 8)
    return OrderIntent(
        "CANCEL_ALL", None, nonce, 1_700_000_000_100, "0x" + "22" * 32,
        canonical_payload({
            "cancel_product_orders": {
                "tx": {"sender": SENDER, "productIds": [], "nonce": str(nonce)},
            }
        }), sender=SENDER, owner=OWNER, subaccount_name="default",
    )


def test_cancel_write_binding_emits_exact_payload_and_validates_resting_entry() -> None:
    io = OperationalVenueIO(OWNER, SENDER)
    entry_digest = "0x" + "11" * 32
    entry_nonce = build_order_nonce(1_700_000_000_000, 7)
    remaining = 10**16
    io._resting_orders = {
        entry_digest: OrderEvidence(
            entry_digest, TARGET_PRODUCT_ID, entry_nonce, remaining, "OPEN"
        )
    }
    intent = _cancel_intent()
    captured = {}
    def post(host, path, body):
        captured.update(host=host, path=path, body=body)
        return {
            "status": "success", "request_type": "execute_cancel_product_orders",
            "data": {"cancelled_orders": [{
                "digest": entry_digest, "product_id": TARGET_PRODUCT_ID,
                "sender": SENDER,
                "nonce": str(entry_nonce), "unfilled_amount": str(remaining),
                "additive": "ignored response field",
            }]},
        }
    io._post = post
    signature = "0x" + "33" * 65
    assert io.dispatch(intent, signature) == intent.digest
    assert captured == {
        "host": "gateway.test.nado.xyz", "path": "/v1/execute",
        "body": {
            "cancel_product_orders": {
                "tx": {
                    "sender": SENDER, "productIds": [], "nonce": str(intent.nonce),
                },
                "signature": signature,
            }
        },
    }


@pytest.mark.parametrize(
    ("field", "wrong"),
    [
        ("digest", "0x" + "44" * 32),
        ("product_id", 3),
        ("sender", "0x" + "55" * 32),
        ("nonce", "1"),
        ("unfilled_amount", "1"),
    ],
)
def test_cancel_response_rejects_non_exact_resting_entry(field: str, wrong: object) -> None:
    io = OperationalVenueIO(OWNER, SENDER)
    entry_digest = "0x" + "11" * 32
    entry_nonce = build_order_nonce(1_700_000_000_000, 7)
    remaining = 10**16
    io._resting_orders = {
        entry_digest: OrderEvidence(
            entry_digest, TARGET_PRODUCT_ID, entry_nonce, remaining, "OPEN"
        )
    }
    cancelled = {
        "digest": entry_digest, "product_id": TARGET_PRODUCT_ID, "sender": SENDER,
        "nonce": str(entry_nonce), "unfilled_amount": str(remaining),
    }
    cancelled[field] = wrong
    io._post = lambda *_args: {
        "status": "success", "request_type": "execute_cancel_product_orders",
        "data": {"cancelled_orders": [cancelled]},
    }
    with pytest.raises(ExecuteFailure, match=EXECUTE_RESPONSE_AMBIGUITY):
        io.dispatch(_cancel_intent(), "0x" + "33" * 65)


def _prepared_execute_intent(path: Path) -> tuple[IntentStore, OrderIntent]:
    recv, salt = 1_700_000_000_100, 7
    order = SyntheticOrderVector(
        OWNER, "default", SENDER, TARGET_PRODUCT_ID, 7_766_000_000_000_000,
        650 * X18, 2**32 - 1, recv, salt, build_order_nonce(recv, salt),
        IOC_APPENDIX,
    )
    intent = OrderIntent(
        "ENTRY", TARGET_PRODUCT_ID, order.nonce, recv, order_digest(order),
        canonical_payload(order.as_payload()), order.amount_x18, order.appendix,
        sender=SENDER, owner=OWNER, subaccount_name="default",
    )
    store = IntentStore(path)
    store.prepare(intent)
    return store, intent


def test_terminal_execute_rejection_is_sanitized_durable_and_not_ambiguous(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rejected.sqlite"
    store, intent = _prepared_execute_intent(path)
    io = OperationalVenueIO(OWNER, SENDER)
    io._post = lambda *_args: {
        "status": "failure", "request_type": "execute_place_order",
        "error_code": 2020, "error": "RAW_VENUE_DETAIL",
        "additive": {"raw": "ignored"},
    }
    try:
        with pytest.raises(ExecuteFailure, match=EXECUTE_VENUE_REJECTION) as caught:
            store.dispatch_prepared(
                intent.digest, lambda durable: io.dispatch(durable, "0x" + "11" * 65)
            )
        assert "RAW_VENUE_DETAIL" not in str(caught.value)
        assert store.state(intent.digest) == "REJECTED"
        assert store.lifecycle_status() == HALTED
        assert store.execute_failure(intent.digest) == (EXECUTE_VENUE_REJECTION, 2020)
    finally:
        store.close()
    assert b"RAW_VENUE_DETAIL" not in path.read_bytes()


def test_runner_propagates_only_durable_terminal_rejection_class_and_code(
    tmp_path: Path,
) -> None:
    io = FixtureIO()

    def reject(_intent, _signature):
        raise ExecuteFailure(EXECUTE_VENUE_REJECTION, 2011)

    io.dispatch = reject
    with pytest.raises(DurableExecuteFailure) as caught:
        _fixture_run(
            path=tmp_path / "sealed-rejection.sqlite", io=io,
            capability_loader=capability, owner=OWNER, sender=SENDER,
        )
    assert caught.value.failure_class == EXECUTE_VENUE_REJECTION
    assert caught.value.venue_code == 2011

    store = IntentStore(tmp_path / "sealed-rejection.sqlite")
    try:
        intent, state = store.intents()[0]
        assert state == "REJECTED"
        assert store.lifecycle_status() == HALTED
        assert store.execute_failure(intent.digest) == (EXECUTE_VENUE_REJECTION, 2011)
    finally:
        store.close()


@pytest.mark.parametrize(
    ("failure_class", "venue_code"),
    [
        (EXECUTE_VENUE_REJECTION, 2011),
        (EXECUTE_TRANSPORT_AMBIGUITY, None),
        (EXECUTE_RESPONSE_AMBIGUITY, None),
    ],
)
def test_outer_sealed_report_preserves_execute_failure_class_and_code(
    monkeypatch, capsys, failure_class: str, venue_code: int | None,
) -> None:
    module = importlib.import_module("risex_farmer.nado_testnet_lifecycle_operational")
    monkeypatch.setattr(
        module, "run",
        lambda: (_ for _ in ()).throw(
            DurableExecuteFailure(failure_class, venue_code)
        ),
    )
    module.main()
    assert json.loads(capsys.readouterr().out) == {
        "schema_version": 1,
        "status": "BLOCKED",
        "path": REDACTED_STORE_PATH,
        "reason": failure_class,
        "venue_code": venue_code,
    }


def test_outer_sealed_report_sanitizes_unknown_failure_without_generic_reason(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    module = importlib.import_module(
        "risex_farmer.nado_testnet_lifecycle_operational"
    )
    monkeypatch.setattr(
        module, "run",
        lambda: (_ for _ in ()).throw(RuntimeError("RAW_OUTER_FAILURE")),
    )
    module.main()
    report = json.loads(capsys.readouterr().out)
    assert report == {
        "schema_version": 1, "status": "BLOCKED", "path": REDACTED_STORE_PATH,
        "reason": "UNEXPECTED_FAILURE", "stage": "OUTER",
    }
    assert "RAW_OUTER_FAILURE" not in json.dumps(report)


@pytest.mark.parametrize(
    ("response", "failure_class"),
    [
        (TimeoutError("RAW_TRANSPORT_DETAIL"), EXECUTE_TRANSPORT_AMBIGUITY),
        ({"status": "success"}, EXECUTE_RESPONSE_AMBIGUITY),
    ],
)
def test_ambiguous_execute_failure_class_is_durable_and_never_replayable(
    tmp_path: Path, response: object, failure_class: str,
) -> None:
    path = tmp_path / f"{failure_class}.sqlite"
    store, intent = _prepared_execute_intent(path)
    io = OperationalVenueIO(OWNER, SENDER)
    if isinstance(response, BaseException):
        io._post = lambda *_args: (_ for _ in ()).throw(response)
    else:
        io._post = lambda *_args: response
    try:
        with pytest.raises(ExecuteFailure, match=failure_class) as caught:
            store.dispatch_prepared(
                intent.digest, lambda durable: io.dispatch(durable, "0x" + "11" * 65)
            )
        assert "RAW_TRANSPORT_DETAIL" not in str(caught.value)
        assert store.state(intent.digest) == "AMBIGUOUS"
        assert store.lifecycle_status() == HALTED
        assert store.execute_failure(intent.digest) == (failure_class, None)
        with pytest.raises(Exception, match="prepared intent"):
            store.dispatch_prepared(
                intent.digest, lambda durable: io.dispatch(durable, "0x" + "11" * 65)
            )
    finally:
        store.close()
    assert b"RAW_TRANSPORT_DETAIL" not in path.read_bytes()


class _Response:
    def __init__(self, body: bytes, encoding: str | None) -> None:
        self.status = 200
        self.body = body
        self.encoding = encoding

    def getheader(self, name: str):
        if name == "Content-Length":
            return str(len(self.body))
        if name == "Content-Encoding":
            return self.encoding
        return None

    def read(self, maximum: int) -> bytes:
        return self.body[:maximum]


class _Connection:
    def __init__(self, response: _Response) -> None:
        self.response = response
        self.request_args = None

    def request(self, *args):
        self.request_args = args

    def getresponse(self):
        return self.response

    def close(self):
        return None


@pytest.mark.parametrize(
    ("encoding", "compress"),
    [
        ("gzip", gzip.compress),
        ("br", brotli.compress),
        ("deflate", zlib.compress),
    ],
)
def test_transport_requests_and_strictly_decodes_required_encodings(
    encoding: str, compress,
) -> None:
    raw = b'{"status":"success"}'
    connection = _Connection(_Response(compress(raw), encoding))
    io = OperationalVenueIO(OWNER, SENDER)
    io._connection_factory = lambda _host: connection
    assert io._post("gateway.test.nado.xyz", "/v1/query", {"type": "status"}) == {
        "status": "success"
    }
    assert connection.request_args[3]["Accept-Encoding"] == "gzip, br, deflate"


@pytest.mark.parametrize(
    ("request_type", "body_bytes", "accepted"),
    [
        ("all_products", 66_250, True),
        (
            "all_products",
            nado_operational.ALL_PRODUCTS_MAX_RESPONSE_BYTES + 1,
            False,
        ),
        ("status", 65_536, True),
        ("status", 65_537, False),
    ],
)
def test_level_c_gateway_response_limits_are_endpoint_local_and_bounded(
    request_type: str, body_bytes: int, accepted: bool,
) -> None:
    prefix, suffix = b'{"padding":"', b'"}'
    padding_bytes = body_bytes - len(prefix) - len(suffix)
    raw = prefix + b"x" * padding_bytes + suffix
    assert len(raw) == body_bytes
    connection = _Connection(_Response(raw, None))
    io = OperationalVenueIO(OWNER, SENDER)
    io._connection_factory = lambda _host: connection

    if accepted:
        assert io._post(
            "gateway.test.nado.xyz", "/v1/query", {"type": request_type},
        ) == {"padding": "x" * padding_bytes}
    else:
        with pytest.raises(
            OperationalSafetyError,
            match="transport response (schema rejected|size exceeded)",
        ):
            io._post(
                "gateway.test.nado.xyz", "/v1/query", {"type": request_type},
            )


def test_level_c_compressed_all_products_is_bounded_after_decode() -> None:
    prefix, suffix = b'{"padding":"', b'"}'
    body_bytes = nado_operational.ALL_PRODUCTS_MAX_RESPONSE_BYTES + 1
    padding_bytes = body_bytes - len(prefix) - len(suffix)
    raw = prefix + b"x" * padding_bytes + suffix
    compressed = gzip.compress(raw)
    assert len(compressed) < nado_operational.MAX_RESPONSE_BYTES
    connection = _Connection(_Response(compressed, "gzip"))
    io = OperationalVenueIO(OWNER, SENDER)
    io._connection_factory = lambda _host: connection

    with pytest.raises(
        OperationalSafetyError,
        match="transport (content encoding rejected|response size exceeded)",
    ):
        io._post("gateway.test.nado.xyz", "/v1/query", {"type": "all_products"})


def test_v2_pairs_transport_is_fixed_get_without_request_body() -> None:
    raw = b'[{"product_id":44,"ticker_id":"SKR-PERP_USDT0",' \
          b'"base":"SKR-PERP","quote":"USDT0"}]'
    connection = _Connection(_Response(raw, None))
    io = OperationalVenueIO(OWNER, SENDER)
    io._connection_factory = lambda _host: connection
    assert io._get("gateway.test.nado.xyz", "/v2/pairs")[0]["product_id"] == 44
    assert connection.request_args[:3] == ("GET", "/v2/pairs", None)


def test_transport_failure_class_retains_http_and_schema_boundaries() -> None:
    module = importlib.import_module(
        "risex_farmer.nado_testnet_lifecycle_operational"
    )
    http_connection = _Connection(_Response(b"{}", None))
    http_connection.response.status = 503
    io = OperationalVenueIO(OWNER, SENDER)
    io._connection_factory = lambda _host: http_connection
    with pytest.raises(OperationalSafetyError, match="HTTP status") as http_error:
        io._post("gateway.test.nado.xyz", "/v1/query", {"type": "status"})
    assert module._failure_class(http_error.value) == "HTTP"

    schema_connection = _Connection(_Response(b"not-json", None))
    io._connection_factory = lambda _host: schema_connection
    with pytest.raises(OperationalSafetyError, match="schema") as schema_error:
        io._post("gateway.test.nado.xyz", "/v1/query", {"type": "status"})
    assert module._failure_class(schema_error.value) == "SCHEMA"


@pytest.mark.parametrize("encoding", ["compress", "gzip, br", "x-gzip"])
def test_transport_rejects_unknown_or_composed_content_encoding(encoding: str) -> None:
    connection = _Connection(_Response(b"{}", encoding))
    io = OperationalVenueIO(OWNER, SENDER)
    io._connection_factory = lambda _host: connection
    with pytest.raises(OperationalSafetyError, match="content encoding"):
        io._post("gateway.test.nado.xyz", "/v1/query", {"type": "status"})


def test_unexpected_resting_ioc_is_exactly_cancelled_and_finishes_flat(tmp_path: Path) -> None:
    io = FixtureIO("RESTING")
    report, store = run_fixture(tmp_path, io)
    try:
        assert report.status == COMPLETE
        assert report.writes == 2 and report.close_attempts == 0
        assert report.final_zero_regular and report.final_zero_trigger and report.final_exact_flat
        assert io.dispatch_states == ["PREPARED", "PREPARED"]
        assert [intent.kind for intent, _ in store.intents()] == ["ENTRY", "CANCEL_ALL"]
        assert all(state == "RECONCILED" for _, state in store.intents())
    finally:
        store.close()


def test_full_entry_uses_fresh_position_reduce_only_ioc_close(tmp_path: Path) -> None:
    io = FixtureIO("FILLED")
    report, store = run_fixture(tmp_path, io)
    try:
        assert report.status == COMPLETE and report.writes == 2
        entry, close = [intent for intent, _ in store.intents()]
        assert entry.kind == "ENTRY" and entry.appendix == IOC_APPENDIX
        assert close.kind == "CLOSE" and close.appendix == 2561
        assert close.starting_position_x18 == entry.amount_x18
        assert close.amount_x18 == -entry.amount_x18
        assert close.snapshot_id and close.snapshot_observed_at_ms
    finally:
        store.close()


def test_partial_ioc_fill_is_cancelled_then_clamped_close_uses_residual(
    tmp_path: Path,
) -> None:
    io = FixtureIO("PARTIAL")
    report, store = run_fixture(tmp_path, io)
    try:
        assert report.status == COMPLETE and report.writes == 3
        entry, cancel, close = [intent for intent, _ in store.intents()]
        assert [entry.kind, cancel.kind, close.kind] == ["ENTRY", "CANCEL_ALL", "CLOSE"]
        assert close.starting_position_x18 == entry.amount_x18 - io.product.step_x18
        assert close.clamp_expected is True
        assert close.amount_x18 == -entry.amount_x18
        fills = store.persisted_fill_map()
        assert sum(amount for _, amount in fills.values()) == 0
    finally:
        store.close()


def test_restart_with_existing_or_ambiguous_intent_never_dispatches(tmp_path: Path) -> None:
    io = FixtureIO("RESTING")
    report, store = run_fixture(tmp_path, io)
    store.close()
    writes = len(io.dispatch_states)
    with pytest.raises(OperationalSafetyError, match="existing lifecycle"):
        _fixture_run(
            path=tmp_path / "nado.sqlite", io=io, capability_loader=capability,
            owner=OWNER, sender=SENDER,
        )
    assert len(io.dispatch_states) == writes


def test_prewrite_blocked_runtime_row_is_terminal_without_inventing_intent(
    tmp_path: Path,
) -> None:
    path = tmp_path / "nado.sqlite"
    blocked = FixtureIO("RESTING")
    blocked.observe = lambda _digests: (_ for _ in ()).throw(
        RuntimeError("RAW_UNKNOWN_PRE_INTENT_FAILURE")
    )
    with pytest.raises(DurableOperationalFailure) as caught:
        _fixture_run(
            path=path, io=blocked, capability_loader=capability,
            owner=OWNER, sender=SENDER,
        )
    assert caught.value.failure_class == "UNEXPECTED_FAILURE"
    assert caught.value.stage == "LIVE_OBSERVATION"

    assert runtime_terminal(path) == (
        (("BLOCKED", "UNEXPECTED_FAILURE", "LIVE_OBSERVATION"),),
        (("HALTED",),),
    )
    with pytest.raises(DurableOperationalFailure) as retry:
        _fixture_run(
            path=path, io=FixtureIO("RESTING"), capability_loader=capability,
            owner=OWNER, sender=SENDER,
        )
    assert retry.value.failure_class == "UNEXPECTED_FAILURE"
    assert retry.value.stage == "RUNNER_STARTUP"
    runtime, lifecycle = runtime_terminal(path)
    assert len(runtime) == 2
    assert all(row[0] == "BLOCKED" for row in runtime)
    assert lifecycle == (("HALTED",),)


@pytest.mark.parametrize(
    ("failure_kind", "failure_class", "stage"),
    [
        ("observe_transport", "TRANSPORT", "LIVE_OBSERVATION"),
        ("observe_unknown", "UNEXPECTED_FAILURE", "LIVE_OBSERVATION"),
        ("derive_identity", "IDENTITY", "ORDER_DERIVATION"),
        ("preflight_safety", "SAFETY", "ENTRY_PREFLIGHT"),
        ("signature_auth", "AUTH", "ENTRY_SIGNATURE"),
        ("validation_auth", "AUTH", "ENTRY_VALIDATION"),
        ("prepare_contract", "SAFETY", "ENTRY_PREPARATION"),
        ("prepare_unknown", "UNEXPECTED_FAILURE", "ENTRY_PREPARATION"),
    ],
)
def test_each_pre_intent_stage_persists_only_sanitized_terminal_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure_kind: str,
    failure_class: str, stage: str,
) -> None:
    module = importlib.import_module(
        "risex_farmer.nado_testnet_lifecycle_operational"
    )
    io = FixtureIO("RESTING")
    loader = capability
    if failure_kind == "observe_transport":
        io.observe = lambda _digests: (_ for _ in ()).throw(
            OperationalSafetyError("transport outcome requires manual recovery")
        )
    elif failure_kind == "observe_unknown":
        io.observe = lambda _digests: (_ for _ in ()).throw(
            RuntimeError("RAW_UNKNOWN_OBSERVATION")
        )
    elif failure_kind == "derive_identity":
        monkeypatch.setattr(
            module, "_entry_order",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                OperationalSafetyError("fixed target product identity unavailable")
            ),
        )
    elif failure_kind == "preflight_safety":
        monkeypatch.setattr(
            module, "validate_entry_preflight",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                module.NadoPreflightError("public evidence temporal order mismatch")
            ),
        )
    elif failure_kind == "signature_auth":
        loader = lambda _owner: (_ for _ in ()).throw(
            OperationalSafetyError("owner capability unavailable")
        )
    elif failure_kind == "validation_auth":
        io.validate_order = lambda *_args: (_ for _ in ()).throw(
            OperationalSafetyError("signed order validation failed")
        )
    elif failure_kind == "prepare_contract":
        monkeypatch.setattr(
            module.LifecycleCore, "prepare_entry",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                module.NadoContractError("entry preflight contract rejected")
            ),
        )
    elif failure_kind == "prepare_unknown":
        monkeypatch.setattr(
            module.LifecycleCore, "prepare_entry",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                RuntimeError("RAW_UNKNOWN_PREPARE_FAILURE")
            ),
        )
    else:
        raise AssertionError(failure_kind)

    path = tmp_path / f"{failure_kind}.sqlite"
    with pytest.raises(DurableOperationalFailure) as caught:
        _fixture_run(
            path=path, io=io, capability_loader=loader,
            owner=OWNER, sender=SENDER,
        )
    assert (caught.value.failure_class, caught.value.stage) == (failure_class, stage)
    assert runtime_terminal(path) == (
        (("BLOCKED", failure_class, stage),),
        (("HALTED",),),
    )
    connection = sqlite3.connect(path)
    try:
        assert connection.execute("SELECT COUNT(*) FROM nado_intents").fetchone() == (0,)
        assert connection.execute(
            "SELECT COUNT(*) FROM nado_execute_failures"
        ).fetchone() == (0,)
    finally:
        connection.close()


def test_embedded_private_read_barrier_class_is_returned_and_durable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    module = importlib.import_module(
        "risex_farmer.nado_testnet_lifecycle_operational"
    )
    path = tmp_path / "sealed-private-read.sqlite"
    monkeypatch.setattr(module, "_strict_identity", lambda: (OWNER, SENDER))
    monkeypatch.setattr(module, "_production_store_path", lambda: path)

    async def blocked_private_read():
        return {
            "status": "BLOCKED", "failure_class": "SCHEMA",
            "reason": "RAW_PRIVATE_READ_DETAIL",
        }

    monkeypatch.setattr(module, "_accepted_private_read", blocked_private_read)
    module.main()
    report = json.loads(capsys.readouterr().out)
    assert report == {
        "schema_version": 1, "status": "BLOCKED", "path": REDACTED_STORE_PATH,
        "reason": "SCHEMA", "stage": "PRIVATE_READ_BARRIER",
    }
    assert runtime_terminal(path) == (
        (("BLOCKED", "SCHEMA", "PRIVATE_READ_BARRIER"),),
        (("HALTED",),),
    )
    assert b"RAW_PRIVATE_READ_DETAIL" not in path.read_bytes()


def test_close_residual_stops_after_three_durable_attempts(tmp_path: Path) -> None:
    io = FixtureIO("FILLED", ("CANCELLED", "CANCELLED", "CANCELLED"))
    with pytest.raises(OperationalSafetyError, match="three close attempts"):
        _fixture_run(
            path=tmp_path / "nado.sqlite", io=io, capability_loader=capability,
            owner=OWNER, sender=SENDER,
        )
    from risex_farmer.nado_testnet_lifecycle import IntentStore
    store = IntentStore(tmp_path / "nado.sqlite")
    try:
        assert store.count_kind("CLOSE") == 3
        assert store.lifecycle_status() == HALTED
        assert io.dispatch_states == ["PREPARED"] * 4
    finally:
        store.close()


def test_dispatch_exception_is_durable_ambiguous_and_never_replayed(tmp_path: Path) -> None:
    io = FixtureIO("RESTING")
    def ambiguous(intent, signature):
        io.dispatch_states.append(io.store.state(intent.digest))
        raise TimeoutError("synthetic ambiguous transport")
    io.dispatch = ambiguous
    with pytest.raises(TimeoutError):
        _fixture_run(
            path=tmp_path / "nado.sqlite", io=io, capability_loader=capability,
            owner=OWNER, sender=SENDER,
        )
    store = IntentStore(tmp_path / "nado.sqlite")
    try:
        assert store.lifecycle_status() == HALTED
        assert len(store.intents()) == 1
        assert store.intents()[0][1] == "AMBIGUOUS"
        assert io.dispatch_states == ["PREPARED"]
    finally:
        store.close()
    with pytest.raises(OperationalSafetyError, match="existing lifecycle"):
        _fixture_run(
            path=tmp_path / "nado.sqlite", io=io, capability_loader=capability,
            owner=OWNER, sender=SENDER,
        )
    assert io.dispatch_states == ["PREPARED"]


def test_unrelated_prestate_stops_before_capability_or_dispatch(tmp_path: Path) -> None:
    io = FixtureIO("RESTING")
    original = io.observe
    def unrelated(digests):
        value = original(digests)
        account = replace(
            value.evidence.account, cross_perp_amounts_x18={TARGET_PRODUCT_ID: X18}
        )
        return replace(value, evidence=replace(value.evidence, account=account))
    io.observe = unrelated
    loads = 0
    def loader(owner):
        nonlocal loads
        loads += 1
        return capability(owner)
    with pytest.raises(Exception):
        _fixture_run(
            path=tmp_path / "nado.sqlite", io=io, capability_loader=loader,
            owner=OWNER, sender=SENDER,
        )
    assert loads == 0 and not io.dispatch_states


def test_final_barrier_rejects_reintroduced_trigger_state(tmp_path: Path) -> None:
    io = FixtureIO("RESTING")
    original = io.observe
    calls = 0
    def triggered(digests):
        nonlocal calls
        calls += 1
        value = original(digests)
        if calls >= 4:
            triggers = replace(value.evidence.triggers, active_digests=("0x" + "11" * 32,))
            return replace(value, evidence=replace(value.evidence, triggers=triggers))
        return value
    io.observe = triggered
    with pytest.raises(OperationalSafetyError, match="terminal"):
        _fixture_run(
            path=tmp_path / "nado.sqlite", io=io, capability_loader=capability,
            owner=OWNER, sender=SENDER,
        )
