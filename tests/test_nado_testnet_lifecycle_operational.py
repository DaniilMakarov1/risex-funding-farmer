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

from risex_farmer.nado_testnet_lifecycle import (
    ACTIVE_PERP, COMPLETE, HALTED, AccountSnapshot, CatalogSnapshot,
    EngineEvidence, FillEvidence, FixedEnvironment, IntentStore, OrderEvidence, Product,
    OrderIntent, SyntheticOrderVector, TriggerSnapshot, build_order_nonce,
    canonical_payload, order_digest,
)
from risex_farmer.nado_testnet_lifecycle_operational import (
    OperationalSafetyError, OperationalVenueIO, OwnerOrderCapability, REDACTED_STORE_PATH,
    RUN_STORE_BASENAME, SealedLifecycleRunner, TARGET_PRODUCT_ID, TARGET_TICKER_ID,
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
            10**12, 50 * X18, 100 * X18, 5 * X18,
        )

    def now_ms(self) -> int:
        self.clock += 101
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


def run_fixture(tmp_path: Path, io: FixtureIO):
    path = tmp_path / "nado.sqlite"
    result = _fixture_run(
        path=path, io=io, capability_loader=capability, owner=OWNER, sender=SENDER,
    )
    # inspect through a fresh connection after the runner closes its owner
    io.store = IntentStore(path)
    return result, io.store


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
        module._entry_order(observed, OWNER, SENDER, io.now_ms() + 100)


def test_trigger_read_reuses_fresh_server_time_envelope(monkeypatch) -> None:
    module = importlib.import_module("risex_farmer.nado_testnet_lifecycle_operational")
    now_ms = 1_700_000_000_500
    server_ms = now_ms - 200
    old_recv = now_ms + module.RECV_WINDOW_MS
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
        if recv == old_recv:
            return {
                "status": "failure", "request_type": "query_list_trigger_orders",
                "error_code": 1000, "error": "expired",
            }
        assert recv == server_ms + MAX_FRESHNESS_MS
        return {
            "status": "success", "request_type": "query_list_trigger_orders",
            "data": {"orders": []},
        }

    assert trigger_response(old_recv) == {
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
    with pytest.raises(OperationalSafetyError, match="exact cancellation"):
        io.dispatch(_cancel_intent(), "0x" + "33" * 65)


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


def test_v2_pairs_transport_is_fixed_get_without_request_body() -> None:
    raw = b'[{"product_id":44,"ticker_id":"SKR-PERP_USDT0",' \
          b'"base":"SKR-PERP","quote":"USDT0"}]'
    connection = _Connection(_Response(raw, None))
    io = OperationalVenueIO(OWNER, SENDER)
    io._connection_factory = lambda _host: connection
    assert io._get("gateway.test.nado.xyz", "/v2/pairs")[0]["product_id"] == 44
    assert connection.request_args[:3] == ("GET", "/v2/pairs", None)


@pytest.mark.parametrize("encoding", ["compress", "gzip, br", "x-gzip"])
def test_transport_rejects_unknown_or_composed_content_encoding(encoding: str) -> None:
    connection = _Connection(_Response(b"{}", encoding))
    io = OperationalVenueIO(OWNER, SENDER)
    io._connection_factory = lambda _host: connection
    with pytest.raises(OperationalSafetyError, match="content encoding"):
        io._post("gateway.test.nado.xyz", "/v1/query", {"type": "status"})


def test_post_only_no_fill_is_exactly_cancelled_and_finishes_flat(tmp_path: Path) -> None:
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
        assert entry.kind == "ENTRY" and entry.appendix == 1537
        assert close.kind == "CLOSE" and close.appendix == 2561
        assert close.starting_position_x18 == entry.amount_x18
        assert close.amount_x18 == -entry.amount_x18
        assert close.snapshot_id and close.snapshot_observed_at_ms
    finally:
        store.close()


def test_partial_post_only_fill_is_cancelled_then_clamped_close_uses_residual(
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


def test_prewrite_blocked_runtime_row_allows_fresh_retry_without_erasing_history(
    tmp_path: Path,
) -> None:
    path = tmp_path / "nado.sqlite"
    blocked = FixtureIO("RESTING")
    blocked.observe = lambda _digests: (_ for _ in ()).throw(
        OperationalSafetyError("synthetic pre-write block")
    )
    with pytest.raises(OperationalSafetyError, match="pre-write"):
        _fixture_run(
            path=path, io=blocked, capability_loader=capability,
            owner=OWNER, sender=SENDER,
        )

    connection = sqlite3.connect(path)
    try:
        first_rows = connection.execute(
            "SELECT run_id, state FROM nado_runtime_runs ORDER BY rowid"
        ).fetchall()
        assert len(first_rows) == 1 and first_rows[0][1] == "STARTED"
        assert connection.execute("SELECT COUNT(*) FROM nado_intents").fetchone() == (0,)
    finally:
        connection.close()

    report, store = run_fixture(tmp_path, FixtureIO("RESTING"))
    store.close()
    assert report.status == COMPLETE
    connection = sqlite3.connect(path)
    try:
        rows = connection.execute(
            "SELECT run_id, state FROM nado_runtime_runs ORDER BY rowid"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0] == first_rows[0]
        assert rows[0][0] != rows[1][0]
        assert [state for _, state in rows] == ["STARTED", "STARTED"]
    finally:
        connection.close()


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
