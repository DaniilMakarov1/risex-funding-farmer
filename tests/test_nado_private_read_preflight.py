from __future__ import annotations

import asyncio
import copy
import importlib
import inspect
import json
import math
import sqlite3
import ssl
import time
from dataclasses import replace
from pathlib import Path
from typing import Callable

import pytest
from eth_account import Account
from eth_account.messages import encode_typed_data

import risex_farmer.nado_private_read_preflight as nado


FIXTURE = Path(__file__).parent / "fixtures/nado_private_read_preflight/official_contract.json"


@pytest.fixture
def contract() -> dict[str, object]:
    contract = json.loads(FIXTURE.read_text())
    for entry in list(contract["round_a"]) + list(contract["round_b"]):
        operation = str(entry["op"])
        if operation.startswith("subaccount_orders:"):
            product_id = int(operation.split(":", 1)[1])
            response = {
                "status": "success",
                "data": {
                    "sender": contract["sender"], "product_id": product_id,
                    "orders": [],
                },
            }
        else:
            response = contract["wire"][operation]
        entry["response"] = copy.deepcopy(response)
    contract["trigger"]["response"] = copy.deepcopy(contract["wire"]["trigger"])
    return contract


def _config(contract: dict[str, object], **changes: object) -> object:
    values = {
        "owner": contract["owner"], "subaccount_name": contract["subaccount_name"],
        "sender": contract["sender"], "invocation_id": "fixture-invocation-001",
        "exclusive_owner_lease": True, "direct_owner_eoa": True,
    }
    values.update(changes)
    return nado.PreflightConfig(**values)


class PublicFixture:
    def __init__(self, entries: list[dict[str, object]]) -> None:
        self.entries = copy.deepcopy(entries)
        self.calls: list[dict[str, object]] = []
        self.policies: list[object] = []
        self.fail_at: int | None = None

    def __call__(self, url: str, request: dict[str, object], policy: object) -> object:
        self.calls.append(copy.deepcopy(request))
        self.policies.append(policy)
        if self.fail_at == len(self.calls) - 1:
            raise RuntimeError("RAW_SECRET_PUBLIC")
        if not self.entries:
            raise AssertionError("unexpected public callback")
        entry = self.entries.pop(0)
        request_type = request.get("type")
        operation = (
            f"subaccount_orders:{request.get('product_id')}"
            if request_type == "subaccount_orders" else request_type
        )
        assert operation == entry["op"]
        assert url == nado.FixedPreflightIdentity.gateway_query
        return nado.ObservedResponse(
            url=url, final_url=url, http_status=200,
            observed_at_ms=entry["observed_at_ms"], payload=entry["response"],
        )


class SignedFixture:
    def __init__(self, trigger: dict[str, object]) -> None:
        self.trigger = copy.deepcopy(trigger)
        self.calls: list[dict[str, object]] = []
        self.policies: list[object] = []
        self.fail = False

    def __call__(self, url: str, request: dict[str, object], policy: object) -> object:
        self.calls.append(copy.deepcopy(request))
        self.policies.append(policy)
        if self.fail:
            raise RuntimeError("RAW_SECRET_SIGNED_TRANSPORT")
        return nado.ObservedResponse(
            url=url, final_url=url, http_status=200,
            observed_at_ms=self.trigger["observed_at_ms"],
            payload=self.trigger["response"],
        )


class TimeFixture:
    def __init__(self, contract: dict[str, object]) -> None:
        self.response = copy.deepcopy(contract["wire"]["time"])
        self.calls: list[dict[str, object]] = []
        self.fail = False

    def __call__(self, url: str, request: dict[str, object], policy: object) -> object:
        self.calls.append(copy.deepcopy(request))
        if self.fail:
            raise RuntimeError("RAW_SECRET_TIME_TRANSPORT")
        assert url == nado.FixedPreflightIdentity.gateway_edge_query
        return nado.ObservedResponse(
            url=url, final_url=url, http_status=200,
            observed_at_ms=1_700_000_000_008, payload=self.response,
        )


class Calls:
    def __init__(self, contract: dict[str, object]) -> None:
        self.contract = contract
        self.loader = 0
        self.derive = 0
        self.sign = 0
        self.recover = 0
        self.typed: list[dict[str, object]] = []
        self.fail_stage: str | None = None

    def _fail(self, stage: str) -> None:
        if self.fail_stage == stage:
            raise RuntimeError(f"RAW_SECRET_{stage.upper()}")

    def load(self) -> object:
        self.loader += 1
        self._fail("loader")
        return object()

    def derive_owner(self, credential: object) -> str:
        self.derive += 1
        self._fail("derive")
        return str(self.contract["owner"])

    def signer(self, credential: object, typed_data: dict[str, object]) -> str:
        self.sign += 1
        self.typed.append(copy.deepcopy(typed_data))
        self._fail("signer")
        return str(self.contract["signature"])

    def recover_owner(self, typed_data: dict[str, object], signature: str) -> str:
        self.recover += 1
        self._fail("recover")
        return str(self.contract["owner"])


def _public(entries: list[dict[str, object]], **policy: object) -> tuple[object, PublicFixture]:
    callback = PublicFixture(entries)
    return nado.SealedPublicTransport(callback=callback, **policy), callback


def _signed(trigger: dict[str, object], **policy: object) -> tuple[object, SignedFixture]:
    callback = SignedFixture(trigger)
    return nado.SealedSignedTransport(callback=callback, **policy), callback


def _time(contract: dict[str, object], **policy: object) -> tuple[object, TimeFixture]:
    callback = TimeFixture(contract)
    return nado.SealedTimeTransport(callback=callback, **policy), callback


def _run(
    tmp_path: Path, contract: dict[str, object], *,
    public_entries: list[dict[str, object]] | None = None,
    store: object | None = None, config: object | None = None,
    calls: Calls | None = None, public: object | None = None,
    signed: object | None = None, time_transport: object | None = None,
    clock_ms: Callable[[], int] | None = None,
) -> tuple[object, object, Calls, PublicFixture | None, SignedFixture | None]:
    public_callback = None
    signed_callback = None
    if public is None:
        public, public_callback = _public(
            list(contract["round_a"]) + list(contract["round_b"])
            if public_entries is None else public_entries
        )
    if signed is None:
        signed, signed_callback = _signed(dict(contract["trigger"]))
    if time_transport is None:
        time_transport, _ = _time(contract)
    store = store or nado.OneShotStore(tmp_path / "intent.sqlite3")
    calls = calls or Calls(contract)
    result = nado.run_fixture_preflight(
        config=config or _config(contract), public_transport=public,
        time_transport=time_transport,
        credential_loader=calls.load, derive_owner=calls.derive_owner,
        signer=calls.signer, recover_owner=calls.recover_owner,
        signed_transport=signed,
        clock_ms=clock_ms or (lambda: 1_700_000_000_025), store=store,
    )
    return result, store, calls, public_callback, signed_callback


def _assert_sanitized(error: BaseException, sentinel: str = "RAW_SECRET") -> None:
    assert sentinel not in str(error)
    assert sentinel not in repr(error)
    assert error.__cause__ is None
    assert error.__context__ is None


def test_exact_official_wire_success_and_one_shot_request(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    result, store, calls, public, signed = _run(tmp_path, contract)
    assert result.status == nado.FINALIZED
    assert result.zero_regular_orders and result.exact_flat and result.zero_trigger_history
    assert (calls.loader, calls.derive, calls.sign, calls.recover) == (1, 1, 1, 1)
    assert signed is not None and len(signed.calls) == 1
    expected = {
        "type": "list_trigger_orders",
        "tx": {"sender": contract["sender"], "recvTime": "1700000030009"},
        "signature": contract["signature"], "limit": 1,
    }
    assert signed.calls == [expected]
    assert calls.typed == [{
        "types": {
            "ListTriggerOrders": [
                {"name": "sender", "type": "bytes32"},
                {"name": "recvTime", "type": "uint64"},
            ],
        },
        "primaryType": "ListTriggerOrders",
        "domain": {
            "name": "Nado",
            "version": "0.0.1",
            "chainId": 763373,
            "verifyingContract": "0x698D87105274292B5673367DEC81874Ce3633Ac2",
        },
        "message": {"sender": contract["sender"], "recvTime": "1700000030009"},
    }]
    assert store.state("fixture-invocation-001") == nado.FINALIZED
    assert public is not None and len(public.calls) == 19
    expected_policy = nado.TransportPolicy(True, False, False, 5_000, 65_536)
    assert set(public.policies) == {expected_policy}
    assert signed.policies == [expected_policy]
    assert public.calls[:8] == [
        {"type": "contracts"}, {"type": "status"}, {"type": "all_products"},
        {"type": "linked_signer", "subaccount": contract["sender"]},
        {"type": "subaccount_info", "subaccount": contract["sender"]},
        {"type": "subaccount_orders", "sender": contract["sender"], "product_id": 0},
        {"type": "subaccount_orders", "sender": contract["sender"], "product_id": 1},
        {"type": "isolated_positions", "subaccount": contract["sender"]},
    ]


@pytest.mark.parametrize(
    "recv_time",
    [1_700_000_030_010, True, "", "01", "+1", "-1", "1 ", str(2**64)],
)
def test_trigger_typed_data_rejects_noncanonical_recv_time(
    contract: dict[str, object], recv_time: object,
) -> None:
    with pytest.raises(nado.NadoPreflightError, match="trigger receive time"):
        nado.list_trigger_orders_typed_data(str(contract["sender"]), recv_time)


def test_trigger_typed_data_exact_pinned_shape(contract: dict[str, object]) -> None:
    typed = nado.list_trigger_orders_typed_data(
        str(contract["sender"]), "1700000030010"
    )
    assert set(typed["types"]) == {"ListTriggerOrders"}
    assert typed["message"] == {
        "sender": contract["sender"], "recvTime": "1700000030010",
    }


def test_pins_identity_and_official_fixture_shapes(contract: dict[str, object]) -> None:
    assert nado.SOURCE_PINS == contract["sources"]
    assert nado.FixedPreflightIdentity.as_dict() == contract["environment"]
    contracts = contract["round_a"][0]["response"]
    products = contract["round_a"][2]["response"]
    account = contract["round_a"][4]["response"]
    isolated = contract["round_a"][7]["response"]
    assert set(contracts) == set(products) == set(account) == set(isolated) == {"status", "data"}
    assert set(contracts["data"]) == {"chain_id", "endpoint_addr"}
    assert contract["round_a"][1]["response"]["data"] == "active"
    assert set(products["data"]) == {"spot_products", "perp_products"}
    assert "symbol" not in products["data"]["spot_products"][0]
    assert set(products["data"]["spot_products"][0]) == {
        "product_id", "oracle_price_x18", "risk", "config", "state", "book_info"
    }
    assert set(products["data"]["perp_products"][0]) == {
        "product_id", "oracle_price_x18", "index_price_x18", "risk", "state", "book_info"
    }
    assert isinstance(account["data"]["spot_balances"], list)
    assert isinstance(account["data"]["perp_balances"], list)
    assert len(account["data"]["healths"]) == 3
    assert "pre_state" not in account["data"]
    assert type(account["data"]["spot_count"]) is int
    assert type(account["data"]["perp_count"]) is int
    assert set(isolated["data"]) == {"isolated_positions"}
    assert set(contract["trigger"]["response"]["data"]) == {"orders"}


@pytest.mark.parametrize("bad", [1, 0, "true", None])
@pytest.mark.parametrize("field", ["exclusive_owner_lease", "direct_owner_eoa"])
def test_config_boolean_aliases_are_rejected(
    contract: dict[str, object], field: str, bad: object
) -> None:
    with pytest.raises(nado.NadoPreflightError):
        _config(contract, **{field: bad})


@pytest.mark.parametrize("bad", [None, 1, True, b"sender"])
def test_config_sender_non_string_is_sanitized_contract_error(
    contract: dict[str, object], bad: object
) -> None:
    with pytest.raises(nado.NadoPreflightError) as captured:
        _config(contract, sender=bad)
    assert captured.value.__cause__ is None


def test_observation_urls_require_exact_string_type(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    class EqualityAlias:
        def __eq__(self, other: object) -> bool:
            return True
        def __ne__(self, other: object) -> bool:
            return False

    class AliasPublic(PublicFixture):
        def __call__(self, url: str, request: dict[str, object], policy: object) -> object:
            observed = super().__call__(url, request, policy)
            return replace(observed, url=EqualityAlias(), final_url=EqualityAlias())

    store = nado.OneShotStore(tmp_path / "url-alias.sqlite3")
    with pytest.raises(nado.NadoPreflightError):
        _run(
            tmp_path, contract,
            public=nado.SealedPublicTransport(
                callback=AliasPublic(list(contract["round_a"]) + list(contract["round_b"]))
            ),
            store=store,
        )
    assert store.state("fixture-invocation-001") == nado.NEW


@pytest.mark.parametrize(
    ("field", "bad"),
    [("trust_env", 0), ("allow_redirects", 0), ("tls_verified", 1),
     ("timeout_ms", True), ("timeout_ms", 0), ("timeout_ms", 30_001),
     ("max_response_bytes", True), ("max_response_bytes", 0)],
)
@pytest.mark.parametrize("kind", ["public", "signed"])
def test_module_owned_transport_policy_rejects_scalar_aliases_before_callbacks(
    contract: dict[str, object], kind: str, field: str, bad: object
) -> None:
    callback: Callable[..., object] = PublicFixture([]) if kind == "public" else SignedFixture(dict(contract["trigger"]))
    cls = nado.SealedPublicTransport if kind == "public" else nado.SealedSignedTransport
    with pytest.raises(nado.NadoPreflightError, match="transport policy"):
        cls(callback=callback, **{field: bad})
    assert callback.calls == []


@pytest.mark.parametrize("bad", [200.0, True, "200", 403])
def test_http_status_is_exact_integer_200_before_claim(
    tmp_path: Path, contract: dict[str, object], bad: object
) -> None:
    class BadPublic(PublicFixture):
        def __call__(self, url: str, request: dict[str, object], policy: object) -> object:
            observed = super().__call__(url, request, policy)
            return replace(observed, http_status=bad)
    callback = BadPublic(list(contract["round_a"]) + list(contract["round_b"]))
    public = nado.SealedPublicTransport(callback=callback)
    store = nado.OneShotStore(tmp_path / "http.sqlite3")
    with pytest.raises(nado.NadoPreflightError):
        _run(tmp_path, contract, public=public, store=store)
    assert store.state("fixture-invocation-001") == nado.NEW


@pytest.mark.parametrize(
    "mutation",
    [lambda r: r.update(status=200), lambda r: r.update(status="Success"),
     lambda r: r.update(extra=True), lambda r: r.pop("data")],
)
def test_wire_envelope_is_exact_status_success_data(
    tmp_path: Path, contract: dict[str, object], mutation: Callable[[dict[str, object]], object]
) -> None:
    entries = copy.deepcopy(list(contract["round_a"]) + list(contract["round_b"]))
    mutation(entries[0]["response"])
    store = nado.OneShotStore(tmp_path / "envelope.sqlite3")
    with pytest.raises(nado.NadoPreflightError):
        _run(tmp_path, contract, public_entries=entries, store=store)
    assert store.state("fixture-invocation-001") == nado.NEW


@pytest.mark.parametrize("bad", [763373, 763373.0, True, "0763373", "+763373", "763373 "])
def test_contract_chain_is_canonical_official_string(
    tmp_path: Path, contract: dict[str, object], bad: object
) -> None:
    entries = copy.deepcopy(list(contract["round_a"]) + list(contract["round_b"]))
    entries[0]["response"]["data"]["chain_id"] = bad
    store = nado.OneShotStore(tmp_path / f"chain-{bad}.sqlite3")
    with pytest.raises(nado.NadoPreflightError):
        _run(tmp_path, contract, public_entries=entries, store=store)
    assert store.state("fixture-invocation-001") == nado.NEW


@pytest.mark.parametrize("bad", [0.0, False, "0", None, -1, 2**32])
def test_product_id_rejects_float_bool_and_string_aliases(
    tmp_path: Path, contract: dict[str, object], bad: object
) -> None:
    entries = copy.deepcopy(list(contract["round_a"]) + list(contract["round_b"]))
    entries[2]["response"]["data"]["spot_products"][0]["product_id"] = bad
    store = nado.OneShotStore(tmp_path / f"product-{bad}.sqlite3")
    with pytest.raises(nado.NadoPreflightError):
        _run(tmp_path, contract, public_entries=entries, store=store)
    assert store.state("fixture-invocation-001") == nado.NEW


@pytest.mark.parametrize(
    ("kind", "section", "field", "bad"),
    [
        ("spot_products", "product", "oracle_price_x18", 1),
        ("spot_products", "risk", "long_weight_initial_x18", True),
        ("spot_products", "config", "interest_floor_x18", "00"),
        ("spot_products", "state", "total_borrows_normalized", "Infinity"),
        ("spot_products", "book_info", "min_size", 1.0),
        ("perp_products", "product", "index_price_x18", False),
        ("perp_products", "risk", "large_position_penalty_x18", "NaN"),
        ("perp_products", "state", "open_interest", 0),
        ("perp_products", "book_info", "collected_fees", "-0"),
    ],
)
def test_full_product_nested_scalars_are_strict_pinned_schema(
    tmp_path: Path, contract: dict[str, object], kind: str,
    section: str, field: str, bad: object,
) -> None:
    entries = copy.deepcopy(list(contract["round_a"]) + list(contract["round_b"]))
    product = entries[2]["response"]["data"][kind][0]
    target = product if section == "product" else product[section]
    target[field] = bad
    store = nado.OneShotStore(tmp_path / f"nested-{kind}-{section}-{field}.sqlite3")
    with pytest.raises(nado.NadoPreflightError):
        _run(tmp_path, contract, public_entries=entries, store=store)
    assert store.state("fixture-invocation-001") == nado.NEW


@pytest.mark.parametrize("defect", ["missing", "extra", "token", "health-width", "contribution", "pre-state", "embedded-product"])
def test_full_pinned_account_schema_and_embedded_catalog_are_required(
    tmp_path: Path, contract: dict[str, object], defect: str
) -> None:
    entries = copy.deepcopy(list(contract["round_a"]) + list(contract["round_b"]))
    data = entries[4]["response"]["data"]
    if defect == "missing":
        data.pop("health_contributions")
    elif defect == "extra":
        data["health"] = "1"
    elif defect == "token":
        data["spot_products"][0]["config"]["token"] = "0x01"
    elif defect == "health-width":
        data["healths"].pop()
    elif defect == "contribution":
        data["health_contributions"][0][1] = 0
    elif defect == "pre-state":
        data["pre_state"] = None
    else:
        data["perp_products"][0]["index_price_x18"] = "1"
    store = nado.OneShotStore(tmp_path / f"account-schema-{defect}.sqlite3")
    with pytest.raises(nado.NadoPreflightError):
        _run(tmp_path, contract, public_entries=entries, store=store)
    assert store.state("fixture-invocation-001") == nado.NEW


@pytest.mark.parametrize("field,bad", [("sender", "0x" + "00" * 32), ("product_id", 2), ("product_id", 0.0)])
def test_subaccount_orders_exact_echo_and_scalar_type_are_required(
    tmp_path: Path, contract: dict[str, object], field: str, bad: object
) -> None:
    entries = copy.deepcopy(list(contract["round_a"]) + list(contract["round_b"]))
    entries[5]["response"]["data"][field] = bad
    store = nado.OneShotStore(tmp_path / f"orders-echo-{field}-{bad}.sqlite3")
    with pytest.raises(nado.NadoPreflightError):
        _run(tmp_path, contract, public_entries=entries, store=store)
    assert store.state("fixture-invocation-001") == nado.NEW


@pytest.mark.parametrize(
    ("path", "bad"),
    [("health", 1), ("health", "01"), ("health", "NaN"),
     ("amount", 0), ("amount", "00"), ("amount", "Infinity"),
     ("v_quote_balance", 0), ("v_quote_balance", "-0"),
     ("last_cumulative_funding_x18", 0),
     ("spot_count", "1"), ("spot_count", False), ("spot_count", 1.0)],
)
def test_account_decimal_scalars_are_exact_official_strings(
    tmp_path: Path, contract: dict[str, object], path: str, bad: object
) -> None:
    entries = copy.deepcopy(list(contract["round_a"]) + list(contract["round_b"]))
    data = entries[4]["response"]["data"]
    if path == "health":
        data["healths"][0]["health"] = bad
    elif path in {"amount", "v_quote_balance", "last_cumulative_funding_x18"}:
        data["perp_balances"][0]["balance"][path] = bad
    else:
        data[path] = bad
    store = nado.OneShotStore(tmp_path / f"scalar-{path}-{bad}.sqlite3")
    with pytest.raises(nado.NadoPreflightError):
        _run(tmp_path, contract, public_entries=entries, store=store)
    assert store.state("fixture-invocation-001") == nado.NEW


def test_complete_catalog_account_vector_must_be_exact_flat(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    for label, mutate in (
        ("missing-perp", lambda d: d["perp_balances"].pop()),
        ("duplicate", lambda d: d["perp_balances"].append(copy.deepcopy(d["perp_balances"][0]))),
        ("nonflat", lambda d: d["perp_balances"][0]["balance"].update(amount="1")),
        ("vquote", lambda d: d["perp_balances"][0]["balance"].update(v_quote_balance="1")),
    ):
        entries = copy.deepcopy(list(contract["round_a"]) + list(contract["round_b"]))
        mutate(entries[4]["response"]["data"])
        store = nado.OneShotStore(tmp_path / f"vector-{label}.sqlite3")
        with pytest.raises(nado.NadoPreflightError):
            _run(tmp_path, contract, public_entries=entries, store=store)
        assert store.state("fixture-invocation-001") == nado.NEW


def test_public_contract_failure_never_reaches_sensitive_callbacks(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    entries = copy.deepcopy(list(contract["round_a"]) + list(contract["round_b"]))
    entries[2]["response"]["data"]["spot_products"][0]["product_id"] = False
    public, _ = _public(entries)
    signed, signed_callback = _signed(dict(contract["trigger"]))
    calls = Calls(contract)
    store = nado.OneShotStore(tmp_path / "pre-sensitive.sqlite3")
    with pytest.raises(nado.NadoPreflightError):
        _run(
            tmp_path, contract, public=public, signed=signed, calls=calls, store=store
        )
    assert (calls.loader, calls.derive, calls.sign, calls.recover) == (0, 0, 0, 0)
    assert signed_callback.calls == []
    assert store.state("fixture-invocation-001") == nado.NEW


def test_no_invented_normalized_containers_are_accepted(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    for index, invented in (
        (5, {"orders": []}),
        (7, {"sender": contract["sender"], "positions": []}),
    ):
        entries = copy.deepcopy(list(contract["round_a"]) + list(contract["round_b"]))
        entries[index]["response"]["data"] = invented
        with pytest.raises(nado.NadoPreflightError):
            _run(tmp_path, contract, public_entries=entries,
                 store=nado.OneShotStore(tmp_path / f"invented-{index}.sqlite3"))


@pytest.mark.parametrize(
    "stage", ["public_a", "loader", "derive", "time", "signer", "recover", "signed", "public_b"]
)
def test_every_external_callback_exception_is_fully_sanitized_and_state_bounded(
    tmp_path: Path, contract: dict[str, object], stage: str
) -> None:
    public_callback = PublicFixture(list(contract["round_a"]) + list(contract["round_b"]))
    public_callback.fail_at = 0 if stage == "public_a" else (8 if stage == "public_b" else None)
    public = nado.SealedPublicTransport(callback=public_callback)
    signed_callback = SignedFixture(dict(contract["trigger"]))
    signed_callback.fail = stage == "signed"
    signed = nado.SealedSignedTransport(callback=signed_callback)
    time_transport, time_callback = _time(contract)
    time_callback.fail = stage == "time"
    calls = Calls(contract)
    if stage in {"loader", "derive", "signer", "recover"}:
        calls.fail_stage = stage
    store = nado.OneShotStore(tmp_path / f"sanitize-{stage}.sqlite3")
    with pytest.raises(nado.NadoPreflightError) as captured:
        _run(tmp_path, contract, public=public, time_transport=time_transport,
             signed=signed, calls=calls, store=store)
    _assert_sanitized(captured.value)
    expected = nado.NEW if stage == "public_a" else (nado.OBSERVED if stage == "public_b" else nado.CLAIMED)
    assert store.state("fixture-invocation-001") == expected
    assert len(signed_callback.calls) <= 1 and calls.sign <= 1
    if stage in {"public_a", "public_b"}:
        assert _run(
            tmp_path, contract, public=public, time_transport=time_transport,
            signed=signed, calls=calls, store=store
        )[0].status == nado.FINALIZED
    else:
        with pytest.raises(nado.NadoPreflightError):
            _run(tmp_path, contract, public=public, time_transport=time_transport,
                 signed=signed, calls=calls, store=store)
    assert len(signed_callback.calls) <= 1 and calls.sign <= 1


def test_round_b_resume_uses_observed_evidence_without_second_sensitive_callback(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    store = nado.OneShotStore(tmp_path / "resume.sqlite3")
    first_public, first_callback = _public(list(contract["round_a"]) + list(contract["round_b"]))
    first_callback.fail_at = 9
    signed, signed_callback = _signed(dict(contract["trigger"]))
    calls = Calls(contract)
    with pytest.raises(nado.NadoPreflightError):
        _run(tmp_path, contract, public=first_public, signed=signed, calls=calls, store=store)
    assert store.state("fixture-invocation-001") == nado.OBSERVED
    resume_public, _ = _public(list(contract["round_b"]))
    result, *_ = _run(
        tmp_path, contract, public=resume_public, signed=signed, calls=calls, store=store
    )
    assert result.status == nado.FINALIZED
    assert (calls.loader, calls.derive, calls.sign, calls.recover) == (1, 1, 1, 1)
    assert len(signed_callback.calls) == 1


@pytest.mark.parametrize("defect", ["http", "wire", "extra", "nonzero", "redirect"])
def test_signed_response_defects_consume_claim_and_never_replay(
    tmp_path: Path, contract: dict[str, object], defect: str
) -> None:
    class BadSigned(SignedFixture):
        def __call__(self, url: str, request: dict[str, object], policy: object) -> object:
            observed = super().__call__(url, request, policy)
            if defect == "http":
                return replace(observed, http_status=403)
            if defect == "redirect":
                return replace(observed, final_url="https://wrong.test/query")
            payload = copy.deepcopy(observed.payload)
            if defect == "wire":
                payload["status"] = "failure"
            elif defect == "extra":
                payload["data"]["sender"] = contract["sender"]
            else:
                payload["data"]["orders"] = [{"digest": "0x01"}]
            return replace(observed, payload=payload)

    callback = BadSigned(dict(contract["trigger"]))
    signed = nado.SealedSignedTransport(callback=callback)
    calls = Calls(contract)
    store = nado.OneShotStore(tmp_path / f"signed-{defect}.sqlite3")
    with pytest.raises(nado.NadoPreflightError):
        _run(tmp_path, contract, signed=signed, calls=calls, store=store)
    assert store.state("fixture-invocation-001") == nado.CLAIMED
    assert len(callback.calls) == calls.sign == 1
    with pytest.raises(nado.NadoPreflightError, match="cannot be retried"):
        _run(tmp_path, contract, signed=signed, calls=calls, store=store)
    assert len(callback.calls) == calls.sign == 1


@pytest.mark.parametrize("defect", ["fingerprint", "order", "catalog", "linked"])
def test_round_b_contradiction_remains_observed_and_never_reobserves(
    tmp_path: Path, contract: dict[str, object], defect: str
) -> None:
    entries = copy.deepcopy(list(contract["round_a"]) + list(contract["round_b"]))
    if defect == "fingerprint":
        entries[11]["response"]["data"]["healths"][0]["health"] = "2"
    elif defect == "order":
        entries[9]["response"]["data"]["orders"] = [{"digest": "0x01"}]
    elif defect == "catalog":
        entries[17]["response"]["data"]["perp_products"][0]["oracle_price_x18"] = "1"
    else:
        entries[18]["response"]["data"]["linked_signer"] = "0x" + "01" * 20
    signed, callback = _signed(dict(contract["trigger"]))
    calls = Calls(contract)
    store = nado.OneShotStore(tmp_path / f"round-b-{defect}.sqlite3")
    with pytest.raises(nado.NadoPreflightError):
        _run(tmp_path, contract, public_entries=entries, signed=signed,
             calls=calls, store=store)
    assert store.state("fixture-invocation-001") == nado.OBSERVED
    assert len(callback.calls) == calls.sign == 1


def test_corrupt_durable_digest_or_identity_halts_before_round_b(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    for column in ("trigger_hash", "identity_tag"):
        path = tmp_path / f"corrupt-{column}.sqlite3"
        store = nado.OneShotStore(path)
        first, _ = _public(list(contract["round_a"]))
        signed, callback = _signed(dict(contract["trigger"]))
        calls = Calls(contract)
        with pytest.raises(nado.NadoPreflightError):
            _run(tmp_path, contract, public=first, signed=signed, calls=calls, store=store)
        with sqlite3.connect(path) as connection:
            connection.execute(
                f"UPDATE nado_preflight_one_shot SET {column} = ?", ("0" * 64,)
            )
        resume, public_callback = _public(list(contract["round_b"]))
        with pytest.raises(nado.NadoPreflightError):
            _run(tmp_path, contract, public=resume, signed=signed,
                 calls=calls, store=store)
        assert public_callback.calls == []
        assert len(callback.calls) == calls.sign == 1


def _round_b_ending_at(contract: dict[str, object], final_ms: int) -> list[dict[str, object]]:
    entries = copy.deepcopy(list(contract["round_b"]))
    shift = final_ms - int(entries[-1]["observed_at_ms"])
    for entry in entries:
        entry["observed_at_ms"] = int(entry["observed_at_ms"]) + shift
    return entries


@pytest.mark.parametrize("age,accepted", [(30_000, True), (30_001, False), (86_400_000, False)])
def test_observed_resume_freshness_fence_is_inclusive_and_precedes_round_b(
    tmp_path: Path, contract: dict[str, object], age: int, accepted: bool
) -> None:
    store = nado.OneShotStore(tmp_path / f"fresh-{age}.sqlite3")
    public, _ = _public(list(contract["round_a"]))
    signed, signed_callback = _signed(dict(contract["trigger"]))
    calls = Calls(contract)
    with pytest.raises(nado.NadoPreflightError):
        _run(tmp_path, contract, public=public, signed=signed, calls=calls, store=store)
    round_a_last = int(contract["round_a"][-1]["observed_at_ms"])
    now = round_a_last + age
    resume, resume_callback = _public(_round_b_ending_at(contract, now))
    if accepted:
        assert _run(tmp_path, contract, public=resume, signed=signed, calls=calls,
                    store=store, config=_config(contract),
                    clock_ms=lambda: now)[0].status == nado.FINALIZED
    else:
        with pytest.raises(nado.NadoPreflightError):
            _run(tmp_path, contract, public=resume, signed=signed, calls=calls,
                 store=store, config=_config(contract),
                 clock_ms=lambda: now)
        assert resume_callback.calls == []
        assert store.state("fixture-invocation-001") == nado.OBSERVED
    assert len(signed_callback.calls) == 1 and calls.sign == 1


def test_clock_rollback_and_server_before_round_a_fail_without_second_attempt(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    calls = Calls(contract)
    time_transport, time_callback = _time(contract)
    time_callback.response["server_time"] = str(contract["round_a"][-1]["observed_at_ms"])
    signed, signed_callback = _signed(dict(contract["trigger"]))
    store = nado.OneShotStore(tmp_path / "server-order.sqlite3")
    with pytest.raises(nado.NadoPreflightError):
        _run(tmp_path, contract, calls=calls, time_transport=time_transport,
             signed=signed, store=store)
    assert store.state("fixture-invocation-001") == nado.CLAIMED
    assert signed_callback.calls == [] and calls.sign == 0

    clean_calls = Calls(contract)
    clean_store = nado.OneShotStore(tmp_path / "rollback.sqlite3")
    first, _ = _public(list(contract["round_a"]))
    clean_signed, clean_signed_callback = _signed(dict(contract["trigger"]))
    with pytest.raises(nado.NadoPreflightError):
        _run(tmp_path, contract, public=first, signed=clean_signed,
             calls=clean_calls, store=clean_store)
    resume, callback = _public(list(contract["round_b"]))
    with pytest.raises(nado.NadoPreflightError):
        _run(tmp_path, contract, public=resume, signed=clean_signed,
             calls=clean_calls, store=clean_store,
             config=_config(contract),
             clock_ms=lambda: int(contract["trigger"]["observed_at_ms"])-1)
    assert callback.calls == [] and len(clean_signed_callback.calls) == 1


def test_signature_and_identity_are_strict_and_durable_identity_is_redacted(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    for bad in (True, "0x01", "11" * 65, "0x" + "gg" * 65):
        calls = Calls(contract)
        calls.signer = lambda credential, typed, value=bad: value
        store = nado.OneShotStore(tmp_path / f"signature-{str(bad)[:8]}.sqlite3")
        with pytest.raises(nado.NadoPreflightError):
            _run(tmp_path, contract, calls=calls, store=store)
        assert store.state("fixture-invocation-001") == nado.CLAIMED
    path = tmp_path / "redacted.sqlite3"
    _run(tmp_path, contract, store=nado.OneShotStore(path))
    persisted = path.read_bytes()
    assert str(contract["owner"]).encode() not in persisted
    assert str(contract["sender"]).encode() not in persisted
    assert str(contract["signature"]).encode() not in persisted


def test_module_is_disarmed_and_has_no_ambient_network_or_secret_surface() -> None:
    package = importlib.import_module("risex_farmer")
    module = importlib.import_module("risex_farmer.nado_private_read_preflight")
    assert "nado_private_read_preflight" not in Path(package.__file__).read_text()
    source = Path(module.__file__).read_text()
    for forbidden in (
        "requests", "urllib", "os.environ", "getenv(", "Path.home",
        "XLSX", "seed phrase", "socket", "http.client", "subprocess", "urlopen",
    ):
        assert forbidden not in source
    assert "gateway.test.nado.xyz" in source and "trigger.test.nado.xyz" in source
    assert "run_operational_private_read_preflight" in source
    assert "run_operational_private_read_preflight" not in Path(package.__file__).read_text()


def test_exact_edge_time_wire_uses_explicit_observation_clock(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    time_transport, time_callback = _time(contract)
    result, *_ = _run(
        tmp_path, contract, time_transport=time_transport,
        config=_config(contract), clock_ms=lambda: 1_700_000_000_025,
    )
    assert result.status == nado.FINALIZED
    assert time_callback.calls == [{"type": "time"}]


@pytest.mark.parametrize(
    "mutation",
    [lambda p: p.update(data="1700000000009"),
     lambda p: p.update(id=1), lambda p: p.update(method="Time"),
     lambda p: p.update(server_time=1_700_000_000_009),
     lambda p: p.update(server_time="01700000000009")],
)
def test_edge_time_envelope_is_exact_and_fails_before_signing(
    tmp_path: Path, contract: dict[str, object],
    mutation: Callable[[dict[str, object]], object],
) -> None:
    time_transport, callback = _time(contract)
    mutation(callback.response)
    calls = Calls(contract)
    signed, signed_callback = _signed(dict(contract["trigger"]))
    store = nado.OneShotStore(tmp_path / "bad-edge-time.sqlite3")
    with pytest.raises(nado.NadoPreflightError):
        _run(tmp_path, contract, time_transport=time_transport, calls=calls,
             signed=signed, store=store)
    assert store.state("fixture-invocation-001") == nado.CLAIMED
    assert calls.sign == calls.recover == 0 and signed_callback.calls == []


def test_signature_recovery_mismatch_halts_before_signed_post(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    calls = Calls(contract)
    calls.recover_owner = lambda typed, signature: "0x" + "02" * 20
    signed, signed_callback = _signed(dict(contract["trigger"]))
    store = nado.OneShotStore(tmp_path / "signature-mismatch.sqlite3")
    with pytest.raises(nado.NadoPreflightError, match="signature owner"):
        _run(tmp_path, contract, calls=calls, signed=signed, store=store)
    assert store.state("fixture-invocation-001") == nado.CLAIMED
    assert signed_callback.calls == [] and calls.sign == 1


def test_perp_count_is_non_authoritative_when_full_perp_vector_is_exact_flat(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    for value in (0, 999, -1, True, 1.0, "garbage", None, {}, []):
        entries = copy.deepcopy(list(contract["round_a"]) + list(contract["round_b"]))
        entries[4]["response"]["data"]["perp_count"] = value
        entries[11]["response"]["data"]["perp_count"] = value
        result, *_ = _run(
            tmp_path, contract, public_entries=entries,
            store=nado.OneShotStore(tmp_path / f"ignored-perp-count-{value}.sqlite3"),
        )
        assert result.status == nado.FINALIZED


def test_pinned_synthetic_list_trigger_orders_signature_vector(
    contract: dict[str, object]
) -> None:
    vector = contract["signature_vector"]
    typed = nado.list_trigger_orders_typed_data(vector["sender"], vector["recv_time"])
    signable = encode_typed_data(full_message=typed)
    signed = Account.sign_message(signable, vector["synthetic_private_key"])
    assert "0x" + signed.message_hash.hex() == vector["digest"]
    assert "0x" + signed.signature.hex() == vector["signature"]
    assert Account.recover_message(signable, signature=vector["signature"]) == vector["owner"]
    assert vector["request"] == {
        "type": "list_trigger_orders",
        "tx": {"sender": vector["sender"], "recvTime": vector["recv_time"]},
        "signature": vector["signature"], "limit": 1,
    }


class _RawContent:
    def __init__(self, raw: bytes, *, never: bool = False, events: list[str] | None = None):
        self.raw = raw
        self.never = never
        self.events = events if events is not None else []
        self.offset = 0
        self.cancelled = False

    async def read(self, size: int) -> bytes:
        self.events.append("read")
        if self.never:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise
        if self.offset >= len(self.raw):
            return b""
        chunk = self.raw[self.offset:self.offset + size]
        self.offset += len(chunk)
        return chunk


class _HttpResponse:
    def __init__(self, raw: bytes, *, url: str, status: object = 200,
                 never: bool = False, events: list[str] | None = None):
        self.url = url
        self.status = status
        self.content = _RawContent(raw, never=never, events=events)

    async def __aenter__(self) -> object:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None


class _HttpSession:
    def __init__(self, response: _HttpResponse):
        self.response = response
        self.calls: list[tuple[str, dict[str, object]]] = []

    async def __aenter__(self) -> object:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    def post(self, url: str, **kwargs: object) -> _HttpResponse:
        self.calls.append((url, dict(kwargs)))
        return self.response


@pytest.mark.asyncio
async def test_owned_http_transport_seals_post_tls_session_and_observation_clock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    raw = b'{"status":"success","data":"active"}'
    response = _HttpResponse(
        raw, url=nado.FixedPreflightIdentity.gateway_query, events=events,
    )
    session = _HttpSession(response)
    session_kwargs: list[dict[str, object]] = []

    def session_factory(**kwargs: object) -> object:
        session_kwargs.append(kwargs)
        return session

    def clock() -> int:
        events.append("clock")
        return 1_700_000_000_100

    monkeypatch.setattr(nado.aiohttp, "ClientSession", session_factory)
    monkeypatch.setattr(nado, "_system_clock_ms", clock)
    observed = await nado._OperationalGatewayTransport().send_async({"type": "status"})
    assert observed.payload == {"status": "success", "data": "active"}
    assert observed.observed_at_ms == 1_700_000_000_100
    assert events[-1] == "clock" and events.index("clock") > events.index("read")
    assert session_kwargs[0]["trust_env"] is False
    assert session_kwargs[0]["timeout"].total == 5.0
    assert session.calls[0][0] == nado.FixedPreflightIdentity.gateway_query
    kwargs = session.calls[0][1]
    assert kwargs["data"] == b'{"type":"status"}'
    assert kwargs["headers"] == {"Content-Type": "application/json"}
    assert kwargs["allow_redirects"] is False and kwargs["proxy"] is None
    assert isinstance(kwargs["ssl"], ssl.SSLContext)
    assert kwargs["ssl"].check_hostname and kwargs["ssl"].verify_mode == ssl.CERT_REQUIRED


@pytest.mark.parametrize(
    "raw",
    [b"\xff", b"{", b'{"status":NaN}', b'{"status":"success","status":"success"}'],
)
@pytest.mark.asyncio
async def test_owned_http_transport_rejects_non_strict_utf8_or_json_once(
    monkeypatch: pytest.MonkeyPatch, raw: bytes,
) -> None:
    session = _HttpSession(_HttpResponse(raw, url=nado.FixedPreflightIdentity.gateway_query))
    monkeypatch.setattr(nado.aiohttp, "ClientSession", lambda **kwargs: session)
    with pytest.raises(nado.NadoPreflightError) as captured:
        await nado._OperationalGatewayTransport().send_async({"type": "status"})
    _assert_sanitized(captured.value)
    assert len(session.calls) == 1


@pytest.mark.asyncio
async def test_owned_http_transport_rejects_oversize_redirect_and_http_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for response in (
        _HttpResponse(b"x" * 65_537, url=nado.FixedPreflightIdentity.gateway_query),
        _HttpResponse(b"{}", url=nado.FixedPreflightIdentity.trigger_query),
        _HttpResponse(b"{}", url=nado.FixedPreflightIdentity.gateway_query, status=200.0),
    ):
        session = _HttpSession(response)
        monkeypatch.setattr(nado.aiohttp, "ClientSession", lambda **kwargs: session)
        with pytest.raises(nado.NadoPreflightError):
            await nado._OperationalGatewayTransport().send_async({"type": "status"})
        assert len(session.calls) == 1


@pytest.mark.asyncio
async def test_owned_http_transport_rejects_noncanonical_request_before_post(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _HttpSession(_HttpResponse(b"{}", url=nado.FixedPreflightIdentity.gateway_query))
    monkeypatch.setattr(nado.aiohttp, "ClientSession", lambda **kwargs: session)
    with pytest.raises(nado.NadoPreflightError) as captured:
        await nado._OperationalGatewayTransport().send_async({"value": math.nan})
    _assert_sanitized(captured.value)
    assert session.calls == []


def test_operational_runner_and_transport_constructors_have_no_policy_bypass() -> None:
    assert "now_ms" not in inspect.signature(nado.PreflightConfig).parameters
    assert tuple(inspect.signature(nado._OperationalGatewayTransport).parameters) == ()
    assert tuple(inspect.signature(nado._OperationalTimeTransport).parameters) == ()
    assert tuple(inspect.signature(nado._OperationalTriggerTransport).parameters) == ()
    parameters = inspect.signature(nado.run_operational_private_read_preflight).parameters
    assert not ({"transport", "url", "policy", "clock", "callback"} & set(parameters))


@pytest.mark.asyncio
async def test_operational_runner_exact_owned_sequence_with_synthetic_boundaries(
    tmp_path: Path, contract: dict[str, object], monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan: list[tuple[str, object]] = []
    for entry in contract["round_a"]:
        plan.append((nado.FixedPreflightIdentity.gateway_query, entry["response"]))
    plan.append((nado.FixedPreflightIdentity.gateway_edge_query, contract["wire"]["time"]))
    plan.append((nado.FixedPreflightIdentity.trigger_query, contract["trigger"]["response"]))
    for entry in contract["round_b"]:
        plan.append((nado.FixedPreflightIdentity.gateway_query, entry["response"]))
    sessions: list[_HttpSession] = []

    def session_factory(**kwargs: object) -> object:
        url, payload = plan[len(sessions)]
        raw = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), allow_nan=False,
        ).encode()
        session = _HttpSession(_HttpResponse(raw, url=url))
        sessions.append(session)
        return session

    clock_values: list[int] = []
    now = 1_700_000_000_025
    for entry in contract["round_a"]:
        clock_values.extend([int(entry["observed_at_ms"]), now])
    clock_values.extend([1_700_000_000_008, now, now])
    clock_values.extend([int(contract["trigger"]["observed_at_ms"]), now, now])
    for entry in contract["round_b"]:
        clock_values.extend([int(entry["observed_at_ms"]), now])

    def clock() -> int:
        assert clock_values
        return clock_values.pop(0)

    monkeypatch.setattr(nado.aiohttp, "ClientSession", session_factory)
    monkeypatch.setattr(nado, "_system_clock_ms", clock)
    calls = Calls(contract)
    store = nado.OneShotStore(tmp_path / "operational-success.sqlite3")
    result = await nado.run_operational_private_read_preflight(
        config=_config(contract), credential_loader=calls.load,
        derive_owner=calls.derive_owner, signer=calls.signer,
        recover_owner=calls.recover_owner, store=store,
    )
    assert result.status == nado.FINALIZED and not clock_values
    assert (calls.loader, calls.derive, calls.sign, calls.recover) == (1, 1, 1, 1)
    assert len(sessions) == 21 and all(len(session.calls) == 1 for session in sessions)
    assert sessions[8].calls[0][0] == nado.FixedPreflightIdentity.gateway_edge_query
    assert json.loads(sessions[8].calls[0][1]["data"]) == {"type": "time"}
    assert sessions[9].calls[0][0] == nado.FixedPreflightIdentity.trigger_query
    assert json.loads(sessions[9].calls[0][1]["data"])["tx"]["recvTime"] == "1700000030009"


@pytest.mark.parametrize("phase", ["before", "round-a"])
@pytest.mark.asyncio
async def test_operational_cancellation_stops_without_sensitive_or_trigger_work(
    tmp_path: Path, contract: dict[str, object], monkeypatch: pytest.MonkeyPatch,
    phase: str,
) -> None:
    events: list[str] = []
    response = _HttpResponse(
        b"", url=nado.FixedPreflightIdentity.gateway_query,
        never=True, events=events,
    )
    session = _HttpSession(response)
    monkeypatch.setattr(nado.aiohttp, "ClientSession", lambda **kwargs: session)
    sensitive = {"load": 0, "sign": 0, "recover": 0}

    def load() -> object:
        sensitive["load"] += 1
        return object()

    def sign(credential: object, typed: dict[str, object]) -> str:
        sensitive["sign"] += 1
        return str(contract["signature"])

    def recover(typed: dict[str, object], signature: str) -> str:
        sensitive["recover"] += 1
        return str(contract["owner"])

    store = nado.OneShotStore(tmp_path / f"cancel-{phase}.sqlite3")
    task = asyncio.create_task(nado.run_operational_private_read_preflight(
        config=_config(contract), credential_loader=load,
        derive_owner=lambda credential: str(contract["owner"]), signer=sign,
        recover_owner=recover, store=store,
    ))
    if phase == "round-a":
        for _ in range(100):
            if events:
                break
            await asyncio.sleep(0)
        assert events == ["read"]
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert sensitive == {"load": 0, "sign": 0, "recover": 0}
    assert store.state("fixture-invocation-001") == nado.NEW
    assert len(session.calls) <= 1
    if phase == "round-a":
        assert response.content.cancelled


@pytest.mark.asyncio
async def test_owned_http_transport_enforces_real_deadline_and_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _HttpSession(_HttpResponse(
        b"", url=nado.FixedPreflightIdentity.gateway_query, never=True,
    ))
    monkeypatch.setattr(nado.aiohttp, "ClientSession", lambda **kwargs: session)
    started = time.monotonic()
    with pytest.raises(nado.NadoPreflightError) as captured:
        await nado._OperationalGatewayTransport().send_async({"type": "status"})
    assert 4.5 <= time.monotonic() - started < 7
    _assert_sanitized(captured.value)
    assert len(session.calls) == 1
