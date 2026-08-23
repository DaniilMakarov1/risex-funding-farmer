from __future__ import annotations

import copy
import importlib
import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from risex_farmer.nado_private_read_preflight import (
    CLAIMED,
    FINALIZED,
    MAX_FRESHNESS_MS,
    OBSERVED,
    FixedPreflightIdentity,
    NadoPreflightError,
    OneShotStore,
    OperationalSignedObserver,
    PreflightConfig,
    SOURCE_PINS,
    encode_subaccount,
    list_trigger_orders_typed_data,
    run_fixture_preflight,
)


FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "nado_private_read_preflight"
    / "official_contract.json"
)


@pytest.fixture
def contract() -> dict[str, object]:
    return json.loads(FIXTURE.read_text())


class FixturePublicReader:
    def __init__(self, responses: list[dict[str, object]]) -> None:
        self.responses = copy.deepcopy(responses)
        self.calls: list[str] = []
        self.gateway_url = "https://gateway.test.nado.xyz/v1/query"
        self.trust_env = False
        self.allow_redirects = False
        self.tls_verified = True
        self.max_response_bytes = 65_536
        self.timeout_ms = 5_000
        self.max_response_bytes = 65_536

    def read(self, operation: str) -> dict[str, object]:
        self.calls.append(operation)
        if not self.responses:
            raise AssertionError("unexpected public read")
        response = self.responses.pop(0)
        assert response["op"] == operation
        return response


class SyntheticObserver:
    def __init__(self, response: dict[str, object], *, fail: bool = False) -> None:
        self.response = copy.deepcopy(response)
        self.fail = fail
        self.calls: list[dict[str, object]] = []
        self.server_time_ms = int(response["observed_at_ms"])
        self.trigger_url = "https://trigger.test.nado.xyz/v1/query"
        self.trust_env = False
        self.allow_redirects = False
        self.tls_verified = True
        self.max_response_bytes = 65_536
        self.timeout_ms = 5_000
        self.typed_calls: list[dict[str, object] | None] = []

    def observe(
        self, request: dict[str, object],
        typed_data: dict[str, object] | None = None,
    ) -> dict[str, object]:
        self.calls.append(copy.deepcopy(request))
        self.typed_calls.append(copy.deepcopy(typed_data))
        if self.fail:
            raise TimeoutError("ambiguous synthetic dispatch")
        return copy.deepcopy(self.response)


class ForbiddenBoundary:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, *_args: object, **_kwargs: object) -> object:
        self.calls += 1
        raise AssertionError("real credential/signing/network boundary reached in CI")


def _config(contract: dict[str, object]) -> PreflightConfig:
    return PreflightConfig(
        owner=str(contract["owner"]),
        subaccount_name=str(contract["subaccount_name"]),
        sender=str(contract["sender"]),
        invocation_id="fixture-invocation-001",
        exclusive_owner_lease=True,
        direct_owner_eoa=True,
        now_ms=1_700_000_000_025,
    )


def _reader(contract: dict[str, object]) -> FixturePublicReader:
    return FixturePublicReader(list(contract["round_a"]) + list(contract["round_b"]))


def _round_b_ending_at(
    contract: dict[str, object], final_observed_ms: int,
) -> list[dict[str, object]]:
    responses = copy.deepcopy(list(contract["round_b"]))
    shift = final_observed_ms - int(responses[-1]["observed_at_ms"])
    for response in responses:
        response["observed_at_ms"] = int(response["observed_at_ms"]) + shift
    return responses


def test_success_is_two_rounds_around_one_synthetic_observation_and_no_real_boundary(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    reader = _reader(contract)
    synthetic = SyntheticObserver(dict(contract["trigger"]))
    forbidden_loader = ForbiddenBoundary()
    forbidden_transport = ForbiddenBoundary()
    operational = OperationalSignedObserver(
        credential_loader=forbidden_loader,
        derive_owner=forbidden_loader,
        signer=forbidden_loader,
        server_time=forbidden_transport,
        private_post=forbidden_transport,
    )
    store = OneShotStore(tmp_path / "intent.sqlite3")

    result = run_fixture_preflight(
        config=_config(contract),
        public_reader=reader,
        synthetic_observer=synthetic,
        operational_observer=operational,
        store=store,
    )

    assert result.status == FINALIZED
    assert result.zero_regular_orders and result.exact_flat and result.zero_trigger_history
    assert len(synthetic.calls) == 1
    assert synthetic.calls[0] == {
        "type": "list_trigger_orders",
        "sender": contract["sender"],
        "recv_time": 1_700_000_030_010,
        "limit": 1,
    }
    assert forbidden_loader.calls == forbidden_transport.calls == 0
    assert store.state("fixture-invocation-001") == FINALIZED
    assert reader.calls == [entry["op"] for entry in contract["round_a"] + contract["round_b"]]
    with pytest.raises(NadoPreflightError, match="already finalized"):
        run_fixture_preflight(
            config=_config(contract), public_reader=_reader(contract),
            synthetic_observer=synthetic, operational_observer=operational, store=store,
        )
    assert len(synthetic.calls) == 1
    persisted = (tmp_path / "intent.sqlite3").read_bytes()
    assert str(contract["owner"]).encode() not in persisted
    assert str(contract["sender"]).encode() not in persisted


def test_pins_identity_and_subaccount_are_exact(contract: dict[str, object]) -> None:
    assert SOURCE_PINS == contract["sources"]
    assert FixedPreflightIdentity.as_dict() == contract["environment"]
    assert encode_subaccount(str(contract["owner"]), "default") == contract["sender"]
    for bad in ("", "more-than-twelve", "nul\0name", "défaut"):
        with pytest.raises(NadoPreflightError):
            encode_subaccount(str(contract["owner"]), bad)
    with pytest.raises(NadoPreflightError):
        _config(contract).__class__(
            owner=str(contract["owner"]), subaccount_name="default",
            sender="0x" + "00" * 32, invocation_id="x",
            exclusive_owner_lease=True, direct_owner_eoa=True,
            now_ms=1_700_000_000_025,
        )
    typed = list_trigger_orders_typed_data(str(contract["sender"]), 1_700_000_030_010)
    assert typed["primaryType"] == "ListTriggerOrders"
    assert typed["domain"] == {
        "name": "Nado", "version": "0.0.1", "chainId": 763373,
        "verifyingContract": "0x698D87105274292B5673367DEC81874Ce3633Ac2",
    }
    assert typed["types"]["ListTriggerOrders"] == [
        {"name": "sender", "type": "bytes32"},
        {"name": "recvTime", "type": "uint64"},
    ]


@pytest.mark.parametrize(
    ("balance_kind", "bad_key"),
    [
        ("spot_balances", "00"),
        ("spot_balances", "+0"),
        ("spot_balances", "x"),
        ("spot_balances", 0),
        ("spot_balances", True),
        ("spot_balances", None),
        ("perp_balances", "02"),
        ("perp_balances", "+2"),
        ("perp_balances", "x"),
        ("perp_balances", 2),
    ],
)
def test_balance_product_keys_must_be_canonical_decimal_strings(
    tmp_path: Path, contract: dict[str, object], balance_kind: str, bad_key: object
) -> None:
    responses = copy.deepcopy(list(contract["round_a"]) + list(contract["round_b"]))
    account = responses[4]["body"]
    balances = account[balance_kind]
    original = "0" if balance_kind == "spot_balances" else "2"
    balances[bad_key] = balances.pop(original)
    observer = SyntheticObserver(dict(contract["trigger"]))
    store = OneShotStore(tmp_path / f"bad-{balance_kind}-{bad_key}.sqlite3")
    with pytest.raises(NadoPreflightError):
        run_fixture_preflight(
            config=_config(contract), public_reader=FixturePublicReader(responses),
            synthetic_observer=observer, operational_observer=None, store=store,
        )
    assert observer.calls == []
    assert store.state("fixture-invocation-001") == "NEW"


def test_balance_product_key_aliases_cannot_collapse_coverage(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    responses = copy.deepcopy(list(contract["round_a"]) + list(contract["round_b"]))
    responses[4]["body"]["perp_balances"]["02"] = copy.deepcopy(
        responses[4]["body"]["perp_balances"]["2"]
    )
    store = OneShotStore(tmp_path / "alias.sqlite3")
    with pytest.raises(NadoPreflightError):
        run_fixture_preflight(
            config=_config(contract), public_reader=FixturePublicReader(responses),
            synthetic_observer=SyntheticObserver(dict(contract["trigger"])),
            operational_observer=None, store=store,
        )
    assert store.state("fixture-invocation-001") == "NEW"


def test_one_shot_observation_binds_exact_typed_data_to_request(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    observer = SyntheticObserver(dict(contract["trigger"]))
    run_fixture_preflight(
        config=_config(contract), public_reader=_reader(contract),
        synthetic_observer=observer, operational_observer=None,
        store=OneShotStore(tmp_path / "typed.sqlite3"),
    )
    assert len(observer.calls) == len(observer.typed_calls) == 1
    assert observer.typed_calls[0] == list_trigger_orders_typed_data(
        str(observer.calls[0]["sender"]), int(observer.calls[0]["recv_time"])
    )


@pytest.mark.parametrize("timeout_ms", [0, 30_001])
def test_synthetic_signed_timeout_policy_rejects_before_claim(
    tmp_path: Path, contract: dict[str, object], timeout_ms: int
) -> None:
    observer = SyntheticObserver(dict(contract["trigger"]))
    observer.timeout_ms = timeout_ms
    store = OneShotStore(tmp_path / f"timeout-{timeout_ms}.sqlite3")
    with pytest.raises(NadoPreflightError, match="transport policy"):
        run_fixture_preflight(
            config=_config(contract), public_reader=_reader(contract),
            synthetic_observer=observer, operational_observer=None, store=store,
        )
    assert observer.calls == []
    assert store.state("fixture-invocation-001") == "NEW"


def test_signed_timeout_policy_is_required_and_operationally_bounded(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    observer = SyntheticObserver(dict(contract["trigger"]))
    del observer.timeout_ms
    store = OneShotStore(tmp_path / "missing-timeout.sqlite3")
    with pytest.raises(NadoPreflightError, match="transport policy"):
        run_fixture_preflight(
            config=_config(contract), public_reader=_reader(contract),
            synthetic_observer=observer, operational_observer=None, store=store,
        )
    forbidden = ForbiddenBoundary()
    with pytest.raises(NadoPreflightError, match="transport policy"):
        OperationalSignedObserver(
            credential_loader=forbidden, derive_owner=forbidden, signer=forbidden,
            server_time=forbidden, private_post=forbidden, timeout_ms=0,
        )
    assert forbidden.calls == 0


@pytest.mark.parametrize("index", list(range(9)))
def test_every_round_a_public_failure_precedes_claim_and_observation(
    tmp_path: Path, contract: dict[str, object], index: int
) -> None:
    responses = copy.deepcopy(list(contract["round_a"]) + list(contract["round_b"]))
    responses[index]["status"] = 403
    reader = FixturePublicReader(responses)
    synthetic = SyntheticObserver(dict(contract["trigger"]))
    store = OneShotStore(tmp_path / f"intent-{index}.sqlite3")
    with pytest.raises(NadoPreflightError, match="public transport rejected"):
        run_fixture_preflight(
            config=_config(contract), public_reader=reader,
            synthetic_observer=synthetic, operational_observer=None, store=store,
        )
    assert synthetic.calls == []
    assert store.state("fixture-invocation-001") == "NEW"


@pytest.mark.parametrize(
    ("op", "mutation", "message"),
    [
        ("contracts", lambda b: b.update(chain_id=1), "identity"),
        ("contracts", lambda b: b.update(endpoint="0x" + "00" * 20), "identity"),
        ("status", lambda b: b.update(status="inactive"), "active"),
        ("linked_signer", lambda b: b.update(linked_signer="0x" + "01" * 20), "linked signer"),
        ("subaccount_info", lambda b: b.update(exists=False), "exist"),
        ("subaccount_info", lambda b: b.update(health="0"), "health"),
        ("subaccount_info", lambda b: b["spot_balances"].update({"0":"-1"}), "negative"),
        ("subaccount_info", lambda b: b["spot_balances"].update({"0":"4999999999999999999"}), "collateral"),
        ("subaccount_info", lambda b: b["perp_balances"].pop("4"), "cross-perp"),
        ("subaccount_info", lambda b: b["perp_balances"]["2"].update(amount="1"), "flat"),
        ("subaccount_info", lambda b: b["perp_balances"]["2"].update(v_quote_balance="1"), "v_quote"),
        ("open_orders:2", lambda b: b["orders"].append({"digest":"0x01"}), "regular order"),
        ("isolated_positions", lambda b: b["positions"].append({"product_id":2}), "isolated"),
    ],
)
def test_round_a_contract_defects_fail_before_claim(
    tmp_path: Path, contract: dict[str, object], op: str,
    mutation: object, message: str,
) -> None:
    responses = copy.deepcopy(list(contract["round_a"]) + list(contract["round_b"]))
    target = next(item for item in responses[:9] if item["op"] == op)
    mutation(target["body"])
    store = OneShotStore(tmp_path / "intent.sqlite3")
    observer = SyntheticObserver(dict(contract["trigger"]))
    with pytest.raises(NadoPreflightError, match=message):
        run_fixture_preflight(
            config=_config(contract), public_reader=FixturePublicReader(responses),
            synthetic_observer=observer, operational_observer=None, store=store,
        )
    assert observer.calls == []
    assert store.state("fixture-invocation-001") == "NEW"


def test_catalog_must_be_complete_duplicate_free_and_cover_orders(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    for alter in ("incomplete", "duplicate", "missing_order"):
        responses = copy.deepcopy(list(contract["round_a"]) + list(contract["round_b"]))
        catalog = responses[2]["body"]
        if alter == "incomplete":
            catalog["complete"] = False
        elif alter == "duplicate":
            catalog["products"].append(copy.deepcopy(catalog["products"][-1]))
        else:
            responses.pop(6)
        store = OneShotStore(tmp_path / f"{alter}.sqlite3")
        with pytest.raises((NadoPreflightError, AssertionError)):
            run_fixture_preflight(
                config=_config(contract), public_reader=FixturePublicReader(responses),
                synthetic_observer=SyntheticObserver(dict(contract["trigger"])),
                operational_observer=None, store=store,
            )
        assert store.state("fixture-invocation-001") == "NEW"


def test_ambiguous_signed_observation_is_claimed_and_never_retried(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    store = OneShotStore(tmp_path / "intent.sqlite3")
    observer = SyntheticObserver(dict(contract["trigger"]), fail=True)
    forbidden = ForbiddenBoundary()
    operational = OperationalSignedObserver(
        credential_loader=forbidden, derive_owner=forbidden, signer=forbidden,
        server_time=forbidden, private_post=forbidden,
    )
    with pytest.raises(NadoPreflightError, match="ambiguous") as captured:
        run_fixture_preflight(
            config=_config(contract), public_reader=_reader(contract),
            synthetic_observer=observer, operational_observer=operational, store=store,
        )
    assert captured.value.__cause__ is None
    assert store.state("fixture-invocation-001") == CLAIMED
    assert len(observer.calls) == 1
    with pytest.raises(NadoPreflightError, match="already claimed"):
        run_fixture_preflight(
            config=_config(contract), public_reader=_reader(contract),
            synthetic_observer=observer, operational_observer=operational, store=store,
        )
    assert len(observer.calls) == 1
    assert forbidden.calls == 0


def test_claim_conflict_is_atomic_across_store_connections(tmp_path: Path) -> None:
    path = tmp_path / "intent.sqlite3"
    first = OneShotStore(path)
    second = OneShotStore(path)
    first.claim("same-invocation", "a" * 64, "b" * 64, 1)
    with pytest.raises(NadoPreflightError, match="already claimed"):
        second.claim("same-invocation", "a" * 64, "b" * 64, 1)
    assert first.state("same-invocation") == second.state("same-invocation") == CLAIMED


@pytest.mark.parametrize(
    "mutation",
    [
        lambda response: response.update(status=403),
        lambda response: response.update(final_url="https://wrong.test/v1/query"),
        lambda response: response["body"].update(sender="0x" + "00" * 32),
        lambda response: response["body"]["orders"].append({"digest":"0x01"}),
        lambda response: response.update(observed_at_ms=1_699_999_900_000),
        lambda response: response["body"].update(unexpected=True),
    ],
)
def test_bad_or_nonzero_trigger_observation_is_ambiguous_and_not_replayed(
    tmp_path: Path, contract: dict[str, object], mutation: object
) -> None:
    response = copy.deepcopy(dict(contract["trigger"]))
    mutation(response)
    store = OneShotStore(tmp_path / "intent.sqlite3")
    observer = SyntheticObserver(response)
    observer.server_time_ms = int(contract["trigger"]["observed_at_ms"])
    with pytest.raises(NadoPreflightError):
        run_fixture_preflight(
            config=_config(contract), public_reader=_reader(contract),
            synthetic_observer=observer, operational_observer=None, store=store,
        )
    assert store.state("fixture-invocation-001") == CLAIMED
    assert len(observer.calls) == 1


def test_round_b_failure_after_observation_resumes_without_second_signed_attempt(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    first = copy.deepcopy(list(contract["round_a"]) + list(contract["round_b"]))
    first[12]["status"] = 403
    store = OneShotStore(tmp_path / "intent.sqlite3")
    observer = SyntheticObserver(dict(contract["trigger"]))
    with pytest.raises(NadoPreflightError):
        run_fixture_preflight(
            config=_config(contract), public_reader=FixturePublicReader(first),
            synthetic_observer=observer, operational_observer=None, store=store,
        )
    assert store.state("fixture-invocation-001") == OBSERVED
    assert len(observer.calls) == 1
    result = run_fixture_preflight(
        config=_config(contract),
        public_reader=FixturePublicReader(list(contract["round_b"])),
        synthetic_observer=observer, operational_observer=None, store=store,
    )
    assert result.status == FINALIZED
    assert len(observer.calls) == 1


def test_stale_observed_resume_halts_before_public_read_or_second_signature(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    store = OneShotStore(tmp_path / "stale-resume.sqlite3")
    observer = SyntheticObserver(dict(contract["trigger"]))
    with pytest.raises(NadoPreflightError):
        run_fixture_preflight(
            config=_config(contract),
            public_reader=FixturePublicReader(list(contract["round_a"])),
            synthetic_observer=observer, operational_observer=None, store=store,
        )
    assert store.state("fixture-invocation-001") == OBSERVED
    assert len(observer.calls) == 1

    next_day = replace(_config(contract), now_ms=_config(contract).now_ms + 86_400_000)
    reader = FixturePublicReader(
        _round_b_ending_at(contract, next_day.now_ms - 1)
    )
    with pytest.raises(NadoPreflightError, match="durable temporal"):
        run_fixture_preflight(
            config=next_day, public_reader=reader, synthetic_observer=observer,
            operational_observer=None, store=store,
        )
    assert reader.calls == []
    assert store.state("fixture-invocation-001") == OBSERVED
    assert len(observer.calls) == 1


@pytest.mark.parametrize(
    ("age_ms", "accepted"),
    [(MAX_FRESHNESS_MS, True), (MAX_FRESHNESS_MS + 1, False)],
)
def test_resumed_barrier_maximum_interval_edge_is_explicit(
    tmp_path: Path, contract: dict[str, object], age_ms: int, accepted: bool
) -> None:
    store = OneShotStore(tmp_path / f"edge-{age_ms}.sqlite3")
    observer = SyntheticObserver(dict(contract["trigger"]))
    with pytest.raises(NadoPreflightError):
        run_fixture_preflight(
            config=_config(contract),
            public_reader=FixturePublicReader(list(contract["round_a"])),
            synthetic_observer=observer, operational_observer=None, store=store,
        )
    round_a_last = int(contract["round_a"][-1]["observed_at_ms"])
    now_ms = round_a_last + age_ms
    reader = FixturePublicReader(_round_b_ending_at(contract, now_ms))
    config = replace(_config(contract), now_ms=now_ms)
    if accepted:
        assert run_fixture_preflight(
            config=config, public_reader=reader, synthetic_observer=observer,
            operational_observer=None, store=store,
        ).status == FINALIZED
        assert reader.calls
    else:
        with pytest.raises(NadoPreflightError, match="durable temporal"):
            run_fixture_preflight(
                config=config, public_reader=reader, synthetic_observer=observer,
                operational_observer=None, store=store,
            )
        assert reader.calls == []
        assert store.state("fixture-invocation-001") == OBSERVED
    assert len(observer.calls) == 1


def test_resumed_observed_evidence_rejects_clock_rollback_before_public_read(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    store = OneShotStore(tmp_path / "rollback.sqlite3")
    observer = SyntheticObserver(dict(contract["trigger"]))
    with pytest.raises(NadoPreflightError):
        run_fixture_preflight(
            config=_config(contract),
            public_reader=FixturePublicReader(list(contract["round_a"])),
            synthetic_observer=observer, operational_observer=None, store=store,
        )
    trigger_at = int(contract["trigger"]["observed_at_ms"])
    reader = FixturePublicReader(list(contract["round_b"]))
    with pytest.raises(NadoPreflightError, match="durable temporal"):
        run_fixture_preflight(
            config=replace(_config(contract), now_ms=trigger_at - 1),
            public_reader=reader, synthetic_observer=observer,
            operational_observer=None, store=store,
        )
    assert reader.calls == []
    assert store.state("fixture-invocation-001") == OBSERVED
    assert len(observer.calls) == 1


def test_server_time_must_strictly_follow_round_a_before_observation(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    observer = SyntheticObserver(dict(contract["trigger"]))
    observer.server_time_ms = int(contract["round_a"][-1]["observed_at_ms"])
    store = OneShotStore(tmp_path / "server-order.sqlite3")
    with pytest.raises(NadoPreflightError, match="ambiguous"):
        run_fixture_preflight(
            config=_config(contract), public_reader=_reader(contract),
            synthetic_observer=observer, operational_observer=None, store=store,
        )
    assert observer.calls == []
    assert store.state("fixture-invocation-001") == CLAIMED


def test_round_b_resume_is_bound_to_full_subaccount_identity(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    responses = copy.deepcopy(list(contract["round_a"]) + list(contract["round_b"]))
    responses[12]["status"] = 403
    store = OneShotStore(tmp_path / "identity.sqlite3")
    observer = SyntheticObserver(dict(contract["trigger"]))
    with pytest.raises(NadoPreflightError):
        run_fixture_preflight(
            config=_config(contract), public_reader=FixturePublicReader(responses),
            synthetic_observer=observer, operational_observer=None, store=store,
        )
    other_owner = "0x0000000000000000000000000000000000000002"
    other = PreflightConfig(
        owner=other_owner, subaccount_name="default",
        sender=encode_subaccount(other_owner, "default"),
        invocation_id="fixture-invocation-001", exclusive_owner_lease=True,
        direct_owner_eoa=True, now_ms=1_700_000_000_025,
    )
    reader = FixturePublicReader(list(contract["round_b"]))
    with pytest.raises(NadoPreflightError, match="identity mismatch"):
        run_fixture_preflight(
            config=other, public_reader=reader, synthetic_observer=observer,
            operational_observer=None, store=store,
        )
    assert reader.calls == []
    assert len(observer.calls) == 1


def test_corrupt_durable_trigger_evidence_never_finalizes(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    responses = copy.deepcopy(list(contract["round_a"]) + list(contract["round_b"]))
    responses[12]["status"] = 403
    path = tmp_path / "corrupt.sqlite3"
    store = OneShotStore(path)
    observer = SyntheticObserver(dict(contract["trigger"]))
    with pytest.raises(NadoPreflightError):
        run_fixture_preflight(
            config=_config(contract), public_reader=FixturePublicReader(responses),
            synthetic_observer=observer, operational_observer=None, store=store,
        )
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE nado_preflight_one_shot SET trigger_hash = ?",
            ("0" * 64,),
        )
    reader = FixturePublicReader(list(contract["round_b"]))
    with pytest.raises(NadoPreflightError, match="trigger evidence"):
        run_fixture_preflight(
            config=_config(contract), public_reader=reader,
            synthetic_observer=observer, operational_observer=None, store=store,
        )
    assert reader.calls == []
    assert len(observer.calls) == 1


def test_round_b_identity_catalog_or_state_disagreement_never_finalizes(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    for label, index, mutate in (
        ("catalog", 20, lambda b: b["products"][1].update(symbol="XBT-PERP")),
        ("orders", 11, lambda b: b["orders"].append({"digest":"0x01"})),
        ("linked", 21, lambda b: b.update(linked_signer="0x" + "01" * 20)),
    ):
        responses = copy.deepcopy(list(contract["round_a"]) + list(contract["round_b"]))
        mutate(responses[index]["body"])
        store = OneShotStore(tmp_path / f"{label}.sqlite3")
        observer = SyntheticObserver(dict(contract["trigger"]))
        with pytest.raises(NadoPreflightError):
            run_fixture_preflight(
                config=_config(contract), public_reader=FixturePublicReader(responses),
                synthetic_observer=observer, operational_observer=None, store=store,
            )
        assert store.state("fixture-invocation-001") == OBSERVED
        assert len(observer.calls) == 1


@pytest.mark.parametrize(
    ("op", "mutation"),
    [
        ("subaccount_info", lambda b: b.update(exists=False)),
        ("subaccount_info", lambda b: b["perp_balances"]["4"].update(amount="1")),
        ("subaccount_info", lambda b: b["perp_balances"]["4"].update(v_quote_balance="1")),
        ("isolated_positions", lambda b: b["positions"].append({"product_id":4})),
        ("contracts", lambda b: b.update(chain_id=1)),
        ("status", lambda b: b.update(status="inactive")),
    ],
)
def test_every_round_b_safety_predicate_blocks_finalization(
    tmp_path: Path, contract: dict[str, object], op: str, mutation: object
) -> None:
    responses = copy.deepcopy(list(contract["round_a"]) + list(contract["round_b"]))
    target = next(item for item in responses[9:] if item["op"] == op)
    mutation(target["body"])
    store = OneShotStore(tmp_path / f"round-b-{op}.sqlite3")
    observer = SyntheticObserver(dict(contract["trigger"]))
    with pytest.raises(NadoPreflightError):
        run_fixture_preflight(
            config=_config(contract), public_reader=FixturePublicReader(responses),
            synthetic_observer=observer, operational_observer=None, store=store,
        )
    assert store.state("fixture-invocation-001") == OBSERVED
    assert len(observer.calls) == 1


def test_public_transport_is_sealed_fresh_and_strict_schema(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    for label, mutation in (
        ("redirect", lambda r: r.update(final_url="https://gateway.test.nado.xyz/v1/query/")),
        ("stale", lambda r: r.update(observed_at_ms=1_699_999_900_000)),
        ("future", lambda r: r.update(observed_at_ms=1_700_000_000_026)),
        ("schema", lambda r: r["body"].update(unexpected=True)),
    ):
        responses = copy.deepcopy(list(contract["round_a"]) + list(contract["round_b"]))
        mutation(responses[0])
        store = OneShotStore(tmp_path / f"{label}.sqlite3")
        with pytest.raises(NadoPreflightError):
            run_fixture_preflight(
                config=_config(contract), public_reader=FixturePublicReader(responses),
                synthetic_observer=SyntheticObserver(dict(contract["trigger"])),
                operational_observer=None, store=store,
            )
        assert store.state("fixture-invocation-001") == "NEW"


def test_transport_policy_and_temporal_barrier_fail_closed(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    for label, alter in (
        ("proxy", lambda reader, observer: setattr(reader, "trust_env", True)),
        ("redirects", lambda reader, observer: setattr(reader, "allow_redirects", True)),
        ("tls", lambda reader, observer: setattr(reader, "tls_verified", False)),
        ("fallback", lambda reader, observer: setattr(reader, "gateway_url", "https://other.test/query")),
        ("trigger_proxy", lambda reader, observer: setattr(observer, "trust_env", True)),
    ):
        reader = _reader(contract)
        observer = SyntheticObserver(dict(contract["trigger"]))
        alter(reader, observer)
        store = OneShotStore(tmp_path / f"{label}.sqlite3")
        with pytest.raises(NadoPreflightError):
            run_fixture_preflight(
                config=_config(contract), public_reader=reader,
                synthetic_observer=observer, operational_observer=None, store=store,
            )
        assert observer.calls == []
        assert store.state("fixture-invocation-001") == "NEW"

    responses = copy.deepcopy(list(contract["round_a"]) + list(contract["round_b"]))
    responses[9]["observed_at_ms"] = 1_700_000_000_009
    store = OneShotStore(tmp_path / "temporal.sqlite3")
    observer = SyntheticObserver(dict(contract["trigger"]))
    with pytest.raises(NadoPreflightError, match="temporal"):
        run_fixture_preflight(
            config=_config(contract), public_reader=FixturePublicReader(responses),
            synthetic_observer=observer, operational_observer=None, store=store,
        )
    assert store.state("fixture-invocation-001") == OBSERVED
    assert len(observer.calls) == 1


def test_response_size_and_server_time_fail_before_signed_dispatch(
    tmp_path: Path, contract: dict[str, object]
) -> None:
    reader = _reader(contract)
    reader.max_response_bytes = 16
    observer = SyntheticObserver(dict(contract["trigger"]))
    store = OneShotStore(tmp_path / "size.sqlite3")
    with pytest.raises(NadoPreflightError, match="size"):
        run_fixture_preflight(
            config=_config(contract), public_reader=reader,
            synthetic_observer=observer, operational_observer=None, store=store,
        )
    assert observer.calls == []
    assert store.state("fixture-invocation-001") == "NEW"

    reader = _reader(contract)
    observer = SyntheticObserver(dict(contract["trigger"]))
    observer.server_time_ms = 1_700_000_000_026
    store = OneShotStore(tmp_path / "clock.sqlite3")
    with pytest.raises(NadoPreflightError, match="ambiguous"):
        run_fixture_preflight(
            config=_config(contract), public_reader=reader,
            synthetic_observer=observer, operational_observer=None, store=store,
        )
    assert observer.calls == []
    assert store.state("fixture-invocation-001") == CLAIMED


def test_module_is_isolated_and_has_no_ambient_secret_or_network_surface() -> None:
    package = importlib.import_module("risex_farmer")
    module = importlib.import_module("risex_farmer.nado_private_read_preflight")
    assert "nado_private_read_preflight" not in Path(package.__file__).read_text()
    source = Path(module.__file__).read_text()
    for forbidden in (
        "aiohttp", "requests", "urllib", "os.environ", "getenv(",
        "open(", "Path.home", "XLSX", "seed phrase", "socket",
        "http.client", "subprocess", "urlopen", "ClientSession",
    ):
        assert forbidden not in source
    fixture_entry = source[source.index("def run_fixture_preflight("):]
    assert "operational_observer.observe" not in fixture_entry
    assert ".credential_loader" not in fixture_entry
    assert "del operational_observer" in fixture_entry
    assert "PRAGMA synchronous=FULL" in source
