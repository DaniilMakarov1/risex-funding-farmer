import copy
import importlib
import json
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

import pytest

from risex_farmer.extended_testnet_lifecycle import (
    MAX_NOTIONAL_USD,
    SDK_PROVENANCE,
    TESTNET_CONTRACT,
    ContractViolation,
    EvidenceViolation,
    ExtendedLifecycle,
    IntentConflict,
    IntentStore,
    LifecycleHalted,
    NonceViolation,
    build_cancel_by_external_id,
    build_limit_ioc_order,
    build_mass_cancel,
    canonical_payload_digest,
    normalize_official_evidence,
)


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "extended_testnet_001" / "official_lifecycle.json"


@pytest.fixture
def wire():
    return json.loads(FIXTURE_PATH.read_text())


@pytest.fixture
def evidence(wire):
    return normalize_official_evidence(copy.deepcopy(wire), now_ms=1_770_000_000_000)


@pytest.fixture
def loader_calls():
    return []


@pytest.fixture
def lifecycle(tmp_path, loader_calls):

    def forbidden_loader():
        loader_calls.append("credential")
        raise AssertionError("the fixture-only lifecycle must never load credentials")

    return ExtendedLifecycle(
        store=IntentStore(tmp_path / "extended-lifecycle.sqlite3"),
        credential_loader=forbidden_loader,
    )


def prepare_entry(lifecycle, evidence, wire, **overrides):
    params = {
        "nonce": wire["entry"]["nonce"],
        "settlement_hash": wire["entry"]["settlementHash"],
        "market": wire["entry"]["market"],
        "side": wire["entry"]["side"],
        "qty": Decimal(wire["entry"]["qty"]),
        "price": Decimal(wire["entry"]["price"]),
        "expiry_ms": wire["entry"]["expiryEpochMillis"],
        "reduce_only": False,
        "evidence": evidence,
    }
    params.update(overrides)
    return lifecycle.prepare_order(**params)


def claim_entry(lifecycle, evidence, wire, **overrides):
    intent = prepare_entry(lifecycle, evidence, wire, **overrides)
    lifecycle.claim_for_dispatch(intent.id, evidence=evidence)
    return intent


def prepare_close(lifecycle, entry_id, evidence, wire, **overrides):
    params = {
        "entry_id": entry_id,
        "evidence": evidence,
        "nonce": wire["close"]["nonce"],
        "settlement_hash": wire["close"]["settlementHash"],
        "price": Decimal(wire["close"]["price"]),
        "expiry_ms": wire["close"]["expiryEpochMillis"],
    }
    params.update(overrides)
    return lifecycle.prepare_close(**params)


def reopen(lifecycle):
    def forbidden_loader():
        raise AssertionError("the fixture-only lifecycle must never load credentials")

    return ExtendedLifecycle(store=IntentStore(lifecycle.store.path), credential_loader=forbidden_loader)


def filled_evidence(wire, *, now_ms=1_770_000_003_000):
    changed = copy.deepcopy(wire)
    order = changed["filledOrder"]
    position = changed["position"]
    changed["account"]["serverTime"] = now_ms
    changed["account"]["observedAt"] = now_ms
    changed["account"]["openOrders"] = []
    changed["account"]["positions"] = [position]
    changed["account"]["orderStatus"] = order
    changed["account"]["orderHistory"] = [order]
    changed["account"]["fills"] = [changed["fill"]]
    changed["stream"]["eventTime"] = now_ms
    changed["stream"]["receivedAt"] = now_ms
    changed["stream"]["orders"] = [order]
    changed["stream"]["positions"] = [position]
    changed["stream"]["trades"] = [changed["fill"]]
    return normalize_official_evidence(changed, now_ms=now_ms)


def closed_evidence(wire, *, now_ms=1_770_000_020_000):
    changed = copy.deepcopy(wire)
    changed["account"]["serverTime"] = now_ms
    changed["account"]["observedAt"] = now_ms
    changed["account"]["openOrders"] = []
    changed["account"]["positions"] = []
    changed["account"]["orderStatus"] = changed["filledCloseOrder"]
    changed["account"]["orderHistory"] = [changed["filledOrder"], changed["filledCloseOrder"]]
    changed["account"]["fills"] = [changed["fill"], changed["closeFill"]]
    changed["stream"]["eventTime"] = now_ms
    changed["stream"]["receivedAt"] = now_ms
    changed["stream"]["orders"] = [changed["filledOrder"], changed["filledCloseOrder"]]
    changed["stream"]["positions"] = []
    changed["stream"]["trades"] = [changed["fill"], changed["closeFill"]]
    return normalize_official_evidence(changed, now_ms=now_ms)


def test_full_sepolia_domain_and_pinned_official_provenance_are_exact(wire):
    assert SDK_PROVENANCE == wire["provenance"]
    assert TESTNET_CONTRACT == wire["contract"]
    assert TESTNET_CONTRACT["starknetDomain"] == {
        "name": "Perpetuals",
        "version": "v0",
        "chainId": "SN_SEPOLIA",
        "revision": "1",
    }


@pytest.mark.parametrize(
    ("path", "bad_value"),
    [
        (("provenance", "commit"), "0" * 40),
        (("contract", "apiBaseUrl"), "https://api.starknet.extended.exchange/api/v1"),
        (("contract", "signingDomain"), "extended.exchange"),
        (("contract", "starknetDomain", "name"), "Extended"),
        (("contract", "starknetDomain", "version"), "1"),
        (("contract", "starknetDomain", "chainId"), "SN_MAIN"),
        (("contract", "starknetDomain", "revision"), "2"),
    ],
)
def test_contract_or_provenance_mismatch_fails_closed(wire, path, bad_value):
    target = wire
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = bad_value
    with pytest.raises(ContractViolation):
        normalize_official_evidence(wire, now_ms=1_770_000_000_000)


def test_official_wire_schema_is_strict(wire):
    with_extra = copy.deepcopy(wire)
    with_extra["filledOrder"]["undocumentedStatus"] = "FILLED"
    with pytest.raises(EvidenceViolation):
        normalize_official_evidence(with_extra, now_ms=1_770_000_000_000)

    missing = copy.deepcopy(wire)
    del missing["market"]["tradingConfig"]["minOrderSizeChange"]
    with pytest.raises(EvidenceViolation):
        normalize_official_evidence(missing, now_ms=1_770_000_000_000)


def test_sdk_settlement_hash_identity_and_api_key_only_cancel_shapes(wire):
    order = build_limit_ioc_order(copy.deepcopy(wire["entry"]))
    assert order["id"] == str(wire["entry"]["settlementHash"])
    assert order["id"] == wire["entry"]["externalId"]
    assert order["type"] == "LIMIT"
    assert order["timeInForce"] == "IOC"
    assert order["price"] == "40010"
    assert canonical_payload_digest(order) == wire["vectors"]["canonicalPayloadDigest"]
    assert "authorization" not in {key.lower() for key in order}

    cancel = build_cancel_by_external_id(order["id"], wire["identity"]["apiKey"])
    assert cancel == {
        "method": "DELETE",
        "path": "/user/order",
        "query": {"externalId": order["id"]},
        "headers": {"X-Api-Key": "fixture-api-key-0001"},
        "json": None,
    }
    mass = build_mass_cancel([order["id"]], wire["identity"]["apiKey"])
    assert mass == {
        "method": "POST",
        "path": "/user/order/massCancel",
        "query": {},
        "headers": {"X-Api-Key": "fixture-api-key-0001"},
        "json": {"externalOrderIds": [order["id"]], "cancelAll": False},
    }

    close = build_limit_ioc_order(copy.deepcopy(wire["close"]))
    assert close["id"] == wire["close"]["externalId"]
    assert close["nonce"] == 2**31 - 1
    assert close["side"] == "SELL"
    assert close["reduceOnly"] is True
    assert close["timeInForce"] == "IOC"
    assert canonical_payload_digest(close) == wire["vectors"]["closeCanonicalPayloadDigest"]


def test_external_id_must_equal_decimal_sdk_settlement_hash(wire):
    mismatched = copy.deepcopy(wire["entry"])
    mismatched["externalId"] = "123456789012345678901234567891"
    with pytest.raises(ContractViolation, match="EXTERNAL_ID"):
        build_limit_ioc_order(mismatched)


@pytest.mark.parametrize("nonce", [None, 0, -1, 2**31, 2**32 - 1])
def test_nonce_is_explicit_and_inside_documented_intersection(lifecycle, evidence, wire, nonce):
    with pytest.raises(NonceViolation):
        prepare_entry(lifecycle, evidence, wire, nonce=nonce)


def test_nonce_external_id_and_digest_are_never_reused(lifecycle, evidence, wire):
    first = prepare_entry(lifecycle, evidence, wire)
    assert first.state == "PREPARED"
    with pytest.raises((NonceViolation, IntentConflict)):
        prepare_entry(lifecycle, evidence, wire, price=Decimal("40009"))
    with pytest.raises((NonceViolation, IntentConflict)):
        prepare_entry(lifecycle, evidence, wire, nonce=wire["entry"]["nonce"] + 1)


def test_prepared_is_durable_before_claim_and_reopen_denies_second_claim(lifecycle, evidence, wire):
    intent = prepare_entry(lifecycle, evidence, wire)
    reopened = IntentStore(lifecycle.store.path)
    durable = reopened.get(intent.id)
    assert durable.state == "PREPARED"
    assert durable.nonce == wire["entry"]["nonce"]
    assert durable.external_id == wire["entry"]["externalId"]
    assert durable.unsigned_api_payload == build_limit_ioc_order(copy.deepcopy(wire["entry"]))
    assert durable.payload_digest == intent.payload_digest
    assert durable.payload_digest == wire["vectors"]["canonicalPayloadDigest"]
    assert durable.expiry_ms == wire["entry"]["expiryEpochMillis"]

    lifecycle.claim_for_dispatch(intent.id, evidence=evidence)
    assert IntentStore(lifecycle.store.path).get(intent.id).state == "CLAIMED"
    restarted = reopen(lifecycle)
    with pytest.raises(LifecycleHalted):
        restarted.claim_for_dispatch(intent.id, evidence=evidence)
    assert restarted.store.dispatch_count(intent.id) == 1


@pytest.mark.parametrize("server_offset_ms", [0, 1])
def test_prepared_at_or_after_persisted_server_expiry_can_never_be_claimed(
    lifecycle, evidence, wire, server_offset_ms
):
    intent = prepare_entry(lifecycle, evidence, wire)
    server_time = intent.expiry_ms + server_offset_ms
    fresh = evidence.with_server_time(server_time, observed_at=server_time)
    with pytest.raises(LifecycleHalted, match="EXPIRED"):
        lifecycle.claim_for_dispatch(intent.id, evidence=fresh)
    assert lifecycle.store.get(intent.id).state != "CLAIMED"
    assert lifecycle.next_write(fresh) is None


@pytest.mark.parametrize("mismatch", ["ACCOUNT", "MARKET", "STALE", "OPEN_ORDER"])
def test_claim_revalidates_identity_market_freshness_and_account_state(
    lifecycle, evidence, wire, mismatch
):
    intent = prepare_entry(lifecycle, evidence, wire)
    if mismatch == "ACCOUNT":
        claim_evidence = evidence.with_account_identity(account_id=7002)
    elif mismatch == "MARKET":
        claim_evidence = evidence.with_market_name("ETH-USD")
    elif mismatch == "STALE":
        claim_evidence = evidence.with_server_time(1_770_000_030_001, observed_at=1_770_000_030_001)
    else:
        open_order = copy.deepcopy(wire["filledOrder"])
        open_order.update(id=99999, externalId="99999", status="NEW", filledQty="0")
        claim_evidence = evidence.with_open_orders([open_order])

    with pytest.raises((EvidenceViolation, LifecycleHalted)):
        lifecycle.claim_for_dispatch(intent.id, evidence=claim_evidence)
    assert lifecycle.store.get(intent.id).state == "PREPARED"
    assert lifecycle.store.dispatch_count(intent.id) == 0


@pytest.mark.parametrize(
    "change",
    [
        ("type", "MARKET"),
        ("timeInForce", "GTT"),
        ("timeInForce", "FOK"),
        ("timeInForce", "TOB"),
        ("price", None),
        ("expiryEpochMillis", 1_770_000_000_000),
        ("expiryEpochMillis", 1_770_000_060_001),
    ],
)
def test_only_price_bounded_short_expiry_limit_ioc_is_allowed(wire, change):
    key, value = change
    order = copy.deepcopy(wire["entry"])
    order[key] = value
    with pytest.raises(ContractViolation):
        build_limit_ioc_order(order, server_time_ms=1_770_000_000_000)


@pytest.mark.parametrize(
    ("path", "bad_value", "reason"),
    [
        (("market", "active"), False, "MARKET_INACTIVE"),
        (("market", "isRfq"), True, "RFQ_FORBIDDEN"),
        (("market", "type"), "SPOT", "PERPETUAL_REQUIRED"),
        (("market", "tradingConfig", "minOrderSizeChange"), "0.002", "MINIMUM_STEP_MISMATCH"),
        (("entry", "qty"), "0.0015", "QTY_OFF_GRID"),
        (("entry", "price"), "40010.5", "PRICE_OFF_GRID"),
        (("book", "asks", 0, "qty"), "0.000", "DEPTH_INSUFFICIENT"),
        (("account", "fees", 0, "takerFeeRate"), "0.0003", "FEE_MISMATCH"),
        (("account", "leverage", 0, "leverage"), "21", "LEVERAGE_INVALID"),
        (("account", "balance", "availableForTrade"), "0", "BALANCE_INSUFFICIENT"),
        (("account", "openOrders"), "FILLED_ORDER", "NOT_FLAT"),
        (("stream", "gapFree"), False, "STREAM_GAP"),
        (("stream", "connected"), False, "STREAM_DISCONNECTED"),
        (("stream", "accountId"), 7002, "ACCOUNT_IDENTITY_MISMATCH"),
        (("identity", "accountId"), 7002, "ACCOUNT_IDENTITY_MISMATCH"),
        (("identity", "l2Key"), "0x54321", "ACCOUNT_IDENTITY_MISMATCH"),
        (("account", "info", "l2Vault"), 7001004, "ACCOUNT_IDENTITY_MISMATCH"),
        (("entry", "settlement", "starkKey"), "0x54321", "ACCOUNT_IDENTITY_MISMATCH"),
        (("entry", "settlement", "collateralPosition"), "7001004", "ACCOUNT_IDENTITY_MISMATCH"),
    ],
)
def test_preflight_requires_exact_market_account_and_stream_rest_agreement(
    lifecycle, loader_calls, wire, path, bad_value, reason
):
    changed = copy.deepcopy(wire)
    target = changed
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = [changed["filledOrder"]] if bad_value == "FILLED_ORDER" else bad_value
    with pytest.raises((ContractViolation, EvidenceViolation), match=reason):
        candidate = normalize_official_evidence(changed, now_ms=1_770_000_000_000)
        prepare_entry(lifecycle, candidate, changed)
    assert loader_calls == []
    assert lifecycle.store.count() == 0


def test_stale_or_stream_rest_disagreement_blocks(lifecycle, wire):
    stale = copy.deepcopy(wire)
    stale["book"]["eventTime"] -= 30_001
    with pytest.raises(EvidenceViolation, match="STALE"):
        normalize_official_evidence(stale, now_ms=1_770_000_000_000)

    disagree = copy.deepcopy(wire)
    disagree["stream"]["positions"] = [disagree["position"]]
    with pytest.raises(EvidenceViolation, match="STREAM_REST_DISAGREE"):
        normalize_official_evidence(disagree, now_ms=1_770_000_000_000)


def test_every_entry_and_close_is_bounded_to_usd_500(lifecycle, evidence, wire):
    assert MAX_NOTIONAL_USD == Decimal("500")
    with pytest.raises(EvidenceViolation, match="NOTIONAL_CAP"):
        prepare_entry(lifecycle, evidence, wire, qty=Decimal("0.013"))

    exposed = filled_evidence(wire)
    entry = claim_entry(lifecycle, evidence, wire)
    lifecycle.reconcile(entry.id, exposed)
    excessive = exposed.with_position(size=Decimal("0.013"), value=Decimal("520.13"))
    with pytest.raises(LifecycleHalted, match="NOTIONAL_CAP"):
        prepare_close(lifecycle, entry.id, excessive, wire)


def test_unresolved_claimed_place_is_never_rearmed_and_reconciles_only_from_evidence(
    lifecycle, evidence, wire
):
    intent = claim_entry(lifecycle, evidence, wire)
    restarted = reopen(lifecycle)
    unresolved = restarted.reconcile(intent.id, evidence)
    assert restarted.store.get(intent.id).state == "CLAIMED"
    assert unresolved.complete is False
    assert unresolved.next_write is None
    with pytest.raises(LifecycleHalted):
        restarted.claim_for_dispatch(intent.id, evidence=evidence)
    assert restarted.store.dispatch_count(intent.id) == 1


@pytest.mark.parametrize("kind", ["CANCEL", "MASS_CANCEL"])
def test_cancel_and_mass_cancel_are_durable_credential_free_one_shot(
    lifecycle, loader_calls, evidence, wire, kind
):
    entry = claim_entry(lifecycle, evidence, wire)
    action = lifecycle.prepare_cancellation(entry.id, kind=kind, evidence=evidence)
    assert lifecycle.store.get(action.id).state == "PREPARED"
    assert action.target_external_id == wire["entry"]["externalId"]
    if kind == "CANCEL":
        assert action.api_request == {
            "method": "DELETE",
            "path": "/user/order",
            "query": {"externalId": wire["entry"]["externalId"]},
            "json": None,
        }
    else:
        assert action.api_request == {
            "method": "POST",
            "path": "/user/order/massCancel",
            "query": {},
            "json": {
                "externalOrderIds": [wire["entry"]["externalId"]],
                "cancelAll": False,
            },
        }
    assert "headers" not in action.api_request
    assert wire["identity"]["apiKey"].encode() not in Path(lifecycle.store.path).read_bytes()
    assert loader_calls == []
    lifecycle.claim_for_dispatch(action.id, evidence=evidence)
    restarted = reopen(lifecycle)
    with pytest.raises(LifecycleHalted):
        restarted.claim_for_dispatch(action.id, evidence=evidence)
    assert restarted.store.dispatch_count(action.id) == 1
    assert wire["identity"]["apiKey"].encode() not in Path(lifecycle.store.path).read_bytes()


def test_claimed_exact_cancel_then_agreeing_filled_evidence_reconciles_both_lifecycles(
    lifecycle, evidence, wire
):
    entry = claim_entry(lifecycle, evidence, wire)
    cancel = lifecycle.prepare_cancellation(entry.id, kind="CANCEL", evidence=evidence)
    lifecycle.claim_for_dispatch(cancel.id, evidence=evidence)

    restarted = reopen(lifecycle)
    result = restarted.reconcile(cancel.id, filled_evidence(wire))

    durable = reopen(restarted).store.snapshot()
    assert durable.intent_states[cancel.id] == "RECONCILED_NO_CANCEL_EFFECT"
    assert durable.intent_states[entry.id] == "ENTRY_RECONCILED"
    assert durable.lifecycle_state == "EXPOSED"
    assert result.lifecycle_state == "EXPOSED"
    assert result.complete is False
    assert result.next_write is None
    assert restarted.store.dispatch_count(cancel.id) == 1
    with pytest.raises(LifecycleHalted):
        restarted.claim_for_dispatch(cancel.id, evidence=filled_evidence(wire))


def test_entry_fill_is_not_lifecycle_complete(lifecycle, evidence, wire):
    entry = claim_entry(lifecycle, evidence, wire)
    result = lifecycle.reconcile(entry.id, filled_evidence(wire))
    assert result.lifecycle_state == "EXPOSED"
    assert result.complete is False


@pytest.mark.parametrize(
    "overrides",
    [
        {},
        {"settlement_hash": 323456789012345678901234567890},
        {"nonce": 1_700_000_002},
    ],
)
def test_claimed_reconciled_identity_can_never_be_rearmed(
    lifecycle, evidence, wire, overrides
):
    entry = claim_entry(lifecycle, evidence, wire)
    lifecycle.reconcile(entry.id, filled_evidence(wire))
    before = lifecycle.store.snapshot()

    with pytest.raises((NonceViolation, IntentConflict)):
        prepare_entry(lifecycle, evidence, wire, **overrides)

    after = lifecycle.store.snapshot()
    assert after.intent_states == before.intent_states
    assert after.intent_states[entry.id] == "ENTRY_RECONCILED"
    assert lifecycle.store.dispatch_count(entry.id) == 1


def test_close_requires_dispatched_reconciled_entry_and_fresh_exact_position(lifecycle, evidence, wire):
    entry = prepare_entry(lifecycle, evidence, wire)
    with pytest.raises(LifecycleHalted):
        prepare_close(lifecycle, entry.id, filled_evidence(wire), wire)

    lifecycle.claim_for_dispatch(entry.id, evidence=evidence)
    exposed = filled_evidence(wire)
    lifecycle.reconcile(entry.id, exposed)
    stale = exposed.with_server_time(1_770_000_040_000, observed_at=1_770_000_040_000)
    with pytest.raises((EvidenceViolation, LifecycleHalted), match="STALE"):
        prepare_close(
            lifecycle,
            entry.id,
            stale,
            wire,
            nonce=1_700_000_003,
            settlement_hash=333,
        )

    close = prepare_close(lifecycle, entry.id, exposed, wire)
    assert close.kind == "CLOSE"
    assert close.side == "SELL"
    assert close.qty == Decimal("0.001")
    assert close.reduce_only is True
    assert close.nonce == 2**31 - 1
    assert close.external_id == wire["close"]["externalId"]
    assert close.unsigned_api_payload == build_limit_ioc_order(copy.deepcopy(wire["close"]))
    assert close.payload_digest == wire["vectors"]["closeCanonicalPayloadDigest"]
    durable_close = IntentStore(lifecycle.store.path).get(close.id)
    assert durable_close.state == "PREPARED"
    assert durable_close.unsigned_api_payload == close.unsigned_api_payload
    assert durable_close.payload_digest == close.payload_digest
    lifecycle.claim_for_dispatch(close.id, evidence=exposed)
    with pytest.raises(LifecycleHalted):
        reopen(lifecycle).claim_for_dispatch(close.id, evidence=exposed)


@pytest.mark.parametrize("server_offset_ms", [0, 1])
def test_prepared_close_at_or_after_its_expiry_can_never_be_claimed(
    lifecycle, evidence, wire, server_offset_ms
):
    entry = claim_entry(lifecycle, evidence, wire)
    exposed = filled_evidence(wire)
    lifecycle.reconcile(entry.id, exposed)
    close = prepare_close(lifecycle, entry.id, exposed, wire)
    server_time = close.expiry_ms + server_offset_ms
    at_expiry = exposed.with_server_time(server_time, observed_at=server_time)
    with pytest.raises(LifecycleHalted, match="EXPIRED"):
        lifecycle.claim_for_dispatch(close.id, evidence=at_expiry)
    assert lifecycle.store.get(close.id).state != "CLAIMED"
    assert lifecycle.next_write(at_expiry) is None


@pytest.mark.parametrize(
    "mutator",
    [
        lambda data: data["account"].update(openOrders=[data["filledOrder"]]),
        lambda data: data["filledOrder"].update(status="EXPIRED"),
        lambda data: data["account"].update(orderHistory=[]),
        lambda data: data["account"].update(fills=[]),
        lambda data: data["account"].update(positions=[]),
        lambda data: data["stream"].update(gapFree=False),
    ],
)
def test_contradictory_open_expired_status_history_fill_or_position_never_succeeds_or_writes(
    lifecycle, evidence, wire, mutator
):
    entry = claim_entry(lifecycle, evidence, wire)
    changed = copy.deepcopy(wire)
    changed["account"]["serverTime"] = 1_770_000_003_000
    changed["account"]["observedAt"] = 1_770_000_003_000
    changed["account"]["orderStatus"] = changed["filledOrder"]
    changed["account"]["orderHistory"] = [changed["filledOrder"]]
    changed["account"]["fills"] = [changed["fill"]]
    changed["account"]["positions"] = [changed["position"]]
    changed["stream"]["eventTime"] = 1_770_000_003_000
    changed["stream"]["receivedAt"] = 1_770_000_003_000
    changed["stream"]["orders"] = [changed["filledOrder"]]
    changed["stream"]["positions"] = [changed["position"]]
    changed["stream"]["trades"] = [changed["fill"]]
    mutator(changed)
    with pytest.raises((EvidenceViolation, LifecycleHalted)):
        contradictory = normalize_official_evidence(changed, now_ms=1_770_000_003_000)
        lifecycle.reconcile(entry.id, contradictory)
    assert lifecycle.complete is False
    assert lifecycle.next_write(evidence) is None


@pytest.mark.parametrize(
    ("fill_qty", "position_qty", "expected_state"),
    [
        ("0.0005", "0.0005", "HALTED_SUB_MINIMUM_RESIDUAL"),
        ("0.0007", "0.0007", "HALTED_OFF_GRID_RESIDUAL"),
    ],
)
def test_partial_fill_accounting_halts_subminimum_or_offgrid_residual(
    lifecycle, evidence, wire, fill_qty, position_qty, expected_state
):
    entry = claim_entry(lifecycle, evidence, wire)
    changed = copy.deepcopy(wire)
    changed["filledOrder"]["status"] = "PARTIALLY_FILLED"
    changed["filledOrder"]["filledQty"] = fill_qty
    changed["fill"]["qty"] = fill_qty
    changed["position"]["size"] = position_qty
    changed["position"]["value"] = str(Decimal(position_qty) * Decimal("40008"))
    partial = filled_evidence(changed)
    result = lifecycle.reconcile(entry.id, partial)
    assert result.filled_qty == Decimal(fill_qty)
    assert result.position_qty == Decimal(position_qty)
    assert result.lifecycle_state == expected_state
    assert result.next_write is None


def test_close_rejection_halts_without_another_write(lifecycle, evidence, wire):
    entry = claim_entry(lifecycle, evidence, wire)
    exposed = filled_evidence(wire)
    lifecycle.reconcile(entry.id, exposed)
    close = prepare_close(lifecycle, entry.id, exposed, wire)
    lifecycle.claim_for_dispatch(close.id, evidence=exposed)
    changed = copy.deepcopy(wire)
    rejected_close = copy.deepcopy(changed["filledCloseOrder"])
    rejected_close.update(status="REJECTED", statusReason="REDUCE_ONLY_FAILED", filledQty="0")
    now_ms = 1_770_000_006_000
    changed["account"]["serverTime"] = now_ms
    changed["account"]["observedAt"] = now_ms
    changed["account"]["orderStatus"] = rejected_close
    changed["account"]["orderHistory"] = [changed["filledOrder"], rejected_close]
    changed["account"]["fills"] = [changed["fill"]]
    changed["account"]["positions"] = [changed["position"]]
    changed["stream"]["eventTime"] = now_ms
    changed["stream"]["receivedAt"] = now_ms
    changed["stream"]["orders"] = [changed["filledOrder"], rejected_close]
    changed["stream"]["positions"] = [changed["position"]]
    changed["stream"]["trades"] = [changed["fill"]]
    rejected = normalize_official_evidence(changed, now_ms=now_ms)
    result = lifecycle.reconcile(close.id, rejected)
    assert result.lifecycle_state == "HALTED_CLOSE_REJECTED"
    assert result.next_write is None


def test_final_barrier_requires_expiry_gap_free_stream_rest_zero_orders_and_exact_flat(
    lifecycle, evidence, wire
):
    entry = claim_entry(lifecycle, evidence, wire)
    exposed = filled_evidence(wire)
    lifecycle.reconcile(entry.id, exposed)
    close = prepare_close(lifecycle, entry.id, exposed, wire)
    lifecycle.claim_for_dispatch(close.id, evidence=exposed)

    before_expiry = closed_evidence(wire, now_ms=close.expiry_ms)
    assert lifecycle.reconcile(close.id, before_expiry).complete is False
    final = closed_evidence(wire, now_ms=close.expiry_ms + 1)
    result = lifecycle.reconcile(close.id, final)
    assert result.lifecycle_state == "COMPLETE"
    assert result.complete is True
    assert result.next_write is None
    assert result.reconciled_external_ids == frozenset(
        {wire["entry"]["externalId"], wire["close"]["externalId"]}
    )
    assert result.reconciled_fill_ids == frozenset({wire["fill"]["id"], wire["closeFill"]["id"]})
    assert lifecycle.store.get(entry.id).state == "ENTRY_RECONCILED"
    assert lifecycle.store.get(close.id).state == "CLOSE_RECONCILED"


@pytest.mark.parametrize(
    "blocker",
    ["STREAM_GAP", "STALE_STREAM", "OPEN_ORDER", "NONZERO_POSITION", "MISSING_HISTORY", "MISSING_FILL"],
)
def test_each_final_barrier_component_fails_closed(lifecycle, evidence, wire, blocker):
    entry = claim_entry(lifecycle, evidence, wire)
    exposed = filled_evidence(wire)
    lifecycle.reconcile(entry.id, exposed)
    close = prepare_close(lifecycle, entry.id, exposed, wire)
    lifecycle.claim_for_dispatch(close.id, evidence=exposed)

    now_ms = close.expiry_ms + 1
    changed = copy.deepcopy(wire)
    changed["account"]["serverTime"] = now_ms
    changed["account"]["observedAt"] = now_ms
    changed["account"]["orderStatus"] = changed["filledCloseOrder"]
    changed["account"]["orderHistory"] = [changed["filledOrder"], changed["filledCloseOrder"]]
    changed["account"]["fills"] = [changed["fill"], changed["closeFill"]]
    changed["stream"]["eventTime"] = now_ms
    changed["stream"]["receivedAt"] = now_ms
    changed["stream"]["orders"] = [changed["filledOrder"], changed["filledCloseOrder"]]
    changed["stream"]["trades"] = [changed["fill"], changed["closeFill"]]
    if blocker == "STREAM_GAP":
        changed["stream"]["gapFree"] = False
    elif blocker == "STALE_STREAM":
        changed["stream"]["eventTime"] = now_ms - 30_001
        changed["stream"]["receivedAt"] = now_ms - 30_001
    elif blocker == "OPEN_ORDER":
        open_order = copy.deepcopy(changed["filledCloseOrder"])
        open_order.update(id=99999, externalId="99999", status="NEW", filledQty="0")
        changed["account"]["openOrders"] = [open_order]
        changed["stream"]["orders"].append(open_order)
    elif blocker == "NONZERO_POSITION":
        changed["account"]["positions"] = [changed["position"]]
        changed["stream"]["positions"] = [changed["position"]]
    elif blocker == "MISSING_HISTORY":
        changed["account"]["orderHistory"] = [changed["filledOrder"]]
    else:
        changed["account"]["fills"] = [changed["fill"]]
        changed["stream"]["trades"] = [changed["fill"]]

    with pytest.raises((EvidenceViolation, LifecycleHalted)):
        blocked = normalize_official_evidence(changed, now_ms=now_ms)
        lifecycle.reconcile(close.id, blocked)
    assert lifecycle.complete is False
    assert lifecycle.next_write(evidence) is None


def test_no_dispatch_or_state_mutation_shortcuts_are_exposed(lifecycle):
    forbidden_lifecycle = {
        "dispatch_once",
        "mark_dispatched",
        "mark_filled",
        "mark_terminal",
        "record_dispatch_result",
        "save",
        "set_state",
    }
    forbidden_store = {"mark_dispatched", "mark_filled", "mark_terminal", "save", "set_state"}
    assert all(not hasattr(lifecycle, name) for name in forbidden_lifecycle)
    assert all(not hasattr(lifecycle.store, name) for name in forbidden_store)


def test_secret_loader_is_uncalled_and_module_has_no_normal_cli_network_or_other_venue_surface(
    lifecycle, loader_calls, evidence, wire
):
    prepare_entry(lifecycle, evidence, wire)
    assert loader_calls == []

    module = importlib.import_module("risex_farmer.extended_testnet_lifecycle")
    source = Path(module.__file__).read_text()
    assert "aiohttp" not in source
    assert "websocket" not in source.lower()
    assert "click" not in source
    assert "argparse" not in source
    assert "risex_farmer.cli" not in source
    assert "risex_farmer.runtime" not in source
    assert "risex_farmer.scanner" not in source
    assert "risex_farmer.exchanges" not in source
    assert "testnet_risex" not in source
    assert "nado" not in source.lower()

    imported = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import risex_farmer.cli; "
            "assert 'risex_farmer.extended_testnet_lifecycle' not in sys.modules",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert imported.returncode == 0, imported.stderr
