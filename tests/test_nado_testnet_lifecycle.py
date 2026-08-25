from __future__ import annotations

import importlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from risex_farmer.nado_testnet_lifecycle import (
    ACTIVE_PERP, CANCEL_ALL, CLOSE, COMPLETE, ENTRY, HALTED, RUNNING,
    IOC_REDUCE_ONLY_APPENDIX, POST_ONLY_APPENDIX,
    AccountSnapshot, CatalogSnapshot, EngineEvidence, FillEvidence,
    FixedEnvironment, IntentStore, LifecycleCore, NadoContractError,
    OrderEvidence, OrderIntent, Product, Reconciliation,
    SyntheticOrderVector, TriggerSnapshot, build_order_nonce,
    completion_barrier, order_digest, product_verifier,
    sign_synthetic_order, unpack_order_nonce, validate_entry_preflight,
    verify_signed_validation, canonical_payload, encode_subaccount,
    SOURCE_PINS,
)

FIXTURE = Path(__file__).parent / "fixtures" / "nado_testnet_001" / "official_order_vector.json"
X18 = 10**18


@pytest.fixture
def vector() -> dict[str, object]:
    return json.loads(FIXTURE.read_text())


@pytest.fixture
def product() -> Product:
    return Product(2, "BTC-PERP", ACTIVE_PERP, True, 10 * X18, 10**15, 10**16, 5 * X18)


@pytest.fixture
def catalog(product: Product) -> CatalogSnapshot:
    return CatalogSnapshot((product,), True, 1_700_000_000_000, True, "engine")


@pytest.fixture
def flat_account(vector: dict[str, object]) -> AccountSnapshot:
    return AccountSnapshot(
        chain_id=763373,
        domain_name=FixedEnvironment.domain_name,
        domain_version=FixedEnvironment.domain_version,
        endpoint=FixedEnvironment.endpoint,
        gateway=FixedEnvironment.gateway,
        gateway_ws=FixedEnvironment.gateway_ws,
        archive=FixedEnvironment.archive,
        trigger=FixedEnvironment.trigger,
        owner=str(vector["owner"]), subaccount_name="default",
        observed_at_ms=1_700_000_000_000, fresh=True,
        authoritative_source="engine", regular_orders_by_product={2: ()},
        cross_perp_amounts_x18={2: 0}, isolated_positions=(),
        snapshot_id="engine-flat-1",
    )


@pytest.fixture
def zero_triggers(vector: dict[str, object]) -> TriggerSnapshot:
    return TriggerSnapshot(
        str(vector["owner"]), "default", 1_700_000_000_000, True,
        "trigger", (), snapshot_id="trigger-zero-1",
    )


def _entry_intent(vector: dict[str, object]) -> OrderIntent:
    order = SyntheticOrderVector.from_fixture(vector)
    return OrderIntent(
        ENTRY, order.product_id, order.nonce, order.recv_time, order_digest(order),
        canonical_payload(order.as_payload()), order.amount_x18, order.appendix,
        350 * X18, sender=order.sender.lower(), owner=order.owner.lower(),
        subaccount_name=order.subaccount_name,
    )


def _evidence(account: AccountSnapshot, triggers: TriggerSnapshot, observed: int) -> EngineEvidence:
    return EngineEvidence(
        replace(account, observed_at_ms=observed),
        replace(triggers, observed_at_ms=observed),
        (), (), observed,
    )


def _resting_entry(
    store: IntentStore,
    vector: dict[str, object],
    catalog: CatalogSnapshot,
    flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot,
) -> tuple[OrderIntent, AccountSnapshot, TriggerSnapshot]:
    entry = _entry_intent(vector)
    store.prepare(entry)
    observed = entry.recv_time + 1
    account = replace(
        flat_account,
        observed_at_ms=observed,
        regular_orders_by_product={2: (entry.digest,)},
        snapshot_id="entry-resting",
    )
    triggers = replace(zero_triggers, observed_at_ms=observed)
    order = OrderEvidence(entry.digest, 2, entry.nonce, entry.amount_x18, "OPEN")
    assert store.reconcile(
        entry.digest, catalog=catalog,
        evidence=replace(_evidence(account, triggers, observed), orders=(order,)),
    ) == Reconciliation.RESTING
    return entry, account, triggers


def _filled_entry(
    store: IntentStore,
    vector: dict[str, object],
    catalog: CatalogSnapshot,
    flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot,
    *,
    submission_idx: int,
) -> tuple[OrderIntent, FillEvidence, AccountSnapshot, TriggerSnapshot]:
    entry = _entry_intent(vector)
    store.prepare(entry)
    observed = entry.recv_time + 1
    account = replace(
        flat_account,
        observed_at_ms=observed,
        cross_perp_amounts_x18={2: entry.amount_x18},
        snapshot_id="entry-filled",
    )
    triggers = replace(zero_triggers, observed_at_ms=observed)
    fill = FillEvidence(entry.digest, 2, entry.amount_x18, submission_idx)
    assert store.reconcile(
        entry.digest, catalog=catalog,
        evidence=replace(_evidence(account, triggers, observed), fills=(fill,)),
    ) == Reconciliation.FILLED
    return entry, fill, account, triggers


def _other_identity(
    flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot,
) -> tuple[AccountSnapshot, TriggerSnapshot]:
    owner = "0x000000000000000000000000000000000000000b"
    return (
        replace(flat_account, owner=owner, snapshot_id="engine-other-owner"),
        replace(zero_triggers, owner=owner, snapshot_id="trigger-other-owner"),
    )


def test_module_is_not_imported_by_normal_package_startup() -> None:
    package = importlib.import_module("risex_farmer")
    assert "nado_testnet_lifecycle" not in Path(package.__file__).read_text()


def test_fixed_environment_product_verifier_and_nonce_vector(vector: dict[str, object]) -> None:
    assert SOURCE_PINS == vector["sources"]
    assert FixedEnvironment.as_dict() == vector["environment"]
    assert product_verifier(2) == vector["product_verifier"]
    nonce = build_order_nonce(int(vector["recv_time"]), 42)
    assert str(nonce) == vector["nonce"]
    assert unpack_order_nonce(nonce) == (int(vector["recv_time"]), 42)
    environment = FixedEnvironment.as_dict()
    for key, wrong in (
        ("chain_id", 763374),
        ("domain_name", "NADO"),
        ("domain_version", "0.0.2"),
        ("endpoint", "0x" + "00" * 20),
        ("gateway", "https://wrong.test/v1"),
        ("gateway_ws", "wss://wrong.test/v1/ws"),
        ("archive", "https://wrong.test/v1"),
        ("trigger", "https://wrong.test/v1"),
    ):
        with pytest.raises(NadoContractError):
            FixedEnvironment.assert_exact(**(environment | {key: wrong}))
    for recv_time, salt in ((-1, 0), (2**44, 0), (0, -1), (0, 2**20)):
        with pytest.raises(NadoContractError):
            build_order_nonce(recv_time, salt)


def test_preflight_rejects_every_environment_identity_mismatch(
    catalog: CatalogSnapshot, flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot,
) -> None:
    for field, wrong in (
        ("chain_id", 763374),
        ("domain_name", "NADO"),
        ("domain_version", "0.0.2"),
        ("endpoint", "0x" + "00" * 20),
        ("gateway", "https://wrong.test/v1"),
        ("gateway_ws", "wss://wrong.test/v1/ws"),
        ("archive", "https://wrong.test/v1"),
        ("trigger", "https://wrong.test/v1"),
    ):
        with pytest.raises(NadoContractError):
            validate_entry_preflight(
                catalog=catalog, account=replace(flat_account, **{field: wrong}),
                triggers=zero_triggers, product_id=2,
                entry_price_x18=35_000 * X18,
                worst_close_price_x18=36_000 * X18,
                now_ms=1_700_000_000_001,
            )


@pytest.mark.parametrize(
    "catalog_change",
    [
        {"observed_at_ms": 1_700_000_000_002},
        {"fresh": False},
        {"authoritative_source": "archive"},
    ],
)
def test_preflight_rejects_future_or_non_authoritative_catalog_observation(
    catalog: CatalogSnapshot, flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot, catalog_change: dict[str, object],
) -> None:
    now = 1_700_000_000_001
    with pytest.raises(NadoContractError):
        validate_entry_preflight(
            catalog=replace(catalog, **catalog_change),
            account=flat_account, triggers=zero_triggers, product_id=2,
            entry_price_x18=35_000 * X18,
            worst_close_price_x18=36_000 * X18, now_ms=now,
        )


def test_pinned_synthetic_digest_signature_and_signed_validation(vector: dict[str, object]) -> None:
    order = SyntheticOrderVector.from_fixture(vector)
    digest, signature = sign_synthetic_order(order, str(vector["synthetic_key"]))
    assert (digest, signature) == (vector["digest"], vector["signature"])
    assert verify_signed_validation(
        order, signature=signature, validation_product_id=order.product_id,
        validation_valid=True,
    )
    with pytest.raises(NadoContractError):
        verify_signed_validation(
            order, signature=signature, validation_product_id=order.product_id,
            validation_valid=False,
        )
    with pytest.raises(NadoContractError):
        sign_synthetic_order(order, "0x" + "02".zfill(64))


def test_validate_order_rejects_mismatched_returned_product_id(vector: dict[str, object]) -> None:
    order = SyntheticOrderVector.from_fixture(vector)
    _, signature = sign_synthetic_order(order, str(vector["synthetic_key"]))
    with pytest.raises(NadoContractError):
        verify_signed_validation(
            order,
            signature=signature,
            validation_product_id=order.product_id + 1,
            validation_valid=True,
        )


def test_pinned_vector_matches_independent_eip712_library(vector: dict[str, object]) -> None:
    from eth_account import Account
    from eth_account.messages import encode_typed_data

    typed = {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            "Order": [
                {"name": "sender", "type": "bytes32"},
                {"name": "priceX18", "type": "int128"},
                {"name": "amount", "type": "int128"},
                {"name": "expiration", "type": "uint64"},
                {"name": "nonce", "type": "uint64"},
                {"name": "appendix", "type": "uint128"},
            ],
        },
        "primaryType": "Order",
        "domain": {
            "name": "Nado", "version": "0.0.1", "chainId": 763373,
            "verifyingContract": str(vector["product_verifier"]),
        },
        "message": {
            "sender": str(vector["sender"]),
            "priceX18": int(vector["price_x18"]),
            "amount": int(vector["amount_x18"]),
            "expiration": int(vector["expiration"]),
            "nonce": int(vector["nonce"]),
            "appendix": int(vector["appendix"]),
        },
    }
    signed = Account.sign_message(
        encode_typed_data(full_message=typed), str(vector["synthetic_key"])
    )
    assert "0x" + signed.message_hash.hex() == vector["digest"]
    assert "0x" + signed.signature.hex() == vector["signature"]


def test_module_has_no_network_credential_or_cli_surface() -> None:
    source = Path(importlib.import_module(
        "risex_farmer.nado_testnet_lifecycle"
    ).__file__).read_text()
    for forbidden in (
        "import aiohttp", "import requests", "urllib", "import socket",
        "subprocess", "os.environ", "def main(", "credential_loader",
    ):
        assert forbidden not in source


def test_x18_grid_minimum_notional_and_post_only_preflight(
    catalog: CatalogSnapshot, flat_account: AccountSnapshot, zero_triggers: TriggerSnapshot,
) -> None:
    plan = validate_entry_preflight(
        catalog=catalog, account=flat_account, triggers=zero_triggers,
        product_id=2, entry_price_x18=35_000 * X18,
        worst_close_price_x18=36_000 * X18, now_ms=1_700_000_000_001,
    )
    assert (plan.amount_x18, plan.appendix) == (10**16, POST_ONLY_APPENDIX)
    assert (plan.entry_notional_x18, plan.close_notional_x18) == (350 * X18, 360 * X18)
    for entry_price in (35_000 * X18 + 1, 51_000 * X18):
        with pytest.raises(NadoContractError):
            validate_entry_preflight(
                catalog=catalog, account=flat_account, triggers=zero_triggers,
                product_id=2, entry_price_x18=entry_price,
                worst_close_price_x18=51_000 * X18, now_ms=1_700_000_000_001,
            )


@pytest.mark.parametrize(
    ("product_id", "minimum_amount_x18", "step_x18"),
    (
        (38, 100 * X18, 500 * X18),
        (40, 100 * X18, 200 * X18),
        (56, 100 * X18, 200 * X18),
    ),
)
def test_complete_catalog_accepts_observed_unrelated_minimum_step_shapes(
    product: Product,
    product_id: int,
    minimum_amount_x18: int,
    step_x18: int,
) -> None:
    observed = replace(
        product,
        product_id=product_id,
        symbol=f"PRODUCT-{product_id}",
        minimum_amount_x18=minimum_amount_x18,
        step_x18=step_x18,
    )
    assert CatalogSnapshot(
        (observed,), True, 1_700_000_000_000, True, "engine"
    ).by_id() == {product_id: observed}


def test_target_preflight_rejects_off_step_minimum_amount(
    product: Product,
    flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot,
) -> None:
    target = replace(product, minimum_amount_x18=100 * X18, step_x18=500 * X18)
    catalog = CatalogSnapshot((target,), True, 1_700_000_000_000, True, "engine")
    with pytest.raises(NadoContractError, match="minimum amount is off"):
        validate_entry_preflight(
            catalog=catalog,
            account=flat_account,
            triggers=zero_triggers,
            product_id=2,
            entry_price_x18=35_000 * X18,
            worst_close_price_x18=36_000 * X18,
            now_ms=1_700_000_000_001,
        )


def test_complete_dynamic_catalog_and_all_keys_are_mandatory(
    product: Product, flat_account: AccountSnapshot, zero_triggers: TriggerSnapshot,
) -> None:
    second = replace(product, product_id=4, symbol="ETH-PERP")
    for catalog in (
        CatalogSnapshot((), True, 1_700_000_000_000, True, "engine"),
        CatalogSnapshot((product,), False, 1_700_000_000_000, True, "engine"),
        CatalogSnapshot((product, second), True, 1_700_000_000_000, True, "engine"),
    ):
        with pytest.raises(NadoContractError):
            validate_entry_preflight(
                catalog=catalog, account=flat_account, triggers=zero_triggers,
                product_id=2, entry_price_x18=35_000 * X18,
                worst_close_price_x18=36_000 * X18, now_ms=1_700_000_000_001,
            )


@pytest.mark.parametrize("changed", [
    {"fresh": False}, {"authoritative_source": "archive"},
    {"regular_orders_by_product": {2: ("0xopen",)}},
    {"cross_perp_amounts_x18": {2: 1}}, {"isolated_positions": ("isolated",)},
    {"contradictions": ("position disagreement",)},
])
def test_preflight_rejects_nonflat_stale_or_contradictory_engine_state(
    catalog: CatalogSnapshot, flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot, changed: dict[str, object],
) -> None:
    with pytest.raises(NadoContractError):
        validate_entry_preflight(
            catalog=catalog, account=replace(flat_account, **changed), triggers=zero_triggers,
            product_id=2, entry_price_x18=35_000 * X18,
            worst_close_price_x18=36_000 * X18, now_ms=1_700_000_000_001,
        )


def test_trigger_zero_requires_separate_fresh_trigger_service_evidence(
    catalog: CatalogSnapshot, flat_account: AccountSnapshot, zero_triggers: TriggerSnapshot,
) -> None:
    for triggers in (
        replace(zero_triggers, fresh=False),
        replace(zero_triggers, authoritative_source="engine"),
        replace(zero_triggers, active_digests=("0xtrigger",)),
        replace(zero_triggers, contradictions=("trigger disagreement",)),
    ):
        with pytest.raises(NadoContractError):
            validate_entry_preflight(
                catalog=catalog, account=flat_account, triggers=triggers,
                product_id=2, entry_price_x18=35_000 * X18,
                worst_close_price_x18=36_000 * X18, now_ms=1_700_000_000_001,
            )


def test_place_intent_is_durable_before_fixture_dispatch_and_immutable(
    tmp_path: Path, vector: dict[str, object],
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    intent = _entry_intent(vector)
    def fixture_dispatch(prepared: OrderIntent) -> str:
        assert store.get(prepared.digest) == prepared
        assert store.state(prepared.digest) == "PREPARED"
        return "accepted"
    assert store.prepare_then_fixture_dispatch(intent, fixture_dispatch) == "accepted"
    with pytest.raises(NadoContractError):
        store.prepare(intent)
    with pytest.raises(NadoContractError):
        store.prepare(replace(intent, digest="0x" + "11" * 32))
    with pytest.raises(NadoContractError):
        store.replace_payload(intent.digest, b"changed")


def test_place_ambiguous_state_survives_restart_and_cannot_replay(
    tmp_path: Path, vector: dict[str, object],
) -> None:
    path = tmp_path / "intents.sqlite3"
    intent = _entry_intent(vector)
    store = IntentStore(path)
    def timeout(_: OrderIntent) -> str:
        raise TimeoutError("fixture timeout")
    with pytest.raises(TimeoutError):
        store.prepare_then_fixture_dispatch(intent, timeout)
    store.close()
    reopened = IntentStore(path)
    restored = reopened.get(intent.digest)
    assert (restored.sender, restored.owner, restored.subaccount_name) == (
        intent.sender, intent.owner, intent.subaccount_name
    )
    assert reopened.state(intent.digest) == "AMBIGUOUS"
    assert reopened.get(intent.digest) == intent
    with pytest.raises(NadoContractError):
        reopened.prepare_then_fixture_dispatch(intent, lambda _: "replay")


def test_halted_place_ambiguity_allows_only_one_exact_resting_safety_cancel(
    tmp_path: Path, vector: dict[str, object], product: Product,
    catalog: CatalogSnapshot, flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot,
) -> None:
    def timeout(_: OrderIntent) -> str:
        raise TimeoutError("fixture timeout")

    entry = _entry_intent(vector)
    store = IntentStore(tmp_path / "proved-resting.sqlite3")
    with pytest.raises(TimeoutError):
        store.prepare_then_fixture_dispatch(entry, timeout)
    observed = entry.recv_time + 1
    resting_account = replace(
        flat_account,
        regular_orders_by_product={2: (entry.digest,)},
        observed_at_ms=observed,
        snapshot_id="ambiguous-entry-now-resting",
    )
    resting_triggers = replace(zero_triggers, observed_at_ms=observed)
    resting = replace(
        _evidence(resting_account, resting_triggers, observed),
        orders=(
            OrderEvidence(
                entry.digest, 2, entry.nonce, entry.amount_x18, "OPEN"
            ),
        ),
    )
    assert store.reconcile(
        entry.digest, catalog=replace(catalog, observed_at_ms=observed),
        evidence=resting,
    ) == Reconciliation.RESTING
    assert LifecycleCore(store).status == HALTED
    cancel = LifecycleCore(store).prepare_cancel_all(
        catalog=replace(catalog, observed_at_ms=observed),
        account=resting_account, triggers=resting_triggers,
        sender=str(vector["sender"]), recv_time=entry.recv_time + 2,
        salt=918, now_ms=observed,
    )
    assert cancel.kind == CANCEL_ALL
    assert LifecycleCore(store).status == HALTED
    with pytest.raises(NadoContractError):
        LifecycleCore(store).prepare_cancel_all(
            catalog=replace(catalog, observed_at_ms=observed),
            account=resting_account, triggers=resting_triggers,
            sender=str(vector["sender"]), recv_time=entry.recv_time + 3,
            salt=919, now_ms=observed,
        )

    unresolved = IntentStore(tmp_path / "unproved-ambiguous.sqlite3")
    with pytest.raises(TimeoutError):
        unresolved.prepare_then_fixture_dispatch(entry, timeout)
    with pytest.raises(NadoContractError):
        LifecycleCore(unresolved).prepare_cancel_all(
            catalog=replace(catalog, observed_at_ms=observed),
            account=resting_account, triggers=resting_triggers,
            sender=str(vector["sender"]), recv_time=entry.recv_time + 2,
            salt=920, now_ms=observed,
        )
    with pytest.raises(NadoContractError):
        LifecycleCore(store).prepare_close(
            catalog=replace(catalog, observed_at_ms=entry.recv_time + 3),
            product=product,
            account=replace(
                flat_account, cross_perp_amounts_x18={2: entry.amount_x18},
                observed_at_ms=entry.recv_time + 3,
                snapshot_id="halted-cannot-close",
            ),
            triggers=replace(
                zero_triggers, observed_at_ms=entry.recv_time + 3
            ),
            worst_price_x18=36_000 * X18, recv_time=entry.recv_time + 4,
            salt=921, now_ms=entry.recv_time + 3,
        )


def test_lifecycle_prepare_entry_persists_exact_signed_validation(
    vector: dict[str, object], tmp_path: Path, catalog: CatalogSnapshot,
    flat_account: AccountSnapshot, zero_triggers: TriggerSnapshot,
) -> None:
    order = SyntheticOrderVector.from_fixture(vector)
    digest, signature = sign_synthetic_order(order, str(vector["synthetic_key"]))
    store = IntentStore(tmp_path / "intents.sqlite3")
    intent = LifecycleCore(store).prepare_entry(
        order=order, catalog=catalog, account=flat_account,
        triggers=zero_triggers, worst_close_price_x18=36_000 * X18,
        signature=signature, validation_product_id=order.product_id,
        validation_valid=True,
        now_ms=1_700_000_000_001,
    )
    assert store.get(digest) == intent and intent.kind == ENTRY


def test_prewrite_inner_snapshots_cannot_reconcile_or_complete(
    tmp_path: Path, vector: dict[str, object], catalog: CatalogSnapshot,
    flat_account: AccountSnapshot, zero_triggers: TriggerSnapshot,
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    intent = _entry_intent(vector)
    store.prepare(intent)
    evidence = replace(
        EngineEvidence(
            flat_account, zero_triggers, (), (), intent.recv_time + 1
        ),
        terminal_digest=intent.digest, terminal_status="CANCELLED",
    )
    assert flat_account.observed_at_ms < intent.recv_time
    assert zero_triggers.observed_at_ms < intent.recv_time
    assert store.reconcile(
        intent.digest, catalog=catalog, evidence=evidence
    ) == Reconciliation.CONTRADICTORY
    assert not completion_barrier(
        store=store, catalog=catalog, evidence=evidence,
        now_ms=intent.recv_time + 1,
    )


def test_prewrite_inner_snapshots_cannot_authorize_close(
    tmp_path: Path, vector: dict[str, object], product: Product,
    catalog: CatalogSnapshot, flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot,
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    entry, _, _, _ = _filled_entry(
        store, vector, catalog, flat_account, zero_triggers, submission_idx=919
    )
    now = entry.recv_time + 1
    prewrite_account = replace(
        flat_account,
        cross_perp_amounts_x18={2: entry.amount_x18},
        observed_at_ms=entry.recv_time - 90_000,
        snapshot_id="prewrite-position",
    )
    prewrite_triggers = replace(
        zero_triggers, observed_at_ms=entry.recv_time - 90_000,
        snapshot_id="prewrite-triggers",
    )
    with pytest.raises(NadoContractError):
        LifecycleCore(store).prepare_close(
            catalog=replace(catalog, observed_at_ms=now), product=product,
            account=prewrite_account, triggers=prewrite_triggers,
            worst_price_x18=36_000 * X18, recv_time=now + 1,
            salt=919, now_ms=now,
        )
    assert store.count_kind(CLOSE) == 0
    assert LifecycleCore(store).status == HALTED


def test_public_store_has_no_unchecked_lifecycle_transition_methods(
    tmp_path: Path,
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    for name in (
        "mark_complete", "mark_reconciled", "mark_rejected", "mark_ambiguous"
    ):
        assert not hasattr(store, name)
    assert LifecycleCore(store).status != COMPLETE


def test_entry_rejects_signed_owner_a_with_owner_b_preflight(
    vector: dict[str, object], tmp_path: Path, catalog: CatalogSnapshot,
    flat_account: AccountSnapshot, zero_triggers: TriggerSnapshot,
) -> None:
    order = SyntheticOrderVector.from_fixture(vector)
    digest, signature = sign_synthetic_order(order, str(vector["synthetic_key"]))
    other_account, other_triggers = _other_identity(flat_account, zero_triggers)
    with pytest.raises(NadoContractError):
        LifecycleCore(IntentStore(tmp_path / "intents.sqlite3")).prepare_entry(
            order=order, catalog=catalog, account=other_account,
            triggers=other_triggers, worst_close_price_x18=36_000 * X18,
            signature=signature, validation_product_id=order.product_id,
            validation_valid=True,
            now_ms=1_700_000_000_001,
        )


def test_entry_reconciliation_resting_and_full_fill_are_exact(
    tmp_path: Path, vector: dict[str, object], catalog: CatalogSnapshot,
    flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot,
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    intent = _entry_intent(vector)
    store.prepare(intent)
    observed = intent.recv_time + 1
    base = _evidence(
        replace(
            flat_account, regular_orders_by_product={2: (intent.digest,)}
        ),
        zero_triggers, observed,
    )
    open_order = OrderEvidence(intent.digest, 2, intent.nonce, 10**16, "OPEN")
    assert store.reconcile(intent.digest, catalog=catalog, evidence=replace(base, orders=(open_order,))) == Reconciliation.RESTING

    full_store = IntentStore(tmp_path / "full.sqlite3")
    full_store.prepare(intent)
    full = FillEvidence(intent.digest, 2, 10**16, 2)
    full_evidence = _evidence(
        replace(flat_account, cross_perp_amounts_x18={2: 10**16}),
        zero_triggers, observed,
    )
    assert full_store.reconcile(
        intent.digest, catalog=catalog,
        evidence=replace(full_evidence, fills=(full,)),
    ) == Reconciliation.FILLED


def test_repeated_fill_identity_with_contradictory_payload_is_rejected(
    tmp_path: Path, vector: dict[str, object], catalog: CatalogSnapshot,
    flat_account: AccountSnapshot, zero_triggers: TriggerSnapshot,
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    intent = _entry_intent(vector)
    store.prepare(intent)
    first_fill = FillEvidence(
        intent.digest, 2, 4 * 10**15, submission_idx=900
    )
    first = replace(
        _evidence(
            replace(flat_account, cross_perp_amounts_x18={2: 4 * 10**15}),
            zero_triggers, intent.recv_time + 1,
        ),
        fills=(first_fill,),
    )
    assert store.reconcile(
        intent.digest, catalog=catalog, evidence=first
    ) == Reconciliation.PARTIAL
    contradictory = replace(
        first,
        account=replace(flat_account, cross_perp_amounts_x18={2: 5 * 10**15}),
        fills=(replace(first_fill, amount_x18=5 * 10**15),),
    )
    assert store.reconcile(
        intent.digest, catalog=catalog, evidence=contradictory
    ) == Reconciliation.CONTRADICTORY
    assert store.state(intent.digest) == "PARTIAL"


def test_unknown_foreign_fill_cannot_reconcile_or_allow_write(
    tmp_path: Path, vector: dict[str, object], product: Product,
    flat_account: AccountSnapshot, zero_triggers: TriggerSnapshot,
) -> None:
    second = replace(product, product_id=4, symbol="ETH-PERP")
    intent = _entry_intent(vector)
    catalog = CatalogSnapshot(
        (product, second), True, intent.recv_time + 1, True, "engine"
    )
    store = IntentStore(tmp_path / "intents.sqlite3")
    store.prepare(intent)
    account = replace(
        flat_account,
        regular_orders_by_product={2: (), 4: ()},
        cross_perp_amounts_x18={2: intent.amount_x18, 4: 0},
        observed_at_ms=intent.recv_time + 1,
        snapshot_id="two-product-filled",
    )
    triggers = replace(zero_triggers, observed_at_ms=intent.recv_time + 1)
    evidence = replace(
        _evidence(account, triggers, intent.recv_time + 1),
        fills=(
            FillEvidence(intent.digest, 2, intent.amount_x18, 920),
            FillEvidence("0x" + "44" * 32, 4, 10**15, 921),
        ),
    )
    assert store.reconcile(
        intent.digest, catalog=catalog, evidence=evidence
    ) == Reconciliation.CONTRADICTORY
    assert LifecycleCore(store).status == HALTED
    assert not store.write_allowed(
        intent.digest, catalog=catalog, now_ms=intent.recv_time + 1,
        evidence=evidence,
    )


def test_foreign_open_order_disagreeing_with_zero_account_map_halts(
    tmp_path: Path, vector: dict[str, object], catalog: CatalogSnapshot,
    flat_account: AccountSnapshot, zero_triggers: TriggerSnapshot,
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    entry, entry_fill, _, _ = _filled_entry(
        store, vector, catalog, flat_account, zero_triggers, submission_idx=924
    )
    observed = entry.recv_time + 2
    contradictory = replace(
        _evidence(
            replace(flat_account, observed_at_ms=observed),
            replace(zero_triggers, observed_at_ms=observed), observed,
        ),
        orders=(
            OrderEvidence(
                "0x" + "55" * 32, 2, entry.nonce, entry.amount_x18, "OPEN"
            ),
        ),
        fills=(entry_fill,),
    )
    assert store.reconcile(
        entry.digest, catalog=replace(catalog, observed_at_ms=observed),
        evidence=contradictory,
    ) == Reconciliation.CONTRADICTORY
    assert LifecycleCore(store).status == HALTED
    assert not store.write_allowed(
        entry.digest, catalog=replace(catalog, observed_at_ms=observed),
        now_ms=observed, evidence=contradictory,
    )


def test_open_entry_requires_exact_signed_unfilled_arithmetic(
    tmp_path: Path, vector: dict[str, object], catalog: CatalogSnapshot,
    flat_account: AccountSnapshot, zero_triggers: TriggerSnapshot,
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    intent = _entry_intent(vector)
    store.prepare(intent)
    fill_amount = 4 * 10**15
    observed = intent.recv_time + 1
    evidence = replace(
        _evidence(
            replace(
                flat_account, cross_perp_amounts_x18={2: fill_amount},
                regular_orders_by_product={2: (intent.digest,)},
                observed_at_ms=observed,
            ),
            replace(zero_triggers, observed_at_ms=observed), observed,
        ),
        orders=(
            OrderEvidence(intent.digest, 2, intent.nonce, 5 * 10**15, "OPEN"),
        ),
        fills=(FillEvidence(intent.digest, 2, fill_amount, 922),),
    )
    assert store.reconcile(
        intent.digest, catalog=replace(catalog, observed_at_ms=observed),
        evidence=evidence,
    ) == Reconciliation.CONTRADICTORY
    assert LifecycleCore(store).status == HALTED


def test_ioc_reduce_only_close_can_never_reconcile_as_resting(
    tmp_path: Path, vector: dict[str, object], product: Product,
    catalog: CatalogSnapshot, flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot,
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    entry, _, account, triggers = _filled_entry(
        store, vector, catalog, flat_account, zero_triggers, submission_idx=923
    )
    close = LifecycleCore(store).prepare_close(
        catalog=replace(catalog, observed_at_ms=entry.recv_time + 1),
        product=product, account=account, triggers=triggers,
        worst_price_x18=36_000 * X18, recv_time=entry.recv_time + 2,
        salt=49, now_ms=entry.recv_time + 1,
    )
    observed = entry.recv_time + 3
    evidence = replace(
        _evidence(
            replace(account, observed_at_ms=observed, snapshot_id="resting-close"),
            replace(zero_triggers, observed_at_ms=observed), observed,
        ),
        orders=(
            OrderEvidence(
                close.digest, 2, close.nonce, close.amount_x18, "OPEN"
            ),
        ),
    )
    assert store.reconcile(
        close.digest, catalog=replace(catalog, observed_at_ms=observed),
        evidence=evidence,
    ) == Reconciliation.CONTRADICTORY
    assert LifecycleCore(store).status == HALTED


def test_close_rejects_off_step_clamped_target_minimum_before_prepare(
    tmp_path: Path, vector: dict[str, object], product: Product,
    catalog: CatalogSnapshot, flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot,
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    entry, _, account, triggers = _filled_entry(
        store, vector, catalog, flat_account, zero_triggers, submission_idx=924
    )
    target = replace(product, minimum_amount_x18=12 * 10**15, step_x18=5 * 10**15)
    with pytest.raises(NadoContractError, match="close amount is off"):
        LifecycleCore(store).prepare_close(
            catalog=CatalogSnapshot(
                (target,), True, entry.recv_time + 1, True, "engine"
            ),
            product=target,
            account=account,
            triggers=triggers,
            worst_price_x18=36_000 * X18,
            recv_time=entry.recv_time + 2,
            salt=50,
            now_ms=entry.recv_time + 1,
        )
    assert store.intents() == ((entry, "RECONCILED"),)


@pytest.mark.parametrize("terminal", ["CANCELLED", "EXPIRED", "REJECTED"])
def test_exact_engine_terminal_history_is_digest_scoped(
    tmp_path: Path, vector: dict[str, object], catalog: CatalogSnapshot,
    flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot, terminal: str,
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    intent = _entry_intent(vector)
    store.prepare(intent)
    evidence = replace(
        _evidence(flat_account, zero_triggers, intent.recv_time + 1),
        terminal_digest=intent.digest,
        terminal_status=terminal,
    )
    assert store.reconcile(intent.digest, catalog=catalog, evidence=evidence) == Reconciliation(terminal)


def test_cancelled_or_expired_unmatched_entry_is_reconciled_terminal_no_fill(
    tmp_path: Path, vector: dict[str, object], catalog: CatalogSnapshot,
    flat_account: AccountSnapshot, zero_triggers: TriggerSnapshot,
) -> None:
    for terminal in ("CANCELLED", "EXPIRED"):
        store = IntentStore(tmp_path / f"{terminal}.sqlite3")
        intent = _entry_intent(vector)
        store.prepare(intent)
        now = intent.recv_time + 1
        evidence = replace(
            _evidence(flat_account, zero_triggers, now),
            terminal_digest=intent.digest,
            terminal_status=terminal,
        )
        assert store.reconcile(
            intent.digest, catalog=catalog, evidence=evidence
        ) == Reconciliation(terminal)
        assert store.state(intent.digest) == "RECONCILED"
        final_catalog = replace(catalog, observed_at_ms=now)
        assert completion_barrier(
            store=store, catalog=final_catalog, evidence=evidence, now_ms=now
        )


def test_second_entry_is_rejected_after_terminal_cancel(
    tmp_path: Path, vector: dict[str, object], catalog: CatalogSnapshot,
    flat_account: AccountSnapshot, zero_triggers: TriggerSnapshot,
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    first = _entry_intent(vector)
    store.prepare(first)
    evidence = replace(
        _evidence(flat_account, zero_triggers, first.recv_time + 1),
        terminal_digest=first.digest,
        terminal_status="CANCELLED",
    )
    assert store.reconcile(
        first.digest, catalog=catalog, evidence=evidence
    ) == Reconciliation.CANCELLED
    original = SyntheticOrderVector.from_fixture(vector)
    recv_time = original.recv_time + 2
    second = replace(
        original,
        recv_time=recv_time,
        salt=original.salt + 1,
        nonce=build_order_nonce(recv_time, original.salt + 1),
    )
    _, signature = sign_synthetic_order(second, str(vector["synthetic_key"]))
    with pytest.raises(NadoContractError):
        LifecycleCore(store).prepare_entry(
            order=second, catalog=catalog, account=flat_account,
            triggers=zero_triggers, worst_close_price_x18=36_000 * X18,
            signature=signature, validation_product_id=second.product_id,
            validation_valid=True, now_ms=first.recv_time + 1,
        )


def test_rejected_entry_is_non_success_and_cannot_start_another_entry(
    tmp_path: Path, vector: dict[str, object], catalog: CatalogSnapshot,
    flat_account: AccountSnapshot, zero_triggers: TriggerSnapshot,
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    first = _entry_intent(vector)
    store.prepare(first)
    now = first.recv_time + 1
    evidence = replace(
        _evidence(flat_account, zero_triggers, now),
        exact_rejection_digest=first.digest,
    )
    assert store.reconcile(
        first.digest, catalog=catalog, evidence=evidence
    ) == Reconciliation.REJECTED
    store.close()
    store = IntentStore(tmp_path / "intents.sqlite3")
    assert LifecycleCore(store).status == HALTED
    assert not completion_barrier(
        store=store, catalog=catalog, evidence=evidence, now_ms=now
    )
    original = SyntheticOrderVector.from_fixture(vector)
    recv_time = original.recv_time + 2
    second = replace(
        original,
        recv_time=recv_time,
        salt=original.salt + 1,
        nonce=build_order_nonce(recv_time, original.salt + 1),
    )
    _, signature = sign_synthetic_order(second, str(vector["synthetic_key"]))
    with pytest.raises(NadoContractError):
        LifecycleCore(store).prepare_entry(
            order=second, catalog=catalog, account=flat_account,
            triggers=zero_triggers, worst_close_price_x18=36_000 * X18,
            signature=signature, validation_product_id=second.product_id,
            validation_valid=True, now_ms=now,
        )


def test_contradiction_never_reconciles_or_allows_write(
    tmp_path: Path, vector: dict[str, object], catalog: CatalogSnapshot,
    flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot,
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    intent = _entry_intent(vector)
    store.prepare(intent)
    evidence = _evidence(replace(flat_account, contradictions=("conflict",)), zero_triggers, 1_700_000_000_001)
    assert store.reconcile(intent.digest, catalog=catalog, evidence=evidence) == Reconciliation.CONTRADICTORY
    assert not store.write_allowed(intent.digest, catalog=catalog, now_ms=intent.recv_time + 1, evidence=evidence)


def test_other_owner_or_incomplete_catalog_evidence_cannot_reconcile_or_allow_write(
    tmp_path: Path, vector: dict[str, object], catalog: CatalogSnapshot,
    flat_account: AccountSnapshot, zero_triggers: TriggerSnapshot,
) -> None:
    for account, triggers in (
        _other_identity(flat_account, zero_triggers),
        (
            replace(flat_account, regular_orders_by_product={}, cross_perp_amounts_x18={}),
            zero_triggers,
        ),
    ):
        path = tmp_path / f"{account.snapshot_id}.sqlite3"
        store = IntentStore(path)
        intent = _entry_intent(vector)
        store.prepare(intent)
        evidence = replace(
            _evidence(account, triggers, intent.recv_time + 1),
            exact_rejection_digest=intent.digest,
        )
        assert store.reconcile(
            intent.digest, catalog=catalog, evidence=evidence
        ) == Reconciliation.CONTRADICTORY
        assert store.state(intent.digest) == "PREPARED"
        assert not store.write_allowed(
            intent.digest,
            catalog=catalog,
            now_ms=intent.recv_time + 1,
            evidence=evidence,
        )


def test_cross_intent_owner_swap_is_rejected_and_identity_survives_restart(
    tmp_path: Path, vector: dict[str, object]
) -> None:
    path = tmp_path / "intents.sqlite3"
    store = IntentStore(path)
    first = _entry_intent(vector)
    store.prepare(first)
    store.close()
    reopened = IntentStore(path)
    restored = reopened.get(first.digest)
    assert (restored.sender, restored.owner, restored.subaccount_name) == (
        first.sender, first.owner, first.subaccount_name
    )
    order = SyntheticOrderVector.from_fixture(vector)
    owner = "0x000000000000000000000000000000000000000b"
    recv_time = order.recv_time + 1
    swapped = replace(
        order,
        owner=owner,
        sender=encode_subaccount(owner, order.subaccount_name),
        recv_time=recv_time,
        salt=order.salt + 1,
        nonce=build_order_nonce(recv_time, order.salt + 1),
    )
    swapped_intent = OrderIntent(
        ENTRY, swapped.product_id, swapped.nonce, swapped.recv_time,
        order_digest(swapped), canonical_payload(swapped.as_payload()),
        swapped.amount_x18, swapped.appendix, 350 * X18,
        sender=swapped.sender, owner=swapped.owner,
        subaccount_name=swapped.subaccount_name,
    )
    with pytest.raises(NadoContractError):
        reopened.prepare(swapped_intent)


def test_archive_or_indexer_identity_is_audit_only_and_cannot_resolve_ambiguity(
    tmp_path: Path, vector: dict[str, object], catalog: CatalogSnapshot,
    flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot,
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    intent = _entry_intent(vector)
    store.prepare(intent)
    evidence = replace(
        _evidence(flat_account, zero_triggers, intent.recv_time + 1),
        archive_digests=(intent.digest,),
    )
    assert store.reconcile(intent.digest, catalog=catalog, evidence=evidence) == Reconciliation.AMBIGUOUS
    assert store.state(intent.digest) == "AMBIGUOUS"
    store.close()
    assert LifecycleCore(IntentStore(tmp_path / "intents.sqlite3")).status == HALTED


def test_cancel_all_is_durable_empty_products_ambiguous_and_not_replayed(
    tmp_path: Path, vector: dict[str, object], catalog: CatalogSnapshot,
    flat_account: AccountSnapshot, zero_triggers: TriggerSnapshot,
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    entry, account, triggers = _resting_entry(
        store, vector, catalog, flat_account, zero_triggers
    )
    recv = entry.recv_time + 2
    write_catalog = replace(catalog, observed_at_ms=entry.recv_time + 1)
    with pytest.raises(NadoContractError):
        LifecycleCore(store).prepare_cancel_all(
            catalog=write_catalog,
            account=replace(account, regular_orders_by_product={2: ("0xother",)}),
            triggers=triggers, sender=str(vector["sender"]), recv_time=recv,
            salt=42, now_ms=entry.recv_time + 1,
        )
    assert store.count_kind(CANCEL_ALL) == 0
    intent = LifecycleCore(store).prepare_cancel_all(
        catalog=write_catalog, account=account, triggers=triggers,
        sender=str(vector["sender"]), recv_time=recv, salt=43,
        now_ms=entry.recv_time + 1,
    )
    assert intent.kind == CANCEL_ALL
    assert json.loads(intent.payload)["cancel_product_orders"]["tx"]["productIds"] == []
    ambiguous_time = intent.recv_time + 1
    assert store.reconcile(
        intent.digest,
        catalog=replace(catalog, observed_at_ms=ambiguous_time),
        evidence=replace(
            _evidence(account, triggers, ambiguous_time),
            orders=(
                OrderEvidence(
                    entry.digest, 2, entry.nonce, entry.amount_x18, "OPEN"
                ),
            ),
        ),
    ) == Reconciliation.AMBIGUOUS
    with pytest.raises(NadoContractError):
        store.prepare(intent)


def test_cancel_ambiguity_reconciles_only_after_recv_time_and_zero_orders(
    tmp_path: Path, vector: dict[str, object], catalog: CatalogSnapshot,
    flat_account: AccountSnapshot, zero_triggers: TriggerSnapshot,
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    entry, account, triggers = _resting_entry(
        store, vector, catalog, flat_account, zero_triggers
    )
    recv = entry.recv_time + 2
    write_catalog = replace(catalog, observed_at_ms=entry.recv_time + 1)
    intent = LifecycleCore(store).prepare_cancel_all(
        catalog=write_catalog, account=account, triggers=triggers,
        sender=str(vector["sender"]), recv_time=recv, salt=44,
        now_ms=entry.recv_time + 1,
    )
    after = _evidence(flat_account, zero_triggers, recv + 1)
    assert store.reconcile(
        intent.digest,
        catalog=replace(catalog, observed_at_ms=recv + 1),
        evidence=after,
    ) == Reconciliation.CANCELLED


def test_reconciled_cancel_cannot_bypass_partial_entry_manual_gate(
    tmp_path: Path, vector: dict[str, object], product: Product,
    catalog: CatalogSnapshot, flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot,
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    entry = _entry_intent(vector)
    store.prepare(entry)
    partial_amount = 4 * 10**15
    partial_fill = FillEvidence(entry.digest, 2, partial_amount, 32)
    observed = entry.recv_time + 1
    partial_account = replace(
        flat_account,
        cross_perp_amounts_x18={2: partial_amount},
        regular_orders_by_product={2: (entry.digest,)},
        observed_at_ms=observed,
        snapshot_id="partial-entry-with-resting-order",
    )
    partial_triggers = replace(zero_triggers, observed_at_ms=observed)
    assert store.reconcile(
        entry.digest, catalog=catalog,
        evidence=replace(
            _evidence(partial_account, partial_triggers, observed),
            fills=(partial_fill,),
        ),
    ) == Reconciliation.CONTRADICTORY
    assert LifecycleCore(store).status == HALTED
    with pytest.raises(NadoContractError):
        LifecycleCore(store).prepare_cancel_all(
            catalog=replace(catalog, observed_at_ms=observed),
            account=partial_account, triggers=partial_triggers,
            sender=str(vector["sender"]), recv_time=entry.recv_time + 2,
            salt=45, now_ms=observed,
        )
    assert store.count_kind(CANCEL_ALL) == 0


def test_partial_resting_entry_cancel_terminal_close_reaches_exact_flat(
    tmp_path: Path, vector: dict[str, object], product: Product,
    catalog: CatalogSnapshot, flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot,
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    entry = _entry_intent(vector)
    store.prepare(entry)
    partial_amount = 4 * 10**15
    entry_fill = FillEvidence(entry.digest, 2, partial_amount, 33)
    partial_time = entry.recv_time + 1
    partial_account = replace(
        flat_account,
        cross_perp_amounts_x18={2: partial_amount},
        regular_orders_by_product={2: (entry.digest,)},
        observed_at_ms=partial_time, snapshot_id="partial-entry-running",
    )
    partial_triggers = replace(zero_triggers, observed_at_ms=partial_time)
    partial_evidence = replace(
        _evidence(partial_account, partial_triggers, partial_time),
        orders=(
            OrderEvidence(
                entry.digest, 2, entry.nonce,
                entry.amount_x18 - partial_amount, "OPEN",
            ),
        ),
        fills=(entry_fill,),
    )
    assert store.reconcile(
        entry.digest, catalog=replace(catalog, observed_at_ms=partial_time),
        evidence=partial_evidence,
    ) == Reconciliation.PARTIAL
    assert LifecycleCore(store).status == RUNNING

    cancel = LifecycleCore(store).prepare_cancel_all(
        catalog=replace(catalog, observed_at_ms=partial_time),
        account=partial_account, triggers=partial_triggers,
        sender=str(vector["sender"]), recv_time=entry.recv_time + 2,
        salt=47, now_ms=partial_time,
    )
    terminal_time = entry.recv_time + 3
    terminal_account = replace(
        partial_account, regular_orders_by_product={2: ()},
        observed_at_ms=terminal_time, snapshot_id="partial-entry-terminal",
    )
    terminal_triggers = replace(zero_triggers, observed_at_ms=terminal_time)
    terminal_catalog = replace(catalog, observed_at_ms=terminal_time)
    cancel_evidence = replace(
        _evidence(terminal_account, terminal_triggers, terminal_time),
        fills=(entry_fill,), exact_cancel_digest=cancel.digest,
    )
    assert store.reconcile(
        cancel.digest, catalog=terminal_catalog, evidence=cancel_evidence
    ) == Reconciliation.CANCELLED
    entry_terminal = replace(
        cancel_evidence, terminal_digest=entry.digest,
        terminal_status="CANCELLED",
    )
    assert store.reconcile(
        entry.digest, catalog=terminal_catalog, evidence=entry_terminal
    ) == Reconciliation.CANCELLED
    assert LifecycleCore(store).status == RUNNING

    close = LifecycleCore(store).prepare_close(
        catalog=terminal_catalog, product=product, account=terminal_account,
        triggers=terminal_triggers, worst_price_x18=36_000 * X18,
        recv_time=entry.recv_time + 4, salt=48, now_ms=terminal_time,
    )
    assert (close.amount_x18, close.appendix, close.clamp_expected) == (
        -10**16, IOC_REDUCE_ONLY_APPENDIX, True
    )
    final_time = entry.recv_time + 5
    close_fill = FillEvidence(close.digest, 2, -partial_amount, 34)
    final_catalog = replace(catalog, observed_at_ms=final_time)
    final = replace(
        _evidence(
            replace(
                flat_account, observed_at_ms=final_time,
                snapshot_id="partial-entry-exact-flat",
            ),
            replace(zero_triggers, observed_at_ms=final_time), final_time,
        ),
        fills=(entry_fill, close_fill),
    )
    assert store.reconcile(
        close.digest, catalog=final_catalog, evidence=final
    ) == Reconciliation.FILLED
    assert completion_barrier(
        store=store, catalog=final_catalog, evidence=final, now_ms=final_time
    )


def test_close_fill_reconciles_from_recorded_starting_position_to_exact_zero(
    tmp_path: Path, vector: dict[str, object], product: Product,
    catalog: CatalogSnapshot, flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot,
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    entry, entry_fill, account, triggers = _filled_entry(
        store, vector, catalog, flat_account, zero_triggers, submission_idx=30
    )
    recv = entry.recv_time + 2
    close = LifecycleCore(store).prepare_close(
        catalog=replace(catalog, observed_at_ms=entry.recv_time + 1),
        product=product, account=account, triggers=triggers,
        worst_price_x18=36_000 * X18, recv_time=recv, salt=150,
        now_ms=entry.recv_time + 1,
    )
    evidence = _evidence(
        replace(flat_account, observed_at_ms=recv + 1, snapshot_id="engine-flat-after-close"),
        replace(zero_triggers, observed_at_ms=recv + 1),
        recv + 1,
    )
    fill = FillEvidence(close.digest, 2, -10**16, 31)
    assert store.reconcile(
        close.digest, catalog=catalog,
        evidence=replace(evidence, fills=(entry_fill, fill)),
    ) == Reconciliation.FILLED


@pytest.mark.parametrize("first_outcome", ["PARTIAL", "REJECTED"])
def test_partial_or_rejected_close_is_durable_manual_gate(
    tmp_path: Path, vector: dict[str, object], product: Product,
    catalog: CatalogSnapshot, flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot, first_outcome: str,
) -> None:
    store = IntentStore(tmp_path / f"{first_outcome}.sqlite3")
    entry = _entry_intent(vector)
    store.prepare(entry)
    entry_fill = FillEvidence(entry.digest, 2, 10**16, 6)
    position = replace(
        flat_account, cross_perp_amounts_x18={2: 10**16},
        observed_at_ms=entry.recv_time + 1, snapshot_id="entry-filled",
    )
    assert store.reconcile(
        entry.digest, catalog=catalog,
        evidence=replace(
            _evidence(
                position,
                replace(zero_triggers, observed_at_ms=entry.recv_time + 1),
                entry.recv_time + 1,
            ),
            fills=(entry_fill,),
        ),
    ) == Reconciliation.FILLED
    core = LifecycleCore(store)
    first = core.prepare_close(
        catalog=replace(catalog, observed_at_ms=entry.recv_time + 1),
        product=product, account=position,
        triggers=replace(zero_triggers, observed_at_ms=entry.recv_time + 1),
        worst_price_x18=36_000 * X18, recv_time=entry.recv_time + 2,
        salt=610, now_ms=entry.recv_time + 1,
    )
    first_now = entry.recv_time + 3
    if first_outcome == "PARTIAL":
        first_fill = FillEvidence(first.digest, 2, -4 * 10**15, 7)
        residual = 6 * 10**15
        first_evidence = replace(
            _evidence(
                replace(
                    flat_account, cross_perp_amounts_x18={2: residual},
                    observed_at_ms=first_now, snapshot_id="close-partial",
                ),
                replace(zero_triggers, observed_at_ms=first_now), first_now,
            ),
            fills=(first_fill,),
        )
        assert store.reconcile(
            first.digest, catalog=catalog, evidence=first_evidence
        ) == Reconciliation.PARTIAL
    else:
        first_fill = None
        residual = 10**16
        first_evidence = replace(
            _evidence(
                replace(
                    flat_account, cross_perp_amounts_x18={2: residual},
                    observed_at_ms=first_now, snapshot_id="close-rejected",
                ),
                replace(zero_triggers, observed_at_ms=first_now), first_now,
            ),
            exact_rejection_digest=first.digest,
        )
        assert store.reconcile(
            first.digest, catalog=catalog, evidence=first_evidence
        ) == Reconciliation.REJECTED
    assert store.state(first.digest) == "RECONCILED"
    assert core.status == HALTED
    assert not store.write_allowed(
        first.digest, catalog=catalog, now_ms=first_now,
        evidence=first_evidence,
    )
    with pytest.raises(NadoContractError):
        core.prepare_close(
            catalog=catalog, product=product, account=first_evidence.account,
            triggers=first_evidence.triggers, worst_price_x18=36_000 * X18,
            recv_time=entry.recv_time + 4, salt=611, now_ms=first_now,
        )
    assert store.count_kind(CLOSE) == 1
    store.close()
    assert LifecycleCore(
        IntentStore(tmp_path / f"{first_outcome}.sqlite3")
    ).status == HALTED


def test_verified_complete_overrides_three_attempt_exhaustion(
    tmp_path: Path, vector: dict[str, object], product: Product,
    catalog: CatalogSnapshot, flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot,
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    entry = _entry_intent(vector)
    store.prepare(entry)
    entry_fill = FillEvidence(entry.digest, 2, 10**16, 9)
    current = replace(
        flat_account, cross_perp_amounts_x18={2: 10**16},
        observed_at_ms=entry.recv_time + 1, snapshot_id="entry-filled",
    )
    assert store.reconcile(
        entry.digest, catalog=catalog,
        evidence=replace(
            _evidence(
                current,
                replace(zero_triggers, observed_at_ms=entry.recv_time + 1),
                entry.recv_time + 1,
            ),
            fills=(entry_fill,),
        ),
    ) == Reconciliation.FILLED
    core = LifecycleCore(store)
    for attempt in range(3):
        observed = entry.recv_time + 1 + attempt * 2
        close = core.prepare_close(
            catalog=replace(catalog, observed_at_ms=observed), product=product,
            account=replace(
                current, observed_at_ms=observed,
                snapshot_id=f"close-attempt-{attempt}",
            ),
            triggers=replace(
                zero_triggers, observed_at_ms=observed,
                snapshot_id=f"close-trigger-{attempt}",
            ),
            worst_price_x18=36_000 * X18, recv_time=observed + 1,
            salt=700 + attempt, now_ms=observed,
        )
        if attempt == 2:
            assert core.status == RUNNING
        result_time = observed + 2
        if attempt < 2:
            terminal = replace(
                _evidence(
                    replace(
                        current, observed_at_ms=result_time,
                        snapshot_id=f"terminal-{attempt}",
                    ),
                    replace(zero_triggers, observed_at_ms=result_time),
                    result_time,
                ),
                terminal_digest=close.digest,
                terminal_status=("CANCELLED" if attempt == 0 else "EXPIRED"),
            )
            assert store.reconcile(
                close.digest, catalog=catalog, evidence=terminal
            ) == Reconciliation(terminal.terminal_status)
            assert store.state(close.digest) == "RECONCILED"
            assert core.status == RUNNING
            current = terminal.account
        else:
            close_fill = FillEvidence(close.digest, 2, -10**16, 10)
            final = replace(
                _evidence(
                    replace(
                        flat_account, observed_at_ms=result_time,
                        snapshot_id="flat-after-third-close",
                    ),
                    replace(zero_triggers, observed_at_ms=result_time),
                    result_time,
                ),
                fills=(entry_fill, close_fill),
            )
            assert store.reconcile(
                close.digest, catalog=catalog, evidence=final
            ) == Reconciliation.FILLED
    assert completion_barrier(
        store=store, catalog=replace(catalog, observed_at_ms=result_time),
        evidence=final, now_ms=result_time
    )
    store.close()
    assert LifecycleCore(IntentStore(tmp_path / "intents.sqlite3")).status == COMPLETE


@pytest.mark.parametrize("reuse_mode", ["same-id", "not-newer"])
def test_each_close_attempt_requires_genuinely_newer_unconsumed_snapshot(
    tmp_path: Path, vector: dict[str, object], product: Product,
    catalog: CatalogSnapshot, flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot, reuse_mode: str,
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    entry, _, first_account, first_triggers = _filled_entry(
        store, vector, catalog, flat_account, zero_triggers, submission_idx=40
    )
    core = LifecycleCore(store)
    first_account = replace(first_account, snapshot_id="engine-attempt-1")
    first = core.prepare_close(
        catalog=replace(catalog, observed_at_ms=entry.recv_time + 1),
        product=product, account=first_account,
        triggers=first_triggers, worst_price_x18=36_000 * X18,
        recv_time=entry.recv_time + 2, salt=200,
        now_ms=entry.recv_time + 1,
    )
    terminal_time = entry.recv_time + 3
    assert store.reconcile(
        first.digest, catalog=catalog,
        evidence=replace(
            _evidence(
                replace(first_account, observed_at_ms=terminal_time),
                replace(zero_triggers, observed_at_ms=terminal_time),
                terminal_time,
            ),
            terminal_digest=first.digest, terminal_status="CANCELLED",
        ),
    ) == Reconciliation.CANCELLED
    reused = (
        replace(first_account, observed_at_ms=terminal_time)
        if reuse_mode == "same-id"
        else replace(first_account, snapshot_id="new-id")
    )
    with pytest.raises(NadoContractError):
        core.prepare_close(
            catalog=replace(catalog, observed_at_ms=terminal_time),
            product=product, account=reused,
            triggers=replace(zero_triggers, observed_at_ms=terminal_time),
            worst_price_x18=36_000 * X18,
            recv_time=entry.recv_time + 4, salt=201,
            now_ms=terminal_time,
        )


def test_third_no_fill_close_terminal_durably_exhausts_attempts(
    tmp_path: Path, vector: dict[str, object], product: Product,
    catalog: CatalogSnapshot, flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot,
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    entry, _, current, triggers = _filled_entry(
        store, vector, catalog, flat_account, zero_triggers, submission_idx=41
    )
    core = LifecycleCore(store)
    for attempt in range(3):
        observed = entry.recv_time + 1 + attempt * 2
        close = core.prepare_close(
            catalog=replace(catalog, observed_at_ms=observed), product=product,
            account=replace(
                current,
                observed_at_ms=observed, snapshot_id=f"engine-{attempt}",
            ),
            triggers=replace(
                zero_triggers, observed_at_ms=observed,
                snapshot_id=f"trigger-{attempt}",
            ),
            worst_price_x18=36_000 * X18, recv_time=observed + 1,
            salt=300 + attempt, now_ms=observed,
        )
        if attempt == 2:
            assert core.status == RUNNING
        result_time = observed + 2
        terminal = replace(
            _evidence(
                replace(
                    current, observed_at_ms=result_time,
                    snapshot_id=f"terminal-{attempt}",
                ),
                replace(zero_triggers, observed_at_ms=result_time), result_time,
            ),
            terminal_digest=close.digest, terminal_status="CANCELLED",
        )
        assert store.reconcile(
            close.digest, catalog=catalog, evidence=terminal
        ) == Reconciliation.CANCELLED
        current = terminal.account
    assert core.status == HALTED and core.status != COMPLETE
    assert store.lifecycle_status() == HALTED
    assert not store.write_allowed(
        close.digest, catalog=catalog, now_ms=result_time, evidence=terminal
    )
    assert not completion_barrier(
        store=store, catalog=catalog, evidence=terminal, now_ms=result_time
    )
    store.close()
    assert LifecycleCore(IntentStore(tmp_path / "intents.sqlite3")).status == HALTED


@pytest.mark.parametrize("position", [10**16 + 1, 2 * 10**16])
def test_off_grid_or_over_cap_close_halts(
    tmp_path: Path, vector: dict[str, object], product: Product,
    catalog: CatalogSnapshot,
    flat_account: AccountSnapshot, zero_triggers: TriggerSnapshot, position: int,
) -> None:
    path = tmp_path / "intents.sqlite3"
    store = IntentStore(path)
    entry, _, _, _ = _filled_entry(
        store, vector, catalog, flat_account, zero_triggers,
        submission_idx=950 + int(position > 10**16 + 1),
    )
    core = LifecycleCore(store)
    with pytest.raises(NadoContractError):
        core.prepare_close(
            catalog=replace(catalog, observed_at_ms=entry.recv_time + 1),
            product=product,
            account=replace(
                flat_account, cross_perp_amounts_x18={2: position},
                observed_at_ms=entry.recv_time + 1,
            ),
            triggers=replace(zero_triggers, observed_at_ms=entry.recv_time + 1),
            worst_price_x18=(36_000 if position % 10**15 else 30_000) * X18,
            recv_time=entry.recv_time + 2, salt=400,
            now_ms=entry.recv_time + 1,
        )
    assert core.status == HALTED
    core.store.close()
    assert LifecycleCore(IntentStore(path)).status == HALTED


def test_exact_final_barrier_and_adversarial_blockers(
    tmp_path: Path, vector: dict[str, object], catalog: CatalogSnapshot,
    flat_account: AccountSnapshot, zero_triggers: TriggerSnapshot,
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    intent = _entry_intent(vector)
    store.prepare(intent)
    now = intent.recv_time + 1
    evidence = _evidence(flat_account, zero_triggers, now)
    evidence = replace(
        evidence, terminal_digest=intent.digest, terminal_status="CANCELLED"
    )
    final_catalog = replace(catalog, observed_at_ms=now)
    assert store.reconcile(
        intent.digest, catalog=final_catalog, evidence=evidence
    ) == Reconciliation.CANCELLED
    assert completion_barrier(
        store=store, catalog=final_catalog, evidence=evidence, now_ms=now
    )
    for bad in (
        replace(flat_account, regular_orders_by_product={2: ("0xopen",)}),
        replace(flat_account, cross_perp_amounts_x18={2: 1}),
        replace(flat_account, isolated_positions=("isolated",)),
        replace(flat_account, contradictions=("conflict",)),
        replace(flat_account, regular_orders_by_product={}),
        replace(flat_account, cross_perp_amounts_x18={}),
    ):
        assert not completion_barrier(
            store=store, catalog=final_catalog,
            evidence=replace(evidence, account=bad), now_ms=now,
        )
    assert not completion_barrier(
        store=store, catalog=final_catalog,
        evidence=replace(evidence, triggers=replace(zero_triggers, active_digests=("0xtrigger",))),
        now_ms=now,
    )
    other_account, other_triggers = _other_identity(flat_account, zero_triggers)
    assert not completion_barrier(
        store=store, catalog=final_catalog,
        evidence=_evidence(other_account, other_triggers, now), now_ms=now,
    )


def test_final_barrier_requires_exactly_one_entry_not_empty_cancel_only_or_close_only(
    tmp_path: Path, vector: dict[str, object], product: Product,
    catalog: CatalogSnapshot, flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot,
) -> None:
    recv = int(vector["recv_time"])
    evidence = _evidence(flat_account, zero_triggers, recv + 1)
    empty = IntentStore(tmp_path / "empty.sqlite3")
    assert not completion_barrier(
        store=empty, catalog=catalog, evidence=evidence, now_ms=recv + 1
    )

    cancel_store = IntentStore(tmp_path / "cancel.sqlite3")
    with pytest.raises(NadoContractError):
        LifecycleCore(cancel_store).prepare_cancel_all(
            catalog=catalog,
            account=replace(flat_account, regular_orders_by_product={2: ("0xopen",)}),
            triggers=zero_triggers, sender=str(vector["sender"]),
            recv_time=recv, salt=500, now_ms=recv - 1,
        )
    assert cancel_store.count_kind(CANCEL_ALL) == 0
    assert not completion_barrier(
        store=cancel_store, catalog=catalog, evidence=evidence, now_ms=recv + 1
    )

    close_store = IntentStore(tmp_path / "close.sqlite3")
    with pytest.raises(NadoContractError):
        LifecycleCore(close_store).prepare_close(
            catalog=catalog, product=product,
            account=replace(
                flat_account, cross_perp_amounts_x18={2: 10**16},
                observed_at_ms=recv - 1, snapshot_id="close-only",
            ),
            triggers=replace(zero_triggers, observed_at_ms=recv - 1),
            worst_price_x18=36_000 * X18, recv_time=recv, salt=501,
            now_ms=recv - 1,
        )
    assert close_store.count_kind(CLOSE) == 0
    assert not completion_barrier(
        store=close_store, catalog=catalog, evidence=evidence, now_ms=recv + 1
    )


def test_pending_ambiguous_unexpired_or_unknown_fill_never_complete(
    tmp_path: Path, vector: dict[str, object], catalog: CatalogSnapshot,
    flat_account: AccountSnapshot, zero_triggers: TriggerSnapshot,
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    intent = _entry_intent(vector)
    store.prepare(intent)
    evidence = _evidence(flat_account, zero_triggers, intent.recv_time + 1)
    assert not completion_barrier(store=store, catalog=catalog, evidence=evidence, now_ms=intent.recv_time - 1)
    assert store.reconcile(
        intent.digest,
        catalog=replace(catalog, observed_at_ms=intent.recv_time + 1),
        evidence=evidence,
    ) == Reconciliation.AMBIGUOUS
    assert not completion_barrier(store=store, catalog=catalog, evidence=evidence, now_ms=intent.recv_time + 1)


def test_duplicate_fill_identity_cannot_fabricate_net_zero_completion(
    tmp_path: Path, vector: dict[str, object], product: Product,
    catalog: CatalogSnapshot, flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot,
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    entry, entry_fill, account, triggers = _filled_entry(
        store, vector, catalog, flat_account, zero_triggers, submission_idx=901
    )
    close = LifecycleCore(store).prepare_close(
        catalog=replace(catalog, observed_at_ms=entry.recv_time + 1),
        product=product, account=account, triggers=triggers,
        worst_price_x18=36_000 * X18, recv_time=entry.recv_time + 2,
        salt=800, now_ms=entry.recv_time + 1,
    )
    close_fill = FillEvidence(close.digest, 2, -10**16, submission_idx=902)
    now = entry.recv_time + 3
    valid = replace(
        _evidence(
            replace(flat_account, observed_at_ms=now),
            replace(zero_triggers, observed_at_ms=now), now,
        ),
        fills=(entry_fill, close_fill),
    )
    final_catalog = replace(catalog, observed_at_ms=now)
    assert store.reconcile(
        close.digest, catalog=final_catalog, evidence=valid
    ) == Reconciliation.FILLED
    fabricated = replace(
        valid, fills=(entry_fill, entry_fill, close_fill, close_fill),
    )
    assert not completion_barrier(
        store=store, catalog=final_catalog, evidence=fabricated, now_ms=now
    )
    store.close()
    assert LifecycleCore(IntentStore(tmp_path / "intents.sqlite3")).status == HALTED
