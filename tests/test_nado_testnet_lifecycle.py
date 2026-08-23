from __future__ import annotations

import importlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from risex_farmer.nado_testnet_lifecycle import (
    ACTIVE_PERP, CANCEL_ALL, CLOSE, COMPLETE, ENTRY, HALTED,
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
    return CatalogSnapshot((product,), True, 1_700_000_000_000)


@pytest.fixture
def flat_account(vector: dict[str, object]) -> AccountSnapshot:
    return AccountSnapshot(
        763373, FixedEnvironment.endpoint, str(vector["owner"]), "default",
        1_700_000_000_000, True, "engine", {2: ()}, {2: 0}, (),
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
    return EngineEvidence(account, triggers, (), (), observed)


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
    with pytest.raises(NadoContractError):
        FixedEnvironment.assert_exact(chain_id=763374, endpoint=FixedEnvironment.endpoint)
    for recv_time, salt in ((-1, 0), (2**44, 0), (0, -1), (0, 2**20)):
        with pytest.raises(NadoContractError):
            build_order_nonce(recv_time, salt)


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


def test_complete_dynamic_catalog_and_all_keys_are_mandatory(
    product: Product, flat_account: AccountSnapshot, zero_triggers: TriggerSnapshot,
) -> None:
    second = replace(product, product_id=4, symbol="ETH-PERP")
    for catalog in (
        CatalogSnapshot((), True, 1_700_000_000_000),
        CatalogSnapshot((product,), False, 1_700_000_000_000),
        CatalogSnapshot((product, second), True, 1_700_000_000_000),
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


def test_entry_reconciliation_resting_partial_full_reject_and_duplicate_ambiguity(
    tmp_path: Path, vector: dict[str, object], catalog: CatalogSnapshot,
    flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot,
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    intent = _entry_intent(vector)
    store.prepare(intent)
    base = _evidence(flat_account, zero_triggers, 1_700_000_000_001)
    open_order = OrderEvidence(intent.digest, 2, intent.nonce, 10**16, "OPEN")
    assert store.reconcile(intent.digest, catalog=catalog, evidence=replace(base, orders=(open_order,))) == Reconciliation.RESTING
    partial = FillEvidence(intent.digest, 2, 4 * 10**15)
    account = replace(flat_account, cross_perp_amounts_x18={2: 4 * 10**15})
    assert store.reconcile(intent.digest, catalog=catalog, evidence=replace(base, account=account, fills=(partial,))) == Reconciliation.PARTIAL
    full = FillEvidence(intent.digest, 2, 10**16)
    account = replace(flat_account, cross_perp_amounts_x18={2: 10**16})
    assert store.reconcile(intent.digest, catalog=catalog, evidence=replace(base, account=account, fills=(full,))) == Reconciliation.FILLED
    assert store.reconcile(intent.digest, catalog=catalog, evidence=replace(base, exact_rejection_digest=intent.digest)) == Reconciliation.REJECTED
    assert store.reconcile(intent.digest, catalog=catalog, evidence=replace(base, duplicate_digest=True)) == Reconciliation.AMBIGUOUS


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


def test_cancel_all_is_durable_empty_products_ambiguous_and_not_replayed(
    tmp_path: Path, vector: dict[str, object], catalog: CatalogSnapshot,
    flat_account: AccountSnapshot, zero_triggers: TriggerSnapshot,
) -> None:
    recv = int(vector["recv_time"])
    store = IntentStore(tmp_path / "intents.sqlite3")
    account = replace(flat_account, regular_orders_by_product={2: ("0xopen",)})
    intent = LifecycleCore(store).prepare_cancel_all(
        catalog=catalog, account=account, triggers=zero_triggers,
        sender=str(vector["sender"]), recv_time=recv, salt=43, now_ms=recv - 1,
    )
    assert intent.kind == CANCEL_ALL
    assert json.loads(intent.payload)["cancel_product_orders"]["tx"]["productIds"] == []
    store.mark_ambiguous(intent.digest)
    with pytest.raises(NadoContractError):
        store.prepare(intent)


def test_cancel_ambiguity_reconciles_only_after_recv_time_and_zero_orders(
    tmp_path: Path, vector: dict[str, object], catalog: CatalogSnapshot,
    flat_account: AccountSnapshot, zero_triggers: TriggerSnapshot,
) -> None:
    recv = int(vector["recv_time"])
    store = IntentStore(tmp_path / "intents.sqlite3")
    account = replace(flat_account, regular_orders_by_product={2: ("0xopen",)})
    intent = LifecycleCore(store).prepare_cancel_all(
        catalog=catalog, account=account, triggers=zero_triggers,
        sender=str(vector["sender"]), recv_time=recv, salt=44, now_ms=recv - 1,
    )
    before = _evidence(flat_account, zero_triggers, recv)
    assert store.reconcile(intent.digest, catalog=catalog, evidence=before) == Reconciliation.AMBIGUOUS
    assert store.reconcile(intent.digest, catalog=catalog, evidence=replace(before, observed_at_ms=recv + 1)) == Reconciliation.CANCELLED


def test_partial_residual_close_ioc_reduce_only_clamp_and_new_identity(
    tmp_path: Path, vector: dict[str, object], product: Product,
    catalog: CatalogSnapshot, flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot,
) -> None:
    recv = int(vector["recv_time"])
    account = replace(
        flat_account, cross_perp_amounts_x18={2: 4 * 10**15},
        observed_at_ms=recv - 1, snapshot_id="engine-close-1",
    )
    store = IntentStore(tmp_path / "intents.sqlite3")
    close = LifecycleCore(store).prepare_close(
        catalog=catalog, product=product, account=account,
        triggers=replace(zero_triggers, observed_at_ms=recv - 1),
        worst_price_x18=36_000 * X18, recv_time=recv, salt=100, now_ms=recv - 1,
    )
    assert (close.kind, close.amount_x18, close.appendix) == (CLOSE, -10**16, IOC_REDUCE_ONLY_APPENDIX)
    assert close.clamp_expected and close.notional_x18 == 360 * X18
    assert store.get(close.digest).snapshot_id == "engine-close-1"


def test_close_fill_reconciles_from_recorded_starting_position_to_exact_zero(
    tmp_path: Path, vector: dict[str, object], product: Product,
    catalog: CatalogSnapshot, flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot,
) -> None:
    recv = int(vector["recv_time"])
    store = IntentStore(tmp_path / "intents.sqlite3")
    close = LifecycleCore(store).prepare_close(
        catalog=catalog, product=product,
        account=replace(
            flat_account, cross_perp_amounts_x18={2: 10**16},
            observed_at_ms=recv - 1, snapshot_id="engine-close-fill",
        ),
        triggers=replace(zero_triggers, observed_at_ms=recv - 1),
        worst_price_x18=36_000 * X18, recv_time=recv, salt=150, now_ms=recv - 1,
    )
    evidence = _evidence(
        replace(flat_account, observed_at_ms=recv + 1, snapshot_id="engine-flat-after-close"),
        replace(zero_triggers, observed_at_ms=recv + 1),
        recv + 1,
    )
    fill = FillEvidence(close.digest, 2, -10**16)
    assert store.reconcile(close.digest, catalog=catalog, evidence=replace(evidence, fills=(fill,))) == Reconciliation.FILLED


def test_each_close_attempt_requires_genuinely_newer_unconsumed_snapshot(
    tmp_path: Path, vector: dict[str, object], product: Product,
    catalog: CatalogSnapshot, flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot,
) -> None:
    recv = int(vector["recv_time"])
    store = IntentStore(tmp_path / "intents.sqlite3")
    core = LifecycleCore(store)
    first_account = replace(
        flat_account, cross_perp_amounts_x18={2: 10**16},
        observed_at_ms=recv - 1, snapshot_id="engine-attempt-1",
    )
    first = core.prepare_close(
        catalog=catalog, product=product, account=first_account,
        triggers=replace(zero_triggers, observed_at_ms=recv - 1),
        worst_price_x18=36_000 * X18, recv_time=recv, salt=200, now_ms=recv - 1,
    )
    store.mark_rejected(first.digest)
    for reused in (
        replace(first_account, observed_at_ms=recv + 1),
        replace(first_account, snapshot_id="new-id"),
    ):
        with pytest.raises(NadoContractError):
            core.prepare_close(
                catalog=catalog, product=product, account=reused,
                triggers=replace(zero_triggers, observed_at_ms=recv + 1),
                worst_price_x18=36_000 * X18, recv_time=recv + 2,
                salt=201, now_ms=recv + 1,
            )


def test_three_state_based_close_attempts_then_manual_halt(
    tmp_path: Path, vector: dict[str, object], product: Product,
    catalog: CatalogSnapshot, flat_account: AccountSnapshot,
    zero_triggers: TriggerSnapshot,
) -> None:
    base = int(vector["recv_time"])
    store = IntentStore(tmp_path / "intents.sqlite3")
    core = LifecycleCore(store)
    for attempt in range(3):
        observed = base + attempt * 2
        close = core.prepare_close(
            catalog=catalog, product=product,
            account=replace(
                flat_account, cross_perp_amounts_x18={2: 10**16},
                observed_at_ms=observed, snapshot_id=f"engine-{attempt}",
            ),
            triggers=replace(
                zero_triggers, observed_at_ms=observed,
                snapshot_id=f"trigger-{attempt}",
            ),
            worst_price_x18=36_000 * X18, recv_time=observed + 1,
            salt=300 + attempt, now_ms=observed,
        )
        store.mark_rejected(close.digest)
    assert core.status == HALTED and core.status != COMPLETE


@pytest.mark.parametrize("position", [10**16 + 1, 2 * 10**16])
def test_off_grid_or_over_cap_close_halts(
    tmp_path: Path, product: Product, catalog: CatalogSnapshot,
    flat_account: AccountSnapshot, zero_triggers: TriggerSnapshot, position: int,
) -> None:
    core = LifecycleCore(IntentStore(tmp_path / "intents.sqlite3"))
    with pytest.raises(NadoContractError):
        core.prepare_close(
            catalog=catalog, product=product,
            account=replace(flat_account, cross_perp_amounts_x18={2: position}),
            triggers=zero_triggers,
            worst_price_x18=(36_000 if position % 10**15 else 30_000) * X18,
            recv_time=1_700_000_000_100, salt=400, now_ms=1_700_000_000_000,
        )
    assert core.status == HALTED


def test_exact_final_barrier_and_adversarial_blockers(
    tmp_path: Path, vector: dict[str, object], catalog: CatalogSnapshot,
    flat_account: AccountSnapshot, zero_triggers: TriggerSnapshot,
) -> None:
    store = IntentStore(tmp_path / "intents.sqlite3")
    intent = _entry_intent(vector)
    store.prepare(intent)
    store.mark_reconciled(intent.digest)
    now = intent.recv_time + 1
    evidence = _evidence(flat_account, zero_triggers, now)
    assert completion_barrier(store=store, catalog=catalog, evidence=evidence, now_ms=now)
    for bad in (
        replace(flat_account, regular_orders_by_product={2: ("0xopen",)}),
        replace(flat_account, cross_perp_amounts_x18={2: 1}),
        replace(flat_account, isolated_positions=("isolated",)),
        replace(flat_account, contradictions=("conflict",)),
        replace(flat_account, regular_orders_by_product={}),
        replace(flat_account, cross_perp_amounts_x18={}),
    ):
        assert not completion_barrier(
            store=store, catalog=catalog, evidence=replace(evidence, account=bad), now_ms=now,
        )
    assert not completion_barrier(
        store=store, catalog=catalog,
        evidence=replace(evidence, triggers=replace(zero_triggers, active_digests=("0xtrigger",))),
        now_ms=now,
    )
    other_account, other_triggers = _other_identity(flat_account, zero_triggers)
    assert not completion_barrier(
        store=store, catalog=catalog,
        evidence=_evidence(other_account, other_triggers, now), now_ms=now,
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
    store.mark_ambiguous(intent.digest)
    assert not completion_barrier(store=store, catalog=catalog, evidence=evidence, now_ms=intent.recv_time + 1)
    store.mark_reconciled(intent.digest)
    assert not completion_barrier(
        store=store, catalog=catalog,
        evidence=replace(evidence, fills=(FillEvidence("0x" + "ff" * 32, 2, 1),)),
        now_ms=intent.recv_time + 1,
    )
