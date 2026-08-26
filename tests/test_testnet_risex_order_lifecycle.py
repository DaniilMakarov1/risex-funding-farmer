from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from risex_farmer.testnet_risex_order_lifecycle import (
    AccountState, BBO, CANCEL_ACTION, DurableIntentStore, Evidence, FillRecord,
    HEADER_FLAGS, Lifecycle, LifecycleSafetyError, MarketState, OrderRecord,
    Outcome, SyntheticSigner, encode_cancel_action, encode_place_action,
    pack_order_data,
)

NOW = 1_800_000_000
ROUTER = "0x" + "22" * 20
AUTHORIZATION = "0x" + "33" * 20
ACCOUNT = "0x" + "44" * 20
SIGNER_ADDRESS = "0x" + "55" * 20
OTHER_ACCOUNT = "0x" + "66" * 20
OTHER_SIGNER = "0x" + "77" * 20
OTHER_ROUTER = "0x" + "88" * 20
OTHER_AUTHORIZATION = "0x" + "99" * 20
SIGNER = SyntheticSigner(SIGNER_ADDRESS)
EXPECTED_ORDER_DATA = 1_180_591_648_218_017_085_442
EXPECTED_ACTION_HASH = "ee1058ee762d6614bf31b0d75949cacb9c7bf5a9d7817e2c190315df2bbc1085"
EXPECTED_ABI = (
    "1d442a680326a08fbf310b367c5c0194ca94bbc644dc507e0b816b055ccfa2b9"
    "0000000000000000000000000000000000000000000000000000000000000005"
    "00000000000000000000000000000000000000000000004000001902fbd6b802"
    "0000000000000000000000000000000000000000000000000000000000000000"
    "0000000000000000000000000000000000000000000000000000000000000065"
    "0000000000000000000000000000000000000000000000000000000000000000"
)
EXPECTED_CANCEL_ABI = (
    "5b9cc6280e71dfbb2c676ca979b5dac4a65a90e224b393011adb1c9d93b05a83"
    "0000000000000000000000000000000000000000000000000000000000000001"
    "0000000000000000000000000000000000000000000000000000000000000038"
)
EXPECTED_CANCEL_ACTION_HASH = (
    "5639550dec6707e1923c9f0a1b555d2edc00cdce23d52a6c8cbab80f07554921"
)


def composite_order_id(wide_order_id, block_number, log_index):
    return f"0x{wide_order_id:016x}{block_number:016x}{log_index:016x}"


OPENING_ORDER = composite_order_id(113, 100, 1)
CLOSE_1 = composite_order_id(115, 101, 2)
CLOSE_2 = composite_order_id(117, 102, 3)
CLOSE_3 = composite_order_id(119, 103, 4)
AUTHORITATIVE_ORDER = composite_order_id(121, 104, 5)
OTHER_ORDER = composite_order_id(123, 105, 6)


def order_record(order_id, client_id):
    wide_order_id = int.from_bytes(bytes.fromhex(order_id[2:18]), "big")
    return OrderRecord(order_id, wide_order_id, wide_order_id >> 1, client_id)


def market(**changes):
    value = MarketState(
        host="api.testnet.rise.trade", chain_id=11_155_931,
        domain_name="RISEx", domain_version="1", router=ROUTER,
        authorization=AUTHORIZATION, market_id=1, symbol="BTC/USDC",
        active=True, unlocked=True, tick=Decimal("0.1"),
        step=Decimal("0.000001"), minimum=Decimal("0.0001"),
        observed_at=NOW,
    )
    return replace(value, **changes)


def account(**changes):
    value = AccountState(
        account=ACCOUNT, signer=SIGNER_ADDRESS, signer_status="ACTIVE",
        position=Decimal("0"), open_order_ids=(),
        repeated_open_order_ids=(), repeated_position=Decimal("0"),
        unexplained=False, observed_at=NOW,
    )
    return replace(value, **changes)


def bbo(**changes):
    value = BBO(
        bid=Decimal("77982.9"), ask=Decimal("77983.0"),
        bid_depth=Decimal("0.000646"), ask_depth=Decimal("0.000646"),
        observed_at=NOW,
    )
    return replace(value, **changes)


@pytest.fixture
def lifecycle(tmp_path):
    store = DurableIntentStore(tmp_path / "fixture.sqlite3")
    instance = Lifecycle(
        store, now=lambda: NOW, router=ROUTER, authorization=AUTHORIZATION,
        expected_account=ACCOUNT, expected_signer=SIGNER_ADDRESS,
    )
    yield instance
    instance.store.close()


def ready(lifecycle):
    return lifecycle.preflight(market(), account(), bbo())


def new_lifecycle(path, now=NOW):
    store = DurableIntentStore(path)
    return Lifecycle(
        store, now=lambda: now, router=ROUTER, authorization=AUTHORIZATION,
        expected_account=ACCOUNT, expected_signer=SIGNER_ADDRESS,
    )


def open_intent(lifecycle, *, client_id=101, anchor=7, bitmap=3, expires=NOW + 30):
    return lifecycle.prepare_open(ready(lifecycle), client_id, anchor, bitmap, expires)


def exact_evidence(order_id, client_id, *, filled="0", position="0",
                   observed=NOW + 1, open_ids=(), terminal=True):
    has_fill = Decimal(filled) > 0
    record = order_record(order_id, client_id)
    return Evidence(
        account=ACCOUNT, signer=SIGNER_ADDRESS, signer_status="ACTIVE",
        terminal=terminal, filled_size=Decimal(filled),
        position=Decimal(position), observed_at=observed, position_market_id=1,
        by_id_order=record,
        open_orders=tuple(order_record(item, client_id) for item in open_ids),
        history_orders=(record,),
        fills=(FillRecord(order_id, client_id),) if has_fill else (),
    )


def dispatch(lifecycle, intent, order_id):
    lifecycle.dispatch(intent, SIGNER, lambda _: order_id)
    persisted = lifecycle.store.get(intent.intent_id)
    assert persisted.dispatch_count == 1 and persisted.state == "DISPATCHED"


def cancel(lifecycle, order_id, execute, *, observed=NOW + 1):
    lifecycle.cancel_known(
        order_id, market=market(observed_at=observed), nonce_anchor=9,
        nonce_bitmap=0, expires_at=NOW + 40, synthetic_signer=SIGNER,
        execute=execute,
    )


def seed_filled(lifecycle):
    opening = open_intent(lifecycle)
    dispatch(lifecycle, opening, OPENING_ORDER)
    lifecycle._now = lambda: NOW + 1
    lifecycle.reconcile(
        opening.intent_id,
        exact_evidence(OPENING_ORDER, 101, filled="0.0001", position="0.0001"),
    )
    return opening


def prepare_close(lifecycle, *, position="0.0001", client_id=201, anchor=8,
                  bitmap=4, observed=NOW + 1, expires=NOW + 40):
    return lifecycle.prepare_close(
        market(observed_at=observed),
        account(position=Decimal(position), repeated_position=Decimal(position),
                observed_at=observed),
        bbo(observed_at=observed), client_id, anchor, bitmap, expires,
    )


def test_preflight_blocks_before_signer_or_post(lifecycle):
    calls = []
    invalid = [
        (market(active=False), account(), bbo()),
        (market(observed_at=NOW - 6), account(), bbo()),
        (market(tick=Decimal("0.3")), account(), bbo()),
        (market(), account(signer_status="INACTIVE"), bbo()),
        (market(), account(account=OTHER_ACCOUNT), bbo()),
        (market(), account(signer=OTHER_SIGNER), bbo()),
        (market(), account(repeated_position=Decimal("0.0001")), bbo()),
        (market(), account(), bbo(ask_depth=Decimal("0.000099"))),
        (market(), account(), bbo(bid_depth=Decimal("0.000099"))),
        (market(), account(), bbo(ask=Decimal("77983.05"))),
    ]
    for bad_market, bad_account, bad_bbo in invalid:
        with pytest.raises(LifecycleSafetyError):
            lifecycle.run_open(
                bad_market, bad_account, bad_bbo, 1, 1, 0, NOW + 30,
                signer_loader=lambda: calls.append("loader"),
                dispatch=lambda _: "must-not-dispatch",
            )
    assert calls == [] and lifecycle.store.all() == []


def test_intent_nonce_and_digest_are_durable_before_dispatch(lifecycle):
    intent = open_intent(lifecycle)
    path = lifecycle.store.path
    lifecycle.store.close()
    lifecycle.store = DurableIntentStore(path)
    seen = []
    lifecycle.dispatch(
        intent, SIGNER,
        lambda _: seen.append(lifecycle.store.get(intent.intent_id)) or AUTHORITATIVE_ORDER,
    )
    persisted = seen[0]
    assert persisted.state == "DISPATCHING" and persisted.dispatch_count == 1
    assert (persisted.nonce, persisted.nonce_bitmap) == (7, 3)
    assert persisted.payload_digest == EXPECTED_ACTION_HASH
    assert lifecycle.store.get(intent.intent_id).order_id == AUTHORITATIVE_ORDER


def test_ambiguous_open_is_never_replayed(lifecycle, tmp_path):
    intent = open_intent(lifecycle)
    lifecycle.dispatch(intent, SIGNER, lambda _: (_ for _ in ()).throw(TimeoutError()))
    assert lifecycle.store.get(intent.intent_id).state == "AMBIGUOUS"
    with pytest.raises(LifecycleSafetyError):
        lifecycle.dispatch(intent, SIGNER, lambda _: "replay")
    no_identity = Evidence(
        account=ACCOUNT, signer=SIGNER_ADDRESS, signer_status="ACTIVE",
        terminal=True, filled_size=Decimal("0"), position=Decimal("0"),
        observed_at=NOW + 1, position_market_id=1,
    )
    lifecycle._now = lambda: NOW + 1
    with pytest.raises(LifecycleSafetyError):
        lifecycle.reconcile(intent.intent_id, no_identity)
    lifecycle._now = lambda: NOW + 31
    no_identity = replace(no_identity, observed_at=NOW + 31)
    assert lifecycle.reconcile(intent.intent_id, no_identity) == Outcome.COMPLETED_NO_FILL_FLAT

    store = DurableIntentStore(tmp_path / "delayed.sqlite3")
    delayed = Lifecycle(
        store, now=lambda: NOW, router=ROUTER, authorization=AUTHORIZATION,
        expected_account=ACCOUNT, expected_signer=SIGNER_ADDRESS,
    )
    delayed_intent = open_intent(delayed)
    delayed.dispatch(
        delayed_intent, SIGNER,
        lambda _: (_ for _ in ()).throw(TimeoutError()),
    )
    delayed._now = lambda: NOW + 1
    assert delayed.reconcile(
        delayed_intent.intent_id, exact_evidence(AUTHORITATIVE_ORDER, 101),
    ) == Outcome.COMPLETED_NO_FILL_FLAT
    store.close()


def test_malformed_order_identity_halts_after_dispatch(tmp_path):
    for index, response in enumerate(("", "   ", 123)):
        candidate = new_lifecycle(tmp_path / f"bad-dispatch-id-{index}.sqlite3")
        intent = open_intent(candidate)
        candidate.dispatch(intent, SIGNER, lambda _: response)
        persisted = candidate.store.get(intent.intent_id)
        assert persisted.dispatch_count == 1 and persisted.state == "AMBIGUOUS"
        assert candidate.store.load_outcome() == Outcome.ACTIVE
        with pytest.raises(LifecycleSafetyError):
            candidate.dispatch(intent, SIGNER, lambda _: "replay")
        candidate._now = lambda: NOW + 1
        assert candidate.reconcile(
            intent.intent_id, exact_evidence(AUTHORITATIVE_ORDER, 101),
        ) == Outcome.COMPLETED_NO_FILL_FLAT
        candidate.store.close()

    malformed_surfaces = (
        {"by_id_order": OrderRecord("", 121, 60, 101)},
        {"by_id_order": OrderRecord(AUTHORITATIVE_ORDER, 999, 499, 101)},
        {"history_orders": (OrderRecord(AUTHORITATIVE_ORDER, 121, 61, 101),)},
        {"fills": (FillRecord(" ", 101),)},
        {"open_orders": (123,), "terminal": False},
    )
    for index, changes in enumerate(malformed_surfaces):
        candidate = new_lifecycle(tmp_path / f"bad-evidence-id-{index}.sqlite3")
        intent = open_intent(candidate)
        candidate.dispatch(
            intent, SIGNER, lambda _: (_ for _ in ()).throw(TimeoutError()),
        )
        candidate._now = lambda: NOW + 1
        evidence = replace(
            exact_evidence(AUTHORITATIVE_ORDER, 101), **changes,
        )
        with pytest.raises(LifecycleSafetyError):
            candidate.reconcile(intent.intent_id, evidence)
        assert candidate.store.load_outcome() == Outcome.FAILED_HALTED_MANUAL_RECOVERY
        assert candidate.store.get(intent.intent_id).state != "TERMINAL"
        candidate.store.close()


def test_open_is_exact_minimum_price_bounded_crossing_limit_ioc(lifecycle):
    intent = open_intent(lifecycle)
    request = lifecycle.unsigned_request(intent.intent_id, market=market())
    assert intent.size == Decimal("0.0001") and intent.price == Decimal("78217.0")
    assert (intent.size_steps, intent.price_ticks) == (100, 782170)
    assert request["header_flags"] == HEADER_FLAGS == 0x05
    assert request["order_data"] == EXPECTED_ORDER_DATA
    assert request["abi_encoded"].hex() == EXPECTED_ABI
    assert request["action_hash"].hex() == EXPECTED_ACTION_HASH
    assert request["permit"]["primaryType"] == "VerifyWitness"
    assert request["permit"]["domain"] == {
        "name": "RISEx", "version": "1", "chainId": 11_155_931,
        "verifyingContract": AUTHORIZATION,
    }
    assert request["permit"]["message"] == {
        "account": ACCOUNT, "target": ROUTER,
        "hash": "0x" + EXPECTED_ACTION_HASH, "nonceAnchor": 7,
        "nonceBitmap": 3, "deadline": NOW + 30,
    }
    assert request["dispatchable"] is False and request["signature"] is None
    assert request["body"] == {
        "market_id": 1, "size_steps": 100, "price_ticks": 782170,
        "side": 0, "order_type": 1, "time_in_force": 3,
        "post_only": False, "reduce_only": False, "stp_mode": 0,
        "client_order_id": 101, "account": ACCOUNT.lower(),
        "signer": SIGNER_ADDRESS.lower(), "nonce_anchor": "7",
        "nonce_bitmap_index": 3, "deadline": NOW + 30,
    }
    with pytest.raises(TypeError):
        lifecycle.unsigned_request(
            intent.intent_id, market=market(), account=OTHER_ACCOUNT,
        )


def test_wide_positive_spread_preserves_exact_lifecycle_price_bound(lifecycle):
    wide_bbo = bbo(ask=Decimal("90000.0"))

    preflight = lifecycle.preflight(market(), account(), wide_bbo)
    intent = lifecycle.prepare_open(preflight, 101, 7, 3, NOW + 30)

    assert intent.size == Decimal("0.0001")
    assert intent.price == Decimal("90270.0")
    assert intent.size * intent.price <= Decimal("500")


def test_fok_no_fill_finishes_flat_without_close_acceptance(lifecycle):
    intent = open_intent(lifecycle)
    dispatch(lifecycle, intent, OPENING_ORDER)
    lifecycle._now = lambda: NOW + 1
    result = lifecycle.reconcile(intent.intent_id, exact_evidence(OPENING_ORDER, 101))
    assert result == Outcome.COMPLETED_NO_FILL_FLAT and lifecycle.close_count == 0


def test_first_close_uses_exact_authoritative_size_market_fok(lifecycle, tmp_path):
    seed_filled(lifecycle)
    path = lifecycle.store.path
    lifecycle.store.close()
    lifecycle.store = DurableIntentStore(path)
    recovered = Lifecycle(
        lifecycle.store, now=lambda: NOW + 1, router=ROUTER,
        authorization=AUTHORIZATION,
        expected_account=ACCOUNT, expected_signer=SIGNER_ADDRESS,
    )
    assert recovered.observed_opening_fill
    intent = prepare_close(recovered)
    assert (intent.side, intent.size, intent.order_type, intent.time_in_force) == (
        "SELL", Decimal("0.0001"), "MARKET", "FOK",
    )
    assert intent.reduce_only and intent.source_position == Decimal("0.0001")
    bad = new_lifecycle(tmp_path / "bad-close-identity.sqlite3")
    seed_filled(bad)
    with pytest.raises(LifecycleSafetyError):
        bad.prepare_close(
            market(observed_at=NOW + 1),
            account(
                account=OTHER_ACCOUNT, position=Decimal("0.0001"),
                repeated_position=Decimal("0.0001"), observed_at=NOW + 1,
            ),
            bbo(observed_at=NOW + 1), 201, 8, 4, NOW + 40,
        )
    assert bad.store.load_outcome() == Outcome.FAILED_HALTED_MANUAL_RECOVERY
    bad.store.close()


def test_close_fallbacks_use_fresh_state_limit_ioc_and_stop_at_three(lifecycle):
    seed_filled(lifecycle)
    first = prepare_close(lifecycle)
    dispatch(lifecycle, first, CLOSE_1)
    lifecycle.reconcile(first.intent_id, exact_evidence(CLOSE_1, 201, position="0.0001"))
    lifecycle._now = lambda: NOW + 2
    second = prepare_close(lifecycle, client_id=202, anchor=8, bitmap=5, observed=NOW + 2)
    dispatch(lifecycle, second, CLOSE_2)
    lifecycle.reconcile(second.intent_id, exact_evidence(
        CLOSE_2, 202, filled="0.00006", position="0.00004", observed=NOW + 2,
    ))
    lifecycle._now = lambda: NOW + 3
    third = prepare_close(lifecycle, position="0.00004", client_id=203,
                          anchor=8, bitmap=6, observed=NOW + 3)
    dispatch(lifecycle, third, CLOSE_3)
    lifecycle.reconcile(third.intent_id, exact_evidence(
        CLOSE_3, 203, position="0.00004", observed=NOW + 3,
    ))
    assert [(x.order_type, x.time_in_force) for x in (first, second, third)] == [
        ("MARKET", "FOK"), ("LIMIT", "IOC"), ("LIMIT", "IOC"),
    ]
    with pytest.raises(LifecycleSafetyError):
        prepare_close(lifecycle, position="0.00004", client_id=204,
                      anchor=8, bitmap=7, observed=NOW + 3)


def test_partial_ioc_uses_exact_residual_without_rounding(lifecycle):
    seed_filled(lifecycle)
    first = prepare_close(lifecycle)
    dispatch(lifecycle, first, CLOSE_1)
    lifecycle.reconcile(first.intent_id, exact_evidence(CLOSE_1, 201, position="0.0001"))
    lifecycle._now = lambda: NOW + 2
    second = prepare_close(lifecycle, client_id=202, anchor=8, bitmap=5, observed=NOW + 2)
    dispatch(lifecycle, second, CLOSE_2)
    lifecycle.reconcile(second.intent_id, exact_evidence(
        CLOSE_2, 202, filled="0.000063", position="0.000037", observed=NOW + 2,
    ))
    lifecycle._now = lambda: NOW + 3
    third = prepare_close(lifecycle, position="0.000037", client_id=203,
                          anchor=8, bitmap=6, observed=NOW + 3)
    assert third.size == Decimal("0.000037") and third.size_steps == 37


def test_later_close_requires_durable_exact_reconciled_position(tmp_path):
    for index, (drift, restart) in enumerate((
        ("0.00008", True),
        ("0.00002", False),
    )):
        candidate = new_lifecycle(tmp_path / f"position-lineage-{index}.sqlite3")
        seed_filled(candidate)
        first = prepare_close(candidate)
        dispatch(candidate, first, CLOSE_1)
        candidate.reconcile(
            first.intent_id,
            exact_evidence(CLOSE_1, 201, position="0.0001"),
        )
        candidate._now = lambda: NOW + 2
        second = prepare_close(
            candidate, client_id=202, anchor=8, bitmap=5, observed=NOW + 2,
        )
        dispatch(candidate, second, CLOSE_2)
        candidate.reconcile(second.intent_id, exact_evidence(
            CLOSE_2, 202, filled="0.00006", position="0.00004",
            observed=NOW + 2,
        ))
        path = candidate.store.path
        if restart:
            candidate.store.close()
            candidate = new_lifecycle(path, now=NOW + 3)
        else:
            candidate._now = lambda: NOW + 3
        before = candidate.store.all()
        with pytest.raises(LifecycleSafetyError):
            prepare_close(
                candidate, position=drift, client_id=203, anchor=8, bitmap=6,
                observed=NOW + 3,
            )
        after = candidate.store.all()
        assert after == before
        assert all(intent.dispatch_count == 1 for intent in after)
        assert candidate.store.load_outcome() == Outcome.FAILED_HALTED_MANUAL_RECOVERY
        candidate.store.close()


def test_non_step_residual_halts_without_another_dispatch(lifecycle):
    seed_filled(lifecycle)
    first = prepare_close(lifecycle)
    dispatch(lifecycle, first, CLOSE_1)
    lifecycle.reconcile(first.intent_id, exact_evidence(CLOSE_1, 201, position="0.0001"))
    lifecycle._now = lambda: NOW + 2
    with pytest.raises(LifecycleSafetyError):
        prepare_close(lifecycle, position="0.0000375", client_id=202,
                      anchor=8, bitmap=5, observed=NOW + 2)
    assert lifecycle.outcome == Outcome.FAILED_HALTED_MANUAL_RECOVERY


def test_permit_expiry_prevents_delayed_ambiguous_replay(lifecycle, tmp_path):
    intent = open_intent(lifecycle, expires=NOW + 1)
    lifecycle._now = lambda: NOW + 2
    with pytest.raises(LifecycleSafetyError):
        lifecycle.dispatch(intent, SIGNER, lambda _: "late")
    assert lifecycle.store.get(intent.intent_id).dispatch_count == 0
    boundary = new_lifecycle(tmp_path / "deadline-60.sqlite3")
    assert open_intent(boundary, expires=NOW + 60).expires_at == NOW + 60
    boundary.store.close()
    for index, deadline in enumerate((NOW + 61, 2**32 + 5)):
        rejected = new_lifecycle(tmp_path / f"deadline-reject-{index}.sqlite3")
        with pytest.raises(LifecycleSafetyError):
            open_intent(rejected, expires=deadline)
        assert rejected.store.all() == []
        rejected.store.close()


def test_known_open_order_is_cancelled_once_by_exact_id(lifecycle):
    intent = open_intent(lifecycle)
    lifecycle.dispatch(
        intent, SIGNER, lambda _: (_ for _ in ()).throw(TimeoutError()),
    )
    assert lifecycle.store.get(intent.intent_id).order_id is None
    lifecycle._now = lambda: NOW + 1
    open_known = exact_evidence(
        OPENING_ORDER, 101, open_ids=(OPENING_ORDER,), terminal=False,
    )
    assert lifecycle.reconcile(intent.intent_id, open_known) == Outcome.ACTIVE
    assert lifecycle.store.get(intent.intent_id).state == "OPEN_KNOWN"
    calls = []
    cancel(lifecycle, OPENING_ORDER, lambda value: calls.append(value))
    assert lifecycle.store.cancel_states() == ["PENDING_RECONCILIATION"]
    with pytest.raises(LifecycleSafetyError):
        cancel(lifecycle, OPENING_ORDER, lambda value: calls.append(value))
    assert lifecycle.reconcile_cancel(OPENING_ORDER, account(observed_at=NOW + 1))
    assert lifecycle.reconcile(
        intent.intent_id, exact_evidence(OPENING_ORDER, 101),
    ) == Outcome.COMPLETED_NO_FILL_FLAT
    assert len(calls) == 1
    request = calls[0]
    assert request["action"] == CANCEL_ACTION
    assert set(request["body"]) == {"market_id", "order_id", "permit"}
    assert request["body"]["order_id"] == OPENING_ORDER
    assert request["resting_order_id"] == 113 >> 1
    assert request["body"]["market_id"] == request["market_id"] == 1
    assert request["body"]["permit"] == {
        "account": ACCOUNT.lower(), "signer": SIGNER_ADDRESS.lower(),
        "nonce_anchor": "9", "nonce_bitmap_index": 0,
        "deadline": NOW + 40, "signature": None,
    }
    assert request["abi_encoded"].hex() == EXPECTED_CANCEL_ABI
    assert request["action_hash"].hex() == EXPECTED_CANCEL_ACTION_HASH
    assert request["dispatchable"] is False and request["signature"] is None
    cancel_row = lifecycle.store.connection.execute(
        "SELECT order_id, wide_order_id, resting_order_id, payload_digest, "
        "dispatch_count FROM cancels"
    ).fetchone()
    assert cancel_row == (
        OPENING_ORDER, "113", "56", EXPECTED_CANCEL_ACTION_HASH, 1,
    )


def test_ambiguous_cancel_is_never_replayed(lifecycle):
    seed_filled(lifecycle)
    closing = prepare_close(lifecycle)
    dispatch(lifecycle, closing, CLOSE_1)
    assert lifecycle.reconcile(
        closing.intent_id,
        exact_evidence(
            CLOSE_1, 201, position="0.0001", open_ids=(CLOSE_1,),
            terminal=False,
        ),
    ) == Outcome.ACTIVE
    cancel(lifecycle, CLOSE_1, lambda _: (_ for _ in ()).throw(TimeoutError()))
    with pytest.raises(LifecycleSafetyError):
        cancel(lifecycle, CLOSE_1, lambda _: None)
    assert lifecycle.store.cancel_states() == ["AMBIGUOUS"]
    assert lifecycle.finalize(
        account(position=Decimal("0.0001"), repeated_position=Decimal("0.0001"),
                observed_at=NOW + 1),
    ) == Outcome.FAILED_HALTED_MANUAL_RECOVERY


def test_open_terminal_waits_for_exact_cancel_reconciliation(tmp_path):
    for index, ambiguous_cancel in enumerate((False, True)):
        candidate = new_lifecycle(tmp_path / f"cancel-barrier-{index}.sqlite3")
        intent = open_intent(candidate)
        candidate.dispatch(
            intent, SIGNER, lambda _: (_ for _ in ()).throw(TimeoutError()),
        )
        candidate._now = lambda: NOW + 1
        candidate.reconcile(intent.intent_id, exact_evidence(
            OPENING_ORDER, 101, open_ids=(OPENING_ORDER,), terminal=False,
        ))
        if ambiguous_cancel:
            cancel(
                candidate, OPENING_ORDER,
                lambda _: (_ for _ in ()).throw(TimeoutError()),
            )
            expected_cancel_state = "AMBIGUOUS"
        else:
            cancel(candidate, OPENING_ORDER, lambda _: None)
            expected_cancel_state = "PENDING_RECONCILIATION"
        terminal = exact_evidence(OPENING_ORDER, 101)
        with pytest.raises(LifecycleSafetyError):
            candidate.reconcile(intent.intent_id, terminal)
        assert candidate.store.get(intent.intent_id).state == "OPEN_KNOWN"
        assert candidate.store.cancel_states() == [expected_cancel_state]
        assert candidate.outcome == Outcome.ACTIVE
        assert candidate.reconcile_cancel(
            OPENING_ORDER, account(observed_at=NOW + 1),
        )
        assert candidate.reconcile(
            intent.intent_id, terminal,
        ) == Outcome.COMPLETED_NO_FILL_FLAT
        assert candidate.store.cancel_states() == ["TERMINAL"]
        candidate.store.close()


def test_unrelated_order_or_position_drift_halts_without_mutation(lifecycle, tmp_path):
    opening = order_record(OPENING_ORDER, 101)
    other = order_record(OTHER_ORDER, 101)
    contradictory = [
        {"by_id_order": other},
        {"history_orders": (opening, other)},
        {"fills": (FillRecord(OTHER_ORDER, 101),)},
        {"fills": (FillRecord(OPENING_ORDER, 999),)},
        {"history_orders": (replace(opening, client_order_id=999),)},
        {"filled_size": Decimal("0"), "position": Decimal("0"),
         "fills": (FillRecord(OPENING_ORDER, 101),)},
        {"open_orders": (opening, other),
         "terminal": False, "filled_size": Decimal("0"),
         "position": Decimal("0"), "fills": ()},
        {"open_orders": (other,)},
        {"position": Decimal("-0.0001")},
        {"position_market_id": 2},
        {"observed_at": NOW - 6},
    ]
    for index, changes in enumerate(contradictory):
        store = DurableIntentStore(tmp_path / f"contradiction-{index}.sqlite3")
        candidate = Lifecycle(
            store, now=lambda: NOW, router=ROUTER, authorization=AUTHORIZATION,
            expected_account=ACCOUNT, expected_signer=SIGNER_ADDRESS,
        )
        intent = open_intent(candidate)
        dispatch(candidate, intent, OPENING_ORDER)
        candidate._now = lambda: NOW + 1
        evidence = replace(
            exact_evidence(
                OPENING_ORDER, 101, filled="0.0001", position="0.0001",
            ),
            **changes,
        )
        with pytest.raises(LifecycleSafetyError):
            candidate.reconcile(intent.intent_id, evidence)
        assert candidate.outcome == Outcome.FAILED_HALTED_MANUAL_RECOVERY
        with pytest.raises(LifecycleSafetyError):
            cancel(candidate, OPENING_ORDER, lambda _: None)
        store.close()


def test_disconnect_persists_failed_manual_recovery_and_stops_writes(lifecycle):
    seed_filled(lifecycle)
    report = lifecycle.halt_manual(
        account(position=Decimal("0.0001"), open_order_ids=(OPENING_ORDER,)),
        "connectivity_lost",
    )
    assert report["order_ids"] == ["[REDACTED]"]
    path = lifecycle.store.path
    lifecycle.store.close()
    lifecycle.store = DurableIntentStore(path)
    restarted = Lifecycle(
        lifecycle.store, now=lambda: NOW + 2, router=ROUTER,
        authorization=AUTHORIZATION,
        expected_account=ACCOUNT, expected_signer=SIGNER_ADDRESS,
    )
    assert restarted.outcome == Outcome.FAILED_HALTED_MANUAL_RECOVERY
    with pytest.raises(LifecycleSafetyError):
        prepare_close(restarted, observed=NOW + 2)


def test_success_requires_observed_fill_zero_orders_and_exact_flat(lifecycle, tmp_path):
    seed_filled(lifecycle)
    closing = prepare_close(lifecycle)
    dispatch(lifecycle, closing, CLOSE_1)
    lifecycle.reconcile(closing.intent_id, exact_evidence(
        CLOSE_1, 201, filled="0.0001", position="0",
    ))
    lifecycle._now = lambda: NOW + 100
    assert lifecycle.finalize(account(observed_at=NOW)) == Outcome.FAILED_HALTED_MANUAL_RECOVERY
    assert lifecycle.store.load_outcome() == Outcome.FAILED_HALTED_MANUAL_RECOVERY
    with pytest.raises(LifecycleSafetyError):
        prepare_close(lifecycle, observed=NOW + 100)

    store = DurableIntentStore(tmp_path / "success.sqlite3")
    good = Lifecycle(
        store, now=lambda: NOW, router=ROUTER, authorization=AUTHORIZATION,
        expected_account=ACCOUNT, expected_signer=SIGNER_ADDRESS,
    )
    seed_filled(good)
    close = prepare_close(good)
    dispatch(good, close, CLOSE_1)
    good.reconcile(close.intent_id, exact_evidence(
        CLOSE_1, 201, filled="0.0001", position="0",
    ))
    assert good.finalize(account(observed_at=NOW + 1)) == Outcome.SUCCESS_CLOSED_FLAT
    store.close()

    bad = new_lifecycle(tmp_path / "bad-final-identity.sqlite3")
    seed_filled(bad)
    bad_close = prepare_close(bad)
    dispatch(bad, bad_close, CLOSE_1)
    bad.reconcile(bad_close.intent_id, exact_evidence(
        CLOSE_1, 201, filled="0.0001", position="0",
    ))
    assert bad.finalize(
        account(signer=OTHER_SIGNER, observed_at=NOW + 1),
    ) == Outcome.FAILED_HALTED_MANUAL_RECOVERY
    assert bad.store.load_outcome() == Outcome.FAILED_HALTED_MANUAL_RECOVERY
    bad.store.close()


def test_final_flat_must_match_durable_reconciled_position(tmp_path):
    candidate = new_lifecycle(tmp_path / "final-position-lineage.sqlite3")
    seed_filled(candidate)
    closing = prepare_close(candidate)
    dispatch(candidate, closing, CLOSE_1)
    candidate.reconcile(
        closing.intent_id,
        exact_evidence(CLOSE_1, 201, position="0.0001"),
    )
    path = candidate.store.path
    candidate.store.close()
    candidate = new_lifecycle(path, now=NOW + 2)
    assert candidate.store.latest_reconciled_position() == Decimal("0.0001")
    assert candidate.finalize(
        account(observed_at=NOW + 2),
    ) == Outcome.FAILED_HALTED_MANUAL_RECOVERY
    assert candidate.store.load_outcome() == Outcome.FAILED_HALTED_MANUAL_RECOVERY
    with pytest.raises(LifecycleSafetyError):
        prepare_close(candidate, position="0.0001", observed=NOW + 2)
    candidate.store.close()

    for index, corrupt_value in enumerate((None, "not-a-position")):
        corrupt = new_lifecycle(tmp_path / f"corrupt-final-lineage-{index}.sqlite3")
        seed_filled(corrupt)
        close = prepare_close(corrupt)
        dispatch(corrupt, close, CLOSE_1)
        corrupt.reconcile(close.intent_id, exact_evidence(
            CLOSE_1, 201, filled="0.0001", position="0",
        ))
        with corrupt.store.connection:
            if corrupt_value is None:
                corrupt.store.connection.execute(
                    "DELETE FROM terminal WHERE key=?", (f"position:{close.intent_id}",),
                )
            else:
                corrupt.store.connection.execute(
                    "UPDATE terminal SET value=? WHERE key=?",
                    (corrupt_value, f"position:{close.intent_id}"),
                )
        assert corrupt.finalize(
            account(observed_at=NOW + 1),
        ) == Outcome.FAILED_HALTED_MANUAL_RECOVERY
        assert corrupt.store.load_outcome() == Outcome.FAILED_HALTED_MANUAL_RECOVERY
        corrupt.store.close()


def test_minimum_size_and_usd_cap_are_invariants(lifecycle, tmp_path):
    assert ready(lifecycle).size == market().minimum
    with pytest.raises(LifecycleSafetyError):
        lifecycle.preflight(
            market(minimum=Decimal("0.01")), account(),
            bbo(ask=Decimal("60000"), ask_depth=Decimal("1")),
        )
    for kwargs in (
        {"market_id": 2**16, "size_steps": 1, "price_ticks": 1},
        {"market_id": 1, "size_steps": 100_000_000_000, "price_ticks": 1},
        {"market_id": 1, "size_steps": 1, "price_ticks": 2**24},
    ):
        with pytest.raises(LifecycleSafetyError):
            pack_order_data(
                **kwargs, side="BUY", post_only=False, reduce_only=False,
                order_type="MARKET", time_in_force="FOK",
            )
    intent = open_intent(lifecycle)
    dispatch(lifecycle, intent, OPENING_ORDER)
    lifecycle._now = lambda: NOW + 1
    lifecycle.reconcile(intent.intent_id, exact_evidence(
        OPENING_ORDER, 101, filled="0.0001", position="0.0001",
    ))
    with pytest.raises(LifecycleSafetyError):
        prepare_close(lifecycle, client_id=202, anchor=7, bitmap=3)
    maximum = new_lifecycle(tmp_path / "uint64-max.sqlite3")
    max_intent = open_intent(maximum, client_id=2**64 - 1)
    assert maximum.store.get(max_intent.intent_id).client_order_id == 2**64 - 1
    maximum.store.close()
    overflow = new_lifecycle(tmp_path / "uint64-overflow.sqlite3")
    with pytest.raises(LifecycleSafetyError):
        open_intent(overflow, client_id=2**64)
    assert overflow.store.all() == []
    overflow.store.close()


def test_preflight_is_immutable_and_bound_to_issuing_lifecycle(lifecycle, tmp_path):
    forged_values = (
        lambda value: replace(value),
        lambda value: replace(value, size=Decimal("0.0002")),
        lambda value: replace(value, market=market(minimum=Decimal("0.0002"))),
        lambda value: replace(value, account=account(account=OTHER_ACCOUNT)),
        lambda value: replace(value, bbo=bbo(ask=Decimal("77963.5"))),
    )
    for forge in forged_values:
        validated = ready(lifecycle)
        with pytest.raises(LifecycleSafetyError):
            lifecycle.prepare_open(forge(validated), 101, 7, 3, NOW + 30)
        assert lifecycle.store.all() == []

    validated = ready(lifecycle)
    other = new_lifecycle(tmp_path / "cross-lifecycle.sqlite3")
    with pytest.raises(LifecycleSafetyError):
        other.prepare_open(validated, 101, 7, 3, NOW + 30)
    assert other.store.all() == []
    other.store.close()

    intent = lifecycle.prepare_open(validated, 101, 7, 3, NOW + 30)
    assert intent.size == market().minimum
    assert intent.price == Decimal("78217.0")
    calls = []
    with pytest.raises(LifecycleSafetyError):
        lifecycle.dispatch(
            replace(intent, size=Decimal("0.0002")), SIGNER,
            lambda request: calls.append(request),
        )
    assert calls == []
    assert lifecycle.store.get(intent.intent_id).dispatch_count == 0


def test_failed_refresh_or_consume_revokes_preflight_token(tmp_path):
    invalid_accounts = (
        account(position=Decimal("0.0001"), repeated_position=Decimal("0.0001")),
        account(observed_at=NOW - 6),
        account(signer_status="INACTIVE"),
    )
    for index, invalid_account in enumerate(invalid_accounts):
        candidate = new_lifecycle(tmp_path / f"revoked-refresh-{index}.sqlite3")
        old = ready(candidate)
        with pytest.raises(LifecycleSafetyError):
            candidate.preflight(market(), invalid_account, bbo())
        with pytest.raises(LifecycleSafetyError):
            candidate.prepare_open(old, 101, 7, 3, NOW + 30)
        assert candidate.store.all() == []
        candidate.store.close()

    candidate = new_lifecycle(tmp_path / "revoked-consume.sqlite3")
    original = ready(candidate)
    with pytest.raises(LifecycleSafetyError):
        candidate.prepare_open(
            replace(original, size=Decimal("0.0002")), 101, 7, 3, NOW + 30,
        )
    with pytest.raises(LifecycleSafetyError):
        candidate.prepare_open(original, 101, 7, 3, NOW + 30)
    assert candidate.store.all() == []
    candidate.store.close()


def test_dispatch_rejects_stale_bound_open_snapshot(lifecycle):
    opening = open_intent(lifecycle)
    lifecycle._now = lambda: NOW + 6
    open_calls = []
    with pytest.raises(LifecycleSafetyError):
        lifecycle.dispatch(opening, SIGNER, lambda value: open_calls.append(value))
    assert open_calls == []
    assert lifecycle.store.get(opening.intent_id).dispatch_count == 0


def test_dispatch_rejects_stale_bound_close_snapshot(lifecycle):
    seed_filled(lifecycle)
    closing = prepare_close(lifecycle)
    lifecycle._now = lambda: NOW + 7
    close_calls = []
    with pytest.raises(LifecycleSafetyError):
        lifecycle.dispatch(closing, SIGNER, lambda value: close_calls.append(value))
    assert close_calls == []
    assert lifecycle.store.get(closing.intent_id).dispatch_count == 0


def test_secrets_signatures_payloads_and_identities_are_redacted(lifecycle):
    intent = open_intent(lifecycle)
    dispatch(lifecycle, intent, OPENING_ORDER)
    report = lifecycle.halt_manual(
        account(position=Decimal("0.0001"), open_order_ids=(OPENING_ORDER,)),
        "fixture-secret-signature-payload",
    )
    rendered = repr(report) + repr(lifecycle.store.redacted_evidence())
    for forbidden in ("fixture-secret", "signature", "payload", OPENING_ORDER, ROUTER):
        assert forbidden not in rendered
    assert "official RISEx testnet UI" in report["manual_recovery"]


def test_official_encoding_rejects_header_and_uint88_overflow():
    with pytest.raises(LifecycleSafetyError):
        encode_place_action(order_data=1, client_order_id=1, ttl_units=1)
    with pytest.raises(LifecycleSafetyError):
        encode_place_action(order_data=2**86, client_order_id=1)


def test_prepared_intent_has_no_terminal_bypass(lifecycle):
    intent = open_intent(lifecycle)
    assert not hasattr(lifecycle, "mark_dispatched")
    assert not hasattr(lifecycle, "mark_terminal")
    lifecycle.observed_opening_fill = True
    with pytest.raises(LifecycleSafetyError):
        lifecycle.dispatch(intent, object(), lambda _: "must-not-dispatch")
    assert lifecycle.store.get(intent.intent_id).dispatch_count == 0
    with pytest.raises(LifecycleSafetyError):
        lifecycle.dispatch(
            intent, SyntheticSigner(OTHER_SIGNER), lambda _: "must-not-dispatch",
        )
    with pytest.raises(LifecycleSafetyError):
        Lifecycle(
            lifecycle.store, now=lambda: NOW, router=ROUTER,
            authorization=AUTHORIZATION, expected_account=OTHER_ACCOUNT,
            expected_signer=SIGNER_ADDRESS,
        )
    lifecycle._now = lambda: NOW + 1
    assert lifecycle.finalize(account(observed_at=NOW + 1)) == Outcome.FAILED_HALTED_MANUAL_RECOVERY
    assert lifecycle.store.get(intent.intent_id).dispatch_count == 0


def test_durable_identity_binding_cannot_be_reinitialized(tmp_path):
    path = tmp_path / "missing-both-bindings.sqlite3"
    original = new_lifecycle(path)
    intent = open_intent(original)
    with original.store.connection:
        original.store.connection.execute(
            "DELETE FROM terminal WHERE key IN ('account', 'signer')"
        )
    original.store.close()
    store = DurableIntentStore(path)
    before = store.all()
    with pytest.raises(LifecycleSafetyError):
        Lifecycle(
            store, now=lambda: NOW, router=ROUTER, authorization=AUTHORIZATION,
            expected_account=OTHER_ACCOUNT, expected_signer=OTHER_SIGNER,
        )
    assert store.all() == before
    assert store.get(intent.intent_id).dispatch_count == 0
    assert dict(store.connection.execute(
        "SELECT key, value FROM terminal WHERE key IN ('account', 'signer')"
    )) == {}
    store.close()

    path = tmp_path / "missing-one-binding.sqlite3"
    original = new_lifecycle(path)
    open_intent(original)
    with original.store.connection:
        original.store.connection.execute("DELETE FROM terminal WHERE key='signer'")
    original.store.close()
    store = DurableIntentStore(path)
    before = store.all()
    with pytest.raises(LifecycleSafetyError):
        Lifecycle(
            store, now=lambda: NOW, router=ROUTER, authorization=AUTHORIZATION,
            expected_account=ACCOUNT, expected_signer=SIGNER_ADDRESS,
        )
    assert store.all() == before
    assert dict(store.connection.execute(
        "SELECT key, value FROM terminal WHERE key IN ('account', 'signer')"
    )) == {"account": ACCOUNT.lower()}
    store.close()

    for index, evidence_kind in enumerate(("terminal", "cancel")):
        store = DurableIntentStore(tmp_path / f"unbound-{evidence_kind}-{index}.sqlite3")
        with store.connection:
            if evidence_kind == "terminal":
                store.connection.execute(
                    "INSERT INTO terminal VALUES ('opening_fill', '1')"
                )
            else:
                store.connection.execute(
                    "INSERT INTO cancels VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (OPENING_ORDER, 1, "113", "56", 9, 0, "digest", NOW + 30,
                     "AMBIGUOUS", 1),
                )
        with pytest.raises(LifecycleSafetyError):
            Lifecycle(
                store, now=lambda: NOW, router=ROUTER,
                authorization=AUTHORIZATION, expected_account=ACCOUNT,
                expected_signer=SIGNER_ADDRESS,
            )
        assert dict(store.connection.execute(
            "SELECT key, value FROM terminal WHERE key IN ('account', 'signer')"
        )) == {}
        store.close()


def test_durable_contract_binding_cannot_be_substituted(tmp_path):
    for index, overrides in enumerate((
        {"router": OTHER_ROUTER, "authorization": AUTHORIZATION},
        {"router": ROUTER, "authorization": OTHER_AUTHORIZATION},
    )):
        path = tmp_path / f"contract-substitution-{index}.sqlite3"
        original = new_lifecycle(path)
        intent = open_intent(original)
        original.store.close()
        store = DurableIntentStore(path)
        before = store.all()
        with pytest.raises(LifecycleSafetyError):
            Lifecycle(
                store, now=lambda: NOW, expected_account=ACCOUNT,
                expected_signer=SIGNER_ADDRESS, **overrides,
            )
        assert store.all() == before
        assert store.get(intent.intent_id).dispatch_count == 0
        store.close()

    for index, missing_keys in enumerate((("router",), ("router", "authorization"))):
        path = tmp_path / f"missing-contract-binding-{index}.sqlite3"
        original = new_lifecycle(path)
        intent = open_intent(original)
        with original.store.connection:
            placeholders = ",".join("?" for _ in missing_keys)
            original.store.connection.execute(
                f"DELETE FROM terminal WHERE key IN ({placeholders})", missing_keys,
            )
        original.store.close()
        store = DurableIntentStore(path)
        before_bindings = dict(store.connection.execute(
            "SELECT key, value FROM terminal "
            "WHERE key IN ('account', 'signer', 'router', 'authorization')"
        ))
        with pytest.raises(LifecycleSafetyError):
            Lifecycle(
                store, now=lambda: NOW, router=ROUTER,
                authorization=AUTHORIZATION, expected_account=ACCOUNT,
                expected_signer=SIGNER_ADDRESS,
            )
        assert store.get(intent.intent_id).dispatch_count == 0
        assert dict(store.connection.execute(
            "SELECT key, value FROM terminal "
            "WHERE key IN ('account', 'signer', 'router', 'authorization')"
        )) == before_bindings
        store.close()

    path = tmp_path / "exact-contract-restart.sqlite3"
    original = new_lifecycle(path)
    intent = open_intent(original)
    original.store.close()
    restarted = new_lifecycle(path)
    restarted.dispatch(intent, SIGNER, lambda _: AUTHORITATIVE_ORDER)
    assert restarted.store.get(intent.intent_id).dispatch_count == 1
    restarted.store.close()


def test_dispatching_crash_reconciles_without_replay(lifecycle):
    class SyntheticCrash(BaseException):
        pass

    intent = open_intent(lifecycle)
    with pytest.raises(SyntheticCrash):
        lifecycle.dispatch(intent, SIGNER, lambda _: (_ for _ in ()).throw(SyntheticCrash()))
    persisted = lifecycle.store.get(intent.intent_id)
    assert persisted.state == "DISPATCHING" and persisted.dispatch_count == 1
    with pytest.raises(LifecycleSafetyError):
        lifecycle.dispatch(intent, SIGNER, lambda _: "replay")
    lifecycle._now = lambda: NOW + 31
    no_identity = Evidence(
        account=ACCOUNT, signer=SIGNER_ADDRESS, signer_status="ACTIVE",
        terminal=True, filled_size=Decimal("0"), position=Decimal("0"),
        observed_at=NOW + 31, position_market_id=1,
    )
    assert lifecycle.reconcile(intent.intent_id, no_identity) == Outcome.COMPLETED_NO_FILL_FLAT


def test_cancel_dispatching_crash_is_durable_and_never_replayed(lifecycle):
    class SyntheticCrash(BaseException):
        pass

    intent = open_intent(lifecycle)
    lifecycle.dispatch(
        intent, SIGNER, lambda _: (_ for _ in ()).throw(TimeoutError()),
    )
    lifecycle._now = lambda: NOW + 1
    lifecycle.reconcile(
        intent.intent_id,
        exact_evidence(
            OPENING_ORDER, 101, open_ids=(OPENING_ORDER,), terminal=False,
        ),
    )
    with pytest.raises(SyntheticCrash):
        cancel(
            lifecycle, OPENING_ORDER,
            lambda _: (_ for _ in ()).throw(SyntheticCrash()),
        )
    assert lifecycle.store.cancel_state(OPENING_ORDER) == "DISPATCHING"
    assert lifecycle.store.cancel_count(OPENING_ORDER) == 1
    path = lifecycle.store.path
    lifecycle.store.close()
    restarted = new_lifecycle(path, now=NOW + 1)
    lifecycle.store = restarted.store
    replay_calls = []
    with pytest.raises(LifecycleSafetyError):
        cancel(restarted, OPENING_ORDER, lambda value: replay_calls.append(value))
    assert replay_calls == []
    assert restarted.store.cancel_count(OPENING_ORDER) == 1
    assert restarted.reconcile_cancel(
        OPENING_ORDER, account(observed_at=NOW + 1),
    )


def test_official_order_identity_chain_contract_is_explicit(lifecycle):
    intent = open_intent(lifecycle)
    lifecycle.dispatch(
        intent, SIGNER, lambda _: (_ for _ in ()).throw(TimeoutError()),
    )
    lifecycle._now = lambda: NOW + 1
    evidence = exact_evidence(
        OPENING_ORDER, 101, open_ids=(OPENING_ORDER,), terminal=False,
    )
    assert lifecycle.reconcile(intent.intent_id, evidence) == Outcome.ACTIVE
    assert lifecycle.store.order_identity(OPENING_ORDER) == OrderRecord(
        OPENING_ORDER, 113, 113 >> 1, 101,
    )


def test_official_cancel_action_encoding_is_explicit():
    encoded, action_hash = encode_cancel_action(
        market_id=1, resting_order_id=113 >> 1,
    )
    assert len(encoded) == 96
    assert encoded[32:64] == (1).to_bytes(32, "big")
    assert encoded[64:96] == (113 >> 1).to_bytes(32, "big")
    assert encoded.hex() == EXPECTED_CANCEL_ABI
    assert action_hash.hex() == EXPECTED_CANCEL_ACTION_HASH
