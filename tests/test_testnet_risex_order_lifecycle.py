from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from risex_farmer.testnet_risex_order_lifecycle import (
    AccountState,
    BBO,
    DurableIntentStore,
    Evidence,
    Lifecycle,
    LifecycleSafetyError,
    MarketState,
    Outcome,
)


NOW = 1_800_000_000
ROUTER = "0x" + "22" * 20


def market(**changes):
    value = MarketState(
        host="api.testnet.rise.trade",
        chain_id=11_155_931,
        domain_name="RISEx",
        domain_version="1",
        router=ROUTER,
        market_id=1,
        symbol="BTC/USDC",
        active=True,
        unlocked=True,
        tick=Decimal("0.1"),
        step=Decimal("0.000001"),
        minimum=Decimal("0.0001"),
        observed_at=NOW,
    )
    return replace(value, **changes)


def account(**changes):
    value = AccountState(
        signer_status="ACTIVE",
        position=Decimal("0"),
        open_order_ids=(),
        repeated_open_order_ids=(),
        repeated_position=Decimal("0"),
        unexplained=False,
        observed_at=NOW,
    )
    return replace(value, **changes)


def bbo(**changes):
    value = BBO(
        bid=Decimal("77982.9"),
        ask=Decimal("77983.0"),
        bid_depth=Decimal("0.000646"),
        ask_depth=Decimal("0.000646"),
        observed_at=NOW,
    )
    return replace(value, **changes)


@pytest.fixture
def lifecycle(tmp_path):
    store = DurableIntentStore(tmp_path / "fixture.sqlite3")
    instance = Lifecycle(store, now=lambda: NOW, router=ROUTER)
    yield instance
    store.close()


def ready(lifecycle):
    return lifecycle.preflight(market(), account(), bbo())


def open_intent(lifecycle, *, client_id=101, nonce=7, expires=NOW + 30):
    return lifecycle.prepare_open(ready(lifecycle), client_id, nonce, expires)


def test_preflight_blocks_before_signer_or_post(lifecycle):
    calls = []
    invalid = [
        (market(active=False), account(), bbo()),
        (market(observed_at=NOW - 6), account(), bbo()),
        (market(tick=Decimal("0.3")), account(), bbo()),
        (market(), account(signer_status="INACTIVE"), bbo()),
        (market(), account(repeated_position=Decimal("0.0001")), bbo()),
        (market(), account(), bbo(ask_depth=Decimal("0.000099"))),
        (market(), account(), bbo(bid_depth=Decimal("0.000099"))),
        (market(), account(), bbo(ask=Decimal("77983.05"))),
    ]
    for bad_market, bad_account, bad_bbo in invalid:
        with pytest.raises(LifecycleSafetyError):
            lifecycle.run_open(
                bad_market, bad_account, bad_bbo, 1, 1, NOW + 30,
                signer_loader=lambda: calls.append("signer"),
                dispatch=lambda payload: calls.append(payload),
            )
    assert calls == []
    assert lifecycle.store.all() == []


def test_intent_nonce_and_digest_are_durable_before_dispatch(lifecycle):
    intent = open_intent(lifecycle)
    path = lifecycle.store.path
    lifecycle.store.close()
    lifecycle.store = DurableIntentStore(path)
    seen = []
    lifecycle.dispatch(intent, "synthetic", lambda payload: seen.append(lifecycle.store.get(intent.intent_id)))
    persisted = seen[0]
    assert persisted.state == "DISPATCHING"
    assert persisted.nonce == 7 and persisted.client_order_id == 101
    assert len(persisted.payload_digest) == 64
    assert persisted.payload_digest == lifecycle.store.get(intent.intent_id).payload_digest


def test_ambiguous_open_is_never_replayed(lifecycle):
    intent = open_intent(lifecycle)
    lifecycle.dispatch(intent, "synthetic", lambda _: (_ for _ in ()).throw(TimeoutError()))
    assert lifecycle.store.get(intent.intent_id).state == "AMBIGUOUS"
    with pytest.raises(LifecycleSafetyError):
        lifecycle.dispatch(intent, "synthetic", lambda _: None)
    assert lifecycle.store.get(intent.intent_id).dispatch_count == 1
    contradictory = Evidence("delayed-order", 101, True, Decimal("0"), Decimal("0"), ("delayed-order",), NOW + 31)
    lifecycle._now = lambda: NOW + 31
    with pytest.raises(LifecycleSafetyError):
        lifecycle.reconcile(intent.intent_id, contradictory)
    delayed = Evidence("delayed-order", 101, True, Decimal("0"), Decimal("0"), (), NOW + 31)
    assert lifecycle.reconcile(intent.intent_id, delayed) == Outcome.COMPLETED_NO_FILL_FLAT


def test_open_is_exact_minimum_price_bounded_market_fok(lifecycle):
    intent = open_intent(lifecycle)
    assert intent.kind == "OPEN" and intent.side == "BUY"
    assert intent.order_type == "MARKET" and intent.time_in_force == "FOK"
    assert intent.size == Decimal("0.0001") and not intent.reduce_only
    assert intent.price == Decimal("78217.0")
    assert (intent.size_steps, intent.price_ticks) == (100, 782170)
    assert lifecycle.unsigned_action(intent.intent_id)["action"] == "RISE_PERPS_PLACE_ORDER_V1"
    assert intent.size * intent.price <= Decimal("500")


def test_fok_no_fill_finishes_flat_without_close_acceptance(lifecycle):
    intent = open_intent(lifecycle)
    lifecycle.mark_dispatched(intent.intent_id, order_id="fixture-order")
    lifecycle._now = lambda: NOW + 31
    result = lifecycle.reconcile(intent.intent_id, Evidence.terminal_flat("fixture-order", 101, NOW + 31))
    assert result == Outcome.COMPLETED_NO_FILL_FLAT
    assert lifecycle.close_count == 0


def test_first_close_uses_exact_authoritative_size_market_fok(lifecycle):
    lifecycle.observed_opening_fill = True
    intent = lifecycle.prepare_close(market(), account(position=Decimal("0.0001")), bbo(), 201, 8, NOW + 30)
    assert (intent.side, intent.size, intent.order_type, intent.time_in_force) == ("SELL", Decimal("0.0001"), "MARKET", "FOK")
    assert intent.reduce_only and intent.source_position == Decimal("0.0001")


def test_close_fallbacks_use_fresh_state_limit_ioc_and_stop_at_three(lifecycle):
    lifecycle.observed_opening_fill = True
    first = lifecycle.prepare_close(market(), account(position=Decimal("0.0001"), observed_at=NOW), bbo(), 201, 8, NOW + 30)
    lifecycle.mark_terminal(first.intent_id)
    lifecycle._now = lambda: NOW + 1
    second = lifecycle.prepare_close(market(), account(position=Decimal("0.0001"), observed_at=NOW + 1), bbo(observed_at=NOW + 1), 202, 9, NOW + 31)
    lifecycle.mark_terminal(second.intent_id)
    lifecycle._now = lambda: NOW + 2
    third = lifecycle.prepare_close(market(), account(position=Decimal("0.00004"), observed_at=NOW + 2), bbo(observed_at=NOW + 2), 203, 10, NOW + 32)
    lifecycle.mark_terminal(third.intent_id)
    assert [(x.order_type, x.time_in_force) for x in (first, second, third)] == [("MARKET", "FOK"), ("LIMIT", "IOC"), ("LIMIT", "IOC")]
    with pytest.raises(LifecycleSafetyError):
        lifecycle.prepare_close(market(), account(position=Decimal("0.00001"), observed_at=NOW + 3), bbo(observed_at=NOW + 3), 204, 11, NOW + 33)


def test_partial_ioc_uses_exact_residual_without_rounding(lifecycle):
    lifecycle.observed_opening_fill = True
    first = lifecycle.prepare_close(market(), account(position=Decimal("0.0001")), bbo(), 201, 8, NOW + 30)
    lifecycle.mark_terminal(first.intent_id)
    lifecycle._now = lambda: NOW + 1
    second = lifecycle.prepare_close(market(), account(position=Decimal("0.000037"), observed_at=NOW + 1), bbo(observed_at=NOW + 1), 202, 9, NOW + 31)
    assert second.size == Decimal("0.000037")


def test_non_step_residual_halts_without_another_dispatch(lifecycle):
    lifecycle.observed_opening_fill = True
    with pytest.raises(LifecycleSafetyError):
        lifecycle.prepare_close(market(), account(position=Decimal("0.0000375")), bbo(), 201, 8, NOW + 30)
    assert lifecycle.store.all() == []
    assert lifecycle.outcome == Outcome.FAILED_HALTED_MANUAL_RECOVERY


def test_permit_expiry_prevents_delayed_ambiguous_replay(lifecycle):
    intent = open_intent(lifecycle, expires=NOW + 1)
    lifecycle._now = lambda: NOW + 2
    with pytest.raises(LifecycleSafetyError):
        lifecycle.dispatch(intent, "synthetic", lambda _: None)
    assert lifecycle.store.get(intent.intent_id).dispatch_count == 0


def test_known_open_order_is_cancelled_once_by_exact_id(lifecycle):
    intent = open_intent(lifecycle)
    lifecycle.mark_dispatched(intent.intent_id, order_id="fixture-order")
    calls = []
    lifecycle.cancel_known("fixture-order", lambda order_id: calls.append(order_id))
    with pytest.raises(LifecycleSafetyError):
        lifecycle.cancel_known("fixture-order", lambda order_id: calls.append(order_id))
    assert calls == ["fixture-order"]


def test_ambiguous_cancel_is_never_replayed(lifecycle):
    intent = open_intent(lifecycle)
    lifecycle.mark_dispatched(intent.intent_id, order_id="fixture-order")
    lifecycle.cancel_known("fixture-order", lambda _: (_ for _ in ()).throw(TimeoutError()))
    with pytest.raises(LifecycleSafetyError):
        lifecycle.cancel_known("fixture-order", lambda _: None)
    assert lifecycle.store.cancel_count("fixture-order") == 1


def test_unrelated_order_or_position_drift_halts_without_mutation(lifecycle):
    with pytest.raises(LifecycleSafetyError):
        lifecycle.preflight(market(), account(open_order_ids=("unrelated",), repeated_open_order_ids=("unrelated",)), bbo())
    lifecycle.observed_opening_fill = True
    with pytest.raises(LifecycleSafetyError):
        lifecycle.prepare_close(market(), account(position=Decimal("-0.0002")), bbo(), 201, 8, NOW + 30)
    assert lifecycle.store.all() == []


def test_disconnect_persists_failed_manual_recovery_and_stops_writes(lifecycle):
    lifecycle.observed_opening_fill = True
    report = lifecycle.halt_manual(account(position=Decimal("0.0001"), open_order_ids=("fixture-order",)), "connectivity_lost")
    assert lifecycle.outcome == Outcome.FAILED_HALTED_MANUAL_RECOVERY
    assert report["position"] == "0.0001" and report["order_ids"] == ["[REDACTED]"]
    with pytest.raises(LifecycleSafetyError):
        lifecycle.prepare_close(market(), account(position=Decimal("0.0001")), bbo(), 201, 8, NOW + 30)


def test_success_requires_observed_fill_zero_orders_and_exact_flat(lifecycle):
    opening = open_intent(lifecycle)
    lifecycle.mark_dispatched(opening.intent_id, order_id="filled-order")
    filled = Evidence("filled-order", 101, True, Decimal("0.0001"), Decimal("0.0001"), (), NOW + 31)
    lifecycle._now = lambda: NOW + 31
    lifecycle.reconcile(opening.intent_id, filled)
    closing = lifecycle.prepare_close(market(observed_at=NOW + 31), account(position=Decimal("0.0001"), observed_at=NOW + 31), bbo(observed_at=NOW + 31), 201, 8, NOW + 40)
    lifecycle.mark_terminal(closing.intent_id)
    assert lifecycle.finalize(account(position=Decimal("0.000001"))) != Outcome.SUCCESS_CLOSED_FLAT
    assert lifecycle.finalize(account(open_order_ids=("fixture-order",))) != Outcome.SUCCESS_CLOSED_FLAT
    assert lifecycle.finalize(account()) == Outcome.SUCCESS_CLOSED_FLAT


def test_minimum_size_and_usd_cap_are_invariants(lifecycle):
    assert ready(lifecycle).size == market().minimum
    with pytest.raises(LifecycleSafetyError):
        lifecycle.preflight(market(minimum=Decimal("0.01")), account(), bbo(ask=Decimal("60000"), ask_depth=Decimal("1")))
    with pytest.raises(LifecycleSafetyError):
        lifecycle.preflight(market(step=Decimal("0.00003")), account(), bbo())


def test_secrets_signatures_payloads_and_identities_are_redacted(lifecycle):
    intent = open_intent(lifecycle)
    lifecycle.mark_dispatched(intent.intent_id, order_id="fixture-order")
    report = lifecycle.halt_manual(account(position=Decimal("0.0001"), open_order_ids=("fixture-order",)), "fixture-secret-signature-payload")
    rendered = repr(report) + repr(lifecycle.store.redacted_evidence())
    for forbidden in ("fixture-secret", "signature", "payload", "fixture-order", ROUTER):
        assert forbidden not in rendered
    assert "official RISEx testnet UI" in report["manual_recovery"]
