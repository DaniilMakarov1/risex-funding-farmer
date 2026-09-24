from __future__ import annotations

import json
import time
from decimal import Decimal

import pytest

from risex_spread_shadow.hood_handoff.stream_evidence import StreamEvidenceJournal, _safe_projection
from risex_spread_shadow.hood_handoff.stream_measurement import StreamIdentity, StreamProjection
from risex_spread_shadow.hood_handoff.stream_state import ReadStreamState
from risex_spread_shadow.hood_handoff.offline_observer import ObserverLimits


def identity():
    return StreamIdentity.from_config(dict(market_id=1, market_symbol="BTC",
        source_account_index=27331, receiver_account_index=27337, api_key_index=4,
        environment="robinhood", api_base_url="https://api.rh.lighter.xyz", chain_id=466324))


def transaction(**changes):
    return dict(hash="0x" + "a" * 64, account_index=27331, nonce=42, status=2,
                queued_at=1790247747123, executed_at=1790247747423,
                transaction_time=1790247747423000, sequence_index=321,
                block_height=12345, info="PRIVATE_SIGNED_BODY", event_info="PRIVATE_DETAIL",
                **changes)


def frame(tx):
    return json.dumps(dict(type="update/account_tx", channel="account_tx:27331", txs=[tx])).encode()


def test_transaction_projection_preserves_raw_units_without_payload_or_order_proof():
    observer = StreamProjection(identity())
    row, = observer.feed(frame(transaction()), 10)
    saved = _safe_projection(row, None)
    assert saved["venue_queued_at_raw"] == 1790247747123
    assert saved["venue_executed_at_raw"] == 1790247747423
    assert saved["venue_transaction_time_raw"] == 1790247747423000
    assert saved["venue_sequence_index_raw"] == 321
    assert saved["receive_monotonic_seconds"] == 10
    assert saved["execution_proof"] is False
    assert "unverified" in saved["venue_clock_basis"]
    assert "PRIVATE" not in repr(row) + repr(saved)
    assert observer.order_events == 0 and not observer.private_snapshot_accounts
    assert observer.transaction_events == 1


@pytest.mark.parametrize("field,value", [
    ("account_index", True), ("account_index", 27337), ("account_index", "27331"),
    ("hash", "PRIVATE"), ("hash", "a" * 129), ("status", True),
    ("nonce", "42"), ("nonce", -1), ("status", 2**64),
])
def test_invalid_transaction_identity_never_becomes_evidence(field, value):
    tx = transaction(); tx[field] = value
    observer = StreamProjection(identity())
    assert observer.feed(frame(tx), 10) == []
    assert observer.transaction_invalid == 1
    assert observer.malformed == 0  # Diagnostic schema drift cannot veto admission.


@pytest.mark.parametrize("value", [None, True, -1, 1.1, "1790247747123", 2**64])
def test_invalid_raw_times_are_unknown_not_converted_or_zero(value):
    tx = transaction(); tx["queued_at"] = value
    row, = StreamProjection(identity()).feed(frame(tx), 10)
    assert "venue_queued_at_raw" not in _safe_projection(row, None)
    row["venue_queued_at_raw"] = value
    assert "venue_queued_at_raw" not in _safe_projection(row, None)


@pytest.mark.asyncio
async def test_transaction_stream_cannot_admit_an_order_and_is_durable_account_scope(tmp_path):
    sink = StreamEvidenceJournal(tmp_path / "stream-events.jsonl", market_id=1, accounts=(27331, 27337))
    state = ReadStreamState(identity(), evidence=sink)
    await sink.start(); await state.connected_now()
    now = time.monotonic()
    await state.feed(json.dumps(dict(type="subscribed/account_tx", channel="account_tx:27331")).encode(), now)
    await state.feed(frame(transaction()), now + .001)
    assert not state.subscription_ready(now + .001)
    assert state.exact_orders == {} and state.reads.orders == {}
    assert not state.exact_terminal_hint(27331, 42, None, now + .001)
    await sink.close(reason="stopped")
    rows = [json.loads(line) for line in sink.path.read_text().splitlines()]
    tx, = [row for row in rows if row["kind"] == "transaction"]
    assert "market_id" not in tx  # Account channel includes other markets.
    assert tx["nonce"] == 42 and not tx["execution_proof"]
    assert "PRIVATE" not in sink.path.read_text()
    assert rows[-1]["complete"] is True


def test_order_venue_times_survive_projection_to_durable_evidence():
    raw = dict(owner_account_index=27331, market_index=1, client_order_index=99,
               order_id="12345", status="open", filled_base_amount="0", remaining_base_amount="0.0002",
               timestamp=1790247747123, created_at=1790247747000, updated_at=1790247747123,
               transaction_time=1790247747123000, block_height=12345, auth="PRIVATE")
    observer = StreamProjection(identity())
    row, = observer.feed(json.dumps(dict(type="update/account_all_orders", channel="account_all_orders:27331",
                                        orders={"1": [raw]})).encode(), 10)
    saved = _safe_projection(row, None)
    assert saved["venue_created_at_raw"] == 1790247747000
    assert saved["venue_transaction_time_raw"] == 1790247747123000
    assert saved["venue_block_height_raw"] == 12345
    assert saved["receive_monotonic_seconds"] == 10
    assert "PRIVATE" not in repr(saved)
    # Repeated venue fields cannot refresh an order's local observation age.
    assert observer.feed(json.dumps(dict(type="update/account_all_orders", channel="account_all_orders:27331",
                                        orders={"1": [raw]})).encode(), 11) == []


def test_transaction_projection_is_bounded_and_unknown_channels_are_ignored():
    observer = StreamProjection(identity(), ObserverLimits(max_frame_bytes=8 * 1024 * 1024))
    assert observer.feed(json.dumps(dict(type="update/account_tx", channel="account_tx:27331",
                                        txs=[transaction()] * 513)).encode(), 10) == []
    assert observer.transaction_invalid == 1 and observer.malformed == 0
    assert observer.feed(json.dumps(dict(type="update/account_tx", channel="account_tx:999",
                                        txs=[transaction()])).encode(), 11) == []


@pytest.mark.asyncio
async def test_actual_admission_ignores_transaction_success_and_diagnostic_schema_errors():
    from test_hood_ws_confirmed import sdk_stream_client
    from test_hood_ws_reads import put, put_order, order
    from risex_spread_shadow.hood_handoff import OrderPlan
    client, state = await sdk_stream_client()
    anchor = client.begin_ws_admission()
    plan = OrderPlan(account_index=11, market_id=1, side="BUY", quantity=Decimal(".20"),
                     price=Decimal("100"), order_type="LIMIT", time_in_force="POST_ONLY",
                     reduce_only=False, client_order_index=77, quantity_int=20,
                     price_int=1000, order_expiry_ms=9999999999999)
    tx = transaction(); tx["account_index"] = 11
    await put(state, dict(type="update/account_tx", channel="account_tx:11", txs=[tx]))
    assert client.ws_admission_view(anchor, plan, "123")[0] is None
    await put_order(state, order())
    before = state.reads.orders[(11, 77)][1]
    await put(state, dict(type="update/account_tx", channel="account_tx:11", txs=[{"unknown_schema": True}]))
    observed, _ = client.ws_admission_view(anchor, plan, "123")
    assert observed.order_id == "123" and observed.filled_quantity == 0
    assert state.reads.orders[(11, 77)][1] == before
    assert client.read_stream_summary()["transaction_invalid"] == 1
    assert state.observer.malformed == 0
