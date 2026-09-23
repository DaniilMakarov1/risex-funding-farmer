from __future__ import annotations

import json
import pytest

from risex_spread_shadow.hood_handoff.offline_observer import OfflineObserver, ObserverLimits


def frame(value):
    return json.dumps(value).encode()


def book(kind, nonce, begin=None):
    return frame({"type": kind, "channel": "order_book:1",
                  "order_book": {"nonce": nonce, "begin_nonce": begin,
                                 "asks": [{"price": "100", "size": "0.2"}]}})


def private(status, *, account=22, index=44, order_id="12345"):
    return frame({"type": "update/account_all_orders",
                  "channel": f"account_all_orders:{account}",
                  "auth": "PRIVATE_TOKEN_NEVER_PERSIST",
                  "orders": {"1": [{"owner_account_index": account, "market_index": 1,
                                    "client_order_index": index, "order_id": order_id,
                                    "status": status, "Sig": "PRIVATE_SIGNATURE_NEVER_PERSIST",
                                    "remaining_base_amount": "0.1"}]}})


def test_observer_redacts_exact_terminal_and_never_claims_private_completeness():
    observer = OfflineObserver(1, {22: 44})
    observer.reconnect(1)
    assert observer.feed(book("subscribed/order_book", 10), received_at=1.0, epoch=1)["kind"] == "book_snapshot"
    assert observer.feed(book("update/order_book", 11, 10), received_at=1.1, epoch=1)["kind"] == "book_update"
    event = observer.feed(private("filled"), received_at=1.2, epoch=1)
    assert event["terminal"] is True
    assert event["received_at"] == 1.2
    assert event["stream_complete"] is False
    assert observer.summary()["private_snapshot_proved"] is False
    saved = json.dumps(observer.summary())
    assert "PRIVATE_TOKEN" not in saved
    assert "PRIVATE_SIGNATURE" not in saved
    assert "remaining_base_amount" not in saved


def test_exact_terminal_can_be_observed_without_book_but_never_admitted():
    observer = OfflineObserver(1, {22: 44})
    observer.reconnect(1)
    event = observer.feed(private("canceled"), received_at=2.0, epoch=1)
    assert event["terminal"] is True
    assert event["stream_complete"] is False
    assert observer.book_ready is False
    assert observer.private_snapshot_proved is False


def test_gap_duplicate_out_of_order_and_reconnect_need_new_book_snapshot():
    observer = OfflineObserver(1, {22: 44})
    observer.reconnect(1)
    observer.feed(book("subscribed/order_book", 10), received_at=1.0, epoch=1)
    assert observer.feed(book("update/order_book", 11, 10), received_at=1.1, epoch=1)["kind"] == "book_update"
    assert observer.feed(book("update/order_book", 11, 10), received_at=1.2, epoch=1)["kind"] == "book_gap"
    assert observer.book_ready is False
    assert observer.feed(book("update/order_book", 12, 11), received_at=1.3, epoch=1)["kind"] == "book_gap"
    observer.reconnect(2)
    assert observer.book_ready is False
    assert observer.feed(book("update/order_book", 20, 19), received_at=1.4, epoch=2)["kind"] == "book_gap"
    assert observer.feed(book("subscribed/order_book", 21), received_at=1.5, epoch=2)["kind"] == "book_snapshot"
    assert observer.book_ready is True
    assert observer.feed(private("filled"), received_at=1.6, epoch=1) is None
    assert observer.stopped_reason == "connection_epoch_mismatch"


def test_private_identity_conflict_and_incomplete_frames_cannot_be_terminal_proof():
    observer = OfflineObserver(1, {22: 44})
    observer.reconnect(1)
    assert observer.feed(private("filled", account=23), received_at=1.0, epoch=1) is None
    assert observer.feed(private("filled", index=45), received_at=1.1, epoch=1) is None
    assert observer.feed(b"{broken", received_at=1.2, epoch=1) is None
    assert observer.summary()["malformed"] == 1
    assert observer.summary()["observations"] == []


def test_private_field_cannot_smuggle_secret_into_redacted_output():
    observer = OfflineObserver(1, {22: 44})
    observer.reconnect(1)
    assert observer.feed(private("PRIVATE_TOKEN", order_id="12345"), received_at=1.0, epoch=1) is None
    assert observer.feed(private("filled", order_id="PRIVATE_TOKEN"), received_at=1.1, epoch=1) is None
    assert observer.feed(private({"auth": "PRIVATE_TOKEN"}), received_at=1.2, epoch=1) is None
    assert "PRIVATE_TOKEN" not in json.dumps(observer.summary())


def test_private_duplicate_and_terminal_regression_are_explicitly_unproved():
    observer = OfflineObserver(1, {22: 44})
    observer.reconnect(1)
    first = private("filled")
    assert observer.feed(first, received_at=1.0, epoch=1)["terminal"] is True
    assert observer.feed(first, received_at=1.1, epoch=1) is None
    assert observer.summary()["private_duplicates"] == 1
    conflict = observer.feed(private("open"), received_at=1.2, epoch=1)
    assert conflict["kind"] == "private_conflict"
    assert observer.feed(private("canceled"), received_at=1.25, epoch=1)["kind"] == "private_conflict"
    assert observer.summary()["private_conflicts"] == 2
    observer.reconnect(2)
    assert observer.feed(private("open"), received_at=1.3, epoch=2)["terminal"] is False
    assert observer.summary()["private_snapshot_proved"] is False


def test_size_frame_count_and_duration_limits_stop_without_retaining_raw_frame():
    raw = private("filled")
    limits = ObserverLimits(max_seconds=1, max_frames=1, max_bytes=len(raw), max_frame_bytes=len(raw))
    observer = OfflineObserver(1, {22: 44}, limits)
    observer.reconnect(1)
    observer.feed(raw, received_at=1.0, epoch=1)
    assert observer.feed(raw, received_at=1.1, epoch=1) is None
    assert observer.stopped_reason == "byte_limit"
    assert observer.frames == 1
    assert "PRIVATE_TOKEN" not in repr(observer)

    time_limited = OfflineObserver(1, {22: 44}, ObserverLimits(max_seconds=0.1))
    time_limited.reconnect(1)
    time_limited.feed(raw, received_at=1.0, epoch=1)
    assert time_limited.feed(raw, received_at=1.2, epoch=1) is None
    assert time_limited.stopped_reason == "duration_limit"

    count_limited = OfflineObserver(1, {22: 44}, ObserverLimits(max_frames=1))
    count_limited.reconnect(1)
    count_limited.feed(raw, received_at=1.0, epoch=1)
    assert count_limited.feed(raw, received_at=1.01, epoch=1) is None
    assert count_limited.stopped_reason == "frame_limit"


def test_invalid_local_time_and_limit_types_fail_closed():
    with pytest.raises(ValueError):
        ObserverLimits(max_seconds="ten")
    observer = OfflineObserver(1, {22: 44})
    observer.reconnect(1)
    assert observer.feed(private("filled"), received_at="later", epoch=1) is None
    assert observer.stopped_reason == "invalid_input"
    assert observer.frames == 0


@pytest.mark.parametrize("field,valid,invalid", [
    ("owner_account_index", 22, True),
    ("owner_account_index", 22, 22.0),
    ("owner_account_index", 22, "22"),
    ("owner_account_index", 22, None),
    ("owner_account_index", 22, -1),
    ("market_index", 1, True),
    ("market_index", 1, 1.0),
    ("market_index", 1, "1"),
    ("market_index", 1, None),
    ("market_index", 1, -1),
    ("client_order_index", 44, True),
    ("client_order_index", 44, 44.0),
    ("client_order_index", 44, "44"),
    ("client_order_index", 44, None),
    ("client_order_index", 44, -1),
])
def test_private_identity_requires_exact_integer_type(field, valid, invalid):
    observer = OfflineObserver(1, {22: 44})
    observer.reconnect(1)
    base = json.loads(private("filled"))
    assert base["orders"]["1"][0][field] == valid
    base["orders"]["1"][0][field] = invalid
    assert observer.feed(frame(base), received_at=10, epoch=1) is None
    assert observer.summary()["observations"] == []
    del base["orders"]["1"][0][field]
    assert observer.feed(frame(base), received_at=10, epoch=1) is None
    assert observer.summary()["observations"] == []


@pytest.mark.parametrize("bad_epoch", [True, 1.0, "1", None, -1])
def test_feed_epoch_requires_exact_integer_type(bad_epoch):
    observer = OfflineObserver(1, {22: 44})
    observer.reconnect(1)
    assert observer.feed(private("filled"), received_at=10, epoch=bad_epoch) is None
    assert observer.stopped_reason == "connection_epoch_mismatch"
    assert observer.frames == 0
    assert observer.summary()["observations"] == []


@pytest.mark.parametrize("bad_epoch", [True, 1.0, "1", None, -1])
def test_reconnect_epoch_requires_exact_integer_type(bad_epoch):
    observer = OfflineObserver(1, {22: 44})
    with pytest.raises(ValueError):
        observer.reconnect(bad_epoch)
    assert observer.epoch is None


def test_boolean_market_and_epoch_probe_cannot_emit_exact_private_order():
    observer = OfflineObserver(1, {22: 44})
    observer.reconnect(1)
    base = json.loads(private("open"))
    base["orders"]["1"][0]["market_index"] = True
    assert observer.feed(frame(base), received_at=10, epoch=True) is None
    assert observer.summary()["observations"] == []
    assert observer.stopped_reason == "connection_epoch_mismatch"


def test_local_receipt_regression_rejects_terminal_without_resetting_budget():
    observer = OfflineObserver(1, {22: 44}, ObserverLimits(max_seconds=5))
    observer.reconnect(1)
    observer.feed(book("subscribed/order_book", 10), received_at=10, epoch=1)
    observer.feed(private("open"), received_at=12, epoch=1)
    assert observer.feed(private("filled"), received_at=11, epoch=1) is None
    summary = observer.summary()
    assert summary["stopped_reason"] == "receipt_time_regression"
    assert summary["start_at"] == 10
    assert summary["last_received_at"] == 12
    assert summary["frames"] == 2
    assert not any(item.get("terminal") is True for item in summary["observations"])


def test_equal_receipt_times_and_reconnect_keep_chronology_and_limits():
    observer = OfflineObserver(1, {22: 44}, ObserverLimits(max_seconds=2, max_frames=3))
    observer.reconnect(1)
    observer.feed(private("open"), received_at=10, epoch=1)
    observer.reconnect(2)
    terminal = observer.feed(private("filled"), received_at=10, epoch=2)
    assert terminal["terminal"] is True
    assert observer.summary()["start_at"] == observer.summary()["last_received_at"] == 10
    observer.feed(private("open"), received_at=12, epoch=2)
    assert observer.frames == 3
    observer.reconnect(3)
    assert observer.feed(private("filled"), received_at=12, epoch=3) is None
    assert observer.stopped_reason == "frame_limit"
    assert observer.summary()["start_at"] == 10

    budget = OfflineObserver(1, {22: 44}, ObserverLimits(max_seconds=2))
    budget.reconnect(1)
    budget.feed(private("open"), received_at=10, epoch=1)
    budget.reconnect(2)
    assert budget.feed(private("filled"), received_at=12.1, epoch=2) is None
    assert budget.stopped_reason == "duration_limit"
    assert budget.summary()["start_at"] == 10

    chronological = OfflineObserver(1, {22: 44})
    chronological.reconnect(1)
    chronological.feed(private("open"), received_at=10, epoch=1)
    chronological.reconnect(2)
    assert chronological.feed(private("filled"), received_at=9.9, epoch=2) is None
    assert chronological.stopped_reason == "receipt_time_regression"
