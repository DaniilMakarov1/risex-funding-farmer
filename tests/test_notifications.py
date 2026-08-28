import asyncio
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import aiohttp
import pytest

from risex_farmer.notifications import (
    LifecycleNotificationTracker,
    NoopNotificationDelivery,
    NotificationOutbox,
    NotificationPayload,
    NotificationScope,
    TelegramDelivery,
    format_telegram_funding_countdown,
    format_telegram_money,
    full_scan_digest_payloads,
    outbox_from_environment,
)


NOW = datetime(2027, 8, 1, 12, tzinfo=UTC)


@pytest.mark.parametrize(("value", "expected"), (
    (None, "UNKNOWN"),
    (Decimal("0"), "0.00"),
    (Decimal("7.1"), "7.10"),
    (Decimal("1.235"), "1.24"),
    (Decimal("-1.235"), "-1.24"),
    (Decimal("-0.004"), "0.00"),
    (Decimal("-0.005"), "-0.01"),
))
def test_telegram_money_has_exactly_two_fractional_digits(value, expected):
    assert format_telegram_money(value) == expected


@pytest.mark.parametrize(("target_cycle_start", "expected"), (
    ((NOW + timedelta(minutes=42)).isoformat(), "Funding in: 42 min"),
    ((NOW + timedelta(minutes=42, seconds=59)).isoformat(), "Funding in: 42 min"),
    ((NOW + timedelta(seconds=59)).isoformat(), "Funding in: 0 min"),
    (NOW.isoformat(), "Funding in: 0 min"),
    ((NOW - timedelta(microseconds=1)).isoformat(), "Funding in: UNKNOWN"),
    (None, "Funding in: UNKNOWN"),
    ("not-a-timestamp", "Funding in: UNKNOWN"),
    (NOW.replace(tzinfo=None).isoformat(), "Funding in: UNKNOWN"),
    (NOW, "Funding in: UNKNOWN"),
))
def test_telegram_funding_countdown_is_conservative_and_fail_closed(
    target_cycle_start, expected
):
    assert format_telegram_funding_countdown(target_cycle_start, NOW) == expected


def test_full_scan_digest_uses_persisted_target_cycle_and_preserves_unknown_pnl():
    row = {
        "canonical_asset": "ABC",
        "hedge_venue": "EXTENDED",
        "direction": "LONG_RISEX_SHORT_HEDGE",
        "planned_maker_net_pnl_usd": None,
        "blockers": ["FUNDING_ELIGIBILITY_UNKNOWN"],
        "target_cycle_start": (NOW + timedelta(minutes=42, seconds=59)).isoformat(),
        "seconds_to_earliest_funding": "999999",
    }
    before = row.copy()

    payload = full_scan_digest_payloads(
        scan_at=NOW, opportunity=False, route_rows=(row,)
    )[0]

    assert payload.text.splitlines()[1] == (
        "ABC | RISEx LONG / EXTENDED SHORT | Expected PnL: UNKNOWN — funding | "
        "Funding in: 42 min"
    )
    assert row == before


def payload(event_id: str = "event-1") -> NotificationPayload:
    return NotificationPayload(
        event_id,
        "TEST",
        NOW,
        "synthetic notification",
        ticker="ABC",
        route="RISEx LONG / NADO SHORT",
        planned_maker_net_pnl_usd=Decimal("1.234567"),
    )


class FakeResponse:
    def __init__(self, status: int, body: object | None = None) -> None:
        self.status = status
        self.body = body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def json(self, **_kwargs):
        if isinstance(self.body, BaseException):
            raise self.body
        return self.body


class RaisingResponse:
    def __init__(self, error: BaseException) -> None:
        self.error = error

    async def __aenter__(self):
        raise self.error

    async def __aexit__(self, *args):
        return False


class HangingResponse:
    async def __aenter__(self):
        await asyncio.Event().wait()

    async def __aexit__(self, *args):
        return False


class FakeSession:
    def __init__(self, outcomes) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.closed = False

    def post(self, url, *, json):
        self.calls.append((url, json))
        outcome = self.outcomes.pop(0)
        return outcome

    async def close(self):
        self.closed = True


class LifecycleCapture:
    def __init__(self, *, accept: bool = True) -> None:
        self.rows: list[NotificationPayload] = []
        self.accept = accept

    async def start(self):
        pass

    def enqueue(self, row: NotificationPayload) -> bool:
        if self.accept:
            self.rows.append(row)
        return self.accept

    async def close(self):
        pass


def lifecycle_tracker(
    *,
    scope: NotificationScope = NotificationScope.TESTNET,
    lifecycle_key: str = "route-1",
    expected_legs: tuple[str, str] = ("RISEX", "HEDGE"),
    capture: LifecycleCapture | None = None,
) -> tuple[LifecycleNotificationTracker, LifecycleCapture]:
    sink = capture or LifecycleCapture()
    tracker = LifecycleNotificationTracker(NotificationOutbox(sink))
    assert tracker.begin_lifecycle(
        scope=scope,
        lifecycle_key=lifecycle_key,
        ticker="BTC",
        route="RISEx LONG / NADO SHORT",
        expected_legs=expected_legs,
    )
    return tracker, sink


async def drain(delivery: TelegramDelivery) -> None:
    await asyncio.wait_for(delivery._queue.join(), timeout=1)


def test_payload_is_immutable_and_notification_module_is_dependency_inverted():
    row = payload()
    with pytest.raises(FrozenInstanceError):
        row.text = "changed"  # type: ignore[misc]

    source = Path(__file__).parents[1] / "src/risex_farmer/notifications.py"
    text = source.read_text()
    for forbidden in (
        ".scanner", ".exchanges", ".storage", ".paper_broker", ".lifecycle",
        ".runtime", "getUpdates", "scan-once",
    ):
        assert forbidden not in text
    assert "/sendMessage" in text


@pytest.mark.asyncio
async def test_noop_and_disabled_configuration_have_no_side_effects():
    sink = NoopNotificationDelivery()
    await sink.start()
    assert sink.enqueue(payload()) is False
    await sink.close()
    assert outbox_from_environment({}) is None
    assert outbox_from_environment({"RISEX_TELEGRAM_ENABLED": "false"}) is None


def test_enabled_configuration_fails_generically_without_credentials():
    with pytest.raises(RuntimeError) as caught:
        outbox_from_environment({
            "RISEX_TELEGRAM_ENABLED": "true",
            "RISEX_TELEGRAM_BOT_TOKEN": "synthetic-secret-token",
        })
    rendered = str(caught.value)
    assert rendered == "Telegram notification configuration is invalid"
    assert "TOKEN" not in rendered and "CHAT" not in rendered
    assert "synthetic-secret-token" not in rendered


@pytest.mark.asyncio
async def test_outbox_deduplicates_events_and_opportunity_semantic_state():
    class Capture:
        def __init__(self):
            self.rows = []
        async def start(self): pass
        def enqueue(self, row):
            self.rows.append(row)
            return True
        async def close(self): pass

    capture = Capture()
    outbox = NotificationOutbox(capture)
    assert outbox.event(payload())
    assert not outbox.event(payload())
    state = ("ABC:RISEx LONG / NADO SHORT", "cycle-1", "1.23")
    assert outbox.opportunity(state, payload("opportunity-1"))
    assert not outbox.opportunity(state, payload("opportunity-2"))
    assert outbox.opportunity(None, payload("disappeared"))
    assert not outbox.opportunity(None, payload("disappeared-again"))
    assert [row.event_id for row in capture.rows] == [
        "event-1", "opportunity-1", "disappeared",
    ]


def test_outage_recovery_requires_a_queued_critical_notification():
    class RejectingCapture:
        def __init__(self):
            self.rows = []
        async def start(self): pass
        def enqueue(self, row):
            self.rows.append(row)
            return False
        async def close(self): pass

    capture = RejectingCapture()
    outbox = NotificationOutbox(capture)
    assert not outbox.outage(
        "semantic-episode", degraded=True, payload=payload("critical"),
    )
    assert not outbox.outage(
        "semantic-episode", degraded=False, payload=payload("recovery"),
    )
    assert len(capture.rows) == 1
    assert outbox._active_outages == set()


def test_full_scan_digest_part_event_ids_deduplicate_and_text_is_bounded():
    rows = tuple({
        "canonical_asset": f"ASSET-{index}-" + "X" * 100,
        "hedge_venue": "EXTENDED-" + "Y" * 150,
        "direction": (
            "LONG_RISEX_SHORT_HEDGE"
            if index % 2 == 0
            else "SHORT_RISEX_LONG_HEDGE"
        ),
        "planned_maker_net_pnl_usd": "1." + "2" * 150,
    } for index in range(20))
    digests = full_scan_digest_payloads(
        scan_at=NOW, opportunity=True, route_rows=rows
    )

    class Capture:
        def __init__(self):
            self.rows = []
        async def start(self): pass
        def enqueue(self, row):
            self.rows.append(row)
            return True
        async def close(self): pass

    capture = Capture()
    outbox = NotificationOutbox(capture)
    for digest in digests:
        assert outbox.event(digest)
        assert not outbox.event(digest)
        assert len(digest.text) <= 4096
    assert [row.event_id for row in digests] == [
        f"full-scan-digest:{NOW.isoformat()}:part:{index}:{len(digests)}"
        for index in range(1, len(digests) + 1)
    ]
    route_lines = [
        line for digest in digests for line in digest.text.splitlines()[1:]
    ]
    assert len(route_lines) == 10
    assert all(line.count(" | ") == 3 for line in route_lines)
    assert all("Funding in: UNKNOWN" in line for line in route_lines)


def test_full_scan_digest_delivers_only_first_ten_rows_without_loss() -> None:
    rows = tuple({
        "canonical_asset": f"ASSET-{index}-" + "X" * 80,
        "hedge_venue": "EXTENDED-" + "Y" * 100,
        "direction": "LONG_RISEX_SHORT_HEDGE",
        "planned_maker_net_pnl_usd": None,
        "blockers": [f"MARKET_METADATA_STALE:{index}"],
    } for index in range(58))
    payloads = full_scan_digest_payloads(
        scan_at=NOW, opportunity=False, route_rows=rows,
    )
    assert all(len(payload.text) <= 4096 for payload in payloads)
    lines = [line for payload in payloads for line in payload.text.splitlines()[1:]]
    assert len(payloads) == 1
    assert len(lines) == len(set(lines)) == 10
    assert all("Expected PnL: UNKNOWN — market metadata stale" in line for line in lines)
    assert all(f"ASSET-{index}-" in line for index, line in enumerate(lines))
    assert not any("ASSET-10-" in line for line in lines)


def test_full_scan_digest_keeps_fewer_than_ten_rows_whole() -> None:
    rows = tuple({
        "canonical_asset": f"ASSET-{index}",
        "hedge_venue": "EXTENDED",
        "direction": "LONG_RISEX_SHORT_HEDGE",
        "planned_maker_net_pnl_usd": str(index),
    } for index in range(3))
    payloads = full_scan_digest_payloads(
        scan_at=NOW, opportunity=False, route_rows=rows,
    )
    assert len(payloads) == 1
    assert payloads[0].text.splitlines()[1:] == [
        f"ASSET-{index} | RISEx LONG / EXTENDED SHORT | Expected PnL: ${index}.00 | "
        "Funding in: UNKNOWN"
        for index in range(3)
    ]


@pytest.mark.parametrize(("blocker", "label"), (
    ("PARITY_OR_MULTIPLIER_UNKNOWN", "RISEx parity"),
    ("CATALOG_STALE", "Extended catalog"),
    ("MARKET_METADATA_STALE", "market metadata stale"),
    ("BOOK_UNHEALTHY", "book stream"),
    ("TRADE_STREAM_UNHEALTHY", "trade stream"),
    ("FUNDING_ELIGIBILITY_UNKNOWN", "funding"),
))
def test_full_scan_unknown_uses_human_authoritative_label(blocker, label):
    payload = full_scan_digest_payloads(
        scan_at=NOW, opportunity=False, route_rows=({
            "canonical_asset": "ABC", "hedge_venue": "EXTENDED",
            "direction": "LONG_RISEX_SHORT_HEDGE",
            "planned_maker_net_pnl_usd": None, "blockers": [blocker],
        },),
    )[0]
    line = payload.text.splitlines()[1]
    assert f"Expected PnL: UNKNOWN — {label}" in line
    assert line.endswith("Funding in: UNKNOWN")
    assert line.count(" | ") == 3
    assert blocker not in line


@pytest.mark.asyncio
async def test_telegram_only_calls_send_message_and_keeps_secrets_out_of_payload():
    token, chat = "synthetic-secret-token", "synthetic-secret-chat"
    session = FakeSession([FakeResponse(200)])
    delivery = TelegramDelivery(token, chat, session_factory=lambda: session)
    await delivery.start()
    assert delivery.enqueue(payload())
    await drain(delivery)
    await delivery.close()
    assert len(session.calls) == 1
    url, body = session.calls[0]
    assert url.endswith("/sendMessage")
    assert body == {"chat_id": chat, "text": "synthetic notification"}
    assert token not in payload().text and chat not in payload().text
    assert session.closed


@pytest.mark.asyncio
async def test_bounded_queue_drops_on_saturation_without_waiting():
    delivery = TelegramDelivery(
        "synthetic-token", "synthetic-chat", queue_size=1,
        session_factory=lambda: FakeSession([]),
    )
    assert delivery.enqueue(payload("first"))
    assert not delivery.enqueue(payload("second"))


@pytest.mark.asyncio
async def test_ambiguous_timeout_is_not_retried():
    session = FakeSession([RaisingResponse(TimeoutError("synthetic timeout"))])
    delivery = TelegramDelivery(
        "synthetic-token", "synthetic-chat", timeout_seconds=0.01,
        session_factory=lambda: session,
    )
    await delivery.start()
    delivery.enqueue(payload())
    await drain(delivery)
    await delivery.close()
    assert len(session.calls) == 1


@pytest.mark.asyncio
async def test_flood_control_json_retry_after_seven_is_bounded_and_exact():
    sleeps: list[float] = []
    session = FakeSession([
        FakeResponse(429, {"parameters": {"retry_after": 7}}),
        FakeResponse(200),
    ])

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)

    delivery = TelegramDelivery(
        "synthetic-token", "synthetic-chat", max_attempts=2,
        session_factory=lambda: session, sleep=sleep,
    )
    await delivery.start()
    delivery.enqueue(payload())
    await drain(delivery)
    await delivery.close()
    assert len(session.calls) == 2
    assert sleeps == [7.0]


@pytest.mark.asyncio
async def test_flood_control_wait_outlives_request_timeout_then_retries():
    completed_waits: list[float] = []
    session = FakeSession([
        FakeResponse(429, {"parameters": {"retry_after": 0.02}}),
        FakeResponse(200),
    ])

    async def yielding_sleep(seconds: float) -> None:
        await asyncio.sleep(seconds)
        completed_waits.append(seconds)

    delivery = TelegramDelivery(
        "synthetic-token", "synthetic-chat", timeout_seconds=0.005,
        max_attempts=2, session_factory=lambda: session, sleep=yielding_sleep,
    )
    await delivery.start()
    delivery.enqueue(payload())
    await drain(delivery)
    await delivery.close()
    assert completed_waits == [0.02]
    assert len(session.calls) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("body", [
    ValueError("synthetic malformed response"),
    {},
    {"parameters": {}},
    {"parameters": {"retry_after": 0}},
    {"parameters": {"retry_after": -1}},
    {"parameters": {"retry_after": "7"}},
    {"parameters": {"retry_after": float("nan")}},
])
async def test_invalid_or_nonpositive_flood_control_never_retries_immediately(
    body,
):
    session = FakeSession([FakeResponse(429, body)])
    sleeps: list[float] = []

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)

    delivery = TelegramDelivery(
        "synthetic-token", "synthetic-chat", max_attempts=3,
        session_factory=lambda: session, sleep=sleep,
    )
    await delivery.start()
    delivery.enqueue(payload())
    await drain(delivery)
    await delivery.close()
    assert len(session.calls) == 1
    assert sleeps == []


@pytest.mark.asyncio
async def test_oversized_flood_control_caps_at_thirty_and_max_attempts_are_strict():
    sleeps: list[float] = []
    session = FakeSession([
        FakeResponse(429, {"parameters": {"retry_after": 300}}),
        FakeResponse(429, {"parameters": {"retry_after": 7}}),
        FakeResponse(429, {"parameters": {"retry_after": 7}}),
    ])

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)

    delivery = TelegramDelivery(
        "synthetic-token", "synthetic-chat", max_attempts=3,
        session_factory=lambda: session, sleep=sleep,
    )
    await delivery.start()
    delivery.enqueue(payload())
    await drain(delivery)
    await delivery.close()
    assert len(session.calls) == 3
    assert sleeps == [30.0, 7.0]


@pytest.mark.asyncio
async def test_connector_failure_uses_positive_bounded_backoff():
    sleeps: list[float] = []
    connector_error = aiohttp.ClientConnectorError(None, OSError("synthetic"))
    session = FakeSession([
        RaisingResponse(connector_error), RaisingResponse(connector_error),
        FakeResponse(200),
    ])

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)

    delivery = TelegramDelivery(
        "synthetic-token", "synthetic-chat", max_attempts=3,
        session_factory=lambda: session, sleep=sleep,
    )
    await delivery.start()
    delivery.enqueue(payload())
    await drain(delivery)
    await delivery.close()
    assert len(session.calls) == 3
    assert sleeps == [1.0, 1.0]


@pytest.mark.asyncio
async def test_response_description_and_secret_url_never_escape(caplog):
    token = "synthetic-private-token"
    chat = "synthetic-private-chat"
    description = "synthetic confidential response description"
    secret_url = f"https://api.telegram.org/bot{token}/sendMessage"
    session = FakeSession([
        FakeResponse(429, {
            "ok": False,
            "description": description,
            "parameters": {"retry_after": 0},
        }),
        RaisingResponse(RuntimeError(f"{secret_url} {description}")),
    ])
    delivery = TelegramDelivery(token, chat, session_factory=lambda: session)
    await delivery.start()
    delivery.enqueue(payload())
    delivery.enqueue(payload("event-2"))
    await drain(delivery)
    await delivery.close()
    logs = caplog.text
    assert token not in logs and chat not in logs and description not in logs
    assert secret_url not in logs
    assert token not in payload().text and chat not in payload().text
    assert description not in payload().text


@pytest.mark.asyncio
async def test_close_cancels_hung_delivery_without_task_leak():
    session = FakeSession([HangingResponse()])
    delivery = TelegramDelivery(
        "synthetic-token", "synthetic-chat", timeout_seconds=60,
        session_factory=lambda: session,
    )
    await delivery.start()
    delivery.enqueue(payload())
    await asyncio.sleep(0)
    await asyncio.wait_for(delivery.close(), timeout=0.1)
    assert delivery._worker is None
    assert session.closed


def test_testnet_partial_or_non_authoritative_leg_never_emits_open():
    tracker, capture = lifecycle_tracker(
        expected_legs=("RISEX", "NADO"),
    )
    assert not tracker.confirm_leg_open(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        leg="RISEX",
        authoritative=True,
        at=NOW,
        expected_legs=("RISEX", "NADO"),
    )
    assert not tracker.confirm_leg_open(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        leg="NADO",
        authoritative=False,
        at=NOW,
        expected_legs=("RISEX", "NADO"),
    )
    assert not any(row.kind == "POSITION_OPENED" for row in capture.rows)


def test_testnet_exact_authoritative_pair_open_is_one_sanitized_event():
    tracker, capture = lifecycle_tracker(
        expected_legs=("RISEX", "NADO"),
    )
    assert tracker.confirm_pair_open(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        authoritative_legs={"RISEX": True, "NADO": True},
        at=NOW,
        authoritative=True,
        expected_legs=("RISEX", "NADO"),
    )
    assert not tracker.confirm_pair_open(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        authoritative_legs=("RISEX", "NADO"),
        at=NOW,
        authoritative=True,
        expected_legs=("RISEX", "NADO"),
    )
    opened = [row for row in capture.rows if row.kind == "POSITION_OPENED"]
    assert len(opened) == 1
    assert opened[0].text.startswith("TESTNET | OPEN |")
    assert "route-1" not in opened[0].text
    assert len(opened[0].text) <= 4096


def test_funding_status_preserves_zero_negative_and_unresolved_without_fabrication():
    tracker, capture = lifecycle_tracker()
    assert tracker.confirm_pair_open(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        authoritative_legs=("RISEX", "HEDGE"),
        at=NOW,
        authoritative=True,
    )
    assert tracker.funding_status(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        settlement_key="settlement-1",
        status="APPLIED_RATE",
        cash_usd=Decimal("0"),
        at=NOW,
    )
    assert tracker.funding_status(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        settlement_key="settlement-1",
        status="APPLIED_RATE",
        cash_usd=Decimal("-1.235"),
        at=NOW,
    )
    assert tracker.funding_status(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        settlement_key="settlement-1",
        status="UNRESOLVED",
        cash_usd=None,
        at=NOW,
    )
    assert not tracker.funding_status(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        settlement_key="settlement-1",
        status="UNRESOLVED",
        cash_usd=None,
        at=NOW,
    )
    rows = [row for row in capture.rows if row.kind == "FUNDING_STATUS"]
    assert len(rows) == 3
    assert "status APPLIED_RATE" in rows[0].text
    assert "cash USD 0.00" in rows[0].text
    assert "cash USD -1.24" in rows[1].text
    assert "status UNRESOLVED" in rows[2].text
    assert "cash USD UNKNOWN" in rows[2].text
    assert "received" not in rows[2].text.lower()


def test_close_and_final_flat_are_ordered_authoritative_and_deduplicated():
    tracker, capture = lifecycle_tracker()
    assert tracker.confirm_pair_open(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        authoritative_legs=("RISEX", "HEDGE"),
        at=NOW,
        authoritative=True,
    )
    assert not tracker.confirm_pair_closed(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        authoritative_legs=("RISEX", "HEDGE"),
        at=NOW,
        authoritative=True,
    )
    assert tracker.exit_started(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        at=NOW,
    )
    assert not tracker.confirm_leg_closed(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        leg="RISEX",
        authoritative=True,
        at=NOW,
    )
    assert tracker.confirm_leg_closed(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        leg="HEDGE",
        authoritative=True,
        at=NOW,
    )
    assert not tracker.confirm_final_flat(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        authoritative_legs=("RISEX", "HEDGE"),
        zero_orders=False,
        exact_flat=True,
        at=NOW,
        authoritative=True,
    )
    assert tracker.confirm_final_flat(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        authoritative_legs=("RISEX", "HEDGE"),
        zero_orders=True,
        exact_flat=True,
        at=NOW,
        authoritative=True,
    )
    assert not tracker.confirm_final_flat(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        authoritative_legs=("RISEX", "HEDGE"),
        zero_orders=True,
        exact_flat=True,
        at=NOW,
        authoritative=True,
    )
    assert [row.kind for row in capture.rows] == [
        "POSITION_OPENED", "EXIT_STARTED", "POSITION_CLOSED", "FINAL_FLAT",
    ]


def test_blocker_recovery_is_paired_and_queue_failure_creates_no_orphan():
    tracker, capture = lifecycle_tracker()
    assert tracker.lifecycle_blocked(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        episode_key="gap-1",
        failure_class="TRANSPORT",
        stage="RECONCILIATION",
        reason="temporary transport failure",
        at=NOW,
    )
    assert not tracker.lifecycle_blocked(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        episode_key="gap-1",
        failure_class="TRANSPORT",
        stage="RECONCILIATION",
        at=NOW,
    )
    assert not tracker.lifecycle_recovered(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        episode_key="gap-2",
        at=NOW,
    )
    assert tracker.lifecycle_recovered(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        episode_key="gap-1",
        at=NOW,
    )
    assert not tracker.lifecycle_recovered(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        episode_key="gap-1",
        at=NOW,
    )
    assert [row.kind for row in capture.rows] == [
        "LIFECYCLE_BLOCKED", "LIFECYCLE_RECOVERED",
    ]

    rejected = LifecycleCapture(accept=False)
    failed_tracker, failed_capture = lifecycle_tracker(capture=rejected)
    assert not failed_tracker.lifecycle_blocked(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        episode_key="gap-1",
        failure_class="TRANSPORT",
        stage="RECONCILIATION",
        at=NOW,
    )
    assert not failed_tracker.lifecycle_recovered(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        episode_key="gap-1",
        at=NOW,
    )
    assert failed_capture.rows == []


def test_notification_failure_does_not_block_lifecycle_progression():
    capture = LifecycleCapture(accept=False)
    tracker, _ = lifecycle_tracker(capture=capture)
    assert not tracker.confirm_pair_open(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        authoritative_legs=("RISEX", "HEDGE"),
        at=NOW,
        authoritative=True,
    )
    capture.accept = True
    assert tracker.exit_started(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        at=NOW,
    )
    assert tracker.confirm_pair_closed(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        authoritative_legs=("RISEX", "HEDGE"),
        at=NOW,
        authoritative=True,
    )
    assert tracker.confirm_final_flat(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        authoritative_legs=("RISEX", "HEDGE"),
        zero_orders=True,
        exact_flat=True,
        at=NOW,
        authoritative=True,
    )
    assert [row.kind for row in capture.rows] == [
        "EXIT_STARTED", "POSITION_CLOSED", "FINAL_FLAT",
    ]

    class BrokenCapture(LifecycleCapture):
        def enqueue(self, row: NotificationPayload) -> bool:
            raise RuntimeError("synthetic notification sink failure")

    broken = BrokenCapture()
    broken_tracker, _ = lifecycle_tracker(capture=broken)
    assert not broken_tracker.maker_entry_activated(
        scope=NotificationScope.TESTNET,
        lifecycle_key="route-1",
        at=NOW,
    )


def test_lifecycle_notification_text_redacts_private_tokens_and_is_bounded():
    tracker, capture = lifecycle_tracker()
    secret = "0x" + "a" * 64
    private = "api_key=synthetic-private-value"
    assert tracker.begin_lifecycle(
        scope=NotificationScope.PAPER,
        lifecycle_key="paper-private",
        ticker=secret,
        route=f"RISEx LONG / {private} order_id=private-order-id",
    )
    assert tracker.maker_entry_activated(
        scope=NotificationScope.PAPER,
        lifecycle_key="paper-private",
        at=NOW,
    )
    row = capture.rows[-1]
    assert row.text.startswith("PAPER |")
    assert secret not in row.text
    assert private not in row.text
    assert "private-order-id" not in row.text
    assert len(row.text) <= 4096
