import asyncio
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import aiohttp
import pytest

from risex_farmer.notifications import (
    NoopNotificationDelivery,
    NotificationOutbox,
    NotificationPayload,
    TelegramDelivery,
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
    assert len(route_lines) == 20
    assert all(line.count(" | ") == 2 for line in route_lines)


def test_full_scan_digest_splits_all_twenty_rows_without_loss() -> None:
    rows = tuple({
        "canonical_asset": f"ASSET-{index}-" + "X" * 80,
        "hedge_venue": "EXTENDED-" + "Y" * 100,
        "direction": "LONG_RISEX_SHORT_HEDGE",
        "planned_maker_net_pnl_usd": None,
        "blockers": [f"MARKET_METADATA_STALE:{index}"],
    } for index in range(20))
    payloads = full_scan_digest_payloads(
        scan_at=NOW, opportunity=False, route_rows=rows,
    )
    assert all(len(payload.text) <= 4096 for payload in payloads)
    lines = [line for payload in payloads for line in payload.text.splitlines()[1:]]
    assert len(lines) == len(set(lines)) == 20
    assert all("Expected PnL: UNKNOWN — market metadata stale" in line for line in lines)


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
    assert line.endswith(f"Expected PnL: UNKNOWN — {label}")
    assert line.count(" | ") == 2
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
