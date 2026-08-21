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
    outbox_from_environment,
)


NOW = datetime(2027, 8, 1, 12, tzinfo=UTC)


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
    def __init__(self, status: int, headers: dict[str, str] | None = None) -> None:
        self.status = status
        self.headers = headers or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


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
async def test_flood_control_and_connector_failures_retry_only_within_bound():
    sleeps: list[float] = []
    connector_error = aiohttp.ClientConnectorError(None, OSError("synthetic"))
    session = FakeSession([
        RaisingResponse(connector_error),
        FakeResponse(429, {"Retry-After": "5"}),
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
    assert sleeps == [0, 5.0]


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
