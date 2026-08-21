"""Dependency-inverted, outbound-only runtime notifications."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Protocol

import aiohttp


@dataclass(frozen=True, slots=True)
class NotificationPayload:
    event_id: str
    kind: str
    occurred_at: datetime
    text: str
    ticker: str | None = None
    route: str | None = None
    planned_maker_net_pnl_usd: Decimal | None = None
    final_pnl_usd: Decimal | None = None

    def __post_init__(self) -> None:
        if self.occurred_at.tzinfo is None:
            raise ValueError("notification time must be timezone-aware")


class NotificationDelivery(Protocol):
    async def start(self) -> None: ...
    def enqueue(self, payload: NotificationPayload) -> bool: ...
    async def close(self) -> None: ...


class NoopNotificationDelivery:
    """Explicit disabled sink with no task, queue, or external side effect."""

    async def start(self) -> None:
        return None

    def enqueue(self, payload: NotificationPayload) -> bool:
        return False

    async def close(self) -> None:
        return None


class NotificationOutbox:
    """Process-local dedupe in front of a non-blocking delivery queue."""

    def __init__(self, delivery: NotificationDelivery) -> None:
        self.delivery = delivery
        self._event_ids: set[str] = set()
        self._opportunity_state: tuple[str, str, str] | None = None
        self._opportunity_initialized = False
        self._active_outages: set[str] = set()

    async def start(self) -> None:
        await self.delivery.start()

    async def close(self) -> None:
        await self.delivery.close()

    def event(self, payload: NotificationPayload) -> bool:
        if payload.event_id in self._event_ids:
            return False
        self._event_ids.add(payload.event_id)
        return self.delivery.enqueue(payload)

    def opportunity(
        self,
        state: tuple[str, str, str] | None,
        payload: NotificationPayload | None,
    ) -> bool:
        if self._opportunity_initialized and state == self._opportunity_state:
            return False
        previous = self._opportunity_state
        self._opportunity_initialized = True
        self._opportunity_state = state
        if payload is None or (previous is None and state is None):
            return False
        return self.event(payload)

    def outage(
        self, identity: str, *, degraded: bool, payload: NotificationPayload
    ) -> bool:
        if degraded:
            if identity in self._active_outages:
                return False
            self._active_outages.add(identity)
        else:
            if identity not in self._active_outages:
                return False
            self._active_outages.remove(identity)
        return self.event(payload)


class TelegramDelivery:
    """Bounded sendMessage worker; all delivery failures are intentionally silent."""

    def __init__(
        self,
        token: str,
        chat_id: str,
        *,
        queue_size: int = 32,
        timeout_seconds: float = 5,
        max_attempts: int = 3,
        session_factory: Callable[[], object] = aiohttp.ClientSession,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.__token = token
        self.__chat_id = chat_id
        self._queue: asyncio.Queue[NotificationPayload] = asyncio.Queue(queue_size)
        self._timeout_seconds = timeout_seconds
        self._max_attempts = max_attempts
        self._session_factory = session_factory
        self._sleep = sleep
        self._session: object | None = None
        self._worker: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._worker is None:
            self._session = self._session_factory()
            self._worker = asyncio.create_task(self._run())

    def enqueue(self, payload: NotificationPayload) -> bool:
        try:
            self._queue.put_nowait(payload)
        except asyncio.QueueFull:
            return False
        return True

    async def close(self) -> None:
        worker, self._worker = self._worker, None
        if worker is not None:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
        session, self._session = self._session, None
        close = getattr(session, "close", None)
        if close is not None:
            try:
                await close()
            except Exception:
                pass

    async def _run(self) -> None:
        while True:
            payload = await self._queue.get()
            try:
                await self._send(payload)
            finally:
                self._queue.task_done()

    async def _send(self, payload: NotificationPayload) -> None:
        assert self._session is not None
        url = f"https://api.telegram.org/bot{self.__token}/sendMessage"
        body = {"chat_id": self.__chat_id, "text": payload.text}
        for attempt in range(self._max_attempts):
            try:
                async with asyncio.timeout(self._timeout_seconds):
                    async with self._session.post(url, json=body) as response:  # type: ignore[attr-defined]
                        status = int(response.status)
                        if 200 <= status < 300:
                            return
                        if status == 429 and attempt + 1 < self._max_attempts:
                            retry_after = min(
                                float(response.headers.get("Retry-After", "0")), 30.0
                            )
                            await self._sleep(max(0, retry_after))
                            continue
                        return
            except asyncio.CancelledError:
                raise
            except TimeoutError:
                return
            except aiohttp.ClientConnectorError:
                if attempt + 1 >= self._max_attempts:
                    return
                await self._sleep(0)
            except Exception:
                return


def outbox_from_environment(
    environ: Mapping[str, str] | None = None,
) -> NotificationOutbox | None:
    values = os.environ if environ is None else environ
    if values.get("RISEX_TELEGRAM_ENABLED", "").strip().lower() != "true":
        return None
    token = values.get("RISEX_TELEGRAM_BOT_TOKEN")
    chat_id = values.get("RISEX_TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        raise RuntimeError("Telegram notification configuration is invalid")
    return NotificationOutbox(TelegramDelivery(token, chat_id))


def utc_time(value: datetime) -> datetime:
    return value.astimezone(UTC)
