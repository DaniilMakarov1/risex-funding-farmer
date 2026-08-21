"""Dependency-inverted, outbound-only runtime notifications."""

from __future__ import annotations

import asyncio
import math
import os
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, ROUND_HALF_UP, localcontext
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


def format_telegram_money(value: Decimal | str | None) -> str:
    if value is None:
        return "UNKNOWN"
    number = value if isinstance(value, Decimal) else Decimal(value)
    with localcontext() as context:
        context.prec = max(
            28, len(number.as_tuple().digits) + 2, number.adjusted() + 3
        )
        rounded = number.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        return "0.00" if rounded.is_zero() else str(rounded)


def full_scan_digest_payloads(
    *,
    scan_at: datetime,
    opportunity: bool,
    route_rows: Sequence[Mapping[str, object]],
) -> tuple[NotificationPayload, ...]:
    scan_utc = utc_time(scan_at)
    status = "OPPORTUNITY" if opportunity else "NO TRADE"
    route_lines: list[str] = []
    for row in route_rows:
        ticker = _bounded_digest_field(str(row.get("canonical_asset") or "UNKNOWN"), 48)
        hedge = str(row.get("hedge_venue") or "UNKNOWN")
        direction = row.get("direction")
        if direction == "LONG_RISEX_SHORT_HEDGE":
            route = f"RISEx LONG / {hedge} SHORT"
        elif direction == "SHORT_RISEX_LONG_HEDGE":
            route = f"RISEx SHORT / {hedge} LONG"
        else:
            route = f"RISEx UNKNOWN / {hedge} UNKNOWN"
        route = _bounded_digest_field(route, 112)
        pnl = row.get("planned_maker_net_pnl_usd")
        pnl_display = format_telegram_money(
            None if pnl is None else str(pnl)
        )
        if pnl_display == "UNKNOWN":
            blockers = row.get("blockers")
            blocker = (
                str(blockers[0])
                if isinstance(blockers, (list, tuple)) and blockers
                else "AUTHORITATIVE_VALUE_UNAVAILABLE"
            )
            pnl_field = f"Expected PnL: UNKNOWN — {_unknown_digest_label(blocker)}"
        else:
            pnl_field = f"Expected PnL: ${pnl_display}"
        route_lines.append(
            f"{ticker} | {route} | {_bounded_digest_field(pnl_field, 92)}"
        )
    header_budget = len(
        f"Full Scan 99/99 | Scan UTC: {scan_utc.isoformat()} | Status: {status}\n"
    )
    groups: list[list[str]] = [[]]
    current_length = header_budget
    for line in route_lines:
        added = len(line) + 1
        if groups[-1] and current_length + added > 4096:
            groups.append([])
            current_length = header_budget
        groups[-1].append(line)
        current_length += added
    total = len(groups)
    payloads: list[NotificationPayload] = []
    for index, group in enumerate(groups, 1):
        header = (
            f"Full Scan {index}/{total} | Scan UTC: {scan_utc.isoformat()} | "
            f"Status: {status}"
        )
        text = "\n".join((header, *group))
        assert len(text) <= 4096
        payloads.append(NotificationPayload(
            f"full-scan-digest:{scan_utc.isoformat()}:part:{index}:{total}",
            "FULL_SCAN_DIGEST", scan_utc, text,
        ))
    return tuple(payloads)


def _unknown_digest_label(blocker: str) -> str:
    normalized = blocker.upper()
    if "MARKET_METADATA" in normalized:
        return "market metadata stale"
    if "CATALOG" in normalized:
        return "Extended catalog"
    if "BOOK" in normalized or "DEPTH" in normalized:
        return "book stream"
    if "TRADE" in normalized:
        return "trade stream"
    if "FUNDING" in normalized:
        return "funding"
    if "RISEX" in normalized or "PARITY" in normalized or "MULTIPLIER" in normalized:
        return "RISEx parity"
    return "public evidence unavailable"


def _bounded_digest_field(value: str, width: int) -> str:
    return value if len(value) <= width else value[: width - 1] + "…"


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
            flood_control_delay: float | None = None
            try:
                async with asyncio.timeout(self._timeout_seconds):
                    async with self._session.post(url, json=body) as response:  # type: ignore[attr-defined]
                        status = int(response.status)
                        if 200 <= status < 300:
                            return
                        if status == 429 and attempt + 1 < self._max_attempts:
                            retry_after = await _telegram_retry_after(response)
                            if retry_after is None:
                                return
                            flood_control_delay = retry_after
                        else:
                            return
            except asyncio.CancelledError:
                raise
            except TimeoutError:
                return
            except aiohttp.ClientConnectorError:
                if attempt + 1 >= self._max_attempts:
                    return
                await self._sleep(1.0)
            except Exception:
                return
            if flood_control_delay is not None:
                await self._sleep(flood_control_delay)


async def _telegram_retry_after(response: object) -> float | None:
    try:
        body = await response.json(content_type=None)  # type: ignore[attr-defined]
        raw = body["parameters"]["retry_after"]
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            return None
        if raw <= 0 or (isinstance(raw, float) and not math.isfinite(raw)):
            return None
        return float(min(raw, 30))
    except Exception:
        return None


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
