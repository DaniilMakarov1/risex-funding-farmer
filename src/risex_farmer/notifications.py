"""Dependency-inverted, outbound-only runtime notifications."""

from __future__ import annotations

import asyncio
import hashlib
import html
import math
import os
import re
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP, localcontext
from enum import StrEnum
from typing import Protocol

import aiohttp


TELEGRAM_FULL_DIGEST_ROW_LIMIT = 10
MAX_NOTIFICATION_TEXT_LENGTH = 4096
TELEGRAM_HTML_PARSE_MODE = "HTML"
DEFAULT_LIFECYCLE_LEGS = ("RISEX", "HEDGE")


class NotificationScope(StrEnum):
    PAPER = "PAPER"
    TESTNET = "TESTNET"


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
    parse_mode: str | None = None
    raw_expected_pnl_usd: Decimal | None = None
    synthetic_test_pnl_overlay_usd: Decimal | None = None
    test_adjusted_expected_pnl_usd: Decimal | None = None

    def __post_init__(self) -> None:
        if self.occurred_at.tzinfo is None:
            raise ValueError("notification time must be timezone-aware")
        if not self.text or len(self.text) > MAX_NOTIFICATION_TEXT_LENGTH:
            raise ValueError("notification text must be bounded and non-empty")
        if self.parse_mode not in (None, TELEGRAM_HTML_PARSE_MODE):
            raise ValueError("unsupported Telegram parse mode")
        if (
            (self.kind == "FULL_SCAN_DIGEST")
            != (self.parse_mode == TELEGRAM_HTML_PARSE_MODE)
        ):
            raise ValueError(
                "HTML parse mode is reserved for FULL_SCAN_DIGEST payloads"
            )


def format_telegram_money(value: Decimal | str | None) -> str:
    if value is None:
        return "UNKNOWN"
    try:
        number = value if isinstance(value, Decimal) else Decimal(value)
        if not number.is_finite():
            return "UNKNOWN"
        with localcontext() as context:
            context.prec = max(
                28, len(number.as_tuple().digits) + 2, number.adjusted() + 3
            )
            rounded = number.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            return "0.00" if rounded.is_zero() else str(rounded)
    except (ArithmeticError, TypeError, ValueError):
        return "UNKNOWN"


def format_telegram_funding_countdown(
    target_cycle_start: object | None, scan_at: datetime
) -> str:
    """Render the fail-closed whole-minute countdown for one persisted route."""
    if not isinstance(target_cycle_start, str):
        return "Funding in: UNKNOWN"
    try:
        target_at = datetime.fromisoformat(target_cycle_start)
        if target_at.tzinfo is None or target_at.utcoffset() is None:
            return "Funding in: UNKNOWN"
        if scan_at.tzinfo is None or scan_at.utcoffset() is None:
            return "Funding in: UNKNOWN"
        remaining = target_at.astimezone(UTC) - scan_at.astimezone(UTC)
        if remaining < timedelta(0):
            return "Funding in: UNKNOWN"
        return f"Funding in: {remaining // timedelta(minutes=1)} min"
    except (ArithmeticError, OverflowError, TypeError, ValueError):
        return "Funding in: UNKNOWN"


def full_scan_digest_payloads(
    *,
    scan_at: datetime,
    opportunity: bool,
    route_rows: Sequence[Mapping[str, object]],
) -> tuple[NotificationPayload, ...]:
    scan_utc = utc_time(scan_at)
    status = "OPPORTUNITY" if opportunity else "NO TRADE"
    route_cards: list[str] = []
    for rank, row in enumerate(
        route_rows[:TELEGRAM_FULL_DIGEST_ROW_LIMIT], 1
    ):
        ticker = _bounded_digest_field(
            str(row.get("canonical_asset") or "UNKNOWN"), 48
        )
        hedge = str(row.get("hedge_venue") or "UNKNOWN")
        direction = row.get("direction")
        if direction == "LONG_RISEX_SHORT_HEDGE":
            route = f"RISEx LONG / {hedge} SHORT"
        elif direction == "SHORT_RISEX_LONG_HEDGE":
            route = f"RISEx SHORT / {hedge} LONG"
        else:
            route = f"RISEx UNKNOWN / {hedge} UNKNOWN"
        route = _bounded_digest_field(route, 112)
        synthetic_pnl_value = _synthetic_test_digest_value(row)
        if synthetic_pnl_value is not None:
            pnl_value = synthetic_pnl_value
        else:
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
                pnl_value = f"UNKNOWN — {_unknown_digest_label(blocker)}"
            else:
                pnl_value = f"${pnl_display}"
        pnl_value = _bounded_digest_field(
            pnl_value, 240 if synthetic_pnl_value is not None else 92
        )
        funding_field = format_telegram_funding_countdown(
            row.get("target_cycle_start"), scan_utc
        )
        funding_value = funding_field.removeprefix("Funding in: ")
        value_label = "Test economics" if synthetic_pnl_value is not None else "PnL"
        route_cards.append(
            f"<b>{rank}. {_escape_digest_field(ticker)}</b> — "
            f"{_escape_digest_field(route)}\n"
            f"{value_label}: <code>{_escape_digest_field(pnl_value)}</code> | "
            f"Funding in: <code>{_escape_digest_field(funding_value)}</code>"
        )
    scan_display = _escape_digest_field(scan_utc.isoformat())
    status_display = _escape_digest_field(status)

    def header(index: int, total: int) -> str:
        return (
            f"<b>Full Scan {index}/{total} | Status: {status_display}</b>\n"
            f"Scan UTC: <code>{scan_display}</code>"
        )

    # Pack complete two-line cards using a conservative header size. The
    # placeholder is deliberately wider than the actual part count.
    header_budget = len(header(99, 99))
    groups: list[list[str]] = [[]]
    current_length = header_budget
    for card in route_cards:
        added = len(card) + 2
        if groups[-1] and current_length + added > MAX_NOTIFICATION_TEXT_LENGTH:
            groups.append([])
            current_length = header_budget
        groups[-1].append(card)
        current_length += added
    total = len(groups)
    payloads: list[NotificationPayload] = []
    for index, group in enumerate(groups, 1):
        text = header(index, total)
        if group:
            text = "\n\n".join((text, "\n\n".join(group)))
        assert len(text) <= MAX_NOTIFICATION_TEXT_LENGTH
        payloads.append(NotificationPayload(
            f"full-scan-digest:{scan_utc.isoformat()}:part:{index}:{total}",
            "FULL_SCAN_DIGEST", scan_utc, text,
            parse_mode=TELEGRAM_HTML_PARSE_MODE,
        ))
    return tuple(payloads)


def _synthetic_test_digest_value(row: Mapping[str, object]) -> str | None:
    """Render the opt-in values without presenting them as realized PnL."""
    raw_overlay = row.get("synthetic_test_pnl_overlay_usd")
    try:
        overlay = raw_overlay if isinstance(raw_overlay, Decimal) else Decimal(str(raw_overlay))
    except (ArithmeticError, TypeError, ValueError):
        return None
    if not overlay.is_finite() or overlay.is_zero():
        return None
    raw = row.get("raw_expected_pnl_usd", row.get("planned_maker_net_pnl_usd"))
    adjusted = row.get("test_adjusted_expected_pnl_usd")
    if adjusted is None and raw is not None:
        try:
            adjusted = Decimal(str(raw)) + overlay
        except (ArithmeticError, TypeError, ValueError):
            adjusted = None

    def money(value: object | None) -> str:
        rendered = format_telegram_money(
            None if value is None else str(value)
        )
        return rendered if rendered == "UNKNOWN" else f"${rendered}"

    overlay_display = format_telegram_money(overlay)
    overlay_label = (
        "UNKNOWN" if overlay_display == "UNKNOWN"
        else f"+${overlay_display}" if overlay > 0
        else f"${overlay_display}"
    )
    return (
        "SYNTHETIC TEST (not realized) | "
        f"Raw expected: {money(raw)} | Overlay: {overlay_label} | "
        f"Adjusted test expected: {money(adjusted)}"
    )


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


def _escape_digest_field(value: str) -> str:
    return html.escape(value, quote=False)


_SENSITIVE_DISPLAY_TOKEN = re.compile(
    r"(?i)(?:0x[0-9a-f]{8,}|[A-Za-z0-9_-]{32,}|"
    r"(?:api[_ -]?key|token|secret|password|address|order[_ -]?id|"
    r"client[_ -]?id)\s*[:=]?\s*\S+)"
)
_SAFE_CODE = re.compile(r"[A-Z0-9_.:-]{1,64}")


def _safe_display(value: object | None, fallback: str, width: int) -> str:
    rendered = " ".join(str(value).split()) if value is not None else ""
    rendered = _SENSITIVE_DISPLAY_TOKEN.sub("REDACTED", rendered)
    return _bounded_digest_field(rendered or fallback, width)


def _safe_code(value: object | None, fallback: str = "UNCLASSIFIED") -> str:
    candidate = getattr(value, "value", value)
    rendered = str(candidate).strip().upper() if candidate is not None else ""
    return rendered if _SAFE_CODE.fullmatch(rendered) else fallback


def _opaque_identity(scope: object, value: object) -> str:
    material = f"{scope!s}:{value!s}".encode("utf-8", "replace")
    return hashlib.sha256(material).hexdigest()


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
        try:
            return self.delivery.enqueue(payload)
        except Exception:
            # A notification sink is observational.  A broken custom sink must
            # never turn an already-persisted lifecycle transition into a
            # runtime failure.
            return False

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
            notified = self.event(payload)
            if notified:
                self._active_outages.add(identity)
            return notified
        else:
            if identity not in self._active_outages:
                return False
            self._active_outages.remove(identity)
        return self.event(payload)


_FUNDING_STATUSES = frozenset({
    "PENDING", "ESTIMATED", "APPLIED_RATE", "UNRESOLVED",
    "SKIPPED_POSITION_NOT_OPEN", "SKIPPED_POSITION_CLOSED",
})


@dataclass(slots=True)
class _LifecycleNotificationState:
    scope: NotificationScope
    identity: str
    ticker: str
    route: str
    expected_legs: tuple[str, ...]
    opened_legs: set[str] = field(default_factory=set)
    closed_legs: set[str] = field(default_factory=set)
    funding: dict[str, tuple[str, str]] = field(default_factory=dict)
    active_blockers: set[str] = field(default_factory=set)
    activation_notified: bool = False
    pair_opened: bool = False
    exit_started: bool = False
    pair_closed: bool = False
    final_flat: bool = False


class LifecycleNotificationTracker:
    """Feed authoritative route milestones into the outbound-only outbox.

    The tracker is deliberately independent from paper and venue modules.  A
    Chief operational coordinator may feed TESTNET milestones after its own
    authoritative reconciliation, while the paper runtime feeds PAPER
    milestones after its atomic two-leg transitions.  The tracker never makes
    a venue decision, persists lifecycle state, or performs I/O itself.
    """

    def __init__(self, outbox: NotificationOutbox | None) -> None:
        self.outbox = outbox
        self._lifecycles: dict[tuple[NotificationScope, str], _LifecycleNotificationState] = {}

    def begin_lifecycle(
        self,
        *,
        scope: NotificationScope | str,
        lifecycle_key: str,
        ticker: object | None = None,
        route: object | None = None,
        expected_legs: Sequence[str] = DEFAULT_LIFECYCLE_LEGS,
    ) -> bool:
        normalized_scope = self._scope(scope)
        normalized_legs = self._legs(expected_legs)
        if normalized_scope is None or normalized_legs is None:
            return False
        if type(lifecycle_key) is not str or not lifecycle_key:
            return False
        identity = _opaque_identity(normalized_scope.value, lifecycle_key)
        key = (normalized_scope, identity)
        current = self._lifecycles.get(key)
        if current is not None:
            if current.expected_legs != normalized_legs:
                return False
            if not current.pair_opened:
                current.ticker = _safe_display(ticker, current.ticker, 48)
                current.route = _safe_display(route, current.route, 112)
            return True
        self._lifecycles[key] = _LifecycleNotificationState(
            normalized_scope,
            identity,
            _safe_display(ticker, "UNKNOWN", 48),
            _safe_display(route, "UNKNOWN ROUTE", 112),
            normalized_legs,
        )
        return True

    def maker_entry_activated(
        self,
        *,
        scope: NotificationScope | str,
        lifecycle_key: str,
        at: datetime,
        ticker: object | None = None,
        route: object | None = None,
        expected_legs: Sequence[str] = DEFAULT_LIFECYCLE_LEGS,
    ) -> bool:
        state = self._state(
            scope, lifecycle_key, ticker=ticker, route=route,
            expected_legs=expected_legs,
        )
        if state is None or state.activation_notified:
            return False
        state.activation_notified = True
        return self._emit(
            state, "ENTRY_ACTIVATED", at, "entry-activated",
            f"{state.scope.value} | MAKER ENTRY ACTIVATED | "
            f"{state.ticker} | {state.route}",
        )

    def confirm_leg_open(
        self,
        *,
        scope: NotificationScope | str,
        lifecycle_key: str,
        leg: str,
        at: datetime,
        authoritative: bool,
        ticker: object | None = None,
        route: object | None = None,
        expected_legs: Sequence[str] = DEFAULT_LIFECYCLE_LEGS,
    ) -> bool:
        state = self._state(
            scope, lifecycle_key, ticker=ticker, route=route,
            expected_legs=expected_legs,
        )
        normalized_leg = _safe_code(leg, "UNCLASSIFIED")
        if (
            state is None or authoritative is not True
            or normalized_leg not in state.expected_legs
            or state.pair_opened
        ):
            return False
        state.opened_legs.add(normalized_leg)
        if set(state.expected_legs) != state.opened_legs:
            return False
        return self._mark_pair_open(state, at)

    def confirm_pair_open(
        self,
        *,
        scope: NotificationScope | str,
        lifecycle_key: str,
        authoritative_legs: Mapping[str, bool] | Iterable[str],
        at: datetime,
        authoritative: bool = False,
        ticker: object | None = None,
        route: object | None = None,
        expected_legs: Sequence[str] = DEFAULT_LIFECYCLE_LEGS,
    ) -> bool:
        state = self._state(
            scope, lifecycle_key, ticker=ticker, route=route,
            expected_legs=expected_legs,
        )
        confirmed = self._confirmed_legs(authoritative_legs)
        if (
            state is None or authoritative is not True
            or confirmed != set(state.expected_legs)
            or state.pair_opened
        ):
            return False
        state.opened_legs = confirmed
        return self._mark_pair_open(state, at)

    def funding_status(
        self,
        *,
        scope: NotificationScope | str,
        lifecycle_key: str,
        settlement_key: str,
        status: str,
        cash_usd: Decimal | str | None,
        at: datetime,
        ticker: object | None = None,
        route: object | None = None,
        venue: object | None = None,
        market: object | None = None,
        source: object | None = None,
        expected_legs: Sequence[str] = DEFAULT_LIFECYCLE_LEGS,
    ) -> bool:
        state = self._state(
            scope, lifecycle_key, ticker=ticker, route=route,
            expected_legs=expected_legs,
        )
        normalized_status = _safe_code(status, "UNCLASSIFIED")
        if (
            state is None or not state.pair_opened
            or type(settlement_key) is not str or not settlement_key
            or normalized_status not in _FUNDING_STATUSES
        ):
            return False
        cash_display = format_telegram_money(cash_usd)
        settlement_identity = _opaque_identity("funding", settlement_key)
        signature = (normalized_status, cash_display)
        if state.funding.get(settlement_identity) == signature:
            return False
        state.funding[settlement_identity] = signature
        rendered_venue = _safe_display(venue, "UNKNOWN VENUE", 24)
        rendered_market = _safe_display(market, state.ticker, 48)
        rendered_source = _safe_code(source, "UNSPECIFIED")
        return self._emit(
            state,
            "FUNDING_STATUS",
            at,
            f"funding:{settlement_identity}:{normalized_status}:{cash_display}",
            f"{state.scope.value} | FUNDING STATUS | {state.ticker} | "
            f"{state.route} | {rendered_venue} {rendered_market} | "
            f"status {normalized_status} | cash USD {cash_display} | "
            f"source {rendered_source}",
        )

    def exit_started(
        self,
        *,
        scope: NotificationScope | str,
        lifecycle_key: str,
        at: datetime,
        ticker: object | None = None,
        route: object | None = None,
        expected_legs: Sequence[str] = DEFAULT_LIFECYCLE_LEGS,
    ) -> bool:
        state = self._state(
            scope, lifecycle_key, ticker=ticker, route=route,
            expected_legs=expected_legs,
        )
        if state is None or not state.pair_opened or state.exit_started:
            return False
        state.exit_started = True
        return self._emit(
            state, "EXIT_STARTED", at, "exit-started",
            f"{state.scope.value} | EXIT STARTED | {state.ticker} | {state.route}",
        )

    def confirm_leg_closed(
        self,
        *,
        scope: NotificationScope | str,
        lifecycle_key: str,
        leg: str,
        at: datetime,
        authoritative: bool,
        ticker: object | None = None,
        route: object | None = None,
        final_pnl_usd: Decimal | str | None = None,
        expected_legs: Sequence[str] = DEFAULT_LIFECYCLE_LEGS,
    ) -> bool:
        state = self._state(
            scope, lifecycle_key, ticker=ticker, route=route,
            expected_legs=expected_legs,
        )
        normalized_leg = _safe_code(leg, "UNCLASSIFIED")
        if (
            state is None or authoritative is not True
            or not state.pair_opened or not state.exit_started
            or normalized_leg not in state.expected_legs
            or state.pair_closed
        ):
            return False
        state.closed_legs.add(normalized_leg)
        if set(state.expected_legs) != state.closed_legs:
            return False
        return self._mark_pair_closed(state, at, final_pnl_usd)

    def confirm_pair_closed(
        self,
        *,
        scope: NotificationScope | str,
        lifecycle_key: str,
        authoritative_legs: Mapping[str, bool] | Iterable[str],
        at: datetime,
        authoritative: bool = False,
        ticker: object | None = None,
        route: object | None = None,
        final_pnl_usd: Decimal | str | None = None,
        expected_legs: Sequence[str] = DEFAULT_LIFECYCLE_LEGS,
    ) -> bool:
        state = self._state(
            scope, lifecycle_key, ticker=ticker, route=route,
            expected_legs=expected_legs,
        )
        confirmed = self._confirmed_legs(authoritative_legs)
        if (
            state is None or authoritative is not True
            or not state.pair_opened or not state.exit_started
            or confirmed != set(state.expected_legs)
            or state.pair_closed
        ):
            return False
        state.closed_legs = confirmed
        return self._mark_pair_closed(state, at, final_pnl_usd)

    def confirm_final_flat(
        self,
        *,
        scope: NotificationScope | str,
        lifecycle_key: str,
        authoritative_legs: Mapping[str, bool] | Iterable[str],
        zero_orders: bool,
        exact_flat: bool,
        at: datetime,
        authoritative: bool = False,
        ticker: object | None = None,
        route: object | None = None,
        expected_legs: Sequence[str] = DEFAULT_LIFECYCLE_LEGS,
    ) -> bool:
        state = self._state(
            scope, lifecycle_key, ticker=ticker, route=route,
            expected_legs=expected_legs,
        )
        confirmed = self._confirmed_legs(authoritative_legs)
        if (
            state is None or authoritative is not True
            or not state.pair_closed
            or confirmed != set(state.expected_legs)
            or zero_orders is not True or exact_flat is not True
            or state.final_flat
        ):
            return False
        state.final_flat = True
        return self._emit(
            state, "FINAL_FLAT", at, "final-flat",
            f"{state.scope.value} | FINAL FLAT | {state.ticker} | {state.route} | "
            "zero relevant orders | exact flat",
        )

    def lifecycle_blocked(
        self,
        *,
        scope: NotificationScope | str,
        lifecycle_key: str,
        episode_key: str,
        failure_class: object,
        stage: object,
        at: datetime,
        reason: object | None = None,
        ticker: object | None = None,
        route: object | None = None,
        expected_legs: Sequence[str] = DEFAULT_LIFECYCLE_LEGS,
    ) -> bool:
        state = self._state(
            scope, lifecycle_key, ticker=ticker, route=route,
            expected_legs=expected_legs,
        )
        if state is None or type(episode_key) is not str or not episode_key:
            return False
        episode_identity = _opaque_identity("blocker", episode_key)
        if episode_identity in state.active_blockers:
            return False
        failure = _safe_code(failure_class, "")
        selected_stage = _safe_code(stage, "")
        if not failure or not selected_stage:
            return False
        rendered_reason = _safe_code(reason, "") if reason is not None else ""
        suffix = f"blocked:{episode_identity}:{failure}:{selected_stage}"
        text = (
            f"{state.scope.value} | LIFECYCLE BLOCKED | {state.ticker} | "
            f"{state.route} | class {failure} | stage {selected_stage}"
        )
        if rendered_reason:
            text += f" | reason {rendered_reason}"
        queued = self._emit(state, "LIFECYCLE_BLOCKED", at, suffix, text)
        # Recovery is paired only with a blocker that was actually accepted by
        # the outbox.  A saturated/failed sink must not create an orphaned
        # recovery alert, and this bookkeeping never touches lifecycle state.
        if queued:
            state.active_blockers.add(episode_identity)
        return queued

    def lifecycle_recovered(
        self,
        *,
        scope: NotificationScope | str,
        lifecycle_key: str,
        episode_key: str,
        at: datetime,
        failure_class: object | None = None,
        stage: object | None = None,
        ticker: object | None = None,
        route: object | None = None,
        expected_legs: Sequence[str] = DEFAULT_LIFECYCLE_LEGS,
    ) -> bool:
        state = self._state(
            scope, lifecycle_key, ticker=ticker, route=route,
            expected_legs=expected_legs,
        )
        if state is None or type(episode_key) is not str or not episode_key:
            return False
        episode_identity = _opaque_identity("blocker", episode_key)
        if episode_identity not in state.active_blockers:
            return False
        state.active_blockers.remove(episode_identity)
        failure = _safe_code(failure_class, "RECOVERED")
        selected_stage = _safe_code(stage, "RECOVERY")
        return self._emit(
            state,
            "LIFECYCLE_RECOVERED",
            at,
            f"recovered:{episode_identity}",
            f"{state.scope.value} | LIFECYCLE RECOVERED | {state.ticker} | "
            f"{state.route} | class {failure} | stage {selected_stage}",
        )

    def _mark_pair_open(
        self, state: _LifecycleNotificationState, at: datetime
    ) -> bool:
        state.pair_opened = True
        return self._emit(
            state, "POSITION_OPENED", at, "pair-opened",
            f"{state.scope.value} | OPEN | {state.ticker} | {state.route} | "
            "both legs authoritative",
        )

    def _mark_pair_closed(
        self,
        state: _LifecycleNotificationState,
        at: datetime,
        final_pnl_usd: Decimal | str | None,
    ) -> bool:
        state.pair_closed = True
        return self._emit(
            state,
            "POSITION_CLOSED",
            at,
            "pair-closed",
            f"{state.scope.value} | CLOSED | {state.ticker} | {state.route} | "
            f"both legs authoritative | final PnL USD "
            f"{format_telegram_money(final_pnl_usd)}",
            final_pnl_usd=final_pnl_usd,
        )

    def _state(
        self,
        scope: NotificationScope | str,
        lifecycle_key: str,
        *,
        ticker: object | None,
        route: object | None,
        expected_legs: Sequence[str],
    ) -> _LifecycleNotificationState | None:
        if not self.begin_lifecycle(
            scope=scope, lifecycle_key=lifecycle_key, ticker=ticker,
            route=route, expected_legs=expected_legs,
        ):
            return None
        normalized_scope = self._scope(scope)
        assert normalized_scope is not None
        identity = _opaque_identity(normalized_scope.value, lifecycle_key)
        return self._lifecycles[(normalized_scope, identity)]

    @staticmethod
    def _scope(value: NotificationScope | str) -> NotificationScope | None:
        candidate = getattr(value, "value", value)
        rendered = str(candidate).strip().upper()
        try:
            return NotificationScope(rendered)
        except ValueError:
            return None

    @staticmethod
    def _legs(value: Sequence[str]) -> tuple[str, ...] | None:
        if isinstance(value, (str, bytes)):
            return None
        try:
            rows = tuple(_safe_code(item, "UNCLASSIFIED") for item in value)
        except TypeError:
            return None
        if (
            len(rows) != 2
            or any(item == "UNCLASSIFIED" for item in rows)
            or len(set(rows)) != len(rows)
        ):
            return None
        return rows

    @staticmethod
    def _confirmed_legs(
        value: Mapping[str, bool] | Iterable[str],
    ) -> set[str]:
        if isinstance(value, Mapping):
            return {
                _safe_code(item, "UNCLASSIFIED")
                for item, confirmed in value.items()
                if confirmed is True
            }
        if isinstance(value, (str, bytes)):
            return set()
        try:
            return {_safe_code(item, "UNCLASSIFIED") for item in value}
        except TypeError:
            return set()

    def _emit(
        self,
        state: _LifecycleNotificationState,
        kind: str,
        at: datetime,
        suffix: str,
        text: str,
        *,
        final_pnl_usd: Decimal | str | None = None,
    ) -> bool:
        if self.outbox is None:
            return False
        try:
            payload = NotificationPayload(
                f"lifecycle:{state.scope.value.lower()}:{state.identity}:{_opaque_identity(kind, suffix)}",
                kind,
                utc_time(at),
                _bounded_digest_field(text, MAX_NOTIFICATION_TEXT_LENGTH),
                ticker=state.ticker,
                route=state.route,
                final_pnl_usd=(
                    None if final_pnl_usd is None else _decimal_or_none(final_pnl_usd)
                ),
            )
            return self.outbox.event(payload)
        except Exception:
            return False


def _decimal_or_none(value: Decimal | str | None) -> Decimal | None:
    if value is None:
        return None
    try:
        number = value if isinstance(value, Decimal) else Decimal(value)
    except (TypeError, ValueError, ArithmeticError):
        return None
    return number if number.is_finite() else None


# Both names describe the same intentionally small, non-venue-specific seam.
LifecycleNotificationBoundary = LifecycleNotificationTracker


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
        if payload.parse_mode == TELEGRAM_HTML_PARSE_MODE:
            body["parse_mode"] = TELEGRAM_HTML_PARSE_MODE
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
