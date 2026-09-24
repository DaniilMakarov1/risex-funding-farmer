"""Explicit, opt-in Lighter SDK/HTTP adapter for HCR-1.

No SDK module is imported at package import time.  Mutation calls use the
SDK's ``sign_*`` methods and one explicit ``sendTx`` form request.  The
combined ``create_order``/``cancel_order`` helpers are intentionally not used,
because their retry and response boundaries are not suitable for one-attempt
close/reopen semantics.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import importlib
from importlib import metadata as importlib_metadata
import inspect
import json
import math
import re
import time
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol

from .contracts import (
    AccountMarginEvidence,
    AccountSnapshot,
    ContractError,
    HandoffConfig,
    HistoryPage,
    LeverageNotSent,
    MarketMetadata,
    MutationReceipt,
    OrderPlan,
    OrderSnapshot,
    TradeReceipt,
    OFFICIAL_MAINNET_CHAIN_ID,
    _nonnegative,
)
from .journal import sanitize_exception


REQUIRED_LIGHTER_SDK_VERSION = "1.1.2"
# The official orderBookOrders endpoint requires a bounded page size.  This is
# a transport/read bound only; slice quantity remains operator/depth-driven.
ROBINHOOD_ORDER_BOOK_LIMIT = 250


def _leverage_tx_hash(value: Any) -> str | None:
    """Keep the pinned native signer's exact, bounded transaction identity."""
    return value if isinstance(value, str) and re.fullmatch(r"[0-9a-fA-F]{80}", value) else None


def _leverage_tx_diagnostic(raw: Mapping[str, Any], tx_hash: str, now: float) -> dict[str, Any]:
    """Sanitize a transaction read; these fields do not establish finality."""
    result = {key: raw.get(key) for key in (
        "hash", "type", "status", "account_index", "api_key_index",
        "nonce", "executed_at", "committed_at", "verified_at",
    )}
    numeric = ("type", "status", "account_index", "api_key_index", "nonce",
               "executed_at", "committed_at", "verified_at")
    if (_leverage_tx_hash(tx_hash) is None or result["hash"] != tx_hash
            or any(type(result[key]) is not int for key in numeric)):
        raise ContractError("leverage transaction identity or status is incomplete")
    if (result["type"] != 20 or result["status"] not in (0, 1, 2, 3)
            or any(result[key] < 0 for key in numeric)):
        raise ContractError("leverage transaction diagnostic fields are invalid")
    times = [result[key] for key in ("executed_at", "committed_at", "verified_at")]
    # The documented examples use epoch seconds. Do not convert guessed units:
    # an incompatible or future value is diagnostic uncertainty, never proof.
    if (not math.isfinite(now) or now <= 0 or any(t > now for t in times)
            or any(later and (not earlier or later < earlier)
                   for earlier, later in zip(times, times[1:]))):
        raise ContractError("leverage transaction timing is unproved")
    return result


def _leverage_next_nonce(raw: Mapping[str, Any]) -> int:
    _require_success_code(raw, "nextNonce")
    nonce = raw.get("nonce")
    if type(nonce) is not int or nonce < 0:
        raise ContractError("nextNonce is incomplete")
    return nonce


class SecretProvider(Protocol):
    def private_key(self, account_index: int, api_key_index: int) -> str: ...


class MissingSdkError(RuntimeError):
    pass


class SdkVersionError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class _CachedToken:
    value: str
    expires_at: float


def _order_plan_binding(plan: OrderPlan) -> tuple[Any, ...]:
    """Return the immutable fields a prepared mutation is allowed to carry."""

    return (
        plan.account_index,
        plan.market_id,
        plan.side,
        plan.quantity,
        plan.quantity_int,
        plan.price,
        plan.price_int,
        plan.order_type,
        plan.time_in_force,
        plan.reduce_only,
        plan.order_expiry_ms,
        plan.client_order_index,
    )


@dataclass(frozen=True, slots=True, repr=False)
class ReservedNonce:
    """Client-owned, unsent account/key reservation; never serialized."""

    account_index: int
    api_key_index: int
    deadline: float
    _nonce: int
    _owner: object
    _state: str = "RESERVED"
    diagnostic_timings: dict[str, float] = field(default_factory=dict, repr=False)

    def __repr__(self) -> str:
        return f"ReservedNonce(account_index={self.account_index}, state={self._state!r})"


@dataclass(slots=True, repr=False)
class PreparedMutation:
    """One in-memory, single-use signed mutation.

    The signed transaction is deliberately private and is never included in a
    journal, ``as_dict`` payload, exception or representation.  ``state`` is
    consumed before the transport call so an ambiguous response cannot be
    replayed by the caller.
    """

    account_index: int
    api_key_index: int
    plan_binding: tuple[Any, ...]
    deadline: float
    _tx_type: int
    _tx_info: str
    _tx_hash: str | None
    _owner: object
    _nonce: int
    _state: str = "READY"
    # Numeric diagnostics only; signed payloads/nonces never leave this token.
    diagnostic_timings: dict[str, float] = field(default_factory=dict, repr=False)

    def __repr__(self) -> str:
        return (
            "PreparedMutation(account_index="
            f"{self.account_index}, api_key_index={self.api_key_index}, "
            f"deadline={self.deadline!r}, state={self._state!r})"
        )

    def matches(self, plan: OrderPlan, *, api_key_index: int) -> bool:
        return (
            self._state == "READY"
            and self.account_index == plan.account_index
            and self.api_key_index == api_key_index
            and self.plan_binding == _order_plan_binding(plan)
            and (
                plan.mutation_deadline_monotonic is None
                or plan.mutation_deadline_monotonic == self.deadline
            )
        )

    def invalidate(self) -> None:
        if self._state == "READY":
            self._state = "INVALIDATED"

    def consume(self) -> bool:
        if self._state != "READY":
            return False
        self._state = "CONSUMED"
        return True


@dataclass(frozen=True, slots=True)
class StaticSecretProvider:
    """A caller-owned provider; the key is never placed in HCR config/journal."""

    values: Mapping[int, str]

    def private_key(self, account_index: int, api_key_index: int) -> str:
        value = self.values.get(account_index)
        if not isinstance(value, str) or not value:
            raise ContractError(f"private key for account {account_index} is unavailable")
        return value


async def _await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def _close_resource(value: Any) -> bool:
    """Best-effort close for SDK and injected synthetic resources."""

    if value is None:
        return True
    for name in ("aclose", "close"):
        method = getattr(value, name, None)
        if not callable(method):
            continue
        try:
            result = method()
            if inspect.isawaitable(result):
                await result
        except BaseException:
            # Teardown must not replace an already-observed execution result or
            # turn an interrupted attempt into a false terminal claim.
            return False
        return True
    return False


def _model_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if hasattr(value, "to_dict"):
        converted = value.to_dict()
        if isinstance(converted, Mapping):
            return dict(converted)
    result: dict[str, Any] = {}
    for name in dir(value):
        if name.startswith("_"):
            continue
        try:
            item = getattr(value, name)
        except Exception:
            continue
        if callable(item):
            continue
        if isinstance(item, (str, int, float, bool, type(None), list, tuple, dict)):
            result[name] = item
    return result


def _first_mapping(value: Any, *keys: str) -> dict[str, Any]:
    raw = _model_dict(value)
    for key in keys:
        nested = raw.get(key)
        if isinstance(nested, list) and nested:
            return _model_dict(nested[0])
        if isinstance(nested, Mapping):
            return dict(nested)
    return raw


def _order_snapshot_mapping(value: Any, *, observed_at: float) -> dict[str, Any]:
    """Normalize one official Order model without weakening its contract.

    The pinned SDK exposes ``is_ask`` as the authoritative side and labels its
    ``side`` field as legacy.  Its generated ``from_dict`` also supplies a
    default ``buy`` side when the wire response omits that legacy field.  The
    authoritative boolean is therefore required at this SDK boundary; a
    side-only response remains incomplete rather than being guessed.
    """

    mapped = _model_dict(value)
    if "is_ask" not in mapped:
        if "side" not in mapped:
            raise ContractError("order response lacks required is_ask/side")
        raise ContractError("order response lacks required is_ask")
    is_ask = mapped["is_ask"]
    if not isinstance(is_ask, bool):
        raise ContractError("order response is_ask must be bool")
    required_fields = {
        "account_index": ("account_index", "owner_account_index"),
        "market_id": ("market_id", "market_index"),
        "order_id": ("order_id",),
        "client_order_index": ("client_order_index",),
        "status": ("status",),
        "type": ("type", "order_type"),
        "time_in_force": ("time_in_force",),
        "reduce_only": ("reduce_only",),
        "initial_base_amount": ("initial_quantity", "initial_base_amount", "base_amount"),
        "remaining_base_amount": ("remaining_quantity", "remaining_base_amount"),
        "filled_base_amount": ("filled_quantity", "filled_base_amount"),
        "price": ("price", "base_price"),
    }
    for field, aliases in required_fields.items():
        if not any(alias in mapped and mapped[alias] is not None for alias in aliases):
            raise ContractError(f"order response lacks required {field}")
    mapped["side"] = "SELL" if is_ask else "BUY"
    mapped["observed_at"] = observed_at
    return mapped


def _trade_fee_evidence(value: Mapping[str, Any], *, is_ask: bool, distinct_accounts: bool) -> dict[str, Any]:
    """Preserve official fee components without guessing the integer unit.

    The pinned SDK exposes role-specific integers, not a quote-currency fee.
    Explicit zero for BOTH components is unit-independent. Nonzero amounts
    remain unknown until their units are established for this venue.
    """
    maker_ask = value.get("is_maker_ask")
    result: dict[str, Any] = {"fee": None, "fee_role": None, "venue_fee_raw": None,
                              "integrator_fee_raw": None, "fee_evidence": "MISSING_OR_INVALID_COMPONENTS"}
    if not isinstance(maker_ask, bool) or not distinct_accounts:
        return result
    role = "maker" if maker_ask == is_ask else "taker"
    result["fee_role"] = role
    for output, field in (("venue_fee_raw", f"{role}_fee"), ("integrator_fee_raw", f"integrator_{role}_fee")):
        raw = value.get(field)
        if isinstance(raw, int) and not isinstance(raw, bool) and raw >= 0:
            result[output] = raw
    components = (result["venue_fee_raw"], result["integrator_fee_raw"])
    if None not in components:
        if components == (0, 0):
            result.update(fee="0", fee_evidence="EXPLICIT_ZERO_OFFICIAL_COMPONENTS")
        else:
            result["fee_evidence"] = "NONZERO_UNIT_UNVERIFIED"
    return result


def _trade_timestamp_seconds(value: Any) -> float:
    """Convert the documented integer millisecond timestamp to seconds."""

    if isinstance(value, bool) or not isinstance(value, int):
        raise ContractError("trade timestamp must be an integer millisecond value")
    if value < 0:
        raise ContractError("trade timestamp must be non-negative milliseconds")
    try:
        seconds = value / 1_000.0
    except OverflowError as exc:
        raise ContractError("trade timestamp must convert to finite seconds") from exc
    if not math.isfinite(seconds):
        raise ContractError("trade timestamp must convert to finite seconds")
    return seconds


class PlainAioHttp:
    """Small owned HTTP transport with one reusable connection pool.

    The session is created lazily so importing/constructing the adapter remains
    credential- and network-free.  Authentication is supplied on each
    request, and mutation calls continue to disable redirects and retries.
    """

    def __init__(self, base_url: str, *, timeout_seconds: float) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = float(timeout_seconds)
        self._session: Any | None = None
        self._session_lock = asyncio.Lock()
        self._closed = False

    @staticmethod
    def _timing_trace() -> Any:
        """Observe aiohttp milestones without recording request or response data."""
        import aiohttp

        trace = aiohttp.TraceConfig()

        async def mark(name: str, _session: Any, context: Any, _params: Any) -> None:
            times = getattr(context, "trace_request_ctx", None)
            if isinstance(times, dict):
                times.setdefault(name, time.perf_counter())

        for signal, name in (
            (trace.on_connection_queued_start, "queue_start"),
            (trace.on_connection_queued_end, "queue_end"),
            (trace.on_connection_create_start, "connection_start"),
            (trace.on_connection_create_end, "connection_end"),
            (trace.on_connection_reuseconn, "connection_reused"),
            (trace.on_request_headers_sent, "headers_signal"),
            (trace.on_request_chunk_sent, "body_signal"),
        ):
            async def observer(session: Any, context: Any, params: Any, *, _name: str = name) -> None:
                await mark(_name, session, context, params)
            signal.append(observer)
        return trace

    async def _session_for_request(self) -> Any:
        if self._closed:
            raise RuntimeError("HTTP transport is closed")
        session = self._session
        if session is not None and not bool(getattr(session, "closed", False)):
            return session
        try:
            import aiohttp
        except ImportError as exc:
            raise MissingSdkError("aiohttp is required for explicit accountOrders transport") from exc
        async with self._session_lock:
            if self._closed:
                raise RuntimeError("HTTP transport is closed")
            session = self._session
            if session is None or bool(getattr(session, "closed", False)):
                timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
                session = aiohttp.ClientSession(timeout=timeout, trace_configs=[self._timing_trace()])
                self._session = session
            return session

    async def aclose(self) -> None:
        """Close the owned session exactly once, including cancellation."""

        self._closed = True

        async def detach_and_close() -> None:
            async with self._session_lock:
                session, self._session = self._session, None
            if session is not None:
                await _await(session.close())

        # Shield the actual close so cancellation of the caller cannot leave
        # an owned connector behind.  A concurrent/idempotent close simply
        # detaches no session on its second pass.
        cleanup = asyncio.create_task(detach_and_close())
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await asyncio.shield(cleanup)
            raise

    async def close(self) -> None:
        await self.aclose()

    async def get(self, path: str, *, params: Mapping[str, Any], authorization: str) -> dict[str, Any]:
        session = await self._session_for_request()
        async with session.get(
            f"{self.base_url}/{path.lstrip('/')}",
            params=dict(params),
            headers={"Authorization": authorization},
            allow_redirects=False,
        ) as response:
            raw = await response.text()
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Lighter response was not JSON (HTTP {response.status})") from exc
            if response.status < 200 or response.status >= 300:
                code = payload.get("code") if isinstance(payload, Mapping) else None
                raise RuntimeError(f"Lighter read rejected HTTP {response.status}, code={code}")
            if not isinstance(payload, Mapping):
                raise RuntimeError("Lighter read response is not an object")
            return dict(payload)

    async def post_form(self, path: str, *, form: Mapping[str, Any]) -> dict[str, Any]:
        """Send exactly one mutation request without aiohttp-retry or SDK REST."""

        entered = time.perf_counter()
        session = await self._session_for_request()
        ready = time.perf_counter()
        trace_times: dict[str, float] = {}
        async with session.post(
            f"{self.base_url}/{path.lstrip('/')}",
            data=dict(form),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            allow_redirects=False,
            trace_request_ctx=trace_times,
        ) as response:
            headers_at = time.perf_counter()
            content = getattr(response, "content", None)
            if content is not None and callable(getattr(content, "read", None)):
                first = await content.read(1)
                first_byte_at = time.perf_counter() if first else None
                rest = await response.read()
                raw = (first + rest).decode("utf-8")
            else:  # Lightweight offline transport fakes expose text only.
                first_byte_at = None
                raw = await response.text()
            complete_at = time.perf_counter()
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Lighter mutation response was not JSON (HTTP {response.status})") from exc
            if not isinstance(payload, Mapping):
                raise RuntimeError("Lighter mutation response is not an object")
            timing = {
                "http_session_ready_seconds": ready - entered,
                "http_response_headers_seconds": headers_at - entered,
                "http_full_body_seconds": complete_at - entered,
                "http_parse_seconds": time.perf_counter() - complete_at,
            }
            if first_byte_at is not None:
                timing["http_first_body_byte_seconds"] = first_byte_at - entered
            for key, signal in (("http_headers_signal_seconds", "headers_signal"),
                                ("http_body_signal_seconds", "body_signal")):
                if signal in trace_times:
                    timing[key] = trace_times[signal] - entered
            for key, start, end in (("http_queue_seconds", "queue_start", "queue_end"),
                                    ("http_connection_setup_seconds", "connection_start", "connection_end")):
                if start in trace_times and end in trace_times:
                    timing[key] = trace_times[end] - trace_times[start]
            if "connection_reused" in trace_times:
                timing["http_connection_reused"] = 1.0
            return {**dict(payload), "_http_status": response.status, "_transport_timing": timing}


class LighterSdkClient:
    """Production adapter, constructed only after explicit operator opt-in."""

    supports_leverage_prepared_intent = True

    source_account_index: int
    receiver_account_index: int

    def __init__(
        self,
        config: HandoffConfig,
        *,
        source_account_index: int,
        receiver_account_index: int,
        secrets: SecretProvider,
        market_evidence: Mapping[str, Any],
        signer_factory: Callable[..., Any] | None = None,
        api_factory: Callable[..., Any] | None = None,
        http_factory: Callable[..., Any] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if config.api_base_url is None:
            raise ContractError("api_base_url is required for the explicit live adapter")
        if config.api_key_index is None:
            raise ContractError("api_key_index is required for the explicit live adapter")
        if source_account_index == receiver_account_index:
            raise ContractError("source and receiver accounts must differ")
        self.config = config
        self.source_account_index = source_account_index
        self.receiver_account_index = receiver_account_index
        self.secrets = secrets
        self.market_evidence = dict(market_evidence)
        self._signer_factory = signer_factory
        self._api_factory = api_factory
        self._clock = clock
        self._signers: dict[int, Any] = {}
        self._apis: dict[int, Any] = {}
        self._tokens: dict[int, _CachedToken] = {}
        self._api_client: Any | None = None
        self._pending_mutation_deadline: float | None = None
        # Nonce acquisition and signing are serialized per account/key.  A
        # paired source/receiver preparation may therefore overlap when the
        # accounts (or keys) are independent, while same-account mutations
        # retain one nonce owner and deterministic reservation order.
        self._preparation_locks: dict[tuple[int, int], asyncio.Lock] = {}
        # The API nonce manager is authoritative but may return the same nonce
        # until the venue executes it.  Keep ownership in this adapter so two
        # prepared legs, a cancel, or a second client cannot claim one nonce.
        self._prepared_owner = object()
        self._nonce_reservations: dict[tuple[int, int, int], PreparedMutation | ReservedNonce] = {}
        self._blocked_nonces: dict[tuple[int, int], set[int]] = {}
        self._prepared_registry: dict[int, PreparedMutation] = {}
        self._read_stream_state: Any | None = None
        self._read_stream_task: asyncio.Task[str] | None = None
        self._read_stream_stop: asyncio.Event | None = None
        self._read_stream_started = False
        self._ws_read_counts = {"price_book": 0, "order_observation": 0}
        self._last_read_stream_summary: dict[str, int | bool | str | None] | None = None
        self._cancel_client_by_order: dict[tuple[int, str], int] = {}
        self._warmed_ws_sender: Any | None = None
        self._mutation_transport_chosen = False
        self._closed = False
        self.sdk_version = REQUIRED_LIGHTER_SDK_VERSION
        self._http = (http_factory or PlainAioHttp)(config.api_base_url, timeout_seconds=config.request_timeout_seconds)

    def set_mutation_deadline(self, deadline: float) -> None:
        """Bind the next cancellation attempt to the engine's evidence barrier."""

        try:
            value = float(deadline)
        except (TypeError, ValueError) as exc:
            raise ContractError("mutation deadline must be finite") from exc
        if value != value or value in {float("inf"), float("-inf")} or value <= 0:
            raise ContractError("mutation deadline must be finite and positive")
        self._pending_mutation_deadline = value

    async def start_read_stream(self, *, ready_timeout: float = 5.0) -> bool:
        """Warm one read-only stream; keep REST as every execution proof gate."""
        if self._closed or self._read_stream_started:
            return False
        self._read_stream_started = True
        from .stream_measurement import StreamIdentity
        from .stream_state import ReadStreamSession, ReadStreamState
        from .stream_evidence import StreamEvidenceJournal

        try:
            identity = StreamIdentity.from_config({
                "market_id": self.config.market_id,
                "market_symbol": self.config.market_symbol,
                "source_account_index": self.source_account_index,
                "receiver_account_index": self.receiver_account_index,
                "api_key_index": self.config.api_key_index,
                "environment": self.config.environment,
                "api_base_url": self.config.api_base_url,
                "chain_id": self.config.chain_id,
            })
            if (isinstance(ready_timeout, bool) or not isinstance(ready_timeout, (int, float))
                    or not math.isfinite(ready_timeout) or not 0 < ready_timeout <= 5):
                return False
            state = ReadStreamState(identity)
            if getattr(self.config, "cycle_dir", None) is not None:
                state.evidence = StreamEvidenceJournal(
                    Path(self.config.cycle_dir) / "stream-events.jsonl",
                    market_id=identity.market_id, accounts=identity.accounts,
                )
            stop = asyncio.Event()
            task = asyncio.create_task(ReadStreamSession(state).run(self.secrets, stop))
            self._read_stream_state = state
            self._read_stream_stop = stop
            self._read_stream_task = task
            ready = asyncio.create_task(state.wait_subscription_ready(ready_timeout))
            try:
                done, _ = await asyncio.wait((ready, task), return_when=asyncio.FIRST_COMPLETED)
                if ready in done and ready.result() is True and not task.done():
                    return True
            finally:
                if not ready.done():
                    ready.cancel()
                    await asyncio.gather(ready, return_exceptions=True)
            await self.stop_read_stream()
            return False
        except asyncio.CancelledError:
            await self.stop_read_stream()
            raise
        except Exception:
            await self.stop_read_stream()
            return False

    async def stop_read_stream(self) -> None:
        stop, task = self._read_stream_stop, self._read_stream_task
        self._read_stream_stop = None
        self._read_stream_task = None
        if stop is not None:
            stop.set()
        try:
            if task is not None:
                try:
                    await asyncio.wait_for(task, timeout=5)
                except asyncio.CancelledError:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                    raise
                except Exception:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
        finally:
            if self._read_stream_state is not None:
                self._last_read_stream_summary = self.read_stream_summary()
            self._read_stream_state = None

    async def wait_terminal_hint(self, account: int, client: int,
                                 order_id: str | None, timeout: float) -> bool:
        state = self._read_stream_state
        if state is None or not state.connected:
            return False
        return await state.wait_terminal_hint(account, client, order_id, timeout)

    async def wait_order_observation(self, account: int, market: int, client: int,
                                     order_id: str | None, timeout: float, *, terminal_only: bool):
        state = self._read_stream_state
        if state is None or not state.connected:
            return None
        await state.wait_order_observation(account, market, client, order_id, timeout,
                                           terminal_only=terminal_only)
        # Revalidate connection and cache after waking; never return a detached state.
        return self.observed_order(account, market, client, order_id, terminal_only=terminal_only)

    def read_stream_ready(self) -> bool:
        state, task = self._read_stream_state, self._read_stream_task
        return bool(state is not None and task is not None and not task.done()
                    and state.subscription_ready(time.monotonic()))

    def read_stream_summary(self) -> dict[str, int | bool | str | None] | None:
        state = self._read_stream_state
        if state is None:
            return self._last_read_stream_summary
        observer = state.observer
        return {
            "ws_price_reads": self._ws_read_counts["price_book"],
            "ws_order_reads": self._ws_read_counts["order_observation"],
            "connected": state.connected,
            "ready": self.read_stream_ready(),
            "frames": observer.frames,
            "bytes": observer.bytes_seen,
            "book_gaps": observer.book_gaps,
            "private_subscription_controls": observer.private_subscription_controls,
            "order_events": observer.order_events,
            "order_conflicts": observer.order_conflicts,
            "malformed": observer.malformed,
            "stopped_reason": observer.stopped_reason,
            **({} if state.evidence is None else {
                "evidence_accepted": state.evidence.accepted,
                "evidence_dropped": state.evidence.dropped,
                "evidence_write_failed": state.evidence.failed,
            }),
        }

    def bind_read_stream_order(self, account: int, market: int, client: int,
                               *, run_id: str, phase: str, role: str,
                               attempt_index: int | None) -> None:
        state = self._read_stream_state
        if state is not None:
            state.bind_order(account, market, client, run_id=run_id,
                             phase=phase, role=role, attempt_index=attempt_index)

    def record_read_stream_milestone(self, milestone: str, account: int,
                                     client: int, order_id: str | None = None) -> None:
        state = self._read_stream_state
        if state is not None and state.evidence is not None:
            state.evidence.offer_milestone(
                milestone, account=account, client=client, order_id=order_id,
                at=time.monotonic(), context=state.order_contexts.get((account, client)),
            )

    def bind_read_stream_cancel(self, account: int, market: int,
                                client: int, order_id: str) -> None:
        if (self._read_stream_state is not None and type(account) is int
                and account in {self.source_account_index, self.receiver_account_index}
                and type(market) is int and market == self.config.market_id
                and type(client) is int and client >= 0
                and isinstance(order_id, str) and order_id.isascii()
                and order_id.isdecimal() and len(order_id) <= 20):
            self._cancel_client_by_order[(account, order_id)] = client

    def _record_cancel_send(self, account: int, order_id: str) -> None:
        client = self._cancel_client_by_order.get((account, order_id))
        if client is not None:
            self.record_read_stream_milestone("cancel_send_entered", account, client, order_id)

    @staticmethod
    def verify_sdk() -> None:
        try:
            version = importlib_metadata.version("lighter-sdk")
        except importlib_metadata.PackageNotFoundError as exc:
            raise MissingSdkError("lighter-sdk is not installed; install the optional hood-handoff dependency") from exc
        if version != REQUIRED_LIGHTER_SDK_VERSION:
            raise SdkVersionError(
                f"lighter-sdk {REQUIRED_LIGHTER_SDK_VERSION} is required, found {version}"
            )

    def _lighter(self) -> Any:
        self.verify_sdk()
        try:
            return importlib.import_module("lighter")
        except ImportError as exc:
            raise MissingSdkError("lighter-sdk import failed") from exc

    def _signer(self, account_index: int) -> Any:
        if account_index in self._signers:
            return self._signers[account_index]
        module = self._lighter()
        key_index = self.config.api_key_index
        assert key_index is not None
        private_key = self.secrets.private_key(account_index, key_index)
        if not isinstance(private_key, str) or not private_key:
            raise ContractError("secret provider returned an empty private key")
        factory = self._signer_factory or module.SignerClient
        kwargs: dict[str, Any] = {
            "url": self.config.api_base_url,
            "account_index": account_index,
            "api_private_keys": {key_index: private_key},
            "chain_id": self.config.chain_id,
        }
        nonce_types = getattr(getattr(module, "nonce_manager", None), "NonceManagerType", None)
        if nonce_types is not None:
            kwargs["nonce_management_type"] = nonce_types.API
        signer = factory(**kwargs)
        self._signers[account_index] = signer
        return signer

    def _api(self, account_index: int) -> Any:
        if account_index in self._apis:
            return self._apis[account_index]
        module = self._lighter()
        factory = self._api_factory or module.ApiClient
        api = factory(self._generated_api_client(module))
        self._apis[account_index] = api
        return api

    def _generated_api_client(self, module: Any | None = None) -> Any:
        if self._api_client is not None:
            return self._api_client
        module = module or self._lighter()
        # Generated API clients take ApiClient(Configuration), not a URL.  Keep
        # retries disabled (None) so reads and any accidental generated call
        # cannot silently replay; mutation transport is explicit below.
        configuration = module.Configuration(
            host=self.config.api_base_url,
            retries=None,
        )
        self._api_client = module.ApiClient(configuration)
        return self._api_client

    async def _authorization(self, account_index: int) -> str:
        cached = self._tokens.get(account_index)
        if cached is not None and self._clock() < cached.expires_at:
            return cached.value
        signer = self._signer(account_index)
        key_index = self.config.api_key_index
        assert key_index is not None
        lifetime = self.config.auth_token_lifetime_seconds
        auth_deadline = time.monotonic() + self.config.request_timeout_seconds
        result = await self._bounded(
            _await(
                signer.create_auth_token_with_expiry(
                    deadline=int(lifetime),
                    api_key_index=key_index,
                )
            ),
            auth_deadline,
            "auth token acquisition",
        )
        token: Any = result[0] if isinstance(result, tuple) else result
        if isinstance(result, tuple) and len(result) > 1 and result[1]:
            raise RuntimeError("Lighter auth token request was rejected")
        if not isinstance(token, str) or not token:
            raise RuntimeError("Lighter auth token was not returned")
        self._tokens[account_index] = _CachedToken(
            token,
            self._clock() + max(1.0, float(lifetime) - 1.0),
        )
        return token

    async def market_metadata(self, market_id: int) -> MarketMetadata:
        # Market metadata is explicitly supplied by the operator from current
        # orderBookDetails/account-limits evidence.  The SDK endpoint is still
        # queried once to bind this run to the requested market id.
        module = self._lighter()
        api = module.OrderApi(self._generated_api_client(module))
        details = await self._bounded(
            _await(
                api.order_book_details(
                    market_id=market_id,
                    filter="perp",
                    _request_timeout=self.config.request_timeout_seconds,
                )
            ),
            time.monotonic() + self.config.request_timeout_seconds,
            "orderBookDetails read",
        )
        raw_details = _model_dict(details)
        _require_success_code(raw_details, "orderBookDetails")
        observed = _select_perp_market(raw_details, market_id, expected_symbol=self.config.market_symbol)
        # Operator evidence supplies the durable market contract (minimums,
        # increments, fees and margin provenance), but its timestamp is not a
        # timestamp for this request.  orderBookDetails has no universally
        # usable observation timestamp, so bind this returned snapshot to the
        # completion of the actual read.  An explicitly timestamped fixture is
        # retained so stale/future saved responses still fail the normal
        # freshness checks.
        evidence = dict(self.market_evidence)
        try:
            evidence_market_id = int(evidence.get("market_id", market_id))
        except (TypeError, ValueError) as exc:
            raise ContractError("market evidence has no valid market_id") from exc
        if evidence_market_id != market_id:
            raise ContractError("market evidence market_id does not match config")
        if "market_id" in observed:
            try:
                observed_market_id = int(observed["market_id"])
            except (TypeError, ValueError) as exc:
                raise ContractError("orderBookDetails market_id is invalid") from exc
            if observed_market_id != market_id:
                raise ContractError("orderBookDetails market identity does not match config")
        if "symbol" in observed and "symbol" in evidence and str(observed["symbol"]).upper() != str(evidence["symbol"]).upper():
            raise ContractError("market evidence conflicts with orderBookDetails symbol")
        evidence.setdefault("market_id", market_id)
        evidence.setdefault("symbol", self.config.market_symbol)
        evidence.setdefault("market_type", "perp")
        if self.config.environment == "robinhood":
            evidence.setdefault("venue", "robinhood")
        observed_at = observed.get("observed_at")
        required_live_minimums = ("min_base_amount", "min_quote_amount")
        complete_live_minimums = all(field in observed and observed[field] is not None for field in required_live_minimums)
        if complete_live_minimums:
            if observed_at is None:
                observed_at = self._clock()
            evidence["observed_at"] = observed_at
        elif "observed_at" not in evidence:
            raise ContractError(
                "incomplete orderBookDetails cannot establish a fresh market minimum observation"
            )
        # Do not infer status/fees/precision/minimums from undocumented SDK
        # attributes.  Merge only exact named fields supplied by the operator.
        observed_aliases = {
            "status": "status",
            "price_decimals": "supported_price_decimals",
            "size_decimals": "supported_size_decimals",
            "minimum_base_amount": "min_base_amount",
            "minimum_quote_amount": "min_quote_amount",
            "minimum_initial_margin_fraction": "min_initial_margin_fraction",
            "mark_price": "mark_price",
        }
        for target, source in observed_aliases.items():
            if source in observed:
                if target in evidence and str(evidence[target]) != str(observed[source]):
                    raise ContractError(f"market evidence conflicts with orderBookDetails {source}")
                evidence[target] = observed[source]
        # The matching market fee fields are percentages in the official
        # order-book metadata. Keep any higher explicit operator evidence.
        for target, source in (("source_fee_rate", "maker_fee"),
                               ("receiver_fee_rate", "taker_fee")):
            if source in observed and observed[source] is not None:
                observed_rate = _nonnegative(observed[source], source) / 100
                supplied = _nonnegative(evidence[target], target) if evidence.get(target) is not None else observed_rate
                evidence[target] = format(max(observed_rate, supplied), "f")
        market_config = observed.get("market_config")
        if isinstance(market_config, Mapping):
            evidence["market_margin_mode"] = market_config.get("market_margin_mode")
        return MarketMetadata.from_mapping(evidence)

    async def resolve_market(self, symbol: str) -> MarketMetadata:
        """Resolve the current Robinhood perp ID from the official catalog.

        The resolver is only available for the explicit Robinhood deployment;
        it never queries or falls back to ordinary Lighter mainnet.
        """

        requested = str(symbol).strip().upper()
        if self.config.environment != "robinhood":
            raise ContractError("symbol resolution is only available for Robinhood Chain")
        module = self._lighter()
        api = module.OrderApi(self._generated_api_client(module))
        details = await self._bounded(
            _await(
                api.order_book_details(
                    filter="perp",
                    _request_timeout=self.config.request_timeout_seconds,
                )
            ),
            time.monotonic() + self.config.request_timeout_seconds,
            "Robinhood orderBookDetails catalog read",
        )
        payload = _model_dict(details)
        _require_success_code(payload, "orderBookDetails")
        observed = _select_perp_market_by_symbol(payload, requested)
        evidence = dict(self.market_evidence)
        if "observed_at" not in evidence:
            raise ContractError("market evidence must include its original observed_at timestamp")
        if "market_id" in evidence:
            try:
                evidence_market_id = int(evidence["market_id"])
            except (TypeError, ValueError) as exc:
                raise ContractError("market evidence has no valid market_id") from exc
            if evidence_market_id != int(observed["market_id"]):
                raise ContractError("market evidence market_id conflicts with symbol resolution")
        if "symbol" in evidence and str(evidence["symbol"]).upper() != requested:
            raise ContractError("market evidence symbol conflicts with symbol resolution")
        evidence.setdefault("market_id", int(observed["market_id"]))
        evidence.setdefault("symbol", requested)
        evidence.setdefault("market_type", "perp")
        evidence.setdefault("venue", "robinhood")
        aliases = {
            "status": "status",
            "price_decimals": "supported_price_decimals",
            "size_decimals": "supported_size_decimals",
            "minimum_base_amount": "min_base_amount",
            "minimum_quote_amount": "min_quote_amount",
            "mark_price": "mark_price",
        }
        for target, source in aliases.items():
            if source in observed:
                if target in evidence and str(evidence[target]) != str(observed[source]):
                    raise ContractError(f"market evidence conflicts with orderBookDetails {source}")
                evidence[target] = observed[source]
        for target, source in (("source_fee_rate", "maker_fee"),
                               ("receiver_fee_rate", "taker_fee")):
            if source in observed and observed[source] is not None:
                observed_rate = _nonnegative(observed[source], source) / 100
                supplied = _nonnegative(evidence[target], target) if evidence.get(target) is not None else observed_rate
                evidence[target] = format(max(observed_rate, supplied), "f")
        if "min_initial_margin_fraction" in observed:
            evidence["minimum_initial_margin_fraction"] = observed["min_initial_margin_fraction"]
        market_config = observed.get("market_config")
        if isinstance(market_config, Mapping):
            evidence["market_margin_mode"] = market_config.get("market_margin_mode")
        return MarketMetadata.from_mapping(evidence)

    async def resolve_perpetual_market(self, symbol: str) -> MarketMetadata:
        """Compatibility alias for callers that name the perp explicitly."""

        return await self.resolve_market(symbol)

    async def price_book(self, market_id: int) -> Any:
        """L2 for price calculation only; order_book retains exact owner proof."""
        state = self._read_stream_state
        if state is not None and state.connected and market_id == self.config.market_id:
            book = state.reads.book(time.monotonic(), state.limits.max_fresh_age_seconds)
            if book is not None:
                self._ws_read_counts["price_book"] += 1
                return book
        return await self.order_book(market_id)

    def observed_order(self, account: int, market: int, client: int,
                       order_id: str | None = None, *, terminal_only: bool = False) -> OrderSnapshot | None:
        state = self._read_stream_state
        if state is None or not state.connected:
            return None
        result = state.reads.order(account, market, client, order_id, time.monotonic(),
                                  state.limits.max_fresh_age_seconds, terminal_only=terminal_only)
        if result is not None:
            self._ws_read_counts["order_observation"] += 1
            self.record_read_stream_milestone("exact_ws_observed", account, client, result.order_id)
        return result

    async def order_book(self, market_id: int) -> Any:
        """Read one official public orderBookOrders snapshot.

        The Robinhood response has no reliable venue timestamp, so freshness
        is bound to this request observation time.  ``transaction_time`` from
        individual orders is intentionally not used as a book timestamp.
        """

        if self.config.environment != "robinhood":
            raise ContractError("Robinhood order-book reads require the Robinhood deployment")
        module = self._lighter()
        api = module.OrderApi(self._generated_api_client(module))
        payload_raw = await self._bounded(
            _await(
                api.order_book_orders(
                    market_id=market_id,
                    limit=ROBINHOOD_ORDER_BOOK_LIMIT,
                    _request_timeout=self.config.request_timeout_seconds,
                )
            ),
            time.monotonic() + self.config.request_timeout_seconds,
            "Robinhood orderBookOrders read",
        )
        payload = _model_dict(payload_raw)
        _require_success_code(payload, "orderBookOrders")
        from .series import OrderBookSnapshot

        return OrderBookSnapshot.from_mapping(
            payload,
            market_id=market_id,
            symbol=self.config.market_symbol,
            observed_at=self._clock(),
            venue="robinhood",
        )

    async def order_book_snapshot(self, market_id: int) -> Any:
        return await self.order_book(market_id)

    async def account_snapshot(self, account_index: int, market_id: int) -> AccountSnapshot:
        module = self._lighter()
        api = module.AccountApi(self._generated_api_client(module))
        async def read_account() -> tuple[Any, float]:
            raw = await self._bounded(
                _await(api.account(by="index", value=str(account_index), active_only=False,
                                   _request_timeout=self.config.request_timeout_seconds)),
                time.monotonic() + self.config.request_timeout_seconds,
                "account read",
            )
            # Capture this response now, never after waiting for active orders.
            return raw, self._clock()

        tasks = [asyncio.create_task(read_account()),
                 asyncio.create_task(self._active_orders(account_index, market_id))]
        try:
            (raw_account, account_observed_at), active_orders = await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        raw_account_mapping = _model_dict(raw_account)
        _require_success_code(raw_account_mapping, "account")
        account = _first_mapping(raw_account, "accounts")
        if not {"index", "l1_address", "status", "positions", "available_balance"}.issubset(account):
            raise ContractError("Lighter account response is missing required identity/state fields")
        account_identity = account["l1_address"]
        if not isinstance(account_identity, str) or not account_identity.strip():
            raise ContractError("Lighter account response has no exact account identity")
        returned_index = account["index"]
        try:
            if int(returned_index) != account_index:
                raise ContractError("Lighter account response identity does not match requested account")
        except (TypeError, ValueError) as exc:
            raise ContractError("Lighter account response has no exact account identity") from exc
        positions = account["positions"]
        if not isinstance(positions, (list, tuple)):
            raise ContractError("Lighter account positions field is malformed")
        position: Mapping[str, Any] | None = None
        position_rows: list[Mapping[str, Any]] = []
        for candidate in positions:
            candidate_map = _model_dict(candidate)
            position_rows.append(candidate_map)
            if "market_id" not in candidate_map:
                raise ContractError("Lighter account position lacks market identity")
            try:
                candidate_market_id = int(candidate_map["market_id"])
            except (TypeError, ValueError) as exc:
                raise ContractError("Lighter account position has invalid market identity") from exc
            if "market_index" in candidate_map:
                try:
                    candidate_market_index = int(candidate_map["market_index"])
                except (TypeError, ValueError) as exc:
                    raise ContractError("Lighter account position has invalid market identity") from exc
                if candidate_market_index != candidate_market_id:
                    raise ContractError("Lighter account position has conflicting market identity")
            if candidate_market_id == market_id:
                if "position" not in candidate_map or "sign" not in candidate_map:
                    raise ContractError("Lighter account position lacks required sign/position fields")
                if position is not None:
                    raise ContractError("Lighter account response has duplicate selected-market positions")
                position = candidate_map
        selected_position = position
        position = position or {"position": "0", "sign": 1}
        available = account.get("available_balance")
        margin_required = account.get("cross_initial_margin_requirement")
        fee_rate_key = "source_fee_rate" if account_index == self.source_account_index else "receiver_fee_rate"
        incremental_key = (
            "source_incremental_margin_required"
            if account_index == self.source_account_index
            else "receiver_incremental_margin_required"
        )
        incremental_evidence_key = (
            "source_incremental_margin_evidence"
            if account_index == self.source_account_index
            else "receiver_incremental_margin_evidence"
        )
        status = account["status"]
        ready = status in (0, 1, "active", "online")
        if not ready:
            raise ContractError("Lighter account status is not an approved active value")
        fee_rate = self.market_evidence.get(fee_rate_key)
        incremental_margin = self.market_evidence.get(incremental_key)
        incremental_evidence = self.market_evidence.get(incremental_evidence_key, "")
        if incremental_margin is not None:
            # Validate a supplied estimate before missing provenance can turn
            # it into an apparently absent value under explicit deferral.
            _nonnegative(incremental_margin, incremental_key)
        if incremental_margin is None or not incremental_evidence:
            # Do not treat current cross margin as the requirement of adding Q.
            incremental_margin = None
        margin_evidence = AccountMarginEvidence.from_response(
            account_index=account_index,
            market_id=market_id,
            source_identity=account_identity.strip(),
            observed_at=account_observed_at,
            selected_position=selected_position,
            account=account,
            position_rows=position_rows,
            source="lighter-sdk.account response",
            sdk_version=REQUIRED_LIGHTER_SDK_VERSION,
        )
        return AccountSnapshot.from_mapping(
            {
                "account_index": account_index,
                "market_id": market_id,
                "position": position.get("position", "0"),
                "sign": position.get("sign", 1),
                "active_orders": active_orders,
                "observed_at": account_observed_at,
                "authorized": True,
                "ready": ready,
                "margin_available": available,
                "available_balance": available,
                "margin_required": margin_required,
                "fee_rate": fee_rate,
                "source_identity": account_identity.strip(),
                "incremental_margin_required": incremental_margin,
                "incremental_margin_evidence": incremental_evidence,
                "margin_evidence": margin_evidence,
            }
        )

    async def _active_orders(self, account_index: int, market_id: int) -> tuple[OrderSnapshot, ...]:
        module = self._lighter()
        api = module.OrderApi(self._generated_api_client(module))
        token = await self._authorization(account_index)
        raw = await self._bounded(
            _await(
                api.account_active_orders(
                    authorization=token,
                    account_index=account_index,
                    market_id=market_id,
                    _request_timeout=self.config.request_timeout_seconds,
                )
            ),
            time.monotonic() + self.config.request_timeout_seconds,
            "active orders read",
        )
        raw_mapping = _model_dict(raw)
        _require_success_code(raw_mapping, "accountActiveOrders")
        if "orders" not in raw_mapping or not isinstance(raw_mapping["orders"], (list, tuple)):
            raise ContractError("accountActiveOrders response lacks an orders list")
        values = raw_mapping["orders"]
        result: list[OrderSnapshot] = []
        for item in values:
            parsed = OrderSnapshot.from_mapping(
                _order_snapshot_mapping(item, observed_at=self._clock())
            )
            if parsed.account_index != account_index or parsed.market_id != market_id:
                raise ContractError("active order response identity does not match requested account/market")
            result.append(parsed)
        return tuple(result)

    async def lookup_order(
        self,
        account_index: int,
        market_id: int,
        *,
        order_id: str | None = None,
        client_order_index: int | None = None,
    ) -> OrderSnapshot | None:
        if order_id is None and client_order_index is None:
            raise ContractError("lookup_order requires order_id or client_order_index")
        cached = self.observed_order(account_index, market_id, client_order_index, order_id, terminal_only=True)
        if cached is not None:
            return cached
        token = await self._authorization(account_index)
        params: dict[str, Any] = {"account_index": account_index}
        if client_order_index is None:
            raise ContractError("accountOrders lookup requires the exact client order index")
        params["client_order_indexes"] = str(client_order_index)
        payload = await self._bounded(
            self._http.get("api/v1/accountOrders", params=params, authorization=token),
            time.monotonic() + self.config.request_timeout_seconds,
            "accountOrders read",
        )
        _require_success_code(payload, "accountOrders")
        if "orders" not in payload or not isinstance(payload["orders"], (list, tuple)):
            raise ContractError("accountOrders response lacks an orders list")
        values = payload["orders"]
        for item in values:
            parsed = OrderSnapshot.from_mapping(
                _order_snapshot_mapping(item, observed_at=self._clock())
            )
            if parsed.account_index != account_index or parsed.market_id != market_id:
                raise ContractError("accountOrders response identity does not match requested account/market")
            if order_id is not None and parsed.order_id != str(order_id):
                continue
            if client_order_index is not None and str(parsed.client_order_index) != str(client_order_index):
                continue
            self.record_read_stream_milestone("exact_rest_observed", account_index,
                                              client_order_index, parsed.order_id)
            return parsed
        if payload.get("next_cursor"):
            raise ContractError("accountOrders history is paginated beyond the requested page")
        return None

    async def list_trades(
        self,
        account_index: int,
        market_id: int,
        *,
        order_id: str | None = None,
        cursor: str | None = None,
        limit: int = 100,
    ) -> HistoryPage:
        module = self._lighter()
        api = module.OrderApi(self._generated_api_client(module))
        token = await self._authorization(account_index)
        raw = await self._bounded(
            _await(
                api.trades(
                    sort_by="block_height",
                    limit=limit,
                    authorization=token,
                    market_id=market_id,
                    account_index=account_index,
                    order_index=None if order_id is None else int(order_id),
                    sort_dir="desc",
                    cursor=cursor,
                    market_type="perp",
                    type="all",
                    aggregate=False,
                    _request_timeout=self.config.request_timeout_seconds,
                )
            ),
            time.monotonic() + self.config.request_timeout_seconds,
            "trades read",
        )
        payload = _model_dict(raw)
        _require_success_code(payload, "trades")
        values = payload.get("trades")
        if not isinstance(values, (list, tuple)):
            raise ContractError("trades response lacks a trades list")
        trades: list[TradeReceipt] = []
        for item in values:
            mapped = _model_dict(item)
            required = (
                "trade_id",
                "trade_id_str",
                "market_id",
                "size",
                "price",
                "ask_id",
                "bid_id",
                "ask_client_id",
                "ask_client_id_str",
                "bid_client_id",
                "bid_client_id_str",
                "ask_account_id",
                "bid_account_id",
                "timestamp",
            )
            if any(key not in mapped for key in required):
                raise ContractError("trade receipt lacks required official identity/time fields")
            observed_at = _trade_timestamp_seconds(mapped["timestamp"])
            if (
                str(mapped["trade_id"]) != str(mapped["trade_id_str"])
                or str(mapped["ask_client_id"]) != str(mapped["ask_client_id_str"])
                or str(mapped["bid_client_id"]) != str(mapped["bid_client_id_str"])
            ):
                raise ContractError("trade receipt has conflicting numeric/string identities")
            try:
                receipt_market = int(mapped["market_id"])
                ask_id = str(mapped["ask_id"])
                bid_id = str(mapped["bid_id"])
                ask_account = int(mapped["ask_account_id"])
                bid_account = int(mapped["bid_account_id"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ContractError("trade receipt lacks exact ask/bid account identities") from exc
            if receipt_market != market_id:
                raise ContractError("trade receipt market identity does not match requested market")
            if account_index not in {ask_account, bid_account}:
                raise ContractError("trade receipt does not belong to requested account")
            is_ask = account_index == ask_account
            own_order_id = ask_id if is_ask else bid_id
            if order_id is not None and own_order_id != str(order_id):
                continue
            mapped["order_id"] = own_order_id
            mapped["side"] = "SELL" if is_ask else "BUY"
            mapped["quantity"] = mapped.get("size")
            mapped["counterparty_account_index"] = bid_account if is_ask else ask_account
            mapped["counterparty_order_id"] = bid_id if is_ask else ask_id
            mapped["counterparty_client_order_index"] = (
                mapped["bid_client_id_str"] if is_ask else mapped["ask_client_id_str"]
            )
            mapped["client_order_index"] = (
                mapped["ask_client_id_str"] if is_ask else mapped["bid_client_id_str"]
            )
            mapped["trade_id"] = mapped.get("trade_id_str", mapped.get("trade_id"))
            mapped["observed_at"] = observed_at
            mapped.update(_trade_fee_evidence(mapped, is_ask=is_ask, distinct_accounts=ask_account != bid_account))
            trades.append(
                TradeReceipt.from_mapping(
                    {
                        **mapped,
                        "account_index": account_index,
                        "market_id": market_id,
                    }
                )
            )
        return HistoryPage(
            trades=tuple(trades),
            next_cursor=payload.get("next_cursor"),
            complete=not bool(payload.get("next_cursor")),
        )

    async def aclose(self) -> None:
        """Close all adapter-owned SDK/HTTP resources exactly once.

        The generated public client owns one HTTP session and each signer owns
        its own client/session.  A signer's ``close`` method owns its nested
        client, so that nested object is only closed directly when the signer
        does not expose a close method.  Cleanup errors are deliberately
        contained so a terminal handoff result is never replaced by teardown
        noise.
        """

        if self._closed:
            return
        self._closed = True
        await self.stop_read_stream()
        if self._warmed_ws_sender is not None:
            await self._warmed_ws_sender.close()
            self._warmed_ws_sender = None
        seen: set[int] = set()
        self._tokens.clear()

        async def close_once(resource: Any) -> bool:
            if resource is None or id(resource) in seen:
                return True
            seen.add(id(resource))
            return await _close_resource(resource)

        await close_once(self._api_client)
        for signer in tuple(self._signers.values()):
            closed = await close_once(signer)
            if not closed:
                # A failing signer close can leave its private generated client
                # open.  Give that nested session one bounded fallback attempt.
                await close_once(getattr(signer, "api_client", None))
        await close_once(self._http)
        self._signers.clear()
        self._apis.clear()
        self._api_client = None

    async def close(self) -> None:
        await self.aclose()

    async def _bounded(self, awaitable: Any, deadline: float, label: str) -> Any:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"{label} exceeded configured request/freshness deadline")
        try:
            return await asyncio.wait_for(awaitable, timeout=remaining)
        except asyncio.TimeoutError as exc:
            raise TimeoutError(f"{label} exceeded configured request/freshness deadline") from exc

    async def _next_nonce(self, signer: Any, api_key_index: int, *, deadline: float | None = None) -> int:
        manager = getattr(signer, "nonce_manager", None)
        method = getattr(manager, "async_next_nonce", None)
        if method is None:
            raise ContractError("lighter-sdk nonce manager is unavailable; raw signing cannot use nonce=-1")
        if deadline is None:
            deadline = time.monotonic() + min(
                self.config.request_timeout_seconds,
                self.config.freshness_seconds,
            )
        result = await self._bounded(method(api_key_index), deadline, "nonce acquisition")
        if not isinstance(result, tuple) or len(result) != 2:
            raise ContractError("lighter-sdk nonce manager returned an unsupported shape")
        returned_key, nonce = result
        if (
            isinstance(returned_key, bool)
            or not isinstance(returned_key, int)
            or returned_key != api_key_index
            or isinstance(nonce, bool)
            or not isinstance(nonce, int)
            or nonce < 0
        ):
            raise ContractError("lighter-sdk nonce manager returned an invalid account/key nonce")
        return nonce

    @staticmethod
    def _nonce_key(account_index: int, api_key_index: int, nonce: int) -> tuple[int, int, int]:
        return account_index, api_key_index, nonce

    @staticmethod
    def _account_key(account_index: int, api_key_index: int) -> tuple[int, int]:
        return account_index, api_key_index

    def _preparation_lock_for(self, account_index: int, api_key_index: int) -> asyncio.Lock:
        """Return the serialization lock for one account/key nonce domain."""

        # Lock creation is synchronous and runs on the event loop.  Once the
        # lock is returned, every await that can reserve, consume, or release
        # this account/key nonce is protected by the same object.
        key = self._account_key(account_index, api_key_index)
        lock = self._preparation_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._preparation_locks[key] = lock
        return lock

    def _assert_nonce_available(self, account_index: int, api_key_index: int, nonce: int) -> None:
        reservation_key = self._nonce_key(account_index, api_key_index, nonce)
        if reservation_key in self._nonce_reservations:
            raise ContractError("account/key nonce is already reserved by an active mutation")
        if nonce in self._blocked_nonces.get(self._account_key(account_index, api_key_index), set()):
            raise ContractError("account/key nonce was already sent and cannot be reused")

    def _register_prepared(self, prepared: PreparedMutation) -> None:
        reservation_key = self._nonce_key(
            prepared.account_index,
            prepared.api_key_index,
            prepared._nonce,
        )
        self._assert_nonce_available(
            prepared.account_index,
            prepared.api_key_index,
            prepared._nonce,
        )
        self._nonce_reservations[reservation_key] = prepared
        self._prepared_registry[id(prepared)] = prepared

    def _is_owned_prepared(self, prepared: Any) -> bool:
        return (
            isinstance(prepared, PreparedMutation)
            and prepared._owner is self._prepared_owner
            and self._prepared_registry.get(id(prepared)) is prepared
        )

    def _release_prepared_reservation(self, prepared: PreparedMutation) -> None:
        reservation_key = self._nonce_key(
            prepared.account_index,
            prepared.api_key_index,
            prepared._nonce,
        )
        if self._nonce_reservations.get(reservation_key) is prepared:
            self._nonce_reservations.pop(reservation_key, None)

    def _block_nonce(self, account_index: int, api_key_index: int, nonce: int) -> None:
        self._nonce_reservations.pop(self._nonce_key(account_index, api_key_index, nonce), None)
        self._blocked_nonces.setdefault(self._account_key(account_index, api_key_index), set()).add(nonce)

    def _consume_prepared_for_send(self, prepared: PreparedMutation) -> bool:
        if not prepared.consume():
            return False
        self._block_nonce(prepared.account_index, prepared.api_key_index, prepared._nonce)
        return True

    def _invalidate_owned(self, prepared: PreparedMutation) -> None:
        if not self._is_owned_prepared(prepared):
            return
        was_ready = prepared._state == "READY"
        prepared.invalidate()
        if was_ready:
            self._release_prepared_reservation(prepared)

    async def enable_warmed_ws_sender(self, sender: Any | None = None) -> None:
        """Select WS for this client before preparing or sending any mutation.

        The ordinary terminal and Telegram paths never call this method.
        """
        if (self._closed or self._mutation_transport_chosen or self._prepared_registry
                or self._nonce_reservations or self._warmed_ws_sender is not None):
            raise ContractError("transaction transport choice is already bound")
        if sender is None:
            from .ws_sender import WarmTxSender
            sender = WarmTxSender()
        await sender.start()
        self._warmed_ws_sender = sender

    async def _send_signed_tx(self, tx_type: Any, tx_info: Any,
                              *, tx_hash: str | None = None,
                              deadline: float | None = None) -> Mapping[str, Any]:
        if isinstance(tx_type, bool) or not isinstance(tx_type, int) or not isinstance(tx_info, str) or not tx_info:
            raise ContractError("lighter-sdk signer returned malformed transaction data")
        self._mutation_transport_chosen = True
        if self._warmed_ws_sender is not None:
            if tx_hash is None or deadline is None:
                raise ContractError("WS sender requires prepared transaction identity and deadline")
            return await self._warmed_ws_sender.send(tx_type, tx_info, tx_hash,
                                                     deadline=deadline)
        # PlainAioHttp performs exactly one POST to the documented sendTx form
        # endpoint.  The signed tx body is never journaled or included in errors.
        return await self._http.post_form(
            "api/v1/sendTx",
            form={"tx_type": tx_type, "tx_info": tx_info},
        )

    async def reserve_order_nonce(self, account_index: int, *, deadline: float) -> ReservedNonce:
        """Acquire and reserve before price selection, without signing or sending."""

        key = self.config.api_key_index
        if key is None or isinstance(account_index, bool) or not isinstance(account_index, int) or account_index not in {self.source_account_index, self.receiver_account_index}:
            raise ContractError("nonce reservation account/key is not configured")
        if isinstance(deadline, bool) or not math.isfinite(deadline) or deadline <= time.monotonic():
            raise ContractError("nonce reservation deadline is invalid or expired")
        started = time.perf_counter()
        async with self._preparation_lock_for(account_index, key):
            acquired = time.perf_counter()
            nonce = await self._next_nonce(self._signer(account_index), key, deadline=deadline)
            finished = time.perf_counter()
            if time.monotonic() >= deadline:
                raise TimeoutError("nonce reservation crossed its deadline")
            self._assert_nonce_available(account_index, key, nonce)
            token = ReservedNonce(account_index, key, deadline, nonce, self._prepared_owner,
                                  diagnostic_timings={
                                      "preparation_lock_wait_seconds": acquired - started,
                                      "nonce_acquisition_seconds": finished - acquired,
                                  })
            self._nonce_reservations[self._nonce_key(account_index, key, nonce)] = token
            return token

    def _owns_nonce(self, token: Any) -> bool:
        return bool(
            isinstance(token, ReservedNonce)
            and token._owner is self._prepared_owner
            and token._state == "RESERVED"
            and self._nonce_reservations.get(self._nonce_key(token.account_index, token.api_key_index, token._nonce)) is token
        )

    async def invalidate_reserved_nonce(self, token: Any) -> None:
        if not isinstance(token, ReservedNonce):
            return
        async with self._preparation_lock_for(token.account_index, token.api_key_index):
            if self._owns_nonce(token):
                self._nonce_reservations.pop(self._nonce_key(token.account_index, token.api_key_index, token._nonce))
                object.__setattr__(token, "_state", "INVALIDATED")

    async def prepare_order(self, plan: OrderPlan, *, reserved_nonce: ReservedNonce | None = None) -> PreparedMutation:
        """Sign one exact plan, optionally using a previously reserved nonce."""

        key_index = self.config.api_key_index
        if key_index is None:
            raise ContractError("api_key_index is required for order preparation")
        deadline = plan.mutation_deadline_monotonic
        if deadline is None:
            deadline = time.monotonic() + min(
                self.config.request_timeout_seconds,
                self.config.freshness_seconds,
            )
        if time.monotonic() >= deadline:
            raise TimeoutError("order preparation crossed the final mutation barrier")
        preparation_started = time.perf_counter()
        async with self._preparation_lock_for(plan.account_index, key_index):
            lock_acquired = time.perf_counter()
            signer = self._signer(plan.account_index)
            nonce_started = time.perf_counter()
            if reserved_nonce is None:
                nonce = await self._next_nonce(signer, key_index, deadline=deadline)
            else:
                if (
                    not self._owns_nonce(reserved_nonce)
                    or reserved_nonce.account_index != plan.account_index
                    or reserved_nonce.api_key_index != key_index
                    or deadline > reserved_nonce.deadline
                    or time.monotonic() >= reserved_nonce.deadline
                ):
                    raise ContractError("reserved nonce identity, state or deadline does not match order")
                nonce = reserved_nonce._nonce
            if time.monotonic() >= deadline:
                raise TimeoutError("nonce acquisition crossed the final mutation barrier")
            nonce_finished = time.perf_counter()
            signer_type = type(signer)
            signing_started = time.perf_counter()
            result = await self._bounded(
                _await(
                    signer.sign_create_order(
                        market_index=plan.market_id,
                        client_order_index=plan.client_order_index,
                        base_amount=plan.quantity_int,
                        price=plan.price_int,
                        is_ask=plan.side == "SELL",
                        order_type=getattr(signer_type, "ORDER_TYPE_LIMIT", 0) if plan.order_type == "LIMIT" else getattr(signer_type, "ORDER_TYPE_MARKET", 1),
                        time_in_force=getattr(signer_type, "ORDER_TIME_IN_FORCE_POST_ONLY", 2) if plan.time_in_force == "POST_ONLY" else getattr(signer_type, "ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL", 0),
                        reduce_only=plan.reduce_only,
                        order_expiry=plan.order_expiry_ms,
                        skip_nonce=getattr(signer_type, "SKIP_NONCE_OFF", 0),
                        nonce=nonce,
                        api_key_index=key_index,
                    )
                ),
                deadline,
                "order signing",
            )
            signing_finished = time.perf_counter()
            if not isinstance(result, tuple) or len(result) != 4:
                raise RuntimeError("lighter-sdk sign_create_order returned an unsupported shape")
            tx_type, tx_info, tx_hash, error = result
            if error:
                raise ContractError("order signing was rejected")
            if (
                isinstance(tx_type, bool)
                or not isinstance(tx_type, int)
                or not isinstance(tx_info, str)
                or not tx_info
            ):
                raise ContractError("lighter-sdk signer returned malformed transaction data")
            if time.monotonic() >= deadline:
                raise TimeoutError("order signing crossed the final mutation barrier")
            prepared = PreparedMutation(
                account_index=plan.account_index,
                api_key_index=key_index,
                plan_binding=_order_plan_binding(plan),
                deadline=deadline,
                _tx_type=tx_type,
                _tx_info=tx_info,
                _tx_hash=_safe_text(tx_hash),
                _owner=self._prepared_owner,
                _nonce=nonce,
                diagnostic_timings={
                    "preparation_lock_wait_seconds": lock_acquired - preparation_started,
                    "nonce_acquisition_seconds": nonce_finished - nonce_started,
                    "signing_call_seconds": signing_finished - signing_started,
                },
            )
            if reserved_nonce is not None:
                # Transfer ownership under the same account/key lock. Signing
                # failure leaves the reservation available only for invalidation.
                self._nonce_reservations.pop(self._nonce_key(plan.account_index, key_index, nonce))
                object.__setattr__(reserved_nonce, "_state", "CONSUMED")
                prepared.diagnostic_timings.update(reserved_nonce.diagnostic_timings)
                prepared.diagnostic_timings["nonce_reserved_before_quote"] = 1.0
            self._register_prepared(prepared)
            return prepared

    async def submit_prepared_order(
        self,
        plan: OrderPlan,
        prepared: PreparedMutation,
        *,
        deadline: float | None = None,
    ) -> MutationReceipt:
        """Send one prepared order exactly once.

        ``deadline`` is an optional stricter final evidence barrier supplied by
        the engine.  The prepared object's own deadline and immutable binding
        remain mandatory; a rejected validation consumes the preparation.
        """

        key_index = self.config.api_key_index
        if key_index is None or not isinstance(prepared, PreparedMutation) or not self._is_owned_prepared(prepared):
            return MutationReceipt(False, None, None, "prepared order binding is invalid")
        if deadline is not None:
            try:
                deadline = float(deadline)
            except (TypeError, ValueError):
                async with self._preparation_lock_for(prepared.account_index, prepared.api_key_index):
                    self._invalidate_owned(prepared)
                return MutationReceipt(False, None, None, "prepared order deadline is invalid")
            if not math.isfinite(deadline) or deadline <= 0:
                async with self._preparation_lock_for(prepared.account_index, prepared.api_key_index):
                    self._invalidate_owned(prepared)
                return MutationReceipt(False, None, None, "prepared order deadline is invalid")
        async with self._preparation_lock_for(prepared.account_index, prepared.api_key_index):
            if not prepared.matches(plan, api_key_index=key_index):
                self._invalidate_owned(prepared)
                return MutationReceipt(False, None, None, "prepared order binding changed or was consumed")
            if deadline is not None and deadline > prepared.deadline:
                self._invalidate_owned(prepared)
                return MutationReceipt(False, None, None, "prepared order final deadline exceeds its bound")
            dispatch_deadline = min(prepared.deadline, deadline) if deadline is not None else prepared.deadline
            if time.monotonic() >= dispatch_deadline:
                self._invalidate_owned(prepared)
                return MutationReceipt(False, None, None, "prepared order crossed the final mutation barrier")
            if not self._consume_prepared_for_send(prepared):
                return MutationReceipt(False, None, None, "prepared order was already consumed or invalidated")
        transport_started = time.perf_counter()
        try:
            response = await self._bounded(
                self._send_signed_tx(prepared._tx_type, prepared._tx_info,
                                     tx_hash=prepared._tx_hash,
                                     deadline=dispatch_deadline),
                dispatch_deadline,
                "order dispatch",
            )
        finally:
            prepared.diagnostic_timings["transport_roundtrip_seconds"] = time.perf_counter() - transport_started
        if isinstance(response, Mapping):
            trace = response.get("_transport_timing")
            if isinstance(trace, Mapping):
                for key in ("http_session_ready_seconds", "http_response_headers_seconds",
                            "http_first_body_byte_seconds", "http_full_body_seconds",
                            "http_parse_seconds", "http_headers_signal_seconds",
                            "http_body_signal_seconds", "http_queue_seconds",
                            "http_connection_setup_seconds", "http_connection_reused",
                            "ws_write_seconds", "ws_ack_wait_seconds", "ws_total_seconds"):
                    value = trace.get(key)
                    if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0:
                        prepared.diagnostic_timings[key] = float(value)
        code = _response_code(response)
        if code is None:
            raise RuntimeError("malformed or undecidable sendTx response")
        return MutationReceipt(
            accepted=code == 200,
            order_id=None,
            tx_hash=prepared._tx_hash or _safe_text(_model_dict(response).get("tx_hash")),
            error=None if code == 200 else f"send_tx response code {code}",
            response_code=code,
        )

    async def invalidate_prepared_order(self, prepared: PreparedMutation) -> None:
        """Invalidate an unused preparation after a source-side barrier."""

        if isinstance(prepared, PreparedMutation):
            async with self._preparation_lock_for(prepared.account_index, prepared.api_key_index):
                self._invalidate_owned(prepared)

    async def submit_order(self, plan: OrderPlan) -> MutationReceipt:
        """Prepare and send one order, retaining the legacy generic surface."""

        try:
            prepared = await self.prepare_order(plan)
        except Exception as exc:
            return MutationReceipt(False, None, None, sanitize_exception(exc))
        return await self.submit_prepared_order(plan, prepared)

    async def update_leverage_fraction(
        self, account_index: int, market_id: int, fraction: int, margin_mode: int = 0,
        *, prepared_intent: Callable[[Mapping[str, Any]], None] | None = None,
        cancelled_before_transport: Callable[[], None] | None = None,
    ) -> MutationReceipt:
        """Send one exact cross-margin setting transaction; never retry a send."""
        key_index = self.config.api_key_index
        if (key_index is None or account_index not in {self.source_account_index, self.receiver_account_index}
                or market_id != self.config.market_id or margin_mode != 0
                or isinstance(fraction, bool) or not isinstance(fraction, int)
                or not 2500 <= fraction <= 10000):
            raise ContractError("leverage setting identity, mode or 1x..4x fraction is invalid")
        deadline = time.monotonic() + min(self.config.request_timeout_seconds, self.config.freshness_seconds)
        try:
            async with self._preparation_lock_for(account_index, key_index):
                signer = self._signer(account_index)
                nonce = await self._next_nonce(signer, key_index, deadline=deadline)
                self._assert_nonce_available(account_index, key_index, nonce)
                signer_type = type(signer)
                result = await self._bounded(
                    _await(signer.sign_update_leverage(
                        market_index=market_id, fraction=fraction, margin_mode=margin_mode,
                        skip_nonce=getattr(signer_type, "SKIP_NONCE_OFF", 0),
                        nonce=nonce, api_key_index=key_index,
                    )), deadline, "leverage signing",
                )
                if not isinstance(result, tuple) or len(result) != 4:
                    raise ContractError("leverage signer returned an unsupported shape")
                tx_type, tx_info, tx_hash, error = result
                if error:
                    return MutationReceipt(False, None, _leverage_tx_hash(tx_hash), "leverage signing rejected")
                if time.monotonic() >= deadline:
                    raise TimeoutError("leverage signing crossed its mutation deadline")
                if prepared_intent is not None:
                    if tx_type != 20 or _leverage_tx_hash(tx_hash) is None:
                        raise ContractError("leverage signer did not provide provable transaction identity")
                    # Durable identity before the only transport attempt. Never
                    # expose signed info or the credential through this callback.
                    prepared_intent({
                        "account_index": account_index, "market_id": market_id,
                        "api_key_index": key_index, "fraction_bps": fraction,
                        "margin_mode": margin_mode, "nonce": nonce,
                        "tx_hash": tx_hash, "tx_type": tx_type,
                    })
            # A synchronous journal callback can request cancellation without
            # an await at which asyncio would deliver it before sendTx.
            task = asyncio.current_task()
            if task is not None and task.cancelling():
                raise asyncio.CancelledError()
            if time.monotonic() >= deadline:
                raise TimeoutError("leverage preparation crossed its mutation deadline")
        except asyncio.CancelledError:
            # Cancellation in this block has not entered sendTx. Persist that
            # fact synchronously before propagating the caller's cancellation.
            if cancelled_before_transport is not None:
                cancelled_before_transport()
            raise
        except Exception as exc:
            raise LeverageNotSent("leverage setting failed before transport") from exc
        # No await separates lock release, nonce block and the one send entry.
        # A cancellation while releasing the preparation lock is still a
        # proved no-send and leaves this nonce reusable by the same adapter.
        self._block_nonce(account_index, key_index, nonce)
        response = await self._bounded(self._send_signed_tx(tx_type, tx_info,
                                                            tx_hash=_safe_text(tx_hash),
                                                            deadline=deadline), deadline,
                                       "leverage dispatch")
        code = _response_code(response)
        if code is None or code >= 500:
            raise RuntimeError("malformed or undecidable leverage sendTx response")
        return MutationReceipt(
            accepted=code == 200, order_id=None,
            tx_hash=_leverage_tx_hash(tx_hash) or _leverage_tx_hash(_model_dict(response).get("tx_hash")),
            error=None if code == 200 else f"send_tx response code {code}", response_code=code,
        )

    async def read_leverage_transaction(self, tx_hash: str) -> dict[str, Any]:
        """Read only the public transaction fields needed for later reconciliation."""

        if _leverage_tx_hash(tx_hash) is None:
            raise ContractError("leverage transaction hash is invalid")
        module = self._lighter()
        api = module.TransactionApi(self._generated_api_client(module))
        response = await self._bounded(
            _await(api.tx(by="hash", value=tx_hash,
                          _request_timeout=self.config.request_timeout_seconds)),
            time.monotonic() + self.config.request_timeout_seconds,
            "leverage transaction read",
        )
        raw = _model_dict(response)
        _require_success_code(raw, "transaction")
        return _leverage_tx_diagnostic(raw, tx_hash, self._clock())

    async def read_leverage_next_nonce(self, account_index: int, api_key_index: int) -> int:
        if (account_index not in {self.source_account_index, self.receiver_account_index}
                or api_key_index != self.config.api_key_index):
            raise ContractError("nextNonce identity is invalid")
        module = self._lighter()
        api = module.TransactionApi(self._generated_api_client(module))
        response = await self._bounded(
            _await(api.next_nonce(account_index=account_index, api_key_index=api_key_index,
                                  _request_timeout=self.config.request_timeout_seconds)),
            time.monotonic() + self.config.request_timeout_seconds,
            "nextNonce read",
        )
        return _leverage_next_nonce(_model_dict(response))

    async def cancel_order(self, account_index: int, market_id: int, order_id: str) -> MutationReceipt:
        deadline = self._pending_mutation_deadline
        self._pending_mutation_deadline = None
        timings: dict[str, float] = {}
        key_index = self.config.api_key_index
        assert key_index is not None
        # As with submit_order, keep known pre-send failures as a rejected
        # local receipt while allowing every post-send uncertainty to reach the
        # caller.  The caller then preserves unresolved execution and performs
        # only its bounded read-only reconciliation/cleanup.
        try:
            if deadline is None:
                deadline = time.monotonic() + min(
                    self.config.request_timeout_seconds,
                    self.config.freshness_seconds,
                )
            async with self._preparation_lock_for(account_index, key_index):
                signer = self._signer(account_index)
                nonce_started = time.perf_counter()
                nonce = await self._next_nonce(signer, key_index, deadline=deadline)
                timings["cancel_nonce_seconds"] = time.perf_counter() - nonce_started
                self._assert_nonce_available(account_index, key_index, nonce)
                if time.monotonic() >= deadline:
                    raise TimeoutError("nonce acquisition crossed the final mutation barrier")
                signer_type = type(signer)
                signing_started = time.perf_counter()
                result = await self._bounded(
                    _await(
                        signer.sign_cancel_order(
                            market_index=market_id,
                            order_index=int(order_id),
                            skip_nonce=getattr(signer_type, "SKIP_NONCE_OFF", 0),
                            nonce=nonce,
                            api_key_index=key_index,
                        )
                    ),
                    deadline,
                    "cancel signing",
                )
                timings["cancel_signing_seconds"] = time.perf_counter() - signing_started
                if not isinstance(result, tuple) or len(result) != 4:
                    raise RuntimeError("lighter-sdk sign_cancel_order returned an unsupported shape")
                tx_type, tx_info, tx_hash, error = result
                if error:
                    return MutationReceipt(False, order_id, _safe_text(tx_hash), sanitize_exception(ValueError(str(error))), diagnostic_timings=timings)
                if (
                    isinstance(tx_type, bool)
                    or not isinstance(tx_type, int)
                    or not isinstance(tx_info, str)
                    or not tx_info
                ):
                    raise ContractError("lighter-sdk signer returned malformed transaction data")
                if time.monotonic() >= deadline:
                    raise TimeoutError("cancel signing crossed the final mutation barrier")
                # Cancellation has no PreparedMutation object, so reserve its
                # nonce directly before releasing the serialized signing lock.
                self._block_nonce(account_index, key_index, nonce)
        except Exception as exc:
            return MutationReceipt(False, order_id, None, sanitize_exception(exc), diagnostic_timings=timings)
        transport_started = time.perf_counter()
        self._record_cancel_send(account_index, order_id)
        response = await self._bounded(
            self._send_signed_tx(tx_type, tx_info, tx_hash=_safe_text(tx_hash),
                                 deadline=deadline),
            deadline,
            "cancel dispatch",
        )
        timings["cancel_transport_roundtrip_seconds"] = time.perf_counter() - transport_started
        if isinstance(response, Mapping) and isinstance(response.get("_transport_timing"), Mapping):
            for name, value in response["_transport_timing"].items():
                if (name in {"http_session_ready_seconds", "http_response_headers_seconds",
                             "http_first_body_byte_seconds", "http_full_body_seconds",
                             "http_parse_seconds", "http_headers_signal_seconds",
                             "http_body_signal_seconds", "http_queue_seconds",
                             "http_connection_setup_seconds", "http_connection_reused"}
                        and isinstance(value, (int, float)) and not isinstance(value, bool)
                        and math.isfinite(value) and value >= 0):
                    timings[name] = float(value)
        code = _response_code(response)
        if code is None:
            raise RuntimeError("malformed or undecidable sendTx response")
        return MutationReceipt(
            accepted=code == 200,
            order_id=order_id,
            tx_hash=_safe_text(tx_hash) or _safe_text(_model_dict(response).get("tx_hash")),
            error=None if code == 200 else f"send_tx response code {code}",
            response_code=code,
            diagnostic_timings=timings,
        )

    async def reserve_cancel_nonce(self, account_index: int) -> ReservedNonce:
        """Read/reserve after accepted source ACK, before its exact ID is visible."""
        return await self.reserve_order_nonce(account_index, deadline=time.monotonic() + min(
            self.config.request_timeout_seconds, self.config.freshness_seconds))

    async def prepare_cancel_order(self, account_index: int, market_id: int,
                                   order_id: str, *, reserved_nonce: ReservedNonce | None = None) -> PreparedMutation:
        """Sign one identified cancel before the final exact-order decision.

        This reserves the source account/key nonce in the same registry as
        prepared orders.  It does not authorize dispatch or infer an order fill.
        """
        key = self.config.api_key_index
        if (key is None or type(account_index) is not int
                or account_index not in {self.source_account_index, self.receiver_account_index}
                or type(market_id) is not int or market_id != self.config.market_id
                or not isinstance(order_id, str) or not order_id.isascii()
                or not order_id.isdecimal() or len(order_id) > 20):
            raise ContractError("prepared cancel identity is invalid")
        deadline = time.monotonic() + min(self.config.request_timeout_seconds,
                                          self.config.freshness_seconds)
        started = time.perf_counter()
        async with self._preparation_lock_for(account_index, key):
            signer = self._signer(account_index)
            nonce_started = time.perf_counter()
            if reserved_nonce is None:
                nonce = await self._next_nonce(signer, key, deadline=deadline)
                self._assert_nonce_available(account_index, key, nonce)
            else:
                if (not self._owns_nonce(reserved_nonce) or reserved_nonce.account_index != account_index
                        or reserved_nonce.api_key_index != key or time.monotonic() >= reserved_nonce.deadline):
                    raise ContractError("cancel nonce reservation is invalid or expired")
                deadline = min(deadline, reserved_nonce.deadline)
                nonce = reserved_nonce._nonce
            nonce_finished = time.perf_counter()
            signer_type = type(signer)
            result = await self._bounded(
                _await(signer.sign_cancel_order(
                    market_index=market_id, order_index=int(order_id),
                    skip_nonce=getattr(signer_type, "SKIP_NONCE_OFF", 0),
                    nonce=nonce, api_key_index=key,
                )), deadline, "prepared cancel signing",
            )
            signed_at = time.perf_counter()
            if not isinstance(result, tuple) or len(result) != 4:
                raise ContractError("cancel signer returned an unsupported shape")
            tx_type, tx_info, tx_hash, error = result
            if (error or type(tx_type) is not int or not isinstance(tx_info, str)
                    or not tx_info or time.monotonic() >= deadline):
                raise ContractError("prepared cancel signing was rejected or expired")
            prepared = PreparedMutation(
                account_index=account_index, api_key_index=key,
                plan_binding=("CANCEL", account_index, market_id, order_id),
                deadline=deadline, _tx_type=tx_type, _tx_info=tx_info,
                _tx_hash=_safe_text(tx_hash), _owner=self._prepared_owner,
                _nonce=nonce,
                diagnostic_timings={
                    "cancel_preparation_seconds": signed_at - started,
                    "cancel_nonce_seconds": nonce_finished - nonce_started,
                    "cancel_signing_seconds": signed_at - nonce_finished,
                },
            )
            if reserved_nonce is not None:
                self._nonce_reservations.pop(self._nonce_key(account_index, key, nonce))
                object.__setattr__(reserved_nonce, "_state", "CONSUMED")
                prepared.diagnostic_timings["cancel_nonce_reserved_before_visibility"] = 1.0
                prepared.diagnostic_timings["cancel_nonce_reservation_seconds"] = reserved_nonce.diagnostic_timings["nonce_acquisition_seconds"]
            self._register_prepared(prepared)
            return prepared

    async def submit_prepared_cancel_order(self, account_index: int, market_id: int,
                                           order_id: str, prepared: PreparedMutation,
                                           *, deadline: float) -> MutationReceipt:
        """Consume a prepared cancel once, after the engine's fresh exact proof."""
        self._pending_mutation_deadline = None
        key = self.config.api_key_index
        if key is None or not isinstance(prepared, PreparedMutation):
            return MutationReceipt(False, order_id, None, "prepared cancel not sent",
                                   diagnostic_timings={"cancel_not_sent": 1.0})
        async with self._preparation_lock_for(account_index, key):
            if (not self._is_owned_prepared(prepared) or prepared._state != "READY"
                    or prepared.plan_binding != ("CANCEL", account_index, market_id, order_id)
                    or prepared.api_key_index != key or type(deadline) not in {int, float}
                    or not math.isfinite(deadline) or time.monotonic() >= min(deadline, prepared.deadline)):
                self._invalidate_owned(prepared)
                return MutationReceipt(False, order_id, None, "prepared cancel not sent",
                                       diagnostic_timings={"cancel_not_sent": 1.0})
            self._consume_prepared_for_send(prepared)
        timings = dict(prepared.diagnostic_timings)
        timings["cancel_prepared_before_dispatch"] = 1.0
        started = time.perf_counter()
        self._record_cancel_send(account_index, order_id)
        response = await self._bounded(
            self._send_signed_tx(prepared._tx_type, prepared._tx_info,
                                 tx_hash=prepared._tx_hash,
                                 deadline=min(deadline, prepared.deadline)),
            min(deadline, prepared.deadline), "prepared cancel dispatch",
        )
        timings["cancel_transport_roundtrip_seconds"] = time.perf_counter() - started
        if isinstance(response, Mapping) and isinstance(response.get("_transport_timing"), Mapping):
            for name, value in response["_transport_timing"].items():
                if (name in {"ws_write_seconds", "ws_ack_wait_seconds", "ws_total_seconds",
                             "http_session_ready_seconds", "http_response_headers_seconds",
                             "http_first_body_byte_seconds", "http_full_body_seconds",
                             "http_parse_seconds", "http_headers_signal_seconds", "http_body_signal_seconds",
                             "http_queue_seconds", "http_connection_setup_seconds", "http_connection_reused"}
                        and type(value) in {int, float} and math.isfinite(value) and value >= 0):
                    timings[name] = float(value)
        code = _response_code(response)
        if code is None:
            raise RuntimeError("malformed or undecidable prepared cancel response")
        return MutationReceipt(
            accepted=code == 200, order_id=order_id,
            tx_hash=prepared._tx_hash or _safe_text(_model_dict(response).get("tx_hash")),
            error=None if code == 200 else f"send_tx response code {code}",
            response_code=code, diagnostic_timings=timings,
        )

def _response_code(value: Any) -> int | None:
    mapping = _model_dict(value)
    # A transport HTTP status is not an application-level sendTx outcome.  A
    # body code is mandatory; without it, or when a proxy/server 5xx conflicts
    # with a nominal body success, the wire result remains undecidable.
    code = mapping.get("code")
    if code is None or isinstance(code, bool):
        return None
    if isinstance(code, int):
        numeric = code
    elif isinstance(code, str) and re.fullmatch(r"[0-9]+", code.strip()):
        numeric = int(code.strip())
    else:
        return None
    status = mapping.get("_http_status")
    if status is not None:
        if isinstance(status, bool):
            return None
        if isinstance(status, int):
            http_status = status
        elif isinstance(status, str) and re.fullmatch(r"[0-9]+", status.strip()):
            http_status = int(status.strip())
        else:
            return None
        if http_status >= 500:
            return None
        if http_status != 200 and numeric == 200:
            return None
    return numeric


def _require_success_code(payload: Mapping[str, Any], label: str) -> None:
    code = payload.get("code")
    if code is None:
        raise ContractError(f"{label} response lacks required code")
    try:
        numeric = int(code)
    except (TypeError, ValueError) as exc:
        raise ContractError(f"{label} response code is malformed") from exc
    if numeric != 200:
        raise ContractError(f"{label} response was not successful")


def _select_perp_market(
    payload: Mapping[str, Any],
    market_id: int,
    *,
    expected_symbol: str = "HOOD",
) -> dict[str, Any]:
    values = payload.get("order_book_details")
    if not isinstance(values, (list, tuple)):
        raise ContractError("orderBookDetails response lacks order_book_details")
    for item in values:
        candidate = _model_dict(item)
        try:
            candidate_id = int(candidate.get("market_id"))
        except (TypeError, ValueError):
            continue
        if candidate_id == market_id:
            market_type = candidate.get("market_type")
            if not isinstance(market_type, str) or market_type.strip().lower() != "perp":
                raise ContractError("orderBookDetails identity is not a perpetual market")
            symbol = candidate.get("symbol")
            if not isinstance(symbol, str) or not symbol.strip():
                raise ContractError("orderBookDetails market identity lacks symbol")
            if symbol.strip().upper() != expected_symbol.strip().upper():
                raise ContractError(
                    f"orderBookDetails market identity is not {expected_symbol.strip().upper()}"
                )
            return candidate
    raise ContractError(
        f"orderBookDetails response lacks the requested {expected_symbol.strip().upper()} market"
    )


def _select_perp_market_by_symbol(payload: Mapping[str, Any], symbol: str) -> dict[str, Any]:
    values = payload.get("order_book_details")
    if not isinstance(values, (list, tuple)):
        raise ContractError("orderBookDetails response lacks order_book_details")
    expected = symbol.strip().upper()
    for item in values:
        candidate = _model_dict(item)
        candidate_symbol = candidate.get("symbol")
        if not isinstance(candidate_symbol, str) or candidate_symbol.strip().upper() != expected:
            continue
        market_type = candidate.get("market_type")
        if not isinstance(market_type, str) or market_type.strip().lower() != "perp":
            raise ContractError("orderBookDetails symbol resolved to a non-perpetual market")
        try:
            candidate["market_id"] = int(candidate["market_id"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ContractError("orderBookDetails market identity has no valid market_id") from exc
        return candidate
    raise ContractError(f"orderBookDetails response lacks the requested {expected} perpetual")


def _safe_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    if re.fullmatch(r"0x[0-9a-fA-F]{8,128}", text):
        return text
    return None


__all__ = [
    "LighterSdkClient",
    "MissingSdkError",
    "PlainAioHttp",
    "ROBINHOOD_ORDER_BOOK_LIMIT",
    "REQUIRED_LIGHTER_SDK_VERSION",
    "SecretProvider",
    "SdkVersionError",
    "StaticSecretProvider",
]
