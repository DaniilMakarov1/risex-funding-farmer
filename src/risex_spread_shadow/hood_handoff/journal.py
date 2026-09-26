"""Durable, secret-free intent journal for one HCR-1 attempt."""

from __future__ import annotations

from dataclasses import dataclass
import asyncio
import errno
import fcntl
import json
import os
from pathlib import Path
import re
import time
from typing import Any, Mapping, Sequence
import uuid


_SECRET_KEY = re.compile(
    r"(?:secret|private|password|credential|authorization|auth[_-]?token|access[_-]?token|api[_-]?key|"
    r"tx[_-]?(?:info|payload)|signed[_-]?(?:tx|transaction|payload)|signature)",
    re.IGNORECASE,
)
_SECRET_VALUE = re.compile(
    r"-----BEGIN [^-]+-----.*?-----END [^-]+-----|(?i:bearer)\s+[A-Za-z0-9._~+/=-]+|"
    r"(?i:authorization)\s*[:=]\s*[^,;\s]+|(?i:private[_ -]?key)\s*[:=]\s*[^,;\s]+|"
    r"(?i:tx[_-]?(?:info|payload)|signed[_-]?(?:tx|transaction|payload))\s*[:=]\s*[^,;]+",
    re.DOTALL,
)
_SAFE_ID_KEYS = frozenset(
    {
        "account_index",
        "market_id",
        "order_id",
        "trade_id",
        "client_order_index",
        "source_account_index",
        "receiver_account_index",
        "source_order_id",
        "receiver_order_id",
        "api_key_index",
        "chain_id",
        "base_asset_id",
        "quote_asset_id",
    }
)

# Non-authorizing observations, responses and planning may wait for the next
# barrier: responses already have durable preceding intents. Unknown events,
# intents, settings and terminal results stay durable.
EXECUTION_DIAGNOSTICS = frozenset({
    "ATTEMPT_STARTED", "PREFLIGHT_PROVED", "PLAN_READY",
    "SOURCE_DISPATCH_RESULT", "PRE_RECEIVER_GUARD", "ORDER_OBSERVED",
    "RECEIVER_DISPATCH_RESULT", *(f"RECEIVER_{n}_DISPATCH_RESULT" for n in range(2, 17)),
    "RECONCILIATION_STARTED", "LEG_RECONCILED", "SOURCE_OBSERVATION_RESOLVED",
    "SELECTION_PROVED", "LEVERAGE_PLAN", "PREPARATION_ATTEMPT",
    "FRESH_OPENING_MARGIN_BUDGET", "OPENING_PRICE_UPDATED", "OPENING_BOUNDS_REFRESHED",
    "PREPARATION_ACCEPTED", "OPENING_PLAN_READY", "CLOSING_PLAN_READY",
    "FALLBACK_SEND_BARRIER", "FALLBACK_PREPARATION_TIMINGS", "FALLBACK_PREPARED_SEND_BARRIER",
    "FALLBACK_ORDER_OBSERVATION", "FALLBACK_ATTEMPT_EVIDENCE",
    "FALLBACK_POST_ATTEMPT_ACCOUNT_OBSERVATION",
    "PILOT_READ_STREAM_SUMMARY", "PILOT_STREAM_TIMELINE", "HTTP_READ_TIMINGS",
})


def _write_all(fd: int, value: bytes) -> None:
    """Write one record completely, retrying only an interrupted syscall."""

    offset = 0
    while offset < len(value):
        try:
            written = os.write(fd, value[offset:])
        except OSError as exc:
            if isinstance(exc, InterruptedError) or exc.errno == errno.EINTR:
                continue
            raise
        if written <= 0 or written > len(value) - offset:
            raise OSError("journal write made no progress")
        offset += written


def sanitize(value: Any, *, key: str | None = None) -> Any:
    """Return JSON-safe data with secret-bearing fields removed/redacted."""

    lowered_key = key.lower() if key is not None else ""
    safe_index_key = lowered_key in _SAFE_ID_KEYS or (
        lowered_key.endswith(("_index", "_id")) and not _SECRET_KEY.search(lowered_key)
    )
    safe_config_key = key is not None and key.lower() in {"auth_token_lifetime_seconds"}
    if key is not None and _SECRET_KEY.search(key) and not safe_index_key and not safe_config_key:
        return "[REDACTED]"
    if isinstance(value, Mapping):
        return {str(item_key): sanitize(item, key=str(item_key)) for item_key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [sanitize(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        if isinstance(value, str):
            return _SECRET_VALUE.sub("[REDACTED]", value)
        return value
    if hasattr(value, "value") and isinstance(value.value, (str, int, float, bool)):
        return sanitize(value.value, key=key)
    return sanitize(str(value), key=key)


def sanitize_exception(exc: BaseException) -> str:
    """Return a non-sensitive, bounded error class at an external boundary."""

    name = type(exc).__name__.lower()
    if isinstance(exc, (TimeoutError, asyncio.TimeoutError)) or "timeout" in name:
        return "timeout"
    if isinstance(exc, (ConnectionError, OSError)) or any(token in name for token in ("connection", "clientconnector")):
        return "connection_error"
    if isinstance(exc, json.JSONDecodeError) or "json" in name:
        return "invalid_response"
    if "contract" in name or "preflight" in name or name in {"valueerror", "typeerror"}:
        return "contract_error"
    text = str(exc)
    match = re.search(r"\bHTTP\s+(\d{3})\b", text, re.IGNORECASE)
    if match:
        return f"http_error_{match.group(1)}"
    if "invalid" in text.lower() or "malformed" in text.lower():
        return "invalid_response"
    return "sdk_error"


@dataclass(frozen=True, slots=True)
class JournalEvent:
    sequence: int
    run_id: str
    event: str
    at: float
    payload: dict[str, Any]


class DurableJournal:
    """Append-only JSONL journal with fsync before mutation calls proceed."""

    def __init__(self, path: str | os.PathLike[str], *, run_id: str | None = None, clock=time.time,
                 deferred_events: frozenset[str] = frozenset()) -> None:
        if not deferred_events <= EXECUTION_DIAGNOSTICS:
            raise ValueError("only allowlisted diagnostics may be deferred")
        self.path = Path(path)
        self.run_id = run_id or uuid.uuid4().hex
        self._clock = clock
        self._lock_path = self.path.with_name(self.path.name + ".lock")
        self._lock_fd: int | None = None
        self._write_failed = False
        self._deferred_events = deferred_events
        self._pending: list[tuple[JournalEvent, bytes]] = []
        self._pending_bytes = 0
        self._sequence = self._read_last_sequence()

    def _ensure_owned_parent(self) -> None:
        """Create only missing path components and never chmod a caller's parent."""

        missing: list[Path] = []
        current = self.path.parent
        while not current.exists():
            missing.append(current)
            parent = current.parent
            if parent == current:
                raise RuntimeError("journal parent does not exist")
            current = parent
        for item in reversed(missing):
            item.mkdir(mode=0o700)
        parent = self.path.parent
        try:
            info = parent.stat()
        except OSError as exc:
            raise RuntimeError("journal parent cannot be inspected") from exc
        if info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise RuntimeError("journal must use an owner-only directory")

    def acquire_attempt(self) -> None:
        """Claim this journal path atomically for one bounded attempt."""

        if self._lock_fd is not None:
            raise RuntimeError("journal attempt is already claimed by this engine")
        self._ensure_owned_parent()
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            fd = os.open(self._lock_path, flags, 0o600)
        except OSError as exc:
            raise RuntimeError("journal lock cannot be opened safely") from exc
        try:
            info = os.fstat(fd)
            if info.st_uid != os.geteuid() or info.st_mode & 0o077:
                raise RuntimeError("journal lock must be an owner-only file")
            os.fchmod(fd, 0o600)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            try:
                os.close(fd)
            except OSError:
                pass
            raise RuntimeError("journal attempt is already owned by another process") from exc
        except BaseException:
            try:
                os.close(fd)
            except OSError:
                pass
            raise
        self._lock_fd = fd
        try:
            payload = json.dumps({"pid": os.getpid(), "run_id": self.run_id}, sort_keys=True).encode("utf-8")
            os.ftruncate(fd, 0)
            os.lseek(fd, 0, os.SEEK_SET)
            try:
                _write_all(fd, payload)
                os.fsync(fd)
            except BaseException:
                self._write_failed = True
                raise
            # Another engine may have constructed this journal before the lock was
            # released.  Refresh the sequence while holding the exclusive claim
            # so the next durable append cannot reuse a sequence number.
            self._sequence = self._read_last_sequence()
        except BaseException:
            try:
                self.release_attempt()
            except BaseException:
                pass
            raise

    def release_attempt(self) -> None:
        fd = self._lock_fd
        if fd is None:
            return
        try:
            if not self._write_failed:
                self.flush()
        finally:
            self._lock_fd = None
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)
        # Keep the owner-only lock record.  flock is released by close and by
        # process exit, so a crashed attempt remains restartable without
        # deleting the durable ownership evidence.

    def _read_last_sequence(self) -> int:
        if self.path.is_symlink():
            raise RuntimeError("journal path must not be a symlink")
        if not self.path.exists():
            return 0
        last = 0
        try:
            with self.path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    item = json.loads(line)
                    sequence = item.get("sequence")
                    if isinstance(sequence, int):
                        last = max(last, sequence)
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"journal is unreadable: {self.path}") from exc
        return last

    @property
    def events(self) -> tuple[JournalEvent, ...]:
        if not self.path.exists():
            return tuple(item for item, _ in self._pending)
        events: list[JournalEvent] = []
        with self.path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                item = json.loads(line)
                events.append(
                    JournalEvent(
                        sequence=int(item["sequence"]),
                        run_id=str(item["run_id"]),
                        event=str(item["event"]),
                        at=float(item["at"]),
                        payload=dict(item.get("payload") or {}),
                    )
                )
        return (*events, *(item for item, _ in self._pending))

    def append(self, event: str, payload: Mapping[str, Any] | None = None, *, run_id: str | None = None) -> JournalEvent:
        result = self._enqueue(event, payload, run_id=run_id)
        # Deferral is opt-in and only while this process owns the journal.
        if (self._lock_fd is None or event not in self._deferred_events
                or len(self._pending) >= 64 or self._pending_bytes >= 256 * 1024):
            self.flush()
        return result

    def append_many(self, rows: Sequence[tuple[str, Mapping[str, Any]]], *,
                    run_id: str | None = None) -> tuple[JournalEvent, ...]:
        """Persist every record in order with one fsync before any send.

        A torn batch is unresolved evidence, never a partially authorised send.
        The caller must not dispatch any member until this method returns.
        """
        result = tuple(self._enqueue(event, payload, run_id=run_id) for event, payload in rows)
        self.flush()
        return result

    async def append_many_async(self, rows: Sequence[tuple[str, Mapping[str, Any]]], *,
                                run_id: str | None = None) -> tuple[JournalEvent, ...]:
        """One durability wait without stopping the account/book read stream.

        The exclusive owner awaits this call; it must not append concurrently.
        Cancellation drains the worker before the fd/lock can be released.
        """
        result = tuple(self._enqueue(event, payload, run_id=run_id) for event, payload in rows)
        task = asyncio.create_task(asyncio.to_thread(self.flush))
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            try:
                await task
            finally:
                raise
        return result

    def _enqueue(self, event: str, payload: Mapping[str, Any] | None, *,
                 run_id: str | None) -> JournalEvent:
        if not isinstance(event, str) or not event.strip():
            raise ValueError("journal event must be non-empty")
        if self._write_failed:
            raise RuntimeError("journal write previously failed; refusing further appends")
        resolved_run_id = run_id or self.run_id
        safe_payload = sanitize(dict(payload or {}))
        assert isinstance(safe_payload, dict)
        self._sequence += 1
        value = {
            "sequence": self._sequence,
            "run_id": resolved_run_id,
            "event": event,
            "at": float(self._clock()),
            "payload": safe_payload,
        }
        encoded = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        result = JournalEvent(self._sequence, resolved_run_id, event, float(value["at"]), safe_payload)
        self._pending.append((result, encoded))
        self._pending_bytes += len(encoded)
        return result

    def flush(self) -> None:
        """Complete the preceding records or poison this writer on any failure."""
        if self._write_failed:
            raise RuntimeError("journal write previously failed; refusing further appends")
        if not self._pending:
            return
        try:
            self._flush_pending()
        except BaseException:
            self._write_failed = True
            raise
        self._pending.clear()
        self._pending_bytes = 0

    def _flush_pending(self) -> None:
        self._ensure_owned_parent()
        if self.path.is_symlink():
            raise RuntimeError("journal path must not be a symlink")
        created = not self.path.exists()
        if self.path.exists():
            try:
                info = self.path.stat()
            except OSError as exc:
                raise RuntimeError("journal cannot be inspected") from exc
            if info.st_uid != os.geteuid() or info.st_mode & 0o077:
                raise RuntimeError("journal must be an owner-only file")
        flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(self.path, flags, 0o600)
        try:
            info = os.fstat(fd)
            if info.st_uid != os.geteuid() or info.st_mode & 0o077:
                raise RuntimeError("journal must use an owner-only file")
            os.fchmod(fd, 0o600)
            try:
                _write_all(fd, b"".join(encoded for _, encoded in self._pending))
                os.fsync(fd)
            except BaseException:
                # A short/failed append may have left an undecodable suffix.
                # Preserve that prefix and prevent any later mutation from
                # treating the journal as a trustworthy append stream.
                self._write_failed = True
                raise
        finally:
            os.close(fd)
        if created:
            # The first mutation must survive loss of the new directory entry,
            # not merely loss of the file's cached data.
            parent_fd = os.open(self.path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(parent_fd)
            finally:
                os.close(parent_fd)

    def latest(self, *events: str) -> JournalEvent | None:
        allowed = set(events)
        for item in reversed(self.events):
            if not allowed or item.event in allowed:
                return item
        return None

    def has_unresolved_mutation(self) -> bool:
        """Return true after a dispatch intent without a proven terminal run."""

        events = self.events
        if not events:
            return False
        terminal = {"PREFLIGHT_BLOCKED", "PREVIEW"}
        run_ids = {item.run_id for item in events}
        for run_id in run_ids:
            run_events = [item for item in events if item.run_id == run_id]
            if not run_events:
                continue
            last = run_events[-1]
            complete = last.event == "COMPLETE" and last.payload.get("outcome") != "UNKNOWN"
            if last.event not in terminal and not complete:
                if any(item.event in ("SOURCE_DISPATCH_INTENT", "RECEIVER_DISPATCH_INTENT", "CANCEL_DISPATCH_INTENT")
                       or re.fullmatch(r"RECEIVER_(?:[2-9]|1[0-6])_DISPATCH_INTENT", item.event)
                       for item in run_events):
                    return True
        return False

    def has_completed_mutation(self) -> bool:
        """Return true when this path already records a terminal mutation run."""

        return any(
            item.event == "COMPLETE" and item.payload.get("outcome") != "PREVIEW"
            for item in self.events
        )

    def mutation_intent_exists(self, run_id: str, event: str) -> bool:
        return any(item.run_id == run_id and item.event == event for item in self.events)

    def assert_safe_to_start(self) -> None:
        if self.has_unresolved_mutation():
            raise RuntimeError("journal has unresolved mutation; use reconciliation-only resume")
        if self.has_completed_mutation():
            raise RuntimeError("journal already contains a completed mutation attempt")


__all__ = ["DurableJournal", "JournalEvent", "sanitize", "sanitize_exception"]
