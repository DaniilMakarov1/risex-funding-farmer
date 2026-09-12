"""Durable, secret-free intent journal for one HCR-1 attempt."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import time
from typing import Any, Iterable, Mapping
import uuid


_SECRET_KEY = re.compile(
    r"(?:secret|private|password|credential|authorization|auth[_-]?token|api[_-]?key)",
    re.IGNORECASE,
)
_SECRET_VALUE = re.compile(
    r"-----BEGIN [^-]+-----.*?-----END [^-]+-----|(?<![A-Za-z0-9])[0-9a-fA-F]{64}(?![A-Za-z0-9])",
    re.DOTALL,
)


def sanitize(value: Any, *, key: str | None = None) -> Any:
    """Return JSON-safe data with secret-bearing fields removed/redacted."""

    if key is not None and _SECRET_KEY.search(key):
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
    return str(sanitize(str(exc)))[:500] or type(exc).__name__


@dataclass(frozen=True, slots=True)
class JournalEvent:
    sequence: int
    run_id: str
    event: str
    at: float
    payload: dict[str, Any]


class DurableJournal:
    """Append-only JSONL journal with fsync before mutation calls proceed."""

    def __init__(self, path: str | os.PathLike[str], *, run_id: str | None = None, clock=time.time) -> None:
        self.path = Path(path)
        self.run_id = run_id or uuid.uuid4().hex
        self._clock = clock
        self._sequence = self._read_last_sequence()

    def _read_last_sequence(self) -> int:
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
            return ()
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
        return tuple(events)

    def append(self, event: str, payload: Mapping[str, Any] | None = None, *, run_id: str | None = None) -> JournalEvent:
        if not isinstance(event, str) or not event.strip():
            raise ValueError("journal event must be non-empty")
        resolved_run_id = run_id or self.run_id
        safe_payload = sanitize(dict(payload or {}))
        assert isinstance(safe_payload, dict)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self.path.parent, 0o700)
        except OSError:
            pass
        self._sequence += 1
        value = {
            "sequence": self._sequence,
            "run_id": resolved_run_id,
            "event": event,
            "at": float(self._clock()),
            "payload": safe_payload,
        }
        encoded = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
        fd = os.open(self.path, flags, 0o600)
        try:
            os.write(fd, encoded)
            os.fsync(fd)
        finally:
            os.close(fd)
        try:
            os.chmod(self.path, 0o600)
        except OSError:
            pass
        return JournalEvent(
            sequence=self._sequence,
            run_id=resolved_run_id,
            event=event,
            at=float(value["at"]),
            payload=safe_payload,
        )

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
                if any(item.event in ("SOURCE_DISPATCH_INTENT", "RECEIVER_DISPATCH_INTENT", "CANCEL_DISPATCH_INTENT") for item in run_events):
                    return True
        return False

    def mutation_intent_exists(self, run_id: str, event: str) -> bool:
        return any(item.run_id == run_id and item.event == event for item in self.events)

    def assert_safe_to_start(self) -> None:
        if self.has_unresolved_mutation():
            raise RuntimeError("journal has unresolved mutation; use reconciliation-only resume")


__all__ = ["DurableJournal", "JournalEvent", "sanitize", "sanitize_exception"]
