"""Owner-only append-only evidence storage for SS-001B."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
import json
import math
import os
from pathlib import Path
import secrets
import stat
from typing import Any, Iterator


_FORBIDDEN_FIELD_PARTS = (
    "credential",
    "password",
    "private_key",
    "secret",
    "signing_key",
    "access_token",
    "api_token",
    "api_key",
    "apikey",
    "token",
    "wallet",
    "private",
    "signer",
    "auth",
    "auth_header",
)


MAX_EVIDENCE_RECORDS = 2_500_000
MAX_EVIDENCE_FILE_BYTES = 12 * 1024 * 1024 * 1024
TERMINAL_FAILURE_RECORD_RESERVE = 1
TERMINAL_FAILURE_BYTES_RESERVE = 16 * 1024


class EvidenceStorageLimitExceeded(RuntimeError):
    """Raised before an append would cross a bounded evidence-store limit."""

    def __init__(self, limit: str) -> None:
        self.limit = limit
        super().__init__(f"evidence storage {limit} limit exceeded")


def new_run_id() -> str:
    """Return an unpredictable, path-safe identity for one fresh run."""

    return secrets.token_urlsafe(18)


def _reject_secret_field(name: str) -> None:
    lowered = name.lower()
    if any(part in lowered for part in _FORBIDDEN_FIELD_PARTS):
        raise ValueError("secret-bearing fields are not accepted by the evidence store")


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("non-finite Decimal cannot be persisted")
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("evidence mapping keys must be strings")
            _reject_secret_field(key)
            result[key] = _json_value(item)
        return result
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError("non-finite float cannot be persisted")
        return value
    raise TypeError(f"unsupported evidence value: {type(value).__name__}")


def _record_order(record: Mapping[str, Any], ordinal: int) -> tuple[Any, ...]:
    def text(name: str) -> str:
        value = record.get(name)
        return "" if value is None else str(value)

    raw_time = record.get("observed_monotonic_ns", record.get("received_monotonic_ns", 0))
    try:
        monotonic = int(raw_time)
    except (TypeError, ValueError):
        monotonic = 0
    return (
        monotonic,
        text("kind"),
        text("canonical_market"),
        text("policy_id"),
        text("quote_version_id"),
        text("horizon_ms"),
        text("record_id"),
        ordinal,
    )


class AppendOnlyEvidenceStore:
    """A fresh owner-only JSONL file with batched, single-sync appends."""

    def __init__(self, path: Path, run_id: str, metadata: Mapping[str, Any]) -> None:
        self.path = Path(path)
        self.run_id = run_id
        self._closed = False
        self._record_index = 0
        self._bytes_written = 0
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.path.parent, stat.S_IRWXU)
        descriptor = os.open(
            self.path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            stat.S_IRUSR | stat.S_IWUSR,
        )
        self._file = os.fdopen(descriptor, "w", encoding="utf-8", newline="\n")
        os.chmod(self.path, stat.S_IRUSR | stat.S_IWUSR)
        self.append_batch(
            ({"kind": "RUN_METADATA", "metadata": dict(metadata)},),
            sync=True,
        )

    @classmethod
    def create(
        cls,
        root: str | os.PathLike[str],
        *,
        metadata: Mapping[str, Any],
        run_id: str | None = None,
    ) -> "AppendOnlyEvidenceStore":
        identity = run_id or new_run_id()
        if not identity or "/" in identity or "\\" in identity:
            raise ValueError("run_id must be a non-empty path-safe identity")
        run_dir = Path(root) / f"run-{identity}"
        run_dir.mkdir(parents=True, exist_ok=False, mode=0o700)
        os.chmod(run_dir, stat.S_IRWXU)
        payload = dict(metadata)
        payload["run_id"] = identity
        payload["evidence_mode"] = payload.get("evidence_mode", "OBSERVATIONAL")
        return cls(run_dir / "evidence.jsonl", identity, payload)

    @property
    def is_closed(self) -> bool:
        return self._closed

    @property
    def record_count(self) -> int:
        return self._record_index

    @property
    def byte_count(self) -> int:
        return self._bytes_written

    def append_batch(
        self,
        records: Sequence[Mapping[str, Any]],
        *,
        sync: bool = True,
    ) -> tuple[int, ...]:
        if self._closed:
            raise RuntimeError("evidence store is closed")
        if not records:
            return ()
        normalized = tuple(dict(record) for record in records)
        for record in normalized:
            if not isinstance(record, Mapping):
                raise TypeError("evidence records must be mappings")
            supplied_run = record.get("run_id")
            if supplied_run is not None and supplied_run != self.run_id:
                raise ValueError("record run_id does not match the fresh store")
            _json_value(record)
        ordered = tuple(
            record
            for _, record in sorted(
                enumerate(normalized), key=lambda item: _record_order(item[1], item[0])
            )
        )
        terminal_failure = (
            len(ordered) == 1 and ordered[0].get("kind") == "RUN_FAILED"
        )
        if MAX_EVIDENCE_RECORDS <= TERMINAL_FAILURE_RECORD_RESERVE:
            raise ValueError("MAX_EVIDENCE_RECORDS must leave a terminal-marker reserve")
        record_limit = (
            MAX_EVIDENCE_RECORDS
            if terminal_failure
            else MAX_EVIDENCE_RECORDS - TERMINAL_FAILURE_RECORD_RESERVE
        )
        serialized: list[str] = []
        assigned: list[int] = []
        for offset, record in enumerate(ordered):
            index = self._record_index + offset
            payload = dict(record)
            payload["run_id"] = self.run_id
            payload["record_index"] = index
            serialized.append(
                json.dumps(_json_value(payload), sort_keys=True, separators=(",", ":"))
                + "\n"
            )
            assigned.append(index)
        batch_bytes = sum(len(line.encode("utf-8")) for line in serialized)
        byte_limit = (
            MAX_EVIDENCE_FILE_BYTES
            if terminal_failure
            else MAX_EVIDENCE_FILE_BYTES - TERMINAL_FAILURE_BYTES_RESERVE
        )
        if self._record_index + len(ordered) > record_limit:
            raise EvidenceStorageLimitExceeded("RECORD_COUNT")
        if self._bytes_written + batch_bytes > byte_limit:
            raise EvidenceStorageLimitExceeded("FILE_BYTES")
        for line in serialized:
            self._file.write(line)
            self._record_index += 1
            self._bytes_written += len(line.encode("utf-8"))
        self._file.flush()
        if sync:
            os.fsync(self._file.fileno())
        return tuple(assigned)

    def close(self) -> None:
        if self._closed:
            return
        self._file.flush()
        os.fsync(self._file.fileno())
        self._file.close()
        self._closed = True

    def __enter__(self) -> "AppendOnlyEvidenceStore":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


def iter_records(path: str | os.PathLike[str]) -> Iterator[dict[str, Any]]:
    with Path(path).open("r", encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid evidence JSON at line {line_number}") from exc
            if not isinstance(value, dict):
                raise ValueError(f"evidence line {line_number} is not an object")
            yield value


def store_permissions(path: str | os.PathLike[str]) -> int:
    return stat.S_IMODE(Path(path).stat().st_mode)


__all__ = [
    "AppendOnlyEvidenceStore",
    "EvidenceStorageLimitExceeded",
    "MAX_EVIDENCE_FILE_BYTES",
    "MAX_EVIDENCE_RECORDS",
    "TERMINAL_FAILURE_BYTES_RESERVE",
    "TERMINAL_FAILURE_RECORD_RESERVE",
    "iter_records",
    "new_run_id",
    "store_permissions",
]
