"""Streaming, offline reports for saved HCR random-cycle journals.

This module intentionally contains no SDK, HTTP, authentication, signing, or
mutation imports.  It reads an already persisted parent journal and its child
journals one line at a time.  A report is evidence about the journal only:
missing or contradictory records are never promoted to a successful fill,
flat inventory, known fees, or known PnL.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter, deque
from decimal import Decimal, InvalidOperation
from pathlib import Path
import re
from typing import Any, Iterable, Mapping, Sequence

from .journal import sanitize


REPORT_SCHEMA = "hcr-27-offline-cycle-report-v1"
REPORT_BATCH_SCHEMA = "hcr-27-offline-cycle-report-batch-v1"

# Reports must remain useful on an unexpectedly large or additive journal.
# The complete input is still hashed and counted, while only a bounded factual
# projection is retained for detail-oriented sections below.
MAX_DETAIL_RECORDS = 512
MAX_DETAIL_HEAD_RECORDS = 256
MAX_PROJECTED_LIST_ITEMS = 64
MAX_PROJECTED_STRING = 1024

_PROJECTED_KEYS = frozenset(
    {
        "accepted",
        "after",
        "admission_before",
        "account_index",
        "active_orders",
        "admission_reasons",
        "attempt",
        "attempt_index",
        "at",
        "authorized",
        "available_balance",
        "before",
        "binding",
        "book_observation_available",
        "book_observed_at",
        "boundary_books_available",
        "classifications",
        "client_order_index",
        "closing",
        "config",
        "counterparty_account_index",
        "counterparty_match_status",
        "counterparty_matched_quantity",
        "counterparty_client_order_index",
        "counterparty_order_id",
        "direction",
        "dispatched",
        "economic_findings",
        "economic_status",
        "error",
        "event",
        "fee",
        "fee_rate",
        "fee_total",
        "filled_quantity",
        "findings",
        "from_attempt",
        "gross_notional",
        "history_complete",
        "hold_seconds",
        "incremental_margin_evidence",
        "initial_quantity",
        "journal_path",
        "joint_match_quantity",
        "joint_match_status",
        "joint_trade_match",
        "latency",
        "market_id",
        "market_metadata",
        "market_symbol",
        "maximum_attempts",
        "named_counterparty_quantity",
        "named_counterparty_status",
        "next_attempt",
        "operation_mode",
        "order",
        "order_book_observed_at",
        "order_expiry_ms",
        "order_id",
        "order_type",
        "outcome",
        "paired_execution",
        "paired_quantity",
        "phase",
        "plan",
        "position_after",
        "position_before",
        "position_observed_at",
        "priority_guard",
        "priority_proof_admitted",
        "priority_reason",
        "priority_status",
        "price",
        "reason",
        "receiver",
        "receiver_account_index",
        "receiver_filled_quantity",
        "receiver_identity",
        "receiver_order_id",
        "receiver_position",
        "receiver_public_level",
        "receiver_recheck_transition",
        "receipt",
        "reconciliation_state",
        "remaining_positions",
        "remaining_quantity",
        "retryable_pair",
        "run_id",
        "selection",
        "side",
        "signed_position",
        "source",
        "source_account_index",
        "source_filled_quantity",
        "source_identity",
        "source_order_id",
        "source_position",
        "source_public_level",
        "source_recheck_transition",
        "status",
        "time_in_force",
        "trade_id",
        "trade_ids",
        "trades",
        "tx_hash",
        "unknown_reasons",
        "upper_quantity",
        "quantity",
        "quantity_int",
        "request_started_at",
        "request_finished_at",
        "response_code",
        "observed_at",
        "remaining_position_observed_at",
        "source_side",
        "source_price",
        "external_better_price_volume",
        "fallbacks",
        "opening",
        "receiver_dispatched",
        "reconciliation_seconds",
        "history_pages",
        "reduce_only", "ready", "owner_account_index", "requested_quantity",
        "source_position_before", "receiver_position_before",
        "opening_reason", "closing_reason",
        "source_quote_age_seconds", "quote_age_to_source_dispatch_seconds",
        "paired_preparation_seconds", "source_submit_ack_seconds", "source_visibility_seconds",
        "receiver_admission_seconds", "receiver_submit_ack_seconds", "receiver_visibility_seconds",
        "receiver_fill_observation_seconds", "coalesced_pre_receiver_window_seconds",
        "concurrent_pre_receiver_checks_seconds", "public_book_read_seconds", "pre_receiver_checks_seconds",
        "source_dispatch_intent_at", "source_dispatch_ack_at", "receiver_dispatch_intent_at",
        "receiver_dispatch_ack_at", "receiver_dispatch_outcome_at", "receiver_fill_observed_at",
        "receiver_terminal_observed_at", "receiver_admission_at",
    }
)

_OMIT_BULKY_KEYS = frozenset(
    {
        "dispatch_evidence",
        "history_pages",
        "better_price_evidence",
        "same_price_evidence",
        "source_recheck",
        "receiver_recheck",
        "source_order",
        "market_metadata",
    }
)


class OfflineReportError(ValueError):
    """The requested offline report input cannot be identified safely."""


class _Record(dict[str, Any]):
    """Small JSON-compatible record retained after one line was parsed."""

    @property
    def event(self) -> str:
        return str(self["event"])

    @property
    def payload(self) -> dict[str, Any]:
        value = self.get("payload")
        return value if isinstance(value, dict) else {}


class _FileData:
    def __init__(self, path: Path, kind: str) -> None:
        self.path = path
        self.kind = kind
        self._head: list[_Record] = []
        self._tail: deque[_Record] = deque(maxlen=MAX_DETAIL_RECORDS - MAX_DETAIL_HEAD_RECORDS)
        self.record_count = 0
        self.event_counts: Counter[str] = Counter()
        self.detail_truncated = False
        self.issues: list[dict[str, Any]] = []
        self.line_count = 0
        self.sha256 = hashlib.sha256()
        self.run_id: str | None = None
        self.missing = False
        self.last_sequence: int | None = None

    @property
    def records(self) -> list[_Record]:
        """Return bounded details while preserving deterministic head/tail order."""

        return [*self._head, *self._tail]

    def add_record(self, record: _Record) -> None:
        self.record_count += 1
        self.event_counts[record.event] += 1
        if len(self._head) < MAX_DETAIL_HEAD_RECORDS:
            self._head.append(record)
            return
        before = len(self._tail)
        self._tail.append(record)
        if before == len(self._tail) or self.record_count > MAX_DETAIL_RECORDS:
            if not self.detail_truncated:
                self.issue("DETAIL_TRUNCATED", "bounded report detail omitted journal records; aggregate proofs are unavailable")
            self.detail_truncated = True

    @property
    def status(self) -> str:
        if self.missing or self.issues:
            return "INCOMPLETE"
        return "OK"

    def issue(
        self,
        code: str,
        message: str,
        *,
        line: int | None = None,
        event: str | None = None,
    ) -> None:
        item: dict[str, Any] = {
            "code": code,
            "message": message,
            "path": str(self.path),
        }
        if line is not None:
            item["line"] = line
        if event is not None:
            item["event"] = event
        if len(self.issues) < MAX_DETAIL_RECORDS:
            self.issues.append(item)

    def as_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "path": str(self.path),
            "kind": self.kind,
            "status": self.status,
            "line_count": self.line_count,
            "record_count": self.record_count,
            "event_counts": dict(sorted(self.event_counts.items())),
            "detail_records_retained": len(self.records),
            "detail_truncated": self.detail_truncated,
            "sha256": self.sha256.hexdigest(),
        }
        if self.run_id is not None:
            value["run_id"] = self.run_id
        if self.issues:
            value["issues"] = list(self.issues)
        return value


def _finite_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _decimal_value(value: Any) -> Decimal | None:
    """Parse a finite Decimal without float conversion or underflow."""

    if isinstance(value, bool) or value is None:
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _copy_json(value: Any) -> Any:
    """Copy only JSON values; journal input is already JSON but may be hostile."""

    if isinstance(value, Mapping):
        return {str(key): _copy_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_copy_json(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _bounded_json(value: Any, *, key: str | None = None, depth: int = 0, truncated: list[bool] | None = None) -> Any:
    """Sanitize and project JSON values to bounded, factual report detail."""

    if depth > 16:
        if truncated is not None:
            truncated.append(True)
        return "[DETAIL_TRUNCATED]"
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for raw_key, item in value.items():
            item_key = str(raw_key)
            if item_key in _OMIT_BULKY_KEYS:
                continue
            if item_key not in _PROJECTED_KEYS:
                continue
            result[item_key] = _bounded_json(sanitize(item, key=item_key), key=item_key, depth=depth + 1, truncated=truncated)
        return result
    if isinstance(value, (list, tuple)):
        items = list(value)
        result = [_bounded_json(sanitize(item), depth=depth + 1, truncated=truncated) for item in items[:MAX_PROJECTED_LIST_ITEMS]]
        if len(items) > MAX_PROJECTED_LIST_ITEMS:
            if truncated is not None:
                truncated.append(True)
            result.append(f"[DETAIL_ITEMS_OMITTED:{len(items) - MAX_PROJECTED_LIST_ITEMS}]")
        return result
    if isinstance(value, str):
        safe = sanitize(value, key=key)
        if not isinstance(safe, str):
            return safe
        if len(safe) > MAX_PROJECTED_STRING and truncated is not None:
            truncated.append(True)
        return safe if len(safe) <= MAX_PROJECTED_STRING else safe[:MAX_PROJECTED_STRING] + "…"
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return str(value)


def _project_payload(event: str, payload: Mapping[str, Any], data: _FileData) -> dict[str, Any]:
    """Keep allowlisted facts only; exact input identity remains in the file hash."""

    truncated: list[bool] = []
    projected = _bounded_json(payload, truncated=truncated)
    if truncated:
        data.issue("PAYLOAD_TRUNCATED", "required projected detail exceeds the report display bound", event=event)
    return projected if isinstance(projected, dict) else {}


def _read_jsonl(path: Path, kind: str) -> _FileData:
    data = _FileData(path, kind)
    if not path.exists():
        data.missing = True
        data.issue("MISSING_FILE", "journal file does not exist")
        return data
    if not path.is_file():
        data.missing = True
        data.issue("NOT_A_FILE", "journal input is not a regular file")
        return data

    try:
        handle = path.open("rb")
    except OSError as exc:
        data.missing = True
        data.issue("READ_ERROR", f"journal cannot be opened: {type(exc).__name__}")
        return data

    expected_sequence = 1
    try:
        with handle:
            for line_number, raw in enumerate(handle, 1):
                data.line_count += 1
                data.sha256.update(raw)
                if not raw.strip():
                    data.issue("BLANK_LINE", "blank lines are not journal records", line=line_number)
                    continue
                try:
                    text = raw.decode("utf-8")
                except UnicodeDecodeError:
                    data.issue("INVALID_ENCODING", "journal line is not valid UTF-8", line=line_number)
                    continue
                has_newline = text.endswith("\n")
                if has_newline:
                    text = text[:-1]
                    if text.endswith("\r"):
                        text = text[:-1]
                try:
                    value = json.loads(text)
                except (TypeError, json.JSONDecodeError):
                    data.issue("MALFORMED_JSON", "journal line is not complete JSON", line=line_number)
                    if not has_newline:
                        data.issue(
                            "TRUNCATED_LINE",
                            "malformed final journal line has no terminating newline",
                            line=line_number,
                        )
                    continue
                if not isinstance(value, Mapping):
                    data.issue("RECORD_NOT_OBJECT", "journal record must be a JSON object", line=line_number)
                    continue

                sequence = value.get("sequence")
                run_id = value.get("run_id")
                event = value.get("event")
                at = value.get("at")
                payload = value.get("payload")
                valid = True
                if isinstance(sequence, bool) or not isinstance(sequence, int):
                    data.issue("MISSING_FIELD", "sequence must be an integer", line=line_number)
                    valid = False
                if not isinstance(run_id, str) or not run_id.strip():
                    data.issue("MISSING_FIELD", "run_id must be non-empty text", line=line_number)
                    valid = False
                if not isinstance(event, str) or not event.strip():
                    data.issue("MISSING_FIELD", "event must be non-empty text", line=line_number)
                    valid = False
                at_number = _finite_number(at)
                if at_number is None:
                    data.issue("MISSING_FIELD", "at must be a finite number", line=line_number)
                    valid = False
                elif at_number < 0:
                    data.issue("INVALID_TIMESTAMP", "at must be non-negative", line=line_number)
                    valid = False
                if not isinstance(payload, Mapping):
                    data.issue("MISSING_FIELD", "payload must be a JSON object", line=line_number)
                    valid = False
                if not valid:
                    continue

                if sequence != expected_sequence:
                    code = "CONFLICTING_SEQUENCE" if sequence < expected_sequence else "SEQUENCE_GAP"
                    data.issue(
                        code,
                        f"expected sequence {expected_sequence}, observed {sequence}",
                        line=line_number,
                        event=event,
                    )
                expected_sequence = max(expected_sequence, sequence + 1)
                if data.run_id is None:
                    data.run_id = run_id
                elif data.run_id != run_id:
                    data.issue(
                        "CONFLICTING_RUN_ID",
                        "one journal contains more than one run_id",
                        line=line_number,
                        event=event,
                    )
                data.last_sequence = sequence
                data.add_record(
                    _Record(
                        sequence=sequence,
                        run_id=run_id,
                        event=event,
                        at=at_number,
                        payload=_project_payload(event, payload, data),
                        line=line_number,
                        path=str(path),
                        kind=kind,
                    )
                )
                if not has_newline:
                    data.issue(
                        "TRUNCATED_LINE",
                        "last journal record has no terminating newline",
                        line=line_number,
                        event=event,
                    )
    except OSError as exc:
        data.issue("READ_ERROR", f"journal read failed: {type(exc).__name__}")
    if data.line_count == 0:
        data.issue("EMPTY_JOURNAL", "journal contains no records")
    return data


def _natural_key(path: Path) -> tuple[str, int, str]:
    match = re.search(r"-(\d+)\.jsonl$", path.name)
    return (path.stem.split("-attempt")[0], int(match.group(1)) if match else 1, path.name)


def _kind_for(path: Path) -> str:
    if path.name == "cycle.jsonl":
        return "cycle"
    if path.name.startswith("opening"):
        return "opening"
    if path.name.startswith("closing"):
        return "closing"
    return "unknown"


def _discover_files(input_path: Path) -> tuple[Path, list[tuple[Path, str]], list[dict[str, Any]]]:
    """Resolve one cycle directory without reading or modifying it."""

    issues: list[dict[str, Any]] = []
    path = input_path.expanduser()
    if path.is_dir():
        cycle_path = path / "cycle.jsonl"
        if not cycle_path.exists():
            cycle_dirs = sorted(
                (item for item in path.glob("cycle-*") if item.is_dir()),
                key=lambda item: item.name,
            )
            if len(cycle_dirs) == 1:
                path = cycle_dirs[0]
                cycle_path = path / "cycle.jsonl"
            elif cycle_dirs:
                issues.append(
                    {
                        "code": "AMBIGUOUS_INPUT_DIRECTORY",
                        "message": "input directory contains multiple cycle directories; choose one cycle directory",
                        "path": str(input_path),
                    }
                )
                return path / "cycle.jsonl", [(path / "cycle.jsonl", "cycle")], issues
        paths = [(cycle_path, "cycle")]
        children = sorted(
            (
                item
                for item in path.glob("*.jsonl")
                if item.name != "cycle.jsonl" and item.name.startswith(("opening", "closing"))
            ),
            key=_natural_key,
        )
        paths.extend((item, _kind_for(item)) for item in children)
        return cycle_path, paths, issues

    if path.name == "cycle.jsonl":
        parent = path.parent
        paths = [(path, "cycle")]
        children = sorted(
            (
                item
                for item in parent.glob("*.jsonl")
                if item.name != "cycle.jsonl" and item.name.startswith(("opening", "closing"))
            ),
            key=_natural_key,
        )
        paths.extend((item, _kind_for(item)) for item in children)
        return path, paths, issues

    if path.name.startswith(("opening", "closing")) and (path.parent / "cycle.jsonl").exists():
        cycle_path = path.parent / "cycle.jsonl"
        return _discover_files(cycle_path)

    issues.append(
        {
            "code": "CYCLE_JOURNAL_NOT_FOUND",
            "message": "input must be a cycle directory or cycle.jsonl",
            "path": str(path),
        }
    )
    return path, [(path, "cycle")], issues


def _records(data: _FileData, event: str | None = None) -> list[_Record]:
    if event is None:
        return list(data.records)
    return [item for item in data.records if item.event == event]


def _last_record(data: _FileData, event: str) -> _Record | None:
    for item in reversed(data.records):
        if item.event == event:
            return item
    return None


def _first_record(data: _FileData, event: str) -> _Record | None:
    for item in data.records:
        if item.event == event:
            return item
    return None


def _phase_for_event(event: str, kind: str) -> str:
    if kind in {"opening", "closing"}:
        return kind
    if event.startswith("PREPARATION") or event in {"SELECTION_PROVED", "OPENING_PRICE_UPDATED", "OPENING_BOUNDS_REFRESHED"}:
        return "preparation"
    if event.startswith("OPENING") or event == "FIRST_MUTATION_BOUNDARY" or event.startswith("PAIR_ATTEMPT"):
        return "opening"
    if event.startswith("HOLD"):
        return "hold"
    if event.startswith("CLOSING"):
        return "closing"
    if event.startswith("FALLBACK"):
        return "fallback"
    return "cycle"


def _attempt_from(payload: Mapping[str, Any], fallback: Any = None) -> int | None:
    for key in ("attempt", "attempt_index", "from_attempt", "next_attempt"):
        value = payload.get(key)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    return fallback if isinstance(fallback, int) else None


def _reason_values(payload: Mapping[str, Any]) -> list[str]:
    values: list[str] = []
    for key in ("reason", "priority_reason", "opening_reason", "closing_reason"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            values.append(value.strip())
    for key in ("unknown_reasons", "admission_reasons", "findings", "economic_findings"):
        value = payload.get(key)
        if isinstance(value, list):
            values.extend(item.strip() for item in value if isinstance(item, str) and item.strip())
    return values


def _plan_legs(plan: Mapping[str, Any]) -> Iterable[tuple[str, Mapping[str, Any]]]:
    for leg in ("source", "receiver"):
        value = plan.get(leg)
        if isinstance(value, Mapping):
            yield leg, value


def _plan_records(files: Sequence[_FileData]) -> list[dict[str, Any]]:
    planned: list[dict[str, Any]] = []
    for data in files:
        if data.kind not in {"cycle", "opening", "closing"}:
            continue
        for record in data.records:
            payload = record.payload
            if record.event == "PLAN_READY":
                plan = payload.get("plan")
                if isinstance(plan, Mapping):
                    for leg, leg_plan in _plan_legs(plan):
                        planned.append(
                            {
                                "phase": data.kind,
                                "attempt": _attempt_from(payload, payload.get("binding", {}).get("attempt_index") if isinstance(payload.get("binding"), Mapping) else None),
                                "leg": leg,
                                "kind": "ORDER_PLAN",
                                "planned_at": record["at"],
                                "source_file": str(data.path),
                                "source_event": record.event,
                                "plan": _copy_json(leg_plan),
                            }
                        )
            elif record.event in {"OPENING_PLAN_READY", "CLOSING_PLAN_READY"}:
                config = payload.get("config")
                if isinstance(config, Mapping):
                    for leg in ("source", "receiver"):
                        plan = {
                            "account_index": config.get(f"{leg}_account_index"),
                            "market_id": config.get("market_id"),
                            "quantity": config.get("quantity", payload.get("paired_quantity")),
                            "operation_mode": config.get("operation_mode"),
                        }
                        # A cycle direction such as LONG is not an order side.
                        # Keep a side only when the journal explicitly binds it.
                        explicit_side = config.get(f"{leg}_side") or config.get(f"{leg}_order_side")
                        if explicit_side is not None:
                            plan["side"] = explicit_side
                        if any(value is not None for value in plan.values()):
                            planned.append(
                                {
                                    "phase": (
                                        "opening"
                                        if record.event.startswith("OPENING")
                                        else "closing"
                                        if record.event.startswith("CLOSING")
                                        else _phase_for_event(record.event, data.kind)
                                    ),
                                    "attempt": _attempt_from(payload),
                                    "leg": leg,
                                    "kind": "CYCLE_PLAN",
                                    "planned_at": record["at"],
                                    "source_file": str(data.path),
                                    "source_event": record.event,
                                    "plan": plan,
                                }
                            )
    # Child PLAN_READY is authoritative when it exists.  Parent cycle plans
    # are retained only for a phase/attempt/leg for which no child plan exists.
    child_keys = {
        (item["phase"], item.get("attempt"), item["leg"])
        for item in planned
        if item["kind"] == "ORDER_PLAN"
    }
    return [
        item
        for item in planned
        if item["kind"] == "ORDER_PLAN"
        or (item["phase"], item.get("attempt"), item["leg"]) not in child_keys
    ]


def _intent_actions(files: Sequence[_FileData]) -> list[dict[str, Any]]:
    prefix_map = {
        "SOURCE_DISPATCH": "source",
        "RECEIVER_DISPATCH": "receiver",
        "CANCEL_DISPATCH": "cancel",
        "FALLBACK_DISPATCH": "fallback",
    }
    actions: list[dict[str, Any]] = []
    for data in files:
        pending: list[dict[str, Any]] = []
        for record in data.records:
            event = record.event
            if event.endswith("_DISPATCH_INTENT"):
                prefix = event.removesuffix("_INTENT")
                leg = prefix_map.get(prefix)
                if leg is None:
                    continue
                payload = record.payload
                action = {
                    "phase": data.kind if data.kind in {"opening", "closing"} else "fallback",
                    "attempt": _attempt_from(payload),
                    "leg": leg,
                    "kind": "MUTATION_INTENT",
                    "intent_at": record["at"],
                    "source_file": str(data.path),
                    "source_event": event,
                    "intent_recorded": True,
                    "possible_dispatch": True,
                    "status": "UNKNOWN_INCOMPLETE",
                    "accepted": None,
                    "response_observed": False,
                    "confirmed_response": False,
                    "dispatched": False,
                    "plan": _copy_json(payload.get("plan")) if isinstance(payload.get("plan"), Mapping) else None,
                }
                actions.append(action)
                pending.append(action)
                continue
            matching_prefix = None
            if event.endswith("_DISPATCH_RESULT"):
                matching_prefix = event.removesuffix("_RESULT")
            elif event.endswith("_DISPATCH_UNKNOWN"):
                matching_prefix = event.removesuffix("_UNKNOWN")
            if matching_prefix is None:
                continue
            leg = prefix_map.get(matching_prefix)
            if leg is None:
                continue
            payload = record.payload
            candidate = next(
                (
                    item
                    for item in reversed(pending)
                    if item["leg"] == leg and "response_event" not in item
                ),
                None,
            )
            if candidate is None:
                data.issue(
                    "ORPHAN_DISPATCH_RESULT",
                    "dispatch result/unknown has no preceding intent",
                    line=record.get("line"),
                    event=event,
                )
                continue
            candidate["response_observed"] = event.endswith("_RESULT")
            candidate["confirmed_response"] = event.endswith("_RESULT")
            candidate["response_at"] = record["at"]
            candidate["response_event"] = event
            if event.endswith("_UNKNOWN"):
                candidate["status"] = "DISPATCHED_UNKNOWN"
                candidate["accepted"] = None
                candidate["dispatched"] = False
                candidate["reason"] = payload.get("reason")
            else:
                accepted = payload.get("accepted")
                candidate["accepted"] = accepted if isinstance(accepted, bool) else None
                candidate["status"] = (
                    "ACCEPTED"
                    if accepted is True
                    else "REJECTED"
                    if accepted is False
                    else "RESPONSE_MALFORMED"
                )
                candidate["dispatched"] = accepted is True
                if payload.get("error") is not None:
                    candidate["reason"] = payload.get("error")
            if isinstance(payload.get("order_id"), (str, int)):
                candidate["order_id"] = str(payload["order_id"])
        for action in pending:
            if "response_event" not in action:
                action["status"] = "INTENT_ONLY_UNFINISHED"
                action["reason"] = "journal ended before a matching dispatch result or unknown record"
    return actions


def _receipt_from_complete(record: _Record) -> Mapping[str, Any] | None:
    payload = record.payload
    receipt = payload.get("receipt")
    if isinstance(receipt, Mapping):
        return receipt
    return payload if isinstance(payload.get("source"), Mapping) or isinstance(payload.get("receiver"), Mapping) else None


def _issue(
    code: str,
    message: str,
    *,
    data: _FileData | None = None,
    record: _Record | None = None,
    **extra: Any,
) -> dict[str, Any]:
    value: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        value["path"] = str(data.path)
    if record is not None:
        value["line"] = record.get("line")
        value["event"] = record.event
    value.update(extra)
    return value


def _execution_evidence(
    files: Sequence[_FileData],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Validate each persisted terminal receipt against its own immutable plan."""
    executions: list[dict[str, Any]] = []
    fills: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []
    terminal_statuses = {
        "filled", "canceled", "cancelled", "canceled-post-only",
        "canceled-too-much-slippage", "canceled-not-enough-liquidity", "rejected", "expired",
    }
    cycle = next((item for item in files if item.kind == "cycle"), None)
    binding = next((r.payload["binding"] for r in (cycle.records if cycle else [])
                    if isinstance(r.payload.get("binding"), Mapping)), {})

    def append_execution(data: _FileData, record: _Record, leg: str,
                         value: Mapping[str, Any], plan: Mapping[str, Any],
                         attempt: int | None) -> None:
        fallback = leg == "fallback"
        phase = "fallback" if fallback else data.kind
        order = value.get("order") if isinstance(value.get("order"), Mapping) else None
        after = value.get("after") if isinstance(value.get("after"), Mapping) else {}
        before = value.get("before") if isinstance(value.get("before"), Mapping) else {}
        account = value.get("account_index")
        quantity = _decimal_value(value.get("filled_quantity", order.get("filled_quantity") if order else None))
        position_before = _decimal_value(value.get("position_before", before.get("signed_position")))
        position_after = _decimal_value(value.get("position_after", after.get("signed_position")))
        history = value.get("history_complete") is True
        receipt = value.get("receipt") if isinstance(value.get("receipt"), Mapping) else {}
        dispatched = (order is not None or receipt.get("accepted") is True) if fallback else value.get("dispatched")
        if not isinstance(plan, Mapping):
            plan = {}
        invalid = False

        def fail(code: str, message: str) -> None:
            nonlocal invalid
            invalid = True
            issues.append(_issue(code, message, data=data, record=record, phase=phase, leg=leg, attempt=attempt))

        identity_fields = ("account_index", "market_id", "client_order_index", "side", "order_type", "time_in_force", "reduce_only")
        if not plan or any(plan.get(key) is None for key in identity_fields):
            fail("UNBOUND_EXECUTION", "terminal leg lacks its complete immutable order plan")
        if account is None or str(account) != str(plan.get("account_index")):
            fail("UNBOUND_EXECUTION", "receipt account does not match its order plan")
        if any(binding.get(k) is None for k in ("source_account_index", "receiver_account_index", "market_id")) or binding.get("source_account_index") == binding.get("receiver_account_index"):
            fail("UNBOUND_EXECUTION", "parent cycle account/market identity is incomplete")
        expected_account = binding.get(f"{leg}_account_index")
        if not fallback and expected_account is not None and str(account) != str(expected_account):
            fail("UNBOUND_EXECUTION", "child account does not match the parent cycle role")
        if binding.get("market_id") is not None and str(plan.get("market_id")) != str(binding["market_id"]):
            fail("UNBOUND_EXECUTION", "child market does not match the parent cycle")
        if fallback and binding.get("source_account_index") is not None and account not in (binding.get("source_account_index"), binding.get("receiver_account_index")):
            fail("UNBOUND_EXECUTION", "fallback account is outside the cycle")
        expected_quantity = _decimal_value(plan.get("quantity"))
        price_bound = _decimal_value(plan.get("price"))
        if expected_quantity is None or expected_quantity <= 0 or price_bound is None or price_bound <= 0:
            fail("UNBOUND_EXECUTION", "order plan lacks positive exact quantity/price")
        if not history:
            fail("INCOMPLETE_TRADE_HISTORY", "terminal leg lacks complete trade history")
        if value.get("unknown_reasons") or not isinstance(dispatched, bool):
            fail("UNRESOLVED_EXECUTION", "terminal leg has unknown execution or missing dispatch state")
        if not fallback and not isinstance(value.get("unknown_reasons"), list):
            fail("UNRESOLVED_EXECUTION", "terminal leg lacks explicit unknown-reasons state")
        if (value.get("outcome", record.payload.get("outcome")) == "UNKNOWN"):
            fail("UNRESOLVED_EXECUTION", "terminal execution is explicitly UNKNOWN")
        if not fallback:
            intent_present = any(r.event == f"{leg.upper()}_DISPATCH_INTENT" for r in data.records)
            if intent_present != dispatched:
                fail("UNRESOLVED_EXECUTION", "terminal dispatch state does not agree with durable intent")
        if order is not None:
            if not dispatched:
                fail("UNRESOLVED_EXECUTION", "an unsent leg has an order snapshot")
            for key in identity_fields:
                if order.get(key) is None or str(order[key]) != str(plan.get(key)):
                    fail("UNBOUND_EXECUTION", f"terminal order does not match plan field {key}")
            if not order.get("order_id") or (value.get("order_id") is not None and str(value["order_id"]) != str(order["order_id"])):
                fail("UNBOUND_EXECUTION", "terminal order ID is missing or contradictory")
            if str(order.get("status", "")).lower() not in terminal_statuses:
                fail("UNRESOLVED_EXECUTION", "order snapshot is not terminal")
            if _decimal_value(order.get("price")) != price_bound or _decimal_value(order.get("initial_quantity")) != expected_quantity:
                fail("UNBOUND_EXECUTION", "terminal order price/initial quantity differs from plan")
            remaining = _decimal_value(order.get("remaining_quantity"))
            if remaining is None or remaining < 0 or quantity is None or quantity < 0 or expected_quantity is None or quantity > expected_quantity or _decimal_value(order.get("filled_quantity")) != quantity:
                fail("CONFLICTING_FILL_QUANTITY", "terminal order quantities are missing or contradict the leg")
            if str(order.get("status", "")).lower() == "filled" and (remaining != 0 or quantity != expected_quantity):
                fail("CONFLICTING_FILL_QUANTITY", "filled order does not prove its full planned quantity")
            order_at = _finite_number(order.get("observed_at"))
            if order_at is None or order_at < 0 or order_at > record["at"]:
                fail("INVALID_INTERVAL", "terminal order observation is missing or after its journal boundary")
        elif dispatched or not (quantity == 0 or fallback and receipt.get("accepted") is False):
            fail("UNRESOLVED_EXECUTION", "possible execution has no terminal order snapshot")
        if fallback and quantity is None and receipt.get("accepted") is False:
            quantity = Decimal(0)
        raw_trades = value.get("trades")
        if not isinstance(raw_trades, list):
            fail("INCOMPLETE_TRADE_HISTORY", "terminal trade list is missing")
            raw_trades = []
        trades: list[dict[str, Any]] = []
        seen: dict[str, dict[str, Any]] = {}
        for trade in raw_trades:
            if not isinstance(trade, Mapping):
                fail("UNBOUND_TRADE_EVIDENCE", "trade receipt is not an object")
                continue
            tid = trade.get("trade_id")
            if not isinstance(tid, (str, int)) or isinstance(tid, bool) or not str(tid).strip():
                fail("UNBOUND_TRADE_EVIDENCE", "trade identity is missing")
                continue
            if str(tid) in seen:
                if trade != seen[str(tid)]:
                    fail("CONFLICTING_TRADE_ID", "repeated trade ID has conflicting receipt fields")
                continue
            seen[str(tid)] = dict(trade)
            tq, tp = _decimal_value(trade.get("quantity")), _decimal_value(trade.get("price"))
            if tq is None or tq <= 0 or tp is None or tp <= 0:
                fail("UNBOUND_TRADE_EVIDENCE", "trade quantity/price is not positive finite Decimal")
            for key in ("account_index", "market_id", "client_order_index", "side"):
                if trade.get(key) is None or str(trade[key]) != str(plan.get(key)):
                    fail("UNBOUND_TRADE_EVIDENCE", f"trade does not bind to planned {key}")
            if order is None or not trade.get("order_id") or str(trade["order_id"]) != str(order.get("order_id")):
                fail("UNBOUND_TRADE_EVIDENCE", "trade does not bind to reconciled order")
            if tp is not None and price_bound is not None and ((plan.get("side") == "BUY" and tp > price_bound) or (plan.get("side") == "SELL" and tp < price_bound)):
                fail("UNBOUND_TRADE_EVIDENCE", "trade price violates the saved planned bound")
            observed = _finite_number(trade.get("observed_at"))
            if observed is None or observed < 0 or observed > record["at"]:
                fail("INVALID_INTERVAL", "trade observation is missing or after terminal evidence")
            trades.append(dict(trade))
        trade_total = sum((_decimal_value(t.get("quantity")) or Decimal(0) for t in trades), Decimal(0))
        if quantity is None or trade_total != quantity:
            fail("CONFLICTING_FILL_QUANTITY", "complete history sum differs from reconciled quantity")
        if position_before is None or position_after is None or plan.get("side") not in {"BUY", "SELL"}:
            fail("UNRESOLVED_EXECUTION", "exact causal positions are missing")
        elif position_before + (trade_total if plan["side"] == "BUY" else -trade_total) != position_after:
            fail("CONFLICTING_POSITION", "trade sum does not explain the recorded position change")
        if fallback and (after.get("active_orders") != [] or str(after.get("account_index")) != str(account) or str(after.get("market_id")) != str(plan.get("market_id"))):
            fail("UNRESOLVED_EXECUTION", "fallback final account snapshot is incomplete or has active orders")
        execution = {
            "phase": phase, "attempt": attempt, "leg": leg, "account_index": account,
            "market_id": plan.get("market_id"), "plan": dict(plan),
            "filled_quantity": None if quantity is None else str(quantity),
            "fee_total": value.get("fee_total"), "history_complete": history,
            "resolved": not invalid, "dispatched": dispatched,
            "outcome": value.get("outcome", record.payload.get("outcome")),
            "economic_status": value.get("economic_status", record.payload.get("economic_status")),
            "position_before": None if position_before is None else str(position_before),
            "position_after": None if position_after is None else str(position_after),
            "after": dict(after), "observed_at": record["at"], "order": order,
            "trades": trades if not invalid else [], "source_file": str(data.path),
            "source_event": record.event, "line": record.get("line"), "fallback": fallback,
            "guard_status": (_last_record(data, "PRE_RECEIVER_GUARD").payload.get("status")
                             if _last_record(data, "PRE_RECEIVER_GUARD") else None),
        }
        executions.append(execution)
        if not invalid:
            for trade in trades:
                fills.append({**trade, "phase": phase, "attempt": attempt, "leg": leg,
                              "history_complete": True, "source_file": str(data.path),
                              "source_event": record.event, "line": record.get("line")})

    for data in files:
        if data.kind in {"opening", "closing"}:
            complete = _records(data, "COMPLETE")
            if len(complete) > 1:
                issues.append(_issue("CONFLICTING_TERMINAL", "child has multiple terminal receipts", data=data))
            for record in complete:
                receipt = _receipt_from_complete(record) or {}
                plan_record = _last_record(data, "PLAN_READY")
                persisted_plan = plan_record.payload.get("plan", {}) if plan_record else {}
                plan = receipt.get("plan") if isinstance(receipt.get("plan"), Mapping) else persisted_plan
                if not isinstance(plan, Mapping):
                    plan = {}
                if persisted_plan and plan != persisted_plan:
                    issues.append(_issue("CONFLICTING_PLAN", "terminal plan differs from PLAN_READY", data=data, record=record))
                if receipt.get("outcome") is not None and receipt["outcome"] != record.payload.get("outcome"):
                    issues.append(_issue("CONFLICTING_TERMINAL", "receipt outcome contradicts terminal envelope", data=data, record=record))
                if receipt.get("run_id") is not None and receipt["run_id"] != data.run_id:
                    issues.append(_issue("CONFLICTING_RUN_ID", "receipt run identity differs from its journal", data=data, record=record))
                for leg in ("source", "receiver"):
                    value = receipt.get(leg)
                    if not isinstance(value, Mapping):
                        issues.append(_issue("INCOMPLETE_EXECUTION", "terminal receipt is missing a required leg", data=data, record=record))
                        continue
                    append_execution(data, record, leg, value, plan.get(leg, {}), _attempt_from(record.payload, receipt.get("attempt_index")))
        elif data.kind == "cycle":
            evidence = [r for r in data.records if r.event == "FALLBACK_ATTEMPT_EVIDENCE"]
            keys = {(_attempt_from(r.payload), r.payload.get("account_index")) for r in evidence}
            evidence.extend(r for r in data.records if r.event == "FALLBACK_RECONCILED" and (_attempt_from(r.payload), r.payload.get("account_index")) not in keys)
            for record in evidence:
                append_execution(data, record, "fallback", record.payload, record.payload.get("plan", {}), _attempt_from(record.payload))
    by_trade: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for fill in fills:
        key = (str(fill["account_index"]), str(fill["market_id"]), str(fill["trade_id"]))
        by_trade.setdefault(key, []).append(fill)
    reused = [group for group in by_trade.values() if len(group) > 1]
    for group in reused:
        issues.append(_issue("REUSED_TRADE_ID", "one account trade appears in multiple execution boundaries", trade_id=group[0]["trade_id"]))
        for fill in group:
            fills.remove(fill)
        for execution in executions:
            if any(execution["source_file"] == fill["source_file"] and execution["leg"] == fill["leg"] for fill in group):
                execution["resolved"] = False
    return executions, fills, issues


def _fill_records(files: Sequence[_FileData]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Return validated fills, execution evidence, and explicit evidence issues."""

    executions, fills, issues = _execution_evidence(files)
    return fills, executions, issues


def _terminal_order_records(files: Sequence[_FileData]) -> list[dict[str, Any]]:
    """Return only order snapshots persisted at a reconciliation boundary."""

    values: list[dict[str, Any]] = []
    seen: set[tuple[str, int | None, str, str, str | None]] = set()

    def append(value: dict[str, Any]) -> None:
        key = (
            str(value.get("phase")),
            value.get("attempt") if isinstance(value.get("attempt"), int) else None,
            str(value.get("leg")),
            str(value.get("order_id") or ""),
            value.get("status") if isinstance(value.get("status"), str) else None,
        )
        if key in seen:
            return
        seen.add(key)
        values.append(value)

    for data in files:
        if data.kind in {"opening", "closing"}:
            for record in data.records:
                if record.event != "COMPLETE":
                    continue
                receipt = _receipt_from_complete(record)
                if not isinstance(receipt, Mapping):
                    continue
                attempt = _attempt_from(record.payload, receipt.get("attempt_index"))
                for leg in ("source", "receiver"):
                    value = receipt.get(leg)
                    if not isinstance(value, Mapping) or not isinstance(value.get("order"), Mapping):
                        continue
                    order = value["order"]
                    append(
                        {
                            "phase": data.kind,
                            "attempt": attempt,
                            "leg": leg,
                            "order_id": value.get("order_id") or order.get("order_id"),
                            "status": order.get("status"),
                            "filled_quantity": order.get("filled_quantity"),
                            "remaining_quantity": order.get("remaining_quantity"),
                            "observed_at": order.get("observed_at"),
                            "order": _copy_json(order),
                            "source_file": str(data.path),
                            "source_event": record.event,
                        }
                    )
        elif data.kind == "cycle":
            for record in data.records:
                if record.event not in {"FALLBACK_ATTEMPT_EVIDENCE", "FALLBACK_RECONCILED"}:
                    continue
                order = record.payload.get("order")
                if not isinstance(order, Mapping):
                    continue
                append(
                    {
                        "phase": "fallback",
                        "attempt": _attempt_from(record.payload),
                        "leg": "fallback",
                        "order_id": order.get("order_id"),
                        "status": order.get("status"),
                        "filled_quantity": order.get("filled_quantity"),
                        "remaining_quantity": order.get("remaining_quantity"),
                        "observed_at": order.get("observed_at"),
                        "order": _copy_json(order),
                        "source_file": str(data.path),
                        "source_event": record.event,
                    }
                )
    return values


def _semantic_issues(files: Sequence[_FileData]) -> list[dict[str, Any]]:
    """Detect contradictory identity evidence without judging normal status transitions."""

    issues: list[dict[str, Any]] = []
    identity_by_order: dict[str, dict[str, Any]] = {}
    identity_fields = ("account_index", "market_id", "client_order_index", "side", "order_type", "price")

    def inspect_order(data: _FileData, record: _Record, order: Mapping[str, Any]) -> None:
        order_id = order.get("order_id")
        if order_id is None:
            return
        key = str(order_id)
        current = {field: order.get(field) for field in identity_fields if order.get(field) is not None}
        previous = identity_by_order.get(key)
        if previous is not None:
            conflicts = {
                field: {"previous": previous[field], "observed": current[field]}
                for field in current
                if field in previous and str(previous[field]) != str(current[field])
            }
            if conflicts:
                issues.append(
                    {
                        "code": "CONFLICTING_ORDER_EVIDENCE",
                        "message": "one order_id has contradictory immutable identity fields",
                        "path": str(data.path),
                        "line": record.get("line"),
                        "event": record.event,
                        "order_id": key,
                        "conflicts": conflicts,
                    }
                )
        else:
            identity_by_order[key] = current

    for data in files:
        for record in data.records:
            payload = record.payload
            if record.event in {"ORDER_OBSERVED", "LEG_RECONCILED"}:
                order = payload.get("order") if isinstance(payload.get("order"), Mapping) else payload
                if isinstance(order, Mapping):
                    inspect_order(data, record, order)
            elif record.event == "COMPLETE":
                receipt = _receipt_from_complete(record)
                if isinstance(receipt, Mapping):
                    for leg in ("source", "receiver"):
                        value = receipt.get(leg)
                        if isinstance(value, Mapping) and isinstance(value.get("order"), Mapping):
                            inspect_order(data, record, value["order"])
            elif record.event in {"FALLBACK_ATTEMPT_EVIDENCE", "FALLBACK_RECONCILED"}:
                order = payload.get("order")
                if isinstance(order, Mapping):
                    inspect_order(data, record, order)
    return issues


def _event_progression(files: Sequence[_FileData]) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = []
    for data in files:
        for record in data.records:
            payload = record.payload
            values.append(
                {
                    "event": record.event,
                    "at": record["at"],
                    "phase": _phase_for_event(record.event, data.kind),
                    "attempt": _attempt_from(payload),
                    "source_file": str(data.path),
                    "line": record.get("line"),
                }
            )
    values.sort(key=lambda item: (item["at"], item["source_file"], item.get("line", 0)))
    return values


def _duration(
    start: float | None,
    end: float | None,
    *,
    issues: list[dict[str, Any]] | None = None,
    label: str = "interval",
) -> float | None:
    if start is None or end is None:
        return None
    if end < start:
        if issues is not None:
            issues.append(
                {
                    "code": "INVALID_INTERVAL",
                    "message": "journal interval has an end timestamp before its start timestamp",
                    "interval": label,
                    "start_at": start,
                    "end_at": end,
                }
            )
        return None
    return end - start


def _latency_measure(
    name: str,
    value: Any,
    *,
    basis: str | None = None,
    unavailable_reason: str | None = None,
    overlap: bool = False,
    issues: list[dict[str, Any]] | None = None,
    label: str | None = None,
) -> dict[str, Any]:
    number = _finite_number(value)
    if number is not None and number < 0:
        if issues is not None:
            issues.append(
                {
                    "code": "INVALID_INTERVAL",
                    "message": "latency measurement is negative",
                    "interval": label or name,
                    "value": number,
                }
            )
        number = None
    if number is None:
        return {
            "status": "UNAVAILABLE",
            "seconds": None,
            "basis": basis,
            "overlap": overlap,
            "unavailable_reason": unavailable_reason or "journal does not contain this measurement",
        }
    return {
        "status": "AVAILABLE",
        "seconds": number,
        "basis": basis or "saved journal measurement",
        "overlap": overlap,
        "unavailable_reason": None,
    }


def _latency_unknown(name: str, *, basis: str, reason: str) -> dict[str, Any]:
    return {
        "status": "UNKNOWN",
        "seconds": None,
        "basis": basis,
        "overlap": False,
        "unavailable_reason": reason,
    }


def _latency_reports(
    files: Sequence[_FileData],
) -> tuple[dict[str, Any], list[str], list[dict[str, Any]]]:
    reports: dict[str, list[dict[str, Any]]] = {"opening": [], "closing": [], "fallback": []}
    interval_issues: list[dict[str, Any]] = []
    notes: list[str] = [
        "Latency intervals are local journal/adapter observations; no network, exchange, signing, or sequencer attribution is inferred.",
        "coalesced_pre_receiver_window_seconds, concurrent_pre_receiver_checks_seconds, and public_book_read_seconds overlap and must not be summed.",
    ]
    for data in files:
        if data.kind not in {"opening", "closing"}:
            continue
        complete_records = [record for record in data.records if record.event == "COMPLETE"]
        complete = complete_records[-1] if complete_records else None
        payload = {} if complete is None else complete.payload
        receipt = _receipt_from_complete(complete) if complete is not None else None
        latency = payload.get("latency") if isinstance(payload.get("latency"), Mapping) else None
        if latency is None and isinstance(receipt, Mapping) and isinstance(receipt.get("latency"), Mapping):
            latency = receipt.get("latency")
        latency = dict(latency or {})
        attempt = _attempt_from(payload, receipt.get("attempt_index") if isinstance(receipt, Mapping) else None)
        plan_record = _first_record(data, "PLAN_READY")
        source_intent = _first_record(data, "SOURCE_DISPATCH_INTENT")
        source_result = _first_record(data, "SOURCE_DISPATCH_RESULT")
        receiver_intent = _first_record(data, "RECEIVER_DISPATCH_INTENT")
        receiver_result = _first_record(data, "RECEIVER_DISPATCH_RESULT")
        source_dispatch_unknown = _first_record(data, "SOURCE_DISPATCH_UNKNOWN") is not None
        receiver_dispatch_unknown = _first_record(data, "RECEIVER_DISPATCH_UNKNOWN") is not None
        guard = _first_record(data, "PRE_RECEIVER_GUARD")
        source_observed = next(
            (record for record in data.records if record.event == "ORDER_OBSERVED" and record.payload.get("account_index") == (source_intent.payload.get("plan", {}).get("account_index") if source_intent else None)),
            None,
        )
        receiver_observed = next(
            (record for record in data.records if record.event == "ORDER_OBSERVED" and receiver_intent is not None and record.payload.get("account_index") == receiver_intent.payload.get("plan", {}).get("account_index")),
            None,
        )
        values: dict[str, dict[str, Any]] = {}
        quote_age = latency.get("source_quote_age_seconds", latency.get("quote_age_to_source_dispatch_seconds"))
        values["quote_age"] = _latency_measure("quote_age", quote_age, basis="latency.source_quote_age_seconds", issues=interval_issues, label="quote_age")
        preparation = latency.get("paired_preparation_seconds")
        if preparation is None and plan_record is not None and source_intent is not None:
            preparation = _duration(plan_record["at"], source_intent["at"], issues=interval_issues, label="preparation")
            values["preparation"] = _latency_measure(
                "preparation", preparation, basis="PLAN_READY to SOURCE_DISPATCH_INTENT journal interval", issues=interval_issues, label="preparation"
            )
        else:
            values["preparation"] = _latency_measure("preparation", preparation, basis="latency.paired_preparation_seconds", issues=interval_issues, label="preparation")
        source_ack = latency.get("source_submit_ack_seconds")
        if source_ack is None and source_intent is not None and source_result is not None:
            source_ack = _duration(source_intent["at"], source_result["at"], issues=interval_issues, label="source_dispatch_ack")
        values["source_dispatch_ack"] = (
            _latency_unknown(
                "source_dispatch_ack",
                basis="SOURCE_DISPATCH_UNKNOWN",
                reason="source transport outcome is unknown after the durable intent",
            )
            if source_dispatch_unknown
            else _latency_measure(
                "source_dispatch_ack", source_ack, basis="latency.source_submit_ack_seconds or intent/result journal interval", issues=interval_issues, label="source_dispatch_ack"
            )
        )
        source_visibility = latency.get("source_visibility_seconds")
        if source_visibility is None and source_result is not None and source_observed is not None:
            source_visibility = _duration(source_result["at"], source_observed["at"], issues=interval_issues, label="source_visibility")
        values["source_visibility"] = _latency_measure(
            "source_visibility", source_visibility, basis="latency.source_visibility_seconds or result/observation interval", issues=interval_issues, label="source_visibility"
        )
        receiver_admission = latency.get("receiver_admission_seconds")
        if receiver_admission is None and guard is not None:
            gp = guard.payload
            receiver_admission = _duration(
                _finite_number(gp.get("request_started_at")),
                _finite_number(gp.get("request_finished_at")),
                issues=interval_issues,
                label="receiver_admission",
            )
        values["receiver_admission"] = _latency_measure(
            "receiver_admission",
            receiver_admission,
            basis="latency.receiver_admission_seconds or PRE_RECEIVER_GUARD request interval",
            unavailable_reason="no complete PRE_RECEIVER_GUARD request interval was persisted",
            issues=interval_issues,
            label="receiver_admission",
        )
        receiver_ack = latency.get("receiver_submit_ack_seconds")
        if receiver_ack is None and receiver_intent is not None and receiver_result is not None:
            receiver_ack = _duration(receiver_intent["at"], receiver_result["at"], issues=interval_issues, label="receiver_dispatch_ack")
        values["receiver_dispatch_ack"] = (
            _latency_unknown(
                "receiver_dispatch_ack",
                basis="RECEIVER_DISPATCH_UNKNOWN",
                reason="receiver transport outcome is unknown after the durable intent",
            )
            if receiver_dispatch_unknown
            else _latency_measure(
                "receiver_dispatch_ack",
                receiver_ack,
                basis="latency.receiver_submit_ack_seconds or intent/result journal interval",
                unavailable_reason="receiver dispatch was not recorded or its response boundary is absent",
                issues=interval_issues,
                label="receiver_dispatch_ack",
            )
        )
        receiver_visibility = latency.get("receiver_visibility_seconds", latency.get("receiver_fill_observation_seconds"))
        if receiver_visibility is None and receiver_result is not None and receiver_observed is not None:
            receiver_visibility = _duration(receiver_result["at"], receiver_observed["at"], issues=interval_issues, label="receiver_visibility")
        values["receiver_visibility"] = _latency_measure(
            "receiver_visibility",
            receiver_visibility,
            basis="latency.receiver_fill_observation_seconds or result/observation interval",
            unavailable_reason="receiver order was not dispatched or terminal observation is absent",
            issues=interval_issues,
            label="receiver_visibility",
        )
        reconciliation = latency.get("reconciliation_seconds")
        values["reconciliation"] = _latency_measure(
            "reconciliation",
            reconciliation,
            basis="latency.reconciliation_seconds",
            unavailable_reason="COMPLETE latency did not contain reconciliation_seconds",
            issues=interval_issues,
            label="reconciliation",
        )
        item: dict[str, Any] = {
            "phase": data.kind,
            "attempt": attempt,
            "source_file": str(data.path),
            "measurements": values,
            "unavailable": [name for name, value in values.items() if value["status"] == "UNAVAILABLE"],
            "overlap": {
                "coalesced_pre_receiver_window_seconds": latency.get("coalesced_pre_receiver_window_seconds"),
                "concurrent_pre_receiver_checks_seconds": latency.get("concurrent_pre_receiver_checks_seconds"),
                "public_book_read_seconds": latency.get("public_book_read_seconds"),
                "pre_receiver_checks_seconds": latency.get("pre_receiver_checks_seconds"),
                "not_additive": True,
            },
            "timestamps": {
                key: latency.get(key)
                for key in (
                    "source_dispatch_intent_at",
                    "source_dispatch_ack_at",
                    "receiver_dispatch_intent_at",
                    "receiver_dispatch_ack_at",
                    "receiver_dispatch_outcome_at",
                    "receiver_fill_observed_at",
                    "receiver_terminal_observed_at",
                    "receiver_admission_at",
                )
                if key in latency
            },
        }
        # Flat aliases make the report convenient for operators and keep the
        # stage values machine-readable without forcing callers to understand
        # the nested evidence metadata.
        for name, value in values.items():
            item[f"{name}_seconds"] = value["seconds"]
        reports[data.kind].append(item)

    cycle_data = next((data for data in files if data.kind == "cycle"), None)
    cycle_latency: dict[str, Any] = {"phase_intervals": []}
    if cycle_data is not None:
        first = _first_record(cycle_data, "CYCLE_STARTED")
        selection = _first_record(cycle_data, "SELECTION_PROVED")
        boundary = _first_record(cycle_data, "FIRST_MUTATION_BOUNDARY")
        complete = _last_record(cycle_data, "CYCLE_COMPLETE")
        if first and selection:
            cycle_latency["phase_intervals"].append(
                {"phase": "selection", "start_at": first["at"], "end_at": selection["at"], "seconds": _duration(first["at"], selection["at"], issues=interval_issues, label="selection"), "basis": "CYCLE_STARTED to SELECTION_PROVED"}
            )
        if selection and boundary:
            cycle_latency["phase_intervals"].append(
                {"phase": "preparation", "start_at": selection["at"], "end_at": boundary["at"], "seconds": _duration(selection["at"], boundary["at"], issues=interval_issues, label="cycle_preparation"), "basis": "SELECTION_PROVED to FIRST_MUTATION_BOUNDARY; retries are one interval"}
            )
        hold = _first_record(cycle_data, "HOLD_ANCHORED")
        closing_plan = _first_record(cycle_data, "CLOSING_PLAN_READY")
        closing_complete = _last_record(cycle_data, "CLOSING_COMPLETE")
        if hold and closing_plan:
            cycle_latency["phase_intervals"].append(
                {"phase": "hold", "start_at": hold["at"], "end_at": closing_plan["at"], "seconds": _duration(hold["at"], closing_plan["at"], issues=interval_issues, label="hold"), "basis": "HOLD_ANCHORED to CLOSING_PLAN_READY"}
            )
        if closing_plan and closing_complete:
            cycle_latency["phase_intervals"].append(
                {"phase": "closing", "start_at": closing_plan["at"], "end_at": closing_complete["at"], "seconds": _duration(closing_plan["at"], closing_complete["at"], issues=interval_issues, label="closing"), "basis": "CLOSING_PLAN_READY to CLOSING_COMPLETE"}
            )
        fallback_intent = _first_record(cycle_data, "FALLBACK_DISPATCH_INTENT")
        fallback_end = None
        for event in ("FALLBACK_RECONCILED", "FALLBACK_ATTEMPT_EVIDENCE", "CYCLE_COMPLETE"):
            candidate = _last_record(cycle_data, event)
            if candidate is not None and (fallback_end is None or candidate["at"] > fallback_end["at"]):
                fallback_end = candidate
        if fallback_intent and fallback_end:
            cycle_latency["phase_intervals"].append(
                {"phase": "fallback", "start_at": fallback_intent["at"], "end_at": fallback_end["at"], "seconds": _duration(fallback_intent["at"], fallback_end["at"], issues=interval_issues, label="fallback"), "basis": "first fallback intent to last durable fallback evidence"}
            )
        if complete is not None:
            cycle_latency["terminal_at"] = complete["at"]
    return {"cycle": cycle_latency, **reports}, notes, interval_issues


def _positions_from_cycle(
    cycle: _FileData | None,
    files: Sequence[_FileData],
    executions: Sequence[Mapping[str, Any]],
    issues: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], list[str]]:
    """Classify inventory only after terminal child and causal position proof."""

    notes: list[str] = []
    if cycle is None:
        return {
            "status": "UNKNOWN",
            "source": None,
            "receiver": None,
            "observed_at": {"source": None, "receiver": None},
            "proof": None,
        }, ["cycle journal was not available for inventory proof"]

    complete = _last_record(cycle, "CYCLE_COMPLETE")
    payload = {} if complete is None else complete.payload
    positions = payload.get("remaining_positions")
    observed = payload.get("remaining_position_observed_at")
    if not isinstance(positions, Mapping):
        positions = {}
    if not isinstance(observed, Mapping):
        observed = {}
    source = positions.get("source")
    receiver = positions.get("receiver")
    source_at = observed.get("source")
    receiver_at = observed.get("receiver")
    source_decimal = _decimal_value(source)
    receiver_decimal = _decimal_value(receiver)
    source_time = _finite_number(source_at)
    receiver_time = _finite_number(receiver_at)

    account_roles: dict[str, str] = {}
    binding = payload.get("binding") if isinstance(payload.get("binding"), Mapping) else {}
    for role in ("source", "receiver"):
        value = binding.get(f"{role}_account_index")
        if value is not None:
            account_roles[str(value)] = role
    for record in cycle.records:
        candidate = record.payload.get("binding")
        if not isinstance(candidate, Mapping):
            continue
        for role in ("source", "receiver"):
            value = candidate.get(f"{role}_account_index")
            if value is not None:
                account_roles[str(value)] = role
    for candidate in (payload.get("opening"), payload.get("closing")):
        if not isinstance(candidate, Mapping):
            continue
        plan = candidate.get("plan") if isinstance(candidate.get("plan"), Mapping) else {}
        for role in ("source", "receiver"):
            leg = plan.get(role)
            if isinstance(leg, Mapping) and leg.get("account_index") is not None:
                account_roles[str(leg["account_index"])] = role

    observations: dict[str, list[tuple[float, Decimal]]] = {"source": [], "receiver": []}
    for execution in executions:
        at = _finite_number(execution.get("observed_at"))
        if at is None:
            continue
        position_after = _decimal_value(execution.get("position_after"))
        leg = execution.get("leg")
        if leg in observations and position_after is not None:
            observations[str(leg)].append((at, position_after))
        after = execution.get("after")
        if isinstance(after, Mapping):
            labelled = any(isinstance(after.get(role), Mapping) for role in ("source", "receiver"))
            if labelled:
                for role in ("source", "receiver"):
                    account = after.get(role)
                    if isinstance(account, Mapping):
                        value = _decimal_value(account.get("signed_position"))
                        observed_at = _finite_number(account.get("observed_at"))
                        if value is not None and observed_at is not None:
                            observations[role].append((observed_at, value))
            else:
                role = account_roles.get(str(execution.get("account_index")))
                value = _decimal_value(after.get("signed_position"))
                observed_at = _finite_number(after.get("observed_at"))
                if role is not None and value is not None and observed_at is not None:
                    observations[role].append((observed_at, value))
        if execution.get("fallback"):
            role = account_roles.get(str(execution.get("account_index")))
            value = _decimal_value(execution.get("position_after"))
            if role is not None and value is not None:
                observations[role].append((at, value))

    # Child COMPLETE rows are the authoritative causal observations for their
    # account roles.  A fallback's account payload is already role-labelled.
    child_terminal_missing = any(issue.get("code") in {"CHILD_TERMINAL_MISSING", "REFERENCED_CHILD_MISSING"} for issue in issues)
    structural_uncertainty = bool(issues)
    child_has_intent = any(
        data.kind in {"opening", "closing"}
        and any(record.event.endswith("_DISPATCH_INTENT") for record in data.records)
        for data in files
    )
    fallback_has_intent = any(
        data.kind == "cycle" and any(record.event == "FALLBACK_DISPATCH_INTENT" for record in data.records)
        for data in files
    )
    child_resolution = all(bool(execution.get("resolved")) for execution in executions)
    if fallback_has_intent and not any(bool(execution.get("fallback")) for execution in executions):
        child_resolution = False
    execution_evidence_present = all(bool(observations[role]) for role in ("source", "receiver"))
    observed_agrees = True
    for role, expected in (("source", source_decimal), ("receiver", receiver_decimal)):
        if expected is None:
            observed_agrees = False
            continue
        latest = max(observations[role], key=lambda item: item[0]) if observations[role] else None
        if latest is None or latest[1] != expected or _finite_number(observed.get(role)) is None or _finite_number(observed[role]) < latest[0] or _finite_number(observed[role]) > complete["at"]:
            observed_agrees = False
    exact_zero = source_decimal == Decimal(0) and receiver_decimal == Decimal(0)
    valid_times = source_time is not None and receiver_time is not None and source_time >= 0 and receiver_time >= 0
    parent_unknown = str(payload.get("outcome", "")).upper() == "UNKNOWN"
    terminal_proof = complete is not None and isinstance(payload.get("remaining_positions"), Mapping)
    causal_latest = {
        role: (
            {"observed_at": max(observations[role], key=lambda item: item[0])[0], "position": str(max(observations[role], key=lambda item: item[0])[1])}
            if observations[role]
            else None
        )
        for role in ("source", "receiver")
    }

    if source_decimal is None or receiver_decimal is None:
        status = "UNKNOWN"
        notes.append("inventory quantities are not finite Decimal values")
    elif not terminal_proof or not valid_times:
        status = "UNKNOWN"
        notes.append("flat-looking positions are not promoted without CYCLE_COMPLETE and both valid observation timestamps")
    elif parent_unknown or child_terminal_missing or structural_uncertainty or not child_resolution:
        status = "UNKNOWN"
        notes.append("inventory proof is unknown because a child execution boundary or evidence validation is unresolved")
    elif not execution_evidence_present or not observed_agrees:
        status = "UNKNOWN"
        notes.append("parent position snapshot does not causally agree with terminal child/fallback account observations")
    elif exact_zero:
        status = "CONFIRMED_FLAT"
    else:
        status = "OPEN_INVENTORY"

    return {
        "status": status,
        "source": source,
        "receiver": receiver,
        "observed_at": {"source": source_at, "receiver": receiver_at},
        "proof": {
            "event": "CYCLE_COMPLETE" if terminal_proof else None,
            "source_position_present": source is not None,
            "receiver_position_present": receiver is not None,
            "observation_times_present": valid_times,
            "exact_decimal_zero": exact_zero,
            "terminal_child_resolution": child_resolution,
            "causal_position_observations_present": execution_evidence_present,
            "causal_position_observations_agree": observed_agrees,
            "causal_latest": causal_latest,
            "parent_outcome": payload.get("outcome"),
        },
    }, notes


def _economics(
    executions: Sequence[Mapping[str, Any]],
    fills: Sequence[Mapping[str, Any]],
    evidence_issues: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Prove fees from exact individual receipts, including explicit zero fills."""
    missing: list[str] = []
    totals: list[str] = []
    if evidence_issues or not executions or any(not item.get("resolved") for item in executions):
        missing.append("execution history or receipt binding is unresolved")
    for execution in executions:
        trades = execution.get("trades", [])
        fees = [_decimal_value(trade.get("fee")) for trade in trades]
        if any(fee is None for fee in fees):
            missing.append(f"{execution.get('phase')} {execution.get('leg')}: trade fee is missing or non-finite")
            continue
        total = sum((fee for fee in fees if fee is not None), Decimal(0))
        recorded = execution.get("fee_total")
        if recorded is not None and _decimal_value(recorded) != total:
            missing.append(f"{execution.get('phase')} {execution.get('leg')}: receipt fee total contradicts individual trades")
        totals.append(str(total))
    proven = not missing
    return {
        "fees": {
            "status": "PROVEN" if proven else "UNKNOWN", "complete": proven,
            "reason": "complete individual fee receipts or resolved no-fill legs" if proven else "fee completeness is not proven",
            "missing_evidence": list(dict.fromkeys(missing)), "known_fee_totals": totals,
            "total": str(sum((Decimal(t) for t in totals), Decimal(0))) if proven else None,
        },
        "funding_pnl": {"status": "UNKNOWN", "reason": "saved cycle journals do not independently attribute funding or closed PnL"},
    }


def _paired_execution(
    actions: Sequence[Mapping[str, Any]],
    fills: Sequence[Mapping[str, Any]],
    executions: Sequence[Mapping[str, Any]],
    evidence_issues: Sequence[Mapping[str, Any]],
    *,
    parent_payload: Mapping[str, Any] | None = None,
    parent_complete: bool = False,
) -> dict[str, Any]:
    """Separate completed two-phase exposure from optional mutual trade matching."""
    parent_payload = parent_payload or {}
    explicit = parent_payload.get("paired_execution")
    receiver_actions = [a for a in actions if a.get("leg") == "receiver"]
    receiver_dispatched = any(a.get("dispatched") is True for a in receiver_actions)
    source_fills = [f for f in fills if f.get("leg") == "source"]
    receiver_fills = [f for f in fills if f.get("leg") == "receiver"]
    phase_results: list[dict[str, Any]] = []
    mutual_results: list[dict[str, Any]] = []
    for phase in ("opening", "closing"):
        legs = [e for e in executions if e.get("phase") == phase]
        positive = [e for e in legs if (_decimal_value(e.get("filled_quantity")) or Decimal(0)) > 0]
        source = [e for e in positive if e.get("leg") == "source"]
        receiver = [e for e in positive if e.get("leg") == "receiver"]
        valid = bool(legs) and all(e.get("resolved") for e in legs) and len(source) == len(receiver) == 1
        quantity: Decimal | None = None
        if valid:
            a, b = source[0], receiver[0]
            quantity = _decimal_value(a.get("filled_quantity"))
            valid = (
                quantity == _decimal_value(b.get("filled_quantity"))
                and quantity == _decimal_value(a["plan"].get("quantity")) == _decimal_value(b["plan"].get("quantity"))
                and a.get("account_index") != b.get("account_index")
                and a.get("market_id") == b.get("market_id")
                and a["plan"].get("side") != b["plan"].get("side")
                and all(e.get("outcome") == "SUCCESS" for e in (a, b))
            )
        phase_results.append({"phase": phase, "status": "PROVEN" if valid else "NOT_PROVEN", "quantity": str(quantity) if valid else None,
                              "reason": "complete planned exposure with terminal orders and exact position deltas" if valid else "both complete planned legs are required"})
        mutual = "UNKNOWN"
        if valid:
            a, b = source[0], receiver[0]
            receiver_trades = {str(t["trade_id"]): t for t in b["trades"]}
            matched = len(a["trades"]) == len(receiver_trades)
            foreign = False
            for t in a["trades"]:
                other = receiver_trades.get(str(t["trade_id"]))
                if t.get("counterparty_account_index") not in (None, b["account_index"]):
                    foreign = True
                if other is None:
                    matched = False
                    continue
                matched = matched and (
                    t.get("counterparty_account_index") == b["account_index"]
                    and other.get("counterparty_account_index") == a["account_index"]
                    and t.get("counterparty_order_id") == other["order_id"]
                    and other.get("counterparty_order_id") == t["order_id"]
                    and t["order_id"] != other["order_id"]
                    and t["side"] != other["side"]
                    and _decimal_value(t["quantity"]) == _decimal_value(other["quantity"])
                    and _decimal_value(t["price"]) == _decimal_value(other["price"])
                    and (t.get("counterparty_client_order_index") is None or str(t["counterparty_client_order_index"]) == str(other["client_order_index"]))
                    and (other.get("counterparty_client_order_index") is None or str(other["counterparty_client_order_index"]) == str(t["client_order_index"]))
                )
            mutual = "MATCHED" if matched else "NOT_MATCHED" if foreign else "UNKNOWN"
        mutual_results.append({"phase": phase, "status": mutual})
    reasons: list[str] = []
    unresolved = bool(evidence_issues) or not parent_complete or not executions or any(not e.get("resolved") for e in executions)
    all_phases = all(p["status"] == "PROVEN" for p in phase_results)
    fallback = any(e.get("fallback") for e in executions)
    if unresolved:
        status = "UNKNOWN"
        reasons.append("complete, consistent terminal execution evidence is required")
    elif all_phases and phase_results[0]["quantity"] == phase_results[1]["quantity"] and not fallback and parent_payload.get("outcome") == "SUCCESS":
        status = "SUCCESS"
        reasons.append("opening and closing exposure are independently proven; direct counterparty matching is separate")
    elif receiver_fills and not source_fills or not receiver_dispatched and not receiver_fills:
        status = "FAILED"
        reasons.append("the intended paired cycle did not execute both required legs")
    else:
        status = "PARTIAL" if parent_payload.get("outcome") in {"SUCCESS", "PARTIAL"} and receiver_fills and source_fills else "UNKNOWN"
        reasons.append("a complete paired opening and closing without fallback is not proven")
    return {
        "status": status, "explicit_status": explicit,
        "receiver_possible_dispatch": bool(receiver_actions),
        "receiver_response_observed": any(a.get("response_observed") is True for a in receiver_actions),
        "receiver_dispatched": receiver_dispatched, "receiver_fill_observed": bool(receiver_fills),
        "source_confirmed_fill_count": len(source_fills), "receiver_confirmed_fill_count": len(receiver_fills),
        "guard_statuses": list(dict.fromkeys(str(e["guard_status"]) for e in executions if e.get("guard_status"))),
        "phase_proof": phase_results, "direct_counterparty_match": mutual_results, "reasons": reasons,
    }


def _regression_signals(files: Sequence[_FileData], issues: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    events: set[str] = set()
    reasons: list[str] = []
    typed_orders: list[tuple[str, Mapping[str, Any]]] = []
    incomplete_history: list[str] = []
    source_disappearance: list[str] = []
    public_ownership: list[str] = []
    external_fill: list[str] = []
    for data in files:
        for record in data.records:
            events.add(record.event)
            values = _reason_values(record.payload)
            reasons.extend(values)
            lowered = " ".join(values).lower()
            if record.event in {"SOURCE_ORDER_DISAPPEARED", "SOURCE_DISAPPEARED"} or "source order disappeared" in lowered or "source order identity or status is unresolved" in lowered:
                source_disappearance.append(record.event)
            if record.event == "PRE_RECEIVER_GUARD":
                public_level = record.payload.get("source_public_level")
                if public_level is None or "owner" in lowered and ("absent" in lowered or "unproved" in lowered or "does not prove" in lowered):
                    public_ownership.append(record.event)
            if record.event in {"SOURCE_FILLED_BEFORE_RECEIVER", "SOURCE_FILL_BEFORE_RECEIVER"} or "source filled before receiver" in lowered or "source fill observed before receiver" in lowered:
                external_fill.append(record.event)
            if record.payload.get("history_complete") is False:
                incomplete_history.append(record.event)
            if record.event == "COMPLETE":
                receipt = _receipt_from_complete(record)
                if isinstance(receipt, Mapping):
                    for leg in ("source", "receiver"):
                        value = receipt.get(leg)
                        if isinstance(value, Mapping):
                            filled = _decimal_value(value.get("filled_quantity"))
                            if filled is not None and filled > 0 and value.get("history_complete") is not True:
                                incomplete_history.append(f"{record.event}:{leg}")
            for event_key in ("order",):
                order = record.payload.get(event_key)
                if isinstance(order, Mapping):
                    typed_orders.append((record.event, order))
            if record.event in {"LEG_RECONCILED", "COMPLETE"}:
                receipt = _receipt_from_complete(record) if record.event == "COMPLETE" else None
                if isinstance(receipt, Mapping):
                    for leg in ("source", "receiver"):
                        value = receipt.get(leg)
                        if isinstance(value, Mapping) and isinstance(value.get("order"), Mapping):
                            typed_orders.append((record.event, value["order"]))
            if record.event in {"FALLBACK_ATTEMPT_EVIDENCE", "FALLBACK_RECONCILED"}:
                order = record.payload.get("order")
                if isinstance(order, Mapping):
                    typed_orders.append((record.event, order))

    def signal(name: str, observed: bool, evidence: list[str]) -> dict[str, Any]:
        return {"status": "OBSERVED" if observed else "NOT_OBSERVED", "evidence": list(dict.fromkeys(evidence))[:20]}

    canceled_ioc: list[str] = []
    canceled_limit: list[str] = []
    for event, order in typed_orders:
        status = str(order.get("status", "")).lower()
        filled = _decimal_value(order.get("filled_quantity"))
        tif = str(order.get("time_in_force", "")).upper()
        if status in {"canceled", "cancelled", "canceled-post-only", "canceled-too-much-slippage", "canceled-not-enough-liquidity"} and filled == Decimal(0):
            if tif == "IOC":
                canceled_ioc.append(event)
            elif tif:
                canceled_limit.append(event)

    transport = [event for event in events if event.endswith("_DISPATCH_UNKNOWN")]
    issue_codes = [str(issue.get("code")) for issue in issues]

    return {
        "source_disappearance": signal(
            "source_disappearance",
            bool(source_disappearance),
            source_disappearance,
        ),
        "public_ownership_absent": signal(
            "public_ownership_absent",
            bool(public_ownership),
            public_ownership,
        ),
        "external_source_fill_before_receiver": signal(
            "external_source_fill_before_receiver",
            bool(external_fill),
            external_fill,
        ),
        "delayed_or_incomplete_history": signal(
            "delayed_or_incomplete_history",
            bool(incomplete_history) or any(issue.get("code") == "INCOMPLETE_TRADE_HISTORY" for issue in issues),
            incomplete_history + ["INCOMPLETE_TRADE_HISTORY" for issue in issues if issue.get("code") == "INCOMPLETE_TRADE_HISTORY"],
        ),
        "canceled_zero_fill_ioc": signal(
            "canceled_zero_fill_ioc",
            bool(canceled_ioc),
            canceled_ioc,
        ),
        "canceled_zero_fill_limit": signal(
            "canceled_zero_fill_limit",
            bool(canceled_limit),
            canceled_limit,
        ),
        "ambiguous_transport_after_send": signal(
            "ambiguous_transport_after_send",
            bool(transport) or any(issue.get("code") == "ORPHAN_DISPATCH_RESULT" for issue in issues),
            transport + ["ORPHAN_DISPATCH_RESULT" for issue in issues if issue.get("code") == "ORPHAN_DISPATCH_RESULT"],
        ),
        "incomplete_input": signal(
            "incomplete_input",
            bool(issues),
            issue_codes[:20],
        ),
    }


def _coverage_map() -> dict[str, Any]:
    """Name the existing behavioral evidence behind each offline projection."""

    return {
        "source_disappearance": {
            "signal": "source_disappearance",
            "tests": ["tests/spread_shadow/test_hood_handoff_corrections.py::test_foreign_active_order_seen_then_absent_remains_a_barrier"],
        },
        "public_ownership_absent": {
            "signal": "public_ownership_absent",
            "tests": ["tests/spread_shadow/test_hood_handoff_paired_opening.py::test_paired_guard_rejects_incomplete_or_ambiguous_source_public_evidence"],
        },
        "external_source_fill_before_receiver": {
            "signal": "external_source_fill_before_receiver",
            "tests": ["tests/spread_shadow/test_hood_handoff_paired_opening.py::test_source_fill_before_receiver_stops_second_leg"],
        },
        "delayed_or_incomplete_history": {
            "signal": "delayed_or_incomplete_history",
            "tests": ["tests/spread_shadow/test_hood_handoff_corrections.py::test_permanent_empty_history_remains_unknown_without_fabricated_fill"],
        },
        "canceled_zero_fill_ioc": {
            "signal": "canceled_zero_fill_ioc",
            "tests": ["tests/spread_shadow/test_hood_handoff_engine.py::test_terminal_receiver_no_fill_is_partial_and_not_success"],
        },
        "canceled_zero_fill_limit": {
            "signal": "canceled_zero_fill_limit",
            "tests": ["tests/spread_shadow/test_hood_handoff_random_cycle.py::test_canceled_post_only_zero_fill_retries_opening_with_preserved_guard_evidence"],
        },
        "ambiguous_transport_no_replay": {
            "signal": "ambiguous_transport_after_send",
            "tests": [
                "tests/spread_shadow/test_hood_handoff_sdk_audit.py::test_sdk_transport_timeout_reaches_handoff_unknown_barrier",
                "tests/spread_shadow/test_hood_handoff_paired_opening.py::test_paired_restart_and_mode_mismatch_never_replay_or_cancel",
                "tests/spread_shadow/test_hood_handoff_random_cycle.py::test_cycle_interruption_leaves_consumed_journal_and_never_replays",
            ],
        },
        "exact_flat_inventory": {
            "projection": "inventory",
            "tests": ["tests/spread_shadow/test_hood_handoff_random_cycle.py::test_inventory_flatness_requires_causal_terminal_evidence_but_not_fee_evidence"],
        },
        "fee_complete_and_missing": {
            "projection": "economics.fees",
            "tests": [
                "tests/spread_shadow/test_hood_handoff_corrections.py::test_actual_fee_economics_are_reported_without_cap_admission",
                "tests/spread_shadow/test_hood_handoff_corrections.py::test_missing_fee_is_unknown_economics_without_erasing_fill_quantity",
            ],
        },
        "crash_boundaries": {
            "tests": [
                "tests/spread_shadow/test_hood_handoff_offline_report.py::test_engine_crash_boundaries_preserve_unknown_and_never_replay",
                "tests/spread_shadow/test_hood_handoff_paired_opening.py::test_paired_restart_and_mode_mismatch_never_replay_or_cancel",
                "tests/spread_shadow/test_hood_handoff_random_cycle.py::test_cycle_interruption_leaves_consumed_journal_and_never_replays",
            ],
            "required_observations": ["send count", "cancel count", "inventory UNKNOWN at unresolved restart boundary"],
        },
    }


def load_saved_cycle_report(path: str | Path) -> dict[str, Any]:
    """Read one saved cycle directory and return a machine-readable report."""

    input_path = Path(path)
    root, discovered, discovery_issues = _discover_files(input_path)
    files = [_read_jsonl(item, kind) for item, kind in discovered]
    issues: list[dict[str, Any]] = [*discovery_issues]
    for data in files:
        issues.extend(data.issues)
    cycle = next((data for data in files if data.kind == "cycle"), None)

    referenced_paths: set[str] = set()

    def collect_references(value: Any) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                if key == "journal_path" and isinstance(item, str):
                    referenced_paths.add(Path(item).name)
                collect_references(item)
        elif isinstance(value, list):
            for item in value:
                collect_references(item)

    if cycle is not None:
        for record in cycle.records:
            collect_references(record.payload)
    discovered_names = {data.path.name for data in files}
    for name in sorted(referenced_paths - discovered_names):
        missing = root.parent / name
        issue = {
            "code": "REFERENCED_CHILD_MISSING",
            "message": "parent journal references a child journal that was not found",
            "path": str(missing),
        }
        issues.append(issue)

    # A durable parent completion record does not make an unfinished child
    # trustworthy.  The child may have persisted an intent and then crashed
    # before its terminal reconciliation record; surface that boundary even
    # when the parent file itself is syntactically complete.
    child_terminal_events = {"COMPLETE", "PREFLIGHT_BLOCKED", "PREVIEW", "EXECUTION_UNKNOWN"}
    for data in files:
        if data.kind not in {"opening", "closing"} or not data.records:
            continue
        if any(record.event in child_terminal_events for record in data.records):
            if data.records[-1].event not in child_terminal_events:
                issues.append(_issue("POST_TERMINAL_RECORD", "child has records after its terminal result", data=data))
            continue
        if data.records:
            issues.append(
                {
                    "code": "CHILD_TERMINAL_MISSING",
                    "message": "child journal has no durable terminal result",
                    "path": str(data.path),
                }
            )

    all_files = files
    progression = _event_progression(all_files)
    planned = _plan_records(all_files)
    actions = _intent_actions(all_files)
    for data in all_files:
        for issue in data.issues:
            if issue not in issues:
                issues.append(issue)
    issues.extend(_semantic_issues(all_files))
    fills, executions, fill_issues = _fill_records(all_files)
    issues.extend(fill_issues)
    terminal_orders = _terminal_order_records(all_files)
    latency, latency_notes, latency_issues = _latency_reports(all_files)
    issues.extend(latency_issues)

    cycle_complete = _last_record(cycle, "CYCLE_COMPLETE") if cycle else None
    interrupted = _last_record(cycle, "CYCLE_INTERRUPTED") if cycle else None
    unknown_terminal = _last_record(cycle, "CYCLE_EXECUTION_UNKNOWN") if cycle else None
    preflight_blocked = _last_record(cycle, "CYCLE_PREFLIGHT_BLOCKED") if cycle else None
    terminal = cycle_complete is not None
    cycle_payload = {} if cycle_complete is None else cycle_complete.payload
    opening = cycle_payload.get("opening") if isinstance(cycle_payload.get("opening"), Mapping) else {}
    closing = cycle_payload.get("closing") if isinstance(cycle_payload.get("closing"), Mapping) else {}
    parent_outcome = cycle_payload.get("outcome")
    if isinstance(parent_outcome, str) and parent_outcome.strip():
        outcome = parent_outcome
        outcome_source = "parent_terminal_outcome"
    elif isinstance(cycle_payload.get("closing"), Mapping) and isinstance(closing.get("outcome"), str):
        outcome = closing.get("outcome")
        outcome_source = "closing_child_outcome"
    elif isinstance(cycle_payload.get("opening"), Mapping) and isinstance(opening.get("outcome"), str):
        outcome = opening.get("outcome")
        outcome_source = "opening_child_outcome"
    else:
        outcome = "UNKNOWN" if terminal else "INCOMPLETE"
        outcome_source = "missing_terminal_outcome" if terminal else "no_terminal_record"
    child_outcomes: dict[str, Any] = {
        "opening": opening.get("outcome") if isinstance(opening, Mapping) else None,
        "closing": closing.get("outcome") if isinstance(closing, Mapping) else None,
    }
    reason_values: list[str] = []
    for data in all_files:
        for record in data.records:
            reason_values.extend(_reason_values(record.payload))
    if interrupted is not None:
        reason_values.append("journal records process/operator interruption")
    if unknown_terminal is not None:
        reason_values.append("journal records an execution-unknown terminal boundary")
    if preflight_blocked is not None:
        reason_values.append("cycle preflight was blocked before completion")
    reasons = list(dict.fromkeys(reason_values))

    if not terminal:
        issues.append(_issue("CYCLE_TERMINAL_MISSING", "cycle has no durable completion"))
    if cycle and _last_record(cycle, "CYCLE_COMPLETE") is not None and cycle.records[-1].event != "CYCLE_COMPLETE":
        issues.append(_issue("POST_TERMINAL_RECORD", "cycle contains evidence after its terminal record", data=cycle))
    if cycle and len(_records(cycle, "CYCLE_COMPLETE")) > 1:
        issues.append(_issue("CONFLICTING_TERMINAL", "cycle has multiple terminal records", data=cycle))
    for action in actions:
        expected_leg = "source" if action["leg"] == "cancel" else action["leg"]
        matching = [e for e in executions if e["source_file"] == action["source_file"] and e["leg"] == expected_leg
                    and (action.get("attempt") is None or e.get("attempt") == action["attempt"])
                    and e["observed_at"] >= action["intent_at"]]
        if action["leg"] != "cancel" and any(e["resolved"] and e["dispatched"] for e in matching):
            action["dispatched"] = True
            action["dispatch_proof"] = "terminal order and complete bound history"
        if not matching or not all(e["resolved"] for e in matching):
            issues.append(_issue("UNRESOLVED_INTENT", "mutation intent has no resolved terminal execution", phase=action["phase"], leg=action["leg"]))
    latest_positions: dict[str, Decimal] = {}
    for execution in sorted(executions, key=lambda e: (e["observed_at"], {"opening": 0, "closing": 1, "fallback": 2}[e["phase"]], e.get("attempt") or 0)):
        account = str(execution["account_index"])
        before = _decimal_value(execution.get("position_before"))
        after = _decimal_value(execution.get("position_after"))
        if account in latest_positions and before != latest_positions[account]:
            issues.append(_issue("CONFLICTING_POSITION_CHAIN", "an execution does not start at the preceding proved account position", account_index=account))
        if after is not None:
            latest_positions[account] = after
    if cycle and any(e["phase"] == "closing" for e in executions):
        hold = _first_record(cycle, "HOLD_ANCHORED")
        closing_plan = _first_record(cycle, "CLOSING_PLAN_READY")
        hold_seconds = _finite_number(hold.payload.get("hold_seconds")) if hold else None
        opening_end = max((e["observed_at"] for e in executions if e["phase"] == "opening"), default=None)
        if hold is None or closing_plan is None or hold_seconds is None or hold_seconds < 0 or opening_end is None or hold["at"] < opening_end or closing_plan["at"] - hold["at"] < hold_seconds:
            issues.append(_issue("INCOMPLETE_HOLD_EVIDENCE", "closing lacks a causally complete persisted hold interval"))
    inventory, inventory_notes = _positions_from_cycle(cycle, all_files, executions, issues)
    economics = _economics(executions, fills, issues)
    paired = _paired_execution(
        actions,
        fills,
        executions,
        issues,
        parent_payload=cycle_payload,
        parent_complete=terminal,
    )

    if paired["status"] == "SUCCESS" and inventory["status"] != "CONFIRMED_FLAT":
        paired["status"] = "UNKNOWN"
        paired["reasons"].append("complete cycle success requires causally proved final flat inventory")

    binding: dict[str, Any] = {}
    for record in (cycle.records if cycle else []):
        candidate = record.payload.get("binding")
        if isinstance(candidate, Mapping):
            binding = dict(_copy_json(candidate))
            break
    if not binding:
        for data in all_files:
            for record in data.records:
                candidate = record.payload.get("binding")
                if isinstance(candidate, Mapping):
                    binding = dict(_copy_json(candidate))
                    break
            if binding:
                break

    source_files = [data.as_dict() for data in all_files]
    report: dict[str, Any] = {
        "schema": REPORT_SCHEMA,
        "status": "COMPLETE" if terminal and not issues else "INCOMPLETE",
        "input_path": str(input_path.expanduser()),
        "cycle_path": str(root),
        "source_files": source_files,
        "issues": issues,
        "cycle": {
            "run_id": None if cycle is None else cycle.run_id,
            "terminal": terminal,
            "terminal_event": None if cycle_complete is None else cycle_complete.event,
            "terminal_at": None if cycle_complete is None else cycle_complete["at"],
            "outcome": outcome,
            "outcome_source": outcome_source,
            "child_outcomes": child_outcomes,
            "reason": None if not reasons else reasons[0],
            "process_exit_ignored": True,
        },
        "binding": binding,
        "progression": progression,
        "progression_total_events": sum(data.record_count for data in all_files),
        "progression_detail_truncated": any(data.detail_truncated for data in all_files),
        "planned_actions": planned,
        "dispatched_actions": actions,
        "confirmed_fills": fills,
        "terminal_orders": terminal_orders,
        "paired_execution": paired,
        "inventory": inventory,
        "economics": economics,
        "latency": latency,
        "reasons": reasons,
        "regression_signals": _regression_signals(all_files, issues),
        "coverage": _coverage_map(),
        "source_provenance": {
            "inputs_are_read_only": True,
            "files": [
                {
                    "path": item["path"],
                    "kind": item["kind"],
                    "sha256": item["sha256"],
                    "line_count": item["line_count"],
                    "record_count": item["record_count"],
                }
                for item in source_files
            ],
        },
        "report_notes": list(
            dict.fromkeys(
                [
                    "Process exit status is not treated as a fill, terminal journal record, or flat-inventory proof.",
                    "A dispatch response accepted by the venue is not itself a confirmed fill; only persisted trade receipts contribute to confirmed_fills.",
                    "Input payload detail is sanitized, allowlisted, and bounded; line counts, event counts, and SHA-256 hashes cover the complete read-only sources.",
                    *latency_notes,
                    *inventory_notes,
                ]
            )
        ),
    }
    return report


def report_saved_paths(paths: Iterable[str | Path]) -> list[dict[str, Any]]:
    """Build one report per input path without combining independent cycles."""

    return [load_saved_cycle_report(path) for path in paths]


def build_cycle_report(path: str | Path) -> dict[str, Any]:
    """Compatibility alias for callers that prefer a builder verb."""

    return load_saved_cycle_report(path)


def report_saved_cycle(path: str | Path) -> dict[str, Any]:
    """Compatibility alias used by offline operator scripts."""

    return load_saved_cycle_report(path)


def _display_number(value: Any) -> str:
    if value is None:
        return "UNKNOWN"
    if isinstance(value, float):
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value)


def render_human(report: Mapping[str, Any]) -> str:
    """Render the conservative report in concise Russian for an operator."""

    cycle = report.get("cycle") if isinstance(report.get("cycle"), Mapping) else {}
    inventory = report.get("inventory") if isinstance(report.get("inventory"), Mapping) else {}
    paired = report.get("paired_execution") if isinstance(report.get("paired_execution"), Mapping) else {}
    economics = report.get("economics") if isinstance(report.get("economics"), Mapping) else {}
    fees = economics.get("fees") if isinstance(economics.get("fees"), Mapping) else {}
    fills = report.get("confirmed_fills") if isinstance(report.get("confirmed_fills"), list) else []
    actions = report.get("dispatched_actions") if isinstance(report.get("dispatched_actions"), list) else []
    planned = report.get("planned_actions") if isinstance(report.get("planned_actions"), list) else []
    issues = report.get("issues") if isinstance(report.get("issues"), list) else []
    lines = [
        "HCR-27 offline report",
        f"Статус отчёта: {report.get('status', 'UNKNOWN')}; цикл: {cycle.get('outcome', 'UNKNOWN')}; terminal={cycle.get('terminal', False)}.",
        f"Путь: {report.get('cycle_path', report.get('input_path', 'UNKNOWN'))}.",
        f"Планы: {len(planned)}; mutation intents: {len(actions)}; подтверждённые fills: {len(fills)}.",
        f"Paired execution: {paired.get('status', 'UNKNOWN')}; receiver dispatched={paired.get('receiver_dispatched', False)}.",
        f"Inventory: {inventory.get('status', 'UNKNOWN')} — source={_display_number(inventory.get('source'))}, receiver={_display_number(inventory.get('receiver'))}.",
        f"Fees: {fees.get('status', 'UNKNOWN')}; funding/PnL: UNKNOWN (отдельная классификация).",
    ]
    if report.get("progression_detail_truncated"):
        lines.append(f"Детали ограничены: списки действий и fills могут быть неполными; всего записей {report.get('progression_total_events')}.")
    if issues:
        lines.append(f"Проблемы входа: {len(issues)}; результат нельзя считать полным.")
    reasons = report.get("reasons")
    if isinstance(reasons, list) and reasons:
        lines.append("Причины: " + "; ".join(str(item) for item in reasons[:5]) + ".")
    latency = report.get("latency")
    if isinstance(latency, Mapping):
        for phase in ("opening", "closing", "fallback"):
            items = latency.get(phase)
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, Mapping):
                    continue
                available = []
                for key in ("quote_age", "preparation", "source_dispatch_ack", "source_visibility", "receiver_admission", "receiver_dispatch_ack", "receiver_visibility", "reconciliation"):
                    seconds = item.get(f"{key}_seconds")
                    if seconds is not None:
                        available.append(f"{key}={_display_number(seconds)}s")
                if available:
                    lines.append(f"Latency {phase} attempt {item.get('attempt', '?')}: " + ", ".join(available) + ".")
    notes = report.get("report_notes")
    if isinstance(notes, list) and notes:
        lines.append("Ограничение: " + str(notes[0]))
    return "\n".join(lines)


def format_report(report: Mapping[str, Any], *, output_format: str = "human") -> str:
    if output_format == "json":
        return json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    if output_format == "both":
        return render_human(report) + "\n" + json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return render_human(report)


__all__ = [
    "REPORT_SCHEMA",
    "REPORT_BATCH_SCHEMA",
    "OfflineReportError",
    "build_cycle_report",
    "format_report",
    "load_saved_cycle_report",
    "render_human",
    "report_saved_cycle",
    "report_saved_paths",
]
