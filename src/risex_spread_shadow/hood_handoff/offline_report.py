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
from pathlib import Path
import re
from typing import Any, Iterable, Mapping, Sequence


REPORT_SCHEMA = "hcr-27-offline-cycle-report-v1"
REPORT_BATCH_SCHEMA = "hcr-27-offline-cycle-report-batch-v1"


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
        self.records: list[_Record] = []
        self.issues: list[dict[str, Any]] = []
        self.line_count = 0
        self.sha256 = hashlib.sha256()
        self.run_id: str | None = None
        self.missing = False
        self.last_sequence: int | None = None

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
        self.issues.append(item)

    def as_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "path": str(self.path),
            "kind": self.kind,
            "status": self.status,
            "line_count": self.line_count,
            "record_count": len(self.records),
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


def _copy_json(value: Any) -> Any:
    """Copy only JSON values; journal input is already JSON but may be hostile."""

    if isinstance(value, Mapping):
        return {str(key): _copy_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_copy_json(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


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
                data.records.append(
                    _Record(
                        sequence=sequence,
                        run_id=run_id,
                        event=event,
                        at=at_number,
                        payload=dict(_copy_json(payload)),
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
        if data.kind not in {"opening", "closing"}:
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
                            "side": (
                                config.get("direction")
                                if leg == "source"
                                else config.get("direction")
                            ),
                            "quantity": config.get("quantity", payload.get("paired_quantity")),
                            "operation_mode": config.get("operation_mode"),
                        }
                        if any(value is not None for value in plan.values()):
                            planned.append(
                                {
                                    "phase": "opening" if record.event.startswith("OPENING") else "closing",
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
                    "status": "UNKNOWN_INCOMPLETE",
                    "accepted": None,
                    "response_observed": False,
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
                    if item["leg"] == leg and not item["response_observed"]
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
            candidate["response_observed"] = True
            candidate["response_at"] = record["at"]
            candidate["response_event"] = event
            if event.endswith("_UNKNOWN"):
                candidate["status"] = "DISPATCHED_UNKNOWN"
                candidate["accepted"] = None
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
                if payload.get("error") is not None:
                    candidate["reason"] = payload.get("error")
            if isinstance(payload.get("order_id"), (str, int)):
                candidate["order_id"] = str(payload["order_id"])
        for action in pending:
            if not action["response_observed"]:
                action["status"] = "INTENT_ONLY_UNFINISHED"
                action["reason"] = "journal ended before a matching dispatch result or unknown record"
    return actions


def _receipt_from_complete(record: _Record) -> Mapping[str, Any] | None:
    payload = record.payload
    receipt = payload.get("receipt")
    if isinstance(receipt, Mapping):
        return receipt
    return payload if isinstance(payload.get("source"), Mapping) or isinstance(payload.get("receiver"), Mapping) else None


def _fill_records(files: Sequence[_FileData]) -> list[dict[str, Any]]:
    fills: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()

    def add_trades(
        phase: str,
        attempt: int | None,
        leg: str,
        trades: Any,
        data: _FileData,
        record: _Record,
    ) -> None:
        if not isinstance(trades, list):
            return
        for trade in trades:
            if not isinstance(trade, Mapping):
                continue
            quantity = _finite_number(trade.get("quantity"))
            if quantity is None or quantity <= 0:
                continue
            trade_id = str(trade.get("trade_id") or "")
            order_id = str(trade.get("order_id") or "")
            key = (phase, leg, trade_id or order_id or f"line-{record.get('line')}-{len(fills)}")
            if key in seen:
                continue
            seen.add(key)
            fills.append(
                {
                    "phase": phase,
                    "attempt": attempt,
                    "leg": leg,
                    "trade_id": trade.get("trade_id"),
                    "order_id": trade.get("order_id"),
                    "account_index": trade.get("account_index"),
                    "quantity": trade.get("quantity"),
                    "price": trade.get("price"),
                    "fee": trade.get("fee"),
                    "observed_at": trade.get("observed_at"),
                    "source_file": str(data.path),
                    "source_event": record.event,
                    "line": record.get("line"),
                }
            )

    for data in files:
        if data.kind in {"opening", "closing"}:
            for record in data.records:
                if record.event != "COMPLETE":
                    continue
                receipt = _receipt_from_complete(record)
                if receipt is None:
                    continue
                attempt = _attempt_from(record.payload, receipt.get("attempt_index"))
                for leg in ("source", "receiver"):
                    value = receipt.get(leg)
                    if isinstance(value, Mapping):
                        add_trades(data.kind, attempt, leg, value.get("trades"), data, record)
        elif data.kind == "cycle":
            for record in data.records:
                if record.event != "FALLBACK_ATTEMPT_EVIDENCE":
                    continue
                add_trades("fallback", _attempt_from(record.payload), "fallback", record.payload.get("trades"), data, record)
    return fills


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


def _duration(start: float | None, end: float | None) -> float | None:
    if start is None or end is None:
        return None
    return max(0.0, end - start)


def _latency_measure(
    name: str,
    value: Any,
    *,
    basis: str | None = None,
    unavailable_reason: str | None = None,
    overlap: bool = False,
) -> dict[str, Any]:
    number = _finite_number(value)
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


def _latency_reports(files: Sequence[_FileData]) -> tuple[dict[str, Any], list[str]]:
    reports: dict[str, list[dict[str, Any]]] = {"opening": [], "closing": [], "fallback": []}
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
        values["quote_age"] = _latency_measure("quote_age", quote_age, basis="latency.source_quote_age_seconds")
        preparation = latency.get("paired_preparation_seconds")
        if preparation is None and plan_record is not None and source_intent is not None:
            preparation = _duration(plan_record["at"], source_intent["at"])
            values["preparation"] = _latency_measure(
                "preparation", preparation, basis="PLAN_READY to SOURCE_DISPATCH_INTENT journal interval"
            )
        else:
            values["preparation"] = _latency_measure("preparation", preparation, basis="latency.paired_preparation_seconds")
        source_ack = latency.get("source_submit_ack_seconds")
        if source_ack is None and source_intent is not None and source_result is not None:
            source_ack = _duration(source_intent["at"], source_result["at"])
        values["source_dispatch_ack"] = (
            _latency_unknown(
                "source_dispatch_ack",
                basis="SOURCE_DISPATCH_UNKNOWN",
                reason="source transport outcome is unknown after the durable intent",
            )
            if source_dispatch_unknown
            else _latency_measure(
                "source_dispatch_ack", source_ack, basis="latency.source_submit_ack_seconds or intent/result journal interval"
            )
        )
        source_visibility = latency.get("source_visibility_seconds")
        if source_visibility is None and source_result is not None and source_observed is not None:
            source_visibility = _duration(source_result["at"], source_observed["at"])
        values["source_visibility"] = _latency_measure(
            "source_visibility", source_visibility, basis="latency.source_visibility_seconds or result/observation interval"
        )
        receiver_admission = latency.get("receiver_admission_seconds")
        if receiver_admission is None and guard is not None:
            gp = guard.payload
            receiver_admission = _duration(
                _finite_number(gp.get("request_started_at")),
                _finite_number(gp.get("request_finished_at")),
            )
        values["receiver_admission"] = _latency_measure(
            "receiver_admission",
            receiver_admission,
            basis="latency.receiver_admission_seconds or PRE_RECEIVER_GUARD request interval",
            unavailable_reason="no complete PRE_RECEIVER_GUARD request interval was persisted",
        )
        receiver_ack = latency.get("receiver_submit_ack_seconds")
        if receiver_ack is None and receiver_intent is not None and receiver_result is not None:
            receiver_ack = _duration(receiver_intent["at"], receiver_result["at"])
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
            )
        )
        receiver_visibility = latency.get("receiver_visibility_seconds", latency.get("receiver_fill_observation_seconds"))
        if receiver_visibility is None and receiver_result is not None and receiver_observed is not None:
            receiver_visibility = _duration(receiver_result["at"], receiver_observed["at"])
        values["receiver_visibility"] = _latency_measure(
            "receiver_visibility",
            receiver_visibility,
            basis="latency.receiver_fill_observation_seconds or result/observation interval",
            unavailable_reason="receiver order was not dispatched or terminal observation is absent",
        )
        reconciliation = latency.get("reconciliation_seconds")
        values["reconciliation"] = _latency_measure(
            "reconciliation",
            reconciliation,
            basis="latency.reconciliation_seconds",
            unavailable_reason="COMPLETE latency did not contain reconciliation_seconds",
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
                {"phase": "selection", "start_at": first["at"], "end_at": selection["at"], "seconds": _duration(first["at"], selection["at"]), "basis": "CYCLE_STARTED to SELECTION_PROVED"}
            )
        if selection and boundary:
            cycle_latency["phase_intervals"].append(
                {"phase": "preparation", "start_at": selection["at"], "end_at": boundary["at"], "seconds": _duration(selection["at"], boundary["at"]), "basis": "SELECTION_PROVED to FIRST_MUTATION_BOUNDARY; retries are one interval"}
            )
        hold = _first_record(cycle_data, "HOLD_ANCHORED")
        closing_plan = _first_record(cycle_data, "CLOSING_PLAN_READY")
        closing_complete = _last_record(cycle_data, "CLOSING_COMPLETE")
        if hold and closing_plan:
            cycle_latency["phase_intervals"].append(
                {"phase": "hold", "start_at": hold["at"], "end_at": closing_plan["at"], "seconds": _duration(hold["at"], closing_plan["at"]), "basis": "HOLD_ANCHORED to CLOSING_PLAN_READY"}
            )
        if closing_plan and closing_complete:
            cycle_latency["phase_intervals"].append(
                {"phase": "closing", "start_at": closing_plan["at"], "end_at": closing_complete["at"], "seconds": _duration(closing_plan["at"], closing_complete["at"]), "basis": "CLOSING_PLAN_READY to CLOSING_COMPLETE"}
            )
        fallback_intent = _first_record(cycle_data, "FALLBACK_DISPATCH_INTENT")
        fallback_end = None
        for event in ("FALLBACK_RECONCILED", "FALLBACK_ATTEMPT_EVIDENCE", "CYCLE_COMPLETE"):
            candidate = _last_record(cycle_data, event)
            if candidate is not None and (fallback_end is None or candidate["at"] > fallback_end["at"]):
                fallback_end = candidate
        if fallback_intent and fallback_end:
            cycle_latency["phase_intervals"].append(
                {"phase": "fallback", "start_at": fallback_intent["at"], "end_at": fallback_end["at"], "seconds": _duration(fallback_intent["at"], fallback_end["at"]), "basis": "first fallback intent to last durable fallback evidence"}
            )
        if complete is not None:
            cycle_latency["terminal_at"] = complete["at"]
    return {"cycle": cycle_latency, **reports}, notes


def _positions_from_cycle(cycle: _FileData | None) -> tuple[dict[str, Any], list[str]]:
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
        observations = [record for record in cycle.records if record.event == "FALLBACK_POST_ATTEMPT_ACCOUNT_OBSERVATION"]
        if observations:
            last = observations[-1].payload
            source = last.get("source") if isinstance(last.get("source"), Mapping) else {}
            receiver = last.get("receiver") if isinstance(last.get("receiver"), Mapping) else {}
            positions = {"source": source.get("signed_position"), "receiver": receiver.get("signed_position")}
            observed = {"source": source.get("observed_at"), "receiver": receiver.get("observed_at")}
        else:
            positions = {}
            observed = {}
    source = positions.get("source") if isinstance(positions, Mapping) else None
    receiver = positions.get("receiver") if isinstance(positions, Mapping) else None
    source_at = observed.get("source") if isinstance(observed, Mapping) else None
    receiver_at = observed.get("receiver") if isinstance(observed, Mapping) else None
    source_number = _finite_number(source)
    receiver_number = _finite_number(receiver)
    terminal_proof = complete is not None and isinstance(payload.get("remaining_positions"), Mapping)
    if not terminal_proof or source_at is None or receiver_at is None:
        status = "UNKNOWN"
        notes.append("flat-looking positions are not promoted without CYCLE_COMPLETE and both observation timestamps")
    elif source_number == 0 and receiver_number == 0:
        status = "CONFIRMED_FLAT"
    elif source_number is not None or receiver_number is not None:
        status = "OPEN_INVENTORY"
    else:
        status = "UNKNOWN"
    return {
        "status": status,
        "source": source,
        "receiver": receiver,
        "observed_at": {"source": source_at, "receiver": receiver_at},
        "proof": {
            "event": "CYCLE_COMPLETE" if terminal_proof else None,
            "source_position_present": source is not None,
            "receiver_position_present": receiver is not None,
            "observation_times_present": source_at is not None and receiver_at is not None,
        },
    }, notes


def _economics(files: Sequence[_FileData], fills: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    fee_unknown: list[str] = []
    fee_values: list[Any] = []
    proven_no_execution = False
    statuses: list[str] = []
    for data in files:
        for record in data.records:
            if record.event not in {"COMPLETE", "FALLBACK_ATTEMPT_EVIDENCE", "FALLBACK_RECONCILED", "CYCLE_COMPLETE"}:
                continue
            payload = record.payload
            for candidate in (payload, payload.get("receipt")):
                if not isinstance(candidate, Mapping):
                    continue
                status = candidate.get("economic_status")
                if isinstance(status, str):
                    statuses.append(status)
                if status == "PROVEN" and not fills:
                    proven_no_execution = True
                fee_total = candidate.get("fee_total")
                if fee_total is not None:
                    fee_values.append(fee_total)
                for leg in ("source", "receiver"):
                    value = candidate.get(leg)
                    if isinstance(value, Mapping):
                        if value.get("fee_total") is not None:
                            fee_values.append(value.get("fee_total"))
                        if value.get("fee_total") is None and _finite_number(value.get("filled_quantity")) not in (None, 0.0):
                            fee_unknown.append(f"{data.path}: {leg} fee_total is missing for a filled leg")
                        for trade in value.get("trades", ()) if isinstance(value.get("trades"), list) else ():
                            if isinstance(trade, Mapping) and trade.get("fee") is None:
                                fee_unknown.append(f"{data.path}: {leg} trade fee is missing")
                if isinstance(payload.get("trades"), list):
                    for trade in payload["trades"]:
                        if (
                            isinstance(trade, Mapping)
                            and trade.get("fee") is None
                            and _finite_number(trade.get("quantity")) not in (None, 0.0)
                        ):
                            fee_unknown.append(f"{data.path}: fallback trade fee is missing")
    if fee_unknown:
        fees = {
            "status": "UNKNOWN",
            "complete": False,
            "reason": "at least one confirmed execution has no fee value",
            "missing_evidence": list(dict.fromkeys(fee_unknown)),
            "known_fee_totals": fee_values,
        }
    elif proven_no_execution or (not fills and any(status == "PROVEN" for status in statuses)):
        fees = {
            "status": "PROVEN",
            "complete": True,
            "reason": "journal proves no execution fee was due for the recorded no-fill legs",
            "missing_evidence": [],
            "known_fee_totals": fee_values,
        }
    elif fills:
        fees = {
            "status": "UNKNOWN",
            "complete": False,
            "reason": "confirmed fills exist but complete fee evidence is absent",
            "missing_evidence": ["fee completeness is not proven for every confirmed fill"],
            "known_fee_totals": fee_values,
        }
    else:
        fees = {
            "status": "UNKNOWN",
            "complete": False,
            "reason": "journal does not prove fee completeness",
            "missing_evidence": ["no explicit economic proof"],
            "known_fee_totals": fee_values,
        }
    return {
        "fees": fees,
        "funding_pnl": {
            "status": "UNKNOWN",
            "reason": "saved cycle journals do not independently attribute funding or closed PnL",
        },
    }


def _paired_execution(
    files: Sequence[_FileData],
    actions: Sequence[Mapping[str, Any]],
    fills: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    cycle = next((item for item in files if item.kind == "cycle"), None)
    parent_complete = _last_record(cycle, "CYCLE_COMPLETE") if cycle else None
    parent_payload = {} if parent_complete is None else parent_complete.payload
    explicit: str | None = None
    reasons: list[str] = []
    for candidate in (parent_payload, parent_payload.get("opening"), parent_payload.get("closing")):
        if isinstance(candidate, Mapping):
            value = candidate.get("paired_execution")
            if isinstance(value, str):
                explicit = value
                break
            reasons.extend(_reason_values(candidate))
    receiver_actions = [item for item in actions if item.get("leg") == "receiver" and item.get("phase") in {"opening", "closing"}]
    receiver_dispatched = bool(receiver_actions)
    source_fills = [item for item in fills if item.get("leg") == "source" and item.get("phase") in {"opening", "closing"}]
    receiver_fills = [item for item in fills if item.get("leg") == "receiver" and item.get("phase") in {"opening", "closing"}]
    guard_statuses: list[str] = []
    for data in files:
        if data.kind in {"opening", "closing"}:
            for record in _records(data, "PRE_RECEIVER_GUARD"):
                status = record.payload.get("status")
                if isinstance(status, str):
                    guard_statuses.append(status)
                reasons.extend(_reason_values(record.payload))
    if explicit:
        status = explicit
    elif source_fills and receiver_fills:
        status = "SUCCESS" if not guard_statuses or "PROVED" in guard_statuses else "UNKNOWN"
    elif receiver_dispatched and receiver_fills and not source_fills:
        status = "FAILED"
        reasons.append("receiver execution was confirmed without a paired source fill")
    elif not receiver_dispatched and guard_statuses:
        status = "FAILED"
        reasons.append("receiver was never dispatched after the pre-receiver guard")
    elif not parent_complete:
        status = "UNKNOWN"
        reasons.append("cycle has no durable terminal classification")
    else:
        status = "UNKNOWN"
        reasons.append("journals do not prove a paired execution classification")
    if not reasons:
        reasons.append("paired execution classification is taken from explicit result evidence or conservative leg comparison")
    return {
        "status": status,
        "receiver_dispatched": receiver_dispatched,
        "source_confirmed_fill_count": len(source_fills),
        "receiver_confirmed_fill_count": len(receiver_fills),
        "guard_statuses": list(dict.fromkeys(guard_statuses)),
        "reasons": list(dict.fromkeys(reasons))[:20],
    }


def _regression_signals(files: Sequence[_FileData], issues: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    text_parts: list[str] = []
    events: set[str] = set()
    for data in files:
        for record in data.records:
            events.add(record.event)
            text_parts.extend(_reason_values(record.payload))
            text_parts.append(record.event)
            text_parts.append(json.dumps(record.payload, ensure_ascii=False, sort_keys=True))
    text = " ".join(text_parts).lower()

    def signal(name: str, observed: bool, evidence: list[str]) -> dict[str, Any]:
        return {"status": "OBSERVED" if observed else "NOT_OBSERVED", "evidence": evidence}

    return {
        "source_disappearance": signal(
            "source_disappearance",
            "source order disappeared" in text or "source order identity or status is unresolved" in text,
            [event for event in events if "SOURCE" in event and ("UNKNOWN" in event or "OBSERVED" in event)],
        ),
        "public_ownership_absent": signal(
            "public_ownership_absent",
            "owner" in text and ("absent" in text or "does not prove" in text or "unproved" in text),
            [event for event in events if event == "PRE_RECEIVER_GUARD"],
        ),
        "external_source_fill_before_receiver": signal(
            "external_source_fill_before_receiver",
            "source_filled_before_receiver" in text or "consumed_by_fill" in text or "source fill observed before receiver" in text,
            [event for event in events if "SOURCE_FILLED" in event],
        ),
        "delayed_or_incomplete_history": signal(
            "delayed_or_incomplete_history",
            "history incomplete" in text or "history" in text and ("delayed" in text or "unknown" in text),
            ["journal reason/evidence mentions incomplete or unknown history"] if "history" in text else [],
        ),
        "canceled_zero_fill_ioc": signal(
            "canceled_zero_fill_ioc",
            "terminal_zero_fill" in text or ("canceled" in text and "filled_quantity\": \"0" in text),
            [event for event in events if "CANCEL" in event or "FALLBACK" in event],
        ),
        "ambiguous_transport_after_send": signal(
            "ambiguous_transport_after_send",
            any(event.endswith("_DISPATCH_UNKNOWN") for event in events)
            or any(issue.get("code") == "ORPHAN_DISPATCH_RESULT" for issue in issues),
            [event for event in events if event.endswith("_DISPATCH_UNKNOWN")],
        ),
        "incomplete_input": signal(
            "incomplete_input",
            bool(issues),
            [str(issue.get("code")) for issue in issues[:20]],
        ),
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
            continue
        if any(record.event.endswith("_DISPATCH_INTENT") for record in data.records):
            issues.append(
                {
                    "code": "CHILD_TERMINAL_MISSING",
                    "message": "child journal contains mutation intent but no terminal reconciliation record",
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
    fills = _fill_records(all_files)
    terminal_orders = _terminal_order_records(all_files)
    latency, latency_notes = _latency_reports(all_files)
    inventory, inventory_notes = _positions_from_cycle(cycle)
    economics = _economics(all_files, fills)
    paired = _paired_execution(all_files, actions, fills)

    cycle_complete = _last_record(cycle, "CYCLE_COMPLETE") if cycle else None
    interrupted = _last_record(cycle, "CYCLE_INTERRUPTED") if cycle else None
    unknown_terminal = _last_record(cycle, "CYCLE_EXECUTION_UNKNOWN") if cycle else None
    preflight_blocked = _last_record(cycle, "CYCLE_PREFLIGHT_BLOCKED") if cycle else None
    terminal = cycle_complete is not None
    cycle_payload = {} if cycle_complete is None else cycle_complete.payload
    opening = cycle_payload.get("opening") if isinstance(cycle_payload.get("opening"), Mapping) else {}
    closing = cycle_payload.get("closing") if isinstance(cycle_payload.get("closing"), Mapping) else {}
    outcome = opening.get("outcome") or closing.get("outcome") or ("COMPLETE" if terminal else "INCOMPLETE")
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
            "reason": None if not reasons else reasons[0],
            "process_exit_ignored": True,
        },
        "binding": binding,
        "progression": progression,
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
        "report_notes": list(
            dict.fromkeys(
                [
                    "Process exit status is not treated as a fill, terminal journal record, or flat-inventory proof.",
                    "A dispatch response accepted by the venue is not itself a confirmed fill; only persisted trade receipts contribute to confirmed_fills.",
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
