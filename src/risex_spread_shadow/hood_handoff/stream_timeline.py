"""Offline comparison of exact local WS and REST visibility for saved orders."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any


def compare_saved_stream(path: Path) -> dict[str, Any]:
    """Read a bounded saved evidence file, never a live stream or process state."""
    header: dict[str, Any] | None = None
    terminal: dict[str, Any] | None = None
    groups: dict[tuple[int, int, str | None], dict[str, Any]] = {}
    pending: dict[tuple[int, int], list[dict[str, Any]]] = {}
    count = 0
    with Path(path).open(encoding="utf-8") as source:
        for line in source:
            count += 1
            if count > 600 or len(line) > 8192:
                raise ValueError("stream evidence exceeds its offline bounds")
            row = json.loads(line)
            if not isinstance(row, dict):
                raise ValueError("stream evidence row must be an object")
            kind = row.get("kind")
            if count == 1:
                if row.get("schema") != "hood-stream-evidence-v1" or kind != "session_start":
                    raise ValueError("stream evidence header is missing")
                header = row
                continue
            if kind == "session_end":
                terminal = row
                continue
            if kind not in {"order", "local_milestone"}:
                continue
            account, client, order_id = (row.get("account_index"), row.get("client_order_index"),
                                         row.get("order_id"))
            at = row.get("receive_monotonic_seconds")
            if (type(account) is not int or type(client) is not int
                    or (order_id is not None and (not isinstance(order_id, str)
                                                  or not order_id.isdecimal()))
                    or (kind == "order" and order_id is None)
                    or type(at) not in {int, float} or not math.isfinite(at) or at < 0
                    or row.get("clock_basis") != "time.monotonic/process-local"):
                continue
            if kind == "local_milestone" and order_id is None:
                pending.setdefault((account, client), []).append({"at": float(at),
                                                                  "name": row.get("milestone")})
                continue
            key = (account, client, order_id)
            group = groups.setdefault(key, {"account_index": account,
                                             "client_order_index": client,
                                             "order_id": order_id,
                                             "ws_events": [], "milestones": []})
            if kind == "order":
                group["ws_events"].append({"at": float(at), "status": row.get("status"),
                                           "terminal": row.get("terminal") is True,
                                           "stream_complete": row.get("stream_complete") is True,
                                           "epoch": row.get("connection_epoch")})
            else:
                group["milestones"].append({"at": float(at), "name": row.get("milestone")})
    if header is None:
        raise ValueError("stream evidence is empty")
    for (account, client), milestones in pending.items():
        candidates = [group for (a, c, order_id), group in groups.items()
                      if a == account and c == client and order_id is not None]
        if len(candidates) == 1:
            candidates[0]["milestones"].extend(milestones)
        else:
            group = groups.setdefault((account, client, None), {
                "account_index": account, "client_order_index": client,
                "order_id": None, "ws_events": [], "milestones": [],
            })
            group["milestones"].extend(milestones)
    results = []
    for group in groups.values():
        ws = min((item["at"] for item in group["ws_events"]), default=None)
        rest = min((item["at"] for item in group["milestones"]
                    if item["name"] == "exact_rest_observed"), default=None)
        ack = min((item["at"] for item in group["milestones"]
                   if item["name"] == "ack_parsed"), default=None)
        if ws is None or rest is None:
            ordering, lead = "UNMEASURED", None
        elif ws <= rest:
            ordering, lead = "WS_FIRST" if ws < rest else "SAME_LOCAL_TIME", rest - ws
        else:
            ordering, lead = "REST_FIRST", None
        results.append({
            **{name: group[name] for name in ("account_index", "client_order_index", "order_id")},
            "first_ws_monotonic_seconds": ws,
            "first_rest_monotonic_seconds": rest,
            "first_ack_monotonic_seconds": ack,
            "visibility_order": ordering,
            "ws_lead_seconds_when_first": lead,
            "ws_event_before_ack": None if ws is None or ack is None else ws < ack,
            "ws_statuses": [item["status"] for item in group["ws_events"]],
            "ws_executable_admission_proof": False,
            "rest_exact_identity_observed": rest is not None,
        })
    return {
        "schema": "hood-stream-timeline-v1",
        "source_path": str(path),
        "terminal_present": terminal is not None,
        "evidence_dropped": None if terminal is None else terminal.get("dropped"),
        "transport_complete": bool(terminal and terminal.get("complete") is True),
        "clock_basis": "time.monotonic/process-local",
        "orders": sorted(results, key=lambda item: (item["account_index"], item["client_order_index"], item["order_id"] or "")),
    }
