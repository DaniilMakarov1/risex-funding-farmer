from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any

import pytest

from risex_spread_shadow.hood_handoff import load_saved_cycle_report


def _event(sequence: int, event: str, at: float, payload: dict[str, Any], *, run_id: str = "run") -> dict[str, Any]:
    return {
        "sequence": sequence,
        "run_id": run_id,
        "event": event,
        "at": at,
        "payload": payload,
    }


def _write(path: Path, rows: list[dict[str, Any]], *, newline: bool = True) -> None:
    encoded = "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows)
    if not newline:
        encoded = encoded.rstrip("\n")
    path.write_text(encoded, encoding="utf-8")


def _plan(*, quantity: str = "0.20") -> dict[str, Any]:
    return {
        "operation_mode": "PAIRED_OPENING",
        "source": {
            "account_index": 11,
            "market_id": 7,
            "order_id": "source-order",
            "client_order_index": 101,
            "side": "SELL",
            "quantity": quantity,
        },
        "receiver": {
            "account_index": 22,
            "market_id": 7,
            "order_id": "receiver-order",
            "client_order_index": 202,
            "side": "BUY",
            "quantity": quantity,
        },
    }


def _child_attempt(path: Path, *, attempt: int, guard_status: str = "LOST") -> None:
    plan = _plan()
    rows = [
        _event(1, "ATTEMPT_STARTED", 10 + attempt, {"binding": {"attempt_index": attempt}}),
        _event(2, "PLAN_READY", 10.1 + attempt, {"attempt_index": attempt, "plan": plan}),
        _event(3, "SOURCE_DISPATCH_INTENT", 10.2 + attempt, {"plan": plan["source"], "attempt": attempt}),
        _event(4, "SOURCE_DISPATCH_RESULT", 10.3 + attempt, {"accepted": True, "order_id": "source-order"}),
        _event(
            5,
            "PRE_RECEIVER_GUARD",
            10.5 + attempt,
            {
                "status": guard_status,
                "priority_reason": "fresh public book does not prove owner",
                "request_started_at": 10.4 + attempt,
                "request_finished_at": 10.5 + attempt,
                "source_public_level": None,
            },
        ),
        _event(6, "CANCEL_DISPATCH_INTENT", 10.6 + attempt, {"account_index": 11, "order_id": "source-order", "attempt": attempt}),
        _event(7, "CANCEL_DISPATCH_RESULT", 10.7 + attempt, {"accepted": True, "order_id": "source-order"}),
        _event(
            8,
            "COMPLETE",
            11 + attempt,
            {
                "attempt_index": attempt,
                "economic_status": "PROVEN",
                "latency": {
                    "paired_preparation_seconds": 0.2,
                    "source_quote_age_seconds": 0.4,
                    "source_submit_ack_seconds": 0.1,
                    "source_visibility_seconds": 0.3,
                    "reconciliation_seconds": 0.5,
                    "concurrent_pre_receiver_checks_seconds": 0.2,
                    "coalesced_pre_receiver_window_seconds": 0.25,
                    "public_book_read_seconds": 0.15,
                },
                "receipt": {
                    "attempt_index": attempt,
                    "economic_status": "PROVEN",
                    "source": {"filled_quantity": "0", "fee_total": "0", "trades": []},
                    "receiver": {"filled_quantity": "0", "fee_total": "0", "trades": []},
                },
            },
        ),
    ]
    _write(path, rows)


def _cycle_parent(path: Path, *, child_names: list[str], complete: bool = True) -> None:
    rows = [
        _event(
            1,
            "CYCLE_STARTED",
            1.0,
            {"binding": {"market_id": 7, "market_symbol": "BTC", "source_account_index": 11, "receiver_account_index": 22}},
        ),
        _event(2, "SELECTION_PROVED", 1.2, {"selection": {"quantity": "0.20", "hold_seconds": 20, "book_observed_at": 1.1}}),
        _event(3, "PREPARATION_ATTEMPT", 1.3, {"attempt": 1}),
        _event(
            4,
            "OPENING_PLAN_READY",
            1.4,
            {"attempt": 1, "config": {"journal_path": str(path / child_names[0]), "market_id": 7, "quantity": "0.20", "operation_mode": "PAIRED_OPENING"}},
        ),
        _event(5, "FIRST_MUTATION_BOUNDARY", 1.5, {"attempt": 1, "journal_path": str(path / child_names[0])}),
    ]
    if complete:
        rows.append(
            _event(
                6,
                "CYCLE_COMPLETE",
                20.0,
                {
                    "opening": {
                        "outcome": "PARTIAL",
                        "reason": "paired opening did not prove a complete cycle",
                        "journal_path": str(path / child_names[0]),
                    },
                    "remaining_positions": {"source": "0", "receiver": "0"},
                    "remaining_position_observed_at": {"source": 19.9, "receiver": 19.95},
                },
            )
        )
    _write(path / "cycle.jsonl", rows)


def test_report_distinguishes_planned_receiver_from_never_dispatched_and_keeps_flat_proof(tmp_path: Path):
    child_names = ["opening.jsonl", "opening-attempt-002.jsonl", "opening-attempt-003.jsonl"]
    _cycle_parent(tmp_path, child_names=child_names)
    for attempt, name in enumerate(child_names, 1):
        _child_attempt(tmp_path / name, attempt=attempt)

    report = load_saved_cycle_report(tmp_path)

    assert report["status"] == "COMPLETE"
    assert len(report["planned_actions"]) == 6
    assert [item for item in report["dispatched_actions"] if item["leg"] == "receiver"] == []
    assert report["paired_execution"]["status"] == "FAILED"
    assert report["inventory"]["status"] == "CONFIRMED_FLAT"
    assert report["confirmed_fills"] == []
    assert report["terminal_orders"] == []
    assert report["economics"]["fees"]["status"] == "PROVEN"
    assert report["economics"]["funding_pnl"]["status"] == "UNKNOWN"
    first_latency = report["latency"]["opening"][0]
    assert first_latency["receiver_admission_seconds"] == pytest.approx(0.1)
    assert first_latency["receiver_dispatch_ack_seconds"] is None
    assert first_latency["receiver_visibility_seconds"] is None
    assert first_latency["overlap"]["not_additive"] is True
    assert report["regression_signals"]["public_ownership_absent"]["status"] == "OBSERVED"


def test_report_keeps_confirmed_trade_and_fee_unknown_separate_from_inventory(tmp_path: Path):
    _cycle_parent(tmp_path, child_names=["opening.jsonl"])
    plan = _plan()
    _write(
        tmp_path / "opening.jsonl",
        [
            _event(1, "ATTEMPT_STARTED", 2.0, {"binding": {"attempt_index": 1}}),
            _event(2, "PLAN_READY", 2.1, {"attempt_index": 1, "plan": plan}),
            _event(3, "SOURCE_DISPATCH_INTENT", 2.2, {"plan": plan["source"]}),
            _event(4, "SOURCE_DISPATCH_RESULT", 2.3, {"accepted": True}),
            _event(5, "RECEIVER_DISPATCH_INTENT", 2.4, {"plan": plan["receiver"]}),
            _event(6, "RECEIVER_DISPATCH_RESULT", 2.5, {"accepted": True}),
            _event(
                7,
                "COMPLETE",
                3.0,
                {
                    "attempt_index": 1,
                    "economic_status": "UNKNOWN",
                    "receipt": {
                        "attempt_index": 1,
                        "economic_status": "UNKNOWN",
                        "source": {"filled_quantity": "0", "fee_total": "0", "trades": []},
                        "receiver": {
                            "filled_quantity": "0.20",
                            "fee_total": None,
                            "trades": [{"trade_id": "trade-1", "order_id": "receiver-order", "account_index": 22, "quantity": "0.20", "price": "100", "fee": None, "observed_at": 2.6}],
                        },
                    },
                },
            ),
        ],
    )

    report = load_saved_cycle_report(tmp_path)

    assert len(report["confirmed_fills"]) == 1
    assert report["confirmed_fills"][0]["trade_id"] == "trade-1"
    assert report["terminal_orders"] == []  # the synthetic receipt intentionally omits an order snapshot
    assert report["paired_execution"]["status"] == "FAILED"
    assert report["inventory"]["status"] == "CONFIRMED_FLAT"
    assert report["economics"]["fees"]["status"] == "UNKNOWN"
    assert report["economics"]["funding_pnl"]["status"] == "UNKNOWN"


def test_report_marks_malformed_truncated_and_conflicting_input_without_flat_upgrade(tmp_path: Path):
    _cycle_parent(tmp_path, child_names=["opening.jsonl"])
    child = tmp_path / "opening.jsonl"
    _write(
        child,
        [
            _event(1, "ATTEMPT_STARTED", 2.0, {}),
            _event(3, "SOURCE_DISPATCH_INTENT", 2.2, {"plan": _plan()["source"]}),
        ],
        newline=False,
    )
    with child.open("a", encoding="utf-8") as handle:
        handle.write("\n{\"sequence\":4,\"event\":")

    report = load_saved_cycle_report(tmp_path)
    codes = {issue["code"] for issue in report["issues"]}
    assert report["status"] == "INCOMPLETE"
    assert "SEQUENCE_GAP" in codes
    assert "MALFORMED_JSON" in codes
    assert "TRUNCATED_LINE" in codes
    assert report["inventory"]["status"] == "CONFIRMED_FLAT"
    assert report["dispatched_actions"][0]["status"] == "INTENT_ONLY_UNFINISHED"


def test_report_does_not_treat_intent_only_or_process_exit_as_terminal(tmp_path: Path):
    _cycle_parent(tmp_path, child_names=["opening.jsonl"], complete=False)
    plan = _plan()
    _write(
        tmp_path / "opening.jsonl",
        [
            _event(1, "ATTEMPT_STARTED", 2.0, {}),
            _event(2, "PLAN_READY", 2.1, {"plan": plan}),
            _event(3, "SOURCE_DISPATCH_INTENT", 2.2, {"plan": plan["source"]}),
        ],
    )
    report = load_saved_cycle_report(tmp_path)

    assert report["status"] == "INCOMPLETE"
    assert report["cycle"]["terminal"] is False
    assert report["cycle"]["process_exit_ignored"] is True
    assert report["inventory"]["status"] == "UNKNOWN"
    assert report["dispatched_actions"][0]["status"] == "INTENT_ONLY_UNFINISHED"
    assert report["confirmed_fills"] == []


def test_cli_report_is_sdk_free(tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch):
    _cycle_parent(tmp_path, child_names=["opening.jsonl"], complete=False)
    _write(tmp_path / "opening.jsonl", [_event(1, "ATTEMPT_STARTED", 2.0, {})])
    import risex_spread_shadow.hood_handoff.cli as cli

    def fail_constructor(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("SDK client must not be constructed by offline report")

    monkeypatch.setattr(cli, "LighterSdkClient", fail_constructor)
    assert cli.main(["report", "--path", str(tmp_path), "--json"]) == 0
    value = json.loads(capsys.readouterr().out)
    assert value["schema"] == "hcr-27-offline-cycle-report-v1"
    assert value["status"] == "INCOMPLETE"


def test_crash_before_response_preserves_intent_and_report_does_not_replay(tmp_path: Path):
    _write(tmp_path / "cycle.jsonl", [_event(1, "CYCLE_STARTED", 1.0, {"binding": {"market_id": 7}})])
    child = tmp_path / "opening.jsonl"
    source = (
        "import os\n"
        "from pathlib import Path\n"
        "from risex_spread_shadow.hood_handoff import DurableJournal\n"
        f"path = Path({str(child)!r})\n"
        "journal = DurableJournal(path, run_id='crashed-run')\n"
        "journal.acquire_attempt()\n"
        "journal.append('ATTEMPT_STARTED', {'binding': {'attempt_index': 1}})\n"
        "journal.append('PLAN_READY', {'plan': {'source': {'account_index': 11}, 'receiver': {'account_index': 22}}})\n"
        "journal.append('SOURCE_DISPATCH_INTENT', {'plan': {'account_index': 11, 'quantity': '0.20'}})\n"
        "os._exit(17)\n"
    )
    env = dict(os.environ)
    env["PYTHONPATH"] = str(Path(__file__).parents[2] / "src")
    completed = subprocess.run([sys.executable, "-c", source], env=env, check=False)
    assert completed.returncode == 17

    report = load_saved_cycle_report(tmp_path)
    assert report["status"] == "INCOMPLETE"
    assert report["dispatched_actions"][0]["status"] == "INTENT_ONLY_UNFINISHED"
    assert report["confirmed_fills"] == []


def test_report_does_not_call_unknown_transport_duration_an_ack(tmp_path: Path):
    _cycle_parent(tmp_path, child_names=["opening.jsonl"])
    plan = _plan()
    _write(
        tmp_path / "opening.jsonl",
        [
            _event(1, "ATTEMPT_STARTED", 2.0, {}),
            _event(2, "PLAN_READY", 2.1, {"plan": plan}),
            _event(3, "SOURCE_DISPATCH_INTENT", 2.2, {"plan": plan["source"]}),
            _event(4, "SOURCE_DISPATCH_UNKNOWN", 2.3, {"reason": "transport outcome unknown after send"}),
            _event(
                5,
                "COMPLETE",
                2.4,
                {"latency": {"source_submit_ack_seconds": 0.1}, "receipt": {"source": {"filled_quantity": "0", "trades": []}, "receiver": {"filled_quantity": "0", "trades": []}}},
            ),
        ],
    )

    report = load_saved_cycle_report(tmp_path)

    stage = report["latency"]["opening"][0]["measurements"]["source_dispatch_ack"]
    assert stage["status"] == "UNKNOWN"
    assert stage["seconds"] is None
    assert report["regression_signals"]["ambiguous_transport_after_send"]["status"] == "OBSERVED"


def test_report_surfaces_conflicting_order_identity_evidence(tmp_path: Path):
    _cycle_parent(tmp_path, child_names=["opening.jsonl"])
    plan = _plan()
    _write(
        tmp_path / "opening.jsonl",
        [
            _event(1, "ATTEMPT_STARTED", 2.0, {}),
            _event(2, "PLAN_READY", 2.1, {"plan": plan}),
            _event(3, "ORDER_OBSERVED", 2.2, {"account_index": 11, "market_id": 7, "order_id": "same-order", "status": "open"}),
            _event(4, "LEG_RECONCILED", 2.3, {"order_id": "same-order", "order": {"account_index": 99, "market_id": 7, "order_id": "same-order", "status": "canceled"}}),
            _event(5, "COMPLETE", 2.4, {"receipt": {"source": {"filled_quantity": "0", "trades": []}, "receiver": {"filled_quantity": "0", "trades": []}}}),
        ],
    )

    report = load_saved_cycle_report(tmp_path)

    assert report["status"] == "INCOMPLETE"
    assert any(issue["code"] == "CONFLICTING_ORDER_EVIDENCE" for issue in report["issues"])
