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


def test_report_distinguishes_planned_receiver_and_incomplete_terminal_receipts(tmp_path: Path):
    child_names = ["opening.jsonl", "opening-attempt-002.jsonl", "opening-attempt-003.jsonl"]
    _cycle_parent(tmp_path, child_names=child_names)
    for attempt, name in enumerate(child_names, 1):
        _child_attempt(tmp_path / name, attempt=attempt)

    report = load_saved_cycle_report(tmp_path)

    assert report["status"] == "INCOMPLETE"
    assert len(report["planned_actions"]) == 6
    assert [item for item in report["dispatched_actions"] if item["leg"] == "receiver"] == []
    assert report["paired_execution"]["status"] == "UNKNOWN"
    assert report["inventory"]["status"] == "UNKNOWN"
    assert report["confirmed_fills"] == []
    assert report["terminal_orders"] == []
    assert report["economics"]["fees"]["status"] == "UNKNOWN"
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

    assert report["confirmed_fills"] == []
    assert any(issue["code"] == "INCOMPLETE_TRADE_HISTORY" for issue in report["issues"])
    assert report["terminal_orders"] == []  # the synthetic receipt intentionally omits an order snapshot
    assert report["paired_execution"]["status"] == "UNKNOWN"
    assert report["inventory"]["status"] == "UNKNOWN"
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
    assert report["inventory"]["status"] == "UNKNOWN"
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


def _complete_leg(
    account_index: int,
    order_id: str,
    trade_id: str,
    quantity: str,
    counterparty_account_index: int,
    counterparty_order_id: str,
    *,
    fee: str | None = "0.01",
) -> dict[str, Any]:
    return {
        "account_index": account_index,
        "order_id": order_id,
        "filled_quantity": quantity,
        "fee_total": fee,
        "history_complete": True,
        "position_after": "0",
        "trades": [
            {
                "trade_id": trade_id,
                "order_id": order_id,
                "account_index": account_index,
                "market_id": 7,
                "quantity": quantity,
                "price": "100",
                "fee": fee,
                "counterparty_account_index": counterparty_account_index,
                "counterparty_order_id": counterparty_order_id,
                "observed_at": 3.0,
            }
        ],
        "order": {
            "order_id": order_id,
            "account_index": account_index,
            "market_id": 7,
            "filled_quantity": quantity,
            "remaining_quantity": "0",
            "status": "filled",
            "time_in_force": "IOC",
        },
    }


@pytest.mark.parametrize("fee", ["0.01", None])
def test_report_does_not_bless_unbound_terminal_only_receipts(
    tmp_path: Path,
    fee: str | None,
):
    _write(
        tmp_path / "cycle.jsonl",
        [
            _event(1, "CYCLE_STARTED", 1.0, {"binding": {"source_account_index": 11, "receiver_account_index": 22}}),
            _event(
                2,
                "CYCLE_COMPLETE",
                4.0,
                {
                    "outcome": "SUCCESS",
                    "opening": {"outcome": "SUCCESS"},
                    "remaining_positions": {"source": "0", "receiver": "0"},
                    "remaining_position_observed_at": {"source": 3.9, "receiver": 3.9},
                },
            ),
        ],
    )
    source = _complete_leg(11, "source-order", "trade-1", "0.20", 22, "receiver-order", fee=fee)
    receiver = _complete_leg(22, "receiver-order", "trade-1", "0.20", 11, "source-order", fee=fee)
    _write(
        tmp_path / "opening.jsonl",
        [
            _event(
                1,
                "COMPLETE",
                3.0,
                {
                    "outcome": "SUCCESS",
                    "economic_status": "PROVEN",
                    "receipt": {"source": source, "receiver": receiver},
                },
            )
        ],
    )

    report = load_saved_cycle_report(tmp_path)

    assert report["paired_execution"]["status"] == "UNKNOWN"
    assert report["inventory"]["status"] == "UNKNOWN"
    assert report["economics"]["fees"]["status"] == "UNKNOWN"


def test_report_keeps_parent_unknown_outcome_and_rejects_unequal_receipts(tmp_path: Path):
    _write(
        tmp_path / "cycle.jsonl",
        [
            _event(1, "CYCLE_STARTED", 1.0, {}),
            _event(
                2,
                "CYCLE_COMPLETE",
                4.0,
                {
                    "outcome": "UNKNOWN",
                    "opening": {"outcome": "SUCCESS"},
                    "closing": {"outcome": "UNKNOWN"},
                    "remaining_positions": {"source": "0", "receiver": "0"},
                    "remaining_position_observed_at": {"source": 3.9, "receiver": 3.9},
                },
            ),
        ],
    )
    source = _complete_leg(11, "source-order", "source-trade", "0.10", 22, "receiver-order")
    receiver = _complete_leg(22, "receiver-order", "receiver-trade", "0.20", 11, "source-order")
    _write(
        tmp_path / "opening.jsonl",
        [_event(1, "COMPLETE", 3.0, {"outcome": "SUCCESS", "economic_status": "PROVEN", "receipt": {"source": source, "receiver": receiver}})],
    )

    report = load_saved_cycle_report(tmp_path)

    assert report["cycle"]["outcome"] == "UNKNOWN"
    assert report["cycle"]["child_outcomes"] == {"opening": "SUCCESS", "closing": "UNKNOWN"}
    assert report["paired_execution"]["status"] == "UNKNOWN"
    assert report["economics"]["fees"]["status"] == "UNKNOWN"


def test_report_typed_cancellation_signals_distinguish_ioc_from_post_only(tmp_path: Path):
    _write(
        tmp_path / "cycle.jsonl",
        [
            _event(1, "CYCLE_STARTED", 1.0, {}),
            _event(
                2,
                "FALLBACK_ATTEMPT_EVIDENCE",
                2.0,
                {
                    "attempt": 1,
                    "history_complete": True,
                    "reconciliation_state": "TERMINAL_ZERO_FILL",
                    "order": {"order_id": "ioc", "status": "canceled", "filled_quantity": "0", "time_in_force": "IOC"},
                    "trades": [],
                },
            ),
            _event(3, "CYCLE_COMPLETE", 3.0, {"outcome": "PARTIAL", "remaining_positions": {"source": "0", "receiver": "0"}, "remaining_position_observed_at": {"source": 2.9, "receiver": 2.9}}),
        ],
    )
    _write(
        tmp_path / "opening.jsonl",
        [
            _event(
                1,
                "LEG_RECONCILED",
                2.1,
                {"order": {"order_id": "limit", "status": "canceled", "filled_quantity": "0", "time_in_force": "POST_ONLY"}, "history_complete": True, "filled_quantity": "0", "trades": []},
            ),
            _event(2, "COMPLETE", 2.2, {"outcome": "PARTIAL", "receipt": {"source": {"filled_quantity": "0", "history_complete": True, "trades": []}, "receiver": {"filled_quantity": "0", "history_complete": True, "trades": []}}}),
        ],
    )

    signals = load_saved_cycle_report(tmp_path)["regression_signals"]

    assert signals["canceled_zero_fill_ioc"]["status"] == "OBSERVED"
    assert signals["canceled_zero_fill_limit"]["status"] == "OBSERVED"


def test_report_marks_reversed_latency_interval_without_clamping(tmp_path: Path):
    _cycle_parent(tmp_path, child_names=["opening.jsonl"], complete=False)
    plan = _plan()
    _write(
        tmp_path / "opening.jsonl",
        [
            _event(1, "PLAN_READY", 5.0, {"plan": plan}),
            _event(2, "SOURCE_DISPATCH_INTENT", 4.0, {"plan": plan["source"]}),
            _event(3, "SOURCE_DISPATCH_RESULT", 4.1, {"accepted": True}),
        ],
    )

    report = load_saved_cycle_report(tmp_path)

    assert any(issue["code"] == "INVALID_INTERVAL" for issue in report["issues"])
    assert report["latency"]["opening"][0]["preparation_seconds"] is None


def test_report_bounds_detail_but_preserves_exact_source_counts_and_hashes(tmp_path: Path):
    rows = [_event(index, "NOOP", float(index), {"unrelated_secret": "do-not-echo", "reason": "bounded"}) for index in range(1, 700)]
    _write(tmp_path / "cycle.jsonl", rows)

    report = load_saved_cycle_report(tmp_path)
    source = report["source_files"][0]

    assert source["record_count"] == 699
    assert source["line_count"] == 699
    assert source["detail_truncated"] is True
    assert report["progression_total_events"] == 699
    assert report["progression_detail_truncated"] is True
    assert "unrelated_secret" not in json.dumps(report, ensure_ascii=False)


@pytest.fixture
def complete_cycle(tmp_path: Path) -> Path:
    """Generate real engine journals using the existing fully offline adapter."""
    import asyncio
    from dataclasses import replace
    from decimal import Decimal
    from test_hood_handoff_random_cycle import AdvancingClock, CycleClient, FixedRng, cycle_config
    from risex_spread_shadow.hood_handoff import run_random_cycle

    class FeeClient(CycleClient):
        async def list_trades(self, *args, **kwargs):
            page = await super().list_trades(*args, **kwargs)
            return replace(page, trades=tuple(replace(t, fee=Decimal("0.01")) for t in page.trades))

    clock = AdvancingClock()
    client = FeeClient(clock)
    path = tmp_path / "saved"
    result = asyncio.run(run_random_cycle(cycle_config(path), client, clock=clock, rng=FixedRng(20, 20)))
    assert result.outcome.value == "SUCCESS"
    assert len(client.submissions) == 4
    return path


def _change_records(path: Path, change) -> None:
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    change(rows)
    for index, row in enumerate(rows, 1):
        row["sequence"] = index
    _write(path, rows)


def test_real_engine_cycle_report_keeps_exact_fees_latency_and_pair_proof(complete_cycle: Path):
    report = load_saved_cycle_report(complete_cycle)
    assert report["issues"] == []
    assert report["paired_execution"]["status"] == "SUCCESS"
    assert report["inventory"]["status"] == "CONFIRMED_FLAT"
    # Independent four actual adapter trades, each with fee 0.01.
    assert report["economics"]["fees"]["total"] == "0.04"
    assert len(report["confirmed_fills"]) == 4
    assert len(report["planned_actions"]) == 4
    assert all(p["status"] == "MATCHED" for p in report["paired_execution"]["direct_counterparty_match"])
    for phase in ("opening", "closing"):
        rows = [json.loads(line) for line in (complete_cycle / f"{phase}.jsonl").read_text().splitlines()]
        saved = next(r["payload"]["latency"] for r in rows if r["event"] == "COMPLETE")
        measured = report["latency"][phase][0]
        assert measured["source_dispatch_ack_seconds"] == saved["source_submit_ack_seconds"]
        assert measured["quote_age_seconds"] == saved["source_quote_age_seconds"]
        assert measured["receiver_admission_seconds"] == saved["receiver_admission_seconds"]
        assert measured["overlap"]["public_book_read_seconds"] == saved["public_book_read_seconds"]


@pytest.mark.parametrize("defect", ["missing_order", "missing_market", "foreign_client", "wrong_side", "negative_price", "missing_trade", "incomplete_history", "wrong_position", "unresolved", "missing_dispatch", "active_order"])
def test_real_engine_receipt_defects_cannot_prove_completion(complete_cycle: Path, defect: str):
    def corrupt(rows):
        receipt = next(r["payload"]["receipt"] for r in rows if r["event"] == "COMPLETE")
        leg = receipt["source"]
        trade = leg["trades"][0]
        if defect == "missing_order": leg["order"] = None
        elif defect == "missing_market": trade.pop("market_id")
        elif defect == "foreign_client": trade["client_order_index"] = 999
        elif defect == "wrong_side": trade["side"] = "SELL" if trade["side"] == "BUY" else "BUY"
        elif defect == "negative_price": trade["price"] = "-1"
        elif defect == "missing_trade": leg["trades"] = []
        elif defect == "incomplete_history": leg["history_complete"] = False
        elif defect == "wrong_position": leg["position_after"] = "0.01"
        elif defect == "unresolved": leg["unknown_reasons"] = ["pending cancellation"]
        elif defect == "missing_dispatch": leg.pop("dispatched")
        elif defect == "active_order": leg["order"]["status"] = "open"
    _change_records(complete_cycle / "closing.jsonl", corrupt)
    report = load_saved_cycle_report(complete_cycle)
    assert report["status"] == "INCOMPLETE"
    assert report["paired_execution"]["status"] == "UNKNOWN"
    assert report["inventory"]["status"] == "UNKNOWN"
    assert report["economics"]["fees"]["status"] == "UNKNOWN"


@pytest.mark.parametrize("fee", [None, "NaN", "nonsense"])
def test_invalid_or_missing_fees_do_not_erase_proven_exposure(complete_cycle: Path, fee):
    def corrupt(rows):
        receipt = next(r["payload"]["receipt"] for r in rows if r["event"] == "COMPLETE")
        receipt["source"]["trades"][0]["fee"] = fee
    _change_records(complete_cycle / "opening.jsonl", corrupt)
    report = load_saved_cycle_report(complete_cycle)
    assert report["paired_execution"]["status"] == "SUCCESS"
    assert report["inventory"]["status"] == "CONFIRMED_FLAT"
    assert report["economics"]["fees"]["status"] == "UNKNOWN"


def test_fee_total_must_match_individual_receipts(complete_cycle: Path):
    def corrupt(rows):
        next(r["payload"]["receipt"] for r in rows if r["event"] == "COMPLETE")["receiver"]["fee_total"] = "900"
    _change_records(complete_cycle / "closing.jsonl", corrupt)
    report = load_saved_cycle_report(complete_cycle)
    assert report["economics"]["fees"]["status"] == "UNKNOWN"
    assert report["inventory"]["status"] == "CONFIRMED_FLAT"


def test_counterparty_conflict_is_separate_from_valid_exposure(complete_cycle: Path):
    def corrupt(rows):
        receipt = next(r["payload"]["receipt"] for r in rows if r["event"] == "COMPLETE")
        receipt["source"]["trades"][0]["counterparty_order_id"] = "different-order"
    _change_records(complete_cycle / "opening.jsonl", corrupt)
    report = load_saved_cycle_report(complete_cycle)
    assert report["paired_execution"]["status"] == "SUCCESS"
    assert report["paired_execution"]["direct_counterparty_match"][0]["status"] == "UNKNOWN"


@pytest.mark.parametrize("defect", ["missing_close", "stale_positions", "missing_hold", "truncated", "underflow", "middle_intent"])
def test_cycle_evidence_cannot_be_promoted_from_partial_history(complete_cycle: Path, defect: str):
    if defect == "missing_close":
        (complete_cycle / "closing.jsonl").unlink()
    else:
        def corrupt(rows):
            terminal = rows[-1]
            if defect == "stale_positions": terminal["payload"]["remaining_position_observed_at"]["source"] = 1
            elif defect == "missing_hold": rows[:] = [r for r in rows if r["event"] != "HOLD_ANCHORED"]
            elif defect == "underflow": terminal["payload"]["remaining_positions"]["source"] = "1e-400"
            else:
                extra = [_event(i, "NOOP", terminal["at"], {}) for i in range(700)]
                if defect == "middle_intent": extra[350]["event"] = "FALLBACK_DISPATCH_INTENT"
                rows[-1:-1] = extra
        _change_records(complete_cycle / "cycle.jsonl", corrupt)
    report = load_saved_cycle_report(complete_cycle)
    assert report["paired_execution"]["status"] != "SUCCESS"
    assert report["inventory"]["status"] != "CONFIRMED_FLAT"


def test_additive_fields_do_not_displace_required_facts(complete_cycle: Path):
    def add(rows):
        for row in rows:
            row["payload"] = {**{f"irrelevant_{i}": "not-for-output" for i in range(150)}, **row["payload"]}
    _change_records(complete_cycle / "closing.jsonl", add)
    report = load_saved_cycle_report(complete_cycle)
    assert report["status"] == "COMPLETE"
    assert report["paired_execution"]["status"] == "SUCCESS"
    assert "not-for-output" not in json.dumps(report)


@pytest.mark.parametrize("boundary, initial_sends", [("after_intent", 0), ("during_send", 1), ("before_response", 1), ("after_response", 1), ("before_terminal", 2)])
def test_engine_crash_boundaries_preserve_unknown_and_never_replay(tmp_path: Path, boundary: str, initial_sends: int):
    root = Path(__file__).resolve().parents[2]
    child = tmp_path / "opening.jsonl"
    marker = tmp_path / "mutations.jsonl"
    script = r'''
import asyncio, json, os, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from test_hood_handoff_engine import FakeClient, FakeClock, make_config
from risex_spread_shadow.hood_handoff import DurableJournal, run_handoff
path, marker, boundary, mode = Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4], sys.argv[5]
original = DurableJournal.append
class Client(FakeClient):
    async def submit_order(self, plan):
        with marker.open('a') as f:
            f.write(json.dumps({'mode': mode, 'kind': 'send'}) + '\n')
            f.flush(); os.fsync(f.fileno())
        if mode == 'first' and boundary == 'during_send': os._exit(71)
        return await super().submit_order(plan)
    async def cancel_order(self, *args):
        with marker.open('a') as f: f.write(json.dumps({'mode': mode, 'kind': 'cancel'}) + '\n')
        return await super().cancel_order(*args)
def append(self, event, *args, **kwargs):
    if mode == 'first' and ((boundary == 'before_response' and event == 'SOURCE_DISPATCH_RESULT') or (boundary == 'before_terminal' and event == 'COMPLETE')): os._exit(71)
    result = original(self, event, *args, **kwargs)
    if mode == 'first' and ((boundary == 'after_intent' and event == 'SOURCE_DISPATCH_INTENT') or (boundary == 'after_response' and event == 'SOURCE_DISPATCH_RESULT')): os._exit(71)
    return result
DurableJournal.append = append
client = Client(source_fills=True)
result = asyncio.run(run_handoff(make_config(path), client, clock=FakeClock()))
print(json.dumps({'outcome': result.outcome.value, 'sends': len(client.submissions), 'cancels': len(client.cancellations)}))
'''
    env = dict(os.environ, PYTHONPATH=str(root / "src"))
    args = [sys.executable, "-c", script, str(root / "tests/spread_shadow"), str(child), str(marker), boundary]
    first = subprocess.run([*args, "first"], env=env, capture_output=True, text=True, timeout=20)
    assert first.returncode == 71, first.stderr
    before = child.read_bytes()
    _write(tmp_path / "cycle.jsonl", [_event(1, "CYCLE_STARTED", 1, {})])
    report = load_saved_cycle_report(tmp_path)
    assert report["inventory"]["status"] == "UNKNOWN"
    assert report["paired_execution"]["status"] == "UNKNOWN"
    assert report["economics"]["fees"]["status"] == "UNKNOWN"
    assert report["dispatched_actions"]
    assert child.read_bytes() == before
    resumed = subprocess.run([*args, "resume"], env=env, capture_output=True, text=True, timeout=20)
    assert resumed.returncode == 0, resumed.stderr
    result = json.loads(resumed.stdout)
    assert result["sends"] == result["cancels"] == 0
    assert result["outcome"] != "SUCCESS"
    mutations = [json.loads(line) for line in marker.read_text().splitlines()] if marker.exists() else []
    assert sum(m["kind"] == "send" for m in mutations) == initial_sends
    assert all(m["mode"] == "first" for m in mutations)


def test_trade_cannot_be_reused_across_opening_and_closing(complete_cycle: Path):
    opening = [json.loads(line) for line in (complete_cycle / "opening.jsonl").read_text().splitlines()]
    original = next(r["payload"]["receipt"] for r in opening if r["event"] == "COMPLETE")
    def corrupt(rows):
        receipt = next(r["payload"]["receipt"] for r in rows if r["event"] == "COMPLETE")
        for leg in ("source", "receiver"):
            receipt[leg]["trades"][0]["trade_id"] = original[leg]["trades"][0]["trade_id"]
    _change_records(complete_cycle / "closing.jsonl", corrupt)
    report = load_saved_cycle_report(complete_cycle)
    assert any(i["code"] == "REUSED_TRADE_ID" for i in report["issues"])
    assert report["paired_execution"]["status"] == "UNKNOWN"
    assert report["inventory"]["status"] == "UNKNOWN"
    assert report["confirmed_fills"] == []


def test_extra_unfinished_child_blocks_complete_cycle_proof(complete_cycle: Path):
    _write(complete_cycle / "closing-attempt-002.jsonl", [_event(1, "ATTEMPT_STARTED", 1020, {})])
    report = load_saved_cycle_report(complete_cycle)
    assert report["status"] == "INCOMPLETE"
    assert report["inventory"]["status"] == "UNKNOWN"
    assert report["paired_execution"]["status"] == "UNKNOWN"


def test_payload_trade_truncation_is_explicit_and_cannot_prove_fees(complete_cycle: Path):
    def corrupt(rows):
        receipt = next(r["payload"]["receipt"] for r in rows if r["event"] == "COMPLETE")
        receipt["source"]["trades"] *= 65
    _change_records(complete_cycle / "opening.jsonl", corrupt)
    report = load_saved_cycle_report(complete_cycle)
    assert any(i["code"] == "PAYLOAD_TRUNCATED" for i in report["issues"])
    assert report["economics"]["fees"]["status"] == "UNKNOWN"
    assert report["paired_execution"]["status"] == "UNKNOWN"


def test_quote_acquisition_and_public_visibility_have_distinct_evidence(complete_cycle: Path):
    report = load_saved_cycle_report(complete_cycle)
    for phase in ("opening", "closing"):
        metrics = report["latency"][phase][0]
        assert metrics["quote_read_seconds"] is not None
        assert metrics["source_visibility_seconds"] is not None
        assert metrics["public_source_observation_seconds"] is not None
        assert metrics["attribution"]["local_cpu_only"]["status"] == "UNKNOWN"
        assert metrics["quote_age_breakdown"]["stale_at_intent"] is False
    def remove_public(rows):
        for row in rows:
            if row["event"] == "PRE_RECEIVER_GUARD": row["payload"]["source_public_level"] = None
    _change_records(complete_cycle / "opening.jsonl", remove_public)
    modified = load_saved_cycle_report(complete_cycle)["latency"]["opening"][0]
    assert modified["source_visibility_seconds"] is not None
    assert modified["public_source_observation_seconds"] is None


def test_report_preserves_sdk_timing_numbers_but_never_invents_percentages(complete_cycle: Path):
    def add_metrics(rows):
        for row in rows:
            if row["event"] == "COMPLETE":
                row["payload"]["latency"].update(source_signing_call_seconds=0.03, source_nonce_acquisition_seconds=0.02, source_transport_roundtrip_seconds=0.07)
    _change_records(complete_cycle / "opening.jsonl", add_metrics)
    metrics = load_saved_cycle_report(complete_cycle)["latency"]["opening"][0]
    assert metrics["source_signing_call_seconds"] == 0.03
    assert metrics["source_nonce_acquisition_seconds"] == 0.02
    assert metrics["source_transport_roundtrip_seconds"] == 0.07
    assert metrics["attribution"]["network_only"]["fraction"] is None
    assert metrics["attribution"]["exchange_processing_only"]["fraction"] is None


def test_projection_preserves_nested_secret_redaction_and_safe_ids():
    from risex_spread_shadow.hood_handoff.offline_report import _bounded_json

    value = {'plan': {'api_key_index': 4, 'reason': 'Bearer synthetic-canary',
                      'private_key': 'synthetic-secret',
                      'trades': [{'reason': 'private_key=synthetic-canary', 'quantity': '0.20'}]}}
    projected = _bounded_json(value)
    assert projected == {'plan': {'api_key_index': 4, 'reason': '[REDACTED]',
                                 'trades': [{'reason': '[REDACTED]', 'quantity': '0.20'}]}}
    assert 'synthetic' not in json.dumps(projected)


def test_projection_bounds_traversal_before_visiting_omitted_subtrees():
    from risex_spread_shadow.hood_handoff.offline_report import _bounded_json

    nested = {'reason': 'deep synthetic sentinel'}
    for _ in range(1500):
        nested = {'plan': nested}
    omitted = _bounded_json({'plan': {'unrelated_field': nested}})
    assert omitted == {'plan': {}}
    truncated = []
    projected = _bounded_json(nested, truncated=truncated)
    assert truncated
    assert '[DETAIL_TRUNCATED]' in json.dumps(projected)
    assert 'sentinel' not in json.dumps(projected)
