"""Portable projections of immutable real incidents, never executable plans."""
import hashlib
import json
from pathlib import Path

import pytest

from risex_spread_shadow.hood_handoff.offline_report import load_saved_cycle_report

FIXTURES = Path(__file__).parents[1] / "fixtures/hood_handoff"
PARENT_HASHES = {
    "003": "fab5f4a9dfca1342e31193b36e22012973c102045e2a94ffe0537ff8b0d9dc5e",
    "004": "efbda6822eadf397c07b3e37c45585b97178f6587ff0481c4a00e419f15cbb42",
    "007": "741592d27bb20fe46d832757038dcb877f310978428d69f5581ec27f5ad3b4c5",
}


def restore(tmp_path, number):
    fixture = json.loads((FIXTURES / f"cycle-{number}.json").read_text())
    for item in fixture["provenance"]:
        name = Path(item["source"]).name
        rows = fixture["journals"][name]
        canonical = json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()
        assert hashlib.sha256(canonical).hexdigest() == item["projection_sha256"]
        if name == "cycle.jsonl": assert item["original_sha256"] == PARENT_HASHES[number]
        (tmp_path / name).write_text("".join(json.dumps(r) + "\n" for r in rows))
    return fixture


@pytest.mark.parametrize("number", ["003", "004", "007"])
def test_saved_incident_projection_preserves_action_sequence_and_provenance(tmp_path, number):
    fixture = restore(tmp_path, number)
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    report = load_saved_cycle_report(tmp_path)
    assert before == {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    assert report["paired_execution"]["status"] != "SUCCESS"
    assert report["economics"]["funding_pnl"]["status"] == "UNKNOWN"
    actions = [(a["phase"], a["leg"]) for a in report["dispatched_actions"]]
    if number == "003":
        assert not any(leg == "receiver" for _, leg in actions)
        assert "source fill observed before receiver dispatch" in " ".join(report["reasons"])
        assert report["regression_signals"]["external_source_fill_before_receiver"]["status"] == "OBSERVED"
    elif number == "004":
        assert actions == [("fallback", "fallback"), ("fallback", "fallback"), ("opening", "source"), ("opening", "receiver"), ("opening", "cancel")]
        chronological = [r["event"] for r in report["progression"] if r["event"].endswith("_DISPATCH_INTENT")]
        assert chronological == ["SOURCE_DISPATCH_INTENT", "RECEIVER_DISPATCH_INTENT", "CANCEL_DISPATCH_INTENT", "FALLBACK_DISPATCH_INTENT", "FALLBACK_DISPATCH_INTENT"]
        assert report["inventory"]["status"] == "CONFIRMED_FLAT"
        assert report["economics"]["fees"]["status"] == "UNKNOWN"
        assert report["regression_signals"]["canceled_zero_fill_ioc"]["status"] == "OBSERVED"
    else:
        assert actions == [("opening", leg) for _ in range(3) for leg in ("source", "cancel")]
        assert len(report["planned_actions"]) == 6
        assert report["confirmed_fills"] == []
        assert report["inventory"]["status"] == "CONFIRMED_FLAT"
        assert report["economics"]["fees"]["total"] == "0"
        assert report["regression_signals"]["canceled_zero_fill_ioc"]["status"] == "NOT_OBSERVED"
        assert report["latency"]["opening"][0]["source_signing_call_seconds"] is None
        for stage in report["latency"]["opening"]:
            age = stage["quote_age_breakdown"]
            assert age["quote_to_plan_seconds"] + age["plan_to_intent_seconds"] == pytest.approx(stage["quote_age_seconds"], abs=1e-9)
    for phase in ("opening", "closing"):
        for stage in report["latency"][phase]:
            assert stage["attribution"]["network_only"]["status"] == "UNKNOWN"
            assert stage["attribution"]["exchange_processing_only"]["fraction"] is None


def test_saved_source_intent_with_lost_response_stays_unresolved(tmp_path):
    fixture = restore(tmp_path, "003")
    rows = fixture["journals"]["opening.jsonl"]
    boundary = next(i for i, r in enumerate(rows) if r["event"] == "SOURCE_DISPATCH_INTENT")
    selected = rows[:boundary + 1]
    intent = selected[-1]
    selected.append({**intent, "sequence": intent["sequence"] + 1, "event": "SOURCE_DISPATCH_UNKNOWN", "payload": {"reason": "synthetic lost response after the saved incident intent"}})
    (tmp_path / "opening.jsonl").write_text("".join(json.dumps(r) + "\n" for r in selected))
    report = load_saved_cycle_report(tmp_path)
    assert report["inventory"]["status"] == "UNKNOWN"
    assert report["paired_execution"]["status"] == "UNKNOWN"
    assert report["order_state"]["unresolved_intents"]
    assert not report["paired_execution"]["receiver_dispatched"]


def test_last_observed_live_order_is_not_hidden_by_incomplete_completion(tmp_path):
    fixture = restore(tmp_path, "007")
    rows = fixture["journals"]["opening.jsonl"]
    boundary = next(i for i, r in enumerate(rows) if r["event"] == "ORDER_OBSERVED")
    (tmp_path / "opening.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows[:boundary + 1]))
    report = load_saved_cycle_report(tmp_path)
    state = report["order_state"]
    assert state["unresolved_observed_orders"]
    assert all(row["historical_only"] for row in state["unresolved_observed_orders"])
    assert state["unresolved_intents"]
    assert report["inventory"]["status"] == "UNKNOWN"


@pytest.mark.parametrize('event,key', [
    ('FALLBACK_ATTEMPT_EVIDENCE', 'account_index'),
    ('FALLBACK_RECONCILED', 'account_index'),
    ('SOURCE_DISPATCH_INTENT', 'plan'),
    ('RECEIVER_DISPATCH_INTENT', 'plan'),
])
@pytest.mark.parametrize('invalid', [[], {}, None])
def test_malformed_incident_identity_returns_incomplete_report(tmp_path, event, key, invalid):
    fixture = restore(tmp_path, '004')
    name = 'cycle.jsonl' if event.startswith('FALLBACK') else 'opening.jsonl'
    rows = fixture['journals'][name]
    row = next(r for r in rows if r['event'] == event)
    row['payload'][key] = invalid
    (tmp_path / name).write_text(''.join(json.dumps(r) + '\n' for r in rows))
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    report = load_saved_cycle_report(tmp_path)
    assert report['status'] == 'INCOMPLETE'
    assert report['inventory']['status'] == 'UNKNOWN'
    assert report['paired_execution']['status'] != 'SUCCESS'
    assert any(i['code'] == 'INVALID_EVIDENCE_TYPE' for i in report['issues'])
    assert report['dispatched_actions']
    assert before == {p.name: p.read_bytes() for p in tmp_path.iterdir()}
