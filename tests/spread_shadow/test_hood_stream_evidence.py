from __future__ import annotations

import asyncio
import json
from pathlib import Path
import time

import pytest

from risex_spread_shadow.hood_handoff.stream_evidence import StreamEvidenceJournal
from risex_spread_shadow.hood_handoff.stream_measurement import StreamIdentity
from risex_spread_shadow.hood_handoff.stream_state import ReadStreamState
from risex_spread_shadow.hood_handoff.stream_timeline import compare_saved_stream


def order(at: float, *, status: str = "open", filled: str = "0",
          remaining: str = "0.0002", order_id: str = "123") -> dict[str, object]:
    return {"kind": "order", "epoch": 0, "received_at": at,
            "account_index": 27331, "market_id": 1, "client_order_index": 99,
            "order_id": order_id, "status": status,
            "filled_base_amount": filled, "remaining_base_amount": remaining,
            "terminal": status in {"filled", "canceled"}, "stream_complete": False,
            "auth": "must-never-appear", "raw": "must-never-appear"}


def read_rows(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_saved_timeline_preserves_early_nonterminal_partial_terminal_and_rest(tmp_path):
    async def scenario():
        path = tmp_path / "stream-events.jsonl"
        sink = StreamEvidenceJournal(path, market_id=1, accounts=(27331, 27337))
        await sink.start()
        sink.offer({"kind": "book_update", "epoch": 0, "received_at": 9.0})
        context = {"run_id": "run1", "phase": "PAIRED_OPENING", "role": "source",
                   "attempt_index": 1, "private_key": "must-never-appear"}
        sink.offer_milestone("prepare_done", account=27331, client=99,
                             order_id=None, at=9.8, context=context)
        sink.offer_milestone("send_entered", account=27331, client=99,
                             order_id=None, at=9.9, context=context)
        sink.offer(order(10.0), context)
        sink.offer_milestone("ack_parsed", account=27331, client=99,
                             order_id="123", at=10.2, context=context)
        sink.offer(order(10.3, status="partial", filled="0.0001", remaining="0.0001"), context)
        sink.offer({"kind": "order_duplicate", "epoch": 0, "received_at": 10.4,
                    "account_index": 27331, "client_order_index": 99}, context)
        sink.offer_milestone("exact_rest_observed", account=27331, client=99,
                             order_id="123", at=10.5, context=context)
        sink.offer(order(10.6, status="filled", filled="0.0002", remaining="0"), context)
        await sink.close(reason="stopped")
        rows = read_rows(path)
        assert [row["kind"] for row in rows].count("order") == 3
        assert any(row["kind"] == "order_duplicate" for row in rows)
        assert "must-never-appear" not in path.read_text()
        assert rows[-1]["kind"] == "session_end" and rows[-1]["dropped"] == 0
        timeline = compare_saved_stream(path)
        assert timeline["terminal_present"] and timeline["transport_complete"]
        result, = timeline["orders"]
        assert result["ws_statuses"] == ["open", "partial", "filled"]
        assert result["first_ws_monotonic_seconds"] == 10.0
        assert result["first_rest_monotonic_seconds"] == 10.5
        assert result["ws_lead_seconds_when_first"] == pytest.approx(0.5)
        assert result["ws_event_before_ack"] is True
        assert result["ws_executable_admission_proof"] is False

    asyncio.run(scenario())


def test_stream_state_retains_nonterminal_duplicate_and_terminal_after_disconnect(tmp_path):
    async def scenario():
        identity = StreamIdentity.from_config({
            "market_id": 1, "market_symbol": "BTC",
            "source_account_index": 27331, "receiver_account_index": 27337,
            "api_key_index": 4, "environment": "robinhood",
            "api_base_url": "https://api.rh.lighter.xyz", "chain_id": 466324,
        })
        path = tmp_path / "stream-events.jsonl"
        sink = StreamEvidenceJournal(path, market_id=1, accounts=identity.accounts)
        state = ReadStreamState(identity, evidence=sink)
        await sink.start()
        await state.connected_now()
        state.bind_order(27331, 1, 99, run_id="run1", phase="PAIRED_OPENING",
                         role="source", attempt_index=1)

        def frame(status, filled, remaining):
            return json.dumps({"type": "update/account_all_orders",
                               "channel": "account_all_orders:27331",
                               "orders": {"1": [{"owner_account_index": 27331,
                                                 "market_index": 1, "client_order_index": 99,
                                                 "order_id": "123", "status": status,
                                                 "filled_base_amount": filled,
                                                 "remaining_base_amount": remaining,
                                                 "auth": "must-never-appear"}]}}).encode()

        now = time.monotonic()
        await state.feed(frame("open", "0", "0.0002"), now - 0.2)
        await state.feed(frame("open", "0", "0.0002"), now - 0.1)
        await state.feed(frame("filled", "0.0002", "0"), now)
        assert await state.wait_terminal_hint(27331, 99, "123", 0.01)
        await state.disconnected()
        assert state.exact_orders == {}
        await sink.close(reason="transport_error")
        rows = read_rows(path)
        assert [row["kind"] for row in rows].count("order") == 2
        assert [row["kind"] for row in rows].count("order_duplicate") == 1
        assert all(row.get("run_id") == "run1" for row in rows if row["kind"] == "order")
        assert "must-never-appear" not in path.read_text()

    asyncio.run(scenario())


def test_stream_evidence_flush_failure_is_explicit(tmp_path):
    async def scenario():
        sink = StreamEvidenceJournal(tmp_path / "stream-events.jsonl",
                                     market_id=1, accounts=(27331, 27337))
        await sink.start()
        def broken(_fd, _row):
            raise OSError("synthetic disk failure")
        sink._write = broken
        sink.offer(order(10.0))
        with pytest.raises(OSError):
            await sink.close(reason="stopped")
        assert sink.failed
        assert not compare_saved_stream(sink.path)["terminal_present"]

    asyncio.run(scenario())


def test_rest_first_and_never_delivered_do_not_claim_negative_or_zero_lead(tmp_path):
    async def scenario():
        path = tmp_path / "stream-events.jsonl"
        sink = StreamEvidenceJournal(path, market_id=1, accounts=(27331, 27337))
        await sink.start()
        sink.offer_milestone("exact_rest_observed", account=27331, client=99,
                             order_id="123", at=10.0)
        sink.offer(order(10.2))
        sink.offer_milestone("send_entered", account=27337, client=100,
                             order_id=None, at=11.0)
        await sink.close(reason="transport_error")
        rows = compare_saved_stream(path)["orders"]
        first = next(row for row in rows if row["order_id"] == "123")
        missing = next(row for row in rows if row["client_order_index"] == 100)
        assert first["visibility_order"] == "REST_FIRST"
        assert first["ws_lead_seconds_when_first"] is None
        assert missing["visibility_order"] == "UNMEASURED"
        assert missing["first_ws_monotonic_seconds"] is None
        assert not compare_saved_stream(path)["transport_complete"]

    asyncio.run(scenario())


def test_overflow_invalid_identity_redaction_and_missing_terminal(tmp_path):
    async def scenario():
        path = tmp_path / "stream-events.jsonl"
        sink = StreamEvidenceJournal(path, market_id=1, accounts=(27331, 27337),
                                     max_records=1)
        await sink.start()
        sink.offer(order(10.0))
        sink.offer(order(10.1, order_id="124"))
        sink.offer({**order(10.2), "account_index": 999})
        sink.offer({**order(10.3), "status": "PRIVATE_SECRET_" * 8})
        summary = await sink.close(reason="stopped")
        assert summary["accepted"] == 1 and summary["dropped"] == 3
        assert not compare_saved_stream(path)["transport_complete"]
        assert "PRIVATE_SECRET" not in path.read_text()
        incomplete = tmp_path / "incomplete.jsonl"
        incomplete.write_text('\n'.join(json.dumps(row) for row in read_rows(path)[:-1]) + '\n')
        assert not compare_saved_stream(incomplete)["terminal_present"]

    asyncio.run(scenario())


def test_stream_evidence_rejects_non_owner_directory(tmp_path):
    async def scenario():
        tmp_path.chmod(0o755)
        sink = StreamEvidenceJournal(tmp_path / "events.jsonl", market_id=1,
                                     accounts=(27331, 27337))
        with pytest.raises(RuntimeError, match="owner-only"):
            await sink.start()

    asyncio.run(scenario())
