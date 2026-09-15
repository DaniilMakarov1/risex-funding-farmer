from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from risex_spread_shadow.hood_handoff import (
    LocalAttemptInputs,
    Outcome,
    collect_local_attempt_inputs,
    preview_payload,
    run_local_attempt,
)
from risex_spread_shadow.hood_handoff import local_attempt
from risex_spread_shadow.hood_handoff.cli import main

from test_hood_handoff_paired_opening import Clock, PairedClient, metadata


def inputs(path: Path, **overrides) -> LocalAttemptInputs:
    values = {
        "market_symbol": "BTC",
        "quantity": Decimal("0.00020"),
        "direction": "LONG",
        "source_account_index": 11,
        "receiver_account_index": 22,
        "api_key_index": 4,
        "source_limit_price": Decimal("100.00"),
        "receiver_worst_price": Decimal("101.00"),
        "freshness_seconds": 10,
        "request_timeout_seconds": 1,
        "order_timeout_seconds": 1,
        "reconcile_timeout_seconds": 1,
        "poll_interval_seconds": 0.001,
        "max_poll_count": 2,
        "source_order_lifetime_seconds": 300,
        "client_order_prefix": "hcr9-local",
        "attempt_dir": path,
        "defer_incremental_margin_calculation": True,
    }
    values.update(overrides)
    return LocalAttemptInputs(**values)


class Secrets:
    def __init__(self, trace: list[str]) -> None:
        self.trace = trace

    def private_key(self, account_index: int, api_key_index: int) -> str:
        self.trace.append(f"secret:{account_index}:{api_key_index}")
        return f"synthetic-secret-{account_index}"

    def close(self) -> None:
        self.trace.append("secret-close")


class Reader:
    sdk_version = "1.1.2"

    def __init__(self, trace: list[str]) -> None:
        self.trace = trace

    async def resolve_market(self, symbol: str):
        self.trace.append(f"metadata:{symbol}")
        return metadata()


@pytest.mark.asyncio
async def test_local_attempt_runs_real_engine_against_synthetic_sdk_and_writes_packet(tmp_path):
    trace: list[str] = []
    client = PairedClient()

    result = await run_local_attempt(
        inputs(tmp_path / "attempt", quantity=Decimal("0.20")),
        execute=True,
        input_fn=lambda _: "LAUNCH",
        output_fn=lambda _: None,
        secret_provider_factory=lambda _: Secrets(trace),
        market_reader_factory=lambda _, __: Reader(trace),
        execution_client_factory=lambda _config, _inputs, _secrets: client,
        clock=Clock(),
    )

    assert result.status == "COMPLETED"
    assert result.exit_code == 0
    assert result.terminal_status == "RECORDED"
    assert result.result is not None and result.result.outcome is Outcome.SUCCESS
    assert trace[:3] == ["secret:11:4", "secret:22:4", "metadata:BTC"]
    assert [plan.side for plan in client.submissions] == ["SELL", "BUY"]

    attempt_dir = tmp_path / "attempt"
    packet = json.loads((attempt_dir / "attempt-packet.json").read_text(encoding="utf-8"))
    terminal = json.loads((attempt_dir / "terminal-result.json").read_text(encoding="utf-8"))
    exit_status = json.loads((attempt_dir / "exit-status.json").read_text(encoding="utf-8"))
    assert packet["source"]["sdk_required_version"] == "1.1.2"
    assert packet["source"]["source_fingerprint"].startswith("sha256:")
    assert packet["provenance"]["metadata"]["observed_at"] == 1000.0
    assert packet["config"]["market_id"] == 1
    assert packet["terminal"]["status"] == "RECORDED"
    assert terminal["terminal_status"] == "RECORDED"
    assert exit_status["exit_code"] == 0
    assert "synthetic-secret" not in "\n".join(path.read_text(encoding="utf-8") for path in attempt_dir.iterdir() if path.is_file())
    assert (attempt_dir / "intent.jsonl").exists()
    assert all(path.stat().st_mode & 0o077 == 0 for path in attempt_dir.iterdir() if path.is_file())


@pytest.mark.asyncio
async def test_preview_and_cancel_are_credential_and_network_free(tmp_path):
    calls: list[str] = []

    def forbidden_secret(_):
        calls.append("secret")
        raise AssertionError("preview/cancel must not access secrets")

    def forbidden_reader(*_):
        calls.append("metadata")
        raise AssertionError("preview/cancel must not resolve metadata")

    local_inputs = inputs(tmp_path / "attempt")
    preview = await run_local_attempt(
        local_inputs,
        execute=False,
        output_fn=lambda _: None,
        secret_provider_factory=forbidden_secret,
        market_reader_factory=forbidden_reader,
    )
    assert preview.status == "PREVIEW"
    assert not (tmp_path / "attempt").exists()

    cancelled = await run_local_attempt(
        local_inputs,
        execute=True,
        input_fn=lambda _: "CANCEL",
        output_fn=lambda _: None,
        secret_provider_factory=forbidden_secret,
        market_reader_factory=forbidden_reader,
    )
    assert cancelled.status == "CANCELLED"
    assert not (tmp_path / "attempt").exists()
    assert calls == []


def test_collector_prompts_every_omitted_price_and_timing_without_defaults(tmp_path):
    values = iter((
        "100.000",
        "101.000",
        "10",
        "1",
        "2",
        "3",
        "0.5",
        "7",
        "300",
        "hcr9-prefix",
    ))
    prompts: list[str] = []

    def ask(prompt: str) -> str:
        prompts.append(prompt)
        return next(values)

    collected = collect_local_attempt_inputs(
        market_symbol="BTC",
        quantity="0.00020",
        direction="LONG",
        source_account_index=27331,
        receiver_account_index=27337,
        api_key_index=4,
        attempt_dir=tmp_path / "attempt",
        input_fn=ask,
        defer_incremental_margin_calculation=True,
    )
    assert len(prompts) == 10
    assert collected.quantity == Decimal("0.00020")
    assert format(collected.source_limit_price, "f") == "100.000"
    assert collected.max_poll_count == 7
    assert collected.source_order_lifetime_seconds == 300


@pytest.mark.asyncio
async def test_same_directory_rerun_refuses_before_secrets_and_preserves_journal(tmp_path):
    path = tmp_path / "attempt"
    first = await run_local_attempt(
        inputs(path),
        execute=True,
        input_fn=lambda _: "LAUNCH",
        output_fn=lambda _: None,
        secret_provider_factory=lambda _: Secrets([]),
        market_reader_factory=lambda _, __: Reader([]),
        execution_client_factory=lambda _config, _inputs, _secrets: PairedClient(),
        clock=Clock(),
    )
    assert first.status == "COMPLETED"
    journal_before = (path / "intent.jsonl").read_text(encoding="utf-8")
    second = await run_local_attempt(
        inputs(path),
        execute=True,
        input_fn=lambda _: (_ for _ in ()).throw(AssertionError("rerun must refuse before launch input")),
        output_fn=lambda _: None,
        secret_provider_factory=lambda _: (_ for _ in ()).throw(AssertionError("rerun must not access secrets")),
    )
    assert second.status == "REFUSED"
    assert "inspect preserved journal" in (second.reason or "")
    assert (path / "intent.jsonl").read_text(encoding="utf-8") == journal_before


@pytest.mark.asyncio
async def test_missing_terminal_result_is_incomplete_and_secret_free(tmp_path):
    trace: list[str] = []

    async def crash(*_args, **_kwargs):
        raise RuntimeError("private_key=should-never-escape")

    result = await run_local_attempt(
        inputs(tmp_path / "crash"),
        execute=True,
        input_fn=lambda _: "LAUNCH",
        output_fn=lambda _: None,
        secret_provider_factory=lambda _: Secrets(trace),
        market_reader_factory=lambda _, __: Reader(trace),
        execution_client_factory=lambda _config, _inputs, _secrets: PairedClient(),
        run_engine=crash,
        clock=Clock(),
    )
    assert result.status == "INCOMPLETE"
    assert result.terminal_status == "MISSING"
    terminal_text = (tmp_path / "crash" / "terminal-result.json").read_text(encoding="utf-8")
    assert "should-never-escape" not in terminal_text
    terminal = json.loads(terminal_text)
    assert terminal["terminal_status"] == "MISSING"
    assert terminal["error_class"] == "sdk_error"


def test_preview_reports_exact_leg_notionals(tmp_path):
    preview = preview_payload(
        inputs(
            tmp_path / "preview",
            quantity=Decimal("0.00020"),
            source_limit_price=Decimal("80000"),
            receiver_worst_price=Decimal("80100"),
        )
    )
    expected_source = Decimal("0.00020") * Decimal("80000")
    expected_receiver = Decimal("0.00020") * Decimal("80100")
    assert format(expected_source, "f") == "16.00000"
    assert format(expected_receiver, "f") == "16.02000"
    assert preview["operation"]["source_limit_notional"] == "16.00000"
    assert preview["operation"]["receiver_worst_bound_notional"] == "16.02000"


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_file", ("exit-status.json", "attempt-packet.json"))
async def test_recorded_terminal_survives_packet_finalization_failure(tmp_path, monkeypatch, failed_file):
    original_atomic_json = local_attempt._atomic_json

    def fail_final_packet(path, value):
        path_name = Path(path).name
        final_packet = path_name == "attempt-packet.json" and value.get("terminal", {}).get("status") == "RECORDED"
        final_exit = path_name == "exit-status.json" and value.get("status") == "RECORDED"
        if path_name == failed_file and (final_packet or final_exit):
            raise OSError("private_key=must-not-escape")
        original_atomic_json(path, value)

    monkeypatch.setattr(local_attempt, "_atomic_json", fail_final_packet)
    attempt_path = tmp_path / failed_file.replace(".json", "")
    result = await run_local_attempt(
        inputs(attempt_path, quantity=Decimal("0.20")),
        execute=True,
        input_fn=lambda _: "LAUNCH",
        output_fn=lambda _: None,
        secret_provider_factory=lambda _: Secrets([]),
        market_reader_factory=lambda _, __: Reader([]),
        execution_client_factory=lambda _config, _inputs, _secrets: PairedClient(),
        clock=Clock(),
    )

    assert result.status == "INCOMPLETE"
    assert result.exit_code == 2
    assert result.terminal_status == "RECORDED"
    assert result.result is not None and result.result.outcome is Outcome.SUCCESS
    assert "diagnostic packet finalization incomplete" in (result.reason or "")
    terminal = json.loads((attempt_path / "terminal-result.json").read_text(encoding="utf-8"))
    exit_status = json.loads((attempt_path / "exit-status.json").read_text(encoding="utf-8"))
    assert terminal["terminal_status"] == "RECORDED"
    assert terminal["outcome"] == "SUCCESS"
    assert exit_status["status"] == "INCOMPLETE"
    assert exit_status["exit_code"] == 2
    assert "must-not-escape" not in (result.reason or "")


@pytest.mark.asyncio
async def test_initial_packet_failure_stops_before_secrets_or_external_calls(tmp_path, monkeypatch):
    calls: list[str] = []

    def fail_atomic(_path, _value):
        raise OSError("private_key=must-not-escape")

    def forbidden_secret(_inputs):
        calls.append("secret")
        raise AssertionError("secret provider must not be created")

    def forbidden_reader(*_args):
        calls.append("reader")
        raise AssertionError("market reader must not be created")

    monkeypatch.setattr(local_attempt, "_atomic_json", fail_atomic)
    attempt_path = tmp_path / "initial-write-failure"
    result = await run_local_attempt(
        inputs(attempt_path),
        execute=True,
        input_fn=lambda _: "LAUNCH",
        output_fn=lambda _: None,
        secret_provider_factory=forbidden_secret,
        market_reader_factory=forbidden_reader,
    )

    assert result.status == "INCOMPLETE"
    assert result.exit_code == 2
    assert result.terminal_status == "MISSING"
    assert "initial diagnostic packet failed before secrets" in (result.reason or "")
    assert "must-not-escape" not in (result.reason or "")
    assert calls == []
    assert (attempt_path / local_attempt.CLAIM_NAME).exists()
    assert not (attempt_path / local_attempt.PACKET_NAME).exists()


def test_cli_local_attempt_direct_flags_stays_offline_without_execute(tmp_path, capsys):
    assert main([
        "local-attempt",
        "--market-symbol", "BTC",
        "--quantity", "0.00020",
        "--direction", "LONG",
        "--source-account-index", "27331",
        "--receiver-account-index", "27337",
        "--api-key-index", "4",
        "--source-limit-price", "100.00",
        "--receiver-worst-price", "101.00",
        "--freshness-seconds", "10",
        "--request-timeout-seconds", "1",
        "--order-timeout-seconds", "2",
        "--reconcile-timeout-seconds", "3",
        "--poll-interval-seconds", "0.5",
        "--max-poll-count", "7",
        "--source-order-lifetime-seconds", "300",
        "--client-order-prefix", "hcr9-prefix",
        "--attempt-dir", str(tmp_path / "attempt"),
        "--defer-incremental-margin-calculation",
    ]) == 0
    output = capsys.readouterr().out
    assert "OWNER_LOCAL_PENDING_LAUNCH" in output
    assert "no SDK import" in output
    assert not (tmp_path / "attempt").exists()
