from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from pathlib import Path

import pytest

from risex_spread_shadow.hood_handoff import (
    AutomaticPriceProposal,
    ContractError,
    DEFAULT_FRESHNESS_SECONDS,
    DEFAULT_MAX_POLL_COUNT,
    DEFAULT_ORDER_TIMEOUT_SECONDS,
    DEFAULT_POLL_INTERVAL_SECONDS,
    DEFAULT_RECONCILE_TIMEOUT_SECONDS,
    DEFAULT_REQUEST_TIMEOUT_SECONDS,
    DEFAULT_SOURCE_ORDER_LIFETIME_SECONDS,
    DepthLevel,
    Direction,
    LocalAttemptInputs,
    OrderBookSnapshot,
    Outcome,
    select_automatic_prices,
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


def test_collector_uses_automatic_prices_and_declared_timing_defaults(tmp_path):
    prompts: list[str] = []

    def ask(prompt: str) -> str:
        prompts.append(prompt)
        return "hcr11-prefix"

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
    assert len(prompts) == 1
    assert collected.quantity == Decimal("0.00020")
    assert collected.automatic_price_selection is True
    assert collected.source_limit_price is None
    assert collected.receiver_worst_price is None
    assert collected.freshness_seconds == DEFAULT_FRESHNESS_SECONDS
    assert collected.request_timeout_seconds == DEFAULT_REQUEST_TIMEOUT_SECONDS
    assert collected.order_timeout_seconds == DEFAULT_ORDER_TIMEOUT_SECONDS
    assert collected.reconcile_timeout_seconds == DEFAULT_RECONCILE_TIMEOUT_SECONDS
    assert collected.poll_interval_seconds == DEFAULT_POLL_INTERVAL_SECONDS
    assert collected.max_poll_count == DEFAULT_MAX_POLL_COUNT
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


def test_cli_automatic_preview_is_concise_and_keeps_fixed_parameters_visible(tmp_path, capsys):
    assert main([
        "local-attempt",
        "--market-symbol", "BTC",
        "--quantity", "0.00020",
        "--direction", "LONG",
        "--source-account-index", "27331",
        "--receiver-account-index", "27337",
        "--api-key-index", "4",
        "--client-order-prefix", "hcr12-prefix",
        "--attempt-dir", str(tmp_path / "automatic"),
        "--defer-incremental-margin-calculation",
    ]) == 0
    output = capsys.readouterr().out
    assert "OWNER_LOCAL_PENDING_LAUNCH" in output
    assert "prices=UNRESOLVED" in output
    assert "venue=robinhood-chain" in output
    assert "api_base_url=https://api.rh.lighter.xyz" in output
    assert "signing_domain=robinhood-chain chain_id=466324" in output
    assert "source_leg=SELL LIMIT POST_ONLY" in output
    assert "receiver_leg=BUY MARKET IOC" in output
    assert "auth_lifetime:600s" in output
    assert "margin_mode=DEFERRED" in output
    assert "{" not in output
    assert "bids" not in output
    assert "no SDK import" in output
    assert not (tmp_path / "automatic").exists()


def _auto_book(*, bid: str = "99.0", ask: str = "100.0", observed_at: float = 1000.0) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        market_id=1,
        symbol="BTC",
        bids=(DepthLevel(Decimal(bid), Decimal("1")),),
        asks=(DepthLevel(Decimal(ask), Decimal("1")),),
        observed_at=observed_at,
        market_type="perp",
        venue="robinhood",
    )


def _auto_book_mapping(*, market_id: object = 1) -> dict[str, object]:
    return {
        "market_id": market_id,
        "symbol": "BTC",
        "market_type": "perp",
        "venue": "robinhood",
        "observed_at": 1000.0,
        "bids": [{"price": "99.0", "quantity": "1"}],
        "asks": [{"price": "100.0", "quantity": "1"}],
    }


def _auto_metadata_mapping(*, market_id: object = 1, observed_at: float = 1000.0) -> dict[str, object]:
    return {
        "market_id": market_id,
        "symbol": "BTC",
        "status": "active",
        "price_decimals": 1,
        "size_decimals": 2,
        "minimum_base_amount": "0.10",
        "minimum_quote_amount": "10",
        "observed_at": observed_at,
        "market_type": "perp",
        "venue": "robinhood",
    }


def test_automatic_price_selector_uses_one_tick_and_mirrors_for_both_directions():
    long_proposal = select_automatic_prices(
        Direction.LONG,
        metadata(),
        _auto_book(),
        quantity=Decimal("0.20"),
        now=1000.0,
    )
    short_proposal = select_automatic_prices(
        Direction.SHORT,
        metadata(),
        _auto_book(),
        quantity=Decimal("0.20"),
        now=1000.0,
    )
    assert isinstance(long_proposal, AutomaticPriceProposal)
    assert long_proposal.source_limit_price == Decimal("99.9")
    assert long_proposal.receiver_worst_price == Decimal("99.9")
    assert short_proposal.source_limit_price == Decimal("99.1")
    assert short_proposal.receiver_worst_price == Decimal("99.1")
    assert long_proposal.used_tick_adjustment is True
    assert short_proposal.used_tick_adjustment is True

    one_tick_book = _auto_book(bid="99.9", ask="100.0")
    tight_long = select_automatic_prices(
        Direction.LONG,
        metadata(),
        one_tick_book,
        quantity=Decimal("0.20"),
        now=1000.0,
    )
    tight_short = select_automatic_prices(
        Direction.SHORT,
        metadata(),
        one_tick_book,
        quantity=Decimal("0.20"),
        now=1000.0,
    )
    assert tight_long.source_limit_price == Decimal("100.0")
    assert tight_short.source_limit_price == Decimal("99.9")
    assert tight_long.used_tick_adjustment is False
    assert tight_short.used_tick_adjustment is False


@pytest.mark.parametrize("market_id", (True, 1.5))
def test_automatic_price_selector_rejects_bool_and_lossy_mapping_identity(market_id):
    with pytest.raises(ContractError, match="market_id is malformed"):
        select_automatic_prices(
            Direction.LONG,
            metadata(),
            _auto_book_mapping(market_id=market_id),
            quantity=Decimal("0.20"),
            now=1000.0,
        )


@pytest.mark.parametrize("market_id", (1, "1"))
def test_automatic_price_selector_keeps_exact_integer_mapping_identity_compatibility(market_id):
    proposal = select_automatic_prices(
        Direction.LONG,
        metadata(),
        _auto_book_mapping(market_id=market_id),
        quantity=Decimal("0.20"),
        now=1000.0,
    )
    assert proposal.source_limit_price == Decimal("99.9")


@pytest.mark.parametrize(
    "book, error",
    (
        (_auto_book(ask="99.0"), "crossed"),
        (OrderBookSnapshot(1, "BTC", (), (DepthLevel(Decimal("100.0"), Decimal("1")),), 1000.0, "perp", "robinhood"), "both"),
        (OrderBookSnapshot(1, "BTC", (DepthLevel(Decimal("99.0"), Decimal("1")),), (), 1000.0, "perp", "robinhood"), "both"),
        (_auto_book(observed_at=989.0), "stale"),
        (OrderBookSnapshot(2, "BTC", (DepthLevel(Decimal("99.0"), Decimal("1")),), (DepthLevel(Decimal("100.0"), Decimal("1")),), 1000.0, "perp", "robinhood"), "identity"),
    ),
)
def test_automatic_price_selector_refuses_unusable_public_book(book, error):
    with pytest.raises(ContractError, match=error):
        select_automatic_prices(
            Direction.LONG,
            metadata(),
            book,
            quantity=Decimal("0.20"),
            now=1000.0,
        )


def test_automatic_collector_applies_declared_defaults_without_numeric_prompts(tmp_path):
    prompts: list[str] = []

    def forbidden(prompt: str) -> str:
        prompts.append(prompt)
        raise AssertionError("automatic collector must not prompt for omitted prices or timing")

    collected = collect_local_attempt_inputs(
        market_symbol="BTC",
        quantity="0.00020",
        direction="LONG",
        source_account_index=27331,
        receiver_account_index=27337,
        api_key_index=4,
        attempt_dir=tmp_path / "automatic",
        client_order_prefix="hcr11",
        defer_incremental_margin_calculation=True,
        automatic_price_selection=True,
        input_fn=forbidden,
    )
    assert collected.automatic_price_selection is True
    assert collected.source_limit_price is None
    assert collected.receiver_worst_price is None
    assert collected.freshness_seconds == DEFAULT_FRESHNESS_SECONDS
    assert collected.request_timeout_seconds == DEFAULT_REQUEST_TIMEOUT_SECONDS
    assert collected.order_timeout_seconds == DEFAULT_ORDER_TIMEOUT_SECONDS
    assert collected.reconcile_timeout_seconds == DEFAULT_RECONCILE_TIMEOUT_SECONDS
    assert collected.poll_interval_seconds == DEFAULT_POLL_INTERVAL_SECONDS
    assert collected.max_poll_count == DEFAULT_MAX_POLL_COUNT
    assert collected.source_order_lifetime_seconds == DEFAULT_SOURCE_ORDER_LIFETIME_SECONDS
    assert prompts == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("direction", "expected_price", "expected_notional"),
    (
        ("LONG", Decimal("99.9"), "19.980"),
        ("SHORT", Decimal("99.1"), "19.820"),
    ),
)
async def test_automatic_attempt_loads_keys_then_selects_one_fresh_book(
    tmp_path,
    direction,
    expected_price,
    expected_notional,
):
    trace: list[str] = []

    class AutoSecrets:
        def private_key(self, account_index: int, api_key_index: int) -> str:
            trace.append(f"secret:{account_index}:{api_key_index}")
            return "synthetic-secret"

        def close(self) -> None:
            trace.append("secret-close")

    class AutoReader:
        sdk_version = "1.1.2"

        async def resolve_market(self, symbol: str):
            trace.append(f"metadata:{symbol}")
            return metadata()

        async def order_book_snapshot(self, market_id: int):
            trace.append(f"book:{market_id}")
            return _auto_book()

    path = tmp_path / "automatic"
    local_inputs = inputs(
        path,
        direction=direction,
        quantity=Decimal("0.20"),
        source_limit_price=None,
        receiver_worst_price=None,
    )
    client = PairedClient()
    console: list[str] = []
    result = await run_local_attempt(
        local_inputs,
        execute=True,
        input_fn=lambda _: "LAUNCH",
        output_fn=console.append,
        secret_provider_factory=lambda _: AutoSecrets(),
        market_reader_factory=lambda *_: AutoReader(),
        execution_client_factory=lambda *_: client,
        clock=Clock(),
    )

    assert result.status == "COMPLETED"
    assert [plan.price for plan in client.submissions] == [expected_price, expected_price]
    assert trace[:4] == ["secret:11:4", "secret:22:4", "metadata:BTC", "book:1"]
    assert trace[-1] == "secret-close"
    assert len(console) == 2
    assert "UNRESOLVED" in console[0]
    assert "bids" not in console[0]
    assert "POST_LAUNCH_PRICE_SELECTED" in console[1]
    assert f"price={expected_price}" in console[1]
    assert f"notional={expected_notional}" in console[1]
    packet = json.loads((path / "attempt-packet.json").read_text())
    assert packet["provenance"]["proposal"]["status"] == "UNRESOLVED_BEFORE_LAUNCH"
    assert packet["provenance"]["proposal"]["prices"] is None
    assert packet["provenance"]["final"]["prices"]["source_limit_price"] == format(expected_price, "f")
    assert packet["execution"]["state"] == "TERMINAL_RECORDED"


@pytest.mark.asyncio
async def test_automatic_price_change_after_launch_is_selected_without_old_expiry_gate(tmp_path):
    class AutoSecrets:
        def private_key(self, account_index: int, api_key_index: int) -> str:
            return "synthetic-secret"

        def close(self) -> None:
            return None

    class ChangedReader:
        sdk_version = "1.1.2"

        async def resolve_market(self, symbol: str):
            return metadata()

        async def order_book_snapshot(self, market_id: int):
            return _auto_book(ask="100.2")

    client = PairedClient()
    result = await run_local_attempt(
        inputs(
            tmp_path / "changed",
            quantity=Decimal("0.20"),
            source_limit_price=None,
            receiver_worst_price=None,
        ),
        execute=True,
        input_fn=lambda _: "LAUNCH",
        output_fn=lambda _: None,
        secret_provider_factory=lambda _: AutoSecrets(),
        market_reader_factory=lambda *_: ChangedReader(),
        execution_client_factory=lambda *_: client,
        clock=Clock(),
    )
    assert result.status == "COMPLETED"
    assert [plan.price for plan in client.submissions] == [Decimal("100.1"), Decimal("100.1")]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("reader_error", "reason_code"),
    (
        ("stale", "STALE_PUBLIC_OBSERVATION"),
        ("invalid", "INVALID_PUBLIC_OBSERVATION"),
        ("read", "PUBLIC_READ_FAILED"),
        ("transport-stale-text", "PUBLIC_READ_FAILED"),
    ),
)
async def test_automatic_invalid_or_stale_actual_evidence_is_a_known_pre_execution_stop(
    tmp_path,
    reader_error,
    reason_code,
):
    class AutoSecrets:
        def private_key(self, account_index: int, api_key_index: int) -> str:
            return "synthetic-secret"

        def close(self) -> None:
            return None

    class FailingReader:
        sdk_version = "1.1.2"

        async def resolve_market(self, symbol: str):
            if reader_error in {"read", "transport-stale-text"}:
                message = (
                    "synthetic stale connection while reading public metadata"
                    if reader_error == "transport-stale-text"
                    else "synthetic public metadata read failed"
                )
                raise RuntimeError(message)
            return metadata()

        async def order_book_snapshot(self, market_id: int):
            if reader_error == "stale":
                return _auto_book(observed_at=989.0)
            return {
                **_auto_book_mapping(market_id=2),
                "observed_at": 1000.0,
            }

    path = tmp_path / reader_error
    client = PairedClient()
    result = await run_local_attempt(
        inputs(
            path,
            quantity=Decimal("0.20"),
            source_limit_price=None,
            receiver_worst_price=None,
        ),
        execute=True,
        input_fn=lambda _: "LAUNCH",
        output_fn=lambda _: None,
        secret_provider_factory=lambda _: AutoSecrets(),
        market_reader_factory=lambda *_: FailingReader(),
        execution_client_factory=lambda *_: client,
        clock=Clock(),
    )
    assert result.status == "INCOMPLETE"
    assert result.terminal_status == "MISSING"
    assert result.reason_code == reason_code
    assert result.execution_state == "PRE_EXECUTION_STOP"
    assert result.as_dict()["known_pre_execution_stop"] is True
    assert client.submissions == []
    terminal = json.loads((path / "terminal-result.json").read_text())
    assert terminal["terminal_status"] == "MISSING"
    packet = json.loads((path / "attempt-packet.json").read_text())
    assert packet["execution"]["state"] == "PRE_EXECUTION_STOP"
    assert packet["execution"]["dispatch_status"] == "NOT_DISPATCHED_BY_THIS_ATTEMPT"
    assert packet["provenance"]["status"] == "FINAL_REVALIDATION_FAILED"


@pytest.mark.asyncio
async def test_engine_error_with_stale_text_remains_unknown_execution(tmp_path):
    class AutoSecrets:
        def private_key(self, account_index: int, api_key_index: int) -> str:
            return "synthetic-secret"

        def close(self) -> None:
            return None

    class Reader:
        sdk_version = "1.1.2"

        async def resolve_market(self, symbol: str):
            return metadata()

        async def order_book_snapshot(self, market_id: int):
            return _auto_book()

    async def engine_error(*_args, **_kwargs):
        raise RuntimeError("stale engine transport state")

    result = await run_local_attempt(
        inputs(
            tmp_path / "engine-error",
            quantity=Decimal("0.20"),
            source_limit_price=None,
            receiver_worst_price=None,
        ),
        execute=True,
        input_fn=lambda _: "LAUNCH",
        output_fn=lambda _: None,
        secret_provider_factory=lambda _: AutoSecrets(),
        market_reader_factory=lambda *_: Reader(),
        execution_client_factory=lambda *_: PairedClient(),
        run_engine=engine_error,
        clock=Clock(),
    )
    assert result.status == "INCOMPLETE"
    assert result.reason_code == "TERMINAL_RESULT_MISSING"
    assert result.execution_state == "UNKNOWN_EXECUTION"
    assert result.as_dict()["known_pre_execution_stop"] is False
    assert "stopped before engine dispatch" not in (result.reason or "")
    packet = json.loads((tmp_path / "engine-error" / "attempt-packet.json").read_text())
    assert packet["execution"]["state"] == "UNKNOWN_EXECUTION"
    assert packet["execution"]["dispatch_status"] == "UNKNOWN"


@pytest.mark.asyncio
async def test_interruption_after_engine_admission_leaves_unknown_packet_state(tmp_path):
    class AutoSecrets:
        def private_key(self, account_index: int, api_key_index: int) -> str:
            return "synthetic-secret"

        def close(self) -> None:
            return None

    class Reader:
        sdk_version = "1.1.2"

        async def resolve_market(self, symbol: str):
            return metadata()

        async def order_book_snapshot(self, market_id: int):
            return _auto_book()

    async def interrupted(*_args, **_kwargs):
        raise asyncio.CancelledError()

    path = tmp_path / "interrupted"
    with pytest.raises(asyncio.CancelledError):
        await run_local_attempt(
            inputs(path, quantity=Decimal("0.20")),
            execute=True,
            input_fn=lambda _: "LAUNCH",
            output_fn=lambda _: None,
            secret_provider_factory=lambda _: AutoSecrets(),
            market_reader_factory=lambda *_: Reader(),
            execution_client_factory=lambda *_: PairedClient(),
            run_engine=interrupted,
            clock=Clock(),
        )
    packet = json.loads((path / "attempt-packet.json").read_text())
    exit_status = json.loads((path / "exit-status.json").read_text())
    assert packet["execution"]["state"] == "UNKNOWN_EXECUTION"
    assert packet["execution"]["dispatch_status"] == "UNKNOWN"
    assert exit_status["execution_state"] == "UNKNOWN_EXECUTION"
    assert not (path / "terminal-result.json").exists()


@pytest.mark.asyncio
async def test_result_formatter_shows_engine_outcome_and_known_inventory(tmp_path):
    client = PairedClient(source_fills_before_receiver=True)
    result = await run_local_attempt(
        inputs(tmp_path / "engine-failure", quantity=Decimal("0.20")),
        execute=True,
        input_fn=lambda _: "LAUNCH",
        output_fn=lambda _: None,
        secret_provider_factory=lambda _: Secrets([]),
        market_reader_factory=lambda _, __: Reader([]),
        execution_client_factory=lambda *_: client,
        clock=Clock(),
    )
    output = local_attempt.format_local_attempt_result(result)
    assert "LOCAL_ATTEMPT status=COMPLETED terminal=RECORDED" in output
    assert "engine outcome=UNKNOWN phase=COMPLETE" in output
    assert "engine_reason=source fill observed before receiver dispatch" in output
    assert "inventory=source:position_after=-0.20,filled=0; receiver:position_after=0,filled=0" in output
