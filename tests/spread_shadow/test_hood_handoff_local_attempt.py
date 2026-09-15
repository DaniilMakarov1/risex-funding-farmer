from __future__ import annotations

import json
from dataclasses import replace
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
async def test_automatic_attempt_revalidates_exact_quote_before_synthetic_engine(tmp_path):
    trace: list[str] = []

    class AutoSecrets:
        def private_key(self, account_index: int, api_key_index: int) -> str:
            trace.append(f"secret:{account_index}:{api_key_index}")
            return "synthetic-secret"

        def close(self) -> None:
            trace.append("secret-close")

    class AutoReader:
        sdk_version = "1.1.2"

        def __init__(
            self,
            *,
            changed: bool = False,
            final_observed_at: float = 1000.0,
            proposal_observed_at: float = 1000.0,
            final_bad_identity: bool = False,
            proposal_bad_identity: object | None = None,
            final_bad_metadata: bool = False,
            final_book_failure: bool = False,
        ) -> None:
            self.metadata_calls = 0
            self.book_calls = 0
            self.changed = changed
            self.final_observed_at = final_observed_at
            self.proposal_observed_at = proposal_observed_at
            self.final_bad_identity = final_bad_identity
            self.proposal_bad_identity = proposal_bad_identity
            self.final_bad_metadata = final_bad_metadata
            self.final_book_failure = final_book_failure

        async def resolve_market(self, symbol: str):
            trace.append(f"metadata:{symbol}")
            self.metadata_calls += 1
            if self.metadata_calls == 1:
                return replace(metadata(), observed_at=self.proposal_observed_at)
            if self.final_bad_metadata:
                return _auto_metadata_mapping(observed_at=self.final_observed_at, market_id=True)
            return replace(metadata(), observed_at=self.final_observed_at)

        async def order_book_snapshot(self, market_id: int):
            trace.append(f"book:{market_id}")
            self.book_calls += 1
            if self.final_book_failure and self.book_calls == 2:
                raise RuntimeError("synthetic partial public book read")
            ask = "100.2" if self.changed and self.book_calls == 2 else "100.0"
            observed_at = 1000.0 if self.book_calls == 1 else self.final_observed_at
            if self.proposal_bad_identity is not None and self.book_calls == 1:
                return {
                    **_auto_book_mapping(market_id=self.proposal_bad_identity),
                    "observed_at": observed_at,
                }
            if self.final_bad_identity and self.book_calls == 2:
                return {
                    **_auto_book_mapping(market_id=2),
                    "observed_at": observed_at,
                }
            return _auto_book(ask=ask, observed_at=observed_at)

    local_inputs = inputs(
        tmp_path / "automatic",
        quantity=Decimal("0.20"),
        source_limit_price=None,
        receiver_worst_price=None,
    )
    reader = AutoReader()
    client = PairedClient()
    proposal_output: list[str] = []
    result = await run_local_attempt(
        local_inputs,
        execute=True,
        input_fn=lambda _: "LAUNCH",
        output_fn=proposal_output.append,
        secret_provider_factory=lambda _: AutoSecrets(),
        market_reader_factory=lambda *_: reader,
        execution_client_factory=lambda *_: client,
        clock=Clock(),
    )
    assert result.status == "COMPLETED"
    assert [plan.price for plan in client.submissions] == [Decimal("99.9"), Decimal("99.9")]
    assert len(proposal_output) == 2
    assert "\"bids\"" not in proposal_output[1]
    assert json.loads(proposal_output[1])["timing"]["freshness_seconds"] == DEFAULT_FRESHNESS_SECONDS
    assert trace[:4] == ["metadata:BTC", "book:1", "secret:11:4", "secret:22:4"]
    assert trace[4:] == ["metadata:BTC", "book:1", "secret-close"]
    packet = json.loads((tmp_path / "automatic" / "attempt-packet.json").read_text())
    assert packet["provenance"]["proposal"]["prices"]["source_limit_price"] == "99.9"
    assert packet["provenance"]["final"]["prices"]["source_limit_price"] == "99.9"

    changed_inputs = inputs(
        tmp_path / "changed",
        quantity=Decimal("0.20"),
        source_limit_price=None,
        receiver_worst_price=None,
    )
    changed_reader = AutoReader(changed=True)
    changed_client = PairedClient()
    changed = await run_local_attempt(
        changed_inputs,
        execute=True,
        input_fn=lambda _: "LAUNCH",
        output_fn=lambda _: None,
        secret_provider_factory=lambda _: AutoSecrets(),
        market_reader_factory=lambda *_: changed_reader,
        execution_client_factory=lambda *_: changed_client,
        clock=Clock(),
    )
    assert changed.status == "INCOMPLETE"
    assert changed_client.submissions == []
    changed_packet = json.loads((tmp_path / "changed" / "attempt-packet.json").read_text())
    assert changed_packet["provenance"]["status"] == "FINAL_REVALIDATION_FAILED"
    assert changed_packet["provenance"]["final"]["prices"]["source_limit_price"] == "100.1"

    class DelayedClock:
        def __init__(self) -> None:
            self.calls = 0

        def now(self) -> float:
            self.calls += 1
            return 1000.0 if self.calls == 1 else 1011.0

        async def sleep(self, seconds: float) -> None:
            return None

    delayed_reader = AutoReader(final_observed_at=1011.0)
    delayed_client = PairedClient()
    delayed = await run_local_attempt(
        inputs(
            tmp_path / "delayed",
            quantity=Decimal("0.20"),
            source_limit_price=None,
            receiver_worst_price=None,
        ),
        execute=True,
        input_fn=lambda _: "LAUNCH",
        output_fn=lambda _: None,
        secret_provider_factory=lambda _: AutoSecrets(),
        market_reader_factory=lambda *_: delayed_reader,
        execution_client_factory=lambda *_: delayed_client,
        clock=DelayedClock(),
    )
    assert delayed.status == "INCOMPLETE"
    assert delayed_client.submissions == []
    delayed_packet = json.loads((tmp_path / "delayed" / "attempt-packet.json").read_text())
    assert delayed_packet["provenance"]["status"] == "FINAL_REVALIDATION_FAILED"
    assert delayed_packet["provenance"]["proposal"]["prices"]["observed_at"] == 1000.0
    assert delayed_packet["provenance"]["final"]["prices"]["observed_at"] == 1011.0

    malformed_inputs = inputs(
        tmp_path / "malformed-final",
        quantity=Decimal("0.20"),
        source_limit_price=None,
        receiver_worst_price=None,
    )
    malformed_reader = AutoReader(final_bad_identity=True, proposal_observed_at=999.0)
    malformed_client = PairedClient()
    malformed = await run_local_attempt(
        malformed_inputs,
        execute=True,
        input_fn=lambda _: "LAUNCH",
        output_fn=lambda _: None,
        secret_provider_factory=lambda _: AutoSecrets(),
        market_reader_factory=lambda *_: malformed_reader,
        execution_client_factory=lambda *_: malformed_client,
        clock=Clock(),
    )
    assert malformed.status == "INCOMPLETE"
    assert malformed_client.submissions == []
    malformed_packet = json.loads((tmp_path / "malformed-final" / "attempt-packet.json").read_text())
    assert malformed_packet["provenance"]["status"] == "FINAL_REVALIDATION_FAILED"
    assert malformed_packet["provenance"]["final"]["book"]["market_id"] == 2
    assert malformed_packet["provenance"]["final"]["book"]["observed_at"] == 1000.0
    assert malformed_packet["provenance"]["final"]["prices"] is None

    invalid_metadata_path = tmp_path / "invalid-final-metadata"
    invalid_metadata_client = PairedClient()
    invalid_metadata = await run_local_attempt(
        inputs(
            invalid_metadata_path,
            quantity=Decimal("0.20"),
            source_limit_price=None,
            receiver_worst_price=None,
        ),
        execute=True,
        input_fn=lambda _: "LAUNCH",
        output_fn=lambda _: None,
        secret_provider_factory=lambda _: AutoSecrets(),
        market_reader_factory=lambda *_: AutoReader(final_bad_metadata=True),
        execution_client_factory=lambda *_: invalid_metadata_client,
        clock=Clock(),
    )
    assert invalid_metadata.status == "INCOMPLETE"
    assert invalid_metadata_client.submissions == []
    invalid_metadata_packet = json.loads(
        (invalid_metadata_path / "attempt-packet.json").read_text()
    )
    assert invalid_metadata_packet["provenance"]["status"] == "FINAL_REVALIDATION_FAILED"
    assert invalid_metadata_packet["provenance"]["final"]["metadata"]["market_id"] is True
    assert invalid_metadata_packet["provenance"]["final"]["book"] is None
    assert invalid_metadata_packet["provenance"]["final"]["prices"] is None

    partial_path = tmp_path / "partial-final-book"
    partial_client = PairedClient()
    partial = await run_local_attempt(
        inputs(
            partial_path,
            quantity=Decimal("0.20"),
            source_limit_price=None,
            receiver_worst_price=None,
        ),
        execute=True,
        input_fn=lambda _: "LAUNCH",
        output_fn=lambda _: None,
        secret_provider_factory=lambda _: AutoSecrets(),
        market_reader_factory=lambda *_: AutoReader(final_book_failure=True),
        execution_client_factory=lambda *_: partial_client,
        clock=Clock(),
    )
    assert partial.status == "INCOMPLETE"
    assert partial_client.submissions == []
    partial_packet = json.loads((partial_path / "attempt-packet.json").read_text())
    assert partial_packet["provenance"]["status"] == "FINAL_REVALIDATION_FAILED"
    assert partial_packet["provenance"]["final"]["metadata"]["observed_at"] == 1000.0
    assert partial_packet["provenance"]["final"]["book"] is None
    assert partial_packet["provenance"]["final"]["prices"] is None

    for malformed_identity in (True, 1.5):
        prelaunch_path = tmp_path / f"malformed-proposal-{malformed_identity}"
        before = len(trace)
        prelaunch = await run_local_attempt(
            inputs(
                prelaunch_path,
                quantity=Decimal("0.20"),
                source_limit_price=None,
                receiver_worst_price=None,
            ),
            execute=True,
            input_fn=lambda _: (_ for _ in ()).throw(AssertionError("invalid proposal must stop before LAUNCH")),
            output_fn=lambda _: None,
            secret_provider_factory=lambda _: (_ for _ in ()).throw(AssertionError("invalid proposal must stop before keys")),
            market_reader_factory=lambda *_: AutoReader(proposal_bad_identity=malformed_identity),
            execution_client_factory=lambda *_: (_ for _ in ()).throw(AssertionError("invalid proposal must stop before mutation client")),
            clock=Clock(),
        )
        assert prelaunch.status == "REFUSED"
        assert not prelaunch_path.exists()
        assert trace[before:] == ["metadata:BTC", "book:1"]
