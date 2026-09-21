from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from risex_spread_shadow.hood_handoff import (
    AccountSnapshot,
    ContractError,
    Direction,
    DurableJournal,
    HandoffConfig,
    HistoryPage,
    LighterSdkClient,
    MutationReceipt,
    OrderPlan,
    OrderSnapshot,
    Outcome,
    StaticSecretProvider,
    TradeReceipt,
    run_handoff,
    sanitize,
    sanitize_exception,
)
from risex_spread_shadow.hood_handoff.cli import PromptSecretProvider, _config, main
from risex_spread_shadow.hood_handoff.engine import HandoffEngine
from risex_spread_shadow.hood_handoff import journal as journal_module

from test_hood_handoff_engine import FakeClient, FakeClock, make_config
from test_hood_handoff_sdk_interface import FakeHttp, FakeModule, FakeSigner


def _json_config(path: Path, *, direction: str = "LONG") -> dict[str, object]:
    return {
        "market_id": 7,
        "direction": direction,
        "quantity": "0.125",
        "source_limit_price": "100.25",
        "receiver_worst_price": "101.25",
        "freshness_seconds": "10",
        "request_timeout_seconds": "1",
        "order_timeout_seconds": "1",
        "reconcile_timeout_seconds": "1",
        "poll_interval_seconds": "0.1",
        "max_poll_count": "2",
        "source_order_lifetime_seconds": "300",
        "client_order_prefix": "cli-correction",
        "journal_path": str(path),
        "api_base_url": "https://mainnet.zklighter.elliot.ai",
        "chain_id": 304,
        "api_key_index": 4,
    }


def test_cli_normalizes_exact_json_and_prints_reviewable_plan(tmp_path, capsys):
    config_path = tmp_path / "config.json"
    evidence_path = tmp_path / "evidence.json"
    config_path.write_text(json.dumps(_json_config(tmp_path / "journal.jsonl")), encoding="utf-8")
    evidence_path.write_text(json.dumps({"market_id": 7}), encoding="utf-8")

    assert main(
        [
            "run",
            "--config",
            str(config_path),
            "--market-evidence",
            str(evidence_path),
            "--source-account-index",
            "11",
            "--receiver-account-index",
            "22",
        ]
    ) == 0
    preview = json.loads(capsys.readouterr().out)
    assert preview["quantity"] == "0.125"
    assert preview["source_side"] == "SELL"
    assert preview["receiver_side"] == "BUY"
    assert preview["source_time_in_force"] == "POST_ONLY"
    assert preview["receiver_time_in_force"] == "IOC"
    assert preview["source_limit_price"] == "100.25"
    assert preview["receiver_worst_price"] == "101.25"
    assert "max_gross_notional" not in preview
    assert "source_fee_budget" not in preview
    assert "receiver_fee_budget" not in preview
    assert "receiver_price_cap" not in preview
    assert preview["plan_reviewed"] is False

    assert main(
        [
            "run",
            "--config",
            str(config_path),
            "--market-evidence",
            str(evidence_path),
            "--source-account-index",
            "11",
            "--receiver-account-index",
            "22",
            "--confirm-plan",
        ]
    ) == 0
    assert json.loads(capsys.readouterr().out)["plan_reviewed"] is True


@pytest.mark.asyncio
async def test_preview_receipt_marks_both_legs_undispatched(tmp_path):
    result = await run_handoff(
        make_config(tmp_path / "preview.jsonl", operator_execution_opt_in=False),
        FakeClient(),
        clock=FakeClock(),
    )
    assert result.outcome is Outcome.PREVIEW
    assert result.source is not None and result.receiver is not None
    assert result.source.dispatched is False
    assert result.receiver.dispatched is False
    assert result.as_dict()["source"]["dispatched"] is False
    assert result.as_dict()["receiver"]["dispatched"] is False


def test_cli_rejects_float_and_json_plan_review_flag_is_not_authority(tmp_path):
    raw = _json_config(tmp_path / "journal.jsonl")
    raw["quantity"] = 0.125
    with pytest.raises(SystemExit, match="exact decimal"):
        _config(raw, execute=False)

    raw = _json_config(tmp_path / "journal.jsonl")
    raw["operator_plan_reviewed"] = True
    config = _config(raw, execute=False)
    assert config.operator_plan_reviewed is False


def test_journal_claim_is_atomic_and_completed_attempt_cannot_replay(tmp_path):
    path = tmp_path / "evidence" / "attempt.jsonl"
    first = DurableJournal(path, run_id="first", clock=lambda: 1.0)
    second = DurableJournal(path, run_id="second", clock=lambda: 1.0)
    first.acquire_attempt()
    try:
        with pytest.raises(RuntimeError, match="already owned"):
            second.acquire_attempt()
    finally:
        first.release_attempt()
    second.acquire_attempt()
    second.release_attempt()
    lock_path = path.with_name(path.name + ".lock")
    assert lock_path.exists()
    assert lock_path.stat().st_mode & 0o777 == 0o600


def test_journal_refreshes_sequence_after_a_stale_engine_is_released(tmp_path):
    path = tmp_path / "evidence" / "sequence.jsonl"
    first = DurableJournal(path, run_id="first", clock=lambda: 1.0)
    second = DurableJournal(path, run_id="second", clock=lambda: 2.0)
    first.acquire_attempt()
    first.append("FIRST")
    first.release_attempt()
    second.acquire_attempt()
    try:
        second.append("SECOND")
    finally:
        second.release_attempt()
    events = second.events
    assert [event.sequence for event in events] == [1, 2]


def test_journal_completes_short_writes_and_retries_eintr(tmp_path, monkeypatch):
    path = tmp_path / "evidence" / "short-write.jsonl"
    journal = DurableJournal(path, run_id="short-write", clock=lambda: 1.0)
    journal.acquire_attempt()
    real_write = journal_module.os.write
    calls = 0

    def short_write(fd, value):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise InterruptedError()
        return real_write(fd, value[: max(1, len(value) // 2)])

    monkeypatch.setattr(journal_module.os, "write", short_write)
    try:
        event = journal.append("SOURCE_DISPATCH_INTENT", {"plan": {"order_id": "safe-id"}})
    finally:
        journal.release_attempt()

    assert event.sequence == 1
    assert journal.events[0].event == "SOURCE_DISPATCH_INTENT"
    assert calls >= 3


def test_journal_sequence_refresh_failure_releases_lock(tmp_path, monkeypatch):
    path = tmp_path / "evidence" / "sequence-refresh.jsonl"
    first = DurableJournal(path, run_id="first", clock=lambda: 1.0)

    def fail_refresh():
        raise RuntimeError("injected sequence refresh failure")

    monkeypatch.setattr(first, "_read_last_sequence", fail_refresh)
    with pytest.raises(RuntimeError, match="sequence refresh"):
        first.acquire_attempt()

    second = DurableJournal(path, run_id="second", clock=lambda: 2.0)
    second.acquire_attempt()
    second.release_attempt()


def test_journal_lock_metadata_baseexception_releases_lock(tmp_path, monkeypatch):
    path = tmp_path / "evidence" / "lock-write.jsonl"
    real_write = journal_module.os.write
    calls = 0

    def fail_once(fd, value):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise KeyboardInterrupt()
        return real_write(fd, value)

    monkeypatch.setattr(journal_module.os, "write", fail_once)
    first = DurableJournal(path, run_id="first", clock=lambda: 1.0)
    with pytest.raises(KeyboardInterrupt):
        first.acquire_attempt()
    monkeypatch.setattr(journal_module.os, "write", real_write)

    second = DurableJournal(path, run_id="second", clock=lambda: 2.0)
    second.acquire_attempt()
    second.release_attempt()


def test_journal_failed_append_preserves_existing_prefix(tmp_path, monkeypatch):
    path = tmp_path / "evidence" / "prefix.jsonl"
    journal = DurableJournal(path, run_id="prefix", clock=lambda: 1.0)
    journal.acquire_attempt()
    try:
        journal.append("PREFIX", {"value": "durable"})
        prefix = path.read_bytes()
        real_write = journal_module.os.write
        failed = False

        def partial_then_fail(fd, value):
            nonlocal failed
            if not failed:
                failed = True
                real_write(fd, value[:4])
                raise OSError("injected partial append failure")
            return real_write(fd, value)

        monkeypatch.setattr(journal_module.os, "write", partial_then_fail)
        with pytest.raises(OSError, match="partial append"):
            journal.append("BROKEN")
    finally:
        journal.release_attempt()

    assert path.read_bytes() == prefix + b'{"at'
    with pytest.raises(RuntimeError, match="journal is unreadable"):
        DurableJournal(path, run_id="reopen")


@pytest.mark.asyncio
async def test_journal_intent_write_failure_fails_closed_before_submit(tmp_path, monkeypatch):
    path = tmp_path / "intent-write-failure.jsonl"
    real_write = journal_module.os.write

    def fail_intent(fd, value):
        if b'"event":"SOURCE_DISPATCH_INTENT"' in value:
            raise OSError("injected intent write failure")
        return real_write(fd, value)

    monkeypatch.setattr(journal_module.os, "write", fail_intent)
    client = FakeClient()
    with pytest.raises(RuntimeError, match="write previously failed"):
        await run_handoff(make_config(path), client, clock=FakeClock())

    assert client.submissions == []


def test_journal_redacts_signed_payload_and_private_key_identifier():
    value = sanitize(
        {
            "tx_info": '{"L1Sig":"fixture-signed-payload"}',
            "signed_transaction_payload": "fixture-payload",
            "private_key_id": "fixture-private-id",
            "order_id": "145",
        }
    )
    encoded = json.dumps(value)
    assert "fixture-signed-payload" not in encoded
    assert "fixture-payload" not in encoded
    assert "fixture-private-id" not in encoded
    assert value["order_id"] == "145"


@pytest.mark.asyncio
async def test_uncertain_cancel_has_one_underlying_call(tmp_path):
    class CancelTwice(FakeClient):
        async def submit_order(self, plan):
            receipt = await super().submit_order(plan)
            if plan.reduce_only:
                self.source_order = replace(
                    self.source_order,
                    filled_quantity=Decimal("0.025"),
                    remaining_quantity=Decimal("0.100"),
                )
            return receipt

        async def cancel_order(self, account_index, market_id, order_id):
            self.cancellations.append(order_id)
            raise TimeoutError("synthetic response lost")

    client = CancelTwice()
    result = await run_handoff(make_config(tmp_path / "cancel.jsonl"), client, clock=FakeClock())
    assert result.outcome is Outcome.UNKNOWN
    assert client.cancellations == ["source-1"]
    events = [
        json.loads(line)
        for line in (tmp_path / "cancel.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert sum(event["event"] == "CANCEL_DISPATCH_INTENT" for event in events) == 1


@pytest.mark.asyncio
async def test_proven_pre_receiver_partial_is_partial_and_never_opens_receiver(tmp_path):
    class ProvenPartial(FakeClient):
        async def submit_order(self, plan):
            receipt = await super().submit_order(plan)
            if plan.reduce_only:
                self.source_order = replace(
                    self.source_order,
                    filled_quantity=Decimal("0.025"),
                    remaining_quantity=Decimal("0.100"),
                )
                self.source_position = Decimal("0.975")
            return receipt

        async def list_trades(self, account_index, market_id, *, order_id=None, cursor=None, limit=100):
            if account_index == self.source_account_index and order_id == "source-1":
                return HistoryPage(
                    trades=(
                        TradeReceipt(
                            "partial-source",
                            self.source_account_index,
                            7,
                            "source-1",
                            "SELL",
                            Decimal("0.025"),
                            Decimal("100.25"),
                            Decimal("0.0025"),
                            self.receiver_account_index,
                            1_000,
                        ),
                    )
                )
            return HistoryPage()

    client = ProvenPartial()
    result = await run_handoff(make_config(tmp_path / "partial.jsonl"), client, clock=FakeClock())
    assert result.outcome is Outcome.PARTIAL
    assert result.source.filled_quantity == Decimal("0.025")
    assert not [plan for plan in client.submissions if not plan.reduce_only]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "direction, fill_fraction",
    [
        (Direction.LONG, Decimal("1")),
        (Direction.LONG, Decimal("0.5")),
        (Direction.SHORT, Decimal("1")),
        (Direction.SHORT, Decimal("0.5")),
    ],
)
async def test_reconciled_source_only_external_fill_after_lookup_miss_is_known_partial(
    tmp_path,
    direction,
    fill_fraction,
):
    class ExternalSourceRace(FakeClient):
        def __init__(self):
            super().__init__(source_fills=True)
            self.source_position = Decimal("1") if direction is Direction.LONG else Decimal("-1")
            self.source_lookup_count = 0
            self.injected = False

        async def lookup_order(self, account_index, market_id, *, order_id=None, client_order_index=None):
            value = await super().lookup_order(
                account_index,
                market_id,
                order_id=order_id,
                client_order_index=client_order_index,
            )
            if account_index == self.source_account_index and order_id == "source-1":
                self.source_lookup_count += 1
                if self.source_lookup_count == 2 and not self.injected:
                    self.injected = True
                    assert self.source_order is not None
                    filled = self.source_order.initial_quantity * fill_fraction
                    status = "filled" if filled == self.source_order.initial_quantity else "canceled"
                    self.source_order = replace(
                        self.source_order,
                        status=status,
                        filled_quantity=filled,
                        remaining_quantity=self.source_order.initial_quantity - filled,
                    )
                    self.source_position += -filled if self.source_order.side == "SELL" else filled
                    return None
            return value

        async def list_trades(self, account_index, market_id, *, order_id=None, cursor=None, limit=100):
            page = await super().list_trades(
                account_index,
                market_id,
                order_id=order_id,
                cursor=cursor,
                limit=limit,
            )
            if account_index == self.source_account_index and order_id == "source-1" and self.source_order is not None:
                return replace(
                    page,
                    trades=tuple(
                        replace(trade, quantity=self.source_order.filled_quantity)
                        for trade in page.trades
                    ),
                )
            return page

    client = ExternalSourceRace()
    result = await run_handoff(
        make_config(
            tmp_path / f"source-only-{direction.value.lower()}-{fill_fraction}.jsonl",
            direction=direction,
        ),
        client,
        clock=FakeClock(),
    )

    assert result.outcome is Outcome.PARTIAL, result.as_dict()
    assert result.reason == "source order disappeared during pre-receiver recheck"
    assert result.source.filled_quantity == Decimal("0.125") * fill_fraction
    assert result.source.history_complete
    assert result.receiver.dispatched is False
    assert result.receiver.order is None
    assert result.receiver.trades == ()
    assert "source order disappeared during pre-receiver recheck" in result.unknown_reasons
    assert not [plan for plan in client.submissions if not plan.reduce_only]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "direction, fill_fraction, visibility",
    [
        (Direction.LONG, Decimal("1"), "terminal-lookup"),
        (Direction.LONG, Decimal("0.5"), "terminal-lookup"),
        (Direction.SHORT, Decimal("1"), "terminal-lookup"),
        (Direction.SHORT, Decimal("0.5"), "terminal-lookup"),
        (Direction.LONG, Decimal("1"), "account-visible"),
        (Direction.LONG, Decimal("0.5"), "account-visible"),
        (Direction.SHORT, Decimal("1"), "account-visible"),
        (Direction.SHORT, Decimal("0.5"), "account-visible"),
    ],
)
async def test_reconciled_external_fill_variants_are_known_partial(
    tmp_path,
    direction,
    fill_fraction,
    visibility,
):
    class SourceFillRace(FakeClient):
        def __init__(self):
            super().__init__(source_fills=True)
            self.source_position = Decimal("1") if direction is Direction.LONG else Decimal("-1")
            self.source_lookup_count = 0
            self.injected = False

        def _inject_fill(self) -> None:
            assert self.source_order is not None
            filled = self.source_order.initial_quantity * fill_fraction
            status = "filled" if filled == self.source_order.initial_quantity else "canceled"
            self.source_order = replace(
                self.source_order,
                status=status,
                filled_quantity=filled,
                remaining_quantity=self.source_order.initial_quantity - filled,
            )
            self.source_position += -filled if self.source_order.side == "SELL" else filled
            self.injected = True

        async def account_snapshot(self, account_index, market_id):
            value = await super().account_snapshot(account_index, market_id)
            if (
                visibility == "account-visible"
                and account_index == self.source_account_index
                and self.source_order is not None
                and not self.injected
            ):
                self._inject_fill()
                return await super().account_snapshot(account_index, market_id)
            return value

        async def lookup_order(self, account_index, market_id, *, order_id=None, client_order_index=None):
            value = await super().lookup_order(
                account_index,
                market_id,
                order_id=order_id,
                client_order_index=client_order_index,
            )
            if account_index == self.source_account_index and order_id == "source-1":
                self.source_lookup_count += 1
                if visibility == "terminal-lookup" and self.source_lookup_count == 2 and not self.injected:
                    self._inject_fill()
                    return self.source_order
            return value

        async def list_trades(self, account_index, market_id, *, order_id=None, cursor=None, limit=100):
            page = await super().list_trades(
                account_index,
                market_id,
                order_id=order_id,
                cursor=cursor,
                limit=limit,
            )
            if account_index == self.source_account_index and order_id == "source-1" and self.source_order is not None:
                return replace(
                    page,
                    trades=tuple(
                        replace(trade, quantity=self.source_order.filled_quantity)
                        for trade in page.trades
                    ),
                )
            return page

    client = SourceFillRace()
    result = await run_handoff(
        make_config(
            tmp_path / f"source-fill-{direction.value.lower()}-{fill_fraction}-{visibility}.jsonl",
            direction=direction,
        ),
        client,
        clock=FakeClock(),
    )

    assert result.outcome is Outcome.PARTIAL, result.as_dict()
    assert result.reason == "source fill or quantity change before receiver dispatch"
    assert result.source.filled_quantity == Decimal("0.125") * fill_fraction
    assert result.source.history_complete
    assert result.receiver.dispatched is False
    assert result.receiver.order is None
    assert result.receiver.trades == ()
    assert "source fill or quantity change before receiver dispatch" in result.unknown_reasons
    assert not [plan for plan in client.submissions if not plan.reduce_only]


@pytest.mark.asyncio
@pytest.mark.parametrize("direction", [Direction.LONG, Direction.SHORT])
async def test_stale_recheck_with_lookup_miss_remains_unknown(tmp_path, direction):
    class StaleSourceRace(FakeClient):
        def __init__(self):
            super().__init__(source_fills=True)
            self.source_position = Decimal("1") if direction is Direction.LONG else Decimal("-1")
            self.source_lookup_count = 0
            self.injected = False

        async def account_snapshot(self, account_index, market_id):
            value = await super().account_snapshot(account_index, market_id)
            if self.source_order is not None and not self.injected:
                return replace(value, observed_at=1_000.0 - 100)
            return value

        async def lookup_order(self, account_index, market_id, *, order_id=None, client_order_index=None):
            value = await super().lookup_order(
                account_index,
                market_id,
                order_id=order_id,
                client_order_index=client_order_index,
            )
            if account_index == self.source_account_index and order_id == "source-1":
                self.source_lookup_count += 1
                if self.source_lookup_count == 2 and not self.injected:
                    assert self.source_order is not None
                    filled = self.source_order.initial_quantity / Decimal("2")
                    self.source_order = replace(
                        self.source_order,
                        status="canceled",
                        filled_quantity=filled,
                        remaining_quantity=self.source_order.initial_quantity - filled,
                    )
                    self.source_position += -filled if self.source_order.side == "SELL" else filled
                    self.injected = True
                    return None
            return value

        async def list_trades(self, account_index, market_id, *, order_id=None, cursor=None, limit=100):
            page = await super().list_trades(
                account_index,
                market_id,
                order_id=order_id,
                cursor=cursor,
                limit=limit,
            )
            if account_index == self.source_account_index and order_id == "source-1" and self.source_order is not None:
                return replace(
                    page,
                    trades=tuple(
                        replace(trade, quantity=self.source_order.filled_quantity)
                        for trade in page.trades
                    ),
                )
            return page

    client = StaleSourceRace()
    result = await run_handoff(
        make_config(tmp_path / f"stale-{direction.value.lower()}.jsonl", direction=direction),
        client,
        clock=FakeClock(),
    )

    assert result.outcome is Outcome.UNKNOWN, result.as_dict()
    assert "source order disappeared during pre-receiver recheck" in result.unknown_reasons
    assert any("recheck is stale" in item for item in result.unknown_reasons)
    assert not [plan for plan in client.submissions if not plan.reduce_only]


@pytest.mark.asyncio
async def test_contradictory_terminal_order_cannot_be_success(tmp_path):
    class Contradictory(FakeClient):
        async def lookup_order(self, account_index, market_id, *, order_id=None, client_order_index=None):
            current = await super().lookup_order(
                account_index,
                market_id,
                order_id=order_id,
                client_order_index=client_order_index,
            )
            if current is not None and account_index == self.source_account_index and current.status == "filled":
                return replace(
                    current,
                    status="canceled",
                    filled_quantity=Decimal("0"),
                    remaining_quantity=Decimal("0.125"),
                )
            return current

    result = await run_handoff(
        make_config(tmp_path / "contradiction.jsonl"), Contradictory(source_fills=True), clock=FakeClock()
    )
    assert result.outcome is Outcome.UNKNOWN
    assert any("terminal order filled quantity conflicts" in item for item in result.unknown_reasons)
    assert result.source.filled_quantity == Decimal("0.125")


@pytest.mark.asyncio
async def test_fresh_delayed_snapshots_pass_after_acquisition(tmp_path):
    class TickClock:
        def __init__(self):
            self.value = 1_000.0

        def now(self):
            self.value += 0.001
            return self.value

        async def sleep(self, seconds):
            self.value += seconds

    class Delayed(FakeClient):
        def __init__(self, clock):
            super().__init__(source_fills=True)
            self.tick_clock = clock

        async def account_snapshot(self, account_index, market_id):
            self.tick_clock.value += 0.001
            snapshot = await super().account_snapshot(account_index, market_id)
            return replace(snapshot, observed_at=self.tick_clock.value)

    clock = TickClock()
    result = await run_handoff(make_config(tmp_path / "delayed.jsonl"), Delayed(clock), clock=clock)
    assert result.outcome is Outcome.SUCCESS


@pytest.mark.asyncio
async def test_pre_receiver_account_identity_drift_blocks_receiver(tmp_path):
    class IdentityDrift(FakeClient):
        def __init__(self):
            super().__init__()
            self.account_reads = 0

        async def account_snapshot(self, account_index, market_id):
            self.account_reads += 1
            snapshot = await super().account_snapshot(account_index, market_id)
            if self.account_reads == 3 and account_index == self.source_account_index:
                return replace(snapshot, source_identity="foreign-account")
            return snapshot

    client = IdentityDrift()
    result = await run_handoff(make_config(tmp_path / "identity-drift.jsonl"), client, clock=FakeClock())
    assert result.outcome is Outcome.UNKNOWN
    assert "source recheck account/market identity conflicts with plan" in result.unknown_reasons
    assert not [plan for plan in client.submissions if not plan.reduce_only]


@pytest.mark.asyncio
async def test_restart_binding_mismatch_is_read_only_and_complete_receipt_is_durable(tmp_path):
    path = tmp_path / "restart.jsonl"
    first = FakeClient(ambiguous_source=True)
    result = await run_handoff(make_config(path), first, clock=FakeClock())
    assert result.outcome is Outcome.UNKNOWN
    changed = make_config(path, request_timeout_seconds=2)
    second = FakeClient()
    resumed = await run_handoff(changed, second, clock=FakeClock())
    assert resumed.outcome is Outcome.UNKNOWN
    assert second.submissions == []
    assert "RESTART_BINDING_MISMATCH" in path.read_text(encoding="utf-8")

    complete_path = tmp_path / "complete.jsonl"
    completed = await run_handoff(
        make_config(complete_path), FakeClient(source_fills=True), clock=FakeClock()
    )
    assert completed.outcome is Outcome.SUCCESS
    journal = complete_path.read_text(encoding="utf-8")
    assert '"event":"COMPLETE"' in journal
    assert '"receipt"' in journal
    assert '"trades"' in journal
    assert '"client_order_index"' in journal
    assert '"implementation_fingerprint"' in journal
    assert '"account_index":11' in journal
    replay_client = FakeClient(source_fills=True)
    replay = await run_handoff(make_config(complete_path), replay_client, clock=FakeClock())
    assert replay.outcome is Outcome.UNKNOWN
    assert replay_client.submissions == []


@pytest.mark.asyncio
async def test_legacy_cap_config_is_not_bound_to_new_attempt_journal(tmp_path):
    path = tmp_path / "legacy-cap-config.jsonl"
    result = await run_handoff(
        make_config(
            path,
            max_gross_notional=Decimal("0.001"),
            source_fee_budget=Decimal("0.001"),
            receiver_fee_budget=Decimal("0.001"),
            receiver_price_cap=Decimal("0.001"),
        ),
        FakeClient(ambiguous_source=True),
        clock=FakeClock(),
    )
    assert result.outcome is Outcome.UNKNOWN
    journal = path.read_text(encoding="utf-8")
    assert "max_gross_notional" not in journal
    assert "source_fee_budget" not in journal
    assert "receiver_fee_budget" not in journal
    assert "receiver_price_cap" not in journal


@pytest.mark.asyncio
async def test_restart_binds_plan_events_to_the_unresolved_mutation_run(tmp_path):
    path = tmp_path / "same-path-runs.jsonl"
    first = await run_handoff(
        make_config(path), FakeClient(ambiguous_source=True), clock=FakeClock()
    )
    assert first.plan is not None
    later_plan = replace(first.plan, run_id="later-preview")
    journal = DurableJournal(path, clock=lambda: 1_000.0)
    journal.append("PLAN_READY", {"plan": later_plan.as_dict()}, run_id="later-preview")
    resumed = await run_handoff(make_config(path), FakeClient(), clock=FakeClock())
    assert resumed.outcome is Outcome.UNKNOWN
    assert resumed.run_id == first.run_id
    assert resumed.plan is not None and resumed.plan.run_id == first.run_id


@pytest.mark.asyncio
async def test_restart_rejects_plan_parameter_tampering_before_reconciliation_reads(tmp_path):
    path = tmp_path / "tampered-plan.jsonl"
    first = await run_handoff(
        make_config(path), FakeClient(ambiguous_source=True), clock=FakeClock()
    )
    assert first.plan is not None
    tampered = replace(
        first.plan,
        source=replace(first.plan.source, price=Decimal("99.25"), price_int=9925),
    )
    journal = DurableJournal(path, clock=lambda: 1_000.0)
    journal.append("PLAN_READY", {"plan": tampered.as_dict()}, run_id=first.run_id)
    second = FakeClient()
    resumed = await run_handoff(make_config(path), second, clock=FakeClock())
    assert resumed.outcome is Outcome.UNKNOWN
    assert "plan identity/parameters conflict" in resumed.reason
    assert second.submissions == []


def test_official_order_values_and_required_receipt_fields_are_strict():
    official = OrderSnapshot.from_mapping(
        {
            "account_index": 11,
            "market_id": 7,
            "order_id": "145",
            "client_order_index": 123,
            "status": "canceled-not-enough-liquidity",
            "side": "SELL",
            "type": 0,
            "time_in_force": 2,
            "reduce_only": True,
            "initial_base_amount": "0.125",
            "remaining_base_amount": "0",
            "filled_base_amount": "0",
            "base_price": "100.25",
            "timestamp": 1_000,
        }
    )
    assert official.order_type == "LIMIT"
    assert official.time_in_force == "POST_ONLY"
    assert official.terminal
    with pytest.raises(ContractError, match="side"):
        TradeReceipt.from_mapping(
            {
                "trade_id": "t",
                "account_index": 11,
                "market_id": 7,
                "order_id": "145",
                "quantity": "0.125",
                "price": "100.25",
                "observed_at": 1_000,
            }
        )
    with pytest.raises(ContractError, match="sign"):
        AccountSnapshot.from_mapping(
            {
                "account_index": 11,
                "market_id": 7,
                "position": "1",
                "active_orders": [],
                "observed_at": 1_000,
                "authorized": True,
                "ready": True,
                "margin_available": "10",
                "margin_required": "1",
                "incremental_margin_required": "1",
                "incremental_margin_evidence": "fixture",
                "fee_rate": "0.001",
                "source_identity": "account-11",
            }
        )


@pytest.mark.asyncio
async def test_sdk_own_side_attribution_and_one_shot_http(monkeypatch):
    config = HandoffConfig(
        market_id=7,
        direction=Direction.LONG,
        quantity=Decimal("0.125"),
        source_limit_price=Decimal("100.25"),
        receiver_worst_price=Decimal("101.25"),
        receiver_price_cap=Decimal("101.25"),
        max_gross_notional=Decimal("200"),
        source_fee_budget=Decimal("1"),
        receiver_fee_budget=Decimal("1"),
        freshness_seconds=10,
        request_timeout_seconds=1,
        order_timeout_seconds=1,
        reconcile_timeout_seconds=1,
        poll_interval_seconds=0.1,
        max_poll_count=2,
        source_order_lifetime_seconds=300,
        client_order_prefix="correction-sdk",
        journal_path="/tmp/correction-sdk.jsonl",
        api_base_url="https://mainnet.zklighter.elliot.ai",
        api_key_index=4,
        chain_id=304,
    )

    class CountingHttp:
        def __init__(self, *args, **kwargs):
            self.calls = []

        async def post_form(self, path, *, form):
            self.calls.append((path, dict(form)))
            return {"code": 500, "message": "fixture-token must stay private"}

    http = CountingHttp()
    signer = FakeSigner()
    client = LighterSdkClient(
        config,
        source_account_index=11,
        receiver_account_index=22,
        secrets=StaticSecretProvider({11: "fixture-secret-a", 22: "fixture-secret-b"}),
        market_evidence={},
        signer_factory=lambda **kwargs: signer,
        http_factory=lambda *args, **kwargs: http,
    )
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client._lighter = lambda: FakeModule
    page = await client.list_trades(22, 7, order_id="145")
    assert page.trades == ()
    own = await client.list_trades(22, 7, order_id="245")
    assert own.trades[0].account_index == 22
    assert own.trades[0].order_id == "245"
    assert own.trades[0].side == "BUY"
    plan = OrderPlan(
        account_index=11,
        market_id=7,
        side="SELL",
        quantity=Decimal("0.125"),
        quantity_int=125,
        price=Decimal("100.25"),
        price_int=10025,
        order_type="LIMIT",
        time_in_force="POST_ONLY",
        reduce_only=True,
        order_expiry_ms=1_500_000,
        client_order_index=123,
    )
    rejected = await client.submit_order(plan)
    assert not rejected.accepted
    assert len(signer.sign_calls) == 1
    assert len(http.calls) == 1
    assert http.calls[0][0] == "api/v1/sendTx"
    assert "fixture-token" not in rejected.error


@pytest.mark.asyncio
async def test_sdk_auth_token_refresh_is_bounded_by_configured_lifetime(monkeypatch):
    config = HandoffConfig(
        market_id=7,
        direction=Direction.LONG,
        quantity=Decimal("0.125"),
        source_limit_price=Decimal("100.25"),
        receiver_worst_price=Decimal("101.25"),
        max_gross_notional=Decimal("200"),
        source_fee_budget=Decimal("1"),
        receiver_fee_budget=Decimal("1"),
        freshness_seconds=10,
        request_timeout_seconds=1,
        order_timeout_seconds=1,
        reconcile_timeout_seconds=1,
        poll_interval_seconds=0.1,
        max_poll_count=2,
        source_order_lifetime_seconds=300,
        client_order_prefix="token-refresh",
        journal_path="/tmp/token-refresh.jsonl",
        api_base_url="https://mainnet.zklighter.elliot.ai",
        api_key_index=4,
        chain_id=304,
        auth_token_lifetime_seconds=60,
    )
    class TokenSigner(FakeSigner):
        def __init__(self):
            super().__init__()
            self.auth_calls = []

        async def create_auth_token_with_expiry(self, **kwargs):
            self.auth_calls.append(kwargs)
            return await super().create_auth_token_with_expiry(**kwargs)

    signer = TokenSigner()
    now = [0.0]
    client = LighterSdkClient(
        config,
        source_account_index=11,
        receiver_account_index=22,
        secrets=StaticSecretProvider({11: "fixture-secret-a", 22: "fixture-secret-b"}),
        market_evidence={},
        signer_factory=lambda **kwargs: signer,
        clock=lambda: now[0],
    )
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client._lighter = lambda: FakeModule
    assert await client._authorization(11) == "fixture-token"
    now[0] = 10
    assert await client._authorization(11) == "fixture-token"
    now[0] = 60
    assert await client._authorization(11) == "fixture-token"
    assert len(signer.auth_calls) == 2
    assert all(call["api_key_index"] == 4 for call in signer.auth_calls)
    assert all(call["deadline"] == 60 for call in signer.auth_calls)


@pytest.mark.asyncio
async def test_exposure_success_does_not_require_direct_counterparty_pair(tmp_path):
    class Unrelated(FakeClient):
        async def list_trades(self, *args, **kwargs):
            page = await super().list_trades(*args, **kwargs)
            return replace(
                page,
                trades=tuple(replace(trade, counterparty_account_index=999) for trade in page.trades),
            )

    result = await run_handoff(
        make_config(tmp_path / "unrelated.jsonl"),
        Unrelated(source_fills=True),
        clock=FakeClock(),
    )
    assert result.outcome is Outcome.SUCCESS
    assert result.source.filled_quantity == Decimal("0.125")
    assert result.receiver.filled_quantity == Decimal("0.125")
    assert result.joint_match_status == "KNOWN_ZERO"
    assert result.joint_match_quantity == Decimal("0")
    assert result.as_dict()["source"]["counterparty_match_status"] == "KNOWN_ZERO"
    assert result.as_dict()["receiver"]["counterparty_matched_quantity"] == "0"


@pytest.mark.asyncio
async def test_joint_match_requires_compatible_shared_trade_identity(tmp_path):
    contradictory = await run_handoff(
        make_config(tmp_path / "contradictory-pair.jsonl"),
        FakeClient(source_fills=True),
        clock=FakeClock(),
    )
    assert contradictory.outcome is Outcome.SUCCESS
    assert contradictory.source.filled_quantity == Decimal("0.125")
    assert contradictory.receiver.filled_quantity == Decimal("0.125")
    assert contradictory.joint_match_status == "CONFLICTING"
    assert contradictory.joint_match_quantity == Decimal("0")
    assert contradictory.as_dict()["source"]["counterparty_matched_quantity"] == "0"
    assert contradictory.as_dict()["source"]["named_counterparty_quantity"] == "0.125"
    assert any("joint trade matching is CONFLICTING" in item for item in contradictory.findings)

    class Agreed(FakeClient):
        async def list_trades(self, *args, **kwargs):
            page = await super().list_trades(*args, **kwargs)
            return replace(
                page,
                trades=tuple(
                    replace(trade, trade_id="same-trade", price=Decimal("100.25"))
                    for trade in page.trades
                ),
            )

    agreed = await run_handoff(
        make_config(tmp_path / "agreed-pair.jsonl"),
        Agreed(source_fills=True),
        clock=FakeClock(),
    )
    assert agreed.outcome is Outcome.SUCCESS
    assert agreed.joint_match_status == "MATCHED"
    assert agreed.joint_match_quantity == Decimal("0.125")
    assert agreed.as_dict()["source"]["counterparty_matched_quantity"] == "0.125"


@pytest.mark.asyncio
async def test_missing_counterparty_is_unknown_separate_from_known_zero(tmp_path):
    class Missing(FakeClient):
        async def list_trades(self, *args, **kwargs):
            page = await super().list_trades(*args, **kwargs)
            return replace(
                page,
                trades=tuple(replace(trade, counterparty_account_index=None) for trade in page.trades),
            )

    result = await run_handoff(
        make_config(tmp_path / "missing-counterparty.jsonl"),
        Missing(source_fills=True),
        clock=FakeClock(),
    )
    assert result.outcome is Outcome.SUCCESS
    assert result.joint_match_status == "UNKNOWN"
    assert result.as_dict()["source"]["counterparty_match_status"] == "UNKNOWN"


@pytest.mark.asyncio
async def test_delayed_nonce_cannot_reach_sign_or_dispatch(monkeypatch, tmp_path):
    class SlowNonce:
        async def async_next_nonce(self, api_key_index):
            await asyncio.sleep(0.03)
            return api_key_index, 41

    signer = FakeSigner()
    signer.nonce_manager = SlowNonce()
    config = make_config(tmp_path / "slow-nonce.jsonl", request_timeout_seconds=0.001)
    client = LighterSdkClient(
        config,
        source_account_index=11,
        receiver_account_index=22,
        secrets=StaticSecretProvider({}),
        market_evidence={},
        signer_factory=lambda **kwargs: signer,
        http_factory=FakeHttp,
    )
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client._signers[11] = signer
    plan = OrderPlan(
        account_index=11,
        market_id=7,
        side="SELL",
        quantity=Decimal("0.125"),
        quantity_int=125,
        price=Decimal("100.25"),
        price_int=10025,
        order_type="LIMIT",
        time_in_force="POST_ONLY",
        reduce_only=True,
        order_expiry_ms=1_500_000,
        client_order_index=123,
    )
    receipt = await client.submit_order(plan)
    assert not receipt.accepted
    assert signer.sign_calls == []
    assert client._http.calls == []


@pytest.mark.asyncio
async def test_signing_crossing_final_deadline_cannot_reach_send_tx(monkeypatch, tmp_path):
    import risex_spread_shadow.hood_handoff.sdk as sdk_module

    ticks = [100.0]

    class LateSigner(FakeSigner):
        async def sign_create_order(self, **kwargs):
            ticks[0] += 0.02
            return await super().sign_create_order(**kwargs)

    original_sdk_time = sdk_module.time
    sdk_module.time = SimpleNamespace(monotonic=lambda: ticks[0])
    try:
        signer = LateSigner()
        client = LighterSdkClient(
            make_config(tmp_path / "sign-crossing.jsonl"),
            source_account_index=11,
            receiver_account_index=22,
            secrets=StaticSecretProvider({}),
            market_evidence={},
            signer_factory=lambda **kwargs: signer,
            http_factory=FakeHttp,
        )
        client._signers[11] = signer
        plan = OrderPlan(
            account_index=11,
            market_id=7,
            side="SELL",
            quantity=Decimal("0.125"),
            quantity_int=125,
            price=Decimal("100.25"),
            price_int=10025,
            order_type="LIMIT",
            time_in_force="POST_ONLY",
            reduce_only=True,
            order_expiry_ms=1_500_000,
            client_order_index=123,
            mutation_deadline_monotonic=100.01,
        )

        receipt = await client.submit_order(plan)

        assert not receipt.accepted
        assert len(signer.sign_calls) == 1
        assert client._http.calls == []
    finally:
        sdk_module.time = original_sdk_time


@pytest.mark.asyncio
async def test_preparation_read_timeout_blocks_without_mutation(tmp_path):
    class SlowMetadata(FakeClient):
        async def market_metadata(self, market_id):
            await asyncio.sleep(0.03)
            return await super().market_metadata(market_id)

    client = SlowMetadata()
    result = await run_handoff(
        make_config(tmp_path / "slow-read.jsonl", request_timeout_seconds=0.001),
        client,
        clock=FakeClock(),
    )
    assert result.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED
    assert client.submissions == []


@pytest.mark.asyncio
async def test_actual_fee_economics_are_reported_without_cap_admission(tmp_path):
    class FeeDrift(FakeClient):
        def __init__(self, fee):
            super().__init__(source_fills=True)
            self.fee = fee

        async def list_trades(self, *args, **kwargs):
            page = await super().list_trades(*args, **kwargs)
            return replace(page, trades=tuple(replace(trade, fee=self.fee) for trade in page.trades))

    for label, fee, expected_status, expected_outcome in (
        ("below", Decimal("0.5"), "PROVEN", Outcome.SUCCESS),
        ("at", Decimal("1"), "PROVEN", Outcome.SUCCESS),
        ("above", Decimal("2"), "PROVEN", Outcome.SUCCESS),
    ):
        result = await run_handoff(
            make_config(tmp_path / f"fee-{label}.jsonl"),
            FeeDrift(fee),
            clock=FakeClock(),
        )
        assert result.outcome is expected_outcome
        assert result.economic_status == expected_status
        assert result.source.filled_quantity == Decimal("0.125")
        assert result.receiver.filled_quantity == Decimal("0.125")
        assert any("observed gross" in item and "fee" in item for item in result.economic_findings)


@pytest.mark.asyncio
async def test_missing_fee_is_unknown_economics_without_erasing_fill_quantity(tmp_path):
    class MissingFee(FakeClient):
        async def list_trades(self, *args, **kwargs):
            page = await super().list_trades(*args, **kwargs)
            return replace(page, trades=tuple(replace(trade, fee=None) for trade in page.trades))

    result = await run_handoff(
        make_config(tmp_path / "missing-fee.jsonl"),
        MissingFee(source_fills=True),
        clock=FakeClock(),
    )
    assert result.outcome is Outcome.SUCCESS
    assert result.economic_status == "UNKNOWN"
    assert result.source.filled_quantity == Decimal("0.125")
    assert result.receiver.filled_quantity == Decimal("0.125")
    assert result.economic_findings
    assert any("fee economics UNKNOWN" in item for item in result.findings)


@pytest.mark.asyncio
async def test_missing_preflight_fee_evidence_does_not_block_proven_exposure(tmp_path):
    class NoFeeEvidence(FakeClient):
        async def market_metadata(self, market_id):
            return replace(
                await super().market_metadata(market_id),
                source_fee_rate=None,
                receiver_fee_rate=None,
            )

        async def account_snapshot(self, account_index, market_id):
            return replace(await super().account_snapshot(account_index, market_id), fee_rate=None)

    result = await run_handoff(
        make_config(tmp_path / "missing-preflight-fee.jsonl"),
        NoFeeEvidence(source_fills=True),
        clock=FakeClock(),
    )
    assert result.outcome is Outcome.SUCCESS
    assert result.economic_status == "PROVEN"
    assert result.source.gross_notional == Decimal("12.53125")
    assert result.receiver.gross_notional == Decimal("12.65625")


@pytest.mark.asyncio
async def test_sdk_account_requires_official_identity_and_position_fields(monkeypatch):
    config = HandoffConfig(
        market_id=7,
        direction=Direction.LONG,
        quantity=Decimal("0.125"),
        source_limit_price=Decimal("100.25"),
        receiver_worst_price=Decimal("101.25"),
        max_gross_notional=Decimal("200"),
        source_fee_budget=Decimal("1"),
        receiver_fee_budget=Decimal("1"),
        freshness_seconds=10,
        request_timeout_seconds=1,
        order_timeout_seconds=1,
        reconcile_timeout_seconds=1,
        poll_interval_seconds=0.1,
        max_poll_count=2,
        source_order_lifetime_seconds=300,
        client_order_prefix="account-schema",
        journal_path="/tmp/account-schema.jsonl",
        api_base_url="https://mainnet.zklighter.elliot.ai",
        api_key_index=4,
        chain_id=304,
    )

    class AccountApi:
        def __init__(self, api_client):
            self.api_client = api_client

        async def account(self, **kwargs):
            return {
                "code": 200,
                "accounts": [
                    {
                        "index": 11,
                        "l1_address": "0xsource",
                        "status": 1,
                        "available_balance": "10",
                        "cross_initial_margin_requirement": "1",
                        "positions": [{"market_id": 7, "position": "1", "sign": 1}],
                    }
                ],
            }

    class OrderApi:
        def __init__(self, api_client):
            self.api_client = api_client

        async def account_active_orders(self, **kwargs):
            return {"code": 200, "orders": []}

    class AccountModule:
        Configuration = FakeModule.Configuration
        ApiClient = FakeModule.ApiClient

    AccountModule.AccountApi = AccountApi
    AccountModule.OrderApi = OrderApi

    client = LighterSdkClient(
        config,
        source_account_index=11,
        receiver_account_index=22,
        secrets=StaticSecretProvider({11: "fixture-secret-a", 22: "fixture-secret-b"}),
        market_evidence={"source_fee_rate": "0.001"},
        signer_factory=lambda **kwargs: FakeSigner(**kwargs),
    )
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client._lighter = lambda: AccountModule
    snapshot = await client.account_snapshot(11, 7)
    assert snapshot.account_index == 11
    assert snapshot.signed_position == Decimal("1")

    class MissingSign(AccountApi):
        async def account(self, **kwargs):
            payload = await super().account(**kwargs)
            payload["accounts"][0]["positions"][0].pop("sign")
            return payload

    AccountModule.AccountApi = MissingSign
    with pytest.raises(ContractError, match="sign/position"):
        await client.account_snapshot(11, 7)


def test_secret_boundaries_and_tty_guard(monkeypatch):
    value = sanitize_exception(RuntimeError("Authorization: fixture-not-a-real-token"))
    assert value == "sdk_error"
    assert "fixture-not-a-real-token" not in value
    assert sanitize({"private_key_id": "fixture-private-key"})["private_key_id"] == "[REDACTED]"
    provider = PromptSecretProvider((11,), 4)
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    monkeypatch.setattr("sys.stderr.isatty", lambda: False)
    with pytest.raises(RuntimeError, match="interactive TTY"):
        provider.private_key(11, 4)


@pytest.mark.asyncio
async def test_mutation_barrier_preserves_observation_age_and_allows_fresh_attempt(monkeypatch, tmp_path):
    import risex_spread_shadow.hood_handoff.engine as engine_module
    import risex_spread_shadow.hood_handoff.sdk as sdk_module

    ticks = [100.0]
    original_engine_time = engine_module.time
    original_sdk_time = sdk_module.time
    engine_module.time = SimpleNamespace(monotonic=lambda: ticks[0])
    sdk_module.time = SimpleNamespace(monotonic=lambda: ticks[0])
    try:
        class AdvancingNonce:
            async def async_next_nonce(self, api_key_index):
                ticks[0] += 0.006
                return api_key_index, 41

        config = make_config(
            tmp_path / "barrier.jsonl",
            freshness_seconds=0.01,
            request_timeout_seconds=1,
        )
        signer = FakeSigner()
        signer.nonce_manager = AdvancingNonce()
        client = LighterSdkClient(
            config,
            source_account_index=11,
            receiver_account_index=22,
            secrets=StaticSecretProvider({}),
            market_evidence={},
            signer_factory=lambda **kwargs: signer,
            http_factory=FakeHttp,
        )
        client._signers[11] = signer
        plan = OrderPlan(
            account_index=11,
            market_id=7,
            side="SELL",
            quantity=Decimal("0.125"),
            quantity_int=125,
            price=Decimal("100.25"),
            price_int=10025,
            order_type="LIMIT",
            time_in_force="POST_ONLY",
            reduce_only=True,
            order_expiry_ms=1_500_000,
            client_order_index=123,
        )
        engine = HandoffEngine(client, clock=FakeClock())
        stale = SimpleNamespace(observed_at=999.991)
        stale_plan = engine._mutation_plan(
            plan,
            config,
            observations=(stale,),
            observation_now=1000.0,
        )
        assert stale_plan.mutation_deadline_monotonic == pytest.approx(100.001)
        rejected = await client.submit_order(stale_plan)
        assert not rejected.accepted
        assert signer.sign_calls == []
        assert client._http.calls == []

        ticks[0] = 100.0
        book_bound_plan = engine._mutation_plan(
            plan,
            config,
            observations=(SimpleNamespace(observed_at=100.0), {"observed_at": 99.995}),
            observation_now=100.0,
        )
        assert book_bound_plan.mutation_deadline_monotonic == pytest.approx(100.005)

        ticks[0] = 200.0
        fresh_plan = engine._mutation_plan(
            plan,
            config,
            observations=(SimpleNamespace(observed_at=2000.0),),
            observation_now=2000.0,
        )
        accepted = await client.submit_order(fresh_plan)
        assert accepted.accepted
        assert len(signer.sign_calls) == 1
        assert len(client._http.calls) == 1
    finally:
        engine_module.time = original_engine_time
        sdk_module.time = original_sdk_time


@pytest.mark.asyncio
async def test_joint_matching_keeps_known_quantity_and_conflict_separate_from_partial(tmp_path):
    class Mixed(FakeClient):
        async def list_trades(self, *args, **kwargs):
            page = await super().list_trades(*args, **kwargs)
            expanded = []
            for trade in page.trades:
                expanded.extend(
                    (
                        replace(trade, trade_id="shared", quantity=Decimal("0.05"), price=Decimal("100.25")),
                        replace(trade, quantity=Decimal("0.075")),
                    )
                )
            return replace(page, trades=tuple(expanded))

    mixed = await run_handoff(
        make_config(tmp_path / "mixed-conflict.jsonl"),
        Mixed(source_fills=True),
        clock=FakeClock(),
    )
    assert mixed.joint_match_quantity == Decimal("0.05")
    assert mixed.joint_match_status == "CONFLICTING"
    assert any("0.05" in finding and "CONFLICTING" in finding for finding in mixed.findings)

    class Unrelated(FakeClient):
        async def list_trades(self, *args, **kwargs):
            page = await super().list_trades(*args, **kwargs)
            expanded = []
            for trade in page.trades:
                expanded.extend(
                    (
                        replace(trade, trade_id="shared", quantity=Decimal("0.05"), price=Decimal("100.25")),
                        replace(
                            trade,
                            trade_id=f"{trade.trade_id}-unrelated",
                            quantity=Decimal("0.075"),
                            counterparty_account_index=999,
                        ),
                    )
                )
            return replace(page, trades=tuple(expanded))

    partial = await run_handoff(
        make_config(tmp_path / "mixed-unrelated.jsonl"),
        Unrelated(source_fills=True),
        clock=FakeClock(),
    )
    assert partial.joint_match_quantity == Decimal("0.05")
    assert partial.joint_match_status == "PARTIAL"
    assert not any("CONFLICTING" in finding for finding in partial.findings)
