"""HCR-38: saved/live attribution and bounded public visibility refresh."""
import json
from dataclasses import replace
from decimal import Decimal

import pytest

from risex_spread_shadow.hood_handoff import (
    Direction, MutationReceipt, OperationMode, Outcome, TradeReceipt,
    run_handoff, run_random_cycle,
)
from risex_spread_shadow.hood_handoff.offline_report import load_saved_cycle_report
from risex_spread_shadow.hood_handoff.operator_view import read_execution_notices, result_lines
from risex_spread_shadow.hood_handoff.telegram_messages import saved_message
from test_hood_handoff_paired_opening import (
    NOW, Clock, PreparedPairedClient, PropagatingPreparedPairedClient, config,
)
from test_hood_handoff_random_cycle import (
    AdvancingClock, CycleClient, ExternalCloseClient, FixedRng, VisibleOpeningClient,
    PostOnlyCancelClient, cycle_config,
)
from test_hood_telegram_messages import valid


class StolenSourceAtMarket(CycleClient):
    """An external trade wins after admission; our price-bounded IOC fills zero."""
    def __init__(self, clock, *, closing):
        super().__init__(clock)
        self.closing = closing
        self.injected = False

    async def submit_order(self, plan):
        if (plan.order_type == "MARKET" and plan.account_index == self.receiver_account_index
                and plan.reduce_only == self.closing and not self.injected):
            self.injected = True
            self.submissions.append(plan)
            source = self.orders[(self.source_account_index, self.latest_order[self.source_account_index])]
            self._replace_order(source, status="filled", filled_quantity=source.initial_quantity,
                                remaining_quantity=Decimal(0))
            self.source_position += source.initial_quantity * (1 if source.side == "BUY" else -1)
            self.trades[source.order_id] = (TradeReceipt(
                "stolen-source", source.account_index, source.market_id, source.order_id,
                source.side, source.initial_quantity, source.price, None, 999, self.clock.now(),
                counterparty_order_id="external-taker", client_order_index=source.client_order_index,
            ),)
            receiver = replace(source, account_index=plan.account_index, order_id="empty-ioc",
                client_order_index=plan.client_order_index, status="canceled-too-much-slippage",
                side=plan.side, order_type="MARKET", time_in_force="IOC",
                filled_quantity=Decimal(0), remaining_quantity=plan.quantity)
            self._save_order(receiver)
            self.trades[receiver.order_id] = ()
            return MutationReceipt(True, receiver.order_id, "tx-empty-ioc")
        return await super().submit_order(plan)


@pytest.mark.asyncio
@pytest.mark.parametrize("closing", [False, True])
async def test_stolen_limit_and_empty_ioc_are_explicit_in_final_and_live_messages(tmp_path, closing):
    clock = AdvancingClock()
    cfg = cycle_config(tmp_path / "saved")
    result = await run_random_cycle(cfg, StolenSourceAtMarket(clock, closing=closing),
                                    clock=clock, rng=FixedRng(20, 20))
    assert result.outcome is Outcome.PARTIAL
    report = load_saved_cycle_report(cfg.cycle_dir)
    assert report["status"] == "COMPLETE"
    assert report["inventory"]["status"] == "CONFIRMED_FLAT"
    assert report["paired_execution"]["status"] == "FAILED"
    phase = report["paired_execution"]["direct_counterparty_match"][int(closing)]
    assert phase["status"] == "NOT_MATCHED"
    assert Decimal(phase["external_source_quantity"]) == Decimal("0.20")
    assert phase["external_source_accounts"] == [999]
    assert phase["receiver_state"] == "NO_FILL"
    assert phase["matched_quantity"] == "0"
    assert report["economics"]["fees"]["status"] == "UNKNOWN"
    assert report["economics"]["closed_execution_pnl"]["net"] is None
    label = "Закрытие" if closing else "Открытие"
    message = valid(saved_message("cycle-synthetic", report))
    assert "Парное исполнение: ⚠️ не состоялось" in message
    assert f"{label} · наша LIMIT: исполнили внешние участники (счета 999) — 0.2" in message
    assert f"{label} · наш MARKET: ордер отправлен, исполнений нет" in message
    terminal = "\n".join(result_lines(report))
    assert f"{label} · наша LIMIT" in terminal and "счета 999" in terminal
    notices = "\n".join(notice for _, notice in read_execution_notices(cfg.journal_path))
    assert "999" in notices and "внешн" in notices
    if closing:
        assert "Открытие · наша LIMIT: исполнил наш парный счёт — 0.2" in message
    else:
        assert "Закрытие: парные ордера не отправлялись." in message


@pytest.mark.asyncio
async def test_source_stolen_before_receiver_remains_proven_external(tmp_path):
    clock = AdvancingClock()
    cfg = cycle_config(tmp_path / "unsent")
    client = VisibleOpeningClient(clock, direction=Direction.LONG, fill_fraction=Decimal(1))
    await run_random_cycle(cfg, client, clock=clock, rng=FixedRng(20, 20))
    report = load_saved_cycle_report(cfg.cycle_dir)
    phase = report["paired_execution"]["direct_counterparty_match"][0]
    assert phase["status"] == "NOT_MATCHED"
    assert phase["source_state"] == "FILLED" and phase["receiver_state"] == "UNSENT"
    text = valid(saved_message("unsent", report))
    assert "счета 999" in text and "наш MARKET: не отправлялся" in text


@pytest.mark.asyncio
@pytest.mark.parametrize("source_fraction", [None, Decimal("0.5"), Decimal(1)])
async def test_external_receiver_and_source_race_preserve_each_known_quantity(tmp_path, source_fraction):
    clock = AdvancingClock()
    cfg = cycle_config(tmp_path / "external-close")
    client = ExternalCloseClient(clock, cancel_fill_fraction=source_fraction)
    await run_random_cycle(cfg, client, clock=clock, rng=FixedRng(20, 20))
    report = load_saved_cycle_report(cfg.cycle_dir)
    phase = report["paired_execution"]["direct_counterparty_match"][1]
    assert phase["status"] == "NOT_MATCHED"
    assert Decimal(phase["external_receiver_quantity"]) == Decimal("0.20")
    expected = Decimal(0) if source_fraction is None else Decimal("0.20") * source_fraction
    assert Decimal(phase["external_source_quantity"]) == expected
    text = valid(saved_message("receiver", report))
    assert "наш MARKET: исполнил чужие заявки (счета 999) — 0.2" in text
    assert report["inventory"]["status"] == "CONFIRMED_FLAT"


@pytest.mark.asyncio
async def test_zero_fill_phase_and_unattempted_close_are_not_unknown_execution(tmp_path):
    clock = AdvancingClock()
    cfg = cycle_config(tmp_path / "no-fill")
    await run_random_cycle(cfg, PostOnlyCancelClient(clock, cancel_count=6), clock=clock, rng=FixedRng(20, 20))
    report = load_saved_cycle_report(cfg.cycle_dir)
    assert report["paired_execution"]["direct_counterparty_match"][0]["status"] == "NO_FILL"
    text = valid(saved_message("no-fill", report))
    assert "наш MARKET: не отправлялся" in text
    assert "Закрытие: парные ордера не отправлялись." in text
    assert "исполнили внешние" not in text




@pytest.mark.asyncio
async def test_mixed_matching_shows_own_and_external_parts_without_erasing_either(tmp_path):
    class Mixed(CycleClient):
        async def submit_order(self, plan):
            receipt = await super().submit_order(plan)
            if plan.order_type == "MARKET" and not plan.reduce_only:
                for account in (self.source_account_index, self.receiver_account_index):
                    order_id = self.latest_order[account]
                    trade = self.trades[order_id][0]
                    half = trade.quantity / 2
                    self.trades[order_id] = (
                        replace(trade, quantity=half),
                        replace(trade, trade_id=f"external-{account}", quantity=half,
                                counterparty_account_index=999, counterparty_order_id=f"foreign-{account}",
                                counterparty_client_order_index=None),
                    )
            return receipt
    clock = AdvancingClock()
    cfg = cycle_config(tmp_path / "mixed")
    await run_random_cycle(cfg, Mixed(clock), clock=clock, rng=FixedRng(20, 20))
    report = load_saved_cycle_report(cfg.cycle_dir)
    assert report["status"] == "COMPLETE"
    phase = report["paired_execution"]["direct_counterparty_match"][0]
    assert phase["status"] == "NOT_MATCHED"
    assert Decimal(phase["matched_quantity"]) == Decimal("0.10")
    assert Decimal(phase["external_source_quantity"]) == Decimal("0.10")
    assert Decimal(phase["external_receiver_quantity"]) == Decimal("0.10")
    assert Decimal(phase["unproved_source_quantity"]) == Decimal(0)
    text = valid(saved_message("mixed", report))
    assert "исполнил наш парный счёт — 0.1; исполнили внешние участники (счета 999) — 0.1" in text
    assert "исполнил нашу LIMIT — 0.1; исполнил чужие заявки (счета 999) — 0.1" in text


class BookOnlyPropagation(PropagatingPreparedPairedClient):
    def __init__(self, *, closing, adverse=None):
        super().__init__(closing=closing)
        self.adverse_refresh = adverse
        self.admission_account_reads = 0
        self.reads_at_refresh = None
        self.age = 0

    async def account_snapshot(self, account_index, market_id):
        value = await PreparedPairedClient.account_snapshot(self, account_index, market_id)
        if self.source_dispatch_started and self.receiver_order is None:
            self.admission_account_reads += 1
        if self.source_dispatch_started and not self.source_visible:
            return replace(value, observed_at=NOW - 2)
        return value

    async def order_book(self, market_id):
        value = await super().order_book(market_id)
        if self.source_visible and self.reads_at_refresh is None:
            self.reads_at_refresh = self.admission_account_reads
            if self.adverse_refresh == "stale_accounts":
                self.age = 9  # Original accounts are now 11s old; exact order is only 9s old.
                value = replace(value, observed_at=NOW + self.age)
            elif self.adverse_refresh == "wrong_owner":
                value = replace(value, asks=tuple(replace(level, owner_account_index=888) for level in value.asks))
        return value

    async def lookup_order(self, *args, **kwargs):
        value = await super().lookup_order(*args, **kwargs)
        if self.source_lookup_calls >= 2 and self.adverse_refresh == "source_disappeared":
            return None
        return value


@pytest.mark.asyncio
@pytest.mark.parametrize("closing", [False, True])
@pytest.mark.parametrize("adverse", [None, "stale_accounts", "wrong_owner", "source_disappeared"])
async def test_book_only_refresh_keeps_original_account_ages_and_final_source_gate(tmp_path, closing, adverse):
    client = BookOnlyPropagation(closing=closing, adverse=adverse)
    class DynamicClock(Clock):
        def now(self):
            return NOW + client.age
    path = tmp_path / "attempt.jsonl"
    result = await run_handoff(config(path, operation_mode=(
        OperationMode.PAIRED_CLOSING if closing else OperationMode.PAIRED_OPENING)),
        client, clock=DynamicClock())
    assert client.reads_at_refresh == 2
    assert result.latency["pre_visibility_refresh_book_only"] == 1.0
    assert result.latency["source_visibility_lookup_seconds"] > 0
    assert result.latency["initial_account_checks_seconds"] > 0
    assert result.latency["initial_public_book_seconds"] > 0
    assert result.latency["final_source_lookup_seconds"] >= 0
    if adverse is None:
        assert result.outcome is Outcome.SUCCESS, result.as_dict()
        assert [p.account_index for p in client.submissions] == [11, 22]
    else:
        assert result.outcome is not Outcome.SUCCESS
        assert [p.account_index for p in client.submissions] == [11]
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    guard = next(row["payload"] for row in rows if row["event"] == "PRE_RECEIVER_GUARD")
    first = guard["causal_pre_visibility_observations"]
    assert first["source"]["observed_at"] == NOW - 2
    assert first["receiver"]["observed_at"] == NOW - 2


@pytest.mark.parametrize("declared,expected", [
    ("telegram", "telegram"), ("terminal", "terminal"), (None, "unknown"),
    ("synthetic-sensitive-canary", "unknown"),
])
def test_interface_provenance_is_only_an_allowlisted_diagnostic(monkeypatch, declared, expected):
    from risex_spread_shadow.hood_handoff.provenance import capture_provenance
    if declared is None:
        monkeypatch.delenv("RISEX_HOOD_OPERATOR_INTERFACE", raising=False)
    else:
        monkeypatch.setenv("RISEX_HOOD_OPERATOR_INTERFACE", declared)
    value = capture_provenance({"market_id": 7})
    assert value["operator_interface"] == expected
    assert "synthetic-sensitive-canary" not in json.dumps(value)


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", [True, 1.0])
async def test_invalid_counterparty_equal_to_numeric_account_does_not_prove_own_match(tmp_path, invalid):
    clock = AdvancingClock()
    cfg = cycle_config(tmp_path / "strict-own", receiver_account_index=1)
    client = CycleClient(clock)
    client.receiver_account_index = 1
    await run_random_cycle(cfg, client, clock=clock, rng=FixedRng(20, 20))
    assert load_saved_cycle_report(cfg.cycle_dir)["paired_execution"]["status"] == "SUCCESS"
    path = cfg.cycle_dir / "opening.jsonl"
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    receipt = next(r["payload"]["receipt"] for r in rows if r["event"] == "COMPLETE")
    receipt["source"]["trades"][0]["counterparty_account_index"] = invalid
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    report = load_saved_cycle_report(cfg.cycle_dir)
    phase = report["paired_execution"]["direct_counterparty_match"][0]
    assert report["paired_execution"]["status"] == "UNKNOWN"
    assert phase["matched_quantity"] == "0"
    assert phase["external_source_quantity"] == "0"
    assert report["inventory"]["status"] == "CONFIRMED_FLAT"
