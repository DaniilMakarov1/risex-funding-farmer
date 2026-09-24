from __future__ import annotations

import asyncio
from dataclasses import replace
from decimal import Decimal
import json
from types import SimpleNamespace

import pytest

from risex_spread_shadow.hood_handoff import (
    AccountSnapshot, ContractError, DepthLevel, Direction, FallbackResult, MarketMetadata,
    MutationReceipt, OpeningMarginReserve, OrderBookSnapshot, Outcome, PreflightBlocked,
    RandomCycleConfig, RandomCycleEngine,
)
from risex_spread_shadow.hood_handoff import random_cycle as cycle_module
from risex_spread_shadow.hood_handoff import cli as cli_module
from risex_spread_shadow.hood_handoff.operator_control import exclusive_lock


NOW = 1000.0


def config(path, **changes):
    values = dict(
        market_id=1, market_symbol="BTC", direction=Direction.LONG,
        source_account_index=27331, receiver_account_index=27337,
        cycle_dir=path, environment="robinhood",
        api_base_url="https://api.rh.lighter.xyz", chain_id=466324,
        api_key_index=4, source_order_lifetime_seconds=300,
        margin_reserve=OpeningMarginReserve(Decimal("0.10"), Decimal("0.02")),
        operator_execution_opt_in=True, operator_plan_reviewed=True,
        confirmed_pilot=True,
    )
    values.update(changes)
    return RandomCycleConfig(**values)


def market():
    return MarketMetadata(
        market_id=1, symbol="BTC", status="active", price_decimals=2,
        size_decimals=5, minimum_base_amount=Decimal("0.00020"),
        minimum_quote_amount=Decimal("10"), source_fee_rate=Decimal("0.00012"),
        receiver_fee_rate=Decimal("0.00035"), observed_at=NOW,
        margin_evidence="synthetic", market_type="perp", venue="robinhood",
        minimum_initial_margin_fraction=2500, market_margin_mode=0,
        mark_price=Decimal("100000"),
    )


def book():
    return OrderBookSnapshot(
        market_id=1, symbol="BTC",
        bids=(DepthLevel(Decimal("99990"), Decimal("1"), "bid"),),
        asks=(DepthLevel(Decimal("100010"), Decimal("1"), "ask"),),
        observed_at=NOW, market_type="perp", venue="robinhood",
    )


def account(index):
    return AccountSnapshot(
        account_index=index, market_id=1, signed_position=Decimal(0),
        active_orders=(), observed_at=NOW, authorized=True, ready=True,
        margin_available=Decimal("1000"), margin_required=Decimal("1"),
        fee_rate=Decimal("0.00035"), source_identity=f"pilot-{index}",
        incremental_margin_required=Decimal("1"),
        incremental_margin_evidence="synthetic", available_balance=Decimal("1000"),
    )


class Clock:
    def now(self):
        return NOW

    def monotonic(self):
        return NOW

    async def sleep(self, _):
        raise AssertionError("unexpected sleep")


class Client:
    source_account_index = 27331
    receiver_account_index = 27337

    def __init__(self):
        self.started = 0
        self.stopped = 0
        self.submitted = 0

    async def market_metadata(self, _):
        return market()

    async def order_book(self, _):
        return book()

    async def account_snapshot(self, index, _):
        return account(index)

    async def start_read_stream(self, *, ready_timeout):
        assert ready_timeout == 5
        self.started += 1
        return False

    async def stop_read_stream(self):
        self.stopped += 1

    async def submit_order(self, _):
        self.submitted += 1
        raise AssertionError("pilot must not submit before all gates")

    async def update_leverage_fraction(self, *_args, **_kwargs):
        raise AssertionError("pilot must never change leverage")


@pytest.mark.parametrize("change", [
    {"market_id": 2}, {"source_account_index": 27332},
    {"margin_reserve": OpeningMarginReserve(Decimal("0.09"), Decimal("0.02"))},
    {"confirmed_pilot": 1},
    {"confirmed_pilot": False, "pilot_allow_leverage_update": True},
])
def test_pilot_config_rejects_unapproved_identity_or_policy(tmp_path, change):
    with pytest.raises(ContractError):
        config(tmp_path / "cycle", **change)


def test_pilot_binding_records_exact_mode(tmp_path):
    assert config(tmp_path / "cycle").binding()["confirmed_pilot"] is True


@pytest.mark.parametrize("quantity,source_price,receiver_price", [
    ("0.00020", "200001", "200000"),
    ("0.00020", "200000", "200001"),
])
def test_pilot_notional_guard_rejects_either_over_cap_leg(quantity, source_price, receiver_price):
    with pytest.raises(PreflightBlocked, match="40.00"):
        RandomCycleEngine._pilot_notional_guard(
            Decimal(quantity), Decimal(source_price), Decimal(receiver_price))
    RandomCycleEngine._pilot_notional_guard(Decimal("0.00020"), Decimal("200000"), Decimal("200000"))


def test_pilot_refuses_unready_stream_before_any_order_and_stops_it(monkeypatch, tmp_path):
    client = Client()
    engine = RandomCycleEngine(client, clock=Clock())

    async def no_setting(_config, _journal, _metadata, _book, _selection, source, receiver):
        return source, receiver

    monkeypatch.setattr(engine, "_configure_leverage", no_setting)
    result = asyncio.run(engine.execute(config(tmp_path / "cycle")))
    assert result.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED
    assert result.selection.quantity == Decimal("0.00020")
    assert result.selection.hold_seconds == 20
    assert client.started == 1 and client.stopped == 1 and client.submitted == 0


def test_pilot_refuses_lost_readiness_before_limit_with_one_attempt_budget(monkeypatch, tmp_path):
    client = Client()
    engine = RandomCycleEngine(client, clock=Clock())
    seen = []

    async def start(*, ready_timeout):
        client.started += 1
        return True

    async def no_setting(_config, _journal, _metadata, _book, _selection, source, receiver):
        return source, receiver

    async def prepared(_config, _journal, selection, metadata, book, source, receiver, budget):
        seen.append((selection.quantity, selection.hold_seconds, budget.limit))
        return metadata, book, source, receiver, selection

    client.start_read_stream = start
    client.read_stream_ready = lambda: False
    monkeypatch.setattr(engine, "_configure_leverage", no_setting)
    monkeypatch.setattr(engine, "_prepare_open_with_retries", prepared)
    result = asyncio.run(engine.execute(config(tmp_path / "cycle")))
    assert result.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED
    assert seen == [(Decimal("0.00020"), 20, 1)]
    assert client.started == 1 and client.stopped == 1 and client.submitted == 0


def test_pilot_refuses_high_fee_evidence_before_stream_or_order(tmp_path):
    class HighFeeClient(Client):
        async def market_metadata(self, _):
            return replace(market(), source_fee_rate=Decimal("0.001"))

    client = HighFeeClient()
    result = asyncio.run(RandomCycleEngine(client, clock=Clock()).execute(config(tmp_path / "cycle")))
    assert result.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED
    assert "fee ceiling" in result.reason
    assert client.started == client.submitted == 0


def test_pilot_refuses_setting_change_before_calling_setter(monkeypatch, tmp_path):
    client = Client()
    engine = RandomCycleEngine(client, clock=Clock())
    monkeypatch.setattr(cycle_module, "_observed_leverage_fraction", lambda *_: 2500)
    plan = SimpleNamespace(quantity=Decimal("0.00020"),
                           opening_source_price=Decimal("100000"),
                           opening_receiver_bound=Decimal("100000"))
    journal = SimpleNamespace(append=lambda *_args, **_kwargs: None)

    async def scenario():
        with pytest.raises(PreflightBlocked, match="forbids leverage"):
            await engine._configure_leverage(
                config(tmp_path / "cycle"), journal, market(), book(), plan,
                account(27331), account(27337))

    asyncio.run(scenario())
    assert client.submitted == 0


def test_separately_opted_pilot_prepares_at_most_one_exact_1x_setting_per_account(monkeypatch, tmp_path):
    client = Client()
    fractions = {27331: 5227, 27337: 5712}
    writes = []

    async def setting(index, market_id, fraction, mode):
        writes.append((index, market_id, fraction, mode))
        fractions[index] = fraction
        return MutationReceipt(True, None, f"synthetic-{index}")

    client.update_leverage_fraction = setting
    engine = RandomCycleEngine(client, clock=Clock())
    monkeypatch.setattr(cycle_module, "_observed_leverage_fraction",
                        lambda snapshot, _label: fractions[snapshot.account_index])

    async def current(_config):
        return account(27331), account(27337)

    monkeypatch.setattr(engine, "_accounts", current)
    plan = SimpleNamespace(quantity=Decimal("0.00020"),
                           opening_source_price=Decimal("100000"),
                           opening_receiver_bound=Decimal("100000"))
    journal = SimpleNamespace(append=lambda *_args, **_kwargs: None)

    async def scenario():
        await engine._configure_leverage(
            config(tmp_path / "cycle", pilot_allow_leverage_update=True), journal,
            market(), book(), plan, account(27331), account(27337))

    asyncio.run(scenario())
    assert writes == [(27331, 1, 10000, 0), (27337, 1, 10000, 0)]
    assert fractions == {27331: 10000, 27337: 10000}


def test_separately_opted_pilot_does_not_replay_ambiguous_setting(monkeypatch, tmp_path):
    client = Client()
    writes = []

    async def ambiguous(index, market_id, fraction, mode):
        writes.append((index, market_id, fraction, mode))
        raise TimeoutError("synthetic unknown delivery")

    client.update_leverage_fraction = ambiguous
    engine = RandomCycleEngine(client, clock=Clock())
    monkeypatch.setattr(cycle_module, "_observed_leverage_fraction", lambda *_: 5227)
    plan = SimpleNamespace(quantity=Decimal("0.00020"),
                           opening_source_price=Decimal("100000"),
                           opening_receiver_bound=Decimal("100000"))
    journal = SimpleNamespace(append=lambda *_args, **_kwargs: None)

    async def scenario():
        with pytest.raises(TimeoutError):
            await engine._configure_leverage(
                config(tmp_path / "cycle", pilot_allow_leverage_update=True), journal,
                market(), book(), plan, account(27331), account(27337))

    asyncio.run(scenario())
    assert writes == [(27331, 1, 10000, 0)]


def test_pilot_partial_residual_attempt_is_not_replayed(monkeypatch, tmp_path):
    engine = RandomCycleEngine(Client(), clock=Clock())
    before = replace(account(27331), signed_position=Decimal("0.00020"))
    after = replace(before, signed_position=Decimal("0.00010"))
    other = account(27337)
    attempts = []
    events = []

    async def fake_attempt(_config, _journal, _before, residual, *, attempt_ordinal, reserved_nonce):
        attempts.append((residual, attempt_ordinal))
        return FallbackResult(
            27331, "SELL", residual, True, Outcome.PARTIAL,
            order_id="synthetic-1", filled_quantity=Decimal("0.00010"),
            position_after=Decimal("0.00010"), attempt=attempt_ordinal,
            reconciliation_state="PARTIAL_FILL", position_observed_at=NOW,
        )

    async def observed(_config, _journal):
        return after, other

    monkeypatch.setattr(engine, "_fallback_one", fake_attempt)
    monkeypatch.setattr(engine, "_recovery_accounts", observed)
    journal = SimpleNamespace(append=lambda name, payload: events.append((name, payload)))
    results, source_left, receiver_left = asyncio.run(
        engine.close_reconciled_positions(config(tmp_path / "cycle"), journal, before, other))
    assert attempts == [(Decimal("0.00020"), 1)]
    assert len(results) == 1
    assert (source_left, receiver_left) == (Decimal("0.00010"), Decimal(0))
    assert any(name == "PILOT_RECOVERY_LIMIT" for name, _ in events)


def test_pilot_cli_holds_operator_lock_and_checks_prior_state(monkeypatch, tmp_path):
    operator = tmp_path / "operator"
    operator.mkdir(mode=0o700)
    cycle = operator / "cycle-001"
    evidence = tmp_path / "market.json"
    evidence.write_text("{}")
    settings = config(cycle).binding()
    settings.update(cycle_dir=str(cycle), operator_execution_opt_in=True,
                    operator_plan_reviewed=True)
    pilot_config_path = operator / "pilot-20260924.json"
    pilot_config_path.write_text(json.dumps(settings))
    seen = []

    class Secrets:
        def close(self):
            seen.append("secrets_closed")

    class FakeSdk:
        def __init__(self, *_args, **_kwargs):
            seen.append("sdk")

        async def aclose(self):
            seen.append("sdk_closed")

    async def inspect(_config, _client, _operator, *, require_flat):
        assert require_flat is True and _operator == operator
        seen.append("prior_checked")

    async def run(_config, _client):
        assert seen[-1] == "prior_checked"
        with pytest.raises(RuntimeError, match="another operator process"):
            with exclusive_lock(operator / ".operator-launch.lock"):
                pass
        seen.append("run")
        return SimpleNamespace(outcome=Outcome.SUCCESS, as_dict=lambda: {"outcome": "SUCCESS"})

    monkeypatch.setattr("builtins.input", lambda _prompt: "LAUNCH")
    monkeypatch.setattr(cli_module, "_keychain_provider", lambda *_args, **_kwargs: Secrets())
    monkeypatch.setattr(cli_module, "_prime_keychain", lambda *_args: None)
    monkeypatch.setattr(cli_module, "LighterSdkClient", FakeSdk)
    monkeypatch.setattr(cli_module, "run_random_cycle", run)
    monkeypatch.setattr("risex_spread_shadow.hood_handoff.operator_recovery.inspect_current", inspect)
    args = cli_module._parser().parse_args([
        "random-cycle", "--config", str(pilot_config_path),
        "--execute", "--confirm-plan",
        "--i-understand-one-attempt-live-operation", "--keychain",
        "--market-evidence", str(evidence),
    ])
    assert asyncio.run(cli_module._run_random_cycle(args, settings)) == 0
    assert seen == ["sdk", "prior_checked", "run", "sdk_closed", "secrets_closed"]
    assert (operator / ".confirmed-pilot-20260924.claim.json").exists()
    with pytest.raises(SystemExit, match="already consumed"):
        asyncio.run(cli_module._run_random_cycle(args, settings))
