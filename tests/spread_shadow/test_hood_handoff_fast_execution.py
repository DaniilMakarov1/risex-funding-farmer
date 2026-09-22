"""Distinguishing regressions for the audited public-book execution races."""
from dataclasses import replace
from decimal import Decimal
import asyncio
import json
import time
from types import SimpleNamespace

import pytest

from risex_spread_shadow.hood_handoff import ContractError, Direction, LighterSdkClient, Outcome, run_random_cycle
from risex_spread_shadow.hood_handoff.engine import HandoffPreflightContext
from risex_spread_shadow.hood_handoff.offline_report import load_saved_cycle_report
from risex_spread_shadow.hood_handoff.telegram_messages import saved_message
from test_hood_handoff_random_cycle import AdvancingClock, CycleClient, FixedRng, cycle_config
from test_hood_handoff_sdk_interface import _delayed_client, _delayed_tracker, _sdk_plan, ConstantNonceSigner, FakeHttp, _sdk_config, FakeModule
from risex_spread_shadow.hood_handoff import StaticSecretProvider


class CounterpartyClient(CycleClient):
    def __init__(self, clock, scenario):
        super().__init__(clock)
        self.scenario = scenario

    async def list_trades(self, *args, **kwargs):
        page = await super().list_trades(*args, **kwargs)
        if self.fallback_plans:
            return page
        changed = []
        for trade in page.trades:
            if self.scenario == "external":
                changed.append(replace(trade, trade_id=trade.trade_id + str(trade.account_index),
                                       counterparty_account_index=999, counterparty_order_id="foreign"))
            elif self.scenario == "missing_order":
                changed.append(replace(trade, counterparty_order_id=None))
            elif self.scenario == "mixed":
                changed += [replace(trade, quantity=Decimal("0.10")),
                            replace(trade, trade_id="external-" + str(trade.account_index),
                                    quantity=Decimal("0.10"), counterparty_account_index=999,
                                    counterparty_order_id="foreign")]
            elif self.scenario == "duplicate":
                changed += [trade, trade]
            else:
                changed.append(trade)
        return replace(page, trades=tuple(changed))


@pytest.mark.asyncio
@pytest.mark.parametrize("direction", [Direction.LONG, Direction.SHORT])
@pytest.mark.parametrize("scenario", ["own", "external", "mixed", "missing_order", "duplicate"])
async def test_only_full_reciprocal_receipts_authorize_hold(tmp_path, direction, scenario):
    clock = AdvancingClock()
    client = CounterpartyClient(clock, scenario)
    cfg = cycle_config(tmp_path / scenario, direction=direction)
    result = await run_random_cycle(cfg, client, clock=clock, rng=FixedRng(20, 20))
    records = [json.loads(line) for line in cfg.journal_path.read_text().splitlines()]
    held = any(row["event"] == "HOLD_ANCHORED" for row in records)
    if scenario in {"own", "duplicate"}:
        assert result.outcome is Outcome.SUCCESS, result.as_dict()
        assert held and result.opening.mutual_execution_proven
        assert result.opening.joint_match_quantity == Decimal("0.20")
    else:
        assert result.outcome is Outcome.PARTIAL, result.as_dict()
        assert not held and result.closing is None
        assert len(client.fallback_plans) == 2
        assert result.inventory == "CONFIRMED_FLAT"
        assert not result.opening.mutual_execution_proven
        if scenario == "mixed":
            assert result.opening.joint_match_quantity == Decimal("0.10")
        if scenario == "external":
            assert result.opening.as_dict()["source"]["external_counterparty_quantity"] == "0.20"
        report = load_saved_cycle_report(cfg.cycle_dir)
        assert report["paired_execution"]["status"] != "SUCCESS"
        assert "Парное исполнение: ✅" not in saved_message(scenario, report)


class PreQuoteClient(CycleClient):
    def __init__(self, clock):
        super().__init__(clock)
        self.events = []
        self.tokens = []

    async def account_snapshot(self, *args):
        self.events.append("account")
        self.clock.value += 0.2
        return await super().account_snapshot(*args)

    async def order_book(self, *args):
        self.events.append("book")
        return await super().order_book(*args)

    async def reserve_order_nonce(self, account_index, *, deadline):
        self.events.append("nonce")
        self.clock.value += 0.4
        token = SimpleNamespace(account=account_index, deadline=deadline, valid=True)
        self.tokens.append(token)
        return token

    async def invalidate_reserved_nonce(self, token):
        token.valid = False

    async def prepare_order(self, plan, *, reserved_nonce=None):
        assert reserved_nonce is not None and reserved_nonce.valid
        assert reserved_nonce.account == plan.account_index
        assert plan.mutation_deadline_monotonic <= reserved_nonce.deadline
        reserved_nonce.valid = False
        self.events.append("sign")
        return SimpleNamespace(plan=plan, deadline=plan.mutation_deadline_monotonic)

    async def submit_prepared_order(self, plan, prepared, *, deadline=None):
        assert prepared.plan == plan
        if plan.order_type == "LIMIT":
            last_book = len(self.events) - 1 - self.events[::-1].index("book")
            assert self.events[last_book + 1:] == ["sign", "sign"]
        self.events.append("send")
        return await super().submit_order(plan)

    async def invalidate_prepared_order(self, prepared):
        pass


@pytest.mark.asyncio
async def test_slow_context_and_nonce_work_finishes_before_each_final_quote(tmp_path):
    clock = AdvancingClock()
    client = PreQuoteClient(clock)
    result = await run_random_cycle(cycle_config(tmp_path / "late", max_quote_age_seconds=0.05),
                                    client, clock=clock, rng=FixedRng(20, 20))
    assert result.outcome is Outcome.SUCCESS, result.as_dict()
    assert result.opening.latency["source_quote_age_seconds"] == 0
    assert result.closing.latency["source_quote_age_seconds"] == 0
    assert len(client.tokens) == 4 and not any(t.valid for t in client.tokens)


@pytest.mark.asyncio
async def test_prepared_context_is_single_use_and_cannot_bind_another_plan(tmp_path):
    from test_hood_handoff_paired_opening import PairedClient, config
    client = PairedClient()
    cfg = config(tmp_path / "child.jsonl")
    metadata = await client.market_metadata(cfg.market_id)
    source = await client.account_snapshot(11, cfg.market_id)
    receiver = await client.account_snapshot(22, cfg.market_id)
    ctx = HandoffPreflightContext(cfg, metadata, source, receiver, {})
    with pytest.raises(ContractError):
        ctx.claim(replace(cfg, quantity=Decimal("0.30")))
    assert ctx.claim(cfg) == (metadata, source, receiver)
    with pytest.raises(ContractError):
        ctx.claim(cfg)


@pytest.mark.asyncio
async def test_reserved_nonce_is_exclusive_and_transfers_once_to_exact_signature(monkeypatch):
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client = LighterSdkClient(_sdk_config("reserved"), source_account_index=11, receiver_account_index=22,
                             secrets=StaticSecretProvider({11: "synthetic-a", 22: "synthetic-b"}),
                             market_evidence={}, signer_factory=ConstantNonceSigner, http_factory=FakeHttp)
    deadline = time.monotonic() + 1
    client._lighter = lambda: FakeModule
    token = await client.reserve_order_nonce(11, deadline=deadline)
    with pytest.raises(ContractError):
        await client.reserve_order_nonce(11, deadline=deadline)
    with pytest.raises(ContractError):
        await client.prepare_order(replace(_sdk_plan(account_index=22), mutation_deadline_monotonic=deadline), reserved_nonce=token)
    plan = replace(_sdk_plan(), mutation_deadline_monotonic=deadline)
    prepared = await client.prepare_order(plan, reserved_nonce=token)
    assert prepared.diagnostic_timings["nonce_reserved_before_quote"] == 1
    with pytest.raises(ContractError):
        await client.prepare_order(plan, reserved_nonce=token)
    changed = replace(plan, price=Decimal("101.25"), price_int=10125)
    assert not (await client.submit_prepared_order(changed, prepared)).accepted
    assert not client._http.calls


@pytest.mark.asyncio
async def test_expired_or_foreign_nonce_cannot_sign_and_unsent_release_is_safe(monkeypatch):
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client = _delayed_client("nonce-origin", _delayed_tracker(delay=0))
    other = _delayed_client("nonce-other", _delayed_tracker(delay=0))
    client._lighter = lambda: FakeModule
    other._lighter = lambda: FakeModule
    deadline = time.monotonic() + 1
    token = await client.reserve_order_nonce(11, deadline=deadline)
    plan = replace(_sdk_plan(), mutation_deadline_monotonic=deadline)
    with pytest.raises(ContractError):
        await other.prepare_order(plan, reserved_nonce=token)
    await client.invalidate_reserved_nonce(token)
    assert not client._nonce_reservations
    deadline = time.monotonic() + 0.01
    expired = await client.reserve_order_nonce(11, deadline=deadline)
    await asyncio.sleep(0.015)
    with pytest.raises((ContractError, TimeoutError)):
        await client.prepare_order(replace(plan, mutation_deadline_monotonic=deadline), reserved_nonce=expired)
    await client.invalidate_reserved_nonce(expired)
    assert not client._signer(11).sign_calls


@pytest.mark.asyncio
async def test_account_and_active_orders_overlap_without_renewing_account_time(monkeypatch, tmp_path):
    from test_hood_handoff_sdk_audit import _client, _Transport, _AccountApi, _OrderApi
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    account_started, orders_started = asyncio.Event(), asyncio.Event()
    clock = SimpleNamespace(value=1000.0)
    class Accounts(_AccountApi):
        async def account(self, **kwargs):
            account_started.set()
            await asyncio.wait_for(orders_started.wait(), 0.2)
            return await super().account(**kwargs)
    class Orders(_OrderApi):
        async def account_active_orders(self, **kwargs):
            orders_started.set()
            await asyncio.wait_for(account_started.wait(), 0.2)
            await asyncio.sleep(0.01)
            clock.value = 1003.0
            return {"code": 200, "orders": []}
    client, _ = _client(tmp_path, _Transport(response={"code": 200}), account_api=Accounts,
                        order_api=Orders, clock=lambda: clock.value)
    snapshot = await client.account_snapshot(11, 7)
    assert snapshot.observed_at == 1000.0 and clock.value == 1003.0


@pytest.mark.parametrize("value", [0, -1, True, float("inf"), 11])
def test_temporal_limits_cannot_loosen_freshness(tmp_path, value):
    for name in ("max_quote_age_seconds", "max_source_to_receiver_seconds"):
        with pytest.raises(ContractError):
            cycle_config(tmp_path / name, **{name: value})


@pytest.mark.asyncio
async def test_quote_expiring_during_signing_never_dispatches_source(tmp_path):
    class SlowSigning(PreQuoteClient):
        async def prepare_order(self, plan, **kwargs):
            token = await super().prepare_order(plan, **kwargs)
            self.clock.value += 0.2
            return token
    clock = AdvancingClock()
    client = SlowSigning(clock)
    result = await run_random_cycle(cycle_config(tmp_path / "expired-quote", max_quote_age_seconds=0.05),
                                    client, clock=clock, rng=FixedRng(20, 20))
    assert not client.submissions
    assert result.outcome is not Outcome.SUCCESS
    assert not any(t.valid for t in client.tokens)


@pytest.mark.asyncio
async def test_receiver_latency_budget_blocks_send_after_slow_admission(tmp_path):
    from test_hood_handoff_paired_opening import PairedClient, config, Clock
    from risex_spread_shadow.hood_handoff import run_handoff
    class SlowAdmission(PairedClient):
        async def account_snapshot(self, *args):
            if self.source_order is not None:
                await asyncio.sleep(0.025)
            return await super().account_snapshot(*args)
    client = SlowAdmission()
    result = await run_handoff(config(tmp_path / "late-receiver.jsonl", max_source_to_receiver_seconds=0.01), client, clock=Clock())
    assert len(client.submissions) == 1
    assert client.cancellations
    assert any("latency budget expired" in r for r in result.unknown_reasons)


@pytest.mark.asyncio
async def test_quote_failure_releases_both_reserved_nonces_without_signing(tmp_path):
    class BadQuote(PreQuoteClient):
        async def order_book(self, *args):
            if self.tokens:
                raise ContractError("synthetic malformed final book")
            return await super().order_book(*args)
    clock = AdvancingClock()
    client = BadQuote(clock)
    await run_random_cycle(cycle_config(tmp_path / "bad-book"), client, clock=clock, rng=FixedRng(20, 20))
    assert len(client.tokens) == 2 and not any(t.valid for t in client.tokens)
    assert "sign" not in client.events and not client.submissions


@pytest.mark.asyncio
async def test_unreachable_receiver_price_is_rejected_before_exposing_source(tmp_path):
    from test_hood_handoff_paired_opening import PairedClient, config, Clock
    from risex_spread_shadow.hood_handoff import run_handoff
    client = PairedClient()
    result = await run_handoff(config(tmp_path / "unreachable.jsonl", direction=Direction.SHORT), client, clock=Clock())
    assert result.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED
    assert not client.submissions


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_partial_nonce_preparation_failure_drains_and_releases(tmp_path, cancel):
    from risex_spread_shadow.hood_handoff import RandomCycleEngine
    first_reserved, second_started = asyncio.Event(), asyncio.Event()
    drained = []

    class FailingReservation(PreQuoteClient):
        async def reserve_order_nonce(self, account_index, *, deadline):
            if account_index == self.source_account_index:
                token = await super().reserve_order_nonce(account_index, deadline=deadline)
                first_reserved.set()
                return token
            await first_reserved.wait()
            second_started.set()
            try:
                if cancel:
                    await asyncio.Event().wait()
                raise ContractError("synthetic nonce failure")
            finally:
                drained.append(account_index)

    clock = AdvancingClock()
    client = FailingReservation(clock)
    engine = RandomCycleEngine(client, clock=clock)
    task = asyncio.create_task(engine._parallel_revalidation_context(
        cycle_config(tmp_path / "reservation"), market_read_label="test metadata"))
    await second_started.wait()
    if cancel:
        task.cancel()
    with pytest.raises(asyncio.CancelledError if cancel else ContractError):
        await task
    assert drained == [client.receiver_account_index]
    assert len(client.tokens) == 1 and not client.tokens[0].valid
    assert not engine._pending_nonces and engine._nonce_deadline is None
    assert not client.submissions


@pytest.mark.asyncio
async def test_reserved_nonce_ambiguous_send_cannot_be_reserved_or_replayed(monkeypatch):
    from test_hood_handoff_sdk_interface import _constant_nonce_client, AmbiguousConstantHttp
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client = _constant_nonce_client(prefix="reserved-ambiguous", http_factory=AmbiguousConstantHttp)
    client._lighter = lambda: FakeModule
    deadline = time.monotonic() + 1
    token = await client.reserve_order_nonce(11, deadline=deadline)
    plan = replace(_sdk_plan(), mutation_deadline_monotonic=deadline)
    prepared = await client.prepare_order(plan, reserved_nonce=token)
    with pytest.raises(TimeoutError):
        await client.submit_prepared_order(plan, prepared)
    await client.invalidate_reserved_nonce(token)
    with pytest.raises(ContractError, match="already sent"):
        await client.reserve_order_nonce(11, deadline=time.monotonic() + 1)
    assert not (await client.submit_prepared_order(plan, prepared)).accepted
    assert len(client._http.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_parallel_sdk_read_failure_drains_sibling(monkeypatch, tmp_path, cancel):
    from test_hood_handoff_sdk_audit import _client, _Transport, _AccountApi, _OrderApi
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    orders_started = asyncio.Event()
    drained = []

    class Accounts(_AccountApi):
        async def account(self, **kwargs):
            await orders_started.wait()
            if cancel:
                await asyncio.Event().wait()
            raise ContractError("synthetic account read failure")

    class Orders(_OrderApi):
        async def account_active_orders(self, **kwargs):
            orders_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                drained.append("orders")

    client, _ = _client(tmp_path, _Transport(), account_api=Accounts, order_api=Orders)
    task = asyncio.create_task(client.account_snapshot(11, 7))
    await orders_started.wait()
    if cancel:
        task.cancel()
    with pytest.raises(asyncio.CancelledError if cancel else ContractError):
        await task
    assert drained == ["orders"]


@pytest.mark.asyncio
async def test_reused_context_keeps_original_staleness_barrier(tmp_path):
    from test_hood_handoff_paired_opening import PairedClient, config, Clock
    from risex_spread_shadow.hood_handoff import run_handoff
    client = PairedClient()
    cfg = config(tmp_path / "stale-context.jsonl")
    metadata = await client.market_metadata(cfg.market_id)
    source = await client.account_snapshot(11, cfg.market_id)
    receiver = await client.account_snapshot(22, cfg.market_id)
    source = replace(source, observed_at=source.observed_at - cfg.freshness_seconds - 1)
    client.handoff_preflight_context = HandoffPreflightContext(cfg, metadata, source, receiver, {})
    result = await run_handoff(cfg, client, clock=Clock())
    assert result.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED
    assert not client.submissions


@pytest.mark.asyncio
async def test_nonce_lifetime_preserves_per_request_timeout(tmp_path):
    from risex_spread_shadow.hood_handoff import RandomCycleEngine
    clock = AdvancingClock()
    client = PreQuoteClient(clock)
    engine = RandomCycleEngine(client, clock=clock)
    cfg = cycle_config(tmp_path / "lifetime", request_timeout_seconds=0.1, freshness_seconds=10)
    before = time.monotonic()
    await engine._reserve_preflight_nonces(cfg)
    assert engine._nonce_deadline >= before + 10
    assert all(t.deadline == engine._nonce_deadline for t in client.tokens)
    await engine._release_preflight_nonces()


@pytest.mark.asyncio
async def test_partial_paired_receipts_keep_their_confirmed_matched_volume(tmp_path):
    clock = AdvancingClock()
    cfg = cycle_config(tmp_path / "partial-match")
    result = await run_random_cycle(cfg, CycleClient(clock, partial_close=True), clock=clock, rng=FixedRng(20, 20))
    assert result.outcome is Outcome.PARTIAL
    report = load_saved_cycle_report(cfg.cycle_dir)
    closing = report["paired_execution"]["direct_counterparty_match"][1]
    assert Decimal(closing["matched_quantity"]) == Decimal("0.10")
    assert closing["status"] == "PARTIAL"


@pytest.mark.asyncio
async def test_cycle_provenance_covers_parent_controller_imports_and_configuration(tmp_path):
    import hashlib
    from pathlib import Path
    from risex_spread_shadow.hood_handoff import provenance, telegram_control
    provenance._implementation.cache_clear()
    clock = AdvancingClock()
    cfg = cycle_config(tmp_path / "provenance")
    await run_random_cycle(cfg, CycleClient(clock), clock=clock, rng=FixedRng(20, 20))
    report = load_saved_cycle_report(cfg.cycle_dir)
    runtime = report["source_provenance"]["runtime"]
    assert len(runtime) == 3
    for item in runtime:
        files = {Path(f["path"]).name: f for f in item["files"]}
        for name in ("engine.py", "random_cycle.py", "sdk.py", "cli.py", "telegram_control.py", "offline_report.py"):
            assert files[name]["sha256"] == hashlib.sha256(Path(files[name]["path"]).read_bytes()).hexdigest()
        imports = {entry["module"]: entry["path"] for entry in item["imports"]}
        assert imports[telegram_control.__name__] == str(Path(telegram_control.__file__).resolve())
    expected = hashlib.sha256(json.dumps(cfg.binding(), sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()
    assert runtime[0]["configuration_sha256"] == expected
    assert provenance.capture_provenance({"market_id": 8})["configuration_sha256"] != expected
