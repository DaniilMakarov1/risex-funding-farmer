from __future__ import annotations

import asyncio
from dataclasses import replace
from decimal import Decimal
import inspect
from pathlib import Path

import pytest

import risex_farmer.testnet_risex_level_c_runner as operational
from risex_farmer.testnet_risex_level_c_runner import (
    AuthoritativeState, RisexLevelCRunner, RunnerReport, RunnerResult,
)
from risex_farmer.testnet_risex_order_lifecycle import (
    AccountState, BBO, DurableIntentStore, Evidence, FillRecord,
    Intent, MarketState, OrderRecord, SyntheticSigner,
)
from risex_farmer.testnet_risex_private_read_preflight import (
    ACCOUNT, AUTHORIZATION, HttpResponse, MARKET_ID, MARKET_SYMBOL, MINIMUM,
    ROUTER, SIGNER, STEP, TICK, expected_url,
)


NOW = 1_800_000_000


def order_id(wide: int, block: int) -> str:
    return f"0x{wide:016x}{block:016x}{1:016x}"


ORDER_IDS = tuple(order_id(113 + 2 * index, 100 + index) for index in range(5))


def record(value: str, client: int) -> OrderRecord:
    wide = int(value[2:18], 16)
    return OrderRecord(value, wide, wide >> 1, client)


def order_update(*, client=101, status="ORDER_STATUS_FILLED", filled=None):
    if filled is None:
        filled = str(MINIMUM)
    return {
        "channel": "orders", "type": "update", "market_id": str(MARKET_ID),
        "worker_timestamp": str(int(NOW * 1_000_000_000)),
        "data": [{
            "id": ORDER_IDS[0], "wide_order_id": "113",
            "resting_order_id": "56", "client_order_id": str(client),
            "market_id": str(MARKET_ID), "sender": ACCOUNT, "side": "BUY",
            "type": "MARKET", "time_in_force": "IOC", "status": status,
            "size": str(MINIMUM), "filled_size": filled, "post_only": False,
            "reduce_only": False, "is_liquidation": False,
        }],
    }


def position_frame(size="0", *, kind="update"):
    row = {"account": ACCOUNT, "market_id": str(MARKET_ID), "size": size}
    timestamp = str(int(NOW * 1_000_000_000))
    if kind == "snapshot":
        return {
            "method": "snapshot", "channel": "positions", "type": "snapshot",
            "data": [row], "position_count": 1, "worker_timestamp": timestamp,
        }
    return {
        "channel": "positions", "type": "update", "market_id": str(MARKET_ID),
        "data": [row], "block_number": 123, "log_index": 1,
        "worker_timestamp": timestamp,
    }


def orders_snapshot():
    return {
        "method": "snapshot", "channel": "orders", "type": "snapshot",
        "data": [], "order_count": 0,
        "worker_timestamp": str(int(NOW * 1_000_000_000)),
    }


def opening_intent(*, lifecycle_state="OPEN_KNOWN"):
    return Intent(
        "intent", 1, "OPEN", 101, 7, 3, "a" * 64, "b" * 64,
        lifecycle_state, "BUY", "MARKET", "IOC", False, False, MARKET_ID,
        MINIMUM, 100, Decimal("3009.00"), 300900,
        Decimal("0"), NOW + 60, 1, ORDER_IDS[0], False,
    )


def market(now: int = NOW) -> MarketState:
    return MarketState(
        host="api.testnet.rise.trade", chain_id=11_155_931,
        domain_name="RISEx", domain_version="1", router=ROUTER,
        authorization=AUTHORIZATION, market_id=MARKET_ID, symbol=MARKET_SYMBOL,
        active=True, unlocked=True, tick=TICK,
        step=STEP, minimum=MINIMUM,
        observed_at=now,
    )


def account(position: str = "0", orders=(), now: int = NOW) -> AccountState:
    value = Decimal(position)
    return AccountState(
        account=ACCOUNT, signer=SIGNER, signer_status="ACTIVE",
        position=value, open_order_ids=tuple(orders),
        repeated_open_order_ids=tuple(orders), repeated_position=value,
        observed_at=now,
    )


def state(
    position: str = "0", orders=(), now: int = NOW,
    nonce_anchor: int = 0, nonce_bitmap: int = 0,
) -> AuthoritativeState:
    return AuthoritativeState(
        market(now), account(position, orders, now),
        BBO(
            bid=Decimal("2999.00"), ask=Decimal("3000.00"),
            bid_depth=MINIMUM, ask_depth=MINIMUM,
            observed_at=now,
        ),
        nonce_anchor, nonce_bitmap,
    )


def terminal(intent, *, filled: str, position: str, now: int = NOW) -> Evidence:
    item = record(intent.order_id, intent.client_order_id)
    amount = Decimal(filled)
    return Evidence(
        account=ACCOUNT, signer=SIGNER, signer_status="ACTIVE",
        terminal=True, filled_size=amount, position=Decimal(position),
        observed_at=now, position_market_id=MARKET_ID, by_id_order=item,
        history_orders=(item,),
        fills=(FillRecord(item.order_id, item.client_order_id),) if amount else (),
    )


class Capability:
    def __init__(self, states, evidence):
        self.states = list(states)
        self._evidence = evidence
        self.closed = False

    def state(self):
        return self.states.pop(0)

    def evidence(self, intent):
        return self._evidence(intent)

    def close(self):
        self.closed = True


class Binding:
    def __init__(self, *, ambiguous=False, crash=False):
        self.run_id = "runtime-run-id"
        self.calls = []
        self.ambiguous = ambiguous
        self.crash = crash

    def dispatch_place(self, lifecycle, intent, market):
        selected = ORDER_IDS[intent.ordinal - 1]

        def execute(_):
            persisted = lifecycle.store.get(intent.intent_id)
            assert persisted.state == "DISPATCHING"
            assert persisted.dispatch_count == 1
            self.calls.append(("PLACE", intent.intent_id))
            if self.crash:
                raise KeyboardInterrupt
            if self.ambiguous:
                raise TimeoutError
            return selected

        lifecycle.dispatch(intent, SyntheticSigner(SIGNER), execute)

    def cancel_known(self, lifecycle, order_id, **values):
        self.calls.append(("CANCEL", order_id))
        lifecycle.cancel_known(
            order_id, synthetic_signer=SyntheticSigner(SIGNER),
            execute=lambda _: None, **values,
        )


def identities():
    values = iter((
        (101, 7, 1), (102, 7, 2), (103, 7, 3),
        (104, 7, 4), (105, 7, 5), (106, 7, 6),
    ))
    return lambda: next(values)


def runner(tmp_path: Path, binding: Binding, *, now=lambda: NOW):
    store = DurableIntentStore(tmp_path / "lifecycle.sqlite")
    return RisexLevelCRunner._fixture(
        store=store, binding=binding, clock=now, identity=identities(),
    )


def test_operational_surface_is_opt_in_fixed_and_absent_from_normal_startup():
    assert tuple(inspect.signature(RisexLevelCRunner).parameters) == ()
    assert tuple(inspect.signature(operational.run_risex_level_c).parameters) == ()
    root = Path(__file__).parents[1]
    assert not any(
        "testnet_risex_level_c_runner" in (root / path).read_text()
        for path in (
            "src/risex_farmer/__init__.py", "src/risex_farmer/cli.py",
            "src/risex_farmer/runtime.py", "src/risex_farmer/orchestrator.py",
        )
    )


def test_production_capability_construction_reuses_fixed_auth_dependencies():
    class Credential:
        def __init__(self):
            self.closed = False

        def derive_signer_address(self):
            return SIGNER

        def sign_register_v2(self, _typed):
            return "0x" + "ab" * 65

        def close(self):
            self.closed = True

    credential = Credential()

    class Source:
        def __init__(self):
            self.loaded = False
            self.closed = False

        def load(self):
            self.loaded = True

        def open(self):
            assert self.loaded
            return credential

        def close(self):
            self.closed = True

    class Transport:
        def __init__(self):
            self.frame = None
            self.closed = False

        async def nonce_get(self):
            return HttpResponse(
                200, expected_url("/v1/auth/nonce", (("account", ACCOUNT),)),
                {"data": {"nonce": "0x0001"}, "request_id": "fixed"},
                NOW, False,
            )

        async def auth_v2_dispatch(self, frame):
            self.frame = frame

        async def auth_v2_receive(self):
            return '{"method":"auth_v2","status":"success"}'

        async def close(self):
            self.closed = True

    source = Source()
    transport = Transport()
    capability = asyncio.run(operational._ProductionReadCapability._create(
        source_factory=lambda: source, transport_factory=lambda: transport,
        clock=lambda: NOW,
    ))
    assert operational._capability_is_exact(capability)
    assert transport.frame["params"]["account"] == ACCOUNT
    assert transport.frame["params"]["signer"] == SIGNER
    assert not credential.closed and not source.closed and not transport.closed
    asyncio.run(capability.close())
    assert credential.closed and source.closed and transport.closed


def test_zero_argument_entrypoint_returns_sanitized_bounded_report(monkeypatch):
    report = RunnerReport(
        "runtime-only", RunnerResult.COMPLETED_NO_FILL_FLAT,
        1, 1, 0, False,
    )

    async def fixed_operation():
        return report

    monkeypatch.setattr(operational, "_run_production", fixed_operation)
    assert operational.run_risex_level_c() == report
    assert report.sanitized() == {
        "result": "COMPLETED_NO_FILL_FLAT", "run_id": "runtime-only",
        "intent_count": 1, "dispatch_count": 1, "close_attempts": 0,
        "manual_recovery": False,
        "opening_place_result": None, "opening_place_failure": None,
    }


def test_production_capability_emits_authoritative_state_and_exact_evidence():
    class Resource:
        def close(self):
            return None

    class Transport:
        def __init__(self):
            self.frames = []

        async def _get(self, path, query):
            assert path == f"/v1/nonce-state/{ACCOUNT}" and query == ()
            return HttpResponse(
                200, "fixed", {
                    "data": {
                        "nonce_anchor": "7", "current_bitmap_index": 3,
                        "bitmap": "0x0",
                    },
                    "request_id": "fixed",
                }, NOW, False,
            )

        async def _receive(self):
            return self.frames.pop(0)

        async def close(self):
            return None

    transport = Transport()
    capability = operational._ProductionReadCapability(
        Resource(), Resource(), transport, lambda: NOW,
    )

    async def public_prestate():
        return None

    async def current_market():
        value = state()
        return value.market, value.bbo

    async def current_account():
        return account()

    capability._full_public_prestate = public_prestate
    capability._market_state = current_market
    capability._account = current_account
    observed = asyncio.run(capability.state())
    assert (observed.nonce_anchor, observed.nonce_bitmap) == (7, 3)

    intent = Intent(
        "intent", 1, "OPEN", 101, 7, 3, "a" * 64, "b" * 64,
        "DISPATCHED", "BUY", "MARKET", "IOC", False, False, MARKET_ID,
        MINIMUM, 100, Decimal("3009.00"), 300900,
        Decimal("0"), NOW + 60, 1, ORDER_IDS[0], False,
    )
    capability._subscribed = True
    capability._updates[101] = order_update()["data"][0]
    capability._position = (Decimal("0"), False, NOW * 1_000_000_000, 1)
    capability._stream_sequence = 1
    transport.frames = [position_frame(str(MINIMUM))]
    evidence = asyncio.run(capability.evidence(intent))
    assert evidence.terminal and evidence.position == MINIMUM
    assert evidence.filled_size == MINIMUM
    assert evidence.by_id_order == record(ORDER_IDS[0], 101)


def test_production_stream_demuxes_interleaved_frames_without_resubscribe():
    class Resource:
        def close(self):
            return None

    class Transport:
        def __init__(self):
            self.orders_subscriptions = 0
            self.positions_subscriptions = 0
            self.frames = [
                {
                    "method": "subscribe", "status": "success", "data": {},
                    "channel": "positions", "type": "success",
                },
                position_frame(kind="snapshot"),
                orders_snapshot(),
            ]

        async def orders_subscribe(self):
            self.orders_subscriptions += 1

        async def positions_subscribe(self):
            self.positions_subscriptions += 1

        async def _receive(self):
            return self.frames.pop(0)

        async def close(self):
            return None

    transport = Transport()
    capability = operational._ProductionReadCapability(
        Resource(), Resource(), transport, lambda: NOW,
    )
    first = asyncio.run(capability._account())
    second = asyncio.run(capability._account())
    assert first == second == account()
    assert transport.orders_subscriptions == transport.positions_subscriptions == 1

    intent = Intent(
        "intent", 1, "OPEN", 101, 7, 3, "a" * 64, "b" * 64,
        "DISPATCHED", "BUY", "MARKET", "IOC", False, False, MARKET_ID,
        MINIMUM, 100, Decimal("3009.00"), 300900,
        Decimal("0"), NOW + 60, 1, ORDER_IDS[0], False,
    )
    transport.frames.extend([position_frame(str(MINIMUM)), order_update()])
    evidence = asyncio.run(capability.evidence(intent))
    assert evidence.position == MINIMUM and evidence.terminal
    current = asyncio.run(capability._account())
    assert current.position == MINIMUM and not current.open_order_ids
    assert transport.orders_subscriptions == transport.positions_subscriptions == 1


def test_production_stream_is_bounded_and_rejects_foreign_updates():
    class Resource:
        def close(self):
            return None

    class Transport:
        def __init__(self, frames):
            self.frames = list(frames)
            self.receives = self.orders_subscriptions = self.positions_subscriptions = 0

        async def orders_subscribe(self):
            self.orders_subscriptions += 1

        async def positions_subscribe(self):
            self.positions_subscriptions += 1

        async def _receive(self):
            self.receives += 1
            return self.frames.pop(0)

        async def close(self):
            return None

    bounded = Transport([position_frame(kind="snapshot") for _ in range(8)])
    capability = operational._ProductionReadCapability(
        Resource(), Resource(), bounded, lambda: NOW,
    )
    with pytest.raises(operational.LifecycleSafetyError, match="unavailable"):
        asyncio.run(capability._account())
    assert bounded.receives == 8
    assert bounded.orders_subscriptions == bounded.positions_subscriptions == 1

    contradictory = Transport([
        position_frame(kind="snapshot"), position_frame(str(MINIMUM)),
    ])
    capability = operational._ProductionReadCapability(
        Resource(), Resource(), contradictory, lambda: NOW,
    )
    with pytest.raises(operational.LifecycleSafetyError, match="contradiction"):
        asyncio.run(capability._account())
    assert contradictory.orders_subscriptions == contradictory.positions_subscriptions == 1

    foreign = Transport([orders_snapshot(), position_frame(kind="snapshot")])
    capability = operational._ProductionReadCapability(
        Resource(), Resource(), foreign, lambda: NOW,
    )
    asyncio.run(capability._account())
    foreign.frames.append(order_update(client=102))
    with pytest.raises(operational.LifecycleSafetyError, match="unrelated"):
        asyncio.run(capability._pump_until(lambda: False, expected_client=101))
    assert foreign.orders_subscriptions == foreign.positions_subscriptions == 1


def test_production_no_event_no_identity_waits_for_expiry_then_proves_zero_flat(
    monkeypatch,
):
    class Resource:
        def close(self):
            return None

    class Transport:
        def __init__(self):
            self.frames = [position_frame() for _ in range(8)]
            self.receives = 0

        async def _receive(self):
            self.receives += 1
            return self.frames.pop(0)

        async def close(self):
            return None

    current = [NOW]
    sleeps = []

    async def advance(seconds):
        sleeps.append(seconds)
        current[0] += seconds

    transport = Transport()
    capability = operational._ProductionReadCapability(
        Resource(), Resource(), transport, lambda: current[0],
    )
    capability._subscribed = True
    capability._orders = ()
    capability._position = (Decimal("0"), False, NOW * 1_000_000_000, 1)
    capability._stream_sequence = 1
    barriers = []

    async def repeated_zero_flat_barrier():
        barriers.append(current[0])
        return int(current[0])

    capability._full_public_prestate = repeated_zero_flat_barrier
    monkeypatch.setattr(operational.asyncio, "sleep", advance)
    intent = replace(
        opening_intent(lifecycle_state="AMBIGUOUS"),
        order_id=None, expires_at=NOW + 60,
    )
    evidence = asyncio.run(capability.evidence(intent))
    assert transport.receives == 8
    assert sleeps == [61] and current[0] == NOW + 61
    assert barriers == [NOW + 61]
    assert evidence == Evidence(
        account=ACCOUNT, signer=SIGNER, signer_status="ACTIVE",
        terminal=True, filled_size=Decimal("0"), position=Decimal("0"),
        observed_at=NOW + 61, position_market_id=MARKET_ID,
    )


def test_production_quiet_timeout_after_initial_zero_snapshots_uses_barrier(
    monkeypatch,
):
    class Resource:
        def close(self):
            return None

    class Transport:
        def __init__(self):
            self.frames = [
                {
                    "method": "subscribe", "status": "success", "data": {},
                    "channel": "orders", "type": "success",
                },
                orders_snapshot(),
                {
                    "method": "subscribe", "status": "success", "data": {},
                    "channel": "positions", "type": "success",
                },
                position_frame(kind="snapshot"),
            ]
            self.receives = self.orders_subscriptions = self.positions_subscriptions = 0

        async def orders_subscribe(self):
            self.orders_subscriptions += 1

        async def positions_subscribe(self):
            self.positions_subscriptions += 1

        async def _receive(self):
            self.receives += 1
            if self.frames:
                return self.frames.pop(0)
            raise TimeoutError

        async def close(self):
            return None

    current = [NOW]
    sleeps = []

    async def advance(seconds):
        sleeps.append(seconds)
        current[0] += seconds

    transport = Transport()
    capability = operational._ProductionReadCapability(
        Resource(), Resource(), transport, lambda: current[0],
    )
    assert asyncio.run(capability._account()) == account()
    barriers = []

    async def repeated_zero_flat_barrier():
        barriers.append(current[0])
        return int(current[0])

    capability._full_public_prestate = repeated_zero_flat_barrier
    monkeypatch.setattr(operational.asyncio, "sleep", advance)
    intent = replace(
        opening_intent(lifecycle_state="AMBIGUOUS"),
        order_id=None, expires_at=NOW + 60,
    )
    evidence = asyncio.run(capability.evidence(intent))
    assert transport.receives == 5
    assert transport.orders_subscriptions == transport.positions_subscriptions == 1
    assert sleeps == [61] and barriers == [NOW + 61]
    assert evidence.terminal and evidence.filled_size == evidence.position == 0
    assert evidence.by_id_order is None


@pytest.mark.parametrize("failure", [ConnectionResetError, TimeoutError])
def test_production_no_event_timeout_fallback_rejects_disconnect_or_missing_snapshot(
    failure,
):
    class Resource:
        def close(self):
            return None

    class Transport:
        async def _receive(self):
            raise failure

        async def close(self):
            return None

    capability = operational._ProductionReadCapability(
        Resource(), Resource(), Transport(), lambda: NOW,
    )
    capability._subscribed = True
    capability._orders = ()
    capability._position = (Decimal("0"), False, NOW * 1_000_000_000, 1)
    capability._stream_sequence = 1
    if failure is ConnectionResetError:
        capability._orders_snapshot_received = True
        capability._position_snapshot_received = True
    intent = replace(
        opening_intent(lifecycle_state="AMBIGUOUS"),
        order_id=None, expires_at=NOW + 60,
    )
    with pytest.raises(failure):
        asyncio.run(capability.evidence(intent))


@pytest.mark.parametrize(
    "change",
    [
        {"kind": "CLOSE", "reduce_only": True},
        {"order_id": ORDER_IDS[0]},
        {"order_type": "MARKET", "time_in_force": "FOK"},
    ],
)
def test_production_no_event_fallback_rejects_non_observed_vectors(change):
    class Resource:
        def close(self):
            return None

    class Transport:
        def __init__(self):
            self.frames = [position_frame() for _ in range(8)]

        async def _receive(self):
            return self.frames.pop(0)

        async def close(self):
            return None

    capability = operational._ProductionReadCapability(
        Resource(), Resource(), Transport(), lambda: NOW,
    )
    capability._subscribed = True
    capability._orders = ()
    capability._position = (Decimal("0"), False, NOW * 1_000_000_000, 1)
    capability._stream_sequence = 1
    values = {"order_id": None, "expires_at": NOW + 60, **change}
    intent = replace(opening_intent(lifecycle_state="AMBIGUOUS"), **values)
    with pytest.raises(operational.LifecycleSafetyError, match="unavailable"):
        asyncio.run(capability.evidence(intent))


def test_production_no_event_fallback_rejects_zero_flat_barrier_contradiction():
    class Resource:
        def close(self):
            return None

    class Transport:
        def __init__(self):
            self.frames = [position_frame() for _ in range(8)]

        async def _receive(self):
            return self.frames.pop(0)

        async def close(self):
            return None

    capability = operational._ProductionReadCapability(
        Resource(), Resource(), Transport(), lambda: NOW,
    )
    capability._subscribed = True
    capability._orders = ()
    capability._position = (Decimal("0"), False, NOW * 1_000_000_000, 1)
    capability._stream_sequence = 1

    async def contradiction():
        raise operational.LifecycleSafetyError("RISEx Level C public prestate stale")

    capability._full_public_prestate = contradiction
    intent = replace(
        opening_intent(lifecycle_state="AMBIGUOUS"),
        order_id=None, expires_at=NOW - 1,
    )
    with pytest.raises(operational.LifecycleSafetyError, match="prestate stale"):
        asyncio.run(capability.evidence(intent))


def test_production_prestate_requires_two_complete_fresh_zero_flat_sweeps():
    class Resource:
        def close(self):
            return None

    class Transport:
        def __init__(self):
            self.calls = []

        async def public_get(self, index):
            self.calls.append(index)
            return type("Response", (), {"observed_at": NOW})()

        async def close(self):
            return None

    class Validator:
        def __init__(self):
            self.sweeps = []

        def _validate_response(self, path, query, response):
            assert response.observed_at == NOW
            return (path, query)

        def _validate_sweep(self, responses):
            assert tuple(responses) == tuple(path for path, _ in operational._PUBLIC_REQUESTS)
            self.sweeps.append(dict(responses))

    transport = Transport()
    capability = operational._ProductionReadCapability(
        Resource(), Resource(), transport, lambda: NOW,
    )
    validator = Validator()
    capability._validator = validator
    asyncio.run(capability._full_public_prestate())
    assert transport.calls == list(range(9)) * 2
    assert len(validator.sweeps) == 2


def test_pre_state_contradiction_blocks_before_any_intent_or_dispatch(tmp_path):
    binding = Binding()
    candidate = runner(tmp_path, binding)
    capability = Capability([state(position=str(MINIMUM))], lambda _: pytest.fail())
    report = asyncio.run(candidate.run(capability))
    assert report.result is RunnerResult.BLOCKED_BEFORE_WRITE
    assert report.intent_count == report.dispatch_count == 0
    assert binding.calls == [] and capability.closed


def test_read_failure_after_dispatch_halts_without_second_write(tmp_path):
    binding = Binding()
    candidate = runner(tmp_path, binding)

    def failed_read(_intent):
        raise TimeoutError

    capability = Capability([state()], failed_read)
    report = asyncio.run(candidate.run(capability))
    assert report.result is RunnerResult.FAILED_HALTED_MANUAL_RECOVERY
    assert report.dispatch_count == 1 and len(binding.calls) == 1
    assert capability.closed


def test_fill_then_close_is_durable_price_bounded_and_finishes_zero_flat(tmp_path):
    binding = Binding()
    candidate = runner(tmp_path, binding)

    def evidence(intent):
        if intent.kind == "OPEN":
            return terminal(intent, filled=str(MINIMUM), position=str(MINIMUM))
        return terminal(intent, filled=str(MINIMUM), position="0")

    capability = Capability(
        [state(), state(str(MINIMUM)), state()], evidence,
    )
    report = asyncio.run(candidate.run(capability))
    intents = candidate._store.all()
    assert report.result is RunnerResult.SUCCESS_CLOSED_FLAT
    assert report.dispatch_count == 2 and report.close_attempts == 1
    assert [(item.kind, item.order_type, item.time_in_force, item.reduce_only) for item in intents] == [
        ("OPEN", "MARKET", "IOC", False),
        ("CLOSE", "MARKET", "FOK", True),
    ]
    assert all(item.size * item.price <= Decimal("500") for item in intents)
    assert capability.closed


def test_fok_no_fill_ends_only_after_fresh_zero_flat_barrier(tmp_path):
    binding = Binding()
    candidate = runner(tmp_path, binding)
    capability = Capability(
        [state(), state()],
        lambda intent: terminal(intent, filled="0", position="0"),
    )
    report = asyncio.run(candidate.run(capability))
    assert report.result is RunnerResult.COMPLETED_NO_FILL_FLAT
    assert report.intent_count == report.dispatch_count == 1


def test_no_fill_final_barrier_rejects_unexplained_account_state(tmp_path):
    binding = Binding()
    candidate = runner(tmp_path, binding)
    unexplained = state()
    unexplained = replace(
        unexplained, account=replace(unexplained.account, unexplained=True),
    )
    capability = Capability(
        [state(), unexplained],
        lambda intent: terminal(intent, filled="0", position="0"),
    )
    report = asyncio.run(candidate.run(capability))
    assert report.result is RunnerResult.FAILED_HALTED_MANUAL_RECOVERY
    assert report.manual_recovery


def test_close_fallback_uses_exact_residual_and_halts_at_three_attempts(tmp_path):
    binding = Binding()
    candidate = runner(tmp_path, binding)
    positions = iter((
        ("0.1", "0.1"),
        ("0", "0.1"),
        ("0.06", "0.04"),
        ("0", "0.04"),
    ))
    capability = Capability(
        [state(), state("0.1"), state("0.1"), state("0.04")],
        lambda intent: terminal(intent, filled=(pair := next(positions))[0], position=pair[1]),
    )
    report = asyncio.run(candidate.run(capability))
    closes = [item for item in candidate._store.all() if item.kind == "CLOSE"]
    assert report.result is RunnerResult.FAILED_HALTED_MANUAL_RECOVERY
    assert [(item.order_type, item.time_in_force, item.size) for item in closes] == [
        ("MARKET", "FOK", Decimal("0.1")),
        ("LIMIT", "IOC", Decimal("0.1")),
        ("LIMIT", "IOC", Decimal("0.04")),
    ]
    assert len(binding.calls) == 4 and report.manual_recovery


def test_ambiguous_dispatch_is_reconciled_once_without_replay(tmp_path):
    binding = Binding(ambiguous=True)
    candidate = runner(tmp_path, binding)
    capability = Capability(
        [state(), state()],
        lambda intent: terminal(
            type("Current", (), {
                "order_id": ORDER_IDS[0], "client_order_id": intent.client_order_id,
            })(),
            filled="0", position="0",
        ),
    )
    report = asyncio.run(candidate.run(capability))
    assert report.result is RunnerResult.COMPLETED_NO_FILL_FLAT
    assert binding.calls == [("PLACE", candidate._store.all()[0].intent_id)]
    assert candidate._store.all()[0].dispatch_count == 1


def test_exact_known_resting_order_is_cancelled_once_before_terminal_no_fill(tmp_path):
    binding = Binding(ambiguous=True)
    candidate = runner(tmp_path, binding)
    calls = 0

    def evidence(intent):
        nonlocal calls
        calls += 1
        item = record(ORDER_IDS[0], intent.client_order_id)
        if calls == 1:
            return Evidence(
                account=ACCOUNT, signer=SIGNER, signer_status="ACTIVE",
                terminal=False, filled_size=Decimal("0"), position=Decimal("0"),
                observed_at=NOW, position_market_id=MARKET_ID, by_id_order=item,
                open_orders=(item,), history_orders=(item,),
            )
        return Evidence(
            account=ACCOUNT, signer=SIGNER, signer_status="ACTIVE",
            terminal=True, filled_size=Decimal("0"), position=Decimal("0"),
            observed_at=NOW, position_market_id=MARKET_ID, by_id_order=item,
            history_orders=(item,),
        )

    capability = Capability(
        [state(), state(orders=(ORDER_IDS[0],)), state(), state()], evidence,
    )
    report = asyncio.run(candidate.run(capability))
    assert report.result is RunnerResult.COMPLETED_NO_FILL_FLAT
    assert [kind for kind, _ in binding.calls] == ["PLACE", "CANCEL"]
    assert candidate._store.cancel_states() == ["TERMINAL"]


def test_production_cache_consumes_terminal_cancel_before_fresh_flat_barrier(tmp_path):
    events = []

    class Resource:
        def close(self):
            events.append("resource-close")

    class Transport:
        def __init__(self):
            self.orders_subscriptions = 0
            self.positions_subscriptions = 0
            self.frames = [
                orders_snapshot(), position_frame(kind="snapshot"),
                order_update(status="ORDER_STATUS_OPEN", filled="0"),
                order_update(status="ORDER_STATUS_CANCELLED", filled="0"),
            ]

        async def orders_subscribe(self):
            self.orders_subscriptions += 1

        async def positions_subscribe(self):
            self.positions_subscriptions += 1

        async def _receive(self):
            value = self.frames.pop(0)
            events.append("cancel-update" if (
                value.get("channel") == "orders"
                and value.get("type") == "update"
                and value["data"][0]["status"] == "ORDER_STATUS_CANCELLED"
            ) else "stream-frame")
            return value

        async def _get(self, path, query):
            assert path == f"/v1/nonce-state/{ACCOUNT}" and query == ()
            return HttpResponse(200, "fixed", {
                "data": {
                    "nonce_anchor": "7", "current_bitmap_index": 3,
                    "bitmap": "0x0",
                },
                "request_id": "fixed",
            }, NOW, False)

        async def close(self):
            events.append("transport-close")

    transport = Transport()
    capability = operational._ProductionReadCapability(
        Resource(), Resource(), transport, lambda: NOW,
    )

    async def public_flat_barrier():
        events.append("flat-barrier")

    async def current_market():
        value = state()
        return value.market, value.bbo

    capability._full_public_prestate = public_flat_barrier
    capability._market_state = current_market
    binding = Binding(ambiguous=True)
    candidate = runner(tmp_path, binding)
    report = asyncio.run(candidate.run(capability))
    assert report.result is RunnerResult.COMPLETED_NO_FILL_FLAT
    assert [kind for kind, _ in binding.calls] == ["PLACE", "CANCEL"]
    assert candidate._store.cancel_states() == ["TERMINAL"]
    assert transport.orders_subscriptions == transport.positions_subscriptions == 1
    cancel_index = events.index("cancel-update")
    assert events[cancel_index + 1:cancel_index + 3] == [
        "flat-barrier", "flat-barrier",
    ]


@pytest.mark.parametrize("failure", ["unrelated", "missing"])
def test_production_cancel_update_is_exact_and_bounded(failure):
    class Resource:
        def close(self):
            return None

    class Transport:
        def __init__(self):
            self.receives = 0
            self.frames = (
                [order_update(client=102, status="ORDER_STATUS_CANCELLED", filled="0")]
                if failure == "unrelated"
                else [position_frame() for _ in range(8)]
            )

        async def _receive(self):
            self.receives += 1
            return self.frames.pop(0)

        async def close(self):
            return None

    transport = Transport()
    capability = operational._ProductionReadCapability(
        Resource(), Resource(), transport, lambda: NOW,
    )
    capability._subscribed = True
    capability._orders = (order_update(status="ORDER_STATUS_OPEN", filled="0")["data"][0],)
    capability._position = (Decimal("0"), False, NOW * 1_000_000_000, 1)
    capability._stream_sequence = 1
    match = "unrelated" if failure == "unrelated" else "unavailable"
    with pytest.raises(operational.LifecycleSafetyError, match=match):
        asyncio.run(capability.evidence(opening_intent()))
    assert transport.receives == (1 if failure == "unrelated" else 8)


def test_process_interruption_leaves_dispatch_claim_and_restart_only_reconciles(tmp_path):
    first_binding = Binding(crash=True)
    first = runner(tmp_path, first_binding)
    with pytest.raises(KeyboardInterrupt):
        asyncio.run(first.run(Capability([state()], lambda _: pytest.fail())))
    persisted = first._store.all()[0]
    assert persisted.state == "DISPATCHING" and persisted.dispatch_count == 1
    first._store.close()

    second_binding = Binding()
    reopened = RisexLevelCRunner._fixture(
        store=DurableIntentStore(tmp_path / "lifecycle.sqlite"),
        binding=second_binding, clock=lambda: NOW + 61, identity=identities(),
    )
    capability = Capability(
        [state(now=NOW + 61)],
        lambda intent: Evidence(
            account=ACCOUNT, signer=SIGNER, signer_status="ACTIVE",
            terminal=True, filled_size=Decimal("0"), position=Decimal("0"),
            observed_at=NOW + 61, position_market_id=MARKET_ID,
        ),
    )
    report = asyncio.run(reopened.run(capability))
    assert report.result is RunnerResult.COMPLETED_NO_FILL_FLAT
    assert second_binding.calls == []


@pytest.mark.parametrize("observed_bitmap,dispatches", [(1, 1), (2, 0)])
def test_prepared_restart_dispatches_only_with_same_unconsumed_nonce(
    tmp_path, observed_bitmap, dispatches,
):
    initial = runner(tmp_path, Binding())
    lifecycle = initial._lifecycle()
    ready = state()
    prepared = lifecycle.prepare_open(
        lifecycle.preflight(ready.market, ready.account, ready.bbo),
        101, 7, 1, NOW + 60,
    )
    initial._store.close()
    binding = Binding()
    reopened = RisexLevelCRunner._fixture(
        store=DurableIntentStore(tmp_path / "lifecycle.sqlite"),
        binding=binding, clock=lambda: NOW, identity=identities(),
    )
    reopened._identity = None
    capability = Capability(
        [state(nonce_anchor=7, nonce_bitmap=observed_bitmap), state()],
        lambda intent: terminal(intent, filled="0", position="0"),
    )
    report = asyncio.run(reopened.run(capability))
    assert len(binding.calls) == dispatches
    assert reopened._store.get(prepared.intent_id).dispatch_count == dispatches
    assert report.result is (
        RunnerResult.COMPLETED_NO_FILL_FLAT if dispatches
        else RunnerResult.FAILED_HALTED_MANUAL_RECOVERY
    )


def test_final_nonflat_or_unrelated_state_fails_manual_recovery(tmp_path):
    binding = Binding()
    candidate = runner(tmp_path, binding)

    def evidence(intent):
        return terminal(
            intent, filled=str(MINIMUM),
            position=str(MINIMUM) if intent.kind == "OPEN" else "0",
        )

    capability = Capability(
        [state(), state(str(MINIMUM)), state(orders=(ORDER_IDS[4],))], evidence,
    )
    report = asyncio.run(candidate.run(capability))
    assert report.result is RunnerResult.FAILED_HALTED_MANUAL_RECOVERY
    assert report.manual_recovery
