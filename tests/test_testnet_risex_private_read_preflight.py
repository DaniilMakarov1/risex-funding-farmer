from __future__ import annotations

import copy
from dataclasses import replace
from decimal import Decimal
import inspect
from pathlib import Path

import pytest

from risex_farmer.testnet_risex_private_read_preflight import (
    ACCOUNT,
    AUTHORIZATION,
    CHAIN_ID,
    DOMAIN_NAME,
    DOMAIN_VERSION,
    HttpResponse,
    Outcome,
    PrivateReadPreflight,
    PrivateReadStore,
    REGISTERED_AT,
    ROUTER,
    SIGNER,
    SIGNER_EXPIRATION,
    SyntheticCredential,
    WS_ORIGIN,
    expected_url,
)


NOW = 1_787_572_800.0
SIGNATURE = "0x" + "ab" * 65


class Clock:
    def __init__(self, value: float = NOW) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


def envelope(data):
    return {"data": data, "request_id": "fixture-request"}


def public_bodies():
    return {
        "/v1/system/config": envelope({
            "chain": {"chain_id": str(CHAIN_ID)},
            "addresses": {"auth": AUTHORIZATION, "router": ROUTER},
            "is_maintenance_mode": False,
        }),
        "/v1/auth/eip712-domain": envelope({
            "name": DOMAIN_NAME,
            "version": DOMAIN_VERSION,
            "chain_id": str(CHAIN_ID),
            "verifying_contract": AUTHORIZATION,
        }),
        "/v1/auth/session-key-status": envelope({
            "status": 1, "status_description": "Active",
        }),
        "/v1/auth/signers": envelope({"signers": [{
            "signer": SIGNER,
            "status": "Active",
            "registered_at": REGISTERED_AT,
            "expiration": SIGNER_EXPIRATION,
        }]}),
        "/v1/markets": envelope({"markets": [{
            "market_id": "1",
            "active": True,
            "base_asset_symbol": "BTC",
            "quote_asset_symbol": "USDC",
            "config": {
                "name": "BTC/USDC",
                "unlocked": True,
                "step_size": "0.000001",
                "step_price": "0.1",
                "min_order_size": "0.0001",
            },
        }]}),
        "/v1/orderbook": envelope({
            "market_id": "1",
            "bids": [{"price": "77963.3", "quantity": "0.0001"}],
            "asks": [{"price": "77963.4", "quantity": "0.0001"}],
        }),
        "/v1/orders/open": envelope({"orders": []}),
        "/v1/account/position": envelope({
            "position": {"market_id": "0", "size": "0", "side": "Long"},
        }),
        "/v1/positions": envelope({"positions": []}),
    }


class PublicTransport:
    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self.calls = []
        self.mutations = {}

    async def __call__(self, path, query):
        sweep = len(self.calls) // 9
        self.calls.append((path, query))
        body = copy.deepcopy(public_bodies()[path])
        response = HttpResponse(
            200, expected_url(path, query), body, self.clock(), False,
        )
        mutation = self.mutations.get((sweep, path)) or self.mutations.get(path)
        return response if mutation is None else mutation(response)


def expected_public_calls():
    account = (("account", ACCOUNT),)
    return [
        ("/v1/system/config", ()),
        ("/v1/auth/eip712-domain", ()),
        ("/v1/auth/session-key-status", account + (("signer", SIGNER),)),
        ("/v1/auth/signers", account),
        ("/v1/markets", (("force_refresh", "true"), ("market_ids", "1"))),
        ("/v1/orderbook", (("market_id", "1"),)),
        ("/v1/orders/open", account),
        ("/v1/account/position", account + (("market_id", "1"),)),
        ("/v1/positions", account),
    ]


def private_frames(*, orders=(), positions=()):
    return (
        {"method": "auth_v2", "status": "success"},
        {"method": "snapshot", "channel": "orders", "type": "snapshot",
         "data": list(orders), "order_count": len(orders),
         "worker_timestamp": "1787572800000000000"},
        {"method": "snapshot", "channel": "positions", "type": "snapshot",
         "data": list(positions), "position_count": len(positions),
         "worker_timestamp": "1787572800000000000"},
    )


def test_exact_official_empty_private_snapshots_are_accepted():
    PrivateReadPreflight._validate_private_frames(private_frames(), NOW)


@pytest.mark.parametrize("case", [
    "fabricated", "extra", "count", "timestamp", "channel", "type",
])
def test_nonofficial_or_contradictory_private_snapshot_schema_rejected(case):
    frames = list(copy.deepcopy(private_frames()))
    if case == "fabricated":
        frames[1] = {
            "method": "snapshot", "channel": "orders", "account": ACCOUNT,
            "data": {"orders": []},
        }
    elif case == "extra":
        frames[1]["account"] = ACCOUNT
    elif case == "count":
        frames[1]["order_count"] = 1
    elif case == "timestamp":
        frames[2]["worker_timestamp"] = "0"
    elif case == "channel":
        frames[1]["channel"] = "positions"
    else:
        frames[2]["type"] = "update"
    with pytest.raises(ValueError):
        PrivateReadPreflight._validate_private_frames(tuple(frames), NOW)


@pytest.mark.parametrize("worker_timestamp", [
    "1", str(int((NOW + 6) * 1_000_000_000)),
])
def test_stale_or_future_private_snapshot_timestamp_rejected(worker_timestamp):
    frames = list(copy.deepcopy(private_frames()))
    frames[1]["worker_timestamp"] = worker_timestamp
    frames[2]["worker_timestamp"] = worker_timestamp
    with pytest.raises(ValueError):
        PrivateReadPreflight._validate_private_frames(tuple(frames), NOW)


async def public_barrier(tmp_path: Path, *, lifecycle_clear=lambda: True):
    clock = Clock()
    transport = PublicTransport(clock)
    store = PrivateReadStore(tmp_path / "preflight.sqlite3")
    controller = PrivateReadPreflight(
        store, clock=clock, public_get=transport,
        lifecycle_clear=lifecycle_clear,
    )
    barrier = await controller.run_public_barrier()
    return controller, store, clock, transport, barrier


@pytest.mark.asyncio
async def test_two_exact_public_sweeps_then_one_private_proof(tmp_path):
    controller, store, clock, transport, barrier = await public_barrier(tmp_path)
    assert transport.calls == expected_public_calls() * 2
    assert barrier is not None and store.outcome() is None

    calls = {"loader": 0, "nonce": 0, "sign": 0, "socket": 0}
    credential = SyntheticCredential(SIGNER, b"synthetic-private-material")

    def loader():
        calls["loader"] += 1
        assert store.outcome() == Outcome.CLAIMED
        return credential

    async def nonce_get(path, query):
        calls["nonce"] += 1
        return HttpResponse(
            200, expected_url(path, query), envelope({"nonce": "0123"}),
            clock(), False,
        )

    def sign(value, typed_data):
        calls["sign"] += 1
        assert value is credential
        assert typed_data["primaryType"] == "RegisterV2"
        assert typed_data["types"]["RegisterV2"] == [
            {"name": "signer", "type": "address"},
            {"name": "message", "type": "string"},
            {"name": "nonce", "type": "uint256"},
        ]
        assert typed_data["message"] == {
            "signer": SIGNER, "message": "sign in with RISEx", "nonce": "0x0123",
        }
        return SIGNATURE

    async def socket(url, outbound_plan):
        calls["socket"] += 1
        assert url == WS_ORIGIN
        assert outbound_plan == (
            {"method": "auth_v2", "params": {
                "account": ACCOUNT, "signer": SIGNER,
                "message": "sign in with RISEx", "nonce": "0x0123",
                "expiration": int(NOW) + 365 * 24 * 60 * 60,
                "signature": SIGNATURE,
            }},
            {"method": "subscribe", "params": {"channel": "orders"}},
            {"method": "subscribe", "params": {"channel": "positions"}},
        )
        return private_frames()

    result = await controller.run_private_proof(
        barrier, signer_loader=loader, nonce_get=nonce_get,
        sign_register_v2=sign, private_exchange=socket,
    )
    assert result.outcome == Outcome.PASSED
    assert calls == {"loader": 1, "nonce": 1, "sign": 1, "socket": 1}
    assert credential.closed and store.outcome() == Outcome.PASSED
    assert result.evidence == {
        "public_get_count": 18, "private_nonce_count": 1,
        "signature_count": 1, "auth_send_count": 1,
        "orders_snapshot_count": 1, "positions_snapshot_count": 1,
        "public_flat": True, "private_flat": True,
    }
    store.close()


@pytest.mark.asyncio
async def test_public_timeout_has_no_retry_and_no_secret_boundary(tmp_path):
    clock = Clock()
    transport = PublicTransport(clock)
    transport.mutations["/v1/orderbook"] = lambda _: (_ for _ in ()).throw(TimeoutError())
    store = PrivateReadStore(tmp_path / "timeout.sqlite3")
    controller = PrivateReadPreflight(store, clock=clock, public_get=transport,
                                      lifecycle_clear=lambda: True)
    assert await controller.run_public_barrier() is None
    assert len(transport.calls) == 6
    assert store.outcome() == Outcome.BLOCKED
    assert store.evidence() == {"public_get_count": 6}
    assert all(transport.calls.count(call) == 1 for call in transport.calls)
    store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["redirect", "url", "schema"])
async def test_redirect_final_url_and_envelope_schema_fail_closed(tmp_path, kind):
    clock = Clock()
    transport = PublicTransport(clock)

    def mutate(response):
        if kind == "redirect":
            return replace(response, redirected=True)
        if kind == "url":
            return replace(response, final_url="https://example.invalid/v1/system/config")
        return replace(response, body={"data": response.body["data"], "extra": True})

    transport.mutations["/v1/system/config"] = mutate
    store = PrivateReadStore(tmp_path / f"{kind}.sqlite3")
    controller = PrivateReadPreflight(store, clock=clock, public_get=transport,
                                      lifecycle_clear=lambda: True)
    assert await controller.run_public_barrier() is None
    assert len(transport.calls) == 1 and store.outcome() == Outcome.BLOCKED
    store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("case", [
    "config", "domain", "inactive", "duplicate_signer", "market", "sweep_conflict",
])
async def test_public_identity_and_cross_sweep_contradictions_block(tmp_path, case):
    clock = Clock()
    transport = PublicTransport(clock)

    def mutate(response):
        body = copy.deepcopy(response.body)
        if case == "config":
            body["data"]["addresses"]["router"] = AUTHORIZATION
        elif case == "domain":
            body["data"]["name"] = "legacy"
        elif case == "inactive":
            body["data"] = {"status": 0, "status_description": "Inactive"}
        elif case == "duplicate_signer":
            body["data"]["signers"] *= 2
        elif case == "market":
            body["data"]["markets"][0]["active"] = False
        else:
            body["data"]["asks"][0]["price"] = "77963.5"
        return replace(response, body=body)

    paths = {
        "config": "/v1/system/config",
        "domain": "/v1/auth/eip712-domain",
        "inactive": "/v1/auth/session-key-status",
        "duplicate_signer": "/v1/auth/signers",
        "market": "/v1/markets",
        "sweep_conflict": "/v1/orderbook",
    }
    key = (1, paths[case]) if case == "sweep_conflict" else paths[case]
    transport.mutations[key] = mutate
    store = PrivateReadStore(tmp_path / f"identity-{case}.sqlite3")
    controller = PrivateReadPreflight(store, clock=clock, public_get=transport,
                                      lifecycle_clear=lambda: True)
    assert await controller.run_public_barrier() is None
    assert store.outcome() == Outcome.BLOCKED
    store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["tick", "depth", "bound", "cap"])
async def test_grid_depth_bound_and_notional_cap_fail_closed(tmp_path, case):
    clock = Clock()
    transport = PublicTransport(clock)

    def mutate(response):
        body = copy.deepcopy(response.body)
        if case == "tick":
            body["data"]["asks"][0]["price"] = "77963.45"
        elif case == "depth":
            body["data"]["asks"][0]["quantity"] = "0.000099"
        elif case == "bound":
            body["data"]["asks"] = [{"price": "90000.0", "quantity": "1"}]
        else:
            body["data"]["asks"] = [{"price": "6000000.0", "quantity": "1"}]
            body["data"]["bids"] = [{"price": "5999999.9", "quantity": "1"}]
        return replace(response, body=body)

    transport.mutations["/v1/orderbook"] = mutate
    store = PrivateReadStore(tmp_path / f"market-{case}.sqlite3")
    controller = PrivateReadPreflight(store, clock=clock, public_get=transport,
                                      lifecycle_clear=lambda: True)
    assert await controller.run_public_barrier() is None
    assert store.outcome() == Outcome.BLOCKED
    store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["open", "position", "positions", "lifecycle"])
async def test_public_flatness_and_lifecycle_evidence_fail_closed(tmp_path, case):
    clock = Clock()
    transport = PublicTransport(clock)
    if case != "lifecycle":
        path = {"open": "/v1/orders/open", "position": "/v1/account/position",
                "positions": "/v1/positions"}[case]

        def mutate(response):
            body = copy.deepcopy(response.body)
            if case == "open":
                body["data"]["orders"] = [{"order_id": "fixture-order"}]
            elif case == "position":
                body["data"]["position"]["size"] = "0.0001"
            else:
                body["data"]["positions"] = [{"size": "0.0001"}]
            return replace(response, body=body)

        transport.mutations[path] = mutate
    store = PrivateReadStore(tmp_path / f"flat-{case}.sqlite3")
    controller = PrivateReadPreflight(
        store, clock=clock, public_get=transport,
        lifecycle_clear=(lambda: False) if case == "lifecycle" else (lambda: True),
    )
    assert await controller.run_public_barrier() is None
    assert store.outcome() == Outcome.BLOCKED
    store.close()


@pytest.mark.asyncio
async def test_stale_public_evidence_blocks_at_five_second_barrier(tmp_path):
    clock = Clock()
    transport = PublicTransport(clock)
    transport.mutations[(0, "/v1/system/config")] = (
        lambda response: replace(response, observed_at=NOW - 6)
    )
    store = PrivateReadStore(tmp_path / "stale.sqlite3")
    controller = PrivateReadPreflight(store, clock=clock, public_get=transport,
                                      lifecycle_clear=lambda: True)
    assert await controller.run_public_barrier() is None
    assert store.outcome() == Outcome.BLOCKED
    store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("now", [1_787_503_220.0, 1_790_092_010.0])
async def test_signer_not_yet_registered_or_expired_blocks(tmp_path, now):
    clock = Clock(now)
    transport = PublicTransport(clock)
    store = PrivateReadStore(tmp_path / "temporal.sqlite3")
    controller = PrivateReadPreflight(
        store, clock=clock, public_get=transport, lifecycle_clear=lambda: True,
    )
    assert await controller.run_public_barrier() is None
    assert store.outcome() == Outcome.BLOCKED
    store.close()


@pytest.mark.asyncio
async def test_earliest_public_evidence_must_still_be_fresh_after_sweep_b(tmp_path):
    clock = Clock()
    transport = PublicTransport(clock)

    def advance(response):
        clock.value += 0.4
        return response

    for path, _query in expected_public_calls():
        transport.mutations[path] = advance
    store = PrivateReadStore(tmp_path / "aggregate-stale.sqlite3")
    controller = PrivateReadPreflight(
        store, clock=clock, public_get=transport, lifecycle_clear=lambda: True,
    )
    assert await controller.run_public_barrier() is None
    assert len(transport.calls) == 18 and store.outcome() == Outcome.BLOCKED
    store.close()


@pytest.mark.asyncio
async def test_address_identity_is_exact_after_case_normalization(tmp_path):
    clock = Clock()
    transport = PublicTransport(clock)

    def uppercase_addresses(response):
        body = copy.deepcopy(response.body)
        data = body["data"]
        if "addresses" in data:
            data["addresses"] = {
                key: "0x" + value[2:].upper()
                for key, value in data["addresses"].items()
            }
        if "verifying_contract" in data:
            data["verifying_contract"] = "0x" + data["verifying_contract"][2:].upper()
        if "signers" in data:
            data["signers"][0]["signer"] = "0x" + SIGNER[2:].upper()
        return replace(response, body=body)

    for path in ("/v1/system/config", "/v1/auth/eip712-domain", "/v1/auth/signers"):
        transport.mutations[path] = uppercase_addresses
    store = PrivateReadStore(tmp_path / "case.sqlite3")
    controller = PrivateReadPreflight(
        store, clock=clock, public_get=transport, lifecycle_clear=lambda: True,
    )
    assert await controller.run_public_barrier() is not None
    assert len(transport.calls) == 18 and store.outcome() is None
    store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["loader", "nonce", "signature"])
async def test_private_loader_nonce_and_signature_fail_once(tmp_path, stage):
    controller, store, clock, _transport, barrier = await public_barrier(tmp_path)
    counts = {"loader": 0, "nonce": 0, "signature": 0, "socket": 0}
    credential = SyntheticCredential(SIGNER, b"private-fixture")

    def loader():
        counts["loader"] += 1
        if stage == "loader":
            raise ValueError("synthetic secret failure")
        return credential

    async def nonce_get(path, query):
        counts["nonce"] += 1
        if stage == "nonce":
            raise TimeoutError()
        return HttpResponse(200, expected_url(path, query), envelope({"nonce": "0x1"}),
                            clock(), False)

    def sign(_credential, _typed_data):
        counts["signature"] += 1
        if stage == "signature":
            raise ValueError("synthetic sign failure")
        return SIGNATURE

    async def socket(_url, _frame):
        counts["socket"] += 1
        return private_frames()

    result = await controller.run_private_proof(
        barrier, signer_loader=loader, nonce_get=nonce_get,
        sign_register_v2=sign, private_exchange=socket,
    )
    assert result.outcome == Outcome.BLOCKED
    assert store.outcome() == Outcome.BLOCKED
    assert counts["loader"] == 1 and counts["socket"] == 0
    assert credential.closed if stage != "loader" else not credential.closed
    store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("case", [
    "auth_error", "legacy_auth", "partial", "duplicate", "out_of_order",
    "orders_nonempty", "position_nonzero", "socket_timeout",
])
async def test_private_frames_and_public_private_conflicts_block(tmp_path, case):
    controller, store, clock, _transport, barrier = await public_barrier(tmp_path)
    credential = SyntheticCredential(SIGNER, b"private-fixture")
    counts = {"socket": 0}

    async def nonce_get(path, query):
        return HttpResponse(200, expected_url(path, query), envelope({"nonce": "0x2"}),
                            clock(), False)

    async def socket(_url, _frame):
        counts["socket"] += 1
        if case == "socket_timeout":
            raise TimeoutError()
        frames = list(private_frames())
        if case == "auth_error":
            frames[0] = {"method": "auth_v2", "status": "error"}
        elif case == "legacy_auth":
            frames[0] = {"method": "auth", "status": "success"}
        elif case == "partial":
            frames.pop()
        elif case == "duplicate":
            frames.append(copy.deepcopy(frames[-1]))
        elif case == "out_of_order":
            frames[1], frames[2] = frames[2], frames[1]
        elif case == "orders_nonempty":
            frames = list(private_frames(orders=({"order_id": "fixture-order"},)))
        elif case == "position_nonzero":
            frames = list(private_frames(positions=({"size": "0.0001"},)))
        return tuple(frames)

    result = await controller.run_private_proof(
        barrier, signer_loader=lambda: credential,
        nonce_get=nonce_get, sign_register_v2=lambda *_: SIGNATURE,
        private_exchange=socket,
    )
    assert result.outcome == Outcome.BLOCKED
    assert counts["socket"] == 1 and credential.closed
    assert store.outcome() == Outcome.BLOCKED
    store.close()


@pytest.mark.asyncio
async def test_durable_claim_restart_never_rearms_secret_or_dispatch(tmp_path):
    controller, store, clock, transport, barrier = await public_barrier(tmp_path)
    credential = SyntheticCredential(SIGNER, b"private-fixture")

    async def crashing_nonce(_path, _query):
        raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        await controller.run_private_proof(
            barrier, signer_loader=lambda: credential,
            nonce_get=crashing_nonce, sign_register_v2=lambda *_: SIGNATURE,
            private_exchange=lambda *_: private_frames(),
        )
    assert store.outcome() == Outcome.CLAIMED and credential.closed
    path = store.path
    store.close()

    restarted_store = PrivateReadStore(path)
    restarted = PrivateReadPreflight(
        restarted_store, clock=clock, public_get=transport,
        lifecycle_clear=lambda: True,
    )
    assert await restarted.run_public_barrier() is None
    assert restarted_store.outcome() == Outcome.BLOCKED
    assert len(transport.calls) == 18
    restarted_store.close()


@pytest.mark.asyncio
async def test_successful_public_attempt_cannot_repeat_or_rearm_after_restart(tmp_path):
    controller, store, clock, transport, barrier = await public_barrier(tmp_path)
    assert barrier is not None and len(transport.calls) == 18
    assert await controller.run_public_barrier() is None
    assert store.outcome() == Outcome.BLOCKED and len(transport.calls) == 18
    path = store.path
    store.close()

    restarted_store = PrivateReadStore(path)
    restarted = PrivateReadPreflight(
        restarted_store, clock=clock, public_get=transport,
        lifecycle_clear=lambda: True,
    )
    assert await restarted.run_public_barrier() is None
    assert len(transport.calls) == 18
    restarted_store.close()


@pytest.mark.asyncio
async def test_stale_private_barrier_and_nonce_schema_never_reach_socket(tmp_path):
    controller, store, clock, _transport, barrier = await public_barrier(tmp_path)
    calls = {"loader": 0, "socket": 0}
    clock.value += 6

    def loader():
        calls["loader"] += 1
        return SyntheticCredential(SIGNER, b"fixture")

    result = await controller.run_private_proof(
        barrier, signer_loader=loader,
        nonce_get=lambda *_: None,
        sign_register_v2=lambda *_: SIGNATURE,
        private_exchange=lambda *_: calls.__setitem__("socket", 1),
    )
    assert result.outcome == Outcome.BLOCKED
    assert calls == {"loader": 0, "socket": 0}
    store.close()

    nonce_path = tmp_path / "nonce-case"
    nonce_path.mkdir()
    controller, store, clock, _transport, barrier = await public_barrier(nonce_path)
    credential = SyntheticCredential(SIGNER, b"fixture")

    async def malformed_nonce(path, query):
        return HttpResponse(
            200, expected_url(path, query),
            envelope({"nonce": "0x1", "extra": True}), clock(), False,
        )

    result = await controller.run_private_proof(
        barrier, signer_loader=lambda: credential, nonce_get=malformed_nonce,
        sign_register_v2=lambda *_: SIGNATURE,
        private_exchange=lambda *_: calls.__setitem__("socket", 1),
    )
    assert result.outcome == Outcome.BLOCKED and calls["socket"] == 0
    assert credential.closed
    store.close()


@pytest.mark.asyncio
async def test_private_stage_rechecks_lifecycle_and_completion_freshness(tmp_path):
    clock = Clock()
    clear = {"value": True}
    transport = PublicTransport(clock)
    store = PrivateReadStore(tmp_path / "changed-lifecycle.sqlite3")
    controller = PrivateReadPreflight(
        store, clock=clock, public_get=transport,
        lifecycle_clear=lambda: clear["value"],
    )
    barrier = await controller.run_public_barrier()
    clear["value"] = False
    loader_calls = 0

    def loader():
        nonlocal loader_calls
        loader_calls += 1
        return SyntheticCredential(SIGNER, b"fixture")

    result = await controller.run_private_proof(
        barrier, signer_loader=loader, nonce_get=lambda *_: None,
        sign_register_v2=lambda *_: SIGNATURE,
        private_exchange=lambda *_: private_frames(),
    )
    assert result.outcome == Outcome.BLOCKED and loader_calls == 0
    store.close()

    fresh_path = tmp_path / "stale-private"
    fresh_path.mkdir()
    controller, store, clock, _transport, barrier = await public_barrier(fresh_path)
    credential = SyntheticCredential(SIGNER, b"fixture")

    async def nonce_get(path, query):
        return HttpResponse(
            200, expected_url(path, query), envelope({"nonce": "0x1"}),
            clock(), False,
        )

    async def slow_socket(_url, _frame):
        clock.value += 6
        return private_frames()

    result = await controller.run_private_proof(
        barrier, signer_loader=lambda: credential, nonce_get=nonce_get,
        sign_register_v2=lambda *_: SIGNATURE, private_exchange=slow_socket,
    )
    assert result.outcome == Outcome.BLOCKED and credential.closed
    store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["negative_age", "lifecycle_exception"])
async def test_private_barrier_clock_or_lifecycle_failure_precedes_loader(tmp_path, case):
    clear = {"raise": False}

    def lifecycle_clear():
        if clear["raise"]:
            raise RuntimeError("synthetic lifecycle read failure")
        return True

    clock = Clock()
    transport = PublicTransport(clock)
    store = PrivateReadStore(tmp_path / "private-gate.sqlite3")
    controller = PrivateReadPreflight(
        store, clock=clock, public_get=transport, lifecycle_clear=lifecycle_clear,
    )
    barrier = await controller.run_public_barrier()
    if case == "negative_age":
        clock.value -= 1
    else:
        clear["raise"] = True
    loader_calls = 0

    def loader():
        nonlocal loader_calls
        loader_calls += 1
        return SyntheticCredential(SIGNER, b"fixture")

    result = await controller.run_private_proof(
        barrier, signer_loader=loader, nonce_get=lambda *_: None,
        sign_register_v2=lambda *_: SIGNATURE,
        private_exchange=lambda *_: private_frames(),
    )
    assert result.outcome == Outcome.BLOCKED and loader_calls == 0
    assert store.outcome() == Outcome.BLOCKED
    store.close()


def test_redaction_and_import_network_surface_isolation(tmp_path):
    store = PrivateReadStore(tmp_path / "redacted.sqlite3")
    store.block(public_get_count=7)
    rendered = repr(store.result()) + repr(store.evidence())
    for forbidden in (ACCOUNT, SIGNER, "nonce", "signature", "raw", "order_id"):
        assert forbidden not in rendered
    import risex_farmer.testnet_risex_private_read_preflight as module
    source = inspect.getsource(module)
    assert "aiohttp" not in source and "requests" not in source
    assert "POST" not in source and "legacy" not in source.lower()
    package = Path(module.__file__).with_name("__init__.py").read_text()
    cli = Path(module.__file__).with_name("cli.py").read_text()
    assert "testnet_risex_private_read_preflight" not in package + cli
    store.close()
