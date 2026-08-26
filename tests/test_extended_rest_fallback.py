import asyncio
import copy
import json
from decimal import Decimal
from pathlib import Path

import pytest

from risex_farmer.extended_private_read_operational import (
    _OperationalStore,
    _run_fixture_operational_private_read,
)
from risex_farmer.extended_private_read_preflight import (
    ACCOUNT_ID,
    ACCOUNT_INDEX,
    L2_KEY,
    L2_VAULT,
    REST_BASE_URL,
    STREAM_URL,
    PreflightStore,
    run_rest_fallback,
)
from risex_farmer.extended_testnet_lifecycle import (
    EvidenceViolation,
    ExtendedLifecycle,
    IntentStore,
    LifecycleHalted,
    normalize_official_evidence,
    normalize_rest_fallback_evidence,
)


FIXTURE = Path(__file__).parent / "fixtures/extended_testnet_001/official_lifecycle.json"
API_KEY = "synthetic-api-key-never-persisted"
_MISSING = object()


def _meta(url):
    return {
        "actual_url": url,
        "method": "GET",
        "header_names": (
            ["User-Agent", "X-Api-Key"]
            if url == STREAM_URL
            else ["Accept", "Content-Type", "User-Agent", "X-Api-Key"]
        ),
        "direct_tls": True,
        "trust_env": False,
        "proxy": None,
        "redirects": 0,
        "retries": 0,
        "fallbacks": 0,
        "api_key_header_count": 1,
        "authorization_present": False,
        "credential_in_query": False,
        "credential_in_body": False,
        "application_frames_sent": False,
    }


def _wrapped(rows, *, cursor=None, count=None):
    return {
        "status": "OK",
        "data": copy.deepcopy(rows),
        "error": None,
        "pagination": {
            "cursor": cursor,
            "count": len(rows) if count is None else count,
        },
    }


def _fallback_wire(state="flat"):
    wire = json.loads(FIXTURE.read_text())
    wire.pop("stream")
    wire["fill"]["externalId"] = wire["entry"]["externalId"]
    wire["closeFill"]["externalId"] = wire["close"]["externalId"]
    if state == "filled":
        wire["account"].update(
            openOrders=[],
            positions=[copy.deepcopy(wire["position"])],
            orderStatus=copy.deepcopy(wire["filledOrder"]),
            orderHistory=[copy.deepcopy(wire["filledOrder"])],
            fills=[copy.deepcopy(wire["fill"])],
        )
    elif state == "closed":
        wire["account"].update(
            openOrders=[],
            positions=[],
            orderStatus=copy.deepcopy(wire["filledCloseOrder"]),
            orderHistory=[
                copy.deepcopy(wire["filledOrder"]),
                copy.deepcopy(wire["filledCloseOrder"]),
            ],
            fills=[copy.deepcopy(wire["fill"]), copy.deepcopy(wire["closeFill"])],
        )
    account = wire["account"]
    if state == "filled":
        observed_times = (1_770_000_003_100, 1_770_000_003_300)
    elif state == "closed":
        observed_times = (
            wire["close"]["expiryEpochMillis"] - 100,
            wire["close"]["expiryEpochMillis"] + 1,
        )
    else:
        observed_times = (1_770_000_000_100, 1_770_000_000_300)
    rounds = []
    for observed_at in observed_times:
        rounds.append(
            {
                "observedAt": observed_at,
                "serverTime": observed_at,
                "account": {
                    "info": copy.deepcopy(account["info"]),
                    "balance": copy.deepcopy(account["balance"]),
                    "fees": copy.deepcopy(account["fees"]),
                    "leverage": copy.deepcopy(account["leverage"]),
                },
                "openOrders": copy.deepcopy(account["openOrders"]),
                "positions": copy.deepcopy(account["positions"]),
                "orderStatus": copy.deepcopy(account.get("orderStatus")),
                "orderHistory": copy.deepcopy(account.get("orderHistory", [])),
                "trades": copy.deepcopy(account.get("fills", [])),
                "pagination": {
                    "openOrders": {
                        "cursor": None,
                        "count": len(account["openOrders"]),
                    },
                    "positions": {
                        "cursor": None,
                        "count": len(account["positions"]),
                    },
                    "orderHistory": {
                        "cursor": None,
                        "count": len(account.get("orderHistory", [])),
                    },
                    "trades": {
                        "cursor": None,
                        "count": len(account.get("fills", [])),
                    },
                },
            }
        )
    wire["restRounds"] = rounds
    return wire


class _Loader:
    def __init__(self):
        self.calls = 0

    def __call__(self):
        self.calls += 1
        return API_KEY


class _Capability:
    def x_api_key_header_value(self):
        return API_KEY

    def matches_account(self, account):
        return (
            account["id"] == ACCOUNT_ID
            and account["accountIndex"] == ACCOUNT_INDEX
            and account["l2Key"] == L2_KEY
            and account["l2Vault"] == L2_VAULT
        )

    def matches_spot_account_id(self, account_id):
        return account_id == ACCOUNT_ID

    def close(self):
        return None


class _Source:
    def open(self):
        return _Capability()


class _Clock:
    def __init__(self):
        self.calls = 0

    def __call__(self):
        self.calls += 1
        return 1_770_000_000_100 if self.calls <= 3 else 1_770_000_000_300


class _Transport:
    def __init__(self, mutation=None):
        self.mutation = mutation
        self.get_calls = []
        self.open_calls = []

    async def get(self, request):
        self.get_calls.append(request)
        if request.path == "/user/account/info":
            body = {
                "status": "OK",
                "data": {
                    "accountId": ACCOUNT_ID,
                    "accountIndex": ACCOUNT_INDEX,
                    "accountIndexForKeyGeneration": ACCOUNT_INDEX,
                    "bridgeStarknetAddress": "0xabc123",
                    "description": "synthetic",
                    "l2Key": L2_KEY,
                    "l2Vault": str(L2_VAULT),
                    "status": "ACTIVE",
                },
            }
        else:
            body = _wrapped([])
        if self.mutation is not None:
            body = self.mutation(request, body)
        observed = 1_770_000_000_100 if request.round_name == "A" else 1_770_000_000_300
        return {
            "status": 200,
            "final_url": request.url,
            "observed_at_ms": observed,
            "body": body,
            "transport": _meta(request.url),
        }

    async def open_stream(self, request):
        self.open_calls.append(request)
        raise AssertionError("REST fallback must not open a WebSocket")


class _Stream:
    def __init__(self):
        self.upgrade_metadata = _meta(STREAM_URL)
        self.outbound_frames = []
        self.reconnect_count = 0
        self.closed = False
        self.barrier_complete = False
        self._done = asyncio.Event()

    async def recv(self):
        await self._done.wait()
        raise StopAsyncIteration

    async def final_barrier(self):
        self.barrier_complete = True
        self._done.set()
        return {
            "connected": True,
            "same_connection": True,
            "outbound_frames": [],
            "reconnect_count": 0,
            "frames": [],
            "transport": _meta(STREAM_URL),
            "observed_at_ms": 1_770_000_000_300,
        }

    async def close(self):
        self.closed = True


class _StreamCompatibleTransport(_Transport):
    def __init__(self):
        super().__init__()
        self.stream = _Stream()

    async def open_stream(self, request):
        self.open_calls.append(request)
        return self.stream


@pytest.mark.asyncio
async def test_rest_fallback_uses_two_sequential_fresh_rounds_without_stream(tmp_path):
    transport = _Transport()
    loader = _Loader()
    result = await run_rest_fallback(
        store=PreflightStore(tmp_path / "preflight.sqlite"),
        credential_loader=loader,
        transport=transport,
        clock_ms=_Clock(),
    )

    assert (result.status, result.reason) == (
        "READY_REST_FALLBACK",
        "REST_FALLBACK_CONTRACT_PROVED",
    )
    assert [request.round_name for request in transport.get_calls] == ["A"] * 3 + ["B"] * 3
    assert transport.open_calls == []
    assert result.rest_calls == 6
    assert result.clock_calls == 6
    replay_transport = _Transport()
    replay_loader = _Loader()
    replay = await run_rest_fallback(
        store=PreflightStore(tmp_path / "preflight.sqlite"),
        credential_loader=replay_loader,
        transport=replay_transport,
        clock_ms=_Clock(),
    )
    assert replay == result
    assert replay_loader.calls == 0
    assert replay_transport.get_calls == [] and replay_transport.open_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pagination_member", [_MISSING, None], ids=["absent", "null"]
)
async def test_rest_fallback_accepts_absent_or_null_pagination_for_exact_empty_lists(
    tmp_path, pagination_member,
):
    def mutate(request, body):
        if request.path == "/user/account/info":
            return body
        body.pop("error")
        if pagination_member is _MISSING:
            body.pop("pagination")
        else:
            body["pagination"] = pagination_member
        return body

    transport = _Transport(mutation=mutate)
    result = await run_rest_fallback(
        store=PreflightStore(tmp_path / "preflight.sqlite"),
        credential_loader=_Loader(),
        transport=transport,
        clock_ms=_Clock(),
    )

    assert (result.status, result.reason) == (
        "READY_REST_FALLBACK",
        "REST_FALLBACK_CONTRACT_PROVED",
    )
    assert result.rest_calls == 6 and result.clock_calls == 6
    assert transport.open_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pagination_member", [_MISSING, None], ids=["absent", "null"]
)
async def test_rest_fallback_rejects_nonempty_lists_without_complete_pagination(
    tmp_path, pagination_member,
):
    def mutate(request, body):
        if request.round_name == "B" and request.path == "/user/orders":
            body["data"] = [{"id": 99}]
            if pagination_member is _MISSING:
                body.pop("pagination")
            else:
                body["pagination"] = pagination_member
        return body

    result = await run_rest_fallback(
        store=PreflightStore(tmp_path / "preflight.sqlite"),
        credential_loader=_Loader(),
        transport=_Transport(mutation=mutate),
        clock_ms=_Clock(),
    )

    assert (result.status, result.reason) == (
        "BLOCKED", "REST_PAGINATION_INCOMPLETE"
    )


@pytest.mark.asyncio
async def test_rest_fallback_rejects_unknown_wrapper_key_for_empty_unpaginated_list(
    tmp_path,
):
    def mutate(request, body):
        if request.round_name == "B" and request.path == "/user/orders":
            body.pop("error")
            body.pop("pagination")
            body["unknown"] = None
        return body

    result = await run_rest_fallback(
        store=PreflightStore(tmp_path / "preflight.sqlite"),
        credential_loader=_Loader(),
        transport=_Transport(mutation=mutate),
        clock_ms=_Clock(),
    )

    assert (result.status, result.reason) == (
        "BLOCKED", "REST_WRAPPER_MALFORMED"
    )


@pytest.mark.asyncio
async def test_operational_rest_fallback_preserves_durable_stream_history_shape(tmp_path):
    path = tmp_path / "operational.sqlite3"
    transport = _Transport()
    result = await _run_fixture_operational_private_read(
        store=_OperationalStore(path, "extended-read-fallback", mode="rest_fallback"),
        credential_source=_Source(),
        transport=transport,
        clock_ms=_Clock(),
    )
    assert (result.status, result.reason, result.phase) == (
        "READY",
        "REST_FALLBACK_CONTRACT_PROVED",
        "TERMINAL",
    )
    assert result.rest_calls == 6 and result.stream_frames == 0 and result.clock_calls == 6
    assert all(
        result.counters[name] == 0
        for name in (
            "stream_open_attempts", "stream_upgrade_attempts",
            "barrier_request_attempts", "barrier_validation_attempts",
            "stream_close_attempts",
        )
    )
    assert transport.open_calls == []
    replay = await _run_fixture_operational_private_read(
        store=_OperationalStore(path, "extended-read-fallback", mode="rest_fallback"),
        credential_source=_Source(),
        transport=_Transport(),
        clock_ms=_Clock(),
    )
    assert replay == result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pagination_member", [_MISSING, None], ids=["absent", "null"]
)
async def test_operational_fallback_persists_empty_wrapper_counters_and_forbids_replay(
    tmp_path, pagination_member,
):
    def mutate(request, body):
        if request.path == "/user/account/info":
            return body
        body.pop("error")
        if pagination_member is _MISSING:
            body.pop("pagination")
        else:
            body["pagination"] = pagination_member
        return body

    class _CountingSource(_Source):
        def __init__(self):
            self.calls = 0

        def open(self):
            self.calls += 1
            return super().open()

    path = tmp_path / "operational.sqlite3"
    source = _CountingSource()
    result = await _run_fixture_operational_private_read(
        store=_OperationalStore(path, "extended-read-fallback", mode="rest_fallback"),
        credential_source=source,
        transport=_Transport(mutation=mutate),
        clock_ms=_Clock(),
    )

    fallback_effects = (
        "loader",
        "rest_a_info", "rest_a_orders", "rest_a_positions",
        "rest_b_info", "rest_b_orders", "rest_b_positions",
        "terminal_persistence",
    )
    stream_effects = (
        "stream_open", "stream_upgrade", "barrier_request",
        "barrier_validation", "stream_close",
    )
    assert (result.status, result.reason, result.phase) == (
        "READY", "REST_FALLBACK_CONTRACT_PROVED", "TERMINAL"
    )
    assert result.rest_calls == 6 and result.stream_frames == 0
    assert result.clock_calls == 6
    assert all(
        result.counters[f"{effect}_{suffix}"] == 1
        for effect in fallback_effects
        for suffix in ("attempts", "completions")
    )
    assert all(
        result.counters[f"{effect}_{suffix}"] == 0
        for effect in stream_effects
        for suffix in ("attempts", "completions")
    )
    assert source.calls == 1

    replay_source = _CountingSource()
    replay_transport = _Transport()
    replay = await _run_fixture_operational_private_read(
        store=_OperationalStore(path, "extended-read-fallback", mode="rest_fallback"),
        credential_source=replay_source,
        transport=replay_transport,
        clock_ms=_Clock(),
    )
    assert replay == result
    assert replay_source.calls == 0
    assert replay_transport.get_calls == [] and replay_transport.open_calls == []


@pytest.mark.asyncio
async def test_fallback_mode_does_not_make_historical_stream_rows_unreadable(tmp_path):
    path = tmp_path / "shared-journal.sqlite3"

    historical = await _run_fixture_operational_private_read(
        store=_OperationalStore(path, "extended-read-history"),
        credential_source=_Source(),
        transport=_StreamCompatibleTransport(),
        clock_ms=_Clock(),
    )
    current = await _run_fixture_operational_private_read(
        store=_OperationalStore(path, "extended-read-fallback", mode="rest_fallback"),
        credential_source=_Source(),
        transport=_Transport(),
        clock_ms=_Clock(),
    )
    assert historical.reason == "OPERATIONAL_CONTRACT_PROVED"
    assert current.reason == "REST_FALLBACK_CONTRACT_PROVED"
    assert await _run_fixture_operational_private_read(
        store=_OperationalStore(path, "extended-read-history"),
        credential_source=_Source(),
        transport=_StreamCompatibleTransport(),
        clock_ms=_Clock(),
    ) == historical


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation,reason", [
    (
        lambda request, body: (
            body | {"pagination": {"cursor": "next", "count": 0}}
            if request.round_name == "B" and request.path == "/user/orders"
            else body
        ),
        "REST_PAGINATION_INCOMPLETE",
    ),
    (
        lambda request, body: (
            body | {"pagination": {"cursor": None, "count": 1}}
            if request.round_name == "B" and request.path == "/user/positions"
            else body
        ),
        "REST_PAGINATION_INVALID",
    ),
])
async def test_rest_fallback_requires_complete_bounded_pagination(tmp_path, mutation, reason):
    result = await run_rest_fallback(
        store=PreflightStore(tmp_path / "preflight.sqlite"),
        credential_loader=_Loader(),
        transport=_Transport(mutation=mutation),
        clock_ms=_Clock(),
    )
    assert (result.status, result.reason) == ("BLOCKED", reason)


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation,reason", [
    (
        lambda request, body: (
            body | {"data": {**body["data"], "accountId": ACCOUNT_ID + 1}}
            if request.round_name == "B" and request.path == "/user/account/info"
            else body
        ),
        "ACCOUNT_IDENTITY_MISMATCH",
    ),
    (
        lambda request, body: (
            body | {
                "data": [{"id": 99}],
                "pagination": {"cursor": None, "count": 1},
            }
            if request.round_name == "B" and request.path == "/user/orders"
            else body
        ),
        "REST_OPEN_ORDER_PRESENT",
    ),
])
async def test_rest_fallback_rejects_identity_or_unrelated_state(tmp_path, mutation, reason):
    result = await run_rest_fallback(
        store=PreflightStore(tmp_path / "preflight.sqlite"),
        credential_loader=_Loader(),
        transport=_Transport(mutation=mutation),
        clock_ms=_Clock(),
    )
    assert (result.status, result.reason) == ("BLOCKED", reason)


@pytest.mark.asyncio
async def test_rest_fallback_rejects_stale_second_round(tmp_path):
    def stale(request, body):
        return body

    transport = _Transport(mutation=stale)
    original_get = transport.get

    async def stale_get(request):
        reply = await original_get(request)
        if request.round_name == "B":
            reply["observed_at_ms"] = 1_770_000_000_300 - 30_001
        return reply

    transport.get = stale_get
    result = await run_rest_fallback(
        store=PreflightStore(tmp_path / "preflight.sqlite"),
        credential_loader=_Loader(),
        transport=transport,
        clock_ms=_Clock(),
    )
    assert (result.status, result.reason) == ("BLOCKED", "EVIDENCE_STALE")


@pytest.mark.asyncio
async def test_rest_fallback_classifies_transport_failure_without_retry(tmp_path):
    class _TransportFailure(_Transport):
        async def get(self, request):
            if request.round_name == "B" and request.path == "/user/orders":
                self.get_calls.append(request)
                raise ConnectionError("synthetic transport failure")
            return await super().get(request)

    transport = _TransportFailure()
    result = await run_rest_fallback(
        store=PreflightStore(tmp_path / "preflight.sqlite"),
        credential_loader=_Loader(),
        transport=transport,
        clock_ms=_Clock(),
    )
    assert (result.status, result.reason) == ("BLOCKED", "TRANSPORT")
    assert [request.round_name for request in transport.get_calls] == ["A"] * 3 + ["B"] * 2


@pytest.mark.asyncio
async def test_rest_fallback_rejects_agreeing_rounds_with_changed_state(tmp_path):
    def changed_state(request, body):
        if request.round_name == "B" and request.path == "/user/account/info":
            body["data"]["description"] = "changed"
        return body

    result = await run_rest_fallback(
        store=PreflightStore(tmp_path / "preflight.sqlite"),
        credential_loader=_Loader(),
        transport=_Transport(mutation=changed_state),
        clock_ms=_Clock(),
    )
    assert (result.status, result.reason) == ("BLOCKED", "REST_ROUNDS_DISAGREE")


@pytest.mark.asyncio
async def test_operational_fallback_process_death_is_unknown_and_not_replayed(tmp_path):
    class _ProcessDeath(BaseException):
        pass

    path = tmp_path / "operational.sqlite3"

    def hook(effect, point):
        if (effect, point) == ("rest_b_orders", "after_effect"):
            raise _ProcessDeath

    with pytest.raises(_ProcessDeath):
        await _run_fixture_operational_private_read(
            store=_OperationalStore(path, "extended-read-fallback", mode="rest_fallback"),
            credential_source=_Source(),
            transport=_Transport(),
            clock_ms=_Clock(),
            _effect_hook=hook,
        )
    transport = _Transport()
    recovered = await _run_fixture_operational_private_read(
        store=_OperationalStore(path, "extended-read-fallback", mode="rest_fallback"),
        credential_source=_Source(),
        transport=transport,
        clock_ms=_Clock(),
    )
    assert (recovered.status, recovered.reason) == ("UNKNOWN", "INTERRUPTED_RUNNING")
    assert recovered.counters["rest_b_orders_attempts"] == 1
    assert recovered.counters["rest_b_orders_completions"] == 0
    assert transport.get_calls == []


def test_rest_fallback_lifecycle_reconciles_exact_order_and_trade_identities(tmp_path):
    wire = _fallback_wire()
    initial = normalize_rest_fallback_evidence(copy.deepcopy(wire), now_ms=1_770_000_000_300)
    assert initial.mode == "REST_FALLBACK"
    assert initial.connected is False and initial.gap_free is False
    assert normalize_official_evidence(copy.deepcopy(wire), now_ms=1_770_000_000_300) == initial
    lifecycle = ExtendedLifecycle(
        store=IntentStore(tmp_path / "lifecycle.sqlite3"),
        credential_loader=lambda: pytest.fail("fallback lifecycle must not load credentials"),
    )
    entry = lifecycle.prepare_order(
        nonce=wire["entry"]["nonce"],
        settlement_hash=wire["entry"]["settlementHash"],
        market=wire["entry"]["market"],
        side=wire["entry"]["side"],
        qty=Decimal(wire["entry"]["qty"]),
        price=Decimal(wire["entry"]["price"]),
        expiry_ms=wire["entry"]["expiryEpochMillis"],
        reduce_only=False,
        evidence=initial,
    )
    lifecycle.claim_for_dispatch(entry.id, evidence=initial)
    filled = normalize_rest_fallback_evidence(_fallback_wire("filled"), now_ms=1_770_000_003_300)
    assert lifecycle.reconcile(entry.id, filled).lifecycle_state == "EXPOSED"
    close = lifecycle.prepare_close(
        entry_id=entry.id,
        evidence=filled,
        nonce=wire["close"]["nonce"],
        settlement_hash=wire["close"]["settlementHash"],
        price=Decimal(wire["close"]["price"]),
        expiry_ms=wire["close"]["expiryEpochMillis"],
    )
    lifecycle.claim_for_dispatch(close.id, evidence=filled)
    closed = normalize_rest_fallback_evidence(
        _fallback_wire("closed"), now_ms=close.expiry_ms + 1
    )
    result = lifecycle.reconcile(close.id, closed)
    assert result.complete is True
    assert result.reconciled_external_ids == frozenset({
        wire["entry"]["externalId"], wire["close"]["externalId"]
    })
    assert result.reconciled_fill_ids == frozenset({
        wire["fill"]["id"], wire["closeFill"]["id"]
    })


@pytest.mark.parametrize("mutation,reason", [
    (
        lambda round_data: round_data["orderStatus"].update(id=90002),
        "REST_ORDER_STATUS_HISTORY_DISAGREE",
    ),
    (
        lambda round_data: round_data["trades"][0].update(orderId=99999),
        "REST_TRADE_ORDER_MISMATCH",
    ),
    (
        lambda round_data: round_data["trades"][0].pop("externalId"),
        "WIRE_SCHEMA",
    ),
])
def test_rest_fallback_binds_returned_order_id_and_both_trade_ids(mutation, reason):
    wire = _fallback_wire("filled")
    for round_data in wire["restRounds"]:
        mutation(round_data)
    with pytest.raises((EvidenceViolation, LifecycleHalted), match=reason):
        normalize_rest_fallback_evidence(wire, now_ms=1_770_000_003_300)


def test_rest_fallback_rejects_stale_rounds_and_stream_mixing():
    wire = _fallback_wire("filled")
    for round_data in wire["restRounds"]:
        round_data["observedAt"] = 1_770_000_003_300 - 30_001
        round_data["serverTime"] = 1_770_000_003_300 - 30_001
    with pytest.raises(EvidenceViolation, match="STALE_EVIDENCE"):
        normalize_rest_fallback_evidence(wire, now_ms=1_770_000_003_300)
    mixed = _fallback_wire()
    mixed["stream"] = {"unexpected": True}
    with pytest.raises(EvidenceViolation, match="WIRE_SCHEMA"):
        normalize_rest_fallback_evidence(mixed, now_ms=1_770_000_000_300)


@pytest.mark.parametrize("mutation,reason", [
    (
        lambda wire: (
            wire["restRounds"][1]["orderHistory"].append({"unexpected": True})
            or wire
        ),
        "WIRE_SCHEMA",
    ),
    (
        lambda wire: (
            wire["restRounds"][1]["trades"][0].update(externalId="wrong")
            or wire
        ),
        "UNRELATED_TRADE",
    ),
    (
        lambda wire: (
            wire["restRounds"][1]["positions"].append(copy.deepcopy(wire["position"]))
            or wire["restRounds"][1]["pagination"]["positions"].update(count=2)
            or wire
        ),
        "REST_ROUNDS_DISAGREE",
    ),
])
def test_rest_fallback_lifecycle_rejects_unrelated_or_disagreeing_evidence(
    mutation, reason
):
    wire = mutation(_fallback_wire("filled"))
    with pytest.raises((EvidenceViolation, LifecycleHalted), match=reason):
        normalize_rest_fallback_evidence(wire, now_ms=1_770_000_003_300)
