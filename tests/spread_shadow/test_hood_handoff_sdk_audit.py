from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from types import SimpleNamespace

import aiohttp
import pytest

from risex_spread_shadow.hood_handoff import (
    AccountSnapshot,
    ContractError,
    Direction,
    HandoffConfig,
    HistoryPage,
    LighterSdkClient,
    MarketMetadata,
    MutationReceipt,
    OrderPlan,
    Outcome,
    PlainAioHttp,
    StaticSecretProvider,
    run_handoff,
)


def _config(tmp_path, **overrides) -> HandoffConfig:
    values = {
        "market_id": 7,
        "direction": Direction.LONG,
        "quantity": Decimal("0.125"),
        "source_limit_price": Decimal("100.25"),
        "receiver_worst_price": Decimal("101.25"),
        "max_gross_notional": Decimal("200"),
        "source_fee_budget": Decimal("1"),
        "receiver_fee_budget": Decimal("1"),
        "freshness_seconds": 10,
        "request_timeout_seconds": 1,
        "order_timeout_seconds": 1,
        "reconcile_timeout_seconds": 1,
        "poll_interval_seconds": 0.01,
        "max_poll_count": 2,
        "source_order_lifetime_seconds": 300,
        "client_order_prefix": "hcr23-sdk-audit",
        "journal_path": str(tmp_path / "audit.jsonl"),
        "api_base_url": "https://mainnet.zklighter.elliot.ai",
        "chain_id": 304,
        "api_key_index": 4,
    }
    values.update(overrides)
    return HandoffConfig(**values)


def _plan(*, account_index: int = 11, client_order_index: int = 123) -> OrderPlan:
    return OrderPlan(
        account_index=account_index,
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
        client_order_index=client_order_index,
    )


class _NonceManager:
    async def async_next_nonce(self, api_key_index):
        return api_key_index, 41


class _Signer:
    ORDER_TYPE_LIMIT = 0
    ORDER_TYPE_MARKET = 1
    ORDER_TIME_IN_FORCE_POST_ONLY = 2
    ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 0
    SKIP_NONCE_OFF = 0

    def __init__(self):
        self.nonce_manager = _NonceManager()
        self.sign_calls = 0
        self.cancel_calls = 0

    async def sign_create_order(self, **kwargs):
        self.sign_calls += 1
        return (14, "signed-create-info", "0xcreate", None)

    async def sign_cancel_order(self, **kwargs):
        self.cancel_calls += 1
        return (15, "signed-cancel-info", "0xcancel", None)

    async def create_auth_token_with_expiry(self, **kwargs):
        return "fixture-token", None


class _Module:
    class Configuration:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class ApiClient:
        def __init__(self, configuration):
            self.configuration = configuration


class _Transport:
    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error
        self.calls = []

    async def post_form(self, path, *, form):
        self.calls.append((path, dict(form)))
        if self.error is not None:
            raise self.error
        return dict(self.response or {})


def _client(tmp_path, transport, *, signer=None, account_api=None, order_api=None, clock=None):
    signer = signer or _Signer()
    module = type("Module", (_Module,), {})
    if account_api is not None:
        module.AccountApi = account_api
    if order_api is not None:
        module.OrderApi = order_api
    client = LighterSdkClient(
        _config(tmp_path),
        source_account_index=11,
        receiver_account_index=22,
        secrets=StaticSecretProvider({11: "fixture-a", 22: "fixture-b"}),
        market_evidence={"source_fee_rate": "0.001"},
        signer_factory=lambda **kwargs: signer,
        http_factory=lambda *args, **kwargs: transport,
        clock=clock or (lambda: 1000.0),
    )
    client._lighter = lambda: module
    return client, signer


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method",
    ["submit", "cancel"],
)
async def test_send_transport_failure_is_raised_as_unknown_not_rejection(tmp_path, monkeypatch, method):
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    transport = _Transport(error=TimeoutError("wire timed out after dispatch"))
    client, signer = _client(tmp_path, transport)

    if method == "submit":
        with pytest.raises(TimeoutError):
            await client.submit_order(_plan())
        assert signer.sign_calls == 1
    else:
        with pytest.raises(TimeoutError):
            await client.cancel_order(11, 7, "99")
        assert signer.cancel_calls == 1
    assert len(transport.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [{"tx_hash": "0xserver"}, {"code": 200, "_http_status": 500}],
)
async def test_missing_or_conflicting_send_response_is_unknown(tmp_path, monkeypatch, response):
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client, _ = _client(tmp_path, _Transport(response=response))
    with pytest.raises(RuntimeError, match="malformed or undecidable"):
        await client.submit_order(_plan())


@pytest.mark.asyncio
async def test_explicit_send_rejection_and_success_remain_authoritative(tmp_path, monkeypatch):
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    rejected, _ = _client(tmp_path, _Transport(response={"code": 400}))
    receipt = await rejected.submit_order(_plan())
    assert isinstance(receipt, MutationReceipt)
    assert receipt.accepted is False
    assert receipt.response_code == 400

    accepted, _ = _client(tmp_path, _Transport(response={"code": 200, "tx_hash": "0xserver"}))
    receipt = await accepted.submit_order(_plan())
    assert receipt.accepted is True
    assert receipt.response_code == 200


@pytest.mark.asyncio
async def test_sdk_transport_timeout_reaches_handoff_unknown_barrier(tmp_path, monkeypatch):
    """The real adapter's uncertain source send must block receiver dispatch."""

    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    transport = _Transport(error=TimeoutError("wire timed out after dispatch"))
    config = _config(
        tmp_path,
        journal_path=str(tmp_path / "cycle.jsonl"),
        operator_execution_opt_in=True,
        operator_plan_reviewed=True,
    )

    class CycleAdapter(LighterSdkClient):
        def __init__(self):
            super().__init__(
                config,
                source_account_index=11,
                receiver_account_index=22,
                secrets=StaticSecretProvider({11: "fixture-a", 22: "fixture-b"}),
                market_evidence={},
                signer_factory=lambda **kwargs: _Signer(),
                http_factory=lambda *args, **kwargs: transport,
                clock=lambda: 1000.0,
            )
            self.submission_plans = []

        async def market_metadata(self, market_id):
            return MarketMetadata(
                market_id=7,
                symbol="HOOD",
                status="active",
                price_decimals=2,
                size_decimals=3,
                minimum_base_amount=Decimal("0.001"),
                minimum_quote_amount=Decimal("1"),
                source_fee_rate=None,
                receiver_fee_rate=None,
                observed_at=1000.0,
                margin_evidence="fixture",
            )

        async def account_snapshot(self, account_index, market_id):
            return AccountSnapshot.from_mapping(
                {
                    "account_index": account_index,
                    "market_id": market_id,
                    "signed_position": "1" if account_index == 11 else "0",
                    "active_orders": [],
                    "observed_at": 1000.0,
                    "authorized": True,
                    "ready": True,
                    "margin_available": "1000",
                    "margin_required": "1",
                    "incremental_margin_required": "1",
                    "incremental_margin_evidence": "fixture",
                    "source_identity": f"fixture-{account_index}",
                }
            )

        async def lookup_order(self, *args, **kwargs):
            return None

        async def list_trades(self, *args, **kwargs):
            return HistoryPage()

        async def submit_order(self, plan):
            self.submission_plans.append(plan)
            return await super().submit_order(plan)

    class Clock:
        def now(self):
            return 1000.0

        async def sleep(self, seconds):
            return None

    client = CycleAdapter()
    client._lighter = lambda: _Module
    result = await run_handoff(config, client, clock=Clock())
    assert result.outcome is Outcome.UNKNOWN
    assert len(client.submission_plans) == 1
    assert client.submission_plans[0].account_index == 11
    assert len(transport.calls) == 1
    assert "source dispatch outcome unknown" in " ".join(result.unknown_reasons)


class _FakeResponse:
    def __init__(self, payload, *, status=200):
        self.status = status
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def text(self):
        return json.dumps(self._payload)


class _CountingSession:
    instances = []

    def __init__(self, *, timeout):
        self.timeout = timeout
        self.closed = False
        self.calls = []
        self.close_calls = 0
        type(self).instances.append(self)

    def get(self, url, **kwargs):
        self.calls.append(("GET", url, kwargs))
        return _FakeResponse({"code": 200, "items": []})

    def post(self, url, **kwargs):
        self.calls.append(("POST", url, kwargs))
        return _FakeResponse({"code": 200, "tx_hash": "0xserver"})

    async def close(self):
        self.close_calls += 1
        self.closed = True


@pytest.mark.asyncio
async def test_plain_http_reuses_one_session_and_keeps_request_local_auth(monkeypatch):
    _CountingSession.instances.clear()
    monkeypatch.setattr(aiohttp, "ClientSession", _CountingSession)
    transport = PlainAioHttp("https://example.invalid/", timeout_seconds=2)

    first = await transport.get("api/v1/accountOrders", params={"account_index": 11}, authorization="token-a")
    second = await transport.get("api/v1/accountOrders", params={"account_index": 22}, authorization="token-b")
    mutation = await transport.post_form("api/v1/sendTx", form={"tx_type": 14, "tx_info": "signed"})
    assert first["code"] == second["code"] == mutation["code"] == 200
    assert len(_CountingSession.instances) == 1
    session = _CountingSession.instances[0]
    assert len(session.calls) == 3
    assert session.calls[0][2]["headers"]["Authorization"] == "token-a"
    assert session.calls[1][2]["headers"]["Authorization"] == "token-b"
    assert session.calls[2][2]["allow_redirects"] is False
    assert session.calls[2][2]["headers"] == {"Content-Type": "application/x-www-form-urlencoded"}

    await transport.aclose()
    await transport.aclose()
    assert session.close_calls == 1
    assert session.closed is True


@pytest.mark.asyncio
async def test_plain_http_close_survives_cancellation(monkeypatch):
    started = asyncio.Event()
    release = asyncio.Event()

    class SlowCloseSession(_CountingSession):
        async def close(self):
            self.close_calls += 1
            started.set()
            await release.wait()
            self.closed = True

    _CountingSession.instances.clear()
    monkeypatch.setattr(aiohttp, "ClientSession", SlowCloseSession)
    transport = PlainAioHttp("https://example.invalid", timeout_seconds=2)
    await transport.get("api/v1/accountOrders", params={}, authorization="token")
    task = asyncio.create_task(transport.aclose())
    await started.wait()
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    session = _CountingSession.instances[0]
    assert session.closed is True
    assert session.close_calls == 1
    await transport.aclose()
    assert session.close_calls == 1


class _AccountApi:
    duplicate = False

    def __init__(self, api_client):
        self.api_client = api_client

    async def account(self, **kwargs):
        positions = [{"market_id": 7, "position": "0.2", "sign": 1}]
        if self.duplicate:
            positions.append({"market_id": 7, "position": "0.3", "sign": 1})
        return {
            "code": 200,
            "accounts": [
                {
                    "index": 11,
                    "l1_address": "0xsource",
                    "status": 1,
                    "available_balance": "10",
                    "cross_initial_margin_requirement": "1",
                    "positions": positions,
                }
            ],
        }


class _OrderApi:
    def __init__(self, api_client):
        self.api_client = api_client
        self.clock = None

    async def account_active_orders(self, **kwargs):
        if self.clock is not None:
            self.clock.value = 1003.0
        return {"code": 200, "orders": []}


@pytest.mark.asyncio
async def test_account_snapshot_keeps_account_observation_age_before_active_orders(monkeypatch, tmp_path):
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    clock = SimpleNamespace(value=1000.0)

    class OrderApi(_OrderApi):
        def __init__(self, api_client):
            super().__init__(api_client)
            self.clock = clock

    client, _ = _client(
        tmp_path,
        _Transport(response={"code": 200}),
        account_api=_AccountApi,
        order_api=OrderApi,
        clock=lambda: clock.value,
    )
    snapshot = await client.account_snapshot(11, 7)
    assert snapshot.signed_position == Decimal("0.2")
    assert snapshot.observed_at == 1000.0
    assert clock.value == 1003.0


@pytest.mark.asyncio
async def test_account_snapshot_rejects_duplicate_selected_market_positions(monkeypatch, tmp_path):
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))

    class DuplicateAccountApi(_AccountApi):
        duplicate = True

    client, _ = _client(
        tmp_path,
        _Transport(response={"code": 200}),
        account_api=DuplicateAccountApi,
        order_api=_OrderApi,
    )
    with pytest.raises(ContractError, match="duplicate selected-market positions"):
        await client.account_snapshot(11, 7)


@pytest.mark.asyncio
async def test_account_snapshot_rejects_conflicting_position_identity(monkeypatch, tmp_path):
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))

    class ConflictingAccountApi(_AccountApi):
        async def account(self, **kwargs):
            payload = await super().account(**kwargs)
            payload["accounts"][0]["positions"][0]["market_index"] = 8
            return payload

    client, _ = _client(
        tmp_path,
        _Transport(response={"code": 200}),
        account_api=ConflictingAccountApi,
        order_api=_OrderApi,
    )
    with pytest.raises(ContractError, match="conflicting market identity"):
        await client.account_snapshot(11, 7)
