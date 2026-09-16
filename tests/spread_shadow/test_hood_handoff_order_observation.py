from __future__ import annotations

import pytest

from risex_spread_shadow.hood_handoff import (
    ContractError,
    HistoryPage,
    LighterSdkClient,
    MutationReceipt,
    Outcome,
    StaticSecretProvider,
    run_handoff,
)

from test_hood_handoff_engine import FakeClient, FakeClock, make_config


def official_order(
    *,
    account_index: int = 11,
    market_id: int = 7,
    order_id: str = "145",
    client_order_index: int = 123,
    status: str = "open",
    is_ask: bool | None = True,
    side: str | None = "buy",
    filled: str = "0",
    remaining: str = "0.125",
) -> dict[str, object]:
    value: dict[str, object] = {
        "order_index": int(order_id),
        "order_id": order_id,
        "client_order_index": client_order_index,
        "market_index": market_id,
        "owner_account_index": account_index,
        "initial_base_amount": "0.125",
        "remaining_base_amount": remaining,
        "filled_base_amount": filled,
        "is_ask": is_ask,
        "price": "100.25",
        "type": "limit",
        "time_in_force": "post-only",
        "reduce_only": True,
        "status": status,
        "timestamp": 1000,
    }
    if side is not None:
        value["side"] = side
    return value


class AuthSigner:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.close_calls = 0

    async def create_auth_token_with_expiry(self, **kwargs):
        return "synthetic-auth-token", None

    async def close(self):
        self.close_calls += 1


class ObservationHttp:
    def __init__(self, response, *args, **kwargs):
        self.response = response
        self.calls: list[tuple[str, dict[str, object], str]] = []
        self.close_calls = 0

    async def get(self, path, *, params, authorization):
        self.calls.append((path, dict(params), authorization))
        return self.response

    async def aclose(self):
        self.close_calls += 1


class ObservationApiClient:
    def __init__(self, configuration):
        self.configuration = configuration
        self.close_calls = 0

    async def close(self):
        self.close_calls += 1


class ObservationModule:
    Configuration = type("Configuration", (), {"__init__": lambda self, **kwargs: setattr(self, "kwargs", kwargs)})
    ApiClient = ObservationApiClient


@pytest.mark.asyncio
async def test_order_observation_uses_authoritative_is_ask_for_active_and_terminal(monkeypatch, tmp_path):
    terminal = official_order(
        order_id="145",
        status="filled",
        is_ask=False,
        filled="0.125",
        remaining="0",
    )
    http = ObservationHttp({"code": 200, "orders": [terminal]})

    class OrderApi:
        def __init__(self, api_client):
            self.api_client = api_client

        async def account_active_orders(self, **kwargs):
            return {
                "code": 200,
                "orders": [official_order(status="open", is_ask=True)],
            }

    Module = type("Module", (ObservationModule,), {"OrderApi": OrderApi})

    signer = AuthSigner()
    client = LighterSdkClient(
        make_config(tmp_path / "observation.jsonl"),
        source_account_index=11,
        receiver_account_index=22,
        secrets=StaticSecretProvider({11: "synthetic-key", 22: "synthetic-key-2"}),
        market_evidence={},
        signer_factory=lambda **kwargs: signer,
        http_factory=lambda *args, **kwargs: http,
        clock=lambda: 1000.0,
    )
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client._lighter = lambda: Module

    active = await client._active_orders(11, 7)
    looked_up = await client.lookup_order(11, 7, client_order_index=123)

    assert active[0].side == "SELL"
    assert active[0].active
    assert looked_up is not None
    assert looked_up.side == "BUY"
    assert looked_up.terminal
    assert http.calls == [
        (
            "api/v1/accountOrders",
            {"account_index": 11, "client_order_indexes": "123"},
            "synthetic-auth-token",
        )
    ]
    await client.aclose()
    assert signer.close_calls == 1
    assert http.close_calls == 1


@pytest.mark.asyncio
async def test_order_observation_rejects_incomplete_or_foreign_identity(monkeypatch, tmp_path):
    class OrderApi:
        def __init__(self, api_client):
            self.api_client = api_client

        async def account_active_orders(self, **kwargs):
            return {"code": 200, "orders": [official_order(side=None, is_ask=None)]}

    Module = type("Module", (ObservationModule,), {"OrderApi": OrderApi})

    client = LighterSdkClient(
        make_config(tmp_path / "invalid.jsonl"),
        source_account_index=11,
        receiver_account_index=22,
        secrets=StaticSecretProvider({11: "synthetic-key", 22: "synthetic-key-2"}),
        market_evidence={},
        signer_factory=AuthSigner,
        http_factory=lambda *args, **kwargs: ObservationHttp(
            {"code": 200, "orders": [official_order(account_index=99)]}
        ),
        clock=lambda: 1000.0,
    )
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client._lighter = lambda: Module

    with pytest.raises(ContractError, match="is_ask must be bool"):
        await client._active_orders(11, 7)
    with pytest.raises(ContractError, match="accountOrders response identity"):
        await client.lookup_order(11, 7, client_order_index=123)
    await client.aclose()


@pytest.mark.asyncio
async def test_order_observation_rejects_missing_legacy_and_authoritative_side(monkeypatch, tmp_path):
    class OrderApi:
        def __init__(self, api_client):
            self.api_client = api_client

        async def account_active_orders(self, **kwargs):
            value = official_order(side=None)
            value.pop("is_ask")
            return {"code": 200, "orders": [value]}

    Module = type("Module", (ObservationModule,), {"OrderApi": OrderApi})
    client = LighterSdkClient(
        make_config(tmp_path / "missing-side.jsonl"),
        source_account_index=11,
        receiver_account_index=22,
        secrets=StaticSecretProvider({11: "synthetic-key", 22: "synthetic-key-2"}),
        market_evidence={},
        signer_factory=AuthSigner,
        clock=lambda: 1000.0,
    )
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client._lighter = lambda: Module

    with pytest.raises(ContractError, match="is_ask"):
        await client._active_orders(11, 7)
    await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["filled_base_amount", "reduce_only"])
async def test_order_observation_rejects_missing_official_required_fields(monkeypatch, tmp_path, missing):
    class OrderApi:
        def __init__(self, api_client):
            self.api_client = api_client

        async def account_active_orders(self, **kwargs):
            value = official_order()
            value.pop(missing)
            return {"code": 200, "orders": [value]}

    Module = type("Module", (ObservationModule,), {"OrderApi": OrderApi})
    client = LighterSdkClient(
        make_config(tmp_path / f"missing-{missing}.jsonl"),
        source_account_index=11,
        receiver_account_index=22,
        secrets=StaticSecretProvider({11: "synthetic-key", 22: "synthetic-key-2"}),
        market_evidence={},
        signer_factory=AuthSigner,
        clock=lambda: 1000.0,
    )
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client._lighter = lambda: Module

    with pytest.raises(ContractError, match=missing):
        await client._active_orders(11, 7)
    await client.aclose()


@pytest.mark.asyncio
async def test_trades_request_uses_documented_descending_sort(monkeypatch, tmp_path):
    seen: list[dict[str, object]] = []

    class OrderApi:
        def __init__(self, api_client):
            self.api_client = api_client

        async def trades(self, **kwargs):
            seen.append(kwargs)
            return {"code": 200, "trades": [], "next_cursor": None}

    Module = type("Module", (ObservationModule,), {"OrderApi": OrderApi})

    client = LighterSdkClient(
        make_config(tmp_path / "trades.jsonl"),
        source_account_index=11,
        receiver_account_index=22,
        secrets=StaticSecretProvider({11: "synthetic-key", 22: "synthetic-key-2"}),
        market_evidence={},
        signer_factory=AuthSigner,
        clock=lambda: 1000.0,
    )
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client._lighter = lambda: Module

    page = await client.list_trades(11, 7)

    assert isinstance(page, HistoryPage)
    assert seen and seen[0]["sort_dir"] == "desc"
    assert seen[0]["sort_by"] == "block_height"
    await client.aclose()


@pytest.mark.asyncio
async def test_execution_adapter_closes_generated_client_signers_and_http_once(monkeypatch, tmp_path):
    signers: dict[int, AuthSigner] = {}

    def signer_factory(**kwargs):
        signer = AuthSigner(**kwargs)
        signers[int(kwargs["account_index"])] = signer
        return signer

    http = ObservationHttp({"code": 200, "orders": []})
    client = LighterSdkClient(
        make_config(tmp_path / "close.jsonl"),
        source_account_index=11,
        receiver_account_index=22,
        secrets=StaticSecretProvider({11: "synthetic-key", 22: "synthetic-key-2"}),
        market_evidence={},
        signer_factory=signer_factory,
        http_factory=lambda *args, **kwargs: http,
    )
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client._lighter = lambda: ObservationModule

    api_client = client._generated_api_client(ObservationModule)
    client._signer(11)
    client._signer(22)
    await client.aclose()

    assert api_client.close_calls == 1
    assert signers[11].close_calls == 1
    assert signers[22].close_calls == 1
    assert http.close_calls == 1
    await client.close()
    assert api_client.close_calls == 1
    assert signers[11].close_calls == 1
    assert signers[22].close_calls == 1
    assert http.close_calls == 1


@pytest.mark.asyncio
async def test_execution_adapter_falls_back_to_failing_signer_nested_client(monkeypatch, tmp_path):
    class NestedClient:
        def __init__(self):
            self.close_calls = 0

        async def close(self):
            self.close_calls += 1

    class FailingSigner(AuthSigner):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.api_client = NestedClient()

        async def close(self):
            self.close_calls += 1
            raise RuntimeError("synthetic signer close failure")

    http = ObservationHttp({"code": 200, "orders": []})
    client = LighterSdkClient(
        make_config(tmp_path / "close-failure.jsonl"),
        source_account_index=11,
        receiver_account_index=22,
        secrets=StaticSecretProvider({11: "synthetic-key", 22: "synthetic-key-2"}),
        market_evidence={},
        signer_factory=FailingSigner,
        http_factory=lambda *args, **kwargs: http,
    )
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client._lighter = lambda: ObservationModule

    signer = client._signer(11)
    await client.aclose()

    assert signer.close_calls == 1
    assert signer.api_client.close_calls == 1
    assert http.close_calls == 1


@pytest.mark.asyncio
async def test_missing_order_id_does_not_claim_trade_history_pagination(tmp_path):
    class MissingOrderId(FakeClient):
        def __init__(self):
            super().__init__()
            self.history_calls = 0

        async def submit_order(self, plan):
            self.submissions.append(plan)
            if not plan.reduce_only:
                raise AssertionError("receiver must stay undispatched")
            return MutationReceipt(True, None, "0xsource")

        async def lookup_order(self, *args, **kwargs):
            return None

        async def list_trades(self, *args, **kwargs):
            self.history_calls += 1
            return HistoryPage(
                next_cursor=f"synthetic-next-{self.history_calls}",
                complete=False,
            )

    client = MissingOrderId()
    result = await run_handoff(
        make_config(tmp_path / "missing-order-id.jsonl"),
        client,
        clock=FakeClock(),
    )

    assert result.outcome is Outcome.UNKNOWN
    assert client.history_calls == 0
    assert any("order id is missing" in item for item in result.unknown_reasons)
    assert not any("trade history pagination exceeded" in item for item in result.unknown_reasons)
    assert [plan.account_index for plan in client.submissions] == [client.source_account_index]


@pytest.mark.asyncio
async def test_identified_order_keeps_true_pagination_bound_diagnostic(tmp_path):
    class BoundedHistory(FakeClient):
        def __init__(self):
            super().__init__(source_fills=True)
            self.history_calls = 0

        async def list_trades(self, *args, **kwargs):
            self.history_calls += 1
            return HistoryPage(
                next_cursor=f"synthetic-next-{self.history_calls}",
                complete=False,
            )

    client = BoundedHistory()
    result = await run_handoff(
        make_config(tmp_path / "bounded-history.jsonl"),
        client,
        clock=FakeClock(),
    )

    assert result.outcome is Outcome.UNKNOWN
    assert client.history_calls > 0
    assert any("trade history pagination exceeded" in item for item in result.unknown_reasons)
