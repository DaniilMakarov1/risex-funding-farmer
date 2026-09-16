from pathlib import Path
from decimal import Decimal

import pytest

from risex_spread_shadow.hood_handoff import (
    HandoffConfig,
    LighterSdkClient,
    OrderPlan,
    REQUIRED_LIGHTER_SDK_VERSION,
    StaticSecretProvider,
)


def test_sdk_pin_and_single_attempt_surface_are_explicit():
    text = Path("pyproject.toml").read_text()
    assert f"lighter-sdk=={REQUIRED_LIGHTER_SDK_VERSION}" in text
    source = Path("src/risex_spread_shadow/hood_handoff/sdk.py").read_text()
    assert "sign_create_order" in source
    assert "sign_cancel_order" in source
    assert "send_tx" in source
    assert ".create_order(" not in source
    assert ".cancel_order(" not in source
    assert "aiohttp_retry" not in source


class FakeSigner:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.sign_calls = []
        self.cancel_calls = []
        self.nonce_manager = FakeNonceManager()

    async def sign_create_order(self, **kwargs):
        self.sign_calls.append(kwargs)
        return (14, "signed-create-info", "0xcreate", None)

    async def sign_cancel_order(self, **kwargs):
        self.cancel_calls.append(kwargs)
        return (15, "signed-cancel-info", "0xcancel", None)

    async def create_auth_token_with_expiry(self, **kwargs):
        return "fixture-token", None


class FakeNonceManager:
    def __init__(self):
        self.value = 40

    async def async_next_nonce(self, api_key_index):
        self.value += 1
        return api_key_index, self.value


class FakeHttp:
    def __init__(self, *args, **kwargs):
        self.calls = []

    async def post_form(self, path, *, form):
        self.calls.append((path, dict(form)))
        return {"code": 200, "tx_hash": "0xserver"}


class FakeTxApi:
    instances = []

    def __init__(self, api_client):
        self.api_client = api_client
        self.calls = []
        self.__class__.instances.append(self)

    async def send_tx(self, **kwargs):
        self.calls.append(kwargs)
        return {"code": 200, "tx_hash": "0xserver"}


class FakeModule:
    TransactionApi = FakeTxApi

    class OrderApi:
        def __init__(self, api_client):
            self.api_client = api_client

        async def order_book_details(self, **kwargs):
            return {
                "code": 200,
                "order_book_details": [
                    {
                        "symbol": "HOOD",
                        "market_id": 7,
                        "market_type": "perp",
                        "status": "active",
                        "min_base_amount": "0.001",
                        "min_quote_amount": "1",
                        "supported_size_decimals": 3,
                        "supported_price_decimals": 2,
                    }
                ],
                "spot_order_book_details": [],
            }

        async def trades(self, **kwargs):
            return {
                "code": 200,
                "trades": [
                    {
                        "trade_id": 9,
                        "trade_id_str": "9",
                        "market_id": 7,
                        "size": "0.125",
                        "price": "100.25",
                        "ask_id": 145,
                        "bid_id": 245,
                        "ask_client_id": 123,
                        "ask_client_id_str": "123",
                        "bid_client_id": 456,
                        "bid_client_id_str": "456",
                        "ask_account_id": 11,
                        "bid_account_id": 22,
                        "is_maker_ask": True,
                        "timestamp": 1000,
                        "type": "trade",
                        "tx_hash": "0xtrade",
                        "usd_amount": "12.53125",
                    }
                ],
                "next_cursor": None,
            }

    class Configuration:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class ApiClient:
        def __init__(self, configuration):
            self.configuration = configuration


@pytest.mark.asyncio
async def test_sdk_sign_tuple_and_send_are_each_single_explicit_call(monkeypatch):
    signer = FakeSigner()
    config = HandoffConfig(
        market_id=7,
        direction="LONG",
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
        client_order_prefix="sdk-test",
        journal_path="/tmp/sdk-test.jsonl",
        api_base_url="https://mainnet.zklighter.elliot.ai",
        chain_id=304,
        api_key_index=4,
        operator_execution_opt_in=True,
    )
    client = LighterSdkClient(
        config,
        source_account_index=11,
        receiver_account_index=22,
        secrets=StaticSecretProvider({11: "secret-a", 22: "secret-b"}),
        market_evidence={},
        signer_factory=lambda **kwargs: signer,
        http_factory=FakeHttp,
    )
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client._lighter = lambda: FakeModule
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
    assert receipt.accepted
    assert len(signer.sign_calls) == 1
    assert len(client._http.calls) == 1
    assert client._http.calls[0][0] == "api/v1/sendTx"
    assert client._http.calls[0][1]["tx_type"] == 14
    assert signer.sign_calls[0]["nonce"] == 41
    cancel = await client.cancel_order(11, 7, "99")
    assert cancel.accepted
    assert len(signer.cancel_calls) == 1
    assert len(client._http.calls) == 2
    assert signer.cancel_calls[0]["nonce"] == 42
    short_receiver = OrderPlan(
        account_index=22,
        market_id=7,
        side="SELL",
        quantity=Decimal("0.125"),
        quantity_int=125,
        price=Decimal("250.00"),
        price_int=25000,
        order_type="MARKET",
        time_in_force="IOC",
        reduce_only=False,
        order_expiry_ms=0,
        client_order_index=456,
    )
    short_receipt = await client.submit_order(short_receiver)
    assert short_receipt.accepted
    assert signer.sign_calls[-1]["is_ask"] is True
    assert signer.sign_calls[-1]["order_type"] == 1
    assert signer.sign_calls[-1]["time_in_force"] == 0
    assert signer.sign_calls[-1]["reduce_only"] is False
    assert signer.sign_calls[-1]["price"] == 25000
    assert len(client._http.calls) == 3


@pytest.mark.asyncio
async def test_official_orderbook_details_schema_is_selected_without_fee_invention(monkeypatch):
    config = HandoffConfig(
        market_id=7,
        direction="LONG",
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
        client_order_prefix="schema-test",
        journal_path="/tmp/schema-test.jsonl",
        api_base_url="https://mainnet.zklighter.elliot.ai",
        chain_id=304,
        api_key_index=4,
    )
    client = LighterSdkClient(
        config,
        source_account_index=11,
        receiver_account_index=22,
        secrets=StaticSecretProvider({11: "secret-a", 22: "secret-b"}),
        market_evidence={
            "market_id": 7,
            "symbol": "HOOD",
            "source_fee_rate": "0.001",
            "receiver_fee_rate": "0.001",
            "observed_at": 1000.0,
            "margin_evidence": "accountLimits fixture",
        },
        signer_factory=lambda **kwargs: FakeSigner(**kwargs),
        clock=lambda: 2000.0,
    )
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client._lighter = lambda: FakeModule
    metadata = await client.market_metadata(7)
    assert metadata.symbol == "HOOD"
    assert metadata.observed_at == 2000.0
    assert metadata.price_decimals == 2
    assert metadata.size_decimals == 3
    assert metadata.minimum_quote_amount == Decimal("1")
    page = await client.list_trades(11, 7, order_id="145")
    assert page.trades[0].order_id == "145"
    assert page.trades[0].side == "SELL"
    assert page.trades[0].quantity == Decimal("0.125")
    assert page.trades[0].counterparty_account_index == 22
    assert page.trades[0].counterparty_order_id == "245"
    assert page.trades[0].client_order_index == "123"
    assert page.trades[0].counterparty_client_order_index == "456"
