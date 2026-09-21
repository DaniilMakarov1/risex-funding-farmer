import copy
from pathlib import Path
from dataclasses import replace
from decimal import Decimal

import pytest

from risex_spread_shadow.hood_handoff import (
    ContractError,
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


class ConstantNonceManager:
    """Model the API nonce manager before a prior mutation is executed."""

    async def async_next_nonce(self, api_key_index):
        return api_key_index, 41


class ConstantNonceSigner(FakeSigner):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.nonce_manager = ConstantNonceManager()


class AmbiguousConstantHttp(FakeHttp):
    async def post_form(self, path, *, form):
        self.calls.append((path, dict(form)))
        raise TimeoutError("synthetic response ambiguity")


def _sdk_config(prefix: str) -> HandoffConfig:
    return HandoffConfig(
        market_id=7,
        direction="LONG",
        quantity=Decimal("0.125"),
        source_limit_price=Decimal("100.25"),
        receiver_worst_price=Decimal("101.25"),
        freshness_seconds=10,
        request_timeout_seconds=1,
        order_timeout_seconds=1,
        reconcile_timeout_seconds=1,
        poll_interval_seconds=0.1,
        max_poll_count=2,
        source_order_lifetime_seconds=300,
        client_order_prefix=prefix,
        journal_path=f"/tmp/{prefix}.jsonl",
        api_base_url="https://mainnet.zklighter.elliot.ai",
        chain_id=304,
        api_key_index=4,
        operator_execution_opt_in=True,
    )


def _sdk_plan(
    *,
    account_index: int = 11,
    reduce_only: bool = False,
    order_type: str = "LIMIT",
    time_in_force: str = "POST_ONLY",
    client_order_index: int = 123,
) -> OrderPlan:
    return OrderPlan(
        account_index=account_index,
        market_id=7,
        side="SELL" if account_index == 11 else "BUY",
        quantity=Decimal("0.125"),
        quantity_int=125,
        price=Decimal("100.25") if account_index == 11 else Decimal("101.25"),
        price_int=10025 if account_index == 11 else 10125,
        order_type=order_type,
        time_in_force=time_in_force,
        reduce_only=reduce_only,
        order_expiry_ms=1_500_000 if order_type == "LIMIT" else 0,
        client_order_index=client_order_index,
    )


def _constant_nonce_client(
    *,
    prefix: str,
    signer: FakeSigner | None = None,
    http_factory=FakeHttp,
) -> LighterSdkClient:
    return LighterSdkClient(
        _sdk_config(prefix),
        source_account_index=11,
        receiver_account_index=22,
        secrets=StaticSecretProvider({11: "synthetic-source", 22: "synthetic-receiver"}),
        market_evidence={},
        signer_factory=lambda **kwargs: signer or ConstantNonceSigner(**kwargs),
        http_factory=http_factory,
    )


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
async def test_prepared_order_is_single_use_and_ambiguous_send_cannot_replay(monkeypatch):
    class AmbiguousHttp(FakeHttp):
        async def post_form(self, path, *, form):
            self.calls.append((path, dict(form)))
            raise TimeoutError("synthetic response ambiguity")

    config = HandoffConfig(
        market_id=7,
        direction="LONG",
        quantity=Decimal("0.125"),
        source_limit_price=Decimal("100.25"),
        receiver_worst_price=Decimal("101.25"),
        freshness_seconds=10,
        request_timeout_seconds=1,
        order_timeout_seconds=1,
        reconcile_timeout_seconds=1,
        poll_interval_seconds=0.1,
        max_poll_count=2,
        source_order_lifetime_seconds=300,
        client_order_prefix="prepared-single-use",
        journal_path="/tmp/prepared-single-use.jsonl",
        api_base_url="https://mainnet.zklighter.elliot.ai",
        chain_id=304,
        api_key_index=4,
        operator_execution_opt_in=True,
    )
    signer = FakeSigner()
    client = LighterSdkClient(
        config,
        source_account_index=11,
        receiver_account_index=22,
        secrets=StaticSecretProvider({11: "synthetic-source", 22: "synthetic-receiver"}),
        market_evidence={},
        signer_factory=lambda **kwargs: signer,
        http_factory=AmbiguousHttp,
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

    prepared = await client.prepare_order(plan)
    assert client._http.calls == []
    assert "signed-create-info" not in repr(prepared)
    with pytest.raises(TimeoutError, match="order dispatch"):
        await client.submit_prepared_order(plan, prepared)
    assert len(client._http.calls) == 1

    replay = await client.submit_prepared_order(plan, prepared)
    assert not replay.accepted
    assert "consumed" in (replay.error or "")
    assert len(client._http.calls) == 1


@pytest.mark.asyncio
async def test_prepared_order_invalidates_when_price_binding_changes(monkeypatch):
    import risex_spread_shadow.hood_handoff.sdk as sdk_module

    config = HandoffConfig(
        market_id=7,
        direction="LONG",
        quantity=Decimal("0.125"),
        source_limit_price=Decimal("100.25"),
        receiver_worst_price=Decimal("101.25"),
        freshness_seconds=10,
        request_timeout_seconds=1,
        order_timeout_seconds=1,
        reconcile_timeout_seconds=1,
        poll_interval_seconds=0.1,
        max_poll_count=2,
        source_order_lifetime_seconds=300,
        client_order_prefix="prepared-binding",
        journal_path="/tmp/prepared-binding.jsonl",
        api_base_url="https://mainnet.zklighter.elliot.ai",
        chain_id=304,
        api_key_index=4,
        operator_execution_opt_in=True,
    )
    signer = FakeSigner()
    client = LighterSdkClient(
        config,
        source_account_index=11,
        receiver_account_index=22,
        secrets=StaticSecretProvider({11: "synthetic-source", 22: "synthetic-receiver"}),
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
    prepared = await client.prepare_order(plan)
    changed = replace(plan, price=Decimal("100.35"), price_int=10035)
    rejected = await client.submit_prepared_order(changed, prepared)
    assert not rejected.accepted
    assert "binding" in (rejected.error or "")
    assert client._http.calls == []
    replay = await client.submit_prepared_order(plan, prepared)
    assert not replay.accepted
    assert client._http.calls == []

    expired = await client.prepare_order(plan)
    expired_deadline = expired.deadline
    monkeypatch.setattr(sdk_module.time, "monotonic", lambda: expired_deadline + 1)
    expired_receipt = await client.submit_prepared_order(plan, expired)
    assert not expired_receipt.accepted
    assert "final mutation barrier" in (expired_receipt.error or "")
    assert client._http.calls == []


@pytest.mark.asyncio
async def test_constant_api_nonce_has_one_owner_across_prepare_and_cancel(monkeypatch):
    client = _constant_nonce_client(prefix="constant-owner")
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client._lighter = lambda: FakeModule
    plan = _sdk_plan()

    first = await client.prepare_order(plan)
    with pytest.raises(ContractError, match="already reserved"):
        await client.prepare_order(plan)

    cancel = await client.cancel_order(11, 7, "99")
    assert not cancel.accepted
    assert cancel.error == "contract_error"
    assert client._http.calls == []
    assert client._signers[11].cancel_calls == []

    await client.invalidate_prepared_order(first)
    second = await client.prepare_order(plan)
    receipt = await client.submit_prepared_order(plan, second)
    assert receipt.accepted
    assert len(client._http.calls) == 1
    assert client._signers[11].sign_calls[-1]["nonce"] == 41


@pytest.mark.asyncio
async def test_ambiguous_constant_nonce_blocks_new_preparation(monkeypatch):
    client = _constant_nonce_client(
        prefix="constant-ambiguous",
        http_factory=AmbiguousConstantHttp,
    )
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client._lighter = lambda: FakeModule
    plan = _sdk_plan()
    prepared = await client.prepare_order(plan)

    with pytest.raises(TimeoutError, match="order dispatch"):
        await client.submit_prepared_order(plan, prepared)
    with pytest.raises(ContractError, match="already sent"):
        await client.prepare_order(plan)
    assert len(client._http.calls) == 1


@pytest.mark.asyncio
async def test_prepared_token_is_bound_to_originating_client_and_identity(monkeypatch):
    first = _constant_nonce_client(prefix="owner-a")
    second = _constant_nonce_client(prefix="owner-b")
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    first._lighter = lambda: FakeModule
    second._lighter = lambda: FakeModule
    plan = _sdk_plan()
    prepared = await first.prepare_order(plan)

    foreign = await second.submit_prepared_order(plan, prepared)
    copied = await second.submit_prepared_order(plan, copy.copy(prepared))
    assert not foreign.accepted
    assert not copied.accepted
    assert "binding" in (foreign.error or "")
    assert second._http.calls == []
    # The originating client still owns the untouched preparation.
    accepted = await first.submit_prepared_order(plan, prepared)
    assert accepted.accepted
    assert len(first._http.calls) == 1


@pytest.mark.asyncio
async def test_prepared_sdk_pair_covers_reduce_only_closing_cycle(monkeypatch):
    signer = FakeSigner()
    client = _constant_nonce_client(prefix="prepared-closing", signer=signer)
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client._lighter = lambda: FakeModule
    source = _sdk_plan(account_index=11, reduce_only=True, client_order_index=301)
    receiver = _sdk_plan(
        account_index=22,
        reduce_only=True,
        order_type="MARKET",
        time_in_force="IOC",
        client_order_index=302,
    )

    source_prepared = await client.prepare_order(source)
    receiver_prepared = await client.prepare_order(receiver)
    source_receipt = await client.submit_prepared_order(source, source_prepared)
    receiver_receipt = await client.submit_prepared_order(receiver, receiver_prepared)

    assert source_receipt.accepted and receiver_receipt.accepted
    assert [call["reduce_only"] for call in signer.sign_calls] == [True, True]
    assert [call["nonce"] for call in signer.sign_calls] == [41, 42]
    assert len(client._http.calls) == 2


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


@pytest.mark.asyncio
async def test_incomplete_live_minimums_do_not_refresh_old_market_evidence(monkeypatch):
    class IncompleteOrderApi(FakeModule.OrderApi):
        async def order_book_details(self, **kwargs):
            payload = await super().order_book_details(**kwargs)
            payload["order_book_details"][0].pop("min_quote_amount")
            return payload

    class IncompleteModule(FakeModule):
        OrderApi = IncompleteOrderApi

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
        client_order_prefix="missing-minimum-test",
        journal_path="/tmp/missing-minimum-test.jsonl",
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
            "minimum_quote_amount": "10",
            "observed_at": 1000.0,
            "margin_evidence": "saved operator evidence",
        },
        signer_factory=lambda **kwargs: FakeSigner(**kwargs),
        clock=lambda: 1300.0,
    )
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client._lighter = lambda: IncompleteModule

    metadata = await client.market_metadata(7)

    assert metadata.minimum_quote_amount == Decimal("10")
    assert metadata.observed_at == 1000.0
