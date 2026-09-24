import asyncio
import copy
from pathlib import Path
from dataclasses import replace
from decimal import Decimal
from time import perf_counter, monotonic

import pytest

from risex_spread_shadow.hood_handoff import (
    ContractError,
    HandoffConfig,
    HandoffEngine,
    LighterSdkClient,
    OrderPlan,
    REQUIRED_LIGHTER_SDK_VERSION,
    StaticSecretProvider,
)
from risex_spread_shadow.hood_handoff.contracts import LeverageNotSent


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
        self.leverage_calls = []
        self.nonce_manager = FakeNonceManager()

    async def sign_create_order(self, **kwargs):
        self.sign_calls.append(kwargs)
        return (14, "signed-create-info", "0xcreate", None)

    async def sign_cancel_order(self, **kwargs):
        self.cancel_calls.append(kwargs)
        return (15, "signed-cancel-info", "0xcancel", None)

    async def sign_update_leverage(self, **kwargs):
        self.leverage_calls.append(kwargs)
        return (16, "signed-leverage-info", "0xleverage", None)

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


class DelayedSigner(FakeSigner):
    """Synthetic signer with shared timing and failure instrumentation."""

    def __init__(self, tracker, **kwargs):
        super().__init__(**kwargs)
        self.tracker = tracker

    async def sign_create_order(self, **kwargs):
        account_index = self.kwargs["account_index"]
        self.tracker["active"] += 1
        self.tracker["peak"] = max(self.tracker["peak"], self.tracker["active"])
        self.tracker["started"].append((account_index, perf_counter()))
        try:
            delay = self.tracker["delays"].get(account_index, self.tracker["delay"])
            await asyncio.sleep(delay)
            if account_index in self.tracker["fail_accounts"]:
                raise ContractError(f"synthetic signing failure for account {account_index}")
            return await super().sign_create_order(**kwargs)
        finally:
            self.tracker["active"] -= 1


def _delayed_tracker(*, delay=0.03, delays=None, fail_accounts=()):
    return {
        "active": 0,
        "peak": 0,
        "delay": delay,
        "delays": {} if delays is None else dict(delays),
        "fail_accounts": set(fail_accounts),
        "started": [],
    }


def _delayed_client(prefix: str, tracker) -> LighterSdkClient:
    return LighterSdkClient(
        _sdk_config(prefix),
        source_account_index=11,
        receiver_account_index=22,
        secrets=StaticSecretProvider({11: "synthetic-source", 22: "synthetic-receiver"}),
        market_evidence={},
        signer_factory=lambda **kwargs: DelayedSigner(tracker, **kwargs),
        http_factory=FakeHttp,
    )


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
    assert cancel.diagnostic_timings is not None
    assert {"cancel_nonce_seconds", "cancel_signing_seconds", "cancel_transport_roundtrip_seconds"} <= set(cancel.diagnostic_timings)
    assert all(value >= 0 for value in cancel.diagnostic_timings.values())
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
async def test_source_nonce_still_stale_after_send_while_receiver_domain_is_independent(monkeypatch):
    client = _constant_nonce_client(prefix="source-stale-receiver-independent")
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client._lighter = lambda: FakeModule
    source = _sdk_plan(account_index=11)
    receiver = _sdk_plan(account_index=22, order_type="MARKET", time_in_force="IOC")

    prepared_source = await client.prepare_order(source)
    assert (await client.submit_prepared_order(source, prepared_source)).accepted
    prepared_receiver = await client.prepare_order(receiver)
    assert prepared_receiver.account_index == 22
    assert client._signers[22].sign_calls[-1]["nonce"] == 41

    # A source cancel reservation attempted before the venue advances this
    # account/key nonce would reuse the already-sent source-create nonce.
    cancel = await client.cancel_order(11, 7, "99")
    assert not cancel.accepted
    assert cancel.error == "contract_error"
    assert client._signers[11].cancel_calls == []
    assert len(client._http.calls) == 1
    await client.invalidate_prepared_order(prepared_receiver)


@pytest.mark.asyncio
async def test_prepared_cancel_uses_fresh_source_nonce_and_one_send(monkeypatch):
    client = LighterSdkClient(
        _sdk_config("prepared-cancel-fresh"), source_account_index=11,
        receiver_account_index=22,
        secrets=StaticSecretProvider({11: "synthetic-source", 22: "synthetic-receiver"}),
        market_evidence={}, signer_factory=lambda **kwargs: FakeSigner(**kwargs),
        http_factory=FakeHttp,
    )
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client._lighter = lambda: FakeModule
    source = _sdk_plan(account_index=11)
    receiver = _sdk_plan(account_index=22, order_type="MARKET", time_in_force="IOC")
    first = await client.prepare_order(source)
    assert (await client.submit_prepared_order(source, first)).accepted
    other = await client.prepare_order(receiver)
    assert other._nonce == 41
    cancel = await client.prepare_cancel_order(11, 7, "99")
    assert cancel._nonce == 42
    assert client._signers[11].cancel_calls[-1]["nonce"] == 42
    assert len(client._http.calls) == 1
    receipt = await client.submit_prepared_cancel_order(11, 7, "99", cancel,
                                                         deadline=monotonic() + 1)
    assert receipt.accepted
    assert receipt.diagnostic_timings["cancel_prepared_before_dispatch"] == 1.0
    assert len(client._http.calls) == 2
    again = await client.submit_prepared_cancel_order(11, 7, "99", cancel,
                                                       deadline=monotonic() + 1)
    assert not again.accepted and again.error == "prepared cancel not sent"
    assert len(client._http.calls) == 2
    await client.invalidate_prepared_order(other)


@pytest.mark.asyncio
async def test_prepared_cancel_stale_nonce_identity_expiry_and_ambiguous_send(monkeypatch):
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    stale = _constant_nonce_client(prefix="prepared-cancel-stale")
    stale._lighter = lambda: FakeModule
    first = await stale.prepare_order(_sdk_plan())
    assert (await stale.submit_prepared_order(_sdk_plan(), first)).accepted
    with pytest.raises(ContractError, match="already sent"):
        await stale.prepare_cancel_order(11, 7, "99")
    assert len(stale._http.calls) == 1

    ambiguous = _constant_nonce_client(prefix="prepared-cancel-unknown",
                                       http_factory=AmbiguousConstantHttp)
    ambiguous._lighter = lambda: FakeModule
    token = await ambiguous.prepare_cancel_order(11, 7, "99")
    wrong = await ambiguous.submit_prepared_cancel_order(11, 7, "100", token,
                                                         deadline=monotonic() + 1)
    assert wrong.error == "prepared cancel not sent" and ambiguous._http.calls == []
    token = await ambiguous.prepare_cancel_order(11, 7, "99")
    expired = await ambiguous.submit_prepared_cancel_order(11, 7, "99", token,
                                                           deadline=monotonic() - 1)
    assert expired.error == "prepared cancel not sent" and ambiguous._http.calls == []
    token = await ambiguous.prepare_cancel_order(11, 7, "99")
    with pytest.raises(TimeoutError):
        await ambiguous.submit_prepared_cancel_order(11, 7, "99", token,
                                                     deadline=monotonic() + 1)
    retry = await ambiguous.submit_prepared_cancel_order(11, 7, "99", token,
                                                         deadline=monotonic() + 1)
    assert retry.error == "prepared cancel not sent" and len(ambiguous._http.calls) == 1


@pytest.mark.asyncio
async def test_warmed_ws_transport_is_bound_before_preparation_and_never_http_replays(monkeypatch):
    class ValidHashSigner(ConstantNonceSigner):
        async def sign_create_order(self, **kwargs):
            tx_type, tx_info, _tx_hash, error = await super().sign_create_order(**kwargs)
            return tx_type, tx_info, "0x" + "a" * 64, error

    class Sender:
        def __init__(self):
            self.calls = []
            self.started = self.closed = False

        async def start(self):
            self.started = True

        async def send(self, tx_type, tx_info, tx_hash, *, deadline):
            self.calls.append((tx_type, tx_hash))
            raise TimeoutError("synthetic ambiguous WS response")

        async def close(self):
            self.closed = True

    client = _constant_nonce_client(prefix="warmed-ws-selection", signer=ValidHashSigner())
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client._lighter = lambda: FakeModule
    sender = Sender()
    await client.enable_warmed_ws_sender(sender)
    assert sender.started
    plan = _sdk_plan()
    prepared = await client.prepare_order(plan)
    with pytest.raises(ContractError, match="already bound"):
        await client.enable_warmed_ws_sender(Sender())
    with pytest.raises(TimeoutError):
        await client.submit_prepared_order(plan, prepared)
    retry = await client.submit_prepared_order(plan, prepared)
    assert not retry.accepted and "consumed" in (retry.error or "")
    assert sender.calls == [(14, "0x" + "a" * 64)]
    assert client._http.calls == []
    await client.aclose()
    assert sender.closed


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
async def test_sdk_preparation_overlaps_distinct_accounts_but_serializes_same_account(monkeypatch):
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))

    overlap_tracker = _delayed_tracker()
    overlap_client = _delayed_client("lock-overlap", overlap_tracker)
    overlap_client._lighter = lambda: FakeModule
    source = _sdk_plan(account_index=11, client_order_index=701)
    receiver = _sdk_plan(
        account_index=22,
        order_type="MARKET",
        time_in_force="IOC",
        client_order_index=702,
    )
    started = perf_counter()
    source_prepared, receiver_prepared = await asyncio.gather(
        overlap_client.prepare_order(source),
        overlap_client.prepare_order(receiver),
    )
    overlap_elapsed = perf_counter() - started

    assert overlap_tracker["peak"] == 2
    assert overlap_tracker["started"]
    assert max(at for _, at in overlap_tracker["started"]) - min(
        at for _, at in overlap_tracker["started"]
    ) < overlap_tracker["delay"] * 0.5
    assert overlap_elapsed < overlap_tracker["delay"] * 1.8
    await overlap_client.invalidate_prepared_order(source_prepared)
    await overlap_client.invalidate_prepared_order(receiver_prepared)
    assert overlap_client._nonce_reservations == {}
    assert {item._state for item in overlap_client._prepared_registry.values()} == {"INVALIDATED"}

    serial_tracker = _delayed_tracker()
    serial_client = _delayed_client("lock-serial", serial_tracker)
    serial_client._lighter = lambda: FakeModule
    same_account = _sdk_plan(account_index=11, client_order_index=703)
    same_account_again = replace(same_account, client_order_index=704)
    started = perf_counter()
    first, second = await asyncio.gather(
        serial_client.prepare_order(same_account),
        serial_client.prepare_order(same_account_again),
    )
    serial_elapsed = perf_counter() - started

    assert serial_tracker["peak"] == 1
    assert serial_elapsed > overlap_elapsed * 1.35
    await serial_client.invalidate_prepared_order(first)
    await serial_client.invalidate_prepared_order(second)
    assert serial_client._nonce_reservations == {}
    assert {item._state for item in serial_client._prepared_registry.values()} == {"INVALIDATED"}


@pytest.mark.asyncio
async def test_sdk_prepared_pair_failure_drains_successful_preparation(monkeypatch):
    tracker = _delayed_tracker(delay=0.01, fail_accounts={22})
    client = _delayed_client("lock-failure-drain", tracker)
    client._lighter = lambda: FakeModule
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))

    with pytest.raises(ContractError, match="synthetic signing failure"):
        await HandoffEngine(client)._prepare_pair(
            _sdk_plan(account_index=11, client_order_index=711),
            _sdk_plan(
                account_index=22,
                order_type="MARKET",
                time_in_force="IOC",
                client_order_index=712,
            ),
        )

    assert tracker["active"] == 0
    assert client._nonce_reservations == {}
    assert {item._state for item in client._prepared_registry.values()} == {"INVALIDATED"}
    assert client._blocked_nonces == {}


@pytest.mark.asyncio
async def test_sdk_prepared_pair_cancellation_drains_registered_source(monkeypatch):
    tracker = _delayed_tracker(delay=0.05, delays={11: 0.001})
    client = _delayed_client("lock-cancel-drain", tracker)
    client._lighter = lambda: FakeModule
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    pair_task = asyncio.create_task(
        HandoffEngine(client)._prepare_pair(
            _sdk_plan(account_index=11, client_order_index=721),
            _sdk_plan(
                account_index=22,
                order_type="MARKET",
                time_in_force="IOC",
                client_order_index=722,
            ),
        )
    )
    for _ in range(100):
        if client._prepared_registry:
            break
        await asyncio.sleep(0.001)
    assert client._prepared_registry
    pair_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pair_task

    assert tracker["active"] == 0
    assert client._nonce_reservations == {}
    assert {item._state for item in client._prepared_registry.values()} == {"INVALIDATED"}
    assert client._blocked_nonces == {}


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


@pytest.mark.asyncio
@pytest.mark.parametrize("transport_fails", [False, True])
async def test_prepared_numeric_timings_separate_nonce_signing_and_transport(monkeypatch, transport_fails):
    from types import SimpleNamespace
    import time as real_time
    import risex_spread_shadow.hood_handoff.sdk as sdk_module
    ticks = [0.0]
    class Nonce(FakeNonceManager):
        async def async_next_nonce(self, index):
            ticks[0] += 0.2
            return await super().async_next_nonce(index)
    class Signer(FakeSigner):
        def __init__(self):
            super().__init__()
            self.nonce_manager = Nonce()
        async def sign_create_order(self, **kwargs):
            ticks[0] += 0.3
            return await super().sign_create_order(**kwargs)
    class Transport(FakeHttp):
        async def post_form(self, path, *, form):
            ticks[0] += 0.5
            if transport_fails:
                self.calls.append((path, dict(form)))
                raise TimeoutError("synthetic lost response")
            return await super().post_form(path, form=form)
    monkeypatch.setattr(sdk_module, "time", SimpleNamespace(monotonic=real_time.monotonic, time=real_time.time, perf_counter=lambda: ticks[0]))
    monkeypatch.setattr(LighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    signer = Signer()
    client = _constant_nonce_client(prefix="numeric-timing", signer=signer, http_factory=Transport)
    client._lighter = lambda: FakeModule
    plan = _sdk_plan()
    prepared = await client.prepare_order(plan)
    assert prepared.diagnostic_timings == {"preparation_lock_wait_seconds": 0.0, "nonce_acquisition_seconds": 0.2, "signing_call_seconds": 0.3}
    if transport_fails:
        with pytest.raises(TimeoutError): await client.submit_prepared_order(plan, prepared)
    else:
        assert (await client.submit_prepared_order(plan, prepared)).accepted
    assert prepared.diagnostic_timings["transport_roundtrip_seconds"] == 0.5
    assert len(client._http.calls) == 1
    assert len(signer.sign_calls) == 1
    assert not (await client.submit_prepared_order(plan, prepared)).accepted
    assert len(client._http.calls) == 1
    assert "signed-create-info" not in repr(prepared.diagnostic_timings)


@pytest.mark.asyncio
@pytest.mark.parametrize('maker_ask,account,role', [(True,11,'maker'),(True,22,'taker'),(False,11,'taker'),(False,22,'maker')])
@pytest.mark.parametrize('component,expected', [(0,'0'), (123,None), (None,None), (False,None), ('0',None), (-1,None)])
async def test_actual_trade_fee_components_follow_own_role_without_guessing_units(monkeypatch, maker_ask, account, role, component, expected):
    class API(FakeModule.OrderApi):
        async def trades(self, **kwargs):
            payload = await super().trades(**kwargs)
            receipt = payload['trades'][0]
            receipt.update(is_maker_ask=maker_ask, maker_fee=999, taker_fee=999,
                           integrator_maker_fee=999, integrator_taker_fee=999, fee='777')
            receipt[role + '_fee'] = component
            receipt['integrator_' + role + '_fee'] = 0
            return payload
    monkeypatch.setattr(FakeModule, 'OrderApi', API)
    monkeypatch.setattr(LighterSdkClient, 'verify_sdk', staticmethod(lambda: None))
    client = _constant_nonce_client(prefix='fee-proof')
    client._lighter = lambda: FakeModule
    page = await client.list_trades(account, 7)
    receipt = page.trades[0]
    assert receipt.fee == (None if expected is None else Decimal(expected))
    assert receipt.fee_role == role
    assert receipt.integrator_fee_raw == 0
    assert receipt.venue_fee_raw == (component if type(component) is int and component >= 0 else None)
    assert receipt.fee_evidence == ('EXPLICIT_ZERO_OFFICIAL_COMPONENTS' if expected == '0'
                                    else 'NONZERO_UNIT_UNVERIFIED' if component == 123 else 'MISSING_OR_INVALID_COMPONENTS')
    await client.aclose()


@pytest.mark.parametrize('fields', [
    {'maker_fee':0}, {'maker_fee':0,'integrator_maker_fee':False},
    {'maker_fee':0,'integrator_maker_fee':8}, {'maker_fee':0,'integrator_maker_fee':0,'is_maker_ask':1},
])
def test_incomplete_integrator_or_role_evidence_never_assumes_free_trade(fields):
    from risex_spread_shadow.hood_handoff.sdk import _trade_fee_evidence
    assert _trade_fee_evidence({'is_maker_ask':True, **fields}, is_ask=True, distinct_accounts=True)['fee'] is None
    assert _trade_fee_evidence({'is_maker_ask':True, 'maker_fee':0, 'integrator_maker_fee':0},
                               is_ask=True, distinct_accounts=False)['fee'] is None


@pytest.mark.asyncio
async def test_leverage_fraction_is_signed_exactly_once_and_ambiguous_send_is_not_replayed():
    signer = FakeSigner(account_index=11)
    client = _constant_nonce_client(prefix="leverage-exact", signer=signer, http_factory=AmbiguousConstantHttp)
    client._lighter = lambda: FakeModule
    with pytest.raises(TimeoutError):
        await client.update_leverage_fraction(11, 7, 4166, 0)
    assert len(signer.leverage_calls) == 1
    assert signer.leverage_calls[0]["fraction"] == 4166
    assert signer.leverage_calls[0]["margin_mode"] == 0
    assert len(client._http.calls) == 1
    await client.aclose()


@pytest.mark.asyncio
async def test_leverage_prepared_identity_is_durable_boundary_before_single_send():
    class ValidLeverageSigner(FakeSigner):
        async def sign_update_leverage(self, **kwargs):
            self.leverage_calls.append(kwargs)
            return (20, 'signed-leverage-info', 'aA' * 40, None)

    signer = ValidLeverageSigner(account_index=11)
    client = _constant_nonce_client(prefix="leverage-prepared-identity", signer=signer,
                                    http_factory=AmbiguousConstantHttp)
    client._lighter = lambda: FakeModule
    identities = []
    with pytest.raises(TimeoutError):
        await client.update_leverage_fraction(
            11, 7, 4166, 0, prepared_intent=lambda value: identities.append(dict(value)))
    assert identities == [{
        'account_index': 11, 'market_id': 7, 'api_key_index': client.config.api_key_index,
        'fraction_bps': 4166, 'margin_mode': 0, 'nonce': 41,
        'tx_hash': 'aA' * 40, 'tx_type': 20,
    }]
    assert len(client._http.calls) == 1
    assert not {'tx_info', 'signature', 'private_key'} & identities[0].keys()
    await client.aclose()

    refused = _constant_nonce_client(prefix="leverage-prepared-refusal", signer=ValidLeverageSigner(account_index=11))
    refused._lighter = lambda: FakeModule
    with pytest.raises(LeverageNotSent):
        await refused.update_leverage_fraction(
            11, 7, 4166, 0, prepared_intent=lambda _: (_ for _ in ()).throw(OSError('journal unavailable')))
    assert not refused._http.calls
    await refused.aclose()


@pytest.mark.asyncio
async def test_leverage_transaction_read_returns_only_identity_and_finality_fields():
    tx_hash = 'bB' * 40

    class TxApi:
        def __init__(self, api_client):
            pass

        async def tx(self, *, by, value, _request_timeout):
            assert (by, value) == ('hash', tx_hash)
            return {'code': 200, 'hash': tx_hash, 'type': 20, 'status': 2,
                    'account_index': 11, 'api_key_index': 4, 'nonce': 41,
                    'executed_at': 1000, 'committed_at': 1001, 'verified_at': 1002,
                    'info': 'signed/private transaction body'}

    class Module(FakeModule):
        TransactionApi = TxApi

    client = _constant_nonce_client(prefix='leverage-transaction-read')
    client._lighter = lambda: Module
    value = await client.read_leverage_transaction(tx_hash)
    assert value == {'hash': tx_hash, 'type': 20, 'status': 2,
                     'account_index': 11, 'api_key_index': 4, 'nonce': 41,
                     'executed_at': 1000, 'committed_at': 1001, 'verified_at': 1002}
    assert 'info' not in value
    await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize('times', [
    (1000, 1001, 10**18), (1000, 1001, 999), (1000, 0, 1002),
])
async def test_leverage_transaction_diagnostic_rejects_unproved_times(times):
    tx_hash = 'bB' * 40

    class TxApi:
        def __init__(self, api_client):
            pass

        async def tx(self, *, by, value, _request_timeout):
            return {'code': 200, 'hash': tx_hash, 'type': 20, 'status': 2,
                    'account_index': 11, 'api_key_index': 4, 'nonce': 41,
                    'executed_at': times[0], 'committed_at': times[1],
                    'verified_at': times[2]}

    class Module(FakeModule):
        TransactionApi = TxApi

    client = _constant_nonce_client(prefix='leverage-transaction-invalid-time')
    client._lighter = lambda: Module
    try:
        with pytest.raises(ContractError, match='timing is unproved'):
            await client.read_leverage_transaction(tx_hash)
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_leverage_fraction_rejects_outside_owner_range_before_signing():
    signer = FakeSigner(account_index=11)
    client = _constant_nonce_client(prefix="leverage-limit", signer=signer)
    client._lighter = lambda: FakeModule
    with pytest.raises(ContractError, match="1x..4x"):
        await client.update_leverage_fraction(11, 7, 2499, 0)
    assert not signer.leverage_calls
    assert not client._http.calls
    await client.aclose()
