from __future__ import annotations

import json
from decimal import Decimal

import pytest

from risex_spread_shadow.hood_handoff import (
    AccountSnapshot,
    DepthLevel,
    Direction,
    MarketMetadata,
    OrderBookSnapshot,
    OrderSnapshot,
    ReadinessConfig,
    ReadinessSecretProvider,
    ReadOnlyLighterSdkClient,
    StaticSecretProvider,
    run_readiness,
)
from risex_spread_shadow.hood_handoff import cli


NOW = 1_000.0


def readiness_config(**overrides) -> ReadinessConfig:
    values = {
        "market_symbol": "BTC",
        "quantity": Decimal("0.20"),
        "direction": Direction.LONG,
        "source_account_index": 11,
        "receiver_account_index": 22,
        "api_key_index": 4,
        "freshness_seconds": 10.0,
        "request_timeout_seconds": 1.0,
        "source_limit_price": Decimal("100.0"),
        "receiver_worst_price": Decimal("101.0"),
    }
    values.update(overrides)
    return ReadinessConfig(**values)


def metadata() -> MarketMetadata:
    return MarketMetadata(
        market_id=7,
        symbol="BTC",
        status="active",
        price_decimals=1,
        size_decimals=2,
        minimum_base_amount=Decimal("0.10"),
        minimum_quote_amount=Decimal("10"),
        source_fee_rate=None,
        receiver_fee_rate=None,
        observed_at=NOW,
        margin_evidence="synthetic catalog constraints",
        market_type="perp",
        venue="robinhood",
    )


def book() -> OrderBookSnapshot:
    return OrderBookSnapshot(
        market_id=7,
        symbol="BTC",
        bids=(DepthLevel(Decimal("99.9"), Decimal("1.0"), "bid-1"),),
        asks=(DepthLevel(Decimal("100.1"), Decimal("1.0"), "ask-1"),),
        observed_at=NOW,
    )


def account(
    index: int,
    *,
    position: str = "0",
    active_orders=(),
    incremental: str | None = "2",
    evidence: str = "official opening-margin fixture",
    identity: str | None = None,
) -> AccountSnapshot:
    return AccountSnapshot.from_mapping(
        {
            "account_index": index,
            "market_id": 7,
            "signed_position": position,
            "active_orders": list(active_orders),
            "observed_at": NOW,
            "authorized": True,
            "ready": True,
            "margin_available": "100",
            "margin_required": "1",
            "source_identity": identity or f"account-{index}",
            "incremental_margin_required": incremental,
            "incremental_margin_evidence": evidence,
        }
    )


class FakeReadClient:
    source_account_index = 11
    receiver_account_index = 22

    def __init__(self, *, source=None, receiver=None):
        self.source = source or account(11)
        self.receiver = receiver or account(22)
        self.calls: list[tuple[str, int | str]] = []

    async def resolve_market(self, symbol):
        self.calls.append(("market", symbol))
        return metadata()

    async def order_book_snapshot(self, market_id):
        self.calls.append(("book", market_id))
        return book()

    async def account_snapshot(self, account_index, market_id):
        self.calls.append(("account", account_index))
        return self.source if account_index == 11 else self.receiver


def check(result, name):
    return next(item for item in result.checks if item.name == name)


@pytest.mark.asyncio
async def test_readiness_requires_operator_bounds_and_proven_incremental_margin():
    client = FakeReadClient()
    result = await run_readiness(readiness_config(), client, now=NOW)
    assert result.outcome == "READY"
    assert check(result, "public_book").status == "PASS"
    assert check(result, "source_account_margin").code == "INCREMENTAL_MARGIN_PROVEN"
    assert result.as_dict()["execution"] == "READ_ONLY"
    assert result.as_dict()["trade_plan"]["execution_authorized"] is False
    assert [call[0] for call in client.calls] == ["market", "book", "account", "account"]


@pytest.mark.asyncio
async def test_readiness_missing_incremental_margin_is_unknown_and_never_ready():
    client = FakeReadClient(
        source=account(11, incremental=None, evidence=""),
        receiver=account(22, incremental=None, evidence=""),
    )
    result = await run_readiness(readiness_config(), client, now=NOW)
    assert result.outcome == "UNKNOWN"
    assert check(result, "source_account_margin").status == "UNKNOWN"
    assert check(result, "source_account_margin").code == "MARGIN_EVIDENCE_REQUIRED"
    assert check(result, "receiver_account_margin").code == "MARGIN_EVIDENCE_REQUIRED"
    assert "MARGIN_EVIDENCE_REQUIRED" in json.dumps(result.as_dict())


@pytest.mark.asyncio
async def test_readiness_blocks_nonflat_accounts_and_active_orders():
    active = OrderSnapshot(
        account_index=22,
        market_id=7,
        order_id="active-1",
        client_order_index=1,
        status="open",
        side="BUY",
        order_type="LIMIT",
        time_in_force="POST_ONLY",
        reduce_only=False,
        initial_quantity=Decimal("0.20"),
        remaining_quantity=Decimal("0.20"),
        filled_quantity=Decimal("0"),
        price=Decimal("100.0"),
        observed_at=NOW,
    )
    result = await run_readiness(
        readiness_config(),
        FakeReadClient(source=account(11, position="-0.1"), receiver=account(22, active_orders=(active,))),
        now=NOW,
    )
    assert result.outcome == "BLOCKED"
    assert check(result, "source_account_position").code == "POSITION_NOT_FLAT"
    assert check(result, "receiver_account_active_orders").code == "ACTIVE_ORDERS_PRESENT"


@pytest.mark.asyncio
async def test_readiness_explicitly_reports_unset_trade_bounds():
    result = await run_readiness(readiness_config(source_limit_price=None, receiver_worst_price=None), FakeReadClient(), now=NOW)
    assert result.outcome == "UNKNOWN"
    assert check(result, "source_quote_minimum").status == "UNSET"
    assert check(result, "receiver_quote_minimum").code == "TRADE_BOUND_UNSET"
    payload = result.as_dict()
    assert payload["config"]["trade_bounds"] == "UNSET"
    assert payload["trade_plan"]["order_prices"] == "UNSET"


@pytest.mark.asyncio
async def test_readiness_stale_book_is_blocked_without_refreshing_timestamp():
    class StaleBookClient(FakeReadClient):
        async def order_book_snapshot(self, market_id):
            value = book()
            return OrderBookSnapshot(
                market_id=value.market_id,
                symbol=value.symbol,
                bids=value.bids,
                asks=value.asks,
                observed_at=NOW - 11,
            )

    result = await run_readiness(readiness_config(), StaleBookClient(), now=NOW)
    assert result.outcome == "BLOCKED"
    assert check(result, "public_book").code == "BOOK_STALE"
    assert check(result, "public_book").details["observed_at"] == NOW - 11


@pytest.mark.asyncio
async def test_readiness_account_identity_mismatch_is_blocked():
    result = await run_readiness(
        readiness_config(),
        FakeReadClient(source=account(99)),
        now=NOW,
    )
    assert result.outcome == "BLOCKED"
    assert check(result, "source_account_identity").code == "ACCOUNT_IDENTITY_CONFLICT"


def test_readiness_cli_rejects_execution_flags_before_secret_input(monkeypatch):
    class ExplodingProvider:
        def __init__(self, *args, **kwargs):
            raise AssertionError("secret provider must not be initialized")

    monkeypatch.setattr(cli, "ReadinessSecretProvider", ExplodingProvider)
    with pytest.raises(SystemExit, match="read-only"):
        cli.main(
            [
                "readiness",
                "--symbol",
                "BTC",
                "--quantity",
                "0.20",
                "--direction",
                "LONG",
                "--source-account-index",
                "11",
                "--receiver-account-index",
                "22",
                "--api-key-index",
                "4",
                "--freshness-seconds",
                "10",
                "--request-timeout-seconds",
                "1",
                "--execute",
            ]
        )


def test_readiness_hidden_key_input_fails_closed_without_tty(monkeypatch):
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    monkeypatch.setattr("sys.stderr.isatty", lambda: False)
    provider = ReadinessSecretProvider((11,), 4)
    with pytest.raises(RuntimeError, match="interactive TTY"):
        provider.private_key(11, 4)
    provider.close()


class FakeSigner:
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.auth_calls = []
        self.mutation_calls = []
        FakeSigner.instances.append(self)

    async def create_auth_token_with_expiry(self, **kwargs):
        self.auth_calls.append(kwargs)
        return "synthetic-auth-token", None

    def __getattr__(self, name):
        if name in {"sign_create_order", "sign_cancel_order", "send_tx", "nonce_manager"}:
            raise AssertionError(f"forbidden mutation surface reached: {name}")
        raise AttributeError(name)


class FakeApiClient:
    closed = False

    def __init__(self, configuration):
        self.configuration = configuration

    def close(self):
        self.closed = True


class FakeOrderApi:
    def __init__(self, api_client):
        self.api_client = api_client

    async def order_book_details(self, **kwargs):
        return {
            "code": 200,
            "order_book_details": [
                {
                    "symbol": "BTC",
                    "market_id": 7,
                    "market_type": "perp",
                    "status": "active",
                    "min_base_amount": "0.10",
                    "min_quote_amount": "10",
                    "supported_size_decimals": 2,
                    "supported_price_decimals": 1,
                }
            ],
        }

    async def order_book_orders(self, **kwargs):
        return {
            "code": 200,
            "bids": [{"price": "99.9", "remaining_base_amount": "1.0", "order_id": "bid-1"}],
            "asks": [{"price": "100.1", "remaining_base_amount": "1.0", "order_id": "ask-1"}],
        }

    async def account_active_orders(self, **kwargs):
        assert kwargs["authorization"] == "synthetic-auth-token"
        return {"code": 200, "orders": []}


class FakeAccountApi:
    def __init__(self, api_client):
        self.api_client = api_client

    async def account(self, **kwargs):
        index = int(kwargs["value"])
        return {
            "code": 200,
            "accounts": [
                {
                    "index": index,
                    "l1_address": f"synthetic-account-{index}",
                    "status": "active",
                    "positions": [],
                    "available_balance": "100",
                    "cross_initial_margin_requirement": "1",
                }
            ],
        }


class FakeLighter:
    OrderApi = FakeOrderApi
    AccountApi = FakeAccountApi
    SignerClient = FakeSigner

    class Configuration:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    ApiClient = FakeApiClient

    class nonce_manager:
        class NonceManagerType:
            NONE = "NONE"


class AuthRejectingSigner(FakeSigner):
    async def create_auth_token_with_expiry(self, **kwargs):
        raise RuntimeError("auth token rejected")


class FakeAuthRejectingLighter(FakeLighter):
    SignerClient = AuthRejectingSigner


@pytest.mark.asyncio
async def test_readonly_sdk_reads_official_shapes_without_mutation_or_nonce(monkeypatch):
    FakeSigner.instances.clear()
    config = readiness_config()
    client = ReadOnlyLighterSdkClient(
        config,
        source_account_index=11,
        receiver_account_index=22,
        secrets=StaticSecretProvider({11: "synthetic-key-a", 22: "synthetic-key-b"}),
        signer_factory=FakeSigner,
        clock=lambda: NOW,
    )
    monkeypatch.setattr(ReadOnlyLighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client._lighter = lambda: FakeLighter
    result = await run_readiness(config, client, now=NOW)
    assert result.outcome == "UNKNOWN"
    assert [item.auth_calls for item in FakeSigner.instances] == [[{"deadline": 600, "api_key_index": 4}], [{"deadline": 600, "api_key_index": 4}]]
    assert all(item.kwargs["nonce_management_type"] == "NONE" for item in FakeSigner.instances)
    assert client._api_client is not None
    api_client = client._api_client
    await client.aclose()
    assert api_client.closed is True
    serialized = json.dumps(result.as_dict(), sort_keys=True)
    assert "synthetic-key" not in serialized
    assert "synthetic-auth-token" not in serialized


@pytest.mark.asyncio
async def test_readonly_sdk_auth_failure_is_unknown_and_cleanup_stays_read_only(monkeypatch):
    config = readiness_config()
    client = ReadOnlyLighterSdkClient(
        config,
        source_account_index=11,
        receiver_account_index=22,
        secrets=StaticSecretProvider({11: "synthetic-key-a", 22: "synthetic-key-b"}),
        signer_factory=AuthRejectingSigner,
        clock=lambda: NOW,
    )
    monkeypatch.setattr(ReadOnlyLighterSdkClient, "verify_sdk", staticmethod(lambda: None))
    client._lighter = lambda: FakeAuthRejectingLighter
    result = await run_readiness(config, client, now=NOW)
    assert result.outcome == "UNKNOWN"
    assert check(result, "source_account_authorization").code == "ACCOUNT_READ_AUTH_FAILED"
    assert check(result, "receiver_account_authorization").code == "ACCOUNT_READ_AUTH_FAILED"
    assert not hasattr(client, "submit_order")
    assert not hasattr(client, "cancel_order")
    await client.aclose()
