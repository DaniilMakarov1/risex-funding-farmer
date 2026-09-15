from __future__ import annotations

import json
from decimal import Decimal

import pytest

from risex_spread_shadow.hood_handoff import (
    AccountMarginEvidence,
    AccountSnapshot,
    Direction,
    ReadinessConfig,
    ReadinessMarketMarginEvidence,
    ReadinessMarketMetadata,
    ReadOnlyLighterSdkClient,
    StaticSecretProvider,
    run_readiness,
)


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


def account_response(index: int, *, positions=None, **overrides):
    value = {
        "index": index,
        "l1_address": f"synthetic-account-{index}",
        "status": "active",
        "positions": (
            [
                {
                    "market_id": 7,
                    "symbol": "BTC",
                    "position": "0",
                    "sign": 1,
                    "initial_margin_fraction": "0.1000",
                    "margin_mode": 0,
                    "allocated_margin": "0",
                    "open_order_count": 0,
                    "pending_order_count": 0,
                    "position_tied_order_count": 0,
                }
            ]
            if positions is None
            else positions
        ),
        "available_balance": "100",
        "cross_initial_margin_requirement": "1",
        "cross_maintenance_margin_requirement": "0.5",
        "cross_asset_value": "100",
        "total_order_count": 2,
        "pending_order_count": 1,
        "total_isolated_order_count": 1,
        # This additive field is intentionally secret-bearing and must be
        # discarded by the strict response whitelist.
        "private_key": "secret-sentinel-must-not-escape",
    }
    value.update(overrides)
    return value


def account_with_other_market_row(index: int, **overrides):
    value = account_response(index, **overrides)
    value["positions"] = [
        *value["positions"],
        {
            "market_id": 99,
            "open_order_count": 1,
            "pending_order_count": 1,
            "position_tied_order_count": 0,
            "private_key": "secret-other-market-sentinel-must-not-escape",
        },
    ]
    return value


class FakeSigner:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    async def create_auth_token_with_expiry(self, **kwargs):
        return "synthetic-auth-token"


class FakeApiClient:
    def __init__(self, configuration):
        self.configuration = configuration

    def close(self):
        return None


class FakeOrderApi:
    account_payloads = {}
    market_details = {
        "symbol": "BTC",
        "market_id": 7,
        "market_type": "perp",
        "status": "active",
        "min_base_amount": "0.10",
        "min_quote_amount": "10",
        "supported_size_decimals": 2,
        "supported_price_decimals": 1,
        "default_initial_margin_fraction": 500,
        "min_initial_margin_fraction": 100,
        "private_key": "secret-market-sentinel-must-not-escape",
    }

    def __init__(self, api_client):
        self.api_client = api_client

    async def order_book_details(self, **kwargs):
        return {
            "code": 200,
            "order_book_details": [dict(self.market_details)],
        }

    async def order_book_orders(self, **kwargs):
        return {
            "code": 200,
            "bids": [{"price": "99.9", "remaining_base_amount": "1.0", "order_id": "bid-1"}],
            "asks": [{"price": "100.1", "remaining_base_amount": "1.0", "order_id": "ask-1"}],
        }

    async def account_active_orders(self, **kwargs):
        return {"code": 200, "orders": []}


class FakeAccountApi:
    payloads = {}

    def __init__(self, api_client):
        self.api_client = api_client

    async def account(self, **kwargs):
        return {"code": 200, "accounts": [self.payloads[int(kwargs["value"])]]}


class FakeLighter:
    OrderApi = FakeOrderApi
    AccountApi = FakeAccountApi
    SignerClient = FakeSigner
    ApiClient = FakeApiClient

    class Configuration:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class nonce_manager:
        class NonceManagerType:
            NONE = "NONE"


def sdk_client(payloads=None):
    FakeAccountApi.payloads = payloads or {11: account_response(11), 22: account_response(22)}
    client = ReadOnlyLighterSdkClient(
        readiness_config(),
        source_account_index=11,
        receiver_account_index=22,
        secrets=StaticSecretProvider({11: "synthetic-key-a", 22: "synthetic-key-b"}),
        clock=lambda: NOW,
    )
    client._lighter = lambda: FakeLighter
    return client


def complete_account_evidence(**overrides) -> AccountMarginEvidence:
    account = {
        "total_order_count": 0,
        "pending_order_count": 0,
        "total_isolated_order_count": 0,
        "cross_asset_value": "0",
        "cross_initial_margin_requirement": "0",
        "cross_maintenance_margin_requirement": "0",
    }
    account.update(overrides.pop("account", {}))
    selected = {
        "margin_mode": 0,
        "initial_margin_fraction": "0",
        "allocated_margin": "0",
        "open_order_count": 0,
        "pending_order_count": 0,
        "position_tied_order_count": 0,
    }
    selected.update(overrides.pop("selected_position", {}))
    return AccountMarginEvidence.from_response(
        account_index=11,
        market_id=7,
        source_identity="synthetic-account-11",
        observed_at=NOW,
        selected_position=selected,
        account=account,
        source="synthetic account response",
        sdk_version="1.1.2",
        **overrides,
    )


def test_account_margin_evidence_preserves_explicit_zero_and_absent_position_row():
    observed = complete_account_evidence()
    observed_payload = observed.as_dict()
    assert observed.status == "OBSERVED"
    assert observed_payload["selected_position"]["present"] is True
    assert observed_payload["selected_position"]["allocated_margin"] == {
        "present": True,
        "valid": True,
        "raw": "0",
        "units": "unverified",
    }
    assert observed_payload["account"]["total_order_count"]["raw"] == 0

    absent = AccountMarginEvidence.from_response(
        account_index=11,
        market_id=7,
        source_identity="synthetic-account-11",
        observed_at=NOW,
        selected_position=None,
        account={
            "total_order_count": 0,
            "pending_order_count": 0,
            "total_isolated_order_count": 0,
            "cross_asset_value": "0",
            "cross_initial_margin_requirement": "0",
        },
    )
    assert absent.status == "POSITION_ROW_ABSENT"
    assert absent.as_dict()["selected_position"]["present"] is False
    assert absent.as_dict()["selected_position"]["margin_mode"]["present"] is False

    unknown_mode = complete_account_evidence(selected_position={"margin_mode": 9})
    assert unknown_mode.status == "INCOMPLETE"
    assert unknown_mode.as_dict()["selected_position"]["margin_mode"]["label"] == "unknown"


def test_margin_evidence_rejects_invalid_and_conflicting_values_without_raw_escape():
    evidence = complete_account_evidence(
        selected_position={
            "margin_mode": -1,
            "initial_margin_fraction": "NaN",
            "allocated_margin": "-1",
            "open_order_count": True,
            "pending_order_count": 2,
            "position_tied_order_count": 3,
            "secret_field": "secret-sentinel-must-not-escape",
        },
        account={
            "total_order_count": 1,
            "pending_order_count": 2,
            "total_isolated_order_count": 3,
            "cross_asset_value": "not-a-number",
            "cross_initial_margin_requirement": "Infinity",
        },
    )
    assert evidence.status == "INVALID"
    assert {
        "margin_mode",
        "initial_margin_fraction",
        "allocated_margin",
        "open_order_count",
        "account_order_counts",
        "cross_asset_value",
        "cross_initial_margin_requirement",
    }.issubset(set(evidence.invalid_fields))
    selected_conflict = complete_account_evidence(
        selected_position={"open_order_count": 1, "pending_order_count": 2}
    )
    assert "selected_order_counts" in selected_conflict.invalid_fields
    selected_account_conflict = complete_account_evidence(
        selected_position={"open_order_count": 1, "pending_order_count": 1},
        account={"total_order_count": 1, "pending_order_count": 0},
    )
    assert "account_order_counts" in selected_account_conflict.invalid_fields
    serialized = json.dumps(evidence.as_dict(), sort_keys=True)
    assert "secret-sentinel-must-not-escape" not in serialized
    assert evidence.as_dict()["selected_position"]["initial_margin_fraction"]["raw"] is None

    scoped_pending = complete_account_evidence(
        selected_position={"pending_order_count": "malformed"},
        account={"pending_order_count": 1, "total_order_count": 2},
    )
    assert "selected_pending_order_count" in scoped_pending.invalid_fields
    assert scoped_pending.as_dict()["selected_position"]["pending_order_count"]["raw"] is None
    assert scoped_pending.as_dict()["account"]["pending_order_count"]["raw"] == 1


def test_market_margin_evidence_preserves_raw_values_and_marks_units_unverified():
    evidence = ReadinessMarketMarginEvidence.from_response(
        {
            "default_initial_margin_fraction": 500,
            "min_initial_margin_fraction": 100,
            "secret_field": "secret-market-sentinel-must-not-escape",
        },
        market_id=7,
        symbol="btc",
        observed_at=NOW,
        source="synthetic orderBookDetails response",
        sdk_version="1.1.2",
    )
    assert evidence.status == "OBSERVED"
    payload = evidence.as_dict()
    assert payload["default_initial_margin_fraction"]["raw"] == 500
    assert payload["default_initial_margin_fraction"]["units"] == "unverified"
    assert payload["minimum_initial_margin_fraction"]["raw"] == 100
    assert "secret-market-sentinel-must-not-escape" not in json.dumps(payload)

    invalid = ReadinessMarketMarginEvidence.from_response(
        {"default_initial_margin_fraction": -1, "min_initial_margin_fraction": float("nan")},
        market_id=7,
        symbol="BTC",
        observed_at=NOW,
    )
    assert invalid.status == "INVALID"
    assert set(invalid.invalid_fields) == {"default_initial_margin_fraction", "minimum_initial_margin_fraction"}

    equivalent_aliases = ReadinessMarketMarginEvidence.from_response(
        {
            "default_initial_margin_fraction": 500,
            "minimum_initial_margin_fraction": 100,
            "min_initial_margin_fraction": 100,
        },
        market_id=7,
        symbol="BTC",
        observed_at=NOW,
    )
    assert equivalent_aliases.status == "OBSERVED"
    assert equivalent_aliases.as_dict()["minimum_initial_margin_fraction"]["raw"] == 100

    malformed_alias = ReadinessMarketMarginEvidence.from_response(
        {
            "default_initial_margin_fraction": 500,
            "minimum_initial_margin_fraction": 100,
            "min_initial_margin_fraction": "malformed",
        },
        market_id=7,
        symbol="BTC",
        observed_at=NOW,
    )
    assert malformed_alias.status == "INVALID"
    assert malformed_alias.as_dict()["minimum_initial_margin_fraction"]["raw"] is None

    conflicting = ReadinessMarketMarginEvidence.from_response(
        {"default_initial_margin_fraction": 100, "min_initial_margin_fraction": 200},
        market_id=7,
        symbol="BTC",
        observed_at=NOW,
    )
    assert conflicting.status == "INVALID"
    assert conflicting.invalid_fields == ("margin_fractions",)

    unavailable = ReadinessMarketMarginEvidence(
        market_id=7,
        symbol="BTC",
        observed_at=NOW,
        default_initial_margin_fraction=None,
        minimum_initial_margin_fraction=None,
        source="not returned",
    )
    assert unavailable.status == "UNAVAILABLE"


@pytest.mark.asyncio
async def test_real_read_adapter_retains_whitelisted_margin_evidence_and_provenance():
    client = sdk_client()
    result = await run_readiness(readiness_config(), client, now=NOW)
    payload = result.as_dict()
    assert result.outcome == "UNKNOWN"
    assert payload["market"]["margin_evidence"]["status"] == "OBSERVED"
    assert payload["market"]["margin_evidence"]["binding"] == {"market_id": 7, "symbol": "BTC"}
    assert payload["market"]["margin_evidence"]["default_initial_margin_fraction"]["raw"] == 500
    assert payload["market"]["margin_evidence"]["provenance"]["sdk_version"] == "1.1.2"
    source = payload["accounts"]["source"]["margin_evidence"]
    assert source["status"] == "OBSERVED"
    assert source["binding"] == {
        "account_index": 11,
        "market_id": 7,
        "source_identity": "synthetic-account-11",
    }
    assert source["selected_position"]["present"] is True
    assert source["selected_position"]["margin_mode"]["raw"] == 0
    assert source["selected_position"]["margin_mode"]["label"] == "cross"
    assert source["selected_position"]["initial_margin_fraction"]["raw"] == "0.1000"
    assert source["account"]["total_order_count"]["raw"] == 2
    assert source["account"]["pending_order_count"]["raw"] == 1
    assert source["account"]["cross_asset_value"]["raw"] == "100"
    assert source["provenance"]["observed_at"] == NOW
    serialized = json.dumps(payload, sort_keys=True)
    assert "secret-sentinel-must-not-escape" not in serialized
    assert "synthetic-auth-token" not in serialized


@pytest.mark.asyncio
async def test_invalid_market_evidence_is_reported_without_readiness_proof(monkeypatch):
    monkeypatch.setattr(
        FakeOrderApi,
        "market_details",
        {
            "symbol": "BTC",
            "market_id": 7,
            "market_type": "perp",
            "status": "active",
            "min_base_amount": "0.10",
            "min_quote_amount": "10",
            "supported_size_decimals": 2,
            "supported_price_decimals": 1,
            "default_initial_margin_fraction": 100,
            "min_initial_margin_fraction": 200,
        },
    )
    result = await run_readiness(readiness_config(), sdk_client(), now=NOW)
    check = next(item for item in result.checks if item.name == "market_margin_evidence")
    assert check.status == "UNKNOWN"
    assert check.code == "MARKET_MARGIN_EVIDENCE_INVALID"
    assert result.outcome == "UNKNOWN"


@pytest.mark.asyncio
async def test_conflicting_market_aliases_are_invalid_through_read_adapter(monkeypatch):
    monkeypatch.setattr(
        FakeOrderApi,
        "market_details",
        {
            "symbol": "BTC",
            "market_id": 7,
            "market_type": "perp",
            "status": "active",
            "min_base_amount": "0.10",
            "min_quote_amount": "10",
            "supported_size_decimals": 2,
            "supported_price_decimals": 1,
            "default_initial_margin_fraction": 500,
            "minimum_initial_margin_fraction": 100,
            "min_initial_margin_fraction": 900,
        },
    )
    result = await run_readiness(readiness_config(), sdk_client(), now=NOW)
    evidence = result.as_dict()["market"]["margin_evidence"]
    assert evidence["status"] == "INVALID"
    assert evidence["invalid_fields"] == ["minimum_initial_margin_fraction"]
    assert evidence["minimum_initial_margin_fraction"]["raw"] is None
    check = next(item for item in result.checks if item.name == "market_margin_evidence")
    assert check.status == "UNKNOWN"
    assert check.code == "MARKET_MARGIN_EVIDENCE_INVALID"


@pytest.mark.asyncio
async def test_absent_selected_row_is_distinct_and_does_not_prove_flatness_or_readiness():
    client = sdk_client(
        {
            11: account_response(11, positions=[]),
            22: account_response(22, positions=[]),
        }
    )
    result = await run_readiness(readiness_config(), client, now=NOW)
    source_check = next(item for item in result.checks if item.name == "source_account_position")
    assert result.outcome == "UNKNOWN"
    assert source_check.status == "UNKNOWN"
    assert source_check.code == "POSITION_ROW_ABSENT"
    payload = result.as_dict()
    source = payload["accounts"]["source"]["margin_evidence"]
    assert source["status"] == "POSITION_ROW_ABSENT"
    assert source["selected_position"]["present"] is False
    assert source["selected_position"]["margin_mode"]["present"] is False


@pytest.mark.asyncio
async def test_other_market_orders_are_retained_without_claiming_selected_market_is_empty():
    client = sdk_client(
        {
            11: account_with_other_market_row(11),
            22: account_with_other_market_row(22),
        }
    )
    result = await run_readiness(readiness_config(), client, now=NOW)
    payload = result.as_dict()
    selected_orders = next(item for item in result.checks if item.name == "source_account_active_orders")
    assert selected_orders.code == "NO_ACTIVE_ORDERS"
    account_evidence = payload["accounts"]["source"]["margin_evidence"]["account"]
    assert account_evidence["total_order_count"]["raw"] == 2
    assert account_evidence["pending_order_count"]["raw"] == 1
    assert "NO_ACCOUNT_WIDE_ORDERS" not in json.dumps(payload)
    assert payload["accounts"]["source"]["margin_evidence"]["status"] == "OBSERVED"


@pytest.mark.asyncio
async def test_other_market_row_lower_bound_conflict_is_not_reported_as_no_orders():
    client = sdk_client(
        {
            11: account_with_other_market_row(
                11,
                total_order_count=0,
                pending_order_count=0,
                total_isolated_order_count=0,
            ),
            22: account_response(22),
        }
    )
    result = await run_readiness(readiness_config(), client, now=NOW)
    source_evidence = result.as_dict()["accounts"]["source"]["margin_evidence"]
    assert source_evidence["status"] == "INVALID"
    assert "account_order_counts" in source_evidence["invalid_fields"]
    assert source_evidence["account"]["total_order_count"]["raw"] == 0
    assert source_evidence["account"]["pending_order_count"]["raw"] == 0
    check = next(item for item in result.checks if item.name == "source_account_margin_evidence")
    assert check.status == "UNKNOWN"
    assert check.code == "MARGIN_EVIDENCE_INVALID"
    assert "NO_ACCOUNT_WIDE_ORDERS" not in json.dumps(result.as_dict())


def test_evidence_identity_must_match_snapshot_and_market_metadata():
    evidence = complete_account_evidence()
    with pytest.raises(ValueError, match="identity"):
        AccountSnapshot.from_mapping(
            {
                "account_index": 22,
                "market_id": 7,
                "signed_position": "0",
                "active_orders": [],
                "observed_at": NOW,
                "authorized": True,
                "ready": True,
                "margin_available": "100",
                "margin_required": "1",
                "source_identity": "synthetic-account-22",
                "margin_evidence": evidence,
            }
        )

    stale_market_evidence = ReadinessMarketMarginEvidence.from_response(
        {"default_initial_margin_fraction": 500, "min_initial_margin_fraction": 100},
        market_id=7,
        symbol="BTC",
        observed_at=NOW + 1,
    )
    with pytest.raises(ValueError, match="identity"):
        ReadinessMarketMetadata(
            market_id=7,
            symbol="BTC",
            status="active",
            price_decimals=1,
            size_decimals=2,
            minimum_base_amount=Decimal("0.10"),
            minimum_quote_amount=Decimal("10"),
            observed_at=NOW,
            margin_evidence=stale_market_evidence,
        )
