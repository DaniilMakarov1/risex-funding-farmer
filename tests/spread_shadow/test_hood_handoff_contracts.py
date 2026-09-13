from decimal import Decimal
from dataclasses import replace

import pytest

from risex_spread_shadow.hood_handoff import (
    ContractError,
    Direction,
    HandoffConfig,
    MarketMetadata,
    decimal_to_integer,
)


def metadata(**overrides):
    value = {
        "market_id": 7,
        "symbol": "HOOD",
        "status": "active",
        "price_decimals": 2,
        "size_decimals": 3,
        "minimum_base_amount": "0.001",
        "minimum_quote_amount": "1",
        "source_fee_rate": "0.001",
        "receiver_fee_rate": "0.001",
        "observed_at": 1_000.0,
        "margin_evidence": "accountLimits fixture",
    }
    value.update(overrides)
    return MarketMetadata.from_mapping(value)


def config(**overrides):
    value = {
        "market_id": 7,
        "direction": Direction.LONG,
        "quantity": Decimal("0.125"),
        "source_limit_price": Decimal("100.25"),
        "receiver_worst_price": Decimal("101.25"),
        "receiver_price_cap": Decimal("101.25"),
        "max_gross_notional": Decimal("200"),
        "source_fee_budget": Decimal("1"),
        "receiver_fee_budget": Decimal("1"),
        "freshness_seconds": 10,
        "request_timeout_seconds": 1,
        "order_timeout_seconds": 1,
        "reconcile_timeout_seconds": 1,
        "poll_interval_seconds": 0.001,
        "max_poll_count": 3,
        "source_order_lifetime_seconds": 300,
        "client_order_prefix": "test-hcr",
        "journal_path": "/tmp/hcr-journal.jsonl",
        "api_base_url": "https://mainnet.zklighter.elliot.ai",
        "api_key_index": 4,
        "chain_id": 304,
    }
    value.update(overrides)
    return HandoffConfig(**value)


def test_integer_conversion_rejects_off_grid_without_rounding():
    assert decimal_to_integer(Decimal("1.25"), 2, "price") == 125
    with pytest.raises(ContractError, match="off the 2-decimal grid"):
        decimal_to_integer(Decimal("1.251"), 2, "price")


def test_config_rejects_non_hood_and_short_order_lifetime():
    with pytest.raises(ContractError, match="only HOOD"):
        config(market_symbol="BTC")
    with pytest.raises(ContractError, match="at least 300"):
        config(source_order_lifetime_seconds=299)


def test_future_evidence_is_blocked():
    from risex_spread_shadow.hood_handoff import AccountSnapshot

    source = AccountSnapshot.from_mapping(
        {
            "account_index": 1,
            "market_id": 7,
            "signed_position": "1",
            "active_orders": [],
            "observed_at": 2_000,
            "authorized": True,
            "ready": True,
            "margin_available": "100",
            "margin_required": "1",
            "incremental_margin_required": "1",
            "incremental_margin_evidence": "fixture planned delta",
            "fee_rate": "0.001",
            "source_identity": "source",
        }
    )
    receiver = AccountSnapshot.from_mapping(
        {
            "account_index": 2,
            "market_id": 7,
            "signed_position": "0",
            "active_orders": [],
            "observed_at": 1_000,
            "authorized": True,
            "ready": True,
            "margin_available": "100",
            "margin_required": "1",
            "incremental_margin_required": "1",
            "incremental_margin_evidence": "fixture planned delta",
            "fee_rate": "0.001",
            "source_identity": "receiver",
        }
    )
    with pytest.raises(Exception, match="future"):
        config().validate_against(metadata(), source, receiver, 1_005)


def test_opposite_receiver_fails_but_legacy_cap_fields_are_not_admission_gates():
    from risex_spread_shadow.hood_handoff import AccountSnapshot

    source = AccountSnapshot.from_mapping(
        {
            "account_index": 1,
            "market_id": 7,
            "signed_position": "1",
            "active_orders": [],
            "observed_at": 1_000,
            "authorized": True,
            "ready": True,
            "margin_available": "100",
            "margin_required": "1",
            "incremental_margin_required": "1",
            "incremental_margin_evidence": "fixture planned delta",
            "fee_rate": "0.001",
            "source_identity": "0xsource",
        }
    )
    receiver = AccountSnapshot.from_mapping(
        {
            "account_index": 2,
            "market_id": 7,
            "signed_position": "-0.1",
            "active_orders": [],
            "observed_at": 1_000,
            "authorized": True,
            "ready": True,
            "margin_available": "100",
            "margin_required": "1",
            "incremental_margin_required": "1",
            "incremental_margin_evidence": "fixture planned delta",
            "fee_rate": "0.001",
            "source_identity": "0xreceiver",
        }
    )
    with pytest.raises(Exception, match="opposite HOOD exposure"):
        config().validate_against(metadata(), source, receiver, 1_005)
    assert config(
        source_fee_budget=Decimal("0.001"),
        receiver_fee_budget=Decimal("0.001"),
        max_gross_notional=Decimal("0.001"),
        receiver_price_cap=Decimal("0.001"),
    ).validate_against(metadata(), source, replace(receiver, signed_position=Decimal("0")), 1_005) is None
