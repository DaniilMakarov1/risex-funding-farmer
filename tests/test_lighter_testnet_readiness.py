from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from risex_farmer import lighter_testnet_readiness as lighter


ADDRESS = lighter.EXPECTED_L1_ADDRESS
ACCOUNT_INDEX = 17
API_KEY_INDEX = lighter.FIRST_USER_API_KEY_INDEX


def protected_wallet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> lighter.WalletMetadata:
    directory = tmp_path / "lighter-testnet"
    directory.mkdir(mode=0o700)
    directory.chmod(0o700)
    path = directory / "wallet-private-key"
    path.write_bytes(b"synthetic-wallet-material-not-read")
    path.chmod(0o600)
    monkeypatch.setattr(lighter, "TESTNET_WALLET_PATH", path)
    return lighter.inspect_protected_wallet()


def ready_wallet() -> lighter.WalletMetadata:
    return lighter.WalletMetadata(
        path=str(lighter.TESTNET_WALLET_PATH),
        address=ADDRESS,
        present=True,
        protected=True,
        reason="PROTECTED_FIXED_WALLET_METADATA_VALID",
    )


def account_discovery_response() -> dict:
    return {
        "code": 200,
        "l1_address": ADDRESS,
        "sub_accounts": [{"index": ACCOUNT_INDEX, "l1_address": ADDRESS}],
    }


def detailed_account_response(**changes) -> dict:
    account = {
        "code": 200,
        "account_type": 0,
        "account_trading_mode": 1,
        "index": ACCOUNT_INDEX,
        "l1_address": ADDRESS,
        "cancel_all_time": 0,
        "total_order_count": 0,
        "total_isolated_order_count": 0,
        "pending_order_count": 0,
        "available_balance": "100.000000",
        "status": 1,
        "collateral": "100.000000",
        "account_index": ACCOUNT_INDEX,
        "pending_unlocks": [],
        "positions": [],
        "assets": [
            {
                "symbol": "USDC",
                "asset_id": 1,
                "balance": "100.000000",
                "locked_balance": "0.000000",
                "margin_balance": "100.000000",
                "margin_mode": "enabled",
                "multiplier": "1.000000000000000000",
            }
        ],
        "total_asset_value": "100.000000",
        "cross_asset_value": "100.000000",
        "pool_info": None,
        "shares": [],
    }
    account.update(changes)
    return {"code": 200, "total": 1, "accounts": [account]}


def limits_response(**changes) -> dict:
    payload = {
        "code": 200,
        "max_llp_percentage": 25,
        "user_tier": "std",
        "can_create_public_pool": True,
        "max_llp_amount": "1000000",
        "current_maker_fee_tick": 0,
        "current_taker_fee_tick": 0,
        "effective_lit_stakes": "0",
        "leased_lit": "0",
        "user_tier_name": "standard",
    }
    payload.update(changes)
    return payload


def all_ready_responses() -> dict[str, dict]:
    return {
        "/api/v1/accountsByL1Address": account_discovery_response(),
        "/api/v1/apikeys": {
            "code": 200,
            "api_keys": [
                {
                    "account_index": ACCOUNT_INDEX,
                    "api_key_index": API_KEY_INDEX,
                    "nonce": 0,
                    "public_key": "0x" + "11" * 32,
                    "transaction_time": 1,
                }
            ],
        },
        "/api/v1/account": detailed_account_response(),
        "/api/v1/accountLimits": limits_response(),
        "/api/v1/accountActiveOrders": {"code": 200, "orders": []},
        "/api/v1/trades": {"code": 200, "trades": []},
        "/api/v1/positionFunding": {"code": 200, "position_fundings": []},
    }


def run_ready(responses=None, *, wallet=None, authorization_loader=None):
    transport = lighter.FixtureReadTransport(responses or all_ready_responses())
    result = lighter.run_level_b(
        transport,
        wallet=wallet or ready_wallet(),
        authorization_loader=authorization_loader
        or (lambda: lighter.AuthorizationCapability("fixture-auth", ACCOUNT_INDEX, API_KEY_INDEX)),
    )
    return result, transport


def test_protected_wallet_inspection_is_metadata_only(tmp_path, monkeypatch):
    wallet = protected_wallet(tmp_path, monkeypatch)

    def forbidden(*args, **kwargs):
        raise AssertionError("wallet contents must not be opened")

    monkeypatch.setattr(Path, "read_bytes", forbidden)
    monkeypatch.setattr(Path, "read_text", forbidden)
    assert wallet.ready
    assert wallet.address == ADDRESS
    assert wallet.reason == "PROTECTED_FIXED_WALLET_METADATA_VALID"


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ("mode", "WALLET_FILE_NOT_PRIVATE"),
        ("symlink", "WALLET_FILE_NOT_REGULAR"),
        ("hardlink", "WALLET_FILE_LINK_COUNT_INVALID"),
    ],
)
def test_protected_wallet_rejects_unsafe_file_metadata(tmp_path, monkeypatch, change, reason):
    wallet = protected_wallet(tmp_path, monkeypatch)
    path = Path(wallet.path)
    if change == "mode":
        path.chmod(0o640)
    elif change == "symlink":
        replacement = path.with_name("wallet-target")
        path.rename(replacement)
        path.symlink_to(replacement)
    else:
        os.link(path, path.with_name("wallet-hardlink"))
    assert not lighter.inspect_protected_wallet().ready
    assert lighter.inspect_protected_wallet().reason == reason


def test_fixed_path_and_public_identity_are_exact(tmp_path, monkeypatch):
    wallet = protected_wallet(tmp_path, monkeypatch)
    other = tmp_path / "other-wallet"
    other.write_bytes(b"synthetic")
    assert lighter.inspect_protected_wallet(other).reason == "FIXED_WALLET_PATH_REQUIRED"
    bypass = lighter.WalletMetadata(
        path=str(other),
        address=ADDRESS,
        present=True,
        protected=True,
        reason="PROTECTED_FIXED_WALLET_METADATA_VALID",
    )
    result = lighter.run_level_b(
        lighter.FixtureReadTransport({}),
        wallet=bypass,
        authorization_loader=lambda: "fixture-auth",
    )
    assert result.reason == "FIXED_WALLET_PATH_REQUIRED"
    assert lighter.canonical_testnet_address(ADDRESS.lower()) == ADDRESS
    with pytest.raises(ValueError):
        lighter.canonical_testnet_address("0x" + "22" * 20)


def test_creation_and_api_key_plans_are_blocked_and_non_executable():
    creation = lighter.build_account_creation_plan(ready_wallet())
    assert creation.reason == lighter.ACCOUNT_CREATION_BLOCKER
    assert not creation.invocation_allowed
    assert not creation.write_capable

    plan = lighter.build_api_key_provisioning_plan(ACCOUNT_INDEX)
    assert plan.api_key_index == 4
    assert plan.reserved_indices == (0, 1, 2, 3)
    assert "ChangePubKey" in plan.association
    assert not plan.invocation_allowed
    assert not plan.write_capable


def test_account_not_found_is_identity_block_and_never_requests_auth():
    transport = lighter.FixtureReadTransport(
        {
            "/api/v1/accountsByL1Address": {
                "status_code": 400,
                "body": {"code": lighter.ACCOUNT_NOT_FOUND_CODE, "message": "account not found"},
            }
        }
    )
    called = False

    def load_auth():
        nonlocal called
        called = True
        return "fixture-auth"

    result = lighter.run_level_b(transport, wallet=ready_wallet(), authorization_loader=load_auth)
    assert result.status == "BLOCKED"
    assert result.failure_class == "IDENTITY"
    assert result.reason == "ACCOUNT_NOT_FOUND"
    assert not called
    assert len(transport.requests) == 1
    assert "account not found" not in result.evidence()


def test_missing_api_key_stops_after_public_discovery():
    transport = lighter.FixtureReadTransport({"/api/v1/accountsByL1Address": account_discovery_response()})
    result = lighter.run_level_b(transport, wallet=ready_wallet())
    assert result.status == "BLOCKED"
    assert result.failure_class == "AUTH"
    assert result.reason == "API_KEY_NOT_PROVISIONED"
    assert result.identity_verified
    assert len(transport.requests) == 1


@pytest.mark.parametrize(
    "capability",
    [
        lambda: "fixture-auth",
        lambda: lighter.AuthorizationCapability("fixture-auth", ACCOUNT_INDEX + 1, API_KEY_INDEX),
        lambda: lighter.AuthorizationCapability("fixture-auth", ACCOUNT_INDEX, API_KEY_INDEX + 1),
    ],
)
def test_unbound_authorization_cannot_start_private_reads(capability):
    transport = lighter.FixtureReadTransport({"/api/v1/accountsByL1Address": account_discovery_response()})
    result = lighter.run_level_b(
        transport,
        wallet=ready_wallet(),
        authorization_loader=capability,
    )
    assert result.status == "BLOCKED"
    assert result.failure_class in {"AUTH", "IDENTITY"}
    assert result.reason in {"AUTHORIZATION_IDENTITY_UNPROVEN", "AUTHORIZATION_IDENTITY_MISMATCH"}
    assert len(transport.requests) == 1


def test_ready_gate_reads_every_account_safety_surface_and_stays_read_only():
    result, transport = run_ready()
    assert result.status == "READY"
    assert result.failure_class == "NONE"
    assert result.identity_verified
    assert result.authorization_identity_verified
    assert result.api_key_verified
    assert result.collateral_positive
    assert result.fees_verified
    assert result.active_orders_zero
    assert result.positions_flat
    assert result.unrelated_state_clear
    assert result.trades_read and result.funding_history_read
    assert result.write_capable is False
    assert result.write_authority == lighter.NO_TESTNET_WRITE_AUTHORITY
    assert result.requests == 7
    assert result.retries == 0
    assert [request.method for request in transport.requests] == ["GET"] * 7
    assert all(request.url.startswith(lighter.TESTNET_API_URL) for request in transport.requests)
    assert "fixture-auth" not in result.evidence()


def test_transport_retries_once_but_schema_does_not_retry():
    responses = all_ready_responses()
    responses["/api/v1/accountsByL1Address"] = [lighter.PrematureResponse(), account_discovery_response()]
    result, transport = run_ready(responses)
    assert result.status == "READY"
    assert result.retries == 1
    assert result.requests == 8
    assert len(transport.requests) == 8

    malformed = all_ready_responses()
    malformed["/api/v1/accountsByL1Address"] = {"code": 200}
    result, transport = run_ready(malformed)
    assert result.status == "BLOCKED"
    assert result.failure_class == "SCHEMA"
    assert result.retries == 0
    assert result.requests == 1
    assert len(transport.requests) == 1


def test_trade_and_funding_history_pages_are_validated_and_completed():
    trade = {
        "trade_id": 1,
        "trade_id_str": "1",
        "tx_hash": "0x" + "aa" * 32,
        "type": "trade",
        "market_id": 1,
        "size": "0.1",
        "price": "100",
        "usd_amount": "10",
        "ask_id": 11,
        "bid_id": 12,
        "ask_client_id": 21,
        "ask_client_id_str": "21",
        "bid_client_id": 22,
        "bid_client_id_str": "22",
        "ask_id_str": "11",
        "bid_id_str": "12",
        "ask_account_id": ACCOUNT_INDEX,
        "bid_account_id": 18,
        "is_maker_ask": True,
        "block_height": 1,
        "timestamp": 1,
        "taker_position_size_before": "0",
        "taker_entry_quote_before": "0",
        "taker_initial_margin_fraction_before": 0,
        "taker_position_sign_changed": False,
        "maker_position_size_before": "0",
        "maker_entry_quote_before": "0",
        "maker_initial_margin_fraction_before": 0,
        "maker_position_sign_changed": False,
        "transaction_time": 1,
        "ask_account_pnl": "0",
        "bid_account_pnl": "0",
        "integrator_taker_fee": 0,
        "integrator_taker_fee_collector_index": 0,
        "integrator_maker_fee": 0,
        "integrator_maker_fee_collector_index": 0,
        "taker_allocated_margin_usdc_before": 0,
        "taker_allocated_margin_usdc_after": 0,
        "maker_allocated_margin_usdc_before": 0,
        "maker_allocated_margin_usdc_after": 0,
        "ask_order_version": 1,
        "bid_order_version": 1,
    }
    funding = {
        "timestamp": 1,
        "market_id": 1,
        "funding_id": 1,
        "change": "0",
        "discount": "0",
        "rate": "0",
        "position_size": "0.1",
        "position_side": "long",
    }
    responses = all_ready_responses()
    responses["/api/v1/trades"] = [
        {"code": 200, "trades": [trade], "next_cursor": "trade-next"},
        {"code": 200, "trades": []},
    ]
    responses["/api/v1/positionFunding"] = [
        {"code": 200, "position_fundings": [funding], "next_cursor": "funding-next"},
        {"code": 200, "position_fundings": []},
    ]
    result, _ = run_ready(responses)
    assert result.status == "READY"
    assert result.trade_count == 1
    assert result.funding_count == 1


def test_fixture_loader_rejects_sensitive_fields(tmp_path):
    fixture = tmp_path / "fixture.json"
    fixture.write_text(
        json.dumps({"authorization_available": False, "api_key": "not-real", "responses": {}}),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="FIXTURE_CONTAINS_SENSITIVE_FIELD"):
        lighter._load_fixture(fixture)


def test_active_regular_or_trigger_order_blocks_level_b():
    responses = all_ready_responses()
    responses["/api/v1/accountActiveOrders"] = {
        "code": 200,
        "orders": [
            {"owner_account_index": ACCOUNT_INDEX, "type": "limit"},
            {"owner_account_index": ACCOUNT_INDEX, "type": "stop-loss"},
        ],
    }
    result, _ = run_ready(responses)
    assert result.status == "BLOCKED"
    assert result.failure_class == "SAFETY"
    assert result.reason == "ACTIVE_ORDERS_PRESENT"
    assert result.regular_order_count == 1
    assert result.trigger_order_count == 1


def test_unrelated_asset_and_nonflat_position_block_level_b():
    responses = all_ready_responses()
    account = detailed_account_response()
    account["accounts"][0]["assets"].append(
        {
            "symbol": "LIT",
            "asset_id": 2,
            "balance": "1",
            "locked_balance": "0",
            "margin_balance": "1",
            "margin_mode": "enabled",
            "multiplier": "1",
        }
    )
    responses["/api/v1/account"] = account
    result, _ = run_ready(responses)
    assert result.status == "BLOCKED"
    assert result.reason == "UNRELATED_ASSET_BALANCE_PRESENT"


def test_market_contract_and_two_sided_book_are_strict_read_parsers():
    market = lighter.parse_market_contract(
        {
            "code": 200,
            "order_book_details": [
                {
                    "market_id": 1,
                    "symbol": "ETH",
                    "market_type": "perp",
                    "min_base_amount": "0.001",
                    "min_quote_amount": "10",
                    "size_decimals": 3,
                    "price_decimals": 1,
                    "supported_quote_decimals": 6,
                    "status": "active",
                    "market_config": {"force_reduce_only": False},
                }
            ],
        },
        market_id=1,
    )
    assert market.min_base_amount == lighter.Decimal("0.001")
    book = lighter.parse_two_sided_liquidity(
        {
            "code": 200,
            "bids": [{"price": "99", "remaining_base_amount": "1"}],
            "asks": [{"price": "101", "remaining_base_amount": "1"}],
        },
        market_id=1,
    )
    assert book.two_sided and book.best_bid < book.best_ask


def test_evidence_is_sanitized_json():
    result, _ = run_ready()
    decoded = json.loads(result.evidence())
    assert decoded["status"] == "READY"
    assert "fixture-auth" not in result.evidence().lower()
    assert "header_value" not in result.evidence().lower()
    assert "private" not in result.evidence().lower()
