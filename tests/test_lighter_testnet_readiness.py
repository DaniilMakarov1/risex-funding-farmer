from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from risex_farmer import lighter_testnet_readiness as lighter


ADDRESS = lighter.EXPECTED_L1_ADDRESS
ACCOUNT_INDEX = 17
API_KEY_INDEX = lighter.FIRST_USER_API_KEY_INDEX
EXPECTED_API_PUBLIC_KEY = "0x" + "11" * 32


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
        "available_balance": "10000.000000",
        "margin_balance": "10000.000000",
        "status": 0,
        "collateral": "10000.000000",
        "account_index": ACCOUNT_INDEX,
        "pending_unlocks": [],
        "positions": [],
        "assets": [
            {
                "symbol": "ETH",
                "asset_id": 1,
                "balance": "3",
                "locked_balance": "0",
                "margin_balance": "0",
                "margin_mode": "disabled",
                "multiplier": "1.000000000000000000",
            },
            {
                "symbol": "LIT",
                "asset_id": 2,
                "balance": "1000000",
                "locked_balance": "0",
                "margin_balance": "0",
                "margin_mode": "disabled",
                "multiplier": "1.000000000000000000",
            },
            {
                "symbol": "USDC",
                "asset_id": 3,
                "balance": "0",
                "locked_balance": "0",
                "margin_balance": "10000",
                "margin_mode": "enabled",
                "multiplier": "1.000000000000000000",
            }
        ],
        "total_asset_value": "10000.000000",
        "cross_asset_value": "10000.000000",
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
                    "public_key": EXPECTED_API_PUBLIC_KEY,
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


def run_ready(
    responses=None,
    *,
    wallet=None,
    authorization_loader=None,
    expected_api_public_key=EXPECTED_API_PUBLIC_KEY,
    expected_account_index=None,
):
    transport = lighter.FixtureReadTransport(responses or all_ready_responses())
    result = lighter.run_level_b(
        transport,
        wallet=wallet or ready_wallet(),
        expected_account_index=expected_account_index,
        expected_api_public_key=expected_api_public_key,
        authorization_loader=authorization_loader
        or (lambda: lighter.AuthorizationCapability("fixture-auth", ACCOUNT_INDEX, API_KEY_INDEX)),
    )
    return result, transport


FAUCET_ACCOUNT_INDEX = 202


def official_faucet_bootstrap_responses() -> dict[str, dict]:
    responses = all_ready_responses()
    responses["/api/v1/accountsByL1Address"] = {
        "code": 200,
        "l1_address": ADDRESS,
        "sub_accounts": [
            {"index": FAUCET_ACCOUNT_INDEX, "l1_address": ADDRESS},
        ],
    }
    account = detailed_account_response()
    account["accounts"][0]["account_index"] = FAUCET_ACCOUNT_INDEX
    account["accounts"][0]["index"] = FAUCET_ACCOUNT_INDEX
    responses["/api/v1/account"] = account
    responses["/api/v1/apikeys"]["api_keys"][0]["account_index"] = FAUCET_ACCOUNT_INDEX
    return responses


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
        expected_api_public_key=EXPECTED_API_PUBLIC_KEY,
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
                "status_code": 200,
                "body": {"code": lighter.ACCOUNT_NOT_FOUND_CODE, "message": "account not found"},
            }
        }
    )
    called = False

    def load_auth():
        nonlocal called
        called = True
        return "fixture-auth"

    result = lighter.run_level_b(
        transport,
        wallet=ready_wallet(),
        expected_api_public_key=EXPECTED_API_PUBLIC_KEY,
        authorization_loader=load_auth,
    )
    assert result.status == "BLOCKED"
    assert result.failure_class == "IDENTITY"
    assert result.reason == "ACCOUNT_NOT_FOUND"
    assert not called
    assert len(transport.requests) == 1
    assert "account not found" not in result.evidence()


def test_missing_api_key_stops_after_public_discovery():
    transport = lighter.FixtureReadTransport({"/api/v1/accountsByL1Address": account_discovery_response()})
    result = lighter.run_level_b(
        transport,
        wallet=ready_wallet(),
        expected_api_public_key=EXPECTED_API_PUBLIC_KEY,
    )
    assert result.status == "BLOCKED"
    assert result.failure_class == "AUTH"
    assert result.reason == "API_KEY_NOT_PROVISIONED"
    assert result.identity_verified
    assert len(transport.requests) == 1


def test_discovery_without_expected_index_rejects_multiple_distinct_subaccounts():
    responses = all_ready_responses()
    responses["/api/v1/accountsByL1Address"]["sub_accounts"] = [
        {"index": ACCOUNT_INDEX, "l1_address": ADDRESS},
        {"index": ACCOUNT_INDEX + 1, "l1_address": ADDRESS},
    ]
    result, transport = run_ready(responses)
    assert result.status == "BLOCKED"
    assert result.failure_class == "IDENTITY"
    assert result.reason == "DISCOVERY_ACCOUNT_INDEX_AMBIGUOUS"
    assert result.requests == 1
    assert len(transport.requests) == 1


def test_explicit_discovery_index_tolerates_other_named_subaccounts():
    responses = all_ready_responses()
    responses["/api/v1/accountsByL1Address"]["sub_accounts"] = [
        {"index": ACCOUNT_INDEX, "l1_address": ADDRESS},
        {"index": ACCOUNT_INDEX + 1, "l1_address": ADDRESS},
    ]
    result, _ = run_ready(responses, expected_account_index=ACCOUNT_INDEX)
    assert result.status == "READY"
    assert result.account_index == ACCOUNT_INDEX


def test_explicit_discovery_index_requires_exactly_one_matching_identity():
    responses = all_ready_responses()
    responses["/api/v1/accountsByL1Address"]["sub_accounts"] = [
        {"index": ACCOUNT_INDEX, "l1_address": ADDRESS},
        {"index": ACCOUNT_INDEX, "l1_address": ADDRESS},
    ]
    result, _ = run_ready(responses, expected_account_index=ACCOUNT_INDEX)
    assert result.status == "BLOCKED"
    assert result.failure_class == "IDENTITY"
    assert result.reason == "DISCOVERY_ACCOUNT_INDEX_AMBIGUOUS"
    assert result.requests == 1


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
        expected_api_public_key=EXPECTED_API_PUBLIC_KEY,
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
    assert result.api_key_public_key_verified
    assert result.collateral_positive
    assert result.fees_verified
    assert result.active_orders_zero
    assert result.positions_flat
    assert result.unrelated_state_clear
    assert result.trades_read and result.funding_history_read
    assert result.write_capable is False
    assert result.write_authority == lighter.NO_TESTNET_WRITE_AUTHORITY
    evidence = json.loads(result.evidence())
    assert evidence["bootstrap_asset_baseline_verified"] is True
    assert [asset["symbol"] for asset in evidence["accepted_bootstrap_assets"]] == [
        "ETH",
        "LIT",
        "USDC",
    ]
    assert evidence["accepted_bootstrap_assets"][-1]["balance"] == "0"
    assert evidence["accepted_bootstrap_assets"][-1]["margin_balance"] == "10000"
    assert result.requests == 7
    assert result.retries == 0
    assert [request.method for request in transport.requests] == ["GET"] * 7
    assert all(request.url.startswith(lighter.TESTNET_API_URL) for request in transport.requests)
    assert "fixture-auth" not in result.evidence()


def test_official_faucet_account_202_shape_accepts_zero_spot_quote_and_omitted_pool_info():
    result, _ = run_ready(
        official_faucet_bootstrap_responses(),
        expected_account_index=FAUCET_ACCOUNT_INDEX,
        authorization_loader=lambda: lighter.AuthorizationCapability(
            "fixture-auth", FAUCET_ACCOUNT_INDEX, API_KEY_INDEX
        ),
    )
    assert result.status == "READY"
    assert result.account_index == FAUCET_ACCOUNT_INDEX
    assert result.collateral_positive
    assert result.unrelated_state_clear
    evidence = json.loads(result.evidence())
    assert evidence["accepted_bootstrap_assets"] == [
        {
            "asset_id": 1,
            "balance": "3",
            "locked_balance": "0",
            "margin_balance": "0",
            "margin_mode": "disabled",
            "symbol": "ETH",
        },
        {
            "asset_id": 2,
            "balance": "1000000",
            "locked_balance": "0",
            "margin_balance": "0",
            "margin_mode": "disabled",
            "symbol": "LIT",
        },
        {
            "asset_id": 3,
            "balance": "0",
            "locked_balance": "0",
            "margin_balance": "10000",
            "margin_mode": "enabled",
            "symbol": "USDC",
        },
    ]


def test_api_public_key_matching_is_exact_but_prefix_and_case_normalized():
    expected = "0x" + "ab" * 32
    responses = all_ready_responses()
    responses["/api/v1/apikeys"]["api_keys"][0]["public_key"] = expected[2:].upper()
    result, _ = run_ready(responses, expected_api_public_key=expected)
    assert result.status == "READY"
    assert result.api_key_verified
    assert result.api_key_public_key_verified


def test_api_public_key_mismatch_blocks_auth_before_private_reads():
    result, transport = run_ready(expected_api_public_key="0x" + "22" * 32)
    assert result.status == "BLOCKED"
    assert result.failure_class == "AUTH"
    assert result.reason == "API_KEY_PUBLIC_KEY_MISMATCH"
    assert result.requests == 2
    assert not any(request.path == "/api/v1/account" for request in transport.requests)


def test_expected_api_public_key_is_required_and_validated_locally():
    result, transport = run_ready(expected_api_public_key=None)
    assert result.status == "BLOCKED"
    assert result.failure_class == "SAFETY"
    assert result.reason == "READINESS_CONFIGURATION_INVALID"
    assert result.requests == 0
    assert transport.requests == []


@pytest.mark.parametrize("mutation", ["unknown", "locked", "margin_enabled", "wrong_id", "missing"])
def test_bootstrap_asset_baseline_deviations_fail_closed(mutation):
    responses = official_faucet_bootstrap_responses()
    assets = responses["/api/v1/account"]["accounts"][0]["assets"]
    if mutation == "unknown":
        assets.append(
            {
                "symbol": "ABC",
                "asset_id": 9,
                "balance": "0",
                "locked_balance": "0",
                "margin_balance": "0",
                "margin_mode": "disabled",
                "multiplier": "1",
            }
        )
    elif mutation == "locked":
        assets[0]["locked_balance"] = "1"
    elif mutation == "margin_enabled":
        assets[1]["margin_mode"] = "enabled"
    elif mutation == "wrong_id":
        assets[0]["asset_id"] = 8
    else:
        assets.pop(1)
    result, _ = run_ready(
        responses,
        expected_account_index=FAUCET_ACCOUNT_INDEX,
        authorization_loader=lambda: lighter.AuthorizationCapability(
            "fixture-auth", FAUCET_ACCOUNT_INDEX, API_KEY_INDEX
        ),
    )
    assert result.status == "BLOCKED"
    assert result.failure_class == "SAFETY"
    assert result.reason == (
        "UNKNOWN_BOOTSTRAP_ASSET"
        if mutation == "unknown"
        else "BOOTSTRAP_ASSET_BASELINE_MISMATCH"
    )
    assert not result.unrelated_state_clear
    assert json.loads(result.evidence())["accepted_bootstrap_assets"] == []


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


def test_second_transport_failure_is_terminal_after_one_retry():
    responses = all_ready_responses()
    responses["/api/v1/accountsByL1Address"] = [
        lighter.PrematureResponse(),
        lighter.PrematureResponse(),
    ]
    result, transport = run_ready(responses)
    assert result.status == "BLOCKED"
    assert result.failure_class == "TRANSPORT"
    assert result.reason == "TRANSPORT_RETRY_EXHAUSTED"
    assert result.requests == 2
    assert result.retries == 1
    assert len(transport.requests) == 2


def test_non_200_text_body_is_http_without_decode_or_retry():
    responses = all_ready_responses()
    responses["/api/v1/accountsByL1Address"] = {
        "status_code": 500,
        "body": "upstream unavailable",
    }
    result, transport = run_ready(responses)
    assert result.status == "BLOCKED"
    assert result.failure_class == "HTTP"
    assert result.reason == "HTTP_STATUS_NON_200"
    assert result.requests == 1
    assert result.retries == 0
    assert len(transport.requests) == 1


def test_final_host_redirect_violation_is_safety_failure_without_retry():
    responses = all_ready_responses()
    responses["/api/v1/accountsByL1Address"] = {
        "status_code": 200,
        "body": account_discovery_response(),
        "final_url": "https://evil.example/api/v1/accountsByL1Address",
    }
    result, transport = run_ready(responses)
    assert result.status == "BLOCKED"
    assert result.failure_class == "SAFETY"
    assert result.reason == "FIXED_HOST_OR_REDIRECT_VIOLATION"
    assert result.requests == 1
    assert result.retries == 0
    assert len(transport.requests) == 1


def test_duplicate_json_keys_are_schema_failure_without_retry():
    responses = all_ready_responses()
    responses["/api/v1/accountsByL1Address"] = {
        "status_code": 200,
        "body": '{"code":200,"code":200,"l1_address":"' + ADDRESS + '\",'
        '"sub_accounts":[]}',
    }
    result, transport = run_ready(responses)
    assert result.status == "BLOCKED"
    assert result.failure_class == "SCHEMA"
    assert result.reason == "RESPONSE_SCHEMA_INVALID"
    assert result.requests == 1
    assert result.retries == 0
    assert len(transport.requests) == 1


def test_response_byte_cap_is_schema_failure_without_retry():
    responses = all_ready_responses()
    responses["/api/v1/accountsByL1Address"] = {
        "status_code": 200,
        "body": b"x" * (lighter.MAX_JSON_BYTES + 1),
    }
    result, transport = run_ready(responses)
    assert result.status == "BLOCKED"
    assert result.failure_class == "SCHEMA"
    assert result.reason == "RESPONSE_SCHEMA_INVALID"
    assert result.requests == 1
    assert result.retries == 0
    assert len(transport.requests) == 1


@pytest.mark.parametrize(
    ("path", "page", "reason"),
    [
        (
            "/api/v1/accountsByL1Address",
            {
                "code": 200,
                "l1_address": ADDRESS,
                "sub_accounts": [{"index": ACCOUNT_INDEX, "l1_address": ADDRESS}],
                "next_cursor": "same-cursor",
            },
            "DISCOVERY_CURSOR_REPEATED",
        ),
        (
            "/api/v1/trades",
            {"code": 200, "trades": [], "next_cursor": "same-cursor"},
            "TRADES_CURSOR_REPEATED",
        ),
        (
            "/api/v1/positionFunding",
            {"code": 200, "position_fundings": [], "next_cursor": "same-cursor"},
            "FUNDING_CURSOR_REPEATED",
        ),
    ],
)
def test_repeated_history_cursor_is_safety_failure(path, page, reason):
    responses = all_ready_responses()
    responses[path] = page
    result, transport = run_ready(responses)
    assert result.status == "BLOCKED"
    assert result.failure_class == "SAFETY"
    assert result.reason == reason
    assert result.retries == 0
    assert len(transport.requests) == result.requests


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


@pytest.mark.parametrize(
    "field",
    [
        "api_key",
        "mnemonic",
        "seed_phrase",
        "seedPhrase",
        "secret_key",
        "secret-key",
        "AuthorizationHeader",
        "authorization-header",
        "authorizationToken",
        "AUTHORIZATIONTOKEN",
        "AUTHORIZATION_TOKEN",
        "authorization-header-value",
        "walletPrivateKey",
    ],
)
def test_fixture_loader_rejects_normalized_sensitive_fields(tmp_path, field):
    fixture = tmp_path / "fixture.json"
    fixture.write_text(
        json.dumps({"authorization_available": False, field: "not-real", "responses": {}}),
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
    assert result.reason == "DUPLICATE_ACCOUNT_ASSET"


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
