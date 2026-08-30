import ast
import copy
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

from risex_farmer import extended_mainnet_credential_onboarding as onboarding
from risex_farmer import extended_mainnet_private_read as gate
from risex_farmer import cli


API_KEY = "synthetic-read-only-api-key"
SECRET_MARKER = "synthetic-key-must-never-appear-in-evidence"
FIXTURE_PATH = (
    Path(__file__).parent
    / "fixtures"
    / "extended_mainnet_private_read"
    / "official_contract.json"
)


def _envelope(data, pagination=None):
    result = {"status": "OK", "data": data}
    if pagination is not None:
        result["pagination"] = pagination
    return result


def _info(**overrides):
    value = {
        "status": "ACTIVE",
        "l2Key": gate.EXPECTED_L2_KEY,
        "l2Vault": gate.EXPECTED_L2_VAULT,
        "accountId": gate.EXPECTED_ACCOUNT_ID,
        "accountIndex": gate.EXPECTED_ACCOUNT_INDEX,
        "bridgeStarknetAddress": None,
    }
    value.update(overrides)
    return _envelope(value)


def _account_row(**overrides):
    value = {
        "accountId": gate.EXPECTED_ACCOUNT_ID,
        "accountIndex": gate.EXPECTED_ACCOUNT_INDEX,
        "status": "ACTIVE",
        "l2Key": gate.EXPECTED_L2_KEY,
        "l2Vault": gate.EXPECTED_L2_VAULT,
        "bridgeStarknetAddress": None,
        "accountIndexForKeyGeneration": gate.EXPECTED_ACCOUNT_INDEX,
    }
    value.update(overrides)
    return value


def _balance(**overrides):
    value = {
        "accountId": gate.EXPECTED_ACCOUNT_ID,
        "collateralName": "USD",
        "balance": "100",
        "equity": "100",
        "availableForTrade": "100",
        "availableForWithdrawal": "100",
        "unrealisedPnl": "0",
        "withdrawableUnrealisedPnl": "0",
        "initialMargin": "0",
        "marginRatio": "0",
        "exposure": "0",
        "leverage": "0",
        "spotEquity": "0",
        "spotEquityForAvailableForTrade": "0",
        "collateralReservedForSpotOrders": "0",
        "updatedTime": 1700000000000,
    }
    value.update(overrides)
    return value


def _spot(asset="USD", **overrides):
    collateral = asset in {"USD", "USDC"}
    value = {
        "accountId": gate.EXPECTED_ACCOUNT_ID,
        "asset": asset,
        "balance": "100" if collateral else "0",
        "indexPrice": "1" if collateral else "100",
        "notionalValue": "100" if collateral else "0",
        "contributionFactor": "1" if collateral else "0",
        "equityContribution": "100" if collateral else "0",
        "updatedAt": 1700000000000,
    }
    value.update(overrides)
    return value


def _order(**overrides):
    value = {
        "id": 901,
        "accountId": gate.EXPECTED_ACCOUNT_ID,
        "market": "BTC-USD",
        "status": "NEW",
        "type": "LIMIT",
        "side": "BUY",
        "qty": "0.001",
        "filledQty": "0",
    }
    value.update(overrides)
    return value


def _history_order(order_id, **overrides):
    value = _order(id=order_id, status="CANCELLED")
    value.update(overrides)
    return value


def _trade(**overrides):
    value = {
        "id": 701,
        "accountId": gate.EXPECTED_ACCOUNT_ID,
        "market": "BTC-USD",
        "orderId": 901,
        "side": "BUY",
        "averagePrice": "100",
        "filledQty": "0.001",
        "value": "0.1",
        "fee": "0",
        "isTaker": False,
        "tradeType": "TRADE",
        "createdTime": 1700000000000,
    }
    value.update(overrides)
    return value


def _position(**overrides):
    value = {
        "id": 801,
        "accountId": gate.EXPECTED_ACCOUNT_ID,
        "market": "BTC-USD",
        "side": "LONG",
        "size": "0.001",
        "openPrice": "100",
    }
    value.update(overrides)
    return value


def _funding(**overrides):
    value = {
        "id": 601,
        "accountId": gate.EXPECTED_ACCOUNT_ID,
        "market": "BTC-USD",
        "positionId": 801,
        "side": "LONG",
        "value": "0.1",
        "markPrice": "100",
        "fundingFee": "0",
        "fundingRate": "0",
        "paidTime": 1700000000000,
    }
    value.update(overrides)
    return value


def _base_responses():
    return {
        gate.ACCOUNT_INFO_PATH: _info(),
        gate.ACCOUNTS_PATH: _envelope([_account_row()]),
        gate.BALANCE_PATH: _envelope(_balance()),
        gate.SPOT_BALANCES_PATH: _envelope([_spot()]),
        gate.ASSET_OPERATIONS_PATH: _envelope([]),
        gate.FEES_PATH: _envelope(
            [
                {
                    "market": gate.FEE_MARKET,
                    "makerFeeRate": "0",
                    "takerFeeRate": "0.00025",
                    "builderFeeRate": "0",
                }
            ]
        ),
        gate.OPEN_ORDERS_PATH: _envelope([]),
        gate.ORDER_HISTORY_PATH: _envelope([]),
        gate.TRADES_PATH: _envelope([]),
        gate.POSITIONS_PATH: _envelope([]),
        gate.POSITION_HISTORY_PATH: _envelope([]),
        gate.FUNDING_HISTORY_PATH: _envelope([]),
    }


def _stream_frames(**overrides):
    balance = _balance()
    spot = _spot()
    frames = [
        {
            "type": "BALANCE",
            "data": {"balance": balance},
            "ts": 1700000000000,
            "seq": 1,
        },
        {
            "type": "ORDER",
            "data": {"orders": []},
            "ts": 1700000000001,
            "seq": 2,
        },
        {
            "type": "TRADE",
            "data": {"trades": []},
            "ts": 1700000000002,
            "seq": 3,
        },
        {
            "type": "POSITION",
            "data": {"positions": []},
            "ts": 1700000000003,
            "seq": 4,
        },
        {
            "type": "SPOT_BALANCE",
            "data": {"spotBalances": [spot]},
            "ts": 1700000000004,
            "seq": 5,
        },
    ]
    for index, value in overrides.items():
        frames[index] = value
    return frames


class Capability:
    def __init__(self, identity=gate.EXPECTED_IDENTITY, api_key=API_KEY):
        self.identity = identity
        self._api_key = api_key
        self.api_key_fingerprint = hashlib.sha256(api_key.encode("ascii")).hexdigest()
        self.closed = False

    def api_key(self):
        if self.closed:
            raise RuntimeError("closed")
        return self._api_key

    def close(self):
        self.closed = True


class Source:
    def __init__(self, identity=gate.EXPECTED_IDENTITY, api_key=API_KEY, error=None):
        self.capability = Capability(identity, api_key)
        self.error = error
        self.calls = 0

    def open(self):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.capability


class Stream:
    def __init__(self, frames, *, metadata=None, application_frames_sent=0):
        self.frames = list(frames)
        self.closed = False
        self.application_frames_sent = application_frames_sent
        self.reconnect_count = 0
        self.upgrade_metadata = {
            "actual_url": gate.MAINNET_STREAM_URL,
            "method": "GET",
            "header_names": ["User-Agent", gate.API_KEY_HEADER],
            "direct_tls": True,
            "trust_env": False,
            "proxy": None,
            "redirects": 0,
            "retries": 0,
            "application_frames_sent": application_frames_sent != 0,
        }
        if metadata:
            self.upgrade_metadata.update(metadata)

    async def recv(self):
        if not self.frames:
            raise StopAsyncIteration
        return self.frames.pop(0)

    async def close(self):
        self.closed = True


class Transport:
    def __init__(self, responses=None, streams=None):
        self.responses = _base_responses() if responses is None else responses
        self.scripts = {}
        self.streams = list(streams if streams is not None else [_stream_frames()])
        self.calls = []
        self.stream_requests = []
        self.closed = False

    def script(self, path, *items):
        self.scripts[path] = list(items)

    async def get(self, request):
        self.calls.append(request)
        items = self.scripts.get(request.path)
        if items:
            item = items.pop(0)
        else:
            item = self.responses[request.path]
        if callable(item):
            item = item(request)
        if isinstance(item, BaseException):
            raise item
        if isinstance(item, gate.RestReply):
            return item
        return gate.RestReply(200, request.url, item)

    async def open_stream(self, request):
        self.stream_requests.append(request)
        if not self.streams:
            raise gate.TransportInterruption()
        item = self.streams.pop(0)
        if callable(item):
            item = item(request)
        if isinstance(item, BaseException):
            raise item
        return Stream(item) if isinstance(item, list) else item

    async def close(self):
        self.closed = True


def _clock():
    return 1700000000000


async def _run(tmp_path, *, transport=None, source=None, invocation_id="synthetic-run"):
    tmp_path.mkdir(parents=True, exist_ok=True)
    if transport is None:
        transport = Transport()
    if source is None:
        source = Source()
    result = await gate.run_fixture(
        store_path=tmp_path / "runs.sqlite3",
        invocation_id=invocation_id,
        credential_source=source,
        transport=transport,
        clock_ms=_clock,
    )
    return result, transport, source


def test_official_fixture_records_current_read_contract():
    value = json.loads(FIXTURE_PATH.read_text())
    assert value["provenance"]["documentation"] == "https://api.docs.extended.exchange/"
    assert "/api/v1/user/account/info" in value["read_paths"]
    assert "/api/v1/user/fees?market=BTC-USD" in value["read_paths"]
    assert value["write_methods_forbidden"] == ["POST", "PUT", "PATCH", "DELETE"]


def test_balance_uses_observed_usd_denomination_and_rejects_usdc():
    decoded = gate._decode_balance(_envelope(_balance(collateralName="USD")))
    assert decoded["collateral_name"] == "USD"

    with pytest.raises(gate.GateFailure) as error:
        gate._decode_balance(_envelope(_balance(collateralName="USDC")))
    assert error.value.reason == "COLLATERAL_ASSET_UNEXPECTED"
    assert error.value.failure_class == "SAFETY"


def test_spot_collateral_allowlist_is_closed_and_keeps_other_assets_unrelated():
    for asset in ("USD", "USDC"):
        rows = gate._decode_spot_rows(_envelope([_spot(asset)]))
        assert gate._noncollateral_nonzero_spot_assets(rows) == []

    rows = gate._decode_spot_rows(
        _envelope(
            [
                _spot(
                    "ETHSPOT",
                    balance="1",
                    indexPrice="2000",
                    notionalValue="2000",
                    contributionFactor="0.9",
                    equityContribution="1800",
                )
            ]
        )
    )
    assert gate._noncollateral_nonzero_spot_assets(rows) == ["ETHSPOT"]


def test_observed_mainnet_balance_arithmetic_includes_spot_equity():
    balance = _balance(
        balance="-0.005052",
        equity="20.711382",
        availableForTrade="20.7113811",
        initialMargin="0",
        unrealisedPnl="0",
        spotEquity="20.716434",
        spotEquityForAvailableForTrade="20.7164331",
        collateralReservedForSpotOrders="0",
        exposure="0",
        leverage="0",
        marginRatio="0",
    )
    rows = gate._decode_spot_rows(
        _envelope(
            [
                _spot(
                    "USD",
                    balance="20.716434",
                    notionalValue="20.716434",
                    equityContribution="20.716434",
                )
            ]
        )
    )
    summary = gate._empty_summary()
    gate._set_summary_state(
        summary,
        balance=gate._decode_balance_data(balance),
        spot=rows,
        asset_operations=(),
        fees=(),
        open_orders=(),
        order_history=(),
        trades=(),
        positions=(),
        position_history=(),
        funding=(),
        page_counts={},
    )
    assert summary["flatness"]["exact"] is True
    assert summary["flatness"]["formula_agreement"] is True
    assert summary["flatness"]["zero_fields"] == {
        "initialMargin": "0",
        "marginRatio": "0",
        "exposure": "0",
        "leverage": "0",
        "unrealisedPnl": "0",
        "withdrawableUnrealisedPnl": "0",
        "collateralReservedForSpotOrders": "0",
    }
    assert summary["spot_balances"]["assets"] == ["USD"]
    assert summary["spot_balances"]["noncollateral_nonzero_assets"] == []


def test_production_run_directory_accepts_macos_directory_link_count(tmp_path):
    path = tmp_path / "extended-mainnet-private-read"
    path.mkdir(mode=gate.RUN_DIRECTORY_MODE)
    path.chmod(gate.RUN_DIRECTORY_MODE)

    assert path.lstat().st_nlink == 2
    gate._ensure_run_directory(path)


@pytest.mark.asyncio
async def test_ready_reads_exact_account_and_proves_flatness_without_write_surface(tmp_path):
    result, transport, source = await _run(tmp_path)

    assert result.ready
    assert result.reason == "MAINNET_PRIVATE_READ_PROVED"
    assert result.failure_class is None
    assert result.identity == gate.EXPECTED_IDENTITY.to_metadata()
    assert result.summary["identity_verified"] is True
    assert result.summary["balance"]["balance"] == "100"
    assert result.summary["balance"]["equity"] == "100"
    assert result.summary["balance"]["availableForTrade"] == "100"
    assert result.summary["balance"]["collateral_name"] == "USD"
    assert result.summary["fees"]["rates"]["BTC-USD"]["taker"] == "0.00025"
    assert result.summary["funding"]["status"] == "AUTHORITATIVE_EMPTY_HISTORY"
    assert result.summary["funding"]["cash_total"] is None
    assert result.summary["flatness"]["exact"] is True
    assert result.summary["unrelated_state"] == {
        "status": "CLEAR",
        "categories": [],
        "active_order_count": 0,
        "open_position_count": 0,
    }
    assert result.summary["private_stream"]["rest_agreement"] is True
    assert result.summary["pagination"]["funding"]["pages"] == 1
    assert result.summary["pagination"]["funding"]["freshness"] == (
        "MONOTONIC_LOCAL_OBSERVATIONS"
    )
    assert result.summary["pagination"]["funding"]["observed_at_ms"] == [
        1700000000000
    ]
    assert result.to_metadata()["write_ready"] is False
    assert result.to_metadata()["mainnet_write_authority"] == gate.NO_MAINNET_WRITE_AUTHORITY
    assert SECRET_MARKER not in result.evidence()
    assert API_KEY not in result.evidence()
    assert source.capability.closed
    assert API_KEY not in (tmp_path / "runs.sqlite3").read_bytes().decode(
        "utf-8", errors="ignore"
    )
    assert all(request.method == "GET" for request in transport.calls)
    assert all(request.url.startswith(gate.MAINNET_REST_BASE_URL) for request in transport.calls)
    assert any(
        request.path == gate.FEES_PATH
        and request.query == (("market", gate.FEE_MARKET),)
        for request in transport.calls
    )


@pytest.mark.asyncio
async def test_protected_identity_mismatch_blocks_before_network(tmp_path):
    wrong = onboarding.ExtendedPublicIdentity.from_inputs(
        "303920", "0", gate.EXPECTED_L2_KEY, "403920"
    )
    result, transport, source = await _run(tmp_path, source=Source(identity=wrong))

    assert result.status == gate.STATUS_BLOCKED
    assert result.reason == "PROTECTED_IDENTITY_MISMATCH"
    assert result.failure_class == "IDENTITY"
    assert transport.calls == []
    assert source.capability.closed
    assert API_KEY not in result.evidence()


@pytest.mark.asyncio
async def test_account_info_and_accounts_must_agree(tmp_path):
    transport = Transport()
    transport.script(
        gate.ACCOUNT_INFO_PATH,
        _info(accountId=303920, l2Vault=403920),
    )
    result, _, _ = await _run(tmp_path, transport=transport)

    assert result.reason == "ACCOUNT_INFO_ACCOUNTS_DISAGREE"
    assert result.failure_class == "IDENTITY"
    assert len(transport.calls) == 2


@pytest.mark.asyncio
async def test_http_auth_and_schema_failures_are_terminal_without_retry(tmp_path):
    transport = Transport()
    transport.script(
        gate.ACCOUNT_INFO_PATH,
        gate.RestReply(401, gate.MAINNET_REST_BASE_URL + gate.ACCOUNT_INFO_PATH, None),
    )
    result, _, _ = await _run(tmp_path / "auth", transport=transport, invocation_id="auth-run")
    assert result.reason == "AUTHENTICATION_REJECTED"
    assert result.failure_class == "AUTH"
    assert len(transport.calls) == 1
    assert result.counters["rest_account_info_attempts"] == 1

    transport = Transport()
    transport.script(gate.BALANCE_PATH, _envelope({"collateralName": "USDC"}))
    result, _, _ = await _run(tmp_path / "schema", transport=transport, invocation_id="schema-run")
    assert result.failure_class == "SCHEMA"
    assert result.reason == "BALANCE_FIELDS_MISSING"
    assert sum(call.path == gate.BALANCE_PATH for call in transport.calls) == 1


@pytest.mark.asyncio
async def test_only_transport_interruption_gets_one_fresh_retry(tmp_path):
    transport = Transport()
    transport.script(gate.ACCOUNT_INFO_PATH, gate.TransportInterruption(), _info())
    result, _, _ = await _run(tmp_path / "retry", transport=transport, invocation_id="retry-run")
    assert result.ready
    assert sum(call.path == gate.ACCOUNT_INFO_PATH for call in transport.calls) == 2
    assert result.counters["rest_account_info_attempts"] == 2
    assert result.counters["rest_account_info_completions"] == 1

    transport = Transport()
    transport.script(
        gate.ACCOUNT_INFO_PATH,
        gate.TransportInterruption(),
        gate.TransportInterruption(),
    )
    result, _, _ = await _run(tmp_path / "exhausted", transport=transport, invocation_id="exhausted-run")
    assert result.reason == "TRANSPORT_RETRY_EXHAUSTED"
    assert result.failure_class == "TRANSPORT"
    assert sum(call.path == gate.ACCOUNT_INFO_PATH for call in transport.calls) == 2

    transport = Transport()
    transport.script(gate.ACCOUNT_INFO_PATH, ValueError("unexpected"))
    result, _, _ = await _run(tmp_path / "unknown", transport=transport, invocation_id="unknown-run")
    assert result.reason == "UNCLASSIFIED_FAILURE"
    assert result.failure_class == "SAFETY"
    assert len(transport.calls) == 1


def _two_page_history(request):
    if ("cursor", "17") in request.query:
        return _envelope([_history_order(3)], {"count": 1})
    return _envelope([_history_order(1), _history_order(2)], {"count": 2, "cursor": 17})


@pytest.mark.asyncio
async def test_cursor_pagination_is_bounded_and_canonical(tmp_path, monkeypatch):
    monkeypatch.setattr(gate, "MAX_PAGE_ITEMS", 2)
    transport = Transport()
    transport.responses[gate.ORDER_HISTORY_PATH] = _two_page_history
    result, _, _ = await _run(tmp_path, transport=transport)

    assert result.ready
    assert result.summary["order_history"]["count"] == 3
    assert result.summary["order_history"]["pages"] == 2
    history_calls = [call for call in transport.calls if call.path == gate.ORDER_HISTORY_PATH]
    assert [call.query for call in history_calls] == [
        (("limit", "2"),),
        (("limit", "2"), ("cursor", "17")),
    ]


@pytest.mark.asyncio
async def test_repeated_or_missing_full_page_cursor_blocks(tmp_path, monkeypatch):
    monkeypatch.setattr(gate, "MAX_PAGE_ITEMS", 2)

    repeated = Transport()
    repeated.responses[gate.ORDER_HISTORY_PATH] = lambda request: _envelope(
        [_history_order(1), _history_order(2)], {"count": 2, "cursor": 7}
    )
    result, _, _ = await _run(tmp_path / "repeated", transport=repeated, invocation_id="repeated-run")
    assert result.reason == "PAGINATION_CURSOR_REPEATED_ORDER_HISTORY"
    assert result.failure_class == "SAFETY"

    missing = Transport()
    missing.responses[gate.ORDER_HISTORY_PATH] = _envelope(
        [_history_order(1), _history_order(2)], {"count": 2}
    )
    result, _, _ = await _run(tmp_path / "missing", transport=missing, invocation_id="missing-run")
    assert result.reason == "PAGINATION_CURSOR_MISSING_ORDER_HISTORY"
    assert result.failure_class == "SCHEMA"


@pytest.mark.asyncio
async def test_funding_empty_is_not_cash_zero_and_missing_funding_blocks(tmp_path):
    result, _, _ = await _run(tmp_path / "empty", invocation_id="funding-empty")
    assert result.ready
    assert result.summary["funding"]["cash_total"] is None

    transport = Transport()
    transport.responses[gate.FUNDING_HISTORY_PATH] = _envelope(
        [{}], {"count": 1}
    )
    result, _, _ = await _run(tmp_path / "missing", transport=transport, invocation_id="funding-missing")
    assert result.reason == "FIELD_MISSING_FUNDING"
    assert result.failure_class == "SCHEMA"
    assert result.summary["funding"]["cash_total"] is None

    transport = Transport()
    transport.responses[gate.FUNDING_HISTORY_PATH] = _envelope(
        [_funding(fundingFee="-0.25")], {"count": 1}
    )
    result, _, _ = await _run(tmp_path / "present", transport=transport, invocation_id="funding-present")
    assert result.ready
    assert result.summary["funding"]["status"] == "APPLIED_RECORDS"
    assert result.summary["funding"]["cash_total"] == "-0.25"


@pytest.mark.asyncio
async def test_documented_zero_balance_404_is_explicit_zero_not_missing_data(tmp_path):
    zero = _balance(
        balance="0",
        equity="0",
        availableForTrade="0",
        availableForWithdrawal="0",
    )
    frames = _stream_frames()
    frames[0] = {
        "type": "BALANCE",
        "data": {"balance": zero},
        "ts": 1700000000000,
        "seq": 1,
    }
    frames[4] = {
        "type": "SPOT_BALANCE",
        "data": {"spotBalances": []},
        "ts": 1700000000004,
        "seq": 5,
    }
    transport = Transport(streams=[frames])
    transport.responses[gate.BALANCE_PATH] = lambda request: gate.RestReply(
        404, request.url, None
    )
    transport.responses[gate.SPOT_BALANCES_PATH] = lambda request: gate.RestReply(
        404, request.url, None
    )
    result, _, _ = await _run(tmp_path, transport=transport, invocation_id="zero-404-run")
    assert result.ready
    assert result.summary["balance"]["balance_source"] == "OFFICIAL_404_ZERO_BALANCE"
    assert result.summary["balance"]["collateral_name"] == "USD"
    assert result.summary["balance"]["equity"] == "0"
    assert result.summary["spot_balances"]["count"] == 0


@pytest.mark.asyncio
async def test_private_stream_requires_auth_surface_empty_components_and_rest_agreement(tmp_path):
    transport = Transport(streams=[_stream_frames()[:-1], _stream_frames()[:-1]])
    result, _, _ = await _run(tmp_path / "missing", transport=transport, invocation_id="stream-missing")
    assert result.reason == "TRANSPORT_RETRY_EXHAUSTED"
    assert result.failure_class == "TRANSPORT"
    assert len(transport.stream_requests) == 2

    active_order = _order()
    frames = _stream_frames()
    frames[1] = {
        "type": "ORDER",
        "data": {"orders": [active_order]},
        "ts": 1700000000001,
        "seq": 2,
    }
    transport = Transport(streams=[frames])
    result, _, _ = await _run(tmp_path / "active", transport=transport, invocation_id="stream-active")
    assert result.reason == "STREAM_ORDER_ACTIVITY"
    assert result.failure_class == "SAFETY"

    frames = _stream_frames()
    frames[4] = {
        "type": "SPOT_BALANCE",
        "data": {"spotBalances": [_spot("ETH", balance="1")]},
        "ts": 1700000000004,
        "seq": 5,
    }
    transport = Transport(streams=[frames])
    result, _, _ = await _run(tmp_path / "spot", transport=transport, invocation_id="stream-spot")
    assert result.reason == "STREAM_UNRELATED_SPOT_STATE"
    assert result.failure_class == "SAFETY"

    frames = _stream_frames()
    frames[0] = {
        "type": "BALANCE",
        "data": {"balance": _balance(equity="99")},
        "ts": 1700000000000,
        "seq": 1,
    }
    transport = Transport(streams=[frames])
    result, _, _ = await _run(tmp_path / "disagree", transport=transport, invocation_id="stream-disagree")
    assert result.reason == "REST_STREAM_BALANCE_DISAGREE"
    assert result.failure_class == "SAFETY"


@pytest.mark.asyncio
async def test_observed_rest_stream_spot_classification_and_ethspot_block(tmp_path):
    observed_balance = _balance(
        balance="-0.005052",
        equity="20.711382",
        availableForTrade="20.7113811",
        spotEquity="20.716434",
        spotEquityForAvailableForTrade="20.7164331",
    )
    observed_usd = _spot(
        "USD",
        balance="20.716434",
        notionalValue="20.716434",
        equityContribution="20.716434",
    )
    responses = _base_responses()
    responses[gate.BALANCE_PATH] = _envelope(observed_balance)
    responses[gate.SPOT_BALANCES_PATH] = _envelope([observed_usd])
    frames = _stream_frames()
    frames[0] = {
        "type": "BALANCE",
        "data": {"balance": observed_balance},
        "ts": 1700000000000,
        "seq": 1,
    }
    frames[4] = {
        "type": "SPOT_BALANCE",
        "data": {"spotBalances": [observed_usd]},
        "ts": 1700000000004,
        "seq": 5,
    }
    result, _, _ = await _run(
        tmp_path / "usd-only",
        transport=Transport(responses=responses, streams=[frames]),
        invocation_id="observed-usd-only",
    )
    assert result.ready
    assert result.summary["flatness"]["exact"] is True
    assert result.summary["flatness"]["formula_agreement"] is True
    assert result.summary["spot_balances"]["assets"] == ["USD"]
    assert result.summary["spot_balances"]["noncollateral_nonzero_assets"] == []

    observed_ethspot = _spot(
        "ETHSPOT",
        balance="1",
        indexPrice="2000",
        notionalValue="2000",
        contributionFactor="0.9",
        equityContribution="1800",
    )
    responses = _base_responses()
    responses[gate.BALANCE_PATH] = _envelope(observed_balance)
    responses[gate.SPOT_BALANCES_PATH] = _envelope([observed_usd, observed_ethspot])
    frames = _stream_frames()
    frames[0] = {
        "type": "BALANCE",
        "data": {"balance": observed_balance},
        "ts": 1700000000000,
        "seq": 1,
    }
    frames[4] = {
        "type": "SPOT_BALANCE",
        "data": {"spotBalances": [observed_usd, observed_ethspot]},
        "ts": 1700000000004,
        "seq": 5,
    }
    result, _, _ = await _run(
        tmp_path / "ethspot",
        transport=Transport(responses=responses, streams=[frames]),
        invocation_id="observed-ethspot",
    )
    assert result.reason == "STREAM_UNRELATED_SPOT_STATE"
    assert result.failure_class == "SAFETY"
    assert result.summary["spot_balances"]["assets"] == ["ETHSPOT", "USD"]
    assert result.summary["spot_balances"]["noncollateral_nonzero_assets"] == ["ETHSPOT"]
    assert "ETHSPOT" in result.evidence()


@pytest.mark.asyncio
async def test_stream_auth_and_transport_metadata_fail_closed(tmp_path):
    transport = Transport(streams=[gate.GateFailure("STREAM_AUTHENTICATION_REJECTED", "AUTH")])
    result, _, _ = await _run(tmp_path / "auth", transport=transport, invocation_id="stream-auth")
    assert result.reason == "STREAM_AUTHENTICATION_REJECTED"
    assert result.failure_class == "AUTH"
    assert len(transport.stream_requests) == 1

    bad_stream = Stream(_stream_frames(), application_frames_sent=1)
    transport = Transport(streams=[bad_stream])
    result, _, _ = await _run(tmp_path / "outbound", transport=transport, invocation_id="stream-outbound")
    assert result.reason == "STREAM_TRANSPORT_UNVERIFIABLE"
    assert result.failure_class == "SAFETY"

    bad_stream = Stream(_stream_frames(), metadata={"actual_url": "wss://other.example/"})
    transport = Transport(streams=[bad_stream])
    result, _, _ = await _run(tmp_path / "redirect", transport=transport, invocation_id="stream-redirect")
    assert result.reason == "STREAM_TRANSPORT_UNVERIFIABLE"
    assert result.failure_class == "SAFETY"


@pytest.mark.asyncio
async def test_pending_asset_operation_is_classified_as_unrelated_state(tmp_path):
    operation = {
        "id": 11,
        "type": "DEPOSIT",
        "status": "IN_PROGRESS",
        "amount": "10",
        "fee": "0",
        "asset": "USDC",
        "time": 1700000000000,
        "accountId": gate.EXPECTED_ACCOUNT_ID,
    }
    transport = Transport()
    transport.responses[gate.ASSET_OPERATIONS_PATH] = _envelope(
        [operation], {"count": 1}
    )
    result, _, _ = await _run(tmp_path, transport=transport)
    assert result.reason == "UNRELATED_STATE_PRESENT"
    assert result.failure_class == "SAFETY"
    assert result.summary["unrelated_state"]["categories"] == [
        "PENDING_ASSET_OPERATIONS"
    ]


@pytest.mark.asyncio
async def test_terminal_invocation_is_not_replayed_and_running_invocation_is_blocked(tmp_path):
    first_transport = Transport()
    first, _, first_source = await _run(tmp_path, transport=first_transport)
    assert first.ready
    assert first_source.capability.closed

    second_transport = Transport()
    second_source = Source()
    second, _, _ = await _run(
        tmp_path,
        transport=second_transport,
        source=second_source,
    )
    assert second.evidence() == first.evidence()
    assert second_transport.calls == []
    assert second_source.calls == 0

    running_path = tmp_path / "running.sqlite3"
    store = gate.RunStore(running_path, "running-run")
    assert store.claim() is None
    transport = Transport()
    result = await gate.run_fixture(
        store_path=running_path,
        invocation_id="running-run",
        credential_source=Source(),
        transport=transport,
        clock_ms=_clock,
    )
    assert result.reason == "INTERRUPTED_RUNNING"
    assert result.failure_class == "SAFETY"
    assert transport.calls == []


@pytest.mark.asyncio
async def test_durable_evidence_tampering_is_terminal_safety_failure(tmp_path):
    result, _, _ = await _run(tmp_path, invocation_id="tampered-run")
    assert result.ready
    path = tmp_path / "runs.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE extended_mainnet_private_read_runs SET evidence=? WHERE invocation_id=?",
            ("{}", "tampered-run"),
        )
        connection.commit()
    with pytest.raises(gate.StoreFailure) as error:
        await gate.run_fixture(
            store_path=path,
            invocation_id="tampered-run",
            credential_source=Source(),
            transport=Transport(),
            clock_ms=_clock,
        )
    assert error.value.reason == "DURABLE_EVIDENCE_INVALID"


def test_no_write_surface_and_normal_startup_import_is_unchanged():
    source = Path(gate.__file__).read_text()
    tree = ast.parse(source)
    called_attributes = {
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and isinstance(node.ctx, ast.Load)
    }
    assert not {"post", "put", "patch", "delete"} & called_attributes
    assert gate.HTTP_METHOD == "GET"
    assert "extended_mainnet_private_read" not in Path(cli.__file__).read_text()
    assert "stark-private-key" not in source
    assert "create_order" not in source
    assert "sign(" not in source
    assert "dispatch(" not in source
    assert "https://api.starknet.sepolia.extended.exchange" not in source
