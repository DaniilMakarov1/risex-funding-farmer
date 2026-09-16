from __future__ import annotations

from decimal import Decimal
from fractions import Fraction
import math

import pytest

from risex_spread_shadow.hood_handoff import (
    AccountSnapshot,
    ContractError,
    LighterSdkClient,
    MarketMetadata,
    MutationReceipt,
    OperationMode,
    OrderSnapshot,
    Outcome,
    StaticSecretProvider,
    run_handoff,
)
from risex_spread_shadow.hood_handoff.sdk import _trade_timestamp_seconds

from test_hood_handoff_engine import make_config


NOW = 1_789_541_681.0
TRADE_TIMESTAMP_MS = 1_789_541_676_073
SOURCE_ORDER_ID = "562950026284934"
RECEIVER_ORDER_ID = "844424857365178"


def _raw_trade(timestamp: object) -> dict[str, object]:
    return {
        "trade_id": 16164557907,
        "trade_id_str": "16164557907",
        "market_id": 7,
        "size": "0.00020",
        "price": "75961.0",
        "ask_id": 562950026284934,
        "bid_id": 844424857365178,
        "ask_client_id": 1001,
        "ask_client_id_str": "1001",
        "bid_client_id": 1002,
        "bid_client_id_str": "1002",
        "ask_account_id": 11,
        "bid_account_id": 22,
        "timestamp": timestamp,
        "transaction_time": 1_789_541_676_086_065,
    }


class _AuthSigner:
    def __init__(self, **kwargs):
        self.close_calls = 0

    async def create_auth_token_with_expiry(self, **kwargs):
        return "synthetic-auth-token", None

    async def close(self):
        self.close_calls += 1


class _ApiClient:
    def __init__(self, configuration):
        self.close_calls = 0

    async def close(self):
        self.close_calls += 1


class _Http:
    def __init__(self, *args, **kwargs):
        self.close_calls = 0

    async def aclose(self):
        self.close_calls += 1


class _Clock:
    def now(self):
        return NOW

    async def sleep(self, seconds):
        return None


def _receiver_only_adapter(tmp_path, *, timestamp: object):
    raw_trade = _raw_trade(timestamp)

    class OrderApi:
        def __init__(self, api_client):
            self.api_client = api_client

        async def trades(self, **kwargs):
            if (
                kwargs["account_index"] == 22
                and kwargs["order_index"] == int(RECEIVER_ORDER_ID)
            ):
                return {"code": 200, "trades": [raw_trade], "next_cursor": None}
            return {"code": 200, "trades": [], "next_cursor": None}

    order_api = OrderApi

    class Module:
        Configuration = type(
            "Configuration",
            (),
            {"__init__": lambda self, **kwargs: setattr(self, "kwargs", kwargs)},
        )
        ApiClient = _ApiClient
        OrderApi = order_api
        SignerClient = _AuthSigner

    client = LighterSdkClient(
        make_config(
            tmp_path / "receiver-only.jsonl",
            operation_mode=OperationMode.PAIRED_OPENING,
            source_limit_price=Decimal("75960.0"),
            receiver_worst_price=Decimal("75961.0"),
        ),
        source_account_index=11,
        receiver_account_index=22,
        secrets=StaticSecretProvider({11: "synthetic-source", 22: "synthetic-receiver"}),
        market_evidence={},
        signer_factory=_AuthSigner,
        http_factory=_Http,
        clock=lambda: NOW,
    )
    client._lighter = lambda: Module
    state: dict[str, object] = {
        "source_order": None,
        "receiver_order": None,
        "source_position": Decimal("0"),
        "receiver_position": Decimal("0"),
    }

    async def market_metadata(market_id):
        return MarketMetadata(
            market_id=7,
            symbol="HOOD",
            status="active",
            price_decimals=2,
            size_decimals=5,
            minimum_base_amount=Decimal("0.00001"),
            minimum_quote_amount=Decimal("1"),
            source_fee_rate=Decimal("0.001"),
            receiver_fee_rate=Decimal("0.001"),
            observed_at=NOW,
            margin_evidence="synthetic market metadata",
        )

    async def account_snapshot(account_index, market_id):
        key = "source_order" if account_index == 11 else "receiver_order"
        order = state[key]
        active_orders = [] if order is None or not order.active else [order]
        return AccountSnapshot.from_mapping(
            {
                "account_index": account_index,
                "market_id": 7,
                "signed_position": str(state["source_position" if account_index == 11 else "receiver_position"]),
                "active_orders": active_orders,
                "observed_at": NOW,
                "authorized": True,
                "ready": True,
                "margin_available": "1000",
                "margin_required": "1",
                "incremental_margin_required": "1",
                "incremental_margin_evidence": "synthetic planned delta",
                "fee_rate": "0.001",
                "source_identity": f"synthetic-account-{account_index}",
            }
        )

    async def lookup_order(account_index, market_id, *, order_id=None, client_order_index=None):
        key = "source_order" if account_index == 11 else "receiver_order"
        order = state[key]
        if order is None:
            return None
        if order_id is not None and order.order_id != str(order_id):
            return None
        return order

    async def submit_order(plan):
        if plan.account_index == 11:
            assert plan.side == "SELL"
            assert plan.order_type == "LIMIT"
            state["source_order"] = OrderSnapshot(
                account_index=11,
                market_id=7,
                order_id=SOURCE_ORDER_ID,
                client_order_index=plan.client_order_index,
                status="open",
                side=plan.side,
                order_type=plan.order_type,
                time_in_force=plan.time_in_force,
                reduce_only=plan.reduce_only,
                initial_quantity=plan.quantity,
                remaining_quantity=plan.quantity,
                filled_quantity=Decimal("0"),
                price=plan.price,
                observed_at=NOW,
            )
            return MutationReceipt(True, SOURCE_ORDER_ID, "synthetic-source-tx")
        assert plan.account_index == 22
        assert plan.side == "BUY"
        source_order = state["source_order"]
        assert source_order is not None
        state["source_order"] = OrderSnapshot(
            account_index=11,
            market_id=7,
            order_id=SOURCE_ORDER_ID,
            client_order_index=source_order.client_order_index,
            status="canceled",
            side="SELL",
            order_type="LIMIT",
            time_in_force="POST_ONLY",
            reduce_only=False,
            initial_quantity=plan.quantity,
            remaining_quantity=Decimal("0"),
            filled_quantity=Decimal("0"),
            price=source_order.price,
            observed_at=NOW,
        )
        state["receiver_order"] = OrderSnapshot(
            account_index=22,
            market_id=7,
            order_id=RECEIVER_ORDER_ID,
            client_order_index=plan.client_order_index,
            status="filled",
            side=plan.side,
            order_type=plan.order_type,
            time_in_force=plan.time_in_force,
            reduce_only=plan.reduce_only,
            initial_quantity=plan.quantity,
            remaining_quantity=plan.quantity - Decimal("0.00020"),
            filled_quantity=Decimal("0.00020"),
            price=plan.price,
            observed_at=NOW,
        )
        state["receiver_position"] = Decimal("0.00020")
        return MutationReceipt(True, RECEIVER_ORDER_ID, "synthetic-receiver-tx")

    client.market_metadata = market_metadata
    client.account_snapshot = account_snapshot
    client.lookup_order = lookup_order
    client.submit_order = submit_order
    return client


@pytest.mark.parametrize(
    "value",
    [
        True,
        False,
        1.5,
        Fraction(3, 2),
        "1789541676073",
        Decimal("1789541676073"),
        None,
        -1,
        math.nan,
        math.inf,
        10**400,
    ],
)
def test_trade_timestamp_rejects_non_strict_millisecond_values(value):
    with pytest.raises(ContractError, match="trade timestamp"):
        _trade_timestamp_seconds(value)


@pytest.mark.asyncio
async def test_adapter_to_engine_normalizes_realistic_timestamp_and_keeps_receiver_only_partial(tmp_path):
    client = _receiver_only_adapter(tmp_path, timestamp=TRADE_TIMESTAMP_MS)
    try:
        result = await run_handoff(
            make_config(
                tmp_path / "receiver-only.jsonl",
                operation_mode=OperationMode.PAIRED_OPENING,
                source_limit_price=Decimal("75960.0"),
                receiver_worst_price=Decimal("75961.0"),
            ),
            client,
            clock=_Clock(),
        )
        assert result.outcome is Outcome.PARTIAL
        assert result.source.filled_quantity == Decimal("0")
        assert result.receiver.filled_quantity == Decimal("0.00020")
        assert result.source.position_after == Decimal("0")
        assert result.receiver.position_after == Decimal("0.00020")
        assert result.receiver.trades[0].observed_at == 1_789_541_676.073
        assert result.receiver.trades[0].observed_at < NOW
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_adapter_to_engine_preserves_true_future_receipt_as_unknown(tmp_path):
    future_timestamp = int((NOW + 5) * 1_000)
    client = _receiver_only_adapter(tmp_path, timestamp=future_timestamp)
    try:
        result = await run_handoff(
            make_config(
                tmp_path / "future.jsonl",
                operation_mode=OperationMode.PAIRED_OPENING,
                source_limit_price=Decimal("75960.0"),
                receiver_worst_price=Decimal("75961.0"),
            ),
            client,
            clock=_Clock(),
        )
        assert result.outcome is Outcome.UNKNOWN
        assert result.receiver.trades == ()
        assert any("trade receipt is from the future" in item for item in result.unknown_reasons)
    finally:
        await client.aclose()
