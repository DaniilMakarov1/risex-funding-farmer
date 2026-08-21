import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import aiohttp

from risex_farmer.exchanges.base import (
    HEALTH_CHECK_CADENCE_SECONDS,
    PublicDataUnavailable,
    WebSocketFrameAction,
    timestamp,
)
from risex_farmer.exchanges.extended import ExtendedAdapter
from risex_farmer.exchanges.nado import NadoAdapter
from risex_farmer.exchanges.risex import RisexAdapter
from risex_farmer.market_data import BookStream, funding_is_fresh
from risex_farmer.models import (
    BookLevel,
    ContractType,
    DataQuality,
    FundingQuality,
    SettlementStatus,
    MarketType,
    OrderBook,
    Side,
    Venue,
)


D = Decimal
NOW = datetime(2027, 1, 15, 12, tzinfo=UTC)
FIXTURES = Path(__file__).parent / "fixtures" / "paper_002"


def fixture(name: str) -> dict[str, Any]:
    return json.loads(
        (FIXTURES / f"{name}.json").read_text(), parse_float=Decimal
    )


class FakeResponse:
    def __init__(self, payload: Any, status: int = 200) -> None:
        self.payload = payload
        self.status = status

    async def __aenter__(self) -> "FakeResponse":
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise aiohttp.ClientResponseError(None, (), status=self.status)

    async def text(self) -> str:
        return json.dumps(self.payload)


class FakeSession:
    def __init__(self, response: FakeResponse) -> None:
        self.response = response
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append((url, kwargs))
        return self.response


def test_market_normalization_and_exclusions() -> None:
    extended = ExtendedAdapter(None)
    extended_data = fixture("extended")
    market = extended.normalize_market(extended_data["market"])
    assert market.market_type is MarketType.PERPETUAL
    assert market.contract_type is ContractType.LINEAR
    assert market.base_multiplier == D("1")
    assert extended.normalize_market(extended_data["spot"]).market_type is MarketType.SPOT

    rfq = deepcopy(extended_data["market"])
    rfq["isRfq"] = True
    assert extended.normalize_market(rfq).is_rfq
    rwa = deepcopy(extended_data["market"])
    rwa["category"] = "RWA"
    assert extended.normalize_market(rwa).contract_type is ContractType.OTHER
    assert not extended.normalize_market(rwa).is_active

    nado = NadoAdapter(None)
    nado_data = fixture("nado")
    crypto = nado.normalize_market(nado_data["market"])
    assert crypto.contract_type is ContractType.LINEAR
    assert crypto.minimum_notional_usd == D("20")
    assert crypto.minimum_fee_notional_usd == D("20")
    off_hours = nado.normalize_market(nado_data["off_hours"])
    assert off_hours.is_off_hours
    assert not off_hours.is_active
    assert off_hours.base_multiplier is None


def test_documented_quote_volume_is_normalized_exactly() -> None:
    expected = D("1234.50")
    assert RisexAdapter(None).normalize_volume(
        fixture("risex")["market"], observed_at=NOW
    ).quote_volume_usd == expected
    assert ExtendedAdapter(None).normalize_volume(
        fixture("extended")["market"], observed_at=NOW
    ).quote_volume_usd == expected
    nado_volume = NadoAdapter(None).normalize_volume(
        fixture("nado")["ticker"], observed_at=NOW
    )
    assert nado_volume.canonical_market == "ABC-PERP"
    assert nado_volume.quote_volume_usd == expected


@pytest.mark.asyncio
async def test_extended_public_access_never_adds_auth_and_marks_rejection() -> None:
    success_session = FakeSession(
        FakeResponse({"data": [fixture("extended")["market"]]})
    )
    adapter = ExtendedAdapter(success_session)
    assert len(await adapter.fetch_markets()) == 1
    assert "headers" not in success_session.calls[0][1]
    assert adapter.public_data_available is True

    rejected = ExtendedAdapter(FakeSession(FakeResponse({}, status=401)))
    with pytest.raises(PublicDataUnavailable):
        await rejected.fetch_markets()
    assert rejected.public_data_available is False


@pytest.mark.asyncio
async def test_extended_catalog_normalizes_markets_and_volumes_from_one_fetch() -> None:
    session = FakeSession(FakeResponse({"data": [fixture("extended")["market"]]}))
    markets, volumes = await ExtendedAdapter(session).fetch_catalog()
    assert len(session.calls) == 1
    assert markets[0].venue_symbol == volumes[0].canonical_market
    assert volumes[0].quote_volume_usd == D("1234.50")
    assert session.calls[0][1]["timeout"].total == 60


@pytest.mark.asyncio
async def test_extended_required_catalog_repeats_market_query_and_is_atomic() -> None:
    abc = fixture("extended")["market"]
    xyz = deepcopy(abc)
    xyz.update({"name": "XYZ-USD", "assetName": "XYZ"})
    session = FakeSession(FakeResponse({"data": [xyz, abc]}))
    markets, _ = await ExtendedAdapter(session).fetch_required_catalog(
        ("ABC-USD", "XYZ-USD")
    )
    assert [market.venue_symbol for market in markets] == ["ABC-USD", "XYZ-USD"]
    assert session.calls[0][1]["params"] == [
        ("market", "ABC-USD"), ("market", "XYZ-USD"),
    ]
    incomplete = ExtendedAdapter(FakeSession(FakeResponse({"data": [abc]})))
    with pytest.raises(ValueError, match="incomplete"):
        await incomplete.fetch_required_catalog(("ABC-USD", "XYZ-USD"))


def test_risex_paper_fallback_is_explicit_and_unknown_funding_still_fails_closed() -> None:
    adapter = RisexAdapter(None)
    market = adapter.normalize_market(fixture("risex")["market"])
    quote = adapter.unknown_funding_quote(
        market, observed_at=NOW, assumed_open_at=NOW
    )
    assert market.base_multiplier is None
    assert market.contract_type is ContractType.OTHER
    assert quote.quality is FundingQuality.UNKNOWN
    assert not quote.eligibility_known
    assert quote.long_cash_per_canonical_base_usd is None
    assert quote.short_cash_per_canonical_base_usd is None
    synthetic = deepcopy(fixture("risex")["market"])
    synthetic["config"]["name"] = "1000ABC/USDC"
    synthetic["base_asset_symbol"] = "1000ABC/USDC"
    assert RisexAdapter(None).normalize_market(synthetic).contract_type is ContractType.OTHER


def test_risex_positive_grid_aligned_below_minimum_is_valid_unit_evidence() -> None:
    adapter = RisexAdapter(None)
    row = deepcopy(fixture("risex")["market"])
    row["config"]["min_order_size"] = "10"
    assert adapter.normalize_market(row).contract_type is ContractType.OTHER
    adapter.normalize_book(fixture("risex")["book"], observed_at=NOW)
    adapter.normalize_trade(
        fixture("risex")["trade"], received_at=NOW,
        session_id="below-minimum", ordinal=1,
    )
    assert adapter.normalize_market(row).contract_type is ContractType.LINEAR


def test_risex_unit_evidence_has_precise_authoritative_blocker() -> None:
    adapter = RisexAdapter(None)
    row = fixture("risex")["market"]
    adapter.normalize_market(row)
    off_grid = deepcopy(fixture("risex")["book"])
    off_grid["bids"][0]["quantity"] = "2.005"
    adapter.normalize_book(off_grid, observed_at=NOW)
    adapter.normalize_trade(
        fixture("risex")["trade"], received_at=NOW,
        session_id="precise", ordinal=1,
    )
    blocked = adapter.normalize_market(row)
    assert "RISEX_BOOK_QUANTITY_OFF_STEP" in blocked.evidence_blockers

    zero_step = deepcopy(row)
    zero_step["config"]["step_size"] = "0"
    zero_grid = adapter.normalize_market(zero_step)
    assert "RISEX_GRID_OR_MINIMUM_NONPOSITIVE" in zero_grid.evidence_blockers


@pytest.mark.asyncio
async def test_risex_funding_fallback_is_labeled_and_consistency_gated() -> None:
    adapter = RisexAdapter(None)
    row = deepcopy(fixture("risex")["market"])
    assumed_at = datetime.now(UTC)
    settlement_at = assumed_at + timedelta(hours=1)
    row.update(
        {
            "current_funding_rate": "0.001",
            "mark_price": "100",
            "next_funding_time": str(int(settlement_at.timestamp() * 1_000_000_000)),
        }
    )
    adapter.normalize_market(row)
    adapter.normalize_book(fixture("risex")["book"], observed_at=assumed_at)
    adapter.normalize_trade(fixture("risex")["trade"], received_at=assumed_at, session_id="proof", ordinal=1)
    market = adapter.normalize_market(row)
    quote = await adapter.fetch_funding_quote(market, assumed_open_at=assumed_at)
    assert quote.quality is FundingQuality.ESTIMATED
    assert quote.source == "PAPER_ASSUMPTION_CURRENT_NEXT_RATE"
    assert quote.long_cash_per_canonical_base_usd == D("-0.100")
    inconsistent = deepcopy(row)
    inconsistent["quote_asset_symbol"] = "EUR"
    blocked_market = adapter.normalize_market(inconsistent)
    blocked = await adapter.fetch_funding_quote(
        blocked_market, assumed_open_at=assumed_at
    )
    assert blocked.quality is FundingQuality.UNKNOWN
    assert blocked_market.base_multiplier is None


@pytest.mark.asyncio
async def test_risex_history_requires_stable_three_interval_cadence() -> None:
    row = deepcopy(fixture("risex")["market"])
    row.update({"mark_price": "100"})
    interval = 3_600_000_000_000
    future = int((datetime.now(UTC) + timedelta(hours=1)).timestamp() * 1_000_000_000)
    records = [
        {"start_time": str(future - interval * (index + 1)),
         "end_time": str(future - interval * index), "funding_rate": "0.001"}
        for index in range(3)
    ]
    session = FakeSession(FakeResponse({"data": {"records": records}}))
    adapter = RisexAdapter(session)
    adapter.normalize_market(row)
    adapter.normalize_book(fixture("risex")["book"], observed_at=NOW)
    adapter.normalize_trade(fixture("risex")["trade"], received_at=NOW, session_id="proof", ordinal=1)
    market = adapter.normalize_market(row)
    quote = await adapter.fetch_funding_quote(market, assumed_open_at=datetime.now(UTC))
    assert quote.quality is FundingQuality.ESTIMATED
    assert quote.source == "PAPER_ASSUMPTION_LAST_APPLIED_RATE"
    assert session.calls[0][0].endswith("/v1/markets/id/7/funding-rate-history")
    records[1]["start_time"] = str(int(records[1]["start_time"]) + 1)
    blocked = await adapter.fetch_funding_quote(market, assumed_open_at=datetime.now(UTC))
    assert blocked.quality is FundingQuality.UNKNOWN
    records[1]["start_time"] = str(int(records[1]["end_time"]) - interval)
    for record in records:
        record["index_price"] = "100"
    applied = await adapter.fetch_applied_funding_quotes(
        market, since=datetime.now(UTC),
        until=datetime.fromtimestamp(future / 1_000_000_000, UTC),
        assumed_open_at=datetime.now(UTC) - timedelta(hours=1),
    )
    assert applied and applied[0].quality is FundingQuality.APPLIED_RATE
    assert applied[0].source == adapter.FUNDING_SOURCE


def test_trade_ids_aggressors_and_synthetic_keys() -> None:
    risex = RisexAdapter(None)
    risex.normalize_market(fixture("risex")["market"])
    r_trade = risex.normalize_trade(
        fixture("risex")["trade"], received_at=NOW, session_id="s1", ordinal=1
    )
    assert r_trade.trade_event_key == "RISEX|ABC/USDC|maker-taker"
    assert r_trade.aggressor_side is Side.SELL
    assert r_trade.is_orderbook_match is True
    assert r_trade.exchange_timestamp is not None
    assert r_trade.received_at == NOW
    assert r_trade.raw_timestamp == fixture("risex")["trade"]["block_timestamp"]
    worker_only = deepcopy(fixture("risex")["trade"])
    worker_only["worker_timestamp"] = worker_only.pop("block_timestamp")
    safe = risex.normalize_trade(worker_only, received_at=NOW, session_id="s1", ordinal=2)
    assert safe.raw_timestamp == worker_only["worker_timestamp"]
    assert safe.exchange_timestamp is not None
    assert "RISEX_WORKER_TIMESTAMP_USED_AS_SERVER_EVENT_TIME" in safe.paper_assumptions

    extended = ExtendedAdapter(None)
    e_trade = extended.normalize_trade(
        fixture("extended")["trade"], received_at=NOW, session_id="s1", ordinal=1
    )
    assert e_trade.trade_event_key == "EXTENDED|ABC-USD|42"
    assert e_trade.aggressor_side is Side.SELL
    seq, wrapped = extended.normalize_trade_message(
        {"seq": 2, "data": [fixture("extended")["trade"]]},
        received_at=NOW, session_id="s1", starting_ordinal=0,
    )
    assert seq == 2 and wrapped == (e_trade,)
    liquidation = deepcopy(fixture("extended")["trade"])
    liquidation["tT"] = "LIQUIDATION"
    assert ExtendedAdapter(None).normalize_trade(
        liquidation, received_at=NOW, session_id="s1", ordinal=2
    ).is_orderbook_match is False
    unknown = deepcopy(fixture("extended")["trade"])
    unknown.pop("tT")
    assert ExtendedAdapter(None).normalize_trade(
        unknown, received_at=NOW, session_id="s1", ordinal=3
    ).is_orderbook_match is None

    nado = NadoAdapter(None)
    nado.normalize_market(fixture("nado")["market"])
    payload = fixture("nado")["trade"]
    first = nado.normalize_trade(payload, received_at=NOW, session_id="s1", ordinal=1)
    duplicate = nado.normalize_trade(payload, received_at=NOW, session_id="s1", ordinal=1)
    next_event = nado.normalize_trade(payload, received_at=NOW, session_id="s1", ordinal=2)
    assert first.trade_event_key == duplicate.trade_event_key
    assert first.trade_event_key != next_event.trade_event_key
    assert first.aggressor_side is Side.BUY
    assert first.is_orderbook_match is True


def test_extended_and_nado_funding_conversion() -> None:
    extended = ExtendedAdapter(None)
    e_market = extended.normalize_market(fixture("extended")["market"])
    e_quote = extended.funding_quote(
        e_market,
        funding_rate="0.001",
        mark_price="100",
        observed_at=NOW,
        assumed_open_at=NOW,
        settlement_at=NOW + timedelta(minutes=30),
    )
    assert e_quote.long_cash_per_canonical_base_usd == D("-0.100")
    assert e_quote.short_cash_per_canonical_base_usd == D("0.100")
    assert e_quote.assumed_or_actual_position_opened_at == NOW
    e_spot = extended.normalize_market(fixture("extended")["spot"])
    assert extended.funding_quote(
        e_spot,
        funding_rate="0.001",
        mark_price="100",
        observed_at=NOW,
        assumed_open_at=NOW,
        settlement_at=NOW + timedelta(hours=1),
    ).quality is FundingQuality.UNKNOWN
    e_applied = extended.normalize_applied_funding_message(
        fixture("extended")["funding"],
        e_market,
    )
    assert e_applied.status is SettlementStatus.UNRESOLVED
    assert e_applied.cash_usd is None
    assert e_applied.canonical_market == e_market.venue_symbol

    nado = NadoAdapter(None)
    n_market = nado.normalize_market(fixture("nado")["market"])
    predicted = nado.predicted_funding_quote(
        n_market,
        funding_rate_x18="24000000000000000",
        index_price_x18="100000000000000000000",
        observed_at=NOW,
        assumed_open_at=NOW,
        settlement_at=NOW + timedelta(hours=1),
    )
    assert predicted.long_cash_per_canonical_base_usd == D("-0.100")
    assert predicted.short_cash_per_canonical_base_usd == D("0.100")
    streamed = nado.normalize_funding_rate_message(
        fixture("nado")["funding_rate"],
        n_market,
        index_price_x18="100000000000000000000",
        assumed_open_at=NOW,
    )
    assert streamed.long_cash_per_canonical_base_usd == D("-0.100")

    applied = nado.applied_cumulative_funding_quote(
        n_market,
        previous_long_x18="1000000000000000000",
        current_long_x18="1200000000000000000",
        previous_short_x18="-1000000000000000000",
        current_short_x18="-1200000000000000000",
        observed_at=NOW,
        assumed_open_at=NOW - timedelta(hours=1),
        settlement_at=NOW,
    )
    assert applied.long_cash_per_canonical_base_usd == D("0.2")
    assert applied.short_cash_per_canonical_base_usd == D("-0.2")
    streamed_applied = nado.normalize_funding_payment_message(
        fixture("nado")["funding_payment"],
        n_market,
        previous_long_x18="1000000000000000000",
        previous_short_x18="-1000000000000000000",
        assumed_open_at=NOW - timedelta(hours=1),
    )
    assert streamed_applied.long_cash_per_canonical_base_usd == D("0.2")
    assert streamed_applied.short_cash_per_canonical_base_usd == D("-0.2")


def test_float_market_data_is_rejected() -> None:
    payload = fixture("extended")["trade"]
    payload["p"] = 100.0
    with pytest.raises(TypeError, match="decimal string"):
        ExtendedAdapter(None).normalize_trade(
            payload, received_at=NOW, session_id="s", ordinal=1
        )


def test_nanosecond_timestamp_boundaries_floor_sub_microseconds() -> None:
    exact_raw = 1_800_014_400_000_000_000
    exact = timestamp(str(exact_raw), "nanoseconds")
    before = timestamp(str(exact_raw - 1), "nanoseconds")
    after = timestamp(str(exact_raw + 1), "nanoseconds")
    assert exact == NOW
    assert before == exact - timedelta(microseconds=1)
    assert after == exact
    assert before < exact <= after


def test_public_websocket_heartbeat_actions() -> None:
    assert HEALTH_CHECK_CADENCE_SECONDS == 10

    risex_payload = fixture("risex")["heartbeat"]["server_ping_payload"].encode()
    risex_action = RisexAdapter.handle_server_ping(risex_payload)
    assert risex_action.frame_action is WebSocketFrameAction.PONG
    assert risex_action.payload == risex_payload
    assert risex_action.connection_confirmed

    extended_payload = fixture("extended")["heartbeat"][
        "server_ping_payload"
    ].encode()
    extended_action = ExtendedAdapter.handle_server_ping(extended_payload)
    assert extended_action.frame_action is WebSocketFrameAction.PONG
    assert extended_action.payload == extended_payload
    assert extended_action.connection_confirmed

    nado_data = fixture("nado")["heartbeat"]
    nado_ping = NadoAdapter.client_ping_action(
        nado_data["client_ping_payload"].encode()
    )
    assert nado_ping.frame_action is WebSocketFrameAction.PING
    assert not nado_ping.connection_confirmed
    nado_pong = NadoAdapter.handle_server_pong(
        nado_data["server_pong_payload"].encode()
    )
    assert nado_pong.frame_action is WebSocketFrameAction.NONE
    assert nado_pong.connection_confirmed
    assert NadoAdapter.subscription("trade", 7) == {
        "method": "subscribe", "stream": {"type": "trade", "product_id": 7}, "id": 7
    }


def test_documented_book_delta_shapes_are_normalized() -> None:
    risex = RisexAdapter(None)
    risex_data = fixture("risex")
    risex.normalize_market(risex_data["market"])
    r_delta = risex.normalize_book_message(risex_data["book_update"])
    assert r_delta.checksum == 123  # type: ignore[union-attr]

    extended = ExtendedAdapter(None)
    e_delta = extended.normalize_book_message(fixture("extended")["book_delta"])
    assert e_delta.sequence == 2
    assert e_delta.bids[0].canonical_quantity == D("1")

    nado = NadoAdapter(None)
    nado_data = fixture("nado")
    nado.normalize_market(nado_data["market"])
    n_delta = nado.normalize_book_message(nado_data["book_delta"])
    assert n_delta.previous_sequence == 1800000000000000000
    assert n_delta.bids[0].canonical_quantity == D("1")


def test_funding_freshness_uses_observation_age() -> None:
    assert funding_is_fresh(NOW, NOW + timedelta(seconds=120))
    assert not funding_is_fresh(NOW, NOW + timedelta(seconds=121))
    assert not funding_is_fresh(NOW, NOW - timedelta(seconds=1))


def book(venue: Venue, sequence: int) -> OrderBook:
    return OrderBook(
        venue,
        "ABC",
        (BookLevel(D("99"), D("2")),),
        (BookLevel(D("101"), D("3")),),
        NOW,
        sequence,
    )


def test_reconnect_sequence_gap_heartbeat_and_recovery() -> None:
    stream = BookStream(Venue.EXTENDED, "ABC")
    stream.connected(NOW)
    stream.snapshot(book(Venue.EXTENDED, 1))
    assert stream.health(NOW + timedelta(seconds=25)).data_quality is DataQuality.COMPLETE
    assert not stream.extended_delta((), (), sequence=3, observed_at=NOW)
    assert stream.book() is None

    stream.snapshot(book(Venue.EXTENDED, 7))
    assert stream.extended_delta(
        (BookLevel(D("99"), D("1")),), (), sequence=8, observed_at=NOW
    )
    assert stream.health(NOW + timedelta(seconds=26)).data_quality is DataQuality.DEGRADED
    stream.disconnected()
    stream.connected(NOW + timedelta(seconds=27))
    assert stream.book() is None
    assert stream.health(NOW + timedelta(seconds=27)).data_quality is DataQuality.DEGRADED


def test_nado_gap_and_risex_checksum_detection() -> None:
    nado = BookStream(Venue.NADO, "ABC")
    nado.connected(NOW)
    nado.snapshot(book(Venue.NADO, 100))
    assert nado.nado_delta((), (), last_max_timestamp=100, max_timestamp=110, observed_at=NOW)
    assert nado.nado_delta(
        (), (), last_max_timestamp=109, max_timestamp=120, observed_at=NOW
    )
    assert not nado.nado_delta(
        (), (), last_max_timestamp=121, max_timestamp=130, observed_at=NOW
    )

    risex = BookStream(Venue.RISEX, "ABC")
    risex.connected(NOW)
    risex.snapshot(book(Venue.RISEX, 1))
    checksum = risex.risex_checksum()
    assert risex.risex_update((), (), checksum=checksum, observed_at=NOW)
    assert not risex.risex_update((), (), checksum=checksum + 1, observed_at=NOW)
