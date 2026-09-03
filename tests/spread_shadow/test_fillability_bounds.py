from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal as D
from pathlib import Path
import json

import pytest

from risex_farmer.models import CanonicalMarket, ContractType, MarketType
from risex_spread_shadow import (
    AppendOnlyEvidenceStore,
    BookEvidence,
    BookLevel,
    DataGapEvidence,
    EntryViabilityOutcome,
    FeedBookEvent,
    FeedTradeEvent,
    FillabilityModel,
    MarketPair,
    QuotePolicy,
    QuoteVersion,
    SampleStopController,
    SampleStopReason,
    ShadowConfig,
    Side,
    SpreadDirection,
    SpreadObserver,
    TradeEvidence,
    Venue,
    build_entry_viability_episode,
    build_hypothetical_maker_quote,
    detect_optimistic_would_fill,
    detect_strict_would_fill,
    build_report,
)


UTC = timezone.utc
NOW = datetime(2026, 9, 3, tzinfo=UTC)


def _market(venue: Venue, symbol: str) -> CanonicalMarket:
    return CanonicalMarket(
        canonical_asset="BTC",
        venue=venue,
        venue_symbol=symbol,
        market_type=MarketType.PERPETUAL,
        contract_type=ContractType.LINEAR,
        base_multiplier=D("1"),
        quote_asset="USDC",
        settlement_asset="USDC",
        tick_size_raw=D("1"),
        quantity_step_raw=D("1"),
        minimum_quantity_raw=D("1"),
        minimum_notional_usd=D("0"),
        minimum_fee_notional_usd=None,
        is_active=True,
        is_rfq=False,
        is_off_hours=False,
    )


RISEX_MARKET = _market(Venue.RISEX, "BTC/USDC")
LIGHTER_MARKET = _market(Venue.LIGHTER, "BTC")


def _lighter_book(
    *,
    received: int = 100,
    session: str = "lighter-1",
    recovery: int = 0,
    revision: int = 1,
    bids: tuple[tuple[str, str], ...] = (("101", "10"),),
    asks: tuple[tuple[str, str], ...] = (("105", "10"),),
) -> BookEvidence:
    return BookEvidence(
        venue=Venue.LIGHTER,
        canonical_market="BTC",
        bids=tuple(BookLevel(D(price), D(quantity)) for price, quantity in bids),
        asks=tuple(BookLevel(D(price), D(quantity)) for price, quantity in asks),
        received_monotonic_ns=received,
        stream_session_id=session,
        recovery_generation=recovery,
        book_revision=revision,
        sequence=revision,
        checksum="ok",
        sequence_valid=True,
        checksum_valid=True,
        received_utc=NOW,
        fresh=True,
    )


def _version(
    *,
    version_id: str = "v1",
    created: int = 100,
    expires: int | None = 500,
    target: str = "307",
) -> QuoteVersion:
    policy = QuotePolicy(
        canonical_market="BTC",
        direction=SpreadDirection.RISEX_BUY_LIGHTER_SELL,
        target_notional_usd=D(target),
        target_margin_bps=D("0"),
        risex_maker_fee_rate=D("0"),
        lighter_taker_fee_rate=D("0"),
        risex_market=RISEX_MARKET,
        lighter_market=LIGHTER_MARKET,
        risex_best_bid=D("99"),
        risex_best_ask=D("101"),
        risex_tick_size=D("1"),
    )
    quote = build_hypothetical_maker_quote(policy, _lighter_book(received=90))
    assert quote.outcome is EntryViabilityOutcome.QUOTE_ACTIVE
    return QuoteVersion(
        version_id=version_id,
        quote=quote,
        quote_created_utc=NOW,
        quote_created_monotonic_ns=created,
        stream_session_id="risex-1",
        recovery_generation=0,
        quote_expires_monotonic_ns=expires,
        hedge_stream_session_id="lighter-1",
        hedge_recovery_generation=0,
    )


def _trade(
    key: str,
    *,
    price: str = "99",
    quantity: str = "1",
    received: int = 101,
    market: str = "BTC",
    aggressor: Side = Side.SELL,
    session: str = "risex-1",
    recovery: int = 0,
) -> TradeEvidence:
    return TradeEvidence(
        trade_event_key=key,
        venue=Venue.RISEX,
        canonical_market=market,
        canonical_price=D(price),
        canonical_quantity=D(quantity),
        aggressor_side=aggressor,
        received_utc=NOW,
        received_monotonic_ns=received,
        stream_session_id=session,
        recovery_generation=recovery,
        exchange_event_utc=NOW,
        exchange_event_time_provenance="FIXTURE",
    )


def test_optimistic_equality_and_subtick_are_not_strict_fills() -> None:
    version = _version()

    equality = _trade("at", price="100", quantity="3")
    assert detect_strict_would_fill(version, [equality]) is None
    evidence = detect_optimistic_would_fill(version, [equality])
    assert evidence is not None
    assert evidence.fillability_model is FillabilityModel.OPTIMISTIC_UPPER_BOUND

    subtick = _trade("subtick", price="99.5", quantity="3")
    assert detect_strict_would_fill(version, [subtick]) is None
    assert detect_optimistic_would_fill(version, [subtick]) is not None


def test_both_bounds_use_exact_cumulative_threshold_and_local_time() -> None:
    version = _version()
    trades = (
        _trade("second", price="99", quantity="2", received=102),
        _trade("first", price="99", quantity="1", received=101),
        _trade("late", price="99", quantity="9", received=130),
    )

    strict = detect_strict_would_fill(version, trades)
    optimistic = detect_optimistic_would_fill(version, trades)
    assert strict is not None and optimistic is not None
    for evidence in (strict, optimistic):
        assert evidence.qualifying_trade_event_keys == ("first", "second")
        assert evidence.cumulative_eligible_quantity == D("3")
        assert evidence.would_fill_detected_monotonic_ns == 102


@pytest.mark.parametrize(
    "changes",
    [
        {"received": 100},
        {"market": "ETH"},
        {"aggressor": Side.BUY},
        {"session": "other"},
        {"recovery": 1},
        {"received": 500},
    ],
)
def test_optimistic_identity_order_and_expiry_are_fail_closed(changes: dict[str, object]) -> None:
    version = _version()
    assert detect_optimistic_would_fill(
        version,
        [_trade("bad", quantity="3", **changes)],
    ) is None


def test_replacement_resets_preexisting_trade_eligibility() -> None:
    old = _version(version_id="old", created=100)
    replacement = _version(version_id="new", created=200)
    observed = _trade("before-replacement", quantity="3", received=150)
    assert detect_optimistic_would_fill(old, [observed]) is not None
    assert detect_optimistic_would_fill(replacement, [observed]) is None


def test_duplicate_and_conflicting_keys_never_add_volume() -> None:
    version = _version()
    duplicate = _trade("same", quantity="2")
    assert detect_optimistic_would_fill(version, [duplicate, duplicate]) is None

    conflicting = _trade("same", quantity="3")
    assert detect_optimistic_would_fill(version, [duplicate, conflicting]) is None
    assert detect_optimistic_would_fill(version, [conflicting, duplicate]) is None


def test_overlapping_risex_gap_rejects_both_fillability_bounds() -> None:
    version = _version()
    gap = DataGapEvidence(
        source_venue=Venue.RISEX,
        canonical_market="BTC",
        stream_session_id="risex-1",
        recovery_generation=0,
        gap_start_monotonic_ns=101,
        gap_end_monotonic_ns=101,
        reason="TEST_GAP",
    )
    trades = [_trade("through", price="99", quantity="3")]
    assert detect_strict_would_fill(version, trades, data_gaps=[gap]) is None
    assert detect_optimistic_would_fill(version, trades, data_gaps=[gap]) is None


def test_model_scoped_episode_and_horizon_identity_is_separate() -> None:
    version = _version()
    trade = _trade("at", price="100", quantity="3")
    books = {0: (_lighter_book(received=101, bids=(("101", "3"),)),)}

    strict = build_entry_viability_episode(
        version,
        [trade],
        books_by_horizon=books,
        horizons=(0,),
        fillability_model=FillabilityModel.STRICT_LOWER_BOUND,
    )
    optimistic = build_entry_viability_episode(
        version,
        [trade],
        books_by_horizon=books,
        horizons=(0,),
        fillability_model=FillabilityModel.OPTIMISTIC_UPPER_BOUND,
    )
    assert strict.outcome is EntryViabilityOutcome.NO_WOULD_FILL
    assert strict.fillability_model is FillabilityModel.STRICT_LOWER_BOUND
    assert optimistic.outcome is EntryViabilityOutcome.WOULD_FILL
    assert optimistic.would_fill_evidence is not None
    assert optimistic.would_fill_evidence.fillability_model is FillabilityModel.OPTIMISTIC_UPPER_BOUND
    assert optimistic.horizon_captures[0].fillability_model is FillabilityModel.OPTIMISTIC_UPPER_BOUND
    assert optimistic.horizon_captures[0].horizon_deadline_monotonic_ns == 101

    late = build_entry_viability_episode(
        version,
        [trade],
        books_by_horizon={0: (_lighter_book(received=102, bids=(("101", "3"),)),)},
        horizons=(0,),
        fillability_model=FillabilityModel.OPTIMISTIC_UPPER_BOUND,
    )
    assert late.horizon_captures[0].outcome is EntryViabilityOutcome.HEDGE_DATA_MISSING


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({"strict_episode_increment": 2}, SampleStopReason.STRICT_EPISODE_LIMIT),
        ({"eligible_trade_increment": 2}, SampleStopReason.ELIGIBLE_TRADE_LIMIT),
        ({"observed_monotonic_ns": 110}, SampleStopReason.WALL_CLOCK_LIMIT),
        ({"integrity_reason": "TEST_INTEGRITY"}, SampleStopReason.INTEGRITY_FAILURE),
    ],
)
def test_sample_stop_controller_latches_first_reason_wins(kwargs, expected) -> None:
    controller = SampleStopController(
        started_monotonic_ns=100,
        strict_episode_limit=2,
        eligible_trade_limit=2,
        wall_clock_limit_ns=10,
    )
    kwargs.setdefault("observed_monotonic_ns", 101)
    signal = controller.observe(**kwargs)
    assert signal is not None
    assert signal.reason is expected
    assert controller.observe(
        observed_monotonic_ns=1_000,
        strict_episode_increment=2,
        eligible_trade_increment=2,
    ) is signal


def _pair() -> MarketPair:
    return MarketPair("BTC", RISEX_MARKET, LIGHTER_MARKET)


def _feed_book(venue: Venue, received: int, session: str) -> FeedBookEvent:
    if venue is Venue.RISEX:
        book = BookEvidence(
            venue=venue,
            canonical_market="BTC",
            bids=(BookLevel(D("99"), D("10")),),
            asks=(BookLevel(D("101"), D("10")),),
            received_monotonic_ns=received,
            stream_session_id=session,
            recovery_generation=0,
            book_revision=1,
            sequence=1,
            checksum="ok",
            sequence_valid=True,
            checksum_valid=True,
            received_utc=NOW,
            fresh=True,
        )
    else:
        book = _lighter_book(received=received, session=session)
    return FeedBookEvent(book, _pair(), "SNAPSHOT", "FIXTURE")


def test_observer_counts_one_eligible_trade_across_policies_and_keeps_models_independent(tmp_path: Path) -> None:
    config = ShadowConfig(
        target_notionals_usd=(D("307"), D("608")),
        target_margins_bps=(D("1"),),
        ingress_queue_capacity=16,
    )
    store = AppendOnlyEvidenceStore.create(tmp_path, metadata={"evidence_mode": "FIXTURE"})
    observer = SpreadObserver(config, (_pair(),), store)
    trade = TradeEvidence(
        trade_event_key="trade-1",
        venue=Venue.RISEX,
        canonical_market="BTC",
        canonical_price=D("99"),
        canonical_quantity=D("3"),
        aggressor_side=Side.SELL,
        received_utc=NOW,
        received_monotonic_ns=101,
        stream_session_id="risex",
        recovery_generation=0,
        exchange_event_utc=NOW,
        exchange_event_time_provenance="FIXTURE",
    )

    async def run() -> None:
        observer._replay_mode = True
        await observer.handle_book(_feed_book(Venue.RISEX, 100, "risex"))
        await observer.handle_book(_feed_book(Venue.LIGHTER, 100, "lighter"))
        await observer.handle_trade(FeedTradeEvent(trade, _pair(), "fixture"))
        observer._replay_mode = False
        await observer.flush_pending(force=True)
        await observer.handle_trade(FeedTradeEvent(trade, _pair(), "duplicate"))
        await observer.close()

    import asyncio

    asyncio.run(run())
    store.close()
    records = [json.loads(line) for line in store.path.read_text().splitlines()]
    fills = [record for record in records if record.get("kind") == "WOULD_FILL"]
    trades = [record for record in records if record.get("kind") == "RISEX_TRADE"]
    assert len(trades) == 1
    assert trades[0]["eligible_trade"] is True
    assert len(trades[0]["eligible_policy_ids"]) == 2
    assert observer.eligible_trade_count == 1
    assert observer.strict_episode_count == 1
    assert observer.optimistic_episode_count == 1
    assert {record["fillability_model"] for record in fills} == {
        "STRICT_LOWER_BOUND",
        "OPTIMISTIC_UPPER_BOUND",
    }
    assert len(fills) == 2
    report = build_report(store.path)
    row = next(group for group in report["groups"] if group["horizon_ms"] == 0)
    assert row["strict_would_fill_count"] == 1
    assert row["optimistic_upper_bound_count"] == 1
    assert row["fillability_models"]["STRICT_LOWER_BOUND"]["horizon"]["observation_count"] == 1
    assert row["fillability_models"]["OPTIMISTIC_UPPER_BOUND"]["horizon"]["observation_count"] == 1


def test_large_stream_report_is_deterministic_without_full_file_materialization(tmp_path: Path) -> None:
    store = AppendOnlyEvidenceStore.create(
        tmp_path,
        metadata={
            "source_commit": "fixture",
            "evidence_mode": "FIXTURE",
            "fillability_models": (
                "STRICT_LOWER_BOUND",
                "OPTIMISTIC_UPPER_BOUND",
            ),
        },
    )
    records = [
        {
            "kind": "QUOTE",
            "canonical_market": "BTC",
            "direction": "RISEX_BUY_LIGHTER_SELL",
            "target_notional_usd": "307",
            "target_margin_bps": "1",
            "policy_id": "p",
            "quote_version_id": f"v-{index}",
            "outcome": "QUOTE_ACTIVE",
            "quote_created_monotonic_ns": index * 10,
            "quote_expires_monotonic_ns": index * 10 + 5,
            "quote_lifetime_ns": 5,
            "risex_tick_size": "1",
            "post_only_bound_price": "100",
            "maker_price": "99",
            "canonical_quantity": "3",
            "actual_edge_usd": "1",
            "observed_monotonic_ns": index * 10,
        }
        for index in range(5_000)
    ]
    records.append({"kind": "RUN_STOP", "fatal_reason": None})
    store.append_batch(records)
    path = store.path
    store.close()

    first = build_report(path)
    second = build_report(path)
    assert first == second
    assert first["record_count"] == 5_002
    assert first["evidence_mode"] == "FIXTURE"
    assert len(first["groups"]) == 4
    assert first["groups"][0]["quote_evaluation_count"] == 5_000
    assert first["byte_count"] == path.stat().st_size


def test_report_retires_more_than_256_complete_episodes_without_losing_identity(
    tmp_path: Path,
) -> None:
    store = AppendOnlyEvidenceStore.create(
        tmp_path,
        metadata={
            "source_commit": "fixture",
            "evidence_mode": "FIXTURE",
            "fillability_models": (
                "STRICT_LOWER_BOUND",
                "OPTIMISTIC_UPPER_BOUND",
            ),
        },
    )
    records: list[dict[str, object]] = []
    models = ("STRICT_LOWER_BOUND", "OPTIMISTIC_UPPER_BOUND")
    for index in range(300):
        base = index * 2_000_000_000
        version = f"version-{index}"
        records.append(
            {
                "kind": "QUOTE",
                "canonical_market": "BTC",
                "direction": "RISEX_BUY_LIGHTER_SELL",
                "target_notional_usd": "100",
                "target_margin_bps": "1",
                "policy_id": "policy-1",
                "quote_version_id": version,
                "outcome": "QUOTE_ACTIVE",
                "quote_created_monotonic_ns": base,
                "quote_expires_monotonic_ns": base + 1_500_000_000,
                "quote_stream_session_id": "risex",
                "quote_recovery_generation": 0,
                "hedge_stream_session_id": "lighter",
                "hedge_recovery_generation": 0,
                "maker_price": "99",
                "canonical_quantity": "1",
                "risex_tick_size": "1",
                "post_only_bound_price": "100",
                "actual_edge_usd": "1",
                "observed_monotonic_ns": base,
            }
        )
        for model in models:
            detected = base + 10
            records.append(
                {
                    "kind": "WOULD_FILL",
                    "canonical_market": "BTC",
                    "venue": "RISEX",
                    "policy_id": "policy-1",
                    "fillability_model": model,
                    "quote_version_id": version,
                    "direction": "RISEX_BUY_LIGHTER_SELL",
                    "quote_created_monotonic_ns": base,
                    "quote_expires_monotonic_ns": base + 1_500_000_000,
                    "quote_stream_session_id": "risex",
                    "quote_recovery_generation": 0,
                    "hedge_stream_session_id": "lighter",
                    "hedge_recovery_generation": 0,
                    "maker_price": "99",
                    "risex_tick_size": "1",
                    "canonical_quantity": "1",
                    "cumulative_eligible_quantity": "1",
                    "would_fill_detected_monotonic_ns": detected,
                    "observed_monotonic_ns": detected,
                }
            )
            for horizon in (0, 300, 500, 1000):
                horizon_record = {
                    "kind": "HEDGE_HORIZON",
                    "canonical_market": "BTC",
                    "venue": "LIGHTER",
                    "policy_id": "policy-1",
                    "fillability_model": model,
                    "quote_version_id": version,
                    "direction": "RISEX_BUY_LIGHTER_SELL",
                    "target_notional_usd": "100",
                    "target_margin_bps": "1",
                    "horizon_ms": horizon,
                    "would_fill_detected_monotonic_ns": detected,
                    "horizon_deadline_monotonic_ns": detected + horizon * 1_000_000,
                    "expected_stream_session_id": "lighter",
                    "expected_recovery_generation": 0,
                    "outcome": "HEDGE_FULL",
                    "filled_quantity": "1",
                    "notional_usd": "99",
                    "vwap_price": "99",
                    "entry_edge_usd": "1",
                    "conditional_markout_usd": "0",
                    "observed_monotonic_ns": detected + horizon * 1_000_000,
                }
                records.append(horizon_record)
                if horizon == 500:
                    records.append(dict(horizon_record))
    records.append({"kind": "RUN_STOP", "fatal_reason": None, "observed_monotonic_ns": 10**15})
    store.append_batch(records)
    path = store.path
    store.close()

    report = build_report(path)
    assert report["strict_would_fill_count"] == 300
    assert report["optimistic_upper_bound_count"] == 300
    assert report["record_count"] == len(records) + 1
    assert report["byte_count"] == path.stat().st_size
    for horizon in (0, 300, 500, 1000):
        row = next(group for group in report["groups"] if group["horizon_ms"] == horizon)
        assert row["fillability_models"]["STRICT_LOWER_BOUND"]["horizon"]["observation_count"] == 300
        assert row["fillability_models"]["OPTIMISTIC_UPPER_BOUND"]["horizon"]["observation_count"] == 300
        assert row["fillability_models"]["STRICT_LOWER_BOUND"]["horizon"]["data_completeness"] == "COMPLETE"
        assert row["fillability_models"]["OPTIMISTIC_UPPER_BOUND"]["horizon"]["data_completeness"] == "COMPLETE"


def test_report_preserves_a_late_gap_after_episode_retirement(tmp_path: Path) -> None:
    store = AppendOnlyEvidenceStore.create(
        tmp_path,
        metadata={"source_commit": "fixture", "evidence_mode": "FIXTURE"},
    )
    records: list[dict[str, object]] = [
        {
            "kind": "QUOTE",
            "canonical_market": "BTC",
            "direction": "RISEX_BUY_LIGHTER_SELL",
            "target_notional_usd": "100",
            "target_margin_bps": "1",
            "policy_id": "policy-1",
            "quote_version_id": "version-1",
            "outcome": "QUOTE_ACTIVE",
            "quote_created_monotonic_ns": 100,
            "quote_expires_monotonic_ns": 2_000_000_000,
            "quote_stream_session_id": "risex",
            "quote_recovery_generation": 0,
            "hedge_stream_session_id": "lighter",
            "hedge_recovery_generation": 0,
            "maker_price": "99",
            "canonical_quantity": "1",
            "risex_tick_size": "1",
            "post_only_bound_price": "100",
            "observed_monotonic_ns": 100,
        },
        {
            "kind": "WOULD_FILL",
            "canonical_market": "BTC",
            "venue": "RISEX",
            "policy_id": "policy-1",
            "fillability_model": "STRICT_LOWER_BOUND",
            "quote_version_id": "version-1",
            "direction": "RISEX_BUY_LIGHTER_SELL",
            "quote_created_monotonic_ns": 100,
            "quote_expires_monotonic_ns": 2_000_000_000,
            "quote_stream_session_id": "risex",
            "quote_recovery_generation": 0,
            "hedge_stream_session_id": "lighter",
            "hedge_recovery_generation": 0,
            "maker_price": "99",
            "canonical_quantity": "1",
            "cumulative_eligible_quantity": "1",
            "would_fill_detected_monotonic_ns": 110,
            "observed_monotonic_ns": 110,
        },
    ]
    for horizon in (0, 300, 500, 1000):
        records.append(
            {
                "kind": "HEDGE_HORIZON",
                "canonical_market": "BTC",
                "venue": "LIGHTER",
                "policy_id": "policy-1",
                "fillability_model": "STRICT_LOWER_BOUND",
                "quote_version_id": "version-1",
                "direction": "RISEX_BUY_LIGHTER_SELL",
                "target_notional_usd": "100",
                "target_margin_bps": "1",
                "horizon_ms": horizon,
                "would_fill_detected_monotonic_ns": 110,
                "horizon_deadline_monotonic_ns": 110 + horizon * 1_000_000,
                "expected_stream_session_id": "lighter",
                "expected_recovery_generation": 0,
                "outcome": "HEDGE_FULL",
                "filled_quantity": "1",
                "notional_usd": "99",
                "entry_edge_usd": "1",
                "conditional_markout_usd": "0",
                "observed_monotonic_ns": 110 + horizon * 1_000_000,
            }
        )
    records.extend(
        (
            {
                "kind": "DATA_GAP",
                "canonical_market": "BTC",
                "venue": "LIGHTER",
                "stream_session_id": "lighter",
                "recovery_generation": 0,
                "gap_start_monotonic_ns": 210,
                "gap_end_monotonic_ns": 210,
                "reason": "LATE_TEST_GAP",
                "observed_monotonic_ns": 10**12,
            },
            {"kind": "RUN_STOP", "fatal_reason": None, "observed_monotonic_ns": 10**12 + 1},
        )
    )
    store.append_batch(records)
    path = store.path
    store.close()

    report = build_report(path)
    zero = next(group for group in report["groups"] if group["horizon_ms"] == 0)
    delayed = next(group for group in report["groups"] if group["horizon_ms"] == 300)
    assert zero["data_completeness"] == "COMPLETE"
    assert delayed["data_completeness"] == "DEGRADED"
