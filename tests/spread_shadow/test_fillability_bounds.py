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
from risex_spread_shadow.report import _gap_contaminates


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
        material_valid_strict_episode_limit=2,
        material_detection_timestamp_limit=2,
    )
    kwargs.setdefault("observed_monotonic_ns", 101)
    signal = controller.observe(**kwargs)
    if expected is SampleStopReason.STRICT_EPISODE_LIMIT:
        assert signal is None
        assert controller.material_counts("policy-1") == (0, 0)
        assert controller.observe_material(
            policy_id="policy-1",
            episode_id="episode-1",
            detection_monotonic_ns=101,
            observed_monotonic_ns=101,
        ) is None
        signal = controller.observe_material(
            policy_id="policy-1",
            episode_id="episode-2",
            detection_monotonic_ns=102,
            observed_monotonic_ns=102,
        )
    assert signal is not None
    assert signal.reason is expected
    assert controller.observe(
        observed_monotonic_ns=1_000,
        strict_episode_increment=2,
        eligible_trade_increment=2,
    ) is signal


def test_material_stop_requires_one_policy_distinct_timestamps_and_survives_invalidation() -> None:
    controller = SampleStopController(
        started_monotonic_ns=100,
        material_valid_strict_episode_limit=4,
        material_detection_timestamp_limit=2,
        eligible_trade_limit=500,
        wall_clock_limit_ns=1_000,
    )
    for episode_id in ("a", "b"):
        assert controller.observe_material(
            policy_id="policy-a",
            episode_id=episode_id,
            detection_monotonic_ns=101,
            observed_monotonic_ns=101,
        ) is None
    assert controller.material_counts("policy-a") == (2, 1)
    assert controller.observe_material(
        policy_id="policy-a",
        episode_id="c",
        detection_monotonic_ns=102,
        observed_monotonic_ns=102,
    ) is None
    assert controller.material_counts("policy-a") == (3, 2)
    controller.invalidate_material(policy_id="policy-a", episode_id="b")
    assert controller.material_counts("policy-a") == (2, 2)
    assert controller.observe_material(
        policy_id="policy-b",
        episode_id="other",
        detection_monotonic_ns=103,
        observed_monotonic_ns=103,
    ) is None
    signal = controller.observe_material(
        policy_id="policy-a",
        episode_id="d",
        detection_monotonic_ns=104,
        observed_monotonic_ns=104,
    )
    assert signal is None
    signal = controller.observe_material(
        policy_id="policy-a",
        episode_id="e",
        detection_monotonic_ns=105,
        observed_monotonic_ns=106,
    )
    assert signal is not None
    assert signal.reason is SampleStopReason.STRICT_EPISODE_LIMIT
    assert signal.material_policy_id == "policy-a"
    assert signal.material_valid_strict_episode_count == 4
    assert signal.material_detection_timestamp_count == 4
    assert signal.observed_monotonic_ns == 106


def test_material_stop_rejects_a_backdated_observation() -> None:
    controller = SampleStopController(started_monotonic_ns=100)
    with pytest.raises(ValueError, match="must not precede detection"):
        controller.observe_material(
            policy_id="policy-a",
            episode_id="episode-1",
            detection_monotonic_ns=101,
            observed_monotonic_ns=100,
        )


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
        await observer.append_terminal(
            {"kind": "RUN_STOP", "fatal_reason": None, "observed_monotonic_ns": 0}
        )
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


def _gap_test_record(
    *,
    start: object = 10,
    end: object = 20,
    session: object = "lighter-1",
    recovery: object = 0,
    market: object = "BTC",
    venue: object = "LIGHTER",
    policy_id: str = "policy-1",
) -> dict[str, object]:
    return {
        "kind": "HEDGE_HORIZON",
        "canonical_market": market,
        "venue": venue,
        "policy_id": policy_id,
        "would_fill_detected_monotonic_ns": start,
        "horizon_deadline_monotonic_ns": end,
        "expected_stream_session_id": session,
        "expected_recovery_generation": recovery,
    }


def _gap_test_gap(
    *,
    start: object = 30,
    end: object = 40,
    session: object = "lighter-1",
    recovery: object = 0,
    market: object = "BTC",
    venue: object = "LIGHTER",
) -> dict[str, object]:
    return {
        "kind": "DATA_GAP",
        "canonical_market": market,
        "venue": venue,
        "stream_session_id": session,
        "recovery_generation": recovery,
        "gap_start_monotonic_ns": start,
        "gap_end_monotonic_ns": end,
        "reason": "TEST_GAP",
    }


@pytest.mark.parametrize(
    ("label", "interval", "expected"),
    (
        ("before", (10, 20), False),
        ("exact_boundary", (10, 30), True),
        ("inside", (10, 40), True),
        ("after", (40, 50), True),
    ),
)
def test_null_ended_gap_is_open_from_inclusive_start(
    label: str,
    interval: tuple[int, int],
    expected: bool,
) -> None:
    del label
    gap = _gap_test_gap(start=30, end=None)
    assert _gap_contaminates(
        gap,
        _gap_test_record(start=interval[0], end=interval[1]),
    ) is expected


@pytest.mark.parametrize(
    ("label", "interval", "gap_start", "gap_end", "expected"),
    (
        ("before", (10, 20), 30, 40, False),
        ("after", (50, 60), 30, 40, False),
        ("overlap_left", (20, 35), 30, 40, True),
        ("overlap_right", (35, 50), 30, 40, True),
        ("malformed_order", (10, 20), 40, 39, True),
        ("malformed_value", (10, 20), 30, "not-an-integer", True),
    ),
)
def test_finite_gap_boundaries_and_malformed_endpoints(
    label: str,
    interval: tuple[int, int],
    gap_start: object,
    gap_end: object,
    expected: bool,
) -> None:
    del label
    assert _gap_contaminates(
        _gap_test_gap(start=gap_start, end=gap_end),
        _gap_test_record(start=interval[0], end=interval[1]),
    ) is expected


def test_gap_identity_mismatches_do_not_contaminate_matching_interval() -> None:
    record = _gap_test_record(start=10, end=20)
    for field, value in (
        ("venue", "RISEX"),
        ("canonical_market", "ETH"),
        ("stream_session_id", "lighter-2"),
        ("recovery_generation", 1),
    ):
        gap = _gap_test_gap(start=10, end=20)
        gap[field] = value
        assert _gap_contaminates(gap, record) is False, field


def test_same_policy_recovery_generations_are_classified_independently() -> None:
    gap = _gap_test_gap(start=100, end=None)
    same_recovery = _gap_test_record(
        start=90,
        end=110,
        recovery=0,
        policy_id="same-policy",
    )
    recovered = _gap_test_record(
        start=90,
        end=110,
        recovery=1,
        policy_id="same-policy",
    )
    completed_before_gap = _gap_test_record(
        start=10,
        end=20,
        recovery=0,
        policy_id="same-policy",
    )
    assert _gap_contaminates(gap, same_recovery) is True
    assert _gap_contaminates(gap, recovered) is False
    assert _gap_contaminates(gap, completed_before_gap) is False


def test_missing_or_malformed_gap_and_record_timestamps_fail_closed() -> None:
    record = _gap_test_record()
    assert _gap_contaminates(
        _gap_test_gap(start=30, end=None),
        {**record, "horizon_deadline_monotonic_ns": 5},
    ) is True
    missing_end = _gap_test_gap(start=30)
    del missing_end["gap_end_monotonic_ns"]
    assert _gap_contaminates(missing_end, record) is True
    assert _gap_contaminates(
        _gap_test_gap(start="not-an-integer", end=40),
        record,
    ) is True


def test_report_attributes_each_horizon_to_only_overlapping_episode(
    tmp_path: Path,
) -> None:
    store = AppendOnlyEvidenceStore.create(
        tmp_path,
        metadata={"source_commit": "fixture", "evidence_mode": "FIXTURE"},
    )
    records: list[dict[str, object]] = []
    gap_offsets = (None, 100_000_000, 400_000_000, 700_000_000)
    for index, gap_offset in enumerate(gap_offsets):
        detected = index * 2_000_000_000 + 10
        version = f"episode-{index}"
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
                "quote_created_monotonic_ns": detected - 10,
                "quote_expires_monotonic_ns": detected + 1_000_000_000,
                "quote_stream_session_id": "risex",
                "quote_recovery_generation": 0,
                "hedge_stream_session_id": "lighter",
                "hedge_recovery_generation": 0,
                "maker_price": "99",
                "canonical_quantity": "1",
                "risex_tick_size": "1",
                "post_only_bound_price": "100",
                "observed_monotonic_ns": detected - 10,
            }
        )
        records.append(
            {
                "kind": "WOULD_FILL",
                "canonical_market": "BTC",
                "venue": "RISEX",
                "policy_id": "policy-1",
                "fillability_model": "STRICT_LOWER_BOUND",
                "quote_version_id": version,
                "direction": "RISEX_BUY_LIGHTER_SELL",
                "quote_created_monotonic_ns": detected - 10,
                "quote_stream_session_id": "risex",
                "quote_recovery_generation": 0,
                "hedge_stream_session_id": "lighter",
                "hedge_recovery_generation": 0,
                "canonical_quantity": "1",
                "cumulative_eligible_quantity": "1",
                "would_fill_detected_monotonic_ns": detected,
                "observed_monotonic_ns": detected,
            }
        )
        for horizon in (0, 300, 500, 1000):
            records.append(
                {
                    "kind": "HEDGE_HORIZON",
                    "canonical_market": "BTC",
                    "venue": "LIGHTER",
                    "policy_id": "policy-1",
                    "fillability_model": "STRICT_LOWER_BOUND",
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
                    "entry_edge_usd": str(index + 1),
                    "conditional_markout_usd": "0",
                    "observed_monotonic_ns": detected + horizon * 1_000_000,
                }
            )
        if gap_offset is not None:
            gap_start = detected + gap_offset
            records.append(
                {
                    "kind": "DATA_GAP",
                    "canonical_market": "BTC",
                    "venue": "LIGHTER",
                    "stream_session_id": "lighter",
                    "recovery_generation": 0,
                    "gap_start_monotonic_ns": gap_start,
                    "gap_end_monotonic_ns": gap_start + 1,
                    "reason": f"ROUND_{index}",
                    "observed_monotonic_ns": gap_start,
                }
            )
    records.append({"kind": "RUN_STOP", "fatal_reason": None, "observed_monotonic_ns": 10**15})
    store.append_batch(records)
    path = store.path
    store.close()

    report = build_report(path)
    assert report["strict_valid_episode_count"] == 1
    assert report["strict_contaminated_episode_count"] == 3
    for horizon, contaminated_count in ((0, 0), (300, 1), (500, 2), (1000, 3)):
        row = next(group for group in report["groups"] if group["horizon_ms"] == horizon)
        nested = row["fillability_models"]["STRICT_LOWER_BOUND"]["horizon"]
        assert nested["raw_observation_count"] == 4
        assert nested["valid_observation_count"] == 4 - contaminated_count
        assert nested["contaminated_observation_count"] == contaminated_count
        assert nested["raw_edge_count"] == 4
        assert nested["valid_edge_count"] == 1
        assert nested["contaminated_edge_count"] == contaminated_count
        assert nested["edge_excluded_by_fill_count"] == 3 - contaminated_count
        assert row["strict_valid_would_fill_count"] == 1
        assert row["strict_contaminated_would_fill_count"] == 3
        assert row["raw_full_hedge_rate"] == "1"
        assert row["valid_full_hedge_rate"] == "1"
        if contaminated_count:
            assert nested["contamination_reason_counts"] == {
                f"ROUND_{index}": 1 for index in range(1, contaminated_count + 1)
            }
        else:
            assert nested["contamination_reason_counts"] == {}


def test_dg006_replay_classifies_only_all_four_clean_horizon_episodes(
    tmp_path: Path,
) -> None:
    horizons = (0, 300, 500, 1000)
    gap_windows = (
        (1_000_000_000, 17_000_000_000),
        (30_000_000_000, 44_000_000_000),
        (60_000_000_000, 74_000_000_000),
    )
    strict_times = (
        tuple(1_000_000_000 + index * 2_000_000_000 for index in range(9))
        + tuple(30_000_000_000 + index * 2_000_000_000 for index in range(8))
        + tuple(60_000_000_000 + index * 2_000_000_000 for index in range(8))
        + tuple(100_000_000_000 + index * 2_000_000_000 for index in range(31))
    )
    optimistic_times = strict_times + (
        2_000_000_000,
        4_000_000_000,
        6_000_000_000,
        8_000_000_000,
    ) + tuple(200_000_000_000 + index * 2_000_000_000 for index in range(27))

    def overlaps_gap(detected: int) -> bool:
        interval = (detected, detected + 1_000_000_000)
        return any(
            not (gap_end < interval[0] or gap_start > interval[1])
            for gap_start, gap_end in gap_windows
        )

    expected = {
        "STRICT_LOWER_BOUND": (
            len(strict_times),
            sum(not overlaps_gap(detected) for detected in strict_times),
            sum(overlaps_gap(detected) for detected in strict_times),
        ),
        "OPTIMISTIC_UPPER_BOUND": (
            len(optimistic_times),
            sum(not overlaps_gap(detected) for detected in optimistic_times),
            sum(overlaps_gap(detected) for detected in optimistic_times),
        ),
    }
    assert expected == {
        "STRICT_LOWER_BOUND": (56, 31, 25),
        "OPTIMISTIC_UPPER_BOUND": (87, 58, 29),
    }

    store = AppendOnlyEvidenceStore.create(
        tmp_path,
        metadata={
            "source_commit": "4f83f8dea9f7a5deea4902f0c5cc6443e28004c1",
            "evidence_mode": "OBSERVATIONAL",
            "fillability_models": [
                "STRICT_LOWER_BOUND",
                "OPTIMISTIC_UPPER_BOUND",
            ],
        },
        run_id="dg006-shaped",
    )
    records: list[dict[str, object]] = []
    all_episodes = (
        ("STRICT_LOWER_BOUND", strict_times),
        ("OPTIMISTIC_UPPER_BOUND", optimistic_times),
    )
    for model, detected_times in all_episodes:
        model_prefix = "strict" if model == "STRICT_LOWER_BOUND" else "optimistic"
        for index, detected in enumerate(detected_times):
            version = f"{model_prefix}-{index:03d}"
            created = detected - 100_000_000
            common = {
                "canonical_market": "BTC",
                "direction": "RISEX_BUY_LIGHTER_SELL",
                "policy_id": "policy-dg006",
                "quote_version_id": version,
                "quote_created_monotonic_ns": created,
                "quote_expires_monotonic_ns": detected + 1_000_000_000,
                "quote_stream_session_id": "risex-dg006",
                "quote_recovery_generation": 0,
                "hedge_stream_session_id": "lighter-dg006",
                "hedge_recovery_generation": 0,
                "maker_price": "99",
                "canonical_quantity": "1",
                "risex_tick_size": "1",
                "post_only_bound_price": "100",
                "actual_edge_usd": "1",
            }
            records.append(
                {
                    "kind": "QUOTE",
                    **common,
                    "target_notional_usd": "100",
                    "target_margin_bps": "1",
                    "outcome": "QUOTE_ACTIVE",
                    "observed_monotonic_ns": created,
                }
            )
            records.append(
                {
                    "kind": "WOULD_FILL",
                    "venue": "RISEX",
                    "fillability_model": model,
                    **common,
                    "cumulative_eligible_quantity": "1",
                    "would_fill_detected_monotonic_ns": detected,
                    "observed_monotonic_ns": detected,
                }
            )
            for horizon in horizons:
                records.append(
                    {
                        "kind": "HEDGE_HORIZON",
                        "venue": "LIGHTER",
                        "fillability_model": model,
                        **common,
                        "target_notional_usd": "100",
                        "target_margin_bps": "1",
                        "horizon_ms": horizon,
                        "would_fill_detected_monotonic_ns": detected,
                        "horizon_deadline_monotonic_ns": (
                            detected + horizon * 1_000_000
                        ),
                        "expected_stream_session_id": "lighter-dg006",
                        "expected_recovery_generation": 0,
                        "outcome": "HEDGE_FULL",
                        "filled_quantity": "1",
                        "notional_usd": "99",
                        "entry_edge_usd": "1",
                        "conditional_markout_usd": "0",
                        "observed_monotonic_ns": (
                            detected + horizon * 1_000_000
                        ),
                    }
                )
    for index, (gap_start, gap_end) in enumerate(gap_windows, start=1):
        records.append(
            {
                "kind": "DATA_GAP",
                "canonical_market": "BTC",
                "venue": "LIGHTER",
                "stream_session_id": "lighter-dg006",
                "recovery_generation": 0,
                "gap_start_monotonic_ns": gap_start,
                "gap_end_monotonic_ns": gap_end,
                "reason": f"DG006_ROUND_{index}",
                "observed_monotonic_ns": gap_start,
            }
        )
    records.append(
        {
            "kind": "RUN_STOP",
            "fatal_reason": None,
            "observed_monotonic_ns": 200_000_000_000,
        }
    )
    store.append_batch(records)
    path = store.path
    store.close()

    report = build_report(path)
    assert report["horizon_record_count"] == 572
    assert report["strict_raw_episode_count"] == expected["STRICT_LOWER_BOUND"][0]
    assert report["strict_valid_episode_count"] == expected["STRICT_LOWER_BOUND"][1]
    assert report["strict_contaminated_episode_count"] == expected["STRICT_LOWER_BOUND"][2]
    assert report["optimistic_raw_episode_count"] == expected["OPTIMISTIC_UPPER_BOUND"][0]
    assert report["optimistic_valid_episode_count"] == expected["OPTIMISTIC_UPPER_BOUND"][1]
    assert report["optimistic_contaminated_episode_count"] == expected["OPTIMISTIC_UPPER_BOUND"][2]
    for group in report["groups"]:
        for model, (raw, valid, contaminated) in expected.items():
            horizon = group["fillability_models"][model]["horizon"]
            assert horizon["raw_observation_count"] == raw
            assert horizon["valid_observation_count"] == valid
            assert horizon["contaminated_observation_count"] == contaminated
    assert build_report(path) == report
