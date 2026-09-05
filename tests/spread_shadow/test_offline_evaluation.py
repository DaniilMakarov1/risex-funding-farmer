from __future__ import annotations

from decimal import Decimal

from risex_farmer.models import BookLevel, Venue
import risex_spread_shadow.offline_evaluation as offline_evaluation
from risex_spread_shadow import build_offline_evaluation
from risex_spread_shadow.book_chain import BookRevisionEncoder
from risex_spread_shadow.book_chain import book_state_sha256
from risex_spread_shadow.models import BookEvidence
from risex_spread_shadow.offline_evaluation import (
    _POLICY_FINGERPRINT,
    _compact_book,
    _Unit,
    _fingerprint,
    _pair_unit,
    _quote_from_record,
    _replay_books,
    _stage_contract,
    _stats,
)


def _quote(
    policy: str,
    version: str,
    margin: str,
    price: str,
    *,
    created: int = 100,
    expiry: int = 1_000,
) -> dict[str, object]:
    return {
        "kind": "QUOTE",
        "policy_id": policy,
        "quote_version_id": version,
        "canonical_market": "BTC",
        "direction": "RISEX_SELL_LIGHTER_BUY",
        "target_notional_usd": "100",
        "target_margin_bps": margin,
        "outcome": "QUOTE_ACTIVE",
        "quote_created_monotonic_ns": created,
        "quote_expires_monotonic_ns": expiry,
        "quote_stream_session_id": "rh",
        "quote_recovery_generation": 0,
        "hedge_stream_session_id": "lh",
        "hedge_recovery_generation": 0,
        "maker_price": price,
        "risex_tick_size": "1",
        "canonical_quantity": "1",
        "risex_maker_fee_rate": "0.0001",
        "lighter_taker_fee_rate": "0",
    }


def _trade(
    key: str,
    maker: str,
    taker: str,
    *,
    received: int = 110,
    price: str = "102",
) -> dict[str, object]:
    return {
        "kind": "RISEX_TRADE",
        "canonical_market": "BTC",
        "trade_event_key": f"RISEX|BTC/USDC|{maker}-{taker}",
        "maker_order_id": maker,
        "taker_order_id": taker,
        "aggressor_side": "BUY",
        "canonical_price": price,
        "canonical_quantity": "1",
        "received_monotonic_ns": received,
        "stream_session_id": "rh",
        "recovery_generation": 0,
        "eligible_trade": True,
        "eligible_policy_ids": ["n", "w"],
        "test_key": key,
    }


def _fill(
    version: str,
    keys: list[str],
    *,
    cumulative: str = "1",
    detected: int = 110,
) -> dict[str, object]:
    return {
        "kind": "WOULD_FILL",
        "policy_id": "n" if version.startswith("vn") else "w",
        "quote_version_id": version,
        "canonical_market": "BTC",
        "direction": "RISEX_SELL_LIGHTER_BUY",
        "canonical_quantity": "1",
        "cumulative_eligible_quantity": cumulative,
        "qualifying_trade_event_keys": keys,
        "would_fill_detected_monotonic_ns": detected,
        "fillability_model": "STRICT_LOWER_BOUND",
    }


def _book(
    received: int = 110,
    revision: int = 1,
    *,
    ask: str = "99",
) -> tuple[BookEvidence, dict[str, object]]:
    evidence = BookEvidence(
        Venue.LIGHTER,
        "BTC",
        (BookLevel(Decimal("98"), Decimal("2")),),
        (BookLevel(Decimal(ask), Decimal("2")),),
        received,
        "lh",
        0,
        revision,
        sequence=revision,
    )
    return evidence, BookRevisionEncoder().encode(evidence, source_kind="SNAPSHOT")


def _horizon(
    evidence: BookEvidence,
    encoded: dict[str, object],
    version: str,
    horizon: int,
    *,
    detected: int = 110,
    notional: str = "99",
    vwap: str = "99",
    edge: str = "0.99",
) -> dict[str, object]:
    return {
        "kind": "HEDGE_HORIZON",
        "quote_version_id": version,
        "fillability_model": "STRICT_LOWER_BOUND",
        "canonical_market": "BTC",
        "direction": "RISEX_SELL_LIGHTER_BUY",
        "horizon_ms": horizon,
        "would_fill_detected_monotonic_ns": detected,
        "horizon_deadline_monotonic_ns": detected + horizon * 1_000_000,
        "expected_stream_session_id": "lh",
        "expected_recovery_generation": 0,
        "outcome": "HEDGE_FULL",
        "requested_quantity": "1",
        "filled_quantity": "1",
        "notional_usd": notional,
        "vwap_price": vwap,
        "entry_edge_usd": edge,
        "conditional_markout_usd": "999",  # diagnostic only; h0 is recomputed
        "freshness_max_age_ns": 2_000_000_000,
        "book_received_monotonic_ns": evidence.received_monotonic_ns,
        "book_stream_session_id": evidence.stream_session_id,
        "book_recovery_generation": evidence.recovery_generation,
        "book_revision": evidence.book_revision,
        "book_revision_id": evidence.book_revision_id,
        "book_state_sha256": encoded["book_state_sha256"],
    }


def _complete_stage_metadata() -> dict[str, object]:
    fees = {
        "risex_maker_fee_rate": "0.0001",
        "risex_fee_tier": "TIER_1",
        "risex_fee_provenance": "official",
        "lighter_taker_fee_rate": "0",
        "lighter_taker_latency_ms": "300",
        "lighter_fee_provenance": "official",
    }
    limits = {
        "eligible_trade_limit": 250,
        "wall_clock_seconds": 1200,
        "record_cap": 1_000_000,
        "byte_cap": 4 * 1024 * 1024 * 1024,
        "fill_count_stop": None,
    }
    stage: dict[str, object] = {
        "stage_kind": "PUBLIC",
        "stage_name": "SCAN-002",
        "run_id": "synthetic-run",
        "accepted_source": "accepted-main",
        "canonical_markets": ["BTC"],
        "direction": "RISEX_SELL_LIGHTER_BUY",
        "target_notionals_usd": ["100"],
        "target_margins_bps": ["1", "2"],
        "horizons_ms": [0, 300, 500, 1000],
        "fees": fees,
        "sample_interval": {"start_monotonic_ns": 0, "end_monotonic_ns": 800_000_000_000},
        "limits": limits,
        "policy_fingerprint": _POLICY_FINGERPRINT,
    }
    stage["stage_fingerprint"] = _fingerprint(
        {
            "stage_kind": stage["stage_kind"],
            "stage_name": stage["stage_name"],
            "run_id": stage["run_id"],
            "accepted_source": stage["accepted_source"],
            "policy_fields": {
                "canonical_markets": stage["canonical_markets"],
                "direction": stage["direction"],
                "target_notionals_usd": stage["target_notionals_usd"],
                "target_margins_bps": stage["target_margins_bps"],
                "horizons_ms": stage["horizons_ms"],
                "fees": fees,
            },
            "sample_interval": stage["sample_interval"],
            "limits": limits,
            "policy_fingerprint": _POLICY_FINGERPRINT,
        }
    )
    return stage


def _qualified_records(
    *,
    wide_fill_indices: set[int] | None = None,
    negative_unit: int | None = None,
    default_ask: str = "99",
) -> list[dict[str, object]]:
    wide_fill_indices = set(range(50)) if wide_fill_indices is None else wide_fill_indices
    records: list[dict[str, object]] = [
        {"kind": "RUN_METADATA", "metadata": {"run_id": "synthetic-run", "scan_002": _complete_stage_metadata()}},
    ]
    for index in range(50):
        detected = 10_000_000_000 + index * 15_000_000_000
        created = detected - 100
        expiry = detected + 10_000_000_000
        narrow_version = f"vn-{index}"
        wide_version = f"vw-{index}"
        ask = "1000" if index == negative_unit else default_ask
        trade_price = "102" if index in wide_fill_indices or negative_unit == index else "101"
        book, book_record = _book(received=detected, revision=index + 1, ask=ask)
        records.extend(
            [
                _quote("n", narrow_version, "1", "100", created=created, expiry=expiry),
                _quote("w", wide_version, "2", "101", created=created, expiry=expiry),
                _trade(
                    f"trade-{index}",
                    f"m{index}",
                    f"t{index}",
                    received=detected,
                    price=trade_price,
                ),
                _fill(narrow_version, [f"RISEX|BTC/USDC|m{index}-t{index}"], detected=detected),
            ]
        )
        if index in wide_fill_indices:
            records.append(
                _fill(wide_version, [f"RISEX|BTC/USDC|m{index}-t{index}"], detected=detected)
            )
        narrow_edge = str(Decimal("100") - Decimal(ask) - Decimal("0.01"))
        wide_edge = str(Decimal("101") - Decimal(ask) - Decimal("0.0101"))
        for version, edge in ((narrow_version, narrow_edge), (wide_version, wide_edge)):
            if version == wide_version and index not in wide_fill_indices:
                continue
            for horizon in (0, 300, 500, 1000):
                records.append(
                    _horizon(
                        book,
                        book_record,
                        version,
                        horizon,
                        detected=detected,
                        notional=ask,
                        vwap=ask,
                        edge=edge,
                    )
                )
        records.append(book_record)
    records.append({"kind": "RUN_STOP", "fatal_reason": None})
    return records


def test_transitive_order_and_cumulative_bridge_make_one_dependence_unit() -> None:
    key_one = "RISEX|BTC/USDC|m1-t1"
    key_two = "RISEX|BTC/USDC|m2-m1"
    key_three = "RISEX|BTC/USDC|m3-t3"
    records = [
        _quote("n", "vn", "1", "100"),
        _quote("w", "vw", "2", "101"),
        _trade("one", "m1", "t1"),
        _trade("two", "m2", "m1"),
        _trade("three", "m3", "t3"),
        _fill("vn", [key_one, key_three], cumulative="2"),
    ]
    result = build_offline_evaluation(records)
    assert result["population"]["raw_unit_count"] == 1
    assert result["population"]["units"][0]["eligible_trade_count"] == 3
    assert result["population"]["units"][0]["strict_fill_versions"] == ["vn"]


def test_quote_snapshot_from_trade_context_survives_nominal_replacement() -> None:
    key = "RISEX|BTC/USDC|m-t"
    records = [
        _quote("n", "vn", "1", "100"),
        _trade("one", "m", "t"),
        _quote("n", "vn2", "1", "110"),
        _fill("vn", [key]),
    ]
    result = build_offline_evaluation(records)
    unit = result["population"]["units"][0]
    assert unit["strict_fill_versions"] == ["vn"]
    assert "QUOTE_VERSION_MISSING" not in unit["reasons"]


def test_null_ended_gap_contaminates_a_no_fill_unit() -> None:
    records = [
        _quote("n", "vn", "1", "100"),
        _quote("w", "vw", "2", "101"),
        _trade("one", "m", "t", price="100"),
        {
            "kind": "DATA_GAP",
            "venue": "RISEX",
            "canonical_market": "BTC",
            "stream_session_id": "rh",
            "recovery_generation": 0,
            "gap_start_monotonic_ns": 110,
            "gap_end_monotonic_ns": None,
            "reason": "TRANSPORT",
        },
    ]
    result = build_offline_evaluation(records)
    assert result["population"]["contaminated_unit_count"] == 1
    assert result["population"]["units"][0]["status"] == "CONTAMINATED"


def test_pairing_is_same_event_and_classifies_zero_plus_positive_as_collision() -> None:
    narrow_zero = _quote_from_record(_quote("n", "n0", "1", "100"))
    wide_zero = _quote_from_record(_quote("w", "w0", "2", "100"))
    narrow_positive = _quote_from_record(_quote("n", "n1", "1", "100"))
    wide_positive = _quote_from_record(_quote("w", "w1", "2", "101"))
    assert narrow_zero and wide_zero and narrow_positive and wide_positive
    unit = _Unit("u")
    unit.active_by_arm = {
        "1": {"event-zero": (narrow_zero,), "event-positive": (narrow_positive,)},
        "2": {"event-zero": (wide_zero,), "event-positive": (wide_positive,)},
    }
    _pair_unit(unit)
    assert unit.pair_classification == "EFFECTIVE_PRICE_COLLISION"

    reversed_unit = _Unit("reversed")
    reversed_quote = _quote_from_record(_quote("w", "wr", "2", "99"))
    assert reversed_quote
    reversed_unit.active_by_arm = {"1": {"event": (narrow_positive,)}, "2": {"event": (reversed_quote,)}}
    _pair_unit(reversed_unit)
    assert reversed_unit.pair_classification == "NOMINAL_WIDER_REVERSED"

    mixed_unit = _Unit("mixed")
    mixed_unit.active_by_arm = {
        "1": {"positive": (narrow_positive,), "negative": (narrow_positive,)},
        "2": {"positive": (wide_positive,), "negative": (reversed_quote,)},
    }
    _pair_unit(mixed_unit)
    assert mixed_unit.pair_classification == "MIXED_EFFECTIVE_LEVEL"

    unresolved_unit = _Unit("unresolved")
    unresolved_unit.active_by_arm = {"1": {"event": (narrow_positive,)}, "2": {}}
    _pair_unit(unresolved_unit)
    assert unresolved_unit.pair_classification == "EFFECTIVE_LEVEL_UNRESOLVED"


def test_exact_witness_and_h0_markout_ignore_stored_conditional_markout() -> None:
    evidence, book_record = _book()
    key = "RISEX|BTC/USDC|m-t"
    records: list[dict[str, object]] = [
        {"kind": "REPLAY_MODE"},
        _quote("n", "vn", "1", "100"),
        _quote("w", "vw", "2", "101"),
        _trade("one", "m", "t"),
        _fill("vn", [key]),
        _fill("vw", [key]),
    ]
    for version in ("vn", "vw"):
        for horizon in (0, 300, 500, 1000):
            row = _horizon(evidence, book_record, version, horizon)
            if version == "vw":
                row["entry_edge_usd"] = "1.9899"
            records.append(row)
    records.append(book_record)
    result = build_offline_evaluation(records)
    assert result["population"]["units"][0]["status"] == "CLEAN"
    assert Decimal(result["arms"]["1"]["conditional_markout_scores"]["300"]["sum"]) == 0


def test_older_valid_selected_book_cannot_pass_when_a_newer_book_was_available() -> None:
    expected, expected_record = _book(received=110, revision=1)
    _newer, newer_record = _book(received=120, revision=2)
    key = "RISEX|BTC/USDC|m-t"
    records: list[dict[str, object]] = [
        {"kind": "REPLAY_MODE"},
        _quote("n", "vn", "1", "100"),
        _quote("w", "vw", "2", "101"),
        _trade("one", "m", "t"),
        _fill("vn", [key]),
        _fill("vw", [key]),
        expected_record,
        newer_record,
    ]
    for version in ("vn", "vw"):
        for horizon in (0, 300, 500, 1000):
            row = _horizon(expected, expected_record, version, horizon)
            if version == "vw":
                row["entry_edge_usd"] = "1.9899"
            records.append(row)
    result = build_offline_evaluation(records)
    assert result["population"]["units"][0]["status"] == "CONTAMINATED"
    assert "SELECTED_BOOK_NOT_LATEST_BEFORE_DEADLINE" in result["population"]["units"][0]["reasons"]


def test_deep_book_witness_keeps_only_exact_q_asks_and_identity_digest() -> None:
    deep = BookEvidence(
        Venue.LIGHTER,
        "BTC",
        (BookLevel(Decimal("98"), Decimal("1")),),
        tuple(BookLevel(Decimal(100 + index), Decimal("1")) for index in range(2_000)),
        110,
        "lh",
        0,
        1,
        sequence=1,
    )
    compact = _compact_book(deep, Decimal("1"))
    assert len(compact.asks) == 1
    assert compact.bids == ()
    assert compact.state_sha256 == book_state_sha256(deep)


def test_replay_hashes_one_revision_once_across_horizon_requests(monkeypatch) -> None:
    evidence, book_record = _book()
    calls = 0
    original = offline_evaluation.book_state_sha256

    def counted(book):
        nonlocal calls
        calls += 1
        return original(book)

    monkeypatch.setattr(offline_evaluation, "book_state_sha256", counted)
    reference = (
        "LIGHTER",
        "BTC",
        "lh",
        0,
        1,
        evidence.book_revision_id,
    )
    requests = {
        ("STRICT_LOWER_BOUND", "v", horizon): ("BTC", 1_000_000_000, "lh", 0, Decimal("1"))
        for horizon in (0, 300, 500, 1000)
    }
    _replay_books(
        lambda: iter((book_record,)),
        False,
        [],
        {reference: Decimal("1")},
        requests,
    )
    assert calls == 1


def test_complete_fixture_reaches_math_qualification_but_not_stage_admission() -> None:
    result = build_offline_evaluation(_qualified_records())
    assert result["population"]["clean_unit_count"] == 50
    assert result["effective_level_pairing"]["paired_clean_unit_count"] == 50
    assert result["mathematical_verdict"] == "NUMERICAL_QUALIFIED"
    assert result["selector"]["selected_margin_bps"] == "2"
    assert result["stage_verdict"] == "DATA_INSUFFICIENT"
    assert result["stage_qualified"] is False
    assert result["candidate_eligible"] is False
    assert result["failed_gates"] == []

    fixture_result = build_offline_evaluation([{"kind": "REPLAY_MODE"}, *_qualified_records()])
    assert fixture_result["mathematical_verdict"] == "NUMERICAL_QUALIFIED"
    assert fixture_result["stage_verdict"] == "FIXTURE_ONLY"
    assert fixture_result["stage_qualified"] is False


def test_exact_percentile_and_inclusive_exclusive_edge_boundaries() -> None:
    stats = _stats([Decimal("-1"), *([Decimal("0.01")] * 19)])
    assert stats["p05"] == "-1"
    assert stats["median"] == "0.01"
    assert Decimal("0.01") >= Decimal("0.01")
    assert Decimal("0.005") >= Decimal("0.005")
    assert not (Decimal("0") > Decimal("0"))
    assert Decimal("0.0001") > Decimal("0")

    result = build_offline_evaluation(_qualified_records(default_ask="99.98"))
    assert result["mathematical_verdict"] == "NUMERICAL_QUALIFIED"
    assert not any(
        name.startswith("ARM_1_EDGE_")
        for name in result["failed_gates"]
    )


def test_one_arm_can_qualify_with_common_denominator_and_pairing_intact() -> None:
    wide_indices = set(range(10)) | set(range(20, 30))
    result = build_offline_evaluation(_qualified_records(wide_fill_indices=wide_indices))
    assert result["population"]["common_eligible_unit_count"] == 50
    assert result["arms"]["2"]["clean_filled_unit_count"] == 20
    assert result["selector"]["arm_scores"]["2"]["denominator"] == 50
    concentration = result["arms"]["2"]["concentration"]
    assert concentration["one_minute"]["unit_count"] == 20
    assert concentration["five_minute"]["unit_count"] == 20
    assert result["selector"]["arm_qualifies"]["1"] is True
    assert result["selector"]["selected_margin_bps"] == "1"
    assert result["selector"]["selection_pass"] is True
    assert result["mathematical_verdict"] == "NUMERICAL_QUALIFIED"


def test_tie_selector_prefers_one_bps_and_rare_loss_preserves_negative_sum_failure() -> None:
    assert offline_evaluation._select_arm(
        {"1": True, "2": True},
        {"1": {"sum_300ms": "1"}, "2": {"sum_300ms": "1"}},
    ) == "1"
    assert offline_evaluation._select_arm(
        {"1": False, "2": True},
        {"1": {"sum_300ms": "999"}, "2": {"sum_300ms": "1"}},
    ) == "2"

    result = build_offline_evaluation(_qualified_records(negative_unit=0))
    stats = result["arms"]["1"]["horizon_scores"]["0"]
    assert int(stats["negative_count"]) == 1
    assert Decimal(stats["sum"]) < 0
    assert "ARM_1_POSITIVE_SUM_0MS" in result["failed_gates"]
    assert result["mathematical_verdict"] == "NUMERICAL_FAILED"


def test_incomplete_and_contaminated_units_remain_visible_and_block_admission() -> None:
    missing_fill = _qualified_records()
    missing_fill = [
        row
        for row in missing_fill
        if not (
            row.get("quote_version_id") == "vw-0"
            and row.get("kind") in {"WOULD_FILL", "HEDGE_HORIZON"}
        )
    ]
    missing_fill_result = build_offline_evaluation(missing_fill)
    assert "STRICT_EPISODE_MISSING" in missing_fill_result["population"]["units"][0]["reasons"]
    assert missing_fill_result["population"]["unresolved_unit_count"] == 1

    incomplete = _qualified_records()
    incomplete = [
        row
        for row in incomplete
        if not (
            row.get("kind") == "HEDGE_HORIZON"
            and row.get("quote_version_id") == "vn-0"
            and row.get("horizon_ms") == 500
        )
    ]
    incomplete_result = build_offline_evaluation(incomplete)
    assert incomplete_result["population"]["unresolved_unit_count"] == 1
    assert "ALL_ELIGIBLE_UNITS_CLEAN" in incomplete_result["failed_gates"]

    contaminated = _qualified_records()
    contaminated.insert(
        1,
        {
            "kind": "DATA_GAP",
            "venue": "RISEX",
            "canonical_market": "BTC",
            "stream_session_id": "rh",
            "recovery_generation": 0,
            "gap_start_monotonic_ns": 10_000_000_000,
            "gap_end_monotonic_ns": 10_000_000_001,
            "reason": "TRANSPORT",
        },
    )
    contaminated_result = build_offline_evaluation(contaminated)
    assert contaminated_result["population"]["contaminated_unit_count"] == 1
    assert "ALL_ELIGIBLE_UNITS_CLEAN" in contaminated_result["failed_gates"]


def test_duplicate_partial_and_tampered_horizon_or_fee_evidence_fails_closed() -> None:
    duplicate = _qualified_records()
    original = next(
        row
        for row in duplicate
        if row.get("kind") == "HEDGE_HORIZON"
        and row.get("quote_version_id") == "vn-0"
        and row.get("horizon_ms") == 0
    )
    duplicate.append({**original, "notional_usd": "98"})
    duplicate_result = build_offline_evaluation(duplicate)
    assert "DUPLICATE_HORIZON_CONFLICT" in duplicate_result["integrity_issues"]
    assert duplicate_result["stage_verdict"] == "DATA_INSUFFICIENT"

    notional = _qualified_records()
    next(
        row
        for row in notional
        if row.get("kind") == "HEDGE_HORIZON"
        and row.get("quote_version_id") == "vn-0"
        and row.get("horizon_ms") == 0
    )["notional_usd"] = "98"
    notional_result = build_offline_evaluation(notional)
    assert "HEDGE_NOTIONAL_MISMATCH" in notional_result["population"]["units"][0]["reasons"]

    fee = _qualified_records()
    next(row for row in fee if row.get("kind") == "QUOTE" and row.get("quote_version_id") == "vn-0")[
        "risex_maker_fee_rate"
    ] = "0.2"
    fee_result = build_offline_evaluation(fee)
    assert "ENTRY_EDGE_ARITHMETIC_MISMATCH" in fee_result["population"]["units"][0]["reasons"]


def test_stage_fingerprints_are_checked_but_admission_stays_closed() -> None:
    stage = {
        "stage_kind": "PUBLIC",
        "stage_name": "SCAN-002",
        "run_id": "run-1",
        "accepted_source": "accepted-main",
        "canonical_markets": ["BTC"],
        "direction": "RISEX_SELL_LIGHTER_BUY",
        "target_notionals_usd": ["100"],
        "target_margins_bps": ["1", "2"],
        "horizons_ms": [0, 300, 500, 1000],
        "fees": {
            "risex_maker_fee_rate": "0.0001",
            "risex_fee_tier": "TIER_1",
            "risex_fee_provenance": "official",
            "lighter_taker_fee_rate": "0",
            "lighter_taker_latency_ms": "300",
            "lighter_fee_provenance": "official",
        },
        "sample_interval": {"start_monotonic_ns": 1, "end_monotonic_ns": 2},
        "limits": {
            "eligible_trade_limit": 250,
            "wall_clock_seconds": 1200,
            "record_cap": 1_000_000,
            "byte_cap": 4 * 1024 * 1024 * 1024,
            "fill_count_stop": None,
        },
        "policy_fingerprint": _POLICY_FINGERPRINT,
    }
    fingerprint_payload = {
        "stage_kind": "PUBLIC",
        "stage_name": "SCAN-002",
        "run_id": "run-1",
        "accepted_source": "accepted-main",
        "policy_fields": {
            "canonical_markets": ["BTC"],
            "direction": "RISEX_SELL_LIGHTER_BUY",
            "target_notionals_usd": ["100"],
            "target_margins_bps": ["1", "2"],
            "horizons_ms": [0, 300, 500, 1000],
            "fees": stage["fees"],
        },
        "sample_interval": {"start_monotonic_ns": 1, "end_monotonic_ns": 2},
        "limits": stage["limits"],
        "policy_fingerprint": _POLICY_FINGERPRINT,
    }
    stage["stage_fingerprint"] = _fingerprint(fingerprint_payload)
    provenance = _stage_contract({"scan_002": stage}, "OBSERVATIONAL", "run-1")
    assert provenance["valid"] is True

    result = build_offline_evaluation(
        [{"kind": "RUN_METADATA", "metadata": {"run_id": "run-1", "scan_002": stage}}, {"kind": "RUN_STOP"}]
    )
    assert result["stage_qualified"] is False
    assert result["stage_admission"]["status"] == "CLOSED_PENDING_SCAN_003"
    assert result["candidate_eligible"] is False
