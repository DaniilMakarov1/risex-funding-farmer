from __future__ import annotations

from copy import deepcopy

from risex_spread_shadow import build_calibration_evidence


_MAKER_NARROW = "0x" + "1" * 48
_MAKER_WIDE = "0x" + "2" * 48
_MAKER_OPPOSITE_NARROW = "0x" + "3" * 48
_MAKER_OPPOSITE_WIDE = "0x" + "4" * 48
_MAKER_NARROW_X = "0x" + "5" * 48
_MAKER_NARROW_Y = "0x" + "6" * 48
_SHARED_TAKER = "0x" + "a" * 48
_OTHER_TAKER = "0x" + "b" * 48


def _maker_for(value: str) -> str:
    if "opposite-narrow" in value:
        return _MAKER_OPPOSITE_NARROW
    if "opposite-wide" in value:
        return _MAKER_OPPOSITE_WIDE
    return _MAKER_NARROW if "narrow" in value else _MAKER_WIDE


def _quote(
    policy_id: str,
    version: str,
    margin: str,
    maker_price: str,
    *,
    direction: str = "RISEX_SELL_LIGHTER_BUY",
) -> dict[str, object]:
    return {
        "kind": "QUOTE",
        "policy_id": policy_id,
        "quote_version_id": version,
        "canonical_market": "BTC",
        "direction": direction,
        "target_notional_usd": "100",
        "target_margin_bps": margin,
        "outcome": "QUOTE_ACTIVE",
        "quote_created_monotonic_ns": 100,
        "quote_expires_monotonic_ns": 1_000,
        "raw_risex_price_bound": maker_price,
        "post_only_bound_price": "99",
        "maker_price": maker_price,
        "risex_tick_size": "1",
        "canonical_quantity": "1",
        "maker_order_id": _maker_for(version),
        "observed_monotonic_ns": 100,
    }


def _trade(key: str, taker_order_id: object, *, side: str = "BUY") -> dict[str, object]:
    maker_order_id = _maker_for(key)
    trade_event_key = (
        f"RISEX|BTC/USDC|{maker_order_id}-{taker_order_id}"
        if isinstance(taker_order_id, str) and taker_order_id.startswith("0x")
        else key
    )
    return {
        "kind": "RISEX_TRADE",
        "canonical_market": "BTC",
        "trade_event_key": trade_event_key,
        "aggressor_side": side,
        "taker_order_id": taker_order_id,
        "maker_order_id": maker_order_id,
        "eligible_trade": True,
        "eligible_policy_ids": ["narrow", "wide"],
        "received_monotonic_ns": 110,
    }


def _trade_with_maker(maker_order_id: str, taker_order_id: str, *, side: str = "BUY") -> dict[str, object]:
    return {
        "kind": "RISEX_TRADE",
        "canonical_market": "BTC",
        "trade_event_key": f"RISEX|BTC/USDC|{maker_order_id}-{taker_order_id}",
        "aggressor_side": side,
        "taker_order_id": taker_order_id,
        "maker_order_id": maker_order_id,
        "eligible_trade": True,
        "eligible_policy_ids": ["narrow", "wide"],
        "received_monotonic_ns": 110,
    }


def _fill(policy_id: str, version: str, key: str, margin: str) -> dict[str, object]:
    return {
        "kind": "WOULD_FILL",
        "policy_id": policy_id,
        "quote_version_id": version,
        "canonical_market": "BTC",
        "direction": "RISEX_SELL_LIGHTER_BUY",
        "target_notional_usd": "100",
        "target_margin_bps": margin,
        "fillability_model": "STRICT_LOWER_BOUND",
        "qualifying_trade_event_keys": [key],
        "would_fill_detected_monotonic_ns": 120,
        "canonical_quantity": "1",
    }


def _horizon(version: str, horizon_ms: int, edge: str) -> dict[str, object]:
    return {
        "kind": "HEDGE_HORIZON",
        "quote_version_id": version,
        "fillability_model": "STRICT_LOWER_BOUND",
        "canonical_market": "BTC",
        "direction": "RISEX_SELL_LIGHTER_BUY",
        "horizon_ms": horizon_ms,
        "outcome": "HEDGE_FULL",
        "entry_edge_usd": edge,
        "conditional_markout_usd": "0.1",
    }


def _distinct_records() -> list[dict[str, object]]:
    narrow_trade = _trade("trade-narrow", _SHARED_TAKER)
    wide_trade = _trade("trade-wide", _SHARED_TAKER)
    return [
        _quote("narrow", "v-narrow", "1", "100"),
        _quote("wide", "v-wide", "2", "101"),
        narrow_trade,
        wide_trade,
        _fill("narrow", "v-narrow", narrow_trade["trade_event_key"], "1"),
        _fill("wide", "v-wide", wide_trade["trade_event_key"], "2"),
        *[_horizon("v-wide", horizon, "1.5") for horizon in (0, 300, 500, 1_000)],
    ]


def test_actual_tick_separation_pairs_shared_taker_and_reports_complete_curve() -> None:
    section = build_calibration_evidence(_distinct_records())

    assert section["identity"]["cluster_key"] == [
        "canonical_market",
        "aggressor_side",
        "taker_order_id",
    ]
    assert section["record_counts"]["venue_cluster"] == 1
    assert section["record_counts"]["clustered_filled_observation"] == 2
    effective = section["effective_level"]
    assert effective["paired_cluster_count"] == 1
    assert effective["distinct_wider_level_count"] == 1
    assert effective["effective_price_collision_count"] == 0
    pair = effective["paired_evidence"][0]
    assert pair["classification"] == "DISTINCT_EFFECTIVE_LEVEL"
    assert pair["signed_price_separation"]["minimum"] == "1"
    assert pair["signed_tick_separation"]["minimum"] == "1"
    assert pair["narrower_quote_versions"] == ["v-narrow"]
    assert pair["wider_quote_versions"] == ["v-wide"]
    assert pair["cluster_key"]["taker_order_id"] == _SHARED_TAKER

    observation = next(
        item for item in section["filled_observations"] if item["quote_version_id"] == "v-wide"
    )
    assert observation["raw_risex_price_bound"] == "101"
    assert observation["maker_price"] == "101"
    assert observation["maker_order_id"] == _MAKER_WIDE
    curve = section["distinct_wider_level_horizon_curves"][0]
    assert [row["horizon_ms"] for row in curve["horizons"]] == [0, 300, 500, 1_000]
    assert [row["observation_count"] for row in curve["horizons"]] == [1, 1, 1, 1]
    assert all(row["full_hedge_rate"] == "1" for row in curve["horizons"])


def test_equal_actual_prices_are_one_collision_and_repeated_versions_are_visible() -> None:
    records = _distinct_records()
    records.insert(2, _quote("wide", "v-wide-repeat", "2", "101"))
    repeat_trade = _trade("trade-wide-repeat", _SHARED_TAKER)
    records.insert(5, repeat_trade)
    records.insert(7, _fill("wide", "v-wide-repeat", repeat_trade["trade_event_key"], "2"))
    section = build_calibration_evidence(records)

    effective = section["effective_level"]
    assert effective["paired_cluster_count"] == 1
    assert effective["nominal_arm_pair_count"] == 1
    assert effective["effective_price_collision_count"] == 0
    assert effective["distinct_wider_level_count"] == 1
    pair = effective["paired_evidence"][0]
    assert pair["repeated_wider_quote_version_count"] == 1
    assert pair["wider_quote_versions"] == ["v-wide", "v-wide-repeat"]

    collision_records = _distinct_records()
    collision_records[1] = _quote("wide", "v-wide", "2", "100")
    section = build_calibration_evidence(collision_records)
    effective = section["effective_level"]
    assert effective["effective_price_collision_count"] == 1
    assert effective["distinct_wider_level_count"] == 0
    assert effective["paired_evidence"][0]["classification"] == "EFFECTIVE_PRICE_COLLISION"
    assert section["distinct_wider_level_horizon_curves"] == []


def test_margin_order_is_numeric_and_invalid_margins_do_not_pair() -> None:
    records = _distinct_records()
    for record in records:
        if record.get("kind") not in {"QUOTE", "WOULD_FILL"}:
            continue
        record["target_margin_bps"] = "2" if record.get("policy_id") == "narrow" else "10"
    section = build_calibration_evidence(records)
    pair = section["effective_level"]["paired_evidence"][0]
    assert pair["narrower_target_margin_bps"] == "2"
    assert pair["wider_target_margin_bps"] == "10"

    invalid = _distinct_records()
    for record in invalid:
        if record.get("kind") in {"QUOTE", "WOULD_FILL"}:
            record["target_margin_bps"] = "-1"
    assert build_calibration_evidence(invalid)["effective_level"]["paired_evidence"] == []


def test_duplicate_horizon_count_and_selection_are_order_independent() -> None:
    records = _distinct_records()
    wide_trade_key = records[3]["trade_event_key"]
    records.append(
        {
            **_horizon("v-wide", 500, "9.9"),
        }
    )
    records.append(
        {
            **_fill("wide", "v-wide", wide_trade_key, "2"),
            "canonical_quantity": "2",
            "would_fill_detected_monotonic_ns": 999,
        }
    )
    records.append(_quote("wide", "v-wide", "2", "102"))
    forward = build_calibration_evidence(records)
    reverse = build_calibration_evidence(list(reversed(deepcopy(records))))
    assert forward == reverse
    assert forward["identity"]["duplicate_horizon_record_count"] == 1
    assert forward["identity"]["duplicate_fill_record_count"] == 1
    row = forward["distinct_wider_level_horizon_curves"][0]["horizons"][2]
    assert row["full_hedge_rate"] == "1"
    assert row["entry_edge_usd"]["mean"] == "1.5"
    observation = next(item for item in forward["filled_observations"] if item["quote_version_id"] == "v-wide")
    assert observation["canonical_quantity"] == "1"
    assert observation["maker_price"] == "101"


def test_fill_identity_without_an_exact_trade_key_stays_unclustered() -> None:
    records = _distinct_records()
    for record in records:
        if record.get("kind") == "WOULD_FILL":
            record["qualifying_trade_event_keys"] = []
            record["taker_order_id"] = "copied-without-trade"
            record["aggressor_side"] = "BUY"
    section = build_calibration_evidence(records)
    assert section["effective_level"]["paired_evidence"] == []
    assert section["record_counts"]["clustered_filled_observation"] == 0
    assert all(item["identity_status"] == "UNCLUSTERED" for item in section["filled_observations"])


def test_cross_market_trade_key_stays_unclustered() -> None:
    records = _distinct_records()
    bad_key = f"RISEX|ETH/USDC|{_MAKER_NARROW}-{_SHARED_TAKER}"
    records[2]["trade_event_key"] = bad_key
    records[4]["qualifying_trade_event_keys"] = [bad_key]
    section = build_calibration_evidence(records)

    narrow = next(item for item in section["filled_observations"] if item["quote_version_id"] == "v-narrow")
    assert narrow["identity_status"] == "UNCLUSTERED"
    assert "TRADE_EVENT_KEY_MARKET_CONFLICT" in narrow["identity_issues"]
    assert section["effective_level"]["paired_evidence"] == []


def test_exact_shared_event_pairing_does_not_cross_product_unrelated_keys() -> None:
    records = [
        {**_quote("narrow", "v-narrow-x", "1", "100"), "maker_order_id": _MAKER_NARROW_X},
        {**_quote("narrow", "v-narrow-y", "1", "102"), "maker_order_id": _MAKER_NARROW_Y},
        {**_quote("wide", "v-wide-x", "2", "101"), "maker_order_id": _MAKER_NARROW_X},
        {**_quote("wide", "v-wide-y", "2", "103"), "maker_order_id": _MAKER_NARROW_Y},
        _trade_with_maker(_MAKER_NARROW_X, _SHARED_TAKER),
        _trade_with_maker(_MAKER_NARROW_Y, _SHARED_TAKER),
    ]
    records.extend(
        (
            _fill("narrow", "v-narrow-x", records[4]["trade_event_key"], "1"),
            _fill("narrow", "v-narrow-y", records[5]["trade_event_key"], "1"),
            _fill("wide", "v-wide-x", records[4]["trade_event_key"], "2"),
            _fill("wide", "v-wide-y", records[5]["trade_event_key"], "2"),
        )
    )
    section = build_calibration_evidence(records)
    pair = section["effective_level"]["paired_evidence"][0]

    assert pair["classification"] == "DISTINCT_EFFECTIVE_LEVEL"
    assert pair["comparison_count"] == 2
    assert {
        (item["narrower_observation_id"], item["wider_observation_id"])
        for item in pair["comparisons"]
    } == {
        ("STRICT_LOWER_BOUND|v-narrow-x", "STRICT_LOWER_BOUND|v-wide-x"),
        ("STRICT_LOWER_BOUND|v-narrow-y", "STRICT_LOWER_BOUND|v-wide-y"),
    }


def test_opposite_direction_and_missing_or_malformed_identity_never_pair_by_time() -> None:
    opposite = _distinct_records()
    opposite_narrow_trade = _trade("opposite-narrow-trade", _SHARED_TAKER, side="SELL")
    opposite_wide_trade = _trade("opposite-wide-trade", _SHARED_TAKER, side="SELL")
    opposite.extend(
        (
            _quote(
                "opposite-narrow",
                "v-opposite-narrow",
                "1",
                "100",
                direction="RISEX_BUY_LIGHTER_SELL",
            ),
            _quote(
                "opposite-wide",
                "v-opposite-wide",
                "2",
                "99",
                direction="RISEX_BUY_LIGHTER_SELL",
            ),
            {**opposite_narrow_trade, "eligible_policy_ids": ["opposite-narrow", "opposite-wide"]},
            {**opposite_wide_trade, "eligible_policy_ids": ["opposite-narrow", "opposite-wide"]},
            {
                **_fill("opposite-narrow", "v-opposite-narrow", opposite_narrow_trade["trade_event_key"], "1"),
                "direction": "RISEX_BUY_LIGHTER_SELL",
            },
            {
                **_fill("opposite-wide", "v-opposite-wide", opposite_wide_trade["trade_event_key"], "2"),
                "direction": "RISEX_BUY_LIGHTER_SELL",
            },
        )
    )
    opposite_section = build_calibration_evidence(opposite)
    assert opposite_section["effective_level"]["paired_cluster_count"] == 2
    assert {
        pair["direction"] for pair in opposite_section["effective_level"]["paired_evidence"]
    } == {"RISEX_SELL_LIGHTER_BUY", "RISEX_BUY_LIGHTER_SELL"}
    assert all(
        pair["classification"] == "DISTINCT_EFFECTIVE_LEVEL"
        for pair in opposite_section["effective_level"]["paired_evidence"]
    )

    missing = _distinct_records()
    missing_trade = {**_trade("trade-narrow", _SHARED_TAKER), "trade_event_key": "RISEX|BTC|malformed", "taker_order_id": None}
    other_trade = _trade("trade-wide", _OTHER_TAKER)
    missing[2] = missing_trade
    missing[3] = {**other_trade, "eligible_policy_ids": ["wide"]}
    missing[4]["qualifying_trade_event_keys"] = [missing_trade["trade_event_key"]]
    missing[5]["qualifying_trade_event_keys"] = [other_trade["trade_event_key"]]
    missing_section = build_calibration_evidence(missing)
    assert missing_section["effective_level"]["paired_evidence"] == []
    assert missing_section["identity"]["unclustered_filled_observation_count"] >= 1
    assert missing_section["identity"]["identity_issue_counts"]["TAKER_ORDER_ID_MISSING"] >= 1

    malformed = _distinct_records()
    malformed_trade = {**_trade("trade-narrow", _SHARED_TAKER), "trade_event_key": "RISEX|BTC|malformed", "taker_order_id": []}
    malformed[2] = malformed_trade
    malformed[4]["qualifying_trade_event_keys"] = [malformed_trade["trade_event_key"]]
    malformed_section = build_calibration_evidence(malformed)
    assert malformed_section["effective_level"]["paired_evidence"] == []
    assert malformed_section["identity"]["identity_issue_counts"]["TAKER_ORDER_ID_MALFORMED"] >= 1

    contradictory = _distinct_records()
    contradictory[2] = {**contradictory[2], "taker_order_id": _OTHER_TAKER}
    contradictory_section = build_calibration_evidence(contradictory)
    assert contradictory_section["effective_level"]["paired_evidence"] == []
    assert contradictory_section["identity"]["identity_issue_counts"]["TAKER_ORDER_ID_CONFLICT"] >= 1


def test_calibration_is_deterministic_under_record_order_and_does_not_use_time_proxy() -> None:
    records = _distinct_records()
    shuffled = list(reversed(deepcopy(records)))
    assert build_calibration_evidence(records) == build_calibration_evidence(shuffled)
