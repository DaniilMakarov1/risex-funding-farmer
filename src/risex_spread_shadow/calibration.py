"""Descriptive effective-level and venue-cluster evidence for offline reports."""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, InvalidOperation
from itertools import combinations
import json
import re
from typing import Any, Iterable


_HORIZONS = (0, 300, 500, 1000)
_MODELS = ("STRICT_LOWER_BOUND", "OPTIMISTIC_UPPER_BOUND")
_NS_PER_HOUR = Decimal("3600000000000")
_NS_PER_MINUTE = 60_000_000_000
_NS_PER_FIVE_MINUTES = 300_000_000_000
_RISEX_TRADE_KEY = re.compile(r"^RISEX\|([^|]+)\|(0x[0-9a-fA-F]{48})-(0x[0-9a-fA-F]{48})$")
_RISEX_MARKET = re.compile(r"^([^/|]+)/([^/|]+)$")


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _non_negative_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _number(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _display(value: Any) -> Any:
    return _number(value) if isinstance(value, Decimal) else value


def _rate(numerator: int, denominator: int) -> str | None:
    return _number(Decimal(numerator) / Decimal(denominator)) if denominator else None


def _rate_payload(numerator: int, denominator: int) -> dict[str, Any]:
    return {"numerator": numerator, "denominator": denominator, "rate": _rate(numerator, denominator)}


def _stats(values: Iterable[Decimal]) -> dict[str, Any]:
    ordered = sorted(values)
    if not ordered:
        return {"count": 0, "mean": None, "median": None, "p05": None, "minimum": None, "maximum": None}
    middle = len(ordered) // 2
    median = ordered[middle] if len(ordered) % 2 else (ordered[middle - 1] + ordered[middle]) / Decimal("2")
    return {"count": len(ordered), "mean": _number(sum(ordered, Decimal("0")) / Decimal(len(ordered))), "median": _number(median), "p05": _number(ordered[int((len(ordered) - 1) * Decimal("0.05"))]), "minimum": _number(ordered[0]), "maximum": _number(ordered[-1])}


def _model(record: dict[str, Any]) -> str:
    return "OPTIMISTIC_UPPER_BOUND" if _text(record.get("fillability_model") or record.get("model")).upper() == "OPTIMISTIC_UPPER_BOUND" else "STRICT_LOWER_BOUND"


def _present(record: dict[str, Any], *names: str) -> tuple[bool, Any]:
    for name in names:
        if name in record and record.get(name) is not None:
            return True, record.get(name)
    return False, None


def _identity(value: Any) -> tuple[str, Any] | None:
    if isinstance(value, str) and value:
        return "str", value
    if isinstance(value, int) and not isinstance(value, bool):
        return "int", value
    return None


def _same_identity(left: tuple[str, Any] | None, right: tuple[str, Any] | None) -> bool:
    if left is None or right is None:
        return left == right
    if left[0] == right[0] == "str":
        return left[1].lower() == right[1].lower()
    return left == right


def _parse_trade_identity(market: str, key: Any) -> tuple[tuple[str, Any] | None, tuple[str, Any] | None, set[str]]:
    if not isinstance(key, str):
        return None, None, {"TRADE_EVENT_KEY_MALFORMED"}
    match = _RISEX_TRADE_KEY.fullmatch(key)
    if match is None:
        return None, None, {"TRADE_EVENT_KEY_MALFORMED"}
    key_market, maker, taker = match.groups()
    market_match = _RISEX_MARKET.fullmatch(key_market)
    if market_match is None:
        return None, None, {"TRADE_EVENT_KEY_MALFORMED"}
    key_base, _key_quote = market_match.groups()
    errors: set[str] = set()
    if market and key_base.casefold() != market.casefold():
        errors.add("TRADE_EVENT_KEY_MARKET_CONFLICT")
    # The persisted adapter key carries the exact RISEx venue symbol (for
    # example BTC/USDC), while the report record carries the canonical asset
    # (BTC).  Only the base asset is compared; no symbol rewrite or time-based
    # market inference is allowed.
    return ("str", maker), ("str", taker), errors


def _trade_identity(record: dict[str, Any], market: str, key: Any) -> tuple[tuple[str, Any] | None, tuple[str, Any] | None, set[str]]:
    maker, taker, errors = _parse_trade_identity(market, key)
    for name, parsed in (("maker_order_id", maker), ("taker_order_id", taker)):
        value = record.get(name)
        if name not in record or value is None:
            continue
        explicit = _identity(value)
        if explicit is None:
            errors.add(f"{name.upper()}_MALFORMED")
        elif not _same_identity(explicit, parsed):
            errors.add(f"{name.upper()}_CONFLICT")
    return maker, taker, errors


def _identity_sort(value: tuple[str, Any]) -> tuple[str, str]:
    return value[0], str(value[1])


def _identity_output(value: tuple[str, Any] | None) -> Any:
    return None if value is None else value[1]


def _canonical(value: Any) -> str:
    parsed = _decimal(value)
    return "" if parsed is None else format(parsed.normalize(), "f")


def _tie(record: dict[str, Any]) -> tuple[str, ...]:
    fields = tuple(_text(record.get(name)) for name in ("kind", "policy_id", "quote_version_id", "horizon_ms", "canonical_market", "direction", "target_margin_bps", "maker_price", "outcome", "trade_event_key"))
    return fields + (json.dumps(record, sort_keys=True, separators=(",", ":"), default=str),)


def _snapshot(record: dict[str, Any]) -> dict[str, Any] | None:
    if record.get("quote_version_id") is None:
        return None
    return {"policy_id": _text(record.get("policy_id")), "market": _text(record.get("canonical_market")), "direction": _text(record.get("direction")), "target": record.get("target_notional_usd"), "margin": record.get("target_margin_bps"), "version": _text(record.get("quote_version_id")), "created": _non_negative_int(record.get("quote_created_monotonic_ns")), "raw_bound": record.get("raw_risex_price_bound", record.get("raw_price_bound")), "post_only_bound": record.get("post_only_bound_price"), "maker_price": record.get("maker_price"), "tick": record.get("risex_tick_size"), "quantity": record.get("canonical_quantity", record.get("quote_canonical_quantity")), "maker_order_id": record.get("maker_order_id"), "taker_order_id": record.get("taker_order_id"), "transaction_hash": record.get("transaction_hash", record.get("tx_hash")), "block_number": record.get("block_number"), "log_index": record.get("log_index"), "tie": _tie(record)}


def _quote_value(record: dict[str, Any], quote: dict[str, Any] | None, name: str, quote_name: str | None = None, *aliases: str) -> Any:
    present, value = _present(record, name, *aliases)
    if present:
        return value
    return None if quote is None else quote.get(quote_name or name)


def _trade_keys(record: dict[str, Any]) -> tuple[str, ...]:
    value = record.get("qualifying_trade_event_keys")
    if value is None:
        value = record.get("trade_event_key", record.get("risex_trade_event_key"))
    if isinstance(value, (list, tuple)):
        return tuple(sorted({_text(item) for item in value if item is not None and _text(item)}))
    return (_text(value),) if value is not None and _text(value) else ()


def _quote_interval(record: dict[str, Any]) -> tuple[int, int] | None:
    start = _non_negative_int(record.get("quote_created_monotonic_ns"))
    if start is None:
        return None
    end = _non_negative_int(record.get("quote_expires_monotonic_ns"))
    if end is None:
        lifetime = _non_negative_int(record.get("quote_lifetime_ns"))
        end = None if lifetime is None else start + lifetime
    return None if end is None or end < start else (start, end)


def _duration(intervals: list[tuple[int, int]]) -> int:
    if not intervals:
        return 0
    ordered = sorted(intervals)
    total, start, end = 0, *ordered[0]
    for next_start, next_end in ordered[1:]:
        if next_start <= end:
            end = max(end, next_end)
        else:
            total += max(0, end - start)
            start, end = next_start, next_end
    return total + max(0, end - start)


def _actual(observation: dict[str, Any]) -> tuple[Decimal | None, Decimal | None, set[str]]:
    price, tick = _decimal(observation.get("maker_price")), _decimal(observation.get("tick"))
    errors: set[str] = set()
    if price is None:
        errors.add("MAKER_PRICE_MISSING_OR_MALFORMED")
    elif price <= 0:
        errors.add("MAKER_PRICE_NON_POSITIVE")
    if tick is None:
        errors.add("RISEX_TICK_MISSING_OR_MALFORMED")
    elif tick <= 0:
        errors.add("RISEX_TICK_NON_POSITIVE")
    if price is not None and tick is not None and tick > 0 and price % tick:
        errors.add("MAKER_PRICE_OFF_TICK")
    return price, tick, errors


def _signed(direction: str, narrower: Decimal, wider: Decimal) -> Decimal | None:
    if direction == "RISEX_BUY_LIGHTER_SELL":
        return narrower - wider
    if direction == "RISEX_SELL_LIGHTER_BUY":
        return wider - narrower
    return None


def _resolve(observation: dict[str, Any], quotes: dict[str, dict[str, Any]], trades: dict[tuple[str, str], list[dict[str, Any]]]) -> None:
    raw = observation["raw"]
    quote = quotes.get(observation.get("version"))
    if not observation["policy_id"] and quote is not None:
        observation["policy_id"] = quote["policy_id"]
    for field, quote_field in (("market", "market"), ("direction", "direction"), ("target", "target"), ("margin", "margin"), ("created", "created"), ("raw_bound", "raw_bound"), ("post_only_bound", "post_only_bound"), ("maker_price", "maker_price"), ("tick", "tick"), ("quantity", "quantity")):
        if observation.get(field) is None and quote is not None:
            observation[field] = quote.get(quote_field)
    associated = [row for key in observation["trade_keys"] for row in trades.get((observation["market"], key), ()) if row.get("eligible_trade") is not False]
    associated.sort(key=_tie)
    associated_keys = {(observation["market"], key) for key in observation["trade_keys"] if any(row.get("eligible_trade") is not False for row in trades.get((observation["market"], key), ()))}
    errors: set[str] = set(observation.get("identity_issues", ()))
    trade_makers: set[tuple[str, Any]] = set()
    trade_takers: set[tuple[str, Any]] = set()
    for row in associated:
        maker, taker, trade_errors = _trade_identity(row, observation["market"], row.get("trade_event_key"))
        errors.update(trade_errors)
        if maker is not None:
            trade_makers.add(maker)
        if taker is not None:
            trade_takers.add(taker)
    if not observation["market"]:
        errors.add("MARKET_MISSING")
    if not observation["direction"]:
        errors.add("DIRECTION_MISSING")
    if not observation.get("version"):
        errors.add("QUOTE_VERSION_ID_MISSING")
    if not observation["trade_keys"]:
        errors.add("TRADE_EVENT_KEY_MISSING")
    elif len(associated_keys) != len(observation["trade_keys"]):
        errors.add("TRADE_EVENT_RECORD_MISSING")

    def identity_values(name: str, quote_value: Any = None) -> tuple[set[tuple[str, Any]], bool, bool]:
        values: set[tuple[str, Any]] = set()
        malformed = False
        present, value = _present(raw, name)
        if present:
            parsed = _identity(value)
            malformed |= parsed is None
            if parsed is not None:
                values.add(parsed)
        elif name in raw and raw.get(name) is not None:
            malformed = True
        if not present and quote_value is not None:
            parsed = _identity(quote_value)
            malformed |= parsed is None
            if parsed is not None:
                values.add(parsed)
        for row in associated:
            present, value = _present(row, name)
            if present:
                parsed = _identity(value)
                malformed |= parsed is None
                if parsed is not None:
                    values.add(parsed)
            elif name in row and row.get(name) is not None:
                malformed = True
        return values, malformed, len(values) > 1

    explicit_makers, maker_malformed, maker_conflict = identity_values("maker_order_id", None if quote is None else quote.get("maker_order_id"))
    makers = set(explicit_makers)
    makers.update(trade_makers)
    maker_conflict = maker_conflict or bool(explicit_makers and trade_makers and not any(_same_identity(left, right) for left in explicit_makers for right in trade_makers))
    if maker_malformed:
        errors.add("MAKER_ORDER_ID_MALFORMED")
    if maker_conflict:
        errors.add("MAKER_ORDER_ID_CONFLICT")
    observation["maker_order_ids"] = tuple(sorted(makers, key=_identity_sort))
    observation["maker_order_id"] = observation["maker_order_ids"][0] if len(observation["maker_order_ids"]) == 1 else None

    takers, malformed, conflict = identity_values("taker_order_id", None if quote is None else quote.get("taker_order_id"))
    takers.update(trade_takers)
    conflict = conflict or len(takers) > 1
    if malformed:
        errors.add("TAKER_ORDER_ID_MALFORMED")
    if conflict:
        errors.add("TAKER_ORDER_ID_CONFLICT")
    if not takers:
        errors.add("TAKER_ORDER_ID_MISSING")
    observation["taker_order_id"] = sorted(takers, key=_identity_sort)[0] if takers else None

    sides: set[str] = set()
    for row in (raw, *associated):
        if "aggressor_side" not in row:
            continue
        side = _text(row.get("aggressor_side")).upper()
        if side in {"BUY", "SELL"}:
            sides.add(side)
        else:
            errors.add("AGGRESSOR_SIDE_MALFORMED")
    if not sides:
        errors.add("AGGRESSOR_SIDE_MISSING")
    if len(sides) > 1:
        errors.add("AGGRESSOR_SIDE_CONFLICT")
    observation["aggressor_side"] = next(iter(sides)) if len(sides) == 1 else None

    def optional(name: str, quote_value: Any = None, *aliases: str) -> Any:
        present, value = _present(raw, name, *aliases)
        if present:
            return value
        if name in raw or any(alias in raw for alias in aliases):
            errors.add(f"{name.upper()}_MALFORMED")
            return None
        if quote_value is not None:
            return quote_value
        for row in associated:
            present, value = _present(row, name, *aliases)
            if present:
                return value
            if name in row or any(alias in row for alias in aliases):
                errors.add(f"{name.upper()}_MALFORMED")
        return None

    observation["transaction_hash"] = optional("transaction_hash", None if quote is None else quote.get("transaction_hash"), "tx_hash")
    observation["block_number"] = optional("block_number", None if quote is None else quote.get("block_number"))
    observation["log_index"] = optional("log_index", None if quote is None else quote.get("log_index"))
    observation["identity_issues"] = errors
    blocked = {"TRADE_EVENT_KEY_MALFORMED", "TRADE_EVENT_KEY_MARKET_CONFLICT", "MAKER_ORDER_ID_MALFORMED", "MAKER_ORDER_ID_CONFLICT", "TAKER_ORDER_ID_MALFORMED", "TAKER_ORDER_ID_CONFLICT", "AGGRESSOR_SIDE_MALFORMED", "AGGRESSOR_SIDE_CONFLICT"}
    observation["cluster_key"] = (observation["market"], observation["aggressor_side"], observation["taker_order_id"]) if observation["trade_keys"] and len(associated_keys) == len(observation["trade_keys"]) and observation["market"] and observation["aggressor_side"] and observation["taker_order_id"] and not errors.intersection(blocked) else None


def _pair(direction: str, narrower: list[dict[str, Any]], wider: list[dict[str, Any]]) -> tuple[str, list[Decimal], list[Decimal], list[tuple[dict[str, Any], dict[str, Any], Decimal | None, Decimal | None, set[str]]], set[str]]:
    comparisons, prices, ticks = [], [], []
    exact_event_pair_exists = any(
        set(left["trade_keys"]).intersection(right["trade_keys"])
        for left in narrower
        for right in wider
    )
    for left in narrower:
        left_price, left_tick, left_errors = _actual(left)
        for right in wider:
            if exact_event_pair_exists and not set(left["trade_keys"]).intersection(right["trade_keys"]):
                continue
            right_price, right_tick, right_errors = _actual(right)
            errors = left_errors | right_errors
            signed_price = signed_ticks = None
            if not errors and left_price is not None and right_price is not None:
                signed_price = _signed(direction, left_price, right_price)
                if signed_price is None:
                    errors.add("DIRECTION_MALFORMED")
                elif left_tick != right_tick:
                    errors.add("TICK_SIZE_CONFLICT")
                elif left_tick is not None and left_tick > 0:
                    signed_ticks = signed_price / left_tick
            if signed_price is not None:
                prices.append(signed_price)
            if signed_ticks is not None:
                ticks.append(signed_ticks)
            comparisons.append((left, right, signed_price, signed_ticks, errors))
    comparable = [item for item in comparisons if not item[4]]
    if any(item[2] == 0 for item in comparable):
        classification = "EFFECTIVE_PRICE_COLLISION"
    elif any(item[2] is not None and item[2] > 0 for item in comparable):
        classification = "DISTINCT_EFFECTIVE_LEVEL"
    elif any(item[2] is not None and item[2] < 0 for item in comparable):
        classification = "NOMINAL_WIDER_REVERSED"
    else:
        classification = "EFFECTIVE_LEVEL_UNRESOLVED"
    distinct = {right["observation_id"] for left, right, price, _ticks, errors in comparisons if classification == "DISTINCT_EFFECTIVE_LEVEL" and not errors and price is not None and price > 0}
    return classification, prices, ticks, comparisons, distinct


def _observation_payload(observation: dict[str, Any]) -> dict[str, Any]:
    return {"observation_id": observation["observation_id"], "fillability_model": observation["model"], "policy_id": observation["policy_id"], "canonical_market": observation["market"], "direction": observation["direction"], "target_notional_usd": _display(observation.get("target")), "target_margin_bps": _display(observation.get("margin")), "quote_version_id": observation.get("version"), "quote_created_monotonic_ns": observation.get("created"), "would_fill_detected_monotonic_ns": observation.get("detected"), "trade_event_keys": list(observation["trade_keys"]), "trade_event_key": observation["trade_keys"][0] if len(observation["trade_keys"]) == 1 else None, "maker_order_id": _identity_output(observation.get("maker_order_id")), "maker_order_ids": [_identity_output(value) for value in observation.get("maker_order_ids", ())], "taker_order_id": _identity_output(observation.get("taker_order_id")), "transaction_hash": _display(observation.get("transaction_hash")), "block_number": _non_negative_int(observation.get("block_number")), "log_index": _non_negative_int(observation.get("log_index")), "raw_risex_price_bound": _display(observation.get("raw_bound")), "post_only_bound_price": _display(observation.get("post_only_bound")), "maker_price": _display(observation.get("maker_price")), "risex_tick_size": _display(observation.get("tick")), "canonical_quantity": _display(observation.get("quantity")), "aggressor_side": observation.get("aggressor_side"), "identity_status": "CLUSTERED" if observation.get("cluster_key") else "UNCLUSTERED", "identity_issues": sorted(observation["identity_issues"]), "cluster_key": None if observation.get("cluster_key") is None else {"canonical_market": observation["cluster_key"][0], "aggressor_side": observation["cluster_key"][1], "taker_order_id": _identity_output(observation["cluster_key"][2])}}


def _curve(pair: dict[str, Any], by_id: dict[str, dict[str, Any]], horizons: dict[tuple[str, str], dict[int, dict[str, Any]]]) -> dict[str, Any]:
    rows_by_horizon = []
    for horizon_ms in _HORIZONS:
        rows = [horizons.get((by_id[item]["model"], by_id[item].get("version")), {}).get(horizon_ms) for item in pair["distinct_wider_observation_ids"]]
        rows = [row for row in rows if row is not None]
        outcomes: dict[str, int] = defaultdict(int)
        edges, markouts = [], []
        for row in rows:
            outcomes[_text(row.get("outcome"))] += 1
            if (value := _decimal(row.get("entry_edge_usd"))) is not None:
                edges.append(value)
            if (value := _decimal(row.get("conditional_markout_usd"))) is not None:
                markouts.append(value)
        rows_by_horizon.append({"horizon_ms": horizon_ms, "horizon_label": "DIAGNOSTIC_500MS" if horizon_ms == 500 else f"{horizon_ms}MS", "observation_count": len(rows), "expected_observation_count": len(pair["distinct_wider_observation_ids"]), "missing_observation_count": max(0, len(pair["distinct_wider_observation_ids"]) - len(rows)), "hedge_outcome_counts": {key: outcomes[key] for key in sorted(outcomes)}, "outcome_counts": {key: outcomes[key] for key in sorted(outcomes)}, "full_hedge_rate": _rate(sum(_text(row.get("outcome")) == "HEDGE_FULL" for row in rows), len(rows)), "entry_edge_usd": _stats(edges), "conditional_markout_usd": _stats(markouts)})
    return {key: pair[key] for key in ("fillability_model", "canonical_market", "direction", "target_notional_usd", "narrower_target_margin_bps", "wider_target_margin_bps", "cluster_key", "classification")} | {"wider_observation_ids": pair["distinct_wider_observation_ids"], "horizons": rows_by_horizon}


def _concentration(observations: list[dict[str, Any]], width: int, *, include_models: bool = True) -> dict[str, Any]:
    buckets: dict[int, list[dict[str, Any]]] = defaultdict(list)
    missing = 0
    for observation in observations:
        detected = observation.get("detected")
        if detected is None:
            missing += 1
        else:
            buckets[(detected // width) * width].append(observation)
    total = sum(len(values) for values in buckets.values())
    shares = [Decimal(len(values)) / Decimal(total) for values in buckets.values()] if total else []
    return {"bucket_width_ms": width // 1_000_000, "bucket_count": len(buckets), "filled_event_count": total, "missing_detection_timestamp_count": missing, "top_bucket_share": _number(max(shares)) if shares else None, "concentration_index": _number(sum((share * share for share in shares), Decimal("0"))), "buckets": [{"bucket_start_monotonic_ns": start, "filled_event_count": len(buckets[start]), "filled_notional_usd": _number(sum((row["notional"] for row in buckets[start]), Decimal("0"))), "fillability_models": sorted({row["model"] for row in buckets[start]}), "quote_versions": sorted({row["version"] for row in buckets[start] if row.get("version")})} for start in sorted(buckets)], "by_model": {model: _concentration([row for row in observations if row["model"] == model], width, include_models=False) for model in _MODELS} if include_models else {}}


def _new_policy(policy_id: str) -> dict[str, Any]:
    return {"id": policy_id, "market": "", "direction": "", "target": None, "margin": None, "intervals": [], "quote_count": 0, "quoteable_count": 0, "invalid_quote_interval_count": 0, "fills": defaultdict(int), "notional": defaultdict(lambda: Decimal("0"))}


def build_calibration_evidence(records: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Build SS-001J evidence from already validated record mappings."""
    policies: dict[str, dict[str, Any]] = {}
    quotes: dict[str, dict[str, Any]] = {}
    trades: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    eligible_events: set[tuple[str, str]] = set()
    eligible_clusters: set[tuple[str, str, tuple[str, Any]]] = set()
    cluster_events: dict[tuple[str, str, tuple[str, Any]], set[tuple[str, str]]] = defaultdict(set)
    policy_events: dict[str, set[tuple[str, str]]] = defaultdict(set)
    policy_clusters: dict[str, set[tuple[str, str, tuple[str, Any]]]] = defaultdict(set)
    observations: list[dict[str, Any]] = []
    observation_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    horizons: dict[tuple[str, str], dict[int, dict[str, Any]]] = defaultdict(dict)
    counts: dict[str, int] = defaultdict(int)
    issue_counts: dict[str, int] = defaultdict(int)
    duplicate_fills = duplicate_horizons = 0
    horizon_seen: set[tuple[str, str, int]] = set()

    def policy(policy_id: str, record: dict[str, Any]) -> dict[str, Any]:
        state = policies.setdefault(policy_id, _new_policy(policy_id))
        state["market"] = state["market"] or _text(record.get("canonical_market", record.get("market")))
        state["direction"] = state["direction"] or _text(record.get("direction"))
        if state["target"] is None:
            state["target"] = record.get("target_notional_usd", record.get("target"))
        if state["margin"] is None:
            state["margin"] = record.get("target_margin_bps", record.get("margin"))
        return state

    def observation_for(record: dict[str, Any], model: str, version: str | None) -> dict[str, Any]:
        return {"raw": dict(record), "model": model, "policy_id": _text(record.get("policy_id")), "market": _text(record.get("canonical_market")), "direction": _text(record.get("direction")), "target": record.get("target_notional_usd"), "margin": record.get("target_margin_bps"), "version": version, "created": record.get("quote_created_monotonic_ns"), "detected": _non_negative_int(record.get("would_fill_detected_monotonic_ns")), "raw_bound": record.get("raw_risex_price_bound", record.get("raw_price_bound")), "post_only_bound": record.get("post_only_bound_price"), "maker_price": record.get("maker_price"), "tick": record.get("risex_tick_size"), "quantity": record.get("canonical_quantity", record.get("quote_canonical_quantity")), "trade_keys": _trade_keys(record), "identity_issues": set()}

    for record in records:
        if not isinstance(record, dict):
            continue
        kind = record.get("kind")
        if kind == "QUOTE":
            counts["quote"] += 1
            state = policy(_text(record.get("policy_id")), record)
            state["quote_count"] += 1
            if record.get("outcome") == "QUOTE_ACTIVE":
                state["quoteable_count"] += 1
                if (interval := _quote_interval(record)) is None:
                    state["invalid_quote_interval_count"] += 1
                else:
                    state["intervals"].append(interval)
            if (snapshot := _snapshot(record)) is not None and (snapshot["version"] not in quotes or snapshot["tie"] < quotes[snapshot["version"]]["tie"]):
                quotes[snapshot["version"]] = snapshot
        elif kind == "RISEX_TRADE":
            counts["risex_trade"] += 1
            market, key = _text(record.get("canonical_market")), record.get("trade_event_key")
            if market and key is not None and _text(key):
                event_key = (market, _text(key))
                trades[event_key].append(record)
                if record.get("eligible_trade") is not False:
                    eligible_events.add(event_key)
                    side = _text(record.get("aggressor_side")).upper()
                    _maker, taker, trade_identity_errors = _trade_identity(record, market, key)
                    if side in {"BUY", "SELL"} and taker is not None and not trade_identity_errors:
                        cluster_key = (market, side, taker)
                        eligible_clusters.add(cluster_key)
                        cluster_events[cluster_key].add(event_key)
                        policy_ids = record.get("eligible_policy_ids")
                        if isinstance(policy_ids, (list, tuple)):
                            for value in policy_ids:
                                pid = _text(value)
                                if pid:
                                    policy_events[pid].add(event_key)
                                    policy_clusters[pid].add(cluster_key)
        elif kind == "WOULD_FILL":
            counts["would_fill"] += 1
            model = _model(record)
            version = None if record.get("quote_version_id") is None else _text(record.get("quote_version_id"))
            quote = quotes.get(version) if version else None
            pid = _text(record.get("policy_id")) or ("" if quote is None else quote["policy_id"])
            policy(pid, record if quote is None else quote)
            observation = observation_for(record, model, version)
            key = (model, version or f"missing-{len(observations)}")
            if version is not None and key in observation_by_key:
                duplicate_fills += 1
                existing = observation_by_key[key]
                existing["identity_issues"].add("DUPLICATE_FILL_RECORD")
                if _tie(record) < _tie(existing["raw"]):
                    observation["observation_id"] = existing["observation_id"]
                    observation["identity_issues"] = existing["identity_issues"]
                    observations[observations.index(existing)] = observation
                    observation_by_key[key] = observation
            else:
                observation["observation_id"] = f"{model}|{version or f'missing-{len(observations)}'}"
                observations.append(observation)
                observation_by_key[key] = observation
        elif kind == "HEDGE_HORIZON":
            counts["hedge_horizon"] += 1
            if record.get("quote_version_id") is None:
                continue
            try:
                horizon_ms = int(record.get("horizon_ms"))
            except (TypeError, ValueError):
                continue
            if horizon_ms not in _HORIZONS:
                continue
            key = (_model(record), _text(record.get("quote_version_id")))
            seen_key = (*key, horizon_ms)
            current = horizons[key].get(horizon_ms)
            if seen_key in horizon_seen:
                duplicate_horizons += 1
                if current is not None and _tie(record) < _tie(current):
                    horizons[key][horizon_ms] = dict(record)
            else:
                horizon_seen.add(seen_key)
                horizons[key][horizon_ms] = dict(record)

    for observation in observations:
        _resolve(observation, quotes, trades)
        observation["notional"] = (_decimal(observation.get("quantity")) or Decimal("0")) * (_decimal(observation.get("maker_price")) or Decimal("0"))
        state = policies.setdefault(observation["policy_id"], _new_policy(observation["policy_id"]))
        state["fills"][observation["model"]] += 1
        state["notional"][observation["model"]] += observation["notional"]
        if observation.get("cluster_key") is not None:
            policy_clusters[observation["policy_id"]].add(observation["cluster_key"])
        margin_value = _decimal(observation.get("margin"))
        if margin_value is None or margin_value <= 0:
            observation["identity_issues"].add("TARGET_MARGIN_MISSING_OR_MALFORMED")
        for issue in observation["identity_issues"]:
            issue_counts[issue] += 1

    if len(policies) == 1:
        only = next(iter(policies))
        policy_events[only].update(eligible_events)
        policy_clusters[only].update(eligible_clusters)
    clusters: dict[tuple[str, str, tuple[str, Any]], list[dict[str, Any]]] = defaultdict(list)
    for cluster_key in eligible_clusters:
        clusters[cluster_key]
    for observation in observations:
        if observation.get("cluster_key") is not None:
            clusters[observation["cluster_key"]].append(observation)

    groups: dict[tuple[tuple[str, str, tuple[str, Any]], str, str, str, str], dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for cluster_key, rows in clusters.items():
        for observation in rows:
            margin_value = _decimal(observation.get("margin"))
            margin = _canonical(margin_value) if margin_value is not None and margin_value > 0 else ""
            if margin:
                groups[(cluster_key, observation["model"], observation["market"], observation["direction"], _canonical(observation.get("target")))][margin].append(observation)

    pairs, pair_counts, paired_clusters, collision_clusters = [], defaultdict(int), set(), set()
    all_prices, all_ticks = [], []
    for (cluster_key, model, market, direction, target), arms in sorted(groups.items(), key=lambda item: (item[0][0][0], item[0][0][1], _identity_sort(item[0][0][2]), item[0][1], item[0][2], item[0][3], item[0][4])):
        arm_margins = sorted(arms, key=lambda value: (_decimal(value) or Decimal("Infinity"), value))
        for narrow_margin, wide_margin in combinations(arm_margins, 2):
            narrow, wide = sorted(arms[narrow_margin], key=lambda item: item["observation_id"]), sorted(arms[wide_margin], key=lambda item: item["observation_id"])
            classification, prices, ticks, comparisons, distinct_ids = _pair(direction, narrow, wide)
            all_prices.extend(prices)
            all_ticks.extend(ticks)
            pair = {"fillability_model": model, "canonical_market": market, "direction": direction, "target_notional_usd": target, "narrower_target_margin_bps": narrow_margin, "wider_target_margin_bps": wide_margin, "policy_ids": sorted({item["policy_id"] for item in (*narrow, *wide)}), "cluster_key": {"canonical_market": cluster_key[0], "aggressor_side": cluster_key[1], "taker_order_id": _identity_output(cluster_key[2])}, "classification": classification, "narrower_observation_count": len(narrow), "wider_observation_count": len(wide), "narrower_quote_versions": sorted({item["version"] for item in narrow if item.get("version")}), "wider_quote_versions": sorted({item["version"] for item in wide if item.get("version")}), "repeated_narrower_quote_version_count": max(0, len({item.get("version") for item in narrow if item.get("version")} ) - 1), "repeated_wider_quote_version_count": max(0, len({item.get("version") for item in wide if item.get("version")} ) - 1), "comparison_count": len(comparisons), "comparable_comparison_count": sum(not item[4] for item in comparisons), "signed_price_separation": _stats(prices), "signed_tick_separation": _stats(ticks), "narrower_observation_ids": sorted(item["observation_id"] for item in narrow), "wider_observation_ids": sorted(item["observation_id"] for item in wide), "distinct_wider_observation_ids": sorted(distinct_ids), "comparisons": [{"narrower_observation_id": left["observation_id"], "wider_observation_id": right["observation_id"], "signed_price_separation": _number(price), "signed_tick_separation": _number(tick), "issues": sorted(error)} for left, right, price, tick, error in sorted(comparisons, key=lambda item: (item[0]["observation_id"], item[1]["observation_id"]))]}
            pairs.append(pair)
            pair_counts[classification] += 1
            paired_clusters.add(cluster_key)
            if classification == "EFFECTIVE_PRICE_COLLISION":
                collision_clusters.add(cluster_key)
    pairs.sort(key=lambda pair: (pair["fillability_model"], pair["canonical_market"], pair["direction"], pair["target_notional_usd"], pair["narrower_target_margin_bps"], pair["wider_target_margin_bps"], _text(pair["cluster_key"]["taker_order_id"])))

    by_id = {observation["observation_id"]: observation for observation in observations}
    curves = [_curve(pair, by_id, horizons) for pair in pairs if pair["classification"] == "DISTINCT_EFFECTIVE_LEVEL"]
    curves.sort(key=lambda curve: (curve["fillability_model"], curve["canonical_market"], curve["direction"], curve["target_notional_usd"], curve["narrower_target_margin_bps"], curve["wider_target_margin_bps"], _text(curve["cluster_key"]["taker_order_id"])))

    cluster_payloads = [{"cluster_key": {"canonical_market": key[0], "aggressor_side": key[1], "taker_order_id": _identity_output(key[2])}, "eligible_event_count": len(cluster_events.get(key, set())), "filled_observation_count": len(rows), "quote_versions": sorted({row["version"] for row in rows if row.get("version")}), "repeated_quote_version_count": max(0, len(rows) - len({(row["model"], row.get("version")) for row in rows if row.get("version")})), "observation_ids": sorted(row["observation_id"] for row in rows)} for key, rows in sorted(clusters.items(), key=lambda item: (item[0][0], item[0][1], _identity_sort(item[0][2])))]

    all_duration = sum(_duration(state["intervals"]) for state in policies.values())
    all_hours = Decimal(all_duration) / _NS_PER_HOUR if all_duration else None
    global_rates = {}
    for model in _MODELS:
        rows = [row for row in observations if row["model"] == model]
        model_clusters = {row.get("cluster_key") for row in rows if row.get("cluster_key") is not None}
        notional = sum((row["notional"] for row in rows), Decimal("0"))
        global_rates[model] = {"filled_event_count": len(rows), "filled_notional_usd": _number(notional), "fill_event_rate": _rate_payload(len(rows), len(eligible_events)), "fill_venue_cluster_rate": _rate_payload(len(model_clusters), len(eligible_clusters)), "fill_quoteable_hour_rate": {"numerator": len(rows), "denominator_hours": _number(all_hours), "rate": _number(Decimal(len(rows)) / all_hours) if all_hours else None}, "filled_notional_per_quoteable_hour_usd": {"numerator_usd": _number(notional), "denominator_hours": _number(all_hours), "rate_usd_per_hour": _number(notional / all_hours) if all_hours else None}}

    policy_rates = {}
    for pid, state in sorted(policies.items()):
        duration = _duration(state["intervals"])
        hours = Decimal(duration) / _NS_PER_HOUR if duration else None
        event_count, cluster_count = len(policy_events[pid]), len(policy_clusters[pid])
        by_model = {}
        for model in _MODELS:
            filled, notional = state["fills"].get(model, 0), state["notional"].get(model, Decimal("0"))
            fill_clusters = {row.get("cluster_key") for row in observations if row["policy_id"] == pid and row["model"] == model and row.get("cluster_key") is not None}
            by_model[model] = {"filled_event_count": filled, "filled_notional_usd": _number(notional), "fill_event_rate": _rate_payload(filled, event_count), "fill_venue_cluster_rate": _rate_payload(len(fill_clusters), cluster_count), "fill_quoteable_hour_rate": {"numerator": filled, "denominator_hours": _number(hours), "rate": _number(Decimal(filled) / hours) if hours else None}, "filled_notional_per_quoteable_hour_usd": {"numerator_usd": _number(notional), "denominator_hours": _number(hours), "rate_usd_per_hour": _number(notional / hours) if hours else None}}
        policy_rates[pid] = {"quote_count": state["quote_count"], "quoteable_count": state["quoteable_count"], "invalid_quote_interval_count": state["invalid_quote_interval_count"], "quoteable_duration_ns": duration, "quoteable_hours": _number(hours), "eligible_trade_event_count": event_count, "venue_cluster_count": cluster_count, "by_model": by_model}

    cluster_times = sorted(min(row["detected"] for row in rows if row.get("detected") is not None) for rows in clusters.values() if any(row.get("detected") is not None for row in rows))
    inter_cluster = [Decimal(later - earlier) / Decimal("1000000") for earlier, later in zip(cluster_times, cluster_times[1:]) if later >= earlier]
    ordered_observations = sorted(observations, key=lambda item: (item["model"], item["market"], item["direction"], _canonical(item.get("target")), _canonical(item.get("margin")), item["observation_id"]))
    unclustered = [_observation_payload(row) for row in ordered_observations if row.get("cluster_key") is None]
    observation_payloads = [_observation_payload(row) for row in ordered_observations]
    comparable = sum(pair["comparable_comparison_count"] > 0 for pair in pairs)
    collision_count, distinct_count = pair_counts["EFFECTIVE_PRICE_COLLISION"], pair_counts["DISTINCT_EFFECTIVE_LEVEL"]
    distinct_observation_count = len({item for pair in pairs if pair["classification"] == "DISTINCT_EFFECTIVE_LEVEL" for item in pair["distinct_wider_observation_ids"]})
    effective = {"paired_cluster_count": len(paired_clusters), "nominal_arm_pair_count": len(pairs), "comparable_arm_pair_count": comparable, "effective_price_collision_count": collision_count, "distinct_wider_level_count": distinct_count, "distinct_wider_observation_count": distinct_observation_count, "nominal_wider_reversed_count": pair_counts["NOMINAL_WIDER_REVERSED"], "unresolved_pair_count": pair_counts["EFFECTIVE_LEVEL_UNRESOLVED"], "effective_price_collision_rate": _rate(collision_count, comparable), "distinct_wider_level_rate": _rate(distinct_count, comparable), "paired_cluster_collision_rate": _rate(len(collision_clusters), len(paired_clusters)), "actual_signed_price_separation": _stats(all_prices), "actual_signed_tick_separation": _stats(all_ticks), "pair_counts": {key: pair_counts[key] for key in sorted(pair_counts)}, "paired_evidence": pairs}
    counts.update({"filled_observation": len(observations), "clustered_filled_observation": len(observations) - len(unclustered), "unclustered_filled_observation": len(unclustered), "venue_cluster": len(clusters), "eligible_trade_event": len(eligible_events), "eligible_venue_cluster": len(eligible_clusters)})
    return {"schema_version": 1, "section": "SS-001J_EFFECTIVE_LEVEL_CLUSTER_CALIBRATION", "descriptive_only": True, "no_fitted_probability": True, "no_profitability_claim": True, "record_counts": {key: counts[key] for key in sorted(counts)}, "identity": {"cluster_key": ["canonical_market", "aggressor_side", "taker_order_id"], "clustered_filled_observation_count": len(observations) - len(unclustered), "unclustered_filled_observation_count": len(unclustered), "missing_or_malformed_identity_count": sum(bool(row["identity_issues"]) for row in observations), "identity_issue_counts": {key: issue_counts[key] for key in sorted(issue_counts)}, "duplicate_fill_record_count": duplicate_fills, "duplicate_horizon_record_count": duplicate_horizons}, "descriptive_rates": {"overall": {"eligible_trade_event_count": len(eligible_events), "eligible_venue_cluster_count": len(eligible_clusters), "quoteable_duration_ns": all_duration, "quoteable_hours": _number(all_hours), "by_model": global_rates}, "by_policy": policy_rates}, "concentration": {"one_minute": _concentration(observations, _NS_PER_MINUTE), "five_minute": _concentration(observations, _NS_PER_FIVE_MINUTES)}, "inter_cluster_interval_ms": _stats(inter_cluster), "inter_cluster_interval_count": len(inter_cluster), "effective_level": effective, "venue_clusters": cluster_payloads, "filled_observations": observation_payloads, "unclustered_observations": unclustered, "distinct_wider_level_horizon_curves": curves}


__all__ = ["build_calibration_evidence"]
