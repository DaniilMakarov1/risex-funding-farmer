"""Deterministic offline aggregation of SS-001B JSONL evidence."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
from typing import Any

from .store import iter_records


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _percentile(values: Iterable[Decimal], fraction: Decimal) -> Decimal | None:
    ordered = sorted(values)
    if not ordered:
        return None
    if not Decimal("0") <= fraction <= Decimal("1"):
        raise ValueError("percentile fraction must be between zero and one")
    index = int((len(ordered) - 1) * fraction)
    return ordered[index]


def _median(values: Iterable[Decimal]) -> Decimal | None:
    ordered = sorted(values)
    if not ordered:
        return None
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / Decimal("2")


def _key_text(value: Any) -> str:
    return "" if value is None else str(value)


def _json_number(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _union_length(intervals: Iterable[tuple[int, int]]) -> int:
    total = 0
    current_start: int | None = None
    current_end: int | None = None
    for start, end in sorted(intervals):
        if end <= start:
            continue
        if current_start is None:
            current_start, current_end = start, end
        elif start <= current_end:
            current_end = max(current_end, end)
        else:
            total += current_end - current_start
            current_start, current_end = start, end
    if current_start is not None and current_end is not None:
        total += current_end - current_start
    return total


def build_report(path: str | Path) -> dict[str, Any]:
    records = tuple(iter_records(path))
    metadata = next(
        (record.get("metadata", {}) for record in records if record.get("kind") == "RUN_METADATA"),
        {},
    )
    if not isinstance(metadata, dict):
        metadata = {}
    mode = _key_text(metadata.get("evidence_mode", "OBSERVATIONAL"))
    if any(record.get("kind") == "REPLAY_MODE" for record in records):
        mode = "FIXTURE"
    quotes = [record for record in records if record.get("kind") == "QUOTE"]
    fills = [record for record in records if record.get("kind") == "WOULD_FILL"]
    horizons = [record for record in records if record.get("kind") == "HEDGE_HORIZON"]
    gaps = [record for record in records if record.get("kind") == "DATA_GAP"]
    quote_by_version = {
        _key_text(record.get("quote_version_id")): record
        for record in quotes
        if record.get("quote_version_id") is not None
    }
    fills_by_policy: dict[str, int] = defaultdict(int)
    filled_notional_by_policy: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for fill in fills:
        quote = quote_by_version.get(_key_text(fill.get("quote_version_id")))
        if quote is None:
            continue
        policy_id = _key_text(quote.get("policy_id"))
        fills_by_policy[policy_id] += 1
        quantity = _decimal(quote.get("canonical_quantity"))
        price = _decimal(quote.get("maker_price"))
        if quantity is not None and price is not None:
            filled_notional_by_policy[policy_id] += quantity * price

    quote_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for quote in quotes:
        quote_groups[_key_text(quote.get("policy_id"))].append(quote)
    horizon_groups: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for horizon in horizons:
        horizon_groups[(_key_text(horizon.get("policy_id")), int(horizon.get("horizon_ms", 0)))].append(horizon)

    output_groups: list[dict[str, Any]] = []
    all_policy_ids = set(quote_groups) | {key[0] for key in horizon_groups}
    for policy_id in sorted(all_policy_ids):
        policy_quotes = quote_groups.get(policy_id, [])
        if policy_quotes:
            first = policy_quotes[0]
            market = _key_text(first.get("canonical_market"))
            direction = _key_text(first.get("direction"))
            target = _key_text(first.get("target_notional_usd"))
            margin = _key_text(first.get("target_margin_bps"))
        else:
            first_horizon = next(
                record for (key, _), values in horizon_groups.items() if key == policy_id for record in values
            )
            market = _key_text(first_horizon.get("canonical_market"))
            direction = _key_text(first_horizon.get("direction"))
            target = _key_text(first_horizon.get("target_notional_usd"))
            margin = _key_text(first_horizon.get("target_margin_bps"))
        quoteable = [quote for quote in policy_quotes if quote.get("outcome") == "QUOTE_ACTIVE"]
        edges = [
            parsed
            for quote in quoteable
            if (parsed := _decimal(quote.get("actual_edge_usd"))) is not None
        ]
        quote_count = len(policy_quotes)
        quoteable_count = len(quoteable)
        bbo_distances: list[Decimal] = []
        quote_lifetimes: list[Decimal] = []
        active_intervals: list[tuple[int, int]] = []
        observation_times: list[int] = []
        for quote in quoteable:
            created = quote.get("quote_created_monotonic_ns")
            expiry = quote.get("quote_expires_monotonic_ns")
            try:
                created_int = int(created)
                if expiry is not None:
                    expiry_int = int(expiry)
                    active_intervals.append((created_int, expiry_int))
                    quote_lifetimes.append(Decimal(expiry_int - created_int) / Decimal("1000000"))
            except (TypeError, ValueError):
                pass
            tick = _decimal(quote.get("risex_tick_size"))
            bound = _decimal(quote.get("post_only_bound_price"))
            maker = _decimal(quote.get("maker_price"))
            if tick and tick > 0 and bound is not None and maker is not None:
                bbo_distances.append(abs(bound - maker) / tick)
            lifetime = _decimal(quote.get("quote_lifetime_ns"))
            if lifetime is not None and not active_intervals:
                quote_lifetimes.append(lifetime / Decimal("1000000"))
        for quote in policy_quotes:
            try:
                observation_times.append(int(quote.get("quote_created_monotonic_ns")))
                if quote.get("quote_expires_monotonic_ns") is not None:
                    observation_times.append(int(quote.get("quote_expires_monotonic_ns")))
            except (TypeError, ValueError):
                pass
        observed_span = (
            max(observation_times) - min(observation_times)
            if observation_times
            else 0
        )
        active_span = _union_length(active_intervals)
        quoteable_share = (
            Decimal(active_span) / Decimal(max(observed_span, 1))
            if active_intervals
            else Decimal("0")
        )
        policy_gaps = [gap for gap in gaps if _key_text(gap.get("canonical_market")) == market]
        for horizon_ms in (0, 300, 500, 1000):
            observations = horizon_groups.get((policy_id, horizon_ms), [])
            outcomes = [_key_text(item.get("outcome")) for item in observations]
            markouts = [
                parsed
                for item in observations
                if (parsed := _decimal(item.get("conditional_markout_usd"))) is not None
            ]
            full = outcomes.count("HEDGE_FULL")
            partial = outcomes.count("HEDGE_PARTIAL")
            missing = sum(
                outcomes.count(name)
                for name in (
                    "HEDGE_DATA_MISSING",
                    "HEDGE_DATA_STALE",
                    "HEDGE_SESSION_DISPLACED",
                    "HEDGE_DATA_GAP",
                    "HEDGE_OUTCOME_UNKNOWN",
                )
            )
            positive = sum(1 for edge in markouts if edge > 0)
            expected = fills_by_policy.get(policy_id, 0)
            completeness = (
                "COMPLETE"
                if expected == len(observations) and expected > 0 and not policy_gaps
                else "DEGRADED"
            )
            output_groups.append(
                {
                    "canonical_market": market,
                    "direction": direction,
                    "target_notional_usd": target,
                    "target_margin_bps": margin,
                    "horizon_ms": horizon_ms,
                    "horizon_label": "DIAGNOSTIC_500MS" if horizon_ms == 500 else f"{horizon_ms}MS",
                    "opportunity_count": quoteable_count,
                    "quote_evaluation_count": quote_count,
                    "quoteable_time_share": (
                        _json_number(min(quoteable_share, Decimal("1")))
                        if quote_count
                        else None
                    ),
                    "median_quote_lifetime_ms": _json_number(_median(quote_lifetimes)),
                    "risex_bbo_distance_ticks": _json_number(_median(bbo_distances)),
                    "strict_would_fill_count": fills_by_policy.get(policy_id, 0),
                    "optimistic_upper_bound_count": 0,
                    "optimistic_model": "NOT_IMPLEMENTED",
                    "full_hedge_rate": _json_number(
                        Decimal(full) / Decimal(len(observations)) if observations else None
                    ),
                    "partial_or_missing_rate": _json_number(
                        Decimal(partial + missing) / Decimal(len(observations))
                        if observations
                        else None
                    ),
                    "mean_entry_edge_usd": _json_number(
                        sum(edges, Decimal("0")) / Decimal(len(edges)) if edges else None
                    ),
                    "median_entry_edge_usd": _json_number(_median(edges)),
                    "p05_entry_edge_usd": _json_number(_percentile(edges, Decimal("0.05"))),
                    "mean_conditional_markout_usd": _json_number(
                        sum(markouts, Decimal("0")) / Decimal(len(markouts)) if markouts else None
                    ),
                    "median_conditional_markout_usd": _json_number(_median(markouts)),
                    "p05_conditional_markout_usd": _json_number(
                        _percentile(markouts, Decimal("0.05"))
                    ),
                    "positive_edge_share": _json_number(
                        Decimal(positive) / Decimal(len(markouts)) if markouts else None
                    ),
                    "maximum_adverse_markout_usd": _json_number(min(markouts) if markouts else None),
                    "hypothetical_risex_filled_notional_usd": _json_number(
                        filled_notional_by_policy.get(policy_id, Decimal("0"))
                    ),
                    "concentration": {
                        "strict_episode_share": _json_number(
                            Decimal(fills_by_policy.get(policy_id, 0)) / Decimal(len(fills))
                            if fills
                            else None
                        ),
                    },
                    "data_gap_count": len(policy_gaps),
                    "data_completeness": completeness,
                    "evidence_mode": mode,
                }
            )
    output_groups.sort(
        key=lambda row: (
            row["canonical_market"],
            row["direction"],
            row["target_notional_usd"],
            row["target_margin_bps"],
            row["horizon_ms"],
        )
    )
    return {
        "schema_version": 1,
        "run_id": metadata.get("run_id"),
        "source_commit": metadata.get("source_commit"),
        "evidence_mode": mode,
        "record_count": len(records),
        "gap_count": len(gaps),
        "strict_would_fill_count": len(fills),
        "horizon_record_count": len(horizons),
        "markets": sorted(
            {
                _key_text(record.get("canonical_market"))
                for record in records
                if record.get("canonical_market") is not None
            }
        ),
        "groups": output_groups,
    }


def render_report(path: str | Path, *, format: str = "json") -> str:
    report = build_report(path)
    if format == "json":
        return json.dumps(report, sort_keys=True, separators=(",", ":"))
    if format != "table":
        raise ValueError("report format must be json or table")
    lines = [
        f"run_id={report.get('run_id')}",
        f"mode={report.get('evidence_mode')} records={report.get('record_count')} gaps={report.get('gap_count')}",
        "market direction size margin horizon fills full_rate markout completeness",
    ]
    for row in report["groups"]:
        lines.append(
            " ".join(
                (
                    row["canonical_market"],
                    row["direction"],
                    row["target_notional_usd"],
                    row["target_margin_bps"],
                    str(row["horizon_ms"]),
                    str(row["strict_would_fill_count"]),
                    _key_text(row["full_hedge_rate"]),
                    _key_text(row["median_conditional_markout_usd"]),
                    row["data_completeness"],
                )
            )
        )
    return "\n".join(lines)


__all__ = ["build_report", "render_report"]
