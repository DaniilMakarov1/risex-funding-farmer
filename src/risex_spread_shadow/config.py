"""Small, immutable configuration for the SS-001D public measurement."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import hashlib
import json
from typing import Any, Mapping


_DEFAULT_NOTIONALS = (Decimal("100"), Decimal("250"), Decimal("500"))
_DEFAULT_MARGINS = (
    Decimal("1"),
    Decimal("2"),
    Decimal("3"),
    Decimal("5"),
)
_DEFAULT_HORIZONS = (0, 300, 500, 1000)
MAX_PUBLIC_DURATION_SECONDS = 1_200

# SCAN-003 is deliberately a profile, not a general-purpose configuration
# surface.  Keep the research inputs in one place so the producer, offline
# evaluator, and CLI cannot silently drift apart.
FIXED_SCANNER_STAGE_NAMES = ("CAL-001", "HOLDOUT-001")
FIXED_SCANNER_MARKET = "BTC"
FIXED_SCANNER_DIRECTION = "RISEX_SELL_LIGHTER_BUY"
FIXED_SCANNER_NOTIONAL_USD = Decimal("100")
FIXED_SCANNER_MARGINS_BPS = (Decimal("1"), Decimal("2"))
FIXED_SCANNER_HORIZONS_MS = _DEFAULT_HORIZONS
FIXED_SCANNER_RISEX_MAKER_FEE_RATE = Decimal("0.0001")
FIXED_SCANNER_RISEX_FEE_TIER = "TIER_1"
FIXED_SCANNER_RISEX_FEE_PROVENANCE = "SS-001Q"
FIXED_SCANNER_LIGHTER_TAKER_FEE_RATE = Decimal("0")
FIXED_SCANNER_LIGHTER_TAKER_LATENCY_MS = 300
FIXED_SCANNER_LIGHTER_FEE_PROVENANCE = (
    "OFFICIAL_LIGHTER_ACCOUNT_TYPES_2026-09-05"
)
FIXED_SCANNER_ELIGIBLE_TRADE_LIMIT = 250
FIXED_SCANNER_WALL_CLOCK_SECONDS = 1_200
FIXED_SCANNER_RECORD_CAP = 1_000_000
FIXED_SCANNER_BYTE_CAP = 4 * 1024 * 1024 * 1024
FIXED_SCANNER_TERMINAL_DRAIN_ALLOWANCE_NS = 2_200_000_000


def _fingerprint(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode(
            "utf-8"
        )
    ).hexdigest()


def fixed_scanner_policy_fields(accepted_release: str) -> dict[str, Any]:
    """Return the immutable SCAN-003 policy payload before its fingerprint."""

    if not isinstance(accepted_release, str) or not accepted_release:
        raise ValueError("accepted_release must be a non-empty string")
    return {
        "accepted_release": accepted_release,
        "configuration": {
            "canonical_markets": [FIXED_SCANNER_MARKET],
            "direction": FIXED_SCANNER_DIRECTION,
            "target_notionals_usd": [str(FIXED_SCANNER_NOTIONAL_USD)],
            "target_margins_bps": [str(value) for value in FIXED_SCANNER_MARGINS_BPS],
            "horizons_ms": list(FIXED_SCANNER_HORIZONS_MS),
            "quote_lifetime_ns": 5_000_000_000,
            "freshness_max_age_ns": 25_000_000_000,
            "material_fill_stop": False,
        },
        "fees": {
            "risex_maker_fee_rate": str(FIXED_SCANNER_RISEX_MAKER_FEE_RATE),
            "risex_fee_tier": FIXED_SCANNER_RISEX_FEE_TIER,
            "risex_fee_provenance": FIXED_SCANNER_RISEX_FEE_PROVENANCE,
            "lighter_taker_fee_rate": str(FIXED_SCANNER_LIGHTER_TAKER_FEE_RATE),
            "lighter_taker_latency_ms": FIXED_SCANNER_LIGHTER_TAKER_LATENCY_MS,
            "lighter_fee_provenance": FIXED_SCANNER_LIGHTER_FEE_PROVENANCE,
        },
        "formulas": {
            "entry_edge": (
                "q*(risex_sell_price-lighter_buy_vwap)-"
                "q*risex_sell_price*risex_maker_fee-"
                "lighter_buy_notional*lighter_taker_fee"
            ),
            "conditional_markout": "entry_edge_h-entry_edge_0",
            "unit_score": "minimum_clean_strict_filled_value_per_dependence_unit",
            "selector": "common_eligible_units_sum_300ms_zero_for_clean_no_fill_tie_to_1bps",
            "p05": "ordered_values[floor(0.05*(n-1))]",
            "median": "middle_value_or_midpoint_of_two_middle_values",
            "positive_sum": "sum_of_all_clean_unit_values_strictly_greater_than_zero",
        },
        "thresholds": {
            "common_eligible_units": 50,
            "clean_strict_filled_units_per_arm": 20,
            "venue_clusters_per_arm": 20,
            "detection_timestamps_per_arm": 15,
            "paired_clean_units": 20,
            "distinct_effective_levels": 16,
            "distinct_effective_level_share": "0.80",
            "collision_count_max": 4,
            "collision_share_max": "0.20",
            "edge_p05_0ms": "0.01",
            "edge_p05_300ms": "0.01",
            "edge_median_300ms": "0.01",
            "positive_share_300ms": "0.95",
            "markout_p05_300ms": "-0.005",
            "markout_median_300ms": "0",
            "edge_p05_500ms": "0.005",
            "edge_median_500ms": "0.01",
            "markout_p05_500ms": "-0.01",
            "markout_median_500ms": "-0.005",
            "edge_p05_1000ms": "0",
            "edge_median_1000ms": "0.005",
            "markout_p05_1000ms": "-0.015",
            "markout_median_1000ms": "-0.01",
            "full_hedge_share": "1",
            "one_minute_concentration_max": "0.25",
            "five_minute_concentration_max": "0.50",
            "all_horizon_positive_sum": True,
            "strict_positive_p05_1000ms": True,
            "raw_eligible_units_must_all_be_clean": True,
            "strict_episode_requires_four_clean_horizons": True,
            "zero_reversed_effective_levels": True,
            "zero_unresolved_effective_levels": True,
        },
        "stop_contract": {
            "eligible_trade_limit": FIXED_SCANNER_ELIGIBLE_TRADE_LIMIT,
            "wall_clock_seconds": FIXED_SCANNER_WALL_CLOCK_SECONDS,
            "record_cap": FIXED_SCANNER_RECORD_CAP,
            "byte_cap": FIXED_SCANNER_BYTE_CAP,
            "terminal_drain_allowance_ns": FIXED_SCANNER_TERMINAL_DRAIN_ALLOWANCE_NS,
            "fill_count_stop": None,
            "after_stop": "drain_already_pending_horizons_only",
            "retry": False,
            "extension": False,
            "parameter_change": False,
        },
    }


def fixed_scanner_policy_fingerprint(accepted_release: str) -> str:
    """Return the stable SCAN-003 policy fingerprint for one release."""

    return _fingerprint(fixed_scanner_policy_fields(accepted_release))


def is_exact_release(value: Any) -> bool:
    """Return whether a release identity is a full lowercase Git SHA."""

    return (
        isinstance(value, str)
        and len(value) == 40
        and value == value.lower()
        and all(character in "0123456789abcdef" for character in value)
    )


def fixed_scanner_stage_payload(
    *,
    stage_name: str,
    stage_kind: str,
    run_id: str,
    accepted_release: str,
    sample_interval: Mapping[str, Any],
    policy_fingerprint: str,
) -> dict[str, Any]:
    """Build the exact payload hashed into a terminal-bound stage identity."""

    return {
        "stage_kind": stage_kind,
        "stage_name": stage_name,
        "run_id": run_id,
        "accepted_release": accepted_release,
        "policy_fingerprint": policy_fingerprint,
        "sample_interval": dict(sample_interval),
    }


def fixed_scanner_stage_fingerprint(
    *,
    stage_name: str,
    stage_kind: str,
    run_id: str,
    accepted_release: str,
    sample_interval: Mapping[str, Any],
    policy_fingerprint: str,
) -> str:
    return _fingerprint(
        fixed_scanner_stage_payload(
            stage_name=stage_name,
            stage_kind=stage_kind,
            run_id=run_id,
            accepted_release=accepted_release,
            sample_interval=sample_interval,
            policy_fingerprint=policy_fingerprint,
        )
    )


def _positive_decimal_tuple(value: tuple[Decimal, ...], name: str) -> tuple[Decimal, ...]:
    if not value:
        raise ValueError(f"{name} must not be empty")
    if any(not isinstance(item, Decimal) for item in value):
        raise TypeError(f"{name} must contain Decimal values")
    result = tuple(value)
    if any(not item.is_finite() or item <= 0 for item in result):
        raise ValueError(f"{name} must contain positive finite values")
    if len(set(result)) != len(result):
        raise ValueError(f"{name} must not contain duplicates")
    return result


@dataclass(frozen=True, slots=True)
class ShadowConfig:
    """Only the fixed SS-001D measurement grid and bounded runtime limits."""

    target_notionals_usd: tuple[Decimal, ...] = _DEFAULT_NOTIONALS
    target_margins_bps: tuple[Decimal, ...] = _DEFAULT_MARGINS
    horizons_ms: tuple[int, ...] = _DEFAULT_HORIZONS
    quote_lifetime_ns: int = 5_000_000_000
    freshness_max_age_ns: int = 25_000_000_000
    trade_retention_ns: int = 30_000_000_000
    book_history_retention_ns: int = 120_000_000_000
    book_history_capacity: int | None = None
    ingress_queue_capacity: int = 4096
    store_batch_size: int = 128
    store_batch_interval_seconds: float = 0.25
    max_markets: int = 3
    duration_seconds: int = 60
    strict_episode_limit: int = 50
    eligible_trade_limit: int = 500
    sample_wall_clock_seconds: int = 1_200
    risex_maker_fee_rate: Decimal = Decimal("0.00005")
    lighter_taker_fee_rate: Decimal = Decimal("0")
    risex_fee_source: str = "CONFIGURED_RISEX_RESEARCH_INPUT"
    lighter_fee_source: str = "OFFICIAL_LIGHTER_STANDARD_RESEARCH_INPUT"

    def __post_init__(self) -> None:
        notionals = _positive_decimal_tuple(self.target_notionals_usd, "target_notionals_usd")
        margins = _positive_decimal_tuple(self.target_margins_bps, "target_margins_bps")
        object.__setattr__(self, "target_notionals_usd", notionals)
        object.__setattr__(self, "target_margins_bps", margins)
        horizons = tuple(self.horizons_ms)
        if horizons != _DEFAULT_HORIZONS:
            raise ValueError("SS-001B horizons are fixed at 0/300/500/1000 ms")
        object.__setattr__(self, "horizons_ms", horizons)
        if any(isinstance(item, bool) or not isinstance(item, int) or item < 0 for item in horizons):
            raise TypeError("horizons_ms must contain non-negative integers")
        for value, name in (
            (self.quote_lifetime_ns, "quote_lifetime_ns"),
            (self.freshness_max_age_ns, "freshness_max_age_ns"),
            (self.trade_retention_ns, "trade_retention_ns"),
            (self.book_history_retention_ns, "book_history_retention_ns"),
            (self.ingress_queue_capacity, "ingress_queue_capacity"),
            (self.store_batch_size, "store_batch_size"),
            (self.max_markets, "max_markets"),
            (self.duration_seconds, "duration_seconds"),
            (self.strict_episode_limit, "strict_episode_limit"),
            (self.eligible_trade_limit, "eligible_trade_limit"),
            (self.sample_wall_clock_seconds, "sample_wall_clock_seconds"),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.book_history_capacity is not None and (
            isinstance(self.book_history_capacity, bool)
            or not isinstance(self.book_history_capacity, int)
            or self.book_history_capacity <= 0
        ):
            raise ValueError("book_history_capacity must be a positive integer or None")
        if (
            isinstance(self.store_batch_interval_seconds, bool)
            or not isinstance(self.store_batch_interval_seconds, (int, float))
            or not float(self.store_batch_interval_seconds) > 0
            or not float(self.store_batch_interval_seconds) < float("inf")
        ):
            raise ValueError("store_batch_interval_seconds must be positive")
        if self.max_markets > 3:
            raise ValueError("max_markets must not exceed three")
        if self.duration_seconds > MAX_PUBLIC_DURATION_SECONDS:
            raise ValueError(
                f"duration_seconds must not exceed {MAX_PUBLIC_DURATION_SECONDS}"
            )
        if self.sample_wall_clock_seconds < 1:
            raise ValueError("sample_wall_clock_seconds must be positive")
        for value, name in (
            (self.risex_maker_fee_rate, "risex_maker_fee_rate"),
            (self.lighter_taker_fee_rate, "lighter_taker_fee_rate"),
        ):
            if not isinstance(value, Decimal) or not value.is_finite() or value < 0 or value >= 1:
                raise ValueError(f"{name} must be a Decimal in [0, 1)")
        if not self.risex_fee_source or not self.lighter_fee_source:
            raise ValueError("fee sources must be non-empty")

    @property
    def policy_count(self) -> int:
        return 2 * len(self.target_notionals_usd) * len(self.target_margins_bps)

    @property
    def sample_wall_clock_limit_ns(self) -> int:
        return self.sample_wall_clock_seconds * 1_000_000_000


def fixed_scanner_config() -> ShadowConfig:
    """Return the only configuration admitted by the SCAN-003 command."""

    return ShadowConfig(
        target_notionals_usd=(FIXED_SCANNER_NOTIONAL_USD,),
        target_margins_bps=FIXED_SCANNER_MARGINS_BPS,
        horizons_ms=FIXED_SCANNER_HORIZONS_MS,
        max_markets=1,
        duration_seconds=FIXED_SCANNER_WALL_CLOCK_SECONDS,
        strict_episode_limit=50,
        eligible_trade_limit=FIXED_SCANNER_ELIGIBLE_TRADE_LIMIT,
        sample_wall_clock_seconds=FIXED_SCANNER_WALL_CLOCK_SECONDS,
        risex_maker_fee_rate=FIXED_SCANNER_RISEX_MAKER_FEE_RATE,
        lighter_taker_fee_rate=FIXED_SCANNER_LIGHTER_TAKER_FEE_RATE,
        risex_fee_source=FIXED_SCANNER_RISEX_FEE_PROVENANCE,
        lighter_fee_source=FIXED_SCANNER_LIGHTER_FEE_PROVENANCE,
    )


__all__ = [
    "FIXED_SCANNER_BYTE_CAP",
    "FIXED_SCANNER_DIRECTION",
    "FIXED_SCANNER_ELIGIBLE_TRADE_LIMIT",
    "FIXED_SCANNER_HORIZONS_MS",
    "FIXED_SCANNER_LIGHTER_FEE_PROVENANCE",
    "FIXED_SCANNER_LIGHTER_TAKER_FEE_RATE",
    "FIXED_SCANNER_LIGHTER_TAKER_LATENCY_MS",
    "FIXED_SCANNER_MARKET",
    "FIXED_SCANNER_MARGINS_BPS",
    "FIXED_SCANNER_NOTIONAL_USD",
    "FIXED_SCANNER_RECORD_CAP",
    "FIXED_SCANNER_RISEX_FEE_PROVENANCE",
    "FIXED_SCANNER_RISEX_FEE_TIER",
    "FIXED_SCANNER_RISEX_MAKER_FEE_RATE",
    "FIXED_SCANNER_STAGE_NAMES",
    "FIXED_SCANNER_TERMINAL_DRAIN_ALLOWANCE_NS",
    "FIXED_SCANNER_WALL_CLOCK_SECONDS",
    "MAX_PUBLIC_DURATION_SECONDS",
    "ShadowConfig",
    "fixed_scanner_config",
    "is_exact_release",
    "fixed_scanner_policy_fields",
    "fixed_scanner_policy_fingerprint",
    "fixed_scanner_stage_fingerprint",
    "fixed_scanner_stage_payload",
]
