"""Small, immutable configuration for the SS-001B public measurement."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


_DEFAULT_NOTIONALS = (Decimal("100"), Decimal("250"), Decimal("500"))
_DEFAULT_MARGINS = (
    Decimal("1"),
    Decimal("2"),
    Decimal("3"),
    Decimal("5"),
)
_DEFAULT_HORIZONS = (0, 300, 500, 1000)


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
    """Only the fixed SS-001B measurement grid and bounded runtime limits."""

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
        if self.duration_seconds > 900:
            raise ValueError("duration_seconds must not exceed 900")
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
