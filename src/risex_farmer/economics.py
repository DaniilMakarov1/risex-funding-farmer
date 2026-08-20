"""Pure, exact paper-economics functions.

All numeric inputs must be :class:`~decimal.Decimal`; rejecting floats at the
boundary prevents binary rounding from entering stored paper evidence.
"""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from fractions import Fraction
from functools import reduce
from math import gcd, lcm
from typing import Iterable, Sequence

from .models import (
    BookLevel,
    CanonicalMarket,
    ExactVwap,
    FundingSettlement,
    LiquidityRole,
    PositionSide,
    SettlementStatus,
    Side,
    Venue,
)


def _decimal(value: Decimal, name: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be Decimal")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    return value


def is_tick_aligned(value: Decimal, tick_size: Decimal) -> bool:
    value = _decimal(value, "value")
    tick_size = _decimal(tick_size, "tick_size")
    if tick_size <= 0:
        raise ValueError("tick_size must be positive")
    return value % tick_size == 0


def spread_ticks(best_bid: Decimal, best_ask: Decimal, tick_size: Decimal) -> int:
    best_bid = _decimal(best_bid, "best_bid")
    best_ask = _decimal(best_ask, "best_ask")
    tick_size = _decimal(tick_size, "tick_size")
    if tick_size <= 0:
        raise ValueError("tick_size must be positive")
    if best_bid >= best_ask:
        raise ValueError("best bid must be below best ask")
    if not is_tick_aligned(best_bid, tick_size) or not is_tick_aligned(best_ask, tick_size):
        raise ValueError("BBO must be tick aligned")
    ticks = (best_ask - best_bid) / tick_size
    integral = ticks.to_integral_value()
    if ticks != integral or integral <= 0:
        raise ValueError("spread must be a positive integer tick count")
    return int(integral)


def maker_price(side: Side, best_bid: Decimal, best_ask: Decimal, tick_size: Decimal) -> Decimal:
    ticks = spread_ticks(best_bid, best_ask, tick_size)
    if side is Side.BUY:
        return best_bid if ticks == 1 else best_bid + tick_size
    if side is Side.SELL:
        return best_ask if ticks == 1 else best_ask - tick_size
    raise ValueError(f"unsupported side: {side}")


def _fraction_lcm(left: Fraction, right: Fraction) -> Fraction:
    return Fraction(lcm(left.numerator, right.numerator), gcd(left.denominator, right.denominator))


def canonical_quantity_step(raw_step: Decimal, base_multiplier: Decimal) -> Decimal:
    raw_step = _decimal(raw_step, "raw_step")
    base_multiplier = _decimal(base_multiplier, "base_multiplier")
    if raw_step <= 0 or base_multiplier <= 0:
        raise ValueError("steps and multipliers must be positive")
    return raw_step * base_multiplier


def common_canonical_quantity_step(
    raw_steps_and_multipliers: Iterable[tuple[Decimal, Decimal]],
) -> Decimal:
    steps = [
        Fraction(canonical_quantity_step(step, multiplier))
        for step, multiplier in raw_steps_and_multipliers
    ]
    if not steps:
        raise ValueError("at least one quantity step is required")
    common = reduce(_fraction_lcm, steps)
    return Decimal(common.numerator) / Decimal(common.denominator)


def floor_quantity_to_step(raw_quantity: Decimal, canonical_step: Decimal) -> Decimal:
    raw_quantity = _decimal(raw_quantity, "raw_quantity")
    canonical_step = _decimal(canonical_step, "canonical_step")
    if raw_quantity < 0 or canonical_step <= 0:
        raise ValueError("quantity must be non-negative and step positive")
    return (raw_quantity // canonical_step) * canonical_step


def sized_canonical_quantity(
    target_notional_usd: Decimal,
    planned_hedge_maker_canonical_price: Decimal,
    canonical_step: Decimal,
) -> Decimal:
    target_notional_usd = _decimal(target_notional_usd, "target_notional_usd")
    price = _decimal(planned_hedge_maker_canonical_price, "planned_hedge_maker_canonical_price")
    if target_notional_usd < 0 or price <= 0:
        raise ValueError("target notional must be non-negative and price positive")
    return floor_quantity_to_step(target_notional_usd / price, canonical_step)


def minimum_order_eligible(
    canonical_quantity: Decimal,
    canonical_price: Decimal,
    market: CanonicalMarket,
) -> bool:
    quantity = _decimal(canonical_quantity, "canonical_quantity")
    price = _decimal(canonical_price, "canonical_price")
    if market.base_multiplier is None:
        return False
    multiplier = _decimal(market.base_multiplier, "market.base_multiplier")
    minimum_quantity = _decimal(
        market.minimum_quantity_raw, "market.minimum_quantity_raw"
    )
    minimum_notional = _decimal(
        market.minimum_notional_usd, "market.minimum_notional_usd"
    )
    if quantity <= 0 or price <= 0 or multiplier <= 0:
        return False
    raw_quantity = quantity / multiplier
    return (
        raw_quantity >= minimum_quantity
        and abs(quantity * price) >= minimum_notional
    )


def exact_quantity_vwap(
    side: Side,
    canonical_quantity: Decimal,
    bids: Sequence[BookLevel],
    asks: Sequence[BookLevel],
) -> ExactVwap:
    quantity = _decimal(canonical_quantity, "canonical_quantity")
    if quantity <= 0:
        raise ValueError("canonical_quantity must be positive")
    levels = asks if side is Side.BUY else bids if side is Side.SELL else None
    if levels is None:
        raise ValueError(f"unsupported side: {side}")
    remaining = quantity
    notional = Decimal("0")
    for level in levels:
        price = _decimal(level.canonical_price, "level.canonical_price")
        available = _decimal(level.canonical_quantity, "level.canonical_quantity")
        if price <= 0 or available <= 0:
            raise ValueError("book levels must have positive price and quantity")
        taken = min(remaining, available)
        notional += taken * price
        remaining -= taken
        if remaining == 0:
            return ExactVwap(quantity, quantity, notional, notional / quantity)
    filled = quantity - remaining
    return ExactVwap(quantity, filled, notional, None)


def fee_amount_usd(
    fill_notional_usd: Decimal,
    fee_rate: Decimal,
    minimum_fee_notional_usd: Decimal | None = None,
) -> Decimal:
    notional = abs(_decimal(fill_notional_usd, "fill_notional_usd"))
    rate = _decimal(fee_rate, "fee_rate")
    if rate < 0:
        raise ValueError("fee_rate must be non-negative")
    fee_base = notional
    if minimum_fee_notional_usd is not None:
        minimum = _decimal(minimum_fee_notional_usd, "minimum_fee_notional_usd")
        if minimum < 0:
            raise ValueError("minimum fee notional must be non-negative")
        fee_base = max(fee_base, minimum)
    return fee_base * rate


def venue_fee_amount_usd(
    venue: Venue,
    liquidity_role: LiquidityRole,
    fill_notional_usd: Decimal,
    fee_rate: Decimal,
    minimum_fee_notional_usd: Decimal | None = None,
) -> Decimal:
    minimum = (
        minimum_fee_notional_usd
        if venue is Venue.NADO and liquidity_role is LiquidityRole.TAKER
        else None
    )
    return fee_amount_usd(fill_notional_usd, fee_rate, minimum)


def funding_cash_usd(canonical_base_quantity: Decimal, cash_per_canonical_base_usd: Decimal) -> Decimal:
    quantity = _decimal(canonical_base_quantity, "canonical_base_quantity")
    cash_per_base = _decimal(cash_per_canonical_base_usd, "cash_per_canonical_base_usd")
    return quantity * cash_per_base


def price_pnl_usd(
    position_side: PositionSide,
    canonical_base_quantity: Decimal,
    entry_price: Decimal,
    exit_price: Decimal,
) -> Decimal:
    quantity = _decimal(canonical_base_quantity, "canonical_base_quantity")
    entry = _decimal(entry_price, "entry_price")
    exit_ = _decimal(exit_price, "exit_price")
    if quantity < 0:
        raise ValueError("canonical_base_quantity must be non-negative")
    if position_side is PositionSide.LONG:
        return quantity * (exit_ - entry)
    if position_side is PositionSide.SHORT:
        return quantity * (entry - exit_)
    raise ValueError(f"unsupported position side: {position_side}")


_SKIPPED = {
    SettlementStatus.SKIPPED_POSITION_NOT_OPEN,
    SettlementStatus.SKIPPED_POSITION_CLOSED,
}
_ALLOWED_TRANSITIONS = {
    SettlementStatus.PENDING: {
        SettlementStatus.ESTIMATED,
        SettlementStatus.APPLIED_RATE,
        SettlementStatus.UNRESOLVED,
        *_SKIPPED,
    },
    SettlementStatus.ESTIMATED: {SettlementStatus.APPLIED_RATE, SettlementStatus.UNRESOLVED},
    SettlementStatus.UNRESOLVED: {SettlementStatus.APPLIED_RATE},
}


def replace_funding_settlement(
    current: FundingSettlement,
    new_status: SettlementStatus,
    cash_usd: Decimal | None,
) -> FundingSettlement:
    if new_status not in _ALLOWED_TRANSITIONS.get(current.status, set()):
        raise ValueError(f"invalid settlement transition: {current.status} -> {new_status}")
    if new_status is SettlementStatus.UNRESOLVED and cash_usd is None:
        cash_usd = current.cash_usd
    if cash_usd is not None:
        _decimal(cash_usd, "cash_usd")
    if new_status in _SKIPPED and cash_usd is not None:
        raise ValueError("skipped settlement cannot contain cash")
    if new_status in {SettlementStatus.ESTIMATED, SettlementStatus.APPLIED_RATE} and cash_usd is None:
        raise ValueError(f"{new_status} settlement requires cash")
    return replace(current, status=new_status, cash_usd=cash_usd)


def authoritative_funding_cash_usd(settlement: FundingSettlement) -> Decimal | None:
    if settlement.status in {SettlementStatus.ESTIMATED, SettlementStatus.APPLIED_RATE}:
        if settlement.cash_usd is None:
            raise ValueError(f"{settlement.status} settlement requires cash")
        return _decimal(settlement.cash_usd, "settlement.cash_usd")
    if settlement.status is SettlementStatus.UNRESOLVED:
        return (
            None
            if settlement.cash_usd is None
            else _decimal(settlement.cash_usd, "settlement.cash_usd")
        )
    if settlement.status in _SKIPPED or settlement.status is SettlementStatus.PENDING:
        return Decimal("0")
    return None


def recognized_funding_cash_usd(settlements: Iterable[FundingSettlement]) -> Decimal | None:
    total = Decimal("0")
    for settlement in settlements:
        cash = authoritative_funding_cash_usd(settlement)
        if cash is None:
            return None
        total += cash
    return total


def applied_rate_complete(settlements: Iterable[FundingSettlement]) -> bool:
    return all(
        settlement.status is SettlementStatus.APPLIED_RATE or settlement.status in _SKIPPED
        for settlement in settlements
    )


def closed_net_pnl_usd(
    long_price_pnl_usd: Decimal,
    short_price_pnl_usd: Decimal,
    recognized_funding_usd: Decimal,
    actual_fee_amounts_usd: Iterable[Decimal],
) -> Decimal:
    long_pnl = _decimal(long_price_pnl_usd, "long_price_pnl_usd")
    short_pnl = _decimal(short_price_pnl_usd, "short_price_pnl_usd")
    funding = _decimal(recognized_funding_usd, "recognized_funding_usd")
    fees = sum(
        (_decimal(fee, "actual_fee_amount_usd") for fee in actual_fee_amounts_usd),
        Decimal("0"),
    )
    return funding + long_pnl + short_pnl - fees


def pair_price_pnl_usd(
    canonical_base_quantity: Decimal,
    long_entry_price: Decimal,
    long_exit_price: Decimal,
    short_entry_price: Decimal,
    short_exit_price: Decimal,
) -> Decimal:
    """Return planned or actual pair price PnL using the same exact signs."""

    return price_pnl_usd(
        PositionSide.LONG, canonical_base_quantity, long_entry_price, long_exit_price
    ) + price_pnl_usd(
        PositionSide.SHORT, canonical_base_quantity, short_entry_price, short_exit_price
    )


def planned_maker_net_pnl_usd(
    expected_target_cycle_funding_usd: Decimal,
    planned_execution_pnl_usd: Decimal,
    planned_fee_amounts_usd: Iterable[Decimal],
) -> Decimal:
    funding = _decimal(
        expected_target_cycle_funding_usd, "expected_target_cycle_funding_usd"
    )
    execution = _decimal(planned_execution_pnl_usd, "planned_execution_pnl_usd")
    fees = sum(
        (_decimal(fee, "planned_fee_amount_usd") for fee in planned_fee_amounts_usd),
        Decimal("0"),
    )
    return funding + execution - fees
