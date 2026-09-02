"""Deterministic quote, sizing, VWAP, and fee calculations for SS-001A."""

from __future__ import annotations

from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Iterable, Sequence

from risex_farmer.economics import (
    common_canonical_quantity_step,
    exact_quantity_vwap,
    is_tick_aligned,
)
from risex_farmer.models import BookLevel, CanonicalMarket, ExactVwap, LiquidityRole, Side, Venue

from .models import (
    BookEvidence,
    EntryViabilityOutcome,
    FeeEvidence,
    HypotheticalMakerQuote,
    QuotePolicy,
    SizingEvidence,
    SpreadDirection,
)


def _decimal(value: Decimal, name: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be Decimal")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    return value


def _levels(book: object, side: Side) -> tuple[BookLevel, ...]:
    if side is Side.BUY:
        levels = getattr(book, "asks", None)
    elif side is Side.SELL:
        levels = getattr(book, "bids", None)
    else:
        raise ValueError(f"unsupported side: {side}")
    if levels is None:
        raise ValueError("book does not expose the requested side")
    if not isinstance(levels, tuple):
        levels = tuple(levels)
    return levels


def _market_for(market: CanonicalMarket | None, venue: Venue, canonical_market: str) -> CanonicalMarket:
    if market is None:
        raise ValueError(f"{venue} market metadata is required")
    if market.venue is not venue:
        raise ValueError(f"market metadata must identify {venue}")
    identities = tuple(
        value
        for value in (
            getattr(market, "canonical_market", None),
            getattr(market, "venue_symbol", None),
            getattr(market, "canonical_asset", None),
        )
        if isinstance(value, str) and value
    )
    if not identities:
        raise ValueError("market must expose a non-empty canonical identity")
    if canonical_market not in identities:
        raise ValueError("market metadata does not match policy market")
    if market.base_multiplier is None:
        raise ValueError("market base multiplier is required")
    return market


def _raw_quantity(canonical_quantity: Decimal, market: CanonicalMarket) -> Decimal:
    if market.base_multiplier is None or market.base_multiplier <= 0:
        raise ValueError("market base multiplier must be positive")
    return canonical_quantity / market.base_multiplier


def _minimum_flags(
    quantity: Decimal,
    price: Decimal,
    market: CanonicalMarket,
) -> tuple[bool, bool]:
    raw_quantity = _raw_quantity(quantity, market)
    return (
        raw_quantity >= market.minimum_quantity_raw,
        abs(quantity * price) >= market.minimum_notional_usd,
    )


def _reference_price(direction: SpreadDirection, book: object) -> Decimal:
    levels = _levels(book, direction.hedge_side)
    if not levels:
        raise ValueError("required-side Lighter book is empty")
    price = _decimal(levels[0].canonical_price, "required-side top price")
    if price <= 0:
        raise ValueError("required-side top price must be positive")
    return price


def compute_sizing_evidence(
    policy: QuotePolicy,
    risex_market: CanonicalMarket,
    lighter_market: CanonicalMarket,
    lighter_book: object,
    *,
    risex_validation_price: Decimal | None = None,
) -> SizingEvidence:
    """Compute the one deterministic quantity for a policy.

    The required-side Lighter top price is the sole sizing reference.  The
    retained raw quantities, common step, and minimum flags make the result
    independently auditable and allow callers to reject forged evidence.
    """

    risex_market = _market_for(risex_market, Venue.RISEX, policy.canonical_market)
    lighter_market = _market_for(lighter_market, Venue.LIGHTER, policy.canonical_market)
    reference_price = _reference_price(policy.direction, lighter_book)
    if risex_validation_price is None:
        risex_validation_price = (
            policy.risex_best_bid
            if policy.direction.maker_side is Side.BUY
            else policy.risex_best_ask
        )
    if risex_validation_price is None:
        risex_validation_price = reference_price
    risex_validation_price = _decimal(risex_validation_price, "risex_validation_price")
    if risex_validation_price <= 0:
        raise ValueError("risex_validation_price must be positive")
    if risex_market.base_multiplier is None or lighter_market.base_multiplier is None:
        raise ValueError("both market base multipliers are required")
    common_step = common_canonical_quantity_step(
        (
            (risex_market.quantity_step_raw, risex_market.base_multiplier),
            (lighter_market.quantity_step_raw, lighter_market.base_multiplier),
        )
    )
    target = _decimal(policy.target_notional_usd, "target_notional_usd")
    q_raw = target / reference_price
    floored = (q_raw // common_step) * common_step
    risex_raw = _raw_quantity(floored, risex_market)
    lighter_raw = _raw_quantity(floored, lighter_market)
    risex_min_quantity_ok, risex_min_notional_ok = _minimum_flags(
        floored, risex_validation_price, risex_market
    )
    lighter_min_quantity_ok, lighter_min_notional_ok = _minimum_flags(
        floored, reference_price, lighter_market
    )
    return SizingEvidence(
        canonical_market=policy.canonical_market,
        direction=policy.direction,
        target_notional_usd=target,
        reference_price=reference_price,
        risex_validation_price=risex_validation_price,
        q_raw=q_raw,
        common_quantity_step=common_step,
        floored_quantity=floored,
        risex_raw_quantity=risex_raw,
        lighter_raw_quantity=lighter_raw,
        risex_quantity_step_raw=risex_market.quantity_step_raw,
        lighter_quantity_step_raw=lighter_market.quantity_step_raw,
        risex_base_multiplier=risex_market.base_multiplier,
        lighter_base_multiplier=lighter_market.base_multiplier,
        risex_minimum_quantity_raw=risex_market.minimum_quantity_raw,
        lighter_minimum_quantity_raw=lighter_market.minimum_quantity_raw,
        risex_minimum_notional_usd=risex_market.minimum_notional_usd,
        lighter_minimum_notional_usd=lighter_market.minimum_notional_usd,
        risex_min_quantity_ok=risex_min_quantity_ok,
        risex_min_notional_ok=risex_min_notional_ok,
        lighter_min_quantity_ok=lighter_min_quantity_ok,
        lighter_min_notional_ok=lighter_min_notional_ok,
        risex_market=risex_market,
        lighter_market=lighter_market,
    )


def validate_sizing_evidence(
    evidence: SizingEvidence | None,
    policy: QuotePolicy,
    lighter_book: object,
    *,
    risex_market: CanonicalMarket | None = None,
    lighter_market: CanonicalMarket | None = None,
    risex_validation_price: Decimal | None = None,
) -> bool:
    """Recompute every sizing field and compare ordinary immutable values."""

    if evidence is None:
        return False
    if evidence.direction is not policy.direction:
        return False
    if evidence.canonical_market != policy.canonical_market:
        return False
    if evidence.target_notional_usd != policy.target_notional_usd:
        return False
    risex_market = risex_market or evidence.risex_market or policy.risex_market
    lighter_market = lighter_market or evidence.lighter_market or policy.lighter_market
    if risex_market is None or lighter_market is None:
        return False
    if risex_validation_price is None:
        if policy.risex_best_bid is not None and policy.risex_best_ask is not None:
            risex_validation_price = (
                policy.risex_best_bid
                if policy.direction.maker_side is Side.BUY
                else policy.risex_best_ask
            )
        else:
            risex_validation_price = evidence.risex_validation_price
    try:
        expected = compute_sizing_evidence(
            policy,
            risex_market,
            lighter_market,
            lighter_book,
            risex_validation_price=risex_validation_price,
        )
    except (TypeError, ValueError, ArithmeticError):
        return False
    return evidence == expected


def exact_vwap(
    side: Side,
    canonical_quantity: Decimal,
    bids: Sequence[BookLevel],
    asks: Sequence[BookLevel],
) -> ExactVwap:
    """Use the accepted exact-q walk while retaining exact accumulated notional."""

    return exact_quantity_vwap(side, canonical_quantity, bids, asks)


def exact_entry_edge_usd(
    direction: SpreadDirection,
    canonical_quantity: Decimal,
    risex_maker_price: Decimal,
    lighter_hedge_notional_usd: Decimal,
    entry_fee_usd: Decimal,
) -> Decimal:
    """Return the direction-correct edge using exact hedge notional."""

    quantity = _decimal(canonical_quantity, "canonical_quantity")
    maker_price = _decimal(risex_maker_price, "risex_maker_price")
    hedge_notional = _decimal(lighter_hedge_notional_usd, "lighter_hedge_notional_usd")
    fees = _decimal(entry_fee_usd, "entry_fee_usd")
    if quantity <= 0 or maker_price <= 0 or hedge_notional < 0 or fees < 0:
        raise ValueError("edge inputs must be positive or non-negative")
    maker_notional = quantity * maker_price
    if direction.maker_side is Side.BUY:
        return hedge_notional - maker_notional - fees
    return maker_notional - hedge_notional - fees


def _fee(
    venue: Venue,
    role: LiquidityRole,
    fill_notional_usd: Decimal,
    rate: Decimal,
    source: str,
    configured_at,
) -> FeeEvidence:
    notional = abs(_decimal(fill_notional_usd, "fill_notional_usd"))
    fee_rate = _decimal(rate, "fee_rate")
    if fee_rate < 0:
        raise ValueError("fee rate must be non-negative")
    amount = notional * fee_rate
    return FeeEvidence(
        venue=venue,
        liquidity_role=role,
        fill_notional_usd=notional,
        fee_base_notional_usd=notional,
        rate=fee_rate,
        amount_usd=amount,
        source=source,
        observed_or_configured_at=configured_at,
    )


def _round_down_to_tick(value: Decimal, tick: Decimal) -> Decimal:
    value = _decimal(value, "value")
    tick = _decimal(tick, "tick")
    if tick <= 0:
        raise ValueError("tick must be positive")
    return (value / tick).to_integral_value(rounding=ROUND_FLOOR) * tick


def _round_up_to_tick(value: Decimal, tick: Decimal) -> Decimal:
    value = _decimal(value, "value")
    tick = _decimal(tick, "tick")
    if tick <= 0:
        raise ValueError("tick must be positive")
    return (value / tick).to_integral_value(rounding=ROUND_CEILING) * tick


def _invalid_quote(
    policy: QuotePolicy,
    outcome: EntryViabilityOutcome,
    *,
    sizing_evidence: SizingEvidence | None = None,
    canonical_quantity: Decimal | None = None,
    lighter_filled_quantity: Decimal | None = None,
    lighter_notional_usd: Decimal | None = None,
    lighter_vwap_price: Decimal | None = None,
) -> HypotheticalMakerQuote:
    return HypotheticalMakerQuote(
        policy=policy,
        outcome=outcome,
        maker_side=policy.direction.maker_side,
        lighter_side=policy.direction.hedge_side,
        canonical_quantity=canonical_quantity,
        maker_price=None,
        lighter_vwap_price=lighter_vwap_price,
        lighter_filled_quantity=lighter_filled_quantity,
        lighter_notional_usd=lighter_notional_usd,
        maker_notional_usd=None,
        fee_components=(),
        total_entry_fees_usd=None,
        target_edge_usd=None,
        actual_edge_usd=None,
        raw_risex_price_bound=None,
        post_only_bound_price=None,
        sizing_evidence=sizing_evidence,
        exact_hedge_vwap=None,
    )


def build_hypothetical_maker_quote(
    policy: QuotePolicy,
    lighter_book: object,
    *,
    risex_market: CanonicalMarket | None = None,
    lighter_market: CanonicalMarket | None = None,
    risex_best_bid: Decimal | None = None,
    risex_best_ask: Decimal | None = None,
    risex_tick_size: Decimal | None = None,
    sizing_evidence: SizingEvidence | None = None,
) -> HypotheticalMakerQuote:
    """Build one quote and fail closed when any required economics is absent."""

    risex_market = risex_market or policy.risex_market
    lighter_market = lighter_market or policy.lighter_market
    best_bid = risex_best_bid if risex_best_bid is not None else policy.risex_best_bid
    best_ask = risex_best_ask if risex_best_ask is not None else policy.risex_best_ask
    tick = risex_tick_size if risex_tick_size is not None else policy.risex_tick_size
    if risex_market is None or lighter_market is None or best_bid is None or best_ask is None or tick is None:
        return _invalid_quote(policy, EntryViabilityOutcome.QUOTE_NOT_ECONOMIC)
    try:
        best_bid = _decimal(best_bid, "risex_best_bid")
        best_ask = _decimal(best_ask, "risex_best_ask")
        tick = _decimal(tick, "risex_tick_size")
        if (
            best_bid <= 0
            or best_ask <= 0
            or tick <= 0
            or best_bid >= best_ask
            or not is_tick_aligned(best_bid, tick)
            or not is_tick_aligned(best_ask, tick)
        ):
            return _invalid_quote(policy, EntryViabilityOutcome.QUOTE_NOT_POST_ONLY)
        validation_price = best_bid if policy.direction.maker_side is Side.BUY else best_ask
        if sizing_evidence is None:
            sizing_evidence = compute_sizing_evidence(
                policy,
                risex_market,
                lighter_market,
                lighter_book,
                risex_validation_price=validation_price,
            )
        elif not validate_sizing_evidence(
            sizing_evidence,
            policy,
            lighter_book,
            risex_market=risex_market,
            lighter_market=lighter_market,
            risex_validation_price=validation_price,
        ):
            return _invalid_quote(
                policy,
                EntryViabilityOutcome.QUOTE_NOT_ECONOMIC,
                sizing_evidence=sizing_evidence,
            )
        if not sizing_evidence.is_valid:
            return _invalid_quote(
                policy,
                EntryViabilityOutcome.QUOTE_NOT_ECONOMIC,
                sizing_evidence=sizing_evidence,
                canonical_quantity=sizing_evidence.floored_quantity,
            )
        q = sizing_evidence.floored_quantity
        vwap = exact_quantity_vwap(
            policy.direction.hedge_side,
            q,
            tuple(getattr(lighter_book, "bids")),
            tuple(getattr(lighter_book, "asks")),
        )
        if not vwap.is_executable or vwap.price is None:
            return _invalid_quote(
                policy,
                EntryViabilityOutcome.QUOTE_NOT_ECONOMIC,
                sizing_evidence=sizing_evidence,
                canonical_quantity=q,
                lighter_filled_quantity=vwap.filled_quantity,
                lighter_notional_usd=vwap.notional_usd,
            )
        if vwap.notional_usd < lighter_market.minimum_notional_usd:
            return _invalid_quote(
                policy,
                EntryViabilityOutcome.QUOTE_NOT_ECONOMIC,
                sizing_evidence=sizing_evidence,
                canonical_quantity=q,
                lighter_filled_quantity=q,
                lighter_notional_usd=vwap.notional_usd,
                lighter_vwap_price=vwap.price,
            )
        hedge_price = vwap.price
        margin = policy.target_margin_bps / Decimal("10000")
        risex_fee = policy.risex_maker_fee_rate
        lighter_fee = policy.lighter_taker_fee_rate
        if policy.direction.maker_side is Side.BUY:
            numerator = Decimal("1") - lighter_fee - margin
            if numerator <= 0:
                return _invalid_quote(
                    policy,
                    EntryViabilityOutcome.QUOTE_NOT_ECONOMIC,
                    sizing_evidence=sizing_evidence,
                    canonical_quantity=q,
                    lighter_filled_quantity=q,
                    lighter_notional_usd=vwap.notional_usd,
                    lighter_vwap_price=hedge_price,
                )
            raw_bound = hedge_price * numerator / (Decimal("1") + risex_fee)
            post_only_bound = best_ask - tick
            maker_price = min(_round_down_to_tick(raw_bound, tick), post_only_bound)
        else:
            denominator = Decimal("1") - risex_fee
            if denominator <= 0:
                return _invalid_quote(
                    policy,
                    EntryViabilityOutcome.QUOTE_NOT_ECONOMIC,
                    sizing_evidence=sizing_evidence,
                    canonical_quantity=q,
                    lighter_filled_quantity=q,
                    lighter_notional_usd=vwap.notional_usd,
                    lighter_vwap_price=hedge_price,
                )
            raw_bound = hedge_price * (Decimal("1") + lighter_fee + margin) / denominator
            post_only_bound = best_bid + tick
            maker_price = max(_round_up_to_tick(raw_bound, tick), post_only_bound)
        if maker_price <= 0:
            return _invalid_quote(
                policy,
                EntryViabilityOutcome.QUOTE_NOT_ECONOMIC,
                sizing_evidence=sizing_evidence,
                canonical_quantity=q,
                lighter_filled_quantity=q,
                lighter_notional_usd=vwap.notional_usd,
                lighter_vwap_price=hedge_price,
            )
        post_only = (
            maker_price <= best_ask - tick
            if policy.direction.maker_side is Side.BUY
            else maker_price >= best_bid + tick
        )
        if not post_only:
            return _invalid_quote(
                policy,
                EntryViabilityOutcome.QUOTE_NOT_POST_ONLY,
                sizing_evidence=sizing_evidence,
                canonical_quantity=q,
                lighter_filled_quantity=q,
                lighter_notional_usd=vwap.notional_usd,
                lighter_vwap_price=hedge_price,
            )
        maker_notional = q * maker_price
        risex_min_quantity_ok, risex_min_notional_ok = _minimum_flags(
            q, maker_price, risex_market
        )
        if not (risex_min_quantity_ok and risex_min_notional_ok):
            return _invalid_quote(
                policy,
                EntryViabilityOutcome.QUOTE_NOT_ECONOMIC,
                sizing_evidence=sizing_evidence,
                canonical_quantity=q,
                lighter_filled_quantity=q,
                lighter_notional_usd=vwap.notional_usd,
                lighter_vwap_price=hedge_price,
            )
        maker_fee = _fee(
            Venue.RISEX,
            LiquidityRole.MAKER,
            maker_notional,
            risex_fee,
            policy.risex_fee_source,
            policy.fee_observed_or_configured_at,
        )
        lighter_fee_evidence = _fee(
            Venue.LIGHTER,
            LiquidityRole.TAKER,
            vwap.notional_usd,
            lighter_fee,
            policy.lighter_fee_source,
            policy.fee_observed_or_configured_at,
        )
        fees = (maker_fee, lighter_fee_evidence)
        total_fees = sum((fee.amount_usd for fee in fees), Decimal("0"))
        actual_edge = exact_entry_edge_usd(
            policy.direction,
            q,
            maker_price,
            vwap.notional_usd,
            total_fees,
        )
        target_edge = vwap.notional_usd * margin
        outcome = (
            EntryViabilityOutcome.QUOTE_ACTIVE
            if actual_edge >= target_edge
            else EntryViabilityOutcome.QUOTE_NOT_ECONOMIC
        )
        quote = HypotheticalMakerQuote(
            policy=policy,
            outcome=outcome,
            maker_side=policy.direction.maker_side,
            lighter_side=policy.direction.hedge_side,
            canonical_quantity=q,
            maker_price=maker_price,
            lighter_vwap_price=hedge_price,
            lighter_filled_quantity=q,
            lighter_notional_usd=vwap.notional_usd,
            maker_notional_usd=maker_notional,
            fee_components=fees,
            total_entry_fees_usd=total_fees,
            target_edge_usd=target_edge,
            actual_edge_usd=actual_edge,
            raw_risex_price_bound=raw_bound,
            post_only_bound_price=post_only_bound,
            sizing_evidence=sizing_evidence,
            exact_hedge_vwap=vwap,
            risex_tick_size=tick,
        )
        if not validate_quote_economics(quote, require_target=False):
            return _invalid_quote(
                policy,
                EntryViabilityOutcome.QUOTE_NOT_ECONOMIC,
                sizing_evidence=sizing_evidence,
                canonical_quantity=q,
                lighter_filled_quantity=q,
                lighter_notional_usd=vwap.notional_usd,
                lighter_vwap_price=hedge_price,
            )
        return quote
    except (AttributeError, TypeError, ValueError, ArithmeticError):
        return _invalid_quote(policy, EntryViabilityOutcome.QUOTE_NOT_ECONOMIC)


def validate_quote_economics(
    quote: HypotheticalMakerQuote,
    *,
    require_target: bool = True,
) -> bool:
    """Recompute quote-local arithmetic without trusting stored edge fields."""

    q = quote.canonical_quantity
    maker_price = quote.maker_price
    hedge_notional = quote.lighter_notional_usd
    maker_notional = quote.maker_notional_usd
    total = quote.total_entry_fees_usd
    target = quote.target_edge_usd
    actual = quote.actual_edge_usd
    if any(value is None for value in (q, maker_price, hedge_notional, maker_notional, total, target, actual)):
        return False
    if not isinstance(quote.sizing_evidence, SizingEvidence):
        return False
    if not quote.sizing_evidence.is_valid:
        return False
    if not isinstance(quote.exact_hedge_vwap, ExactVwap):
        return False
    if quote.sizing_evidence.direction is not quote.policy.direction:
        return False
    if quote.sizing_evidence.target_notional_usd != quote.policy.target_notional_usd:
        return False
    assert q is not None and maker_price is not None and hedge_notional is not None
    assert maker_notional is not None and total is not None and target is not None and actual is not None
    if q <= 0 or maker_price <= 0 or hedge_notional <= 0:
        return False
    if quote.lighter_filled_quantity != q or quote.lighter_vwap_price is None:
        return False
    if maker_notional != q * maker_price:
        return False
    if len(quote.fee_components) != 2:
        return False
    expected_fee_identity = {
        (Venue.RISEX, LiquidityRole.MAKER),
        (Venue.LIGHTER, LiquidityRole.TAKER),
    }
    if {
        (fee.venue, fee.liquidity_role) for fee in quote.fee_components
    } != expected_fee_identity:
        return False
    computed_total = sum((fee.amount_usd for fee in quote.fee_components), Decimal("0"))
    if total != computed_total:
        return False
    for fee in quote.fee_components:
        if fee.amount_usd != fee.fee_base_notional_usd * fee.rate:
            return False
        if fee.fill_notional_usd != fee.fee_base_notional_usd:
            return False
        if fee.venue is Venue.RISEX and (
            fee.rate != quote.policy.risex_maker_fee_rate
            or fee.fee_base_notional_usd != maker_notional
        ):
            return False
        if fee.venue is Venue.LIGHTER and (
            fee.rate != quote.policy.lighter_taker_fee_rate
            or fee.fee_base_notional_usd != hedge_notional
        ):
            return False
    computed_actual = exact_entry_edge_usd(
        quote.policy.direction,
        q,
        maker_price,
        hedge_notional,
        computed_total,
    )
    computed_target = hedge_notional * (quote.policy.target_margin_bps / Decimal("10000"))
    tick = quote.risex_tick_size or quote.policy.risex_tick_size
    if tick is None or tick <= 0 or not is_tick_aligned(maker_price, tick):
        return False
    if quote.policy.risex_best_bid is not None and quote.policy.risex_best_ask is not None:
        expected_bound = (
            quote.policy.risex_best_ask - tick
            if quote.policy.direction.maker_side is Side.BUY
            else quote.policy.risex_best_bid + tick
        )
        if quote.post_only_bound_price != expected_bound:
            return False
        if quote.policy.direction.maker_side is Side.BUY and maker_price > expected_bound:
            return False
        if quote.policy.direction.maker_side is Side.SELL and maker_price < expected_bound:
            return False
    return actual == computed_actual and target == computed_target and (
        not require_target or actual >= target
    )


# Short names kept as direct domain operations, not alternate implementations.
size_quote = build_hypothetical_maker_quote
validate_quote = validate_quote_economics


__all__ = [
    "build_hypothetical_maker_quote",
    "compute_sizing_evidence",
    "exact_entry_edge_usd",
    "exact_vwap",
    "size_quote",
    "validate_quote",
    "validate_quote_economics",
    "validate_sizing_evidence",
]
