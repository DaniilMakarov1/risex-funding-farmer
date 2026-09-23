"""Independent arithmetic for conservative Robinhood opening snapshots."""
from dataclasses import replace
from decimal import Decimal

import pytest

from risex_spread_shadow.hood_handoff.contracts import AccountMarginEvidence, Direction, PreflightBlocked
from risex_spread_shadow.hood_handoff.random_cycle import (
    _opening_budget_components, compute_quantity_bounds, minimal_sufficient_leverage_fraction,
)
from test_hood_handoff_random_cycle import account, metadata


def _flat_with_margin(index, balance):
    original = account(index, Decimal(0), available_balance=Decimal(balance))
    evidence = AccountMarginEvidence.from_response(
        account_index=index, market_id=7, source_identity=original.source_identity,
        observed_at=original.observed_at,
        selected_position={'margin_mode': 0, 'initial_margin_fraction': '100.00'},
        account={},
    )
    return replace(original, margin_evidence=evidence)


def test_cycle050_fee_component_changes_minimum_fraction_without_arbitrary_reserve():
    # Independently: 63.638626 * 0.00035 = 0.02227351910;
    # (22.054166 - fee) * 10000 / 63.638626 floors to 3462.
    market = replace(metadata(), mark_price=Decimal('63.638626'))
    receiver = _flat_with_margin(22, '22.054166')
    parts = _opening_budget_components(market, receiver, label='receiver',
        quantity=Decimal(1), worst_price=Decimal('63.638626'), side='BUY')
    assert parts['mark_notional'] == Decimal('63.638626')
    assert parts['fee_cost'] == Decimal('0.02227351910')
    assert parts['adverse_entry_loss'] == 0
    fraction = minimal_sufficient_leverage_fraction(Decimal('22.054166'),
        parts['mark_notional'], 2500, fee_cost=parts['fee_cost'],
        adverse_entry_loss=parts['adverse_entry_loss'])
    assert fraction == 3462
    assert Decimal('22.054166') - parts['fee_cost'] - parts['mark_notional'] * fraction / 10000 == Decimal('0.00020015970')


def test_adverse_entry_price_reduces_collateral_and_moves_fraction():
    market = replace(metadata(), mark_price=Decimal('40'))
    receiver = _flat_with_margin(22, '20')
    parts = _opening_budget_components(market, receiver, label='receiver',
        quantity=Decimal(1), worst_price=Decimal('40.4'), side='BUY')
    assert parts['fee_cost'] == Decimal('0.014140')
    assert parts['adverse_entry_loss'] == Decimal('0.4')
    assert minimal_sufficient_leverage_fraction(Decimal('20'), Decimal('40'), 2500,
        fee_cost=parts['fee_cost'], adverse_entry_loss=parts['adverse_entry_loss']) == 4896


def test_exact_fourfold_cap_excludes_fee_funded_boundary_and_missing_mark():
    market = replace(metadata(), mark_price=Decimal('80'))
    receiver = _flat_with_margin(22, '20')
    parts = _opening_budget_components(market, receiver, label='receiver',
        quantity=Decimal(1), worst_price=Decimal('80'), side='BUY')
    assert parts['fee_cost'] == Decimal('0.02800')
    with pytest.raises(PreflightBlocked, match='more than supported 4x'):
        minimal_sufficient_leverage_fraction(Decimal('20'), Decimal('80'), 2500,
            fee_cost=parts['fee_cost'])
    with pytest.raises(PreflightBlocked, match='mark price is missing'):
        _opening_budget_components(metadata(), receiver, label='receiver',
            quantity=Decimal(1), worst_price=Decimal('80'), side='BUY')


def test_two_account_quantity_cap_uses_both_fees_and_mark_before_single_draw():
    market = replace(metadata(), mark_price=Decimal('100.1'),
                     minimum_initial_margin_fraction=2500)
    source = _flat_with_margin(11, '20')
    receiver = _flat_with_margin(22, '24')
    bounds = compute_quantity_bounds(market, source, receiver, Decimal('100.1'),
        receiver_bound=Decimal('100.1'), direction=Direction.LONG)
    # Independent inequalities at minimum IMF: per-unit requirements are
    # 100.1*0.25 + 100.1*0.00012 and 100.1*0.25 + 100.1*0.00035.
    source_unit = Decimal('100.1') * (Decimal('.25') + Decimal('.00012'))
    receiver_unit = Decimal('100.1') * (Decimal('.25') + Decimal('.00035'))
    expected = min(int(Decimal('20') / source_unit / Decimal('.01')),
                   int(Decimal('24') / receiver_unit / Decimal('.01')))
    assert bounds.upper_tick == expected
    assert bounds.upper_quantity * source_unit <= Decimal('20')
    assert bounds.upper_quantity * receiver_unit <= Decimal('24')
    assert ((bounds.upper_tick + 1) * bounds.size_step * source_unit > Decimal('20')
            or (bounds.upper_tick + 1) * bounds.size_step * receiver_unit > Decimal('24'))


def test_favorable_mark_side_cannot_exceed_original_owner_fourfold_notional_cap():
    market = replace(metadata(), mark_price=Decimal('100'),
                     minimum_initial_margin_fraction=2500)
    source = _flat_with_margin(11, '20')  # SELL at 110 has no adverse entry loss.
    receiver = _flat_with_margin(22, '100')
    bounds = compute_quantity_bounds(market, source, receiver, Decimal('110'),
        receiver_bound=Decimal('111'), direction=Direction.LONG)
    assert bounds.upper_tick == 72  # floor((20 * 4 / 110) / 0.01)
    assert bounds.upper_quantity * Decimal('110') <= Decimal('80')
