"""Explicit offline headroom contract; no owner reserve is active by default."""
from dataclasses import replace
from decimal import Decimal as D
import json

import pytest

from risex_spread_shadow.hood_handoff.contracts import AccountMarginEvidence, Direction, PreflightBlocked
from risex_spread_shadow.hood_handoff.journal import DurableJournal
from risex_spread_shadow.hood_handoff.cli import _simple_event_line
from risex_spread_shadow.hood_handoff.operator_view import opening_margin_refusal, result_lines
from risex_spread_shadow.hood_handoff.random_cycle import (
    OpeningMarginReserve, RandomCycleEngine, RandomCycleSelection,
    _opening_budget_components, compute_quantity_bounds, minimal_sufficient_leverage_fraction,
)
from test_hood_handoff_random_cycle import AdvancingClock, FixedRng, account, book, cycle_config, metadata, run_random_cycle, CycleClient


def _margin_account(index, balance):
    original = account(index, D(0), available_balance=D(balance))
    evidence = AccountMarginEvidence.from_response(
        account_index=index, market_id=7, source_identity=original.source_identity,
        observed_at=original.observed_at,
        selected_position={'margin_mode': 0, 'initial_margin_fraction': '25.00'},
        account={},
    )
    return replace(original, margin_evidence=evidence)


def test_reserve_survives_smaller_quantity_and_reselected_fraction():
    reserve = D('.10')
    # Independent equations: floor((20 - fee - .10) * 10000 / N).
    for notional, fee, fraction, headroom in (
        (D('40'), D('.014'), 4971, D('.102')),
        (D('30'), D('.0105'), 6629, D('.1025')),
    ):
        actual = minimal_sufficient_leverage_fraction(D('20'), notional, 2500,
            fee_cost=fee, initial_reserve_quote=reserve)
        assert actual == fraction
        assert D('20') - fee - notional * D(actual) / 10000 == headroom
        assert headroom >= reserve


def test_reserve_quantity_bounds_apply_to_both_accounts_without_widening_owner_cap():
    market = replace(metadata(), mark_price=D('100'), minimum_initial_margin_fraction=2500)
    source, receiver = _margin_account(11, '20'), _margin_account(22, '24')
    bounds = compute_quantity_bounds(market, source, receiver, D('100'),
        receiver_bound=D('100'), direction=Direction.LONG, initial_reserve_quote=D('1'))
    source_unit = D('100') * (D('.25') + D('.00012'))
    receiver_unit = D('100') * (D('.25') + D('.00035'))
    expected = min(int((D('20') - 1) / source_unit / D('.01')),
                   int((D('24') - 1) / receiver_unit / D('.01')),
                   int(D('20') * 4 / D('100') / D('.01')))
    assert bounds.upper_tick == expected
    assert bounds.upper_quantity * source_unit <= D('19')
    assert bounds.upper_quantity * receiver_unit <= D('23')
    assert bounds.upper_quantity * D('100') <= D('80')


def test_reserve_fraction_exact_1x_4x_and_minimum_boundaries():
    assert minimal_sufficient_leverage_fraction(D('20'), D('19'), 2500,
        initial_reserve_quote=D('1')) == 10000
    assert minimal_sufficient_leverage_fraction(D('20'), D('76'), 2500,
        initial_reserve_quote=D('1')) == 2500
    with pytest.raises(PreflightBlocked, match='more than supported 4x'):
        minimal_sufficient_leverage_fraction(D('20'), D('76.01'), 2500,
            initial_reserve_quote=D('1'))
    with pytest.raises(PreflightBlocked, match='more than supported 4x'):
        minimal_sufficient_leverage_fraction(D('20'), D('64'), 3000,
            initial_reserve_quote=D('1'))
    with pytest.raises(PreflightBlocked, match='reserve consume'):
        minimal_sufficient_leverage_fraction(D('20'), D('19'), 2500,
            initial_reserve_quote=D('20'))
    with pytest.raises(Exception, match='dispatch margin reserve exceeds'):
        OpeningMarginReserve(D('0.01'), D('0.02'))


def test_short_source_adverse_mark_loss_is_budgeted_before_reserve():
    market = replace(metadata(), mark_price=D('101'))
    parts = _opening_budget_components(market, _margin_account(11, '6'),
        label='source', quantity=D('.20'), worst_price=D('100.1'), side='SELL')
    # Independent: 20.2 mark notional, .20*(101-100.1)=.18 loss,
    # .20*100.1*.00012=.0024024 fee bound.
    assert parts['mark_notional'] == D('20.2')
    assert parts['adverse_entry_loss'] == D('.18')
    assert parts['fee_cost'] == D('.0024024')
    fraction = minimal_sufficient_leverage_fraction(D('6'), D('20.2'), 2500,
        fee_cost=parts['fee_cost'], adverse_entry_loss=parts['adverse_entry_loss'],
        initial_reserve_quote=D('.05'))
    independent = int(((D('6') - D('.18') - D('.0024024') - D('.05')) * 10000 / D('20.2')))
    assert fraction == independent
    assert D('6') - D('20.2') * fraction / 10000 - D('.1824024') >= D('.05')


@pytest.mark.asyncio
async def test_fresh_budget_records_both_legs_before_first_refusal(tmp_path):
    market = replace(metadata(), mark_price=D('100'), minimum_initial_margin_fraction=2500,
                     market_margin_mode=0)
    initial_source, initial_receiver = _margin_account(11, '20'), _margin_account(22, '20')
    fresh_source, fresh_receiver = _margin_account(11, '5'), _margin_account(22, '5')

    class ReadOnlyClient:
        async def market_metadata(self, market_id):
            return market

        async def account_snapshot(self, account_index, market_id):
            return fresh_source if account_index == 11 else fresh_receiver

        async def order_book(self, market_id):
            return book()

        async def submit_order(self, *args, **kwargs):
            raise AssertionError('preflight must not send a source order')

    config = cycle_config(tmp_path / 'cycle', margin_reserve=OpeningMarginReserve(D('.1'), D('.01')))
    bounds = compute_quantity_bounds(market, initial_source, initial_receiver, D('100.1'),
        receiver_bound=D('100.1'), direction=Direction.LONG, initial_reserve_quote=D('.1'))
    selection = RandomCycleSelection(quantity=D('.20'), quantity_tick=20, hold_seconds=20,
        opening_source_price=D('100.1'), opening_receiver_bound=D('100.1'),
        bounds=bounds, metadata_observed_at=1000, book_observed_at=1000)
    engine = RandomCycleEngine(ReadOnlyClient(), clock=AdvancingClock())
    engine._leverage_fractions = {11: 2500, 22: 2500}
    journal = DurableJournal(config.journal_path, clock=lambda: 1000)
    with pytest.raises(PreflightBlocked, match=r'source selected quantity.*shortfall 0.01240240 quote'):
        await engine._revalidate_open(config, selection, market, book(),
                                      initial_source, initial_receiver, journal=journal)
    rows = [json.loads(line) for line in config.journal_path.read_text().splitlines()]
    legs = next(row['payload']['legs'] for row in rows if row['event'] == 'FRESH_OPENING_MARGIN_BUDGET')
    assert len(legs) == 2
    assert [leg['status'] for leg in legs] == ['INSUFFICIENT', 'INSUFFICIENT']
    # Source: 5 margin + .20*100.1*.00012 fee; receiver additionally loses
    # .20*(100.1-100) against mark and pays the taker bound.
    assert D(legs[0]['total_required']) == D('5.0024024')
    assert D(legs[0]['headroom']) == D('-0.0024024')
    assert D(legs[1]['total_required']) == D('5.0270070')
    assert D(legs[1]['headroom']) == D('-0.0270070')
    assert legs[0]['fee_rate_provenance'] == ['published_cap']
    assert legs[1]['fee_rate_provenance'] == ['published_cap']


@pytest.mark.asyncio
async def test_reserve_below_venue_minimum_stops_before_any_draw_or_order(tmp_path):
    clock = AdvancingClock()
    client = CycleClient(clock)
    rng = FixedRng(20, 20)
    result = await run_random_cycle(
        cycle_config(tmp_path / 'blocked', margin_reserve=OpeningMarginReserve(D('1000'), D('1000'))),
        client, clock=clock, rng=rng)
    assert result.outcome.value == 'FAILED_PREFLIGHT_BLOCKED'
    assert not client.submissions
    assert rng.values == [20, 20]


@pytest.mark.asyncio
async def test_fresh_higher_fee_blocks_receiver_with_source_budget_retained(tmp_path):
    initial = replace(metadata(), mark_price=D('100'), minimum_initial_margin_fraction=2500,
                      market_margin_mode=0)
    fresh = replace(initial, receiver_fee_rate=D('.001'))
    source, receiver = _margin_account(11, '20'), _margin_account(22, '5.04')

    class ReadOnlyClient:
        async def market_metadata(self, market_id):
            return fresh

        async def account_snapshot(self, account_index, market_id):
            return source if account_index == 11 else receiver

        async def order_book(self, market_id):
            return book()

    config = cycle_config(tmp_path / 'fee-rise', margin_reserve=OpeningMarginReserve(D('.05'), D('.01')))
    bounds = compute_quantity_bounds(initial, _margin_account(11, '20'), _margin_account(22, '20'),
        D('100.1'), receiver_bound=D('100.1'), direction=Direction.LONG,
        initial_reserve_quote=D('.05'))
    selection = RandomCycleSelection(quantity=D('.20'), quantity_tick=20, hold_seconds=20,
        opening_source_price=D('100.1'), opening_receiver_bound=D('100.1'),
        bounds=bounds, metadata_observed_at=1000, book_observed_at=1000)
    engine = RandomCycleEngine(ReadOnlyClient(), clock=AdvancingClock())
    engine._leverage_fractions = {11: 2500, 22: 2500}
    journal = DurableJournal(config.journal_path, clock=lambda: 1000)
    with pytest.raises(PreflightBlocked, match=r'receiver selected quantity.*shortfall 0.010020 quote'):
        await engine._revalidate_open(config, selection, initial, book(),
                                      _margin_account(11, '20'), _margin_account(22, '20'), journal=journal)
    rows = [json.loads(line) for line in config.journal_path.read_text().splitlines()]
    legs = next(row['payload']['legs'] for row in rows if row['event'] == 'FRESH_OPENING_MARGIN_BUDGET')
    assert [leg['status'] for leg in legs] == ['ADMITTED', 'INSUFFICIENT']
    assert legs[1]['fee_rate_provenance'] == ['market_observed']
    assert D(legs[1]['fee_bound']) == D('.02002')
    assert D(legs[1]['headroom']) == D('-.00002')


@pytest.mark.asyncio
async def test_fresh_budget_success_omits_unproved_baseline_delta_and_preserves_quantity(tmp_path):
    market = replace(metadata(), mark_price=D('100'), minimum_initial_margin_fraction=2500,
                     market_margin_mode=0)
    source, receiver = _margin_account(11, '5.1'), _margin_account(22, '5.1')

    class ReadOnlyClient:
        async def market_metadata(self, market_id):
            return market

        async def account_snapshot(self, account_index, market_id):
            return source if account_index == 11 else receiver

        async def order_book(self, market_id):
            return book()

    config = cycle_config(tmp_path / 'admitted', margin_reserve=OpeningMarginReserve(D('.05'), D('.01')))
    original = _margin_account(11, '20')
    original_receiver = _margin_account(22, '20')
    bounds = compute_quantity_bounds(market, original, original_receiver, D('100.1'),
        receiver_bound=D('100.1'), direction=Direction.LONG, initial_reserve_quote=D('.05'))
    selection = RandomCycleSelection(quantity=D('.20'), quantity_tick=20, hold_seconds=20,
        opening_source_price=D('100.1'), opening_receiver_bound=D('100.1'),
        bounds=bounds, metadata_observed_at=1000, book_observed_at=1000)
    engine = RandomCycleEngine(ReadOnlyClient(), clock=AdvancingClock())
    engine._leverage_fractions = {11: 2500, 22: 2500}
    initial_book = book()
    engine._opening_plan_evidence = {
        'source': {'account_index': 11, 'account_source_identity': original.source_identity,
                   'account_observed_at': original.observed_at, 'available_balance': D('20'),
                   'book_market_id': initial_book.market_id, 'book_symbol': initial_book.symbol,
                   'book_observed_at': initial_book.observed_at, 'worst_price': D('100.1')},
        'receiver': {'account_index': 22, 'account_source_identity': original_receiver.source_identity,
                     'account_observed_at': original_receiver.observed_at, 'available_balance': D('20'),
                     'book_market_id': initial_book.market_id, 'book_symbol': initial_book.symbol,
                     'book_observed_at': initial_book.observed_at, 'worst_price': D('100.1')},
    }
    journal = DurableJournal(config.journal_path, clock=lambda: 1000)
    refreshed = await engine._revalidate_open(config, selection, market, initial_book,
                                               original, original_receiver, journal=journal)
    assert refreshed[-1].quantity == D('.20')
    assert refreshed[-1].quantity_tick == 20
    rows = [json.loads(line) for line in config.journal_path.read_text().splitlines()]
    legs = next(row['payload']['legs'] for row in rows if row['event'] == 'FRESH_OPENING_MARGIN_BUDGET')
    assert [leg['status'] for leg in legs] == ['ADMITTED', 'ADMITTED']
    assert [leg['initial_plan_provenance'] for leg in legs] == ['INCOMPLETE_OR_CONFLICTING'] * 2
    assert all(leg['initial_account_observed_at'] == 1000.0 for leg in legs)
    assert all(leg['initial_metadata_observed_at'] is None for leg in legs)
    assert all(leg['initial_book_observed_at'] == initial_book.observed_at for leg in legs)
    assert [leg['initial_to_fresh'] for leg in legs] == [
        {'available_balance': '-14.9'}, {'available_balance': '-14.9'}]
    assert all(D(leg['headroom']) >= D('.01') for leg in legs)


def test_margin_refusal_is_concise_in_terminal_and_saved_view():
    reason = ('opening preparation blocked: source selected quantity exceeds fresh '
              'free-balance margin model (account 11; shortfall 0.01240240 quote)')
    rendered = opening_margin_refusal(reason)
    assert rendered == ('Открытие остановлено до ордера: на первом счёте 11 '
                        'не хватает 0.0124024 в валюте баланса для расчётной маржи и запаса.')
    report = {'cycle': {'preflight_reason': reason}, 'binding': {}, 'inventory': {},
              'paired_execution': {}, 'economics': {}}
    assert rendered in result_lines(report)
    assert _simple_event_line({'event': 'PREPARATION_BLOCKED', 'payload': {'reason': reason}}) == rendered
    assert opening_margin_refusal('wrong account 11; shortfall 1 quote') is None
