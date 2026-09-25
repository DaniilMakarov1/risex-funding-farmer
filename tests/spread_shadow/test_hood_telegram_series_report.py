"""Series membership, truthful execution labels and independent aggregate PnL."""
import asyncio
import copy
import json
from decimal import Decimal

import pytest

from risex_spread_shadow.hood_handoff.telegram_series_report import report_pages, add_exact
from test_hood_telegram_control import setup, update
from test_hood_telegram_series import cycle_slot
from test_hood_auto_close_before_run import proof
from test_hood_telegram_messages import valid


def report(gross='0.03', net='0.02'):
    return {'status': 'COMPLETE',
            'binding': {'api_base_url': 'https://api.rh.lighter.xyz', 'environment': 'robinhood', 'chain_id': 466324, 'market_id': 1, 'market_symbol': 'BTC',
                        'source_account_index': 11, 'receiver_account_index': 22},
            'inventory': {'status': 'CONFIRMED_FLAT'},
            'order_state': {'unresolved_intents': [], 'unresolved_observed_orders': []},
            'paired_execution': {'direct_counterparty_match': [
                {'phase': phase, 'status': 'MATCHED', 'matched_quantity': '0.001',
                 'external_source_quantity': '0', 'external_receiver_quantity': '0',
                 'unproved_source_quantity': '0', 'unproved_receiver_quantity': '0',
                 'source_state': 'FILLED', 'receiver_state': 'FILLED'}
                for phase in ('opening', 'closing')]},
            'economics': {'closed_execution_pnl': {
                'status': 'GROSS_ONLY' if net is None else 'PROVEN', 'unit': 'quote_currency',
                'gross': gross, 'net': net}}, 'confirmed_fills': []}


def state(names, **changes):
    return {'status': 'FINISHED', 'series_total': len(names), 'series_completed': len(names),
            'series_slots': names, **changes}


def render(records, **changes):
    return '\n'.join(valid(page) for page in report_pages(state(list(records), **changes), records.get))


def test_exact_negative_positive_and_zero_pnl_no_account_double_count():
    result = render({'cycle-001': report('0.03', '0.02'),
                     'cycle-002': report('-0.07', '-0.09'), 'cycle-003': report('0', '0')})
    assert 'До комиссий: -0.04' in result
    assert 'После комиссий: -0.07' in result
    assert result.count('открытие — парное; закрытие — парное') == 3
    assert 'Фандинг не включён' in result
    assert add_exact(Decimal('123456789012345678901234567890'), Decimal('0.00000001')) == Decimal('123456789012345678901234567890.00000001')


def test_missing_fee_preserves_gross_but_never_fabricates_net():
    result = render({'cycle-001': report('0.03', '0.02'), 'cycle-002': report('-0.07', None)})
    assert 'До комиссий: -0.04' in result
    assert 'После комиссий: неизвестен' in result
    assert 'Известная часть после комиссий (1/2): 0.02' in result


@pytest.mark.parametrize('bad', ['missing', 'incomplete', 'nonflat', 'unresolved', 'foreign_market', 'nan'])
def test_bad_cycle_never_becomes_zero_or_valid_total(bad):
    second = report('-0.07', '-0.09')
    if bad == 'missing': second = None
    elif bad == 'incomplete': second['status'] = 'INCOMPLETE'
    elif bad == 'nonflat': second['inventory']['status'] = 'OPEN_INVENTORY'
    elif bad == 'unresolved': second['order_state']['unresolved_intents'] = ['unknown']
    elif bad == 'foreign_market': second['binding']['market_id'] = 2
    elif bad == 'nan': second['economics']['closed_execution_pnl']['gross'] = 'NaN'
    result = render({'cycle-001': report(), 'cycle-002': second})
    assert 'До комиссий: неизвестен' in result and 'После комиссий: неизвестен' in result
    assert 'Известная часть до комиссий (1/2): 0.03' in result


def test_external_mixed_unknown_unattempted_and_residual_labels():
    external, mixed, empty = report(), report(), report()
    a = external['paired_execution']['direct_counterparty_match'][0]
    a.update(status='NOT_MATCHED', matched_quantity='0', external_receiver_quantity='0.001')
    mixed['paired_execution']['direct_counterparty_match'][1].update(status='NOT_MATCHED', external_source_quantity='0.0002', unproved_receiver_quantity='0.0001')
    empty['paired_execution']['direct_counterparty_match'][0]['status'] = 'NO_FILL'
    empty['paired_execution']['direct_counterparty_match'][1]['status'] = 'NOT_ATTEMPTED'
    external['confirmed_fills'] = [{'phase': 'fallback', 'counterparty_account_index': 39}]
    result = render({'cycle-001': external, 'cycle-002': mixed, 'cycle-003': empty})
    assert 'открытие — внешние участники' in result
    assert 'закрытие — смешанное: свои + внешние; есть неизвестное' in result
    assert 'открытие — без исполнений; закрытие — не выполнялось' in result
    assert 'остаток: внешние участники' in result


def test_large_report_paginates_all_cycles_and_has_one_total():
    names = [f'cycle-{n:03d}' for n in range(1, 701)]
    pages = list(report_pages(state(names), lambda _: report()))
    assert len(pages) > 1
    assert all(len(page.encode('utf-16-le')) // 2 <= 3500 for page in pages)
    full = '\n'.join(valid(p) for p in pages)
    assert all(full.count(name + ':') == 1 for name in names)
    assert full.count('Общий PnL') == 1
    assert 'До комиссий: 21' in full and 'После комиссий: 14' in full


def test_restart_legacy_membership_and_duplicate_protection():
    legacy = list(report_pages({'series_total': 6, 'series_completed': 6, 'status': 'FINISHED'}, lambda _: pytest.fail('must not invent membership')))
    assert 'не сохранён' in legacy[0]
    duplicate = '\n'.join(report_pages(state(['cycle-001', 'cycle-001']), lambda _: report()))
    assert 'После комиссий: неизвестен' in duplicate


@pytest.mark.parametrize('command', ['/run 6', '/run ack 1 12', '/run ws 5 10'])
async def test_more_than_five_cycles_persist_membership_and_recover_report(tmp_path, command):
    async def recovery(*, require_flat):
        return proof('READY' if require_flat else 'CLOSE_READY')
    async def launch(**options):
        number = c.store.data['active']['series_index']
        saved = json.loads(c.store.path.read_text())['active']
        assert saved['series_slots'] == [f'cycle-{n:03d}' for n in range(1, number)]
        cycle_slot(tmp_path, number)
    c = setup(tmp_path, launch)
    c.recovery = recovery
    async def close(): pytest.fail('flat accounts')
    c.close = close
    await c.handle(update(text=command))
    await c.task
    await c._notice_task
    count = int(command.split()[-1])
    assert c.store.data['last']['series_completed'] == count
    assert len(c.store.data['last']['series_slots']) == count
    assert c.store.data['active'] is None
    reloaded = setup(tmp_path, launch)
    await reloaded.handle(update(2, '/report'))
    await reloaded._notice_task
    output = '\n'.join(text for _, text in reloaded.transport.messages)
    assert 'Общий PnL' in output
    assert all(name in output for name in c.store.data['last']['series_slots'])


def test_slot_sorting_after_999(tmp_path):
    for n in (998, 999, 1000): (tmp_path / f'cycle-{n}').mkdir()
    assert setup(tmp_path, None).slots() == ['cycle-998', 'cycle-999', 'cycle-1000']


@pytest.mark.parametrize('names', [['cycle-001', 'cycle-001'], ['../other'], 'cycle-001', [False]])
def test_invalid_membership_is_refused_on_restart(tmp_path, names):
    c = setup(tmp_path, None)
    c.store.data['last'] = state(names, series_total=20, series_completed=0, cycle=None)
    c.store.save()
    with pytest.raises(RuntimeError, match='membership'):
        setup(tmp_path, None)


async def test_report_pages_do_not_fill_lossy_queue(tmp_path):
    c = setup(tmp_path, None)
    c.report = lambda _: report()
    names = [f'cycle-{n:03d}' for n in range(1, 5001)]
    c.store.data['last'] = state(names)
    assert c.queue_series_report()
    assert c._notice_queue.qsize() == 1
    await c._notice_task
    combined = '\n'.join(m for _, m in c.transport.messages)
    assert 'cycle-5000:' in combined
    assert 'После комиссий: 100' in combined


async def test_incomplete_step_stops_large_series_and_keeps_both_results(tmp_path):
    async def recovery(*, require_flat):
        return proof('READY' if require_flat else 'CLOSE_READY')
    async def launch(**options):
        index = c.store.data['active']['series_index']
        cycle_slot(tmp_path, index, fixture='004' if index == 1 else '003')
    c = setup(tmp_path, launch)
    c.recovery = recovery
    async def close(): pytest.fail('no close expected')
    c.close = close
    await c.handle(update(text='/run 20'))
    await c.task
    await c._notice_task
    assert c.store.data['last']['series_slots'] == ['cycle-001', 'cycle-002']
    assert c.store.data['last']['series_completed'] == 1
    assert c.store.data['active'] is not None
    output = '\n'.join(m for _, m in c.transport.messages)
    assert 'Серия остановлена: 1/20' in output
    assert 'После комиссий: неизвестен' in output
    reloaded = setup(tmp_path, launch)
    reloaded.finish()
    assert reloaded.store.data['last']['series_slots'] == ['cycle-001', 'cycle-002']


async def test_next_read_failure_keeps_completed_series_membership(tmp_path):
    reads = 0
    async def recovery(*, require_flat):
        nonlocal reads
        reads += 1
        if reads == 3: raise RuntimeError('read unavailable')
        return proof('READY' if require_flat else 'CLOSE_READY')
    async def launch(**options): cycle_slot(tmp_path, 1)
    c = setup(tmp_path, launch)
    c.recovery = recovery
    async def close(): pytest.fail('not reached')
    c.close = close
    await c.handle(update(text='/run 6'))
    await c.task
    assert c.store.data['last']['series_slots'] == ['cycle-001']
    assert c.store.data['last']['series_completed'] == 1


async def test_final_report_snapshot_survives_new_command_state(tmp_path):
    c = setup(tmp_path, None)
    c.report = lambda _: report()
    c.store.data['last'] = state(['cycle-001', 'cycle-002'])
    c.queue_series_report()
    c.store.data['last']['series_slots'].clear()
    c.store.data['last'] = None
    await c._notice_task
    assert 'cycle-002:' in '\n'.join(m for _, m in c.transport.messages)


def test_precycle_close_exclusion_is_explicit_and_does_not_invent_entry_pnl():
    result = render({'cycle-001': report()}, series_precloses=['close-001'])
    assert 'Предварительные закрытия старых позиций исключены' in result
    assert 'После комиссий: 0.02' in result


@pytest.mark.parametrize('command', ['/run 0006', '/run 6.5', '/run -20', '/run ack 1 +10', '/run ' + '9' * 4096])
async def test_bad_or_oversized_count_never_launches(tmp_path, command):
    async def launch(**options): pytest.fail('invalid count')
    c = setup(tmp_path, launch)
    await c.handle(update(text=command))
    assert c.task is None


def fill(account, trade, quantity, price, side='BUY', phase='opening'):
    return {'account_index': account, 'market_id': 1, 'trade_id': trade,
            'quantity': quantity, 'price': price, 'side': side,
            'phase': phase, 'history_complete': True, 'counterparty_account_index': 39}


def test_usd_pnl_and_total_two_account_open_close_external_and_residual_volume():
    first, second = report(), report('-0.07', None)
    first['confirmed_fills'] = [
        fill(11, 'own-open', '0.001', '50000'),
        fill(22, 'own-open', '0.001', '50000', 'SELL'),
        fill(11, 'own-close', '0.001', '50100', 'SELL', 'closing'),
        fill(22, 'own-close', '0.001', '50100', 'BUY', 'closing')]
    second['confirmed_fills'] = [fill(11, 'external-open', '0.0002', '50000'),
                                 fill(11, 'residual-close', '0.0002', '50200', 'SELL', 'fallback')]
    result = render({'cycle-001': first, 'cycle-002': second})
    # 50 + 50 + 50.1 + 50.1 + 10 + 10.04, account receipts counted once.
    assert 'Исполненный объём обоих счетов: 220.24 USD' in result
    assert 'До комиссий: -0.04 USD' in result
    assert 'После комиссий: неизвестен' in result
    assert 'PnL: 0.02 USD' in result
    assert 'USD по номиналу USDG' in result


def test_proved_empty_fills_have_zero_turnover():
    result = render({'cycle-001': report('0', '0')})
    assert 'Исполненный объём обоих счетов: 0 USD' in result


@pytest.mark.parametrize('bad', ['missing', 'incomplete', 'unknown_order', 'bad_quantity', 'bad_price',
                                'wrong_account', 'wrong_market', 'missing_history', 'duplicate', 'conflict'])
def test_unproved_volume_never_becomes_complete_total(bad):
    r = report()
    r['confirmed_fills'] = [fill(11, '1', '0.001', '50000')]
    if bad == 'missing': r.pop('confirmed_fills')
    elif bad == 'incomplete': r['status'] = 'INCOMPLETE'
    elif bad == 'unknown_order': r['order_state']['unresolved_intents'] = ['unknown']
    elif bad == 'bad_quantity': r['confirmed_fills'][0]['quantity'] = '-1'
    elif bad == 'bad_price': r['confirmed_fills'][0]['price'] = 'NaN'
    elif bad == 'wrong_account': r['confirmed_fills'][0]['account_index'] = 99
    elif bad == 'wrong_market': r['confirmed_fills'][0]['market_id'] = 2
    elif bad == 'missing_history': r['confirmed_fills'][0].pop('history_complete')
    else:
        r['confirmed_fills'].append(copy.deepcopy(r['confirmed_fills'][0]))
        if bad == 'conflict': r['confirmed_fills'][1]['price'] = '50001'
    assert 'Исполненный объём обоих счетов: неизвестен' in render({'cycle-001': r})


def test_duplicate_account_receipt_across_cycles_is_not_added_twice():
    first, second = report(), report()
    first['confirmed_fills'] = second['confirmed_fills'] = [fill(11, 'same', '0.001', '50000')]
    result = render({'cycle-001': first, 'cycle-002': second})
    assert 'Исполненный объём обоих счетов: неизвестен' in result
    assert 'Подтверждённая часть объёма (1/2): 50 USD' in result


def test_incomplete_series_reports_known_turnover_subtotal():
    r = report()
    r['confirmed_fills'] = [fill(11, 'known', '0.001', '50000')]
    result = render({'cycle-001': r, 'cycle-002': None}, status='BLOCKED', series_completed=1)
    assert 'Исполненный объём обоих счетов: неизвестен' in result
    assert 'Подтверждённая часть объёма (1/2): 50 USD' in result


@pytest.mark.parametrize('field,value', [('api_base_url','https://other.invalid'), ('environment','mainnet'),
                                       ('chain_id',304), ('market_symbol','ETH')])
def test_foreign_denomination_is_not_relabelled_usd(field, value):
    r = report()
    r['binding'][field] = value
    r['confirmed_fills'] = [fill(11, '1', '0.001', '50000')]
    result = render({'cycle-001': r})
    assert 'До комиссий: неизвестен' in result
    assert 'После комиссий: неизвестен' in result
    assert 'Исполненный объём обоих счетов: неизвестен' in result


def test_turnover_product_preserves_low_order_digits():
    from risex_spread_shadow.hood_handoff.telegram_series_report import executed_turnover
    r = report()
    r['confirmed_fills'] = [fill(11, 'precise', '12345678901234567890.123456789', '0.000000001')]
    assert executed_turnover(r, set()) == Decimal('12345678901.234567890123456789')


def test_single_cycle_view_also_identifies_usd_nominal():
    from risex_spread_shadow.hood_handoff.operator_view import result_lines
    result = '\n'.join(result_lines(report()))
    assert 'PnL указан в USD по номиналу USDG' in result


@pytest.mark.parametrize('supported', [True, False])
def test_real_saved_report_preserves_only_supported_public_usd_binding(tmp_path, supported):
    from risex_spread_shadow.hood_handoff.offline_report import load_saved_cycle_report
    from risex_spread_shadow.hood_handoff.operator_view import nominal_usd
    cycle_slot(tmp_path, 1)
    path = tmp_path / 'cycle-001' / 'cycle.jsonl'
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    rows[0]['payload']['binding'].update(api_base_url='https://api.rh.lighter.xyz' if supported else 'https://private.invalid/SECRET_VALUE',
                                         environment='robinhood', chain_id=466324, market_id=1, market_symbol='BTC')
    path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
    saved = load_saved_cycle_report(path.parent)
    assert nominal_usd(saved['binding']) is supported
    assert 'SECRET_VALUE' not in json.dumps(saved)
