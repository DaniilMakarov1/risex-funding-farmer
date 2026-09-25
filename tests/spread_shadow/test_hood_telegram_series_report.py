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
            'binding': {'api_base_url': 'https://venue.invalid', 'market_id': 1, 'market_symbol': 'BTC',
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
