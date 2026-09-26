"""Human cycle cards: brief, truthful, escaped and outside the trading path."""
import asyncio
import copy
import json
from decimal import Decimal

import pytest

from risex_spread_shadow.hood_handoff import telegram_control as bot
from risex_spread_shadow.hood_handoff.telegram_cards import cycle_card, seconds, usd
from test_hood_auto_close_before_run import proof
from test_hood_telegram_control import setup, update
from test_hood_telegram_messages import valid
from test_hood_telegram_series import cycle_slot


def match(phase, status='MATCHED', own='0.001', **changes):
    row = {'phase': phase, 'status': status, 'matched_quantity': own,
           'external_source_quantity': '0', 'external_receiver_quantity': '0',
           'unproved_source_quantity': '0', 'unproved_receiver_quantity': '0',
           'external_source_accounts': [], 'external_receiver_accounts': [],
           'source_state': 'FILLED', 'receiver_state': 'FILLED'}
    row.update(changes)
    return row


def fill(account, trade, quantity, price, side, role, phase='opening', peer=22):
    return {'account_index': account, 'market_id': 1, 'trade_id': trade, 'quantity': quantity,
            'price': price, 'side': side, 'phase': phase, 'history_complete': True,
            'fee': None, 'fee_evidence': 'MISSING_OR_INVALID_COMPONENTS', 'venue_fee_raw': None,
            'integrator_fee_raw': None, 'fee_role': role, 'counterparty_account_index': peer}


def report(**changes):
    value = {
        'status': 'COMPLETE',
        'binding': {'api_base_url': 'https://api.rh.lighter.xyz', 'environment': 'robinhood', 'chain_id': 466324,
                    'market_id': 1, 'market_symbol': 'BTC', 'direction': 'LONG', 'receiver_admission': 'ack',
                    'price_improvement_ticks': 1, 'source_account_index': 11, 'receiver_account_index': 22},
        'inventory': {'status': 'CONFIRMED_FLAT', 'source': '0', 'receiver': '0'},
        'order_state': {'unresolved_intents': [], 'unresolved_observed_orders': []},
        'paired_execution': {'status': 'SUCCESS', 'direct_counterparty_match': [match('opening'), match('closing')]},
        'economics': {'fees': {'status': 'UNKNOWN', 'total': None},
                      'closed_execution_pnl': {'status': 'GROSS_ONLY', 'unit': 'quote_currency',
                                               'gross': '-0.02', 'net': None}},
        'planned_actions': [{'phase': 'opening', 'leg': 'source', 'attempt': 1, 'plan': {'quantity': '0.001'}}],
        'confirmed_fills': [fill(11, 'o', '0.001', '50000', 'SELL', 'maker'),
                            fill(22, 'o', '0.001', '50000', 'BUY', 'taker', peer=11),
                            fill(11, 'c', '0.001', '50010', 'BUY', 'maker', 'closing'),
                            fill(22, 'c', '0.001', '50010', 'SELL', 'taker', 'closing', peer=11)],
        'dispatched_actions': [],
        'latency': {'cycle': {'terminal_at': 1000.0 + 64.4, 'phase_intervals': [
            {'phase': 'selection', 'start_at': 1000.0, 'seconds': 2.5},
            {'phase': 'preparation', 'start_at': 1002.5, 'seconds': 3.5},
            {'phase': 'hold', 'start_at': 1010.0, 'seconds': 50.2},
            {'phase': 'closing', 'start_at': 1060.2, 'seconds': 3.4}]},
            'opening': [{}], 'closing': [{}, {}]},
        'cycle': {'outcome': 'SUCCESS'},
    }
    value.update(changes)
    return value


def card(**changes):
    return {'kind': 'cycle', 'name': 'cycle-007', 'index': 3, 'total': 20, 'safe': True, 'pause': 17, **changes}


def test_paired_card_shows_volume_route_time_pnl_zero_tariff_and_pause():
    result = valid(cycle_card(card(), report()))
    lines = result.splitlines()
    assert lines[0] == '✅ Цикл 3/20 · cycle-007'
    assert lines[1].endswith('· 1 мин 4 с')
    # Turnover counts both account receipts of both phases: 4 × 0.001 BTC.
    assert '0.001 BTC · оборот 200.02 $ · ACK · +1 тик' in result
    assert 'LIMIT 11 SELL → MARKET 22 BUY' in result
    assert 'Открытие: 🤝 свои счета' in result and 'Закрытие: 🤝 свои счета · попыток 2' in result
    assert '⏱ подготовка 6.0 с · удержание 50 с · закрытие 3.4 с' in result
    # Every fill has an empty fee: the owner-confirmed 0% tariff gives net = gross.
    assert '💰 PnL −0.0200 $ · комиссии 0 (тариф биржи 0%)' in result
    assert 'неизвестн' not in result
    assert result.endswith('⏸ Следующий цикл через 17 с')


@pytest.mark.parametrize('bad', ['nonzero_raw', 'integrator_raw', 'legacy_evidence', 'fee_value', 'no_fills'])
def test_zero_tariff_needs_every_fill_to_have_an_empty_fee(bad):
    r = report()
    target = r['confirmed_fills'][2]
    if bad == 'nonzero_raw':
        target.update(fee_evidence='NONZERO_UNIT_UNVERIFIED', venue_fee_raw=5)
    elif bad == 'integrator_raw':
        target['integrator_fee_raw'] = 1
    elif bad == 'legacy_evidence':
        target.pop('fee_evidence')
    elif bad == 'fee_value':
        target['fee'] = '0.001'
    else:
        r['confirmed_fills'] = []
    result = valid(cycle_card(card(), r))
    assert '💰 PnL −0.0200 $ до комиссий' in result and 'Комиссии: неизвестны' in result
    assert 'тариф' not in result


def test_proved_fees_show_net_and_actual_fee_without_estimate():
    r = report()
    r['economics'] = {'fees': {'status': 'PROVEN', 'total': '0.01'},
                      'closed_execution_pnl': {'status': 'PROVEN', 'unit': 'quote_currency',
                                               'gross': '0.05', 'net': '0.04'}}
    result = valid(cycle_card(card(pause=None), r))
    assert '💰 PnL +0.0400 $ после комиссий' in result
    assert 'Комиссии: 0.0100 $' in result and 'тариф' not in result and '⏸' not in result


def test_external_mixed_unknown_and_residual_phases_are_named_with_accounts():
    r = report()
    r['paired_execution'] = {'status': 'PARTIAL', 'direct_counterparty_match': [
        match('opening', own='0.0006', external_receiver_quantity='0.0004', external_receiver_accounts=[3026]),
        match('closing', 'NOT_MATCHED', own='0', external_source_quantity='0.001',
              external_source_accounts=[26085], receiver_state='NO_FILL')]}
    r['confirmed_fills'] = r['confirmed_fills'][:2] + [fill(11, 'r', '0.001', '50020', 'BUY', 'taker', 'fallback', peer=39)]
    result = valid(cycle_card(card(), r))
    assert result.startswith('🟡 Цикл 3/20 · cycle-007 · позиции закрыты, пара неполная')
    assert 'Открытие: 🔀 свои счета 0.0006; MARKET исполнился о чужие 0.0004 (счёт 3026)' in result
    assert 'Закрытие: 👥 LIMIT взяли чужие 0.001 (счёт 26085)' in result
    assert '🛠 Остаток закрыт reduce-only: внешние участники' in result
    unknown = report()
    unknown['paired_execution']['direct_counterparty_match'][1].update(unproved_source_quantity='0.001', matched_quantity='0')
    assert 'Закрытие: ❔ контрагент не доказан 0.001' in valid(cycle_card(card(), unknown))


def test_no_fill_and_unattempted_phases_have_no_timing_claims():
    r = report()
    r['paired_execution'] = {'status': 'FAILED', 'direct_counterparty_match': [
        match('opening', 'NO_FILL', own='0'), match('closing', 'NOT_ATTEMPTED', own='0')]}
    r['confirmed_fills'] = []
    r['cycle'] = {'outcome': 'FAILED', 'reason': 'WS_ADMISSION_STOP: L2 shows better-priced volume'}
    result = valid(cycle_card(card(), r))
    assert 'Открытие: ∅ ордера не исполнились' in result and 'Закрытие: — не выполнялось' in result
    assert 'оборот 0 $' in result
    assert 'Причина: Встречный MARKET остановлен: в стакане появилась более выгодная цена.' in result


@pytest.mark.parametrize('bad', ['unsafe', 'nonflat', 'incomplete', 'unresolved'])
def test_uncertain_cycle_never_looks_successful_or_continues(bad):
    r = report()
    safe = bad != 'unsafe'
    if bad == 'nonflat':
        r['inventory'] = {'status': 'OPEN_INVENTORY', 'source': '-0.001', 'receiver': '0'}
    elif bad == 'incomplete':
        r['status'] = 'INCOMPLETE'
    elif bad == 'unresolved':
        r['order_state']['unresolved_intents'] = ['x']
    result = valid(cycle_card(card(safe=safe), r))
    assert result.startswith('⛔ Цикл 3/20 · cycle-007 · итог требует сверки')
    assert '💰 PnL неизвестен' in result and '⏸' not in result and '/close — закрыть остаток' in result
    # Settled receipts remain a proven turnover fact (as in the series total);
    # unsettled evidence yields neither turnover nor a fee estimate.
    settled = bad in ('unsafe', 'nonflat')
    assert ('оборот 200.02 $' in result) is settled
    # An uncertain cycle never gets a tariff-based fee or net PnL.
    assert 'Комиссии: неизвестны' in result and 'тариф' not in result


def test_missing_report_and_foreign_denomination_do_not_invent_values():
    missing = valid(cycle_card(card(), None))
    assert missing.startswith('⛔ Цикл 3/20 · cycle-007') and 'PnL' not in missing
    foreign = report()
    foreign['binding']['chain_id'] = 304
    result = valid(cycle_card(card(), foreign))
    assert result.startswith('✅')
    assert 'оборот' not in result and '💰 PnL неизвестен' in result and 'Комиссии: неизвестны' in result


def test_dynamic_values_are_escaped():
    r = report()
    r['paired_execution']['direct_counterparty_match'][0].update(
        matched_quantity='0', external_source_quantity='0.001', external_source_accounts=['<b>x</b>&'])
    message = cycle_card(card(name='cycle-<i>9'), r)
    valid(message)
    assert '&lt;b&gt;x&lt;/b&gt;&amp;' in message and 'cycle-&lt;i&gt;9' in message


def test_human_number_and_duration_formats():
    assert usd(Decimal('1234567.891')) == '1 234 567.89'
    assert usd('-0.00123') == '−0.0012' and usd('0.5', signed=True) == '+0.5000'
    assert usd('0') == '0' and usd('NaN') == 'неизвестно' and usd(None) == 'неизвестно'
    assert seconds(3.24) == '3.2 с' and seconds(64.4) == '1 мин 4 с' and seconds(3600) == '1 ч 0 мин'
    assert seconds(-1) is None and seconds(float('nan')) is None and seconds(True) is None


def test_real_saved_cycle_card_reads_phase_timing_from_journals(tmp_path):
    from risex_spread_shadow.hood_handoff.offline_report import load_saved_cycle_report
    cycle_slot(tmp_path, 1)
    slot = tmp_path / 'cycle-001'
    result = valid(cycle_card(card(name='cycle-001', total=1, pause=None), load_saved_cycle_report(slot), slot))
    assert result.splitlines()[0].startswith(('✅ Цикл · cycle-001', '🟡 Цикл · cycle-001'))
    assert 'Открытие:' in result and 'Закрытие:' in result


async def test_series_without_new_slot_never_reuses_previous_cycle_card(tmp_path):
    async def recover(*, require_flat):
        return proof('READY' if require_flat else 'CLOSE_READY')
    launched = []
    async def launch(**options):
        launched.append(True)
        if len(launched) == 1:
            cycle_slot(tmp_path, 1)
    c = setup(tmp_path, launch)
    c.recovery = recover
    async def close(): pytest.fail('unexpected close')
    c.close = close
    await c.handle(update(text='/run 2'))
    await c.task
    await c._notice_task
    messages = [m for _, m in c.transport.messages]
    assert sum('<code>cycle-001</code>' in m and 'Цикл 1/2' in m for m in messages) == 1
    assert not any('Цикл 2/2</b> · <code>' in m for m in messages)
    assert any('Серия' in m for m in messages)


async def test_single_run_sends_one_card_instead_of_long_summary(tmp_path):
    async def launch(**options):
        cycle_slot(tmp_path, 1)
    c = setup(tmp_path, launch)
    await c.handle(update(text='/run'))
    await c.task
    await c._notice_task
    messages = [m for _, m in c.transport.messages]
    assert len(messages) == 2 and 'Принято: один цикл' in messages[0]
    assert '<b>Цикл</b> · <code>cycle-001</code>' in messages[1]


def journal_row(sequence, event, payload=None, at=None):
    return json.dumps({'sequence': sequence, 'at': at or 1000.0 + sequence, 'run_id': 'r1',
                       'event': event, 'payload': payload or {}}) + '\n'


async def test_rare_alerts_only_including_resize_and_429(tmp_path, monkeypatch):
    slot = tmp_path / 'cycle-001'
    slot.mkdir()
    (slot / 'cycle.jsonl').write_text(
        journal_row(1, 'CYCLE_STARTED', {'binding': {}}) + journal_row(2, 'INITIAL_SPREAD_WAIT')
        + journal_row(3, 'OPENING_QUANTITY_RECALCULATED', {'attempt': 1, 'old_quantity': '0.00087',
                                                             'new_quantity': '0.00070'})
        + journal_row(4, 'HOLD_ANCHORED', {'hold_seconds': 20}))
    monkeypatch.setattr(bot, 'read_execution_notices', lambda path: [
        ('opening-1-accepted', 'LIMIT'), ('opening-1-limited', '429'), ('opening-1-execution', 'done')])
    c = setup(tmp_path, None)
    c.store.data['active'] = {'action': 'run', 'before': [], 'series_index': 1, 'series_total': 2}
    task = asyncio.create_task(c.lifecycle_notices())
    await asyncio.sleep(0.8)
    c.store.data['active'] = None
    await asyncio.wait_for(task, 2)
    await c._notice_task
    messages = [m for _, m in c.transport.messages]
    assert len(messages) == 3
    assert any(m.startswith('↘️ <b>Цикл 1/2</b> · объём уменьшен по свежей марже: 0.00087 → 0.0007 BTC') for m in messages)
    assert any(m.startswith('⏳ <b>Цикл 1/2</b> · жду спред') for m in messages)
    assert any('HTTP 429' in m and m.startswith('⚠️') for m in messages)
    assert not any('LIMIT' in m or 'done' in m for m in messages)
