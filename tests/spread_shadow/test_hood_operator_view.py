"""Operator views consume saved proof; notifications cannot control trading."""
import asyncio
import copy
import json
from pathlib import Path

import pytest

from risex_spread_shadow.hood_handoff import operator_view as view
from risex_spread_shadow.hood_handoff import telegram_messages as messages
from test_hood_telegram_control import setup, update, Transport
from test_hood_telegram_messages import valid


def rows():
    opening = {'mutual_execution_proven':True, 'joint_trade_match':{'status':'MATCHED','quantity':'0.00023'}}
    for role, account, position in [('source',11,'0.00023'), ('receiver',22,'-0.00023')]:
        opening[role] = {'account_index':account,'position_before':'0','position_after':position,
                         'history_complete':True,'dispatched':True,'filled_quantity':'0.00023'}
    values = [('CYCLE_STARTED', {'binding':{'market_symbol':'BTC','source_account_index':11,'receiver_account_index':22}}),
              ('OPENING_COMPLETE', {'result':opening}),
              ('HOLD_ANCHORED', {'anchor_wall':1790098682, 'hold_seconds':56})]
    return [{'sequence':i,'event':event,'payload':payload,'run_id':'test-cycle','at':1790098680+i-1}
            for i,(event,payload) in enumerate(values,1)]


def write(path, values):
    path.write_text(''.join(json.dumps(r)+'\n' for r in values))


def test_proved_hold_displays_accounts_quantity_duration_and_moscow_deadline(tmp_path):
    path = tmp_path/'cycle.jsonl'
    write(path, rows())
    state = view.read_lifecycle(path)
    plain = valid(messages.running_message(['cycle-011'], state))
    assert 'Счёт 11: LONG 0.00023 BTC' in plain
    assert 'Счёт 22: SHORT 0.00023 BTC' in plain
    assert 'Удержание 56 с с 22.09 20:38:02 МСК' in plain
    assert 'плановое начало закрытия — 22.09 20:38:58 МСК' in plain
    assert 'Время полного закрытия зависит' in plain
    assert len(plain) < 700


@pytest.mark.parametrize('defect', ['external', 'same_side', 'quantity', 'incomplete', 'wrong_account', 'clock', 'sequence'])
def test_bad_opening_or_ordered_journal_cannot_announce_hold(tmp_path, defect):
    values = rows()
    opening = values[1]['payload']['result']
    if defect == 'external': opening['mutual_execution_proven'] = False
    elif defect == 'same_side': opening['receiver']['position_after'] = '0.00023'
    elif defect == 'quantity': opening['receiver']['filled_quantity'] = '0.00022'
    elif defect == 'incomplete': opening['source']['history_complete'] = False
    elif defect == 'wrong_account': opening['receiver']['account_index'] = 99
    elif defect == 'clock': values[2]['payload']['anchor_wall'] = 0
    else: values[2]['sequence'] = 55
    path = tmp_path/'cycle.jsonl'
    write(path, values)
    state = view.read_lifecycle(path)
    assert not state or state['stage'] != 'HOLD'
    assert 'Позиции открыты между нашими счетами.' not in '\n'.join(view.lifecycle_lines(state))


def test_trailing_write_is_tolerated_but_closing_shows_opening_positions_as_historical(tmp_path):
    path = tmp_path/'cycle.jsonl'
    values = rows()
    write(path, values)
    with path.open('a') as stream: stream.write('{"sequence":4')
    assert view.read_lifecycle(path)['stage'] == 'HOLD'
    values.append({'sequence':4,'event':'CLOSING_PLAN_READY','payload':{},'run_id':'test-cycle','at':1790098738})
    write(path, values)
    state = view.read_lifecycle(path)
    assert state['stage'] == 'CLOSING'
    assert 'не текущий снимок' in '\n'.join(view.lifecycle_lines(state))
    assert view.amount('-0.000023') == '-0.000023'
    assert view.amount('1e10000000') == '1E+10000000'


@pytest.mark.asyncio
async def test_slow_progress_delivery_cannot_delay_child_completion_or_replay(tmp_path):
    notified, child_done = asyncio.Event(), asyncio.Event()
    class Slow(Transport):
        async def send(self, owner, text):
            if 'Позиции открыты между нашими счетами' in text:
                notified.set()
                await asyncio.Event().wait()
            await super().send(owner, text)
    calls = []
    async def launch():
        calls.append(True)
        cycle = tmp_path/'cycle-001'
        cycle.mkdir()
        write(cycle/'cycle.jsonl', rows())
        await asyncio.wait_for(notified.wait(), 2)
        child_done.set()
    c = setup(tmp_path, launch, Slow())
    await c.handle(update())
    await asyncio.wait_for(c.task, 3)
    assert child_done.is_set()
    assert c.store.data['active'] is not None  # Synthetic journal is unfinished.
    await c.handle(update(2))
    assert calls == [True]


@pytest.mark.asyncio
async def test_one_notice_per_stage_and_live_status_uses_same_observations(tmp_path):
    received = asyncio.Event()
    class Notify(Transport):
        async def send(self, owner, text):
            await super().send(owner, text)
            if 'Позиции открыты между нашими счетами' in text: received.set()
    release = asyncio.Event()
    async def launch():
        cycle = tmp_path/'cycle-001'
        cycle.mkdir()
        write(cycle/'cycle.jsonl', rows())
        await release.wait()
    c = setup(tmp_path, launch, Notify())
    await c.handle(update())
    await asyncio.wait_for(received.wait(), 2)
    assert '56 с' in c.summary()
    await asyncio.sleep(0.6)
    release.set()
    await c.task
    notices = [text for _,text in c.transport.messages if 'Позиции открыты между нашими счетами' in text]
    assert len(notices) == 1


def test_idle_report_includes_new_local_cycle_without_changing_controller_barriers(tmp_path):
    from test_hood_telegram_control import restore_cycle
    c = setup(tmp_path, None)
    restore_cycle(tmp_path, '007')
    c.store.data['last'] = {'cycle':'cycle-000','status':'FINISHED'}
    before = copy.deepcopy(c.store.data)
    assert 'cycle-001' in c.summary()
    assert c.store.data == before
    c.store.data['active'] = {'before':[], 'update_id':1}
    assert 'cycle-001' in c.summary()  # New observations do not resolve an older unknown intent.
    assert 'сверк' in c.summary()
    assert c.store.data['active'] is not None


@pytest.mark.asyncio
async def test_closed_terminal_does_not_raise_from_progress_task(tmp_path, monkeypatch):
    from risex_spread_shadow.hood_handoff.cli import _stream_simple_progress
    path = tmp_path/'cycle.jsonl'
    write(path, [{'event':'PREPARATION_ATTEMPT','payload':{'attempt':1}}])
    def unavailable(*args, **kwargs): raise BrokenPipeError('closed terminal')
    monkeypatch.setattr('builtins.print', unavailable)
    await asyncio.wait_for(_stream_simple_progress(path, set(), asyncio.Event()), 1)
