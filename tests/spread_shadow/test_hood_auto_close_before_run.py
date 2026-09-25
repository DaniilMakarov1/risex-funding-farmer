"""One fresh /run may close proved inventory once before it opens a cycle."""
import asyncio
import json

import pytest

from test_hood_telegram_control import setup, update, restore_cycle


def proof(status, source='0', receiver='0'):
    def account(index, position):
        return {'account_index': index, 'market_id': 1, 'signed_position': position,
                'active_orders': [], 'authorized': True, 'ready': True}
    return {'status': status, 'at': 1000, 'previous_intents': 1,
            'source': account(11, source), 'receiver': account(22, receiver)}


def close_slot(root, name='close-001', status='CONFIRMED_FLAT', *, terminal=True):
    slot = root / name
    slot.mkdir()
    rows = [{'sequence': 1, 'run_id': name, 'event': 'CLOSE_STARTED',
             'at': 1000, 'payload': {'binding': {'market_id': 1}}}]
    if terminal:
        rows.append({'sequence': 2, 'run_id': name, 'event': 'CLOSE_COMPLETE',
                     'at': 1001, 'payload': {'status': status, 'at': 1001, 'symbol': 'BTC',
                                           'positions': [{'account_index': 11, 'position': '0'},
                                                         {'account_index': 22, 'position': '0'}],
                                           'attempts': []}})
    (slot / 'close.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in rows))


async def test_flat_run_keeps_existing_path_without_close(tmp_path):
    calls = []
    async def recover(*, require_flat):
        calls.append(('read', require_flat))
        return proof('READY' if require_flat else 'CLOSE_READY')
    async def close(): calls.append(('close',))
    async def launch(**options):
        calls.append(('run', options))
        restore_cycle(tmp_path, '004')
    c = setup(tmp_path, launch)
    c.recovery, c.close = recover, close
    await c.handle(update(text='/run ws 5'))
    await c.task
    assert calls == [('read', False), ('read', True),
                     ('run', {'receiver_admission': 'ws_confirmed',
                              'price_improvement_ticks': 5})]
    assert c.store.data['active'] is None


async def test_nonflat_run_durably_closes_rechecks_then_preserves_cycle_options(tmp_path):
    calls = []
    async def recover(*, require_flat):
        calls.append(('read', require_flat))
        return proof('READY' if require_flat else 'CLOSE_READY',
                     source='0' if require_flat else '0.00025')
    async def close():
        saved = json.loads(c.store.path.read_text())
        assert saved['active']['phase'] == 'AUTO_CLOSE'
        assert saved['active']['auto_close_before'] == []
        assert saved['offset'] == 2
        calls.append(('close',))
        close_slot(tmp_path)
    async def launch(**options):
        saved = json.loads(c.store.path.read_text())
        assert saved['active']['phase'] == 'RUN'
        assert saved['active']['auto_close_slot'] == 'close-001'
        calls.append(('run', options))
        restore_cycle(tmp_path, '004')
    c = setup(tmp_path, launch)
    c.recovery, c.close = recover, close
    await c.handle(update(text='/run ack 5'))
    await c.task
    assert calls == [('read', False), ('close',), ('read', True),
                     ('run', {'receiver_admission': 'ack', 'price_improvement_ticks': 5})]
    assert c.store.data['active'] is None
    await c.handle(update(text='/run ack 5'))
    assert calls.count(('close',)) == 1


@pytest.mark.parametrize('failure', ['no_slot', 'unknown', 'partial', 'incomplete', 'multiple',
                                      'close_error', 'postcheck_error', 'postcheck_nonflat'])
async def test_failed_or_uncertain_close_never_opens_cycle(tmp_path, failure):
    calls = []
    async def recover(*, require_flat):
        calls.append(('read', require_flat))
        if failure == 'postcheck_error' and require_flat:
            raise RuntimeError('exact current account read unavailable')
        return proof('READY' if require_flat else 'CLOSE_READY',
                     source='0.00025' if not require_flat or failure == 'postcheck_nonflat' else '0')
    async def close():
        calls.append(('close',))
        if failure == 'close_error':
            raise RuntimeError('child outcome unavailable')
        if failure != 'no_slot':
            close_slot(tmp_path, status='UNKNOWN' if failure == 'unknown' else
                       'PARTIAL' if failure == 'partial' else 'CONFIRMED_FLAT',
                       terminal=failure != 'incomplete')
        if failure == 'multiple':
            close_slot(tmp_path, 'close-002')
    async def launch(**options): calls.append(('run', options))
    c = setup(tmp_path, launch)
    c.recovery, c.close = recover, close
    command = update()
    await c.handle(command)
    await c.task
    await c.handle(command)
    assert calls.count(('close',)) == 1
    assert not any(call[0] == 'run' for call in calls)
    assert c.store.data['active']['phase'] == 'AUTO_CLOSE'
    assert c.store.data['last']['status'] == 'BLOCKED'


@pytest.mark.parametrize('bad', ['missing', 'float', 'nan', 'active_order', 'same_account',
                                 'other_market', 'unauthorized', 'not_ready', 'wrong_status', 'unknown'])
async def test_unproved_preclose_account_state_never_dispatches(tmp_path, bad):
    calls = []
    async def recover(*, require_flat):
        calls.append(('read', require_flat))
        if bad == 'unknown':
            raise RuntimeError('prior order unknown')
        value = proof('UNKNOWN' if bad == 'wrong_status' else 'CLOSE_READY', source='0.00025')
        if bad == 'missing': del value['source']['signed_position']
        if bad == 'float': value['source']['signed_position'] = 0.00025
        if bad == 'nan': value['source']['signed_position'] = 'NaN'
        if bad == 'active_order': value['source']['active_orders'] = [{'order_id': 1}]
        if bad == 'same_account': value['receiver']['account_index'] = 11
        if bad == 'other_market': value['receiver']['market_id'] = 2
        if bad == 'unauthorized': value['source']['authorized'] = False
        if bad == 'not_ready': value['receiver']['ready'] = False
        return value
    async def close(): calls.append(('close',))
    async def launch(**options): calls.append(('run',))
    c = setup(tmp_path, launch)
    c.recovery, c.close = recover, close
    await c.handle(update())
    await c.task
    assert calls == [('read', False)]
    assert c.store.data['active'] is None


async def test_position_disappears_before_close_uses_fresh_flat_check(tmp_path):
    calls = []
    async def recover(*, require_flat):
        calls.append(require_flat)
        return proof('READY' if require_flat else 'CLOSE_READY',
                     source='0.00025' if not require_flat else '0')
    async def close():
        calls.append('close')
        close_slot(tmp_path)
    async def launch(**options): calls.append('run')
    c = setup(tmp_path, launch)
    c.recovery, c.close = recover, close
    await c.handle(update())
    await c.task
    assert calls == [False, 'close', True, 'run']


async def test_duplicate_command_during_close_does_not_start_second_close(tmp_path):
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []
    async def recover(*, require_flat):
        return proof('READY' if require_flat else 'CLOSE_READY',
                     source='0.00025' if not require_flat else '0')
    async def close():
        calls.append('close')
        entered.set()
        await release.wait()
        close_slot(tmp_path)
    async def launch(**options): calls.append('run')
    c = setup(tmp_path, launch)
    c.recovery, c.close = recover, close
    await c.handle(update())
    await entered.wait()
    await c.handle(update())
    await c.handle(update(2))
    assert calls == ['close']
    release.set()
    await c.task
    assert calls == ['close', 'run']


@pytest.mark.parametrize('terminal', [False, True])
@pytest.mark.parametrize('phase', ['AUTO_CLOSE', 'RUN'])
def test_restart_after_precycle_close_never_replays_or_opens(tmp_path, terminal, phase):
    c = setup(tmp_path, None)
    c.store.data['active'] = {'before': [], 'update_id': 1, 'action': 'run',
                              'phase': phase, 'auto_close_before': []}
    if phase == 'RUN':
        c.store.data['active']['auto_close_slot'] = 'close-001'
    c.store.save()
    close_slot(tmp_path, terminal=terminal)
    restarted = setup(tmp_path, None)
    restarted.finish()
    assert restarted.store.data['last']['cycle'] == 'close-001'
    assert restarted.store.data['last']['status'] == ('NOT_LAUNCHED' if terminal else 'BLOCKED')
    assert (restarted.store.data['active'] is None) is terminal
    if terminal:
        assert 'Новый цикл не начался' in restarted.summary()


@pytest.mark.parametrize('phase', ['AUTO_CLOSE', 'RUN'])
def test_restart_with_missing_precycle_close_slot_retains_barrier(tmp_path, phase):
    c = setup(tmp_path, None)
    c.store.data['active'] = {'before': [], 'update_id': 1, 'action': 'run',
                              'phase': phase, 'auto_close_before': []}
    if phase == 'RUN':
        c.store.data['active']['auto_close_slot'] = 'close-001'
    c.store.save()
    restarted = setup(tmp_path, None)
    restarted.finish()
    assert restarted.store.data['last'] == {'status': 'BLOCKED', 'cycle': None}
    assert restarted.store.data['active'] is not None


def test_restart_with_different_close_slot_than_saved_does_not_trust_it(tmp_path):
    c = setup(tmp_path, None)
    c.store.data['active'] = {'before': [], 'update_id': 1, 'action': 'run',
                              'phase': 'RUN', 'auto_close_before': [],
                              'auto_close_slot': 'close-001'}
    c.store.save()
    close_slot(tmp_path, 'close-002')
    restarted = setup(tmp_path, None)
    restarted.finish()
    assert restarted.store.data['last'] == {'status': 'BLOCKED', 'cycle': 'close-002'}
    assert restarted.store.data['active'] is not None


def test_malformed_persisted_precycle_close_state_fails_closed(tmp_path):
    c = setup(tmp_path, None)
    c.store.data['active'] = {'before': [], 'update_id': 1, 'action': 'run',
                              'phase': 'AUTO_CLOSE', 'auto_close_before': ['cycle-001']}
    c.store.save()
    with pytest.raises(RuntimeError, match='pre-cycle close state'):
        setup(tmp_path, None)


async def test_precycle_intent_save_failure_prevents_close_and_cycle(tmp_path, monkeypatch):
    calls = []
    async def recover(*, require_flat):
        return proof('CLOSE_READY', source='0.00025')
    async def close(): calls.append('close')
    async def launch(**options): calls.append('run')
    c = setup(tmp_path, launch)
    c.recovery, c.close = recover, close
    original = c.store.save
    def save():
        if (c.store.data.get('active') or {}).get('phase') == 'AUTO_CLOSE':
            raise OSError('synthetic durable state failure')
        original()
    monkeypatch.setattr(c.store, 'save', save)
    await c.handle(update())
    await c.task
    assert calls == []
    assert not list(tmp_path.glob('close-*'))
