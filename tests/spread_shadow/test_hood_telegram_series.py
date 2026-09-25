"""A fresh owner command may request a finite, non-replaying cycle series."""
import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from risex_spread_shadow.hood_handoff import telegram_control as bot
from test_hood_auto_close_before_run import close_slot, proof
from test_hood_telegram_control import setup, update


def cycle_slot(root, number, fixture='004'):
    slot = root / f'cycle-{number:03d}'
    slot.mkdir()
    source = Path(__file__).parents[1] / f'fixtures/hood_handoff/cycle-{fixture}.json'
    for name, rows in json.loads(source.read_text())['journals'].items():
        (slot / name).write_text(''.join(json.dumps(row) + '\n' for row in rows))


async def test_five_cycles_use_one_command_and_recheck_before_each(tmp_path):
    calls = []
    async def recover(*, require_flat):
        calls.append(('read', require_flat))
        return proof('READY' if require_flat else 'CLOSE_READY')
    async def close():
        pytest.fail('flat series should not close positions')
    async def launch(**options):
        saved = json.loads(c.store.path.read_text())['active']
        number = saved['series_index']
        assert saved['series_total'] == 5
        assert len(saved['before']) == number - 1
        calls.append(('run', number, options))
        cycle_slot(tmp_path, number)
    c = setup(tmp_path, launch)
    c.recovery, c.close = recover, close
    await c.handle(update(text='/run ack 1 5'))
    assert 'серия из 5' in c.transport.messages[0][1]
    await c.task
    assert [call[1] for call in calls if call[0] == 'run'] == [1, 2, 3, 4, 5]
    assert [call for call in calls if call[0] == 'read'] == [('read', False), ('read', True)] * 5
    assert all(call[2] == {'receiver_admission': 'ack', 'price_improvement_ticks': 1}
               for call in calls if call[0] == 'run')
    assert c.store.data['active'] is None
    assert c.store.data['last']['series_completed'] == 5
    assert 'Серия завершена: 5/5' in c.summary()
    await c.handle(update(text='/run ack 1 5'))
    assert len([call for call in calls if call[0] == 'run']) == 5


async def test_default_mode_count_and_legacy_tick_form_are_distinct(tmp_path):
    calls = []
    async def recover(*, require_flat):
        return proof('READY' if require_flat else 'CLOSE_READY')
    async def launch(**options):
        calls.append(options)
        cycle_slot(tmp_path, len(calls))
    c = setup(tmp_path, launch)
    c.recovery = recover
    async def close(): pytest.fail('flat series should not close positions')
    c.close = close
    await c.handle(update(text='/run 2'))
    await c.task
    assert calls == [{}, {}]
    assert c.store.data['last']['series_completed'] == 2


@pytest.mark.parametrize('command', [
    '/run 0', '/run 6', '/run 05', '/run -1', '/run 1.5',
    '/run ack 1 0', '/run ack 1 6', '/run ack 1 05', '/run ack 0 5',
    '/run ack 1 5 extra', '/close 2',
])
async def test_invalid_series_command_never_launches(tmp_path, command):
    calls = []
    async def launch(**options): calls.append(options)
    c = setup(tmp_path, launch)
    await c.handle(update(text=command))
    assert c.task is None and calls == []
    assert c.store.data['active'] is None


@pytest.mark.parametrize('failure', ['missing', 'incomplete'])
async def test_unsafe_cycle_stops_series_without_second_launch(tmp_path, failure):
    calls = []
    async def recover(*, require_flat):
        calls.append(('read', require_flat))
        return proof('READY' if require_flat else 'CLOSE_READY')
    async def launch(**options):
        calls.append(('run',))
        if failure == 'missing':
            (tmp_path / 'cycle-001').mkdir()
        else:
            cycle_slot(tmp_path, 1, fixture='003')
    c = setup(tmp_path, launch)
    c.recovery = recover
    async def close(): pytest.fail('no proved inventory to close')
    c.close = close
    await c.handle(update(text='/run ws 1 3'))
    await c.task
    assert calls.count(('run',)) == 1
    assert calls.count(('read', False)) == 1
    assert c.store.data['active'] is not None
    assert c.store.data['last']['series_completed'] == 0
    assert 'Серия остановлена: 0/3' in c.summary()


async def test_second_read_failure_preserves_one_completed_cycle_and_no_second_child(tmp_path):
    calls = []
    reads = 0
    async def recover(*, require_flat):
        nonlocal reads
        reads += 1
        calls.append(('read', require_flat))
        if reads == 3:
            raise RuntimeError('current account evidence unavailable')
        return proof('READY' if require_flat else 'CLOSE_READY')
    async def launch(**options):
        calls.append(('run',))
        cycle_slot(tmp_path, 1)
    c = setup(tmp_path, launch)
    c.recovery = recover
    async def close(): pytest.fail('no proved inventory to close')
    c.close = close
    await c.handle(update(text='/run ws 1 3'))
    await c.task
    assert calls == [('read', False), ('read', True), ('run',), ('read', False)]
    assert c.store.data['active']['series_index'] == 2
    assert c.store.data['last']['series_completed'] == 1
    assert 'Серия остановлена: 1/3' in c.summary()
    assert json.loads(c.store.path.read_text())['active']['series_index'] == 2


async def test_next_step_closes_fresh_proved_inventory_before_launch(tmp_path):
    calls = []
    reads = 0
    async def recover(*, require_flat):
        nonlocal reads
        reads += 1
        calls.append(('read', require_flat))
        return proof('READY' if require_flat else 'CLOSE_READY',
                     source='0.00025' if reads == 3 else '0')
    async def close():
        saved = json.loads(c.store.path.read_text())['active']
        assert saved['series_index'] == 2 and saved['phase'] == 'AUTO_CLOSE'
        calls.append(('close',))
        close_slot(tmp_path)
    async def launch(**options):
        number = c.store.data['active']['series_index']
        calls.append(('run', number))
        cycle_slot(tmp_path, number)
    c = setup(tmp_path, launch)
    c.recovery, c.close = recover, close
    await c.handle(update(text='/run ack 1 2'))
    await c.task
    assert calls == [('read', False), ('read', True), ('run', 1),
                     ('read', False), ('close',), ('read', True), ('run', 2)]
    assert c.store.data['last']['series_completed'] == 2


async def test_unconfirmed_next_step_close_stops_without_opening(tmp_path):
    launches = []
    reads = 0
    async def recover(*, require_flat):
        nonlocal reads
        reads += 1
        return proof('READY' if require_flat else 'CLOSE_READY',
                     source='0.00025' if reads == 3 else '0')
    async def close():
        close_slot(tmp_path, status='UNKNOWN')
    async def launch(**options):
        number = c.store.data['active']['series_index']
        launches.append(number)
        cycle_slot(tmp_path, number)
    c = setup(tmp_path, launch)
    c.recovery, c.close = recover, close
    await c.handle(update(text='/run ack 1 2'))
    await c.task
    assert launches == [1]
    assert c.store.data['active']['series_index'] == 2
    assert c.store.data['last']['status'] == 'BLOCKED'
    assert c.store.data['last']['series_completed'] == 1


@pytest.mark.parametrize('stage', ['before_second', 'final_checkpoint'])
async def test_persistence_failure_keeps_barrier_and_stops_series(tmp_path, monkeypatch, stage):
    launches = []
    async def recover(*, require_flat):
        return proof('READY' if require_flat else 'CLOSE_READY')
    async def close(): pytest.fail('flat series should not close positions')
    async def launch(**options):
        number = c.store.data['active']['series_index']
        launches.append(number)
        cycle_slot(tmp_path, number)
    c = setup(tmp_path, launch)
    c.recovery, c.close = recover, close
    original = c.store.save
    failed = False
    def fail_once():
        nonlocal failed
        active = c.store.data['active']
        last = c.store.data['last']
        match = (stage == 'before_second' and active is not None
                 and active.get('series_index') == 2 and active.get('before') == ['cycle-001']
                 or stage == 'final_checkpoint' and active is None
                 and isinstance(last, dict) and last.get('series_completed') == 2)
        if match and not failed:
            failed = True
            raise OSError('synthetic checkpoint failure')
        original()
    monkeypatch.setattr(c.store, 'save', fail_once)
    await c.handle(update(text='/run 2'))
    await c.task
    assert failed
    assert launches == ([1] if stage == 'before_second' else [1, 2])
    assert c.store.data['active'] is not None
    assert c.store.data['last']['status'] == 'BLOCKED'
    assert json.loads(c.store.path.read_text())['active'] is not None


async def test_duplicate_and_parallel_command_during_series_do_not_overlap(tmp_path):
    entered, release = asyncio.Event(), asyncio.Event()
    launches = []
    async def recover(*, require_flat):
        return proof('READY' if require_flat else 'CLOSE_READY')
    async def launch(**options):
        number = c.store.data['active']['series_index']
        launches.append(number)
        if number == 1:
            entered.set()
            await release.wait()
        cycle_slot(tmp_path, number)
    c = setup(tmp_path, launch)
    c.recovery = recover
    async def close(): pytest.fail('flat series should not close positions')
    c.close = close
    await c.handle(update(text='/run 2'))
    await entered.wait()
    assert 'Серия: цикл 1/2' in c.summary()
    await c.handle(update(text='/run 2'))
    await c.handle(update(2, '/run 2'))
    assert launches == [1]
    release.set()
    await c.task
    assert launches == [1, 2]


@pytest.mark.parametrize('next_claimed', [False, True])
def test_restart_stops_series_without_replaying_next_cycle(tmp_path, next_claimed):
    c = setup(tmp_path, None)
    cycle_slot(tmp_path, 1)
    c.store.data['active'] = {'before': ['cycle-001'] if next_claimed else [],
                              'update_id': 1, 'action': 'run',
                              'series_total': 5, 'series_index': 2 if next_claimed else 1}
    if next_claimed:
        c.store.data['last'] = {'status': 'FINISHED', 'cycle': 'cycle-001',
                                'series_total': 5, 'series_completed': 1}
    c.store.save()
    restarted = setup(tmp_path, None)
    restarted.finish()
    assert restarted.store.data['active'] is None
    assert restarted.store.data['last']['series_completed'] == 1
    assert restarted.store.data['last']['cycle'] == 'cycle-001'
    assert 'Серия остановлена: 1/5' in restarted.summary()


@pytest.mark.parametrize('bad', [
    {'series_total': True, 'series_index': 1},
    {'series_total': 6, 'series_index': 1},
    {'series_total': 5, 'series_index': 0},
    {'series_total': 5},
])
def test_malformed_saved_series_state_fails_closed(tmp_path, bad):
    c = setup(tmp_path, None)
    c.store.data['active'] = {'before': [], 'update_id': 1, 'action': 'run', **bad}
    c.store.save()
    with pytest.raises(RuntimeError, match='active series state'):
        setup(tmp_path, None)


async def test_server_series_spawns_five_fixed_children_without_reusing_command(tmp_path, monkeypatch):
    tmp_path.chmod(0o700)
    config = tmp_path / 'random-cycle.json'
    config.write_text(json.dumps({'market_id': 1, 'market_symbol': 'BTC',
                                  'source_account_index': 11, 'receiver_account_index': 22,
                                  'cycle_dir': str(tmp_path / 'cycle'), 'api_key_index': 4,
                                  'margin_reserve': {'initial_quote': '0.10', 'dispatch_quote': '0.02'}}))
    (tmp_path / 'market-contract.json').write_text('{}')
    store = bot.Store(tmp_path / '.telegram-control', 'binding')
    calls = []
    class Stop(BaseException): pass
    class API:
        def __init__(self, *args): self.reads = 0
        async def send(self, *args): pass
        async def call(self, method, **payload):
            assert method == 'getUpdates'
            self.reads += 1
            if self.reads == 1:
                return [update(10, '/run ack 1 5')]
            if self.reads == 2:
                return [update(11, '/run ack 1 5')]
            for _ in range(100):
                if store.data['active'] is None and len(calls) == 5:
                    raise Stop()
                await asyncio.sleep(0.01)
            pytest.fail('series did not terminate')
    class Session:
        def __init__(self, **kwargs): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
    class Process:
        async def communicate(self, data):
            assert data == b'\n'
            cycle_slot(tmp_path, len(calls))
    async def create(*args, **kwargs):
        calls.append((args, kwargs))
        return Process()
    from risex_spread_shadow.hood_handoff import cli, operator_recovery
    async def recover(*args, require_flat=True):
        return proof('READY' if require_flat else 'CLOSE_READY')
    monkeypatch.setattr(cli, '_validate_simple_local_inputs', lambda *a, **k: (object(), {}))
    monkeypatch.setattr(operator_recovery, 'check_recovery', recover)
    monkeypatch.setattr(bot, 'Telegram', API)
    monkeypatch.setattr(bot.aiohttp, 'ClientSession', Session)
    monkeypatch.setattr(bot.asyncio, 'create_subprocess_exec', create)
    real = bot.Controller
    monkeypatch.setattr(bot, 'Controller', lambda *a, **k: real(*a, **k, now=lambda: 1000))
    original_is_file = Path.is_file
    monkeypatch.setattr(Path, 'is_file',
                        lambda p: True if str(p).endswith('.venv-hood/bin/python') else original_is_file(p))
    with pytest.raises(Stop):
        await bot.serve(SimpleNamespace(config=config, owner_id=42), store, 99, 'unused')
    assert len(calls) == 5
    for argv, kwargs in calls:
        assert argv[1:5] == ('-m', 'risex_spread_shadow.hood_handoff.cli', 'simple', '--keychain')
        assert argv[-4:] == ('--receiver-admission', 'ack', '--price-improvement-ticks', '1')
        assert kwargs['pass_fds'] == (99,)
    assert store.data['offset'] == 12
    assert store.data['active'] is None
    assert store.data['last']['series_completed'] == 5


async def test_slow_cycle_notices_do_not_gate_next_cycle_or_final_state(tmp_path):
    entered, release = asyncio.Event(), asyncio.Event()
    class Slow:
        async def send(self, owner, text):
            if 'Цикл' in text or 'Серия' in text:
                entered.set()
                await release.wait()
    calls = []
    async def recover(*, require_flat):
        return proof('READY' if require_flat else 'CLOSE_READY')
    async def launch(**options):
        calls.append(len(calls) + 1)
        cycle_slot(tmp_path, calls[-1])
        if len(calls) == 1:
            await asyncio.wait_for(entered.wait(), 1)
    c = setup(tmp_path, launch, Slow())
    c.recovery = recover
    async def close(): pytest.fail('unexpected close')
    c.close = close
    try:
        await c.handle(update(text='/run 2'))
        await asyncio.wait_for(c.task, 1)
        assert calls == [1, 2]
        assert c.store.data['active'] is None
        assert c.store.data['last']['series_completed'] == 2
    finally:
        release.set()
        if c._notice_task:
            await c._notice_task


async def test_series_notices_identify_each_transition_and_end(tmp_path):
    async def recover(*, require_flat):
        return proof('READY' if require_flat else 'CLOSE_READY')
    async def launch(**options):
        cycle_slot(tmp_path, c.store.data['active']['series_index'])
    c = setup(tmp_path, launch)
    c.recovery = recover
    async def close(): pytest.fail('unexpected close')
    c.close = close
    await c.handle(update(text='/run 2'))
    await c.task
    await c._notice_task
    text = '\n'.join(m for _, m in c.transport.messages)
    assert 'Цикл 1/2' in text and 'Цикл 2/2' in text
    assert text.index('Далее проверка следующего цикла') < text.index('перед следующим открытием')
    assert 'Это последний цикл; серия закончена' in text


async def test_notice_queue_is_bounded_and_drops_old_progress(tmp_path):
    release = asyncio.Event()
    class Slow:
        async def send(self, *args): await release.wait()
    c = setup(tmp_path, None, Slow())
    for n in range(1000): c.queue_notice(str(n))
    assert c._notice_queue.qsize() == 64
    assert c._notice_queue.get_nowait() == '936'
    release.set()
    await c._notice_task


async def test_notice_timeout_continues_queue_without_replay_or_secret(tmp_path, monkeypatch, capsys):
    timeout = asyncio.timeout
    monkeypatch.setattr(bot.asyncio, 'timeout', lambda seconds: timeout(0.01))
    messages = []
    class Delayed:
        async def send(self, owner, text):
            messages.append(text)
            if text == 'first': await asyncio.Event().wait()
    c = setup(tmp_path, None, Delayed())
    c.queue_notice('first')
    c.queue_notice('second')
    await c._notice_task
    assert messages == ['first', 'second']
    assert 'timed out' in capsys.readouterr().err
    assert c.store.data['active'] is None
