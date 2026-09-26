import asyncio
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from risex_spread_shadow.hood_handoff import telegram_control as bot
from risex_spread_shadow.hood_handoff.operator_control import exclusive_lock


class Transport:
    def __init__(self, fail=False):
        self.messages = []
        self.markups = []
        self.fail = fail

    async def send(self, owner, text, markup=None):
        if self.fail:
            raise RuntimeError('synthetic delivery failure')
        self.messages.append((owner, text))
        self.markups.append(markup)


# '/run 1' is one cycle with the saved mode and ticks (a bare /run asks for a count).
def update(number=1, text='/run 1'):
    return {'update_id': number, 'message': {'date': 1000, 'text': text,
            'from': {'id': 42, 'is_bot': False}, 'chat': {'id': 42, 'type': 'private'}}}


def setup(tmp_path, launch, transport=None):
    tmp_path.chmod(0o700)
    config = tmp_path / 'random-cycle.json'
    store = bot.Store(tmp_path / '.telegram-control', 'binding')
    async def no_series_wait(seconds):
        assert 5 <= seconds <= 30
    return bot.Controller(42, config, store, transport or Transport(), launch, now=lambda: 1000, series_sleep=no_series_wait)


@pytest.mark.parametrize('change', ['foreign_sender', 'foreign_chat', 'group', 'bot', 'forward',
                                    'edited', 'stale', 'future', 'args', 'bool_id', 'missing_date'])
async def test_untrusted_commands_never_launch(tmp_path, change):
    calls = []
    async def launch(): calls.append(True)
    c = setup(tmp_path, launch)
    u = update()
    if change == 'foreign_sender': u['message']['from']['id'] = 43
    if change == 'foreign_chat': u['message']['chat']['id'] = 43
    if change == 'group': u['message']['chat']['type'] = 'group'
    if change == 'bot': u['message']['from']['is_bot'] = True
    if change == 'forward': u['message']['forward_origin'] = {}
    if change == 'edited': u['edited_message'] = u.pop('message')
    if change == 'stale': u['message']['date'] = 800
    if change == 'future': u['message']['date'] = 1006
    if change == 'args': u['message']['text'] = '/run ; arbitrary command'
    if change == 'bool_id': u['message']['from']['id'] = True
    if change == 'missing_date': del u['message']['date']
    await c.handle(u)
    await asyncio.sleep(0)
    assert not calls
    assert c.store.data['active'] is None


@pytest.mark.parametrize('skew', [0, 1, 2, 5])
async def test_small_telegram_clock_skew_answers_once_without_launch(tmp_path, skew):
    calls = []
    async def launch(): calls.append(True)
    c = setup(tmp_path, launch)
    message = update(text='/start')
    message['message']['date'] = 1000 + skew
    await c.handle(message)
    assert len(c.transport.messages) == 1
    assert 'время вне допустимого' not in c.transport.messages[0][1]
    await c.handle(message)
    assert len(c.transport.messages) == 1
    assert not calls


@pytest.mark.parametrize('date,started', [(1006, 1000), (879, 800), (999, 1000), (True, 1000)])
async def test_invalid_owner_timestamp_is_explained_without_launch(tmp_path, date, started):
    calls = []
    async def launch(): calls.append(True)
    c = setup(tmp_path, launch)
    c.started = started
    message = update()
    message['message']['date'] = date
    await c.handle(message)
    assert 'время вне допустимого' in c.transport.messages[0][1]
    assert not calls
    assert c.task is None
    assert c.store.data['offset'] == 2
    await c.handle(message)
    assert len(c.transport.messages) == 1


async def test_failed_notification_is_visible_without_exception_secrets(tmp_path, capsys):
    class SecretFailure:
        async def send(self, *args):
            raise RuntimeError('https://example.invalid/botSECRET')
    c = setup(tmp_path, None, SecretFailure())
    await c.handle(update(text='/start'))
    error = capsys.readouterr().err
    assert 'notification delivery failed' in error
    assert 'SECRET' not in error


async def test_clock_skew_close_keeps_recovery_check_and_deduplication(tmp_path):
    calls = []
    c = setup(tmp_path, None)
    async def recovery(*, require_flat):
        calls.append(('check', require_flat))
        return {'status': 'CLOSE_READY'}
    async def close(): calls.append(('close',))
    c.recovery, c.close = recovery, close
    message = update(text='/close')
    message['message']['date'] = 1002
    await c.handle(message)
    await c.task
    await c.handle(message)
    assert calls == [('check', False), ('close',)]


async def test_duplicate_and_parallel_run_have_one_durable_launch(tmp_path):
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []
    async def launch():
        disk = json.loads((tmp_path / '.telegram-control/state.json').read_text())
        assert disk['active']['update_id'] == 1
        assert disk['offset'] == 2
        calls.append(True)
        entered.set()
        await release.wait()
    c = setup(tmp_path, launch)
    await c.handle(update())
    await entered.wait()
    await c.handle(update())
    await c.handle(update(2))
    assert calls == [True]
    await c.handle(update(3, '/run'))
    assert 'другая операция' in c.transport.messages[-1][1]
    assert c._count_prompt is None
    release.set()
    await c.task
    assert c.store.data['active'] is None
    reloaded = setup(tmp_path, launch)
    await reloaded.handle(update())
    assert calls == [True]


async def test_ambiguous_launch_exception_blocks_future_commands(tmp_path):
    calls = []
    async def launch():
        calls.append(True)
        raise RuntimeError('ambiguous process result')
    c = setup(tmp_path, launch)
    await c.handle(update())
    await c.task
    await c.handle(update(2))
    assert len(calls) == 1
    assert c.store.data['active'] is not None
    assert '/run проверит текущую готовность' in c.summary()


async def test_notification_failure_never_repeats_launch(tmp_path):
    calls = []
    async def launch(): calls.append(True)
    c = setup(tmp_path, launch, Transport(fail=True))
    await c.handle(update())
    await c.task
    await c.handle(update())
    assert calls == [True]


def restore_cycle(root, number):
    p = root / 'cycle-001'
    p.mkdir(mode=0o700)
    fixture = json.loads((Path(__file__).parents[1] / f'fixtures/hood_handoff/cycle-{number}.json').read_text())
    for name, rows in fixture['journals'].items():
        (p / name).write_text(''.join(json.dumps(r) + '\n' for r in rows))


@pytest.mark.parametrize('number,safe', [('003', False), ('004', True), ('007', True)])
def test_restart_uses_terminal_evidence_not_process_exit(tmp_path, number, safe):
    c = setup(tmp_path, None)
    c.store.data['active'] = {'before': [], 'update_id': 1}
    c.store.save()
    restore_cycle(tmp_path, number)
    restarted = setup(tmp_path, None)
    restarted.finish()
    assert (restarted.store.data['active'] is None) == safe
    assert ('Историческая позиция' in restarted.summary())
    assert 'SUCCESS' not in restarted.summary()


def test_no_slot_after_crash_is_not_reported_as_a_successful_cycle(tmp_path):
    c = setup(tmp_path, None)
    c.store.data['active'] = {'before': [], 'update_id': 1}
    c.store.save()
    c.finish()
    assert c.store.data['last']['status'] == 'NOT_LAUNCHED'


def test_state_binding_and_permissions_fail_closed(tmp_path):
    c = setup(tmp_path, None)
    c.store.save()
    assert c.store.path.stat().st_mode & 0o777 == 0o600
    with pytest.raises(RuntimeError, match='mismatch'):
        bot.Store(c.store.directory, 'different-config-or-owner')
    c.store.path.chmod(0o644)
    with pytest.raises(RuntimeError, match='unsafe'):
        bot.Store(c.store.directory, 'binding')


def test_single_instance_lock_survives_parent_close_in_child(tmp_path):
    tmp_path.chmod(0o700)
    path = tmp_path / 'instance.lock'
    with exclusive_lock(path) as fd:
        child = subprocess.Popen([sys.executable, '-c', 'import sys; print("ready", flush=True); sys.stdin.read()'],
                                 pass_fds=(fd,), stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        assert child.stdout.readline().strip() == 'ready'
    try:
        with pytest.raises(RuntimeError, match='active'):
            with exclusive_lock(path): pass
    finally:
        child.communicate('')
    with exclusive_lock(path): pass


def test_symlink_lock_is_rejected(tmp_path):
    tmp_path.chmod(0o700)
    (tmp_path / 'target').write_text('')
    (tmp_path / 'lock').symlink_to(tmp_path / 'target')
    with pytest.raises(OSError):
        with exclusive_lock(tmp_path / 'lock'): pass


async def test_transport_exception_never_exposes_token():
    token = '123456:' + 'synthetic-canary' * 3
    class Session:
        def post(self, *args, **kwargs):
            raise RuntimeError('request failed ' + token)
    api = bot.Telegram(Session(), token)
    with pytest.raises(RuntimeError) as error:
        await api.call('getUpdates')
    assert token not in str(error.value)
    assert error.value.__suppress_context__


def test_help_does_not_read_keychain_or_connect(monkeypatch):
    monkeypatch.setattr(bot, 'MacOSKeychainBackend', lambda: pytest.fail('secret access'))
    with pytest.raises(SystemExit) as result:
        bot.main(['--help'])
    assert result.value.code == 0


def test_token_binding_is_separate_from_trading_keys():
    assert bot.BotBinding().record_account == 'hood-telegram-control-v1'


@pytest.mark.parametrize('command,entry', [('/run 1','simple'),('/close','close-positions')])
async def test_server_discards_backlog_and_uses_fixed_detached_child(tmp_path, monkeypatch, command, entry):
    tmp_path.chmod(0o700)
    config = tmp_path / 'random-cycle.json'
    config.write_text(json.dumps({
        'market_id': 7, 'market_symbol': 'BTC', 'direction': 'LONG',
        'source_account_index': 11, 'receiver_account_index': 22,
        'cycle_dir': str(tmp_path / 'cycle'), 'api_key_index': 4,
        'margin_reserve': {'initial_quote': '0.10', 'dispatch_quote': '0.02'},
    }))
    (tmp_path / 'market-contract.json').write_text('{}')
    store = bot.Store(tmp_path / '.telegram-control', 'binding')
    calls = []
    completed = asyncio.Event()
    class Stop(BaseException): pass
    class API(Transport):
        def __init__(self, *args):
            super().__init__()
            self.reads = 0
        async def call(self, method, **payload):
            assert method == 'getUpdates'
            self.reads += 1
            if self.reads == 1:
                assert payload['offset'] == -1
                return [update(10,command)]
            if self.reads == 2:
                assert payload['offset'] == 11
                return [update(11,command)]
            await completed.wait()
            raise Stop()
    class Session:
        def __init__(self, **kwargs): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
    class Process:
        async def communicate(self, data):
            assert data == b'\n'
            completed.set()
    async def create(*args, **kwargs):
        calls.append((args, kwargs))
        return Process()
    real = bot.Controller
    monkeypatch.setattr(bot, 'Controller', lambda *args, **kwargs: real(*args, **kwargs, now=lambda: 1000))
    from risex_spread_shadow.hood_handoff import cli, operator_recovery
    checks=[]
    async def recover(*args, require_flat=True):
        checks.append(require_flat)
        return {'status':'READY' if require_flat else 'CLOSE_READY','at':1000,
                'source': {'account_index': 11, 'market_id': 7, 'signed_position': '0',
                           'active_orders': [], 'authorized': True, 'ready': True},
                'receiver': {'account_index': 22, 'market_id': 7, 'signed_position': '0',
                             'active_orders': [], 'authorized': True, 'ready': True}}
    monkeypatch.setattr(cli,'_validate_simple_local_inputs',lambda *a,**k:(object(),{}))
    monkeypatch.setattr(operator_recovery,'check_recovery',recover)
    monkeypatch.setattr(bot, 'Telegram', API)
    monkeypatch.setattr(bot.aiohttp, 'ClientSession', Session)
    monkeypatch.setattr(bot.asyncio, 'create_subprocess_exec', create)
    # The production interpreter path is fixed; no SDK or child runs here.
    original_is_file = Path.is_file
    monkeypatch.setattr(Path, 'is_file', lambda p: True if str(p).endswith('.venv-hood/bin/python') else original_is_file(p))
    with pytest.raises(Stop):
        await bot.serve(SimpleNamespace(config=config, owner_id=42), store, 99, 'synthetic-token-not-used')
    assert checks == ([False, True] if command == '/run 1' else [False])
    assert len(calls) == 1
    argv, kwargs = calls[0]
    assert argv[1:5] == ('-m', 'risex_spread_shadow.hood_handoff.cli', entry, '--keychain')
    assert argv[-2:] == ('--config', str(config))
    parsed = cli._random_cycle_config(json.loads(config.read_text()), execute=True, plan_reviewed=True)
    cli._require_owner_opening_margin_reserve(parsed)
    assert parsed.binding()['margin_reserve'] == {'initial_quote': '0.10', 'dispatch_quote': '0.02'}
    assert '--no-progress' in argv
    assert kwargs['env']['RISEX_HOOD_OPERATOR_INTERFACE'] == 'telegram'
    assert kwargs['pass_fds'] == (99,)
    assert kwargs['start_new_session'] is True
    assert kwargs['stdout'] == asyncio.subprocess.DEVNULL
    assert kwargs['stderr'] == asyncio.subprocess.DEVNULL
    assert 'synthetic-token' not in repr(calls)
    assert store.data['offset'] == 12


@pytest.mark.parametrize('action,proof_status,require_flat,cleared', [
    ('close', 'CLOSE_READY', False, True),
    ('close', 'UNKNOWN', False, False),
    ('run', 'CLOSE_READY', True, False),
    ('run', 'READY', True, True),
])
async def test_startup_reconciles_interrupted_action_without_dispatch(
    tmp_path, monkeypatch, action, proof_status, require_flat, cleared,
):
    tmp_path.chmod(0o700)
    config = tmp_path / 'random-cycle.json'
    config.write_text('{}')
    (tmp_path / 'market-contract.json').write_text('{}')
    slot = tmp_path / ('close-001' if action == 'close' else 'cycle-001')
    slot.mkdir()
    store = bot.Store(tmp_path / '.telegram-control', 'binding')
    store.data['active'] = {'before': [], 'update_id': 1, 'action': action}
    store.save()
    calls = []

    class Stop(BaseException): pass
    class API(Transport):
        async def call(self, method, **payload):
            assert method == 'getUpdates'
            raise Stop()
    class Session:
        def __init__(self, **kwargs): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
    async def recover(*args, require_flat=True):
        calls.append(require_flat)
        return {'status': proof_status, 'at': 1000, 'previous_intents': 1}

    from risex_spread_shadow.hood_handoff import cli, operator_recovery
    monkeypatch.setattr(cli, '_validate_simple_local_inputs', lambda *a, **k: (object(), {}))
    monkeypatch.setattr(operator_recovery, 'check_recovery', recover)
    monkeypatch.setattr(bot, 'Telegram', lambda *args: API())
    monkeypatch.setattr(bot.aiohttp, 'ClientSession', Session)
    monkeypatch.setattr(bot.asyncio, 'create_subprocess_exec',
                        lambda *args, **kwargs: pytest.fail('startup dispatched an order'))
    original_is_file = Path.is_file
    monkeypatch.setattr(Path, 'is_file',
                        lambda p: True if str(p).endswith('.venv-hood/bin/python') else original_is_file(p))
    with pytest.raises(Stop):
        await bot.serve(SimpleNamespace(config=config, owner_id=42), store, 99, 'unused')
    assert calls == [require_flat]
    assert (store.data['active'] is None) is cleared
    assert store.data['last'] == {'status': 'BLOCKED', 'cycle': slot.name}
    assert json.loads(store.path.read_text())['active'] == store.data['active']


async def test_nonflat_close_recovery_keeps_run_guard(tmp_path):
    from risex_spread_shadow.hood_handoff.contracts import PreflightBlocked

    launches = []
    async def launch(): launches.append(True)
    c = setup(tmp_path, launch)
    c.store.data['active'] = {'before': [], 'update_id': 1, 'action': 'close'}
    c.store.save()
    calls = []
    async def recover(*, require_flat=True):
        calls.append(require_flat)
        if require_flat:
            raise PreflightBlocked('positions remain; use /close before /run')
        return {'status': 'CLOSE_READY', 'at': 1000, 'previous_intents': 1}
    c.recovery = recover

    await c.reconcile_idle(require_flat=False)
    await c.handle(update(2, '/run 1'))
    await c.task
    assert calls == [False, True]
    assert launches == []
    assert c.store.data['active'] is None
    assert 'Есть открытые позиции' in c.transport.messages[-1][1]


async def test_persistence_failure_prevents_dispatch(tmp_path, monkeypatch):
    calls = []
    async def launch(): calls.append(True)
    c = setup(tmp_path, launch)
    def fail(): raise OSError('synthetic disk failure')
    monkeypatch.setattr(c.store, 'save', fail)
    with pytest.raises(OSError):
        await c.handle(update())
    await asyncio.sleep(0)
    assert calls == []


async def test_local_simple_launcher_obeys_same_operator_lock(tmp_path, monkeypatch):
    from risex_spread_shadow.hood_handoff import cli
    tmp_path.chmod(0o700)
    config = tmp_path / 'random-cycle.json'
    config.write_text('{}')
    monkeypatch.setattr(cli, '_simple_confirmation', lambda: True)
    monkeypatch.setattr(cli, '_simple_operator_dir', lambda _: tmp_path)
    monkeypatch.setattr(cli, '_print_simple_summary', lambda *args, **kwargs: None)
    async def forbidden(*args): pytest.fail('overlapping cycle')
    monkeypatch.setattr(cli, '_run_simple_confirmed', forbidden)
    with exclusive_lock(tmp_path / '.operator-launch.lock'):
        with pytest.raises(RuntimeError, match='active'):
            await cli._run_simple(cli._parser().parse_args(['simple', '--config', str(config)]))


def test_local_provision_saves_only_keychain_not_token_files(tmp_path, monkeypatch, capsys):
    from risex_spread_shadow.hood_handoff import cli
    tmp_path.chmod(0o700)
    config = tmp_path / 'random-cycle.json'
    config.write_text('{}')
    (tmp_path / 'market-contract.json').write_text('{}')
    token = '123456:' + 'syntheticCanaryToken' * 2
    saved = []
    class Backend:
        def put(self, binding, value, *, replace): saved.append((binding.record_account, value, replace))
    monkeypatch.setattr(Path, 'home', lambda: tmp_path)
    monkeypatch.setattr(cli, '_validate_simple_local_inputs', lambda *args, **kwargs: None)
    monkeypatch.setattr(bot, 'MacOSKeychainBackend', Backend)
    monkeypatch.setattr(bot, 'read_hidden_secret', lambda _: token)
    assert bot.main(['provision', '--owner-id', '42', '--config', str(config)]) == 0
    assert saved == [('hood-telegram-control-v1', token, False)]
    assert token not in capsys.readouterr().out
    for p in tmp_path.rglob('*'):
        if p.is_file(): assert token.encode() not in p.read_bytes()


async def test_changed_config_does_not_start_a_child(tmp_path, monkeypatch):
    tmp_path.chmod(0o700)
    config = tmp_path / 'random-cycle.json'
    config.write_text('{}')
    (tmp_path / 'market-contract.json').write_text('{}')
    store = bot.Store(tmp_path / '.telegram-control', 'binding')
    finished = asyncio.Event()
    class Stop(BaseException): pass
    class API(Transport):
        def __init__(self, *args): super().__init__(); self.reads = 0
        async def call(self, *args, **kwargs):
            self.reads += 1
            if self.reads == 1: return []
            if self.reads == 2:
                config.write_text('{"changed": true}')
                return [update()]
            await finished.wait()
            raise Stop()
        async def send(self, owner, text, markup=None):
            if 'Новая операция пока не начата' in text: finished.set()
    class Session:
        def __init__(self, **kwargs): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
    real = bot.Controller
    monkeypatch.setattr(bot, 'Controller', lambda *args, **kwargs: real(*args, **kwargs, now=lambda: 1000))
    monkeypatch.setattr(bot, 'Telegram', API)
    monkeypatch.setattr(bot.aiohttp, 'ClientSession', Session)
    monkeypatch.setattr(Path, 'is_file', lambda _: True)
    monkeypatch.setattr(bot.asyncio, 'create_subprocess_exec', lambda *args, **kwargs: pytest.fail('changed configuration dispatched'))
    with pytest.raises(Stop):
        await bot.serve(SimpleNamespace(config=config, owner_id=42), store, 99, 'unused')
    assert store.data['active'] is None


@pytest.mark.parametrize("failed", [False, True])
async def test_status_during_final_notification_does_not_crash(tmp_path, monkeypatch, failed):
    reached, release = asyncio.Event(), asyncio.Event()
    async def launch():
        if failed:
            restore_cycle(tmp_path, '003')
    c = setup(tmp_path, launch)
    async def delayed_notify(text):
        if c._runner_finished:
            reached.set()
            await release.wait()
    monkeypatch.setattr(c, 'notify', delayed_notify)
    await c.handle(update())
    await reached.wait()
    try:
        summary = c.summary()
        assert summary
        assert 'Цикл выполняется' not in summary
        if failed:
            assert 'сверк' in summary
    finally:
        release.set()
        await c.task


def test_unreadable_report_has_explicit_safe_message(tmp_path, monkeypatch):
    c = setup(tmp_path, None)
    c.store.data['last'] = {'status': 'BLOCKED', 'cycle': 'cycle-001'}
    def fail(*args): raise OSError('synthetic sensitive detail')
    monkeypatch.setattr(bot, 'load_saved_cycle_report', fail)
    text = c.summary()
    assert 'недоступен' in text
    assert 'synthetic' not in text


def test_malformed_last_state_fails_closed_on_load(tmp_path):
    c = setup(tmp_path, None)
    c.store.data['last'] = ['malformed']
    c.store.save()
    with pytest.raises(RuntimeError):
        setup(tmp_path, None)


async def test_close_acknowledgement_never_promises_new_opening(tmp_path):
    calls = []
    async def launch(): pytest.fail('/close must never open')
    async def close(): calls.append('close')
    c = setup(tmp_path, launch)
    c.close = close
    await c.handle(update(text='/close'))
    await c.task
    await c._notice_task
    message = c.transport.messages[0][1]
    assert '/close принята' in message
    assert 'reduce-only' in message
    assert 'один реальный цикл' not in message
    assert 'цикл начнётся' not in message
    assert calls == ['close']
