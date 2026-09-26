import asyncio
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import pytest

from risex_spread_shadow.hood_handoff import telegram_accounts as reader
from risex_spread_shadow.hood_handoff import telegram_messages as views
from risex_spread_shadow.hood_handoff.contracts import AccountSnapshot
from risex_spread_shadow.hood_handoff.keychain import KeychainSecretProvider, MemoryKeychainBackend
from tests.spread_shadow.test_hood_telegram_control import setup, update
from tests.spread_shadow.test_hood_telegram_messages import valid


def config():
    return SimpleNamespace(source_account_index=10, receiver_account_index=20, market_id=1,
        market_symbol='BTC', freshness_seconds=10, request_timeout_seconds=.02,
        api_base_url='https://api.rh.lighter.xyz', chain_id=466324, api_key_index=4)


def snapshot(index, **changes):
    return replace(AccountSnapshot(index, 1, Decimal('0.001') if index == 10 else Decimal('-0.001'),
        (), 1000, True, True, None, None, None, 'private-wallet',
        available_balance=Decimal('12.345678')), **changes)


async def inspect(mode='ok'):
    events = []
    class Secrets:
        def close(self): events.append('secrets closed')
    def secrets(c, indices, *, prompt):
        assert indices == (10, 20)
        with pytest.raises(RuntimeError): prompt('never ask owner')
        return Secrets()
    class Client:
        def __init__(self, *args, **kwargs): pass
        async def account_snapshot(self, index, market):
            events.append(index)
            assert market == 1
            if index == 10:
                if mode == 'error': raise RuntimeError('SECRET raw SDK exception')
                if mode == 'timeout': await asyncio.sleep(1)
                if mode == 'identity': return snapshot(20)
                if mode == 'market': return snapshot(index, market_id=2)
                if mode == 'unauthorized': return snapshot(index, authorized=False)
                if mode == 'stale': return snapshot(index, observed_at=980)
                if mode == 'future': return snapshot(index, observed_at=1001)
                if mode == 'unknown_balance': return snapshot(index, available_balance=None)
                if mode == 'inactive': return snapshot(index, ready=False)
                if mode == 'zero': return snapshot(index, signed_position=Decimal(0), available_balance=Decimal(0))
            return snapshot(index)
        async def aclose(self): events.append('client closed')
    result = await reader.read_accounts(config(), client_factory=Client, secret_factory=secrets, clock=lambda:1000)
    assert events[-2:] == ['client closed', 'secrets closed']
    assert sorted(events[:2]) == [10,20]
    return result


@pytest.mark.parametrize('mode', ['ok','error','timeout','identity','market','unauthorized',
                                 'stale','future','unknown_balance','inactive','zero'])
async def test_accounts_exact_partial_unknown_and_markup(mode):
    result = await inspect(mode)
    message = valid(views.accounts_message(result))
    assert '12.345678' in message and '-0.001 BTC' in message
    assert 'private-wallet' not in message and 'SECRET' not in message
    if mode in ('error','timeout','identity','market','unauthorized'):
        assert result['accounts'][0]['snapshot'] is None
        assert 'неизвестны' in message
    if mode in ('stale','future'):
        assert result['accounts'][0]['stale']
        assert 'Устаревшее наблюдение' in message
    if mode == 'unknown_balance': assert 'нет данных' in message
    if mode == 'inactive': assert 'не подтверждает активный статус' in message
    if mode == 'zero': assert 'нет позиции на этом рынке' in message
    result['symbol'] = '<script>&bad'
    assert '<script>' not in views.accounts_message(result)
    valid(views.accounts_message(result))


async def test_client_creation_failure_clears_keys():
    closed = []
    def fail(*a, **kw): raise RuntimeError('SDK unavailable')
    with pytest.raises(RuntimeError):
        await reader.read_accounts(config(), client_factory=fail,
            secret_factory=lambda *a, **kw: SimpleNamespace(close=lambda:closed.append(True)))
    assert closed == [True]


async def test_cancellation_closes_client_and_keys():
    entered = asyncio.Event()
    closed = []
    class Client:
        def __init__(self, *a, **kw): pass
        async def account_snapshot(self, *a):
            entered.set()
            await asyncio.Event().wait()
        async def aclose(self): closed.append('client')
    task = asyncio.create_task(reader.read_accounts(config(), client_factory=Client,
        secret_factory=lambda *a, **kw: SimpleNamespace(close=lambda:closed.append('keys'))))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError): await task
    assert closed == ['client','keys']


def test_missing_key_never_provisions():
    class Backend(MemoryKeychainBackend):
        def put(self, *a, **kw): pytest.fail('must not provision')
    provider = KeychainSecretProvider.from_config(config(), (10,20), backend=Backend(), prompt=reader.missing_key)
    with pytest.raises(RuntimeError): provider.private_key(10,4)
    provider.close()


@pytest.mark.parametrize('failure', [False,True])
async def test_owner_accounts_preserves_block_and_never_launches(tmp_path, failure):
    calls = []
    async def launch(): pytest.fail('read command cannot launch')
    async def accounts():
        calls.append(True)
        if failure: raise RuntimeError('SECRET SDK detail')
        return await inspect()
    c = setup(tmp_path, launch)
    c.accounts = accounts
    c.store.data['active'] = {'before': [], 'update_id': 0}
    await c.handle(update(text='/accounts'))
    await c.handle(update(text='/accounts'))
    assert calls == [True] and c.task is None
    assert c.store.data['active'] == {'before': [], 'update_id': 0}
    message = valid(c.transport.messages[-1][1])
    assert 'SECRET' not in message
    assert ('Счета недоступны' if failure else 'Балансы и позиции') in message


@pytest.mark.parametrize('change', ['sender','group','forward','stale','edited','args'])
async def test_account_reads_require_fresh_private_owner(tmp_path, change):
    async def forbidden(): pytest.fail('unauthorized private read or launch')
    c = setup(tmp_path, forbidden)
    c.accounts = forbidden
    u = update(text='/accounts')
    if change == 'sender': u['message']['from']['id'] = 43
    if change == 'group': u['message']['chat']['type'] = 'group'
    if change == 'forward': u['message']['forward_origin'] = {}
    if change == 'stale': u['message']['date'] = 800
    if change == 'edited': u['edited_message'] = u.pop('message')
    if change == 'args': u['message']['text'] = '/accounts 99'
    await c.handle(u)
    assert c.task is None


@pytest.mark.parametrize('changed', [None, 'config', 'evidence'])
async def test_serve_accounts_bound_configuration(tmp_path, monkeypatch, changed):
    from pathlib import Path
    from risex_spread_shadow.hood_handoff import telegram_control as bot, cli
    from tests.spread_shadow.test_hood_telegram_control import Transport
    tmp_path.chmod(0o700)
    path = tmp_path / 'random-cycle.json'
    evidence = tmp_path / 'market-contract.json'
    path.write_text('{}')
    evidence.write_text('{}')
    store = bot.Store(tmp_path / 'state', 'binding')
    reads, messages = [], []
    class Stop(BaseException): pass
    class API(Transport):
        def __init__(self, *a): self.count = 0
        async def call(self, *a, **kw):
            self.count += 1
            if self.count == 1: return []
            if self.count == 2:
                if changed: (path if changed == 'config' else evidence).write_text('{"changed":true}')
                return [update(text='/accounts')]
            raise Stop()
        async def send(self, owner, text): messages.append(text)
    class Session:
        def __init__(self, **kw): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): pass
    inspected = await inspect()
    async def read(c, *, operator):
        # The controller passes its operator directory so a wallet pool is read.
        assert operator == path.parent
        reads.append(c)
        return inspected
    real = bot.Controller
    monkeypatch.setattr(bot, 'Controller', lambda *a, **kw: real(*a, **kw, now=lambda:1000))
    monkeypatch.setattr(bot, 'Telegram', API)
    monkeypatch.setattr(bot.aiohttp, 'ClientSession', Session)
    monkeypatch.setattr(Path, 'is_file', lambda _: True)
    monkeypatch.setattr(cli, '_validate_simple_local_inputs', lambda *a, **kw:(config(), {}))
    monkeypatch.setattr(reader, 'read_accounts', read)
    monkeypatch.setattr(bot.asyncio, 'create_subprocess_exec', lambda *a, **kw:pytest.fail('read launched child'))
    with pytest.raises(Stop):
        await bot.serve(SimpleNamespace(config=path, owner_id=42), store, 99, 'unused')
    assert len(reads) == (0 if changed else 1)
    assert ('Счета недоступны' if changed else 'Балансы и позиции') in messages[-1]
    assert store.data['active'] is None and store.data['last'] is None
