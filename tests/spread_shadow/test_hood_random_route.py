"""Random account/side routing, reservation failures and immutable admission."""
import json
from types import SimpleNamespace

import pytest

from risex_spread_shadow.hood_handoff import cli, random_cycle as cycle, telegram_messages as views
from risex_spread_shadow.hood_handoff.contracts import ContractError, Outcome, PreflightBlocked
from tests.spread_shadow.test_hood_handoff_random_cycle import (
    AdvancingClock, CycleClient, FixedRng, cycle_config, _write_simple_launcher_fixture)
from tests.spread_shadow.test_hood_telegram_messages import valid


@pytest.mark.parametrize('draw', [-1, 4, True, '0', None])
def test_invalid_rng_fails_closed(tmp_path, draw):
    with pytest.raises(ContractError):
        cycle.select_random_route(cycle_config(tmp_path/'unused'), SimpleNamespace(randint=lambda a,b:draw))
    assert not (tmp_path/'unused').exists()


@pytest.mark.parametrize('key,value', [('source_account_index',22),('receiver_account_index',11),
                                       ('direction','SHORT')])
async def test_reserved_route_mismatch_blocks_before_account_reads(tmp_path, key, value):
    cfg = cycle_config(tmp_path/'unused')
    route = cycle.select_random_route(cfg, FixedRng(0))
    tmp_path.chmod(0o700)
    path, prefix = cycle.allocate_cycle_slot(tmp_path, random_route=route)
    # Swap both roles to keep config valid; independently flip direction.
    updates = {'source_account_index':22,'receiver_account_index':11} if key != 'direction' else {key:value}
    cfg = cycle_config(path, client_order_prefix=prefix, **updates)
    class NoRead:
        def __getattr__(self, name): pytest.fail('mismatched route reached client')
    result = await cycle.run_random_cycle(cfg, NoRead(), clock=AdvancingClock(), rng=FixedRng())
    assert result.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED
    assert 'reserved random route' in result.reason
    assert not (path/'admission.json').exists() and not (path/'cycle.jsonl').exists()


@pytest.mark.parametrize('corrupt', [None, {}, {'source_account_index':True,'receiver_account_index':22,'direction':'LONG'},
    {'source_account_index':11,'receiver_account_index':11,'direction':'LONG'},
    {'source_account_index':11,'receiver_account_index':22,'direction':'SIDEWAYS'}])
def test_malformed_saved_route_blocks_admission(tmp_path, corrupt):
    tmp_path.chmod(0o700)
    path, _ = cycle.allocate_cycle_slot(tmp_path)
    file = path/'launch.json'
    value = json.loads(file.read_text()); value['random_route'] = corrupt
    file.write_text(json.dumps(value))
    with pytest.raises(PreflightBlocked): cycle.RandomCycleEngine.validate_cycle_directory(path)


@pytest.mark.parametrize('failure', ['before_write','after_write','directory_sync'])
async def test_persistence_failure_never_reaches_credentials(tmp_path, monkeypatch, failure):
    config_path, operator, evidence = _write_simple_launcher_fixture(tmp_path)
    rng = FixedRng(3)
    monkeypatch.setattr('builtins.input', lambda _: '')
    monkeypatch.setattr(cli, '_validate_simple_sdk', lambda:None)
    monkeypatch.setattr(cli, 'select_random_route', lambda cfg:cycle.select_random_route(cfg,rng))
    def forbidden(*a,**kw): pytest.fail('persistence failure accessed secrets or client')
    monkeypatch.setattr(cli, '_keychain_provider', forbidden)
    monkeypatch.setattr(cli, 'LighterSdkClient', forbidden)
    real_write = cycle._atomic_launch_metadata
    def fail_write(path, payload):
        if failure == 'after_write': real_write(path,payload)
        raise PreflightBlocked('simulated crash at reservation')
    if failure == 'directory_sync':
        original = cycle.os.fsync
        count = 0
        def sync(fd):
            nonlocal count
            count += 1
            if count == 2: raise OSError('simulated directory sync failure')
            return original(fd)
        monkeypatch.setattr(cycle.os, 'fsync', sync)
    else: monkeypatch.setattr(cycle, '_atomic_launch_metadata', fail_write)
    args = cli._parser().parse_args(['simple','--keychain','--config',str(config_path)])
    with pytest.raises((SystemExit,OSError)): await cli._run(args)
    assert rng.bounds == [(0,3)]
    slot = operator/'cycle-001'
    assert slot.exists() and not (slot/'admission.json').exists()
    if failure != 'before_write':
        assert json.loads((slot/'launch.json').read_text())['random_route'] == {
            'source_account_index':22,'receiver_account_index':11,'direction':'SHORT'}


async def test_cancel_does_not_sample_route(tmp_path,monkeypatch):
    path,operator,_ = _write_simple_launcher_fixture(tmp_path)
    monkeypatch.setattr('builtins.input',lambda _: 'C')
    monkeypatch.setattr(cli,'select_random_route',lambda _:pytest.fail('cancel sampled a route'))
    assert await cli._run(cli._parser().parse_args(['simple','--config',str(path)])) == 0
    assert not list(operator.glob('cycle-*'))


@pytest.mark.parametrize('draw', [0,1,2,3])
def test_saved_telegram_report_names_actual_first_account_and_side(tmp_path,draw):
    route = cycle.select_random_route(cycle_config(tmp_path/'unused'),FixedRng(draw))
    message = valid(views.saved_message('cycle-001', {'binding':route}))
    assert f'Первый счёт: {11 if draw % 2 == 0 else 22}' in message
    assert ('лимитный SELL' if draw < 2 else 'лимитный BUY') in message
    assert f'Второй счёт: {22 if draw % 2 == 0 else 11}' in message
