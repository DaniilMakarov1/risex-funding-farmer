"""Operator recovery with synthetic accounts; no SDK, secrets or network."""
import asyncio
from dataclasses import replace
from decimal import Decimal
import json
from pathlib import Path

import pytest

from risex_spread_shadow.hood_handoff import operator_recovery as recovery
from risex_spread_shadow.hood_handoff.contracts import OrderPlan, OrderSnapshot, PreflightBlocked
from risex_spread_shadow.hood_handoff.journal import DurableJournal
from risex_spread_shadow.hood_handoff.operator_view import execution_lines, read_execution_notices, close_result_lines
from test_hood_handoff_random_cycle import (AdvancingClock, CycleClient, FixedRng, cycle_config,
    run_random_cycle, GuardCancellationFillClient, ExternalCloseClient)
from test_hood_telegram_control import setup, update


def seed_order(clock):
    return OrderSnapshot(11, 7, 'old', 777, 'canceled', 'SELL', 'LIMIT', 'POST_ONLY', False,
                         Decimal('.2'), Decimal('.2'), Decimal(0), Decimal('100.1'), clock.now())


class RecoveryClient(CycleClient):
    def __init__(self, clock, source='0', receiver='0', *, fractions=None, unknown=False):
        super().__init__(clock, fallback_fill_fractions=fractions)
        self.source_position, self.receiver_position = Decimal(source), Decimal(receiver)
        self._save_order(seed_order(clock))
        self.unknown = unknown
        self.reads = []

    async def submit_order(self, plan):
        assert plan.reduce_only and plan.order_type == 'MARKET' and plan.time_in_force == 'IOC'
        receipt = await super().submit_order(plan)
        if self.unknown:
            raise TimeoutError('ambiguous first send')
        return receipt

    async def lookup_order(self, *args, **kwargs):
        self.reads.append(kwargs)
        return await super().lookup_order(*args, **kwargs)


def intent_journal(operator, *, complete=False, status='canceled', unknown=False):
    slot = operator/'cycle-001'
    slot.mkdir(mode=0o700)
    j = DurableJournal(slot/'opening.jsonl', clock=lambda: 1000)
    p = OrderPlan(11, 7, 'SELL', Decimal('.2'), 20, Decimal('100.1'), 1001, 'LIMIT', 'POST_ONLY', False, 1300000, 777)
    j.acquire_attempt()
    try:
        j.append('SOURCE_DISPATCH_INTENT', {'plan':p.as_dict()})
        if unknown:
            j.append('SOURCE_DISPATCH_UNKNOWN', {'reason':'timeout'})
        else:
            j.append('SOURCE_DISPATCH_RESULT', {'accepted':True,'order_id':'old'})
        if complete:
            order = {'account_index':11, 'market_id':7, 'order_id':'old', 'client_order_index':777,
                     'side':'SELL', 'order_type':'LIMIT', 'time_in_force':'POST_ONLY', 'reduce_only':False,
                     'price':'100.1', 'initial_quantity':'.2', 'filled_quantity':'0', 'remaining_quantity':'.2',
                     'status':status, 'observed_at':1000}
            j.append('COMPLETE', {'outcome':'UNKNOWN','receipt':{'source':{'order':order},'receiver':None}})
    finally:
        j.release_attempt()
    return slot


@pytest.mark.asyncio
@pytest.mark.parametrize('positions,expected', [(('0','0'),[]),(('.2','0'),[11]),(('0','-.2'),[22]),(('.2','-.3'),[11,22]),(('-.2','-.3'),[11,22])])
async def test_close_adopts_only_requested_current_positions(tmp_path, positions, expected):
    clock=AdvancingClock();client=RecoveryClient(clock,*positions)
    slot=recovery.allocate_close_slot(tmp_path)
    result=await recovery.close_positions(cycle_config(slot),client,tmp_path,slot,clock=clock)
    assert result['status']=='CONFIRMED_FLAT',result
    assert [p.account_index for p in client.submissions]==expected
    assert [p.quantity for p in client.submissions]==[abs(Decimal(p)) for p in positions if Decimal(p)]
    assert client.source_position==client.receiver_position==0
    assert recovery.load_close_result(slot)==result
    text='\n'.join(close_result_lines(result))
    assert '✅ Позиции закрыты' in text and 'PnL отдельно закрытых позиций неизвестен' in text
    with pytest.raises(PreflightBlocked,match='replayed'):
        await recovery.close_positions(cycle_config(slot),client,tmp_path,slot,clock=clock)
    assert len(client.submissions)==len(expected)


@pytest.mark.asyncio
async def test_close_reconciles_partial_and_zero_fills_fairly_with_unique_ids(tmp_path):
    clock=AdvancingClock();client=RecoveryClient(clock,'.4','-.4',fractions=[Decimal(0),Decimal('.5'),Decimal(1),Decimal(1)])
    slot=recovery.allocate_close_slot(tmp_path)
    result=await recovery.close_positions(cycle_config(slot),client,tmp_path,slot,clock=clock)
    assert result['status']=='CONFIRMED_FLAT',result
    assert [p.account_index for p in client.submissions]==[11,22,22,11]
    assert [p.quantity for p in client.submissions]==[Decimal('.4'),Decimal('.4'),Decimal('.2'),Decimal('.4')]
    assert len({p.client_order_index for p in client.submissions})==4
    assert clock.sleeps==pytest.approx([.001])


@pytest.mark.asyncio
async def test_close_ambiguous_first_send_stops_other_account_and_no_replay(tmp_path):
    clock=AdvancingClock();client=RecoveryClient(clock,'.2','-.2',unknown=True)
    slot=recovery.allocate_close_slot(tmp_path)
    result=await recovery.close_positions(cycle_config(slot),client,tmp_path,slot,clock=clock)
    assert result['status']=='UNKNOWN'
    assert len(client.submissions)==1
    assert client.receiver_position==Decimal('-.2')
    # A zero current position alone cannot erase an invisible creation intent.
    client.orders.clear();client.latest_order.clear();client.source_position=client.receiver_position=Decimal(0)
    with pytest.raises(PreflightBlocked,match='unresolved'):
        await recovery.inspect_current(cycle_config(slot),client,tmp_path,require_flat=True,clock=clock)
    assert len(client.submissions)==1


@pytest.mark.asyncio
@pytest.mark.parametrize('complete',[False,True])
async def test_old_unknown_outcome_does_not_lock_manually_flat_accounts(tmp_path,complete):
    clock=AdvancingClock();client=RecoveryClient(clock)
    slot=intent_journal(tmp_path,complete=complete)
    before={p:p.read_bytes() for p in slot.iterdir()}
    _,_,proof=await recovery.inspect_current(cycle_config(tmp_path/'unused'),client,tmp_path,require_flat=True,clock=clock)
    assert proof['status']=='READY' and proof['previous_intents']==1
    assert len(client.reads)==(0 if complete else 1)
    assert all(p.read_bytes()==b for p,b in before.items())
    assert not client.submissions


@pytest.mark.asyncio
@pytest.mark.parametrize('bad',['missing','active','different_id','wrong_price','stale','future','position','active_other'])
async def test_current_readiness_rejects_executable_unproved_or_conflicting_state(tmp_path,bad):
    clock=AdvancingClock();client=RecoveryClient(clock)
    intent_journal(tmp_path)
    order=client.orders[(11,'old')]
    if bad=='missing':client.orders.clear();client.latest_order.clear()
    elif bad=='active':client._save_order(replace(order,status='open'))
    elif bad=='different_id':client.orders.clear();client._save_order(replace(order,order_id='wrong'))
    elif bad=='wrong_price':client._save_order(replace(order,price=Decimal('101')))
    elif bad=='stale':client._save_order(replace(order,observed_at=900))
    elif bad=='future':client._save_order(replace(order,observed_at=1001))
    elif bad=='position':client.source_position=Decimal('.2')
    elif bad=='active_other':client._save_order(replace(order,order_id='other',client_order_index=999,status='open'))
    with pytest.raises(PreflightBlocked):
        await recovery.inspect_current(cycle_config(tmp_path/'unused'),client,tmp_path,require_flat=True,clock=clock)
    assert not client.submissions


@pytest.mark.asyncio
async def test_fresh_command_rechecks_sticky_barrier_and_close_is_owner_only(tmp_path):
    entered=asyncio.Event();release=asyncio.Event();calls=[]
    async def launch():calls.append('run')
    async def close():calls.append('close')
    async def check(*,require_flat):
        entered.set();await release.wait()
        return {'status':'READY' if require_flat else 'CLOSE_READY','at':1000,'previous_intents':1}
    c=setup(tmp_path,launch);c.recovery=check;c.close=close
    c.store.data['active']={'before':[],'update_id':0};c.store.save()
    await c.handle(update(1));await entered.wait()
    await c.handle(update(2,'/close')) # Still checking: cannot overlap.
    assert not calls
    release.set();await c.task
    assert calls==['run']
    await c.handle(update(1));assert calls==['run']
    foreign=update(3,'/close');foreign['message']['from']['id']=99
    await c.handle(foreign);assert calls==['run']
    await c.handle(update(4,'/close'));await c.task
    assert calls==['run','close']
    assert c.store.data['last_recovery']['status']=='CLOSE_READY'


@pytest.mark.asyncio
async def test_failed_current_check_is_retryable_without_replaying_old_operation(tmp_path):
    calls=[];checks=[]
    async def launch():calls.append(True)
    async def check(*,require_flat):
        checks.append(True)
        if len(checks)==1:raise PreflightBlocked('positions remain; use /close before /run')
        return {'status':'READY','at':1000}
    c=setup(tmp_path,launch);c.recovery=check
    c.store.data['active']={'before':[],'update_id':0};c.store.save()
    await c.handle(update(1));await c.task
    assert not calls and c.store.data['active'] is not None
    await c.handle(update(2));await c.task
    assert calls==[True] and c.store.data['active'] is None


@pytest.mark.asyncio
@pytest.mark.parametrize('kind',['own','external_source','external_receiver'])
async def test_notices_report_real_counterparties_and_timing(tmp_path,kind):
    clock=AdvancingClock()
    client=CycleClient(clock) if kind=='own' else GuardCancellationFillClient(clock,missing_level=True) if kind=='external_source' else ExternalCloseClient(clock)
    result=await run_random_cycle(cycle_config(tmp_path/'cycle'),client,clock=clock,rng=FixedRng(20,20))
    phase=result.closing if kind=='external_receiver' else result.opening
    text='\n'.join(execution_lines(phase.as_dict(),phase='closing' if kind=='external_receiver' else 'opening'))
    assert 'LIMIT · счёт 11' in text and 'MARKET · счёт 22' in text
    if kind=='own':assert 'наш парный счёт — 0.2' in text and 'LIMIT → MARKET:' in text
    else:assert 'внешние счета 999 — 0.2' in text
    if kind=='external_source':assert 'MARKET · счёт 22: не отправлялся' in text
    notices=read_execution_notices(tmp_path/'cycle/cycle.jsonl')
    assert any('LIMIT принят биржей: счёт 11' in t for _,t in notices)
    assert any(text==t for _,t in notices)
    # A missing reciprocal order cannot be described as our successful match.
    if kind=='own':
        saved=phase.as_dict();saved['receiver']['trades'][0]['counterparty_order_id']=None
        assert 'наш парный счёт — 0.2' not in '\n'.join(execution_lines(saved,phase='closing'))


class ClosingPreparationTimeout(CycleClient):
    def __init__(self, clock, failures):
        super().__init__(clock)
        self.failures=failures
        self.failed=0
    async def order_book(self, market_id):
        if self.clock.now()>=1020 and self.failed<self.failures:
            self.failed+=1
            raise TimeoutError('closing book timeout')
        return await super().order_book(market_id)


@pytest.mark.asyncio
@pytest.mark.parametrize('failures',[1,2,3])
async def test_closing_preparation_timeout_retries_before_exposure_in_same_three_attempt_budget(tmp_path,failures):
    clock=AdvancingClock();client=ClosingPreparationTimeout(clock,failures)
    result=await run_random_cycle(cycle_config(tmp_path/'cycle'),client,clock=clock,rng=FixedRng(20,20))
    assert result.inventory=='CONFIRMED_FLAT'
    assert client.failed==failures
    if failures<3:
        assert result.closing.mutual_execution_proven
        assert result.closing.attempt_index==failures+1
        assert not result.fallbacks
        assert [p.order_type for p in client.submissions]==['LIMIT','MARKET','LIMIT','MARKET']
    else:
        assert result.closing is None
        assert len(result.fallbacks)==2
        assert [p.order_type for p in client.submissions]==['LIMIT','MARKET','MARKET','MARKET']
    rows=[json.loads(x) for x in (tmp_path/'cycle/cycle.jsonl').read_text().splitlines()]
    assert len([r for r in rows if r['event']=='CLOSING_PREPARATION_RETRY'])==min(failures,2)


@pytest.mark.asyncio
@pytest.mark.parametrize('mode',['found','missing','repeated','foreign','duplicate','active'])
async def test_old_order_uses_bounded_inactive_history_without_assuming_absence_is_terminal(tmp_path,monkeypatch,mode):
    config=cycle_config(tmp_path/'cycle')
    client=recovery.RecoveryReadClient.__new__(recovery.RecoveryReadClient)
    client.config=config;client._clock=lambda:1000
    async def token(index):return 'synthetic-auth-canary'
    client._authorization=token
    calls=[]
    order={'account_index':11,'market_id':7,'order_id':'old','client_order_index':777,
           'is_ask':True,'order_type':'LIMIT','time_in_force':'POST_ONLY','reduce_only':False,
           'initial_quantity':'.2','remaining_quantity':'.2','filled_quantity':'0','price':'100.1','status':'canceled'}
    class HTTP:
        def __init__(self,*a,**k):pass
        async def get(self,path,**kwargs):
            assert kwargs['authorization']=='synthetic-auth-canary'
            calls.append(path)
            if path.endswith('accountOrders'):return {'code':200,'orders':[]}
            if len(calls)==2:return {'code':200,'orders':[],'next_cursor':'page2'}
            if mode=='missing':return {'code':200,'orders':[]}
            if mode=='repeated':return {'code':200,'orders':[],'next_cursor':'page2'}
            value=dict(order)
            if mode=='foreign':value['market_id']=99
            if mode=='active':value['status']='open'
            return {'code':200,'orders':[value]* (2 if mode=='duplicate' else 1)}
        async def aclose(self):pass
    monkeypatch.setattr(recovery,'PlainAioHttp',HTTP)
    plan=OrderPlan(11,7,'SELL',Decimal('.2'),20,Decimal('100.1'),1001,'LIMIT','POST_ONLY',False,1300000,777)
    if mode in {'repeated','foreign','duplicate'}:
        with pytest.raises(ValueError):await recovery.lookup_previous_order(client,config,plan)
    else:
        result=await recovery.lookup_previous_order(client,config,plan)
        if mode=='missing':assert result is None
        else:assert recovery.terminal_matches(result,{'plan':plan})==(mode=='found')
    assert calls==['api/v1/accountOrders','api/v1/accountInactiveOrders','api/v1/accountInactiveOrders']


@pytest.mark.asyncio
async def test_local_close_entry_point_confirmation_and_shared_lock(tmp_path,monkeypatch,capsys):
    from risex_spread_shadow.hood_handoff import cli
    from risex_spread_shadow.hood_handoff.operator_control import exclusive_lock
    from test_hood_handoff_random_cycle import _write_simple_launcher_fixture
    import time
    config,operator,evidence=_write_simple_launcher_fixture(tmp_path)
    clock=AdvancingClock();clock.value=time.time()
    created=[]
    class Client(RecoveryClient):
        def __init__(self,*args,**kwargs):
            super().__init__(clock,'.2','-.2');created.append(self)
        async def aclose(self):pass
    monkeypatch.setattr(cli,'LighterSdkClient',Client)
    monkeypatch.setattr(cli,'_validate_simple_sdk',lambda:None)
    args=cli._parser().parse_args(['close-positions','--config',str(config),'--market-evidence',str(evidence)])
    monkeypatch.setattr('builtins.input',lambda _: 'C')
    assert await cli._run(args)==0
    assert not created and not list(operator.glob('close-*'))
    monkeypatch.setattr('builtins.input',lambda _: '')
    with exclusive_lock(operator/'.operator-launch.lock'):
        with pytest.raises(RuntimeError,match='active'):await cli._run(args)
    assert not created
    assert await cli._run(args)==0
    assert len(created)==1 and len(created[0].submissions)==2
    assert len(list(operator.glob('close-*/close.jsonl')))==1
    assert '✅ Позиции закрыты' in capsys.readouterr().out


@pytest.mark.asyncio
async def test_recovery_disk_failure_prevents_new_launch(tmp_path,monkeypatch):
    calls=[]
    async def launch():calls.append(True)
    async def check(**kwargs):return {'status':'READY','at':1000}
    c=setup(tmp_path,launch);c.recovery=check
    original=c.store.save
    saved=[]
    def fail_after_offset():
        saved.append(True)
        if len(saved)>1:raise OSError('disk full')
        original()
    monkeypatch.setattr(c.store,'save',fail_after_offset)
    await c.handle(update());await c.task
    assert not calls
