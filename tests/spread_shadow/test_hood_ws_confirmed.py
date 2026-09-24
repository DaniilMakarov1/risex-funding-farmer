import asyncio
import json
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
import time

import pytest

from risex_spread_shadow.hood_handoff import run_random_cycle, Outcome, Direction, load_saved_cycle_report, ContractError
from test_hood_handoff_random_cycle import CycleClient, AdvancingClock, FixedRng, cycle_config, book


class WsCycleClient(CycleClient):
    def __init__(self, clock, adverse=None, fail_closing=False):
        super().__init__(clock)
        self.adverse=adverse;self.fail_closing=fail_closing;self.calls=[];self.prepared=[]
        self.in_phase_closing=False
    def bad(self):return self.adverse if self.in_phase_closing==self.fail_closing else None
    async def prepare_order(self,plan):
        token={'plan':plan,'ready':True};self.prepared.append(token);return token
    async def submit_prepared_order(self,plan,prepared,*,deadline=None):
        assert prepared['ready'] and prepared['plan']==plan
        prepared['ready']=False
        return await self.submit_order(plan)
    async def invalidate_prepared_order(self,token):token['ready']=False
    async def submit_order(self,plan):
        self.calls.append(('send',plan.order_type,plan.account_index,plan.reduce_only))
        if plan.order_type=='LIMIT':self.in_phase_closing=plan.reduce_only
        return await super().submit_order(plan)
    async def account_snapshot(self,*args):
        self.calls.append(('account',));return await super().account_snapshot(*args)
    async def order_book(self,*args):
        self.calls.append(('book',));return await super().order_book(*args)
    async def lookup_order(self,*args,**kwargs):
        self.calls.append(('rest_order',))
        value = await super().lookup_order(*args, **kwargs)
        # Production REST stamps the freshly read observation, not order creation time.
        return None if value is None else replace(value, observed_at=self.clock.now())
    def begin_ws_admission(self):
        if self.adverse=='no_stream':raise ContractError('no stream')
        return object()
    async def wait_order_observation(self,account,market,client,order_id,timeout,*,terminal_only):
        self.calls.append(('ws_wait',))
        if not terminal_only and self.bad()=='missing':return None
        if not terminal_only and self.bad()=='timeout':raise asyncio.TimeoutError()
        value=next((v for v in self.orders.values() if v.account_index==account and v.client_order_index==client),None)
        return value
    def ws_admission_view(self,anchor,plan,order_id):
        if self.bad() in ('disconnect','stale','unexpected'):
            raise ContractError(self.bad())
        value=self.orders[(plan.account_index,order_id)]
        if self.bad()=='old_event':value=replace(value,observed_at=value.observed_at-100)
        b=self._book_with_source_level(book(self.clock.now()))
        if self.bad()=='same_price':
            side='asks' if plan.side=='SELL' else 'bids';levels=getattr(b,side)
            b=replace(b,**{side:(replace(levels[0],quantity=levels[0].quantity+Decimal('.01')),)+levels[1:]})
        return value,b


@pytest.mark.asyncio
@pytest.mark.parametrize('direction',[Direction.LONG,Direction.SHORT])
async def test_full_ws_cycle_has_no_rest_between_limit_and_market(tmp_path,direction):
    clock=AdvancingClock();client=WsCycleClient(clock)
    cfg=cycle_config(tmp_path/'cycle',direction=direction,receiver_admission='ws_confirmed')
    result=await run_random_cycle(cfg,client,clock=clock,rng=FixedRng(20,20))
    assert result.outcome is Outcome.SUCCESS, result.reason
    assert result.inventory=='CONFIRMED_FLAT'
    for i,c in enumerate(client.calls):
        if c[:2]==('send','LIMIT'):
            j=next(j for j in range(i+1,len(client.calls)) if client.calls[j][0]=='send')
            assert client.calls[j][1]=='MARKET'
            assert not any(v[0] in {'account','book','rest_order'} for v in client.calls[i+1:j])
    assert result.opening.priority_guard['status']=='WS_CONFIRMED'
    assert result.closing.priority_guard['priority_proof_admitted'] is False
    report=load_saved_cycle_report(tmp_path/'cycle')
    assert report['status']=='COMPLETE',report['issues']
    assert report['binding']['receiver_admission']=='ws_confirmed'


@pytest.mark.asyncio
@pytest.mark.parametrize('adverse',['missing','timeout','disconnect','stale','unexpected','old_event','same_price'])
@pytest.mark.parametrize('closing',[False,True])
async def test_failed_ws_admission_reconciles_and_closes_without_market(tmp_path,adverse,closing):
    clock=AdvancingClock();client=WsCycleClient(clock,adverse,closing)
    result=await run_random_cycle(cycle_config(tmp_path/'cycle',receiver_admission='ws_confirmed'),client,clock=clock,rng=FixedRng(20,20))
    phase=result.closing if closing else result.opening
    assert phase is not None
    assert not phase.receiver.dispatched
    assert phase.outcome is Outcome.PARTIAL, (result.reason,phase.unknown_reasons)
    assert result.inventory=='CONFIRMED_FLAT',result.reason
    assert client.source_position==client.receiver_position==0


@pytest.mark.asyncio
async def test_no_ws_before_limit_never_exposes_source(tmp_path):
    clock=AdvancingClock();client=WsCycleClient(clock,'no_stream')
    result=await run_random_cycle(cycle_config(tmp_path/'cycle',receiver_admission='ws_confirmed'),client,clock=clock,rng=FixedRng(20,20))
    assert not client.submissions
    assert result.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED, result.reason
    assert client.source_position==client.receiver_position==0


@pytest.mark.parametrize('value',['',True,None,'fast',1])
def test_unknown_admission_mode_rejected(tmp_path,value):
    with pytest.raises(ContractError):cycle_config(tmp_path/'cycle',receiver_admission=value)


async def sdk_stream_client():
    from test_hood_handoff_sdk_interface import _constant_nonce_client
    from test_hood_ws_reads import identity, put, depth
    from risex_spread_shadow.hood_handoff.stream_state import ReadStreamState
    c=_constant_nonce_client(prefix='ws-confirmed');s=ReadStreamState(identity())
    c._read_stream_state=s;c._read_stream_task=SimpleNamespace(done=lambda:False)
    await s.connected_now();await put(s,depth(bids=[{'price':'100','size':'.20'}]))
    for a in (11,22):
        await put(s,{'type':'subscribed/account_all_orders','channel':f'account_all_orders:{a}','orders':{'1':[]}})
    return c,s


@pytest.mark.asyncio
@pytest.mark.parametrize('adverse',[None,'disconnect','reconnect','gap','stale_book','stale_order','other_order','invalid_order','malformed','foreign_anchor'])
async def test_actual_sdk_ws_admission_view_is_local_and_rejects_uncertainty(adverse):
    from test_hood_ws_reads import put, put_order, order, depth
    from risex_spread_shadow.hood_handoff import OrderPlan
    c,s=await sdk_stream_client();token=c.begin_ws_admission()
    plan=OrderPlan(account_index=11,market_id=1,side='BUY',quantity=Decimal('.20'),price=Decimal('100'),
                   order_type='LIMIT',time_in_force='POST_ONLY',reduce_only=False,client_order_index=77,
                   quantity_int=20,price_int=1000,order_expiry_ms=9999999999999)
    await put_order(s,order())
    if adverse=='disconnect':await s.disconnected()
    if adverse=='reconnect':await s.disconnected();await s.connected_now()
    if adverse=='gap':await put(s,depth(3,2,False))
    if adverse=='stale_book':s.reads.book_at=time.monotonic()-1
    if adverse=='stale_order':s.reads.orders[(11,77)]=(s.reads.orders[(11,77)][0],time.monotonic()-1)
    if adverse=='other_order':await put_order(s,dict(order(),client_order_index=78,order_id='124'))
    if adverse=='invalid_order':await put_order(s,dict(order(),filled_base_amount='bad'))
    if adverse=='malformed':s.observer.malformed+=1
    if adverse=='foreign_anchor':token=dict(token,owner=object())
    if adverse=='stale_order':assert c.ws_admission_view(token,plan,'123')[0] is None
    elif adverse:
        with pytest.raises(ContractError):c.ws_admission_view(token,plan,'123')
    else:
        value,b=c.ws_admission_view(token,plan,'123')
        assert value.order_id=='123' and value.filled_quantity==0
        assert b.bids[0].quantity==Decimal('.20')


@pytest.mark.asyncio
@pytest.mark.parametrize('closing',[False,True])
async def test_external_source_fill_in_ws_prevents_receiver_and_recovers(tmp_path,closing):
    from test_hood_handoff_random_cycle import GuardCancellationFillClient
    class External(WsCycleClient):
        async def wait_order_observation(self,*args,terminal_only,**kwargs):
            if not terminal_only and self.in_phase_closing==closing:
                current=self.orders[(self.source_account_index,self.latest_order[self.source_account_index])]
                self.closing=closing
                await GuardCancellationFillClient.cancel_order(self,current.account_index,current.market_id,current.order_id)
            return await super().wait_order_observation(*args,terminal_only=terminal_only,**kwargs)
    clock=AdvancingClock();client=External(clock)
    result=await run_random_cycle(cycle_config(tmp_path/'cycle',receiver_admission='ws_confirmed'),client,clock=clock,rng=FixedRng(20,20))
    phase=result.closing if closing else result.opening
    assert not phase.receiver.dispatched
    assert phase.source.filled_quantity>0
    assert result.inventory=='CONFIRMED_FLAT',result.reason
    assert len(result.fallbacks)==1


@pytest.mark.asyncio
@pytest.mark.parametrize('adverse',['partial','late_fill','expired_accounts','ambiguous_source','ambiguous_receiver'])
async def test_ws_failure_boundaries_never_duplicate_or_leave_known_residual(tmp_path,adverse):
    from risex_spread_shadow.hood_handoff import TradeReceipt
    class Client(WsCycleClient):
        async def submit_order(self,plan):
            if adverse=='ambiguous_source' and plan.order_type=='LIMIT':
                await super().submit_order(plan)
                raise TimeoutError('uncertain source write')
            if adverse=='ambiguous_receiver' and plan.order_type=='MARKET':
                await super().submit_order(plan)
                raise TimeoutError('uncertain receiver write')
            return await super().submit_order(plan)
        def ws_admission_view(self,anchor,plan,order_id):
            if adverse=='expired_accounts':self.clock.value+=11
            if adverse in ('partial','late_fill'):
                current=self.orders[(plan.account_index,order_id)]
                quantity=current.initial_quantity/(2 if adverse=='partial' else 1)
                self._replace_order(current,status='open' if adverse=='partial' else 'filled',filled_quantity=quantity,remaining_quantity=current.initial_quantity-quantity)
                self.source_position+=quantity if current.side=='BUY' else -quantity
                self.trades[order_id]=(TradeReceipt('external',plan.account_index,7,order_id,current.side,quantity,current.price,None,999,self.clock.now(),counterparty_order_id='foreign',client_order_index=current.client_order_index),)
            return super().ws_admission_view(anchor,plan,order_id)
    clock=AdvancingClock();c=Client(clock)
    result=await run_random_cycle(cycle_config(tmp_path/'cycle',receiver_admission='ws_confirmed'),c,clock=clock,rng=FixedRng(20,20))
    if adverse.startswith('ambiguous'):
        assert result.outcome is Outcome.UNKNOWN
        assert len([p for p in c.submissions if p.order_type=='LIMIT'])==1
        assert len([p for p in c.submissions if p.order_type=='MARKET'])==(adverse=='ambiguous_receiver')
        assert not result.fallbacks
    else:
        assert not result.opening.receiver.dispatched
        assert result.inventory=='CONFIRMED_FLAT',result.reason
        assert c.source_position==c.receiver_position==0


@pytest.mark.asyncio
async def test_ws_engine_keeps_terminal_and_telegram_mode_visible(tmp_path):
    from risex_spread_shadow.hood_handoff.telegram_messages import saved_message
    clock=AdvancingClock();c=WsCycleClient(clock)
    result=await run_random_cycle(cycle_config(tmp_path/'cycle',receiver_admission='ws_confirmed'),c,clock=clock,rng=FixedRng(20,20))
    report=load_saved_cycle_report(tmp_path/'cycle')
    assert 'WS-подтверждения' in saved_message('cycle-001',report)
    from test_hood_telegram_control import setup
    p=(tmp_path/'cycle').rename(tmp_path/'cycle-001')
    controller=setup(tmp_path,None);controller.store.data['active']={'action':'run','before':[]}
    controller.finish()
    assert controller.store.data['active'] is None
    assert controller.store.data['last']['status']=='FINISHED'


@pytest.mark.asyncio
async def test_unavailable_fresh_cancel_observation_does_not_claim_flat(tmp_path):
    class StaleRest(WsCycleClient):
        def ws_admission_view(self, *args):
            self.clock.value += 11
            return super().ws_admission_view(*args)

        async def lookup_order(self, *args, **kwargs):
            return await CycleClient.lookup_order(self, *args, **kwargs)

    clock = AdvancingClock()
    client = StaleRest(clock)
    result = await run_random_cycle(
        cycle_config(tmp_path / 'cycle', receiver_admission='ws_confirmed'),
        client, clock=clock, rng=FixedRng(20, 20),
    )
    assert not result.opening.receiver.dispatched
    assert result.inventory == 'UNKNOWN'
    assert any(order.active for order in client.orders.values())


def test_cli_parses_explicit_admission_without_changing_default_binding(tmp_path):
    from dataclasses import asdict
    from risex_spread_shadow.hood_handoff.cli import _random_cycle_config
    base = cycle_config(tmp_path / 'cycle')
    assert 'receiver_admission' not in base.binding()
    raw = {key: value for key, value in asdict(base).items() if value is not None}
    raw["direction"] = base.direction.value
    raw['receiver_admission'] = 'ws_confirmed'
    parsed = _random_cycle_config(raw, execute=True, plan_reviewed=True)
    assert parsed.receiver_admission == 'ws_confirmed'
    assert parsed.binding()['receiver_admission'] == 'ws_confirmed'


@pytest.mark.asyncio
async def test_ws_receiver_send_keeps_short_stream_deadline(tmp_path):
    class Deadline(WsCycleClient):
        async def submit_prepared_order(self, plan, prepared, *, deadline=None):
            if plan.order_type == 'MARKET':
                assert 0 < deadline - time.monotonic() <= .5
            return await super().submit_prepared_order(plan, prepared, deadline=deadline)

    clock = AdvancingClock()
    result = await run_random_cycle(
        cycle_config(tmp_path / 'cycle', receiver_admission='ws_confirmed'),
        Deadline(clock), clock=clock, rng=FixedRng(20, 20),
    )
    assert result.outcome is Outcome.SUCCESS, result.reason
