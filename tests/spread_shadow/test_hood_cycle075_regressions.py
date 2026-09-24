import asyncio
import time
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import pytest

from risex_spread_shadow.hood_handoff import HandoffEngine, Direction, Outcome, ContractError
from test_hood_handoff_random_cycle import (
    GuardCancellationFillClient, AdvancingClock, FixedRng, cycle_config, run_random_cycle,
)
from test_hood_handoff_sdk_interface import _constant_nonce_client, FakeSigner
from test_hood_ws_reads import identity, order, put_order
from risex_spread_shadow.hood_handoff.stream_state import ReadStreamState
from risex_spread_shadow.hood_handoff.contracts import OrderSnapshot


class MissingActiveFillClient(GuardCancellationFillClient):
    async def account_snapshot(self, account_index, market_id):
        value = await super().account_snapshot(account_index, market_id)
        if account_index == self.source_account_index and value.active_orders:
            current = value.active_orders[0]
            if current.reduce_only == self.closing:
                return replace(value, active_orders=())
        return value


@pytest.mark.asyncio
@pytest.mark.parametrize('closing', [False, True])
@pytest.mark.parametrize('direction', [Direction.LONG, Direction.SHORT])
async def test_cycle075_empty_early_account_then_external_fill_recovers(tmp_path, closing, direction):
    clock = AdvancingClock()
    client = MissingActiveFillClient(clock, closing=closing)
    result = await run_random_cycle(cycle_config(tmp_path/'case', direction=direction), client,
                                   clock=clock, rng=FixedRng(20,20))
    phase = result.closing if closing else result.opening
    assert 'source active order absent in pre-receiver account snapshot' in phase.unknown_reasons
    assert phase.outcome is Outcome.PARTIAL
    assert phase.receiver.dispatched is False
    assert result.inventory == 'CONFIRMED_FLAT'
    assert client.source_position == client.receiver_position == 0
    assert len(result.fallbacks) == 1
    assert all(p.reduce_only for p in client.fallback_plans)


@pytest.mark.asyncio
@pytest.mark.parametrize('adverse', ['history', 'cancel_unknown'])
async def test_missing_active_does_not_remove_ambiguous_execution_barrier(tmp_path, adverse):
    clock=AdvancingClock(); client=MissingActiveFillClient(clock, adverse=adverse)
    result=await run_random_cycle(cycle_config(tmp_path/'case'),client,clock=clock,rng=FixedRng(20,20))
    assert result.outcome is Outcome.UNKNOWN
    assert not result.fallbacks
    assert client.source_position != 0
    assert len(client.submissions)==1


def snapshot():
    value=order(); value['side']='BUY'; value['observed_at']=time.time()
    return OrderSnapshot.from_mapping(value)


@pytest.mark.asyncio
@pytest.mark.parametrize('winner', ['ws','rest','ws_invalid','ws_error','rest_error','cancel'])
async def test_visibility_race_returns_exact_winner_and_drains_other_task(winner):
    started=asyncio.Event(); release=asyncio.Event(); stopped=[]; value=snapshot()
    async def lookup(*args,**kwargs):
        started.set()
        try:
            if winner in {'rest','ws_invalid','ws_error'}:
                await asyncio.sleep(.01);return value
            if winner=='rest_error':raise TimeoutError('read failed')
            await release.wait();return value
        finally:stopped.append('rest')
    async def wait(*args,**kwargs):
        await started.wait()
        try:
            if winner=='ws_error':raise RuntimeError('stream disconnected')
            if winner in {'ws','rest_error'}:return value
            if winner=='ws_invalid':return replace(value,account_index=999)
            await release.wait();return value
        finally:stopped.append('ws')
    engine=HandoffEngine(SimpleNamespace(wait_order_observation=wait))
    engine._lookup_order=lookup
    engine._configured_freshness=10
    # Use real order-plan matching rather than bypassing identity checks.
    from risex_spread_shadow.hood_handoff import OrderPlan
    plan=OrderPlan(account_index=11,market_id=1,side='BUY',quantity=Decimal('.20'),price=Decimal('100'),
                   order_type='LIMIT',time_in_force='POST_ONLY',reduce_only=False,client_order_index=77,
                   quantity_int=20,price_int=1000,order_expiry_ms=9999999999999)
    task=asyncio.create_task(engine._race_order_observation(plan,'123',require_terminal=False))
    if winner=='cancel':
        await started.wait();task.cancel()
        with pytest.raises(asyncio.CancelledError):await task
    else:
        assert await asyncio.wait_for(task,.5)==value
    assert set(stopped)=={'rest','ws'}


@pytest.mark.asyncio
async def test_stream_wait_wakes_on_exact_event_and_disconnect():
    s=ReadStreamState(identity());await s.connected_now()
    task=asyncio.create_task(s.wait_order_observation(11,1,77,'123',.5,terminal_only=False))
    await asyncio.sleep(0);await put_order(s,order())
    assert (await task).order_id=='123'
    terminal=asyncio.create_task(s.wait_order_observation(11,1,77,'123',.5,terminal_only=True))
    await asyncio.sleep(0);await s.disconnected()
    assert await terminal is None


@pytest.mark.asyncio
async def test_reserved_cancel_nonce_is_consumed_without_second_network_read():
    signer=FakeSigner(account_index=11);c=_constant_nonce_client(prefix='early-cancel',signer=signer)
    token=await c.reserve_cancel_nonce(11)
    async def no_read(*args,**kwargs):raise AssertionError('late nonce request')
    c._next_nonce=no_read
    prepared=await c.prepare_cancel_order(11,c.config.market_id,'123',reserved_nonce=token)
    assert prepared.diagnostic_timings['cancel_nonce_reserved_before_visibility']==1
    assert token._state=='CONSUMED'
    with pytest.raises(ContractError):
        await c.prepare_cancel_order(11,c.config.market_id,'123',reserved_nonce=token)
    receipt=await c.submit_prepared_cancel_order(11,c.config.market_id,'123',prepared,deadline=time.monotonic()+2)
    assert receipt.accepted
    again=await c.submit_prepared_cancel_order(11,c.config.market_id,'123',prepared,deadline=time.monotonic()+2)
    assert not again.accepted and again.diagnostic_timings['cancel_not_sent']==1


@pytest.mark.asyncio
@pytest.mark.parametrize('bad', ['foreign','expired','released','wrong_account'])
async def test_cancel_nonce_rejects_invalid_reservation(bad):
    c=_constant_nonce_client(prefix='bad-cancel',signer=FakeSigner(account_index=11))
    other=_constant_nonce_client(prefix='other',signer=FakeSigner(account_index=11))
    token=await (other if bad=='foreign' else c).reserve_cancel_nonce(22 if bad=='wrong_account' else 11)
    if bad=='expired':object.__setattr__(token,'deadline',time.monotonic()-1)
    if bad=='released':await c.invalidate_reserved_nonce(token)
    with pytest.raises(ContractError):await c.prepare_cancel_order(11,c.config.market_id,'123',reserved_nonce=token)


@pytest.mark.asyncio
async def test_nonce_reservation_starts_before_source_visibility_and_is_released(tmp_path):
    from test_hood_handoff_paired_opening import PairedClient, config, Clock
    from risex_spread_shadow.hood_handoff import run_handoff, DepthLevel
    class Client(PairedClient):
        def __init__(self):
            super().__init__(); self.started=asyncio.Event();self.ready=asyncio.Event()
            self.token=object();self.prepared=object();self.released=[];self.sent=0
        async def reserve_cancel_nonce(self,account):
            self.started.set();await asyncio.sleep(.01);self.ready.set();return self.token
        async def lookup_order(self,*args,**kwargs):
            if self.source_order is not None:
                await asyncio.wait_for(self.started.wait(),.2)
                await asyncio.wait_for(self.ready.wait(),.2)
            return await super().lookup_order(*args,**kwargs)
        async def prepare_cancel_order(self,*args,reserved_nonce=None):
            assert self.ready.is_set() and reserved_nonce is self.token
            return self.prepared
        async def invalidate_reserved_nonce(self,token):self.released.append(token)
        async def invalidate_prepared_order(self,token):assert token is self.prepared
        async def submit_prepared_cancel_order(self,a,m,o,p,*,deadline):
            assert p is self.prepared;self.sent+=1;return await super().cancel_order(a,m,o)
        async def order_book(self,m):
            value=await super().order_book(m)
            if self.source_order is not None:
                await asyncio.sleep(.03)
                value=replace(value,bids=(DepthLevel(Decimal('100.1'),Decimal('1'),'outside',999),)+value.bids)
            return value
    c=Client();await run_handoff(config(tmp_path/'early.jsonl'),c,clock=Clock())
    assert c.sent==1 and c.released and all(x is c.token for x in c.released)
    import json
    event=next(json.loads(line) for line in (tmp_path/'early.jsonl').open() if json.loads(line)['event']=='CANCEL_DISPATCH_INTENT')
    assert event['payload']['prepared_before_decision'] is True


@pytest.mark.asyncio
async def test_failed_cancel_signing_releases_nonce_before_fallback():
    released=[]; token=object()
    async def reserve(account):return token
    async def fail(*args,**kwargs):raise ValueError('not signed')
    async def release(value):released.append(value)
    client=SimpleNamespace(reserve_cancel_nonce=reserve,prepare_cancel_order=fail,
                           submit_prepared_cancel_order=fail,invalidate_reserved_nonce=release)
    engine=HandoffEngine(client);value=snapshot()
    from risex_spread_shadow.hood_handoff import OrderPlan
    plan=OrderPlan(account_index=11,market_id=1,side='BUY',quantity=Decimal('.20'),price=Decimal('100'),
                   order_type='LIMIT',time_in_force='POST_ONLY',reduce_only=False,client_order_index=77,
                   quantity_int=20,price_int=1000,order_expiry_ms=9999999999999)
    engine._start_cancel_nonce(plan);engine._start_cancel_preparation(plan,value)
    with pytest.raises(ValueError):await engine._cancel_preparation_task
    assert released==[token]
    await engine._finish_cancel_preparation()
    assert engine._cancel_nonce_task is None


@pytest.mark.asyncio
async def test_stream_terminal_wait_ignores_active_and_other_identity():
    s=ReadStreamState(identity());await s.connected_now()
    task=asyncio.create_task(s.wait_order_observation(11,1,77,'123',.3,terminal_only=True))
    await put_order(s,order());await asyncio.sleep(0)
    assert not task.done()
    await put_order(s,dict(order('filled'),client_order_index=78,order_id='124'));await asyncio.sleep(0)
    assert not task.done()
    await put_order(s,order('filled'))
    assert (await task).filled_quantity==Decimal('.20')


@pytest.mark.asyncio
async def test_completed_rest_identity_conflict_is_not_hidden_by_ws():
    value=snapshot();wrong=replace(value,account_index=999)
    async def lookup(*args,**kwargs):return wrong
    async def wait(*args,**kwargs):return value
    engine=HandoffEngine(SimpleNamespace(wait_order_observation=wait));engine._lookup_order=lookup
    from risex_spread_shadow.hood_handoff import OrderPlan
    plan=OrderPlan(account_index=11,market_id=1,side='BUY',quantity=Decimal('.20'),price=Decimal('100'),
                   order_type='LIMIT',time_in_force='POST_ONLY',reduce_only=False,client_order_index=77,
                   quantity_int=20,price_int=1000,order_expiry_ms=9999999999999)
    assert await engine._race_order_observation(plan,'123',require_terminal=False)==wrong


@pytest.mark.asyncio
async def test_early_cancel_reservation_failure_uses_existing_preparation():
    calls=[]
    async def fail(account):raise TimeoutError('nonce read failed')
    async def prepare(*args,**kwargs):calls.append(kwargs);return 'prepared'
    async def invalidate(value):calls.append(value)
    client=SimpleNamespace(reserve_cancel_nonce=fail,prepare_cancel_order=prepare,
                           submit_prepared_cancel_order=prepare,invalidate_prepared_order=invalidate)
    engine=HandoffEngine(client)
    from risex_spread_shadow.hood_handoff import OrderPlan
    plan=OrderPlan(account_index=11,market_id=1,side='BUY',quantity=Decimal('.20'),price=Decimal('100'),
                   order_type='LIMIT',time_in_force='POST_ONLY',reduce_only=False,client_order_index=77,
                   quantity_int=20,price_int=1000,order_expiry_ms=9999999999999)
    engine._start_cancel_nonce(plan);engine._start_cancel_preparation(plan,snapshot())
    assert await engine._cancel_preparation_task=='prepared'
    await engine._finish_cancel_preparation()
    assert calls==[{},'prepared']
