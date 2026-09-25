import asyncio
import json
from dataclasses import replace
from decimal import Decimal
import pytest
from aiohttp import web
from risex_spread_shadow.hood_handoff import Direction, Outcome
from risex_spread_shadow.hood_handoff.sdk import PlainAioHttp
from risex_spread_shadow.hood_handoff.read_errors import ReadRateLimited, read_rate_limit_delay
from test_hood_account_rate_limit import limited
from test_hood_ticks_ack import AckClient
from test_hood_handoff_random_cycle import AdvancingClock, FixedRng, cycle_config, run_random_cycle

class LimitedAfterCancel(AckClient):
    def __init__(self, clock, kind, closing=False, target=11, count=1, after=None, conflict=False, status=429):
        super().__init__(clock,'better',closing)
        self.kind=kind;self.target=target;self.remaining=count;self.after=after
        self.conflict=conflict;self.status=status;self.failure_times=[]
    def fail(self,kind,a):
        if self.cancellations and a==self.target and self.kind in (kind,'all') and self.remaining:
            self.remaining-=1;self.failure_times.append((kind,self.clock.now()))
            if kind=='order':raise ReadRateLimited(float(self.after or 0))
            raise limited(self.status,self.after)
    async def account_snapshot(self,a,m):
        self.fail('account',a)
        value=await super().account_snapshot(a,m)
        if self.conflict and self.failure_times and a==self.target:
            return replace(value,source_identity='foreign')
        return value
    async def list_trades(self,a,m,**kw):
        self.fail('trades',a)
        return await super().list_trades(a,m,**kw)
    async def lookup_order(self,a,m,**kw):
        self.fail('order',a)
        value=await super().lookup_order(a,m,**kw)
        return replace(value,observed_at=self.clock.now()) if value else value

@pytest.mark.asyncio
@pytest.mark.parametrize('direction',list(Direction))
@pytest.mark.parametrize('closing',[False,True])
@pytest.mark.parametrize('kind,target',[('account',11),('account',22),('trades',11),('order',11)])
async def test_429_after_cancel_recovers_without_identity_barrier_or_duplicate_mutation(tmp_path,kind,target,closing,direction):
    clock=AdvancingClock();client=LimitedAfterCancel(clock,kind,closing,target)
    result=await run_random_cycle(cycle_config(tmp_path/'cycle',direction=direction,receiver_admission='ack',max_poll_count=20,reconcile_timeout_seconds=15),client,clock=clock,rng=FixedRng(20,20))
    assert result.inventory=='CONFIRMED_FLAT',result.reason
    assert client.remaining==0 and 1 in clock.sleeps
    assert len(client.cancellations)==len(set(client.cancellations))
    assert len({p.client_order_index for p in client.submissions})==len(client.submissions)
    journals=''.join(p.read_text() for p in (tmp_path/'cycle').glob('*.jsonl'))
    assert 'secret-' not in journals and 'identity failure barrier' not in journals
    assert 'RECONCILIATION_RATE_LIMIT' in journals

@pytest.mark.asyncio
@pytest.mark.parametrize('after,count,expected',[('3',1,[3]),(None,3,[1,2,4])])
async def test_reconcile_cooldown_respects_headers_and_grows(tmp_path,after,count,expected):
    clock=AdvancingClock();client=LimitedAfterCancel(clock,'account',count=count,after=after)
    result=await run_random_cycle(cycle_config(tmp_path/'cycle',receiver_admission='ack',max_poll_count=20,reconcile_timeout_seconds=15),client,clock=clock,rng=FixedRng(20,20))
    assert result.inventory=='CONFIRMED_FLAT',result.reason
    assert clock.sleeps[:len(expected)]==expected

@pytest.mark.asyncio
@pytest.mark.parametrize('status,conflict,after',[(401,False,None),(429,True,None),(429,False,'60')])
async def test_auth_identity_or_excess_cooldown_remains_unknown(tmp_path,status,conflict,after):
    clock=AdvancingClock();client=LimitedAfterCancel(clock,'account',status=status,conflict=conflict,after=after)
    result=await run_random_cycle(cycle_config(tmp_path/'cycle',receiver_admission='ack',max_poll_count=10,reconcile_timeout_seconds=4),client,clock=clock,rng=FixedRng(20,20))
    assert result.inventory=='UNKNOWN'
    assert len(client.cancellations)==len(set(client.cancellations))
    if after:assert 60 not in clock.sleeps

@pytest.mark.asyncio
async def test_plain_http_429_is_typed_and_not_retried_by_transport():
    calls=[]
    async def endpoint(request):
        calls.append(True);return web.Response(status=429,text='SECRET non-JSON',headers={'Retry-After':'2'})
    app=web.Application();app.router.add_get('/x',endpoint);runner=web.AppRunner(app);await runner.setup()
    site=web.TCPSite(runner,'127.0.0.1',0);await site.start();port=site._server.sockets[0].getsockname()[1]
    http=PlainAioHttp(f'http://127.0.0.1:{port}',timeout_seconds=1)
    try:
        with pytest.raises(ReadRateLimited) as e:await http.get('x',params={},authorization='synthetic')
        assert read_rate_limit_delay(e.value)==2 and 'SECRET' not in str(e.value) and calls==[True]
    finally:await http.aclose();await runner.cleanup()

@pytest.mark.asyncio
@pytest.mark.parametrize('closing',[False,True])
@pytest.mark.parametrize('fraction',['0.5','1'])
async def test_rate_limit_with_external_fill_retains_receipts_and_closes_residual(tmp_path,closing,fraction):
    from test_hood_delayed_cleanup import DelayedCleanup
    class Client(DelayedCleanup):
        failed=False
        async def account_snapshot(self,a,m):
            if (self.cancellations or self.injected) and not self.failed:
                self.failed=True;raise limited()
            return await super().account_snapshot(a,m)
    clock=AdvancingClock();client=Client(clock,closing,fraction=Decimal(fraction))
    result=await run_random_cycle(cycle_config(tmp_path/'cycle',receiver_admission='ack',max_poll_count=20,reconcile_timeout_seconds=15),client,clock=clock,rng=FixedRng(20,20))
    assert result.inventory=='CONFIRMED_FLAT',result.reason
    assert client.failed and client.injected
    phase=result.closing if closing else result.opening
    assert phase.source.filled_quantity==Decimal('.20')*Decimal(fraction)
    assert phase.source.history_complete and not phase.source.unknown_reasons
    assert len(client.cancellations)==len(set(client.cancellations))

@pytest.mark.asyncio
async def test_cancel_during_reconciliation_cooldown_never_replays_mutation(tmp_path):
    class CancelClock(AdvancingClock):
        async def sleep(self,seconds):
            if seconds>=1:raise asyncio.CancelledError()
            await super().sleep(seconds)
    clock=CancelClock();client=LimitedAfterCancel(clock,'account')
    with pytest.raises(asyncio.CancelledError):
        await run_random_cycle(cycle_config(tmp_path/'cycle',receiver_admission='ack',max_poll_count=10,reconcile_timeout_seconds=15),client,clock=clock,rng=FixedRng(20,20))
    assert len(client.cancellations)==1 and len(client.submissions)==1


def test_background_rate_limit_notice_is_once_per_attempt(tmp_path):
    from risex_spread_shadow.hood_handoff.journal import DurableJournal
    from risex_spread_shadow.hood_handoff.operator_view import read_execution_notices
    p=tmp_path/'opening.jsonl';j=DurableJournal(p,clock=lambda:1000);j.acquire_attempt()
    try:
        j.append('ATTEMPT_STARTED',{})
        for _ in range(3):j.append('RECONCILIATION_RATE_LIMIT',{'will_retry':True})
        notices=read_execution_notices(tmp_path/'cycle.jsonl')
        assert len(notices)==1 and notices[0][0]=='opening-1-limited'
        assert 'заявки не повторяются' in notices[0][1]
    finally:j.release_attempt()
