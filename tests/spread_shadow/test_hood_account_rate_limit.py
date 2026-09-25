"""Cycle-165: SDK 429 during accepted leverage readback is not an identity failure."""
import asyncio
import json
from dataclasses import replace
from decimal import Decimal

import pytest
from lighter.exceptions import ApiException
from risex_spread_shadow.hood_handoff import Outcome, ContractError, RandomCycleEngine
from risex_spread_shadow.hood_handoff.random_cycle import _account_rate_limit, _AccountIdentityFailure
from test_hood_hcr40 import LeverageClient
from test_hood_handoff_random_cycle import AdvancingClock, FixedRng, cycle_config, run_random_cycle


def limited(status=429, retry_after=None):
    error = ApiException(status=status, reason='secret-url-token')
    error.body = 'secret-body'
    error.headers = {'Authorization':'secret-key'}
    if retry_after is not None:
        error.headers['Retry-After'] = retry_after
    return error


class RateClient(LeverageClient):
    def __init__(self, clock, count=1, target=22, status=429, after=None, conflict=False):
        super().__init__(clock)
        self.remaining = count
        self.target = target
        self.status = status
        self.after = after
        self.conflict = conflict
        self.failed_times = []
    async def account_snapshot(self, account_index, market_id):
        if len(self.settings) == 2:
            if self.conflict and account_index == 11:
                return replace(await super().account_snapshot(account_index,market_id), account_index=999)
            if account_index == self.target and self.remaining:
                self.remaining -= 1
                self.failed_times.append(self.clock.now())
                raise limited(self.status, self.after)
        return await super().account_snapshot(account_index, market_id)


@pytest.mark.asyncio
@pytest.mark.parametrize('target', [11,22])
@pytest.mark.parametrize('count,after,delays', [(1,None,[1]),(3,None,[1,2,4]),(1,'3',[3])])
async def test_rate_limited_setting_readback_recovers_without_resending(tmp_path,target,count,after,delays):
    clock=AdvancingClock();client=RateClient(clock,count,target,after=after)
    result=await run_random_cycle(cycle_config(tmp_path/'cycle', max_poll_count=10,
                                 reconcile_timeout_seconds=15),client,clock=clock,rng=FixedRng(40,20))
    assert result.outcome is Outcome.SUCCESS,result.reason
    assert result.inventory=='CONFIRMED_FLAT'
    assert client.settings==[(11,7,4993,0),(22,7,5990,0)]
    assert clock.sleeps==delays+[20]
    text=(tmp_path/'cycle/cycle.jsonl').read_text()
    assert 'secret-' not in text
    retry=[json.loads(l)['payload'] for l in text.splitlines() if json.loads(l)['event']=='RECOVERY_READ_RETRY']
    assert [r['delay_seconds'] for r in retry]==delays


@pytest.mark.asyncio
@pytest.mark.parametrize('status,after,conflict', [(429,None,False),(429,'60',False),(401,None,False),(403,None,False),(500,None,False),(429,None,True)])
async def test_exhausted_or_invalid_readback_never_resends_or_opens(tmp_path,status,after,conflict):
    clock=AdvancingClock();client=RateClient(clock,99,status=status,after=after,conflict=conflict)
    result=await run_random_cycle(cycle_config(tmp_path/'cycle',max_poll_count=10,
                                 reconcile_timeout_seconds=4),client,clock=clock,rng=FixedRng(40,20))
    assert result.outcome is Outcome.UNKNOWN
    assert not client.submissions
    assert client.settings==[(11,7,4993,0),(22,7,5990,0)]
    assert 'secret-' not in (tmp_path/'cycle/cycle.jsonl').read_text()
    assert sum(clock.sleeps)<=4
    if status!=429 or after or conflict: assert not clock.sleeps
    else: assert clock.sleeps==[1,2]


@pytest.mark.parametrize('header,expected', [('2',2),('0',0),('-1',0),('nan',0),('inf',0),('bad',0)])
def test_retry_after_is_numeric_bounded_evidence(header,expected):
    value=_account_rate_limit(limited(retry_after=header),'account read')
    assert value.retry_after==expected
    assert 'secret' not in str(value)


def test_retry_after_http_date(monkeypatch):
    monkeypatch.setattr('risex_spread_shadow.hood_handoff.random_cycle.time.time',lambda:0)
    assert _account_rate_limit(limited(retry_after='Thu, 01 Jan 1970 00:00:05 GMT'),'read').retry_after==5


def test_status_shaped_non_sdk_error_is_not_retryable():
    error=RuntimeError('429');error.status=429
    assert _account_rate_limit(error,'read') is None


@pytest.mark.asyncio
async def test_normal_setting_path_has_no_new_sleep(tmp_path):
    clock=AdvancingClock();client=RateClient(clock,0)
    result=await run_random_cycle(cycle_config(tmp_path/'cycle'),client,clock=clock,rng=FixedRng(40,20))
    assert result.outcome is Outcome.SUCCESS
    assert clock.sleeps==[20]


@pytest.mark.asyncio
async def test_no_setting_mutation_replay_on_sdk_rate_limit(tmp_path):
    class Client(LeverageClient):
        async def update_leverage_fraction(self,*args):
            self.settings.append(args)
            raise limited()
    clock=AdvancingClock();client=Client(clock)
    result=await run_random_cycle(cycle_config(tmp_path/'cycle'),client,clock=clock,rng=FixedRng(40,20))
    assert result.outcome is Outcome.UNKNOWN
    assert len(client.settings)==1 and not client.submissions
    assert not clock.sleeps


@pytest.mark.asyncio
@pytest.mark.parametrize('recovery', [False,True])
async def test_parallel_rate_limits_use_longest_observed_retry_after(tmp_path,recovery):
    clock=AdvancingClock()
    class Client(LeverageClient):
        seen=set()
        async def account_snapshot(self,a,m):
            if a not in self.seen:
                self.seen.add(a)
                raise limited(retry_after='3' if a==11 else '5')
            return await super().account_snapshot(a,m)
    client=Client(clock);engine=RandomCycleEngine(client,clock=clock,rng=FixedRng(40,20))
    cfg=cycle_config(tmp_path/'cycle',max_poll_count=3,reconcile_timeout_seconds=10)
    method=engine._recovery_accounts if recovery else engine._rate_limited_accounts
    source,receiver=await method(cfg)
    assert source.signed_position==receiver.signed_position==Decimal(0)
    assert clock.sleeps==[5]
    assert not client.settings and not client.submissions


@pytest.mark.asyncio
@pytest.mark.parametrize('failure',['identity','stale','active'])
async def test_after_cooldown_revalidates_new_account_evidence(tmp_path,failure):
    clock=AdvancingClock()
    class Client(LeverageClient):
        first=True
        async def account_snapshot(self,a,m):
            if a==11 and self.first:
                self.first=False
                raise limited()
            value=await super().account_snapshot(a,m)
            if a==11:
                if failure=='identity':return replace(value,account_index=999)
                if failure=='stale':return replace(value,observed_at=clock.now()-100)
                from test_hood_cycle075_regressions import snapshot
                return replace(value,active_orders=(replace(snapshot(),market_id=7),))
            return value
    client=Client(clock);engine=RandomCycleEngine(client,clock=clock,rng=FixedRng(40,20))
    with pytest.raises(ContractError):
        await engine._rate_limited_accounts(cycle_config(tmp_path/'cycle',reconcile_timeout_seconds=5))
    assert clock.sleeps==[1]
    assert not client.settings and not client.submissions


@pytest.mark.asyncio
async def test_cancellation_during_backoff_does_not_retry(tmp_path):
    class Clock(AdvancingClock):
        async def sleep(self,seconds):
            raise asyncio.CancelledError()
    clock=Clock()
    class Client(LeverageClient):
        reads=0
        async def account_snapshot(self,*args):
            self.reads+=1
            raise limited()
    client=Client(clock);engine=RandomCycleEngine(client,clock=clock,rng=FixedRng(40,20))
    with pytest.raises(asyncio.CancelledError):
        await engine._rate_limited_accounts(cycle_config(tmp_path/'cycle',reconcile_timeout_seconds=5))
    assert client.reads==2 and not client.settings and not client.submissions
