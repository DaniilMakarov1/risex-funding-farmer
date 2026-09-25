import asyncio
import json
from dataclasses import replace
from decimal import Decimal
import pytest
from test_hood_handoff_random_cycle import AdvancingClock, CycleClient, FixedRng, cycle_config
from risex_spread_shadow.hood_handoff import random_cycle as r
from risex_spread_shadow.hood_handoff.contracts import ContractError, Direction, PreflightBlocked
from risex_spread_shadow.hood_handoff.journal import DurableJournal
from risex_spread_shadow.hood_handoff.operator_view import read_lifecycle, lifecycle_lines

class Narrow(CycleClient):
    def __init__(self, clock, bad_reads=1, fault=None):
        super().__init__(clock)
        self.reads = 0
        self.bad_reads = bad_reads
        self.fault = fault
        self.metadata_reads = 0
        self.account_reads = 0
    async def market_metadata(self, market_id):
        self.metadata_reads += 1
        return await super().market_metadata(market_id)
    async def order_book(self, market_id):
        self.reads += 1
        b = await super().order_book(market_id)
        if self.reads <= self.bad_reads:
            return replace(b, asks=(replace(b.asks[0], price=Decimal('100.1')),))
        if self.fault == 'book': return replace(b, observed_at=self.clock.now()-100)
        if self.fault == 'deadline': self.clock.value += 2
        if self.fault == 'timeout': await asyncio.sleep(1)
        if self.fault == 'cancel': raise asyncio.CancelledError()
        return b
    async def account_snapshot(self, a, m):
        self.account_reads += 1
        v = await super().account_snapshot(a, m)
        if self.reads > self.bad_reads:
            if self.fault == 'position': return replace(v, signed_position=Decimal('.1'))
            if self.fault == 'identity': return replace(v, account_index=999)
            if self.fault == 'stale': return replace(v, observed_at=self.clock.now()-100)
        return v

@pytest.mark.asyncio
@pytest.mark.parametrize('direction', [Direction.LONG, Direction.SHORT])
async def test_initial_wait_then_full_cycle_without_extra_random_draws(tmp_path, direction):
    clock=AdvancingClock();client=Narrow(clock);rng=FixedRng(10,20)
    if direction is Direction.SHORT:
        client.source_account_index,client.receiver_account_index=11,22
    result=await r.RandomCycleEngine(client,clock=clock,rng=rng).execute(cycle_config(tmp_path,direction=direction,price_improvement_ticks=1))
    assert result.inventory == 'CONFIRMED_FLAT'
    assert len(rng.bounds)==2 and len(client.submissions)==4
    rows=[json.loads(x) for x in (tmp_path/'cycle.jsonl').read_text().splitlines()]
    events=[x['event'] for x in rows]
    assert events.count('INITIAL_SPREAD_WAIT')==events.count('INITIAL_SPREAD_READY')==1
    assert events.index('INITIAL_SPREAD_READY')<events.index('SELECTION_PROVED')
    assert result.selection.opening_source_price==Decimal('100.1')

@pytest.mark.asyncio
@pytest.mark.parametrize('fault', ['position','identity','stale','book','deadline','timeout','cancel'])
async def test_wait_never_waives_other_failures(tmp_path,fault):
    clock=AdvancingClock();client=Narrow(clock,fault=fault);engine=r.RandomCycleEngine(client,clock=clock,rng=FixedRng())
    cfg=cycle_config(tmp_path,price_improvement_ticks=1,reconcile_timeout_seconds=.05 if fault=='timeout' else 1)
    if fault=='cancel':
        with pytest.raises(asyncio.CancelledError):await engine.execute(cfg)
    else:
        result=await engine.execute(cfg)
        assert result.selection is None and result.outcome is r.Outcome.FAILED_PREFLIGHT_BLOCKED
    assert not client.submissions and not client.cancellations and client.reads==2

@pytest.mark.asyncio
@pytest.mark.parametrize('max_reads,delay,expected', [(1,.001,1),(3,.001,3),(40,1,1)])
async def test_exhaustion_is_bounded_and_cannot_replay(tmp_path,max_reads,delay,expected):
    clock=AdvancingClock();client=Narrow(clock,bad_reads=100);rng=FixedRng()
    cfg=cycle_config(tmp_path,price_improvement_ticks=1,max_poll_count=max_reads,poll_interval_seconds=delay)
    result=await r.RandomCycleEngine(client,clock=clock,rng=rng).execute(cfg)
    assert result.outcome is r.Outcome.FAILED_PREFLIGHT_BLOCKED and 'exhausted' in result.reason
    assert client.reads==expected and not rng.bounds and not client.submissions
    again=await r.RandomCycleEngine(client,clock=clock,rng=rng).execute(cfg)
    assert again.outcome is r.Outcome.FAILED_PREFLIGHT_BLOCKED and client.reads==expected

@pytest.mark.asyncio
async def test_success_has_no_added_reads_or_waits(tmp_path):
    clock=AdvancingClock();client=Narrow(clock,bad_reads=0);engine=r.RandomCycleEngine(client,clock=clock)
    cfg=cycle_config(tmp_path,price_improvement_ticks=1);j=DurableJournal(tmp_path/'cycle.jsonl',clock=clock.now)
    await engine._initial_open_context(cfg,j)
    assert (client.reads,client.metadata_reads,client.account_reads)==(1,1,2)
    assert not clock.sleeps

@pytest.mark.parametrize('exc,wanted',[(ContractError('PRICE_OFFSET_NO_ROOM: spread cannot fit the requested tick improvement'),True),(RuntimeError('PRICE_OFFSET_NO_ROOM: spread cannot fit the requested tick improvement'),False),(ContractError('PRICE_OFFSET_NO_ROOM: identity conflict'),False)])
def test_retry_classifier_is_exact(exc,wanted):
    assert r._is_retryable_preparation_error(exc) is wanted

def test_background_wait_lifecycle(tmp_path):
    p=tmp_path/'cycle.jsonl';j=DurableJournal(p,clock=lambda:1000);j.acquire_attempt()
    try:
        j.append('CYCLE_STARTED',{'binding':{}});j.append('INITIAL_SPREAD_WAIT',{})
        assert read_lifecycle(p)['stage']=='SPREAD_WAIT'
        assert 'заявки ещё не отправлены' in lifecycle_lines(read_lifecycle(p))[0]
        j.append('INITIAL_SPREAD_READY',{})
        assert read_lifecycle(p)['stage']=='PREPARING'
    finally:j.release_attempt()

@pytest.mark.asyncio
@pytest.mark.parametrize('failure_call,event', [(2,'PREPARATION_RETRY'),(3,'CLOSING_PREPARATION_RETRY')])
async def test_paired_preparation_retries_no_room_without_replaying_orders(tmp_path,monkeypatch,failure_call,event):
    original=r.select_automatic_prices;calls=0
    def proposal(*args,**kwargs):
        nonlocal calls
        calls+=1
        if calls==failure_call:
            raise ContractError('PRICE_OFFSET_NO_ROOM: spread cannot fit the requested tick improvement')
        return original(*args,**kwargs)
    monkeypatch.setattr(r,'select_automatic_prices',proposal)
    clock=AdvancingClock();client=CycleClient(clock);rng=FixedRng(10,20)
    result=await r.RandomCycleEngine(client,clock=clock,rng=rng).execute(cycle_config(tmp_path,price_improvement_ticks=1))
    assert result.inventory=='CONFIRMED_FLAT' and len(client.submissions)==4 and len(rng.bounds)==2
    events=[json.loads(x)['event'] for x in (tmp_path/'cycle.jsonl').read_text().splitlines()]
    assert events.count(event)==1
