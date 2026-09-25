from dataclasses import replace
from decimal import Decimal as D
import json
import pytest
from risex_spread_shadow.hood_handoff.contracts import Direction, PreflightBlocked, Outcome
from risex_spread_shadow.hood_handoff.journal import DurableJournal
from risex_spread_shadow.hood_handoff.random_cycle import (RandomCycleEngine, RandomCycleSelection,
    OpeningMarginReserve, compute_quantity_bounds, _PairAttemptBudget, _OpeningQuantityRefresh)
from test_hood_handoff_random_cycle import AdvancingClock, FixedRng, metadata, book, cycle_config, run_random_cycle
from test_hood_margin_reserve import _margin_account
from test_hood_hcr40 import LeverageClient


def context(tmp_path, direction=Direction.LONG, balance='9', target=11):
    clock=AdvancingClock()
    market=replace(metadata(),mark_price=D('100'),minimum_initial_margin_fraction=2500,market_margin_mode=0)
    initial=(_margin_account(11,'20'),_margin_account(22,'20'))
    class Client:
        reads=0
        conflict=False
        async def market_metadata(self,m):return replace(market,observed_at=clock.now())
        async def order_book(self,m):return replace(book(),observed_at=clock.now())
        async def account_snapshot(self,a,m):
            self.reads+=1
            value=_margin_account(a,balance if a==target else '20')
            return replace(value,observed_at=clock.now(),source_identity='foreign' if self.conflict else value.source_identity)
    client=Client();engine=RandomCycleEngine(client,clock=clock,rng=FixedRng())
    engine._leverage_fractions={11:2500,22:2500}
    bounds=compute_quantity_bounds(market,*initial,D('100.1'),receiver_bound=D('100.1'),direction=direction)
    selection=RandomCycleSelection(quantity=D('.40'),quantity_tick=40,hold_seconds=29,
        opening_source_price=D('100.1'),opening_receiver_bound=D('100.1'),bounds=bounds,
        metadata_observed_at=1000,book_observed_at=1000)
    config=cycle_config(tmp_path/'cycle',direction=direction,margin_reserve=OpeningMarginReserve(D('.1'),D('.02')))
    journal=DurableJournal(config.journal_path,clock=clock.now)
    return engine,config,journal,selection,market,initial,client


@pytest.mark.asyncio
@pytest.mark.parametrize('direction',list(Direction))
@pytest.mark.parametrize('target',[11,22])
async def test_preparation_reduces_for_either_account_then_rereads(tmp_path,direction,target):
    e,c,j,s,m,initial,client=context(tmp_path,direction,target=target)
    budget=_PairAttemptBudget(limit=6)
    result=await e._prepare_open_with_retries(c,j,s,m,book(),*initial,budget)
    assert isinstance(result,tuple)
    assert result[-1].quantity==D('.35') and result[-1].hold_seconds==29
    assert budget.used==2 and client.reads==4
    assert not e.rng.values
    rows=[json.loads(l) for l in c.journal_path.read_text().splitlines()]
    resize=[r['payload'] for r in rows if r['event']=='OPENING_QUANTITY_RECALCULATED']
    assert len(resize)==1 and resize[0]['old_quantity']=='0.40' and resize[0]['new_quantity']=='0.35'
    budgets=[r['payload']['legs'] for r in rows if r['event']=='FRESH_OPENING_MARGIN_BUDGET']
    assert all(D(r['headroom'])>=D('.02') for r in budgets[-1])
    # Independent bound: .36*25 already exceeds 9-.02 before any fee/loss.
    assert D('.36')*25 > D('9')-D('.02')


@pytest.mark.asyncio
@pytest.mark.parametrize('barrier',['identity','minimum','exhausted'])
async def test_resize_never_waives_barriers(tmp_path,barrier):
    e,c,j,s,m,initial,client=context(tmp_path,balance='1' if barrier=='minimum' else '9')
    client.conflict=barrier=='identity'
    result=await e._prepare_open_with_retries(c,j,s,m,book(),*initial,_PairAttemptBudget(limit=1 if barrier=='exhausted' else 6))
    assert not isinstance(result,tuple)
    assert result.outcome==Outcome.FAILED_PREFLIGHT_BLOCKED
    assert 'OPENING_QUANTITY_RECALCULATED' not in c.journal_path.read_text()


@pytest.mark.asyncio
async def test_full_cycle_uses_reduced_size_for_both_open_and_close_without_setting_replay(tmp_path):
    class Client(LeverageClient):
        async def account_snapshot(self,a,m):
            if len(self.settings)==2:self.balances[11]=D('19')
            return await super().account_snapshot(a,m)
    clock=AdvancingClock();client=Client(clock);rng=FixedRng(40,29)
    result=await run_random_cycle(cycle_config(tmp_path/'cycle',margin_reserve=OpeningMarginReserve(D('.1'),D('.02'))),client,clock=clock,rng=rng)
    assert result.outcome==Outcome.SUCCESS,result.reason
    # Independent unit cost at the confirmed source IMF 4968 bps, mark/worst
    # 100.1 and published maker cap 0.00012 (no adverse loss at the mark).
    unit=D('100.1')*D('.4968')+D('100.1')*D('.00012')
    assert (D('19')-D('.10'))/unit>=D('.37')>(D('19')-D('.10'))/unit-D('.01')
    # .38 would still pass the 0.02 dispatch reserve but not the 0.10 planning
    # reserve that the original leverage plan also kept.
    assert D('.38')*unit+D('.02')<=D('19')<D('.38')*unit+D('.10')
    assert result.inventory=='CONFIRMED_FLAT' and result.selection.quantity==D('.37')
    assert result.selection.hold_seconds==29 and not rng.values
    assert len(client.settings)==2 and len(client.submissions)==4
    assert all(p.quantity==D('.37') for p in client.submissions)
    assert len({p.client_order_index for p in client.submissions})==4


@pytest.mark.asyncio
async def test_cycle194_numbers_recalculate_to_86_ticks(tmp_path):
    e,c,j,s,m,initial,client=context(tmp_path)
    m=replace(m,mark_price=D('84458.1'),price_decimals=1,size_decimals=5,
              minimum_base_amount=D('.00020'),minimum_quote_amount=D('10'))
    from test_hood_handoff_random_cycle import book as make_book
    b=make_book()
    from risex_spread_shadow.hood_handoff import DepthLevel as BookLevel
    b=replace(b,bids=(BookLevel(D('84320'),D('1')),),asks=(BookLevel(D('84327.7'),D('1')),))
    async def market_read(_):return m
    async def book_read(_):return b
    async def account_read(a,_):
        v=_margin_account(a,'21.638549' if a==11 else '23.796809')
        return replace(v,margin_evidence=replace(v.margin_evidence,selected_initial_margin_fraction='29.30' if a==11 else '32.20'))
    client.market_metadata=market_read;client.order_book=book_read;client.account_snapshot=account_read
    e._leverage_fractions={11:2930,22:3220}
    a1,a2=await account_read(11,7),await account_read(22,7)
    bounds=compute_quantity_bounds(m,a1,a2,D('84465.1'),receiver_bound=D('84465.1'),direction=Direction.LONG)
    s=replace(s,quantity=D('.00087'),quantity_tick=87,bounds=bounds)
    with pytest.raises(_OpeningQuantityRefresh) as err:
        await e._revalidate_open(c,s,m,b,a1,a2,journal=j)
    assert err.value.selection.quantity==D('.00086')
    # Independent from the production sizing helper, original journal terms.
    unit=D('84458.1')*D('.293')+D('84327.6')*D('.00012')+D('130.5')
    assert D('.00086')*unit+D('.02')<=D('21.638549')
    assert D('.00087')*unit+D('.02')>D('21.638549')


@pytest.mark.asyncio
async def test_repeated_falling_balance_exhausts_shared_budget_without_order(tmp_path):
    e,c,j,s,m,initial,client=context(tmp_path)
    count=0
    async def shrinking(a,_):
        nonlocal count
        if a==11:count+=1
        return replace(_margin_account(a,str(D('10')-D(count)/2)),observed_at=e.clock.now())
    client.account_snapshot=shrinking
    result=await e._prepare_open_with_retries(c,j,s,m,book(),*initial,_PairAttemptBudget(limit=3))
    assert result.outcome==Outcome.FAILED_PREFLIGHT_BLOCKED
    assert 'budget exhausted after 3/3' in result.reason
    assert count==3
    rows=[json.loads(l) for l in c.journal_path.read_text().splitlines()]
    assert len([r for r in rows if r['event']=='OPENING_QUANTITY_RECALCULATED'])==2


@pytest.mark.asyncio
@pytest.mark.parametrize('change',['position','leverage','grid','stale'])
async def test_unproved_or_changed_context_cannot_resize(tmp_path,change):
    e,c,j,s,m,initial,client=context(tmp_path)
    original=client.account_snapshot
    async def changed(a,b):
        v=await original(a,b)
        if change=='position':return replace(v,signed_position=D('.1'))
        if change=='leverage':return replace(v,margin_evidence=replace(v.margin_evidence,selected_initial_margin_fraction='30.00'))
        if change=='stale':return replace(v,observed_at=900)
        return v
    client.account_snapshot=changed
    if change=='grid':
        async def changed_market(_):return replace(m,size_decimals=3)
        client.market_metadata=changed_market
    result=await e._prepare_open_with_retries(c,j,s,m,book(),*initial,_PairAttemptBudget(limit=3))
    assert not isinstance(result,tuple)
    assert 'OPENING_QUANTITY_RECALCULATED' not in c.journal_path.read_text()


def test_recalculation_notice_survives_later_parent_events(tmp_path):
    from risex_spread_shadow.hood_handoff.operator_view import read_lifecycle
    j=DurableJournal(tmp_path/'cycle.jsonl',clock=lambda:1000)
    j.append('CYCLE_STARTED',{'binding':{}})
    j.append('OPENING_QUANTITY_RECALCULATED',{'attempt':1,'old_quantity':'.40','new_quantity':'.35'})
    j.append('PREPARATION_ACCEPTED',{})
    state=read_lifecycle(j.path)
    assert state['size_updates']==[{'attempt':1,'old_quantity':'.40','new_quantity':'.35'}]


@pytest.mark.asyncio
async def test_cancel_after_resize_never_reaches_handoff(tmp_path):
    import asyncio
    e,c,j,s,m,initial,client=context(tmp_path)
    async def cancelled(seconds):raise asyncio.CancelledError()
    e.clock.sleep=cancelled
    with pytest.raises(asyncio.CancelledError):
        await e._prepare_open_with_retries(c,j,s,m,book(),*initial,_PairAttemptBudget(limit=6))
    assert client.reads==2
    assert e._selection.quantity==D('.35')


@pytest.mark.asyncio
async def test_fixed_pilot_cannot_resize_even_when_smaller_size_would_fit(tmp_path):
    from types import SimpleNamespace
    from dataclasses import fields
    e,c,j,s,m,initial,client=context(tmp_path)
    # Isolate this private gate without creating a real fixed-pilot launch.
    values={f.name:getattr(c,f.name) for f in fields(c)};values['confirmed_pilot']=True
    pilot=SimpleNamespace(**values)
    with pytest.raises(PreflightBlocked) as err:
        await e._revalidate_open(pilot,s,m,book(),*initial,journal=j)
    assert not isinstance(err.value,_OpeningQuantityRefresh)


@pytest.mark.asyncio
async def test_resize_after_proved_zero_fill_opening_attempt_then_same_size_close(tmp_path):
    """Balance falls after a canceled-post-only zero-fill opening attempt."""
    from test_hood_handoff_random_cycle import PostOnlyCancelClient

    class Client(PostOnlyCancelClient, LeverageClient):
        async def submit_order(self, plan):
            receipt = await super().submit_order(plan)
            if plan.order_type == 'LIMIT' and not plan.reduce_only and self.source_limit_attempts == 1:
                self.balances[11] = D('19')
            return receipt

    clock = AdvancingClock(); client = Client(clock, cancel_count=1); rng = FixedRng(40, 29)
    config = cycle_config(tmp_path / 'cycle', margin_reserve=OpeningMarginReserve(D('.1'), D('.02')))
    result = await run_random_cycle(config, client, clock=clock, rng=rng)
    assert result.outcome == Outcome.SUCCESS, result.reason
    assert result.inventory == 'CONFIRMED_FLAT' and not rng.values
    rows = [json.loads(l) for l in config.journal_path.read_text().splitlines()]
    events = [r['event'] for r in rows]
    assert events.index('PAIR_ATTEMPT_RETRY') < events.index('OPENING_QUANTITY_RECALCULATED')
    resize = [r['payload'] for r in rows if r['event'] == 'OPENING_QUANTITY_RECALCULATED']
    assert len(resize) == 1 and resize[0]['old_quantity'] == '0.40'
    new = D(resize[0]['new_quantity'])
    fraction = client.settings[0][2]
    unit = D('100.1') * D(fraction) / 10000 + D('100.1') * D('.00012')
    assert new * unit + D('.10') <= D('19') < (new + D('.01')) * unit + D('.10')
    first, *rest = client.submissions
    assert first.quantity == D('.40') and first.order_type == 'LIMIT'
    assert rest and all(p.quantity == new for p in rest)  # later opening and closing
    assert len(client.settings) == 2  # no extra leverage write
    assert result.selection.quantity == new and result.selection.hold_seconds == 29


@pytest.mark.asyncio
async def test_closing_never_calls_opening_resize(tmp_path):
    """Structural: paired closing sizes from reconciled positions only."""
    import inspect
    from risex_spread_shadow.hood_handoff import random_cycle as rc
    closing = inspect.getsource(rc.RandomCycleEngine._run_paired_close)
    assert '_revalidate_open' not in closing and '_OpeningQuantityRefresh' not in closing
