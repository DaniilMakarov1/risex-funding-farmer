"""Explicit pricing/admission changes: source proof is never fabricated."""
import asyncio
import json
from dataclasses import replace
from decimal import Decimal

import pytest

from risex_spread_shadow.hood_handoff import ContractError, Direction, Outcome, run_random_cycle, load_saved_cycle_report
from risex_spread_shadow.hood_handoff.local_attempt import select_automatic_prices
from test_hood_handoff_random_cycle import AdvancingClock, FixedRng, cycle_config, book, metadata
from test_hood_ws_confirmed import WsCycleClient
from test_hood_telegram_control import setup, update


@pytest.mark.parametrize('direction,expected', [(Direction.LONG, '100.5'), (Direction.SHORT, '100.5')])
def test_five_ticks_exact_mirrored_price(direction, expected):
    b = book(); b = replace(b, asks=(replace(b.asks[0], price=Decimal('101.0')),))
    p = select_automatic_prices(direction, metadata(), b, now=b.observed_at, price_improvement_ticks=5)
    assert p.source_limit_price == Decimal(expected)
    assert p.receiver_worst_price == Decimal(expected)


@pytest.mark.parametrize('direction', list(Direction))
def test_explicit_offset_never_silently_shrinks_or_crosses(direction):
    b = book()
    with pytest.raises(ContractError, match='PRICE_OFFSET_NO_ROOM'):
        select_automatic_prices(direction, metadata(), b, now=b.observed_at, price_improvement_ticks=5)
    assert select_automatic_prices(direction, metadata(), b, now=b.observed_at, price_improvement_ticks=1).source_limit_price == Decimal('100.1')


@pytest.mark.parametrize('value', [True, False, 0, -1, 6, 1.5, '5'])
def test_bad_ticks_refused(tmp_path, value):
    with pytest.raises(ContractError):
        cycle_config(tmp_path/'cycle', price_improvement_ticks=value)


class AckClient(WsCycleClient):
    def __init__(self, clock, adverse=None, closing=False):
        super().__init__(clock, adverse, closing)

    async def submit_order(self, plan):
        receipt = await super().submit_order(plan)
        if plan.order_type == 'LIMIT':
            code = {'negative_ack': 400, 'missing_code': None, 'bool_code': True}.get(self.bad(), 200)
            return replace(receipt, order_id=None, response_code=code,
                           accepted=self.bad() != 'negative_ack')
        return receipt

    def ws_admission_view(self, anchor, plan, order_id):
        if self.bad() in ('disconnect', 'stale', 'unexpected'):
            raise ContractError(self.bad())
        value = next(v for v in self.orders.values() if v.account_index == plan.account_index and v.client_order_index == plan.client_order_index)
        b = self._book_with_source_level(book(self.clock.now()))
        side = 'asks' if plan.side == 'SELL' else 'bids'
        levels = getattr(b, side)
        if self.bad() == 'better':
            price = plan.price + (Decimal('0.1') if side == 'bids' else Decimal('-0.1'))
            b = replace(b, **{side: (replace(levels[0], price=price),)})
        if self.bad() == 'same':
            b = replace(b, **{side: (replace(levels[0], quantity=plan.quantity + Decimal('1')), )})
        if self.bad() == 'terminal':
            value = self._replace_order(value, status='canceled', remaining_quantity=Decimal(0))
            return value, b
        if self.bad() == 'partial':
            return replace(value, filled_quantity=value.initial_quantity/2, remaining_quantity=value.initial_quantity/2), b
        # Simulate genuine positive ACK without private publication yet.
        return None, b


@pytest.mark.asyncio
@pytest.mark.parametrize('direction', list(Direction))
async def test_ack_sends_without_initial_order_wait_and_reconciles_both_phases(tmp_path, direction):
    clock = AdvancingClock(); client = AckClient(clock)
    cfg = cycle_config(tmp_path/'cycle', direction=direction, receiver_admission='ack')
    result = await run_random_cycle(cfg, client, clock=clock, rng=FixedRng(20,20))
    assert result.outcome is Outcome.SUCCESS, result.reason
    assert result.inventory == 'CONFIRMED_FLAT'
    for i, call in enumerate(client.calls):
        if call[:2] == ('send', 'LIMIT'):
            j = next(j for j in range(i+1, len(client.calls)) if client.calls[j][0] == 'send')
            assert client.calls[j][1] == 'MARKET'
            assert not any(v[0] in {'ws_wait','rest_order','book','account'} for v in client.calls[i+1:j])
    for phase in [result.opening, result.closing]:
        assert phase.priority_guard['status'] == 'ACK_ADMITTED'
        assert phase.priority_guard['source_resting_confirmed'] is False
        assert phase.priority_guard['priority_proof_admitted'] is False
    report = load_saved_cycle_report(tmp_path/'cycle')
    assert report['status'] == 'COMPLETE', report['issues']
    assert report['binding']['receiver_admission'] == 'ack'


@pytest.mark.asyncio
@pytest.mark.parametrize('adverse', ['negative_ack','missing_code','bool_code','disconnect','stale','unexpected','better','same','terminal','partial'])
@pytest.mark.parametrize('closing', [False,True])
async def test_ack_preserves_known_adverse_veto_and_residual_cleanup(tmp_path, adverse, closing):
    clock = AdvancingClock(); client = AckClient(clock, adverse, closing)
    result = await run_random_cycle(cycle_config(tmp_path/'cycle', receiver_admission='ack'), client,
                                    clock=clock, rng=FixedRng(20,20))
    phase = result.closing if closing else result.opening
    assert phase is not None, result.reason
    assert not phase.receiver.dispatched
    if closing and adverse in {'negative_ack', 'missing_code', 'bool_code'}:
        # Existing ambiguous/rejected closing policy reports residual rather than inventing flatness.
        assert result.inventory != 'CONFIRMED_FLAT'
        assert client.source_position != 0 and client.receiver_position != 0
    else:
        assert client.source_position == client.receiver_position == 0, result.reason
    assert all(not o.active for o in client.orders.values())


@pytest.mark.asyncio
@pytest.mark.parametrize('text,mode,ticks', [('/run ack 5','ack',5),('/run ack 1','ack',1),('/run ack','ack',None)])
async def test_owner_command_passes_only_selected_options_once(tmp_path, text, mode, ticks):
    calls=[]
    async def launch(**options): calls.append(options)
    c=setup(tmp_path, launch)
    c.config.write_text(json.dumps({'receiver_admission':'ws_confirmed','price_improvement_ticks':5}))
    await c.handle(update(text=text))
    await c.task
    await c.handle(update(text=text))
    expected={'receiver_admission':mode}
    if ticks is not None:expected['price_improvement_ticks']=ticks
    assert calls==[expected]
    assert json.loads(c.config.read_text())['receiver_admission']=='ws_confirmed'


@pytest.mark.asyncio
@pytest.mark.parametrize('text',['/run ack 0','/run ack 6','/run ACK 5','/run ws','/run ws 1','/run ws 1; close','/run ack 5 extra'])
async def test_invalid_switch_never_launches(tmp_path,text):
    calls=[]
    async def launch(**options):calls.append(options)
    c=setup(tmp_path,launch)
    await c.handle(update(text=text))
    assert not calls and c.task is None


@pytest.mark.asyncio
@pytest.mark.parametrize('late_status', ['canceled-post-only', 'open'])
async def test_ack_receiver_external_fill_then_exact_source_cancel_and_residual_close(tmp_path, late_status):
    class LateClient(AckClient):
        async def submit_order(self, plan):
            source_before = self.source_position
            maker = next((v for v in self.orders.values() if v.order_type=='LIMIT' and v.active), None)
            receipt = await super().submit_order(plan)
            if plan.order_type=='MARKET' and not plan.reduce_only:
                # Model B filling externally while A is rejected or still resting.
                self.source_position = source_before
                self._replace_order(maker, status=late_status, filled_quantity=Decimal(0),
                                    remaining_quantity=maker.initial_quantity if late_status=='open' else Decimal(0))
                self.trades[maker.order_id] = ()
                self.trades[receipt.order_id] = tuple(replace(t, counterparty_account_index=999,
                    counterparty_order_id='external', counterparty_client_order_index=None) for t in self.trades[receipt.order_id])
            return receipt
    clock=AdvancingClock();client=LateClient(clock)
    result=await run_random_cycle(cycle_config(tmp_path/'cycle',receiver_admission='ack'),client,clock=clock,rng=FixedRng(20,20))
    assert result.opening.receiver.dispatched
    assert result.outcome is not Outcome.SUCCESS
    assert result.inventory=='CONFIRMED_FLAT',result.reason
    assert client.source_position==client.receiver_position==0
    assert all(not o.active for o in client.orders.values())
    assert len([p for p in client.submissions if p.order_type=='LIMIT'])==1
    assert any(p.reduce_only for p in client.submissions)


@pytest.mark.asyncio
async def test_narrow_opening_spread_exposes_no_order(tmp_path):
    clock=AdvancingClock();client=AckClient(clock)
    result=await run_random_cycle(cycle_config(tmp_path/'cycle',receiver_admission='ack',price_improvement_ticks=5),client,clock=clock,rng=FixedRng(20,20))
    assert not client.submissions
    assert 'PRICE_OFFSET_NO_ROOM' in result.reason


@pytest.mark.asyncio
async def test_bounded_changed_public_quotes_are_retained_without_raw_payload(tmp_path):
    import time
    from test_hood_ws_reads import identity, depth, put
    from risex_spread_shadow.hood_handoff.stream_state import ReadStreamState
    from risex_spread_shadow.hood_handoff.stream_evidence import StreamEvidenceJournal
    state=ReadStreamState(identity())
    sink=StreamEvidenceJournal(tmp_path/'stream-events.jsonl',market_id=1,accounts=(11,22))
    state.evidence=sink
    await sink.start();await state.connected_now()
    state.bind_order(11,1,123,run_id='test',phase='PAIRED_OPENING',role='source',attempt_index=1)
    for i in range(35):
        frame=depth(n=i+1,bids=[{'price':str(100+i/100),'size':'.20'}])
        await put(state,frame)
        await asyncio.sleep(0)
    await sink.close(reason='stopped')
    rows=[json.loads(x) for x in (tmp_path/'stream-events.jsonl').read_text().splitlines()]
    tops=[r for r in rows if r['kind']=='book_top']
    assert len(tops) == 32
    assert tops[0]['bid_price']=='100.0'
    assert tops[0]['identity_or_fifo_proof'] is False
    assert tops[0]['phase']=='PAIRED_OPENING'
    assert rows[-1]['complete'] is True


@pytest.mark.asyncio
@pytest.mark.parametrize('mode',['ws_confirmed','ack'])
@pytest.mark.parametrize('direction',list(Direction))
async def test_five_tick_policy_reaches_opening_and_closing_and_saved_report(tmp_path,monkeypatch,mode,direction):
    import test_hood_handoff_random_cycle as fixture
    import test_hood_ws_confirmed as ws_fixture
    original=book
    def wide(at=fixture.NOW):
        b=original(at)
        return replace(b,asks=(replace(b.asks[0],price=Decimal('101.0')),))
    monkeypatch.setattr(fixture,'book',wide)
    monkeypatch.setattr(ws_fixture,'book',wide)
    monkeypatch.setitem(globals(),'book',wide)
    clock=AdvancingClock();client=AckClient(clock) if mode=='ack' else WsCycleClient(clock)
    result=await run_random_cycle(cycle_config(tmp_path/'cycle',receiver_admission=mode,price_improvement_ticks=5,direction=direction),client,clock=clock,rng=FixedRng(20,20))
    assert result.outcome is Outcome.SUCCESS,result.reason
    limits=[p for p in client.submissions if p.order_type=='LIMIT']
    assert len(limits)==2
    assert [p.price for p in limits]==[Decimal('100.5'),Decimal('100.5')]
    assert {p.side for p in limits}=={'BUY','SELL'}
    report=load_saved_cycle_report(tmp_path/'cycle')
    assert report['status']=='COMPLETE',report['issues']
    assert report['binding']['price_improvement_ticks']==5


@pytest.mark.asyncio
async def test_simple_cli_overrides_reach_launch_without_changing_file(tmp_path,monkeypatch):
    from types import SimpleNamespace
    from risex_spread_shadow.hood_handoff import cli
    config=tmp_path/'random-cycle.json'
    original={'receiver_admission':'ws_confirmed','price_improvement_ticks':1}
    config.write_text(json.dumps(original))
    seen=[]
    async def confirmed(args,value,path,operator):seen.append(value);return 0
    monkeypatch.setattr(cli,'_run_simple_confirmed',confirmed)
    monkeypatch.setattr(cli,'_simple_confirmation',lambda:True)
    monkeypatch.setattr(cli,'_simple_operator_dir',lambda path:tmp_path)
    args=SimpleNamespace(execute=False,confirm_plan=False,i_understand_one_attempt_live_operation=False,
                         config=config,receiver_admission='ack',price_improvement_ticks=5,keychain=True,keychain_replace=False)
    assert await cli._run_simple(args)==0
    assert seen==[{'receiver_admission':'ack','price_improvement_ticks':5}]
    assert json.loads(config.read_text())==original
