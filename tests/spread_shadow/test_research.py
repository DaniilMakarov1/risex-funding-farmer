from dataclasses import asdict, replace
from decimal import Decimal as D
import json
import hashlib

import pytest

from risex_farmer.models import Venue
from risex_spread_shadow import AppendOnlyEvidenceStore, MarketPair, SpreadObserver, Side
from risex_spread_shadow.feed import FeedBookEvent, FeedReceiptEvent, FeedTradeEvent
from risex_spread_shadow.research import build_research_report, render_research_report
from risex_spread_shadow.recording import RecordingReadbackError
from tests.spread_shadow.test_cycle import _market, _book, _trade, NOW
from tests.spread_shadow.test_pipeline import config
from tests.spread_shadow.test_recording import envelope, sample

NS = 1_000_000_000

async def saved_market(root, *, exit_price='97', loss=False, partial=False, residue=False, stale=False, delayed=False, duration=8):
    pair = MarketPair('BTC', _market(Venue.RISEX, 'BTC/USDC', minimum_quantity='0.5' if residue else '0.01'),
                      _market(Venue.LIGHTER, 'BTC', minimum_quantity='0.5' if residue else '0.01'))
    env = envelope()
    store = AppendOnlyEvidenceStore.create(root, metadata={
        'recording_mode': True, 'evidence_mode':'FIXTURE_ONLY', 'started_monotonic_ns':0,
        'recording_envelope':env}, max_records=env['max_records'], max_bytes=env['max_bytes'],
        terminal_record_reserve=env['record_reserve'], terminal_bytes_reserve=env['bytes_reserve'])
    clock=[0]
    observer=SpreadObserver(config(), (pair,), store, recording_mode=True,
                            sample_started_monotonic_ns=0, monotonic_ns=lambda:clock[0], now_utc=lambda:NOW)
    await observer._append(({'kind':'RUN_START','observed_monotonic_ns':0,'duration_seconds':duration,
                            'market_metadata':[{'risex':asdict(pair.risex_market),'lighter':asdict(pair.lighter_market)}]},))
    serial=0
    async def deliver(payload, *, delay=0):
        nonlocal serial
        serial+=1
        clock[0]=payload.received_monotonic_ns+delay
        receipt=FeedReceiptEvent(f'receipt-{serial}',payload.venue,payload.stream_session_id,
                                payload.recovery_generation,NOW,payload.received_monotonic_ns,
                                'TEXT','JSON',2,hashlib.sha256(b'{}').hexdigest(),
                                hasattr(payload,'bids'),canonical_market='BTC')
        observer.ingress.offer(receipt)
        if hasattr(payload,'bids'):
            observer.ingress.offer(FeedBookEvent(payload,pair,'SNAPSHOT','FIXTURE',receipt.receipt_id))
        else:
            observer.ingress.offer(FeedTradeEvent(payload,pair,receipt_id=receipt.receipt_id))
        while observer.ingress.has_pending:
            item=await observer.ingress.next_item()
            await observer.handle_item(item)
            observer.ingress.complete_item()
    events=[]
    # Frequent changed depth is explicit fixture evidence, never a timer refresh.
    frequency = 2 if residue else 5
    for step in range(1, duration*frequency):
        at=step*NS//frequency
        if stale and at > NS:
            break
        quantity=str(10+step/100)
        events.extend([
            _book(Venue.RISEX,received=at,revision=step,bids=(('97',quantity),),asks=(('103',quantity),)),
            _book(Venue.LIGHTER,received=at,revision=step,bids=(('99',quantity),),asks=((('110' if loss and at>=2_400_000_000 else '100'),quantity),))])
    events.append(_trade('entry',received=2*NS+100_000_000,quantity='0.2' if partial or residue else '1',price='102'))
    if not residue:
        events.append(_trade('exit',received=5*NS+100_000_000,quantity='1',price=exit_price,aggressor=Side.SELL))
    for event in sorted(events,key=lambda x:x.received_monotonic_ns):
        await deliver(event,delay=700_000_000 if delayed else 0)
    clock[0]=duration*NS+int(delayed)*NS
    observer.ingress.close()
    await observer.consume()
    await observer.close()
    await observer.append_terminal({'kind':'RUN_STOP','fatal_reason':None,'observed_monotonic_ns':clock[0]})
    store.close()
    return store.path


def material(report):
    report=json.loads(render_research_report(report,format='json'))
    report.pop('offline_compute_duration_ns')
    for lane in report['alternatives']:
        lane.pop('offline_compute_duration_ns')
    return report

@pytest.mark.asyncio
async def test_recording_to_four_lanes_profit_and_determinism(tmp_path):
    path=await saved_market(tmp_path)
    report=build_research_report(path)
    assert material(report)==material(build_research_report(path))
    for lane in report['alternatives']:
        assert lane['summary']['fills']>=4
        assert lane['summary']['closed_count']==1
        episode=lane['episodes'][0]
        assert episode['closed']
        # Cashflows: sell101-buy100-buy98+sell99 = 2. Maker fees .0101+.0098.
        stress = lane['scenario']=='STRESS'
        assert D(episode['closed_execution_pnl_usd'])==D('2')-D('0.0199')*(2 if stress else 1)
        assert D(episode['fees_usd'])==D('0.0199')
        assert D(episode['stress_cost_usd'])==(D('0.0199') if stress else D('0'))

@pytest.mark.asyncio
async def test_partial_residue_stale_and_actual_processing(tmp_path):
    for case, kwargs in [('partial',{'partial':True}),('residue',{'residue':True}),('stale',{'stale':True}),('delayed',{'delayed':True})]:
        path=await saved_market(tmp_path/case,**kwargs)
        report=build_research_report(path)

        if case=='partial':
            assert all(D(lane['episodes'][0]['entry_quantity'])==D('0.2') for lane in report['alternatives'])
        elif case=='residue':
            assert all(not lane['episodes'][0]['closed'] for lane in report['alternatives'])
        elif case=='stale':
            assert report['data_ineligible_duration_ns']>report['data_valid_duration_ns']
        else:
            assert report['data_valid_duration_ns']==0

@pytest.mark.asyncio
async def test_bad_saved_input_and_cli(tmp_path,capsys):
    from risex_spread_shadow.cli import main
    path=await sample(tmp_path)
    assert main(['research-report',str(path),'--output-json',str(tmp_path/'report.json')])==0
    assert 'TRADE_THROUGH_ONLY' in capsys.readouterr().out
    assert json.loads((tmp_path/'report.json').read_text())['technical_status']=='COMPLETE'
    path.write_text(path.read_text().rsplit('\n',2)[0]+'\n')
    with pytest.raises(RecordingReadbackError):build_research_report(path)

@pytest.mark.asyncio
async def test_closed_loss_and_minimum_blocked_residue(tmp_path):
    loss=build_research_report(await saved_market(tmp_path/'loss',loss=True))
    for lane in loss['alternatives']:
        assert lane['summary']['closed_count']==1
        # sell101-buy110-buy98+sell99 = -8; same two maker fees/cost.
        assert D(lane['summary']['closed_execution_pnl_usd'])==D('-8')-D('0.0199')*(2 if lane['scenario']=='STRESS' else 1)
    residue=build_research_report(await saved_market(tmp_path/'residue',residue=True,duration=126))
    for lane in residue['alternatives']:
        s=lane['summary']
        assert s['closed_count']==0
        assert s['lane_blocked_duration_ns']>0
        e=lane['episodes'][0]
        assert e['policy_blocked']
        assert D(e['positions']['risex_signed_quantity'])==D('-0.2')
        assert e['closed_execution_pnl_usd'] is None

@pytest.mark.asyncio
async def test_fixed_pilot_cutoff_and_corrupt_trade_link(tmp_path):
    path=await saved_market(tmp_path)
    records=[json.loads(line) for line in path.read_text().splitlines()]
    records[1]['duration_seconds']=900
    records[-1]['observed_monotonic_ns']=900*NS
    path.write_text(''.join(json.dumps(r)+'\n' for r in records))
    report=build_research_report(path)
    assert report['entry_requote_cutoff_monotonic_ns']==765*NS
    for lane in report['alternatives']:
        assert lane['summary']['decision_attempts']==764
        assert all(d['decision_monotonic_ns']<765*NS for d in lane['decisions'])
    for r in records:
        if r['kind']=='RISEX_TRADE':
            r['receipt_id']='missing'
    path.write_text(''.join(json.dumps(r)+'\n' for r in records))
    with pytest.raises(RecordingReadbackError,match='TRADE_RECEIPT_LINK_INVALID'):
        build_research_report(path)

@pytest.mark.asyncio
async def test_observed_queue_loss_is_incomplete_not_economic_loss(tmp_path):
    report=build_research_report(await sample(tmp_path,queue_capacity=1))
    assert report['technical_status']=='INCOMPLETE'
    assert report['readback']['observed_loss_count']>0
    assert all(lane['summary']['fills']==0 for lane in report['alternatives'])
