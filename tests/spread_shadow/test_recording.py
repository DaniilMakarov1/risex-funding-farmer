from __future__ import annotations

import asyncio
from dataclasses import asdict
import json
from pathlib import Path
import subprocess
import sys

import pytest

from risex_farmer.exchanges.risex import RisexAdapter
from risex_farmer.exchanges.lighter import LighterAdapter
from risex_farmer.models import Venue
from risex_spread_shadow import AppendOnlyEvidenceStore, MarketPair, SpreadObserver
from risex_spread_shadow.feed import PublicFeedRunner
from risex_spread_shadow.recording import (
    build_recording_readback, RecordingReadbackError,
    RECORDING_MAX_RECORDS, RECORDING_RECORD_RESERVE, RECORDING_MAX_BYTES,
    RECORDING_BYTES_RESERVE, RECORDING_TERMINAL_RETENTION_CAPACITY,
)
from tests.spread_shadow.test_pipeline import market, config, NOW

PAIR = MarketPair('BTC', market(Venue.RISEX, 'BTC'), market(Venue.LIGHTER, 'BTC'))
START = 1_000_000_000
END = START + 2_000_000_000

def envelope():
    return dict(max_records=RECORDING_MAX_RECORDS, record_reserve=RECORDING_RECORD_RESERVE,
                max_bytes=RECORDING_MAX_BYTES, bytes_reserve=RECORDING_BYTES_RESERVE,
                terminal_retention_capacity=RECORDING_TERMINAL_RETENTION_CAPACITY)

def risex_snapshot(price='99'):
    return {'channel':'orderbook','type':'snapshot','market_id':'1','block_number':700,
            'log_index':2,'worker_timestamp':int(NOW.timestamp()*1e9),
            'data':{'market_id':1,'bids':[{'price':price,'quantity':'5'}],
                    'asks':[{'price':'101','quantity':'5'}]}}

def lighter_frame(nonce, begin, *, snapshot=False, price='100'):
    return {'type':'subscribed/order_book' if snapshot else 'update/order_book',
            'channel':'order_book/2','market_id':2,'timestamp':str(int(NOW.timestamp()*1000)),
            'order_book':{'code':0,'nonce':nonce,'begin_nonce':begin,
                          'bids':[{'price':price,'size':'10'}], 'asks':[{'price':'102','size':'10'}]}}

async def sample(root, *, bad=False, queue_capacity=32):
    store = AppendOnlyEvidenceStore.create(root, metadata={
        'evidence_mode':'FIXTURE_ONLY','recording_mode':True,'started_monotonic_ns':START,
        'recording_envelope':envelope()}, max_records=RECORDING_MAX_RECORDS,
        max_bytes=RECORDING_MAX_BYTES, terminal_record_reserve=RECORDING_RECORD_RESERVE,
        terminal_bytes_reserve=RECORDING_BYTES_RESERVE)
    clock=[START]
    observer=SpreadObserver(config(ingress_queue_capacity=queue_capacity), (PAIR,), store,
                            recording_mode=True, sample_started_monotonic_ns=START,
                            monotonic_ns=lambda:clock[0], now_utc=lambda:NOW)
    r=RisexAdapter(None);r._market_ids={'BTC':'1'};r._symbols_by_id={'1':'BTC'}
    r._raw_markets={'BTC':{'config':{'step_size':'1','step_price':'1'}}}
    l=LighterAdapter(None);l._market_ids={'BTC':2};l._symbols_by_id={2:'BTC'}
    feed=PublicFeedRunner(None,(PAIR,),observer.ingress,config=config(),risex_adapter=r,
                          lighter_adapter=l,capture_receipts=True,monotonic_ns=lambda:clock[0],now_utc=lambda:NOW)
    await observer._append(({'kind':'RUN_START','market_metadata':[{'risex':asdict(PAIR.risex_market),'lighter':asdict(PAIR.lighter_market)}], 'observed_monotonic_ns':START},))
    feed.begin_connection(Venue.RISEX,'r');feed.begin_connection(Venue.LIGHTER,'l')
    async def deliver(venue,payload,at):
        clock[0]=at
        await (feed.ingest_risex_payload if venue is Venue.RISEX else feed.ingest_lighter_payload)(
            payload,received_at=NOW,ingress_received_monotonic_ns=at,normalized_ready_monotonic_ns=at+1)
        while observer.ingress.has_pending:
            item=await observer.ingress.next_item()
            await observer.handle_item(item);observer.ingress.complete_item()
    await deliver(Venue.RISEX,risex_snapshot(),START+10)
    await deliver(Venue.LIGHTER,lighter_frame(5,5,snapshot=True),START+20)
    await deliver(Venue.LIGHTER,lighter_frame(6,5),START+100_000_000)
    await deliver(Venue.LIGHTER,lighter_frame(7,6,price='100.5'),START+200_000_000)
    await deliver(Venue.RISEX,risex_snapshot(),START+700_000_000)
    if bad:
        await deliver(Venue.LIGHTER,lighter_frame(9,8),START+800_000_000)
    clock[0]=END
    observer.ingress.close()
    await observer.consume()
    await observer.close()
    await observer.append_terminal({'kind':'RUN_STOP','fatal_reason':None,'observed_monotonic_ns':END})
    store.close()
    return store.path

@pytest.mark.asyncio
async def test_actual_adapters_write_read_unchanged_and_exact_silent_tail(tmp_path):
    path=await sample(tmp_path)
    report=build_recording_readback(path)
    assert report['recording_status']=='COMPLETE'
    assert report['changed_count']==3
    assert report['unchanged_count']==2
    assert report['book_audit']['book_count']==3
    assert report['book_audit']['delta_count']==1
    # Both books first ready at START+21; first RISEx expires at START+500000011.
    assert report['data_valid_duration_ns']==499_999_990
    assert report['collector_observation']['duration_ns']==2_000_000_000
    assert report['data_ineligible_duration_ns']==1_500_000_010
    assert report['observed_loss_count']==0
    assert report['data_eligibility_duration_ns']['STALE_RISEX_BOOK']==1_499_999_989
    assert build_recording_readback(path)==report
    output=subprocess.check_output([sys.executable,'-m','risex_spread_shadow.cli','record-readback',str(path)],text=True)
    assert json.loads(output)['data_valid_duration_ns']==499_999_990

@pytest.mark.asyncio
async def test_rejected_nonce_is_not_message_loss(tmp_path):
    path=await sample(tmp_path,bad=True)
    report=build_recording_readback(path)
    assert report['rejected_count']==1
    assert report['observed_loss_count']==0
    assert report['reason_counts']['LIGHTER_SEQUENCE_INVALID_FRESH_RESUBSCRIBE']>=1

@pytest.mark.asyncio
@pytest.mark.parametrize('corruption',['missing_book','wrong_session','future_link','truncated','terminal_fatal','early_terminal','wrong_market'])
async def test_reject_corrupt_provenance(tmp_path,corruption):
    path=await sample(tmp_path/'source')
    rows=[json.loads(l) for l in path.read_text().splitlines()]
    if corruption=='missing_book': rows=[r for r in rows if not(r['kind']=='BOOK' and r['venue']=='RISEX')]
    elif corruption=='wrong_session': next(r for r in rows if r['kind']=='PUBLIC_RECEIPT')['stream_session_id']='forged'
    elif corruption=='future_link':
        i=next(i for i,r in enumerate(rows) if r['kind']=='BOOK');rows[i],rows[i+1]=rows[i+1],rows[i]
    elif corruption=='truncated':rows.pop()
    elif corruption=='terminal_fatal':rows[-1]['fatal_reason']='QUEUE_OVERFLOW'
    elif corruption=='early_terminal':rows[-1]['observed_monotonic_ns']=START+600_000_000
    elif corruption=='wrong_market':next(r for r in rows if r['kind']=='PUBLIC_RECEIPT')['canonical_market']='ETH'
    for i,r in enumerate(rows):r['record_index']=i
    target=tmp_path/'corrupt.jsonl';target.write_text(''.join(json.dumps(r)+'\n' for r in rows))
    with pytest.raises(RecordingReadbackError):build_recording_readback(target)

@pytest.mark.asyncio
async def test_actual_queue_overflow_reports_incomplete(tmp_path):
    path=await sample(tmp_path,queue_capacity=1)
    report=build_recording_readback(path)
    assert report['recording_status']=='INCOMPLETE'
    assert report['observed_loss_count']>0
    assert 'QUEUE_OVERFLOW' in report['incomplete_reasons']

def test_public_record_cli_uses_actual_writer_and_readback(tmp_path,monkeypatch,capsys):
    import risex_spread_shadow.runner as runtime
    import risex_spread_shadow.cli as cli
    clock=[START]
    monkeypatch.setattr(runtime.time,'monotonic_ns',lambda:clock[0])
    async def select(r,l,**kwargs):
        assert kwargs['requested_markets']==('BTC',) and kwargs['max_markets']==1
        r._market_ids={'BTC':'1'};r._symbols_by_id={'1':'BTC'}
        r._raw_markets={'BTC':{'config':{'step_size':'1','step_price':'1'}}}
        l._market_ids={'BTC':2};l._symbols_by_id={2:'BTC'}
        return (PAIR,)
    monkeypatch.setattr(runtime,'select_public_market_pairs',select)
    original_create=AppendOnlyEvidenceStore.create.__func__
    def fixture_create(cls,*args,metadata,**kwargs):
        return original_create(cls,*args,metadata={**metadata,'evidence_mode':'FIXTURE_ONLY'},**kwargs)
    monkeypatch.setattr(AppendOnlyEvidenceStore,'create',classmethod(fixture_create))
    async def prepared_frames(self,**kwargs):
        assert kwargs=={'duration_seconds':60}
        self.begin_connection(Venue.RISEX,'r');self.begin_connection(Venue.LIGHTER,'l')
        for venue,payload,at in [(Venue.RISEX,risex_snapshot(),START+10),
                                 (Venue.LIGHTER,lighter_frame(5,5,snapshot=True),START+20),
                                 (Venue.LIGHTER,lighter_frame(6,5),START+100_000_000)]:
            clock[0]=at
            await (self.ingest_risex_payload if venue is Venue.RISEX else self.ingest_lighter_payload)(payload,received_at=NOW,ingress_received_monotonic_ns=at,normalized_ready_monotonic_ns=at+1)
            # Let the actual consumer acknowledge this finite input before advancing the model clock.
            async def drain():
                while self.ingress.has_pending: await asyncio.sleep(0)
            await asyncio.wait_for(drain(),2)
        clock[0]=END
    monkeypatch.setattr(PublicFeedRunner,'run',prepared_frames)
    assert cli.main(['record','--store-root',str(tmp_path),'--duration-seconds','60'])==0
    report=json.loads(capsys.readouterr().out)
    assert report['readback']['recording_status']=='COMPLETE'
    assert report['readback']['outcome_counts']=={'CHANGED':2,'UNCHANGED':1}
    records=[json.loads(l) for l in Path(report['store_path']).read_text().splitlines()]
    assert records[0]['metadata']['evidence_mode']=='FIXTURE_ONLY'
    assert next(r for r in records if r['kind']=='RUN_START')['market_metadata'][0]['risex']['venue']=='RISEX'
    assert not any(r['kind'] in ('SAMPLE_STOP','QUOTE','WOULD_FILL') for r in records)

@pytest.mark.asyncio
async def test_write_failure_never_becomes_complete(tmp_path,monkeypatch):
    original=AppendOnlyEvidenceStore.append_batch
    def broken(self,records,**kwargs):
        if any(r.get('kind')=='BOOK' for r in records):raise OSError('fixture write failed')
        return original(self,records,**kwargs)
    monkeypatch.setattr(AppendOnlyEvidenceStore,'append_batch',broken)
    with pytest.raises(OSError): await sample(tmp_path)
    path=next(tmp_path.glob('*/evidence.jsonl'))
    with pytest.raises(RecordingReadbackError,match='TERMINAL_MARKER_MISSING'):
        build_recording_readback(path)

@pytest.mark.asyncio
async def test_silence_is_not_loss_and_empty_window_is_measured(tmp_path):
    store=AppendOnlyEvidenceStore.create(tmp_path,metadata={'recording_mode':True,'recording_envelope':envelope(),'started_monotonic_ns':START})
    store.append_batch(({'kind':'RUN_STOP','fatal_reason':None,'observed_monotonic_ns':END},));store.close()
    report=build_recording_readback(store.path)
    assert report['application_silence'] and report['observed_loss_count']==0
    assert report['data_valid_duration_ns']==0
    assert report['data_ineligible_duration_ns']==2_000_000_000

@pytest.mark.parametrize('args',[['--market','ETH'],['--max-markets','2'],['--duration-seconds','61']])
def test_record_cli_fixed_scope_before_network(args):
    from risex_spread_shadow.cli import main
    with pytest.raises(SystemExit):main(['record',*args])
