import json
from dataclasses import replace
from decimal import Decimal
import pytest
from risex_spread_shadow.hood_handoff import operator_recovery as r
from risex_spread_shadow.hood_handoff.contracts import PreflightBlocked,AccountMarginEvidence
from risex_spread_shadow.hood_handoff.journal import DurableJournal
from test_hood_handoff_random_cycle import AdvancingClock,cycle_config
from test_hood_operator_recovery import RecoveryClient


def append(path,event,payload):
    j=DurableJournal(path,clock=lambda:1000);j.acquire_attempt()
    try:j.append(event,payload)
    finally:j.release_attempt()


def seed(tmp_path):
    slot=tmp_path/'cycle-001';slot.mkdir(mode=0o700)
    j=DurableJournal(slot/'cycle.jsonl',clock=lambda:1000);j.acquire_attempt()
    try:
        j.append('LEVERAGE_UPDATE_INTENT',{'account_index':11,'market_id':7,'fraction_bps':4166,'margin_mode':0,'source_identity':'cycle-account-11'})
        j.append('LEVERAGE_TX_PREPARED',{'account_index':11,'market_id':7,'fraction_bps':4166,'margin_mode':0,'api_key_index':4,'tx_type':20,'nonce':41,'tx_hash':'ab'*40})
    finally:j.release_attempt()
    return cycle_config(slot,api_key_index=4)


def tx():
    return dict(hash='ab'*40,type=20,status=3,account_index=11,api_key_index=4,nonce=41,
                executed_at=1000000,committed_at=0,verified_at=0,
                execution_event={'a':11,'m':7,'imf':4166,'mm':0,'ae':''})


class Client(RecoveryClient):
    tx_reads=0
    async def read_leverage_transaction(self,value):
        self.tx_reads+=1
        return tx()
    async def read_leverage_next_nonce(self,a,k):return 42
    async def account_snapshot(self,a,m):
        value=await super().account_snapshot(a,m)
        evidence=AccountMarginEvidence.from_response(account_index=a,market_id=m,
            source_identity=value.source_identity,observed_at=self.clock.now(),
            selected_position={'market_id':m,'initial_margin_fraction':'41.66','margin_mode':0},
            account={'cross_asset_value':'100'})
        return replace(value,margin_evidence=evidence)


@pytest.mark.asyncio
async def test_shared_reads_then_resolution_then_restart_no_replay(tmp_path):
    cfg=seed(tmp_path);path=tmp_path/'recovery-checks.jsonl'
    for _ in range(3):append(path,'CURRENT_STATE_VERIFIED',{'status':'CLOSE_READY'})
    original=path.read_bytes();clock=AdvancingClock();client=Client(clock)
    proof=await r.inspect_current(cfg,client,tmp_path,clock=clock,require_flat=True)
    assert proof[2]['status']=='READY' and not client.submissions
    assert path.read_bytes().startswith(original)
    append(path,'CURRENT_STATE_VERIFIED',{'status':'READY'})
    again=await r.inspect_current(cfg,client,tmp_path,clock=clock,require_flat=True)
    assert again[2]['status']=='READY' and client.tx_reads==1
    checkpoints=r._leverage_checkpoints(tmp_path)
    assert list(checkpoints.values())[0]['proof_version']==3
    with pytest.raises(PreflightBlocked,match='run identity'):
        list(r.journal_rows(path))  # ordinary trading readers remain strict


@pytest.mark.parametrize('field,value', [('sequence',True),('sequence',9),('at',-2),('at',True),('run_id',''),('run_id',None),('payload',[]),('event','SOURCE_DISPATCH_INTENT')])
def test_shared_reader_rejects_corruption(tmp_path,field,value):
    path=tmp_path/'recovery-checks.jsonl'
    append(path,'CURRENT_STATE_VERIFIED',{})
    row=json.loads(path.read_text());row[field]=value
    path.write_text(json.dumps(row)+'\n')
    with pytest.raises(PreflightBlocked):r._leverage_checkpoints(tmp_path)


@pytest.mark.parametrize('field,value', [('status',2),('status',True),('executed_at',1000001),('committed_at',1),('verified_at',1),('nonce',True),('hash','cd'*40),('execution_event',None)])
def test_execution_parser_never_accepts_incomplete_or_future(field,value):
    raw=tx();raw[field]=value
    with pytest.raises(PreflightBlocked):r._robinhood_leverage_execution(raw,'ab'*40,1000)


@pytest.mark.asyncio
@pytest.mark.parametrize('bad',['fraction','market','error','nonce','identity','past','position','setting','venue','bare_status'])
async def test_recovery_needs_all_independent_proofs(tmp_path,bad):
    cfg=seed(tmp_path);clock=AdvancingClock()
    class Bad(Client):
        async def read_leverage_transaction(self,value):
            result=tx()
            if bad=='fraction':result['execution_event']['imf']=5000
            if bad=='market':result['execution_event']['m']=8
            if bad=='error':result['execution_event']['ae']='failed'
            if bad=='identity':result['account_index']=22
            if bad=='past':result['executed_at']=999999
            if bad=='bare_status':del result['execution_event']
            return result
        async def read_leverage_next_nonce(self,a,k):return 41 if bad=='nonce' else 42
        async def account_snapshot(self,a,m):
            value=await super().account_snapshot(a,m)
            if a==11 and bad=='position':return replace(value,signed_position=Decimal('.2'))
            if a==11 and bad=='setting':return replace(value,margin_evidence=None)
            return value
    if bad=='venue':object.__setattr__(cfg,'chain_id',304)  # forged binding must still fail closed
    client=Bad(clock)
    with pytest.raises(PreflightBlocked):await r.resolve_prior(cfg,client,tmp_path,clock=clock)
    assert not client.submissions and not (tmp_path/'recovery-checks.jsonl').exists()


@pytest.mark.asyncio
async def test_checkpoint_conflict_and_duplicate_still_block(tmp_path):
    cfg=seed(tmp_path);clock=AdvancingClock();client=Client(clock)
    await r.resolve_prior(cfg,client,tmp_path,clock=clock)
    path=tmp_path/'recovery-checks.jsonl';row=json.loads(path.read_text())
    payload=row['payload'];payload['transaction']['execution_event']['imf']=5000
    path.write_text(json.dumps(row)+'\n')
    with pytest.raises(PreflightBlocked):await r.resolve_prior(cfg,client,tmp_path,clock=clock)
    append(path,'LEVERAGE_RESOLUTION_CHECKPOINT',payload)
    with pytest.raises(PreflightBlocked,match='duplicate'):r._leverage_checkpoints(tmp_path)
