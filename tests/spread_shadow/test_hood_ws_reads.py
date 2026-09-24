import asyncio
from dataclasses import replace
import json
import time
from decimal import Decimal
from types import SimpleNamespace

import pytest

from risex_spread_shadow.hood_handoff.stream_state import ReadStreamState
from risex_spread_shadow.hood_handoff.stream_measurement import StreamIdentity
from risex_spread_shadow.hood_handoff.sdk import LighterSdkClient
from test_hood_handoff_sdk_interface import _constant_nonce_client


def identity():
    return StreamIdentity(1, (11, 22), 4, 'https://api.rh.lighter.xyz', 466324, 'robinhood', 'BTC')


def depth(n=1, begin=None, snapshot=True, bids=None, asks=None):
    return {'type': 'subscribed/order_book' if snapshot else 'update/order_book', 'channel': 'order_book:1',
            'order_book': {'nonce': n, 'begin_nonce': begin,
                'bids': bids if bids is not None else [{'price': '100', 'size': '2'}],
                'asks': asks if asks is not None else [{'price': '101', 'size': '3'}]}}


def order(status='open'):
    return {'owner_account_index': 11, 'market_index': 1, 'client_order_index': 77,
            'order_id': '123', 'status': status, 'is_ask': False, 'type': 'limit',
            'time_in_force': 'post-only', 'reduce_only': False, 'initial_base_amount': '0.20',
            'remaining_base_amount': '0.20' if status == 'open' else '0',
            'filled_base_amount': '0.20' if status == 'filled' else '0', 'price': '100'}


async def put(state, value, at=None):
    await state.feed(json.dumps(value).encode(), time.monotonic() if at is None else at)


async def put_order(state, value):
    await put(state, {'type': 'update/account_all_orders', 'channel': 'account_all_orders:11', 'orders': {'1': [value]}})


@pytest.mark.asyncio
async def test_l2_deltas_delete_levels_preserve_exact_amounts_and_expire():
    s = ReadStreamState(identity()); await s.connected_now()
    await put(s, depth())
    await put(s, depth(2, 1, False, [{'price':'100','size':'0'},{'price':'99.9','size':'0.123456789'}], []))
    b = s.reads.book(time.monotonic(), 2)
    assert b.bids[0].price == Decimal('99.9')
    assert b.bids[0].quantity == Decimal('0.123456789')
    assert b.asks[0].price == Decimal('101')
    assert all(x.order_id is None and x.owner_account_index is None for x in b.bids+b.asks)
    assert s.reads.book(s.reads.book_at + 2.01, 2) is None
    await s.disconnected(); assert s.reads.book(time.monotonic(), 2) is None


@pytest.mark.asyncio
@pytest.mark.parametrize('bad', ['gap','duplicate','missing','cross','numeric','nan'])
async def test_invalid_depth_cannot_be_repaired_by_delta_without_snapshot(bad):
    s=ReadStreamState(identity()); await s.connected_now(); await put(s,depth())
    d=depth(2,1,False)
    if bad=='gap': d['order_book']['begin_nonce']=9
    if bad=='duplicate': d['order_book']['bids']*=2
    if bad=='missing': del d['order_book']['asks']
    if bad=='cross': d['order_book']['bids'][0]['price']='102'
    if bad=='numeric': d['order_book']['bids'][0]['size']=2
    if bad=='nan': d['order_book']['bids'][0]['size']='NaN'
    await put(s,d); assert s.reads.book(time.monotonic(),2) is None
    await put(s,depth(3,2,False)); assert s.reads.book(time.monotonic(),2) is None
    await put(s,depth(4)); assert s.reads.book(time.monotonic(),2) is not None


@pytest.mark.asyncio
async def test_positive_order_discovery_and_terminal_are_distinct_from_final_active_proof():
    s=ReadStreamState(identity()); await s.connected_now(); await put_order(s,order())
    args=(11,1,77,'123',time.monotonic(),2)
    assert s.reads.order(*args,terminal_only=False).active
    assert s.reads.order(*args,terminal_only=True) is None
    await put_order(s,order('filled'))
    o=s.reads.order(11,1,77,'123',time.monotonic(),2,terminal_only=True)
    assert o.terminal and o.filled_quantity==Decimal('.20')
    assert s.reads.order(11,1,77,'999',time.monotonic(),2,terminal_only=True) is None
    await s.disconnected(); assert not s.reads.orders


@pytest.mark.asyncio
@pytest.mark.parametrize('change',[{'reduce_only':None},{'price':None},{'is_ask':1},{'initial_base_amount':None},{'remaining_base_amount':'0.01'},{'filled_base_amount':'0.19'},{'owner_account_index':True}])
async def test_incomplete_or_conflicting_terminal_never_replaces_rest(change):
    s=ReadStreamState(identity()); await s.connected_now(); await put_order(s,order())
    item=order('filled');item.update(change);await put_order(s,item)
    assert s.reads.order(11,1,77,'123',time.monotonic(),2,terminal_only=True) is None


@pytest.mark.asyncio
async def test_sdk_uses_ws_for_price_and_terminal_but_not_final_active_lookup(monkeypatch):
    c=_constant_nonce_client(prefix='ws-read-actual-boundary')
    s=ReadStreamState(identity());await s.connected_now(); c._read_stream_state=s
    # Existing fixture market differs; use the configured identity for the price path.
    s.identity=replace(s.identity,market_id=c.config.market_id)
    s.reads.market=c.config.market_id
    s.observer.identity=s.identity
    payload=depth();payload['channel']=f'order_book:{c.config.market_id}'
    await put(s,payload)
    async def forbidden(*args,**kwargs): raise AssertionError('unexpected REST')
    monkeypatch.setattr(c,'order_book',forbidden)
    assert (await c.price_book(c.config.market_id)).bids[0].price==100
    # Full exact terminal observation returns without auth/HTTP.
    s.identity=identity();s.reads.market=1;s.observer.identity=s.identity
    await put_order(s,order('filled'))
    monkeypatch.setattr(c,'_authorization',forbidden)
    assert (await c.lookup_order(11,1,order_id='123',client_order_index=77)).terminal
    assert await c.lookup_order(11,1,order_id='123',client_order_index=77)
    await s.disconnected();await s.connected_now();await put_order(s,order())
    with pytest.raises(AssertionError,match='unexpected REST'):
        await c.lookup_order(11,1,order_id='123',client_order_index=77)

@pytest.mark.asyncio
async def test_conflict_poison_requires_reconnect_and_observation_time_does_not_refresh():
    s=ReadStreamState(identity());await s.connected_now();await put_order(s,order('filled'))
    first=s.reads.order(11,1,77,'123',time.monotonic(),2,terminal_only=True)
    again=s.reads.order(11,1,77,'123',time.monotonic(),2,terminal_only=True)
    assert first.observed_at==again.observed_at
    await put_order(s,order('open'))
    await put_order(s,order('filled'))
    assert s.reads.order(11,1,77,'123',time.monotonic(),2,terminal_only=True) is None
    await s.disconnected();await s.connected_now();await put_order(s,order('filled'))
    assert s.reads.order(11,1,77,'123',time.monotonic(),2,terminal_only=True)


@pytest.mark.asyncio
async def test_price_book_falls_back_on_stale_or_disconnected_state(monkeypatch):
    c=_constant_nonce_client(prefix='price-fallback')
    s=ReadStreamState(identity());await s.connected_now();c._read_stream_state=s
    called=[]
    async def rest(market):called.append(market);return 'rest'
    monkeypatch.setattr(c,'order_book',rest)
    assert await c.price_book(c.config.market_id)=='rest'
    assert len(called)==1
    await s.disconnected()
    assert await c.price_book(c.config.market_id)=='rest'


@pytest.mark.asyncio
async def test_guard_lost_prepares_cancel_while_book_is_still_pending(tmp_path):
    from test_hood_handoff_paired_opening import PairedClient, config, Clock
    from risex_spread_shadow.hood_handoff import run_handoff, DepthLevel
    class Client(PairedClient):
        def __init__(self):
            super().__init__();self.prepared=asyncio.Event();self.sent=0;self.invalidated=0;self.token=object()
        async def prepare_cancel_order(self,*args):
            self.prepared.set();return self.token
        async def submit_prepared_cancel_order(self,a,m,o,token,*,deadline):
            assert token is self.token
            self.sent+=1
            return await super().cancel_order(a,m,o)
        async def invalidate_prepared_order(self,token):
            assert token is self.token;self.invalidated+=1
        async def order_book(self,market):
            b=await super().order_book(market)
            if self.source_order is not None:
                # Fail if preparation waits for this book read to finish.
                await asyncio.wait_for(self.prepared.wait(),.3)
                return replace(b,bids=(DepthLevel(Decimal('100.1'),Decimal('1'),'external',999),)+b.bids)
            return b
    c=Client();r=await run_handoff(config(tmp_path/'guard.jsonl'),c,clock=Clock())
    assert c.sent==1 and len(c.cancellations)==1 and len(c.submissions)==1
    assert c.invalidated==1 and r.source.filled_quantity==0


@pytest.mark.asyncio
async def test_prepared_cancel_unknown_is_not_replayed(tmp_path):
    from test_hood_handoff_paired_opening import PairedClient, config, Clock
    from risex_spread_shadow.hood_handoff import run_handoff
    class Client(PairedClient):
        def __init__(self):super().__init__(receiver_fill_quantity=Decimal('.10'));self.sent=0
        async def prepare_cancel_order(self,*args):return object()
        async def submit_prepared_cancel_order(self,*args,**kwargs):
            self.sent+=1;raise TimeoutError('synthetic ambiguous send')
        async def invalidate_prepared_order(self,token):pass
    c=Client();await run_handoff(config(tmp_path/'unknown.jsonl'),c,clock=Clock())
    assert c.sent==1 and c.cancellations==[]

@pytest.mark.asyncio
async def test_unknown_channel_event_does_not_create_exact_order():
    s=ReadStreamState(identity());await s.connected_now()
    await put(s,{'type':'other','channel':'account_all_orders:11','orders':{'1':[order('filled')]}})
    assert s.reads.order(11,1,77,'123',time.monotonic(),2,terminal_only=True) is None


def test_ordinary_cycle_price_route_prefers_stream_interface():
    from risex_spread_shadow.hood_handoff.random_cycle import RandomCycleEngine
    class Client:
        async def price_book(self,market):return ('ws',market)
        async def order_book(self,market):raise AssertionError('price route used owner-proof REST')
    assert asyncio.run(RandomCycleEngine(Client())._order_book(1))==('ws',1)
