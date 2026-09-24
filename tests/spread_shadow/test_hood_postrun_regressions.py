import asyncio
import json
from dataclasses import replace
from decimal import Decimal

import pytest

from risex_spread_shadow.hood_handoff import load_saved_cycle_report, run_random_cycle, Outcome, Direction, HandoffEngine
from test_hood_handoff_offline_report import complete_cycle, _change_records, _event
from test_hood_handoff_random_cycle import CycleClient, AdvancingClock, FixedRng, cycle_config
from test_hood_cycle075_regressions import MissingActiveFillClient
from test_hood_telegram_control import setup


@pytest.mark.parametrize('trailer', ['PILOT_READ_STREAM_SUMMARY', 'PILOT_STREAM_TIMELINE',
                                    'PILOT_STREAM_TIMELINE_UNKNOWN', 'HTTP_READ_TIMINGS'])
def test_diagnostic_trailer_preserves_complete_report_and_controller_finish(complete_cycle, trailer):
    path = complete_cycle.rename(complete_cycle.parent / 'cycle-001')
    _change_records(path/'cycle.jsonl', lambda rows: rows.append(_event(len(rows)+1, trailer, rows[-1]['at']+.1, {}, run_id=rows[-1]['run_id'])))
    report = load_saved_cycle_report(path)
    assert report['status'] == 'COMPLETE'
    assert report['inventory']['status'] == 'CONFIRMED_FLAT'
    c = setup(path.parent, None)
    c.store.data['active'] = {'action':'run', 'before':[]}
    c.finish()
    assert c.store.data['active'] is None
    assert c.store.data['last']['status'] == 'FINISHED'


@pytest.mark.parametrize('event', ['SOURCE_DISPATCH_INTENT', 'CYCLE_COMPLETE', 'UNKNOWN_DIAGNOSTIC'])
def test_non_allowlisted_trailer_still_blocks(complete_cycle, event):
    _change_records(complete_cycle/'cycle.jsonl', lambda rows: rows.extend([
        _event(len(rows)+1, 'PILOT_STREAM_TIMELINE', rows[-1]['at']+.1, {}),
        _event(len(rows)+2, event, rows[-1]['at']+.2, {}),
    ]))
    report = load_saved_cycle_report(complete_cycle)
    assert report['status'] != 'COMPLETE'
    assert any(i['code'] == 'POST_TERMINAL_RECORD' for i in report['issues'])


class MissingActiveZeroClient(MissingActiveFillClient):
    async def cancel_order(self, *args):
        return await CycleClient.cancel_order(self, *args)


@pytest.mark.asyncio
@pytest.mark.parametrize('closing', [False, True])
@pytest.mark.parametrize('direction', [Direction.LONG, Direction.SHORT])
async def test_zero_fill_missing_active_reconciles_without_unknown(tmp_path, closing, direction):
    clock = AdvancingClock(); client = MissingActiveZeroClient(clock, closing=closing)
    result = await run_random_cycle(cycle_config(tmp_path/'cycle', direction=direction),client,clock=clock,rng=FixedRng(20,20))
    phase = result.closing if closing else result.opening
    assert phase.outcome is Outcome.PARTIAL
    assert phase.source.filled_quantity == Decimal(0)
    assert phase.source.order.terminal
    assert not phase.receiver.dispatched
    assert result.inventory == 'CONFIRMED_FLAT'
    report = load_saved_cycle_report(tmp_path/'cycle')
    assert report['status'] == 'COMPLETE'
    assert report['inventory']['status'] == 'CONFIRMED_FLAT'


@pytest.mark.asyncio
@pytest.mark.parametrize('adverse', ['history', 'position', 'dispatched', 'unknown', 'active'])
async def test_zero_fill_provisional_reason_never_erases_uncertainty(tmp_path, adverse):
    clock=AdvancingClock(); client=MissingActiveZeroClient(clock)
    result=await run_random_cycle(cycle_config(tmp_path/'cycle'),client,clock=clock,rng=FixedRng(20,20))
    leg=result.opening; source=leg.source; receiver=leg.receiver
    # Reuse engine-produced legs, then independently break one proof.
    from types import SimpleNamespace
    plan=SimpleNamespace(quantity=Decimal('.20'), direction=Direction.LONG,
                         source_position_before=Decimal(0), receiver_position_before=Decimal(0))
    if adverse=='history':source=replace(source,history_complete=False)
    if adverse=='position':source=replace(source,position_after=Decimal('.01'))
    if adverse=='dispatched':receiver=replace(receiver,dispatched=True)
    if adverse=='unknown':source=replace(source,unknown_reasons=('ambiguous cancellation',))
    if adverse=='active':source=replace(source,order=replace(source.order,status='open'))
    engine=HandoffEngine(client)
    assert engine._classify(plan,source,receiver,('source active order absent in pre-receiver account snapshot',)) is Outcome.UNKNOWN


@pytest.mark.asyncio
async def test_actual_cycle_cleanup_trailers_are_accepted(tmp_path):
    class StreamClient(CycleClient):
        async def start_read_stream(self, **kwargs):pass
        async def stop_read_stream(self):pass
        def read_stream_summary(self):return {'ws_order_reads':2}
        def http_read_summary(self):return {'plain':[], 'sdk':[]}
    clock=AdvancingClock();client=StreamClient(clock)
    await run_random_cycle(cycle_config(tmp_path/'cycle'),client,clock=clock,rng=FixedRng(20,20))
    events=[json.loads(line)['event'] for line in (tmp_path/'cycle/cycle.jsonl').open()]
    assert 'HTTP_READ_TIMINGS' in events
    assert 'PILOT_READ_STREAM_SUMMARY' in events
    assert events.index('CYCLE_COMPLETE') < events.index('PILOT_READ_STREAM_SUMMARY')
    assert load_saved_cycle_report(tmp_path/'cycle')['status']=='COMPLETE'


@pytest.mark.asyncio
async def test_read_transport_diagnostics_redact_and_measure_reuse_and_cancellation():
    from aiohttp import web
    from risex_spread_shadow.hood_handoff.sdk import PlainAioHttp
    entered=asyncio.Event();release=asyncio.Event()
    async def response(request):
        if request.query.get('slow'):
            entered.set();await release.wait()
        return web.json_response({'code':200,'orders':[]})
    app=web.Application();app.router.add_get('/api/v1/accountOrders',response)
    runner=web.AppRunner(app);await runner.setup()
    site=web.TCPSite(runner,'127.0.0.1',0);await site.start()
    client=PlainAioHttp(f'http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}',timeout_seconds=2)
    try:
        for _ in range(2):
            await client.get('api/v1/accountOrders',params={'secret':'never-log'},authorization='private-token')
        task=asyncio.create_task(client.get('api/v1/accountOrders',params={'slow':1},authorization='private-token'))
        await entered.wait();task.cancel()
        with pytest.raises(asyncio.CancelledError):await task
        release.set()
        await client.get('api/v1/accountOrders',params={},authorization='private-token')
        rows=list(client._read_timings)
        assert len(rows)==4
        assert rows[0]['connection_seconds']>=0
        assert 'connection_reused_seconds' in rows[1]
        assert rows[1]['body_complete_seconds'] >= rows[1]['response_headers_seconds']
        assert rows[2]['cancelled'] is True
        assert 'connection_seconds' in rows[3]
        assert 'private-token' not in repr(rows) and 'never-log' not in repr(rows)
        assert client._read_timings.maxlen==256
    finally:
        release.set();await client.aclose();await runner.cleanup()


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['exact', 'stale', 'wrong_id', 'wrong_account', 'active_terminal_wait', 'missing'])
async def test_ready_ws_cache_avoids_rest_only_with_exact_fresh_eligible_order(state):
    from types import SimpleNamespace
    import time
    from test_hood_cycle075_regressions import snapshot
    from risex_spread_shadow.hood_handoff import OrderPlan
    value=snapshot();cached=value;calls=[]
    if state=='stale':cached=replace(value,observed_at=time.time()-100)
    if state=='wrong_id':cached=replace(value,order_id='wrong')
    if state=='wrong_account':cached=replace(value,account_index=999)
    if state=='missing':cached=None
    async def lookup(*a,**kw):calls.append('rest');return value
    client=SimpleNamespace(observed_order=lambda *a,**kw:cached)
    engine=HandoffEngine(client);engine._lookup_order=lookup;engine._configured_freshness=10
    plan=OrderPlan(account_index=11,market_id=1,side='BUY',quantity=Decimal('.20'),price=Decimal('100'),
                   order_type='LIMIT',time_in_force='POST_ONLY',reduce_only=False,client_order_index=77,
                   quantity_int=20,price_int=1000,order_expiry_ms=9999999999999)
    await engine._race_order_observation(plan,'123',require_terminal=state=='active_terminal_wait')
    assert calls == ([] if state=='exact' else ['rest'])
