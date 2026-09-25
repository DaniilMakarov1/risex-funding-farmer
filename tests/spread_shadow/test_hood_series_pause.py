import asyncio
import json
import pytest
from test_hood_telegram_control import setup,update
from test_hood_telegram_series import cycle_slot
from test_hood_auto_close_before_run import proof
from risex_spread_shadow.hood_handoff import telegram_control as bot

@pytest.mark.asyncio
async def test_pause_endpoints_and_fresh_readiness_after_each_pause(tmp_path):
    events=[];draws=iter([5,30]);i=0
    async def launch(**kwargs):
        nonlocal i
        i+=1;events.append(('run',i));cycle_slot(tmp_path,i)
    async def recovery(*,require_flat):
        events.append(('read',require_flat));return proof('READY' if require_flat else 'CLOSE_READY')
    async def pause(seconds):
        events.append(('pause',seconds))
        assert 'Пауза между циклами' in c.summary()
        assert json.loads(c.store.path.read_text())['active']['series_index']==i+1
    async def close():pytest.fail('flat series must not close')
    c=setup(tmp_path,launch);c.recovery=recovery;c.close=close;c._series_delay=lambda:next(draws);c._series_sleep=pause
    await c.handle(update(text='/run 3'));await c.task
    assert events==[('read',False),('read',True),('run',1),('pause',5),('read',False),('read',True),('run',2),('pause',30),('read',False),('read',True),('run',3)]
    assert c._series_pause is None and c.store.data['last']['series_completed']==3

@pytest.mark.asyncio
async def test_cancel_during_pause_does_not_launch_or_resume_next_step(tmp_path):
    reached=asyncio.Event();calls=[]
    async def launch(**kwargs):calls.append(1);cycle_slot(tmp_path,len(calls))
    async def recovery(*,require_flat):return proof('READY' if require_flat else 'CLOSE_READY')
    async def close():pytest.fail('close')
    async def pause(seconds):reached.set();await asyncio.Event().wait()
    c=setup(tmp_path,launch);c.recovery=recovery;c.close=close;c._series_sleep=pause
    await c.handle(update(text='/run 2'));await reached.wait();c.task.cancel()
    with pytest.raises(asyncio.CancelledError):await c.task
    assert calls==[1] and c._series_pause is None
    fresh=bot.Store(c.store.directory,'binding');assert fresh.data['active']['series_index']==2
    restarted=bot.Controller(42,c.config,fresh,c.transport,launch,recovery=recovery,close=close)
    restarted.finish();assert calls==[1]

@pytest.mark.asyncio
@pytest.mark.parametrize('delay',[4,31,True])
async def test_invalid_delay_cannot_launch_next_cycle(tmp_path,delay):
    calls=[]
    async def launch(**kwargs):calls.append(1);cycle_slot(tmp_path,len(calls))
    async def recovery(*,require_flat):return proof('READY' if require_flat else 'CLOSE_READY')
    c=setup(tmp_path,launch);c.recovery=recovery;c.close=launch;c._series_delay=lambda:delay
    await c.handle(update(text='/run 2'));await c.task
    assert calls==[1] and c.store.data['last']['series_completed']==1
