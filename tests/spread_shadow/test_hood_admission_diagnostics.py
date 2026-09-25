import asyncio
import json
import pytest
from test_hood_telegram_control import setup,update,Transport
from risex_spread_shadow.hood_handoff import telegram_control as b
from risex_spread_shadow.hood_handoff.contracts import PreflightBlocked

@pytest.mark.asyncio
@pytest.mark.parametrize('exc,category', [(TimeoutError('SECRET'), 'READ_TIMEOUT'),(BlockingIOError('SECRET'),'OPERATOR_BUSY'),(PreflightBlocked('SECRET'),'PREFLIGHT_REFUSED'),(RuntimeError('SECRET'),'CHECK_FAILED')])
async def test_failed_admission_survives_failed_reply_and_restart(tmp_path,capsys,exc,category):
    calls=[]
    async def launch(): calls.append('launch')
    async def recover(**kwargs): raise exc
    c=setup(tmp_path,launch,Transport(fail=True));c.recovery=recover
    old={'cycle':'cycle-001','status':'BLOCKED'};c.store.data['last']=old
    await c.handle(update());await c.task
    assert not calls and c.store.data['active'] is None and c.store.data['last']==old
    record=c.store.data['last_admission'];assert record['category']==category and record['stage']=='READY'
    fresh=b.Store(c.store.directory,'binding');assert fresh.data['last_admission']==record
    c.store=fresh;assert category in c.summary() and 'запуск не состоялся' in c.summary()
    assert 'SECRET' not in c.store.path.read_text()+capsys.readouterr().err+c.summary()
    await c.handle(update());assert not calls # consumed command never replayed

@pytest.mark.asyncio
async def test_fresh_success_clears_refusal_without_replaying_failed_command(tmp_path):
    calls=[]
    async def launch():calls.append(True)
    c=setup(tmp_path,launch)
    async def fail(**kwargs):raise TimeoutError()
    c.recovery=fail;await c.handle(update());await c.task
    async def ready(**kwargs):return {'status':'READY','at':1000,'previous_intents':0}
    c.recovery=ready;await c.handle(update(2));await c.task
    assert calls==[True] and c.store.data['last_admission'] is None

@pytest.mark.asyncio
async def test_telegram_http_category_does_not_leak_response(tmp_path):
    class Response:
        status=429
        async def __aenter__(self):return self
        async def __aexit__(self,*args):pass
        async def json(self):return {'ok':False,'description':'SECRET'}
    class Session:
        def post(self,*args,**kwargs):return Response()
    api=b.Telegram(Session(),'123456:'+'syntheticCanary'*3)
    with pytest.raises(b.TelegramFailure) as e:await api.call('sendMessage')
    assert e.value.category=='HTTP_429' and 'SECRET' not in str(e.value)
