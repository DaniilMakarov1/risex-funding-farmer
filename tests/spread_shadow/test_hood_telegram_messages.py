from html.parser import HTMLParser
import json
from pathlib import Path

import pytest

from risex_spread_shadow.hood_handoff import telegram_messages as views
from risex_spread_shadow.hood_handoff.telegram_control import Telegram
from risex_spread_shadow.hood_handoff.offline_report import load_saved_cycle_report


class Markup(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.stack = []
        self.plain = ''
    def handle_starttag(self, tag, attrs):
        assert tag in {'b', 'i', 'code'}
        assert not attrs
        self.stack.append(tag)
    def handle_endtag(self, tag):
        assert self.stack.pop() == tag
    def handle_data(self, data):
        self.plain += data


def valid(message):
    parser = Markup()
    parser.feed(message)
    parser.close()
    assert not parser.stack
    assert 0 < len(parser.plain.encode('utf-16-le')) // 2 <= 4096
    return parser.plain


@pytest.mark.parametrize('message', [views.help_message(), views.startup_message(),
    views.accepted_message(123), views.blocked_message(), views.unknown_message(),
    views.running_message([]), views.running_message(['cycle-008']),
    views.empty_message(), views.empty_message(True), views.unavailable_message(),
    views.unavailable_message(True), views.unavailable_message(not_launched=True)])
def test_every_operator_state_has_balanced_bounded_html(message):
    assert valid(message)


@pytest.mark.parametrize('number,expected', [('003','неизвестна'),('004','неполные данные'),('007','не состоялось')])
def test_saved_incident_display_preserves_failure_and_historical_times(tmp_path, number, expected):
    fixture = json.loads((Path(__file__).parents[1] / f'fixtures/hood_handoff/cycle-{number}.json').read_text())
    for name, rows in fixture['journals'].items():
        (tmp_path/name).write_text(''.join(json.dumps(row)+'\n' for row in rows))
    r = load_saved_cycle_report(tmp_path)
    short = views.saved_message(f'cycle-{number}', r)
    full = views.saved_message(f'cycle-{number}', r, detailed=True)
    assert expected in valid(full)
    assert len(short) < len(full)
    assert 'Последние сохранённые позиции' in full
    assert 'не текущая проверка счетов' in full
    assert 'Историческая позиция' in short
    assert 'UTC' in full or 'нет данных' in full
    assert 'Парное исполнение: ✅ подтверждено' not in full


def test_html_injection_and_large_reasons_are_escaped_and_bounded():
    r = {'status':'INCOMPLETE','reasons':['<b onclick="x">& 🚀</b>'*1000,'Bearer synthetic-canary'],
         'inventory':{'source':'<script>bad</script>', 'receiver':None, 'observed_at':{'source':float('inf')}},
         'order_state':{'unresolved_intents':[], 'unresolved_observed_orders':[]}}
    message = views.saved_message('<b>&cycle</b>', r, blocked=True, detailed=True)
    plain = valid(message)
    assert '<script>' not in message
    assert '&lt;script&gt;' in message
    assert 'onclick' in plain  # text, never a Telegram entity attribute
    assert 'synthetic-canary' not in message
    assert 'заблокированы' in message
    assert len(plain) < 2000


@pytest.mark.parametrize('value', [None, True, -1, float('nan'), float('inf'), 10**400, 'not a timestamp'])
def test_invalid_observation_time_is_unknown(value):
    assert views.timestamp(value) == 'нет данных'


class Response:
    status = 200
    async def __aenter__(self): return self
    async def __aexit__(self, *args): pass
    async def json(self): return {'ok':True,'result':{'message_id':123}}


async def test_transport_uses_html_and_readonly_keyboard_without_cutting_tags():
    payloads = []
    class Session:
        def post(self, url, **kwargs):
            assert kwargs['allow_redirects'] is False
            payloads.append(kwargs['json'])
            return Response()
    api = Telegram(Session(), '123456:'+'syntheticCanary'*3)
    assert await api.send(42, views.help_message()) == {'message_id':123}
    await api.send(42, '<b>' + '🚀'*3000 + '</b>')
    for payload in payloads:
        assert payload['parse_mode'] == 'HTML'
        assert payload['chat_id'] == 42
        assert payload['protect_content'] is True
        assert payload['link_preview_options']['is_disabled'] is True
        valid(payload['text'])
        buttons = [b['text'] for row in payload['reply_markup']['keyboard'] for b in row]
        assert buttons == ['/status','/report','/accounts','/help']
        assert '/run' not in buttons
    assert 'слишком длинное' in payloads[-1]['text']


@pytest.mark.parametrize('status,body', [(429, {'ok':False}), (500, {}), (200, {'ok':False}), (200, [])])
async def test_api_failures_are_sanitized_and_not_retried(status, body):
    calls = []
    class BadResponse(Response):
        async def json(self): return body
    class Session:
        def post(self, *args, **kwargs):
            calls.append(True)
            response = BadResponse()
            response.status = status
            return response
    api = Telegram(Session(), '123456:'+'syntheticCanary'*3)
    with pytest.raises(RuntimeError, match='transport unavailable'):
        await api.send(42, views.help_message())
    assert len(calls) == 1
