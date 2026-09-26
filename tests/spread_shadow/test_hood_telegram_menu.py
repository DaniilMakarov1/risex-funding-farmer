"""Owner menu 2026-09-26: state keyboards, the /run mode and count questions and the 24-hour /report."""
import asyncio
import json
import re

import pytest

from risex_spread_shadow.hood_handoff import telegram_control as bot
from risex_spread_shadow.hood_handoff import telegram_messages as views
from risex_spread_shadow.hood_handoff.telegram_day_report import day_report, slots_in_window
from test_hood_auto_close_before_run import proof
from test_hood_telegram_control import setup, update
from test_hood_telegram_messages import valid
from test_hood_telegram_series import cycle_slot
from test_hood_telegram_series_report import fill, report

ACK_ONE = {'receiver_admission': 'ack', 'price_improvement_ticks': 1}
IDLE = ['/run', '/close', '/report', '/accounts']
BUSY = ['/stop', '/report', '/accounts']


def buttons(markup):
    return [b['text'] for row in markup['keyboard'] for b in row] if 'keyboard' in markup else markup


def said(c):
    return [text for _, text in c.transport.messages]


def message(number, text, date=1000):
    value = update(number, text)
    value['message']['date'] = date
    return value


async def ask(c, first=1, mode=views.MODE_ONE):
    """Bare /run, then the mode button; the count question is open afterwards."""
    await c.handle(update(first, '/run'))
    await c.handle(update(first + 1, mode))


def series(tmp_path, *, block=None):
    """A controller whose launches record options and leave a flat saved cycle."""
    calls = []
    async def recover(*, require_flat):
        return proof('READY' if require_flat else 'CLOSE_READY')
    async def close():
        calls.append('close')
    async def launch(**options):
        calls.append(options)
        cycle_slot(tmp_path, len([call for call in calls if call != 'close']))
        if block is not None:
            await block.wait()
    c = setup(tmp_path, launch)
    c.recovery, c.close = recover, close
    return c, calls


# ---- keyboards -------------------------------------------------------------

async def test_idle_replies_offer_run_and_close_and_never_removed_commands(tmp_path):
    c, _ = series(tmp_path)
    for number, text in enumerate(['/start', '/accounts', '/status', '/help', 'привет'], 1):
        await c.handle(update(number, text))
    assert [buttons(m) for m in c.transport.markups] == [IDLE] * 5
    replies = said(c)
    # /status and /help are gone: answered like any unknown command, with the command list.
    assert all('Команда не распознана' in text for text in replies[2:])
    for text in replies:
        assert '/status' not in text and '/help' not in text
        assert '/run' in text or 'Счета недоступны' in text
    assert c.task is None and c.store.data['active'] is None


async def test_running_operation_shows_stop_then_idle_menu_returns(tmp_path):
    release = asyncio.Event()
    c, calls = series(tmp_path, block=release)
    await c.handle(update(1, '/run 1'))
    assert buttons(c.transport.markups[0]) == BUSY  # The acceptance is sent while the run is admitted.
    while not calls:
        await asyncio.sleep(0.01)
    assert c.busy() and c.menu() is views.RUNNING_MENU
    await c.handle(update(2, '/accounts'))
    await c.handle(update(3, '/run'))
    assert buttons(c.transport.markups[-1]) == BUSY
    assert 'другая операция' in said(c)[-1] and c._count_prompt is None
    release.set()
    await c.task
    await c._notice_task
    assert not c.busy()
    assert buttons(c.transport.markups[-1]) == IDLE  # The final card arrives with /run and /close.
    await c.handle(update(4, '/close'))
    assert buttons(c.transport.markups[-1]) == BUSY
    await c.task
    await c._notice_task
    assert buttons(c.transport.markups[-1]) == IDLE


async def test_stop_procedure_keeps_stop_menu_until_its_result(tmp_path):
    c, _ = series(tmp_path)
    await c.handle(update(1, '/stop'))
    assert c.busy() and buttons(c.transport.markups[-1]) == BUSY
    await c.task
    await c._notice_task
    assert not c.busy()
    assert 'Остановлено' in said(c)[-1] and buttons(c.transport.markups[-1]) == IDLE


def test_post_run_tail_is_idle_but_pending_stop_is_busy(tmp_path):
    c = setup(tmp_path, None)
    class Running:
        def done(self): return False
    c.task, c._runner_finished = Running(), True
    assert not c.busy() and c.menu() is views.IDLE_MENU
    c._stop = {'update_id': 1, 'requested_at': 1000}
    assert c.busy() and c.menu() is views.RUNNING_MENU


def test_no_view_mentions_removed_commands():
    names = [name for name in dir(views) if name.endswith('_message') and not name.startswith('_')]
    samples = {
        'accepted_message': (1,), 'close_accepted_message': (1,), 'recovery_refused_message': ('x',),
        'close_message': ('close-001', {}), 'execution_message': ('x',), 'running_message': ([],),
        'launch_failure_message': ('cycle-001', 'PREFLIGHT_REFUSED'), 'saved_message': ('cycle-001', {}),
        'later_close_message': ('close-001', 1000, True), 'accounts_message': (None,),
        'admission_refusal_message': ({'status': 'REFUSED'},), 'stop_accepted_message': (),
        'stop_result_message': ({},), 'count_prompt_message': (5,), 'count_expired_message': (5,),
        'mode_prompt_message': (5,),
    }
    keywords = {'stop_accepted_message': {'running': True}}
    rendered = [getattr(views, name)(*samples.get(name, ()), **keywords.get(name, {})) for name in names]
    assert len(rendered) >= 20
    for text in rendered + [views.startup_message(), views.commands_text()]:
        assert '/status' not in text and '/help' not in text
    for markup in (views.IDLE_MENU, views.RUNNING_MENU):
        assert not {'/status', '/help'} & set(buttons(markup))


# ---- the /run mode and count questions -------------------------------------

@pytest.mark.parametrize('answer,count', [('3', 3), (' 2 ', 2), ('1', 1)])
async def test_run_asks_then_a_number_runs_ack_one_tick_series(tmp_path, answer, count):
    c, calls = series(tmp_path)
    await c.handle(update(1, '/run'))
    assert c.task is None and c.store.data['active'] is None and calls == []
    assert c.store.data['offset'] == 2  # The question command is consumed like any other.
    assert c.transport.markups[-1] == views.MODE_PROMPT
    assert 'Какой режим' in said(c)[-1] and 'ACK · +1 тик' in said(c)[-1]
    await c.handle(update(2, views.MODE_ONE))
    assert c.task is None and calls == []
    assert c.transport.markups[-1] == views.COUNT_PROMPT
    assert 'Сколько циклов' in said(c)[-1] and 'ACK · +1 тик · 1 LIMIT → 1 MARKET' in said(c)[-1]
    assert 'count' not in c.store.path.read_text() and 'prompt' not in c.store.path.read_text()
    await c.handle(update(3, answer))
    accepted = said(c)[-1]
    assert ('один цикл' if count == 1 else f'серия из {count} циклов') in accepted
    assert '1 LIMIT → 1 MARKET' in accepted
    await c.task
    assert calls == [ACK_ONE] * count
    last = c.store.data['last']
    assert last['status'] == 'FINISHED' and last['cycle'] == f'cycle-{count:03d}'
    if count > 1:
        assert last['series_total'] == count and last['series_completed'] == count
    assert c._count_prompt is None


@pytest.mark.parametrize('answer', ['0', '-1', '1.5', '05', '+3', '10 циклов', 'abc', '١٢', '３', '9' * 4097])
async def test_invalid_answer_launches_nothing_and_keeps_question(tmp_path, answer):
    c, calls = series(tmp_path)
    await ask(c)
    await c.handle(update(3, answer))
    assert c.task is None and calls == [] and c.store.data['active'] is None
    assert c._count_prompt is not None
    assert 'целое число больше нуля' in said(c)[-1] and c.transport.markups[-1] == views.COUNT_PROMPT
    await c.handle(update(4, '2'))
    await c.task
    assert calls == [ACK_ONE] * 2


@pytest.mark.parametrize('delay,launched', [(300, True), (301, False)])
async def test_answer_after_five_minutes_launches_nothing(tmp_path, delay, launched):
    c, calls = series(tmp_path)
    clock = [1000]
    c.now = lambda: clock[0]
    await c.handle(message(1, '/run', 1000))
    await c.handle(message(2, views.MODE_ONE, 1000))
    clock[0] = 1000 + delay
    await c.handle(message(3, '2', 1000 + delay))
    if launched:
        await c.task
        assert calls == [ACK_ONE] * 2
        return
    assert c.task is None and calls == [] and c._count_prompt is None
    assert 'опоздал' in said(c)[-1] and 'Ничего не запущено' in said(c)[-1]
    await c.handle(message(4, '2', 1000 + delay))
    assert c.task is None and calls == [] and 'только в ответ на /run' in said(c)[-1]


async def test_number_or_text_without_question_launches_nothing(tmp_path):
    c, calls = series(tmp_path)
    await c.handle(update(1, '5'))
    assert 'только в ответ на /run' in said(c)[-1]
    await c.handle(update(2, 'запусти 5'))
    assert 'Команда не распознана' in said(c)[-1]
    assert c.task is None and calls == [] and c.store.data['active'] is None


@pytest.mark.parametrize('command', ['/close', '/stop', '/status', '/run ack 0'])
async def test_other_commands_cancel_the_question(tmp_path, command):
    c, calls = series(tmp_path)
    await c.handle(update(1, '/run'))
    await c.handle(update(2, command))
    if c.task is not None:
        await c.task
        await c._notice_task
    await c.handle(update(3, '2'))
    assert [call for call in calls if call != 'close'] == []
    assert c.task is None or c.task.done()
    assert 'только в ответ на /run' in said(c)[-1]


@pytest.mark.parametrize('command', ['/accounts', '/report'])
async def test_read_only_commands_keep_the_question(tmp_path, command):
    c, calls = series(tmp_path)
    await ask(c)
    await c.handle(update(3, command))
    if c._notice_task is not None:
        await c._notice_task
    await c.handle(update(4, '2'))
    await c.task
    assert calls == [ACK_ONE] * 2


async def test_second_run_renews_the_question(tmp_path):
    c, calls = series(tmp_path)
    clock = [1000]
    c.now = lambda: clock[0]
    await c.handle(message(1, '/run', 1000))
    await c.handle(message(2, views.MODE_ONE, 1000))
    clock[0] = 1200
    await c.handle(message(3, '/run', 1200))
    await c.handle(message(4, views.MODE_ONE, 1200))
    clock[0] = 1450  # 450 s after the first question, 250 s after the renewed one.
    await c.handle(message(5, '2', 1450))
    await c.task
    assert calls == [ACK_ONE] * 2


@pytest.mark.parametrize('change', ['foreign_sender', 'foreign_chat', 'group', 'bot', 'forward', 'edited', 'stale', 'future'])
async def test_untrusted_answer_never_launches_and_owner_can_still_answer(tmp_path, change):
    c, calls = series(tmp_path)
    await ask(c)
    u = update(3, '3')
    if change == 'foreign_sender': u['message']['from']['id'] = 43
    if change == 'foreign_chat': u['message']['chat']['id'] = 43
    if change == 'group': u['message']['chat']['type'] = 'group'
    if change == 'bot': u['message']['from']['is_bot'] = True
    if change == 'forward': u['message']['forward_origin'] = {}
    if change == 'edited': u['edited_message'] = u.pop('message')
    if change == 'stale': u['message']['date'] = 800
    if change == 'future': u['message']['date'] = 1006
    await c.handle(u)
    await asyncio.sleep(0)
    assert c.task is None and calls == [] and c.store.data['active'] is None
    await c.handle(update(4, '1'))
    await c.task
    assert calls == [ACK_ONE]


async def test_duplicate_answer_and_restart_never_replay(tmp_path):
    c, calls = series(tmp_path)
    await ask(c)
    answer = update(3, '2')
    await c.handle(answer)
    await c.handle(answer)  # Redelivered update: already consumed.
    await c.task
    await c.handle(answer)
    await c.handle(update(4, '2'))  # The question was answered once.
    assert calls == [ACK_ONE] * 2
    # A question is never saved: after a restart a number launches nothing.
    await ask(c, 5)
    restarted = setup(tmp_path, c.launch)
    restarted.recovery, restarted.close = c.recovery, c.close
    await restarted.handle(update(7, '2'))
    assert restarted.task is None and calls == [ACK_ONE] * 2
    assert 'только в ответ на /run' in said(restarted)[-1]


async def test_run_while_busy_asks_nothing_and_number_is_refused(tmp_path):
    release = asyncio.Event()
    c, calls = series(tmp_path, block=release)
    await c.handle(update(1, '/run 1'))
    while not calls:
        await asyncio.sleep(0.01)
    await c.handle(update(2, '/run'))
    await c.handle(update(3, '4'))
    assert c._count_prompt is None and 'только в ответ на /run' in said(c)[-1]
    release.set()
    await c.task
    assert calls == [{}]


async def test_blocked_state_without_recovery_asks_nothing(tmp_path):
    c = setup(tmp_path, None)
    c.store.data['active'] = {'before': [], 'update_id': 1}
    c.store.save()
    await c.handle(update(2, '/run'))
    assert 'другая операция' in said(c)[-1] and c._count_prompt is None


@pytest.mark.parametrize('text,expected', [
    ('/run 2', [{}, {}]), ('/run ack 1 2', [ACK_ONE] * 2),
    ('/run ack 5', [{'receiver_admission': 'ack', 'price_improvement_ticks': 5}]),
])
async def test_explicit_run_forms_are_unchanged_and_ask_nothing(tmp_path, text, expected):
    c, calls = series(tmp_path)
    await c.handle(update(1, text))
    assert c.transport.markups[0] != views.COUNT_PROMPT
    await c.task
    assert calls == expected


async def test_answer_goes_through_admission_refusal(tmp_path):
    from risex_spread_shadow.hood_handoff.contracts import PreflightBlocked
    c, calls = series(tmp_path)
    async def refuse(*, require_flat):
        raise PreflightBlocked('source has active cycle-market orders')
    c.recovery = refuse
    await ask(c)
    await c.handle(update(3, '3'))
    await c.task
    assert calls == [] and c.store.data['active'] is None
    assert c.store.data['last_admission']['status'] == 'REFUSED'
    assert 'Новая операция пока не начата' in said(c)[-1]
    # Sent from the finishing admission task: nothing runs, so /run and /close are offered.
    assert buttons(c.transport.markups[-1]) == IDLE


async def test_refusal_with_pending_stop_keeps_stop_menu(tmp_path):
    from risex_spread_shadow.hood_handoff.contracts import PreflightBlocked
    c, calls = series(tmp_path)
    entered, release = asyncio.Event(), asyncio.Event()
    checks = []
    async def refuse(*, require_flat):
        checks.append(require_flat)
        if len(checks) == 1:
            entered.set()
            await release.wait()
            raise PreflightBlocked('source has active cycle-market orders')
        return proof('READY' if require_flat else 'CLOSE_READY')
    c.recovery = refuse
    await c.handle(update(1, '/run 1'))
    await entered.wait()
    await c.handle(update(2, '/stop'))
    release.set()
    while 'Новая операция пока не начата' not in ' '.join(said(c)):
        await asyncio.sleep(0.01)
    refusal = next(i for i, text in enumerate(said(c)) if 'Новая операция пока не начата' in text)
    assert buttons(c.transport.markups[refusal]) == BUSY  # The stop procedure follows.
    await c.task
    await c._notice_task
    assert calls == [] and 'Остановлено' in said(c)[-1] and buttons(c.transport.markups[-1]) == IDLE


# ---- the fan-out mode (owner request 2026-09-26) ---------------------------

FANOUT = {**ACK_ONE, 'fanout': True}


@pytest.mark.parametrize('button', [views.MODE_MANY, ' 1 limit -> НЕСКОЛЬКО market '])
async def test_many_markets_mode_launches_fanout_series(tmp_path, button):
    c, calls = series(tmp_path)
    await c.handle(update(1, '/run'))
    await c.handle(update(2, button))
    assert c.task is None and calls == []
    assert c.transport.markups[-1] == views.COUNT_PROMPT
    assert 'ACK · +1 тик · 1 LIMIT → несколько MARKET' in said(c)[-1]
    await c.handle(update(3, '2'))
    accepted = said(c)[-1]
    assert 'серия из 2 циклов' in accepted and '1 LIMIT → несколько MARKET' in accepted
    await c.task
    assert calls == [FANOUT] * 2
    assert c.store.data['last']['series_completed'] == 2 and c._count_prompt is None


@pytest.mark.parametrize('answer', ['2', '1', 'несколько', '1 LIMIT → 2 MARKET', '/run 2 x', ''])
async def test_mode_question_accepts_only_the_two_buttons(tmp_path, answer):
    c, calls = series(tmp_path)
    await c.handle(update(1, '/run'))
    await c.handle(update(2, answer))
    if answer.startswith('/'):
        # Any other command cancels the question, as for the count question.
        assert c._count_prompt is None
        return
    assert c.task is None and calls == [] and c.store.data['active'] is None
    assert c._count_prompt['stage'] == 'mode'  # A number here is never a cycle count.
    assert 'выбрать режим кнопкой' in said(c)[-1] and c.transport.markups[-1] == views.MODE_PROMPT
    await c.handle(update(3, views.MODE_MANY))
    await c.handle(update(4, '1'))
    await c.task
    assert calls == [FANOUT]


@pytest.mark.parametrize('answered,launched', [(1290, True), (1301, False)])
async def test_mode_answer_opens_a_new_count_window(tmp_path, answered, launched):
    c, calls = series(tmp_path)
    clock = [1000]
    c.now = lambda: clock[0]
    await c.handle(message(1, '/run', 1000))
    clock[0] = answered
    await c.handle(message(2, views.MODE_MANY, answered))
    if not launched:
        assert 'опоздал' in said(c)[-1] and c._count_prompt is None
        return
    clock[0] = 1500  # 500 s after /run, 210 s after the mode answer.
    await c.handle(message(3, '1', 1500))
    await c.task
    assert calls == [FANOUT]


async def test_mode_button_without_question_launches_nothing(tmp_path):
    c, calls = series(tmp_path)
    await c.handle(update(1, views.MODE_MANY))
    assert 'только в ответ на /run' in said(c)[-1]
    assert c.task is None and calls == [] and c.store.data['active'] is None


async def test_controller_launches_the_fanout_child_flag(tmp_path, monkeypatch):
    from pathlib import Path
    from types import SimpleNamespace
    from risex_spread_shadow.hood_handoff import cli, operator_recovery
    tmp_path.chmod(0o700)
    config = tmp_path / 'random-cycle.json'
    config.write_text(json.dumps({'market_id': 1, 'market_symbol': 'BTC',
                                  'source_account_index': 11, 'receiver_account_index': 22,
                                  'cycle_dir': str(tmp_path / 'cycle'), 'api_key_index': 4}))
    (tmp_path / 'market-contract.json').write_text('{}')
    store = bot.Store(tmp_path / '.telegram-control', 'binding')
    calls = []
    class Stop(BaseException): pass
    class API:
        def __init__(self, *args): self.reads = 0
        async def send(self, *args): pass
        async def call(self, method, **payload):
            self.reads += 1
            if self.reads == 1:
                return []
            if self.reads == 2:
                return [update(10, '/run'), update(11, views.MODE_MANY), update(12, '1')]
            for _ in range(100):
                if store.data['active'] is None and calls:
                    raise Stop()
                await asyncio.sleep(0.01)
            pytest.fail('run did not terminate')
    class Session:
        def __init__(self, **kwargs): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
    class Process:
        async def communicate(self, data):
            cycle_slot(tmp_path, len(calls))
    async def create(*args, **kwargs):
        calls.append(args)
        return Process()
    async def recover(*args, require_flat=True):
        return proof('READY' if require_flat else 'CLOSE_READY')
    monkeypatch.setattr(cli, '_validate_simple_local_inputs', lambda *a, **k: (object(), {}))
    monkeypatch.setattr(operator_recovery, 'check_recovery', recover)
    monkeypatch.setattr(bot, 'Telegram', API)
    monkeypatch.setattr(bot.aiohttp, 'ClientSession', Session)
    monkeypatch.setattr(bot.asyncio, 'create_subprocess_exec', create)
    real = bot.Controller
    monkeypatch.setattr(bot, 'Controller', lambda *a, **k: real(*a, **k, now=lambda: 1000))
    original_is_file = Path.is_file
    monkeypatch.setattr(Path, 'is_file',
                        lambda p: True if str(p).endswith('.venv-hood/bin/python') else original_is_file(p))
    with pytest.raises(Stop):
        await bot.serve(SimpleNamespace(config=config, owner_id=42), store, 99, 'unused')
    [argv] = calls
    assert argv[1:5] == ('-m', 'risex_spread_shadow.hood_handoff.cli', 'simple', '--keychain')
    assert argv[-5:] == ('--receiver-admission', 'ack', '--price-improvement-ticks', '1', '--fanout')


# ---- the 24-hour /report ---------------------------------------------------

DAY = 86400
UNTIL = 1790433126  # 26.09.2026 17:32:06 Moscow (14:32:06 UTC).


def slot(root, name, at=None, *, journal_at=None, failure=False):
    path = root / name
    path.mkdir(mode=0o700)
    if at is not None:
        (path / 'launch.json').write_text(json.dumps({'schema': 'hcr-19-simple-launch-v1', 'claimed_at': at,
                                                      'cycle_dir': str(path)}))
    if journal_at is not None:
        event = 'CLOSE_STARTED' if name.startswith('close-') else 'CYCLE_STARTED'
        journal = 'close.jsonl' if name.startswith('close-') else 'cycle.jsonl'
        (path / journal).write_text(json.dumps({'at': journal_at, 'event': event, 'payload': {},
                                                'run_id': 'r', 'sequence': 1}) + '\n')
    if failure:
        (path / 'launch-failure.json').write_text(json.dumps({
            'schema': 'hcr-41-launch-failure-v1', 'code': 'WALLETS_UNAVAILABLE', 'cycle_dir': str(path),
            'inventory': 'UNKNOWN', 'execution': 'UNKNOWN', 'cycle_journal_present': False}))
    return path


def pooled(source, receiver, gross, net, fills=()):
    value = report(gross, net)
    value['binding'].update(source_account_index=source, receiver_account_index=receiver)
    value['confirmed_fills'] = list(fills)
    return value


def lines(text):
    return valid(text).splitlines()


def test_window_edges_claim_time_fallback_and_scan_stop(tmp_path):
    tmp_path.chmod(0o700)
    slot(tmp_path, 'cycle-001')                                   # no saved time, beyond the stop point
    slot(tmp_path, 'cycle-002', UNTIL - DAY - 2 * 3600)          # older than the scan margin: stops the scan
    slot(tmp_path, 'cycle-003', UNTIL - DAY - 1)                  # just before the window
    slot(tmp_path, 'cycle-004', UNTIL - DAY)                      # first second of the window
    slot(tmp_path, 'cycle-005', None, journal_at=UNTIL - 50)      # no launch.json: journal start
    slot(tmp_path, 'cycle-006', UNTIL)                            # last second of the window
    slot(tmp_path, 'cycle-007', UNTIL + 1)                        # claimed after the request
    (tmp_path / 'cycle-008').symlink_to(tmp_path / 'cycle-004')   # never followed
    slot(tmp_path, 'cycle-009', 'bad', journal_at=float('nan'))   # no provable time
    names, unknown = slots_in_window(tmp_path, 'cycle', UNTIL - DAY, UNTIL)
    assert names == ['cycle-004', 'cycle-005', 'cycle-006'] and unknown == 1
    loaded = []
    def load(name):
        loaded.append(name)
        return report()
    text = day_report(tmp_path, UNTIL, load)
    assert loaded == ['cycle-004', 'cycle-005', 'cycle-006']
    assert 'Циклов: 3 · ✅ 3 · 🟡 0 · ⛔ 0' in lines(text)
    assert 'Без сохранённого времени и не в итоге: 1' in lines(text)


def test_moscow_window_label_is_utc_plus_three(tmp_path):
    tmp_path.chmod(0o700)
    text = lines(day_report(tmp_path, UNTIL, lambda name: pytest.fail('nothing to load')))
    # 1790433126 = 2026-09-26 14:32:06 UTC; Moscow is UTC+3 all year.
    assert text[:3] == ['📊 Итог за сутки', '25.09 17:32 → 26.09 17:32 МСК (последние 24 часа)',
                        'Завершённых циклов за это время нет.']
    assert len(text) == 3


def test_pool_pairs_are_all_summed_with_independent_totals(tmp_path):
    tmp_path.chmod(0o700)
    reports = {
        'cycle-001': pooled(27331, 34019, '0.03', '0.02', [
            fill(27331, 'a', '0.001', '50000'), fill(34019, 'a', '0.001', '50000', 'SELL')]),
        'cycle-002': pooled(34020, 27337, '-0.07', '-0.09', [
            fill(34020, 'b', '0.0004', '50100', 'SELL'), fill(27337, 'b', '0.0004', '50100')]),
        'cycle-003': pooled(27337, 34019, '0.005', '0.005', [fill(27337, 'c', '0.0002', '49950')]),
    }
    reports['cycle-003']['paired_execution']['status'] = 'FAILED'
    for number, name in enumerate(reports, 1):
        slot(tmp_path, name, UNTIL - 3600 * number)
    text = lines(day_report(tmp_path, UNTIL, reports.get))
    # 50 + 50 + 20.04 + 20.04 + 9.99 = 150.07; gross 0.03 - 0.07 + 0.005; net 0.02 - 0.09 + 0.005.
    assert 'Циклов: 3 · ✅ 2 · 🟡 1 · ⛔ 0' in text
    assert 'Оборот: 150.07 $' in text
    assert 'PnL до комиссий: −0.0350 $' in text
    assert 'PnL после комиссий: −0.0650 $' in text
    assert 'Комиссии: 0.0300 $' in text
    assert not any('по ' in line and 'из 3' in line for line in text)


def test_unknown_cycles_are_named_and_partial_sums_say_so(tmp_path):
    tmp_path.chmod(0o700)
    reports = {'cycle-001': report('0.03', '0.02'), 'cycle-002': report('-0.07', None),
               'cycle-003': report('0.01', '0.01'), 'cycle-004': None}
    reports['cycle-003']['inventory']['status'] = 'OPEN_INVENTORY'
    for number, name in enumerate(reports, 1):
        slot(tmp_path, name, UNTIL - 60 * number)
    text = lines(day_report(tmp_path, UNTIL, reports.get))
    assert 'Циклов: 4 · ✅ 2 · 🟡 0 · ⛔ 2' in text
    assert '⛔ требуют сверки: cycle-003, cycle-004' in text
    assert 'PnL до комиссий: −0.0400 $ — по 2 из 4 циклов' in text
    assert 'PnL после комиссий: +0.0200 $ — по 1 из 4 циклов' in text
    assert 'Комиссии: неизвестны' in text
    assert 'Оборот: 0 $ — по 3 из 4 циклов' in text  # Settled cycles with no fills; one is unreadable.


def test_zero_tariff_day_fee_line(tmp_path):
    tmp_path.chmod(0o700)
    def tariff(value):
        return {**value, 'fee': None, 'fee_evidence': 'MISSING_OR_INVALID_COMPONENTS',
                'venue_fee_raw': None, 'integrator_fee_raw': None}
    first, second = report('0.01', None), report('-0.02', None)
    first['confirmed_fills'] = [tariff(fill(11, 'x', '0.001', '50000'))]
    second['confirmed_fills'] = [tariff(fill(22, 'y', '0.001', '50000', 'SELL'))]
    slot(tmp_path, 'cycle-001', UNTIL - 10)
    slot(tmp_path, 'cycle-002', UNTIL - 5)
    text = lines(day_report(tmp_path, UNTIL, {'cycle-001': first, 'cycle-002': second}.get))
    assert 'PnL после комиссий: −0.0100 $' in text
    assert 'Комиссии: 0 $ (тариф 0%)' in text
    second['confirmed_fills'][0]['venue_fee_raw'] = 3
    text = lines(day_report(tmp_path, UNTIL, {'cycle-001': first, 'cycle-002': second}.get))
    assert 'PnL после комиссий: +0.0100 $ — по 1 из 2 циклов' in text
    assert 'Комиссии: неизвестны' in text


def test_launch_failures_running_cycle_and_closes(tmp_path):
    tmp_path.chmod(0o700)
    slot(tmp_path, 'cycle-001', UNTIL - 300)
    slot(tmp_path, 'cycle-002', UNTIL - 200, failure=True)
    slot(tmp_path, 'cycle-003', UNTIL - 100)
    slot(tmp_path, 'close-001', journal_at=UNTIL - DAY - 10)
    slot(tmp_path, 'close-002', journal_at=UNTIL - 400)
    slot(tmp_path, 'close-003', journal_at=UNTIL - 250)
    def load(name):
        assert name == 'cycle-001', 'a launch failure or the running cycle is never loaded'
        return report()
    text = lines(day_report(tmp_path, UNTIL, load, running='cycle-003', step=(3, 10)))
    assert '⏳ Сейчас идёт цикл 3/10 (cycle-003) — войдёт в итог после завершения' in text
    assert 'Циклов: 1 · ✅ 1 · 🟡 0 · ⛔ 0' in text
    assert 'Не начались (ордера не отправлялись): 1' in text
    assert 'Отдельных закрытий позиций: 2 — в PnL и оборот не входят' in text
    text = lines(day_report(tmp_path, UNTIL, lambda name: report(), step=(2, 5)))
    assert '⏳ Сейчас идёт цикл 2/5' in text  # Between cycles: nothing is excluded.
    assert 'Циклов: 2 · ✅ 2 · 🟡 0 · ⛔ 0' in text


def test_day_report_has_no_footnote_and_bounded_html(tmp_path):
    tmp_path.chmod(0o700)
    for number in range(1, 401):
        slot(tmp_path, f'cycle-{number:03d}', UNTIL - number)
    bad = {f'cycle-{n:03d}' for n in range(1, 401, 2)}
    text = day_report(tmp_path, UNTIL, lambda name: None if name in bad else report())
    plain = valid(text)
    assert len(text) < 2000 and 'и ещё 190' in plain
    assert 'USDG' not in plain and 'фандинг' not in plain and 'оборот =' not in plain


async def test_report_command_renders_day_off_the_loop_and_excludes_running_cycle(tmp_path):
    release = asyncio.Event()
    c, calls = series(tmp_path, block=release)
    clock = [UNTIL]
    c.now = lambda: clock[0]
    c.started = 0
    slot(tmp_path, 'cycle-001', UNTIL - 7200)
    reports = {'cycle-001': report('0.03', '0.02')}
    c.report = reports.get
    async def launch(**options):
        calls.append(options)
        slot(tmp_path, 'cycle-002', UNTIL - 10)
        await release.wait()
    c.launch = launch
    await c.handle(message(1, '/run 1', UNTIL))
    while not calls:
        await asyncio.sleep(0.01)
    await c.handle(message(2, '/report', UNTIL))
    while not any('Итог за сутки' in text for text in said(c)):
        await asyncio.sleep(0.01)
    day = next(text for text in said(c) if 'Итог за сутки' in text)
    assert '⏳ Сейчас идёт цикл (<code>cycle-002</code>) — войдёт в итог после завершения' in day
    assert 'Циклов: <b>1</b> · ✅ 1' in day and 'PnL после комиссий: <b>+0.0200 $</b>' in day
    release.set()
    await c.task


async def test_day_report_failure_is_a_fixed_message(tmp_path, monkeypatch):
    from risex_spread_shadow.hood_handoff import telegram_day_report
    def broken(*args, **kwargs):
        raise OSError('/private/secret path')
    monkeypatch.setattr(telegram_day_report, 'day_report', broken)
    c = setup(tmp_path, None)
    await c.handle(update(1, '/report'))
    await c._notice_task
    assert 'Итог за сутки недоступен' in said(c)[-1] and 'secret' not in said(c)[-1]


def test_moscow_labels_on_cycle_clock_times():
    from risex_spread_shadow.hood_handoff.telegram_cards import headline
    value = report()
    value['latency'] = {'cycle': {'terminal_at': 1790433119.0,
                                  'phase_intervals': [{'phase': 'selection', 'start_at': 1790432976.0, 'seconds': 1}]}}
    # 14:29:36 and 14:31:59 UTC are 17:29:36 and 17:31:59 in Moscow.
    assert re.search(r'17:29:36 → 17:31:59 МСК · 2 мин 23 с', headline({'index': 1, 'total': 1}, value))
