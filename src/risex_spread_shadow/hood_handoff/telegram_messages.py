"""Bounded Telegram HTML views; only escaped, sanitized dynamic values."""
from collections.abc import Mapping
from html import escape

from .journal import sanitize
from .operator_view import lifecycle_lines, result_lines, timestamp, close_result_lines

# Reply keyboards: /run and /close only while nothing runs, /stop while an
# owner operation runs. The controller chooses one when each message is sent.
IDLE_MENU = {'keyboard': [[{'text': '/run'}, {'text': '/close'}], [{'text': '/report'}, {'text': '/accounts'}]],
             'resize_keyboard': True, 'is_persistent': True}
RUNNING_MENU = {'keyboard': [[{'text': '/stop'}], [{'text': '/report'}, {'text': '/accounts'}]],
                'resize_keyboard': True, 'is_persistent': True}
# The /run question: Telegram opens a reply to it; the keyboard stays available.
COUNT_PROMPT = {'force_reply': True, 'input_field_placeholder': 'Число циклов, например 10'}


def text(value, limit=80):
    safe = sanitize(str(value))
    safe = ' '.join(str(safe).split())
    safe = ''.join(c for c in safe if c.isprintable())
    if len(safe) > limit:
        safe = safe[:limit] + '…'
    return escape(safe, quote=False)


def mapping(value):
    return value if isinstance(value, Mapping) else {}


def commands_text():
    return ('/run — запустить циклы: ACK, +1 тик, число циклов спрошу\n'
            '/close — проверить и закрыть открытые позиции\n'
            '/stop — остановить циклы и закрыть позиции\n'
            '/report — итог за последние 24 часа\n'
            '/accounts — балансы и позиции')


def startup_message():
    return ('<b>🟢 Контроллер подключён</b>\n'
            'Старые команды из очереди отброшены.\n\n'
            + commands_text() + '\n\n'
            '<i>Подключение бота само по себе не запускает торговлю.</i>')


def commands_message():
    return '<b>Команды</b>\n' + commands_text()


def count_prompt_message(minutes):
    return ('▶️ <b>Запуск: ACK · +1 тик</b>\n'
            'Сколько циклов сделать? Отправь число сообщением, например <code>10</code>.\n'
            f'<i>Жду ответ {text(minutes, 8)} мин. /close или /stop отменяют запуск.</i>')


def count_invalid_message():
    return ('<b>Нужно целое число больше нуля</b>, например <code>10</code>.\n'
            'Отправь число циклов или выбери другую команду.')


def count_expired_message(minutes):
    return (f'<b>Ответ на /run опоздал</b>: число принимается {text(minutes, 8)} мин после вопроса. '
            'Ничего не запущено. Нажми /run ещё раз.')


def day_report_unavailable_message():
    return ('<b>📊 Итог за сутки недоступен</b>\n'
            'Сохранённые журналы не удалось прочитать. Торговля этим не затронута; повтори /report позже.')


def count_without_prompt_message():
    return ('<b>Ничего не запущено</b>\n'
            'Число циклов принимается только в ответ на /run. Нажми /run, затем отправь число.')


def accepted_message(number, series_total=1, details=None):
    request = f'серия из {series_total} циклов' if series_total > 1 else 'один цикл'
    mode = f' · {details}' if details else ''
    return (f'📨 <b>Принято: {request}</b>{mode}\n'
            'Перед каждым циклом проверю нулевые позиции и старые ордера; при остатке сначала закрою его reduce-only. '
            'Кратко сообщу шаги: открываю → открыто → закрываю → закрыто, затем карточка цикла; в конце серии — итог.\n'
            f'<i>Это ещё не исполнение ордеров.</i> Команда <code>{text(number, 24)}</code> · /stop — остановить')


def close_accepted_message(number):
    return (f'<b>📨 Команда /close принята</b> · <code>{text(number, 24)}</code>\n'
            'Проверю счета (с пулом — все кошельки) и закрою подтверждённые остатки reduce-only. '
            'Открытие нового цикла этой командой не запрашивается.')


def blocked_message():
    return ('<b>⏳ Сейчас другая операция или нужна сверка</b>\n'
            'Параллельные запуски заблокированы. Дождись завершения текущей операции.\n\n'
            '/stop — остановить · /accounts — позиции. '
            'Следующая /run снова проверит возможность запуска.')


def recovery_refused_message(reason):
    translations = {
        'positions remain; use /close before /run': 'Есть открытые позиции. Команда /close проверит и закроет их.',
        'previous order is unresolved or still executable': 'Старый ордер ещё активен или его окончательное состояние не подтверждено.',
        'source has active cycle-market orders': 'На первом счёте есть активные ордера этого рынка.',
        'receiver has active cycle-market orders': 'На втором счёте есть активные ордера этого рынка.',
    }
    return ('<b>Новая операция пока не начата</b>\n' + text(translations.get(reason, reason), 260)
            + '\nПостоянного запрета нет: следующая команда повторит проверку. /accounts — текущие позиции.')


def close_message(name, result):
    return '<b>Закрытие позиций</b> · <code>' + text(name, 32) + '</code>\n' + '\n'.join(
        text(line, 500) for line in close_result_lines(result))


def execution_message(notice):
    return '<b>Исполнение заявок</b>\n' + '\n'.join(text(line, 400) for line in notice.splitlines()[:6])


def recovery_checkpoint(proof):
    if not isinstance(proof, dict) or proof.get('status') != 'READY' or timestamp(proof.get('at')) == 'нет данных':
        return ''
    count = proof.get('wallet_count')
    scope = (f'Все {text(count, 8)} кошельков пула были' if type(count) is int and count > 2
             else 'Оба счёта были')
    return (f'<b>Последняя сверка: {timestamp(proof["at"])}</b>\n'
            f'{scope} без позиций и активных ордеров; старый запрет снят. '
            'Новая команда проверит состояние снова.\n\n')


def unknown_message():
    return '<b>Команда не распознана</b>\n' + commands_text()


def running_message(names, progress=None):
    name = text(', '.join(names[:3])) if names else 'слот ещё не создан'
    details = '\n'.join(text(line, 500) for line in lifecycle_lines(progress))
    return (f'<b>⏳ Цикл выполняется</b> · <code>{name}</code>\n{details}\n'
            'Новые запуски временно заблокированы. /stop — остановить')


def empty_message(blocked=False):
    if blocked:
        return blocked_message()
    return ('<b>Готов к приёму команды</b>\n'
            'Запусков через этот контроллер ещё нет.\n'
            '<i>Готовность контроллера не означает, что счета и рынок прошли проверку.</i>')


def unavailable_message(blocked=False, not_launched=False):
    if not_launched and not blocked:
        return ('<b>Цикл не запущен</b>\nНовый слот не создан. '
                'Это не успешное исполнение. Проверь локальную конфигурацию перед новой командой.')
    return ('<b>⚠️ Отчёт недоступен</b>\n'
            'Состояние ордеров и позиций по этому ответу определить нельзя. '
            'Проверь сохранённый журнал локально.\n\n' +
            ('Следующая /run проверит текущую готовность; /close — закрыть остаток.' if blocked else '/accounts — текущие счета'))


def launch_failure_message(name, code, *, blocked=False, detail=None):
    reasons = {
        'PRIOR_LEVERAGE_UNRESOLVED': 'Прежняя настройка плеча ещё не сверена.',
        'PRIOR_ORDER_UNRESOLVED': 'Прежняя заявка ещё не сверена.',
        'CREDENTIAL_UNAVAILABLE': 'Доступ к сохранённым ключам не подтверждён.',
        'PREFLIGHT_REFUSED': 'Проверка готовности отказала до начала цикла.',
        'PREPARATION_UNAVAILABLE': 'Подготовка запуска завершилась ошибкой.',
        'HISTORY_LIMIT': 'История журналов превысила предел проверки.',
        'WALLETS_UNAVAILABLE': 'Готовых кошельков меньше двух — ордера не отправлялись.',
        'WALLET_POOL': 'Файл пула кошельков некорректен; проверь его командой ./wallet list на компьютере.',
    }
    reason = reasons.get(code)
    if reason is None:
        return unavailable_message(blocked)
    if detail:
        reason += '\n' + text(detail, 700)
    return (f'<b>Цикл {text(name, 32)} не начался</b>\n{reason} '
            'Журнал цикла не создан; исполнение и текущие позиции этим слотом не доказаны. '
            'Автоматического повтора нет. /accounts — проверить текущие счета; '
            '/close — закрыть доказанный остаток.')


def saved_message(name, report, *, blocked=False, detailed=False):
    lines = [f'<b>📋 Результат цикла</b> · <code>{text(name, 32)}</code>']
    if report and report.get('binding', {}).get('receiver_admission') == 'ws_confirmed':
        lines.append('Режим: MARKET после WS-подтверждения лимитки. Приоритет перед чужими заявками не гарантирован.')
    if report and report.get('binding', {}).get('receiver_admission') == 'ack':
        lines.append('Режим ACK: MARKET без ожидания WS лимитки; наличие LIMIT перед отправкой не гарантировано. Проверка стакана сохранена.')
    if report and report.get('binding', {}).get('price_improvement_ticks') is not None:
        lines.append('Улучшение цены: ' + text(report['binding']['price_improvement_ticks'], 8) + ' тиков.')
    if blocked:
        lines.append('<b>⚠️ Прошлый исход требует сверки</b> · /run проверит готовность заново; /close — закрыть остаток.')
    lines.extend(text(line, 500) for line in result_lines(report, detailed=detailed))
    lines.append('/accounts — текущие счета')
    return '\n'.join(lines)


def later_close_message(name, at, flat):
    outcome = '✅ нулевые позиции подтверждены' if flat else '❔ итог закрытия не подтверждён'
    return (f'\n<b>Отдельное закрытие после цикла</b> · <code>{text(name, 32)}</code>\n'
            f'{outcome} по состоянию на {timestamp(at)}. Это сохранённый результат, не текущий снимок счетов. '
            '/accounts — проверить сейчас.')


POOL_ACCOUNT_LINES = 40


def _pool_accounts_message(result):
    symbol = text(result['symbol'], 24)
    wallets = mapping(result.get('wallets'))
    paused = set(wallets.get('paused') or [])
    rows = result['accounts']
    lines, quiet = [], 0
    # Wallets needing attention first; the message stays within Telegram's bound.
    ordered = sorted(rows, key=lambda row: (row['snapshot'] is not None and not row['stale']
                                            and row['snapshot'].signed_position == 0
                                            and not row['snapshot'].active_orders and row['snapshot'].ready))
    for row in ordered:
        snapshot = row['snapshot']
        mark = ' ⏸' if row['index'] in paused else ''
        head = f'• <code>{text(row["index"], 24)}</code>{mark}'
        if snapshot is None:
            line = head + ' · ❔ нет данных'
        else:
            balance = snapshot.available_balance
            position = snapshot.signed_position
            line = (head + ' · ' + (text(format(balance, 'f'), 32) if balance is not None else 'баланс ?')
                    + (' · позиция 0' if position == 0 else
                       f' · {"LONG" if position > 0 else "SHORT"} {text(format(abs(position), "f"), 32)}'))
            if snapshot.active_orders:
                line += f' · ордеров {len(snapshot.active_orders)}'
            if row['stale']:
                line += ' · ⚠️ устарело'
            if not snapshot.ready:
                line += ' · ⚠️ не активен'
        if len(lines) < POOL_ACCOUNT_LINES:
            lines.append(line)
        else:
            quiet += 1
    header = [f'<b>💼 Кошельки пула: {len(rows)}</b> · рынок <code>{symbol}</code>',
              f'активных {len(rows) - len(paused)}' + (f', на паузе {len(paused)} (⏸)' if paused else '')
              + ' · баланс — доступный, валюта котировки']
    tail = [f'… и ещё {quiet} кошельков (без проблем в начале списка).'] if quiet else []
    return '\n'.join(header + [''] + lines + tail + [
        '', '<i>Доступный баланс не равен полной стоимости счёта. Снимки получены отдельно и могут измениться; '
            'другие рынки не проверены. Проверка не снимает блокировку запуска.</i>',
        '/accounts — обновить'])


def accounts_message(result):
    if result is None:
        return ('<b>❔ Счета недоступны</b>\n'
                'Не удалось получить данные. Проверь локальную конфигурацию, '
                'сохранённые ключи и соединение. Повторить: /accounts')
    if 'wallets' in result:
        return _pool_accounts_message(result)
    symbol = text(result['symbol'], 24)
    lines = ['<b>💼 Балансы и позиции</b>',
             f'Рынок позиций и ордеров: <code>{symbol}</code>']
    for role, row in zip(('Счёт A', 'Счёт B'), result['accounts']):
        lines += ['', f'<b>{role} · счёт {text(row["index"], 24)}</b>']
        snapshot = row['snapshot']
        if snapshot is None:
            lines += ['❔ Данные недоступны. Баланс, позиция и ордера неизвестны.']
            continue
        lines += ['⚠️ Устаревшее наблюдение' if row['stale'] else '✅ Данные получены',
                  f'<i>{timestamp(snapshot.observed_at)}</i>']
        balance = snapshot.available_balance
        position = snapshot.signed_position
        side = 'LONG' if position > 0 else 'SHORT' if position < 0 else 'нет позиции на этом рынке'
        lines += ['Доступный баланс (валюта котировки): <code>' +
                  (text(format(balance, 'f'), 48) if balance is not None else 'нет данных') + '</code>',
                  f'Позиция: <code>{text(format(position, "f"), 48)} {symbol}</code> · {side}',
                  f'Активных ордеров на рынке: <code>{len(snapshot.active_orders)}</code>']
        if not snapshot.ready:
            lines += ['⚠️ Биржа не подтверждает активный статус счёта.']
    lines += ['', '<i>Доступный баланс не равен полной стоимости счёта. '
              'Снимки получены отдельно и могут измениться; другие рынки не проверены. '
              'Проверка не снимает блокировку запуска.</i>', '/accounts — обновить']
    return '\n'.join(lines)


def admission_refusal_message(record):
    if not isinstance(record, dict) or record.get('status') != 'REFUSED':
        return ''
    stages = {'VALIDATION': 'проверка команды', 'CLOSE_READY': 'сверка старых заявок и позиций',
              'READY': 'подтверждение готовности к открытию', 'PERSISTENCE': 'сохранение команды'}
    reasons = {'POSITIONS_REMAIN': 'Есть открытые позиции. /close — проверить и закрыть остаток.',
               'OPERATOR_BUSY': 'Другая операция удерживает блокировку.',
               'READ_TIMEOUT': 'Истекло время чтения данных.',
               'READ_CONNECTION': 'Не удалось получить данные по соединению.',
               'PREFLIGHT_REFUSED': 'Проверка безопасности не подтвердила готовность.',
               'HISTORY_LIMIT': 'История журналов превысила предел проверки; нужна правка ограничения, повтор не поможет.',
               'WALLET_KEY_MISSING': 'Для кошелька из пула нет сохранённого ключа. На компьютере: ./wallet list — проверить, '
                                     './wallet add НОМЕР — добавить ключ.',
               'WALLET_POOL': 'Файл пула кошельков некорректен. На компьютере: ./wallet list — проверить.',
               'CHECK_FAILED': 'Проверка не завершилась; требуется повторная сверка.'}
    stage = record.get('stage') if isinstance(record.get('stage'), str) else None
    category = record.get('category') if isinstance(record.get('category'), str) else 'CHECK_FAILED'
    return ('<b>Новая операция пока не начата</b>\nКоманда принята, но запуск не состоялся.\n'
            + 'Команда: ' + text(record.get('update_id'), 24) + ' · ' + timestamp(record.get('at')) + '\n'
            + 'Этап: ' + stages.get(stage, 'проверка готовности') + '.\n'
            + reasons.get(category, reasons['CHECK_FAILED']) + '\n'
            + 'Код: ' + text(category, 32) + '.\n'
            + 'По этой команде торговый цикл не запущен. Старая команда не повторяется. '
              'Новая /run заново проверит готовность; /accounts — текущие позиции.')


def stop_accepted_message(*, running, marker=True):
    if running:
        body = ('Новые циклы не начнутся. Идущий цикл не отправит новых ордеров: удержание прервётся, '
                'и он закроет позиции своим обычным закрытием; уже отправленный ордер не обрывается. '
                'Затем проверю все кошельки и при остатке закрою его reduce-only.')
    else:
        body = 'Сейчас ничего не выполняется: проверю позиции на всех кошельках и при остатке закрою его reduce-only.'
    warning = ('' if marker else '\n<b>Отметку остановки для идущего цикла записать не удалось</b>: он завершится '
               'обычным порядком, после чего позиции будут проверены и закрыты.')
    return f'<b>⏹ Команда /stop принята</b>\n{body}{warning}\nРезультат пришлю отдельным сообщением.'


def stop_pending_message():
    return '<b>⏹ Остановка уже выполняется</b>\nРезультат пришлю отдельным сообщением.'


def stop_marker_refusal_message():
    return ('<b>Новая операция не начата</b>\nНе удалось снять отметку прошлой остановки '
            '(stop-request.json рядом с конфигурацией). Проверь файл на компьютере и повтори /run.')


def stop_result_message(record, positions=None):
    record = mapping(record)
    if record.get('result') == 'FLAT':
        closed = record.get('close_slot')
        return ('<b>⏹ Остановлено</b>\n'
                + (f'Остаток закрыт reduce-only: <code>{text(closed, 32)}</code>.\n' if closed else '')
                + f'Позиции: {text(positions or "0", 300)}; активных ордеров нет.\n'
                'Новые циклы не запускаются; /run — начать снова.')
    reason = text(record.get('reason') or 'проверка не завершилась', 160)
    return ('<b>⛔ Остановка: нулевые позиции не подтверждены</b>\n'
            f'{reason}. Новые циклы не запускаются.\n'
            '/accounts — позиции · /close — закрыть остаток · /stop — повторить остановку.')


def stop_status(record, *, running=False):
    """Short state line for a running or the latest completed /stop."""
    if running:
        return ('<b>⏹ Выполняется /stop</b> · новые циклы не начнутся; '
                'идущий цикл закрывает позиции, затем проверка всех кошельков.\n')
    record = mapping(record)
    if not record:
        return ''
    when = timestamp(record.get('completed_at'))
    if record.get('result') == 'FLAT':
        return f'<b>⏹ Остановлено командой /stop</b> · {when}: позиции 0, ордеров нет.\n'
    return (f'<b>⛔ Остановка /stop не подтвердила ноль</b> · {when}: '
            f'{text(record.get("reason") or "проверка не завершилась", 120)}.\n')
