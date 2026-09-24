"""Bounded Telegram HTML views; only escaped, sanitized dynamic values."""
from collections.abc import Mapping
from html import escape

from .journal import sanitize
from .operator_view import lifecycle_lines, result_lines, timestamp, close_result_lines

READ_MENU = {'keyboard': [[{'text': '/status'}, {'text': '/report'}], [{'text': '/accounts'}, {'text': '/help'}]],
             'resize_keyboard': True, 'is_persistent': True}


def text(value, limit=80):
    safe = sanitize(str(value))
    safe = ' '.join(str(safe).split())
    safe = ''.join(c for c in safe if c.isprintable())
    if len(safe) > limit:
        safe = safe[:limit] + '…'
    return escape(safe, quote=False)


def mapping(value):
    return value if isinstance(value, Mapping) else {}


def help_message():
    return ('<b>Управление торговым циклом</b>\n'
            'Один запуск — один цикл по локальной конфигурации.\n\n'
            '<b>Просмотр</b>\n'
            '/status — краткое состояние\n'
            '/report — последний результат и наблюдения\n'
            '/accounts — балансы и позиции на счетах\n'
            '/help — эта инструкция\n\n'
            '<b>Запуск реальной торговли</b>\n'
            '<code>/run</code> — отправить команду на один Mainnet-цикл. '
            'Первый счёт и сторона лимитки выбираются случайно: четыре равновероятных варианта. Объём и время удержания — по текущей конфигурации.\n\n'
            '<code>/close</code> — проверить оба счёта и закрыть текущие позиции настроенного рынка MARKET reduce-only.\n\n'
            '<i>После ошибки /run заново проверяет счета и старые ордера. '
            'После ручного закрытия постоянной блокировки нет. '
            'Пока операция выполняется или прежний ордер не выяснен, новые ордера не отправляются.</i>')


def startup_message():
    return ('<b>🟢 Контроллер подключён</b>\n'
            'Старые команды из очереди отброшены.\n'
            'Посмотреть состояние: /status\n'
            'Команды и порядок работы: /help\n\n'
            '<i>Подключение бота само по себе не запускает торговлю.</i>')


def accepted_message(number):
    return (f'<b>📨 Команда принята</b> · <code>{text(number, 24)}</code>\n'
            'Запрошен один реальный цикл. Это ещё не подтверждение отправки или исполнения ордеров.\n\n'
            'Ход работы: /status · Итог: /report')


def blocked_message():
    return ('<b>⏳ Сейчас другая операция или нужна сверка</b>\n'
            'Параллельные запуски заблокированы. Дождись завершения текущей операции.\n\n'
            '/status — состояние · /accounts — позиции · /close — проверить и закрыть остаток. '
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
    return (f'<b>Последняя сверка: {timestamp(proof["at"])}</b>\n'
            'Оба счёта были без позиций и активных ордеров; старый запрет снят. '
            'Новая команда проверит состояние снова.\n\n')


def unknown_message():
    return ('<b>Команда не распознана</b>\n'
            'Используй /accounts, /status, /report или /help.\n'
            'Для реальных операций <code>/run</code> и <code>/close</code> должны быть без дополнительных параметров.')


def running_message(names, progress=None):
    name = text(', '.join(names[:3])) if names else 'слот ещё не создан'
    details = '\n'.join(text(line, 500) for line in lifecycle_lines(progress))
    return (f'<b>⏳ Цикл выполняется</b> · <code>{name}</code>\n{details}\n'
            'Новые запуски временно заблокированы. /status — обновить')


def empty_message(blocked=False):
    if blocked:
        return blocked_message()
    return ('<b>Готов к приёму команды</b>\n'
            'Запусков через этот контроллер ещё нет.\n\n'
            '/help — порядок работы\n'
            '<i>Готовность контроллера не означает, что счета и рынок прошли проверку.</i>')


def unavailable_message(blocked=False, not_launched=False):
    if not_launched and not blocked:
        return ('<b>Цикл не запущен</b>\nНовый слот не создан. '
                'Это не успешное исполнение. Проверь локальную конфигурацию перед новой командой.\n\n/help — порядок работы')
    return ('<b>⚠️ Отчёт недоступен</b>\n'
            'Состояние ордеров и позиций по этому ответу определить нельзя. '
            'Проверь сохранённый журнал локально.\n\n' +
            ('Следующая /run проверит текущую готовность; /close — закрыть остаток.' if blocked else '/status — состояние контроллера'))


def launch_failure_message(name, code, *, blocked=False):
    reasons = {
        'PRIOR_LEVERAGE_UNRESOLVED': 'Прежняя настройка плеча ещё не сверена.',
        'PRIOR_ORDER_UNRESOLVED': 'Прежняя заявка ещё не сверена.',
        'CREDENTIAL_UNAVAILABLE': 'Доступ к сохранённым ключам не подтверждён.',
        'PREFLIGHT_REFUSED': 'Проверка готовности отказала до начала цикла.',
        'PREPARATION_UNAVAILABLE': 'Подготовка запуска завершилась ошибкой.',
    }
    reason = reasons.get(code)
    if reason is None:
        return unavailable_message(blocked)
    return (f'<b>Цикл {text(name, 32)} не начался</b>\n{reason} '
            'Журнал цикла не создан; исполнение и текущие позиции этим слотом не доказаны. '
            'Автоматического повтора нет. /accounts — проверить текущие счета; '
            '/close — закрыть доказанный остаток.')


def saved_message(name, report, *, blocked=False, detailed=False):
    lines = [f'<b>📋 Результат цикла</b> · <code>{text(name, 32)}</code>']
    if report and report.get('binding', {}).get('receiver_admission') == 'ws_confirmed':
        lines.append('Режим: MARKET после WS-подтверждения лимитки. Приоритет перед чужими заявками не гарантирован.')
    if blocked:
        lines.append('<b>⚠️ Прошлый исход требует сверки</b> · /run проверит готовность заново; /close — закрыть остаток.')
    lines.extend(text(line, 500) for line in result_lines(report, detailed=detailed))
    lines.append('/status — кратко · /report — подробно · /accounts — текущие счета')
    return '\n'.join(lines)


def later_close_message(name, at, flat):
    outcome = '✅ нулевые позиции подтверждены' if flat else '❔ итог закрытия не подтверждён'
    return (f'\n<b>Отдельное закрытие после цикла</b> · <code>{text(name, 32)}</code>\n'
            f'{outcome} по состоянию на {timestamp(at)}. Это сохранённый результат, не текущий снимок счетов. '
            '/accounts — проверить сейчас.')


def accounts_message(result):
    if result is None:
        return ('<b>❔ Счета недоступны</b>\n'
                'Не удалось получить данные. Проверь локальную конфигурацию, '
                'сохранённые ключи и соединение. Повторить: /accounts')
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
              'Проверка не снимает блокировку запуска.</i>', '/accounts — обновить · /help — команды']
    return '\n'.join(lines)
