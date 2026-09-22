"""Bounded Telegram HTML views; only escaped, sanitized dynamic values."""
from collections.abc import Mapping
from html import escape

from .journal import sanitize
from .operator_view import lifecycle_lines, result_lines, timestamp

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
            '<i>Остановка бота не закрывает позиции. При неопределённом результате '
            'новый запуск блокируется до локальной сверки.</i>')


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
    return ('<b>⛔ Новые запуски заблокированы</b>\n'
            'Предыдущий цикл ещё выполняется либо его результат требует локальной сверки.\n\n'
            'Проверь /status и /report. Повторная команда не отправит дополнительные ордера.')


def unknown_message():
    return ('<b>Команда не распознана</b>\n'
            'Используй /accounts, /status, /report или /help.\n'
            'Для одного реального цикла команда <code>/run</code> должна быть без дополнительных параметров.')


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
            ('<b>Новые запуски заблокированы.</b>' if blocked else '/status — состояние контроллера'))


def saved_message(name, report, *, blocked=False, detailed=False):
    lines = [f'<b>📋 Результат цикла</b> · <code>{text(name, 32)}</code>']
    if blocked:
        lines.append('<b>⛔ Новые запуски заблокированы</b>')
    lines.extend(text(line, 500) for line in result_lines(report, detailed=detailed))
    lines.append('/status — кратко · /report — подробно · /accounts — текущие счета')
    return '\n'.join(lines)


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
