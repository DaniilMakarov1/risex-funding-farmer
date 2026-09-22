"""Bounded Telegram HTML views; only escaped, sanitized dynamic values."""
from collections.abc import Mapping
from datetime import datetime, timezone
from html import escape
import math

from .journal import sanitize

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


def timestamp(value):
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        return 'нет данных'
    try:
        if not math.isfinite(value) or value < 0:
            return 'нет данных'
        return datetime.fromtimestamp(value, timezone.utc).strftime('%d.%m.%Y %H:%M:%S UTC')
    except (ValueError, OverflowError, OSError):
        return 'нет данных'


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
            'Объём и время удержания выбираются по текущей конфигурации.\n\n'
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


def running_message(names):
    name = text(', '.join(names[:3])) if names else 'слот ещё не создан'
    return (f'<b>⏳ Цикл выполняется</b>\nЦикл: <code>{name}</code>\n\n'
            'Итог ещё не доказан. Новые запуски временно заблокированы.\n'
            'Обновить: /status')


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
    report = mapping(report)
    inventory = mapping(report.get('inventory'))
    pair = mapping(report.get('paired_execution'))
    economics = mapping(report.get('economics'))
    fees = mapping(economics.get('fees'))
    orders = mapping(report.get('order_state'))
    lines = [f'<b>📋 Результат цикла</b> · <code>{text(name, 32)}</code>']
    if blocked:
        lines += ['<b>⛔ Новые запуски заблокированы</b>']
    lines += ['', '<b>Результат по журналу</b>',
              'Полнота данных: ' + ('полные' if report.get('status') == 'COMPLETE' else 'неполные'),
              'Парное исполнение: ' + {'SUCCESS': '✅ подтверждено', 'FAILED': '⚠️ не состоялось'}.get(pair.get('status'), '❔ не доказано'),
              'Историческая позиция: ' + {'CONFIRMED_FLAT': '✅ закрытие подтверждено', 'PARTIAL': '⚠️ есть остаток'}.get(inventory.get('status'), '❔ неизвестна'),
              'Комиссии: ' + ('подтверждены' if fees.get('status') == 'PROVEN' else '❔ неполные данные')]
    intents = orders.get('unresolved_intents')
    observed = orders.get('unresolved_observed_orders')
    lines += ['Неразрешённых намерений: ' + (str(len(intents)) if isinstance(intents, list) else 'нет данных'),
              'Неразрешённых наблюдений ордеров: ' + (str(len(observed)) if isinstance(observed, list) else 'нет данных')]
    if detailed:
        times = mapping(inventory.get('observed_at'))
        lines += ['', '<b>Последние сохранённые позиции</b>']
        for key, label in [('source', 'Источник'), ('receiver', 'Приёмник')]:
            value = inventory.get(key)
            lines += [f'{label}: <code>{text(value if value is not None else "нет данных", 48)}</code>',
                      f'<i>{timestamp(times.get(key))}</i>']
        reasons = report.get('reasons')
        if isinstance(reasons, list) and reasons:
            lines += ['', '<b>Диагностика журнала</b>']
            lines += ['• ' + text(reason, 180) for reason in reasons[:2]]
            if len(reasons) > 2:
                lines += ['Остальные причины — в локальном отчёте.']
        lines += ['', '<i>Комиссии не равны итоговой прибыли. Фандинг и PnL требуют отдельного доказательства.</i>']
    lines += ['', '<i>Это сохранённые наблюдения, не текущая проверка счетов.</i>',
              '/status — кратко · /report — подробно · /help — команды']
    return '\n'.join(lines)


def accounts_message(result):
    if result is None:
        return ('<b>❔ Счета недоступны</b>\n'
                'Не удалось получить данные. Проверь локальную конфигурацию, '
                'сохранённые ключи и соединение. Повторить: /accounts')
    symbol = text(result['symbol'], 24)
    lines = ['<b>💼 Балансы и позиции</b>',
             f'Рынок позиций и ордеров: <code>{symbol}</code>']
    for role, row in zip(('Источник', 'Приёмник'), result['accounts']):
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
