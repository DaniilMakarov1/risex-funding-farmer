"""Read-only, bounded Russian lifecycle views shared by terminal and Telegram."""
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import json
import math
from pathlib import Path

from .journal import sanitize

MSK = timezone(timedelta(hours=3))


def clean(value, limit=120):
    value = ' '.join(str(sanitize(str(value))).split())
    value = ''.join(c for c in value if c.isprintable())
    return value[:limit] + ('…' if len(value) > limit else '')


def mapping(value):
    return value if isinstance(value, Mapping) else {}


def number(value):
    try:
        result = Decimal(str(value))
        return result if result.is_finite() else None
    except (InvalidOperation, ValueError, TypeError):
        return None


def amount(value):
    parsed = number(value)
    if parsed is None:
        return 'неизвестно'
    if not parsed:
        return '0'
    if abs(parsed.adjusted()) > 100:
        return clean(str(parsed), 48)
    rendered = format(parsed, 'f')
    return rendered.rstrip('0').rstrip('.') if '.' in rendered else rendered


def timestamp(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 'нет данных'
    try:
        if not math.isfinite(value) or value < 0:
            return 'нет данных'
        return datetime.fromtimestamp(value, MSK).strftime('%d.%m %H:%M:%S МСК')
    except (ValueError, OverflowError, OSError):
        return 'нет данных'


def holding(payload, at=None):
    start = payload.get('anchor_wall', at)
    seconds = payload.get('hold_seconds')
    if timestamp(start) == 'нет данных' or isinstance(seconds, bool) or not isinstance(seconds, (int, float)) or not math.isfinite(seconds) or not 0 <= seconds <= 300:
        return {}
    return {'seconds': seconds, 'started_at': start, 'planned_closing_at': start + seconds}


def hold_line(value):
    value = mapping(value)
    if not value:
        return 'Удержание не началось или не подтверждено.'
    return (f'Удержание {amount(value.get("seconds"))} с с {timestamp(value.get("started_at"))}; '
            f'плановое начало закрытия — {timestamp(value.get("planned_closing_at"))}.')


def position_line(account, position, symbol):
    q = number(position)
    if q is None:
        return f'Счёт {clean(account, 24)}: позиция неизвестна ({clean(position, 48)}).'
    side = 'LONG' if q > 0 else 'SHORT' if q < 0 else 'закрыта'
    return f'Счёт {clean(account, 24)}: {side} {amount(abs(q))} {clean(symbol, 24)}.'


def read_lifecycle(path):
    """Read only this cycle's parent journal; no account, network or key access.

    Incomplete trailing writes are retried on the next observation. Corruption
    or an oversized journal makes progress unavailable, never authorizes work.
    """
    path = Path(path)
    if path.is_symlink():
        return None
    state = {'stage': 'PREPARING', 'binding': {}, 'opening': {}, 'holding': {}}
    run_id = None
    previous_at = -1
    total = 0
    try:
        with path.open(encoding='utf-8') as stream:
            for index in range(1, 10001):
                line = stream.readline(2 * 1024 * 1024 + 1)
                if not line:
                    return state if run_id else None
                total += len(line)
                if total > 8 * 1024 * 1024 or len(line) > 2 * 1024 * 1024:
                    return None
                if not line.endswith('\n'):
                    return state if run_id else None
                row = json.loads(line)
                at = row.get('at')
                if type(row.get('sequence')) is not int or row['sequence'] != index or timestamp(at) == 'нет данных' or at < previous_at:
                    return None
                previous_at = at
                if index == 1:
                    run_id = row.get('run_id')
                    if row.get('event') != 'CYCLE_STARTED' or not isinstance(run_id, str) or not run_id:
                        return None
                if row.get('run_id') != run_id:
                    return None
                event, payload = row.get('event'), mapping(row.get('payload'))
                if event == 'CYCLE_STARTED':
                    state['binding'] = dict(mapping(payload.get('binding')))
                elif event == 'OPENING_COMPLETE':
                    state['opening'] = {}
                    result = mapping(payload.get('result'))
                    a, b = mapping(result.get('source')), mapping(result.get('receiver'))
                    q = number(mapping(result.get('joint_trade_match')).get('quantity'))
                    positions = [number(leg.get('position_after')) for leg in (a, b)]
                    if (result.get('mutual_execution_proven') is True and q is not None and q > 0
                            and mapping(result.get('joint_trade_match')).get('status') == 'MATCHED'
                            and a.get('account_index') != b.get('account_index')
                            and all(mapping(state['binding']).get(f'{role}_account_index') == leg.get('account_index') for role, leg in (('source', a), ('receiver', b)))
                            and all(leg.get('history_complete') is True and leg.get('dispatched') is True
                                    and number(leg.get('position_before')) == 0
                                    and number(leg.get('filled_quantity')) == q for leg in (a, b))
                            and all(p is not None and abs(p) == q for p in positions)
                            and sum(positions) == 0):
                        state['opening'] = {'source': dict(a), 'receiver': dict(b), 'at': at}
                elif event == 'HOLD_ANCHORED' and state['opening']:
                    interval = holding(payload, at)
                    if interval and interval['started_at'] >= state['opening']['at']:
                        state.update(stage='HOLD', holding=interval)
                elif event == 'CLOSING_PLAN_READY':
                    state['stage'] = 'CLOSING'
                elif event in {'FALLBACK_DISPATCH_INTENT', 'FALLBACK_ATTEMPT_STARTED'}:
                    state['stage'] = 'RECOVERY'
                elif event == 'CYCLE_COMPLETE':
                    state['stage'] = 'COMPLETE'
    except (OSError, ValueError, TypeError, AttributeError):
        return None
    return None


def lifecycle_lines(state):
    state = mapping(state)
    stage = state.get('stage')
    labels = {'HOLD': 'Позиции открыты между нашими счетами.', 'CLOSING': 'Началось закрытие позиций.',
              'RECOVERY': 'Идёт отдельное закрытие остатка.', 'COMPLETE': 'Цикл завершён; итог сверяется.'}
    lines = [labels.get(stage, 'Подготовка и открытие; парное исполнение ещё не подтверждено.')]
    opening = mapping(state.get('opening'))
    if opening:
        if stage != 'HOLD':
            lines.append('Позиции при открытии (не текущий снимок):')
        symbol = mapping(state.get('binding')).get('market_symbol', '')
        for leg in ('source', 'receiver'):
            value = mapping(opening.get(leg))
            lines.append(position_line(value.get('account_index'), value.get('position_after'), symbol))
        lines.append(hold_line(state.get('holding')))
        lines.append('Время полного закрытия зависит от исполнения и сверки.')
    return lines


def result_lines(report, *, detailed=False):
    report = mapping(report)
    inventory, pair = mapping(report.get('inventory')), mapping(report.get('paired_execution'))
    economics, binding = mapping(report.get('economics')), mapping(report.get('binding'))
    fees, pnl = mapping(economics.get('fees')), mapping(economics.get('closed_execution_pnl'))
    lines = []
    if binding.get('source_account_index') is not None:
        side = {'LONG': 'SELL', 'SHORT': 'BUY'}.get(binding.get('direction'), '?')
        lines.append(f'Первый счёт: {clean(binding.get("source_account_index"), 24)} · лимитный {side} (A); '
                     f'Второй счёт: {clean(binding.get("receiver_account_index"), 24)} (B).')
    lines += [
        'Парное исполнение: ' + {'SUCCESS': '✅ подтверждено', 'FAILED': '⚠️ не состоялось'}.get(pair.get('status'), '❔ не доказано'),
        'Историческая позиция: ' + {'CONFIRMED_FLAT': '✅ закрытие подтверждено', 'PARTIAL': '⚠️ есть остаток'}.get(inventory.get('status'), '❔ неизвестна'),
    ]
    matches = pair.get('direct_counterparty_match')
    for value in matches[:2] if isinstance(matches, list) else ():
        value = mapping(value)
        label = {'opening': 'Открытие', 'closing': 'Закрытие'}.get(value.get('phase'), 'Фаза')
        lines.append(f'{label}: свой объём {amount(value.get("matched_quantity"))}; '
                     f'внешний A/B {amount(value.get("external_source_quantity"))}/{amount(value.get("external_receiver_quantity"))}; '
                     f'не доказан A/B {amount(value.get("unproved_source_quantity"))}/{amount(value.get("unproved_receiver_quantity"))}.')
    lines.append(hold_line(report.get('holding')))
    lines.append(f'Завершение: {timestamp(mapping(report.get("cycle")).get("terminal_at"))}.')
    lines.append('Комиссии: ' + (amount(fees.get('total')) if fees.get('status') == 'PROVEN' else 'неполные данные, сумма неизвестна') + ' (валюта котировки).')
    if fees.get('status') != 'PROVEN' and any(mapping(f).get('fee_evidence') == 'NONZERO_UNIT_UNVERIFIED' for f in report.get('confirmed_fills', [])):
        lines.append('Ненулевые поля комиссий сохранены; единица их пересчёта ещё не подтверждена.')
    lines.append('PnL сделок до комиссий: ' + amount(pnl.get('gross')) + '; после комиссий: ' + amount(pnl.get('net')) + '.')
    lines.append('PnL указан в валюте котировки, без фандинга. Итог с фандингом неизвестен.')
    if detailed or inventory.get('status') != 'CONFIRMED_FLAT':
        lines.append('Последние сохранённые позиции:')
        times = mapping(inventory.get('observed_at'))
        for role in ('source', 'receiver'):
            lines.append(position_line(binding.get(f'{role}_account_index', role), inventory.get(role), binding.get('market_symbol', ''))
                         + ' ' + timestamp(times.get(role)))
    if detailed:
        actions = report.get('dispatched_actions', [])
        if report.get('status') == 'COMPLETE' and pair.get('status') != 'SUCCESS':
            for phase, label in (('opening', 'открытия'), ('closing', 'закрытия')):
                planned = [a for a in report.get('planned_actions', []) if a.get('phase') == phase]
                if planned:
                    sent = [a for a in actions if a.get('phase') == phase and a.get('leg') == 'receiver']
                    state = ('ордер отправлен' if any(a.get('dispatched') is True for a in sent)
                             else 'отправка не доказана' if sent else 'ордер не отправлялся')
                    lines.append(f'Приёмник {label}: {state}.')
            if any(a.get('phase') == 'fallback' for a in actions):
                lines.append('Парная операция не завершена; восстановление продолжено через fallback (закрытие остатка).')
        for row in pnl.get('per_account', [])[:2]:
            lines.append(f'Счёт {clean(row.get("account_index"), 24)}: PnL до комиссий {amount(row.get("gross"))}, '
                         f'комиссии {amount(row.get("fees"))}, после {amount(row.get("net"))}.')
        orders = mapping(report.get('order_state'))
        for key, label in (('unresolved_intents', 'Неразрешённых намерений'), ('unresolved_observed_orders', 'Неразрешённых ордеров')):
            value = orders.get(key)
            lines.append(f'{label}: {len(value) if isinstance(value, list) else "нет данных"}.')
        if report.get('status') != 'COMPLETE':
            lines.append('Данные неполные; необходима локальная сверка журнала.')
            for reason in report.get('reasons', [])[:2]:
                lines.append('Диагностика журнала: ' + clean(reason, 180))
    lines.append('Это сохранённые наблюдения, не текущая проверка счетов.')
    return lines
