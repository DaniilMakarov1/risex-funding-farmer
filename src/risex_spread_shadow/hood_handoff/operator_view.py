"""Read-only, bounded Russian lifecycle views shared by terminal and Telegram."""
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import json
import math
import os
import re
from pathlib import Path

from .journal import sanitize

MSK = timezone(timedelta(hours=3))

LAUNCH_FAILURE_CODES = frozenset({
    'PRIOR_LEVERAGE_UNRESOLVED', 'PRIOR_ORDER_UNRESOLVED',
    'CREDENTIAL_UNAVAILABLE', 'PREFLIGHT_REFUSED',
    'PREPARATION_UNAVAILABLE',
})


def read_launch_failure(slot):
    """Read a bounded, fixed-code refusal before any cycle journal existed."""
    slot = Path(slot)
    path = slot / 'launch-failure.json'
    if slot.is_symlink() or path.is_symlink() or (slot / 'cycle.jsonl').exists():
        return None
    try:
        if path.stat().st_size > 4096:
            return None
        value = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError, TypeError):
        return None
    stored_slot = value.get('cycle_dir') if isinstance(value, dict) else None
    if (not isinstance(value, dict) or not isinstance(value.get('code'), str)
            or not isinstance(stored_slot, str) or not Path(stored_slot).is_absolute()
            or value.get('schema') != 'hcr-41-launch-failure-v1'
            or os.path.abspath(stored_slot) != os.path.abspath(slot)
            or value.get('code') not in LAUNCH_FAILURE_CODES
            or value.get('inventory') != 'UNKNOWN'
            or value.get('execution') != 'UNKNOWN'
            or value.get('cycle_journal_present') is not False):
        return None
    return value['code']


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


def opening_margin_refusal(reason):
    """Render only our structured pre-send shortfall; never infer venue fees."""
    if not isinstance(reason, str):
        return None
    matched = re.search(
        r'(source|receiver) selected quantity exceeds fresh free-balance margin model '
        r'\(account ([0-9]{1,12}); shortfall ([0-9]+(?:\.[0-9]+)?) quote\)',
        reason[:500],
    )
    if matched is None:
        return None
    role = 'первом' if matched[1] == 'source' else 'втором'
    return (f'Открытие остановлено до ордера: на {role} счёте {matched[2]} '
            f'не хватает {amount(matched[3])} в валюте баланса для расчётной маржи и запаса.')


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


def ws_admission_notice(reason):
    reason = str(reason or '')
    if 'WS_ADMISSION_STOP:' not in reason:
        return None
    descriptions = {
        'L2 shows better-priced volume': 'в стакане появилась более выгодная цена',
        'L2 shows extra volume at source price': 'по нашей цене виден дополнительный объём',
        'L2 source level is smaller than exact private remainder': 'объём публичного уровня противоречит состоянию нашей заявки',
        'L2 best level differs from source price/quantity': 'публичный стакан не совпал с нашей заявкой; детали старого отказа не сохранены',
    }
    for marker, text in descriptions.items():
        if marker in reason:
            return 'Встречный MARKET остановлен: ' + text + '.'
    return 'Встречный MARKET остановлен: актуальное согласованное WS-состояние не подтверждено.'


def execution_lines(result, *, phase, attempt=1):
    """Describe exact terminal legs; never infer own matching from positions."""
    from .contracts import LegReconciliation, OrderPlan, OrderSnapshot, TradeReceipt
    from .engine import _joint_trade_match
    from .random_cycle import _fallback_order_mismatch_map

    result = mapping(result)
    plans = mapping(result.get('plan'))
    legs = {}
    for role in ('source', 'receiver'):
        value = mapping(result.get(role))
        try:
            plan = OrderPlan(**plans[role])
            trades = tuple(TradeReceipt.from_mapping(t) for t in value['trades'])
            order = OrderSnapshot.from_mapping(value['order']) if value['order'] is not None else None
            before, after = number(value['position_before']), number(value['position_after'])
            qty = sum((t.quantity for t in trades), Decimal(0))
            if (value.get('history_complete') is not True or value.get('unknown_reasons') != []
                    or type(value.get('dispatched')) is not bool or before is None or after is None
                    or value.get('account_index') != plan.account_index
                    or before != number(plans.get(f'{role}_position_before'))
                    or qty != number(value.get('filled_quantity'))
                    or after != before + qty * (1 if plan.side == 'BUY' else -1)
                    or len({t.trade_id for t in trades}) != len(trades)):
                raise ValueError('unproved leg')
            if value['dispatched']:
                if (order is None or value.get('order_id') != order.order_id or not order.terminal
                        or _fallback_order_mismatch_map(order, plan) or order.filled_quantity != qty):
                    raise ValueError('unproved terminal order')
            elif order is not None or trades or qty != 0:
                raise ValueError('unsent leg has fills')
            for t in trades:
                if (t.account_index != plan.account_index or t.market_id != plan.market_id or t.side != plan.side
                        or t.order_id != order.order_id or t.observed_at > order.observed_at
                        or (t.client_order_index is not None and str(t.client_order_index) != str(plan.client_order_index))
                        or not (t.price <= plan.price if plan.side == 'BUY' else t.price >= plan.price)):
                    raise ValueError('conflicting receipt')
            legs[role] = LegReconciliation(plan.account_index, None if order is None else order.order_id,
                trades, before, after, order, True, dispatched=value['dispatched'])
        except (KeyError, ValueError, TypeError, AttributeError):
            legs[role] = None
    matched = Decimal(0)
    if (all(legs.values()) and legs['source'].account_index != legs['receiver'].account_index
            and mapping(plans.get('source')).get('market_id') == mapping(plans.get('receiver')).get('market_id')
            and mapping(plans.get('source')).get('side') != mapping(plans.get('receiver')).get('side')):
        _, matched, _ = _joint_trade_match(legs['source'], legs['receiver'], number(plans.get('quantity')) or Decimal(0))
    label = 'Открытие' if phase == 'opening' else 'Закрытие'
    lines = [f'{label} · попытка {clean(attempt, 8)}']
    for role, kind in (('source', 'LIMIT'), ('receiver', 'MARKET')):
        leg = legs[role]
        peer = mapping(plans.get('receiver' if role == 'source' else 'source')).get('account_index')
        account = mapping(plans.get(role)).get('account_index', '?')
        prefix = f'{kind} · счёт {clean(account, 24)}: '
        if leg is None:
            lines.append(prefix + 'исполнение и контрагент не доказаны.')
        elif not leg.dispatched:
            lines.append(prefix + 'не отправлялся.')
        elif not leg.trades:
            status = clean(leg.order.status, 48) if leg.order is not None else 'неизвестен'
            lines.append(prefix + f'исполнений нет; статус заявки: {status}.')
        else:
            external = sum((t.quantity for t in leg.trades if t.counterparty_account_index is not None
                            and peer is not None and t.counterparty_account_index != peer), Decimal(0))
            unproved = max(Decimal(0), leg.filled_quantity - external - matched)
            pieces = []
            if matched:
                pieces.append(f'наш парный счёт — {amount(matched)}')
            if external:
                ids = sorted({t.counterparty_account_index for t in leg.trades
                              if t.counterparty_account_index is not None and t.counterparty_account_index != peer})
                pieces.append(f'внешние счета {", ".join(str(i) for i in ids[:3])} — {amount(external)}')
            if unproved:
                pieces.append(f'контрагент не доказан — {amount(unproved)}')
            lines.append(prefix + '; '.join(pieces) + '.')
    guard = mapping(result.get('priority_guard'))
    notice = ws_admission_notice(guard.get('priority_reason'))
    if notice:
        lines.append(notice)
        if guard.get('best_price') is not None:
            lines.append(f'Наша цена/объём: {amount(guard.get("source_price"))}/{amount(guard.get("source_quantity"))}; '
                         f'лучший уровень: {amount(guard.get("best_price"))}/{amount(guard.get("best_quantity"))}.')
    latency = mapping(result.get('latency'))
    gap = number(latency.get('source_to_receiver_intent_seconds'))
    response = number(latency.get('receiver_submit_ack_seconds'))
    decision = number(latency.get('source_to_receiver_decision_seconds'))
    if gap is None and decision is not None and decision >= 0:
        lines.append(f'LIMIT → решение: {decision:.3f} с; MARKET не отправлен.')
    if gap is not None and gap >= 0:
        lines.append(f'LIMIT → MARKET: {gap:.3f} с' + (f'; ответ на MARKET: {response:.3f} с.' if response is not None and response >= 0 else '.'))
    return lines


def read_execution_notices(path):
    """Bounded local child readback off the execution path, including short phases."""
    from .operator_recovery import journal_rows
    from .random_cycle import MAX_OPENING_ATTEMPTS, MAX_CLOSING_ATTEMPTS
    import re
    path = Path(path)
    notices = []
    try:
        for child in sorted(path.parent.glob('*.jsonl')):
            match = re.fullmatch(r'(opening|closing)(?:-attempt-([0-9]{3}))?\.jsonl', child.name)
            if not match:
                continue
            phase, attempt = match[1], int(match[2] or 1)
            if not 1 <= attempt <= (MAX_OPENING_ATTEMPTS if phase == 'opening' else MAX_CLOSING_ATTEMPTS):
                continue
            plan = {}
            for row in journal_rows(child):
                payload = row['payload']
                if row['event'] in {'PLAN_READY', 'PLAN_REVIEWED'}:
                    plan = mapping(payload.get('plan', payload))
                elif row['event'] == 'SOURCE_DISPATCH_RESULT' and payload.get('accepted') is True:
                    order = mapping(plan.get('source'))
                    label = 'Открытие' if phase == 'opening' else 'Закрытие'
                    text = (f'{label} · LIMIT принят биржей: счёт {clean(order.get("account_index", "?"), 24)}, '
                            f'{clean(order.get("side", "?"))} {amount(order.get("quantity"))} по {amount(order.get("price"))}. '
                            'Исполнение и контрагент ещё проверяются.')
                    notices.append((f'{phase}-{attempt}-accepted', text))
                elif row['event'] == 'COMPLETE':
                    notices.append((f'{phase}-{attempt}-execution', '\n'.join(execution_lines(
                        payload.get('receipt'), phase=phase, attempt=attempt))))
    except (OSError, ValueError, TypeError, AttributeError):
        pass  # Incomplete trailing writes can be retried at the next read.
    # Several phases may finish before a slow display reads them. File-name
    # ordering puts closing before opening and attempt 2 before attempt 1.
    def order(notice):
        phase, attempt, event = notice[0].split('-')
        return (phase == 'closing', int(attempt), event != 'accepted')
    return sorted(notices, key=order)


def recovery_reason_text(reason):
    reason = clean(reason or 'нужна свежая проверка', 260)
    if 'dispatch outcome unknown' in reason:
        return 'Неизвестно, дошёл ли закрывающий ордер до биржи. Повторная отправка остановлена до сверки ордера.'
    if 'transiently unavailable' in reason or 'recovery read deadline exceeded' in reason:
        return 'Не удалось получить свежие данные счёта за время проверки. Новая команда /close повторит проверку.'
    if 'active cycle-market' in reason:
        return 'На счёте есть активные ордера по этому рынку; сначала нужно подтвердить их завершение.'
    if 'below a documented venue minimum' in reason:
        return 'Остаток меньше минимального ордера для этого рынка; исключение для закрытия не подтверждено. ' + reason.split(':', 1)[-1].strip()
    if 'grid' in reason or 'exact integer' in reason:
        return 'Остаток не укладывается в точный шаг объёма биржи; увеличивать его нельзя.'
    if 'identity' in reason:
        return 'Не подтверждена неизменность счёта или ордера; требуется новая сверка.'
    if 'contract_error' in reason:
        return 'Проверка данных остановила закрытие. В этом старом журнале точная причина не сохранена.'
    return reason


def close_result_lines(result):
    result = mapping(result)
    label = {'CONFIRMED_FLAT': '✅ Позиции закрыты', 'PARTIAL': '⚠️ Остался незакрытый объём'}.get(result.get('status'), '❔ Закрытие не подтверждено')
    lines = [label, f'Проверка: {timestamp(result.get("at"))}.']
    for row in result.get('positions', [])[:2]:
        lines.append(position_line(row.get('account_index'), row.get('position'), result.get('symbol', '')))
    attempts = result.get('attempts', [])
    fees = [number(a.get('fee_total')) for a in attempts]
    count = str(sum(a.get('attempted') is True for a in attempts)) if 'attempts' in result else 'неизвестно'
    lines.append(f'MARKET-ордеров отправлено или могла начаться отправка: {count}.')
    lines.append('Комиссии закрытия: ' + (amount(sum(fees, Decimal(0))) if attempts and all(f is not None for f in fees) else '0' if result.get('status') == 'CONFIRMED_FLAT' and not attempts else 'неизвестны') + '.')
    lines.append('PnL отдельно закрытых позиций неизвестен: история входа не привязана к этой команде.')
    if result.get('status') != 'CONFIRMED_FLAT':
        lines.append('Причина: ' + recovery_reason_text(result.get('reason')))
    lines.append('Новая команда снова проверит текущие счета; старый исход не запрещает её навсегда.')
    return lines


def result_lines(report, *, detailed=False):
    report = mapping(report)
    inventory, pair = mapping(report.get('inventory')), mapping(report.get('paired_execution'))
    economics, binding = mapping(report.get('economics')), mapping(report.get('binding'))
    fees, pnl = mapping(economics.get('fees')), mapping(economics.get('closed_execution_pnl'))
    lines = []
    cycle = mapping(report.get('cycle'))
    if cycle.get('outcome') == 'FAILED_PREFLIGHT_BLOCKED' and not report.get('dispatched_actions'):
        lines.append('Запуск завершён до отправки торговых ордеров: ' + clean(cycle.get('reason'), 320))
    notice = ws_admission_notice(mapping(report.get('cycle')).get('reason'))
    if notice:
        lines.append(notice)
    refusal = opening_margin_refusal(mapping(report.get('cycle')).get('preflight_reason'))
    if refusal:
        lines.append(refusal)
    if binding.get('source_account_index') is not None:
        side = {'LONG': 'SELL', 'SHORT': 'BUY'}.get(binding.get('direction'), '?')
        lines.append(f'Первый счёт: {clean(binding.get("source_account_index"), 24)} · лимитный {side} (A); '
                     f'Второй счёт: {clean(binding.get("receiver_account_index"), 24)} (B).')
    lines += [
        'Парное исполнение: ' + {'SUCCESS': '✅ подтверждено', 'FAILED': '⚠️ не состоялось', 'PARTIAL': '⚠️ выполнено частично'}.get(pair.get('status'), '❔ не доказано'),
        'Историческая позиция: ' + {'CONFIRMED_FLAT': '✅ закрытие подтверждено', 'PARTIAL': '⚠️ есть остаток',
                                    'OPEN_INVENTORY': '⚠️ открыт остаток'}.get(inventory.get('status'), '❔ неизвестна'),
    ]
    matches = pair.get('direct_counterparty_match')
    for value in matches[:2] if isinstance(matches, list) else ():
        value = mapping(value)
        label = {'opening': 'Открытие', 'closing': 'Закрытие'}.get(value.get('phase'), 'Фаза')
        if value.get('status') == 'NOT_ATTEMPTED':
            lines.append(f'{label}: парные ордера не отправлялись.')
            continue
        for role, kind in (('source', 'наша LIMIT'), ('receiver', 'наш MARKET')):
            prefix = f'{label} · {kind}: '
            state = value.get(f'{role}_state')
            if state in {'UNSENT', 'NO_FILL'}:
                detail = ('не отправлялся.' if role == 'receiver' else 'не отправлялась.') if state == 'UNSENT' else 'ордер отправлен, исполнений нет; ордер завершён.'
                if state == 'NO_FILL':
                    terminal = [mapping(item) for item in report.get('terminal_orders', [])
                                if mapping(item).get('phase') == value.get('phase')
                                and mapping(item).get('leg') == role]
                    if terminal:
                        status = terminal[-1].get('status')
                        if isinstance(status, str):
                            detail = f'ордер отправлен, исполнений нет; статус: {clean(status, 48)}.'
                lines.append(prefix + detail)
                continue
            own = number(value.get('matched_quantity'))
            external = number(value.get(f'external_{role}_quantity'))
            unproved = number(value.get(f'unproved_{role}_quantity'))
            parts = []
            if own:
                parts.append(('исполнил наш парный счёт' if role == 'source' else 'исполнил нашу LIMIT') + f' — {amount(own)}')
            if external:
                ids = value.get(f'external_{role}_accounts', [])
                ids = ids if isinstance(ids, list) else []
                who = 'счета ' + ', '.join(clean(i, 24) for i in ids[:3]) if ids else 'внешние участники'
                parts.append((f'исполнили внешние участники ({who})' if role == 'source' else f'исполнил чужие заявки ({who})')
                             + f' — {amount(external)}')
            if unproved:
                parts.append(f'контрагент не доказан — {amount(unproved)}')
            lines.append(prefix + ('; '.join(parts) if parts else 'исполнение и контрагент не доказаны') + '.')
    lines.append(hold_line(report.get('holding')))
    lines.append(f'Завершение: {timestamp(mapping(report.get("cycle")).get("terminal_at"))}.')
    lines.append('Комиссии: ' + (amount(fees.get('total')) if fees.get('status') == 'PROVEN' else 'неполные данные, сумма неизвестна') + ' (валюта котировки).')
    if fees.get('status') != 'PROVEN' and any(mapping(f).get('fee_evidence') == 'NONZERO_UNIT_UNVERIFIED' for f in report.get('confirmed_fills', [])):
        lines.append('Ненулевые поля комиссий сохранены; единица их пересчёта ещё не подтверждена.')
    lines.append('PnL сделок до комиссий: ' + amount(pnl.get('gross')) + '; после комиссий: ' + amount(pnl.get('net')) + '.')
    lines.append('PnL указан в валюте котировки, без фандинга. Итог с фандингом неизвестен.')
    if detailed or inventory.get('status') != 'CONFIRMED_FLAT':
        recovery_reason = mapping(report.get('cycle')).get('recovery_stop_reason')
        if recovery_reason and inventory.get('status') != 'CONFIRMED_FLAT':
            lines.append('Закрытие остатка остановлено: ' + recovery_reason_text(recovery_reason))
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
