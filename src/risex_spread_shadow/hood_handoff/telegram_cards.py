"""Short human Telegram cards built from saved, reconciled cycle reports.

Rendering is display-only and runs in the controller's worker thread after a
cycle has finished: it never reads the network, never feeds trading decisions
and never turns missing evidence into a value. The owner confirmed a 0%
Robinhood BTC tariff (2026-09-26): only when every validated fill of a settled
nominal-USD cycle has an empty fee (no raw venue/integrator value) is the fee
displayed as 0 by that tariff and PnL after fees equal to gross, explicitly
labelled. Saved reports and proof keep their own UNKNOWN fee status.
"""
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
import math
from pathlib import Path
import re

from .operator_view import (MSK, amount, nominal_usd, number, opening_margin_refusal, position_line,
                            recovery_reason_text, ws_admission_notice)
from .telegram_messages import mapping, text

ICONS = {'own': '🤝', 'external': '👥', 'mixed': '🔀', 'empty': '∅', 'skipped': '—', 'unknown': '❔'}
LEGEND = '🤝 свои счета · 👥 внешние · 🔀 смешанно · ∅ без сделок · — не было · ❔ не доказано · 🛠 закрыт остаток'
FOOTNOTE = ('USD — номинал USDG без пересчёта курса; фандинг не включён; '
            'оборот = покупки + продажи обоих счетов, включая закрытие остатков.')


def usd(value, *, signed=False):
    """Human USD figure: cents from 1 $, otherwise four decimals; never invents a value."""
    parsed = number(value)
    if parsed is None:
        return 'неизвестно'
    if not parsed:
        return '0'
    places = Decimal('0.01') if abs(parsed) >= 1 else Decimal('0.0001')
    rounded = parsed.quantize(places, rounding=ROUND_HALF_UP)
    whole, _, fraction = format(abs(rounded), 'f').partition('.')
    whole = '{:,}'.format(int(whole)).replace(',', ' ')
    rendered = whole + ('.' + fraction if fraction else '')
    if parsed < 0:
        return '−' + rendered
    if signed and parsed > 0:
        return '+' + rendered
    return rendered


def btc(value):
    parsed = number(value)
    return 'неизвестно' if parsed is None else amount(parsed)


def seconds(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        return None
    if value < 10:
        return f'{value:.1f} с'
    value = int(round(value))
    if value < 60:
        return f'{value} с'
    if value < 3600:
        return f'{value // 60} мин {value % 60} с' if value % 60 else f'{value // 60} мин'
    return f'{value // 3600} ч {value % 3600 // 60} мин'


def clock(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        return None
    try:
        return datetime.fromtimestamp(value, MSK).strftime('%H:%M:%S')
    except (ValueError, OverflowError, OSError):
        return None


def ms(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        return None
    return f'{value * 1000:.0f} мс'


def accepted_text(phase, attempt, plan):
    """The exchange accepted our LIMIT: the phase has started."""
    plan = mapping(plan)
    source, receiver = mapping(plan.get('source')), mapping(plan.get('receiver'))
    verb = 'открываю' if phase == 'opening' else 'закрываю'
    retry = f' (попытка {attempt})' if attempt > 1 else ''
    return (f'{verb}{retry}: LIMIT {text(source.get("account_index", "?"), 24)} {text(source.get("side", "?"), 8)} '
            f'{text(btc(source.get("quantity")), 24)} BTC по {text(amount(source.get("price")), 32)} → '
            f'MARKET {text(receiver.get("account_index", "?"), 24)}')


def execution_text(phase, attempt, receipt, progress=None):
    """(icon, text) for one completed paired attempt from its own validated receipts."""
    from .operator_view import execution_legs
    receipt = mapping(receipt)
    legs, matched, plans = execution_legs(receipt)
    parts, filled, uncertain, foreign = [], False, False, False
    if matched:
        parts.append(f'🤝 свои счета {text(amount(matched), 24)} BTC')
    for role, label in (('source', 'LIMIT взяли чужие'), ('receiver', 'MARKET исполнился о чужие')):
        leg = legs.get(role)
        if leg is None:
            uncertain = True
            continue
        filled = filled or bool(leg.trades)
        peer = mapping(plans.get('receiver' if role == 'source' else 'source')).get('account_index')
        outside = [t for t in leg.trades if t.counterparty_account_index is not None
                   and peer is not None and t.counterparty_account_index != peer]
        external = sum((t.quantity for t in outside), Decimal(0))
        if external:
            foreign = True
            ids = sorted({str(t.counterparty_account_index) for t in outside})[:3]
            parts.append(f'{label} {text(amount(external), 24)} (счёт {text(", ".join(ids), 60)})')
        unproved = max(Decimal(0), leg.filled_quantity - external - matched)
        if unproved:
            uncertain = True
            parts.append(f'контрагент не доказан {text(amount(unproved), 24)}')
    done = 'открыто' if phase == 'opening' else 'закрыто'
    retry = f' (попытка {attempt})' if attempt > 1 else ''
    if uncertain and not parts:
        icon, head = '❔', f'{"открытие" if phase == "opening" else "закрытие"}{retry}: исполнение не доказано'
    elif not filled:
        icon, head = '∅', f'{"открытие" if phase == "opening" else "закрытие"}{retry} не состоялось: сделок нет'
    elif foreign or uncertain:
        icon, head = '⚠️', f'{"открытие" if phase == "opening" else "закрытие"}{retry}: ' + '; '.join(parts)
    else:
        icon, head = ('🟢' if phase == 'opening' else '✅'), f'{done}{retry}: ' + '; '.join(parts)
    details = []
    notice = ws_admission_notice(mapping(receipt.get('priority_guard')).get('priority_reason'))
    if notice:
        details.append(text(notice, 200))
    latency = mapping(receipt.get('latency'))
    gap, response = ms(latency.get('source_to_receiver_intent_seconds')), ms(latency.get('receiver_submit_ack_seconds'))
    decision = ms(latency.get('source_to_receiver_decision_seconds'))
    if gap:
        details.append(f'LIMIT→MARKET {gap}' + (f' · ответ MARKET {response}' if response else ''))
    elif decision:
        details.append(f'LIMIT→решение {decision}; MARKET не отправлен')
    progress = mapping(progress)
    holding = mapping(progress.get('holding'))
    if phase == 'opening' and icon == '🟢' and progress.get('stage') in ('HOLD', 'CLOSING') and holding:
        hold, closing = seconds(holding.get('seconds')), clock(holding.get('planned_closing_at'))
        if hold:
            details.append(f'удержание {hold}' + (f' (закрытие ≈ {closing})' if closing else ''))
    return icon, head + ('\n' + ' · '.join(details) if details else '')


def phase_notices(slot, progress=None):
    """(key, at, icon, text) per accepted LIMIT, completed attempt and 429 cooldown.

    Reads only this cycle's saved phase journals in the controller's worker
    thread; a trailing partial write is simply read again at the next poll.
    """
    from .operator_recovery import journal_rows
    from .random_cycle import MAX_CLOSING_ATTEMPTS, MAX_OPENING_ATTEMPTS
    notices = []
    for child in sorted(Path(slot).glob('*.jsonl')):
        match = re.fullmatch(r'(opening|closing)(?:-attempt-([0-9]{3}))?\.jsonl', child.name)
        if not match or child.is_symlink():
            continue
        phase, attempt = match[1], int(match[2] or 1)
        if not 1 <= attempt <= (MAX_OPENING_ATTEMPTS if phase == 'opening' else MAX_CLOSING_ATTEMPTS):
            continue
        plan, limited = {}, False
        try:
            for row in journal_rows(child):
                event, payload = row['event'], mapping(row['payload'])
                if event in {'PLAN_READY', 'PLAN_REVIEWED'}:
                    plan = mapping(payload.get('plan', payload))
                elif event == 'SOURCE_DISPATCH_RESULT' and payload.get('accepted') is True:
                    notices.append((f'{phase}-{attempt}-accepted', row['at'], '⏳', accepted_text(phase, attempt, plan)))
                elif event == 'RECONCILIATION_RATE_LIMIT' and payload.get('will_retry') is True and not limited:
                    limited = True
                    notices.append((f'{phase}-{attempt}-limited', row['at'], '⚠️',
                                    f'{"открытие" if phase == "opening" else "закрытие"}: биржа временно ограничила '
                                    'чтение (HTTP 429). Жду и перепроверяю; ордера не повторяются.'))
                elif event == 'COMPLETE':
                    icon, message = execution_text(phase, attempt, payload.get('receipt'), progress)
                    notices.append((f'{phase}-{attempt}-execution', row['at'], icon, message))
        except Exception:
            continue  # Incomplete trailing write or unproved row: retried at the next poll.
    rank = {'accepted': 0, 'limited': 1, 'execution': 2}
    return sorted(notices, key=lambda n: (n[0].startswith('closing'), int(n[0].split('-')[1]), rank[n[0].split('-')[2]]))


def leverage(bps):
    try:
        return f'{Decimal(10000) / Decimal(int(bps)):.2f}x'
    except (ArithmeticError, TypeError, ValueError):
        return '?'


def reason(value, limit=160):
    return text(value if value else 'причина не сохранена', limit)


def positions_text(proof):
    """Both accounts' current positions and active orders from a readiness proof.

    A wallet-pool proof lists every wallet with a position or orders, then how
    many other wallets are at zero.
    """
    rows = mapping(proof).get('accounts')
    if isinstance(rows, list):
        return _pool_positions_text(rows)
    parts = []
    for role in ('source', 'receiver'):
        row = mapping(mapping(proof).get(role))
        account = text(row.get('account_index', '?'), 24)
        quantity = number(row.get('signed_position'))
        if quantity is None:
            state = 'позиция неизвестна'
        elif not quantity:
            state = '0'
        else:
            state = f'{"LONG" if quantity > 0 else "SHORT"} {text(amount(abs(quantity)), 24)} BTC'
        orders = row.get('active_orders')
        if isinstance(orders, list) and orders:
            state += f', активных ордеров {len(orders)}'
        parts.append(f'счёт {account}: {state}')
    return '; '.join(parts)


def _pool_positions_text(rows):
    parts, zero = [], 0
    for row in map(mapping, rows):
        quantity = number(row.get('signed_position'))
        orders = row.get('active_orders')
        busy = isinstance(orders, list) and bool(orders)
        if quantity == 0 and not busy:
            zero += 1
            continue
        if quantity is None:
            state = 'позиция неизвестна'
        elif not quantity:
            state = '0'
        else:
            state = f'{"LONG" if quantity > 0 else "SHORT"} {text(amount(abs(quantity)), 24)} BTC'
        if busy:
            state += f', активных ордеров {len(orders)}'
        parts.append(f'счёт {text(row.get("account_index", "?"), 24)}: {state}')
    if not parts:
        return f'все {len(rows)} кошельков: 0'
    shown = '; '.join(parts[:8]) + (f'; ещё {len(parts) - 8} с позициями' if len(parts) > 8 else '')
    return shown + (f'; остальные {zero} — 0' if zero else '')


def wallet_selection_text(record):
    """One line about the per-cycle wallet draw saved in launch.json; display only."""
    from .wallet_pool import SKIP_REASONS

    record = mapping(record)
    eligible, skipped, pair = record.get('eligible'), record.get('skipped'), record.get('pair')
    if not isinstance(eligible, list) or not isinstance(skipped, list):
        return None
    reasons = '; '.join(f'{text(mapping(item).get("account_index", "?"), 24)} — '
                        f'{SKIP_REASONS.get(mapping(item).get("reason"), "не готов")}' for item in skipped[:12])
    if len(skipped) > 12:
        reasons += f'; ещё {len(skipped) - 12}'
    total = record.get('pool_size')
    if isinstance(pair, list) and len(pair) == 2:
        line = (f'кошельки: выбраны {text(pair[0], 24)} и {text(pair[1], 24)} из {len(eligible)} готовых'
                + (f' (всего активных {text(total, 8)})' if total is not None else ''))
    else:
        line = f'кошельки: готовых {len(eligible)} из {text(total, 8)} — нужно минимум два, цикл не начат'
    return line + (f'; пропущены: {reasons}' if reasons else '')


def wallet_steps(slot):
    """The wallet draw as one live step (key, at, icon, text)."""
    from .wallet_pool import read_selection

    record = read_selection(slot)
    line = wallet_selection_text(record) if record is not None else None
    if line is None:
        return []
    pair = record.get('pair')
    return [('wallets', record['at'], '👛' if isinstance(pair, list) else '⛔', line)]


# Must equal random_cycle.OWNER_STOP_REASON (tested); kept here so views stay light.
OWNER_STOP_REASON = 'owner /stop before the first order; no order was sent'
PHASES = {'opening': 'открытие', 'closing': 'закрытие', 'PAIRED_OPENING': 'открытие', 'PAIRED_CLOSING': 'закрытие'}
STOPS = {'CLOSING_BLOCKED': 'закрытие остановлено', 'CYCLE_EXECUTION_UNKNOWN': 'исполнение не доказано',
         'FALLBACK_BLOCKED_IDENTITY_BARRIER': 'закрытие остатка остановлено',
         'FALLBACK_RECONCILIATION_UNKNOWN': 'итог закрытия остатка не доказан'}


def cycle_steps(slot):
    """(key, at, icon, text) for the trading child's own cycle journal events."""
    from .operator_recovery import journal_rows
    steps, binding = [], {}
    try:
        for row in journal_rows(Path(slot) / 'cycle.jsonl'):
            event, payload, at = row.get('event'), mapping(row['payload']), row['at']
            key = f'cycle-{row["sequence"]}'
            if event == 'CYCLE_STARTED':
                binding = mapping(payload.get('binding'))
                steps.append((key, at, '⚙️', 'торговый процесс запущен: читаю рынок и счета'))
            elif event == 'SELECTION_PROVED':
                selection = mapping(payload.get('selection'))
                sides = {'LONG': ('SELL', 'BUY'), 'SHORT': ('BUY', 'SELL')}.get(binding.get('direction'), ('?', '?'))
                hold = seconds(selection.get('hold_seconds'))
                steps.append((key, at, '🎲', f'выбрано: LIMIT {text(binding.get("source_account_index", "?"), 24)} {sides[0]} → '
                              f'MARKET {text(binding.get("receiver_account_index", "?"), 24)} {sides[1]} · '
                              f'{text(btc(selection.get("quantity")), 24)} BTC' + (f' · удержание {hold}' if hold else '')))
            elif event == 'LEVERAGE_PLAN':
                target, observed = mapping(payload.get('target_fraction_bps')), mapping(payload.get('observed_fraction_bps'))
                parts = ', '.join(f'{text(account, 24)} {leverage(bps)}' for account, bps in list(target.items())[:2])
                changed = any(observed.get(account) != bps for account, bps in target.items())
                steps.append((key, at, '⚙️', ('ставлю плечо: ' if changed else 'плечо уже подходит: ') + parts))
            elif event == 'LEVERAGE_UPDATE_CONFIRMED':
                steps.append((key, at, '✓', f'плечо счёта {text(payload.get("account_index", "?"), 24)} → '
                              f'{leverage(payload.get("fraction_bps"))} подтверждено'))
            elif event == 'INITIAL_SPREAD_WAIT':
                steps.append((key, at, '⏳', 'жду спред под заданный отступ цены; ордера ещё не отправлены.'))
            elif event == 'INITIAL_SPREAD_READY':
                steps.append((key, at, '✓', 'спред подходит — продолжаю'))
            elif event == 'OPENING_QUANTITY_RECALCULATED':
                steps.append((key, at, '↘️', f'объём уменьшен по свежей марже: {text(amount(payload.get("old_quantity")), 24)} → '
                              f'{text(amount(payload.get("new_quantity")), 24)} BTC; проверяю перед открытием.'))
            elif event in ('PREPARATION_RETRY', 'CLOSING_PREPARATION_RETRY'):
                what = 'подготовка открытия' if event == 'PREPARATION_RETRY' else 'подготовка закрытия'
                steps.append((key, at, '⚠️', f'{what}: повтор — {reason(payload.get("reason"))}'))
            elif event in ('PREPARATION_FAILED', 'CLOSING_PREPARATION_FAILED'):
                what = 'подготовка открытия' if event == 'PREPARATION_FAILED' else 'подготовка закрытия'
                steps.append((key, at, '⚠️', f'{what} не прошла: {reason(payload.get("reason"))}'))
            elif event == 'PREPARATION_BLOCKED':
                steps.append((key, at, '⛔', f'подготовка открытия остановлена: {reason(payload.get("reason"))}'))
            elif event == 'PAIR_ATTEMPT_RETRY':
                steps.append((key, at, '🔁', f'повторяю {PHASES.get(payload.get("phase"), "попытку")}: '
                              f'попытка {text(payload.get("next_attempt", "?"), 8)}'))
            elif event == 'PAIR_ATTEMPT_EXHAUSTED':
                steps.append((key, at, '⛔', f'{PHASES.get(payload.get("phase"), "фаза")}: попытки исчерпаны '
                              f'({text(payload.get("maximum_attempts", "?"), 8)}) — {reason(payload.get("reason"))}'))
            elif event == 'CLOSING_PLAN_READY' and payload.get('attempt') in (None, 1):
                steps.append((key, at, '⏱', 'удержание закончилось — начинаю закрытие'))
            elif event == 'FALLBACK_DISPATCH_INTENT':
                plan = mapping(payload.get('plan'))
                steps.append((key, at, '🛠', f'закрываю остаток reduce-only: счёт {text(payload.get("account_index", "?"), 24)} '
                              f'{text(plan.get("side", "?"), 8)} {text(btc(plan.get("quantity")), 24)} BTC'))
            elif event == 'FALLBACK_RECONCILED':
                after = number(mapping(payload.get('after')).get('signed_position'))
                ok = payload.get('outcome') == 'SUCCESS'
                steps.append((key, at, '✓' if ok else '⚠️', f'остаток счёта {text(payload.get("account_index", "?"), 24)}: '
                              f'{text(payload.get("outcome", "?"), 24)}' + (f', позиция {text(amount(after), 24)}' if after is not None else '')))
            elif event == 'RECOVERY_READ_RETRY':
                steps.append((key, at, '⚠️', f'повтор чтения ({text(payload.get("operation", "?"), 40)}): {reason(payload.get("reason"))}'))
            elif event in STOPS:
                steps.append((key, at, '⛔', f'{STOPS[event]}: {reason(payload.get("reason"))}'))
            elif event == 'OWNER_STOP_OBSERVED':
                retry = payload.get('stage') == 'BEFORE_OPENING_RETRY'
                steps.append((key, at, '⏹', 'получен /stop — новых попыток открытия не будет; остаток, если есть, '
                              'закрою reduce-only' if retry else
                              'получен /stop — цикл завершён до первого ордера, ордера не отправлялись'))
            elif event == 'HOLD_ENDED_BY_OWNER_STOP':
                held, planned = seconds(payload.get('held_seconds')), seconds(payload.get('hold_seconds'))
                steps.append((key, at, '⏹', 'получен /stop — удержание прервано'
                              + (f' через {held}' if held else '') + (f' из {planned}' if planned else '')
                              + ', закрываю позиции обычным закрытием'))
            elif event == 'CYCLE_PREFLIGHT_BLOCKED':
                if payload.get('reason') == OWNER_STOP_REASON:
                    continue  # Already shown as the owner-stop step.
                steps.append((key, at, '⛔', f'цикл остановлен до ордеров: {reason(payload.get("opening_reason"))}'))
    except Exception:
        pass  # A trailing partial write is read again at the next poll.
    return steps


def live_steps(slot, progress=None):
    """Cycle and phase steps in journal-time order; display only."""
    return sorted(wallet_steps(slot) + cycle_steps(slot) + phase_notices(slot, progress), key=lambda step: step[1])


def phase_row(report, phase):
    rows = mapping(report.get('paired_execution')).get('direct_counterparty_match')
    rows = rows if isinstance(rows, list) else []
    return next((mapping(row) for row in rows if mapping(row).get('phase') == phase), {})


def phase_kind(report, phase):
    """Classify one phase from the reconciled counterparty match only."""
    row = phase_row(report, phase)
    status = row.get('status')
    if status == 'NOT_ATTEMPTED':
        return 'skipped'
    if status == 'NO_FILL':
        return 'empty'
    own = number(row.get('matched_quantity')) or Decimal(0)
    external = any((number(row.get(f'external_{leg}_quantity')) or Decimal(0)) > 0 for leg in ('source', 'receiver'))
    unknown = any((number(row.get(f'unproved_{leg}_quantity')) or Decimal(0)) > 0
                  or row.get(f'{leg}_state') == 'UNKNOWN' for leg in ('source', 'receiver'))
    if external:
        return 'mixed' if own > 0 else 'external'
    if status == 'MATCHED' and not unknown and own > 0:
        return 'own'
    return 'unknown'


def phase_details(report, phase):
    row = phase_row(report, phase)
    kind = phase_kind(report, phase)
    if kind == 'skipped':
        return f'{ICONS[kind]} не выполнялось'
    if kind == 'empty':
        return f'{ICONS[kind]} ордера не исполнились'
    parts = []
    own = number(row.get('matched_quantity'))
    if own:
        parts.append('свои счета' + ('' if kind == 'own' else f' {amount(own)}'))
    for leg, label in (('source', 'LIMIT взяли чужие'), ('receiver', 'MARKET исполнился о чужие')):
        quantity = number(row.get(f'external_{leg}_quantity'))
        if quantity:
            ids = row.get(f'external_{leg}_accounts')
            ids = [text(i, 24) for i in ids[:3]] if isinstance(ids, list) else []
            parts.append(f'{label} {amount(quantity)}' + (f' (счёт {", ".join(ids)})' if ids else ''))
    for leg in ('source', 'receiver'):
        quantity = number(row.get(f'unproved_{leg}_quantity'))
        if quantity:
            parts.append(f'контрагент не доказан {amount(quantity)}')
    if kind == 'unknown' and not parts:
        parts.append('исполнение не доказано')
    return f'{ICONS[kind]} ' + '; '.join(parts)


def residual_details(report):
    fills = [mapping(r) for r in report.get('confirmed_fills', []) if mapping(r).get('phase') == 'fallback']
    actions = [mapping(r) for r in report.get('dispatched_actions', []) if mapping(r).get('phase') == 'fallback']
    if not fills:
        return 'остаток: исполнение не подтверждено' if actions else None
    binding = mapping(report.get('binding'))
    accounts = {binding.get('source_account_index'), binding.get('receiver_account_index')}
    labels = set()
    for fill in fills:
        peer = fill.get('counterparty_account_index')
        labels.add('контрагент неизвестен' if type(peer) is not int else
                   'свой счёт' if peer in accounts else 'внешние участники')
    # cycle-312: one account was closed while the other stayed open; never
    # claim the whole residual closed unless the inventory is proved flat.
    flat = mapping(report.get('inventory')).get('status') == 'CONFIRMED_FLAT'
    return ('остаток закрыт reduce-only: ' if flat else
            'остаток закрыт reduce-only не полностью: ') + ', '.join(sorted(labels))


def planned_quantity(report):
    """Quantity of the last planned opening pair; a plan, not an execution claim."""
    plans = [mapping(a) for a in report.get('planned_actions', [])
             if mapping(a).get('phase') == 'opening' and mapping(a).get('leg') == 'source']
    if not plans:
        return None
    last = max(plans, key=lambda a: a.get('attempt') if type(a.get('attempt')) is int else 0)
    return number(mapping(last.get('plan')).get('quantity'))


def attempts(report, phase):
    rows = mapping(report.get('latency')).get(phase)
    return len(rows) if isinstance(rows, list) else 0


def cycle_times(report):
    timing = mapping(mapping(report.get('latency')).get('cycle'))
    intervals = timing.get('phase_intervals')
    intervals = [mapping(i) for i in intervals] if isinstance(intervals, list) else []
    starts = [i.get('start_at') for i in intervals if isinstance(i.get('start_at'), (int, float))
              and not isinstance(i.get('start_at'), bool)]
    terminal = timing.get('terminal_at')
    if isinstance(terminal, bool) or not isinstance(terminal, (int, float)):
        terminal = None
    spans = {}
    for item in intervals:
        value = item.get('seconds')
        if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0:
            key = 'preparation' if item.get('phase') in ('selection', 'preparation') else item.get('phase')
            spans[key] = spans.get(key, 0) + value
    start = min(starts) if starts else None
    total = terminal - start if start is not None and terminal is not None and terminal >= start else None
    return start, terminal, total, spans


def phase_gaps(slot):
    """LIMIT→MARKET intent gap of the last completed attempt per phase, if journaled."""
    from .operator_recovery import journal_rows
    gaps = {}
    if slot is None:
        return gaps
    slot = Path(slot)
    for child in sorted(slot.glob('*.jsonl')):
        match = re.fullmatch(r'(opening|closing)(?:-attempt-([0-9]{3}))?\.jsonl', child.name)
        if not match or child.is_symlink():
            continue
        attempt = int(match[2] or 1)
        try:
            for row in journal_rows(child):
                if row['event'] != 'COMPLETE':
                    continue
                latency = mapping(mapping(mapping(row['payload']).get('receipt')).get('latency'))
                gap = latency.get('source_to_receiver_intent_seconds')
                if (isinstance(gap, (int, float)) and not isinstance(gap, bool) and math.isfinite(gap) and gap >= 0
                        and attempt >= gaps.get(match[1], (0, None))[0]):
                    gaps[match[1]] = (attempt, gap)
        except Exception:
            continue  # Display only: a malformed journal simply has no timing line.
    return {phase: value[1] for phase, value in gaps.items()}


def settled(report):
    order = mapping(report.get('order_state'))
    return (report.get('status') == 'COMPLETE'
            and order.get('unresolved_intents') == [] and order.get('unresolved_observed_orders') == [])


def resolved(report):
    """Settled and in the evidenced nominal-USD denomination."""
    return settled(report) and nominal_usd(mapping(report.get('binding')))


def outcome(report, safe=None):
    """'ok' (paired, flat), 'partial' (flat, pair incomplete) or 'stop'.

    ``safe`` is the controller's own continuation decision; a card never shows
    a better verdict than the one that governed the series.
    """
    if safe is False or not settled(report) or mapping(report.get('inventory')).get('status') != 'CONFIRMED_FLAT':
        return 'stop'
    return 'ok' if mapping(report.get('paired_execution')).get('status') == 'SUCCESS' else 'partial'


def zero_tariff(report):
    """True only for a settled nominal-USD cycle whose every fill has an empty fee."""
    from .telegram_series_report import executed_turnover
    fills = report.get('confirmed_fills')
    if not resolved(report) or not isinstance(fills, list) or not fills or executed_turnover(report, set()) is None:
        return False
    return all(mapping(f).get('fee_evidence') == 'MISSING_OR_INVALID_COMPONENTS' and mapping(f).get('fee') is None
               and mapping(f).get('venue_fee_raw') is None and mapping(f).get('integrator_fee_raw') is None
               for f in fills)


def pnl_values(report, safe=None):
    """(gross, net, by_tariff) under the series-report rules; unknown stays None.

    ``net`` is the proven receipt net, or gross under the owner-confirmed 0%
    tariff when :func:`zero_tariff` holds (``by_tariff`` is then True).
    """
    if outcome(report, safe) == 'stop' or not resolved(report):
        return None, None, False
    pnl = mapping(mapping(report.get('economics')).get('closed_execution_pnl'))
    if pnl.get('unit') != 'quote_currency' or pnl.get('status') not in ('PROVEN', 'GROSS_ONLY'):
        return None, None, False
    gross = number(pnl.get('gross'))
    if gross is not None and pnl.get('status') == 'PROVEN':
        return gross, number(pnl.get('net')), False
    if gross is not None and zero_tariff(report):
        return gross, gross, True
    return gross, None, False


def fee_line(report, safe=None):
    fees = mapping(mapping(report.get('economics')).get('fees'))
    if fees.get('status') == 'PROVEN' and number(fees.get('total')) is not None:
        return f'Комиссии: {usd(fees.get("total"))} $'
    if outcome(report, safe) != 'stop' and zero_tariff(report):
        return 'Комиссии: 0 $ · тариф биржи 0% (в сделках не указываются)'
    return 'Комиссии: неизвестны'


def headline(card, report):
    index, total = card.get('index', 1), card.get('total', 1)
    title = f'Цикл {text(index, 12)}/{text(total, 12)}' if total != 1 else 'Цикл'
    name = text(card.get('name') or 'слот не создан', 32)
    kind = outcome(report, card.get('safe')) if report else 'stop'
    icon, note = {'ok': ('✅', ''), 'partial': ('🟡', ' · позиции закрыты, пара неполная'),
                  'stop': ('⛔', ' · итог требует сверки')}[kind]
    start, terminal, span, _ = cycle_times(report) if report else (None, None, None, {})
    when = ''
    if clock(start) and clock(terminal) and seconds(span):
        when = f'\n{clock(start)} → {clock(terminal)} · {seconds(span)}'
    return f'{icon} <b>{title}</b> · <code>{name}</code>{note}{when}'


def cycle_card(card, report, slot=None):
    """One short message describing a finished cycle."""
    report = mapping(report) if report is not None else None
    lines = [headline(card, report)]
    if report is None:
        lines.append('Итог не удалось прочитать из сохранённого журнала.')
        lines.append('/status — состояние · /accounts — счета')
        return '\n'.join(lines)
    binding = mapping(report.get('binding'))
    quantity = planned_quantity(report)
    size = []
    if quantity is not None:
        size.append(f'<b>{text(btc(quantity), 24)} BTC</b>')
    from .telegram_series_report import executed_turnover
    turnover = executed_turnover(report, set()) if resolved(report) else None
    if turnover is not None:
        size.append(f'оборот {usd(turnover)} $')
    mode = binding.get('receiver_admission')
    ticks = binding.get('price_improvement_ticks')
    if mode in ('ack', 'ws_confirmed'):
        size.append('ACK' if mode == 'ack' else 'WS')
    if type(ticks) is int and not isinstance(ticks, bool):
        size.append(f'+{ticks} тик')
    if size:
        lines.append(' · '.join(size))
    source, receiver = binding.get('source_account_index'), binding.get('receiver_account_index')
    sides = {'LONG': ('SELL', 'BUY'), 'SHORT': ('BUY', 'SELL')}.get(binding.get('direction'))
    if type(source) is int and type(receiver) is int and sides:
        lines.append(f'LIMIT {source} {sides[0]} → MARKET {receiver} {sides[1]}')
    gaps = phase_gaps(slot)
    for phase, label in (('opening', 'Открытие'), ('closing', 'Закрытие')):
        line = f'{label}: {phase_details(report, phase)}'
        if phase in gaps and phase_kind(report, phase) not in ('skipped', 'empty'):
            line += f' · LIMIT→MARKET {ms(gaps[phase])}'
        tries = attempts(report, phase)
        if tries > 1:
            line += f' · попыток {tries}'
        lines.append(line)
    residual = residual_details(report)
    if residual:
        lines.append('🛠 ' + residual[0].upper() + residual[1:])
    _, _, _, spans = cycle_times(report)
    timing = [f'{label} {seconds(spans[key])}' for key, label in
              (('preparation', 'подготовка'), ('hold', 'удержание'), ('closing', 'закрытие'))
              if key in spans and seconds(spans[key])]
    if timing:
        lines.append('⏱ ' + ' · '.join(timing))
    gross, net, by_tariff = pnl_values(report, card.get('safe'))
    if by_tariff:
        lines.append(f'💰 PnL {usd(net, signed=True)} $ · комиссии 0 (тариф биржи 0%)')
    else:
        if net is not None:
            lines.append(f'💰 PnL {usd(net, signed=True)} $ после комиссий')
        elif gross is not None:
            lines.append(f'💰 PnL {usd(gross, signed=True)} $ до комиссий')
        else:
            lines.append('💰 PnL неизвестен')
        lines.append(fee_line(report, card.get('safe')))
    kind = outcome(report, card.get('safe'))
    cycle = mapping(report.get('cycle'))
    if kind != 'ok':
        reason = (ws_admission_notice(cycle.get('reason'))
                  or opening_margin_refusal(cycle.get('preflight_reason')))
        if not reason and cycle.get('outcome') == 'FAILED_PREFLIGHT_BLOCKED' and not report.get('dispatched_actions'):
            reason = ('остановлено командой /stop до первого ордера' if cycle.get('reason') == OWNER_STOP_REASON
                      else 'остановлено до отправки ордеров: ' + str(cycle.get('reason'))[:200])
        if reason:
            lines.append('Причина: ' + text(reason, 300))
    if kind == 'stop':
        inventory = mapping(report.get('inventory'))
        if cycle.get('recovery_stop_reason'):
            lines.append('Закрытие остатка: ' + text(recovery_reason_text(cycle.get('recovery_stop_reason')), 300))
        for role in ('source', 'receiver'):
            lines.append(text(position_line(binding.get(f'{role}_account_index', role), inventory.get(role),
                                            binding.get('market_symbol', '')), 120))
        lines.append('/accounts — проверить счета · /close — закрыть остаток · /report — подробно')
    pause = card.get('pause')
    if kind != 'stop' and type(pause) is int and not isinstance(pause, bool):
        lines.append(f'⏸ Следующий цикл через {pause} с')
    return '\n'.join(lines)
