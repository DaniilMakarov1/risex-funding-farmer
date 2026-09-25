"""Short human Telegram cards built from saved, reconciled cycle reports.

Rendering is display-only and runs in the controller's worker thread after a
cycle has finished: it never reads the network, never feeds trading decisions
and never turns missing evidence into a value. The optional fee estimate is a
separately labelled upper bound from the published Robinhood caps; it is not a
receipt fee and never enters PnL or proof.
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
    return 'остаток закрыт reduce-only: ' + ', '.join(sorted(labels))


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


def fee_estimate(report):
    """Upper bound from published Robinhood caps over validated fills, or None."""
    from .random_cycle import ROBINHOOD_MAKER_FEE_CAP, ROBINHOOD_TAKER_FEE_CAP
    from .telegram_series_report import executed_turnover
    if not nominal_usd(mapping(report.get('binding'))) or executed_turnover(report, set()) is None:
        return None
    total = Decimal(0)
    for fill in report.get('confirmed_fills', []):
        fill = mapping(fill)
        cap = ROBINHOOD_MAKER_FEE_CAP if fill.get('fee_role') == 'maker' else ROBINHOOD_TAKER_FEE_CAP
        total += number(fill.get('quantity')) * number(fill.get('price')) * cap
    return total


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


def pnl_values(report, safe=None):
    """(gross, net) under the series-report rules; unknown stays None."""
    if outcome(report, safe) == 'stop' or not resolved(report):
        return None, None
    pnl = mapping(mapping(report.get('economics')).get('closed_execution_pnl'))
    if pnl.get('unit') != 'quote_currency' or pnl.get('status') not in ('PROVEN', 'GROSS_ONLY'):
        return None, None
    gross = number(pnl.get('gross'))
    net = number(pnl.get('net')) if gross is not None and pnl.get('status') == 'PROVEN' else None
    return gross, net


def fee_line(report):
    fees = mapping(mapping(report.get('economics')).get('fees'))
    if fees.get('status') == 'PROVEN' and number(fees.get('total')) is not None:
        return f'Комиссии: {usd(fees.get("total"))} $'
    estimate = fee_estimate(report) if resolved(report) else None
    if estimate is not None and estimate > 0:
        return f'Комиссии: биржа не прислала · оценка по тарифу ≤ {usd(estimate)} $'
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
            line += f' · LIMIT→MARKET {gaps[phase]:.2f} с'
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
    gross, net = pnl_values(report, card.get('safe'))
    if net is not None:
        lines.append(f'💰 PnL {usd(net, signed=True)} $ после комиссий')
    elif gross is not None:
        lines.append(f'💰 PnL {usd(gross, signed=True)} $ до комиссий')
    else:
        lines.append('💰 PnL неизвестен')
    lines.append(fee_line(report))
    kind = outcome(report, card.get('safe'))
    cycle = mapping(report.get('cycle'))
    if kind != 'ok':
        reason = (ws_admission_notice(cycle.get('reason'))
                  or opening_margin_refusal(cycle.get('preflight_reason')))
        if not reason and cycle.get('outcome') == 'FAILED_PREFLIGHT_BLOCKED' and not report.get('dispatched_actions'):
            reason = 'остановлено до отправки ордеров: ' + str(cycle.get('reason'))[:200]
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
