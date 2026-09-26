"""/report: totals of the last 24 hours from saved slots; display only.

The controller renders this in its worker thread from saved journals: no
venue request, no credentials and nothing that feeds a trading decision.
A cycle belongs to the window by its claim time (``launch.json``), else by the
CYCLE_STARTED time of its own journal; a close by its CLOSE_STARTED time.
"""
from datetime import datetime
import json
import math
from pathlib import Path
import re

from .operator_view import MSK, read_launch_failure
from .telegram_cards import usd
from .telegram_messages import mapping, text
from .telegram_series_report import Totals

DAY_SECONDS = 24 * 3600
# Slots are claimed one at a time under the operator lock, so claim times grow
# with the slot number; the newest-first scan stops this far before the window.
SCAN_MARGIN_SECONDS = 3600
FIRST_ROW_LIMIT = 2 * 1024 * 1024
STOPPED_NAMES = 10


def _time(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        return None
    return float(value)


def _first_row(path):
    """The first complete journal row, bounded; None when absent or unreadable."""
    try:
        if path.is_symlink():
            return None
        with path.open(encoding='utf-8') as stream:
            line = stream.readline(FIRST_ROW_LIMIT + 1)
        if len(line) > FIRST_ROW_LIMIT or not line.endswith('\n'):
            return None
        row = json.loads(line)
    except (OSError, ValueError):
        return None
    return row if isinstance(row, dict) else None


def slot_time(slot, prefix='cycle'):
    """When a slot was claimed or started; None when no saved time shows it."""
    slot = Path(slot)
    if slot.is_symlink():
        return None
    if prefix == 'cycle':
        path = slot / 'launch.json'
        try:
            if not path.is_symlink() and path.stat().st_size <= 65536:
                value = json.loads(path.read_text(encoding='utf-8'))
                at = _time(value.get('claimed_at')) if isinstance(value, dict) else None
                if at is not None:
                    return at
        except (OSError, ValueError):
            pass
    row = _first_row(slot / f'{prefix}.jsonl')
    event = 'CYCLE_STARTED' if prefix == 'cycle' else 'CLOSE_STARTED'
    return _time(row.get('at')) if row is not None and row.get('event') == event else None


def slots_in_window(operator, prefix, since, until):
    """(slot names with a time in [since, until], oldest first; newer slots without a saved time)."""
    names = sorted((p.name for p in Path(operator).glob(f'{prefix}-*')
                    if re.fullmatch(prefix + r'-[0-9]+', p.name) and not p.is_symlink() and p.is_dir()),
                   key=lambda name: int(name.split('-')[1]), reverse=True)
    found, unknown = [], 0
    for name in names:
        at = slot_time(Path(operator) / name, prefix)
        if at is None:
            unknown += 1
            continue
        if at < since - SCAN_MARGIN_SECONDS:
            break
        if since <= at <= until:
            found.append(name)
    return found[::-1], unknown


def moscow(value):
    return datetime.fromtimestamp(value, MSK).strftime('%d.%m %H:%M')


def figure(label, value, known, count, *, signed=False):
    """A sum over the cycles whose value is proved; a partial sum names its coverage."""
    if not known:
        return f'{label}: неизвестен'
    return (f'{label}: <b>{usd(value, signed=signed)} $</b>'
            + ('' if known == count else f' — по {known} из {count} циклов'))


def total_lines(totals):
    count = totals.cycles
    lines = [figure('Оборот', totals.turnover, totals.turnover_count, count),
             figure('PnL до комиссий', totals.gross, totals.gross_count, count, signed=True),
             figure('PnL после комиссий', totals.net, totals.net_count, count, signed=True)]
    if totals.net_count and totals.net_count == totals.gross_count:
        # A cycle's net is only ever known with its gross, so both sums cover the same cycles.
        fees = figure('Комиссии', totals.gross - totals.net, totals.net_count, count)
        if totals.tariff_count == totals.net_count:
            fees += ' (тариф 0%)'
        elif totals.tariff_count:
            fees += f' (тариф 0% в {totals.tariff_count} из них)'
    else:
        fees = 'Комиссии: неизвестны'
    return lines + [fees]


def day_report(operator, until, load_report, *, running=None, step=None):
    """One bounded message: totals of the cycles claimed in the 24 hours before ``until``.

    ``running`` names the cycle this controller is executing; it is excluded
    and named. ``step`` is (index, total) of the command in progress.
    Sums follow the series rules; a cycle whose value is not proved is left
    out and every partial sum names how many cycles it covers.
    """
    since = until - DAY_SECONDS
    names, unknown = slots_in_window(operator, 'cycle', since, until)
    closes, unknown_closes = slots_in_window(operator, 'close', since, until)
    totals, not_started, stopped = Totals(), 0, []
    for name in names:
        if name == running:
            continue
        if read_launch_failure(Path(operator) / name) is not None:
            not_started += 1
            continue
        valid, _, _ = totals.add(mapping(load_report(name)))
        if not valid:
            stopped.append(name)
    lines = ['📊 <b>Итог за сутки</b>', f'{moscow(since)} → {moscow(until)} МСК (последние 24 часа)']
    if isinstance(step, (list, tuple)) and len(step) == 2:
        what = f'цикл {text(step[0], 12)}/{text(step[1], 12)}' if step[1] != 1 else 'цикл'
        lines.append(f'⏳ Сейчас идёт {what}' + (f' (<code>{text(running, 32)}</code>) — войдёт в итог после завершения'
                                               if running in names else ''))
    outcomes = totals.outcomes
    if totals.cycles:
        lines.append(f'Циклов: <b>{totals.cycles}</b> · ✅ {outcomes["ok"]} · 🟡 {outcomes["partial"]} · ⛔ {outcomes["stop"]}')
    else:
        lines.append('Завершённых циклов за это время нет.')
    if stopped:
        shown = ', '.join(text(name, 32) for name in stopped[:STOPPED_NAMES])
        lines.append(f'⛔ требуют сверки: {shown}' + (f' и ещё {len(stopped) - STOPPED_NAMES}'
                                                    if len(stopped) > STOPPED_NAMES else ''))
    if not_started:
        lines.append(f'Не начались (ордера не отправлялись): {not_started}')
    if totals.cycles:
        lines.extend(total_lines(totals))
    if closes:
        lines.append(f'Отдельных закрытий позиций: {len(closes)} — в PnL и оборот не входят')
    if unknown + unknown_closes:
        lines.append(f'Без сохранённого времени и не в итоге: {unknown + unknown_closes}')
    return '\n'.join(lines)
