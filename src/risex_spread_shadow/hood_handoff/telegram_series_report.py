"""Read-only paginated series results from explicit, durable cycle membership."""
from decimal import Decimal, localcontext

from .operator_view import number, nominal_usd
from .telegram_messages import mapping, text


def add_exact(left, right):
    exponent = min(left.as_tuple().exponent, right.as_tuple().exponent)
    precision = max(len(v.as_tuple().digits) + v.as_tuple().exponent - exponent
                    for v in (left, right)) + 1
    with localcontext() as context:
        context.prec = max(28, precision)
        return left + right


def executed_turnover(report, seen_receipts):
    """Both-account turnover: each validated account receipt contributes once."""
    binding = mapping(report.get('binding'))
    accounts = {binding.get('source_account_index'), binding.get('receiver_account_index')}
    fills = report.get('confirmed_fills')
    if not isinstance(fills, list):
        return None
    total, keys = Decimal(0), set()
    for fill in fills:
        fill = mapping(fill)
        account, trade = fill.get('account_index'), fill.get('trade_id')
        quantity, price = number(fill.get('quantity')), number(fill.get('price'))
        if (type(account) is not int or account not in accounts
                or fill.get('market_id') != binding.get('market_id')
                or not isinstance(trade, (str, int)) or isinstance(trade, bool) or not str(trade)
                or fill.get('side') not in ('BUY', 'SELL') or fill.get('history_complete') is not True
                or quantity is None or quantity <= 0 or price is None or price <= 0):
            return None
        key = (account, str(trade))
        if key in keys or key in seen_receipts:
            return None
        keys.add(key)
        with localcontext() as context:
            context.prec = max(28, len(quantity.as_tuple().digits) + len(price.as_tuple().digits))
            notional = quantity * price
        total = add_exact(total, notional)
    seen_receipts.update(keys)
    return total


def phase_label(report, phase):
    from .telegram_cards import ICONS, phase_kind
    return ICONS[phase_kind(report, phase)]


def residual_label(report):
    from .telegram_cards import residual_details
    detail = residual_details(report)
    return ' 🛠 ' + detail if detail else ''


class Totals:
    """Aggregate of saved cycle reports under one rule set; unknown stays unknown.

    A cycle is summed only when settled, flat and in the nominal-USD Robinhood
    BTC denomination, validated against its own two accounts: a wallet pool
    draws a new pair every cycle, so the first cycle's pair is never required.
    Each account receipt (account, trade) is counted once across all cycles.
    """

    def __init__(self):
        self.cycles = 0
        self.outcomes = {'ok': 0, 'partial': 0, 'stop': 0}
        self.gross, self.net, self.turnover = Decimal(0), Decimal(0), Decimal(0)
        self.gross_count = self.net_count = self.turnover_count = self.tariff_count = 0
        self.starts, self.terminals = [], []
        self.receipts = set()

    def add(self, report):
        """Count one cycle; return (valid, gross, net) for its own line."""
        from .telegram_cards import cycle_times, outcome, zero_tariff
        report = mapping(report)
        self.cycles += 1
        binding = mapping(report.get('binding'))
        accounts = (binding.get('source_account_index'), binding.get('receiver_account_index'))
        pair = all(type(a) is int for a in accounts) and accounts[0] != accounts[1]
        order = mapping(report.get('order_state'))
        resolved = (report.get('status') == 'COMPLETE' and pair and nominal_usd(binding)
                    and order.get('unresolved_intents') == [] and order.get('unresolved_observed_orders') == [])
        volume = executed_turnover(report, self.receipts) if resolved else None
        if volume is not None:
            self.turnover = add_exact(self.turnover, volume)
            self.turnover_count += 1
        valid = resolved and mapping(report.get('inventory')).get('status') == 'CONFIRMED_FLAT'
        pnl = mapping(mapping(report.get('economics')).get('closed_execution_pnl'))
        g = number(pnl.get('gross')) if valid and pnl.get('status') in ('PROVEN', 'GROSS_ONLY') and pnl.get('unit') == 'quote_currency' else None
        n = number(pnl.get('net')) if g is not None and pnl.get('status') == 'PROVEN' else None
        if n is None and g is not None and zero_tariff(report):
            n = g  # Owner-confirmed 0% tariff; every fill has an empty fee.
            self.tariff_count += 1
        if g is not None:
            self.gross = add_exact(self.gross, g)
            self.gross_count += 1
        if n is not None:
            self.net = add_exact(self.net, n)
            self.net_count += 1
        self.outcomes['stop' if not valid else outcome(report)] += 1
        start, terminal, _, _ = cycle_times(report) if report else (None, None, None, {})
        if start is not None and terminal is not None:
            self.starts.append(start)
            self.terminals.append(terminal)
        return valid, g, n

    def lines(self, complete, *, turnover_label='Оборот обоих счетов'):
        """Turnover, PnL and fee lines; a total is shown only when every cycle is known."""
        from .telegram_cards import usd
        count = self.cycles
        gross_known = complete and self.gross_count == count
        net_known = complete and self.net_count == count
        volume_known = complete and self.turnover_count == count
        lines = [f'{turnover_label}: ' + (f'<b>{usd(self.turnover)} $</b>' if volume_known else 'неизвестен')]
        if not volume_known and self.turnover_count:
            lines.append(f'Подтверждённая часть оборота ({self.turnover_count}/{count}): {usd(self.turnover)} $')
        lines.append('PnL до комиссий: ' + (f'<b>{usd(self.gross, signed=True)} $</b>' if gross_known else 'неизвестен'))
        if not gross_known and self.gross_count:
            lines.append(f'Известная часть до комиссий ({self.gross_count}/{count}): {usd(self.gross, signed=True)} $')
        lines.append('PnL после комиссий: ' + (f'<b>{usd(self.net, signed=True)} $</b>' if net_known else 'неизвестен'))
        if not net_known and self.net_count:
            lines.append(f'Известная часть после комиссий ({self.net_count}/{count}): {usd(self.net, signed=True)} $')
        tariff = f' (тариф 0% в {self.tariff_count}/{count} циклах)' if self.tariff_count else ''
        if net_known and gross_known:
            lines.append(f'Комиссии: {usd(self.gross - self.net)} ${tariff}')
        else:
            lines.append('Комиссии: неизвестны' + (f'; тариф 0% в {self.tariff_count}/{count} циклах' if self.tariff_count else ''))
        return lines


def report_pages(state, load_report):
    """Stream bounded pages; only one cycle report is resident at a time.

    The caller runs iteration in a worker thread, then sends pages outside the
    execution task. Values are already reconciled by offline_report; do not
    infer fees, reciprocal fills or completeness from a process exit status.
    """
    from .telegram_cards import LEGEND, btc, clock, planned_quantity, seconds, usd
    total, completed = state.get('series_total'), state.get('series_completed', 0)
    finished = completed == total and state.get('status') == 'FINISHED'
    title = 'Серия завершена' if finished else 'Серия остановлена'
    header = f'{"🏁" if finished else "⛔"} <b>{title}: {text(completed, 48)}/{text(total, 48)}</b>'
    names = state.get('series_slots')
    if not isinstance(names, list):
        yield header + '\nСписок циклов старой команды не сохранён. Общий PnL неизвестен; отдельный результат доступен в локальном отчёте.'
        return
    totals = Totals()
    page = header
    seen = set()
    for index, name in enumerate(names, 1):
        report = mapping(load_report(name)) if name not in seen else {}
        seen.add(name)
        _, g, n = totals.add(report)
        quantity = planned_quantity(report) if report else None
        line = (f'{index}. <code>{text(name, 40)}</code>'
                + (f' · {text(btc(quantity), 24)} BTC' if quantity is not None else '')
                + f' · {phase_label(report, "opening")}/{phase_label(report, "closing")}'
                + (' 🛠' if residual_label(report) else '') + ' · ')
        line += (f'{usd(n, signed=True)} $' if n is not None else
                 f'{usd(g, signed=True)} $ до ком.' if g is not None else 'PnL ?')
        if report.get('status') != 'COMPLETE':
            line += ' · неполный'
        if len((page + '\n' + line).encode('utf-16-le')) // 2 > 3500:
            yield page
            page = header + ' · продолжение'
        page += '\n' + line
    complete = (bool(names) and state.get('status') == 'FINISHED' and completed == len(names)
                and len(seen) == len(names))
    outcomes, starts, terminals = totals.outcomes, totals.starts, totals.terminals
    summary = ['<b>Итого</b>']
    summary.append(f'Циклы: ✅ {outcomes["ok"]} · 🟡 {outcomes["partial"]} · ⛔ {outcomes["stop"]}')
    if names and len(starts) == len(names):
        span = max(terminals) - min(starts)
        if clock(min(starts)) and clock(max(terminals)) and seconds(span):
            summary.append(f'Время: {clock(min(starts))} → {clock(max(terminals))} МСК · {seconds(span)}')
    summary.extend(totals.lines(complete))
    if not names:
        summary.append('Ни один цикл с сохранённым результатом не найден.')
    if state.get('series_precloses'):
        summary.append('Предварительные закрытия старых позиций исключены из PnL и оборота циклов.')
    summary.append(f'<i>{LEGEND}</i>')
    block = '\n'.join(summary)
    if names and len((page + '\n\n' + block).encode('utf-16-le')) // 2 <= 3500:
        yield page + '\n\n' + block
        return
    if names:
        yield page
    yield header + '\n' + block
