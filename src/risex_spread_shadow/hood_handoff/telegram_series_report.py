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


def report_pages(state, load_report):
    """Stream bounded pages; only one cycle report is resident at a time.

    The caller runs iteration in a worker thread, then sends pages outside the
    execution task. Values are already reconciled by offline_report; do not
    infer fees, reciprocal fills or completeness from a process exit status.
    """
    from .telegram_cards import FOOTNOTE, LEGEND, btc, clock, cycle_times, fee_estimate, outcome, planned_quantity, seconds, usd
    total, completed = state.get('series_total'), state.get('series_completed', 0)
    finished = completed == total and state.get('status') == 'FINISHED'
    title = 'Серия завершена' if finished else 'Серия остановлена'
    header = f'{"🏁" if finished else "⛔"} <b>{title}: {text(completed, 48)}/{text(total, 48)}</b>'
    names = state.get('series_slots')
    if not isinstance(names, list):
        yield header + '\nСписок циклов старой команды не сохранён. Общий PnL неизвестен; отдельный результат доступен в локальном отчёте.'
        return
    gross, net, turnover, estimate = Decimal(0), Decimal(0), Decimal(0), Decimal(0)
    gross_count = net_count = turnover_count = estimate_count = 0
    outcomes = {'ok': 0, 'partial': 0, 'stop': 0}
    starts, terminals = [], []
    seen_receipts = set()
    binding_key = None
    page = header
    seen = set()
    for index, name in enumerate(names, 1):
        report = mapping(load_report(name)) if name not in seen else {}
        seen.add(name)
        binding = mapping(report.get('binding'))
        accounts = (binding.get('source_account_index'), binding.get('receiver_account_index'))
        key = (binding.get('api_base_url'), binding.get('market_id'), binding.get('market_symbol'),
               tuple(sorted(accounts)) if all(type(a) is int for a in accounts) else None)
        if binding_key is None and key[-1] is not None:
            binding_key = key
        order = mapping(report.get('order_state'))
        resolved = (report.get('status') == 'COMPLETE' and key[-1] is not None and key == binding_key
                    and nominal_usd(binding)
                    and order.get('unresolved_intents') == [] and order.get('unresolved_observed_orders') == [])
        volume = executed_turnover(report, seen_receipts) if resolved else None
        if volume is not None:
            turnover = add_exact(turnover, volume)
            turnover_count += 1
            bound = fee_estimate(report)
            if bound is not None:
                estimate = add_exact(estimate, bound)
                estimate_count += 1
        valid = resolved and mapping(report.get('inventory')).get('status') == 'CONFIRMED_FLAT'
        pnl = mapping(mapping(report.get('economics')).get('closed_execution_pnl'))
        g = number(pnl.get('gross')) if valid and pnl.get('status') in ('PROVEN', 'GROSS_ONLY') and pnl.get('unit') == 'quote_currency' else None
        n = number(pnl.get('net')) if g is not None and pnl.get('status') == 'PROVEN' else None
        if g is not None:
            gross = add_exact(gross, g)
            gross_count += 1
        if n is not None:
            net = add_exact(net, n)
            net_count += 1
        outcomes['stop' if not valid else outcome(report)] += 1
        start, terminal, _, _ = cycle_times(report) if report else (None, None, None, {})
        if start is not None and terminal is not None:
            starts.append(start)
            terminals.append(terminal)
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
    gross_known = complete and gross_count == len(names)
    net_known = complete and net_count == len(names)
    volume_known = complete and turnover_count == len(names)
    summary = ['<b>Итого</b>']
    summary.append(f'Циклы: ✅ {outcomes["ok"]} · 🟡 {outcomes["partial"]} · ⛔ {outcomes["stop"]}')
    if names and len(starts) == len(names):
        span = max(terminals) - min(starts)
        if clock(min(starts)) and clock(max(terminals)) and seconds(span):
            summary.append(f'Время: {clock(min(starts))} → {clock(max(terminals))} · {seconds(span)}')
    summary.append('Оборот обоих счетов: ' + (f'<b>{usd(turnover)} $</b>' if volume_known else 'неизвестен'))
    if not volume_known and turnover_count:
        summary.append(f'Подтверждённая часть оборота ({turnover_count}/{len(names)}): {usd(turnover)} $')
    summary.append('PnL до комиссий: ' + (f'<b>{usd(gross, signed=True)} $</b>' if gross_known else 'неизвестен'))
    if not gross_known and gross_count:
        summary.append(f'Известная часть до комиссий ({gross_count}/{len(names)}): {usd(gross, signed=True)} $')
    summary.append('PnL после комиссий: ' + (f'<b>{usd(net, signed=True)} $</b>' if net_known else 'неизвестен'))
    if not net_known and net_count:
        summary.append(f'Известная часть после комиссий ({net_count}/{len(names)}): {usd(net, signed=True)} $')
    if net_known and gross_known:
        summary.append(f'Комиссии: {usd(gross - net)} $')
    elif estimate_count and estimate > 0:
        scope = '' if estimate_count == len(names) else f' по {estimate_count}/{len(names)} циклам'
        summary.append(f'Комиссии: биржа не прислала · оценка по тарифу{scope} ≤ {usd(estimate)} $')
    else:
        summary.append('Комиссии: неизвестны')
    if not names:
        summary.append('Ни один цикл с сохранённым результатом не найден.')
    if state.get('series_precloses'):
        summary.append('Предварительные закрытия старых позиций исключены из PnL и оборота циклов.')
    summary.append(f'<i>{LEGEND}</i>')
    summary.append(f'<i>{FOOTNOTE}</i>')
    block = '\n'.join(summary)
    if names and len((page + '\n\n' + block).encode('utf-16-le')) // 2 <= 3500:
        yield page + '\n\n' + block
        return
    if names:
        yield page
    yield header + '\n' + block
