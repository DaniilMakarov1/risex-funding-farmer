"""Read-only paginated series results from explicit, durable cycle membership."""
from decimal import Decimal, localcontext

from .operator_view import amount, number, nominal_usd
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
    rows = mapping(report.get('paired_execution')).get('direct_counterparty_match', [])
    row = next((mapping(r) for r in rows if mapping(r).get('phase') == phase), {})
    status = row.get('status')
    if status == 'NOT_ATTEMPTED':
        return 'не выполнялось'
    if status == 'NO_FILL':
        return 'без исполнений'
    own = number(row.get('matched_quantity')) or Decimal(0)
    external = any((number(row.get(f'external_{leg}_quantity')) or Decimal(0)) > 0
                   for leg in ('source', 'receiver'))
    unknown = any((number(row.get(f'unproved_{leg}_quantity')) or Decimal(0)) > 0
                  or row.get(f'{leg}_state') == 'UNKNOWN' for leg in ('source', 'receiver'))
    if external:
        return ('смешанное: свои + внешние' if own > 0 else 'внешние участники') + ('; есть неизвестное' if unknown else '')
    if status == 'MATCHED' and not unknown:
        return 'парное'
    if own > 0:
        return 'частично парное; остальное не подтверждено'
    return 'не подтверждено'


def residual_label(report):
    fills = [r for r in report.get('confirmed_fills', []) if r.get('phase') == 'fallback']
    actions = [r for r in report.get('dispatched_actions', []) if r.get('phase') == 'fallback']
    if not fills:
        return '; остаток: исполнение не подтверждено' if actions else ''
    binding = mapping(report.get('binding'))
    accounts = {binding.get('source_account_index'), binding.get('receiver_account_index')}
    labels = set()
    for fill in fills:
        peer = fill.get('counterparty_account_index')
        labels.add('неизвестный контрагент' if type(peer) is not int else
                   'свой счёт' if peer in accounts else 'внешние участники')
    return '; остаток: ' + ', '.join(sorted(labels))


def report_pages(state, load_report):
    """Stream bounded pages; only one cycle report is resident at a time.

    The caller runs iteration in a worker thread, then sends pages outside the
    execution task. Values are already reconciled by offline_report; do not
    infer fees, reciprocal fills or completeness from a process exit status.
    """
    total, completed = state.get('series_total'), state.get('series_completed', 0)
    title = ('Серия завершена' if completed == total and state.get('status') == 'FINISHED'
             else 'Серия остановлена')
    header = f'<b>{title}: {text(completed, 48)}/{text(total, 48)}</b>'
    names = state.get('series_slots')
    if not isinstance(names, list):
        yield header + '\nСписок циклов старой команды не сохранён. Общий PnL неизвестен; отдельный результат доступен в локальном отчёте.'
        return
    gross, net, turnover = Decimal(0), Decimal(0), Decimal(0)
    gross_count = net_count = turnover_count = 0
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
        line = (f'{index}. {text(name, 40)}: открытие — {phase_label(report, "opening")}; '
                f'закрытие — {phase_label(report, "closing")}{residual_label(report)}. ')
        line += (f'PnL: {text(amount(n))} USD' if n is not None else
                 f'PnL до комиссий: {text(amount(g))} USD; чистый — неизвестен')
        if report.get('status') != 'COMPLETE':
            line += '; результат неполный'
        if len((page + '\n' + line).encode('utf-16-le')) // 2 > 3500:
            yield page
            page = header + ' · продолжение'
        page += '\n' + line
    if names:
        yield page
    complete = (bool(names) and state.get('status') == 'FINISHED' and completed == len(names)
                and len(seen) == len(names))
    gross_known = complete and gross_count == len(names)
    net_known = complete and net_count == len(names)
    footer = (header + '\n<b>Общий PnL двух счетов, USD</b>\n'
              + 'До комиссий: ' + (text(amount(gross)) + ' USD' if gross_known else 'неизвестен')
              + '\nПосле комиссий: ' + (text(amount(net)) + ' USD' if net_known else 'неизвестен') + '.')
    if not gross_known and gross_count:
        footer += f'\nИзвестная часть до комиссий ({gross_count}/{len(names)}): {text(amount(gross))} USD.'
    if not net_known and net_count:
        footer += f'\nИзвестная часть после комиссий ({net_count}/{len(names)}): {text(amount(net))} USD.'
    volume_known = complete and turnover_count == len(names)
    footer += '\n<b>Исполненный объём обоих счетов:</b> ' + (text(amount(turnover)) + ' USD' if volume_known else 'неизвестен') + '.'
    if not volume_known and turnover_count:
        footer += f'\nПодтверждённая часть объёма ({turnover_count}/{len(names)}): {text(amount(turnover))} USD.'
    footer += '\nОбъём = сумма исполнений покупки и продажи на обоих счетах.'
    footer += '\nUSD по номиналу USDG; обменный курс USDG/USD не применяется.'
    if not names:
        footer += '\nНи один цикл с сохранённым результатом не найден.'
    footer += '\nУчитывает закрытие остатков внутри циклов. Фандинг не включён.'
    if state.get('series_precloses'):
        footer += '\nПредварительные закрытия старых позиций исключены из PnL и объёма циклов.'
    yield footer
