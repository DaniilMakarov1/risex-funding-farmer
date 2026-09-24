"""Saved cycle and separately launched close remain distinct in operator status."""
import json

import pytest

from test_hood_telegram_control import setup
from test_hood_telegram_messages import valid
from risex_spread_shadow.hood_handoff.telegram_messages import saved_message


def report():
    return {
        'status': 'COMPLETE',
        'cycle': {'terminal_at': 100.0},
        'binding': {'market_id': 1, 'market_symbol': 'BTC',
                    'source_account_index': 11, 'receiver_account_index': 22},
        'inventory': {'status': 'OPEN_INVENTORY', 'source': '0.00040', 'receiver': '0'},
        'paired_execution': {'status': 'FAILED'},
    }


def close_slot(root, name, started, *, terminal=True, status='CONFIRMED_FLAT',
               market=1, accounts=(11, 22), positions=('0', '0')):
    slot = root / name
    slot.mkdir()
    binding = {'market_id': market, 'market_symbol': 'BTC',
               'source_account_index': accounts[0], 'receiver_account_index': accounts[1]}
    rows = [{'sequence': 1, 'run_id': name, 'event': 'CLOSE_STARTED',
             'at': started, 'payload': {'binding': binding}}]
    if terminal:
        rows.append({'sequence': 2, 'run_id': name, 'event': 'CLOSE_COMPLETE',
                     'at': started + 2,
                     'payload': {'status': status, 'symbol': 'BTC',
                                 'positions': [{'account_index': account, 'position': position}
                                               for account, position in zip(accounts, positions)]}})
    (slot / 'close.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in rows))


def test_open_inventory_is_explicit_in_saved_result():
    plain = valid(saved_message('cycle-111', report()))
    assert 'Историческая позиция: ⚠️ открыт остаток' in plain
    assert 'Историческая позиция: ❔ неизвестна' not in plain


def test_later_separate_close_is_shown_but_not_rewritten_into_cycle(tmp_path, monkeypatch):
    c = setup(tmp_path, None)
    (tmp_path / 'cycle-111').mkdir()
    monkeypatch.setattr(c, 'report', lambda name: report())
    close_slot(tmp_path, 'close-006', 110)
    plain = valid(c.summary())
    assert 'Историческая позиция: ⚠️ открыт остаток' in plain
    assert 'Отдельное закрытие после цикла · close-006' in plain
    assert '✅ нулевые позиции подтверждены' in plain
    assert 'не текущий снимок' in plain


@pytest.mark.parametrize('case', ['earlier', 'other_market', 'other_account', 'incomplete',
                                   'partial', 'nonzero', 'newer_incomplete'])
def test_close_cannot_improperly_claim_flat_after_cycle(tmp_path, monkeypatch, case):
    c = setup(tmp_path, None)
    (tmp_path / 'cycle-111').mkdir()
    monkeypatch.setattr(c, 'report', lambda name: report())
    if case == 'earlier':
        close_slot(tmp_path, 'close-001', 90)
    elif case == 'other_market':
        close_slot(tmp_path, 'close-001', 110, market=2)
    elif case == 'other_account':
        close_slot(tmp_path, 'close-001', 110, accounts=(11, 33))
    elif case == 'incomplete':
        close_slot(tmp_path, 'close-001', 110, terminal=False)
    elif case == 'partial':
        close_slot(tmp_path, 'close-001', 110, status='PARTIAL', positions=('0.00040', '0'))
    elif case == 'nonzero':
        close_slot(tmp_path, 'close-001', 110, positions=('0.00040', '0'))
    else:
        close_slot(tmp_path, 'close-001', 110)
        close_slot(tmp_path, 'close-002', 120, terminal=False)
    plain = valid(c.summary())
    assert 'Историческая позиция: ⚠️ открыт остаток' in plain
    assert '✅ нулевые позиции подтверждены' not in plain
    if case in {'incomplete', 'partial', 'nonzero', 'newer_incomplete'}:
        assert '❔ итог закрытия не подтверждён' in plain
