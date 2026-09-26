"""Wallet pool: validation, uniform pair draw, eligibility, pool-wide readiness,
pool closing, owner wallet command and Telegram views.  Synthetic only: no
SDK network, no Keychain, no real credentials."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from decimal import Decimal
import hashlib
import itertools
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from risex_spread_shadow.hood_handoff import cli as cli_module
from risex_spread_shadow.hood_handoff import operator_recovery as recovery
from risex_spread_shadow.hood_handoff import random_cycle as random_cycle_module
from risex_spread_shadow.hood_handoff import telegram_control as bot
from risex_spread_shadow.hood_handoff import telegram_messages as views
from risex_spread_shadow.hood_handoff import wallet_pool as wp
from risex_spread_shadow.hood_handoff import wallets as wallet_cli
from risex_spread_shadow.hood_handoff.contracts import AccountMarginEvidence, OrderPlan, OrderSnapshot, PreflightBlocked
from risex_spread_shadow.hood_handoff.journal import DurableJournal
from risex_spread_shadow.hood_handoff.keychain import (
    KeychainBinding,
    KeychainSecretProvider,
    MemoryKeychainBackend,
)
from risex_spread_shadow.hood_handoff.operator_view import close_result_lines, read_launch_failure
from risex_spread_shadow.hood_handoff.random_cycle import _account_payload
from risex_spread_shadow.hood_handoff.telegram_cards import live_steps, positions_text, wallet_steps

from test_hood_handoff_random_cycle import (
    AdvancingClock,
    CycleClient,
    FixedRng,
    _write_simple_launcher_fixture,
    account,
    book,
    cycle_config,
    metadata,
    run_random_cycle,
)
from test_hood_telegram_control import setup
from test_hood_telegram_messages import valid


def write_pool(operator: Path, active, paused=(), *, mode=0o600, extra=None):
    payload = {'schema': wp.POOL_SCHEMA, 'wallets': list(active)}
    if paused:
        payload['paused'] = list(paused)
    payload.update(extra or {})
    path = operator / wp.POOL_FILE_NAME
    path.write_text(json.dumps(payload))
    path.chmod(mode)
    return path


def pair_config(**overrides):
    return SimpleNamespace(source_account_index=11, receiver_account_index=22, **overrides)


# --------------------------------------------------------------------------
# Pool file


def test_absent_pool_is_exactly_the_configured_pair(tmp_path):
    pool = wp.load_wallet_pool(tmp_path, pair_config())
    assert pool == wp.WalletPool(active=(11, 22), paused=(), from_file=False)
    assert pool.all == (11, 22)


def test_valid_pool_file_round_trips_owner_only(tmp_path):
    write_pool(tmp_path, [11, 22, 33], [44])
    pool = wp.load_wallet_pool(tmp_path, pair_config())
    assert (pool.active, pool.paused, pool.from_file) == ((11, 22, 33), (44,), True)
    changed = wp.updated_pool(pool, add=55)
    wp.save_wallet_pool(tmp_path, changed)
    path = tmp_path / wp.POOL_FILE_NAME
    assert path.stat().st_mode & 0o777 == 0o600
    assert wp.load_wallet_pool(tmp_path, pair_config()).all == (11, 22, 33, 55, 44)
    assert not list(tmp_path.glob('.wallets-*.tmp'))


@pytest.mark.parametrize('case', ['symlink', 'group_readable', 'not_json', 'schema', 'extra_key',
                                  'duplicate', 'duplicate_paused', 'one_active', 'bool', 'zero',
                                  'string', 'huge', 'pair_missing'])
def test_unsafe_or_malformed_pool_is_refused(tmp_path, case):
    if case == 'symlink':
        target = write_pool(tmp_path, [11, 22, 33])
        target.rename(tmp_path / 'real.json')
        (tmp_path / wp.POOL_FILE_NAME).symlink_to(tmp_path / 'real.json')
    elif case == 'group_readable':
        write_pool(tmp_path, [11, 22, 33], mode=0o640)
    elif case == 'not_json':
        (tmp_path / wp.POOL_FILE_NAME).write_text('{')
        (tmp_path / wp.POOL_FILE_NAME).chmod(0o600)
    elif case == 'schema':
        write_pool(tmp_path, [11, 22, 33], extra={'schema': 'other'})
    elif case == 'extra_key':
        write_pool(tmp_path, [11, 22, 33], extra={'keys': ['secret']})
    elif case == 'duplicate':
        write_pool(tmp_path, [11, 22, 22])
    elif case == 'duplicate_paused':
        write_pool(tmp_path, [11, 22, 33], [33])
    elif case == 'one_active':
        write_pool(tmp_path, [11], [22])
    elif case == 'bool':
        write_pool(tmp_path, [11, 22, True])
    elif case == 'zero':
        write_pool(tmp_path, [11, 22, 0])
    elif case == 'string':
        write_pool(tmp_path, [11, 22, '33'])
    elif case == 'huge':
        write_pool(tmp_path, [11, 22, 2 ** 48 + 1])
    else:
        write_pool(tmp_path, [11, 33, 44])
    with pytest.raises(wp.WalletPoolError):
        wp.load_wallet_pool(tmp_path, pair_config())


def test_membership_changes_keep_two_active_wallets():
    pool = wp.WalletPool(active=(11, 22), from_file=False)
    with pytest.raises(wp.WalletPoolError, match='at least two active'):
        wp.updated_pool(pool, pause=11)
    grown = wp.updated_pool(pool, add=33)
    assert grown.active == (11, 22, 33) and grown.from_file
    with pytest.raises(wp.WalletPoolError, match='already'):
        wp.updated_pool(grown, add=22)
    paused = wp.updated_pool(grown, pause=22)
    assert (paused.active, paused.paused) == ((11, 33), (22,))
    with pytest.raises(wp.WalletPoolError, match='already'):
        wp.updated_pool(paused, add=22)
    with pytest.raises(wp.WalletPoolError, match='only a paused'):
        wp.updated_pool(paused, resume=11)
    with pytest.raises(wp.WalletPoolError, match='only an active'):
        wp.updated_pool(paused, pause=22)
    assert wp.updated_pool(paused, resume=22).active == (11, 33, 22)


# --------------------------------------------------------------------------
# Uniform draw


class EnumeratingRng:
    def __init__(self, values):
        self.values = list(values)
        self.bounds = []

    def randint(self, lower, upper):
        self.bounds.append((lower, upper))
        return self.values.pop(0)


@pytest.mark.parametrize('size', [2, 3, 5, 8])
def test_every_ordered_pair_has_exactly_one_draw_so_pairs_are_uniform(size):
    wallets = [100 + 7 * i for i in range(size)][::-1]  # Unsorted input.
    outcomes = []
    for first in range(size):
        for second in range(size - 1):
            rng = EnumeratingRng([first, second])
            outcomes.append(wp.draw_pair(wallets, rng))
            assert rng.bounds == [(0, size - 1), (0, size - 2)]
    # Each of the n*(n-1) equally likely integer draws gives a distinct ordered
    # pair of distinct wallets, so every unordered pair has probability 2/(n(n-1)).
    assert sorted(outcomes) == sorted(itertools.permutations(sorted(wallets), 2))


def test_draw_needs_two_distinct_wallets():
    with pytest.raises(wp.WalletsUnavailable):
        wp.draw_pair([5, 5])


def test_system_random_draw_covers_all_pairs():
    seen = {tuple(sorted(wp.draw_pair([1, 2, 3, 4]))) for _ in range(400)}
    assert seen == {tuple(sorted(pair)) for pair in itertools.combinations([1, 2, 3, 4], 2)}


# --------------------------------------------------------------------------
# Eligibility


def test_requirement_is_conservative_minimum_order_at_4x_plus_reserve(tmp_path):
    config = cycle_config(tmp_path, margin_reserve=random_cycle_module.OpeningMarginReserve(
        Decimal('0.10'), Decimal('0.02')))
    # Independent arithmetic: bid 100.0 / ask 100.2, no mark -> ask; minimum
    # 0.10 base and 10 quote (at the bid: 0.10) -> 0.10 BTC; 4x = 25%;
    # taker cap 0.00035; adverse ask-bid distance 0.2; 5% margin; reserve 0.10.
    variable = Decimal('0.10') * (Decimal('100.2') * Decimal('0.25')
                                  + Decimal('100.2') * Decimal('0.00035') + Decimal('0.2'))
    assert variable == Decimal('2.528507')
    expected = variable * Decimal('1.05') + Decimal('0.10')
    assert wp.wallet_requirement(metadata(), book(), config) == expected == Decimal('2.75493235')


def test_requirement_uses_bid_for_quote_minimum(tmp_path):
    config = cycle_config(tmp_path)
    tight = metadata(minimum_base='0.01', minimum_quote='10.01')
    # 10.01 / 100.0 = 0.1001 -> 11 ticks of 0.01 (the ask would give 10 ticks).
    variable = Decimal('0.11') * (Decimal('100.2') * Decimal('0.25')
                                  + Decimal('100.2') * Decimal('0.00035') + Decimal('0.2'))
    assert wp.wallet_requirement(tight, book(), config) == variable * Decimal('1.05')


class SelectionClient:
    def __init__(self, clock, states, *, fail=()):
        self.clock = clock
        self.states = states
        self.fail = set(fail)
        self.reads = []
        self.active = 0
        self.peak = 0

    async def market_metadata(self, market_id):
        return metadata(self.clock.now())

    async def order_book(self, market_id):
        return book(self.clock.now())

    async def public_account_state(self, index, market_id):
        assert market_id == 7
        self.reads.append(index)
        self.active += 1
        self.peak = max(self.peak, self.active)
        try:
            await asyncio.sleep(0)
            if index in self.fail:
                raise RuntimeError('SECRET transport text must not surface')
            balance, position, ready = self.states[index]
            return wp.WalletState(index, Decimal(balance), Decimal(position), ready, self.clock.now())
        finally:
            self.active -= 1


@pytest.mark.asyncio
async def test_selection_skips_and_reports_every_unready_wallet(tmp_path):
    clock = AdvancingClock()
    config = cycle_config(tmp_path)
    states = {11: ('1000', '0', True), 22: ('1000', '0', True), 33: ('1', '0', True),
              44: ('1000', '0.1', True), 55: ('1000', '0', True), 77: ('1000', '0', False),
              88: ('1000', '0', True), 99: ('1000', '0', True)}
    pool = wp.WalletPool(active=(11, 22, 33, 44, 55, 66, 77, 88), paused=(99,), from_file=True)
    client = SelectionClient(clock, states, fail={66})
    rng = EnumeratingRng([1, 0])
    record = await wp.select_wallet_pair(config, pool, client, has_key=lambda index: index != 55,
                                         rng=rng, clock=clock.now)
    assert record['eligible'] == [11, 22, 88]
    assert record['skipped'] == [
        {'account_index': 33, 'reason': 'LOW_BALANCE'},
        {'account_index': 44, 'reason': 'NOT_FLAT'},
        {'account_index': 55, 'reason': 'KEY_MISSING'},
        {'account_index': 66, 'reason': 'UNAVAILABLE'},
        {'account_index': 77, 'reason': 'UNAVAILABLE'},
    ]
    assert record['pair'] == [22, 11]
    assert record['pool_size'] == 8 and record['paused'] == [99]
    assert 99 not in client.reads and sorted(client.reads) == [11, 22, 33, 44, 55, 66, 77, 88]
    assert client.peak <= wp.SELECTION_READ_CONCURRENCY
    assert 'SECRET' not in json.dumps(record)
    assert Decimal(record['requirement_quote']) == wp.wallet_requirement(metadata(), book(), config)


@pytest.mark.asyncio
async def test_fewer_than_two_ready_wallets_draw_no_pair(tmp_path):
    clock = AdvancingClock()
    pool = wp.WalletPool(active=(11, 22, 33), from_file=True)
    client = SelectionClient(clock, {11: ('1000', '0', True), 22: ('0.5', '0', True),
                                     33: ('1000', '-1', True)})
    record = await wp.select_wallet_pair(cycle_config(tmp_path), pool, client, has_key=lambda _: True,
                                         rng=FixedRng(), clock=clock.now)
    assert record['pair'] is None and record['eligible'] == [11]


@pytest.mark.asyncio
async def test_public_account_state_parses_exact_market_position_and_balance(tmp_path):
    from risex_spread_shadow.hood_handoff.sdk import LighterSdkClient

    payloads = {33: {'index': 33, 'status': 1, 'available_balance': '12.5',
                     'positions': [{'market_id': 2, 'position': '5', 'sign': 1},
                                   {'market_id': 7, 'position': '0.25', 'sign': -1}]}}

    class AccountApi:
        def __init__(self, _client):
            pass

        async def account(self, **kwargs):
            assert kwargs['by'] == 'index' and kwargs['active_only'] is False
            return {'code': 200, 'accounts': [payloads[int(kwargs['value'])]]}

    class Module:
        class Configuration:
            def __init__(self, **kwargs):
                pass

        class ApiClient:
            def __init__(self, configuration):
                pass

    Module.AccountApi = AccountApi

    class NoSecrets:
        def private_key(self, *args):
            raise AssertionError('public read must not request a key')

    client = LighterSdkClient(cycle_config(tmp_path, api_key_index=4), source_account_index=11,
                              receiver_account_index=22, secrets=NoSecrets(), market_evidence={},
                              clock=lambda: 1234.0)
    client._lighter = lambda: Module
    state = await client.public_account_state(33, 7)
    assert state == wp.WalletState(33, Decimal('12.5'), Decimal('-0.25'), True, 1234.0)
    payloads[33]['positions'].append({'market_id': 7, 'position': '1', 'sign': 1})
    with pytest.raises(Exception, match='malformed'):
        await client.public_account_state(33, 7)
    payloads[33]['positions'] = [{'market_id': 7, 'position': '1', 'sign': 0}]
    with pytest.raises(Exception, match='sign'):
        await client.public_account_state(33, 7)
    payloads[33] = {'index': 34, 'status': 1, 'available_balance': '1', 'positions': []}
    with pytest.raises(Exception, match='identity'):
        await client.public_account_state(33, 7)


# --------------------------------------------------------------------------
# Pool-wide readiness and closing


class Ledger:
    """Shared synthetic venue state for many wallets."""

    def __init__(self, clock, positions, *, orders=None):
        self.clock = clock
        self.positions = {index: Decimal(value) for index, value in positions.items()}
        self.orders = dict(orders or {})
        self.reads = []
        self.clients = []


class PairClient(CycleClient):
    """A synthetic client bound to one pair; reads any wallet, mutates only its pair."""

    def __init__(self, ledger, source, receiver, *, unknown=False):
        self.ledger = ledger
        self.source_account_index, self.receiver_account_index = source, receiver
        saved = dict(ledger.positions)
        super().__init__(ledger.clock)
        ledger.positions.clear()
        ledger.positions.update(saved)
        # A terminal old order lets the synthetic fallback path find its source.
        self._save_order(OrderSnapshot(source, 7, f'old-{source}', 777, 'canceled', 'SELL', 'LIMIT',
                                       'POST_ONLY', False, Decimal('.2'), Decimal('.2'), Decimal(0),
                                       Decimal('100.1'), ledger.clock.now()))
        self.unknown = unknown
        self.closed = False
        ledger.clients.append(self)

    @property
    def source_position(self):
        return self.ledger.positions[self.source_account_index]

    @source_position.setter
    def source_position(self, value):
        self.ledger.positions[self.source_account_index] = value

    @property
    def receiver_position(self):
        return self.ledger.positions[self.receiver_account_index]

    @receiver_position.setter
    def receiver_position(self, value):
        self.ledger.positions[self.receiver_account_index] = value

    async def account_snapshot(self, account_index, market_id):
        self.ledger.reads.append(account_index)
        if account_index in (self.source_account_index, self.receiver_account_index) \
                and account_index not in self.ledger.orders:
            return await super().account_snapshot(account_index, market_id)
        assert market_id == 7
        return account(account_index, self.ledger.positions[account_index], observed_at=self.clock.now(),
                       active_orders=self.ledger.orders.get(account_index, ()))

    async def submit_order(self, plan):
        assert plan.account_index in (self.source_account_index, self.receiver_account_index)
        assert plan.reduce_only and plan.order_type == 'MARKET' and plan.time_in_force == 'IOC'
        receipt = await super().submit_order(plan)
        if self.unknown:
            raise TimeoutError('ambiguous first send')
        return receipt

    async def aclose(self):
        self.closed = True


def pool_setup(tmp_path, positions, active, paused=(), *, orders=None):
    tmp_path.chmod(0o700)
    write_pool(tmp_path, active, paused)
    clock = AdvancingClock()
    ledger = Ledger(clock, positions, orders=orders)
    return clock, ledger, wp.load_wallet_pool(tmp_path, pair_config())


def active_order(index):
    return OrderSnapshot(index, 7, f'live-{index}', 5, 'open', 'BUY', 'LIMIT', 'POST_ONLY', False,
                         Decimal('.1'), Decimal('.1'), Decimal(0), Decimal('99'), 1000.0)


@pytest.mark.asyncio
async def test_ready_proof_covers_every_pool_wallet_including_paused(tmp_path):
    clock, ledger, pool = pool_setup(tmp_path, {11: 0, 22: 0, 33: 0, 44: 0}, [11, 22, 33], [44])
    client = PairClient(ledger, 11, 22)
    source, receiver, proof = await recovery.inspect_current(
        cycle_config(tmp_path / 'unused'), client, tmp_path, require_flat=True, clock=clock, pool=pool)
    assert (source.account_index, receiver.account_index) == (11, 22)
    assert proof['status'] == 'READY'
    assert [row['account_index'] for row in proof['accounts']] == [11, 22, 33, 44]
    assert proof['wallets'] == {'active': [11, 22, 33], 'paused': [44], 'from_file': True}
    assert proof['source'] == proof['accounts'][0] and proof['receiver'] == proof['accounts'][1]
    assert bot.Controller._exact_recovery_positions(proof) == [0, 0, 0, 0]
    stored = recovery.persisted_proof(proof)
    assert stored['accounts'][2] == {
        'account_index': 33, 'market_id': 7, 'signed_position': '0', 'observed_at': clock.now(),
        'source_identity': 'cycle-account-33', 'available_balance': '1000', 'active_order_count': 0}
    assert 'margin_evidence' not in json.dumps(stored['accounts'])
    # Legacy callers without a pool keep the exact two-account proof shape.
    _, _, legacy = await recovery.inspect_current(
        cycle_config(tmp_path / 'unused'), client, tmp_path, require_flat=True, clock=clock)
    assert 'accounts' not in legacy and 'wallets' not in legacy


@pytest.mark.asyncio
@pytest.mark.parametrize('case', ['paused_position', 'active_position', 'active_order'])
async def test_ready_refuses_any_wallet_position_or_order(tmp_path, case):
    positions = {11: 0, 22: 0, 33: 0, 44: 0}
    orders = None
    if case == 'paused_position':
        positions[44] = '0.1'
    elif case == 'active_position':
        positions[33] = '-0.2'
    else:
        orders = {33: (active_order(33),)}
    clock, ledger, pool = pool_setup(tmp_path, positions, [11, 22, 33], [44], orders=orders)
    message = 'wallet 33 has active cycle-market orders' if case == 'active_order' else 'positions remain'
    with pytest.raises(PreflightBlocked, match=message):
        await recovery.inspect_current(cycle_config(tmp_path / 'unused'), PairClient(ledger, 11, 22),
                                       tmp_path, require_flat=True, clock=clock, pool=pool)
    if case != 'active_order':
        _, _, proof = await recovery.inspect_current(
            cycle_config(tmp_path / 'unused'), PairClient(ledger, 11, 22), tmp_path,
            require_flat=False, clock=clock, pool=pool)
        assert proof['status'] == 'CLOSE_READY'
        assert [Decimal(row['signed_position']) for row in proof['accounts']] == \
            [Decimal(positions[index]) for index in (11, 22, 33, 44)]


@pytest.mark.asyncio
async def test_wallet_reads_are_bounded_and_stale_reads_refused(tmp_path):
    wallets_list = [11, 22] + list(range(30, 38))
    clock, ledger, pool = pool_setup(tmp_path, {index: 0 for index in wallets_list}, wallets_list)
    config = cycle_config(tmp_path / 'unused')

    class Tracking(PairClient):
        active = peak = 0

        async def account_snapshot(self, account_index, market_id):
            type(self).active += 1
            type(self).peak = max(type(self).peak, type(self).active)
            try:
                await asyncio.sleep(0)
                return await super().account_snapshot(account_index, market_id)
            finally:
                type(self).active -= 1

    engine = random_cycle_module.RandomCycleEngine(Tracking(ledger, 11, 22), clock=clock)
    wallets = await recovery._wallet_snapshots(engine, config, pool.all)
    assert sorted(wallets) == sorted(wallets_list)
    assert Tracking.peak <= recovery.WALLET_READ_CONCURRENCY

    class Stale(PairClient):
        async def account_snapshot(self, account_index, market_id):
            snapshot = await super().account_snapshot(account_index, market_id)
            return replace(snapshot, observed_at=snapshot.observed_at - 11) if account_index == 35 else snapshot

    engine = random_cycle_module.RandomCycleEngine(Stale(ledger, 11, 22), clock=clock)
    with pytest.raises(PreflightBlocked, match='wallet 35 account state is stale'):
        await recovery._wallet_snapshots(engine, config, pool.all)


def write_intent(operator, account_index, *, complete=True):
    slot = operator / 'cycle-001'
    slot.mkdir(mode=0o700)
    journal = DurableJournal(slot / 'opening.jsonl', clock=lambda: 1000)
    plan = OrderPlan(account_index, 7, 'SELL', Decimal('.2'), 20, Decimal('100.1'), 1001, 'LIMIT',
                     'POST_ONLY', False, 1300000, 778)
    journal.acquire_attempt()
    try:
        journal.append('SOURCE_DISPATCH_INTENT', {'plan': plan.as_dict()})
        journal.append('SOURCE_DISPATCH_RESULT', {'accepted': True, 'order_id': 'hist'})
        if complete:
            order = {'account_index': account_index, 'market_id': 7, 'order_id': 'hist',
                     'client_order_index': 778, 'side': 'SELL', 'order_type': 'LIMIT',
                     'time_in_force': 'POST_ONLY', 'reduce_only': False, 'price': '100.1',
                     'initial_quantity': '.2', 'filled_quantity': '0', 'remaining_quantity': '.2',
                     'status': 'canceled', 'observed_at': 1000}
            journal.append('COMPLETE', {'outcome': 'UNKNOWN', 'receipt': {'source': {'order': order},
                                                                          'receiver': None}})
    finally:
        journal.release_attempt()


@pytest.mark.asyncio
async def test_history_on_a_pool_wallet_is_accepted_only_with_that_pool(tmp_path):
    clock, ledger, pool = pool_setup(tmp_path, {11: 0, 22: 0, 33: 0}, [11, 22, 33])
    write_intent(tmp_path, 33)
    config = cycle_config(tmp_path / 'unused')
    result = await recovery.resolve_prior(config, PairClient(ledger, 11, 22), tmp_path, clock=clock, pool=pool)
    assert result['previous_intents'] == 1
    with pytest.raises(PreflightBlocked, match='account/market differs'):
        await recovery.resolve_prior(config, PairClient(ledger, 11, 22), tmp_path, clock=clock)
    # A drawn pair's child also accepts the whole pool history.
    drawn = replace(config, source_account_index=22, receiver_account_index=33)
    result = await recovery.resolve_prior(drawn, PairClient(ledger, 22, 33), tmp_path, clock=clock, pool=pool)
    assert result['previous_intents'] == 1


@pytest.mark.asyncio
async def test_unresolved_history_on_a_pool_wallet_is_looked_up_exactly(tmp_path):
    clock, ledger, pool = pool_setup(tmp_path, {11: 0, 22: 0, 33: 0}, [11, 22, 33])
    write_intent(tmp_path, 33, complete=False)
    client = PairClient(ledger, 11, 22)
    with pytest.raises(PreflightBlocked, match='unresolved'):
        await recovery.resolve_prior(cycle_config(tmp_path / 'unused'), client, tmp_path, clock=clock, pool=pool)
    client._save_order(OrderSnapshot(33, 7, 'hist', 778, 'canceled', 'SELL', 'LIMIT', 'POST_ONLY', False,
                                     Decimal('.2'), Decimal('.2'), Decimal(0), Decimal('100.1'), clock.now()))
    result = await recovery.resolve_prior(cycle_config(tmp_path / 'unused'), client, tmp_path, clock=clock, pool=pool)
    assert result['resolved_now'][0]['account_index'] == 33


@pytest.mark.asyncio
async def test_pool_leverage_setting_is_proved_on_its_own_wallet(tmp_path):
    clock, ledger, pool = pool_setup(tmp_path, {11: 0, 22: 0, 33: 0}, [11, 22, 33])
    clock.value = 1000.25
    slot = tmp_path / 'cycle-001'
    slot.mkdir(mode=0o700)
    tx_hash = 'bB' * 40
    journal = DurableJournal(slot / 'cycle.jsonl', clock=clock.now)
    journal.acquire_attempt()
    try:
        journal.append('LEVERAGE_UPDATE_INTENT', {'account_index': 33, 'market_id': 7, 'fraction_bps': 4166,
                                                  'margin_mode': 0, 'source_identity': 'cycle-account-33'})
        journal.append('LEVERAGE_TX_PREPARED', {'account_index': 33, 'market_id': 7, 'fraction_bps': 4166,
                                                'margin_mode': 0, 'api_key_index': 4, 'tx_type': 20,
                                                'nonce': 41, 'tx_hash': tx_hash})
    finally:
        journal.release_attempt()

    class TxClient(PairClient):
        async def account_snapshot(self, index, market_id):
            snapshot = await super().account_snapshot(index, market_id)
            margin = AccountMarginEvidence.from_response(
                account_index=index, market_id=market_id, source_identity=snapshot.source_identity,
                observed_at=snapshot.observed_at,
                selected_position={'margin_mode': 0, 'initial_margin_fraction': '41.66'}, account={})
            return replace(snapshot, margin_evidence=margin)

        async def read_leverage_transaction(self, value):
            return {'hash': value, 'type': 20, 'status': 2, 'account_index': 33, 'api_key_index': 4,
                    'nonce': 41, 'executed_at': 1000, 'committed_at': 1000, 'verified_at': 1000}

        async def read_leverage_next_nonce(self, account_index, api_key_index):
            assert (account_index, api_key_index) == (33, 4)
            return 42

    config = cycle_config(tmp_path / 'unused', api_key_index=4)
    with pytest.raises(PreflightBlocked, match='leverage setting intent is invalid'):
        await recovery.resolve_prior(config, TxClient(ledger, 11, 22), tmp_path, clock=clock)
    ledger.reads.clear()
    result = await recovery.resolve_prior(config, TxClient(ledger, 11, 22), tmp_path, clock=clock, pool=pool)
    assert result['unresolved_leverage_settings'] == 0
    assert ledger.reads == [33]  # Only the wallet whose setting is proved.
    rows = [json.loads(line) for line in (tmp_path / 'recovery-checks.jsonl').read_text().splitlines()]
    assert [row['payload']['account_index'] for row in rows] == [33]


@pytest.mark.asyncio
async def test_pool_close_pairs_every_residual_and_mutates_only_within_each_pair(tmp_path):
    clock, ledger, pool = pool_setup(tmp_path, {11: '.2', 22: 0, 33: '-.3', 44: '.1', 55: 0},
                                     [11, 22, 33, 44], [55])
    base = PairClient(ledger, 11, 22)
    made = []

    def factory(pair_config):
        made.append((pair_config.source_account_index, pair_config.receiver_account_index))
        return PairClient(ledger, pair_config.source_account_index, pair_config.receiver_account_index)

    slot = recovery.allocate_close_slot(tmp_path)
    result = await recovery.close_positions(cycle_config(slot), base, tmp_path, slot, clock=clock,
                                            pool=pool, pair_client_factory=factory)
    assert result['status'] == 'CONFIRMED_FLAT', result
    assert made == [(11, 33), (44, 22)]
    assert all(value == 0 for value in ledger.positions.values())
    submitted = [(plan.account_index, plan.side, plan.quantity)
                 for client in ledger.clients for plan in client.submissions]
    assert submitted == [(11, 'SELL', Decimal('.2')), (33, 'BUY', Decimal('.3')), (44, 'SELL', Decimal('.1'))]
    assert base.submissions == [] and all(client.closed for client in ledger.clients[1:])
    assert [row['account_index'] for row in result['positions']] == [11, 22, 33, 44, 55]
    assert all(Decimal(row['position']) == 0 for row in result['positions'])
    rows = [json.loads(line) for line in (slot / 'close.jsonl').read_text().splitlines()]
    assert rows[0]['payload']['wallets'] == {'active': [11, 22, 33, 44], 'paused': [55], 'from_file': True}
    assert [row['payload'] for row in rows if row['event'] == 'CLOSE_PAIR'] == [
        {'source_account_index': 11, 'receiver_account_index': 33},
        {'source_account_index': 44, 'receiver_account_index': 22}]
    baseline = next(row['payload'] for row in rows if row['event'] == 'CLOSE_BASELINE')
    assert len(baseline['accounts']) == 5 and 'margin_evidence' not in json.dumps(baseline['accounts'])
    assert recovery.load_close_result(slot) == result
    text = '\n'.join(close_result_lines(result))
    assert '✅ Позиции закрыты' in text and 'Кошельков в пуле: 5; с нулевой позицией: 5' in text
    with pytest.raises(PreflightBlocked, match='replayed'):
        await recovery.close_positions(cycle_config(slot), base, tmp_path, slot, clock=clock,
                                       pool=pool, pair_client_factory=factory)


@pytest.mark.asyncio
async def test_pool_close_stops_at_first_unproved_pair(tmp_path):
    clock, ledger, pool = pool_setup(tmp_path, {11: '.2', 22: '-.2', 33: '.1', 44: '-.1'}, [11, 22, 33, 44])

    def factory(pair_config):
        return PairClient(ledger, pair_config.source_account_index, pair_config.receiver_account_index,
                          unknown=True)

    base = PairClient(ledger, 11, 22, unknown=True)
    slot = recovery.allocate_close_slot(tmp_path)
    result = await recovery.close_positions(cycle_config(slot), base, tmp_path, slot, clock=clock,
                                            pool=pool, pair_client_factory=factory)
    assert result['status'] == 'UNKNOWN'
    assert len(base.submissions) == 1  # The first ambiguous send stops everything.
    assert ledger.clients == [base]    # The (33, 44) pair was never touched.
    assert (ledger.positions[33], ledger.positions[44]) == (Decimal('.1'), Decimal('-.1'))


def test_close_pairs_prefer_a_flat_partner():
    assert recovery.close_pairs([], [1, 2]) == []
    assert recovery.close_pairs([3], [1, 2, 3]) == [(3, 1)]
    assert recovery.close_pairs([1, 2, 3], [1, 2, 3, 4]) == [(1, 2), (3, 4)]
    assert recovery.close_pairs([1, 2, 3], [1, 2, 3]) == [(1, 2), (3, 1)]


@pytest.mark.asyncio
async def test_check_recovery_requires_every_pool_key_before_any_read(tmp_path):
    clock, ledger, pool = pool_setup(tmp_path, {11: 0, 22: 0, 33: 0}, [11, 22, 33])
    import time
    clock.value = time.time()  # check_recovery uses the system clock for freshness.
    config = cycle_config(tmp_path / 'unused', api_key_index=4)
    backend = MemoryKeychainBackend({KeychainBinding.from_config(config, index): f'k{index}'
                                     for index in (11, 22)})

    def secrets(cfg, indices, prompt):
        assert tuple(indices) == (11, 22, 33)
        return KeychainSecretProvider.from_config(cfg, indices, backend=backend, prompt=prompt)

    def forbidden(*args, **kwargs):
        pytest.fail('no client may be built while a pool key is missing')

    with pytest.raises(recovery.WalletKeyMissing, match='wallet 33'):
        await recovery.check_recovery(config, tmp_path, client_factory=forbidden, secret_factory=secrets)
    assert ('get', KeychainBinding.from_config(config, 33)) not in backend.calls
    backend.values[KeychainBinding.from_config(config, 33)] = 'k33'
    built = []

    def factory(cfg, **kwargs):
        built.append(kwargs)
        client = PairClient(ledger, 11, 22)
        return client

    proof = await recovery.check_recovery(config, tmp_path, client_factory=factory, secret_factory=secrets)
    assert proof['status'] == 'READY' and len(proof['accounts']) == 3
    assert built[0]['source_account_index'] == 11 and built[0]['receiver_account_index'] == 22
    assert ledger.clients[-1].read_account_indices == (11, 22, 33)
    rows = [json.loads(line) for line in (tmp_path / 'recovery-checks.jsonl').read_text().splitlines()]
    assert rows[-1]['event'] == 'CURRENT_STATE_VERIFIED'
    assert [row['account_index'] for row in rows[-1]['payload']['accounts']] == [11, 22, 33]
    assert 'k33' not in (tmp_path / 'recovery-checks.jsonl').read_text()


# --------------------------------------------------------------------------
# Cycle child: draw, reservation and credentials


def test_selection_is_kept_in_the_immutable_reservation_and_admission_still_works(tmp_path):
    tmp_path.chmod(0o700)
    record = {'schema': wp.SELECTION_SCHEMA, 'at': 5.0, 'pool_size': 3, 'paused': [], 'eligible': [11, 33],
              'skipped': [{'account_index': 22, 'reason': 'LOW_BALANCE'}], 'available_balances': {},
              'requirement_quote': '2', 'pair': [33, 11]}
    route = {'source_account_index': 33, 'receiver_account_index': 11, 'direction': 'SHORT'}
    slot, _ = random_cycle_module.allocate_cycle_slot(tmp_path, random_route=route, wallet_selection=record)
    assert {path.name for path in slot.iterdir()} == {'launch.json'}
    launch = json.loads((slot / 'launch.json').read_text())
    assert launch['wallet_selection'] == record and launch['random_route'] == route
    random_cycle_module.RandomCycleEngine.validate_cycle_directory(slot)
    assert wp.read_selection(slot) == record
    [(key, at, icon, line)] = wallet_steps(slot)
    assert (key, at, icon) == ('wallets', 5.0, '👛')
    assert line == 'кошельки: выбраны 33 и 11 из 2 готовых (всего активных 3); пропущены: 22 — мало баланса'
    assert live_steps(slot)[0][0] == 'wallets'


@pytest.mark.parametrize('exc,code', [(wp.WalletsUnavailable('fewer'), 'WALLETS_UNAVAILABLE'),
                                      (wp.WalletPoolError('bad'), 'WALLET_POOL'),
                                      (recovery.WalletKeyMissing('wallet 33'), 'CREDENTIAL_UNAVAILABLE')])
def test_pool_launch_failures_have_fixed_codes_and_views(tmp_path, exc, code):
    slot = tmp_path / 'cycle-009'
    slot.mkdir(mode=0o700)
    assert cli_module._persist_prejournal_launch_failure(slot, exc) == code
    assert read_launch_failure(slot) == code
    message = views.launch_failure_message('cycle-009', code, detail='кошельки: деталь')
    plain = valid(message)
    assert 'Цикл cycle-009 не начался' in plain and 'кошельки: деталь' in plain


def pool_launcher(tmp_path, monkeypatch, wallets, *, states=None, keys=(11, 22, 33)):
    config_path, operator_dir, evidence_path = _write_simple_launcher_fixture(tmp_path)
    write_pool(operator_dir, wallets)
    clock = AdvancingClock()
    synthetic = CycleClient(clock)
    states = states or {index: ('1000', '0', True) for index in wallets}
    calls = {'providers': [], 'primed': [], 'resolve': [], 'configs': [], 'clients': []}

    class FakeSdkClient:
        def __init__(self, config, *, source_account_index, receiver_account_index, secrets, market_evidence):
            calls['clients'].append((source_account_index, receiver_account_index, type(secrets).__name__))
            self._delegate = synthetic
            synthetic.source_account_index = source_account_index
            synthetic.receiver_account_index = receiver_account_index

        def __getattr__(self, name):
            return getattr(self._delegate, name)

        async def public_account_state(self, index, market_id):
            balance, position, ready = states[index]
            return wp.WalletState(index, Decimal(balance), Decimal(position), ready, clock.now())

        async def aclose(self):
            return None

    backend = MemoryKeychainBackend()

    def provider(config, indices, *, replace, prompt=None):
        for index in keys:
            backend.values.setdefault(KeychainBinding.from_config(config, index), f'synthetic-{index}')
        made = KeychainSecretProvider.from_config(config, indices, backend=backend, replace=replace, prompt=prompt)
        calls['providers'].append((tuple(indices), replace, prompt))
        return made

    def prime(provider_, indices):
        calls['primed'].append(tuple(indices))
        for index in indices:
            provider_.private_key(index, provider_.api_key_index)

    async def resolve(config, client, operator, **kwargs):
        calls['resolve'].append(kwargs.get('pool'))
        calls['read_indices'] = getattr(client, 'read_account_indices', None)
        return {}

    async def run(config, client):
        calls['configs'].append(config)
        return await run_random_cycle(config, client, clock=clock, rng=FixedRng(25, 20))

    monkeypatch.setattr('builtins.input', lambda _prompt: '')
    monkeypatch.setattr(cli_module, '_validate_simple_sdk', lambda: None)
    monkeypatch.setattr(cli_module, 'LighterSdkClient', FakeSdkClient)
    monkeypatch.setattr(cli_module, '_keychain_provider', provider)
    monkeypatch.setattr(cli_module, '_prime_keychain', prime)
    monkeypatch.setattr(recovery, 'resolve_prior', resolve)
    monkeypatch.setattr(cli_module, 'run_random_cycle', run)
    args = cli_module._parser().parse_args(['simple', '--keychain', '--no-progress', '--config', str(config_path),
                                            '--market-evidence', str(evidence_path)])
    return args, operator_dir, synthetic, calls


@pytest.mark.asyncio
@pytest.mark.parametrize('draw,route_draw', [((0, 1), 0), ((2, 0), 3), ((1, 1), 1)])
async def test_cycle_child_draws_pair_from_pool_and_binds_it_everywhere(tmp_path, monkeypatch, capsys,
                                                                         draw, route_draw):
    args, operator_dir, synthetic, calls = pool_launcher(tmp_path, monkeypatch, [11, 22, 33])
    rng = EnumeratingRng(list(draw))
    real_select = wp.select_wallet_pair

    async def select(config, pool, client, *, has_key):
        return await real_select(config, pool, client, has_key=has_key, rng=rng, clock=lambda: 7.0)

    monkeypatch.setattr(wp, 'select_wallet_pair', select)
    route_rng = FixedRng(route_draw)
    monkeypatch.setattr(cli_module, 'select_random_route',
                        lambda c: random_cycle_module.select_random_route(c, route_rng))
    assert await cli_module._run(args) == 0
    first = [11, 22, 33][draw[0]]
    second = [w for w in [11, 22, 33] if w != first][draw[1]]
    pair = {first, second}
    slot = operator_dir / 'cycle-001'
    launch = json.loads((slot / 'launch.json').read_text())
    assert launch['wallet_selection']['pair'] == [first, second]
    assert launch['wallet_selection']['eligible'] == [11, 22, 33]
    route = launch['random_route']
    assert {route['source_account_index'], route['receiver_account_index']} == pair
    [config] = calls['configs']
    assert (config.source_account_index, config.receiver_account_index) == (
        route['source_account_index'], route['receiver_account_index'])
    # Selection reader never holds a signing secret; the cycle reads all pool
    # history but only the drawn pair's keys are loaded.
    assert calls['clients'][0][2] == '_NoSigningSecrets'
    assert calls['providers'][0][0] == (11, 22, 33) and calls['providers'][0][1] is False
    assert calls['providers'][-1][0] == (11, 22, 33)
    assert calls['providers'][-1][2] is not None  # Never an interactive prompt.
    assert calls['primed'] == [(route['source_account_index'], route['receiver_account_index'])]
    assert calls['resolve'][0].all == (11, 22, 33) and calls['read_indices'] == (11, 22, 33)
    assert {plan.account_index for plan in synthetic.submissions} == pair
    assert 'Кошельки: выбрана пара' in capsys.readouterr().out


@pytest.mark.asyncio
async def test_cycle_child_with_fewer_than_two_ready_wallets_records_skip_and_sends_nothing(
        tmp_path, monkeypatch, capsys):
    states = {11: ('1000', '0', True), 22: ('0.01', '0', True), 33: ('1000', '0.3', True)}
    args, operator_dir, synthetic, calls = pool_launcher(tmp_path, monkeypatch, [11, 22, 33], states=states)
    assert await cli_module._run(args) == 2
    slot = operator_dir / 'cycle-001'
    assert read_launch_failure(slot) == 'WALLETS_UNAVAILABLE'
    launch = json.loads((slot / 'launch.json').read_text())
    assert 'random_route' not in launch and launch['wallet_selection']['pair'] is None
    assert calls['configs'] == [] and synthetic.submissions == [] and calls['primed'] == []
    output = capsys.readouterr().out
    assert 'готовых кошельков меньше двух (1 из 3)' in output
    assert '22 — мало баланса' in output and '33 — есть позиция' in output
    c = setup(operator_dir, None)
    c.store.data['last'] = {'status': 'BLOCKED', 'cycle': 'cycle-001'}
    plain = valid(c.summary())
    assert 'Готовых кошельков меньше двух' in plain and '22 — мало баланса' in plain


@pytest.mark.asyncio
async def test_cycle_child_refuses_invalid_pool_after_claiming_a_slot(tmp_path, monkeypatch, capsys):
    args, operator_dir, synthetic, calls = pool_launcher(tmp_path, monkeypatch, [11, 22, 33])
    write_pool(operator_dir, [11, 33, 44])  # Configured account 22 missing.
    assert await cli_module._run(args) == 2
    assert read_launch_failure(operator_dir / 'cycle-001') == 'WALLET_POOL'
    assert calls['clients'] == [] and calls['providers'] == []
    assert 'файл пула кошельков некорректен' in capsys.readouterr().out


@pytest.mark.asyncio
async def test_pool_requires_stored_keychain_credentials(tmp_path, monkeypatch):
    args, operator_dir, synthetic, calls = pool_launcher(tmp_path, monkeypatch, [11, 22, 33])
    args.keychain = False
    with pytest.raises(SystemExit, match='только с сохранёнными ключами'):
        await cli_module._run(args)
    assert not list(operator_dir.glob('cycle-*'))


@pytest.mark.asyncio
async def test_without_pool_file_the_launcher_is_unchanged(tmp_path, monkeypatch):
    args, operator_dir, synthetic, calls = pool_launcher(tmp_path, monkeypatch, [11, 22])
    (operator_dir / wp.POOL_FILE_NAME).unlink()
    route_rng = FixedRng(2)
    monkeypatch.setattr(cli_module, 'select_random_route',
                        lambda c: random_cycle_module.select_random_route(c, route_rng))
    assert await cli_module._run(args) == 0
    launch = json.loads((operator_dir / 'cycle-001' / 'launch.json').read_text())
    assert 'wallet_selection' not in launch
    assert launch['random_route'] == {'source_account_index': 11, 'receiver_account_index': 22,
                                      'direction': 'SHORT'}
    assert calls['providers'] == [((11, 22), False, None)]
    assert calls['resolve'] == [None] and calls['read_indices'] is None


# --------------------------------------------------------------------------
# Owner command: ./wallet

KEY = 'ab' * 40


class VerifyClient:
    """Fake read-only client: records the key it was handed, returns one snapshot."""

    def __init__(self, snapshot=None, error=None):
        self.snapshot, self.error = snapshot, error
        self.keys = []
        self.closed = False

    def __call__(self, config, *, source_account_index, receiver_account_index, secrets):
        assert source_account_index != receiver_account_index
        self.keys.append(secrets.private_key(source_account_index, config.api_key_index))
        self.secrets = secrets
        return self

    async def account_snapshot(self, index, market_id):
        if self.error:
            raise self.error
        return self.snapshot

    async def aclose(self):
        self.closed = True


def wallet_env(tmp_path, *, stored=()):
    tmp_path.chmod(0o700)
    config = cycle_config(tmp_path / 'unused', api_key_index=4)
    backend = MemoryKeychainBackend({KeychainBinding.from_config(config, index): KEY for index in stored})
    return config, backend


@pytest.mark.asyncio
async def test_add_wallet_verifies_key_in_memory_then_stores_it_and_extends_pool(tmp_path, capsys):
    config, backend = wallet_env(tmp_path)
    reader = VerifyClient(account(33, Decimal(0), available_balance=Decimal('25.5')))
    prompts = []

    def read_key(prompt):
        prompts.append(prompt)
        return f'  0x{KEY}\n'

    assert await wallet_cli.add_wallet(config, tmp_path, 33, backend=backend, client_factory=reader,
                                       read_key=read_key) == 0
    assert reader.keys == [f'0x{KEY}'] and reader.closed
    with pytest.raises(Exception):
        reader.secrets.private_key(33, 4)  # The in-memory copy is wiped.
    assert backend.values[KeychainBinding.from_config(config, 33)] == f'0x{KEY}'
    assert 'индекса ключа 4' in prompts[0] and KEY not in prompts[0]
    pool = wp.load_wallet_pool(tmp_path, config)
    assert pool.active == (11, 22, 33) and pool.from_file
    assert (tmp_path / wp.POOL_FILE_NAME).stat().st_mode & 0o777 == 0o600
    output = capsys.readouterr().out
    assert 'Кошелёк 33 добавлен' in output and '25.5' in output and KEY not in output


@pytest.mark.asyncio
@pytest.mark.parametrize('case', ['position', 'orders', 'not_ready', 'auth_error', 'bad_format', 'other_identity'])
async def test_add_wallet_refusals_store_nothing(tmp_path, case, capsys):
    config, backend = wallet_env(tmp_path)
    snapshot = account(33, Decimal(0))
    error = None
    key = KEY
    if case == 'position':
        snapshot = account(33, Decimal('-0.3'))
    elif case == 'orders':
        snapshot = account(33, Decimal(0), active_orders=(active_order(33),))
    elif case == 'not_ready':
        snapshot = replace(snapshot, ready=False)
    elif case == 'auth_error':
        error = RuntimeError('invalid auth SECRET-DETAIL')
    elif case == 'bad_format':
        key = 'not a key'
    else:
        snapshot = account(34, Decimal(0))
    reader = VerifyClient(snapshot, error)
    with pytest.raises(SystemExit) as raised:
        await wallet_cli.add_wallet(config, tmp_path, 33, backend=backend, client_factory=reader,
                                    read_key=lambda _prompt: key)
    assert 'не добавлен' in str(raised.value) and KEY not in str(raised.value)
    assert not backend.values and not (tmp_path / wp.POOL_FILE_NAME).exists()
    if case == 'bad_format':
        assert reader.keys == []
    if case == 'position':
        assert 'позиция -0.3' in str(raised.value) and 'закройте её вручную' in str(raised.value)


@pytest.mark.asyncio
async def test_add_wallet_reuses_a_stored_key_without_prompting(tmp_path):
    config, backend = wallet_env(tmp_path, stored=(33,))
    reader = VerifyClient(account(33, Decimal(0)))

    def no_prompt(_):
        pytest.fail('stored key must be verified without a prompt')

    assert await wallet_cli.add_wallet(config, tmp_path, 33, backend=backend, client_factory=reader,
                                       read_key=no_prompt) == 0
    assert reader.keys == [KEY]
    assert [call[0] for call in backend.calls].count('put') == 0
    assert wp.load_wallet_pool(tmp_path, config).active == (11, 22, 33)
    # A member with a stored key is a no-op; replacement verifies the new key first.
    assert await wallet_cli.add_wallet(config, tmp_path, 33, backend=backend,
                                       client_factory=VerifyClient(None, AssertionError('no read')),
                                       read_key=no_prompt) == 0
    new_key = 'cd' * 40
    replacer = VerifyClient(account(33, Decimal(0)))
    assert await wallet_cli.add_wallet(config, tmp_path, 33, replace_key=True, backend=backend,
                                       client_factory=replacer, read_key=lambda _: new_key) == 0
    assert replacer.keys == [new_key]
    assert backend.values[KeychainBinding.from_config(config, 33)] == new_key
    assert wp.load_wallet_pool(tmp_path, config).active == (11, 22, 33)


def test_wallet_list_pause_resume_and_arguments(tmp_path, capsys, monkeypatch):
    config, backend = wallet_env(tmp_path, stored=(11, 22))
    wallet_cli.list_wallets(config, tmp_path, backend=backend)
    assert 'Пул не создан' in capsys.readouterr().out
    write_pool(tmp_path, [11, 22, 33])
    monkeypatch.setattr(wallet_cli, '_local_inputs', lambda _config: (config, tmp_path))
    assert wallet_cli.main(['pause', '33']) == 0
    assert wp.load_wallet_pool(tmp_path, config).paused == (33,)
    wallet_cli.list_wallets(config, tmp_path, backend=backend)
    out = capsys.readouterr().out
    assert '33: на паузе, НЕТ КЛЮЧА' in out and '11: активен, ключ есть' in out
    with pytest.raises(SystemExit, match='хотя бы два активных'):
        wallet_cli.main(['pause', '22'])
    assert wallet_cli.main(['resume', '33']) == 0
    assert wp.load_wallet_pool(tmp_path, config).active == (11, 22, 33)
    for argv, message in ((['pause'], 'Укажите номер'), (['pause', '0'], 'положительным'),
                          (['pause', '-5'], 'положительным'), (['resume', '33', '--replace-key'], 'только с add'),
                          (['list', '33'], 'не принимает')):
        with pytest.raises(SystemExit, match=message):
            wallet_cli.main(argv)


# --------------------------------------------------------------------------
# Telegram views and controller use of pool proofs


def pool_proof(positions, paused=()):
    rows = [_account_payload(account(index, Decimal(value))) for index, value in positions.items()]
    active = [index for index in positions if index not in paused]
    return {'status': 'CLOSE_READY', 'at': 1000.0, 'source': rows[0], 'receiver': rows[1], 'accounts': rows,
            'wallets': {'active': active, 'paused': list(paused), 'from_file': True}, 'previous_intents': 0}


def test_controller_positions_require_every_pool_wallet_exactly_once():
    proof = pool_proof({11: '0', 22: '0', 33: '0.2', 44: '0'}, paused=(44,))
    assert bot.Controller._exact_recovery_positions(proof) == [0, 0, Decimal('0.2'), 0]
    assert positions_text(proof) == 'счёт 33: LONG 0.2 BTC; остальные 3 — 0'
    assert positions_text(pool_proof({11: '0', 22: '0', 33: '0'})) == 'все 3 кошельков: 0'
    missing = dict(proof, accounts=proof['accounts'][:3])
    with pytest.raises(RuntimeError, match='does not cover'):
        bot.Controller._exact_recovery_positions(missing)
    duplicated = dict(proof, accounts=proof['accounts'] + [proof['accounts'][0]])
    with pytest.raises(RuntimeError, match='conflict'):
        bot.Controller._exact_recovery_positions(duplicated)
    with_orders = pool_proof({11: '0', 22: '0', 33: '0'})
    with_orders['accounts'][2] = dict(with_orders['accounts'][2], active_orders=[{'order_id': 'x'}])
    with pytest.raises(RuntimeError, match='incomplete'):
        bot.Controller._exact_recovery_positions(with_orders)
    record = bot.recovery_record(dict(proof, status='READY'))
    assert record == {'status': 'READY', 'at': 1000.0, 'previous_intents': 0, 'wallet_count': 4}
    assert 'Все 4 кошельков пула были без позиций' in valid(views.recovery_checkpoint(record))
    assert 'Оба счёта были' in valid(views.recovery_checkpoint({'status': 'READY', 'at': 1000.0}))


def test_pool_accounts_view_lists_problems_first_within_bounds():
    rows = []
    for index in range(100, 160):
        rows.append({'index': index, 'snapshot': account(index, Decimal(0), available_balance=Decimal('5')),
                     'stale': False})
    rows[30]['snapshot'] = account(130, Decimal('-0.2'))
    rows[45]['snapshot'] = None
    result = {'symbol': 'BTC', 'accounts': rows, 'wallets': {'active': list(range(100, 159)), 'paused': [159]}}
    plain = valid(views.accounts_message(result))
    lines = plain.splitlines()
    assert lines[0] == '💼 Кошельки пула: 60 · рынок BTC'
    assert 'активных 59, на паузе 1 (⏸)' in lines[1]
    assert lines[3].startswith('• 130 · 1000 · SHORT 0.2') and lines[4] == '• 145 · ❔ нет данных'
    assert '… и ещё 20 кошельков' in plain
    legacy = {'symbol': 'BTC', 'accounts': rows[:2]}
    assert 'Счёт A' in valid(views.accounts_message(legacy))


def test_admission_refusals_name_wallet_problems():
    assert bot.admission_failure_category(recovery.WalletKeyMissing('wallet 33')) == 'WALLET_KEY_MISSING'
    assert bot.admission_failure_category(wp.WalletPoolError('bad')) == 'WALLET_POOL'
    assert bot.admission_failure_category(PreflightBlocked('other')) == 'PREFLIGHT_REFUSED'
    for category, text in (('WALLET_KEY_MISSING', './wallet add НОМЕР'), ('WALLET_POOL', './wallet list')):
        plain = valid(views.admission_refusal_message({'status': 'REFUSED', 'update_id': 3, 'stage': 'READY',
                                                       'category': category, 'at': 1.0}))
        assert text in plain and category in plain


def cycle_report(source, receiver):
    return {'status': 'COMPLETE', 'cycle': {'terminal_at': 100.0},
            'binding': {'market_id': 1, 'market_symbol': 'BTC', 'source_account_index': source,
                        'receiver_account_index': receiver},
            'inventory': {'status': 'OPEN_INVENTORY', 'source': '0.00040', 'receiver': '0'},
            'paired_execution': {'status': 'FAILED'}}


def pool_close_slot(root, name, started, wallets, positions, *, status='CONFIRMED_FLAT'):
    slot = root / name
    slot.mkdir()
    binding = {'market_id': 1, 'market_symbol': 'BTC', 'source_account_index': 11, 'receiver_account_index': 22}
    rows = [{'sequence': 1, 'run_id': name, 'event': 'CLOSE_STARTED', 'at': started,
             'payload': {'binding': binding, 'wallets': wallets}},
            {'sequence': 2, 'run_id': name, 'event': 'CLOSE_COMPLETE', 'at': started + 2,
             'payload': {'status': status, 'symbol': 'BTC',
                         'positions': [{'account_index': index, 'position': value}
                                       for index, value in positions.items()]}}]
    (slot / 'close.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in rows))


@pytest.mark.parametrize('case', ['flat', 'pair_outside_pool', 'missing_row', 'nonzero', 'partial'])
def test_later_pool_close_is_matched_to_a_drawn_pair(tmp_path, monkeypatch, case):
    c = setup(tmp_path, None)
    (tmp_path / 'cycle-111').mkdir()
    monkeypatch.setattr(c, 'report', lambda name: cycle_report(33, 44))
    wallets = {'active': [11, 22, 33, 44], 'paused': [55]}
    positions = {11: '0', 22: '0', 33: '0', 44: '0', 55: '0'}
    status = 'CONFIRMED_FLAT'
    if case == 'pair_outside_pool':
        wallets = {'active': [11, 22, 33], 'paused': []}
        positions = {11: '0', 22: '0', 33: '0'}
    elif case == 'missing_row':
        positions.pop(55)
    elif case == 'nonzero':
        positions[55] = '0.1'
    elif case == 'partial':
        status = 'PARTIAL'
    pool_close_slot(tmp_path, 'close-006', 110, wallets, positions, status=status)
    plain = valid(c.summary())
    if case == 'pair_outside_pool':
        assert 'Отдельное закрытие' not in plain
    elif case == 'flat':
        assert 'Отдельное закрытие после цикла · close-006' in plain
        assert '✅ нулевые позиции подтверждены' in plain
    else:
        assert '❔ итог закрытия не подтверждён' in plain
