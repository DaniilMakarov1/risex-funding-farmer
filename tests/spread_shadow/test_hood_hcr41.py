"""Adverse offline regressions for the HCR-41 corrections."""
import asyncio
from dataclasses import replace
from decimal import Decimal
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

from risex_spread_shadow.hood_handoff import cli
from risex_spread_shadow.hood_handoff.contracts import (
    MutationReceipt, OrderSnapshot, PreflightBlocked, TradeReceipt,
)
from risex_spread_shadow.hood_handoff.journal import DurableJournal
from risex_spread_shadow.hood_handoff.offline_report import load_saved_cycle_report
from risex_spread_shadow.hood_handoff.operator_view import read_launch_failure, result_lines
from risex_spread_shadow.hood_handoff.random_cycle import RandomCycleEngine, run_random_cycle
from risex_spread_shadow.hood_handoff.telegram_messages import saved_message
from test_hood_handoff_random_cycle import AdvancingClock, CycleClient, FixedRng, cycle_config
from test_hood_handoff_sdk_interface import FakeHttp, FakeSigner, _constant_nonce_client
from test_hood_operator_recovery import RecoveryClient


class WallClock:
    now = staticmethod(time.time)
    monotonic = staticmethod(time.monotonic)
    sleep = staticmethod(asyncio.sleep)


@pytest.mark.asyncio
async def test_margin_cancel_is_terminal_zero_fill_in_rebuilt_report(tmp_path):
    clock = AdvancingClock()

    class MarginCancelClient(CycleClient):
        async def submit_order(self, plan):
            if plan.order_type == 'MARKET' and not plan.reduce_only:
                self.submissions.append(plan)
                order = OrderSnapshot(
                    plan.account_index, plan.market_id, f'order-{len(self.orders) + 1}',
                    plan.client_order_index, 'canceled-margin-not-allowed',
                    plan.side, plan.order_type, plan.time_in_force, plan.reduce_only,
                    plan.quantity, Decimal(0), Decimal(0), plan.price, self.clock.now(),
                )
                self._save_order(order)
                return MutationReceipt(True, order.order_id, f'tx-{order.order_id}')
            return await super().submit_order(plan)

        async def cancel_order(self, account_index, market_id, order_id):
            current = self.orders[(account_index, str(order_id))]
            if current.active and not self.trades.get(current.order_id):
                self._replace_order(current, status='filled',
                                    filled_quantity=current.initial_quantity,
                                    remaining_quantity=Decimal(0))
                self.source_position += (current.initial_quantity if current.side == 'BUY'
                                         else -current.initial_quantity)
                self.trades[current.order_id] = (TradeReceipt(
                    'external-source-fill', current.account_index, market_id,
                    current.order_id, current.side, current.initial_quantity,
                    current.price, None, 23942, self.clock.now(),
                    counterparty_order_id='external-market',
                    client_order_index=current.client_order_index),)
                return MutationReceipt(True, current.order_id, 'external-fill-before-cancel')
            return await super().cancel_order(account_index, market_id, order_id)

    client = MarginCancelClient(clock)
    await run_random_cycle(cycle_config(tmp_path / 'cycle-001'), client,
                           clock=clock, rng=FixedRng(20, 20))
    report = load_saved_cycle_report(tmp_path / 'cycle-001')
    assert report['status'] == 'COMPLETE', report['issues']
    assert report['inventory']['status'] == 'CONFIRMED_FLAT'
    assert report['paired_execution']['status'] == 'FAILED'
    assert not report['order_state']['unresolved_intents']
    opening = next(v for v in report['paired_execution']['direct_counterparty_match']
                   if v['phase'] == 'opening')
    assert opening['external_source_accounts'] == [23942]
    assert any('canceled-margin-not-allowed' in line for line in result_lines(report))
    terminal = subprocess.run(
        [sys.executable, '-m', 'risex_spread_shadow.hood_handoff.cli',
         'report', '--path', str(tmp_path / 'cycle-001')],
        cwd=Path(__file__).resolve().parents[2],
        env={**os.environ, 'PYTHONPATH': 'src'},
        capture_output=True, text=True, check=True,
    ).stdout
    telegram = saved_message('cycle-001', report)
    for rendered in (terminal, telegram):
        assert '23942' in rendered
        assert 'canceled-margin-not-allowed' in rendered
        assert 'не состоялось' in rendered or 'FAILED' in rendered
        assert 'закрытие подтверждено' in rendered
        assert 'неизвестн' in rendered  # fee/net/funding uncertainty survives


@pytest.mark.asyncio
async def test_fallback_quote_deadline_reaches_sdk_send_boundary(tmp_path):
    """A slow account read plus nonce cannot reset the original book clock."""
    seen = {}

    class SlowNonce:
        async def async_next_nonce(self, key):
            await asyncio.sleep(0.13)
            return key, 41

    class CaptureHttp(FakeHttp):
        async def post_form(self, path, *, form):
            seen['wire_at'] = time.time()
            return await super().post_form(path, form=form)

    signer = FakeSigner()
    signer.nonce_manager = SlowNonce()
    adapter = _constant_nonce_client(prefix='offline-hcr41-close', signer=signer,
                                     http_factory=CaptureHttp)
    adapter.config = replace(adapter.config, freshness_seconds=0.2)
    adapter._lighter = lambda: SimpleNamespace()

    class Bridge(RecoveryClient):
        async def order_book(self, market_id):
            value = await super().order_book(market_id)
            seen['book_at'] = value.observed_at
            return value

        async def account_snapshot(self, account_index, market_id):
            if 'book_at' in seen and 'wire_at' not in seen:
                await asyncio.sleep(0.13)
            return await super().account_snapshot(account_index, market_id)

        async def submit_order(self, plan):
            seen['deadline'] = plan.mutation_deadline_monotonic
            seen['receipt'] = await adapter.submit_order(plan)
            return seen['receipt']

    clock = WallClock()
    client = Bridge(clock, source='0.20')
    config = cycle_config(tmp_path / 'synthetic-close', freshness_seconds=0.2)
    before = await client.account_snapshot(11, 7)
    journal = DurableJournal(tmp_path / 'synthetic-close.jsonl', clock=clock.now)
    journal.acquire_attempt()
    try:
        await RandomCycleEngine(client, clock=clock)._fallback_one(
            config, journal, before, Decimal('0.20'))
    finally:
        journal.release_attempt()
        await adapter.aclose()
    assert seen['deadline'] is not None
    assert seen['receipt'].accepted is False
    assert 'wire_at' not in seen
    assert any(row.event == 'FALLBACK_SEND_BARRIER' for row in journal.events)


@pytest.mark.asyncio
async def test_prepared_fallback_reserves_before_quote_and_records_phase_timings(tmp_path):
    steps = []

    class PreparedClient(RecoveryClient):
        async def reserve_order_nonce(self, account_index, *, deadline):
            steps.append('nonce')
            return SimpleNamespace(deadline=deadline)

        async def invalidate_reserved_nonce(self, token):
            steps.append('invalidate')

        async def order_book(self, market_id):
            steps.append('book')
            return await super().order_book(market_id)

        async def prepare_order(self, plan, *, reserved_nonce):
            steps.append('prepare')
            assert plan.mutation_deadline_monotonic <= reserved_nonce.deadline
            return SimpleNamespace(diagnostic_timings={
                'nonce_acquisition_seconds': 0.01,
                'signing_call_seconds': 0.002,
                'nonce_reserved_before_quote': 1.0,
            })

        async def invalidate_prepared_order(self, prepared):
            steps.append('invalidate_prepared')

        async def submit_prepared_order(self, plan, prepared, *, deadline):
            steps.append('send')
            assert deadline == plan.mutation_deadline_monotonic
            return await super().submit_order(plan)

    clock = WallClock()
    client = PreparedClient(clock, source='0.20')
    config = cycle_config(tmp_path / 'prepared-close')
    journal = DurableJournal(tmp_path / 'prepared-close.jsonl', clock=clock.now)
    journal.acquire_attempt()
    try:
        source = await client.account_snapshot(11, 7)
        receiver = await client.account_snapshot(22, 7)
        result = await RandomCycleEngine(client, clock=clock).close_reconciled_positions(
            config, journal, source, receiver)
    finally:
        journal.release_attempt()
    assert steps.index('nonce') < steps.index('book') < steps.index('prepare') < steps.index('send')
    assert result[1:] == (Decimal('0'), Decimal('0'))
    timings = [row.payload['timings'] for row in journal.events
               if row.event == 'FALLBACK_PREPARATION_TIMINGS']
    assert timings and timings[0]['nonce_reserved_before_quote'] == 1.0
    assert all(timings[0][key] >= 0 for key in (
        'metadata_read_seconds', 'book_read_seconds',
        'account_recheck_seconds', 'nonce_acquisition_seconds',
        'signing_call_seconds'))


@pytest.mark.asyncio
async def test_prepared_fallback_expired_nonce_cannot_send(tmp_path):
    calls = []

    class ExpiredNonceClient(RecoveryClient):
        async def reserve_order_nonce(self, account_index, *, deadline):
            calls.append('reserve')
            return SimpleNamespace(deadline=time.monotonic() + 1.0)

        async def invalidate_reserved_nonce(self, token):
            calls.append('invalidate')

        async def prepare_order(self, plan, *, reserved_nonce):
            calls.append('prepare')
            raise TimeoutError('nonce expired before signing')

        async def submit_prepared_order(self, plan, prepared, *, deadline):
            calls.append('send')
            raise AssertionError('expired evidence was sent')

        async def invalidate_prepared_order(self, prepared):
            calls.append('invalidate_prepared')

    clock = WallClock()
    client = ExpiredNonceClient(clock, source='0.20')
    config = cycle_config(tmp_path / 'expired-nonce')
    journal = DurableJournal(tmp_path / 'expired-nonce.jsonl', clock=clock.now)
    journal.acquire_attempt()
    try:
        source = await client.account_snapshot(11, 7)
        receiver = await client.account_snapshot(22, 7)
        results, _, _ = await RandomCycleEngine(client, clock=clock).close_reconciled_positions(
            config, journal, source, receiver)
    finally:
        journal.release_attempt()
    assert calls[:2] == ['reserve', 'prepare']
    assert 'send' not in calls
    assert results[0].attempted is False
    assert not any(row.event == 'FALLBACK_DISPATCH_INTENT' for row in journal.events)


@pytest.mark.asyncio
@pytest.mark.parametrize('failure,account', [
    ('nonce', 11), ('nonce', 22), ('signing', 11), ('signing', 22),
    ('unknown_send', 11), ('normal', 0), ('partial', 0), ('zero', 0),
])
async def test_real_adapter_prepared_close_keeps_independent_account_and_timings(
    tmp_path, failure, account,
):
    """Real SDK preparation/send with fake signer, HTTP and account observations."""
    wire = []

    class Nonce:
        def __init__(self, index):
            self.index = index
            self.value = 40

        async def async_next_nonce(self, key):
            if failure == 'nonce' and self.index == account:
                raise TimeoutError('synthetic pre-send nonce read failure')
            self.value += 1
            return key, self.value

    class Signer(FakeSigner):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.index = kwargs['account_index']
            self.nonce_manager = Nonce(self.index)

        async def sign_create_order(self, **kwargs):
            if failure == 'signing' and self.index == account:
                raise TimeoutError('synthetic pre-send signing failure')
            return await super().sign_create_order(**kwargs)

    class Http(FakeHttp):
        async def post_form(self, path, *, form):
            wire.append(path)
            await asyncio.sleep(0.002)
            if failure == 'unknown_send':
                raise TimeoutError('synthetic lost response after possible send')
            return await super().post_form(path, form=form)

    adapter = _constant_nonce_client(prefix=f'hcr41-v3-{failure}-{account}', http_factory=Http)
    adapter._signer_factory = lambda **kwargs: Signer(**kwargs)
    adapter._lighter = lambda: SimpleNamespace()
    adapter.config = replace(adapter.config, freshness_seconds=2, request_timeout_seconds=2)

    class Bridge(RecoveryClient):
        async def reserve_order_nonce(self, index, *, deadline):
            return await adapter.reserve_order_nonce(index, deadline=deadline)

        async def invalidate_reserved_nonce(self, token):
            await adapter.invalidate_reserved_nonce(token)

        async def prepare_order(self, plan, *, reserved_nonce):
            return await adapter.prepare_order(plan, reserved_nonce=reserved_nonce)

        async def invalidate_prepared_order(self, prepared):
            await adapter.invalidate_prepared_order(prepared)

        async def submit_prepared_order(self, plan, prepared, *, deadline):
            receipt = await adapter.submit_prepared_order(plan, prepared, deadline=deadline)
            if receipt.accepted:
                return await super().submit_order(plan)
            return receipt

    fractions = ([Decimal('0.5'), Decimal('1')] if failure == 'partial'
                 else [Decimal('0'), Decimal('1')] if failure == 'zero' else None)
    client = Bridge(WallClock(), source='.20', receiver='-.20', fractions=fractions)
    config = cycle_config(tmp_path / 'cycle-001', freshness_seconds=2,
                          request_timeout_seconds=2)
    journal = DurableJournal(tmp_path / 'cycle-001' / 'cycle.jsonl', clock=time.time)
    journal.acquire_attempt()
    try:
        source = await client.account_snapshot(11, 7)
        receiver = await client.account_snapshot(22, 7)
        results, source_after, receiver_after = await RandomCycleEngine(
            client, clock=WallClock()).close_reconciled_positions(config, journal, source, receiver)
    finally:
        journal.release_attempt()
        await adapter.aclose()
    assert not adapter._nonce_reservations
    if failure in {'nonce', 'signing'}:
        assert (source_after, receiver_after) == ((Decimal('.20'), Decimal('0')) if account == 11
                                                 else (Decimal('0'), Decimal('-.20')))
        assert [plan.account_index for plan in client.submissions] == [22 if account == 11 else 11]
        assert any(r.account_index == account and not r.attempted for r in results)
    elif failure == 'unknown_send':
        assert len(wire) == 1
        assert not client.submissions
        assert results[0].outcome.value == 'UNKNOWN'
        assert (source_after, receiver_after) == (Decimal('.20'), Decimal('-.20'))
    else:
        assert (source_after, receiver_after) == (Decimal('0'), Decimal('0'))
        assert len(wire) == 2 if failure == 'normal' else len(wire) >= 3
    saved = [row.payload for row in journal.events
             if row.event in {'FALLBACK_DISPATCH_RESULT', 'FALLBACK_DISPATCH_UNKNOWN'}]
    for payload in saved:
        assert payload['preparation_timings']['transport_roundtrip_seconds'] >= 0.002
    if saved:
        report = load_saved_cycle_report(tmp_path / 'cycle-001')
        measured = report['latency']['fallback']
        assert any(item['measurements']['transport_roundtrip']['status'] == 'AVAILABLE'
                   for item in measured)
        if failure == 'unknown_send':
            assert measured[0]['dispatch_status'] == 'UNKNOWN'
            assert measured[0]['measurements']['terminal_order_observation']['status'] == 'UNKNOWN'
        else:
            assert any(item['measurements']['terminal_order_observation']['status'] == 'AVAILABLE'
                       for item in measured if item['dispatch_status'] == 'RESULT')


def test_launch_failure_requires_fixed_code_and_no_cycle_journal(tmp_path):
    slot = tmp_path / 'cycle-001'
    slot.mkdir()
    path = slot / 'launch-failure.json'
    payload = {'schema': 'hcr-41-launch-failure-v1',
               'code': 'PRIOR_LEVERAGE_UNRESOLVED', 'cycle_dir': str(slot),
               'inventory': 'UNKNOWN', 'execution': 'UNKNOWN',
               'cycle_journal_present': False}
    path.write_text(json.dumps(payload))
    assert read_launch_failure(slot) == 'PRIOR_LEVERAGE_UNRESOLVED'
    payload['code'] = 'arbitrary_detail'
    path.write_text(json.dumps(payload))
    assert read_launch_failure(slot) is None
    payload['code'] = ['untrusted']
    path.write_text(json.dumps(payload))
    assert read_launch_failure(slot) is None
    payload['code'] = 'PRIOR_LEVERAGE_UNRESOLVED'
    path.write_text(json.dumps(payload))
    (slot / 'cycle.jsonl').write_text('incomplete')
    assert read_launch_failure(slot) is None


def test_prejournal_refusal_persists_only_a_safe_fixed_reason(tmp_path):
    slot = tmp_path / 'cycle-002'
    slot.mkdir()
    code = cli._persist_prejournal_launch_failure(
        slot, PreflightBlocked('previous leverage setting is unresolved; private key=secret'))
    assert code == 'PRIOR_LEVERAGE_UNRESOLVED'
    assert read_launch_failure(slot) == code
    assert 'secret' not in (slot / 'launch-failure.json').read_text()


@pytest.mark.parametrize('format_name', ['human', 'both'])
@pytest.mark.parametrize('relative', [False, True])
@pytest.mark.parametrize('batch', [False, True])
def test_real_report_cli_keeps_prejournal_reason_across_paths_and_formats(
    tmp_path, format_name, relative, batch,
):
    root = Path(__file__).resolve().parents[2]
    slot = tmp_path / 'cycle-001'
    slot.mkdir()
    cli._persist_prejournal_launch_failure(
        slot, PreflightBlocked('previous leverage setting is unresolved'))
    selected = os.path.relpath(slot, root) if relative else str(slot)
    args = [sys.executable, '-m', 'risex_spread_shadow.hood_handoff.cli',
            'report', '--path', selected, '--format', format_name]
    if batch:
        other = tmp_path / 'cycle-002'
        other.mkdir()
        args += ['--path', str(other)]
    result = subprocess.run(args, cwd=root, env={**os.environ, 'PYTHONPATH': 'src'},
                            capture_output=True, text=True, check=True)
    assert 'PRIOR_LEVERAGE_UNRESOLVED' in result.stdout
    assert 'исполнение и позиции UNKNOWN' in result.stdout
    assert result.stderr == ''


def test_launch_failure_rejects_mismatch_oversize_and_symlink(tmp_path):
    slot = tmp_path / 'cycle-001'
    slot.mkdir()
    cli._persist_prejournal_launch_failure(slot, PreflightBlocked('other readiness refusal'))
    path = slot / 'launch-failure.json'
    payload = json.loads(path.read_text())
    payload['cycle_dir'] = str(tmp_path / 'cycle-002')
    path.write_text(json.dumps(payload))
    assert read_launch_failure(slot) is None
    path.write_text(' ' * 4097)
    assert read_launch_failure(slot) is None
    path.unlink()
    path.symlink_to(tmp_path / 'missing')
    assert read_launch_failure(slot) is None
