"""Offline regressions for the failures in owner-run cycles 112–126."""
import asyncio
import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from risex_spread_shadow.hood_handoff import (
    HandoffEngine, HistoryPage, Outcome, Direction, OrderPlan, load_saved_cycle_report,
)
from risex_spread_shadow.hood_handoff.journal import DurableJournal
from risex_spread_shadow.hood_handoff.operator_view import result_lines
from test_hood_ticks_ack import AckClient
from test_hood_cycle075_regressions import snapshot
from test_hood_handoff_sdk_interface import _constant_nonce_client, ConstantNonceSigner, FakeSigner
from test_hood_handoff_random_cycle import (
    AdvancingClock, FixedRng, cycle_config, run_random_cycle, ExternalOpeningClient,
)


def rows(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


@pytest.mark.asyncio
@pytest.mark.parametrize('sign_failure', [False, True])
async def test_ack_late_cancel_consumes_sdk_reservation_once(tmp_path, sign_failure):
    signer = ConstantNonceSigner(account_index=11)
    client = _constant_nonce_client(prefix='ack-late-cancel', signer=signer)
    client._signer(11)  # SDK import belongs to preflight, before the cancellation deadline.
    value = replace(snapshot(), market_id=7)
    plan = OrderPlan(account_index=11, market_id=7, side='BUY', quantity=Decimal('.20'),
                     price=Decimal('100'), order_type='LIMIT', time_in_force='POST_ONLY',
                     reduce_only=False, client_order_index=77, quantity_int=20, price_int=1000,
                     order_expiry_ms=9999999999999)
    engine = HandoffEngine(client)
    engine._configured_freshness = 10
    engine._configured_request_timeout = 1
    if sign_failure:
        original = signer.sign_cancel_order
        calls = 0
        async def fail_once(**kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise ValueError('synthetic unsent signing failure')
            return await original(**kwargs)
        signer.sign_cancel_order = fail_once
    journal = DurableJournal(tmp_path/'cancel.jsonl')
    errors = []
    try:
        engine._start_cancel_nonce(plan)
        token = await engine._cancel_nonce_task
        assert engine._cancel_preparation_task is None
        await engine._cancel_if_safe(plan, value, journal, journal.run_id, errors,
                                     expected_order_id='123', exact_order_just_read=True)
        assert not errors
        assert len(client._http.calls) == 1
        event = next(e for e in journal.events if e.event == 'CANCEL_DISPATCH_RESULT')
        assert event.payload['accepted'] is True
        assert token._state in {'CONSUMED', 'INVALIDATED'}
        await engine._cancel_if_safe(plan, value, journal, journal.run_id, errors,
                                     expected_order_id='123', exact_order_just_read=True)
        assert len(client._http.calls) == 1  # durable intent forbids a duplicate send
    finally:
        await engine._finish_cancel_preparation()
        await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize('closing', [False, True])
async def test_ack_book_veto_cancels_with_real_sdk_nonce_contract(tmp_path, closing):
    adapter = _constant_nonce_client(prefix='ack-veto-sdk', signer=FakeSigner(account_index=11))
    class Client(AckClient):
        async def submit_order(self, plan):
            receipt = await super().submit_order(plan)
            if plan.order_type == 'LIMIT':
                old_id = self.latest_order[plan.account_index]
                old = self.orders.pop((plan.account_index, old_id))
                self._save_order(replace(old, order_id=str(1000 + len(self.orders))))
            return receipt
        async def reserve_cancel_nonce(self, account):
            return await adapter.reserve_cancel_nonce(account)
        async def prepare_cancel_order(self, *args, **kwargs):
            return await adapter.prepare_cancel_order(*args, **kwargs)
        async def submit_prepared_cancel_order(self, a, m, o, prepared, **kwargs):
            receipt = await adapter.submit_prepared_cancel_order(a, m, o, prepared, **kwargs)
            if receipt.accepted:
                await super().cancel_order(a, m, o)
            return receipt
        async def invalidate_reserved_nonce(self, token):
            await adapter.invalidate_reserved_nonce(token)
        async def invalidate_prepared_order(self, token):
            if isinstance(token, dict):
                await super().invalidate_prepared_order(token)
            else:
                await adapter.invalidate_prepared_order(token)
        async def cancel_order(self, *args):
            raise AssertionError('reserved cancellation must use prepared SDK path')
    clock = AdvancingClock(); client = Client(clock, 'better', closing)
    try:
        result = await run_random_cycle(cycle_config(tmp_path/'cycle', receiver_admission='ack'),
                                        client, clock=clock, rng=FixedRng(20,20))
        assert result.inventory == 'CONFIRMED_FLAT', result.reason
        assert client.cancellations
        assert len(adapter._http.calls) == len(client.cancellations)
        assert not any(o.active for o in client.orders.values())
    finally:
        await adapter.aclose()


class MissingSourceAfterMarket(AckClient):
    def __init__(self, clock, failure=None):
        super().__init__(clock)
        self.source_reads = 0
        self.market_sent = False
        self.failure = failure
    def ws_admission_view(self, anchor, plan, order_id):
        _, book = super().ws_admission_view(anchor, plan, order_id)
        order = next(v for v in self.orders.values() if v.account_index == plan.account_index
                     and v.client_order_index == plan.client_order_index)
        return order, book
    async def submit_order(self, plan):
        result = await super().submit_order(plan)
        if plan.order_type == 'MARKET' and not plan.reduce_only:
            self.market_sent = True
            for oid, trades in self.trades.items():
                self.trades[oid] = tuple(replace(t, counterparty_account_index=999,
                                               counterparty_order_id='foreign') for t in trades)
        return result
    async def lookup_order(self, a, m, **kwargs):
        value = await super().lookup_order(a, m, **kwargs)
        if self.market_sent and a == 11 and value is not None and not value.reduce_only:
            self.source_reads += 1
            # The source is already identified by the stream; its cancellation
            # read disappears, then only later terminal proof may resolve it.
            if self.source_reads == 1 or self.failure == 'missing' and self.source_reads > 1:
                return None
        return value
    async def list_trades(self, a, m, **kwargs):
        page = await super().list_trades(a, m, **kwargs)
        if self.failure == 'history' and a == 11 and self.market_sent:
            return HistoryPage(trades=(), complete=False)
        return page


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', [None, 'missing', 'history'])
async def test_missing_source_transition_resolves_only_with_terminal_proof(tmp_path, failure):
    clock = AdvancingClock(); client = MissingSourceAfterMarket(clock, failure)
    result = await run_random_cycle(cycle_config(tmp_path/'cycle', receiver_admission='ack'),
                                    client, clock=clock, rng=FixedRng(20,20))
    events = rows(tmp_path/'cycle/opening.jsonl')
    if failure is None:
        assert any(e['event'] == 'SOURCE_OBSERVATION_RESOLVED' for e in events)
        assert result.opening.outcome is Outcome.PARTIAL
        assert result.inventory == 'CONFIRMED_FLAT', result.reason
        assert len(result.fallbacks) == 2
    else:
        assert not any(e['event'] == 'SOURCE_OBSERVATION_RESOLVED' for e in events)
        assert result.outcome is Outcome.UNKNOWN
        assert not result.fallbacks


class LaggingFallback(ExternalOpeningClient):
    def __init__(self, clock, mode):
        super().__init__(clock, direction=Direction.LONG, fill_fraction=Decimal('1'))
        self.mode = mode
        self.history_reads = 0
        self.position_reads = 0
    async def list_trades(self, a, m, **kwargs):
        page = await super().list_trades(a, m, **kwargs)
        if self.fallback_plans and str(kwargs.get('order_id')) == self.latest_order.get(a):
            self.history_reads += 1
            if self.mode == 'never' or self.mode == 'history' and self.history_reads == 1:
                return HistoryPage(trades=())
        return page
    async def account_snapshot(self, a, m):
        value = await super().account_snapshot(a, m)
        if self.fallback_plans and a == 11 and self.source_position == 0:
            self.position_reads += 1
            if self.mode == 'position' and self.position_reads == 1:
                return replace(value, signed_position=Decimal('-.20'))
            if self.mode == 'identity' and self.position_reads == 1:
                return replace(value, signed_position=Decimal('-.20'))
            if self.mode == 'identity' and self.position_reads == 2:
                return replace(value, source_identity='foreign')
        return value


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['history', 'position', 'never', 'identity'])
async def test_fallback_propagation_reads_never_resubmit(tmp_path, mode):
    clock = AdvancingClock(); client = LaggingFallback(clock, mode)
    result = await run_random_cycle(cycle_config(tmp_path/'cycle'), client,
                                    clock=clock, rng=FixedRng(20,20))
    assert len(client.fallback_plans) == 1
    assert client.fallback_plans[0].reduce_only
    if mode in {'history', 'position'}:
        assert result.inventory == 'CONFIRMED_FLAT', result.reason
        assert result.fallbacks[0].filled_quantity == Decimal('.20')
        assert result.fallbacks[0].position_after == 0
    else:
        assert result.outcome is Outcome.UNKNOWN
        assert result.inventory != 'CONFIRMED_FLAT'
    if mode == 'never':
        assert client.history_reads == 2


@pytest.mark.asyncio
async def test_preflight_refusal_has_terminal_report_without_invented_flatness(tmp_path):
    clock = AdvancingClock(); client = AckClient(clock)
    await run_random_cycle(cycle_config(tmp_path/'cycle', receiver_admission='ack',
                                       price_improvement_ticks=5), client, clock=clock, rng=FixedRng(20,20))
    report = load_saved_cycle_report(tmp_path/'cycle')
    assert report['cycle']['terminal'] is True
    assert report['cycle']['outcome'] == 'FAILED_PREFLIGHT_BLOCKED'
    assert not client.submissions
    assert report['inventory']['status'] != 'CONFIRMED_FLAT'
    assert 'PRICE_OFFSET_NO_ROOM' in report['cycle']['reason']
    assert not any(i['code'] == 'CYCLE_TERMINAL_MISSING' for i in report['issues'])


@pytest.mark.asyncio
@pytest.mark.parametrize('corrupt', [False, True])
async def test_report_keeps_proven_external_fills_when_overall_unknown(tmp_path, corrupt):
    clock = AdvancingClock(); client = MissingSourceAfterMarket(clock)
    await run_random_cycle(cycle_config(tmp_path/'cycle', receiver_admission='ack'),
                           client, clock=clock, rng=FixedRng(20,20))
    # Mutate only this synthetic test journal to model the old sticky UNKNOWN.
    path = tmp_path/'cycle/opening.jsonl'; events = rows(path)
    for e in events:
        if e['event'] == 'COMPLETE':
            e['payload']['outcome'] = 'UNKNOWN'
            e['payload']['receipt']['outcome'] = 'UNKNOWN'
            if corrupt:
                for leg in ('source', 'receiver'):
                    e['payload']['receipt'][leg]['trades'][0]['account_index'] = 888
    path.write_text(''.join(json.dumps(e)+'\n' for e in events))
    report = load_saved_cycle_report(tmp_path/'cycle')
    phase = report['paired_execution']['direct_counterparty_match'][0]
    text = '\n'.join(result_lines(report))
    assert report['paired_execution']['status'] != 'SUCCESS'
    if corrupt:
        assert phase['external_source_quantity'] == '0'
        assert 'исполнили внешние участники' not in text
    else:
        assert phase['external_source_quantity'] == '0.20'
        assert phase['status'] == 'NOT_MATCHED'
        assert 'исполнили внешние участники' in text
        assert '999' in text


@pytest.mark.asyncio
async def test_revalidation_refusal_is_also_terminal(tmp_path):
    class Client(AckClient):
        reads = 0
        async def order_book(self, market):
            value = await super().order_book(market)
            self.reads += 1
            if self.reads > 1:
                value = replace(value, asks=(replace(value.asks[0], price=Decimal('100.1')),))
            return value
    clock = AdvancingClock(); client = Client(clock)
    result = await run_random_cycle(cycle_config(tmp_path/'cycle', receiver_admission='ack',
                                                price_improvement_ticks=1), client,
                                    clock=clock, rng=FixedRng(20,20))
    assert not client.submissions
    assert 'PRICE_OFFSET_NO_ROOM' in result.reason
    report = load_saved_cycle_report(tmp_path/'cycle')
    assert report['cycle']['terminal']
    assert report['cycle']['outcome'] == 'FAILED_PREFLIGHT_BLOCKED'
    assert 'до отправки торговых ордеров' in '\n'.join(result_lines(report))
    assert len([e for e in rows(tmp_path/'cycle/cycle.jsonl') if e['event']=='CYCLE_COMPLETE']) == 1


@pytest.mark.asyncio
async def test_success_report_does_not_use_an_earlier_retry_reason(tmp_path):
    clock = AdvancingClock(); client = AckClient(clock)
    await run_random_cycle(cycle_config(tmp_path/'cycle', receiver_admission='ack'),
                           client, clock=clock, rng=FixedRng(20,20))
    path=tmp_path/'cycle/cycle.jsonl'; events=rows(path)
    # Keep a harmless earlier diagnostic reason; final success remains authoritative.
    events[0]['payload']['reason']='old retry veto'
    path.write_text(''.join(json.dumps(e)+'\n' for e in events))
    report=load_saved_cycle_report(tmp_path/'cycle')
    assert report['cycle']['outcome']=='SUCCESS'
    assert report['cycle']['reason'] is None
    assert 'old retry veto' in report['reasons']  # historical evidence is retained


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', [None, 'conflict', 'duplicate', 'incomplete', 'excess'])
async def test_fallback_partial_history_refresh_is_deduplicated_and_validated(tmp_path, failure):
    class Client(LaggingFallback):
        async def list_trades(self, a, m, **kwargs):
            page = await ExternalOpeningClient.list_trades(self, a, m, **kwargs)
            if self.fallback_plans and str(kwargs.get('order_id')) == self.latest_order.get(a):
                self.history_reads += 1
                original = page.trades[0]
                first = replace(original, quantity=Decimal('.10'))
                second = replace(first, trade_id='second-fill')
                if self.history_reads == 1:
                    return HistoryPage(trades=(first,))
                if failure == 'conflict':
                    first = replace(first, quantity=Decimal('.11'))
                if failure == 'duplicate':
                    return HistoryPage(trades=(first, first))
                if failure == 'excess':
                    second = replace(second, quantity=Decimal('.11'))
                return HistoryPage(trades=(first, second), complete=failure != 'incomplete')
            return page
    clock=AdvancingClock(); client=Client(clock, 'partial')
    result=await run_random_cycle(cycle_config(tmp_path/'cycle'), client,
                                 clock=clock, rng=FixedRng(20,20))
    assert len(client.fallback_plans)==1
    assert client.history_reads==2
    if failure is None:
        assert result.inventory=='CONFIRMED_FLAT',result.reason
        assert result.fallbacks[0].filled_quantity==Decimal('.20')
    else:
        assert result.outcome is Outcome.UNKNOWN
        assert result.inventory!='CONFIRMED_FLAT'


@pytest.mark.asyncio
async def test_cancel_ambiguous_transport_is_not_replayed(tmp_path):
    signer=ConstantNonceSigner(account_index=11)
    client=_constant_nonce_client(prefix='ack-ambiguous-cancel',signer=signer)
    calls=[]
    async def lose_response(*args,**kwargs):
        calls.append(1)
        raise TimeoutError('response lost after transport entered')
    client._http.post_form=lose_response
    value=replace(snapshot(),market_id=7)
    plan=OrderPlan(account_index=11,market_id=7,side='BUY',quantity=Decimal('.20'),price=Decimal('100'),
                   order_type='LIMIT',time_in_force='POST_ONLY',reduce_only=False,client_order_index=77,
                   quantity_int=20,price_int=1000,order_expiry_ms=9999999999999)
    engine=HandoffEngine(client);engine._configured_freshness=10;engine._configured_request_timeout=1
    journal=DurableJournal(tmp_path/'cancel.jsonl');errors=[]
    try:
        engine._start_cancel_nonce(plan)
        await engine._cancel_nonce_task
        for _ in range(2):
            await engine._cancel_if_safe(plan,value,journal,journal.run_id,errors,
                                         expected_order_id='123',exact_order_just_read=True)
        assert len(calls)==1
        assert len([e for e in journal.events if e.event=='CANCEL_DISPATCH_INTENT'])==1
        assert not any(e.event=='CANCEL_DISPATCH_RESULT' and e.payload.get('accepted') for e in journal.events)
    finally:
        await engine._finish_cancel_preparation()
        await client.aclose()
