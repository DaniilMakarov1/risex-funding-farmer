"""Private/public publication order and bounded liquidity-veto recovery."""
import json
from dataclasses import replace
from decimal import Decimal

import pytest

from risex_spread_shadow.hood_handoff import Direction, Outcome, run_random_cycle, load_saved_cycle_report
from risex_spread_shadow.hood_handoff.engine import HandoffEngine
from risex_spread_shadow.hood_handoff.operator_view import execution_lines
from risex_spread_shadow.hood_handoff.telegram_messages import saved_message
from test_hood_ws_confirmed import WsCycleClient
from test_hood_handoff_random_cycle import AdvancingClock, FixedRng, cycle_config, book


class PublicationClient(WsCycleClient):
    def __init__(self, clock, scenario, closing=False, once=False):
        super().__init__(clock)
        self.scenario = scenario
        self.target_closing = closing
        self.once = once
        self.views = 0

    def ws_admission_view(self, anchor, plan, order_id):
        order, depth = super().ws_admission_view(anchor, plan, order_id)
        if plan.reduce_only != self.target_closing:
            return order, depth
        self.views += 1
        if self.once and self.views > 1:
            return order, depth
        if self.scenario == 'lag':
            return order, book(self.clock.now())
        side = 'bids' if plan.side == 'BUY' else 'asks'
        levels = getattr(depth, side)
        best = levels[0]
        if self.scenario == 'better':
            best = replace(best, price=plan.price + (Decimal('.01') if plan.side == 'BUY' else -Decimal('.01')))
        elif self.scenario == 'extra':
            best = replace(best, quantity=plan.quantity + Decimal('.01'))
        elif self.scenario == 'smaller':
            best = replace(best, quantity=plan.quantity / 2)
        else:
            raise AssertionError(self.scenario)
        return order, replace(depth, **{side: (best,) + levels[1:]})


@pytest.mark.asyncio
@pytest.mark.parametrize('direction', [Direction.LONG, Direction.SHORT])
@pytest.mark.parametrize('closing', [False, True])
async def test_private_open_before_public_publication_does_not_require_rest_or_public_equality(tmp_path, direction, closing):
    clock = AdvancingClock()
    client = PublicationClient(clock, 'lag', closing)
    cfg = cycle_config(tmp_path / 'cycle', direction=direction, receiver_admission='ws_confirmed')
    result = await run_random_cycle(cfg, client, clock=clock, rng=FixedRng(20, 20))
    assert result.outcome is Outcome.SUCCESS, result.reason
    assert result.inventory == 'CONFIRMED_FLAT'
    phase = result.closing if closing else result.opening
    assert phase.priority_guard['ws_l2_status'] == 'PUBLICATION_UNCONFIRMED'
    assert phase.priority_guard['priority_proof_admitted'] is False
    for i, call in enumerate(client.calls):
        if call[:2] != ('send', 'LIMIT'):
            continue
        end = next(j for j in range(i + 1, len(client.calls)) if client.calls[j][0] == 'send')
        assert client.calls[end][1] == 'MARKET'
        assert not any(c[0] in {'rest_order', 'account', 'book'} for c in client.calls[i + 1:end])


@pytest.mark.asyncio
@pytest.mark.parametrize('scenario', ['better', 'extra'])
@pytest.mark.parametrize('closing', [False, True])
async def test_proved_zero_fill_liquidity_veto_can_retry_then_succeed(tmp_path, scenario, closing):
    clock = AdvancingClock()
    client = PublicationClient(clock, scenario, closing, once=True)
    cfg = cycle_config(tmp_path / 'cycle', receiver_admission='ws_confirmed')
    result = await run_random_cycle(cfg, client, clock=clock, rng=FixedRng(20, 20))
    assert result.outcome is Outcome.SUCCESS, result.reason
    assert result.inventory == 'CONFIRMED_FLAT'
    assert client.views == 2
    limits = [p for p in client.submissions if p.order_type == 'LIMIT' and p.reduce_only == closing]
    assert len(limits) == 2
    assert len({p.quantity for p in limits}) == 1
    assert len({p.client_order_index for p in limits}) == 2
    report = load_saved_cycle_report(tmp_path / 'cycle')
    assert report['status'] == 'COMPLETE', report['issues']
    before = len(client.submissions)
    again = await run_random_cycle(cfg, client, clock=clock, rng=FixedRng())
    assert again.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED
    assert len(client.submissions) == before


@pytest.mark.asyncio
@pytest.mark.parametrize('scenario, expected', [('better', 'BETTER_PRICE'), ('extra', 'SAME_PRICE_EXTRA_VOLUME'), ('smaller', 'SOURCE_LEVEL_SMALLER')])
async def test_persistent_veto_is_bounded_and_preserves_diagnostic_evidence(tmp_path, scenario, expected):
    clock = AdvancingClock()
    client = PublicationClient(clock, scenario)
    result = await run_random_cycle(cycle_config(tmp_path / 'cycle', receiver_admission='ws_confirmed'), client, clock=clock, rng=FixedRng(20, 20))
    assert result.inventory == 'CONFIRMED_FLAT', result.reason
    assert not any(p.order_type == 'MARKET' for p in client.submissions)
    assert len(client.submissions) == (1 if scenario == 'smaller' else 3)
    guard = result.opening.priority_guard
    assert guard['ws_l2_status'] == expected
    assert Decimal(guard['source_quantity']) == Decimal('.20')
    assert guard['best_price'] and guard['best_quantity'] and guard['source_order']
    notices = '\n'.join(execution_lines(result.opening.as_dict(), phase='opening'))
    assert 'Встречный MARKET остановлен' in notices
    assert 'Наша цена/объём' in notices
    assert 'LIMIT → решение:' in notices
    assert 'LIMIT → MARKET:' not in notices
    report = load_saved_cycle_report(tmp_path / 'cycle')
    assert report['status'] == 'COMPLETE', report['issues']
    assert 'Встречный MARKET остановлен' in saved_message('cycle-001', report)
    rows = [json.loads(line) for line in (tmp_path / 'cycle' / 'opening.jsonl').read_text().splitlines()]
    event = next(row for row in rows if row['event'] == 'PRE_RECEIVER_GUARD')
    intent = next(row for row in rows if row['event'] == 'SOURCE_DISPATCH_INTENT')
    assert event['payload']['request_started_at'] >= intent['at']
    assert event['payload']['ws_l2_status'] == expected


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', ['source_history', 'receiver_history', 'source_unknown', 'receiver_dispatched', 'extra_reason', 'wrong_mode', 'receiver_unknown', 'not_canceled'])
async def test_liquidity_retry_never_overrides_reconciliation_uncertainty(tmp_path, fault):
    clock = AdvancingClock()
    client = PublicationClient(clock, 'extra')
    result = await run_random_cycle(cycle_config(tmp_path / 'cycle', receiver_admission='ws_confirmed'), client, clock=clock, rng=FixedRng(20, 20))
    phase = result.opening
    source, receiver = phase.source, phase.receiver
    reasons, guard = phase.unknown_reasons, dict(phase.priority_guard)
    if fault == 'source_history': source = replace(source, history_complete=False)
    if fault == 'receiver_history': receiver = replace(receiver, history_complete=False)
    if fault == 'source_unknown': source = replace(source, unknown_reasons=('identity conflict',))
    if fault == 'receiver_dispatched': receiver = replace(receiver, dispatched=True)
    if fault == 'extra_reason': reasons = (*reasons, 'ambiguous source write')
    if fault == 'wrong_mode': guard['receiver_admission'] = 'strict'
    if fault == 'receiver_unknown': receiver = replace(receiver, unknown_reasons=('history conflict',))
    if fault == 'not_canceled': source = replace(source, order=replace(source.order, status='filled'))
    assert not HandoffEngine._retryable_pair_after_guard(phase.plan, source, receiver, reasons, guard)


@pytest.mark.asyncio
@pytest.mark.parametrize('closing', [False, True])
async def test_fill_during_cancel_after_liquidity_veto_closes_residual_without_retry(tmp_path, closing):
    from test_hood_handoff_random_cycle import GuardCancellationFillClient

    class CancelRace(PublicationClient):
        async def cancel_order(self, *args):
            self.closing = closing
            return await GuardCancellationFillClient.cancel_order(self, *args)

    clock = AdvancingClock()
    client = CancelRace(clock, 'extra', closing)
    result = await run_random_cycle(cycle_config(tmp_path / 'cycle', receiver_admission='ws_confirmed'), client, clock=clock, rng=FixedRng(20, 20))
    phase = result.closing if closing else result.opening
    assert not phase.receiver.dispatched
    assert phase.source.filled_quantity > 0
    assert not phase.retryable_pair
    assert client.views == 1
    assert result.inventory == 'CONFIRMED_FLAT', result.reason
    assert len(result.fallbacks) == 1
