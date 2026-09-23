"""Cancellation boundary for one signed leverage setting; no live transport."""
import asyncio
from dataclasses import replace
import json
import time

import pytest

from risex_spread_shadow.hood_handoff.contracts import LeverageNotSent, Outcome, PreflightBlocked
from risex_spread_shadow.hood_handoff.operator_recovery import inspect_current
from test_hood_handoff_random_cycle import AdvancingClock, FixedRng, cycle_config, run_random_cycle
from test_hood_handoff_sdk_interface import ConstantNonceSigner, FakeHttp, _constant_nonce_client
from test_hood_hcr40 import LeverageClient


class ValidSigner(ConstantNonceSigner):
    async def sign_update_leverage(self, **kwargs):
        self.leverage_calls.append(kwargs)
        return 20, 'synthetic-signed-body', 'aA' * 40, None


class PendingHttp(FakeHttp):
    started = None

    async def post_form(self, path, *, form):
        self.calls.append((path, dict(form)))
        type(self).started.set()
        await asyncio.Event().wait()


def _events(slot):
    return [json.loads(line)['event'] for line in (slot / 'cycle.jsonl').read_text().splitlines()]


@pytest.mark.asyncio
@pytest.mark.parametrize('stage', ['lock', 'nonce', 'signing'])
async def test_outer_deadline_during_preparation_records_not_sent_and_next_run_ready(tmp_path, stage):
    signer = ValidSigner()
    adapter = _constant_nonce_client(prefix=f'preparation-{stage}', signer=signer)
    adapter.config = replace(adapter.config, request_timeout_seconds=.05)
    adapter._signer(11)  # Keep SDK import outside the bounded preparation interval.
    started = asyncio.Event()
    held_lock = None
    if stage == 'lock':
        held_lock = adapter._preparation_lock_for(11, 4)
        await held_lock.acquire()
    elif stage == 'nonce':
        async def slow_nonce(signer, key_index, *, deadline=None):
            started.set()
            return await adapter._bounded(asyncio.sleep(1, result=41), deadline, 'slow nonce')
        adapter._next_nonce = slow_nonce
    else:
        async def slow_sign(**kwargs):
            started.set()
            await asyncio.sleep(1)
            return 20, 'synthetic-signed-body', 'aA' * 40, None
        signer.sign_update_leverage = slow_sign

    class Client(LeverageClient):
        supports_leverage_prepared_intent = True

        async def update_leverage_fraction(self, *args, **kwargs):
            return await adapter.update_leverage_fraction(*args, **kwargs)

    clock = AdvancingClock()
    client = Client(clock)
    slot = tmp_path / 'cycle-001'
    try:
        result = await run_random_cycle(cycle_config(slot, api_key_index=4,
            request_timeout_seconds=.05), client, clock=clock, rng=FixedRng(40, 20))
        assert result.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED
        if stage != 'lock':
            assert started.is_set()
        assert not adapter._http.calls
        assert _events(slot).count('LEVERAGE_UPDATE_NOT_SENT') == 1
        assert 'LEVERAGE_TX_PREPARED' not in _events(slot)
        _, _, proof = await inspect_current(cycle_config(tmp_path / 'cycle-002', api_key_index=4),
            client, tmp_path, require_flat=True, clock=clock)
        assert proof['unresolved_leverage_settings'] == 0
    finally:
        if held_lock is not None:
            held_lock.release()
        await adapter.aclose()


@pytest.mark.asyncio
async def test_explicit_pretransport_cancel_propagates_and_records_once():
    adapter = _constant_nonce_client(prefix='explicit-before', signer=ValidSigner())
    started = asyncio.Event()
    recorded = []

    async def slow_nonce(signer, key_index, *, deadline=None):
        started.set()
        await asyncio.Event().wait()

    original_nonce = adapter._next_nonce
    adapter._next_nonce = slow_nonce
    task = asyncio.create_task(adapter.update_leverage_fraction(11, 7, 4166, 0,
        cancelled_before_transport=lambda: recorded.append('NOT_SENT')))
    try:
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert recorded == ['NOT_SENT']
        assert not adapter._http.calls
        adapter._next_nonce = original_nonce
        receipt = await adapter.update_leverage_fraction(11, 7, 4166, 0)
        assert receipt.accepted and len(adapter._http.calls) == 1
    finally:
        await adapter.aclose()


@pytest.mark.asyncio
async def test_cancel_after_prepared_identity_but_before_send_is_not_sent():
    adapter = _constant_nonce_client(prefix='prepared-before', signer=ValidSigner())
    recorded = []

    def prepared(identity):
        recorded.append('PREPARED')
        asyncio.current_task().cancel()

    try:
        task = asyncio.create_task(adapter.update_leverage_fraction(11, 7, 4166, 0,
            prepared_intent=prepared,
            cancelled_before_transport=lambda: recorded.append('NOT_SENT')))
        with pytest.raises(asyncio.CancelledError):
            await task
        assert recorded == ['PREPARED', 'NOT_SENT']
        assert not adapter._http.calls
        receipt = await adapter.update_leverage_fraction(11, 7, 4166, 0)
        assert receipt.accepted and len(adapter._http.calls) == 1
    finally:
        await adapter.aclose()


@pytest.mark.asyncio
async def test_prepared_journal_crossing_deadline_is_proved_not_sent():
    adapter = _constant_nonce_client(prefix='prepared-deadline', signer=ValidSigner())
    adapter.config = replace(adapter.config, request_timeout_seconds=.01)
    adapter._signer(11)
    try:
        with pytest.raises(LeverageNotSent):
            await adapter.update_leverage_fraction(11, 7, 4166, 0,
                prepared_intent=lambda identity: time.sleep(.02))
        assert not adapter._http.calls
    finally:
        await adapter.aclose()


@pytest.mark.asyncio
async def test_cancel_after_transport_start_stays_unknown_and_sends_once(tmp_path):
    PendingHttp.started = asyncio.Event()
    adapter = _constant_nonce_client(prefix='explicit-after', signer=ValidSigner(),
                                      http_factory=PendingHttp)
    recorded = []
    task = asyncio.create_task(adapter.update_leverage_fraction(11, 7, 4166, 0,
        prepared_intent=lambda identity: recorded.append('PREPARED'),
        cancelled_before_transport=lambda: recorded.append('NOT_SENT')))
    try:
        await PendingHttp.started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert recorded == ['PREPARED']
        assert len(adapter._http.calls) == 1
        with pytest.raises(LeverageNotSent):
            await adapter.update_leverage_fraction(11, 7, 4166, 0)
        assert len(adapter._http.calls) == 1
    finally:
        await adapter.aclose()


@pytest.mark.asyncio
async def test_outer_deadline_after_transport_stays_blocked(tmp_path):
    PendingHttp.started = asyncio.Event()
    adapter = _constant_nonce_client(prefix='deadline-after', signer=ValidSigner(),
                                      http_factory=PendingHttp)
    adapter.config = replace(adapter.config, request_timeout_seconds=.05)

    class Client(LeverageClient):
        supports_leverage_prepared_intent = True

        async def update_leverage_fraction(self, *args, **kwargs):
            return await adapter.update_leverage_fraction(*args, **kwargs)

    clock = AdvancingClock()
    client = Client(clock)
    slot = tmp_path / 'cycle-001'
    try:
        await run_random_cycle(cycle_config(slot, api_key_index=4,
            request_timeout_seconds=.05), client, clock=clock, rng=FixedRng(40, 20))
        assert PendingHttp.started.is_set()
        assert len(adapter._http.calls) == 1
        assert 'LEVERAGE_TX_PREPARED' in _events(slot)
        assert 'LEVERAGE_UPDATE_NOT_SENT' not in _events(slot)
        with pytest.raises(PreflightBlocked):
            await inspect_current(cycle_config(tmp_path / 'cycle-002', api_key_index=4),
                client, tmp_path, require_flat=True, clock=clock)
    finally:
        await adapter.aclose()
