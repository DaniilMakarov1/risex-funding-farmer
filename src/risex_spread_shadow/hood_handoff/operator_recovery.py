"""Explicit operator recovery; current readiness never rewrites past outcomes."""
from __future__ import annotations

import asyncio
from dataclasses import replace
import hashlib
import json
import math
import os
from pathlib import Path
import re

from .contracts import ContractError, OrderPlan, OrderSnapshot, Outcome, PreflightBlocked
from .journal import DurableJournal, sanitize_exception
from .keychain import KeychainSecretProvider
from .operator_control import exclusive_lock
from .provenance import capture_provenance
from .random_cycle import RandomCycleEngine, _account_payload, _fallback_order_mismatch_map
from .readiness import ReadOnlyLighterSdkClient
from .sdk import PlainAioHttp, _order_snapshot_mapping, _require_success_code
from .telegram_accounts import missing_key

SLOT = re.compile(r'(?:cycle|close)-[0-9]{3,}')
INTENTS = {'SOURCE_DISPATCH_INTENT', 'RECEIVER_DISPATCH_INTENT', 'FALLBACK_DISPATCH_INTENT'}


def terminal_matches(order, item):
    plan = item['plan']
    return (order.terminal and not _fallback_order_mismatch_map(order, plan, expected_order_id=item.get('order_id'))
            and (plan.order_type != 'LIMIT' or order.price == plan.price))


class RecoveryReadClient(ReadOnlyLighterSdkClient):
    """Auth/account/exact-order reads only; no mutation signer or nonce path."""

    async def lookup_order(self, account_index, market_id, *, client_order_index):
        token = await self._authorization(account_index)
        http = PlainAioHttp(self.config.api_base_url, timeout_seconds=self.config.request_timeout_seconds)
        try:
            payload = await http.get('api/v1/accountOrders',
                                     params={'account_index': account_index,
                                             'client_order_indexes': str(client_order_index)},
                                     authorization=token)
        finally:
            await http.aclose()
        _require_success_code(payload, 'accountOrders')
        if not isinstance(payload.get('orders'), list) or payload.get('next_cursor'):
            raise ContractError('exact order read incomplete')
        orders = [OrderSnapshot.from_mapping(_order_snapshot_mapping(item, observed_at=self._clock()))
                  for item in payload['orders']]
        if any(o.account_index != account_index or o.market_id != market_id
               or str(o.client_order_index) != str(client_order_index) for o in orders) or len(orders) > 1:
            raise ContractError('exact order identity conflicts')
        return orders[0] if orders else None


async def lookup_previous_order(client, config, plan):
    """The exact lookup may omit old orders; read bounded inactive history then.

    Never infer cancellation from absence/age. This read is only at the next
    operator admission, not between a live maker and its receiver.
    """
    order = await client.lookup_order(plan.account_index, plan.market_id, client_order_index=plan.client_order_index)
    if order is not None:
        return order
    # Synthetic/custom clients can expose a history reader without transport.
    reader = getattr(client, 'lookup_inactive_order', None)
    if callable(reader):
        return await reader(plan)
    if not isinstance(client, RecoveryReadClient):
        from .sdk import LighterSdkClient
        if not isinstance(client, LighterSdkClient):
            return None
    token = await client._authorization(plan.account_index)
    http = PlainAioHttp(config.api_base_url, timeout_seconds=config.request_timeout_seconds)
    cursor = None
    seen = set()
    try:
        for _ in range(config.max_poll_count):
            params = {'account_index': plan.account_index, 'market_id': plan.market_id,
                      'market_type': 'perp', 'limit': 100}
            if cursor:
                params['cursor'] = cursor
            payload = await http.get('api/v1/accountInactiveOrders', params=params, authorization=token)
            _require_success_code(payload, 'accountInactiveOrders')
            if not isinstance(payload.get('orders'), list):
                raise ContractError('inactive order history lacks orders')
            found = []
            for value in payload['orders']:
                order = OrderSnapshot.from_mapping(_order_snapshot_mapping(value, observed_at=client._clock()))
                if order.account_index != plan.account_index or order.market_id != plan.market_id:
                    raise ContractError('inactive order history identity conflicts')
                if str(order.client_order_index) == str(plan.client_order_index):
                    found.append(order)
            if len(found) > 1:
                raise ContractError('inactive order history duplicates client identity')
            if found:
                return found[0]
            cursor = payload.get('next_cursor')
            if not cursor:
                return None
            if not isinstance(cursor, str) or cursor in seen:
                raise ContractError('inactive order pagination conflicts')
            seen.add(cursor)
        raise ContractError('inactive order pagination exceeded configured bound')
    finally:
        await http.aclose()


def journal_rows(path):
    """Bounded strict reads, never follow links or repair historical inputs."""
    if path.is_symlink() or path.stat().st_size > 32 * 1024 * 1024:
        raise PreflightBlocked('historical journal is unsafe or too large')
    prior = -1
    run_id = None
    with path.open(encoding='utf-8') as stream:
        for sequence, line in enumerate(stream, 1):
            if sequence > 100000 or len(line) > 2 * 1024 * 1024 or not line.endswith('\n'):
                raise PreflightBlocked('historical journal is incomplete')
            row = json.loads(line)
            at = row.get('at')
            if (row.get('sequence') != sequence or type(row.get('sequence')) is not int
                    or isinstance(at, bool) or not isinstance(at, (int, float))
                    or not math.isfinite(at) or at < prior or not isinstance(row.get('payload'), dict)):
                raise PreflightBlocked('historical journal identity/time is invalid')
            if run_id is None:
                run_id = row.get('run_id')
            if not isinstance(run_id, str) or not run_id or row.get('run_id') != run_id:
                raise PreflightBlocked('historical journal run identity conflicts')
            prior = at
            yield row


def prior_intents(operator, config):
    """Return every old creation intent and its exact terminal/rejection proof.

    Admission UNKNOWN does not imply an order can still execute. Conversely an
    empty current book alone does not resolve a transmitted but invisible order.
    """
    intents = {}
    files = []
    indices = {config.source_account_index, config.receiver_account_index}
    for slot in sorted(Path(operator).iterdir()):
        if not SLOT.fullmatch(slot.name):
            continue
        if slot.is_symlink() or not slot.is_dir():
            raise PreflightBlocked('unsafe historical slot')
        for path in sorted(slot.glob('*.jsonl')):
            if len(files) >= 2000:
                raise PreflightBlocked('historical journal count exceeded')
            if path.is_symlink():
                raise PreflightBlocked('unsafe historical journal')
            digest = hashlib.sha256()
            with path.open('rb') as stream:
                for chunk in iter(lambda: stream.read(65536), b''):
                    digest.update(chunk)
            files.append({'path': str(path), 'sha256': digest.hexdigest()})
            local = {}
            for row in journal_rows(path):
                event, payload = row['event'], row['payload']
                if event in INTENTS:
                    plan = OrderPlan(**payload['plan'])
                    if plan.account_index not in indices or plan.market_id != config.market_id:
                        raise PreflightBlocked('historical intent account/market differs')
                    key = (plan.account_index, plan.client_order_index)
                    if key in intents:
                        raise PreflightBlocked('historical client identity reused')
                    item = {'plan': plan, 'resolved': False, 'at': row['at'], 'order': None}
                    intents[key] = item
                    local[event.split('_DISPATCH_')[0]] = item
                elif event.endswith('_DISPATCH_RESULT'):
                    item = local.get(event.split('_DISPATCH_')[0])
                    if item is not None:
                        item['order_id'] = payload.get('order_id')
                        if payload.get('accepted') is False:
                            item['resolved'] = True  # Explicit application rejection, never timeout.
                elif event in {'COMPLETE', 'FALLBACK_ATTEMPT_EVIDENCE'}:
                    receipt = payload.get('receipt') or {}
                    candidates = [payload.get('order')] if event == 'FALLBACK_ATTEMPT_EVIDENCE' else [
                        (receipt.get(leg) or {}).get('order') for leg in ('source', 'receiver')]
                    for value in candidates:
                        if value is None:
                            continue
                        order = OrderSnapshot.from_mapping(value)
                        key = (order.account_index, int(order.client_order_index))
                        item = intents.get(key)
                        if item is not None:
                            if _fallback_order_mismatch_map(order, item['plan'], expected_order_id=item.get('order_id')):
                                raise PreflightBlocked('historical terminal order conflicts with intent')
                            if terminal_matches(order, item) and order.observed_at >= item['at']:
                                item.update(resolved=True, order=order)
    return list(intents.values()), files


async def resolve_prior(config, client, operator, *, clock=None):
    """Resolve outstanding creations before admitting a new operation."""
    engine = RandomCycleEngine(client, clock=clock)
    intents, files = await asyncio.to_thread(prior_intents, operator, config)
    unresolved = [item for item in intents if not item['resolved']]
    if len(unresolved) > config.max_poll_count:
        raise PreflightBlocked('too many unresolved historical intents for a bounded check')
    checked = []
    async def resolve():
        for item in unresolved:
            plan = item['plan']
            order = await asyncio.wait_for(lookup_previous_order(client, config, plan),
                timeout=config.request_timeout_seconds)
            now = engine.clock.now()
            if (not isinstance(order, OrderSnapshot) or not terminal_matches(order, item)
                    or not 0 <= now - order.observed_at <= config.freshness_seconds
                    or order.observed_at < item['at']):
                raise PreflightBlocked('previous order is unresolved or still executable')
            checked.append({'account_index': order.account_index, 'order_id': order.order_id,
                            'client_order_index': order.client_order_index, 'status': order.status,
                            'observed_at': order.observed_at})
    await asyncio.wait_for(resolve(), timeout=config.reconcile_timeout_seconds)
    return {'previous_intents': len(intents), 'resolved_now': checked, 'inputs': files}


async def inspect_current(config, client, operator, *, require_flat, clock=None):
    """Called under the operator lock; read only, with causal accounts last."""
    engine = RandomCycleEngine(client, clock=clock)
    prior = await resolve_prior(config, client, operator, clock=engine.clock)
    source, receiver = await engine._accounts(config)
    if require_flat and (source.signed_position != 0 or receiver.signed_position != 0):
        raise PreflightBlocked('positions remain; use /close before /run')
    proof = {'status': 'READY' if require_flat else 'CLOSE_READY', 'at': engine.clock.now(),
             'source': _account_payload(source), 'receiver': _account_payload(receiver),
             **prior}
    return source, receiver, proof


async def check_recovery(config, operator, *, require_flat=True,
                         client_factory=RecoveryReadClient, secret_factory=KeychainSecretProvider.from_config):
    """Persist a separate current-state checkpoint without rewriting any cycle."""
    with exclusive_lock(Path(operator) / '.operator-launch.lock'):
        secrets = secret_factory(config, (config.source_account_index, config.receiver_account_index), prompt=missing_key)
        client = None
        try:
            client = client_factory(config, source_account_index=config.source_account_index,
                                    receiver_account_index=config.receiver_account_index, secrets=secrets)
            _, _, proof = await inspect_current(config, client, operator, require_flat=require_flat)
            journal = DurableJournal(Path(operator) / 'recovery-checks.jsonl')
            journal.acquire_attempt()
            try:
                journal.append('CURRENT_STATE_VERIFIED', proof)
            finally:
                journal.release_attempt()
            return proof
        finally:
            try:
                if client is not None:
                    await client.aclose()
            finally:
                secrets.close()


def allocate_close_slot(operator):
    operator = Path(operator)
    numbers = [int(p.name[6:]) for p in operator.iterdir() if re.fullmatch(r'close-[0-9]{3,}', p.name)]
    path = operator / f'close-{max(numbers, default=0) + 1:03d}'
    path.mkdir(mode=0o700)
    fd = os.open(operator, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return path


async def close_positions(config, client, operator, slot, *, clock=None):
    """User-triggered closure only; caller owns the normal operator lock."""
    if not config.operator_execution_opt_in or not config.operator_plan_reviewed:
        raise PreflightBlocked('explicit operator closure confirmation required')
    engine = RandomCycleEngine(client, clock=clock)
    config = replace(config, cycle_dir=slot, journal_path=Path(slot) / 'close.jsonl')
    journal = DurableJournal(config.journal_path, clock=engine.clock.now)
    journal.acquire_attempt()
    try:
        if journal.events:
            raise PreflightBlocked('close slot cannot be replayed')
        journal.append('CLOSE_STARTED', {'binding': config.binding(), 'runtime_provenance': capture_provenance(config.binding())})
        try:
            source, receiver, proof = await inspect_current(config, client, operator, require_flat=False, clock=engine.clock)
            journal.append('CLOSE_BASELINE', proof)
            results, source_after, receiver_after = await engine.close_reconciled_positions(config, journal, source, receiver)
            resolved = source_after is not None and receiver_after is not None and all(
                item.outcome is not Outcome.UNKNOWN for item in results)
            flat = resolved and source_after == 0 and receiver_after == 0
            result = {'status': 'CONFIRMED_FLAT' if flat else 'PARTIAL' if resolved else 'UNKNOWN',
                      'at': engine.clock.now(), 'symbol': config.market_symbol,
                      'positions': [{'account_index': index, 'position': None if pos is None else str(pos)}
                                    for index, pos in ((config.source_account_index, source_after), (config.receiver_account_index, receiver_after))],
                      'attempts': [r.as_dict() for r in results],
                      'reason': next((r.reason for r in results if r.reason), None)}
        except Exception as exc:
            result = {'status': 'UNKNOWN', 'at': engine.clock.now(), 'symbol': config.market_symbol,
                      'reason': str(exc) if isinstance(exc, PreflightBlocked) else sanitize_exception(exc)}
        journal.append('CLOSE_COMPLETE', result)
        return result
    finally:
        journal.release_attempt()


def load_close_result(slot):
    rows = journal_rows(Path(slot) / 'close.jsonl')
    result = None
    for row in rows:
        result = row['payload'] if row['event'] == 'CLOSE_COMPLETE' else None
    return result
