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
import time

from .contracts import ContractError, OrderPlan, OrderSnapshot, Outcome, PreflightBlocked
from .journal import DurableJournal, sanitize_exception
from .keychain import KeychainSecretProvider
from .operator_control import exclusive_lock
from .provenance import capture_provenance
from .random_cycle import RandomCycleEngine, _account_payload, _fallback_order_mismatch_map, _observed_leverage_fraction, _recovery_stop_reason
from .readiness import ReadOnlyLighterSdkClient
from .sdk import PlainAioHttp, _leverage_next_nonce, _leverage_tx_diagnostic, _leverage_tx_hash, _order_snapshot_mapping, _require_success_code
from .telegram_accounts import missing_key

SLOT = re.compile(r'(?:cycle|close)-[0-9]{3,}')
INTENTS = {'SOURCE_DISPATCH_INTENT', 'RECEIVER_DISPATCH_INTENT', 'FALLBACK_DISPATCH_INTENT'}


def terminal_matches(order, item):
    plan = item['plan']
    return (order.terminal and not _fallback_order_mismatch_map(order, plan, expected_order_id=item.get('order_id'))
            and (plan.order_type != 'LIMIT' or order.price == plan.price))


class RecoveryReadClient(ReadOnlyLighterSdkClient):
    """Auth/account/exact-order reads only; no mutation signer or nonce path."""

    async def read_leverage_transaction(self, tx_hash):
        if _leverage_tx_hash(tx_hash) is None:
            raise ContractError('leverage transaction hash is invalid')
        token = await self._authorization(self.source_account_index)
        http = PlainAioHttp(self.config.api_base_url, timeout_seconds=self.config.request_timeout_seconds)
        try:
            payload = await http.get('api/v1/tx', params={'by': 'hash', 'value': tx_hash},
                                     authorization=token)
        finally:
            await http.aclose()
        _require_success_code(payload, 'transaction')
        return _leverage_tx_diagnostic(payload, tx_hash, self._clock())

    async def read_leverage_next_nonce(self, account_index, api_key_index):
        if (account_index not in {self.source_account_index, self.receiver_account_index}
                or api_key_index != self.config.api_key_index):
            raise ContractError('nextNonce identity is invalid')
        token = await self._authorization(account_index)
        http = PlainAioHttp(self.config.api_base_url, timeout_seconds=self.config.request_timeout_seconds)
        try:
            payload = await http.get('api/v1/nextNonce',
                                     params={'account_index': account_index, 'api_key_index': api_key_index},
                                     authorization=token)
        finally:
            await http.aclose()
        return _leverage_next_nonce(payload)

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
    unresolved_leverage = []
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
            local_leverage = {}
            for row in journal_rows(path):
                event, payload = row['event'], row['payload']
                if event == 'LEVERAGE_UPDATE_INTENT':
                    index = payload.get('account_index')
                    fraction = payload.get('fraction_bps')
                    if (index not in indices or payload.get('market_id') != config.market_id
                            or payload.get('margin_mode') != 0 or type(fraction) is not int
                            or not 2500 <= fraction <= 10000 or index in local_leverage):
                        raise PreflightBlocked('historical leverage setting intent is invalid')
                    item = {'account_index': index, 'fraction_bps': fraction, 'resolved': False,
                            'source_identity': payload.get('source_identity'),
                            'prepared_identity': None, 'journal_path': str(path),
                            'journal_sha256': digest.hexdigest(), 'intent_at': row['at']}
                    local_leverage[index] = item
                    unresolved_leverage.append(item)
                elif event == 'LEVERAGE_TX_PREPARED':
                    index = payload.get('account_index')
                    item = local_leverage.get(index)
                    if (item is None or item['prepared_identity'] is not None
                            or payload.get('market_id') != config.market_id
                            or payload.get('fraction_bps') != item['fraction_bps']
                            or payload.get('margin_mode') != 0
                            or payload.get('api_key_index') != config.api_key_index
                            or payload.get('tx_type') != 20
                            or type(payload.get('nonce')) is not int
                            or _leverage_tx_hash(payload.get('tx_hash')) is None):
                        raise PreflightBlocked('historical prepared leverage transaction conflicts with intent')
                    item['prepared_identity'] = {
                        'hash': payload['tx_hash'], 'nonce': payload['nonce'],
                        'account_index': index, 'api_key_index': config.api_key_index,
                    }
                elif event in {'LEVERAGE_UPDATE_CONFIRMED', 'LEVERAGE_UPDATE_REJECTED',
                               'LEVERAGE_UPDATE_NOT_SENT'}:
                    index = payload.get('account_index')
                    item = local_leverage.get(index)
                    if (item is None or item['resolved'] or payload.get('market_id') != config.market_id
                            or payload.get('fraction_bps') != item['fraction_bps']):
                        raise PreflightBlocked('historical leverage setting result conflicts with intent')
                    item['resolved'] = True
                elif event in INTENTS:
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
    pending_leverage = [item for item in unresolved_leverage if not item['resolved']]
    return list(intents.values()), files, pending_leverage


def _leverage_checkpoints(operator):
    path = Path(operator) / 'recovery-checks.jsonl'
    if not path.exists():
        return {}
    if path.is_symlink() or not path.is_file():
        raise PreflightBlocked('unsafe recovery checkpoint')
    checkpoints = {}
    for row in journal_rows(path):
        if row['event'] != 'LEVERAGE_RESOLUTION_CHECKPOINT':
            continue
        payload = row['payload']
        # Version 1 was an unaccepted inference from status/timestamps alone.
        if payload.get('proof_version') != 2:
            continue
        original = payload.get('original_path')
        account_index = payload.get('account_index')
        key = (original, account_index)
        if (not isinstance(original, str) or type(account_index) is not int
                or key in checkpoints):
            raise PreflightBlocked('duplicate or invalid leverage checkpoint')
        checkpoints[key] = payload
    return checkpoints


def _checkpoint_matches(item, checkpoints):
    payload = checkpoints.get((item['journal_path'], item['account_index']))
    if payload is None:
        return False
    identity = item['prepared_identity']
    if identity is None:
        raise PreflightBlocked('checkpoint lacks an original prepared transaction')
    expected = {
        'original_path': item['journal_path'], 'original_sha256': item['journal_sha256'],
        'account_index': item['account_index'], 'source_identity': item['source_identity'],
        'fraction_bps': item['fraction_bps'], 'tx_hash': identity['hash'],
        'nonce': identity['nonce'], 'api_key_index': identity['api_key_index'],
        'tx_type': 20,
    }
    if (any(payload.get(key) != value for key, value in expected.items())
            or type(payload.get('tx_status')) is not int
            or payload['tx_status'] not in (0, 2)
            or any(type(payload.get(key)) is not int or payload[key] <= 0
                   for key in ('executed_at', 'committed_at', 'verified_at'))
            or not payload['executed_at'] <= payload['committed_at'] <= payload['verified_at']
            or type(payload.get('next_nonce')) is not int
            or payload['next_nonce'] <= identity['nonce']
            or isinstance(payload.get('account_observed_at'), bool)
            or not isinstance(payload.get('account_observed_at'), (int, float))
            or not math.isfinite(payload['account_observed_at'])
            or payload['account_observed_at'] < payload['verified_at']):
        raise PreflightBlocked('leverage checkpoint conflicts with immutable history')
    return True


async def resolve_prior(config, client, operator, *, clock=None, require_leverage_resolved=True):
    """Resolve outstanding creations before admitting a new operation."""
    engine = RandomCycleEngine(client, clock=clock)
    intents, files, pending_leverage = await asyncio.to_thread(prior_intents, operator, config)
    if pending_leverage and require_leverage_resolved:
        checkpoints = await asyncio.to_thread(_leverage_checkpoints, operator)
        unresolved_settings = [item for item in pending_leverage if not _checkpoint_matches(item, checkpoints)]
        if unresolved_settings:
            if len(unresolved_settings) > config.max_poll_count:
                raise PreflightBlocked('too many unresolved leverage settings for a bounded check')
            reader = getattr(client, 'read_leverage_transaction', None)
            nonce_reader = getattr(client, 'read_leverage_next_nonce', None)
            if (not callable(reader) or not callable(nonce_reader)
                    or any(not item['prepared_identity'] or not item['source_identity']
                           for item in unresolved_settings)):
                raise PreflightBlocked('previous leverage setting lacks provable transaction identity')
            deadline = time.monotonic() + config.reconcile_timeout_seconds

            def remaining():
                value = min(config.request_timeout_seconds, deadline - time.monotonic())
                if value <= 0:
                    raise PreflightBlocked('leverage recovery exceeded configured bound')
                return value

            proved = []
            for item in unresolved_settings:
                identity = item['prepared_identity']
                tx = await asyncio.wait_for(reader(identity['hash']), timeout=remaining())
                try:
                    tx = _leverage_tx_diagnostic(tx, identity['hash'], engine.clock.now())
                except (ContractError, TypeError, AttributeError) as exc:
                    raise PreflightBlocked('previous leverage transaction evidence is incomplete') from exc
                if (any(tx.get(key) != value for key, value in identity.items())
                        or tx['status'] not in (0, 2)
                        or tx['executed_at'] < math.floor(item['intent_at'])
                        or tx['committed_at'] <= 0 or tx['verified_at'] <= 0):
                    raise PreflightBlocked('previous leverage transaction is pending or conflicting')
                next_nonce = await asyncio.wait_for(
                    nonce_reader(identity['account_index'], identity['api_key_index']),
                    timeout=remaining())
                if type(next_nonce) is not int or next_nonce <= identity['nonce']:
                    raise PreflightBlocked('previous leverage nonce consumption is unproved')
                proved.append((item, tx, next_nonce))
            source, receiver = await asyncio.wait_for(engine._recovery_accounts(config), timeout=remaining())
            accounts = {source.account_index: source, receiver.account_index: receiver}
            for item, tx, _ in proved:
                account = accounts[item['account_index']]
                label = 'source' if account.account_index == config.source_account_index else 'receiver'
                if (account.source_identity != item['source_identity']
                        or account.signed_position != 0 or account.active_orders
                        or (tx['status'] == 2 and
                            _observed_leverage_fraction(account, label) != item['fraction_bps'])):
                    raise PreflightBlocked('previous leverage transaction conflicts with fresh account setting')
            checkpoint = DurableJournal(Path(operator) / 'recovery-checks.jsonl', clock=engine.clock.now)
            checkpoint.acquire_attempt()
            try:
                for item, tx, next_nonce in proved:
                    checkpoint.append('LEVERAGE_RESOLUTION_CHECKPOINT', {
                        'proof_version': 2, 'original_path': item['journal_path'],
                        'original_sha256': item['journal_sha256'],
                        'account_index': item['account_index'],
                        'source_identity': item['source_identity'],
                        'fraction_bps': item['fraction_bps'],
                        'tx_hash': tx['hash'], 'nonce': tx['nonce'],
                        'api_key_index': tx['api_key_index'], 'tx_type': tx['type'],
                        'tx_status': tx['status'], 'executed_at': tx['executed_at'],
                        'committed_at': tx['committed_at'], 'verified_at': tx['verified_at'],
                        'next_nonce': next_nonce,
                        'account_observed_at': accounts[item['account_index']].observed_at,
                    })
            finally:
                checkpoint.release_attempt()
        pending_leverage = []
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
    return {'previous_intents': len(intents), 'unresolved_leverage_settings': len(pending_leverage),
            'resolved_now': checked, 'inputs': files}


async def inspect_current(config, client, operator, *, require_flat, clock=None, journal=None):
    """Called under the operator lock; read only, with causal accounts last."""
    engine = RandomCycleEngine(client, clock=clock)
    prior = await resolve_prior(config, client, operator, clock=engine.clock,
                                require_leverage_resolved=require_flat)
    source, receiver = await engine._recovery_accounts(config, journal)
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
            source, receiver, proof = await inspect_current(config, client, operator, require_flat=False, clock=engine.clock, journal=journal)
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
                      'reason': None if flat else (_recovery_stop_reason(journal) or next(
                          (r.reason for r in reversed(results) if r.reason), None))}
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
