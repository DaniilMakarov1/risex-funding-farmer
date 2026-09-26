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
from .random_cycle import RandomCycleEngine, _account_payload, _fallback_order_mismatch_map, _observed_leverage_fraction, _order_payload, _recovery_stop_reason
from .readiness import ReadOnlyLighterSdkClient
from .sdk import PlainAioHttp, _leverage_next_nonce, _leverage_tx_diagnostic, _leverage_tx_hash, _order_snapshot_mapping, _require_success_code
from .telegram_accounts import missing_key
from .wallet_pool import load_wallet_pool

SLOT = re.compile(r'(?:cycle|close)-[0-9]{3,}')
INTENTS = {'SOURCE_DISPATCH_INTENT', 'RECEIVER_DISPATCH_INTENT', 'FALLBACK_DISPATCH_INTENT'}
# Fan-out receivers 2..16 (owner request 2026-09-26) are creation intents too.
FANOUT_RECEIVER_INTENT = re.compile(r'RECEIVER_(?:[2-9]|1[0-6])_DISPATCH_INTENT')
FANOUT_RECEIVER_LEGS = tuple(f'receiver_{position}' for position in range(2, 17))
# Per-slot trading journals keep the original strict bound.  The shared
# append-only recovery journal grows with every readiness check across all
# series, so it has its own (still finite) bound; rows are streamed one line at
# a time and each line keeps the 2 MiB limit.
HISTORICAL_JOURNAL_MAX_BYTES = 32 * 1024 * 1024
SHARED_RECOVERY_JOURNAL_MAX_BYTES = 512 * 1024 * 1024
HISTORICAL_JOURNAL_MAX_FILES = 10000
# Exact terminal proofs of old creation intents live in their own shared
# journal: code that predates it never reads the file, so a rollback cannot
# be blocked by an event it does not know.
INTENT_CHECKPOINT_JOURNAL = 'recovery-intents.jsonl'
SHARED_JOURNAL_EVENTS = {
    'recovery-checks.jsonl': frozenset({'CURRENT_STATE_VERIFIED', 'LEVERAGE_RESOLUTION_CHECKPOINT'}),
    INTENT_CHECKPOINT_JOURNAL: frozenset({'INTENT_RESOLUTION_CHECKPOINT'}),
}


# Concurrent authenticated account reads when a wallet pool is checked.
WALLET_READ_CONCURRENCY = 4


class HistoryBoundExceeded(PreflightBlocked):
    """A finite history-scan bound was reached; not an order or position fact."""


class WalletKeyMissing(PreflightBlocked):
    """A pool wallet has no stored credential; its orders cannot be read."""


def _slim_account(row):
    """Per-wallet journal row: exact state without the full margin evidence."""
    row = row if isinstance(row, dict) else {}
    orders = row.get('active_orders')
    return {**{key: row.get(key) for key in ('account_index', 'market_id', 'signed_position',
                                               'observed_at', 'source_identity', 'available_balance')},
            'active_order_count': len(orders) if isinstance(orders, list) else None}


def persisted_proof(proof):
    """Journal form of a readiness proof: the per-file input list is replaced by
    its exact count and SHA-256 over the canonical sorted list.  The in-memory
    proof is not modified.  This keeps each shared-journal row small instead of
    repeating every historical path/hash on every check.  A wallet-pool proof
    keeps one compact exact row per wallet."""
    value = dict(proof)
    inputs = value.pop('inputs', None)
    if isinstance(inputs, list):
        canonical = json.dumps(sorted(({'path': str(item.get('path')), 'sha256': str(item.get('sha256'))}
                                       for item in inputs), key=lambda item: item['path']),
                               sort_keys=True, separators=(',', ':')).encode('utf-8')
        value['inputs_count'] = len(inputs)
        value['inputs_sha256'] = hashlib.sha256(canonical).hexdigest()
    if isinstance(value.get('accounts'), list):
        value['accounts'] = [_slim_account(row) for row in value['accounts']]
    return value


def terminal_matches(order, item):
    plan = item['plan']
    return (order.terminal and not _fallback_order_mismatch_map(order, plan, expected_order_id=item.get('order_id'))
            and (plan.order_type != 'LIMIT' or order.price == plan.price))


def _robinhood_binding(config):
    return (config.api_base_url == 'https://api.rh.lighter.xyz'
            and config.environment == 'robinhood' and config.chain_id == 466324)


def _robinhood_leverage_execution(raw, tx_hash, now):
    """Exact Robinhood execution event; retain milliseconds, never invent L1 proof."""
    if not isinstance(raw, dict):
        raise PreflightBlocked('Robinhood leverage transaction is malformed')
    result = {key: raw.get(key) for key in (
        'hash', 'type', 'status', 'account_index', 'api_key_index', 'nonce',
        'executed_at', 'committed_at', 'verified_at')}
    numeric = tuple(key for key in result if key != 'hash')
    event = raw.get('execution_event', raw.get('event_info'))
    if isinstance(event, str):
        try:
            event = json.loads(event)
        except (ValueError, TypeError):
            event = None
    if (_leverage_tx_hash(tx_hash) is None or result['hash'] != tx_hash
            or any(type(result[k]) is not int or result[k] < 0 for k in numeric)
            or result['type'] != 20 or result['status'] != 3
            or result['committed_at'] != 0 or result['verified_at'] != 0
            or not math.isfinite(now) or not 0 < result['executed_at'] <= now * 1000
            or not isinstance(event, dict)
            or any(type(event.get(k)) is not int for k in ('a', 'm', 'imf', 'mm'))
            or event['a'] != result['account_index'] or event['m'] < 0
            or not 2500 <= event['imf'] <= 10000 or event['mm'] != 0
            or event.get('ae') != ''):
        raise PreflightBlocked('Robinhood leverage execution event is unproved')
    result['execution_event'] = {k: event[k] for k in ('a', 'm', 'imf', 'mm', 'ae')}
    return result


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
        if _robinhood_binding(self.config) and payload.get('status') == 3:
            return _robinhood_leverage_execution(payload, tx_hash, self._clock())
        return _leverage_tx_diagnostic(payload, tx_hash, self._clock())

    async def read_leverage_next_nonce(self, account_index, api_key_index):
        readable = {self.source_account_index, self.receiver_account_index,
                    *getattr(self, 'read_account_indices', ())}
        if account_index not in readable or api_key_index != self.config.api_key_index:
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


def journal_rows(path, *, shared_recovery=False):
    """Bounded strict reads, never follow links or repair historical inputs."""
    if path.is_symlink():
        raise PreflightBlocked('historical journal is unsafe')
    if shared_recovery and path.name not in SHARED_JOURNAL_EVENTS:
        raise PreflightBlocked('shared recovery journal path is invalid')
    limit = SHARED_RECOVERY_JOURNAL_MAX_BYTES if shared_recovery else HISTORICAL_JOURNAL_MAX_BYTES
    if path.stat().st_size > limit:
        raise HistoryBoundExceeded('historical journal is too large')
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
            row_run = row.get('run_id')
            if (not isinstance(row_run, str) or not row_run
                    or (not shared_recovery and row_run != run_id)):
                raise PreflightBlocked('historical journal run identity conflicts')
            if shared_recovery and row.get('event') not in SHARED_JOURNAL_EVENTS[path.name]:
                raise PreflightBlocked('shared recovery journal event is invalid')
            prior = at
            yield row


def prior_intents(operator, config, indices=None):
    """Return every old creation intent and its exact terminal/rejection proof.

    Admission UNKNOWN does not imply an order can still execute. Conversely an
    empty current book alone does not resolve a transmitted but invisible order.
    ``indices`` is the wallet pool; history on any other account is refused.
    """
    intents = {}
    unresolved_leverage = []
    files = []
    indices = {config.source_account_index, config.receiver_account_index, *(indices or ())}
    for slot in sorted(Path(operator).iterdir()):
        if not SLOT.fullmatch(slot.name):
            continue
        if slot.is_symlink() or not slot.is_dir():
            raise PreflightBlocked('unsafe historical slot')
        for path in sorted(slot.glob('*.jsonl')):
            if len(files) >= HISTORICAL_JOURNAL_MAX_FILES:
                raise HistoryBoundExceeded('historical journal count exceeded')
            if path.is_symlink():
                raise PreflightBlocked('unsafe historical journal')
            digest = hashlib.sha256()
            with path.open('rb') as stream:
                for chunk in iter(lambda: stream.read(65536), b''):
                    digest.update(chunk)
            sha256 = digest.hexdigest()
            files.append({'path': str(path), 'sha256': sha256})
            # WS observations are diagnostic projections, not DurableJournal
            # mutation intents. Retain their hash, but never use them as proof
            # of execution or require a trading-journal envelope from them.
            if path.name == 'stream-events.jsonl':
                continue
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
                            'journal_sha256': sha256, 'intent_at': row['at']}
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
                elif event in INTENTS or FANOUT_RECEIVER_INTENT.fullmatch(event):
                    plan = OrderPlan(**payload['plan'])
                    if plan.account_index not in indices or plan.market_id != config.market_id:
                        raise PreflightBlocked('historical intent account/market differs')
                    key = (plan.account_index, plan.client_order_index)
                    if key in intents:
                        raise PreflightBlocked('historical client identity reused')
                    item = {'plan': plan, 'resolved': False, 'at': row['at'], 'order': None,
                            'plan_payload': payload['plan'], 'journal_path': str(path),
                            'journal_sha256': sha256}
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
                        (receipt.get(leg) or {}).get('order') for leg in ('source', 'receiver', *FANOUT_RECEIVER_LEGS)]
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
    for row in journal_rows(path, shared_recovery=True):
        if row['event'] != 'LEVERAGE_RESOLUTION_CHECKPOINT':
            continue
        payload = row['payload']
        # Version 1 was an unaccepted inference from status/timestamps alone.
        if type(payload.get('proof_version')) is not int or payload['proof_version'] not in (2, 3):
            continue
        original = payload.get('original_path')
        account_index = payload.get('account_index')
        key = (original, account_index)
        if (not isinstance(original, str) or type(account_index) is not int
                or key in checkpoints):
            raise PreflightBlocked('duplicate or invalid leverage checkpoint')
        checkpoints[key] = payload
    return checkpoints


def _checkpoint_matches(item, checkpoints, config=None):
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
    if payload.get('proof_version') == 3:
        if config is None or not _robinhood_binding(config):
            raise PreflightBlocked('Robinhood checkpoint venue binding conflicts')
        at = payload.get('account_observed_at')
        if isinstance(at, bool) or not isinstance(at, (int, float)) or not math.isfinite(at):
            raise PreflightBlocked('Robinhood checkpoint observation is invalid')
        tx = _robinhood_leverage_execution(payload.get('transaction', {}), identity['hash'], at)
        if (any(payload.get(key) != value for key, value in expected.items())
                or any(tx.get(key) != value for key, value in identity.items())
                or payload.get('tx_status') != tx['status']
                or any(payload.get(k) != tx[k] for k in ('executed_at', 'committed_at', 'verified_at'))
                or tx['execution_event']['m'] != config.market_id
                or tx['execution_event']['imf'] != item['fraction_bps']
                or tx['executed_at'] < math.floor(item['intent_at'] * 1000)
                or type(payload.get('next_nonce')) is not int
                or payload['next_nonce'] <= identity['nonce']):
            raise PreflightBlocked('Robinhood checkpoint conflicts with immutable history')
        return True
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


def _intent_checkpoints(operator):
    """Saved terminal proofs of old creation intents, by (account, client index).

    Rows of another proof version are ignored, so an older reader never
    accepts a proof format it does not know.
    """
    path = Path(operator) / INTENT_CHECKPOINT_JOURNAL
    if not path.exists() and not path.is_symlink():
        return {}
    if path.is_symlink() or not path.is_file():
        raise PreflightBlocked('unsafe intent checkpoint journal')
    checkpoints = {}
    for row in journal_rows(path, shared_recovery=True):
        payload = row['payload']
        if type(payload.get('proof_version')) is not int or payload['proof_version'] != 1:
            continue
        plan = payload.get('plan')
        if (not isinstance(plan, dict) or type(plan.get('account_index')) is not int
                or type(plan.get('client_order_index')) is not int):
            raise PreflightBlocked('intent checkpoint is invalid')
        checkpoints.setdefault((plan['account_index'], plan['client_order_index']), []).append(
            (row['at'], payload))
    return checkpoints


def _checkpointed_order(item, checkpoints):
    """The saved terminal order that exactly proves this intent, else None.

    Only a proof about the identical journal bytes is considered.  Such a
    proof must still pass the live rules (plan, expected order ID, terminal
    status, LIMIT price, observed after the intent and not after the proof
    was saved); anything else conflicts with immutable history and blocks.
    """
    plan = item['plan']
    for saved_at, payload in checkpoints.get((plan.account_index, plan.client_order_index), ()):
        if (payload.get('original_path') != item['journal_path']
                or payload.get('original_sha256') != item['journal_sha256']):
            continue
        try:
            order = OrderSnapshot.from_mapping(payload.get('order'))
        except (ContractError, TypeError, ValueError, AttributeError) as exc:
            raise PreflightBlocked('intent checkpoint conflicts with immutable history') from exc
        if (payload.get('plan') != item['plan_payload'] or payload.get('order_id') != item.get('order_id')
                or payload.get('intent_at') != item['at'] or not terminal_matches(order, item)
                or not item['at'] <= order.observed_at <= saved_at):
            raise PreflightBlocked('intent checkpoint conflicts with immutable history')
        return order
    return None


def _append_intent_checkpoints(operator, clock, proved):
    """Durably keep fresh exact terminal proofs; history files stay untouched."""
    journal = DurableJournal(Path(operator) / INTENT_CHECKPOINT_JOURNAL, clock=clock)
    journal.acquire_attempt()
    try:
        for item, order in proved:
            journal.append('INTENT_RESOLUTION_CHECKPOINT', {
                'proof_version': 1, 'original_path': item['journal_path'],
                'original_sha256': item['journal_sha256'], 'intent_at': item['at'],
                'plan': item['plan_payload'], 'order_id': item.get('order_id'),
                'order': _order_payload(order),
            })
    finally:
        journal.release_attempt()


def _resolution_row(order):
    return {'account_index': order.account_index, 'order_id': order.order_id,
            'client_order_index': order.client_order_index, 'status': order.status,
            'observed_at': order.observed_at}


async def resolve_prior(config, client, operator, *, clock=None, require_leverage_resolved=True, pool=None):
    """Resolve outstanding creations before admitting a new operation."""
    engine = RandomCycleEngine(client, clock=clock)
    pooled = pool is not None and pool.from_file
    intents, files, pending_leverage = await asyncio.to_thread(
        prior_intents, operator, config, tuple(pool.all) if pooled else None)
    if pending_leverage and require_leverage_resolved:
        checkpoints = await asyncio.to_thread(_leverage_checkpoints, operator)
        unresolved_settings = [item for item in pending_leverage if not _checkpoint_matches(item, checkpoints, config)]
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
                    robinhood_execution = _robinhood_binding(config) and 'execution_event' in tx
                    tx = (_robinhood_leverage_execution(tx, identity['hash'], engine.clock.now())
                          if robinhood_execution else _leverage_tx_diagnostic(tx, identity['hash'], engine.clock.now()))
                except (ContractError, TypeError, AttributeError) as exc:
                    raise PreflightBlocked('previous leverage transaction evidence is incomplete') from exc
                if (any(tx.get(key) != value for key, value in identity.items())
                        or (robinhood_execution and (
                            tx['execution_event']['m'] != config.market_id
                            or tx['execution_event']['imf'] != item['fraction_bps']
                            or tx['executed_at'] < math.floor(item['intent_at'] * 1000)))
                        or (not robinhood_execution and (tx['status'] not in (0, 2)
                            or tx['executed_at'] < math.floor(item['intent_at'])
                            or tx['committed_at'] <= 0 or tx['verified_at'] <= 0))):
                    raise PreflightBlocked('previous leverage transaction is pending or conflicting')
                next_nonce = await asyncio.wait_for(
                    nonce_reader(identity['account_index'], identity['api_key_index']),
                    timeout=remaining())
                if type(next_nonce) is not int or next_nonce <= identity['nonce']:
                    raise PreflightBlocked('previous leverage nonce consumption is unproved')
                proved.append((item, tx, next_nonce))
            if pooled:
                accounts = await asyncio.wait_for(
                    _wallet_snapshots(engine, config, sorted({item['account_index'] for item, _, _ in proved})),
                    timeout=remaining())
            else:
                source, receiver = await asyncio.wait_for(engine._recovery_accounts(config), timeout=remaining())
                accounts = {source.account_index: source, receiver.account_index: receiver}
            for item, tx, _ in proved:
                account = accounts[item['account_index']]
                label = 'source' if account.account_index == config.source_account_index else 'receiver'
                if (account.source_identity != item['source_identity']
                        or account.signed_position != 0 or account.active_orders
                        or (tx['status'] in (2, 3) and
                            _observed_leverage_fraction(account, label) != item['fraction_bps'])):
                    raise PreflightBlocked('previous leverage transaction conflicts with fresh account setting')
            checkpoint = DurableJournal(Path(operator) / 'recovery-checks.jsonl', clock=engine.clock.now)
            checkpoint.acquire_attempt()
            try:
                for item, tx, next_nonce in proved:
                    checkpoint.append('LEVERAGE_RESOLUTION_CHECKPOINT', {
                        'proof_version': 3 if 'execution_event' in tx else 2, 'original_path': item['journal_path'],
                        'original_sha256': item['journal_sha256'],
                        'account_index': item['account_index'],
                        'source_identity': item['source_identity'],
                        'fraction_bps': item['fraction_bps'],
                        'tx_hash': tx['hash'], 'nonce': tx['nonce'],
                        'api_key_index': tx['api_key_index'], 'tx_type': tx['type'],
                        'tx_status': tx['status'], 'executed_at': tx['executed_at'],
                        'committed_at': tx['committed_at'], 'verified_at': tx['verified_at'],
                        'next_nonce': next_nonce,
                        **({'transaction': tx} if 'execution_event' in tx else {}),
                        'account_observed_at': accounts[item['account_index']].observed_at,
                    })
            finally:
                checkpoint.release_attempt()
        pending_leverage = []
    unresolved = [item for item in intents if not item['resolved']]
    checkpointed = []
    if unresolved:
        # A terminal order never becomes executable again: an exact saved
        # proof replaces a new lookup through deep inactive-order history.
        saved = await asyncio.to_thread(_intent_checkpoints, operator)
        remaining = []
        for item in unresolved:
            order = _checkpointed_order(item, saved)
            if order is None:
                remaining.append(item)
            else:
                checkpointed.append(_resolution_row(order))
        unresolved = remaining
    if len(unresolved) > config.max_poll_count:
        raise PreflightBlocked('too many unresolved historical intents for a bounded check')
    checked = []
    proved = []
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
            proved.append((item, order))
            checked.append(_resolution_row(order))
    try:
        await asyncio.wait_for(resolve(), timeout=config.reconcile_timeout_seconds)
    except BaseException:
        # Proofs completed before a later lookup failed stay valid; keep
        # them, but never let saving them replace the original failure.
        if proved:
            try:
                _append_intent_checkpoints(operator, engine.clock.now, proved)
            except Exception:
                pass
        raise
    if proved:
        _append_intent_checkpoints(operator, engine.clock.now, proved)
    return {'previous_intents': len(intents), 'unresolved_leverage_settings': len(pending_leverage),
            'resolved_now': checked, 'resolved_by_checkpoint': checkpointed, 'inputs': files}


def _validate_wallet_fresh(config, snapshot, index, now):
    """The pair-read rules of the cycle engine, for any pool wallet."""
    label = f'wallet {index}'
    if snapshot.account_index != index or snapshot.market_id != config.market_id:
        raise PreflightBlocked(f'{label} account identity/market does not match the pool')
    if not snapshot.authorized or not snapshot.ready:
        raise PreflightBlocked(f'{label} account authorization/readiness is unproven')
    if snapshot.active_orders:
        raise PreflightBlocked(f'{label} has active cycle-market orders')
    if snapshot.margin_available is None or snapshot.margin_required is None:
        raise PreflightBlocked(f'{label} margin evidence is missing')
    if snapshot.observed_at > now:
        raise PreflightBlocked(f'{label} account state is from the future')
    if now - snapshot.observed_at > config.freshness_seconds:
        raise PreflightBlocked(f'{label} account state is stale')
    if not snapshot.source_identity:
        raise PreflightBlocked(f'{label} account identity is missing')


async def _wallet_snapshots(engine, config, indices, journal=None):
    """Fresh validated reads of every listed wallet, bounded fan-out.

    Each read is validated when it completes and uses the engine's
    read-only retry rules; one failure cancels and drains the rest.
    """
    indices = tuple(indices)
    semaphore = asyncio.Semaphore(WALLET_READ_CONCURRENCY)

    async def one(index):
        async def read():
            snapshot = await engine._read_account(config, index, f'wallet {index} account read')
            _validate_wallet_fresh(config, snapshot, index, engine.clock.now())
            return snapshot
        async with semaphore:
            return await engine._recovery_read(config, read, f'wallet {index} account', journal)

    tasks = [asyncio.create_task(one(index)) for index in indices]
    try:
        values = await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    return dict(zip(indices, values))


async def inspect_wallets(config, client, operator, pool, *, require_flat, clock=None, journal=None):
    """Pool form of ``inspect_current``: every wallet, active or paused."""
    engine = RandomCycleEngine(client, clock=clock)
    prior = await resolve_prior(config, client, operator, clock=engine.clock,
                                require_leverage_resolved=require_flat, pool=pool)
    wallets = await _wallet_snapshots(engine, config, pool.all, journal)
    if require_flat and any(snapshot.signed_position != 0 for snapshot in wallets.values()):
        raise PreflightBlocked('positions remain; use /close before /run')
    proof = {'status': 'READY' if require_flat else 'CLOSE_READY', 'at': engine.clock.now(),
             'source': _account_payload(wallets[config.source_account_index]),
             'receiver': _account_payload(wallets[config.receiver_account_index]),
             'accounts': [_account_payload(wallets[index]) for index in pool.all],
             'wallets': pool.as_dict(), **prior}
    return wallets, proof


async def inspect_current(config, client, operator, *, require_flat, clock=None, journal=None, pool=None):
    """Called under the operator lock; read only, with causal accounts last."""
    if pool is not None and pool.from_file:
        wallets, proof = await inspect_wallets(config, client, operator, pool, require_flat=require_flat,
                                               clock=clock, journal=journal)
        return wallets[config.source_account_index], wallets[config.receiver_account_index], proof
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


def require_pool_credentials(secrets, pool):
    """A pool wallet without a stored key cannot be checked for open orders."""
    check = getattr(secrets, 'has_stored_credential', None)
    if not pool.from_file or not callable(check):
        return
    for index in pool.all:
        if not check(index):
            raise WalletKeyMissing(f'wallet {index} has no stored credential')


async def check_recovery(config, operator, *, require_flat=True,
                         client_factory=RecoveryReadClient, secret_factory=KeychainSecretProvider.from_config):
    """Persist a separate current-state checkpoint without rewriting any cycle."""
    with exclusive_lock(Path(operator) / '.operator-launch.lock'):
        pool = load_wallet_pool(operator, config)
        indices = tuple(pool.all) if pool.from_file else (config.source_account_index, config.receiver_account_index)
        secrets = secret_factory(config, indices, prompt=missing_key)
        client = None
        try:
            require_pool_credentials(secrets, pool)
            client = client_factory(config, source_account_index=config.source_account_index,
                                    receiver_account_index=config.receiver_account_index, secrets=secrets)
            if pool.from_file:
                client.read_account_indices = tuple(pool.all)
            _, _, proof = await inspect_current(config, client, operator, require_flat=require_flat, pool=pool)
            journal = DurableJournal(Path(operator) / 'recovery-checks.jsonl')
            journal.acquire_attempt()
            try:
                journal.append('CURRENT_STATE_VERIFIED', persisted_proof(proof))
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


def close_pairs(nonflat, every):
    """Pair wallets holding a position; an odd one is paired with a flat wallet."""
    nonflat = list(nonflat)
    pairs = [tuple(nonflat[i:i + 2]) for i in range(0, len(nonflat) - 1, 2)]
    if len(nonflat) % 2:
        last = nonflat[-1]
        partner = next((index for index in every if index not in nonflat), None)
        if partner is None:
            partner = next(index for index in every if index != last)
        pairs.append((last, partner))
    return pairs


async def close_positions(config, client, operator, slot, *, clock=None, pool=None, pair_client_factory=None):
    """User-triggered closure only; caller owns the normal operator lock."""
    if not config.operator_execution_opt_in or not config.operator_plan_reviewed:
        raise PreflightBlocked('explicit operator closure confirmation required')
    if pool is not None and pool.from_file:
        return await _close_pool_positions(config, client, operator, slot, pool, clock=clock,
                                           pair_client_factory=pair_client_factory)
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
            journal.append('CLOSE_BASELINE', persisted_proof(proof))
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


async def _close_pool_positions(config, client, operator, slot, pool, *, clock=None, pair_client_factory=None):
    """Close every pool wallet's residual, one existing two-account loop per pair.

    Mutations only ever use a client bound to exactly that pair.  Each pair is
    read fresh right before its closure; the first pair whose outcome is not
    proved stops the operation (later pairs stay untouched for the next check).
    """
    engine = RandomCycleEngine(client, clock=clock)
    config = replace(config, cycle_dir=slot, journal_path=Path(slot) / 'close.jsonl')
    journal = DurableJournal(config.journal_path, clock=engine.clock.now)
    journal.acquire_attempt()
    try:
        if journal.events:
            raise PreflightBlocked('close slot cannot be replayed')
        journal.append('CLOSE_STARTED', {'binding': config.binding(), 'wallets': pool.as_dict(),
                                         'runtime_provenance': capture_provenance(config.binding())})
        results = []
        after = {}
        try:
            wallets, proof = await inspect_wallets(config, client, operator, pool, require_flat=False,
                                                   clock=engine.clock, journal=journal)
            journal.append('CLOSE_BASELINE', persisted_proof(proof))
            after = {index: wallets[index].signed_position for index in pool.all}
            nonflat = [index for index in pool.all if wallets[index].signed_position != 0]
            resolved = True
            for first, second in close_pairs(nonflat, pool.all):
                pair_config = replace(config, source_account_index=first, receiver_account_index=second)
                bound = (client.source_account_index, client.receiver_account_index) == (first, second)
                if not bound and pair_client_factory is None:
                    raise PreflightBlocked('pool closure requires a per-pair client')
                pair_client = client if bound else pair_client_factory(pair_config)
                try:
                    pair_engine = RandomCycleEngine(pair_client, clock=engine.clock)
                    journal.append('CLOSE_PAIR', {'source_account_index': first, 'receiver_account_index': second})
                    source, receiver = await pair_engine._recovery_accounts(pair_config, journal)
                    pair_results, first_after, second_after = await pair_engine.close_reconciled_positions(
                        pair_config, journal, source, receiver)
                finally:
                    if pair_client is not client:
                        await pair_client.aclose()
                results.extend(pair_results)
                after[first], after[second] = first_after, second_after
                if (first_after is None or second_after is None
                        or any(item.outcome is Outcome.UNKNOWN for item in pair_results)):
                    resolved = False
                    break  # Unproved outcome: no further pair is touched.
            flat = resolved and all(value == 0 for value in after.values())
            result = {'status': 'CONFIRMED_FLAT' if flat else 'PARTIAL' if resolved else 'UNKNOWN',
                      'at': engine.clock.now(), 'symbol': config.market_symbol,
                      'positions': [{'account_index': index,
                                     'position': None if after.get(index) is None else str(after[index])}
                                    for index in pool.all],
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
