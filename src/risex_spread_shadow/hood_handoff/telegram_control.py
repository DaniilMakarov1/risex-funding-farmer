"""Owner-operated Telegram control. Import, help and tests are offline.

Only the owner's fresh private /run or /close message requests execution.
Starting the controller itself never submits orders.
"""
from __future__ import annotations

import argparse
import copy
import asyncio
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import sys
import time
import uuid
from decimal import Decimal, InvalidOperation

import aiohttp

from .keychain import MacOSKeychainBackend, read_hidden_secret
from .offline_report import load_saved_cycle_report
from .operator_view import read_lifecycle, read_execution_notices, read_launch_failure
from .operator_control import exclusive_lock
from . import telegram_messages as views

MAX_COMMAND_FUTURE_SKEW_SECONDS = 5


@dataclass(frozen=True)
class BotBinding:
    @property
    def record_account(self):
        return 'hood-telegram-control-v1'


def integer(value):
    return isinstance(value, int) and not isinstance(value, bool)


def validate_token(value):
    if not isinstance(value, str) or not re.fullmatch(r'[0-9]{1,20}:[A-Za-z0-9_-]{20,256}', value):
        raise RuntimeError('invalid Telegram bot token')
    return value


class Store:
    def __init__(self, directory: Path, binding: str):
        directory.mkdir(mode=0o700, exist_ok=True)
        info = directory.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise RuntimeError('controller directory must be owner-only')
        self.directory = directory
        self.path = directory / 'state.json'
        self.data = {'schema': 1, 'binding': binding, 'offset': 0, 'active': None, 'last': None}
        if self.path.exists() or self.path.is_symlink():
            fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(fd) as handle:
                info = os.fstat(handle.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
                    raise RuntimeError('unsafe controller state')
                self.data = json.load(handle)
            if (not isinstance(self.data, dict) or self.data.get('schema') != 1
                or self.data.get('binding') != binding or not integer(self.data.get('offset'))
                or self.data['offset'] < 0 or 'active' not in self.data or 'last' not in self.data):
                raise RuntimeError('controller state/configuration mismatch; inspect locally')
            active = self.data['active']
            if active is not None and (not isinstance(active, dict) or not isinstance(active.get('before'), list)
                or active.get('action', 'run') not in ('run', 'close')
                or not all(isinstance(n, str) and re.fullmatch(r'(?:cycle|close)-[0-9]+', n) for n in active['before'])):
                raise RuntimeError('invalid active operation state')
            if active is not None and ('phase' in active or 'auto_close_before' in active):
                close_before = active.get('auto_close_before')
                if (active.get('action') != 'run' or active.get('phase') not in ('AUTO_CLOSE', 'RUN')
                        or not isinstance(close_before, list)
                        or not all(isinstance(n, str) and re.fullmatch(r'close-[0-9]+', n)
                                   for n in close_before)
                        or len(close_before) != len(set(close_before))
                        or (active.get('phase') == 'RUN' and not re.fullmatch(
                            r'close-[0-9]+', str(active.get('auto_close_slot'))))):
                    raise RuntimeError('invalid pre-cycle close state')
            if active is not None and ('series_total' in active or 'series_index' in active):
                total, index = active.get('series_total'), active.get('series_index')
                if (active.get('action') != 'run' or not integer(total) or not integer(index)
                        or not total >= 2 or not 1 <= index <= total):
                    raise RuntimeError('invalid active series state')
            last = self.data['last']
            if last is not None and (not isinstance(last, dict)
                or last.get('status') not in ('NOT_LAUNCHED', 'FINISHED', 'BLOCKED')
                or last.get('cycle') is not None and (not isinstance(last['cycle'], str)
                    or re.fullmatch(r'(?:cycle|close)-[0-9]+', last['cycle']) is None)):
                raise RuntimeError('invalid last operation state')
            if last is not None and ('series_total' in last or 'series_completed' in last):
                total, completed = last.get('series_total'), last.get('series_completed')
                if (not integer(total) or not integer(completed)
                        or not total >= 2 or not 0 <= completed <= total):
                    raise RuntimeError('invalid last series state')
            for record in (active, last):
                if record is None:
                    continue
                for field, prefix in (('series_slots', 'cycle'), ('series_precloses', 'close')):
                    if field not in record:
                        continue
                    names = record[field]
                    if (not integer(record.get('series_total')) or not isinstance(names, list)
                            or len(names) > record['series_total']
                            or not all(isinstance(n, str) and re.fullmatch(prefix + r'-[0-9]+', n) for n in names)
                            or len(set(names)) != len(names)):
                        raise RuntimeError('invalid series report membership')

    def save(self):
        temporary = self.directory / f'.state-{uuid.uuid4().hex}'
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'w') as handle:
            json.dump(self.data, handle, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, self.path)
        directory_fd = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)


class Telegram:
    def __init__(self, session, token):
        self.session = session
        self.token = validate_token(token)

    async def call(self, method, **payload):
        if method not in {'getUpdates', 'sendMessage'}:
            raise RuntimeError('unsupported bot method')
        try:
            async with self.session.post(f'https://api.telegram.org/bot{self.token}/{method}',
                                         json=payload, allow_redirects=False) as response:
                value = await response.json()
                if response.status != 200 or not isinstance(value, dict) or value.get('ok') is not True:
                    raise RuntimeError('Telegram request failed')
                return value['result']
        except (Exception, asyncio.TimeoutError):
            # aiohttp exception strings can include the credential-bearing URL.
            raise RuntimeError('Telegram transport unavailable') from None

    async def send(self, owner, text):
        # Never cut HTML inside an entity or tag. Views bound dynamic fields;
        # an unexpected oversized view degrades to a complete, valid message.
        if len(text.encode('utf-16-le')) // 2 > 4000:
            text = '<b>Сообщение слишком длинное</b>\nОткрой полный отчёт локально. /status — краткое состояние.'
        return await self.call('sendMessage', chat_id=owner, text=text, parse_mode='HTML',
                              protect_content=True, reply_markup=views.READ_MENU,
                              link_preview_options={'is_disabled': True})


class Controller:
    def __init__(self, owner, config, store, transport, launch, *, now=time.time, accounts=None, recovery=None, close=None):
        self.owner = owner
        self.config = config
        self.operator = config.parent
        self.store = store
        self.transport = transport
        self.launch = launch
        self.accounts = accounts
        self.recovery = recovery
        self.close = close
        self._checking = False
        self.now = now
        self.started = now()
        self.task = None
        self._runner_finished = False
        self._notice_queue = asyncio.Queue(maxsize=64)
        self._notice_task = None

    def slots(self, action='run'):
        prefix = 'close' if action == 'close' else 'cycle'
        return sorted((p.name for p in self.operator.glob(f'{prefix}-*') if p.is_dir()
                       and re.fullmatch(prefix + r'-[0-9]+', p.name)),
                      key=lambda name: int(name.split('-')[1]))

    def close_report(self, name):
        from .operator_recovery import load_close_result
        try:
            if not isinstance(name, str) or not re.fullmatch(r'close-[0-9]+', name) or (self.operator / name).is_symlink():
                return None
            return load_close_result(self.operator / name)
        except Exception:
            return None

    def report(self, name):
        if not isinstance(name, str) or not re.fullmatch(r'cycle-[0-9]+', name):
            return None
        path = self.operator / name
        if path.is_symlink():
            return None
        try:
            return load_saved_cycle_report(path)
        except Exception:
            return None  # No raw exception/credential-bearing payload in chat.

    def later_close(self, report):
        """Return the latest same-market close started after this cycle's terminal event."""
        from .operator_recovery import journal_rows
        cycle = report.get('cycle', {})
        binding = report.get('binding', {})
        terminal = cycle.get('terminal_at') if isinstance(cycle, dict) else None
        if (report.get('status') != 'COMPLETE' or isinstance(terminal, bool)
                or not isinstance(terminal, (int, float)) or not math.isfinite(terminal)
                or not isinstance(binding, dict)):
            return None
        accounts = (binding.get('source_account_index'), binding.get('receiver_account_index'))
        if not all(type(index) is int for index in accounts) or accounts[0] == accounts[1]:
            return None
        selected = None
        for name in self.slots('close'):
            slot = self.operator / name
            if slot.is_symlink():
                continue
            first = last_row = None
            try:
                for row in journal_rows(slot / 'close.jsonl'):
                    if first is None:
                        first = row
                    last_row = row
            except Exception:
                continue
            if first is None or first['event'] != 'CLOSE_STARTED':
                continue
            close_binding = first['payload'].get('binding')
            if (not isinstance(close_binding, dict)
                    or close_binding.get('market_id') != binding.get('market_id')
                    or close_binding.get('market_symbol') != binding.get('market_symbol')
                    or type(close_binding.get('source_account_index')) is not int
                    or type(close_binding.get('receiver_account_index')) is not int
                    or {close_binding['source_account_index'], close_binding['receiver_account_index']} != set(accounts)):
                continue
            started = first['at']
            if started <= terminal or selected is not None and started <= selected[0]:
                continue
            result = last_row['payload'] if last_row['event'] == 'CLOSE_COMPLETE' else None
            flat = False
            if isinstance(result, dict) and result.get('status') == 'CONFIRMED_FLAT' and result.get('symbol') == binding.get('market_symbol'):
                positions = result.get('positions')
                if isinstance(positions, list) and len(positions) == 2:
                    try:
                        values = {row['account_index']: Decimal(row['position']) for row in positions
                                  if isinstance(row, dict) and type(row.get('account_index')) is int}
                        flat = set(values) == set(accounts) and len(values) == 2 and all(
                            value.is_finite() and value == 0 for value in values.values())
                    except (KeyError, TypeError, ValueError, InvalidOperation):
                        pass
            selected = (started, name, last_row['at'], flat)
        return selected

    def finish(self, *, retain_active=False):
        active = self.store.data['active']
        if active is None:
            return False
        action = active.get('action', 'run')
        added = sorted(set(self.slots(action)) - set(active['before']))
        safe_cycle = False
        if not added:
            close_before = active.get('auto_close_before') if action == 'run' else None
            close_added = (sorted(set(self.slots('close')) - set(close_before))
                           if isinstance(close_before, list) else [])
            if len(close_added) == 1:
                report = self.close_report(close_added[0])
                safe = (report is not None and report.get('status') == 'CONFIRMED_FLAT'
                        and (active.get('phase') != 'RUN'
                             or active.get('auto_close_slot') == close_added[0]))
                self.store.data['last'] = {'status': 'NOT_LAUNCHED' if safe else 'BLOCKED',
                                           'cycle': close_added[0]}
                if safe:
                    self.store.data['active'] = None
            elif close_added or close_before is not None:
                self.store.data['last'] = {'status': 'BLOCKED', 'cycle': None}
            else:
                previous = self.store.data['last']
                previous_cycle = (previous.get('cycle') if 'series_total' in active
                                  and isinstance(previous, dict)
                                  and previous.get('status') == 'FINISHED'
                                  and previous.get('series_total') == active['series_total']
                                  and previous.get('series_completed') == active['series_index'] - 1
                                  and isinstance(previous.get('cycle'), str)
                                  and previous['cycle'].startswith('cycle-')
                                  else None)
                self.store.data['last'] = {'status': 'NOT_LAUNCHED', 'cycle': previous_cycle}
                self.store.data['active'] = None
        elif len(added) == 1:
            report = self.report(added[0])
            safe = (report is not None and report['status'] == 'COMPLETE'
                    and report['inventory']['status'] == 'CONFIRMED_FLAT'
                    and not report['order_state']['unresolved_intents']
                    and not report['order_state']['unresolved_observed_orders'])
            if action == 'close':
                report = self.close_report(added[0])
                safe = report is not None and report.get('status') == 'CONFIRMED_FLAT'
            self.store.data['last'] = {'status': 'FINISHED' if safe else 'BLOCKED', 'cycle': added[0]}
            safe_cycle = bool(safe and action == 'run')
            if safe and not (retain_active and safe_cycle):
                self.store.data['active'] = None
        else:
            self.store.data['last'] = {'status': 'BLOCKED', 'cycle': None}
        if action == 'run' and 'series_total' in active:
            self._record_series_slot(active, added)
            self.store.data['last'].update({
                'series_total': active['series_total'],
                'series_completed': active['series_index'] if safe_cycle else active['series_index'] - 1,
                **self._series_membership(active),
            })
        try:
            self.store.save()
        except Exception:
            # A failed terminal checkpoint must not clear the in-memory barrier.
            self.store.data['active'] = active
            raise
        return safe_cycle

    @staticmethod
    def _series_membership(active):
        return {key: list(active[key]) for key in ('series_slots', 'series_precloses') if key in active}

    @staticmethod
    def _record_series_slot(active, added):
        # Older saved commands have no membership ledger. Never infer theirs.
        names = active.get('series_slots')
        if isinstance(names, list) and len(added) == 1 and added[0] not in names:
            names.append(added[0])

    def queue_series_report(self):
        last = self.store.data.get('last')
        if isinstance(last, dict) and 'series_total' in last:
            self.queue_notice(copy.deepcopy(last))
            return True
        return False

    def _series_prefix(self):
        active, last = self.store.data['active'], self.store.data['last']
        if active is not None and 'series_total' in active:
            total, index = active['series_total'], active['series_index']
            if self.task is not None and not self.task.done() and not self._runner_finished:
                return f'<b>Серия: цикл {index}/{total}</b> · завершено {index - 1}\n'
            completed = last.get('series_completed', index - 1) if isinstance(last, dict) else index - 1
            return f'<b>Серия остановлена: {completed}/{total} циклов завершено</b>\n'
        if active is not None:
            return ''
        if isinstance(last, dict) and 'series_total' in last:
            completed, total = last['series_completed'], last['series_total']
            result = 'завершена' if completed == total else 'остановлена'
            return f'<b>Серия {result}: {completed}/{total} циклов завершено</b>\n'
        return ''

    def summary(self, *, detailed=False):
        if self._checking:
            return '<b>Проверяю текущие позиции и старые ордера</b>\nНовая операция ещё не отправлялась.'
        active = self.store.data['active']
        prefix = self._series_prefix()
        # finish() clears active before awaiting final notification; the task
        # can still be alive during that await. It is no longer a running cycle.
        if active is not None and self.task is not None and not self.task.done() and not self._runner_finished:
            added = sorted(set(self.slots(active.get('action', 'run'))) - set(active['before']))
            if active.get('action') == 'close' or active.get('phase') == 'AUTO_CLOSE':
                return prefix + '<b>⏳ Закрытие позиций выполняется</b>\nПроверяю и закрываю остатки на двух настроенных счетах. /status — состояние'
            progress = read_lifecycle(self.operator / added[0] / 'cycle.jsonl') if len(added) == 1 else None
            return prefix + views.running_message(added, progress)
        blocked = active is not None
        last = self.store.data['last']
        if last and str(last.get('cycle')).startswith('close-'):
            message = views.close_message(last['cycle'], self.close_report(last['cycle']))
            if last.get('status') == 'NOT_LAUNCHED' or blocked and active.get('action') == 'run':
                message += '\n<b>⚠️ Новый цикл не начался</b> · требуется свежая проверка перед следующей командой.'
            return prefix + message
        # Read-only display also includes completed terminal-launched cycles.
        # This never clears the controller's durable active/restart barrier.
        if not last or last.get('status') != 'NOT_LAUNCHED':
            slots = self.slots()
            if slots:
                last = {'cycle': slots[-1]}
        if not last:
            return prefix + views.empty_message(blocked)
        failure_code = (read_launch_failure(self.operator / last['cycle'])
                        if isinstance(last.get('cycle'), str) else None)
        if failure_code is not None:
            return prefix + views.launch_failure_message(last['cycle'], failure_code, blocked=blocked)
        report = self.report(last.get('cycle'))
        if report is None:
            return prefix + views.unavailable_message(blocked, last.get('status') == 'NOT_LAUNCHED')
        checkpoint = views.recovery_checkpoint(self.store.data.get('last_recovery')) if not blocked else ''
        message = checkpoint + views.saved_message(last['cycle'], report, blocked=blocked, detailed=detailed)
        later = self.later_close(report)
        if later is not None:
            message += views.later_close_message(later[1], later[2], later[3])
        return prefix + message

    async def notify(self, text):
        try:
            await self.transport.send(self.owner, text)
        except Exception:
            # Never print exception text: a transport URL can contain the token.
            print('Telegram notification delivery failed; command will not be replayed.', file=sys.stderr, flush=True)

    def queue_notice(self, message):
        """Bounded best-effort display only; never awaited by cycle sequencing."""
        if self._notice_queue.full():
            self._notice_queue.get_nowait()
        self._notice_queue.put_nowait(message)
        if self._notice_task is None or self._notice_task.done():
            self._notice_task = asyncio.create_task(self._deliver_notices())

    async def _deliver_notices(self):
        while not self._notice_queue.empty():
            message = self._notice_queue.get_nowait()
            if isinstance(message, dict):
                from .telegram_series_report import report_pages
                pages = report_pages(message, self.report)
                while True:
                    page = await asyncio.to_thread(next, pages, None)
                    if page is None:
                        break
                    await self._deliver_notice(page)
                continue
            await self._deliver_notice(message)

    async def _deliver_notice(self, message):
        try:
            async with asyncio.timeout(10):
                await self.notify(message)
        except asyncio.TimeoutError:
            print('Telegram progress delivery timed out; trading continues.', file=sys.stderr, flush=True)

    def cycle_notice(self, message):
        active = self.store.data.get('active') or {}
        index, total = active.get('series_index', 1), active.get('series_total', 1)
        return f'<b>Цикл {index}/{total}</b> · команда {active.get("update_id", "?")} · ' + message

    async def lifecycle_notices(self):
        """Finite stage and per-attempt execution notices, outside the child."""
        sent = set()
        current_step = None
        while True:
            try:
                active = self.store.data['active']
                if active is None:
                    return
                if active.get('action') == 'close':
                    return
                if active.get('series_index') != current_step:
                    current_step = active.get('series_index')
                    sent.clear()
                added = sorted(set(self.slots()) - set(active['before']))
                if len(added) == 1:
                    path = self.operator / added[0] / 'cycle.jsonl'
                    progress = await asyncio.to_thread(read_lifecycle, path)
                    for key, notice in await asyncio.to_thread(read_execution_notices, path):
                        if key not in sent:
                            sent.add(key)
                            self.queue_notice(self.cycle_notice(views.execution_message(notice)))
                    stage = progress.get('stage') if progress else None
                    if stage in {'HOLD', 'CLOSING', 'RECOVERY'} and stage not in sent:
                        sent.add(stage)
                        # Slow/unavailable delivery cannot hold up the child.
                        self.queue_notice(self.cycle_notice(views.running_message(added, progress)))
            except (Exception, asyncio.TimeoutError):
                pass
            await asyncio.sleep(0.5)

    async def run_one(self, action='run'):
        total = (self.store.data.get('active') or {}).get('series_total', 1)
        try:
            for step in range(1, total + 1):
                if step > 1:
                    await self._prepare_next_series_step()
                notices = asyncio.create_task(self.lifecycle_notices())
                try:
                    options = (self.store.data.get('active') or {}).get('launch_options', {})
                    if action == 'run' and (self.store.data.get('active') or {}).get('phase') == 'AUTO_CLOSE':
                        await self._close_before_run()
                    self.queue_notice(self.cycle_notice('Начинаю подготовку и открытие.') if action == 'run'
                                      else '<b>Закрытие</b> · Проверяю остатки; ордера только reduce-only.')
                    await (self.close() if action == 'close' else self.launch(**options))
                    label = self.cycle_notice('')
                    safe = self.finish(retain_active=step < total)
                    if action == 'run':
                        self.queue_notice(label + ('Завершён: нулевые позиции подтверждены. '
                            + ('Далее проверка следующего цикла.' if step < total else 'Это последний цикл; серия закончена.')
                            if safe else 'Остановлен: безопасное завершение не подтверждено. Продолжения серии нет.'))
                finally:
                    notices.cancel()
                    await asyncio.gather(notices, return_exceptions=True)
                if step < total and not safe:
                    break
        except Exception:
            active = self.store.data.get('active') or {}
            close_before = active.get('auto_close_before') if active.get('phase') == 'AUTO_CLOSE' else None
            close_added = (sorted(set(self.slots('close')) - set(close_before))
                           if isinstance(close_before, list) else [])
            cycle_added = sorted(set(self.slots('run')) - set(active.get('before', [])))
            previous = self.store.data.get('last')
            self.store.data['last'] = {'status': 'BLOCKED',
                                       'cycle': (close_added[0] if len(close_added) == 1 else
                                                 cycle_added[0] if len(cycle_added) == 1 else
                                                 previous.get('cycle') if isinstance(previous, dict)
                                                 and 'series_total' in active else None)}
            if 'series_total' in active:
                self._record_series_slot(active, cycle_added)
                self.store.data['last'].update({'series_total': active['series_total'],
                                                'series_completed': active['series_index'] - 1,
                                                **self._series_membership(active)})
            self.store.save()  # Keep durable active intent; never automatically retry.
        self._runner_finished = True
        if not self.queue_series_report():
            self.queue_notice(self.summary_after_task())

    async def _prepare_next_series_step(self):
        """Durably claim the next step, then require fresh proof before its child."""
        active = self.store.data['active']
        active['series_index'] += 1
        active['before'] = self.slots('run')
        for field in ('phase', 'auto_close_before', 'auto_close_slot'):
            active.pop(field, None)
        self.store.save()
        if self.recovery is None:
            raise RuntimeError('series recovery is unavailable')
        self.queue_notice(self.cycle_notice('Проверяю позиции и старые ордера перед следующим открытием.'))
        proof = await self.recovery(require_flat=False)
        if not isinstance(proof, dict) or proof.get('status') != 'CLOSE_READY':
            raise RuntimeError('next cycle close readiness is unproved')
        positions = self._exact_recovery_positions(proof)
        if any(position != 0 for position in positions):
            if self.close is None:
                raise RuntimeError('next cycle close is unavailable')
            active['phase'] = 'AUTO_CLOSE'
            active['auto_close_before'] = self.slots('close')
            self.store.data['last_recovery'] = {
                k: proof.get(k) for k in ('status', 'at', 'previous_intents')}
            self.store.save()
            return
        ready = await self.recovery(require_flat=True)
        if (not isinstance(ready, dict) or ready.get('status') != 'READY'
                or any(position != 0 for position in self._exact_recovery_positions(ready))):
            raise RuntimeError('next cycle flatness is unproved')
        self.store.data['last_recovery'] = {
            k: ready.get(k) for k in ('status', 'at', 'previous_intents')}
        self.store.save()

    @staticmethod
    def _exact_recovery_positions(proof):
        """Require finite exact positions and the validated no-order account shape."""
        positions = []
        identities = []
        for role in ('source', 'receiver'):
            row = proof.get(role) if isinstance(proof, dict) else None
            if (not isinstance(row, dict) or type(row.get('account_index')) is not int
                    or type(row.get('market_id')) is not int or row.get('active_orders') != []
                    or row.get('authorized') is not True or row.get('ready') is not True
                    or not isinstance(row.get('signed_position'), str)):
                raise RuntimeError('current account proof is incomplete')
            try:
                position = Decimal(row['signed_position'])
            except (InvalidOperation, ValueError):
                raise RuntimeError('current position is invalid') from None
            if not position.is_finite():
                raise RuntimeError('current position is invalid')
            positions.append(position)
            identities.append((row['account_index'], row['market_id']))
        if identities[0][0] == identities[1][0] or identities[0][1] != identities[1][1]:
            raise RuntimeError('current account identities conflict')
        return positions

    async def _close_before_run(self):
        """One saved /run may close once; never advance on a missing terminal or read."""
        active = self.store.data['active']
        before = set(active['auto_close_before'])
        self.queue_notice(self.cycle_notice('Перед открытием закрываю существующий остаток reduce-only.'))
        await self.close()
        added = sorted(set(self.slots('close')) - before)
        if len(added) != 1:
            raise RuntimeError('pre-cycle close slot is ambiguous')
        result = self.close_report(added[0])
        if result is None or result.get('status') != 'CONFIRMED_FLAT':
            raise RuntimeError('pre-cycle close is not confirmed flat')
        proof = await self.recovery(require_flat=True)
        if (not isinstance(proof, dict) or proof.get('status') != 'READY'
                or any(position != 0 for position in self._exact_recovery_positions(proof))):
            raise RuntimeError('fresh post-close flatness is unproved')
        self.store.data['last_recovery'] = {k: proof.get(k) for k in ('status', 'at', 'previous_intents')}
        active['phase'] = 'RUN'
        active['auto_close_slot'] = added[0]
        if 'series_precloses' in active and added[0] not in active['series_precloses']:
            active['series_precloses'].append(added[0])
        self.store.save()

    async def reconcile_idle(self, *, require_flat=True):
        """Replace a historical barrier only after a bounded current check."""
        if self.recovery is None:
            return False
        proof = await self.recovery(require_flat=require_flat)
        expected = 'READY' if require_flat else 'CLOSE_READY'
        if not isinstance(proof, dict) or proof.get('status') != expected:
            raise RuntimeError('current readiness is unproved')
        self.store.data['last_recovery'] = {k: proof.get(k) for k in ('status', 'at', 'previous_intents')}
        self.store.data['active'] = None
        self.store.save()
        return True

    async def admit(self, action, uid, launch_options=None, series_total=1):
        self._checking = True
        try:
            auto_close_before = None
            if (not integer(series_total) or series_total < 1
                    or action != 'run' and series_total != 1):
                raise RuntimeError('invalid series count')
            if action == 'run' and series_total > 1 and (self.recovery is None or self.close is None):
                raise RuntimeError('series recovery or close is unavailable')
            if self.recovery is not None:
                if action == 'run' and self.close is not None:
                    proof = await self.recovery(require_flat=False)
                    if not isinstance(proof, dict) or proof.get('status') != 'CLOSE_READY':
                        raise RuntimeError('pre-cycle close readiness is unproved')
                    positions = self._exact_recovery_positions(proof)
                    if all(position == 0 for position in positions):
                        await self.reconcile_idle(require_flat=True)
                    else:
                        auto_close_before = self.slots('close')
                        self.store.data['last_recovery'] = {
                            k: proof.get(k) for k in ('status', 'at', 'previous_intents')}
                else:
                    await self.reconcile_idle(require_flat=action == 'run')
            elif self.store.data['active'] is not None:
                await self.notify(views.blocked_message())
                return
            self.store.data['active'] = {'before': self.slots(action), 'update_id': uid, 'action': action}
            if action == 'run' and series_total > 1:
                self.store.data['active'].update({'series_total': series_total, 'series_index': 1,
                                                   'series_slots': [], 'series_precloses': []})
            if auto_close_before is not None:
                self.store.data['active'].update({'phase': 'AUTO_CLOSE',
                                                  'auto_close_before': auto_close_before})
            if launch_options:
                self.store.data['active']['launch_options'] = dict(launch_options)
            self.store.save()
        except Exception as exc:
            from .contracts import PreflightBlocked
            reason = str(exc) if isinstance(exc, PreflightBlocked) else 'Проверка недоступна или другая операция ещё выполняется.'
            await self.notify(views.recovery_refused_message(reason))
            return
        finally:
            self._checking = False
        await self.run_one(action)

    def summary_after_task(self):
        task = self.task
        self.task = None
        try:
            return self.summary()
        finally:
            self.task = task

    async def handle(self, update):
        if not isinstance(update, dict) or not integer(update.get('update_id')):
            return
        uid = update['update_id']
        if uid < self.store.data['offset']:
            return
        # Persist consumption BEFORE any launch or outbound response.
        self.store.data['offset'] = uid + 1
        self.store.save()
        message = update.get('message')
        if not isinstance(message, dict):
            return
        sender, chat = message.get('from', {}), message.get('chat', {})
        if not isinstance(sender, dict) or not isinstance(chat, dict):
            return
        if (not integer(sender.get('id')) or sender['id'] != self.owner or sender.get('is_bot') is not False
            or chat.get('type') != 'private' or not integer(chat.get('id')) or chat['id'] != self.owner
            or any(k in message for k in ('forward_origin', 'forward_date', 'via_bot', 'sender_chat'))):
            return
        date = message.get('date')
        if (not integer(date) or date < self.started
                or not -MAX_COMMAND_FUTURE_SKEW_SECONDS <= self.now() - date <= 120):
            print('Telegram owner command rejected: timestamp outside allowed window.', file=sys.stderr, flush=True)
            await self.notify('Команда не выполнена: её время вне допустимого окна. '
                              'Отправьте новую команду. Если отказ повторится, проверьте часы компьютера.')
            return
        command = message.get('text')
        launch_options = {}
        series_total = 1
        if isinstance(command, str) and len(command) <= 4096:
            count_match = re.fullmatch(r'/run ([1-9][0-9]*)', command)
            option_match = re.fullmatch(r'/run (ws|ack)(?: ([1-5])(?: ([1-9][0-9]*))?)?', command)
            if count_match:
                series_total = int(count_match[1])
                command = '/run'
            elif option_match:
                launch_options['receiver_admission'] = 'ws_confirmed' if option_match[1] == 'ws' else 'ack'
                if option_match[2]:
                    launch_options['price_improvement_ticks'] = int(option_match[2])
                if option_match[3]:
                    series_total = int(option_match[3])
                command = '/run'
        if command in ('/start', '/help'):
            await self.notify(views.help_message())
        elif command in ('/status', '/report'):
            if command == '/report' and not (self.task is not None and not self.task.done()) and self.queue_series_report():
                return
            await self.notify(self.summary(detailed=command == '/report'))
        elif command == '/accounts':
            try:
                result = await self.accounts() if self.accounts is not None else None
                response = views.accounts_message(result)
            except Exception:
                response = views.accounts_message(None)
            await self.notify(response)
        elif command in ('/run', '/close'):
            if self.task is not None and not self.task.done():
                await self.notify(views.blocked_message())
                return
            if command == '/close' and self.close is None:
                await self.notify(views.recovery_refused_message('Команда закрытия не настроена.'))
                return
            if self.recovery is None and self.store.data['active'] is not None:
                await self.notify(views.blocked_message())
                return
            self._runner_finished = False
            self._checking = True
            self.task = asyncio.create_task(self.admit(command[1:], uid, launch_options, series_total))
            try:
                selected = json.loads(self.config.read_text())
                selected.update(launch_options)
                mode = selected.get('receiver_admission', 'strict')
                ticks = selected.get('price_improvement_ticks', '1 (старое правило)')
                details = f"\nРежим: {mode}; улучшение цены: {ticks} тиков."
                if mode == 'ack':
                    details += " MARKET без ожидания WS лимитки; её наличие не подтверждено. Проверка стакана сохранена."
            except Exception:
                details = ''
            await self.notify((views.accepted_message(uid, series_total) + details)
                              if command == '/run' else views.close_accepted_message(uid))
        elif isinstance(command, str):
            await self.notify(views.unknown_message())


async def serve(args, store, lock_fd, token):
    config = args.config.resolve()
    root = Path(__file__).resolve().parents[3]
    python = root / '.venv-hood/bin/python'
    if not python.is_file():
        raise RuntimeError('project .venv-hood is missing')

    initial_config = config.read_bytes()
    from .cli import _load_json, _simple_evidence_path
    evidence = _simple_evidence_path(_load_json(config, "controller config"), config.parent, None)
    initial_evidence = evidence.read_bytes()

    async def launch(action='simple', *, receiver_admission=None, price_improvement_ticks=None):
        if config.read_bytes() != initial_config or evidence.read_bytes() != initial_evidence:
            raise RuntimeError('configuration changed; restart controller after local review')
        # Fixed executable/arguments, no shell and no user-supplied command text.
        env = dict(os.environ)
        env.pop('RISEX_HOOD_OPERATOR_DIR', None)
        env.pop('RISEX_HOOD_CONFIG', None)
        env['PYTHONPATH'] = str(root / 'src')
        env['RISEX_HOOD_OPERATOR_INTERFACE'] = 'telegram'
        flags = []
        if receiver_admission is not None:
            if receiver_admission not in ('ws_confirmed', 'ack'):
                raise RuntimeError('invalid receiver admission override')
            flags += ['--receiver-admission', receiver_admission]
        if price_improvement_ticks is not None:
            if type(price_improvement_ticks) is not int or not 1 <= price_improvement_ticks <= 5:
                raise RuntimeError('invalid tick override')
            flags += ['--price-improvement-ticks', str(price_improvement_ticks)]
        process = await asyncio.create_subprocess_exec(
            str(python), '-m', 'risex_spread_shadow.hood_handoff.cli', action,
            '--keychain', '--no-progress', '--config', str(config), *flags, cwd=str(root), env=env,
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL, start_new_session=True, pass_fds=(lock_fd,))
        # The owner's /run is the launcher confirmation. No credential bytes
        # enter stdin, argv, environment, output logs or Telegram.
        await process.communicate(b'\n')

    async def close():
        await launch('close-positions')

    async def recovery(*, require_flat=True):
        if config.read_bytes() != initial_config or evidence.read_bytes() != initial_evidence:
            raise RuntimeError('configuration changed')
        from .cli import _validate_simple_local_inputs
        from .operator_recovery import check_recovery
        try:
            account_config, _ = _validate_simple_local_inputs(json.loads(initial_config), config_path=config,
                operator_dir=config.parent, evidence_path=evidence, defer_incremental_margin_calculation=None)
        except SystemExit:
            raise RuntimeError('account configuration unavailable') from None
        return await check_recovery(account_config, config.parent, require_flat=require_flat)

    async def accounts():
        if config.read_bytes() != initial_config or evidence.read_bytes() != initial_evidence:
            raise RuntimeError('configuration changed')
        from .cli import _validate_simple_local_inputs
        from .telegram_accounts import read_accounts
        try:
            account_config, _ = _validate_simple_local_inputs(
                json.loads(initial_config), config_path=config,
                operator_dir=config.parent, evidence_path=evidence,
                defer_incremental_margin_calculation=None)
        except SystemExit:
            raise RuntimeError('account configuration unavailable') from None
        return await read_accounts(account_config)

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=40)) as session:
        api = Telegram(session, token)
        controller = Controller(args.owner_id, config, store, api, launch, recovery=recovery, close=close)
        controller.accounts = accounts
        # Obtaining the instance lock proves no inherited runner still holds it.
        controller.finish()
        if store.data['active'] is not None:
            try:
                # An interrupted /close can be retried against proved residual
                # inventory; /run still requires both accounts to be flat.
                require_flat = store.data['active'].get('action', 'run') != 'close'
                await controller.reconcile_idle(require_flat=require_flat)
            except Exception:
                pass  # Fresh commands retry the check; no automatic order.
        latest = await api.call('getUpdates', offset=-1, timeout=0, allowed_updates=['message'])
        if not isinstance(latest, list):
            raise RuntimeError('invalid Telegram update envelope')
        if latest:
            ids = [u['update_id'] for u in latest if isinstance(u, dict) and integer(u.get('update_id'))]
            if ids:
                store.data['offset'] = max(ids) + 1
                store.save()
        await controller.notify(views.startup_message())
        while True:
            try:
                updates = await api.call('getUpdates', offset=store.data['offset'], timeout=25,
                                         allowed_updates=['message'])
            except RuntimeError:
                await asyncio.sleep(3)
                continue
            if not isinstance(updates, list):
                raise RuntimeError('invalid Telegram update envelope')
            for update in updates:
                await controller.handle(update)


def main(argv=None):
    parser = argparse.ArgumentParser(description='Owner-operated Telegram control; no connection on help.')
    parser.add_argument('action', choices=['provision', 'run'])
    parser.add_argument('--owner-id', required=True, type=int)
    parser.add_argument('--config', required=True, type=Path)
    parser.add_argument('--replace-token', action='store_true')
    args = parser.parse_args(argv)
    try:
        if args.owner_id <= 0:
            raise RuntimeError('owner ID must be positive')
        config = args.config.resolve(strict=True)
        # Reuse local preflight validation without credentials, SDK or requests.
        from .cli import _load_json, _simple_evidence_path, _validate_simple_local_inputs
        value = _load_json(config, 'Telegram cycle configuration')
        evidence = _simple_evidence_path(value, config.parent, None)
        _validate_simple_local_inputs(value, config_path=config, operator_dir=config.parent,
                                      evidence_path=evidence, defer_incremental_margin_calculation=None)
        binding = hashlib.sha256((str(config) + str(args.owner_id)).encode() + config.read_bytes() + evidence.read_bytes()).hexdigest()
        directory = Path.home() / '.config/risex-spread-shadow/telegram-control'
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        with exclusive_lock(directory / 'instance.lock') as lock_fd:
            store = Store(directory, binding)
            backend = MacOSKeychainBackend()
            key = BotBinding()
            if args.action == 'provision':
                token = validate_token(read_hidden_secret('Telegram bot token (hidden): '))
                backend.put(key, token, replace=args.replace_token)
                store.save()
                print('Bot token stored in Keychain; no network or trading started.')
            else:
                if args.replace_token:
                    raise RuntimeError('--replace-token is only for provision')
                token = validate_token(backend.get(key))
                asyncio.run(serve(args, store, lock_fd, token))
        return 0
    except KeyboardInterrupt:
        print('Controller stopped; a detached cycle may still be running. Do not replay it.')
        return 2
    except BaseException:
        print('Controller unavailable; inspect local configuration, lock and saved state. No automatic retry.', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
