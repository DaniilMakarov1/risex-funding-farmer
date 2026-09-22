"""Owner-operated Telegram control. Import, help and tests are offline.

Only the owner's fresh private /run message requests a cycle. This module is
not started by the coding agent; the operator provisions and runs it locally.
"""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import time
import uuid

import aiohttp

from .keychain import MacOSKeychainBackend, read_hidden_secret
from .offline_report import load_saved_cycle_report
from .operator_control import exclusive_lock
from . import telegram_messages as views


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
                or not all(isinstance(n, str) and re.fullmatch(r'cycle-[0-9]+', n) for n in active['before'])):
                raise RuntimeError('invalid active operation state')
            last = self.data['last']
            if last is not None and (not isinstance(last, dict)
                or last.get('status') not in ('NOT_LAUNCHED', 'FINISHED', 'BLOCKED')
                or last.get('cycle') is not None and (not isinstance(last['cycle'], str)
                    or re.fullmatch(r'cycle-[0-9]+', last['cycle']) is None)):
                raise RuntimeError('invalid last operation state')

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
    def __init__(self, owner, config, store, transport, launch, *, now=time.time):
        self.owner = owner
        self.config = config
        self.operator = config.parent
        self.store = store
        self.transport = transport
        self.launch = launch
        self.now = now
        self.started = now()
        self.task = None
        self._runner_finished = False

    def slots(self):
        return sorted(p.name for p in self.operator.glob('cycle-*') if p.is_dir() and re.fullmatch(r'cycle-[0-9]+', p.name))

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

    def finish(self):
        active = self.store.data['active']
        if active is None:
            return
        added = sorted(set(self.slots()) - set(active['before']))
        if not added:
            self.store.data['last'] = {'status': 'NOT_LAUNCHED', 'cycle': None}
            self.store.data['active'] = None
        elif len(added) == 1:
            report = self.report(added[0])
            safe = (report is not None and report['status'] == 'COMPLETE'
                    and report['inventory']['status'] == 'CONFIRMED_FLAT'
                    and not report['order_state']['unresolved_intents']
                    and not report['order_state']['unresolved_observed_orders'])
            self.store.data['last'] = {'status': 'FINISHED' if safe else 'BLOCKED', 'cycle': added[0]}
            if safe:
                self.store.data['active'] = None
        else:
            self.store.data['last'] = {'status': 'BLOCKED', 'cycle': None}
        self.store.save()

    def summary(self, *, detailed=False):
        active = self.store.data['active']
        # finish() clears active before awaiting final notification; the task
        # can still be alive during that await. It is no longer a running cycle.
        if active is not None and self.task is not None and not self.task.done() and not self._runner_finished:
            added = sorted(set(self.slots()) - set(active['before']))
            return views.running_message(added)
        blocked = active is not None
        last = self.store.data['last']
        if not last:
            return views.empty_message(blocked)
        report = self.report(last.get('cycle'))
        if report is None:
            return views.unavailable_message(blocked, last.get('status') == 'NOT_LAUNCHED')
        return views.saved_message(last['cycle'], report, blocked=blocked, detailed=detailed)

    async def notify(self, text):
        try:
            await self.transport.send(self.owner, text)
        except Exception:
            pass  # Delivery failure never repeats or aborts a cycle.

    async def run_one(self):
        try:
            await self.launch()
            self.finish()
        except Exception:
            self.store.data['last'] = {'status': 'BLOCKED', 'cycle': None}
            self.store.save()  # Keep durable active intent; never automatically retry.
        self._runner_finished = True
        await self.notify(self.summary_after_task())

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
        if not integer(date) or date < self.started or not 0 <= self.now() - date <= 120:
            return
        command = message.get('text')
        if command in ('/start', '/help'):
            await self.notify(views.help_message())
        elif command in ('/status', '/report'):
            await self.notify(self.summary(detailed=command == '/report'))
        elif command == '/run':
            if self.store.data['active'] is not None or self.task is not None and not self.task.done():
                await self.notify(views.blocked_message())
                return
            self.store.data['active'] = {'before': self.slots(), 'update_id': uid}
            self.store.save()
            self._runner_finished = False
            self.task = asyncio.create_task(self.run_one())
            await self.notify(views.accepted_message(uid))
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

    async def launch():
        if config.read_bytes() != initial_config or evidence.read_bytes() != initial_evidence:
            raise RuntimeError('configuration changed; restart controller after local review')
        # Fixed executable/arguments, no shell and no user-supplied command text.
        env = dict(os.environ)
        env.pop('RISEX_HOOD_OPERATOR_DIR', None)
        env.pop('RISEX_HOOD_CONFIG', None)
        env['PYTHONPATH'] = str(root / 'src')
        process = await asyncio.create_subprocess_exec(
            str(python), '-m', 'risex_spread_shadow.hood_handoff.cli', 'simple',
            '--keychain', '--config', str(config), cwd=str(root), env=env,
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL, start_new_session=True, pass_fds=(lock_fd,))
        # The owner's /run is the launcher confirmation. No credential bytes
        # enter stdin, argv, environment, output logs or Telegram.
        await process.communicate(b'\n')

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=40)) as session:
        api = Telegram(session, token)
        controller = Controller(args.owner_id, config, store, api, launch)
        # Obtaining the instance lock proves no inherited runner still holds it.
        controller.finish()
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
