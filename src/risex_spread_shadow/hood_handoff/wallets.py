"""Owner command for the wallet pool: list, add (with a verified key), pause, resume.

Adding a wallet never sends an order.  The owner types the wallet's API
private key into hidden terminal input; the key is first proved in memory by
one authenticated read-only account request for the configured API key index,
and only then stored in the native Keychain record the cycles already use.
A wallet with a position or open orders on the configured market is refused,
so the system never adopts inventory it did not create.
"""
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
import re
import sys
from typing import Any

from .keychain import (
    KeychainConflictError,
    KeychainError,
    KeychainSecretProvider,
    MacOSKeychainBackend,
    read_hidden_secret,
)
from .journal import sanitize_exception
from .wallet_pool import (
    WalletPoolError,
    load_wallet_pool,
    save_wallet_pool,
    updated_pool,
)

KEY_FORMAT = re.compile(r"(?:0x)?(?:[0-9a-fA-F]{2}){32,80}")


class _OneKey:
    """In-memory provider for one account/key index; used only for verification."""

    def __init__(self, account_index: int, api_key_index: int, value: str) -> None:
        self._identity = (account_index, api_key_index)
        self._value: str | None = value

    def private_key(self, account_index: int, api_key_index: int) -> str:
        if (account_index, api_key_index) != self._identity or self._value is None:
            raise KeychainError("verification key request does not match the wallet")
        return self._value

    def close(self) -> None:
        self._value = None


def _local_inputs(config_arg: Path | None):
    from .cli import (
        _load_json,
        _simple_config_path,
        _simple_evidence_path,
        _simple_operator_dir,
        _validate_simple_local_inputs,
    )

    config_path = _simple_config_path(config_arg)
    value = _load_json(config_path, "operator configuration")
    operator_dir = _simple_operator_dir(config_path)
    evidence_path = _simple_evidence_path(value, operator_dir, None)
    config, _ = _validate_simple_local_inputs(
        value, config_path=config_path, operator_dir=operator_dir,
        evidence_path=evidence_path, defer_incremental_margin_calculation=None)
    return config, operator_dir


def _account_index(raw: str) -> int:
    if not re.fullmatch(r"[1-9][0-9]{0,14}", raw or ""):
        raise SystemExit("Номер кошелька (account index) должен быть положительным целым числом.")
    return int(raw)


def _keys(config: Any, indices: tuple[int, ...], backend: Any = None) -> KeychainSecretProvider:
    from .telegram_accounts import missing_key

    return KeychainSecretProvider.from_config(
        config, indices, backend=backend if backend is not None else MacOSKeychainBackend(),
        prompt=missing_key)


def _update_pool(operator_dir: Path, config: Any, **change: int) -> Any:
    from .operator_control import exclusive_lock

    try:
        with exclusive_lock(Path(operator_dir) / ".wallets.lock"):
            pool = updated_pool(load_wallet_pool(operator_dir, config), **change)
            save_wallet_pool(operator_dir, pool)
            return pool
    except WalletPoolError as exc:
        raise SystemExit(f"Пул не изменён: {_pool_error_text(exc)}") from None
    except RuntimeError as exc:
        if "another operator process" in str(exc):
            raise SystemExit("Пул не изменён: другая команда ./wallet сейчас меняет пул; повторите.") from None
        raise


def _pool_error_text(exc: WalletPoolError) -> str:
    texts = {
        "wallet is already in the pool": "кошелёк уже в пуле",
        "only an active wallet can be paused": "приостановить можно только активный кошелёк",
        "at least two active wallets must remain": "должно остаться хотя бы два активных кошелька",
        "only a paused wallet can be resumed": "вернуть можно только приостановленный кошелёк",
        "wallet pool exceeds its bound": "превышен предел пула",
    }
    return texts.get(str(exc), str(exc))


def _describe(pool: Any) -> str:
    paused = f"; на паузе: {', '.join(map(str, pool.paused))}" if pool.paused else ""
    return f"активные: {', '.join(map(str, pool.active))}{paused}"


async def verify_wallet(config: Any, account_index: int, key: str, *, client_factory: Any = None) -> Any:
    """One authenticated read of the wallet with an in-memory key; nothing is stored."""

    from .operator_recovery import RecoveryReadClient

    factory = client_factory or RecoveryReadClient
    other = (config.source_account_index if config.source_account_index != account_index
             else config.receiver_account_index)
    secrets = _OneKey(account_index, config.api_key_index, key)
    client = None
    try:
        client = factory(config, source_account_index=account_index, receiver_account_index=other,
                         secrets=secrets)
        return await asyncio.wait_for(client.account_snapshot(account_index, config.market_id),
                                      timeout=2 * config.request_timeout_seconds)
    finally:
        try:
            if client is not None:
                await client.aclose()
        finally:
            secrets.close()


def check_new_wallet(config: Any, account_index: int, snapshot: Any) -> str | None:
    """Refusal text, or None when the wallet is safe to add."""

    if (getattr(snapshot, "account_index", None) != account_index
            or getattr(snapshot, "market_id", None) != config.market_id):
        return "ответ биржи не совпадает с этим кошельком"
    if not snapshot.authorized:
        return "ключ не подтверждён биржей"
    if not snapshot.ready:
        return "биржа не подтверждает активный статус кошелька"
    if snapshot.active_orders:
        return (f"у кошелька есть активные ордера на {config.market_symbol} "
                f"({len(snapshot.active_orders)}); отмените их вручную и повторите")
    if snapshot.signed_position != 0:
        return (f"у кошелька открыта позиция {format(snapshot.signed_position, 'f')} {config.market_symbol}; "
                "система не принимает чужие позиции — закройте её вручную и повторите")
    return None


async def add_wallet(config: Any, operator_dir: Path, account_index: int, *, replace_key: bool = False,
                     backend: Any = None, client_factory: Any = None,
                     read_key: Any = None) -> int:
    try:
        pool = load_wallet_pool(operator_dir, config)
    except WalletPoolError as exc:
        raise SystemExit(f"Пул не изменён: {_pool_error_text(exc)}") from None
    member = account_index in pool.all
    keys = _keys(config, (account_index,), backend)
    try:
        stored = keys.has_stored_credential(account_index)
        if member and stored and not replace_key:
            print(f"Кошелёк {account_index} уже в пуле, ключ сохранён. Пул: {_describe(pool)}.")
            return 0
        if stored and not replace_key:
            key = keys.private_key(account_index, config.api_key_index)
            print(f"Для кошелька {account_index} уже есть сохранённый ключ — проверяю его.")
        else:
            prompt = (f"Приватный API-ключ кошелька {account_index} для индекса ключа "
                      f"{config.api_key_index} (ввод скрыт, Enter — готово): ")
            key = (read_key or read_hidden_secret)(prompt).strip()
            if not KEY_FORMAT.fullmatch(key):
                raise SystemExit("Кошелёк не добавлен: ключ должен быть шестнадцатеричной строкой "
                                 "(как его выдаёт Lighter). Ничего не сохранено.")
        print("Проверяю ключ одним чтением счёта на бирже (ордера не отправляются)…")
        try:
            snapshot = await verify_wallet(config, account_index, key, client_factory=client_factory)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise SystemExit(
                "Кошелёк не добавлен: биржа не подтвердила ключ для этого кошелька и индекса ключа "
                f"{config.api_key_index} ({sanitize_exception(exc)}). Ключ не сохранён.") from None
        refusal = check_new_wallet(config, account_index, snapshot)
        if refusal is not None:
            raise SystemExit(f"Кошелёк не добавлен: {refusal}. Ключ не сохранён.")
        if not stored or replace_key:
            try:
                keys.store(account_index, key, replace=replace_key)
            except KeychainConflictError:
                raise SystemExit("Кошелёк не добавлен: в Keychain уже есть другой ключ этого кошелька; "
                                 "для замены повторите с --replace-key.") from None
            print(f"Ключ проверен и сохранён в Keychain (кошелёк {account_index}, индекс ключа {config.api_key_index}).")
        balance = snapshot.available_balance
        print(f"Кошелёк {account_index}: позиция 0, активных ордеров нет, доступный баланс "
              f"{format(balance, 'f') if balance is not None else 'нет данных'}.")
    except KeychainError:
        raise SystemExit("Кошелёк не добавлен: доступ к Keychain не подтверждён.") from None
    finally:
        keys.close()
    if member:
        print(f"Кошелёк {account_index} уже был в пуле. Пул: {_describe(pool)}.")
        return 0
    pool = _update_pool(operator_dir, config, add=account_index)
    print(f"Кошелёк {account_index} добавлен. Пул: {_describe(pool)}.")
    print("Каждый цикл берёт два разных готовых кошелька случайно; изменение действует со следующей проверки.")
    return 0


def list_wallets(config: Any, operator_dir: Path, *, backend: Any = None) -> int:
    try:
        pool = load_wallet_pool(operator_dir, config)
    except WalletPoolError as exc:
        raise SystemExit(f"Файл пула некорректен: {exc}") from None
    if not pool.from_file:
        print(f"Пул не создан: работают два счёта из конфигурации ({pool.active[0]} и {pool.active[1]}). "
              "Добавить кошелёк: ./wallet add НОМЕР")
    keys = _keys(config, tuple(pool.all), backend)
    try:
        for index in pool.all:
            try:
                key = "ключ есть" if keys.has_stored_credential(index) else "НЕТ КЛЮЧА"
            except KeychainError:
                key = "Keychain недоступен"
            state = "на паузе" if index in pool.paused else "активен"
            print(f"{index}: {state}, {key}")
    finally:
        keys.close()
    if pool.from_file:
        print(f"Всего: {len(pool.all)} (активных {len(pool.active)}, на паузе {len(pool.paused)}).")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="./wallet", description="Пул кошельков: list, add НОМЕР, pause НОМЕР, resume НОМЕР.")
    parser.add_argument("action", choices=("list", "add", "pause", "resume"))
    parser.add_argument("account_index", nargs="?")
    parser.add_argument("--config", type=Path)
    parser.add_argument("--replace-key", action="store_true",
                        help="add: заменить сохранённый ключ после проверки нового")
    args = parser.parse_args(argv)
    try:
        if args.action == "list":
            if args.account_index is not None or args.replace_key:
                raise SystemExit("list не принимает номер кошелька или --replace-key.")
            config, operator_dir = _local_inputs(args.config)
            return list_wallets(config, operator_dir)
        if args.account_index is None:
            raise SystemExit(f"Укажите номер кошелька: ./wallet {args.action} НОМЕР")
        index = _account_index(args.account_index)
        if args.replace_key and args.action != "add":
            raise SystemExit("--replace-key используется только с add.")
        config, operator_dir = _local_inputs(args.config)
        if args.action == "add":
            return asyncio.run(add_wallet(config, operator_dir, index, replace_key=args.replace_key))
        pool = _update_pool(operator_dir, config, **{args.action: index})
        verb = "приостановлен: не выбирается для новых циклов, но проверяется и закрывается как все" \
            if args.action == "pause" else "снова активен"
        print(f"Кошелёк {index} {verb}. Пул: {_describe(pool)}.")
        return 0
    except KeyboardInterrupt:
        print("Отменено; пул и ключи не изменены.", file=sys.stderr)
        return 2
    except RuntimeError as exc:
        # Hidden-input and lock refusals carry fixed, secret-free text.
        print(f"Не выполнено: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
