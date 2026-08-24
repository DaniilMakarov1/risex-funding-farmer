"""Explicit one-shot Nado testnet read-only operational launcher.

This module is intentionally absent from normal Farmer startup.  Its production
entry accepts no arguments and exposes no URL, transport, identity, replay, or
write selection.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from pathlib import Path
import pwd
import secrets
import stat
from typing import Callable

from .nado_private_read_preflight import (
    NadoPreflightError, OneShotStore, PreflightConfig,
    _run_counted_operational_private_read, encode_subaccount,
    list_trigger_orders_typed_data,
)


STORE_BASENAME = ".risex-funding-farmer-nado-private-read-runs-v1.sqlite3"
KEY_BASENAME = ".risex-funding-farmer-nado-owner-key-v1"
IDENTITY_BASENAME = ".risex-funding-farmer-nado-owner-v1"
SUBACCOUNT_NAME = "default"
REDACTED_STORE_PATH = "<passwd-home>/" + STORE_BASENAME
EXPECTED_PATH_HASH = "8aabcb7a53b1e87f0ca3a0799e71acbdd7aed936218a6f8e1b20cef58e1b2341"
MAX_IDENTITY_BYTES = 160


def _home() -> Path:
    return Path(pwd.getpwuid(os.getuid()).pw_dir)


def _production_store_path() -> Path:
    path = _home() / STORE_BASENAME
    if hashlib.sha256(os.fsencode(path)).hexdigest() != EXPECTED_PATH_HASH:
        raise NadoPreflightError("fixed store identity mismatch")
    return path


def _new_runtime_run_id() -> str:
    return "nado-read-" + secrets.token_hex(16)


def _open_owned_file_once(basename: str, maximum: int) -> bytes:
    home = _home()
    directory = os.open(home, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        descriptor = os.open(
            basename, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0), dir_fd=directory,
        )
        try:
            details = os.fstat(descriptor)
            if (
                not stat.S_ISREG(details.st_mode)
                or details.st_uid != os.getuid()
                or stat.S_IMODE(details.st_mode) != 0o600
                or details.st_nlink != 1
            ):
                raise NadoPreflightError("local capability file rejected")
            raw = os.read(descriptor, maximum + 1)
            if len(raw) > maximum:
                raise NadoPreflightError("local capability file rejected")
            return raw
        finally:
            os.close(descriptor)
    except OSError:
        raise NadoPreflightError("local capability unavailable") from None
    finally:
        os.close(directory)


def _strict_identity() -> tuple[str, str]:
    raw = _open_owned_file_once(IDENTITY_BASENAME, MAX_IDENTITY_BYTES)
    try:
        value = json.loads(raw.decode("ascii"))
    except (UnicodeError, json.JSONDecodeError):
        raise NadoPreflightError("fixed identity file rejected") from None
    if type(value) is not dict or set(value) != {"owner", "subaccount_name"}:
        raise NadoPreflightError("fixed identity file rejected")
    owner, name = value["owner"], value["subaccount_name"]
    if type(owner) is not str or owner != owner.lower() or name != SUBACCOUNT_NAME:
        raise NadoPreflightError("fixed identity file rejected")
    canonical = json.dumps(
        {"owner": owner, "subaccount_name": name},
        sort_keys=True, separators=(",", ":"),
    ).encode("ascii")
    if raw != canonical:
        raise NadoPreflightError("fixed identity file rejected")
    sender = encode_subaccount(owner, name)
    return owner, sender


def _crypto() -> tuple[object, object]:
    try:
        from eth_account import Account
        from eth_account.messages import encode_typed_data
    except (ImportError, AttributeError):
        raise NadoPreflightError("Nado signing capability unavailable") from None
    return Account, encode_typed_data


class _OwnerKeyCapability:
    """Opaque closeable exact-operation handle; raw bytes never leave it."""

    def __init__(self, secret: bytes, sender: str) -> None:
        if type(secret) is not bytes or len(secret) != 32 or int.from_bytes(secret, "big") == 0:
            raise NadoPreflightError("owner key capability rejected")
        self.__secret = bytearray(secret)
        self.__sender = sender
        self.__closed = False

    def derive_owner(self) -> str:
        if self.__closed:
            raise NadoPreflightError("owner key capability is closed")
        Account, _ = _crypto()
        try:
            return str(Account.from_key(bytes(self.__secret)).address).lower()
        except BaseException:
            raise NadoPreflightError("owner derivation rejected") from None

    def sign_list_trigger_orders(self, typed_data: dict[str, object]) -> str:
        if self.__closed:
            raise NadoPreflightError("owner key capability is closed")
        try:
            recv_time = typed_data["message"]["recvTime"]
            expected = list_trigger_orders_typed_data(self.__sender, recv_time)
        except BaseException:
            raise NadoPreflightError("typed data rejected") from None
        if typed_data != expected:
            raise NadoPreflightError("typed data rejected")
        Account, encode_typed_data = _crypto()
        try:
            signed = Account.sign_message(
                encode_typed_data(full_message=typed_data), bytes(self.__secret),
            )
            return "0x" + bytes(signed.signature).hex()
        except BaseException:
            raise NadoPreflightError("sign operation rejected") from None

    def close(self) -> None:
        for index in range(len(self.__secret)):
            self.__secret[index] = 0
        self.__secret.clear()
        self.__closed = True


def _load_owner_capability(sender: str) -> _OwnerKeyCapability:
    secret = bytearray(_open_owned_file_once(KEY_BASENAME, 32))
    try:
        if len(secret) != 32:
            raise NadoPreflightError("owner key capability rejected")
        return _OwnerKeyCapability(bytes(secret), sender)
    finally:
        for index in range(len(secret)):
            secret[index] = 0
        secret.clear()


def _recover_owner(typed_data: dict[str, object], signature: str) -> str:
    Account, encode_typed_data = _crypto()
    try:
        return str(Account.recover_message(
            encode_typed_data(full_message=typed_data), signature=signature,
        )).lower()
    except BaseException:
        raise NadoPreflightError("signature recovery rejected") from None


def _prepare_store(path: Path) -> OneShotStore:
    try:
        descriptor = os.open(
            path, os.O_CREAT | os.O_EXCL | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
    except FileExistsError:
        details = path.lstat()
        if (
            path.is_symlink() or not stat.S_ISREG(details.st_mode)
            or details.st_uid != os.getuid() or stat.S_IMODE(details.st_mode) != 0o600
            or details.st_nlink != 1
        ):
            raise NadoPreflightError("fixed store file rejected") from None
    except OSError:
        raise NadoPreflightError("fixed store unavailable") from None
    else:
        os.fchmod(descriptor, 0o600)
        os.fsync(descriptor)
        os.close(descriptor)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    return OneShotStore(path)


async def run() -> dict[str, object]:
    """Run one fresh production observation; requires a separate Chief gate."""
    owner, sender = _strict_identity()
    config = PreflightConfig(
        owner=owner, subaccount_name=SUBACCOUNT_NAME, sender=sender,
        invocation_id=_new_runtime_run_id(), exclusive_owner_lease=True,
        direct_owner_eoa=True,
    )
    store = _prepare_store(_production_store_path())
    try:
        report = await _run_counted_operational_private_read(
            config=config,
            capability_loader=lambda: _load_owner_capability(sender),
            recover_owner=_recover_owner,
            store=store,
            path_hash=EXPECTED_PATH_HASH,
        )
        return {**report, "path": REDACTED_STORE_PATH}
    finally:
        store.close()


def _fixture_run(
    *, config: PreflightConfig, store: OneShotStore,
    capability_loader: Callable[[], object],
    recover_owner: Callable[[dict[str, object], str], str],
    path_hash: str, transports: tuple[object, object, object],
) -> object:
    """Private synthetic seam; production does not call or expose it."""
    return _run_counted_operational_private_read(
        config=config, capability_loader=capability_loader,
        recover_owner=recover_owner, store=store, path_hash=path_hash,
        _transports=transports,
    )


def main() -> None:
    try:
        report = asyncio.run(run())
    except BaseException:
        report = {
            "schema_version": 2, "status": "BLOCKED",
            "path": REDACTED_STORE_PATH, "reason": "OPERATIONAL_PREREQUISITE_FAILED",
        }
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
