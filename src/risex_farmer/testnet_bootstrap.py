from __future__ import annotations

import asyncio as _asyncio
from dataclasses import dataclass
from enum import Enum
import json as _json
import os
from pathlib import Path as _Path
import pwd as _pwd
import stat as _stat
from typing import Any

import aiohttp
from yarl import URL as _URL


_ORIGIN = _URL("https://api.testnet.rise.trade")
_SYSTEM_CONFIG = "/v1/system/config"
_EIP712_DOMAIN = "/v1/auth/eip712-domain"
_ACCOUNT_BALANCE = "/v1/account/balance"
_ACCOUNT_DEPOSIT = "/v1/account/deposit"
_CHAIN_ID = 11_155_931
_EXPECTED_WALLET = "0x20f9153e2eeba0ff7880fb5a23e976e8b2af56ee"
_DEPOSIT_INTENT = "RISEX_TESTNET_DEPOSIT"
_DEPOSIT_AMOUNT = "1000"
_TIMEOUT_SECONDS = 10
_MARKER = ".risex-funding-farmer-testnet-first-deposit-v1.json"
_READY_TEMP = _MARKER + ".ready.tmp"


class BootstrapStatus(Enum):
    READY = "READY"
    ALREADY_READY = "ALREADY_READY"
    SUBMITTED_UNVERIFIED = "SUBMITTED_UNVERIFIED"
    UNKNOWN_AMBIGUOUS = "UNKNOWN_AMBIGUOUS"
    REJECTED = "REJECTED"
    READY_UNVERIFIED = "READY_UNVERIFIED"


class BootstrapSafetyError(RuntimeError):
    """A fixed, sanitized rejection raised before a permitted write."""


@dataclass(frozen=True)
class AccountState:
    ready: bool
    balance_raw: str


@dataclass(frozen=True)
class BootstrapResult:
    status: BootstrapStatus
    balance_raw: str | None = None
    message: str = ""


@dataclass(frozen=True)
class _Identity:
    usdc: str


_MARKER_BASE = {
    "schema_version": 1,
    "venue": "RISEx",
    "host": "api.testnet.rise.trade",
    "chain_id": _CHAIN_ID,
    "wallet": _EXPECTED_WALLET,
    "operation": "FIRST_DEPOSIT",
    "amount": _DEPOSIT_AMOUNT,
}


def _marker_bytes(state: str) -> bytes:
    return (_json.dumps(
        _MARKER_BASE | {"state": state}, sort_keys=True, separators=(",", ":")
    ) + "\n").encode()


_SPENT_BYTES = _marker_bytes("SPENT_UNKNOWN")
_READY_BYTES = _marker_bytes("READY")


def _passwd_home() -> _Path:
    return _Path(_pwd.getpwuid(os.getuid()).pw_dir)


def _required_open_flags() -> int:
    directory = getattr(os, "O_DIRECTORY", 0)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    if not directory or not nofollow:
        raise _safety_error()
    return directory | nofollow | getattr(os, "O_CLOEXEC", 0)


def _open_home() -> int:
    fd: int | None = None
    try:
        fd = os.open(_passwd_home(), os.O_RDONLY | _required_open_flags())
        details = os.fstat(fd)
        if not _stat.S_ISDIR(details.st_mode) or details.st_uid != os.getuid():
            raise _safety_error()
        return fd
    except BootstrapSafetyError:
        if fd is not None:
            os.close(fd)
        raise
    except (OSError, TypeError, ValueError):
        raise _safety_error() from None


def _write_all(fd: int, payload: bytes) -> None:
    written = 0
    while written < len(payload):
        count = os.write(fd, payload[written:])
        if count <= 0:
            raise OSError("short marker write")
        written += count


def _validate_file(fd: int) -> None:
    details = os.fstat(fd)
    if (
        not _stat.S_ISREG(details.st_mode)
        or details.st_uid != os.getuid()
        or _stat.S_IMODE(details.st_mode) != 0o600
        or details.st_nlink != 1
    ):
        raise _safety_error()


def _read_entry(home_fd: int, name: str) -> bytes | None:
    flags = (
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)
        | _required_open_flags()
    )
    flags &= ~os.O_DIRECTORY
    try:
        fd = os.open(name, flags, dir_fd=home_fd)
    except FileNotFoundError:
        return None
    except OSError:
        raise _safety_error() from None
    try:
        _validate_file(fd)
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(fd, 4096)
            if not chunk:
                break
            total += len(chunk)
            if total > 4096:
                raise _safety_error()
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        os.close(fd)


def _existing_state(home_fd: int) -> str | None:
    marker = _read_entry(home_fd, _MARKER)
    temporary = _read_entry(home_fd, _READY_TEMP)
    if marker is None:
        if temporary is not None:
            return "BLOCKED"
        return None
    if marker == _SPENT_BYTES:
        return "SPENT_UNKNOWN"
    if marker == _READY_BYTES:
        return "READY"
    raise _safety_error()


def _claim_in_home(home_fd: int) -> bool:
    if _existing_state(home_fd) is not None:
        return False
    flags = (
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        fd = os.open(_MARKER, flags, 0o600, dir_fd=home_fd)
    except FileExistsError:
        return False
    except OSError:
        raise _safety_error() from None
    try:
        os.fchmod(fd, 0o600)
        _validate_file(fd)
        _write_all(fd, _SPENT_BYTES)
        os.fsync(fd)
        os.fsync(home_fd)
    except (BootstrapSafetyError, OSError):
        raise _safety_error() from None
    finally:
        os.close(fd)
    return True


def _claim_first_deposit() -> bool:
    home_fd = _open_home()
    try:
        return _claim_in_home(home_fd)
    finally:
        os.close(home_fd)


def _mark_ready(home_fd: int) -> None:
    flags = (
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        fd = os.open(_READY_TEMP, flags, 0o600, dir_fd=home_fd)
    except OSError:
        raise _safety_error() from None
    try:
        os.fchmod(fd, 0o600)
        _validate_file(fd)
        _write_all(fd, _READY_BYTES)
        os.fsync(fd)
    except (BootstrapSafetyError, OSError):
        raise _safety_error() from None
    finally:
        os.close(fd)
    try:
        os.replace(_READY_TEMP, _MARKER, src_dir_fd=home_fd, dst_dir_fd=home_fd)
        os.fsync(home_fd)
    except OSError:
        raise _safety_error() from None


def _safety_error() -> BootstrapSafetyError:
    return BootstrapSafetyError("RISEx testnet identity or response rejected")


def _validate_wallet(wallet: object) -> str:
    if not isinstance(wallet, str) or wallet.lower() != _EXPECTED_WALLET:
        raise _safety_error()
    return _EXPECTED_WALLET


def _valid_contract(value: object) -> bool:
    if not isinstance(value, str) or len(value) != 42 or not value.startswith("0x"):
        return False
    try:
        return int(value[2:], 16) != 0
    except ValueError:
        return False


def _mapping(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise _safety_error()
    return value


def _payload(value: object) -> dict[str, Any]:
    root = _mapping(value)
    if "data" not in root:
        return root
    keys = set(root)
    if keys == {"data"}:
        return _mapping(root["data"])
    if (
        keys != {"data", "request_id"}
        or not isinstance(root.get("request_id"), str)
        or not root["request_id"]
    ):
        raise _safety_error()
    return _mapping(root["data"])


def _session() -> aiohttp.ClientSession:
    return aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=_TIMEOUT_SECONDS),
        trust_env=False,
    )


async def _request_json(
    session: aiohttp.ClientSession,
    method: str,
    path: str,
    *,
    params: dict[str, str] | None = None,
    json: dict[str, str] | None = None,
) -> tuple[int, object]:
    expected = (_ORIGIN.with_path(path)).with_query(params or {})
    async with session.request(
        method,
        _ORIGIN.with_path(path),
        params=params,
        json=json,
        allow_redirects=False,
    ) as response:
        if response.url != expected:
            raise _safety_error()
        status = response.status
        if 300 <= status < 400:
            raise _safety_error()
        body = await response.json()
        return status, body


async def _identity(session: aiohttp.ClientSession) -> _Identity:
    config_status, config_body = await _request_json(session, "GET", _SYSTEM_CONFIG)
    if not 200 <= config_status < 300:
        raise _safety_error()
    config = _payload(config_body)
    chain = _mapping(config.get("chain"))
    addresses = _mapping(config.get("addresses"))
    usdc, auth = addresses.get("usdc"), addresses.get("auth")
    if (
        chain.get("name") != "Rise Testnet"
        or chain.get("chain_id") != str(_CHAIN_ID)
        or not _valid_contract(usdc)
        or not _valid_contract(auth)
    ):
        raise _safety_error()

    domain_status, domain_body = await _request_json(session, "GET", _EIP712_DOMAIN)
    if not 200 <= domain_status < 300:
        raise _safety_error()
    domain = _payload(domain_body)
    if (
        domain.get("name") != "RISEx"
        or domain.get("version") != "1"
        or domain.get("chain_id") != str(_CHAIN_ID)
        or not _valid_contract(domain.get("verifying_contract"))
        or domain["verifying_contract"].lower() != auth.lower()
    ):
        raise _safety_error()
    assert isinstance(usdc, str)
    return _Identity(usdc=usdc)


async def _balance(
    session: aiohttp.ClientSession, wallet: str, identity: _Identity
) -> AccountState:
    status, body = await _request_json(
        session,
        "GET",
        _ACCOUNT_BALANCE,
        params={"account": wallet, "token": identity.usdc},
    )
    if not 200 <= status < 300:
        raise _safety_error()
    balance = _payload(body).get("balance")
    if not isinstance(balance, str) or not balance.isdigit():
        raise _safety_error()
    return AccountState(ready=int(balance) > 0, balance_raw=balance)


async def _preflight_balance(
    session: aiohttp.ClientSession, wallet: str, identity: _Identity
) -> AccountState | None:
    status, body = await _request_json(
        session,
        "GET",
        _ACCOUNT_BALANCE,
        params={"account": wallet, "token": identity.usdc},
    )
    if 200 <= status < 300:
        balance = _payload(body).get("balance")
        if not isinstance(balance, str) or not balance.isdigit():
            raise _safety_error()
        return AccountState(ready=int(balance) > 0, balance_raw=balance)
    if (
        status == 500
        and isinstance(body, dict)
        and set(body) == {"error", "request_id"}
        and isinstance(body.get("request_id"), str)
        and bool(body["request_id"])
        and isinstance(body.get("error"), dict)
        and body["error"] == {
            "code": "Internal",
            "message": "failed to get balance",
        }
    ):
        return None
    raise _safety_error()


async def check_risex_account(wallet: str) -> AccountState:
    """Read the fixed RISEx testnet account without exposing transport controls."""

    expected_wallet = _validate_wallet(wallet)
    try:
        async with _session() as session:
            identity = await _identity(session)
            return await _balance(session, expected_wallet, identity)
    except BootstrapSafetyError:
        raise _safety_error() from None
    except Exception:
        raise _safety_error() from None


async def bootstrap_risex_account(wallet: str, *, intent: str) -> BootstrapResult:
    """Perform at most one fixed RISEx testnet faucet deposit and verify balance."""

    expected_wallet = _validate_wallet(wallet)
    if intent != _DEPOSIT_INTENT:
        raise _safety_error()
    home_fd = _open_home()
    dispatched = False
    try:
        existing = _existing_state(home_fd)
        if existing == "SPENT_UNKNOWN" or existing == "BLOCKED":
            return BootstrapResult(
                BootstrapStatus.UNKNOWN_AMBIGUOUS,
                message="testnet deposit authorization is already consumed",
            )
        if existing == "READY":
            try:
                async with _session() as session:
                    identity = await _identity(session)
                    state = await _balance(session, expected_wallet, identity)
            except _asyncio.CancelledError:
                raise
            except Exception:
                return BootstrapResult(
                    BootstrapStatus.READY_UNVERIFIED,
                    message="local completion cannot verify authoritative balance",
                )
            if state.ready:
                return BootstrapResult(
                    BootstrapStatus.READY,
                    state.balance_raw,
                    "authoritative balance is positive",
                )
            return BootstrapResult(
                BootstrapStatus.READY_UNVERIFIED,
                state.balance_raw,
                "authoritative balance is not positive",
            )

        async with _session() as session:
            try:
                identity = await _identity(session)
                preflight = await _preflight_balance(session, expected_wallet, identity)
                if preflight is not None and preflight.ready:
                    return BootstrapResult(
                        BootstrapStatus.ALREADY_READY,
                        preflight.balance_raw,
                        "authoritative balance already positive",
                    )
                if not _claim_in_home(home_fd):
                    return BootstrapResult(
                        BootstrapStatus.UNKNOWN_AMBIGUOUS,
                        message="testnet deposit authorization is already consumed",
                    )
                identity = await _identity(session)
                expected_wallet = _validate_wallet(expected_wallet)
            except BootstrapSafetyError:
                raise
            except Exception:
                raise _safety_error() from None

            try:
                dispatched = True
                status, body = await _request_json(
                    session,
                    "POST",
                    _ACCOUNT_DEPOSIT,
                    json={"account": expected_wallet, "amount": _DEPOSIT_AMOUNT},
                )
                submitted = _payload(body).get("success") is True
                if 400 <= status < 500:
                    return BootstrapResult(
                        BootstrapStatus.REJECTED, message="testnet deposit rejected"
                    )
                if not 200 <= status < 300 or not submitted:
                    return BootstrapResult(
                        BootstrapStatus.UNKNOWN_AMBIGUOUS,
                        message="testnet deposit result is ambiguous",
                    )
            except Exception:
                return BootstrapResult(
                    BootstrapStatus.UNKNOWN_AMBIGUOUS,
                    message="testnet deposit result is ambiguous",
                )

            try:
                postcondition = await _balance(session, expected_wallet, identity)
            except Exception:
                return BootstrapResult(
                    BootstrapStatus.UNKNOWN_AMBIGUOUS,
                    message="authoritative balance could not be verified",
                )
            if postcondition.ready:
                try:
                    _mark_ready(home_fd)
                except BootstrapSafetyError:
                    return BootstrapResult(
                        BootstrapStatus.UNKNOWN_AMBIGUOUS,
                        postcondition.balance_raw,
                        "authoritative balance is positive but local state is ambiguous",
                    )
                return BootstrapResult(BootstrapStatus.READY, postcondition.balance_raw,
                                       "authoritative balance is positive")
            return BootstrapResult(
                BootstrapStatus.SUBMITTED_UNVERIFIED,
                postcondition.balance_raw,
                "deposit accepted but authoritative balance is not positive",
            )
    except BootstrapSafetyError:
        if not dispatched:
            raise _safety_error() from None
    except Exception:
        if not dispatched:
            raise _safety_error() from None
    finally:
        os.close(home_fd)
    return BootstrapResult(BootstrapStatus.UNKNOWN_AMBIGUOUS,
                           message="testnet deposit result is ambiguous")
