from __future__ import annotations

import asyncio as _asyncio
from dataclasses import dataclass as _dataclass
from enum import Enum as _Enum
import fcntl as _fcntl
import json as _json
import os
from pathlib import Path as _Path
import pwd as _pwd
import stat as _stat
import time as _time
from typing import Any as _Any, Callable as _Callable

import aiohttp
from yarl import URL as _URL


_ORIGIN = _URL("https://api.testnet.rise.trade")
_CHAIN_ID = 11_155_931
_AUTH = "0x6da86f486b5e6536358f5b122dbe184522ca0ee3"
_EXPECTED_WALLET = "0x20f9153e2eeba0ff7880fb5a23e976e8b2af56ee"
_DOMAIN_NAME = "RISEx"
_DOMAIN_VERSION = "1"
_SYSTEM_CONFIG = "/v1/system/config"
_EIP712_DOMAIN = "/v1/auth/eip712-domain"
_NONCE_STATE = "/v1/nonce-state/{account}"
_SIGNER_STATUS = "/v1/auth/session-key-status"
_SIGNERS = "/v1/auth/signers"
_REGISTER = "/v1/auth/register-signer"
_TIMEOUT_SECONDS = 10
_MESSAGE = "RISEx session key"
_LABEL = "RISEx Funding Farmer testnet probe"
_EXPIRATION_SECONDS = 30 * 24 * 60 * 60
_GENERATE_INTENT = "RISEX_TESTNET_GENERATE_SESSION_SIGNER"
_REGISTER_INTENT = "RISEX_TESTNET_REGISTER_SESSION_SIGNER"
_CREDENTIAL = ".risex-funding-farmer-risex-session-signer-v1.key"
_RECORD = ".risex-funding-farmer-risex-session-signer-v1.json"
_RECORD_TEMP = _RECORD + ".tmp"

def _crypto() -> tuple[_Any, _Any, _Any]:
    try:
        from eth_account import Account
        from eth_account.messages import encode_typed_data
        from eth_utils import keccak
    except Exception:
        raise _safety_error() from None
    return Account, encode_typed_data, keccak


REGISTER_SIGNER_TYPEHASH = bytes.fromhex(
    "a526f63b3968e56ae1b177ce9b3dc29766e0891e6397a9c23cf8c53ee8fc8f62"
)
VERIFY_SIGNER_TYPEHASH = bytes.fromhex(
    "4d298dcceb691695f582cc337308236426a0c97201a31834625e8eadc44d4230"
)
REVOKE_SIGNER_TYPEHASH = bytes.fromhex(
    "36db7f392f548b56f37d89469115d138685addf06be45684f9e5b0e8b5d28000"
)


class SignerState(_Enum):
    CREATED = "CREATED"
    SPENT_UNKNOWN = "SPENT_UNKNOWN"
    ACTIVE = "ACTIVE"


class SignerSafetyError(RuntimeError):
    """A sanitized fail-closed signer onboarding rejection."""


@_dataclass(frozen=True)
class SignerResult:
    state: SignerState
    signer_address: str
    expiration: int


@_dataclass(frozen=True)
class _Record:
    signer: str
    expiration: int
    state: SignerState


@_dataclass(frozen=True)
class _Nonce:
    observed_anchor: int
    observed_index: int
    observed_bitmap: int
    signed_anchor: int
    signed_bitmap: int = 0


def _safety_error() -> SignerSafetyError:
    return SignerSafetyError("RISEx testnet signer identity or response rejected")


def _now_unix() -> int:
    return int(_time.time())


def _passwd_home() -> _Path:
    return _Path(_pwd.getpwuid(os.getuid()).pw_dir)


def _generate_private_key() -> bytes:
    Account, _encode_typed_data, _keccak = _crypto()
    try:
        return bytes(Account.create().key)
    except Exception:
        raise _safety_error() from None


def _open_home() -> int:
    directory = getattr(os, "O_DIRECTORY", 0)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    if not directory or not nofollow:
        raise _safety_error()
    fd: int | None = None
    try:
        fd = os.open(
            _passwd_home(), os.O_RDONLY | directory | nofollow
            | getattr(os, "O_CLOEXEC", 0)
        )
        details = os.fstat(fd)
        if not _stat.S_ISDIR(details.st_mode) or details.st_uid != os.getuid():
            raise _safety_error()
        return fd
    except SignerSafetyError:
        if fd is not None:
            os.close(fd)
        raise
    except (OSError, TypeError, ValueError):
        if fd is not None:
            os.close(fd)
        raise _safety_error() from None


def _file_flags(write: bool = False, exclusive: bool = False) -> int:
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    if not nofollow:
        raise _safety_error()
    flags = (os.O_WRONLY if write else os.O_RDONLY) | nofollow
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)
    if exclusive:
        flags |= os.O_CREAT | os.O_EXCL
    return flags


def _validate_file(fd: int) -> None:
    details = os.fstat(fd)
    if (
        not _stat.S_ISREG(details.st_mode)
        or details.st_uid != os.getuid()
        or _stat.S_IMODE(details.st_mode) != 0o600
        or details.st_nlink != 1
    ):
        raise _safety_error()


def _write_all(fd: int, value: bytes) -> None:
    offset = 0
    while offset < len(value):
        count = os.write(fd, value[offset:])
        if count <= 0:
            raise OSError("short write")
        offset += count


def _read_file(home_fd: int, name: str) -> bytes | None:
    try:
        fd = os.open(name, _file_flags(), dir_fd=home_fd)
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


def _create_file(home_fd: int, name: str, value: bytes) -> None:
    try:
        fd = os.open(name, _file_flags(write=True, exclusive=True), 0o600,
                     dir_fd=home_fd)
    except OSError:
        raise _safety_error() from None
    try:
        os.fchmod(fd, 0o600)
        _validate_file(fd)
        _write_all(fd, value)
        os.fsync(fd)
        os.fsync(home_fd)
    except (OSError, SignerSafetyError):
        raise _safety_error() from None
    finally:
        os.close(fd)


def _record_mapping(signer: str, expiration: int, state: SignerState) -> dict[str, _Any]:
    return {
        "schema_version": 1,
        "venue": "RISEx",
        "host": "api.testnet.rise.trade",
        "chain_id": _CHAIN_ID,
        "wallet": _EXPECTED_WALLET,
        "operation": "SESSION_SIGNER_REGISTRATION",
        "signer": signer,
        "expiration": expiration,
        "state": state.value,
    }


def _record_bytes(signer: str, expiration: int, state: SignerState) -> bytes:
    return (_json.dumps(_record_mapping(signer, expiration, state), sort_keys=True,
                        separators=(",", ":")) + "\n").encode()


def _parse_record(value: bytes) -> _Record:
    try:
        data = _json.loads(value)
        state = SignerState(data["state"])
        signer = _normalize_address(data["signer"])
        expiration = data["expiration"]
        if not isinstance(expiration, int):
            raise ValueError
        if data != _record_mapping(signer, expiration, state):
            raise ValueError
        return _Record(signer, expiration, state)
    except Exception:
        raise _safety_error() from None


def _load_record(home_fd: int) -> _Record:
    value = _read_file(home_fd, _RECORD)
    if value is None:
        raise _safety_error()
    return _parse_record(value)


def _load_credential(home_fd: int) -> bytes:
    value = _read_file(home_fd, _CREDENTIAL)
    if value is None or len(value) != 32:
        raise _safety_error()
    return value


def _validate_credential_entry(home_fd: int) -> None:
    try:
        fd = os.open(_CREDENTIAL, _file_flags(), dir_fd=home_fd)
    except OSError:
        raise _safety_error() from None
    try:
        _validate_file(fd)
        if os.fstat(fd).st_size != 32:
            raise _safety_error()
    finally:
        os.close(fd)


def _replace_record(home_fd: int, record: _Record, state: SignerState) -> None:
    if _read_file(home_fd, _RECORD_TEMP) is not None:
        raise _safety_error()
    _create_file(home_fd, _RECORD_TEMP,
                 _record_bytes(record.signer, record.expiration, state))
    try:
        os.replace(_RECORD_TEMP, _RECORD, src_dir_fd=home_fd, dst_dir_fd=home_fd)
        os.fsync(home_fd)
    except OSError:
        raise _safety_error() from None


def _credential_lock(home_fd: int) -> int:
    fd: int | None = None
    try:
        fd = os.open(_CREDENTIAL, _file_flags(), dir_fd=home_fd)
        _validate_file(fd)
        _fcntl.flock(fd, _fcntl.LOCK_EX)
        return fd
    except (OSError, SignerSafetyError):
        if fd is not None:
            os.close(fd)
        raise _safety_error() from None


def _transition_record(expected: SignerState | None,
                       target: SignerState) -> _Record | None:
    home_fd = _open_home()
    lock_fd: int | None = None
    try:
        lock_fd = _credential_lock(home_fd)
        record = _load_record(home_fd)
        if expected is not None and record.state is not expected:
            return None
        if record.state is not target:
            _replace_record(home_fd, record, target)
        return _Record(record.signer, record.expiration, target)
    finally:
        if lock_fd is not None:
            _fcntl.flock(lock_fd, _fcntl.LOCK_UN)
            os.close(lock_fd)
        os.close(home_fd)


def _claim_registration() -> bool:
    return _transition_record(SignerState.CREATED,
                              SignerState.SPENT_UNKNOWN) is not None


def _mark_active() -> _Record:
    record = _transition_record(None, SignerState.ACTIVE)
    assert record is not None
    return record


def _current_record() -> _Record:
    home_fd = _open_home()
    try:
        _validate_credential_entry(home_fd)
        return _load_record(home_fd)
    finally:
        os.close(home_fd)


def _normalize_address(value: object) -> str:
    if not isinstance(value, str) or len(value) != 42 or not value.startswith("0x"):
        raise _safety_error()
    try:
        int(value[2:], 16)
    except ValueError:
        raise _safety_error() from None
    return value.lower()


def _derive_address(secret: bytes) -> str:
    Account, _encode_typed_data, _keccak = _crypto()
    try:
        return Account.from_key(secret).address.lower()
    except Exception:
        raise _safety_error() from None


def generate_risex_session_signer(*, intent: str) -> SignerResult:
    if intent != _GENERATE_INTENT:
        raise _safety_error()
    now = _now_unix()
    expiration = now + _EXPIRATION_SECONDS
    if not 0 < now < expiration <= 2**32 - 1:
        raise _safety_error()
    secret = _generate_private_key()
    if not isinstance(secret, bytes) or len(secret) != 32:
        raise _safety_error()
    signer = _derive_address(secret)
    home_fd = _open_home()
    try:
        if (_read_file(home_fd, _CREDENTIAL) is not None
                or _read_file(home_fd, _RECORD) is not None
                or _read_file(home_fd, _RECORD_TEMP) is not None):
            raise _safety_error()
        _create_file(home_fd, _CREDENTIAL, secret)
        _create_file(home_fd, _RECORD,
                     _record_bytes(signer, expiration, SignerState.CREATED))
    finally:
        os.close(home_fd)
    return SignerResult(SignerState.CREATED, signer, expiration)


def _domain() -> dict[str, _Any]:
    return {
        "name": _DOMAIN_NAME,
        "version": _DOMAIN_VERSION,
        "chainId": _CHAIN_ID,
        "verifyingContract": _AUTH,
    }


def _typed_data(primary: str, fields: list[dict[str, str]],
                message: dict[str, _Any]) -> dict[str, _Any]:
    return {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            primary: fields,
        },
        "primaryType": primary,
        "domain": _domain(),
        "message": message,
    }


def _build_register_typed_data(account: str, signer: str, expiration: int,
                               nonce_anchor: int, nonce_bitmap: int) -> dict[str, _Any]:
    return _typed_data("RegisterSigner", [
        {"name": "account", "type": "address"},
        {"name": "signer", "type": "address"},
        {"name": "message", "type": "string"},
        {"name": "expiration", "type": "uint32"},
        {"name": "nonceAnchor", "type": "uint48"},
        {"name": "nonceBitmap", "type": "uint8"},
    ], {"account": _normalize_address(account), "signer": _normalize_address(signer),
        "message": _MESSAGE, "expiration": expiration,
        "nonceAnchor": nonce_anchor, "nonceBitmap": nonce_bitmap})


def _build_verify_typed_data(account: str, nonce_anchor: int,
                             nonce_bitmap: int) -> dict[str, _Any]:
    return _typed_data("VerifySigner", [
        {"name": "account", "type": "address"},
        {"name": "nonceAnchor", "type": "uint48"},
        {"name": "nonceBitmap", "type": "uint8"},
    ], {"account": _normalize_address(account), "nonceAnchor": nonce_anchor,
        "nonceBitmap": nonce_bitmap})


def _build_revoke_typed_data(account: str, signer: str, nonce_anchor: int,
                             nonce_bitmap: int) -> dict[str, _Any]:
    return _typed_data("RevokeSigner", [
        {"name": "account", "type": "address"},
        {"name": "signer", "type": "address"},
        {"name": "nonceAnchor", "type": "uint48"},
        {"name": "nonceBitmap", "type": "uint8"},
    ], {"account": _normalize_address(account), "signer": _normalize_address(signer),
        "nonceAnchor": nonce_anchor, "nonceBitmap": nonce_bitmap})


def _typed_data_digest(value: dict[str, _Any]) -> bytes:
    _Account, encode_typed_data, keccak = _crypto()
    try:
        message = encode_typed_data(full_message=value)
        return keccak(b"\x19" + message.version + message.header + message.body)
    except Exception:
        raise _safety_error() from None


def _sign_typed_data(secret: bytes, value: dict[str, _Any]) -> str:
    Account, encode_typed_data, _keccak = _crypto()
    try:
        signed = Account.sign_message(encode_typed_data(full_message=value), secret)
        signature = bytes(signed.signature)
        if len(signature) != 65:
            raise ValueError
        return "0x" + signature.hex()
    except Exception:
        raise _safety_error() from None


def _build_revoke_request(account: str, signer: str, nonce_anchor: int,
                          nonce_bitmap: int, signature: str) -> dict[str, _Any]:
    if not _valid_signature(signature):
        raise _safety_error()
    return {"account": _normalize_address(account),
            "signer": _normalize_address(signer),
            "nonce_anchor": str(nonce_anchor),
            "nonce_bitmap_index": nonce_bitmap,
            "account_signature": signature}


def _valid_signature(value: object) -> bool:
    if not isinstance(value, str) or len(value) != 132 or not value.startswith("0x"):
        return False
    try:
        bytes.fromhex(value[2:])
        return True
    except ValueError:
        return False


def _payload(value: object) -> dict[str, _Any]:
    if not isinstance(value, dict) or set(value) != {"data", "request_id"}:
        raise _safety_error()
    if not isinstance(value["request_id"], str) or not value["request_id"]:
        raise _safety_error()
    data = value["data"]
    if not isinstance(data, dict):
        raise _safety_error()
    return data


def _session() -> aiohttp.ClientSession:
    return aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=_TIMEOUT_SECONDS),
                                 trust_env=False)


async def _request_json(session: aiohttp.ClientSession, method: str, path: str,
                        *, params: dict[str, str] | None = None,
                        json: dict[str, _Any] | None = None) -> tuple[int, object]:
    expected = _ORIGIN.with_path(path).with_query(params or {})
    async with session.request(method, _ORIGIN.with_path(path), params=params, json=json,
                               allow_redirects=False) as response:
        if response.url != expected or 300 <= response.status < 400:
            raise _safety_error()
        return response.status, await response.json()


async def _identity(session: aiohttp.ClientSession) -> None:
    status, body = await _request_json(session, "GET", _SYSTEM_CONFIG)
    if not 200 <= status < 300:
        raise _safety_error()
    config = _payload(body)
    chain, addresses = config.get("chain"), config.get("addresses")
    if (not isinstance(chain, dict) or not isinstance(addresses, dict)
            or chain.get("name") != "Rise Testnet"
            or chain.get("chain_id") != str(_CHAIN_ID)
            or _normalize_address(addresses.get("auth")) != _AUTH):
        raise _safety_error()
    status, body = await _request_json(session, "GET", _EIP712_DOMAIN)
    if not 200 <= status < 300:
        raise _safety_error()
    domain = _payload(body)
    if (set(domain) != {"name", "version", "chain_id", "verifying_contract"}
            or domain.get("name") != _DOMAIN_NAME
            or domain.get("version") != _DOMAIN_VERSION
            or domain.get("chain_id") != str(_CHAIN_ID)
            or _normalize_address(domain.get("verifying_contract")) != _AUTH):
        raise _safety_error()


async def _nonce(session: aiohttp.ClientSession, account: str) -> _Nonce:
    status, body = await _request_json(
        session, "GET", _NONCE_STATE.format(account=account)
    )
    if not 200 <= status < 300:
        raise _safety_error()
    data = _payload(body)
    if set(data) != {"nonce_anchor", "current_bitmap_index", "bitmap"}:
        raise _safety_error()
    anchor = data["nonce_anchor"]
    index = data["current_bitmap_index"]
    bitmap = data["bitmap"]
    bitmap_digits = bitmap[2:] if isinstance(bitmap, str) and bitmap.startswith("0x") else ""
    if (not isinstance(anchor, str) or not anchor.isdigit()
            or not isinstance(index, int)
            or not 1 <= len(bitmap_digits) <= 64
            or not all(character in "0123456789abcdefABCDEF"
                       for character in bitmap_digits)):
        raise _safety_error()
    anchor_value, bitmap_value = int(anchor), int(bitmap_digits, 16)
    if not 0 <= anchor_value < 2**48 - 1 or not 0 <= index <= 208:
        raise _safety_error()
    if not 0 <= bitmap_value < 2**256:
        raise _safety_error()
    return _Nonce(anchor_value, index, bitmap_value, anchor_value + 1)


async def _authoritative_state(session: aiohttp.ClientSession, account: str,
                               record: _Record) -> bool:
    status_code, body = await _request_json(
        session, "GET", _SIGNER_STATUS,
        params={"account": account, "signer": record.signer},
    )
    if not 200 <= status_code < 300:
        raise _safety_error()
    status = _payload(body)
    if set(status) != {"status", "status_description"}:
        raise _safety_error()
    if not isinstance(status["status"], int) or not isinstance(
        status["status_description"], str
    ):
        raise _safety_error()
    list_code, body = await _request_json(
        session, "GET", _SIGNERS, params={"account": account}
    )
    if not 200 <= list_code < 300:
        raise _safety_error()
    listing = _payload(body)
    if set(listing) != {"signers"} or not isinstance(listing["signers"], list):
        raise _safety_error()
    matches = []
    for item in listing["signers"]:
        if not isinstance(item, dict):
            raise _safety_error()
        if _normalize_address(item.get("signer")) == record.signer:
            matches.append(item)
    if len(matches) > 1:
        raise _safety_error()
    if status != {"status": 1, "status_description": "Active"}:
        return False
    if len(matches) != 1:
        return False
    item = matches[0]
    return (
        item.get("label") == _LABEL
        and item.get("expiration") == str(record.expiration)
        and item.get("status") == "Active"
        and isinstance(item.get("registered_at"), str)
    )


async def _check_risex_session_signer(wallet: str) -> SignerResult:
    account = _normalize_address(wallet)
    if account != _EXPECTED_WALLET:
        raise _safety_error()
    record = _current_record()
    async with _session() as session:
        await _identity(session)
        active = await _authoritative_state(session, account, record)
    if active:
        record = _mark_active()
        return SignerResult(SignerState.ACTIVE, record.signer, record.expiration)
    state = (SignerState.SPENT_UNKNOWN
             if record.state is not SignerState.CREATED else SignerState.CREATED)
    return SignerResult(state, record.signer, record.expiration)


async def check_risex_session_signer(wallet: str) -> SignerResult:
    return await _sanitized(_check_risex_session_signer(wallet))


def _load_and_validate_secrets(record: _Record,
                               main_secret_loader: _Callable[[], bytes]) -> tuple[bytes, bytes]:
    home_fd = _open_home()
    try:
        signer_secret = _load_credential(home_fd)
    finally:
        os.close(home_fd)
    if _derive_address(signer_secret) != record.signer:
        raise _safety_error()
    try:
        main_secret = main_secret_loader()
    except BaseException as error:
        if isinstance(error, _asyncio.CancelledError):
            raise
        raise _safety_error() from None
    if not isinstance(main_secret, bytes) or len(main_secret) != 32:
        raise _safety_error()
    if _derive_address(main_secret) != _EXPECTED_WALLET:
        raise _safety_error()
    return main_secret, signer_secret


async def _register_risex_session_signer(
    wallet: str, *, intent: str, main_secret_loader: _Callable[[], bytes]
) -> SignerResult:
    account = _normalize_address(wallet)
    if account != _EXPECTED_WALLET or intent != _REGISTER_INTENT:
        raise _safety_error()
    record = _current_record()
    async with _session() as session:
        await _identity(session)
        if await _authoritative_state(session, account, record):
            record = _mark_active()
            return SignerResult(SignerState.ACTIVE, record.signer, record.expiration)
        if record.state is not SignerState.CREATED:
            return SignerResult(SignerState.SPENT_UNKNOWN, record.signer,
                                record.expiration)
        nonce = await _nonce(session, account)
        now = _now_unix()
        if not now < record.expiration <= 2**32 - 1:
            raise _safety_error()
        main_secret, signer_secret = _load_and_validate_secrets(
            record, main_secret_loader
        )
        register_typed = _build_register_typed_data(
            account, record.signer, record.expiration,
            nonce.signed_anchor, nonce.signed_bitmap,
        )
        verify_typed = _build_verify_typed_data(
            account, nonce.signed_anchor, nonce.signed_bitmap
        )
        account_signature = _sign_typed_data(main_secret, register_typed)
        signer_signature = _sign_typed_data(signer_secret, verify_typed)
        await _identity(session)
        repeated_nonce = await _nonce(session, account)
        if repeated_nonce != nonce:
            raise _safety_error()
        if not _claim_registration():
            return SignerResult(SignerState.SPENT_UNKNOWN, record.signer,
                                record.expiration)
        request = {
            "account": account,
            "signer": record.signer,
            "message": _MESSAGE,
            "nonce_anchor": str(nonce.signed_anchor),
            "expiration": str(record.expiration),
            "account_signature": account_signature,
            "signer_signature": signer_signature,
            "nonce_bitmap_index": nonce.signed_bitmap,
            "label": _LABEL,
        }
        try:
            await _request_json(session, "POST", _REGISTER, json=request)
        except _asyncio.CancelledError:
            raise
        except Exception:
            pass
        try:
            active = await _authoritative_state(session, account, record)
        except _asyncio.CancelledError:
            raise
        except Exception:
            active = False
        if active:
            record = _mark_active()
            return SignerResult(SignerState.ACTIVE, record.signer, record.expiration)
        return SignerResult(SignerState.SPENT_UNKNOWN, record.signer,
                            record.expiration)


async def register_risex_session_signer(
    wallet: str, *, intent: str, main_secret_loader: _Callable[[], bytes]
) -> SignerResult:
    return await _sanitized(_register_risex_session_signer(
        wallet, intent=intent, main_secret_loader=main_secret_loader
    ))


async def _sanitized(operation: _Any) -> SignerResult:
    try:
        return await operation
    except _asyncio.CancelledError:
        raise
    except SignerSafetyError:
        raise
    except Exception:
        raise _safety_error() from None
