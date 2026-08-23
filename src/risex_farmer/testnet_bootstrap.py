from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
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


class BootstrapStatus(Enum):
    READY = "READY"
    ALREADY_READY = "ALREADY_READY"
    SUBMITTED_UNVERIFIED = "SUBMITTED_UNVERIFIED"
    UNKNOWN_AMBIGUOUS = "UNKNOWN_AMBIGUOUS"
    REJECTED = "REJECTED"


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
    dispatched = False
    try:
        async with _session() as session:
            try:
                identity = await _identity(session)
                preflight = await _balance(session, expected_wallet, identity)
                if preflight.ready:
                    return BootstrapResult(
                        BootstrapStatus.ALREADY_READY,
                        preflight.balance_raw,
                        "authoritative balance already positive",
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
                return BootstrapResult(
                    BootstrapStatus.READY,
                    postcondition.balance_raw,
                    "authoritative balance is positive",
                )
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
    return BootstrapResult(
        BootstrapStatus.UNKNOWN_AMBIGUOUS,
        message="testnet deposit result is ambiguous",
    )
