"""Bounded Extended mainnet read-only account discovery.

This module is reached only from the explicit ``discover`` operator command.
It accepts one hidden API key, performs the two official account reads, and
hands the selected public identity to the protected local onboarding store.
There is deliberately no write credential, signing, order, transfer, or
withdrawal surface here.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
import getpass
import inspect
import json
import sys
from typing import Any, Callable, Mapping, Sequence

import aiohttp

from . import extended_mainnet_credential_onboarding as onboarding


VENUE = "Extended"
ENVIRONMENT = "MAINNET"
DISCOVERED = "DISCOVERED"
PROVISIONED = onboarding.PROVISIONED
BLOCKED = onboarding.BLOCKED
NO_MAINNET_WRITE_AUTHORITY = onboarding.NO_MAINNET_WRITE_AUTHORITY

MAINNET_REST_BASE_URL = "https://api.starknet.extended.exchange/api/v1"
REST_BASE_URL = MAINNET_REST_BASE_URL
ACCOUNT_INFO_PATH = "/user/account/info"
ACCOUNTS_PATH = "/user/accounts"
USER_AGENT = "X10PythonTradingClient/2.5.0"
API_KEY_HEADER = "X-Api-Key"
REQUEST_TIMEOUT_SECONDS = 10
MAX_RESPONSE_BYTES = 65_536
MAX_ACCOUNT_ROWS = 64

FAILURE_CLASSES = frozenset(
    {"TRANSPORT", "HTTP", "SCHEMA", "AUTH", "IDENTITY", "SAFETY"}
)

_INFO_REQUIRED_FIELDS = frozenset(
    {"status", "l2Key", "l2Vault", "accountId", "bridgeStarknetAddress"}
)
_ACCOUNT_REQUIRED_FIELDS = frozenset(
    {
        "accountId",
        "accountIndex",
        "status",
        "l2Key",
        "l2Vault",
        "bridgeStarknetAddress",
        "accountIndexForKeyGeneration",
    }
)
_ALLOWED_STATUSES = frozenset({"ACTIVE", "INACTIVE", "DISABLED", "SUSPENDED"})


class DiscoveryViolation(ValueError):
    """Fixed, redaction-safe failure from the bounded read-only gate."""

    def __init__(self, reason: str, failure_class: str) -> None:
        if (
            type(reason) is not str
            or not reason
            or any(ord(char) < 33 for char in reason)
            or failure_class not in FAILURE_CLASSES
        ):
            raise ValueError("invalid discovery failure")
        self.reason = reason
        self.failure_class = failure_class
        super().__init__(reason)


class DiscoveryTransportError(Exception):
    """Retryable transport interruption with no raw error text."""


@dataclass(frozen=True)
class RestRequest:
    method: str
    url: str
    path: str
    headers: Mapping[str, str]
    attempt: int
    timeout_seconds: int = REQUEST_TIMEOUT_SECONDS


@dataclass(frozen=True)
class RestReply:
    status: int
    final_url: str
    body: Any
    body_bytes: int | None = None
    complete: bool = True


@dataclass(frozen=True)
class AccountInfoObservation:
    account_id: int
    account_index: int | None
    l2_key: str
    l2_vault: int
    status: str

    def public_identity_tuple(self) -> tuple[int, str, int]:
        return self.account_id, self.l2_key, self.l2_vault


@dataclass(frozen=True)
class AccountCandidate:
    account_id: int
    account_index: int
    l2_key: str
    l2_vault: int
    status: str

    def public_identity(self) -> onboarding.ExtendedPublicIdentity:
        return onboarding.ExtendedPublicIdentity.from_inputs(
            str(self.account_id),
            str(self.account_index),
            self.l2_key,
            str(self.l2_vault),
        )

    def public_identity_tuple(self) -> tuple[int, str, int]:
        return self.account_id, self.l2_key, self.l2_vault

    def public_metadata(self) -> dict[str, Any]:
        """Return only fields safe to display for deterministic selection."""

        return {
            "account_id": self.account_id,
            "account_index": self.account_index,
            "l2_key": self.l2_key,
            "l2_vault": self.l2_vault,
            "status": self.status,
        }


@dataclass(frozen=True)
class IdentityDiscovery:
    status: str
    reason: str
    failure_class: str | None
    account_info: AccountInfoObservation | None
    candidates: tuple[AccountCandidate, ...]
    identity: onboarding.ExtendedPublicIdentity | None
    attempts: Mapping[str, int]

    @property
    def discovered(self) -> bool:
        return self.status == DISCOVERED and self.identity is not None

    def evidence(self) -> str:
        return json.dumps(self.to_metadata(), sort_keys=True, separators=(",", ":"))

    def to_metadata(self) -> dict[str, Any]:
        return {
            "attempts": dict(self.attempts),
            "account_count": len(self.candidates),
            "failure_class": self.failure_class,
            "identity": (
                None if self.identity is None else self.identity.to_metadata()
            ),
            "mainnet_write_authority": NO_MAINNET_WRITE_AUTHORITY,
            "reason": self.reason,
            "status": self.status,
            "write_ready": False,
        }


@dataclass(frozen=True)
class OnboardingDiscoveryResult:
    status: str
    reason: str
    failure_class: str | None
    identity: onboarding.ExtendedPublicIdentity | None
    candidate_count: int
    attempts: Mapping[str, int]
    mainnet_write_authority: str = NO_MAINNET_WRITE_AUTHORITY

    @property
    def provisioned(self) -> bool:
        return self.status == PROVISIONED

    def to_metadata(self) -> dict[str, Any]:
        return {
            "attempts": dict(self.attempts),
            "candidate_count": self.candidate_count,
            "failure_class": self.failure_class,
            "identity": (
                None if self.identity is None else self.identity.to_metadata()
            ),
            "mainnet_write_authority": self.mainnet_write_authority,
            "reason": self.reason,
            "status": self.status,
            "write_ready": False,
        }

    def evidence(self) -> str:
        return json.dumps(self.to_metadata(), sort_keys=True, separators=(",", ":"))


def _blocked(
    reason: str,
    failure_class: str,
    *,
    account_info: AccountInfoObservation | None = None,
    candidates: Sequence[AccountCandidate] = (),
    attempts: Mapping[str, int] | None = None,
) -> IdentityDiscovery:
    return IdentityDiscovery(
        BLOCKED,
        reason,
        failure_class,
        account_info,
        tuple(candidates),
        None,
        dict(attempts or {}),
    )


def _result_from_observation(
    observation: IdentityDiscovery,
    *,
    status: str | None = None,
    reason: str | None = None,
    failure_class: str | None = None,
) -> OnboardingDiscoveryResult:
    return OnboardingDiscoveryResult(
        status or observation.status,
        reason or observation.reason,
        failure_class if failure_class is not None else observation.failure_class,
        observation.identity,
        len(observation.candidates),
        observation.attempts,
    )


def _validate_api_key(value: Any) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise DiscoveryViolation("API_KEY_INVALID", "AUTH")
    if any(ord(char) < 33 or ord(char) > 126 for char in value):
        raise DiscoveryViolation("API_KEY_INVALID", "AUTH")
    try:
        value.encode("ascii")
    except UnicodeEncodeError:
        raise DiscoveryViolation("API_KEY_INVALID", "AUTH") from None
    if len(value.encode("ascii")) > onboarding.MAX_API_KEY_BYTES:
        raise DiscoveryViolation("API_KEY_INVALID", "AUTH")
    return value


def _canonical_integer(
    value: Any,
    reason: str,
    *,
    maximum: int = onboarding.MAX_DECIMAL_IDENTIFIER,
) -> int:
    if type(value) is bool:
        raise DiscoveryViolation(reason, "SCHEMA")
    if type(value) is int:
        parsed = value
    elif type(value) is str:
        if (
            not value
            or len(value) > onboarding.MAX_PUBLIC_INPUT_CHARS
            or not value.isdecimal()
            or (len(value) > 1 and value.startswith("0"))
        ):
            raise DiscoveryViolation(reason, "SCHEMA")
        try:
            parsed = int(value, 10)
        except ValueError:
            raise DiscoveryViolation(reason, "SCHEMA") from None
    else:
        raise DiscoveryViolation(reason, "SCHEMA")
    if parsed < 0 or parsed > maximum:
        raise DiscoveryViolation(reason, "SCHEMA")
    return parsed


def _canonical_l2_key(value: Any) -> str:
    if (
        type(value) is not str
        or not value
        or len(value) > onboarding.MAX_PUBLIC_INPUT_CHARS
        or not value.startswith("0x")
        or not value[2:]
        or any(char not in "0123456789abcdefABCDEF" for char in value[2:])
    ):
        raise DiscoveryViolation("ACCOUNT_L2_KEY_INVALID", "SCHEMA")
    try:
        parsed = int(value[2:], 16)
    except ValueError:
        raise DiscoveryViolation("ACCOUNT_L2_KEY_INVALID", "SCHEMA") from None
    if parsed <= 0:
        raise DiscoveryViolation("ACCOUNT_L2_KEY_INVALID", "SCHEMA")
    return f"0x{parsed:x}"


def _validate_bridge_field(value: Mapping[str, Any]) -> None:
    # The field is required by the official response shape.  Some valid
    # accounts expose no bridge address, so null is accepted but no other
    # malformed value is.
    bridge = value["bridgeStarknetAddress"]
    if bridge is not None and (
        type(bridge) is not str or not bridge or len(bridge) > onboarding.MAX_PUBLIC_INPUT_CHARS
    ):
        raise DiscoveryViolation("ACCOUNT_BRIDGE_ADDRESS_INVALID", "SCHEMA")


def _validate_status(value: Any) -> str:
    if type(value) is not str or value not in _ALLOWED_STATUSES:
        raise DiscoveryViolation("ACCOUNT_STATUS_INVALID", "SCHEMA")
    return value


def _wrapper_data(body: Any) -> Any:
    if not isinstance(body, Mapping) or "status" not in body or "data" not in body:
        raise DiscoveryViolation("RESPONSE_SCHEMA_INVALID", "SCHEMA")
    if type(body["status"]) is not str:
        raise DiscoveryViolation("RESPONSE_SCHEMA_INVALID", "SCHEMA")
    if body["status"] != "OK":
        raise DiscoveryViolation("AUTHENTICATION_REJECTED", "AUTH")
    if "error" in body and body["error"] is not None:
        raise DiscoveryViolation("AUTHENTICATION_REJECTED", "AUTH")
    return body["data"]


def _decode_info(body: Any) -> AccountInfoObservation:
    data = _wrapper_data(body)
    if not isinstance(data, Mapping) or not _INFO_REQUIRED_FIELDS.issubset(data):
        raise DiscoveryViolation("ACCOUNT_INFO_SCHEMA_INVALID", "SCHEMA")
    _validate_bridge_field(data)
    account_index: int | None = None
    if "accountIndex" in data:
        account_index = _canonical_integer(
            data["accountIndex"],
            "ACCOUNT_INDEX_INVALID",
            maximum=onboarding.MAX_ACCOUNT_INDEX,
        )
    return AccountInfoObservation(
        account_id=_canonical_integer(data["accountId"], "ACCOUNT_ID_INVALID"),
        account_index=account_index,
        l2_key=_canonical_l2_key(data["l2Key"]),
        l2_vault=_canonical_integer(data["l2Vault"], "ACCOUNT_L2_VAULT_INVALID"),
        status=_validate_status(data["status"]),
    )


def _decode_accounts(body: Any) -> tuple[AccountCandidate, ...]:
    data = _wrapper_data(body)
    if not isinstance(data, list) or not data or len(data) > MAX_ACCOUNT_ROWS:
        raise DiscoveryViolation("ACCOUNTS_SCHEMA_INVALID", "SCHEMA")
    result: list[AccountCandidate] = []
    seen_ids: set[int] = set()
    seen_indices: set[int] = set()
    for row in data:
        if not isinstance(row, Mapping) or not _ACCOUNT_REQUIRED_FIELDS.issubset(row):
            raise DiscoveryViolation("ACCOUNT_ROW_SCHEMA_INVALID", "SCHEMA")
        _validate_bridge_field(row)
        account_id = _canonical_integer(row["accountId"], "ACCOUNT_ID_INVALID")
        account_index = _canonical_integer(
            row["accountIndex"],
            "ACCOUNT_INDEX_INVALID",
            maximum=onboarding.MAX_ACCOUNT_INDEX,
        )
        _canonical_integer(
            row["accountIndexForKeyGeneration"],
            "ACCOUNT_KEY_INDEX_INVALID",
            maximum=onboarding.MAX_ACCOUNT_INDEX,
        )
        if account_id in seen_ids or account_index in seen_indices:
            raise DiscoveryViolation("ACCOUNT_LIST_DUPLICATE", "IDENTITY")
        seen_ids.add(account_id)
        seen_indices.add(account_index)
        result.append(
            AccountCandidate(
                account_id=account_id,
                account_index=account_index,
                l2_key=_canonical_l2_key(row["l2Key"]),
                l2_vault=_canonical_integer(
                    row["l2Vault"], "ACCOUNT_L2_VAULT_INVALID"
                ),
                status=_validate_status(row["status"]),
            )
        )
    return tuple(result)


def _coerce_reply(value: Any) -> RestReply:
    if isinstance(value, RestReply):
        reply = value
    elif isinstance(value, Mapping):
        required = {"status", "final_url", "body"}
        if not required.issubset(value):
            raise DiscoveryViolation("TRANSPORT_REPLY_SCHEMA_INVALID", "SCHEMA")
        reply = RestReply(
            status=value["status"],
            final_url=value["final_url"],
            body=value["body"],
            body_bytes=value.get("body_bytes"),
            complete=value.get("complete", True),
        )
    else:
        raise DiscoveryViolation("TRANSPORT_REPLY_SCHEMA_INVALID", "SCHEMA")
    if (
        type(reply.status) is not int
        or type(reply.final_url) is not str
        or type(reply.complete) is not bool
        or (
            reply.body_bytes is not None
            and (type(reply.body_bytes) is not int or reply.body_bytes < 0)
        )
    ):
        raise DiscoveryViolation("TRANSPORT_REPLY_SCHEMA_INVALID", "SCHEMA")
    if not reply.complete:
        raise DiscoveryTransportError()
    if reply.body_bytes is not None and reply.body_bytes > MAX_RESPONSE_BYTES:
        raise DiscoveryViolation("RESPONSE_TOO_LARGE", "SCHEMA")
    if reply.body_bytes is None:
        try:
            encoded = json.dumps(
                reply.body, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
        except (TypeError, UnicodeEncodeError, ValueError):
            raise DiscoveryViolation("RESPONSE_SCHEMA_INVALID", "SCHEMA") from None
        if len(encoded) > MAX_RESPONSE_BYTES:
            raise DiscoveryViolation("RESPONSE_TOO_LARGE", "SCHEMA")
    return reply


def _retryable_transport(error: BaseException) -> bool:
    return isinstance(
        error,
        (
            DiscoveryTransportError,
            aiohttp.ClientConnectionError,
            aiohttp.ClientPayloadError,
            asyncio.TimeoutError,
            asyncio.IncompleteReadError,
            ConnectionError,
            EOFError,
            OSError,
        ),
    )


async def _get_with_one_retry(
    transport: Any,
    *,
    path: str,
    api_key: str,
    attempts: dict[str, int],
) -> RestReply:
    url = f"{MAINNET_REST_BASE_URL}{path}"
    for attempt in (1, 2):
        attempts[path] = attempts.get(path, 0) + 1
        request = RestRequest(
            method="GET",
            url=url,
            path=path,
            headers={"User-Agent": USER_AGENT, API_KEY_HEADER: api_key},
            attempt=attempt,
        )
        try:
            result = transport.get(request)
            if inspect.isawaitable(result):
                result = await result
            reply = _coerce_reply(result)
        except DiscoveryViolation:
            raise
        except BaseException as exc:
            if _retryable_transport(exc):
                if attempt == 1:
                    continue
                raise DiscoveryViolation("TRANSPORT_RETRY_EXHAUSTED", "TRANSPORT") from None
            if isinstance(exc, asyncio.CancelledError):
                raise
            raise DiscoveryViolation("UNCLASSIFIED_FAILURE", "SAFETY") from None

        if reply.final_url != url:
            raise DiscoveryViolation("REDIRECT_FORBIDDEN", "SAFETY")
        if reply.status in {401, 403}:
            raise DiscoveryViolation("AUTHENTICATION_REJECTED", "AUTH")
        if reply.status != 200:
            raise DiscoveryViolation("HTTP_STATUS_UNACCEPTED", "HTTP")
        return reply
    raise DiscoveryViolation("TRANSPORT_RETRY_EXHAUSTED", "TRANSPORT")


def _reconcile_info_and_accounts(
    info: AccountInfoObservation,
    candidates: tuple[AccountCandidate, ...],
) -> None:
    matching = [
        candidate
        for candidate in candidates
        if candidate.account_id == info.account_id
        and candidate.l2_key == info.l2_key
        and candidate.l2_vault == info.l2_vault
    ]
    if len(matching) != 1:
        raise DiscoveryViolation("ACCOUNT_INFO_ACCOUNTS_DISAGREE", "IDENTITY")
    if (
        info.account_index is not None
        and matching[0].account_index != info.account_index
    ):
        raise DiscoveryViolation("ACCOUNT_INDEX_DISAGREEMENT", "IDENTITY")
    if matching[0].status != info.status:
        raise DiscoveryViolation("ACCOUNT_STATUS_DISAGREEMENT", "IDENTITY")


async def discover_account_candidates(
    api_key: str,
    *,
    transport: Any | None = None,
) -> IdentityDiscovery:
    """Read and reconcile both official account endpoints once each.

    Each endpoint gets an initial request and at most one retry, and only a
    retryable transport interruption can consume that second attempt.
    """

    attempts: dict[str, int] = {}
    try:
        key = _validate_api_key(api_key)
    except DiscoveryViolation as exc:
        return _blocked(exc.reason, exc.failure_class, attempts=attempts)

    owned_transport = transport is None
    if owned_transport:
        transport = MainnetRestTransport()
    info: AccountInfoObservation | None = None
    candidates: tuple[AccountCandidate, ...] = ()
    try:
        try:
            info_reply = await _get_with_one_retry(
                transport,
                path=ACCOUNT_INFO_PATH,
                api_key=key,
                attempts=attempts,
            )
            info = _decode_info(info_reply.body)
            accounts_reply = await _get_with_one_retry(
                transport,
                path=ACCOUNTS_PATH,
                api_key=key,
                attempts=attempts,
            )
            candidates = _decode_accounts(accounts_reply.body)
            _reconcile_info_and_accounts(info, candidates)
            return IdentityDiscovery(
                DISCOVERED,
                "ACCOUNT_IDENTITY_DISCOVERED",
                None,
                info,
                candidates,
                None,
                attempts,
            )
        except DiscoveryViolation as exc:
            return _blocked(
                exc.reason,
                exc.failure_class,
                account_info=info,
                candidates=candidates,
                attempts=attempts,
            )
    finally:
        if owned_transport:
            await transport.close()


def _parse_operator_hint(
    l2_key: Any,
    l2_vault: Any,
) -> tuple[str, int]:
    if (
        l2_key is None
        or l2_vault is None
        or (type(l2_key) is str and not l2_key.strip())
        or (type(l2_vault) is str and not l2_vault.strip())
    ):
        raise DiscoveryViolation("ACCOUNT_SELECTION_REQUIRED", "IDENTITY")
    return (
        _canonical_l2_key(l2_key),
        _canonical_integer(l2_vault, "ACCOUNT_L2_VAULT_INVALID"),
    )


def resolve_identity(
    observation: IdentityDiscovery,
    *,
    operator_l2_key: Any = None,
    operator_l2_vault: Any = None,
    require_operator_selection: bool = False,
) -> IdentityDiscovery:
    """Select one authoritative public identity without account-ID guessing."""

    if observation.status != DISCOVERED or observation.account_info is None:
        return observation
    if not observation.candidates:
        return _blocked(
            "ACCOUNTS_SCHEMA_INVALID",
            "SCHEMA",
            account_info=observation.account_info,
            attempts=observation.attempts,
        )
    matching_info = [
        candidate
        for candidate in observation.candidates
        if candidate.public_identity_tuple()
        == observation.account_info.public_identity_tuple()
    ]
    if len(matching_info) != 1:
        return _blocked(
            "ACCOUNT_INFO_ACCOUNTS_DISAGREE",
            "IDENTITY",
            account_info=observation.account_info,
            candidates=observation.candidates,
            attempts=observation.attempts,
        )

    has_hint = operator_l2_key is not None or operator_l2_vault is not None
    if has_hint:
        try:
            hint_key, hint_vault = _parse_operator_hint(
                operator_l2_key, operator_l2_vault
            )
        except DiscoveryViolation as exc:
            return _blocked(
                exc.reason,
                exc.failure_class,
                account_info=observation.account_info,
                candidates=observation.candidates,
                attempts=observation.attempts,
            )
        matching_hint = [
            candidate
            for candidate in observation.candidates
            if candidate.l2_key == hint_key and candidate.l2_vault == hint_vault
        ]
        if len(matching_hint) != 1:
            return _blocked(
                "PUBLIC_IDENTITY_MISMATCH",
                "IDENTITY",
                account_info=observation.account_info,
                candidates=observation.candidates,
                attempts=observation.attempts,
            )
        selected = matching_hint[0]
        if selected != matching_info[0]:
            return _blocked(
                "PUBLIC_IDENTITY_MISMATCH",
                "IDENTITY",
                account_info=observation.account_info,
                candidates=observation.candidates,
                attempts=observation.attempts,
            )
    elif require_operator_selection and len(observation.candidates) > 1:
        return _blocked(
            "ACCOUNT_SELECTION_REQUIRED",
            "IDENTITY",
            account_info=observation.account_info,
            candidates=observation.candidates,
            attempts=observation.attempts,
        )
    else:
        selected = matching_info[0]

    if selected.status != "ACTIVE":
        return _blocked(
            "ACCOUNT_INACTIVE",
            "IDENTITY",
            account_info=observation.account_info,
            candidates=observation.candidates,
            attempts=observation.attempts,
        )
    return replace(observation, identity=selected.public_identity())


async def discover_mainnet_identity(
    api_key: str,
    *,
    operator_l2_key: Any = None,
    operator_l2_vault: Any = None,
    transport: Any | None = None,
    require_operator_selection: bool = False,
) -> IdentityDiscovery:
    """Discover one exact mainnet identity using only the API key and reads."""

    observation = await discover_account_candidates(api_key, transport=transport)
    return resolve_identity(
        observation,
        operator_l2_key=operator_l2_key,
        operator_l2_vault=operator_l2_vault,
        require_operator_selection=require_operator_selection,
    )


def _read_hidden(
    input_fn: Callable[[str], str],
    prompt: str,
) -> str:
    try:
        value = input_fn(prompt)
    except BaseException:
        raise DiscoveryViolation("INPUT_CANCELLED", "SAFETY") from None
    if type(value) is not str:
        raise DiscoveryViolation("INPUT_INVALID", "SAFETY")
    return value


def _display_candidates(
    candidates: Sequence[AccountCandidate],
    output: Any,
) -> None:
    print("Authoritative Extended sub-accounts:", file=output)
    for candidate in candidates:
        print(
            json.dumps(candidate.public_metadata(), sort_keys=True),
            file=output,
        )


async def run_discovery(
    *,
    input_fn: Callable[[str], str] | None = None,
    transport: Any | None = None,
    output: Any | None = None,
) -> OnboardingDiscoveryResult:
    """Run hidden-input discovery and persist only the selected read credential."""

    input_fn = getpass.getpass if input_fn is None else input_fn
    output = sys.stderr if output is None else output
    before = onboarding.inspect_protected_credentials()
    if before.reason not in {
        "PROTECTED_DIRECTORY_MISSING",
        "PROTECTED_DIRECTORY_PARENT_MISSING",
    }:
        return OnboardingDiscoveryResult(
            BLOCKED,
            "PROTECTED_PATH_ALREADY_EXISTS"
            if before.directory_present
            else before.reason,
            "SAFETY",
            None,
            0,
            {},
        )

    key_buffer = bytearray()
    try:
        # This is intentionally the first input operation.  No account ID,
        # client ID, public key, vault, or other operator field is accepted
        # before the authenticated read-only discovery begins.
        raw_key = _read_hidden(input_fn, "Extended read-only API key (hidden): ")
        key_buffer = onboarding._secret_bytes(
            raw_key,
            maximum=onboarding.MAX_API_KEY_BYTES,
            code="API_KEY_INVALID",
        )
        api_key = bytes(key_buffer).decode("ascii")
        observation = await discover_account_candidates(api_key, transport=transport)
        if observation.status != DISCOVERED:
            return _result_from_observation(observation)
        if len(observation.candidates) > 1:
            _display_candidates(observation.candidates, output)
            operator_l2_key = _read_hidden(
                input_fn,
                "Extended Stark public key (hidden public metadata): ",
            )
            operator_l2_vault = _read_hidden(
                input_fn,
                "Extended Vault number (hidden public metadata): ",
            )
            observation = resolve_identity(
                observation,
                operator_l2_key=operator_l2_key,
                operator_l2_vault=operator_l2_vault,
                require_operator_selection=True,
            )
        else:
            observation = resolve_identity(observation)
        if not observation.discovered:
            return _result_from_observation(observation)
        stored = onboarding.persist_discovered_credentials(
            observation.identity,
            api_key,
        )
        if not stored.provisioned:
            return _result_from_observation(
                observation,
                status=BLOCKED,
                reason=stored.reason,
                failure_class="SAFETY",
            )
        return _result_from_observation(
            observation,
            status=PROVISIONED,
            reason="ACCOUNT_IDENTITY_DISCOVERED_AND_PROVISIONED",
            failure_class=None,
        )
    except onboarding.CredentialOnboardingError as exc:
        return OnboardingDiscoveryResult(
            BLOCKED,
            exc.code,
            "AUTH" if exc.code == "API_KEY_INVALID" else "SAFETY",
            None,
            0,
            {},
        )
    except DiscoveryViolation as exc:
        return OnboardingDiscoveryResult(
            BLOCKED,
            exc.reason,
            exc.failure_class,
            None,
            0,
            {},
        )
    finally:
        onboarding._zeroize(key_buffer)


class MainnetRestTransport:
    """Direct mainnet GET transport used only by the explicit discovery gate."""

    def __init__(self, timeout_seconds: int = REQUEST_TIMEOUT_SECONDS) -> None:
        if type(timeout_seconds) is not int or timeout_seconds <= 0:
            raise ValueError("invalid timeout")
        self._timeout_seconds = timeout_seconds
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout_seconds),
            trust_env=False,
        )

    async def get(self, request: RestRequest) -> RestReply:
        try:
            async with self._session.get(
                request.url,
                headers=dict(request.headers),
                allow_redirects=False,
                proxy=None,
            ) as response:
                content_length = response.content_length
                raw = await response.content.read(MAX_RESPONSE_BYTES + 1)
                if content_length is not None and content_length > MAX_RESPONSE_BYTES:
                    raise DiscoveryViolation("RESPONSE_TOO_LARGE", "SCHEMA")
                if content_length is not None and content_length > len(raw):
                    raise DiscoveryTransportError()
                if response.status != 200:
                    return RestReply(
                        status=response.status,
                        final_url=str(response.url),
                        body=None,
                    )
                if len(raw) > MAX_RESPONSE_BYTES:
                    raise DiscoveryViolation("RESPONSE_TOO_LARGE", "SCHEMA")
                try:
                    body = json.loads(raw)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    raise DiscoveryViolation("RESPONSE_SCHEMA_INVALID", "SCHEMA") from None
                return RestReply(
                    status=response.status,
                    final_url=str(response.url),
                    body=body,
                    body_bytes=len(raw),
                )
        except DiscoveryViolation:
            raise
        except (
            aiohttp.ClientPayloadError,
            aiohttp.ClientConnectionError,
            asyncio.IncompleteReadError,
            EOFError,
            OSError,
        ):
            raise DiscoveryTransportError() from None
        except asyncio.TimeoutError:
            raise DiscoveryTransportError() from None
        except aiohttp.ClientError:
            raise DiscoveryTransportError() from None

    async def close(self) -> None:
        await self._session.close()


def run_cli() -> int:
    """Visible command entry point with hidden input and sanitized output."""

    try:
        result = asyncio.run(run_discovery())
    except KeyboardInterrupt:
        result = OnboardingDiscoveryResult(
            BLOCKED,
            "INPUT_CANCELLED",
            "SAFETY",
            None,
            0,
            {},
        )
    except Exception:
        result = OnboardingDiscoveryResult(
            BLOCKED,
            "UNEXPECTED_FAILURE",
            "SAFETY",
            None,
            0,
            {},
        )
    print(result.evidence())
    return 0 if result.provisioned else 1


__all__ = [
    "ACCOUNT_INFO_PATH",
    "ACCOUNTS_PATH",
    "API_KEY_HEADER",
    "AccountCandidate",
    "AccountInfoObservation",
    "BLOCKED",
    "DISCOVERED",
    "DiscoveryTransportError",
    "DiscoveryViolation",
    "ENVIRONMENT",
    "IdentityDiscovery",
    "MAINNET_REST_BASE_URL",
    "MainnetRestTransport",
    "NO_MAINNET_WRITE_AUTHORITY",
    "OnboardingDiscoveryResult",
    "PROVISIONED",
    "REST_BASE_URL",
    "REQUEST_TIMEOUT_SECONDS",
    "RestReply",
    "RestRequest",
    "USER_AGENT",
    "discover_account_candidates",
    "discover_mainnet_identity",
    "resolve_identity",
    "run_cli",
    "run_discovery",
]
