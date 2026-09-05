from __future__ import annotations

import asyncio
import base64
from dataclasses import replace
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import risex_spread_shadow.risex_fee_read as fee_read


ACCOUNT = "0x" + "11" * 20
SESSION_SIGNER = "0x" + "22" * 20
OTHER_ACCOUNT = "0x" + "33" * 20
SIGNATURE = "0x" + "ab" * 65
NOW = 1_775_000_000.25


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


ACCESS_TOKEN = ".".join(
    (
        _b64(b'{"alg":"HS256","typ":"JWT"}'),
        _b64(b'{"sub":"fixture"}'),
        _b64(b"fixture-signature"),
    )
)
REFRESH_TOKEN = "fixture-refresh-token"


def envelope(data: dict[str, Any]) -> dict[str, Any]:
    return {"data": data, "request_id": "fixture-request-id"}


def session_status_body(**changes: Any) -> dict[str, Any]:
    data: dict[str, Any] = {"status": 1, "status_description": "Active"}
    data.update(changes)
    return envelope(data)


def domain_body(**changes: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "name": fee_read.MAINNET_DOMAIN_NAME,
        "version": fee_read.MAINNET_DOMAIN_VERSION,
        "chain_id": fee_read.MAINNET_CHAIN_ID,
        "verifying_contract": fee_read.MAINNET_AUTH_CONTRACT,
    }
    data.update(changes)
    return envelope(data)


def nonce_body(value: str = "0x10") -> dict[str, Any]:
    return envelope({"nonce": value})


def login_body(**changes: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "access_token": ACCESS_TOKEN,
        "refresh_token": REFRESH_TOKEN,
        "expires_in": 900,
        "token_type": "Bearer",
    }
    data.update(changes)
    return envelope(data)


def fees_body(**changes: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "tier": 1,
        "taker_bps": 2.5,
        "maker_bps": -0.1,
        "schedule": [
            {"tier": 2, "threshold_usd": "5000", "taker_bps": 2, "maker_bps": 0},
            {"tier": 1, "threshold_usd": "0", "taker_bps": 2.5, "maker_bps": -0.1},
        ],
        "trial_tier": 3,
        "trial_ends_at": "",
        "earned_tier": 1,
        "irrelevant_private-looking-field": "must-not-be-copied",
    }
    data.update(changes)
    return envelope(data)


def expected_url(
    method: str,
    path: str,
    query: tuple[tuple[str, str], ...] = (),
) -> str:
    return str(fee_read.FixedRisexFeeReadTransport._target(method, path, query))


def observation(
    method: str,
    path: str,
    body: Any,
    *,
    query: tuple[tuple[str, str], ...] = (),
    status: int = 200,
    final_url: str | None = None,
) -> fee_read.HttpObservation:
    return fee_read.HttpObservation(
        status=status,
        final_url=expected_url(method, path, query) if final_url is None else final_url,
        body=body,
    )


class FakeFiles:
    def __init__(self, *, required: bool = True, optional_protected: bool = True) -> None:
        self.all_required_protected = required
        self._optional_protected = optional_protected

    def for_name(self, name: str) -> Any:
        return SimpleNamespace(
            present=name == fee_read.REGISTRATION_INTENT_FILENAME,
            protected=self._optional_protected,
        )


def identity(**changes: Any) -> Any:
    value = SimpleNamespace(
        environment="MAINNET",
        chain_id=fee_read.MAINNET_CHAIN_ID,
        verifying_contract=fee_read.MAINNET_AUTH_CONTRACT,
        venue="RISEx",
        registration_status="NOT_PREPARED",
        schema_version=1,
        wallet_address=ACCOUNT,
        session_signer_address=SESSION_SIGNER,
        expiration=int(NOW) + 3600,
    )
    for name, replacement in changes.items():
        setattr(value, name, replacement)
    return value


class FakeCapability:
    def __init__(self, address: str = ACCOUNT, signature: str = SIGNATURE) -> None:
        self.address = address
        self.signature = signature
        self.typed_data: dict[str, Any] | None = None
        self.closed = False

    def wallet_address(self) -> str:
        return self.address

    def sign_login(self, typed_data: dict[str, Any]) -> str:
        self.typed_data = typed_data
        return self.signature

    def close(self) -> None:
        self.closed = True


class FakeTransport:
    def __init__(self, responses: dict[tuple[str, str], list[Any]]) -> None:
        self.responses = {key: list(value) for key, value in responses.items()}
        self.calls: list[dict[str, Any]] = []
        self.closed = False

    async def request(
        self,
        method: str,
        path: str,
        *,
        query: tuple[tuple[str, str], ...] = (),
        body: dict[str, Any] | None = None,
        bearer_token: str | None = None,
    ) -> fee_read.HttpObservation:
        self.calls.append(
            {
                "method": method,
                "path": path,
                "query": query,
                "body": None if body is None else dict(body),
                "bearer_token": bearer_token,
            }
        )
        key = (method, path)
        if key not in self.responses or not self.responses[key]:
            raise AssertionError(f"unexpected fixture call: {key}")
        result = self.responses[key].pop(0)
        if isinstance(result, BaseException):
            raise result
        return result

    async def close(self) -> None:
        self.closed = True


def dependencies(
    transport: FakeTransport,
    capability: FakeCapability | None = None,
    *,
    files: FakeFiles | None = None,
    stored_identity: Any | None = None,
    session_signer: str | None = None,
) -> fee_read._Dependencies:
    capability = capability or FakeCapability()
    return fee_read._Dependencies(
        inspect_files=lambda: files or FakeFiles(),
        read_identity=lambda: stored_identity or identity(),
        owner_capability_factory=lambda: capability,
        transport_factory=lambda: transport,
        clock=lambda: NOW,
        read_session_signer=(
            None if session_signer is None else lambda: session_signer
        ),
    )


def complete_transport() -> FakeTransport:
    status_query = (("account", ACCOUNT), ("signer", SESSION_SIGNER))
    nonce_query = (("account", ACCOUNT),)
    return FakeTransport(
        {
            ("GET", fee_read.DOMAIN_PATH): [
                observation("GET", fee_read.DOMAIN_PATH, domain_body())
            ],
            ("GET", fee_read.SESSION_KEY_STATUS_PATH): [
                observation(
                    "GET",
                    fee_read.SESSION_KEY_STATUS_PATH,
                    session_status_body(),
                    query=status_query,
                )
            ],
            ("GET", fee_read.NONCE_PATH): [
                observation(
                    "GET", fee_read.NONCE_PATH, nonce_body(), query=nonce_query
                )
            ],
            ("POST", fee_read.LOGIN_PATH): [
                observation("POST", fee_read.LOGIN_PATH, login_body())
            ],
            ("GET", fee_read.FEES_PATH): [
                observation("GET", fee_read.FEES_PATH, fees_body())
            ],
        }
    )


@pytest.mark.asyncio
async def test_success_binds_account_nonce_login_and_one_caller_owned_fee_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = complete_transport()
    capability = FakeCapability()
    monkeypatch.setattr(fee_read, "_recover_login", lambda _data, _signature: ACCOUNT)

    report = await fee_read._run_with_dependencies(
        dependencies(transport, capability, session_signer=SESSION_SIGNER)
    )

    assert report.status == fee_read.READY
    assert report.terminal_classification == "COMPLETE"
    assert report.reason == "FEE_READ_COMPLETE"
    assert report.tier == 1
    assert report.maker_bps == "-0.1"
    assert report.taker_bps == "2.5"
    assert report.schedule == (
        {"maker_bps": "-0.1", "taker_bps": "2.5", "threshold_usd": "0", "tier": 1},
        {"maker_bps": "0", "taker_bps": "2", "threshold_usd": "5000", "tier": 2},
    )
    assert report.trial_tier == 3
    assert report.trial_ends_at is None
    assert report.earned_tier == 1
    assert [call["path"] for call in transport.calls] == [
        fee_read.DOMAIN_PATH,
        fee_read.SESSION_KEY_STATUS_PATH,
        fee_read.NONCE_PATH,
        fee_read.LOGIN_PATH,
        fee_read.FEES_PATH,
    ]
    assert transport.calls[0]["query"] == ()
    assert transport.calls[1]["query"] == (
        ("account", ACCOUNT),
        ("signer", SESSION_SIGNER),
    )
    assert transport.calls[2]["query"] == (("account", ACCOUNT),)
    assert transport.calls[3]["body"] == {
        "account": ACCOUNT,
        "nonce": "0x10",
        "deadline": int(NOW) + 300,
        "signature": SIGNATURE,
    }
    assert transport.calls[4]["bearer_token"] == ACCESS_TOKEN
    assert capability.typed_data == fee_read._login_typed_data(
        ACCOUNT, 0x10, int(NOW) + 300
    )
    assert capability.closed and transport.closed
    evidence = report.evidence()
    assert ACCESS_TOKEN not in evidence
    assert REFRESH_TOKEN not in evidence
    assert ACCOUNT not in evidence
    assert SESSION_SIGNER not in evidence
    assert "fixture-request-id" not in evidence
    assert "irrelevant_private-looking-field" not in evidence
    assert report.provenance["lighter_standard"]["cancel_latency_ms"] == 300


def test_login_typed_data_is_exact_and_uses_the_frozen_mainnet_domain() -> None:
    typed = fee_read._login_typed_data(ACCOUNT, 0xAB, 123)

    assert typed == {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            "Login": [
                {"name": "account", "type": "address"},
                {"name": "nonce", "type": "uint256"},
                {"name": "deadline", "type": "uint32"},
            ],
        },
        "primaryType": "Login",
        "domain": {
            "name": "RISEx",
            "version": "1",
            "chainId": 4153,
            "verifyingContract": fee_read.MAINNET_AUTH_CONTRACT,
        },
        "message": {"account": ACCOUNT, "nonce": 171, "deadline": 123},
    }


def test_domain_parser_accepts_official_string_chain_id_wire() -> None:
    fee_read._parse_domain(domain_body(chain_id=str(fee_read.MAINNET_CHAIN_ID)))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("changes", "expected_class", "expected_reason"),
    [
        ({"name": "Other"}, "IDENTITY", "DOMAIN_BINDING_MISMATCH"),
        ({"version": "2"}, "IDENTITY", "DOMAIN_BINDING_MISMATCH"),
        ({"chain_id": 4154}, "IDENTITY", "DOMAIN_BINDING_MISMATCH"),
        ({"chain_id": "4154"}, "IDENTITY", "DOMAIN_BINDING_MISMATCH"),
        ({"verifying_contract": OTHER_ACCOUNT}, "IDENTITY", "DOMAIN_BINDING_MISMATCH"),
    ],
)
async def test_domain_mismatch_is_terminal_before_owner_login_or_fees(
    changes: dict[str, Any], expected_class: str, expected_reason: str
) -> None:
    transport = complete_transport()
    transport.responses[("GET", fee_read.DOMAIN_PATH)] = [
        observation("GET", fee_read.DOMAIN_PATH, domain_body(**changes))
    ]
    called = False

    def owner_capability() -> Any:
        nonlocal called
        called = True
        raise AssertionError("owner capability must not be requested")

    deps = replace(
        dependencies(transport), owner_capability_factory=owner_capability
    )
    report = await fee_read._run_with_dependencies(deps)

    assert report.terminal_classification == expected_class
    assert report.reason == expected_reason
    assert not called
    assert [call["path"] for call in transport.calls] == [fee_read.DOMAIN_PATH]
    assert report.provenance["observed_endpoints"] == [fee_read.DOMAIN_PATH]


@pytest.mark.asyncio
async def test_domain_schema_failure_is_terminal_without_retry_or_owner_access() -> None:
    transport = complete_transport()
    invalid = domain_body()
    del invalid["data"]["verifying_contract"]
    transport.responses[("GET", fee_read.DOMAIN_PATH)] = [
        observation("GET", fee_read.DOMAIN_PATH, invalid)
    ]
    called = False

    def owner_capability() -> Any:
        nonlocal called
        called = True
        raise AssertionError("owner capability must not be requested")

    report = await fee_read._run_with_dependencies(
        replace(
            dependencies(transport), owner_capability_factory=owner_capability
        )
    )

    assert report.terminal_classification == "SCHEMA"
    assert report.reason == "DOMAIN_RESPONSE_INVALID"
    assert not called
    assert len(transport.calls) == 1
    assert transport.calls[0]["path"] == fee_read.DOMAIN_PATH


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [401, 403, 500])
async def test_domain_http_failure_is_terminal_without_retry_or_owner_access(
    status: int,
) -> None:
    transport = complete_transport()
    transport.responses[("GET", fee_read.DOMAIN_PATH)] = [
        observation("GET", fee_read.DOMAIN_PATH, {}, status=status)
    ]
    called = False

    def owner_capability() -> Any:
        nonlocal called
        called = True
        raise AssertionError("owner capability must not be requested")

    report = await fee_read._run_with_dependencies(
        replace(
            dependencies(transport), owner_capability_factory=owner_capability
        )
    )

    assert report.terminal_classification == (
        "AUTH" if status in {401, 403} else "HTTP"
    )
    assert report.reason in {"AUTH_RESPONSE_REJECTED", "HTTP_RESPONSE_REJECTED"}
    assert not called
    assert len(transport.calls) == 1
    assert transport.calls[0]["path"] == fee_read.DOMAIN_PATH


@pytest.mark.asyncio
async def test_domain_transport_failure_gets_only_one_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = complete_transport()
    domain_key = ("GET", fee_read.DOMAIN_PATH)
    domain = observation("GET", fee_read.DOMAIN_PATH, domain_body())
    transport.responses[domain_key] = [asyncio.TimeoutError(), domain]
    monkeypatch.setattr(fee_read, "_recover_login", lambda _data, _signature: ACCOUNT)

    report = await fee_read._run_with_dependencies(dependencies(transport))

    assert report.status == fee_read.READY
    assert report.terminal_classification == "COMPLETE"
    paths = [call["path"] for call in transport.calls]
    assert paths[:2] == [
        fee_read.DOMAIN_PATH,
        fee_read.DOMAIN_PATH,
    ]
    assert paths.count(fee_read.DOMAIN_PATH) == 2


def test_offline_owner_capability_derives_exact_wallet_signs_and_zeroizes() -> None:
    from eth_account import Account

    account = Account.create()
    secret = bytearray(account.key)
    capability = fee_read._OwnerLoginCapability(
        lambda _prompt: "0x" + bytes(secret).hex()
    )
    try:
        typed_data = fee_read._login_typed_data(account.address, 7, int(NOW) + 300)
        assert capability.wallet_address() == account.address.lower()
        signature = capability.sign_login(typed_data)
        assert fee_read._recover_login(typed_data, signature) == account.address.lower()
    finally:
        capability.close()
        secret[:] = b"\x00" * len(secret)
        secret.clear()
    assert capability._secret == bytearray()


@pytest.mark.asyncio
async def test_session_signer_status_mismatch_is_identity_failure_without_nonce_or_retry() -> None:
    transport = complete_transport()
    status_query = (("account", ACCOUNT), ("signer", SESSION_SIGNER))
    transport.responses[("GET", fee_read.SESSION_KEY_STATUS_PATH)] = [
        observation(
            "GET",
            fee_read.SESSION_KEY_STATUS_PATH,
            session_status_body(account=OTHER_ACCOUNT, signer=SESSION_SIGNER),
            query=status_query,
        )
    ]

    report = await fee_read._run_with_dependencies(dependencies(transport))

    assert report.terminal_classification == "IDENTITY"
    assert report.reason == "IDENTITY_BINDING_MISMATCH"
    assert [call["path"] for call in transport.calls] == [
        fee_read.DOMAIN_PATH,
        fee_read.SESSION_KEY_STATUS_PATH
    ]


@pytest.mark.asyncio
async def test_stored_session_signer_mismatch_is_identity_failure_before_network() -> None:
    transport = complete_transport()
    report = await fee_read._run_with_dependencies(
        dependencies(transport, session_signer=OTHER_ACCOUNT)
    )

    assert report.terminal_classification == "IDENTITY"
    assert report.reason == "SESSION_SIGNER_IDENTITY_MISMATCH"
    assert not transport.calls


@pytest.mark.asyncio
async def test_owner_key_identity_mismatch_stops_before_login() -> None:
    transport = complete_transport()
    capability = FakeCapability(address=OTHER_ACCOUNT)

    report = await fee_read._run_with_dependencies(
        dependencies(transport, capability)
    )

    assert report.terminal_classification == "IDENTITY"
    assert report.reason == "OWNER_KEY_IDENTITY_MISMATCH"
    assert [call["path"] for call in transport.calls] == [
        fee_read.DOMAIN_PATH,
        fee_read.SESSION_KEY_STATUS_PATH,
        fee_read.NONCE_PATH,
    ]
    assert capability.closed


@pytest.mark.asyncio
async def test_transport_timeout_gets_one_retry_then_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = complete_transport()
    status_key = ("GET", fee_read.SESSION_KEY_STATUS_PATH)
    status_query = (("account", ACCOUNT), ("signer", SESSION_SIGNER))
    status = observation(
        "GET",
        fee_read.SESSION_KEY_STATUS_PATH,
        session_status_body(),
        query=status_query,
    )
    transport.responses[status_key] = [asyncio.TimeoutError(), status]
    monkeypatch.setattr(fee_read, "_recover_login", lambda _data, _signature: ACCOUNT)

    report = await fee_read._run_with_dependencies(dependencies(transport))

    assert report.status == fee_read.READY
    assert [call["path"] for call in transport.calls].count(
        fee_read.SESSION_KEY_STATUS_PATH
    ) == 2


@pytest.mark.asyncio
async def test_second_transport_failure_is_terminal_and_does_not_advance() -> None:
    transport = complete_transport()
    status_key = ("GET", fee_read.SESSION_KEY_STATUS_PATH)
    transport.responses[status_key] = [asyncio.TimeoutError(), ConnectionError()]

    report = await fee_read._run_with_dependencies(dependencies(transport))

    assert report.terminal_classification == "TRANSPORT"
    assert report.reason == "TRANSPORT_RETRY_EXHAUSTED"
    assert len(transport.calls) == 3
    assert transport.calls[0]["path"] == fee_read.DOMAIN_PATH
    assert all(
        call["path"] == fee_read.SESSION_KEY_STATUS_PATH
        for call in transport.calls[1:]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [401, 403, 409, 500])
async def test_http_and_auth_failures_are_never_retried(status: int) -> None:
    transport = complete_transport()
    status_query = (("account", ACCOUNT), ("signer", SESSION_SIGNER))
    transport.responses[("GET", fee_read.SESSION_KEY_STATUS_PATH)] = [
        observation(
            "GET",
            fee_read.SESSION_KEY_STATUS_PATH,
            {},
            query=status_query,
            status=status,
        )
    ]

    report = await fee_read._run_with_dependencies(dependencies(transport))

    assert report.terminal_classification == ("AUTH" if status in {401, 403} else "HTTP")
    assert report.reason in {"AUTH_RESPONSE_REJECTED", "HTTP_RESPONSE_REJECTED"}
    assert len(transport.calls) == 2
    assert transport.calls[0]["path"] == fee_read.DOMAIN_PATH
    assert transport.calls[1]["path"] == fee_read.SESSION_KEY_STATUS_PATH


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response_path", "response_body", "expected_class"),
    [
        ("login", login_body(access_token="not-a-jwt"), "SCHEMA"),
        ("fees", fees_body(tier="not-an-integer"), "SCHEMA"),
    ],
)
async def test_complete_schema_failure_is_terminal_without_retry_or_later_read(
    monkeypatch: pytest.MonkeyPatch,
    response_path: str,
    response_body: dict[str, Any],
    expected_class: str,
) -> None:
    transport = complete_transport()
    if response_path == "login":
        transport.responses[("POST", fee_read.LOGIN_PATH)] = [
            observation("POST", fee_read.LOGIN_PATH, response_body)
        ]
    else:
        transport.responses[("GET", fee_read.FEES_PATH)] = [
            observation("GET", fee_read.FEES_PATH, response_body)
        ]
    monkeypatch.setattr(fee_read, "_recover_login", lambda _data, _signature: ACCOUNT)

    report = await fee_read._run_with_dependencies(dependencies(transport))

    assert report.terminal_classification == expected_class
    if response_path == "login":
        assert not any(call["path"] == fee_read.FEES_PATH for call in transport.calls)
        assert len([call for call in transport.calls if call["path"] == fee_read.LOGIN_PATH]) == 1
    else:
        assert len([call for call in transport.calls if call["path"] == fee_read.FEES_PATH]) == 1


@pytest.mark.asyncio
async def test_inactive_session_key_is_auth_failure_without_retry() -> None:
    transport = complete_transport()
    status_query = (("account", ACCOUNT), ("signer", SESSION_SIGNER))
    transport.responses[("GET", fee_read.SESSION_KEY_STATUS_PATH)] = [
        observation(
            "GET",
            fee_read.SESSION_KEY_STATUS_PATH,
            session_status_body(status=2, status_description="Revoked"),
            query=status_query,
        )
    ]

    report = await fee_read._run_with_dependencies(dependencies(transport))

    assert report.terminal_classification == "AUTH"
    assert report.reason == "SESSION_KEY_NOT_ACTIVE"
    assert len(transport.calls) == 2
    assert transport.calls[0]["path"] == fee_read.DOMAIN_PATH
    assert transport.calls[1]["path"] == fee_read.SESSION_KEY_STATUS_PATH


@pytest.mark.asyncio
async def test_wrong_final_host_is_safety_failure_without_retry() -> None:
    transport = complete_transport()
    transport.responses[("GET", fee_read.SESSION_KEY_STATUS_PATH)] = [
        observation(
            "GET",
            fee_read.SESSION_KEY_STATUS_PATH,
            session_status_body(),
            query=(("account", ACCOUNT), ("signer", SESSION_SIGNER)),
            final_url="https://evil.example/v1/auth/session-key-status",
        )
    ]

    report = await fee_read._run_with_dependencies(dependencies(transport))

    assert report.terminal_classification == "SAFETY"
    assert report.reason == "HOST_MISMATCH"
    assert len(transport.calls) == 2
    assert transport.calls[0]["path"] == fee_read.DOMAIN_PATH
    assert transport.calls[1]["path"] == fee_read.SESSION_KEY_STATUS_PATH


def test_allow_list_requires_exact_paths_and_identity_queries() -> None:
    assert str(
        fee_read.FixedRisexFeeReadTransport._target("GET", fee_read.DOMAIN_PATH)
    ) == expected_url("GET", fee_read.DOMAIN_PATH)
    with pytest.raises(fee_read._FeeReadFailure) as domain_query:
        fee_read.FixedRisexFeeReadTransport._target(
            "GET", fee_read.DOMAIN_PATH, (("account", ACCOUNT),)
        )
    with pytest.raises(fee_read._FeeReadFailure) as wrong_path:
        fee_read.FixedRisexFeeReadTransport._target("GET", "/v1/orders")
    with pytest.raises(fee_read._FeeReadFailure) as missing_account:
        fee_read.FixedRisexFeeReadTransport._target("GET", fee_read.NONCE_PATH)
    with pytest.raises(fee_read._FeeReadFailure) as swapped:
        fee_read.FixedRisexFeeReadTransport._target(
            "GET",
            fee_read.SESSION_KEY_STATUS_PATH,
            (("signer", SESSION_SIGNER), ("account", ACCOUNT)),
        )
    with pytest.raises(fee_read._FeeReadFailure) as same_identity:
        fee_read.FixedRisexFeeReadTransport._target(
            "GET",
            fee_read.SESSION_KEY_STATUS_PATH,
            (("account", ACCOUNT), ("signer", ACCOUNT)),
        )

    for raised in (domain_query, wrong_path, missing_account, swapped, same_identity):
        assert raised.value.reason == "ENDPOINT_NOT_ALLOWED"
        assert raised.value.failure_class == "SAFETY"


def test_strict_json_rejects_duplicates_nan_and_oversized_body() -> None:
    with pytest.raises(fee_read._FeeReadFailure) as duplicate:
        fee_read._strict_json(b'{"data":{},"data":{}}')
    with pytest.raises(fee_read._FeeReadFailure) as nan:
        fee_read._strict_json(b'{"data":{"value":NaN}}')
    with pytest.raises(fee_read._FeeReadFailure) as oversized:
        fee_read._strict_json(b"x" * (fee_read._MAX_RESPONSE_BYTES + 1))

    assert duplicate.value.failure_class == "SCHEMA"
    assert nan.value.failure_class == "SCHEMA"
    assert oversized.value.failure_class == "SCHEMA"


def test_fee_parser_returns_only_sanitized_allowed_fields() -> None:
    parsed = fee_read._parse_fees(fees_body())
    assert set(parsed) == {
        "tier",
        "maker_bps",
        "taker_bps",
        "schedule",
        "trial_tier",
        "trial_ends_at",
        "earned_tier",
    }
    with pytest.raises(fee_read._FeeReadFailure) as duplicate_tiers:
        fee_read._parse_fees(
            fees_body(
                schedule=[
                    {"tier": 1, "threshold_usd": "0", "taker_bps": 0, "maker_bps": 0},
                    {"tier": 1, "threshold_usd": "1", "taker_bps": 0, "maker_bps": 0},
                ]
            )
        )
    assert duplicate_tiers.value.failure_class == "SAFETY"


def test_hidden_input_failures_are_sanitized_and_capability_zeroizes() -> None:
    with pytest.raises(fee_read._FeeReadFailure) as cancelled:
        fee_read._OwnerLoginCapability(
            lambda _prompt: (_ for _ in ()).throw(EOFError())
        )
    with pytest.raises(fee_read._FeeReadFailure) as invalid:
        fee_read._OwnerLoginCapability(lambda _prompt: "not-a-private-key")

    assert cancelled.value.reason == "OWNER_INPUT_CANCELLED"
    assert invalid.value.reason == "OWNER_KEY_INVALID"
    assert "not-a-private-key" not in str(invalid.value)


def test_argument_surface_is_rejected_before_prompt(capsys: pytest.CaptureFixture[str]) -> None:
    result = fee_read.main(["--private-key", "fixture-value"])
    output = capsys.readouterr().out

    assert result == 2
    report = json.loads(output)
    assert report["reason"] == "ARGUMENTS_REJECTED"
    assert "fixture-value" not in output
    assert "private-key" not in output


@pytest.mark.asyncio
async def test_protected_path_failure_prevents_network_and_owner_access() -> None:
    transport = complete_transport()
    called = False

    def owner_capability() -> Any:
        nonlocal called
        called = True
        raise AssertionError("owner input must not happen")

    deps = replace(
        dependencies(transport, files=FakeFiles(required=False)),
        owner_capability_factory=owner_capability,
    )
    report = await fee_read._run_with_dependencies(deps)

    assert report.terminal_classification == "SAFETY"
    assert report.reason == "PROTECTED_PATH_INVALID"
    assert not transport.calls
    assert not called


@pytest.mark.asyncio
async def test_unclassified_transport_exception_is_sanitized() -> None:
    transport = complete_transport()
    transport.responses[("GET", fee_read.SESSION_KEY_STATUS_PATH)] = [
        RuntimeError("fixture-token-and-key-material")
    ]

    report = await fee_read._run_with_dependencies(dependencies(transport))

    assert report.terminal_classification == "SAFETY"
    assert report.reason == "UNCLASSIFIED_EXCEPTION"
    assert "fixture-token-and-key-material" not in report.evidence()


def test_source_has_no_legacy_private_or_write_dependency() -> None:
    source = Path(fee_read.__file__).read_text()
    assert "risex_farmer" not in source
    assert "/v1/orders" not in source
    assert "/v1/positions" not in source
    assert "/v1/withdraw" not in source
    assert "/v1/deposit" not in source
    assert "place_order" not in source
    assert "dispatch_order" not in source
    assert "main-wallet private key" in source
