import asyncio
import ast
import io
import json
import os
from pathlib import Path
import stat

import pytest

from risex_farmer import extended_mainnet_account_discovery as discovery
from risex_farmer import extended_mainnet_credential_onboarding as onboarding


API_KEY = "synthetic-read-only-api-key"
SECRET_ERROR = "api-key-must-never-appear-in-evidence"


def _info(
    *,
    account_id=1001,
    l2_key="0xabc123",
    l2_vault=321,
    status="ACTIVE",
    account_index=None,
    **extra,
):
    data = {
        "status": status,
        "l2Key": l2_key,
        "l2Vault": l2_vault,
        "accountId": account_id,
        "bridgeStarknetAddress": None,
    }
    if account_index is not None:
        data["accountIndex"] = account_index
    data.update(extra)
    return {"status": "OK", "data": data}


def _row(
    *,
    account_id=1001,
    account_index=0,
    l2_key="0xabc123",
    l2_vault=321,
    status="ACTIVE",
    **extra,
):
    row = {
        "accountId": account_id,
        "description": "safe display label",
        "accountIndex": account_index,
        "status": status,
        "l2Key": l2_key,
        "l2Vault": l2_vault,
        "bridgeStarknetAddress": None,
        "accountIndexForKeyGeneration": 0,
    }
    row.update(extra)
    return row


def _accounts(*rows):
    return {"status": "OK", "data": list(rows)}


class ScriptedTransport:
    def __init__(self, info_body=None, accounts_body=None, scripts=None):
        self.bodies = {
            discovery.ACCOUNT_INFO_PATH: _info() if info_body is None else info_body,
            discovery.ACCOUNTS_PATH: (
                _accounts(_row()) if accounts_body is None else accounts_body
            ),
        }
        self.scripts = {key: list(value) for key, value in (scripts or {}).items()}
        self.calls = []

    async def get(self, request):
        self.calls.append(request)
        script = self.scripts.get(request.path)
        if script:
            item = script.pop(0)
        else:
            item = self.bodies[request.path]
        if isinstance(item, BaseException):
            raise item
        if callable(item):
            item = item(request)
        return discovery.RestReply(
            status=200,
            final_url=request.url,
            body=item,
        )


def _paths(calls):
    return [request.path for request in calls]


def _configure(tmp_path, monkeypatch):
    directory = tmp_path / "config" / "risex-farmer" / "extended-mainnet-credentials"
    monkeypatch.setattr(onboarding, "PROTECTED_DIRECTORY", directory)
    monkeypatch.setattr(
        onboarding,
        "STARK_PROTECTED_DIRECTORY",
        tmp_path / "config" / "risex-farmer" / "extended-mainnet-signing",
    )
    return directory


@pytest.mark.asyncio
async def test_discovery_uses_exact_mainnet_gets_and_reconciles_public_identity():
    transport = ScriptedTransport(
        info_body=_info(account_id="1001", l2_vault="321"),
        accounts_body=_accounts(
            _row(account_id=1001, l2_vault="321"),
            _row(account_id=1002, account_index=1, l2_key="0xdef456", l2_vault="654"),
        ),
    )

    result = await discovery.discover_mainnet_identity(
        API_KEY,
        transport=transport,
        require_operator_selection=False,
    )

    assert result.status == discovery.DISCOVERED
    assert result.discovered
    assert result.identity == onboarding.ExtendedPublicIdentity(
        account_id=1001,
        account_index=0,
        l2_key="0xabc123",
        l2_vault=321,
    )
    assert result.attempts == {
        discovery.ACCOUNT_INFO_PATH: 1,
        discovery.ACCOUNTS_PATH: 1,
    }
    assert _paths(transport.calls) == [
        discovery.ACCOUNT_INFO_PATH,
        discovery.ACCOUNTS_PATH,
    ]
    assert all(request.method == "GET" for request in transport.calls)
    assert all(
        request.url == f"{discovery.MAINNET_REST_BASE_URL}{request.path}"
        for request in transport.calls
    )
    assert all(
        request.headers
        == {
            "User-Agent": discovery.USER_AGENT,
            "X-Api-Key": API_KEY,
        }
        for request in transport.calls
    )
    assert API_KEY not in result.evidence()
    assert "safe display label" not in result.evidence()
    assert "client" not in result.evidence().lower()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "body", "failure_class", "reason"),
    [
        (401, _info(), "AUTH", "AUTHENTICATION_REJECTED"),
        (500, _info(), "HTTP", "HTTP_STATUS_UNACCEPTED"),
    ],
)
async def test_auth_or_http_failure_is_terminal_without_retry(
    status, body, failure_class, reason
):
    class ResponseTransport(ScriptedTransport):
        async def get(self, request):
            self.calls.append(request)
            return discovery.RestReply(status, request.url, body)

    transport = ResponseTransport()
    result = await discovery.discover_mainnet_identity(API_KEY, transport=transport)

    assert (result.status, result.failure_class, result.reason) == (
        discovery.BLOCKED,
        failure_class,
        reason,
    )
    assert len(transport.calls) == 1
    assert result.attempts[discovery.ACCOUNT_INFO_PATH] == 1
    assert discovery.ACCOUNTS_PATH not in result.attempts


@pytest.mark.asyncio
async def test_error_envelope_is_auth_failure_and_redacts_error_body():
    transport = ScriptedTransport(
        info_body={
            "status": "ERROR",
            "data": None,
            "error": {"message": SECRET_ERROR},
        }
    )
    result = await discovery.discover_mainnet_identity(API_KEY, transport=transport)

    assert result.status == discovery.BLOCKED
    assert result.failure_class == "AUTH"
    assert result.reason == "AUTHENTICATION_REJECTED"
    assert SECRET_ERROR not in result.evidence()
    assert API_KEY not in result.evidence()
    assert len(transport.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        {"status": "OK", "data": {"accountId": 1001}},
        {"status": "OK", "data": [_row() | {"accountIndex": "bad"}]},
        {"status": "OK", "data": [_row() | {"l2Key": "0x0"}]},
    ],
)
async def test_missing_or_malformed_required_identity_schema_fails_closed(body):
    if isinstance(body["data"], list):
        transport = ScriptedTransport(accounts_body=body)
    else:
        transport = ScriptedTransport(info_body=body)
    result = await discovery.discover_mainnet_identity(API_KEY, transport=transport)

    assert result.status == discovery.BLOCKED
    assert result.failure_class == "SCHEMA"
    assert result.reason in {
        "ACCOUNT_INFO_SCHEMA_INVALID",
        "ACCOUNT_INDEX_INVALID",
        "ACCOUNT_L2_KEY_INVALID",
    }
    assert API_KEY not in result.evidence()


@pytest.mark.asyncio
async def test_info_and_accounts_identity_contradiction_is_terminal():
    transport = ScriptedTransport(
        info_body=_info(account_id=1001, l2_key="0xabc123", l2_vault=321),
        accounts_body=_accounts(_row(account_id=1001, l2_key="0xdef456", l2_vault=321)),
    )
    result = await discovery.discover_mainnet_identity(API_KEY, transport=transport)

    assert result.status == discovery.BLOCKED
    assert result.failure_class == "IDENTITY"
    assert result.reason == "ACCOUNT_INFO_ACCOUNTS_DISAGREE"
    assert len(transport.calls) == 2
    assert result.attempts == {
        discovery.ACCOUNT_INFO_PATH: 1,
        discovery.ACCOUNTS_PATH: 1,
    }


@pytest.mark.asyncio
async def test_each_endpoint_allows_only_one_transport_retry():
    transport = ScriptedTransport(
        scripts={
            discovery.ACCOUNT_INFO_PATH: [
                discovery.DiscoveryTransportError(),
                _info(),
            ],
            discovery.ACCOUNTS_PATH: [
                asyncio.TimeoutError(),
                _accounts(_row()),
            ],
        }
    )

    result = await discovery.discover_mainnet_identity(API_KEY, transport=transport)

    assert result.discovered
    assert result.attempts == {
        discovery.ACCOUNT_INFO_PATH: 2,
        discovery.ACCOUNTS_PATH: 2,
    }
    assert [request.attempt for request in transport.calls] == [1, 2, 1, 2]


@pytest.mark.asyncio
async def test_transport_retry_exhaustion_never_blindly_calls_a_third_time():
    transport = ScriptedTransport(
        scripts={
            discovery.ACCOUNT_INFO_PATH: [
                discovery.DiscoveryTransportError(),
                discovery.DiscoveryTransportError(),
            ]
        }
    )

    result = await discovery.discover_mainnet_identity(API_KEY, transport=transport)

    assert (result.status, result.failure_class, result.reason) == (
        discovery.BLOCKED,
        "TRANSPORT",
        "TRANSPORT_RETRY_EXHAUSTED",
    )
    assert len(transport.calls) == 2
    assert result.attempts == {discovery.ACCOUNT_INFO_PATH: 2}


@pytest.mark.asyncio
async def test_multiple_authoritative_accounts_require_exact_operator_public_pair():
    transport = ScriptedTransport(
        info_body=_info(account_id=1002, l2_key="0xdef456", l2_vault="654"),
        accounts_body=_accounts(
            _row(),
            _row(account_id=1002, account_index=1, l2_key="0xdef456", l2_vault="654"),
        ),
    )
    observation = await discovery.discover_account_candidates(API_KEY, transport=transport)

    required = discovery.resolve_identity(
        observation,
        require_operator_selection=True,
    )
    assert required.reason == "ACCOUNT_SELECTION_REQUIRED"
    assert required.failure_class == "IDENTITY"
    assert required.identity is None

    selected = discovery.resolve_identity(
        observation,
        operator_l2_key="0xdef456",
        operator_l2_vault="654",
        require_operator_selection=True,
    )
    assert selected.discovered
    assert selected.identity == onboarding.ExtendedPublicIdentity(
        account_id=1002,
        account_index=1,
        l2_key="0xdef456",
        l2_vault=654,
    )

    mismatch = discovery.resolve_identity(
        observation,
        operator_l2_key="0xabc123",
        operator_l2_vault="321",
        require_operator_selection=True,
    )
    assert mismatch.reason == "PUBLIC_IDENTITY_MISMATCH"
    assert mismatch.failure_class == "IDENTITY"


@pytest.mark.asyncio
async def test_cli_discovery_prompts_api_key_first_then_public_selector_and_persists_only_two_files(
    tmp_path, monkeypatch
):
    directory = _configure(tmp_path, monkeypatch)
    transport = ScriptedTransport(
        info_body=_info(account_id=1002, l2_key="0xdef456", l2_vault="654"),
        accounts_body=_accounts(
            _row(),
            _row(account_id=1002, account_index=1, l2_key="0xdef456", l2_vault="654"),
        ),
    )
    prompts = []
    values = iter([API_KEY, "0xdef456", "654"])

    def input_fn(prompt):
        prompts.append(prompt)
        return next(values)

    output = io.StringIO()
    result = await discovery.run_discovery(
        input_fn=input_fn,
        transport=transport,
        output=output,
    )

    assert result.provisioned
    assert result.identity == onboarding.ExtendedPublicIdentity(
        account_id=1002,
        account_index=1,
        l2_key="0xdef456",
        l2_vault=654,
    )
    assert [prompt for prompt in prompts] == [
        "Extended read-only API key (hidden): ",
        "Extended Stark public key (hidden public metadata): ",
        "Extended Vault number (hidden public metadata): ",
    ]
    assert all(
        token not in " ".join(prompts).lower()
        for token in ("private", "client id", "account id")
    )
    assert API_KEY not in result.evidence()
    assert API_KEY not in output.getvalue()
    assert SECRET_ERROR not in output.getvalue()
    assert {path.name for path in directory.iterdir()} == {
        onboarding.IDENTITY_FILENAME,
        onboarding.API_KEY_FILENAME,
    }
    assert stat.S_IMODE(os.lstat(directory).st_mode) == 0o700
    for path in directory.iterdir():
        details = os.lstat(path)
        assert stat.S_ISREG(details.st_mode)
        assert stat.S_IMODE(details.st_mode) == 0o600
        assert details.st_nlink == 1
    metadata = json.loads(
        (directory / onboarding.IDENTITY_FILENAME).read_text(encoding="utf-8")
    )
    assert metadata["identity"] == {
        "account_id": 1002,
        "account_index": 1,
        "l2_key": "0xdef456",
        "l2_vault": 654,
    }
    assert API_KEY not in (directory / onboarding.IDENTITY_FILENAME).read_text()
    assert (directory / onboarding.API_KEY_FILENAME).read_text() == API_KEY
    assert result.attempts == {
        discovery.ACCOUNT_INFO_PATH: 1,
        discovery.ACCOUNTS_PATH: 1,
    }
    assert result.mainnet_write_authority == discovery.NO_MAINNET_WRITE_AUTHORITY
    assert result.to_metadata()["write_ready"] is False


@pytest.mark.asyncio
async def test_single_account_uses_authoritative_info_without_extra_identity_prompt(
    tmp_path, monkeypatch
):
    directory = _configure(tmp_path, monkeypatch)
    transport = ScriptedTransport()
    prompts = []
    result = await discovery.run_discovery(
        input_fn=lambda prompt: prompts.append(prompt) or API_KEY,
        transport=transport,
        output=io.StringIO(),
    )

    assert result.provisioned
    assert prompts == ["Extended read-only API key (hidden): "]
    assert (directory / onboarding.API_KEY_FILENAME).read_text() == API_KEY


@pytest.mark.asyncio
async def test_identity_mismatch_or_invalid_api_key_writes_nothing(tmp_path, monkeypatch):
    directory = _configure(tmp_path, monkeypatch)
    mismatch_transport = ScriptedTransport(
        info_body=_info(account_id=1002, l2_key="0xdef456", l2_vault=654),
        accounts_body=_accounts(
            _row(),
            _row(
                account_id=1002,
                account_index=1,
                l2_key="0xdef456",
                l2_vault=654,
            ),
        ),
    )
    values = iter([API_KEY, "0xabc123", "321"])
    mismatch = await discovery.run_discovery(
        input_fn=lambda _prompt: next(values),
        transport=mismatch_transport,
        output=io.StringIO(),
    )
    assert mismatch.status == discovery.BLOCKED
    assert mismatch.reason == "PUBLIC_IDENTITY_MISMATCH"
    assert not directory.exists()

    invalid_transport = ScriptedTransport()
    prompts = []
    invalid = await discovery.run_discovery(
        input_fn=lambda prompt: prompts.append(prompt) or " ",
        transport=invalid_transport,
        output=io.StringIO(),
    )
    assert invalid.reason == "API_KEY_INVALID"
    assert prompts == ["Extended read-only API key (hidden): "]
    assert invalid_transport.calls == []
    assert not directory.exists()


@pytest.mark.asyncio
async def test_unsafe_existing_protected_path_blocks_before_hidden_input_or_network(
    tmp_path, monkeypatch
):
    directory = _configure(tmp_path, monkeypatch)
    directory.mkdir(parents=True)
    directory.chmod(0o755)
    transport = ScriptedTransport()
    prompts = []
    result = await discovery.run_discovery(
        input_fn=lambda prompt: prompts.append(prompt) or API_KEY,
        transport=transport,
        output=io.StringIO(),
    )

    assert result.status == discovery.BLOCKED
    assert result.failure_class == "SAFETY"
    assert not prompts
    assert transport.calls == []


def test_discovery_cli_rejects_secret_arguments_and_has_no_write_authority():
    parser_source = Path(discovery.__file__).read_text(encoding="utf-8")
    tree = ast.parse(parser_source)
    assert "--api-key" not in parser_source
    assert "client_id" not in parser_source
    assert "stark_private_key" not in parser_source
    assert not any(
        isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and any(
            token in node.name.lower()
            for token in ("sign", "dispatch", "order", "withdraw", "transfer")
        )
        for node in ast.walk(tree)
    )
    assert discovery.NO_MAINNET_WRITE_AUTHORITY in (
        discovery.OnboardingDiscoveryResult(
            discovery.BLOCKED,
            "synthetic",
            "SAFETY",
            None,
            0,
            {},
        ).evidence()
    )


def test_onboarding_command_routes_discovery_without_importing_it_at_startup(
    monkeypatch,
):
    called = []

    def fake_cli():
        called.append(True)
        return 7

    monkeypatch.setattr(discovery, "run_cli", fake_cli)
    assert onboarding.main(["discover"]) == 7
    assert called == [True]
