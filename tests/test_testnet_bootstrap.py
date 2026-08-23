from __future__ import annotations

import asyncio
import importlib
import inspect
import os
import ssl
import subprocess
import sys
import threading
import traceback
import json
import stat
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from yarl import URL


WALLET = "0x20f9153e2eeba0ff7880fb5a23e976e8b2af56ee"
OTHER_WALLET = "0x1111111111111111111111111111111111111111"
USDC = "0x2222222222222222222222222222222222222222"
AUTH = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
WRONG_VERIFIER = "0x4444444444444444444444444444444444444444"
ORIGIN = "https://api.testnet.rise.trade"
DEPOSIT = "/v1/account/deposit"
SENSITIVE = "DO_NOT_USE_VALUE_7af81c9d"


@pytest.fixture
def module():
    try:
        return importlib.import_module("risex_farmer.testnet_bootstrap")
    except ModuleNotFoundError:
        pytest.fail("RED: published main has no sealed testnet bootstrap", pytrace=False)


def config(chain: str = "11155931", name: str = "Rise Testnet") -> dict[str, Any]:
    return {"chain": {"name": name, "chain_id": chain},
            "addresses": {"usdc": USDC, "auth": AUTH}}


def domain(chain: str = "11155931", name: str = "RISEx",
           verifier: str = AUTH) -> dict[str, Any]:
    return {"name": name, "version": "1", "chain_id": chain,
            "verifying_contract": verifier}


class SensitiveError(RuntimeError):
    def __init__(self) -> None:
        super().__init__(SENSITIVE)
        self.response_body = {"credential": SENSITIVE}

    def __repr__(self) -> str:
        return f"SensitiveError(credential={SENSITIVE!r})"


@dataclass
class Scenario:
    balances: list[str] = field(default_factory=lambda: ["0"])
    config_body: Any = field(default_factory=config)
    domain_body: Any = field(default_factory=domain)
    post_status: int = 200
    post_url: str | None = None
    post_body: Any = field(default_factory=lambda: {"success": True})
    post_error: BaseException | None = None
    get_error: BaseException | None = None
    get_url: str | None = None
    wrap_balance: bool = False
    balance_request_id: str | None = None
    constructor_error: BaseException | None = None
    close_error: BaseException | None = None
    balance_responses: list[tuple[int, Any]] = field(default_factory=list)
    on_post: Any = None
    calls: list[tuple[str, URL, dict[str, Any]]] = field(default_factory=list)
    session_kwargs: list[dict[str, Any]] = field(default_factory=list)
    closed: int = 0

    def balance(self) -> str:
        return self.balances.pop(0) if len(self.balances) > 1 else self.balances[0]


class Response:
    def __init__(self, status: int, url: str | URL, body: Any) -> None:
        self.status, self.url, self.body = status, URL(url), body

    async def json(self) -> Any:
        if isinstance(self.body, BaseException):
            raise self.body
        return self.body

    async def text(self) -> str:
        return repr(self.body)

    async def read(self) -> bytes:
        return repr(self.body).encode()

    def release(self) -> None:
        pass


class Request:
    def __init__(self, response: Response | None, error: BaseException | None = None):
        self.response, self.error = response, error

    async def resolve(self) -> Response:
        if self.error is not None:
            raise self.error
        assert self.response is not None
        return self.response

    def __await__(self):
        return self.resolve().__await__()

    async def __aenter__(self) -> Response:
        return await self.resolve()

    async def __aexit__(self, *_: Any) -> None:
        pass


class Session:
    def __init__(self, scenario: Scenario, **kwargs: Any) -> None:
        self.scenario = scenario
        scenario.session_kwargs.append(kwargs)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    async def close(self) -> None:
        self.scenario.closed += 1
        if self.scenario.close_error is not None:
            raise self.scenario.close_error

    def request(self, method: str, url: str | URL, **kwargs: Any) -> Request:
        method, requested = method.upper(), URL(url)
        actual = requested.with_query(kwargs.get("params")) if kwargs.get("params") else requested
        self.scenario.calls.append((method, actual, kwargs))
        if method == "POST" and requested.path == DEPOSIT:
            if self.scenario.on_post is not None:
                self.scenario.on_post()
            response = Response(self.scenario.post_status,
                                self.scenario.post_url or actual,
                                self.scenario.post_body)
            return Request(response, self.scenario.post_error)
        if self.scenario.get_error is not None:
            error, self.scenario.get_error = self.scenario.get_error, None
            return Request(None, error)
        bodies = {"/v1/system/config": self.scenario.config_body,
                  "/v1/auth/eip712-domain": self.scenario.domain_body}
        if requested.path == "/v1/account/balance":
            status = 200
            if self.scenario.balance_responses:
                status, body = self.scenario.balance_responses.pop(0)
                return Request(Response(status, self.scenario.get_url or actual, body))
            body = {"data": {"balance": self.scenario.balance()}}
            if self.scenario.balance_request_id is not None:
                body["request_id"] = self.scenario.balance_request_id
            if not self.scenario.wrap_balance:
                body = body["data"]
        else:
            body = bodies.get(requested.path, {"error": "unexpected"})
        known = requested.path in bodies or requested.path == "/v1/account/balance"
        return Request(Response(200 if known else 404,
                                self.scenario.get_url or actual, body))

    def get(self, url: str | URL, **kwargs: Any) -> Request:
        return self.request("GET", url, **kwargs)

    def post(self, url: str | URL, **kwargs: Any) -> Request:
        return self.request("POST", url, **kwargs)


@pytest.fixture
def transport(monkeypatch: pytest.MonkeyPatch, module):
    installed: list[Scenario] = []

    def install(scenario: Scenario) -> Scenario:
        installed.append(scenario)
        monkeypatch.setattr(module.aiohttp, "ClientSession",
                            lambda *args, **kwargs: _session(args, kwargs, scenario))
        return scenario

    yield install
    for scenario in installed:
        if not scenario.session_kwargs:
            assert not scenario.calls and scenario.closed == 0
            continue
        if scenario.constructor_error is None:
            assert scenario.closed == len(scenario.session_kwargs)
        else:
            assert scenario.closed == 0 and not scenario.calls
        kwargs = scenario.session_kwargs[0]
        assert kwargs.get("trust_env") is False
        assert 0 < kwargs["timeout"].total <= 30
        assert kwargs.get("ssl") is not False
        for _method, _url, request_kwargs in scenario.calls:
            assert request_kwargs.get("allow_redirects") is False
            assert "proxy" not in request_kwargs and "proxy_auth" not in request_kwargs
            assert request_kwargs.get("ssl") is not False


def _session(args: tuple[Any, ...], kwargs: dict[str, Any], scenario: Scenario) -> Session:
    assert not args
    if scenario.constructor_error is not None:
        scenario.session_kwargs.append(kwargs)
        raise scenario.constructor_error
    return Session(scenario, **kwargs)


def posts(scenario: Scenario):
    return [call for call in scenario.calls if call[0] == "POST"]


MARKER = ".risex-funding-farmer-testnet-first-deposit-v1.json"
READY_TEMP = MARKER + ".ready.tmp"
OBSERVED_ABSENT = {
    "error": {"code": "Internal", "message": "failed to get balance"},
    "request_id": "req-fixture-stable",
}


def marker_payload(state: str = "SPENT_UNKNOWN") -> dict[str, Any]:
    return {
        "schema_version": 1,
        "venue": "RISEx",
        "host": "api.testnet.rise.trade",
        "chain_id": 11155931,
        "wallet": WALLET,
        "operation": "FIRST_DEPOSIT",
        "amount": "1000",
        "state": state,
    }


def write_marker(home: Path, payload: Any, *, mode: int = 0o600) -> Path:
    path = home / MARKER
    if isinstance(payload, bytes):
        path.write_bytes(payload)
    else:
        path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")
    path.chmod(mode)
    return path


@pytest.fixture(autouse=True)
def private_passwd_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, module):
    """All bootstrap tests use an existing disposable passwd-home directory."""

    home = tmp_path / "home"
    home.mkdir(mode=0o700)
    monkeypatch.setattr(module, "_passwd_home", lambda: home, raising=False)
    return home


def assert_testnet_calls(scenario: Scenario) -> None:
    permitted = {("GET", "/v1/system/config"), ("GET", "/v1/auth/eip712-domain"),
                 ("GET", "/v1/account/balance"), ("POST", DEPOSIT)}
    for method, url, _kwargs in scenario.calls:
        assert (url.scheme, url.host) == ("https", "api.testnet.rise.trade")
        assert (method, url.path) in permitted


def test_public_surface_is_sealed(module) -> None:
    assert list(inspect.signature(module.check_risex_account).parameters) == ["wallet"]
    bootstrap_signature = inspect.signature(module.bootstrap_risex_account)
    assert list(bootstrap_signature.parameters) == ["wallet", "intent"]
    assert bootstrap_signature.parameters["intent"].kind is inspect.Parameter.KEYWORD_ONLY
    forbidden = {"sender", "transport", "request", "session", "url", "base_url", "method",
                 "path", "proxy", "ssl", "order", "cancel", "replace", "position", "trade"}
    public = {name.lower() for name in vars(module) if not name.startswith("_")}
    assert not public & forbidden
    source = Path(module.__file__).read_text().lower()
    assert not any(token in source for token in
                   ("place_order", "cancel_order", "replace_order", "reduce_only"))


@pytest.mark.asyncio
async def test_read_only_cannot_write_or_obey_fixture_destination(module, transport,
                                                                  monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "https://production.invalid")
    body = config()
    body["chain"]["deposit_url"] = "https://api.rise.trade/v1/account/deposit"
    scenario = transport(Scenario(balances=["23"], config_body=body))
    state = await module.check_risex_account(WALLET)
    assert (state.ready, state.balance_raw) == (True, "23")
    assert not posts(scenario)
    assert_testnet_calls(scenario)
    balance_call = next(call for call in scenario.calls if call[1].path.endswith("/balance"))
    assert balance_call[2]["params"] == {"account": WALLET, "token": USDC}


@pytest.mark.asyncio
async def test_preflight_and_postcondition_are_authoritative(module, transport):
    already = transport(Scenario(balances=["1"]))
    result = await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert result.status is module.BootstrapStatus.ALREADY_READY
    assert not posts(already)

    home = module._passwd_home()
    (home / MARKER).unlink()

    unverified = transport(Scenario(balances=["0"]))
    result = await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert result.status is module.BootstrapStatus.SUBMITTED_UNVERIFIED
    assert result.status is not module.BootstrapStatus.READY
    assert len(posts(unverified)) == 1
    assert posts(unverified)[0][2]["json"] == {"account": WALLET, "amount": "1000"}

    (home / MARKER).unlink()
    if (home / READY_TEMP).exists():
        (home / READY_TEMP).unlink()

    ready = transport(Scenario(balances=["0", "9"]))
    result = await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert (result.status, result.balance_raw) == (module.BootstrapStatus.READY, "9")
    assert len(posts(ready)) == 1
    assert sum(call[1].path.endswith("/system/config") for call in ready.calls) == 2
    assert sum(call[1].path.endswith("eip712-domain") for call in ready.calls) == 2


@pytest.mark.parametrize("shape", ["direct", "data", "gateway"])
@pytest.mark.asyncio
async def test_official_direct_and_gateway_envelopes_are_accepted(shape, module, transport):
    def envelope(value):
        if shape == "direct":
            return value
        wrapped = {"data": value}
        if shape == "gateway":
            wrapped["request_id"] = "req-fixture-1"
        return wrapped

    scenario = transport(Scenario(
        balances=["0", "7"],
        config_body=envelope(config()),
        domain_body=envelope(domain()),
        post_body=envelope({"success": True}),
        wrap_balance=shape != "direct",
        balance_request_id="req-fixture-2" if shape == "gateway" else None,
    ))
    result = await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert (result.status, result.balance_raw) == (module.BootstrapStatus.READY, "7")
    assert len(posts(scenario)) == 1


@pytest.mark.asyncio
async def test_domain_verifier_matches_auth_case_insensitively(module, transport):
    upper_auth = "0x" + AUTH[2:].upper()
    scenario = transport(Scenario(balances=["1"], domain_body=domain(verifier=upper_auth)))
    state = await module.check_risex_account(WALLET)
    assert state.ready is True
    assert not posts(scenario)


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
@pytest.mark.asyncio
async def test_redirect_is_never_followed_or_submitted(status, module, transport):
    scenario = transport(Scenario(post_status=status))
    result = await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert result.status is module.BootstrapStatus.UNKNOWN_AMBIGUOUS
    assert "SUBMITTED" not in result.status.name
    assert len(posts(scenario)) == 1


@pytest.mark.asyncio
async def test_production_final_url_is_never_submitted(module, transport):
    scenario = transport(Scenario(post_url="https://api.rise.trade" + DEPOSIT))
    result = await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert result.status is module.BootstrapStatus.UNKNOWN_AMBIGUOUS
    assert "SUBMITTED" not in result.status.name
    assert len(posts(scenario)) == 1


@pytest.mark.parametrize("change", [
    {"config_body": config(chain="1")}, {"config_body": config(name="Rise")},
    {"domain_body": domain(chain="1")}, {"domain_body": domain(name="RISK")},
    {"domain_body": domain(verifier=WRONG_VERIFIER)},
    {"config_body": config(chain=11_155_931)},
    {"domain_body": domain(chain=11_155_931)},
    {"config_body": {"data": config(), "extra": "ambiguous"}},
    {"config_body": {"data": config(), "request_id": ""}},
    {"config_body": {"data": config(), "request_id": 42}},
    {"get_url": "https://api.rise.trade/v1/system/config"},
    {"get_url": "https://api.testnet.rise.trade/v1/wrong"},
])
@pytest.mark.asyncio
async def test_identity_mismatch_fails_before_write(change, module, transport):
    scenario = transport(Scenario(**change))
    with pytest.raises(module.BootstrapSafetyError):
        await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert not posts(scenario)


@pytest.mark.parametrize("wallet", ["", "0x1234", "1" * 40, "0x" + "gg" * 20,
                                    OTHER_WALLET])
@pytest.mark.asyncio
async def test_wrong_wallet_fails_before_request(wallet, module, transport):
    scenario = transport(Scenario())
    with pytest.raises(module.BootstrapSafetyError):
        await module.bootstrap_risex_account(wallet, intent="RISEX_TESTNET_DEPOSIT")
    assert not scenario.calls


@pytest.mark.asyncio
async def test_wrong_intent_fails_before_request(module, transport):
    scenario = transport(Scenario())
    with pytest.raises(module.BootstrapSafetyError):
        await module.bootstrap_risex_account(WALLET, intent="DEPOSIT_ALL")
    assert not scenario.calls


@pytest.mark.parametrize("error", [asyncio.TimeoutError(), EOFError(),
                                    ssl.SSLError("synthetic TLS failure")])
@pytest.mark.asyncio
async def test_uncertain_post_failure_is_one_attempt(error, module, transport):
    scenario = transport(Scenario(post_error=error))
    result = await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert result.status is module.BootstrapStatus.UNKNOWN_AMBIGUOUS
    assert len(posts(scenario)) == 1


@pytest.mark.asyncio
async def test_malformed_post_is_one_attempt_and_ambiguous(module, transport):
    scenario = transport(Scenario(post_body=["not", "a", "mapping"]))
    result = await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert result.status is module.BootstrapStatus.UNKNOWN_AMBIGUOUS
    assert len(posts(scenario)) == 1


@pytest.mark.asyncio
async def test_extra_data_envelope_after_post_is_ambiguous(module, transport):
    scenario = transport(Scenario(post_body={"data": {"success": True}, "extra": "ambiguous"}))
    result = await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert result.status is module.BootstrapStatus.UNKNOWN_AMBIGUOUS
    assert len(posts(scenario)) == 1


@pytest.mark.asyncio
async def test_cancellation_propagates_without_retry(module, transport):
    scenario = transport(Scenario(post_error=asyncio.CancelledError()))
    with pytest.raises(asyncio.CancelledError):
        await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert len(posts(scenario)) == 1


@pytest.mark.asyncio
async def test_public_failures_redact_body_repr_chain_and_output(module, transport, capsys):
    write = transport(Scenario(post_error=SensitiveError(),
                               post_body={"credential": SENSITIVE}))
    result = await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert SENSITIVE not in repr(result) + str(result)
    assert len(posts(write)) == 1

    read = transport(Scenario(get_error=SensitiveError()))
    with pytest.raises(module.BootstrapSafetyError) as error:
        await module.check_risex_account(WALLET)
    assert SENSITIVE not in "".join(traceback.format_exception(error.value))
    captured = capsys.readouterr()
    assert SENSITIVE not in captured.out + captured.err
    assert read.closed == 1


@pytest.mark.asyncio
async def test_session_constructor_failure_is_sanitized_before_dispatch(module, transport):
    constructor = transport(Scenario(constructor_error=SensitiveError()))
    with pytest.raises(module.BootstrapSafetyError) as constructor_error:
        await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert SENSITIVE not in "".join(traceback.format_exception(constructor_error.value))
    assert not posts(constructor)


@pytest.mark.asyncio
async def test_session_close_failure_is_sanitized_before_dispatch(module, transport):
    before_post = transport(Scenario(balances=["1"], close_error=SensitiveError()))
    with pytest.raises(module.BootstrapSafetyError) as close_error:
        await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert SENSITIVE not in "".join(traceback.format_exception(close_error.value))
    assert not posts(before_post)


@pytest.mark.asyncio
async def test_session_close_failure_is_ambiguous_after_dispatch(module, transport):
    after_post = transport(Scenario(balances=["0", "7"], close_error=SensitiveError()))
    result = await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert result.status is module.BootstrapStatus.UNKNOWN_AMBIGUOUS
    assert SENSITIVE not in repr(result) + str(result)
    assert len(posts(after_post)) == 1


@pytest.mark.asyncio
async def test_session_lifecycle_cancellation_propagates(module, transport):
    transport(Scenario(constructor_error=asyncio.CancelledError()))
    with pytest.raises(asyncio.CancelledError):
        await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")

    after_post = transport(Scenario(
        balances=["0", "7"], close_error=asyncio.CancelledError()
    ))
    with pytest.raises(asyncio.CancelledError):
        await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert len(posts(after_post)) == 1


def test_normal_farmer_import_does_not_load_optional_module() -> None:
    project = Path(__file__).resolve().parents[1]
    env = os.environ | {"PYTHONPATH": str(project / "src")}
    command = ("import sys, risex_farmer; "
               "assert 'risex_farmer.testnet_bootstrap' not in sys.modules; "
               "print(risex_farmer.__file__)")
    completed = subprocess.run([sys.executable, "-c", command], cwd=project, env=env,
                               check=True, capture_output=True, text=True, timeout=10)
    assert completed.stdout.strip() == str(project / "src/risex_farmer/__init__.py")


# TESTNET-001-RECOVERY-005: direct passwd-home durable one-shot RED.


def exact_absent_then(*responses: tuple[int, Any]) -> list[tuple[int, Any]]:
    return [(500, OBSERVED_ABSENT), *responses]


def observed_absent(request_id: str) -> dict[str, Any]:
    return {
        "error": {"code": "Internal", "message": "failed to get balance"},
        "request_id": request_id,
    }


@pytest.mark.asyncio
async def test_positive_preflight_durably_consumes_before_restart(
    module, transport, private_passwd_home
):
    first = transport(Scenario(balances=["12"]))
    result = await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert result.status is module.BootstrapStatus.ALREADY_READY
    assert not posts(first)
    assert json.loads((private_passwd_home / MARKER).read_text()) == marker_payload("READY")

    later = transport(Scenario(balance_responses=[(500, observed_absent("later-request"))]))
    result = await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert result.status is module.BootstrapStatus.READY_UNVERIFIED
    assert not posts(later)


@pytest.mark.asyncio
async def test_positive_preflight_ready_failure_remains_spent_and_never_posts(
    module, transport, private_passwd_home, monkeypatch
):
    real_replace = module.os.replace
    monkeypatch.setattr(module.os, "replace", lambda *_args, **_kwargs:
                        (_ for _ in ()).throw(OSError("synthetic READY failure")))
    first = transport(Scenario(balances=["12"]))
    result = await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert result.status is module.BootstrapStatus.READY_UNVERIFIED
    assert not posts(first)
    assert json.loads((private_passwd_home / MARKER).read_text()) == marker_payload()

    monkeypatch.setattr(module.os, "replace", real_replace)
    later = transport(Scenario(balance_responses=exact_absent_then()))
    result = await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert result.status is module.BootstrapStatus.UNKNOWN_AMBIGUOUS
    assert not posts(later)


@pytest.mark.asyncio
async def test_direct_home_claim_is_durable_before_the_only_post(
    module, transport, private_passwd_home, monkeypatch
):
    events: list[str] = []
    real_open, real_write, real_fsync = module.os.open, module.os.write, module.os.fsync

    def tracked_open(path, flags, mode=0o777, *, dir_fd=None):
        if path == MARKER and flags & os.O_CREAT:
            assert flags & os.O_CREAT and flags & os.O_EXCL
            assert getattr(os, "O_NOFOLLOW", 0) & flags
            events.append("exclusive-create")
        return real_open(path, flags, mode, dir_fd=dir_fd)

    def tracked_write(fd, data):
        events.append("complete-write")
        return real_write(fd, data)

    def tracked_fsync(fd):
        events.append("home-fsync" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file-fsync")
        return real_fsync(fd)

    monkeypatch.setattr(module.os, "open", tracked_open)
    monkeypatch.setattr(module.os, "write", tracked_write)
    monkeypatch.setattr(module.os, "fsync", tracked_fsync)
    def at_post() -> None:
        assert json.loads((private_passwd_home / MARKER).read_text()) == marker_payload()
        events.append("post")

    scenario = transport(Scenario(
        balance_responses=exact_absent_then((200, {"balance": "0"})),
        on_post=at_post,
    ))

    result = await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")

    assert result.status is not module.BootstrapStatus.READY
    assert len(posts(scenario)) == 1
    assert events.index("exclusive-create") < events.index("complete-write")
    assert events.index("complete-write") < events.index("file-fsync")
    assert events.index("file-fsync") < events.index("home-fsync") < events.index("post")
    marker = private_passwd_home / MARKER
    assert marker.parent == private_passwd_home
    assert json.loads(marker.read_text()) == marker_payload()
    assert stat.S_IMODE(marker.stat().st_mode) == 0o600


@pytest.mark.parametrize("boundary", ["create", "write", "file_fsync", "home_fsync"])
def test_claim_boundary_failure_is_terminal_and_never_reusable(
    boundary, module, private_passwd_home, monkeypatch
):
    real_write, real_fsync = module.os.write, module.os.fsync
    fired = False

    def failing_write(fd, data):
        nonlocal fired
        if boundary == "create" and not fired:
            fired = True
            raise OSError("synthetic crash after exclusive create")
        if boundary == "write" and not fired:
            fired = True
            real_write(fd, data)
            raise OSError("synthetic crash after partial write")
        return real_write(fd, data)

    def failing_fsync(fd):
        nonlocal fired
        is_dir = stat.S_ISDIR(os.fstat(fd).st_mode)
        if not fired and ((boundary == "file_fsync" and not is_dir) or
                          (boundary == "home_fsync" and is_dir)):
            fired = True
            raise OSError("synthetic durability failure")
        return real_fsync(fd)

    monkeypatch.setattr(module.os, "write", failing_write)
    monkeypatch.setattr(module.os, "fsync", failing_fsync)
    with pytest.raises(module.BootstrapSafetyError):
        module._claim_first_deposit()
    monkeypatch.setattr(module.os, "write", real_write)
    monkeypatch.setattr(module.os, "fsync", real_fsync)
    try:
        replay = module._claim_first_deposit()
    except module.BootstrapSafetyError:
        replay = False
    assert replay is False
    assert (private_passwd_home / MARKER).exists()


@pytest.mark.asyncio
async def test_existing_spent_or_invalid_marker_always_blocks_post(
    module, transport, private_passwd_home
):
    invalids = [
        marker_payload(), b"", b'{"schema_version":1',
        marker_payload() | {"wallet": OTHER_WALLET},
        marker_payload() | {"extra": "not canonical"},
    ]
    for value in invalids:
        marker = private_passwd_home / MARKER
        if marker.exists() or marker.is_symlink():
            marker.unlink()
        write_marker(private_passwd_home, value)
        scenario = transport(Scenario(balance_responses=exact_absent_then()))
        try:
            result = await module.bootstrap_risex_account(
                WALLET, intent="RISEX_TESTNET_DEPOSIT"
            )
        except module.BootstrapSafetyError as error:
            assert str(error) == "RISEx testnet identity or response rejected"
        else:
            assert result.status is not module.BootstrapStatus.READY
        assert not posts(scenario)


@pytest.mark.parametrize("kind", ["symlink", "directory", "fifo", "wrong_mode", "hardlink"])
def test_unsafe_existing_marker_fails_closed_at_access(
    kind, module, private_passwd_home, tmp_path
):
    marker = private_passwd_home / MARKER
    if kind == "symlink":
        marker.symlink_to(tmp_path / "elsewhere")
    elif kind == "directory":
        marker.mkdir()
    elif kind == "fifo":
        os.mkfifo(marker)
    else:
        write_marker(private_passwd_home, marker_payload(), mode=0o644 if kind == "wrong_mode" else 0o600)
        if kind == "hardlink":
            os.link(marker, tmp_path / "second-link")
    with pytest.raises(module.BootstrapSafetyError):
        module._claim_first_deposit()


def test_wrong_owner_marker_fails_closed_at_access(module, private_passwd_home, monkeypatch):
    marker = write_marker(private_passwd_home, marker_payload())
    real_fstat = module.os.fstat

    def wrong_owner(fd):
        result = real_fstat(fd)
        if stat.S_ISREG(result.st_mode):
            values = list(result)
            values[4] = os.getuid() + 1
            return os.stat_result(values)
        return result

    monkeypatch.setattr(module.os, "fstat", wrong_owner)
    with pytest.raises(module.BootstrapSafetyError):
        module._claim_first_deposit()
    assert marker.exists()


@pytest.mark.parametrize("kind", ["symlink", "file", "wrong_owner",
                                  "no_o_directory", "no_o_nofollow"])
def test_passwd_home_must_be_owned_real_directory_with_required_primitives(
    kind, module, private_passwd_home, tmp_path, monkeypatch
):
    if kind in {"symlink", "file"}:
        private_passwd_home.rmdir()
        if kind == "symlink":
            destination = tmp_path / "redirected-home"
            destination.mkdir()
            private_passwd_home.symlink_to(destination, target_is_directory=True)
        else:
            private_passwd_home.write_text("not a directory")
    elif kind == "wrong_owner":
        real_fstat = module.os.fstat

        def wrong_home_owner(fd):
            result = real_fstat(fd)
            if stat.S_ISDIR(result.st_mode):
                values = list(result)
                values[4] = os.getuid() + 1
                return os.stat_result(values)
            return result

        monkeypatch.setattr(module.os, "fstat", wrong_home_owner)
    elif kind == "no_o_directory":
        monkeypatch.setattr(module.os, "O_DIRECTORY", 0)
    else:
        monkeypatch.setattr(module.os, "O_NOFOLLOW", 0)
    with pytest.raises(module.BootstrapSafetyError):
        module._claim_first_deposit()


def test_abandoned_ready_temp_without_marker_is_terminal_consumed(
    module, private_passwd_home
):
    write_marker(private_passwd_home, marker_payload("READY"))
    (private_passwd_home / MARKER).rename(private_passwd_home / READY_TEMP)
    assert module._claim_first_deposit() is False
    assert not (private_passwd_home / MARKER).exists()


@pytest.mark.asyncio
async def test_ready_marker_never_replays_and_is_not_readiness_authority(
    module, transport, private_passwd_home
):
    write_marker(private_passwd_home, marker_payload("READY"))
    positive = transport(Scenario(balances=["8"]))
    ready = await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert ready.status in {module.BootstrapStatus.READY, module.BootstrapStatus.ALREADY_READY}
    assert not posts(positive)

    (private_passwd_home / MARKER).write_text(
        json.dumps(marker_payload("READY"), sort_keys=True, separators=(",", ":")) + "\n"
    )
    unavailable = transport(Scenario(balance_responses=[(500, OBSERVED_ABSENT)]))
    result = await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert result.status not in {module.BootstrapStatus.READY,
                                 module.BootstrapStatus.ALREADY_READY}
    assert not posts(unavailable)


@pytest.mark.asyncio
async def test_positive_postcondition_uses_same_home_atomic_ready_transition(
    module, transport, private_passwd_home, monkeypatch
):
    events: list[str] = []
    replaces: list[tuple[Any, Any]] = []
    real_open, real_write = module.os.open, module.os.write
    real_fsync, real_replace = module.os.fsync, module.os.replace

    def tracked_open(path, flags, mode=0o777, *, dir_fd=None):
        if path == READY_TEMP and flags & os.O_CREAT:
            assert flags & os.O_CREAT and flags & os.O_EXCL
            assert getattr(os, "O_NOFOLLOW", 0) & flags
            events.append("temp-create")
        return real_open(path, flags, mode, dir_fd=dir_fd)

    def tracked_write(fd, data):
        events.append("temp-write")
        return real_write(fd, data)

    def tracked_fsync(fd):
        events.append("home-fsync" if stat.S_ISDIR(os.fstat(fd).st_mode) else "temp-fsync")
        return real_fsync(fd)

    def tracked_replace(src, dst, *, src_dir_fd=None, dst_dir_fd=None):
        replaces.append((src, dst))
        events.append("replace")
        return real_replace(src, dst, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)

    monkeypatch.setattr(module.os, "open", tracked_open)
    monkeypatch.setattr(module.os, "write", tracked_write)
    monkeypatch.setattr(module.os, "fsync", tracked_fsync)
    monkeypatch.setattr(module.os, "replace", tracked_replace)
    scenario = transport(Scenario(
        balance_responses=exact_absent_then((200, {"balance": "9"}))
    ))
    result = await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert result.status is module.BootstrapStatus.READY
    assert len(posts(scenario)) == 1
    assert replaces == [(READY_TEMP, MARKER)]
    temp_create = events.index("temp-create")
    assert temp_create < events.index("temp-write", temp_create)
    temp_fsync = events.index("temp-fsync", temp_create)
    replace = events.index("replace", temp_create)
    assert events.index("temp-write", temp_create) < temp_fsync < replace
    assert replace < events.index("home-fsync", replace)
    assert json.loads((private_passwd_home / MARKER).read_text()) == marker_payload("READY")
    assert not (private_passwd_home / READY_TEMP).exists()


@pytest.mark.asyncio
async def test_ready_transition_failure_stays_consumed_and_cannot_replay(
    module, transport, private_passwd_home, monkeypatch
):
    real_replace = module.os.replace

    def fail_replace(*_args, **_kwargs):
        raise OSError("synthetic atomic READY update failure")

    monkeypatch.setattr(module.os, "replace", fail_replace)
    first = transport(Scenario(
        balance_responses=exact_absent_then((200, {"balance": "9"}))
    ))
    result = await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert result.status is not module.BootstrapStatus.READY
    assert len(posts(first)) == 1
    monkeypatch.setattr(module.os, "replace", real_replace)
    replay = transport(Scenario(balance_responses=exact_absent_then()))
    result = await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert result.status is not module.BootstrapStatus.READY
    assert not posts(replay)
    assert json.loads((private_passwd_home / MARKER).read_text()) == marker_payload()


@pytest.mark.parametrize("status,body", [
    (400, OBSERVED_ABSENT), (404, OBSERVED_ABSENT), (502, OBSERVED_ABSENT),
    (503, OBSERVED_ABSENT), (504, OBSERVED_ABSENT),
    (500, {"error": {"code": "Other", "message": "failed to get balance"},
           "request_id": "req-fixture-stable"}),
    (500, {"error": {"code": "Internal", "message": "different"},
           "request_id": "req-fixture-stable"}),
    (500, {"error": {"code": "Internal", "message": "failed to get balance"}}),
    (500, {"error": {"code": "Internal", "message": "failed to get balance"},
           "request_id": ""}),
    (500, {"error": {"code": "Internal", "message": "failed to get balance"},
           "request_id": 42}),
    (500, {"error": {"code": "Internal", "message": "failed to get balance"},
           "request_id": "req-fixture-stable", "extra": 1}),
    (500, {"error": {"code": "Internal", "message": "failed to get balance",
                      "extra": 1}, "request_id": "req-fixture-stable"}),
])
@pytest.mark.asyncio
async def test_only_exact_observed_preflight_error_can_reach_claim(
    status, body, module, transport, private_passwd_home
):
    scenario = transport(Scenario(balance_responses=[(status, body)]))
    with pytest.raises(module.BootstrapSafetyError):
        await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert not posts(scenario)
    assert not (private_passwd_home / MARKER).exists()


@pytest.mark.asyncio
async def test_observed_preflight_accepts_any_nonempty_request_id_without_hardcoding(
    module, transport
):
    scenario = transport(Scenario(balance_responses=[
        (500, observed_absent("different-redacted-request-id")),
        (200, {"balance": "0"}),
    ]))
    result = await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert result.status is not module.BootstrapStatus.READY
    assert len(posts(scenario)) == 1


@pytest.mark.asyncio
async def test_async_race_produces_one_claim_and_at_most_one_post(
    module, transport
):
    scenario = transport(Scenario(
        balance_responses=[(500, OBSERVED_ABSENT), (500, OBSERVED_ABSENT),
                           (200, {"balance": "0"})]
    ))
    results = await asyncio.wait_for(asyncio.gather(*[
        module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
        for _ in range(2)
    ]), timeout=3)
    assert len(results) == 2
    assert len(posts(scenario)) == 1


def _claim_subprocess(project: Path, home: Path, *, abrupt: bool = False):
    code = (
        "import pathlib, risex_farmer.testnet_bootstrap as m;"
        f"m._passwd_home=lambda:pathlib.Path({str(home)!r});"
        "claimed=m._claim_first_deposit();"
        + ("__import__('os')._exit(0)" if abrupt else "print('CLAIMED' if claimed else 'BLOCKED')")
    )
    env = os.environ | {"PYTHONPATH": str(project / "src")}
    return subprocess.Popen([sys.executable, "-c", code], cwd=project, env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def test_subprocess_race_and_abrupt_restart_are_one_shot(module, private_passwd_home):
    project = Path(__file__).resolve().parents[1]
    racers = [_claim_subprocess(project, private_passwd_home) for _ in range(2)]
    outputs = [process.communicate(timeout=5) for process in racers]
    assert all(process.returncode == 0 for process in racers), outputs
    assert sorted(output.strip() for output, _ in outputs) == ["BLOCKED", "CLAIMED"]

    (private_passwd_home / MARKER).unlink()
    abrupt = _claim_subprocess(project, private_passwd_home, abrupt=True)
    abrupt.communicate(timeout=5)
    assert abrupt.returncode == 0
    replay = _claim_subprocess(project, private_passwd_home)
    output, error = replay.communicate(timeout=5)
    assert replay.returncode == 0, error
    assert output.strip() == "BLOCKED"


def test_observer_during_exclusive_prewrite_publication_is_terminal_blocked(
    module, monkeypatch
):
    entered_write = threading.Event()
    release_write = threading.Event()
    real_write = module.os.write
    winner_ident: list[int] = []
    outcome: list[object] = []

    def gated_write(fd, data):
        if threading.get_ident() == winner_ident[0] and not entered_write.is_set():
            entered_write.set()
            assert release_write.wait(timeout=3)
        return real_write(fd, data)

    def winner() -> None:
        winner_ident.append(threading.get_ident())
        try:
            outcome.append(module._claim_first_deposit())
        except BaseException as error:
            outcome.append(error)

    monkeypatch.setattr(module.os, "write", gated_write)
    thread = threading.Thread(target=winner)
    thread.start()
    assert entered_write.wait(timeout=3)
    observer = module._claim_first_deposit()
    release_write.set()
    thread.join(timeout=3)
    assert not thread.is_alive()
    assert observer is False
    assert outcome == [True]


@pytest.mark.parametrize("failure", [asyncio.TimeoutError(), EOFError(),
                                      ssl.SSLError("synthetic")])
@pytest.mark.asyncio
async def test_postclaim_network_ambiguity_is_consumed_without_replay(
    failure, module, transport, private_passwd_home
):
    first = transport(Scenario(balance_responses=exact_absent_then(), post_error=failure))
    result = await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert result.status is module.BootstrapStatus.UNKNOWN_AMBIGUOUS
    assert len(posts(first)) == 1
    replay = transport(Scenario(balance_responses=exact_absent_then()))
    result = await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert result.status is not module.BootstrapStatus.READY
    assert not posts(replay)
    assert (private_passwd_home / MARKER).exists()


@pytest.mark.asyncio
async def test_postclaim_cancellation_propagates_and_blocks_replay(
    module, transport, private_passwd_home
):
    first = transport(Scenario(balance_responses=exact_absent_then(),
                               post_error=asyncio.CancelledError()))
    with pytest.raises(asyncio.CancelledError):
        await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert len(posts(first)) == 1
    replay = transport(Scenario(balance_responses=exact_absent_then()))
    await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert not posts(replay)
    assert (private_passwd_home / MARKER).exists()


def test_no_public_path_reset_rearm_retry_or_trading_surface(module) -> None:
    public = {name.lower() for name in vars(module) if not name.startswith("_")}
    assert not public & {"marker", "ledger", "path", "home", "reset", "delete", "rearm",
                         "retry", "order", "cancel", "replace", "position", "trade"}
    source = Path(module.__file__).read_text().lower()
    forbidden = ("$home", "expanduser", "getenv(", "environ", "mainnet", "place_order",
                 "cancel_order", "replace_order", "reduce_only")
    assert not any(token in source for token in forbidden)
