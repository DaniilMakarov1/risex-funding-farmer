from __future__ import annotations

import asyncio
import importlib
import inspect
import os
import ssl
import subprocess
import sys
import traceback
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
            assert scenario.closed == 1
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

    unverified = transport(Scenario(balances=["0"]))
    result = await module.bootstrap_risex_account(WALLET, intent="RISEX_TESTNET_DEPOSIT")
    assert result.status is module.BootstrapStatus.SUBMITTED_UNVERIFIED
    assert result.status is not module.BootstrapStatus.READY
    assert len(posts(unverified)) == 1
    assert posts(unverified)[0][2]["json"] == {"account": WALLET, "amount": "1000"}

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
