from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
from typing import Any

import pytest
from eth_abi import encode
from eth_account import Account
from eth_utils import keccak
from yarl import URL


WALLET = "0x20f9153e2eeba0ff7880fb5a23e976e8b2af56ee"
OTHER_WALLET = "0x1111111111111111111111111111111111111111"
AUTH = "0x6da86f486b5e6536358f5b122dbe184522ca0ee3"
WRONG_AUTH = "0x2222222222222222222222222222222222222222"
ORIGIN = "https://api.testnet.rise.trade"
CHAIN_ID = 11_155_931
NOW = 1_800_000_000
EXPIRATION = NOW + 30 * 24 * 60 * 60
MAIN_KEY = bytes.fromhex("11" * 32)
SIGNER_KEY = bytes.fromhex("22" * 32)
MAIN_ADDRESS = Account.from_key(MAIN_KEY).address
SIGNER_ADDRESS = Account.from_key(SIGNER_KEY).address
assert MAIN_ADDRESS.lower() != WALLET  # the fixture must never resemble the real key

REGISTER_TYPE = (
    "RegisterSigner(address account,address signer,string message,uint32 expiration,"
    "uint48 nonceAnchor,uint8 nonceBitmap)"
)
VERIFY_TYPE = "VerifySigner(address account,uint48 nonceAnchor,uint8 nonceBitmap)"
REVOKE_TYPE = (
    "RevokeSigner(address account,address signer,uint48 nonceAnchor,uint8 nonceBitmap)"
)
PUBLISHED_TYPEHASHES = {
    "REGISTER_SIGNER_TYPEHASH": "a526f63b3968e56ae1b177ce9b3dc29766e0891e6397a9c23cf8c53ee8fc8f62",
    "VERIFY_SIGNER_TYPEHASH": "4d298dcceb691695f582cc337308236426a0c97201a31834625e8eadc44d4230",
    "REVOKE_SIGNER_TYPEHASH": "36db7f392f548b56f37d89469115d138685addf06be45684f9e5b0e8b5d28000",
}

CREDENTIAL = ".risex-funding-farmer-risex-session-signer-v1.key"
RECORD = ".risex-funding-farmer-risex-session-signer-v1.json"
GENERATE_INTENT = "RISEX_TESTNET_GENERATE_SESSION_SIGNER"
REGISTER_INTENT = "RISEX_TESTNET_REGISTER_SESSION_SIGNER"


@pytest.fixture
def module():
    try:
        return importlib.import_module("risex_farmer.testnet_risex_signer")
    except ModuleNotFoundError:
        pytest.skip("RED prerequisite: signer module is absent on published main")


@pytest.fixture
def disposable_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, module):
    home = tmp_path / "home"
    home.mkdir(mode=0o700)
    monkeypatch.setattr(module, "_passwd_home", lambda: home, raising=False)
    monkeypatch.setattr(module, "_now_unix", lambda: NOW, raising=False)
    monkeypatch.setattr(
        module, "_generate_private_key", lambda: SIGNER_KEY, raising=False
    )
    return home


def _envelope(data: Any) -> dict[str, Any]:
    return {"data": data, "request_id": "fixture-request"}


def _config(*, chain: str = str(CHAIN_ID), auth: str = AUTH) -> dict[str, Any]:
    return _envelope(
        {"chain": {"name": "Rise Testnet", "chain_id": chain},
         "addresses": {"auth": auth}}
    )


def _observed_additive_config() -> dict[str, Any]:
    return _envelope({
        "addresses": {"auth": AUTH, "unrelated": {"future": True}},
        "chain": {
            "block_time": None,
            "chain_id": str(CHAIN_ID),
            "name": "Rise Testnet",
            "rpc_endpoints": ["synthetic-unrelated"],
            "selected_rpc": {"synthetic": True},
        },
        "is_maintenance_mode": False,
        "maintenance_end_time": None,
        "maintenance_message": {"synthetic": True},
        "maintenance_phase": ["synthetic-unrelated"],
        "maintenance_time": 0,
    })


def _domain(*, chain: str = str(CHAIN_ID), auth: str = AUTH,
            name: str = "RISEx", version: str = "1") -> dict[str, Any]:
    return _envelope(
        {"name": name, "version": version, "chain_id": chain,
         "verifying_contract": auth}
    )


class Response:
    def __init__(self, status: int, url: URL, body: Any) -> None:
        self.status, self.url, self._body = status, url, body

    async def json(self) -> Any:
        if isinstance(self._body, BaseException):
            raise self._body
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_: Any) -> None:
        return None


class Request:
    def __init__(self, response: Response | None,
                 error: BaseException | None = None) -> None:
        self.response, self.error = response, error

    async def _get(self) -> Response:
        if self.error is not None:
            raise self.error
        assert self.response is not None
        return self.response

    def __await__(self):
        return self._get().__await__()

    async def __aenter__(self) -> Response:
        return await self._get()

    async def __aexit__(self, *_: Any) -> None:
        return None


class Scenario:
    def __init__(self) -> None:
        self.config = _config()
        self.domain = _domain()
        self.nonce = _envelope(
            {"nonce_anchor": "0", "current_bitmap_index": 0, "bitmap": "0"}
        )
        self.nonce_sequence: list[dict[str, Any]] = []
        self.status = 0
        self.status_description = "NotExist"
        self.signers: list[dict[str, Any]] = []
        self.post_status = 200
        self.post_body: Any = _envelope(
            {"transaction_hash": "0x" + "ab" * 32, "success": True,
             "status": "Success", "block_number": "1"}
        )
        self.post_error: BaseException | None = None
        self.final_url: URL | None = None
        self.activate_after_post = False
        self.calls: list[tuple[str, URL, dict[str, Any]]] = []
        self.session_kwargs: list[dict[str, Any]] = []
        self.closed = 0

    def active(self) -> None:
        self.status, self.status_description = 1, "Active"
        self.signers = [{
            "signer": SIGNER_ADDRESS,
            "label": "RISEx Funding Farmer testnet probe",
            "expiration": str(EXPIRATION),
            "status": "Active",
            "registered_at": str(NOW),
        }]


class Session:
    def __init__(self, scenario: Scenario, **kwargs: Any) -> None:
        self.scenario = scenario
        scenario.session_kwargs.append(kwargs)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_: Any) -> None:
        self.scenario.closed += 1

    def request(self, method: str, url: str | URL, **kwargs: Any) -> Request:
        method, requested = method.upper(), URL(url)
        actual = requested.with_query(kwargs.get("params") or {})
        self.scenario.calls.append((method, actual, kwargs))
        path = requested.path
        if path == "/v1/system/config":
            body = self.scenario.config
        elif path == "/v1/auth/eip712-domain":
            body = self.scenario.domain
        elif path.startswith("/v1/nonce-state/"):
            body = (self.scenario.nonce_sequence.pop(0)
                    if self.scenario.nonce_sequence else self.scenario.nonce)
        elif path == "/v1/auth/session-key-status":
            body = _envelope({"status": self.scenario.status,
                              "status_description": self.scenario.status_description})
        elif path == "/v1/auth/signers":
            body = _envelope({"signers": self.scenario.signers})
        elif path == "/v1/auth/register-signer" and method == "POST":
            response = Response(
                self.scenario.post_status,
                self.scenario.final_url or actual,
                self.scenario.post_body,
            )
            if self.scenario.activate_after_post:
                self.scenario.active()
            return Request(response, self.scenario.post_error)
        else:
            return Request(Response(404, actual, {"error": "unexpected fixture path"}))
        return Request(Response(200, self.scenario.final_url or actual, body))


@pytest.fixture
def transport(monkeypatch: pytest.MonkeyPatch, module):
    scenarios: list[Scenario] = []

    def install(scenario: Scenario) -> Scenario:
        scenarios.append(scenario)
        monkeypatch.setattr(
            module.aiohttp,
            "ClientSession",
            lambda *args, **kwargs: _make_session(args, kwargs, scenario),
        )
        return scenario

    yield install
    for scenario in scenarios:
        if not scenario.session_kwargs:
            assert not scenario.calls
            continue
        assert scenario.closed == len(scenario.session_kwargs)
        for kwargs in scenario.session_kwargs:
            assert kwargs.get("trust_env") is False
            assert 0 < kwargs["timeout"].total <= 30
            assert kwargs.get("ssl") is not False
        for _method, _url, kwargs in scenario.calls:
            assert kwargs.get("allow_redirects") is False
            assert kwargs.get("ssl") is not False
            assert "proxy" not in kwargs and "proxy_auth" not in kwargs


def _make_session(args: tuple[Any, ...], kwargs: dict[str, Any],
                  scenario: Scenario) -> Session:
    assert not args
    return Session(scenario, **kwargs)


def _posts(scenario: Scenario) -> list[tuple[str, URL, dict[str, Any]]]:
    return [call for call in scenario.calls if call[0] == "POST"]


def _domain_separator() -> bytes:
    typehash = keccak(
        text="EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
    )
    return keccak(encode(
        ["bytes32", "bytes32", "bytes32", "uint256", "address"],
        [typehash, keccak(text="RISEx"), keccak(text="1"), CHAIN_ID, AUTH],
    ))


def _struct_hash(type_name: str, account: str, *, signer: str | None = None,
                 expiration: int | None = None, anchor: int = 1,
                 bitmap: int = 0) -> bytes:
    if type_name == "RegisterSigner":
        return keccak(encode(
            ["bytes32", "address", "address", "bytes32", "uint32", "uint48", "uint8"],
            [keccak(text=REGISTER_TYPE), account, signer,
             keccak(text="RISEx session key"), expiration, anchor, bitmap],
        ))
    if type_name == "VerifySigner":
        return keccak(encode(
            ["bytes32", "address", "uint48", "uint8"],
            [keccak(text=VERIFY_TYPE), account, anchor, bitmap],
        ))
    return keccak(encode(
        ["bytes32", "address", "address", "uint48", "uint8"],
        [keccak(text=REVOKE_TYPE), account, signer, anchor, bitmap],
    ))


def _digest(struct_hash: bytes) -> bytes:
    return keccak(b"\x19\x01" + _domain_separator() + struct_hash)


def _seed_generated(module, home: Path) -> Any:
    result = module.generate_risex_session_signer(intent=GENERATE_INTENT)
    assert (home / CREDENTIAL).exists() and (home / RECORD).exists()
    return result


def _use_fixture_wallet(monkeypatch: pytest.MonkeyPatch, module) -> None:
    monkeypatch.setattr(module, "_EXPECTED_WALLET", MAIN_ADDRESS.lower())


async def _assert_rejected_before_sensitive_work(
    module, monkeypatch: pytest.MonkeyPatch, home: Path, scenario: Scenario,
) -> None:
    sign_calls = claim_calls = loader_calls = 0
    record_before = (home / RECORD).read_text()

    def loader() -> bytes:
        nonlocal loader_calls
        loader_calls += 1
        return MAIN_KEY

    def sign(*_args: Any, **_kwargs: Any) -> str:
        nonlocal sign_calls
        sign_calls += 1
        raise AssertionError("signing must remain unreachable")

    def claim() -> bool:
        nonlocal claim_calls
        claim_calls += 1
        raise AssertionError("durable claim must remain unreachable")

    monkeypatch.setattr(module, "_sign_typed_data", sign)
    monkeypatch.setattr(module, "_claim_registration", claim)
    with pytest.raises(module.SignerSafetyError):
        await module.register_risex_session_signer(
            MAIN_ADDRESS, intent=REGISTER_INTENT, main_secret_loader=loader
        )
    assert (loader_calls, sign_calls, claim_calls) == (0, 0, 0)
    assert json.loads((home / RECORD).read_text())["state"] == "CREATED"
    assert (home / RECORD).read_text() == record_before
    assert not _posts(scenario)


def test_public_api_is_sealed_optional_and_has_no_trading_surface(module) -> None:
    assert module._ORIGIN == URL(ORIGIN)
    assert module._CHAIN_ID == CHAIN_ID
    assert module._AUTH.lower() == AUTH
    assert module._EXPECTED_WALLET == WALLET
    assert tuple(inspect.signature(module.generate_risex_session_signer).parameters) == (
        "intent",
    )
    assert tuple(inspect.signature(module.check_risex_session_signer).parameters) == (
        "wallet",
    )
    assert tuple(inspect.signature(module.register_risex_session_signer).parameters) == (
        "wallet", "intent", "main_secret_loader",
    )
    forbidden = ("sender", "transport", "url", "base_url", "path",
                 "proxy", "order", "place", "cancel", "position",
                 "trade", "mainnet", "nado", "extended", "reset", "delete",
                 "rearm", "retry")
    public = [name.lower() for name in vars(module) if not name.startswith("_")]
    assert not any(term in name for name in public for term in forbidden)
    normal = subprocess.run(
        [sys.executable, "-c", (
            "import sys,risex_farmer.cli;"
            "assert 'risex_farmer.testnet_risex_signer' not in sys.modules"
        )], capture_output=True, text=True, timeout=10, check=False,
    )
    assert normal.returncode == 0, normal.stderr


def test_published_typehashes_and_cross_library_signatures(module) -> None:
    for name, expected in PUBLISHED_TYPEHASHES.items():
        assert getattr(module, name).hex() == expected
    assert keccak(text=REGISTER_TYPE).hex() == PUBLISHED_TYPEHASHES["REGISTER_SIGNER_TYPEHASH"]
    assert keccak(text=VERIFY_TYPE).hex() == PUBLISHED_TYPEHASHES["VERIFY_SIGNER_TYPEHASH"]
    assert keccak(text=REVOKE_TYPE).hex() == PUBLISHED_TYPEHASHES["REVOKE_SIGNER_TYPEHASH"]

    register = module._build_register_typed_data(
        MAIN_ADDRESS, SIGNER_ADDRESS, EXPIRATION, 1, 0
    )
    verify = module._build_verify_typed_data(MAIN_ADDRESS, 1, 0)
    revoke = module._build_revoke_typed_data(MAIN_ADDRESS, SIGNER_ADDRESS, 1, 0)
    expected = {
        "RegisterSigner": _digest(_struct_hash(
            "RegisterSigner", MAIN_ADDRESS, signer=SIGNER_ADDRESS,
            expiration=EXPIRATION,
        )),
        "VerifySigner": _digest(_struct_hash("VerifySigner", MAIN_ADDRESS)),
        "RevokeSigner": _digest(_struct_hash(
            "RevokeSigner", MAIN_ADDRESS, signer=SIGNER_ADDRESS,
        )),
    }
    for typed in (register, verify, revoke):
        assert module._typed_data_digest(typed) == expected[typed["primaryType"]]
    account_sig = module._sign_typed_data(MAIN_KEY, register)
    signer_sig = module._sign_typed_data(SIGNER_KEY, verify)
    revoke_sig = module._sign_typed_data(MAIN_KEY, revoke)
    for signature in (account_sig, signer_sig, revoke_sig):
        assert len(signature) == 132 and signature.startswith("0x")
    assert Account._recover_hash(expected["RegisterSigner"], signature=account_sig) == MAIN_ADDRESS
    assert Account._recover_hash(expected["VerifySigner"], signature=signer_sig) == SIGNER_ADDRESS
    assert Account._recover_hash(expected["RevokeSigner"], signature=revoke_sig) == MAIN_ADDRESS


def test_generation_is_single_owner_only_and_secret_free_public_record(
    module, disposable_home: Path
) -> None:
    generated = _seed_generated(module, disposable_home)
    assert generated.state.value == "CREATED"
    credential, record = disposable_home / CREDENTIAL, disposable_home / RECORD
    assert stat.S_IMODE(credential.stat().st_mode) == 0o600
    assert stat.S_IMODE(record.stat().st_mode) == 0o600
    assert credential.stat().st_nlink == record.stat().st_nlink == 1
    data = json.loads(record.read_text())
    assert data == {
        "schema_version": 1,
        "venue": "RISEx",
        "host": "api.testnet.rise.trade",
        "chain_id": CHAIN_ID,
        "wallet": WALLET,
        "operation": "SESSION_SIGNER_REGISTRATION",
        "signer": SIGNER_ADDRESS.lower(),
        "expiration": EXPIRATION,
        "state": "CREATED",
    }
    assert SIGNER_KEY.hex() not in record.read_text().lower()
    with pytest.raises(module.SignerSafetyError):
        module.generate_risex_session_signer(intent=GENERATE_INTENT)


@pytest.mark.parametrize("bad", ["mode", "partial", "symlink", "second-link"])
def test_credential_or_record_corruption_fails_closed(
    module, disposable_home: Path, bad: str
) -> None:
    _seed_generated(module, disposable_home)
    credential, record = disposable_home / CREDENTIAL, disposable_home / RECORD
    target = credential if bad in {"mode", "symlink", "second-link"} else record
    if bad == "mode":
        target.chmod(0o644)
    elif bad == "partial":
        target.write_text("{")
    elif bad == "symlink":
        target.unlink()
        target.symlink_to(record)
    else:
        os.link(target, disposable_home / "extra-link")
    with pytest.raises(module.SignerSafetyError):
        module.generate_risex_session_signer(intent=GENERATE_INTENT)


def test_async_and_process_generation_races_create_one_pair(
    module, disposable_home: Path
) -> None:
    async def race() -> list[Any]:
        return await asyncio.gather(*(
            asyncio.to_thread(
                module.generate_risex_session_signer, intent=GENERATE_INTENT
            ) for _ in range(8)
        ), return_exceptions=True)

    results = asyncio.run(race())
    assert sum(not isinstance(value, BaseException) for value in results) == 1
    assert (disposable_home / CREDENTIAL).exists()
    assert (disposable_home / RECORD).exists()

    subprocess_home = disposable_home.parent / "process-home"
    subprocess_home.mkdir(mode=0o700)
    code = (
        "import sys; from pathlib import Path; "
        "import risex_farmer.testnet_risex_signer as m; "
        "m._passwd_home=lambda:Path(sys.argv[1]); "
        "m._now_unix=lambda:1800000000; "
        "m._generate_private_key=lambda:bytes.fromhex('22'*32); "
        "\ntry: m.generate_risex_session_signer(intent='RISEX_TESTNET_GENERATE_SESSION_SIGNER'); print('won')"
        "\nexcept Exception: print('blocked')"
    )
    processes = [subprocess.Popen(
        [sys.executable, "-c", code, str(subprocess_home)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        env={**os.environ, "PYTHONPATH": str(Path.cwd() / "src")},
    ) for _ in range(6)]
    outputs = [process.communicate(timeout=10)[0].strip() for process in processes]
    assert outputs.count("won") == 1 and outputs.count("blocked") == 5

    claim_code = (
        "import sys; from pathlib import Path; "
        "import risex_farmer.testnet_risex_signer as m; "
        "m._passwd_home=lambda:Path(sys.argv[1]); "
        "print('dispatch-token' if m._claim_registration() else 'blocked')"
    )
    claimers = [subprocess.Popen(
        [sys.executable, "-c", claim_code, str(subprocess_home)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        env={**os.environ, "PYTHONPATH": str(Path.cwd() / "src")},
    ) for _ in range(6)]
    claims = [process.communicate(timeout=10)[0].strip() for process in claimers]
    assert claims.count("dispatch-token") == 1 and claims.count("blocked") == 5


@pytest.mark.asyncio
async def test_observed_additive_system_config_preserves_required_identity(
    module, transport,
) -> None:
    scenario = transport(Scenario())
    scenario.config = _observed_additive_config()
    async with module._session() as session:
        await module._identity(session)


@pytest.mark.asyncio
async def test_observed_operational_shapes_reach_only_main_secret_boundary(
    module, monkeypatch: pytest.MonkeyPatch, disposable_home: Path, transport,
) -> None:
    _use_fixture_wallet(monkeypatch, module)
    _seed_generated(module, disposable_home)
    scenario = transport(Scenario())
    scenario.config = _observed_additive_config()
    scenario.nonce = _envelope({
        "nonce_anchor": "0", "current_bitmap_index": 0, "bitmap": "0x0",
    })
    loaded = signed = claimed = 0
    record_before = (disposable_home / RECORD).read_text()

    def loader() -> bytes:
        nonlocal loaded
        loaded += 1
        raise RuntimeError("synthetic loader stop")

    def sign(*_args: Any, **_kwargs: Any) -> str:
        nonlocal signed
        signed += 1
        raise AssertionError("signing must remain unreachable")

    def claim() -> bool:
        nonlocal claimed
        claimed += 1
        raise AssertionError("claim must remain unreachable")

    monkeypatch.setattr(module, "_sign_typed_data", sign)
    monkeypatch.setattr(module, "_claim_registration", claim)
    with pytest.raises(module.SignerSafetyError):
        await module.register_risex_session_signer(
            MAIN_ADDRESS, intent=REGISTER_INTENT, main_secret_loader=loader
        )
    assert (loaded, signed, claimed) == (1, 0, 0)
    assert (disposable_home / RECORD).read_text() == record_before
    assert not _posts(scenario)


@pytest.mark.parametrize("mutation", [
    "data-missing", "data-wrong", "data-type", "chain-missing", "chain-wrong",
    "chain-type",
    "name-missing", "name-wrong", "name-type", "chain-id-missing",
    "chain-id-wrong", "chain-id-type", "addresses-missing",
    "addresses-wrong", "addresses-type", "auth-missing", "auth-wrong",
    "auth-type",
])
@pytest.mark.asyncio
async def test_required_config_identity_rejects_before_sensitive_work(
    module, monkeypatch: pytest.MonkeyPatch, disposable_home: Path, transport,
    mutation: str,
) -> None:
    _use_fixture_wallet(monkeypatch, module)
    _seed_generated(module, disposable_home)
    scenario = transport(Scenario())
    config = _observed_additive_config()
    data = config["data"]
    if mutation == "data-missing":
        del config["data"]
    elif mutation == "data-wrong":
        config["data"] = {"unrelated": True}
    elif mutation == "data-type":
        config["data"] = []
    elif mutation == "chain-missing":
        del data["chain"]
    elif mutation == "chain-wrong":
        data["chain"] = {"name": "Wrong", "chain_id": "1"}
    elif mutation == "chain-type":
        data["chain"] = []
    elif mutation == "name-missing":
        del data["chain"]["name"]
    elif mutation == "name-wrong":
        data["chain"]["name"] = "Wrong"
    elif mutation == "name-type":
        data["chain"]["name"] = ["Rise Testnet"]
    elif mutation == "chain-id-missing":
        del data["chain"]["chain_id"]
    elif mutation == "chain-id-wrong":
        data["chain"]["chain_id"] = "1"
    elif mutation == "chain-id-type":
        data["chain"]["chain_id"] = CHAIN_ID
    elif mutation == "addresses-missing":
        del data["addresses"]
    elif mutation == "addresses-wrong":
        data["addresses"] = {"auth": WRONG_AUTH}
    elif mutation == "addresses-type":
        data["addresses"] = []
    elif mutation == "auth-missing":
        del data["addresses"]["auth"]
    elif mutation == "auth-wrong":
        data["addresses"]["auth"] = WRONG_AUTH
    else:
        data["addresses"]["auth"] = {"address": AUTH}
    scenario.config = config
    await _assert_rejected_before_sensitive_work(
        module, monkeypatch, disposable_home, scenario
    )


@pytest.mark.parametrize("bitmap,expected", [
    ("0x0", 0),
    ("0x7", 7),
    ("0x" + "f" * 64, 2**256 - 1),
])
@pytest.mark.asyncio
async def test_official_hex_bitmap_parses_exact_uint256(
    module, transport, bitmap: str, expected: int,
) -> None:
    scenario = transport(Scenario())
    scenario.nonce = _envelope({
        "nonce_anchor": "40", "current_bitmap_index": 208, "bitmap": bitmap,
    })
    async with module._session() as session:
        parsed = await module._nonce(session, MAIN_ADDRESS.lower())
    assert parsed.observed_anchor == 40
    assert parsed.observed_index == 208
    assert parsed.observed_bitmap == expected
    assert parsed.signed_anchor == 41 and parsed.signed_bitmap == 0


@pytest.mark.parametrize("bitmap", [
    "0x", "-0x1", "+0x1", " 0x1", "0x1 ", "0x1 0", "0xg", "0X0",
    "0", 0, True, {"hex": "0x0"}, "0x1" + "0" * 64,
])
@pytest.mark.asyncio
async def test_invalid_hex_bitmap_rejects_before_sensitive_work(
    module, monkeypatch: pytest.MonkeyPatch, disposable_home: Path, transport,
    bitmap: Any,
) -> None:
    _use_fixture_wallet(monkeypatch, module)
    _seed_generated(module, disposable_home)
    scenario = transport(Scenario())
    scenario.config = _observed_additive_config()
    scenario.nonce = _envelope({
        "nonce_anchor": "0", "current_bitmap_index": 0, "bitmap": bitmap,
    })
    await _assert_rejected_before_sensitive_work(
        module, monkeypatch, disposable_home, scenario
    )


@pytest.mark.parametrize("mutation", [
    "chain", "auth", "domain-name", "domain-version", "verifier", "redirect", "nonce-anchor",
    "nonce-index", "nonce-bitmap", "expiration",
])
@pytest.mark.asyncio
async def test_identity_nonce_expiration_gates_precede_secret_load(
    module, monkeypatch: pytest.MonkeyPatch, disposable_home: Path, transport,
    mutation: str,
) -> None:
    _use_fixture_wallet(monkeypatch, module)
    _seed_generated(module, disposable_home)
    scenario = transport(Scenario())
    if mutation == "chain":
        scenario.config = _config(chain="1")
    elif mutation == "auth":
        scenario.config = _config(auth=WRONG_AUTH)
    elif mutation == "domain-name":
        scenario.domain = _domain(name="Wrong")
    elif mutation == "domain-version":
        scenario.domain = _domain(version="2")
    elif mutation == "verifier":
        scenario.domain = _domain(auth=WRONG_AUTH)
    elif mutation == "redirect":
        scenario.final_url = URL("https://api.rise.trade/v1/system/config")
    elif mutation == "nonce-anchor":
        scenario.nonce = _envelope({"nonce_anchor": str(2**48 - 1),
                                    "current_bitmap_index": 0, "bitmap": "0"})
    elif mutation == "nonce-index":
        scenario.nonce = _envelope({"nonce_anchor": "0",
                                    "current_bitmap_index": 209, "bitmap": "0"})
    elif mutation == "nonce-bitmap":
        scenario.nonce = _envelope({"nonce_anchor": "0",
                                    "current_bitmap_index": 0, "bitmap": "bad"})
    else:
        monkeypatch.setattr(module, "_now_unix", lambda: 2**32 - 10)
    loaded = 0

    def loader() -> bytes:
        nonlocal loaded
        loaded += 1
        return MAIN_KEY

    with pytest.raises(module.SignerSafetyError):
        await module.register_risex_session_signer(
            MAIN_ADDRESS, intent=REGISTER_INTENT, main_secret_loader=loader
        )
    assert loaded == 0 and not _posts(scenario)


@pytest.mark.asyncio
async def test_wrong_wallet_fails_before_loader_or_transport(
    module, disposable_home: Path, transport,
) -> None:
    _seed_generated(module, disposable_home)
    scenario = transport(Scenario())
    loaded = 0

    def loader() -> bytes:
        nonlocal loaded
        loaded += 1
        return MAIN_KEY

    with pytest.raises(module.SignerSafetyError):
        await module.register_risex_session_signer(
            OTHER_WALLET, intent=REGISTER_INTENT, main_secret_loader=loader
        )
    assert loaded == 0 and not scenario.calls


@pytest.mark.asyncio
async def test_full_nonce_anchor_208_advances_anchor_and_uses_bit_zero(
    module, monkeypatch: pytest.MonkeyPatch, disposable_home: Path, transport,
) -> None:
    _use_fixture_wallet(monkeypatch, module)
    _seed_generated(module, disposable_home)
    scenario = transport(Scenario())
    scenario.nonce = _envelope({"nonce_anchor": "40", "current_bitmap_index": 208,
                                "bitmap": str(2**208 - 1)})
    scenario.activate_after_post = True
    result = await module.register_risex_session_signer(
        MAIN_ADDRESS, intent=REGISTER_INTENT, main_secret_loader=lambda: MAIN_KEY
    )
    assert result.state.value == "ACTIVE"
    body = _posts(scenario)[0][2]["json"]
    assert body["nonce_anchor"] == "41" and body["nonce_bitmap_index"] == 0


@pytest.mark.asyncio
async def test_key_mismatch_and_chained_library_errors_are_sanitized(
    module, monkeypatch: pytest.MonkeyPatch, disposable_home: Path, transport,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _use_fixture_wallet(monkeypatch, module)
    _seed_generated(module, disposable_home)
    scenario = transport(Scenario())
    sentinel = "synthetic-sensitive-sdk-value"

    def wrong_loader() -> bytes:
        return bytes.fromhex("33" * 32)

    with pytest.raises(module.SignerSafetyError) as captured:
        await module.register_risex_session_signer(
            MAIN_ADDRESS, intent=REGISTER_INTENT, main_secret_loader=wrong_loader
        )
    assert sentinel not in repr(captured.value)
    assert not _posts(scenario)

    def broken_loader() -> bytes:
        try:
            raise ValueError(sentinel)
        except ValueError as cause:
            raise RuntimeError(sentinel) from cause

    with pytest.raises(module.SignerSafetyError) as captured:
        await module.register_risex_session_signer(
            MAIN_ADDRESS, intent=REGISTER_INTENT, main_secret_loader=broken_loader
        )
    output = capsys.readouterr()
    assert sentinel not in repr(captured.value) + output.out + output.err
    assert captured.value.__cause__ is None and not _posts(scenario)


@pytest.mark.asyncio
async def test_valid_mode_wrong_signer_credential_fails_before_main_loader(
    module, monkeypatch: pytest.MonkeyPatch, disposable_home: Path, transport,
) -> None:
    _use_fixture_wallet(monkeypatch, module)
    _seed_generated(module, disposable_home)
    credential = disposable_home / CREDENTIAL
    credential.write_bytes(bytes.fromhex("44" * 32))
    credential.chmod(0o600)
    scenario = transport(Scenario())
    loaded = 0

    def loader() -> bytes:
        nonlocal loaded
        loaded += 1
        return MAIN_KEY

    with pytest.raises(module.SignerSafetyError) as captured:
        await module.register_risex_session_signer(
            MAIN_ADDRESS, intent=REGISTER_INTENT, main_secret_loader=loader
        )
    assert loaded == 0 and not _posts(scenario)
    assert bytes.fromhex("44" * 32).hex() not in repr(captured.value)


@pytest.mark.asyncio
async def test_authoritative_active_is_zero_post_and_local_active_is_not_authority(
    module, monkeypatch: pytest.MonkeyPatch, disposable_home: Path, transport,
) -> None:
    _use_fixture_wallet(monkeypatch, module)
    _seed_generated(module, disposable_home)
    scenario = transport(Scenario())
    scenario.active()
    loaded = 0

    def loader() -> bytes:
        nonlocal loaded
        loaded += 1
        return MAIN_KEY

    result = await module.register_risex_session_signer(
        MAIN_ADDRESS, intent=REGISTER_INTENT, main_secret_loader=loader
    )
    assert result.state.value == "ACTIVE"
    assert loaded == 0 and not _posts(scenario)

    scenario.status = 0
    scenario.status_description = "NotExist"
    scenario.signers = []
    checked = await module.check_risex_session_signer(MAIN_ADDRESS)
    assert checked.state.value != "ACTIVE"


@pytest.mark.asyncio
async def test_one_registration_dispatch_reconciles_ambiguity_without_retry(
    module, monkeypatch: pytest.MonkeyPatch, disposable_home: Path, transport,
) -> None:
    _use_fixture_wallet(monkeypatch, module)
    _seed_generated(module, disposable_home)
    scenario = transport(Scenario())
    scenario.post_error = TimeoutError("synthetic timeout")
    scenario.activate_after_post = True

    result = await module.register_risex_session_signer(
        MAIN_ADDRESS, intent=REGISTER_INTENT, main_secret_loader=lambda: MAIN_KEY
    )
    assert result.state.value == "ACTIVE"
    assert len(_posts(scenario)) == 1
    post = _posts(scenario)[0][2]["json"]
    assert post.keys() == {
        "account", "signer", "message", "nonce_anchor", "expiration",
        "account_signature", "signer_signature", "nonce_bitmap_index", "label",
    }
    assert post["account"].lower() == MAIN_ADDRESS.lower()
    assert post["signer"].lower() == SIGNER_ADDRESS.lower()
    assert post["nonce_anchor"] == "1" and post["nonce_bitmap_index"] == 0
    assert post["expiration"] == str(EXPIRATION)
    assert len(post["account_signature"]) == len(post["signer_signature"]) == 132

    second = await module.register_risex_session_signer(
        MAIN_ADDRESS, intent=REGISTER_INTENT, main_secret_loader=lambda: MAIN_KEY
    )
    assert second.state.value == "ACTIVE" and len(_posts(scenario)) == 1
    status_reads = [call for call in scenario.calls
                    if call[1].path == "/v1/auth/session-key-status"]
    list_reads = [call for call in scenario.calls
                  if call[1].path == "/v1/auth/signers"]
    assert len(status_reads) <= 3 and len(list_reads) <= 3


@pytest.mark.asyncio
async def test_post_success_without_authoritative_active_state_is_not_active(
    module, monkeypatch: pytest.MonkeyPatch, disposable_home: Path, transport,
) -> None:
    _use_fixture_wallet(monkeypatch, module)
    _seed_generated(module, disposable_home)
    scenario = transport(Scenario())
    result = await module.register_risex_session_signer(
        MAIN_ADDRESS, intent=REGISTER_INTENT, main_secret_loader=lambda: MAIN_KEY
    )
    assert len(_posts(scenario)) == 1
    assert result.state.value == "SPENT_UNKNOWN"


@pytest.mark.asyncio
async def test_nonce_change_between_signing_and_dispatch_fails_without_claim(
    module, monkeypatch: pytest.MonkeyPatch, disposable_home: Path, transport,
) -> None:
    _use_fixture_wallet(monkeypatch, module)
    _seed_generated(module, disposable_home)
    scenario = transport(Scenario())
    scenario.nonce_sequence = [
        _envelope({"nonce_anchor": "0", "current_bitmap_index": 0, "bitmap": "0"}),
        _envelope({"nonce_anchor": "1", "current_bitmap_index": 0, "bitmap": "0"}),
    ]
    with pytest.raises(module.SignerSafetyError):
        await module.register_risex_session_signer(
            MAIN_ADDRESS, intent=REGISTER_INTENT, main_secret_loader=lambda: MAIN_KEY
        )
    assert not _posts(scenario)
    assert json.loads((disposable_home / RECORD).read_text())["state"] == "CREATED"


@pytest.mark.asyncio
async def test_concurrent_registration_claims_at_most_one_post_and_fsyncs_first(
    module, monkeypatch: pytest.MonkeyPatch, disposable_home: Path, transport,
) -> None:
    _use_fixture_wallet(monkeypatch, module)
    _seed_generated(module, disposable_home)
    scenario = transport(Scenario())
    scenario.activate_after_post = True
    events: list[str] = []
    real_fsync = module.os.fsync

    def observed_fsync(fd: int) -> None:
        events.append("fsync")
        real_fsync(fd)

    monkeypatch.setattr(module.os, "fsync", observed_fsync)

    original_request = Session.request

    def observed_request(self: Session, method: str, url: str | URL,
                         **kwargs: Any) -> Request:
        if method.upper() == "POST":
            events.append("post")
        return original_request(self, method, url, **kwargs)

    monkeypatch.setattr(Session, "request", observed_request)
    results = await asyncio.gather(*(
        module.register_risex_session_signer(
            MAIN_ADDRESS, intent=REGISTER_INTENT, main_secret_loader=lambda: MAIN_KEY
        ) for _ in range(6)
    ), return_exceptions=True)
    assert len(_posts(scenario)) == 1
    post_index = events.index("post")
    assert events[:post_index].count("fsync") >= 2
    assert any(not isinstance(value, BaseException) for value in results)


@pytest.mark.asyncio
async def test_crash_or_cancellation_after_claim_is_consumed_and_never_replays(
    module, monkeypatch: pytest.MonkeyPatch, disposable_home: Path, transport,
) -> None:
    _use_fixture_wallet(monkeypatch, module)
    _seed_generated(module, disposable_home)
    assert module._claim_registration() is True
    assert module._claim_registration() is False
    scenario = transport(Scenario())
    result = await module.register_risex_session_signer(
        MAIN_ADDRESS, intent=REGISTER_INTENT, main_secret_loader=lambda: MAIN_KEY
    )
    assert result.state.value == "SPENT_UNKNOWN" and not _posts(scenario)

    # Cancellation is BaseException behavior and must not be sanitized or re-arm state.
    disposable_home.joinpath(RECORD).unlink()
    disposable_home.joinpath(CREDENTIAL).unlink()
    _seed_generated(module, disposable_home)
    scenario.post_error = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await module.register_risex_session_signer(
            MAIN_ADDRESS, intent=REGISTER_INTENT,
            main_secret_loader=lambda: MAIN_KEY,
        )
    assert len(_posts(scenario)) == 1 and module._claim_registration() is False


@pytest.mark.parametrize("status,description,list_status", [
    (1, "Active", "Expired"), (2, "Revoked", "Active"),
    (1, "Active", "Revoked"),
])
@pytest.mark.asyncio
async def test_status_list_contradiction_is_never_active(
    module, monkeypatch: pytest.MonkeyPatch, disposable_home: Path, transport,
    status: int, description: str, list_status: str,
) -> None:
    _use_fixture_wallet(monkeypatch, module)
    _seed_generated(module, disposable_home)
    scenario = transport(Scenario())
    scenario.status, scenario.status_description = status, description
    scenario.signers = [{"signer": SIGNER_ADDRESS,
                         "label": "RISEx Funding Farmer testnet probe",
                         "expiration": str(EXPIRATION), "status": list_status,
                         "registered_at": str(NOW)}]
    result = await module.check_risex_session_signer(MAIN_ADDRESS)
    assert result.state.value != "ACTIVE"


def test_revoke_is_fixture_only_exact_and_has_no_dispatch(module) -> None:
    typed = module._build_revoke_typed_data(MAIN_ADDRESS, SIGNER_ADDRESS, 1, 0)
    signature = module._sign_typed_data(MAIN_KEY, typed)
    request = module._build_revoke_request(
        MAIN_ADDRESS, SIGNER_ADDRESS, 1, 0, signature
    )
    assert request == {
        "account": MAIN_ADDRESS.lower(),
        "signer": SIGNER_ADDRESS.lower(),
        "nonce_anchor": "1",
        "nonce_bitmap_index": 0,
        "account_signature": signature,
    }
    assert Account._recover_hash(
        _digest(_struct_hash("RevokeSigner", MAIN_ADDRESS,
                             signer=SIGNER_ADDRESS)),
        signature=signature,
    ) == MAIN_ADDRESS
    source = inspect.getsource(module)
    assert source.count('"POST"') == 1
    assert "/v1/auth/revoke-signer" not in source
    assert not any(name.startswith("revoke") for name in vars(module))


def test_module_absence_is_the_only_expected_old_main_red() -> None:
    spec = importlib.util.find_spec("risex_farmer.testnet_risex_signer")
    assert spec is not None, "RED: exact accepted main has no signer module"
