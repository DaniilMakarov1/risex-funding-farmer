import asyncio
import copy
import inspect
import json
import os
from pathlib import Path
import sqlite3

import pytest

from risex_farmer.testnet_risex_private_read_preflight import Outcome
from risex_farmer.testnet_risex_private_read_operational import (
    LifecycleClearBinding,
    OperationalAttempt,
    OperationalJournal,
    SealedTransport,
    SessionSignerCredential,
    fixture_adapter,
    _credential_from_secret,
    _canonical_lifecycle_database,
)


def test_production_surface_is_sealed_and_not_normal_startup_imported():
    signature = inspect.signature(OperationalAttempt)
    assert set(signature.parameters) == set()
    source = Path(__file__).parents[1] / "src/risex_farmer/cli.py"
    assert "private_read_operational" not in source.read_text()
    assert SealedTransport.REST_ORIGIN == "https://api.testnet.rise.trade"
    assert SealedTransport.WS_URL == "wss://api.testnet.rise.trade/ws/"
    assert SealedTransport.TRUST_ENV is False
    assert SealedTransport.ALLOW_REDIRECTS is False
    assert SealedTransport.MAX_BYTES > 0
    assert SealedTransport.MAX_FRAMES == 3


def test_lifecycle_binding_initializes_once_and_rejects_any_nonpristine_state(tmp_path):
    path = tmp_path / "lifecycle.sqlite"
    binding = LifecycleClearBinding._fixture(path)
    assert binding() is True
    assert binding() is True
    assert path.stat().st_mode & 0o777 == 0o600
    canonical = sqlite3.connect(path)
    assert _canonical_lifecycle_database(canonical) is True
    canonical.close()
    db = __import__("sqlite3").connect(path)
    db.execute(
        "INSERT INTO intents VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("x", 1, "OPEN", "1", 1, 0, "d", "b", "PREPARED", "BUY", "MARKET",
         "FOK", 0, 0, 1, "0.0001", 100, "1", 10, "0", 1, 0, None, 0),
    )
    db.commit(); db.close()
    assert binding() is False


def test_lifecycle_binding_rejects_spoofed_noncanonical_schema(tmp_path):
    path = tmp_path / "spoof.sqlite"
    db = sqlite3.connect(path)
    db.executescript(
        "CREATE TABLE intents(x TEXT);"
        "CREATE TABLE cancels(x TEXT);"
        "CREATE TABLE terminal(key TEXT, value TEXT);"
    )
    db.executemany(
        "INSERT INTO terminal VALUES (?, ?)",
        (("account", "0x20f9153e2eeba0ff7880fb5a23e976e8b2af56ee"),
         ("signer", "0x6274d6d9f628ba89c36de4b71efa2c602b7f783b"),
         ("router", "0x980b8621b8e03c3f396e1dc34c00b14d84f2a20f"),
         ("authorization", "0x6da86f486b5e6536358f5b122dbe184522ca0ee3"),
         ("account", "0x20f9153e2eeba0ff7880fb5a23e976e8b2af56ee")),
    )
    db.commit(); db.close(); path.chmod(0o600)
    assert LifecycleClearBinding._fixture(path)() is False


@pytest.mark.parametrize("mutation", ["extra", "wrong", "corrupt", "mode", "symlink"])
def test_lifecycle_binding_rejects_noncanonical_or_unsafe_file(tmp_path, mutation):
    path = tmp_path / "lifecycle.sqlite"
    binding = LifecycleClearBinding._fixture(path)
    assert binding() is True
    if mutation in {"extra", "wrong"}:
        db = sqlite3.connect(path)
        if mutation == "extra":
            db.execute("INSERT INTO terminal VALUES ('unexpected', 'evidence')")
        else:
            db.execute("UPDATE terminal SET value='wrong' WHERE key='router'")
        db.commit(); db.close()
    elif mutation == "corrupt":
        path.write_bytes(b"not a sqlite database")
    elif mutation == "mode":
        path.chmod(0o644)
    else:
        target = tmp_path / "target.sqlite"
        path.rename(target)
        path.symlink_to(target)
    assert binding() is False
    path.unlink()
    assert binding() is False


@pytest.mark.asyncio
async def test_durable_attempt_is_consumed_before_transport_and_never_reenters(tmp_path):
    calls = []
    async def run_once():
        calls.append("network")
        raise asyncio.CancelledError
    journal = OperationalJournal._fixture(tmp_path / "attempt.json")
    adapter = fixture_adapter(journal=journal, runner=run_once)
    with pytest.raises(asyncio.CancelledError):
        await adapter.run()
    assert json.loads((tmp_path / "attempt.json").read_text())["result"] == "PREFLIGHT_BLOCKED"
    result = await adapter.run()
    assert result.outcome == Outcome.BLOCKED
    assert calls == ["network"]


@pytest.mark.asyncio
async def test_adapter_preserves_blocked_result_and_redacts_exception(tmp_path):
    secret = "0x" + "ab" * 32
    async def run_once():
        raise RuntimeError(secret)
    path = tmp_path / "attempt.json"
    result = await fixture_adapter(
        journal=OperationalJournal._fixture(path), runner=run_once,
    ).run()
    assert result.outcome == Outcome.BLOCKED
    assert secret not in path.read_text()
    assert set(json.loads(path.read_text())) == {"schema_version", "result"}


def test_no_order_cancel_close_or_main_wallet_surface():
    import risex_farmer.testnet_risex_private_read_operational as module
    names = set(vars(module))
    assert not any(
        token in name.lower()
        for name in names
        for token in ("place_order", "cancel_order", "close_order", "main_secret")
    )
    source = inspect.getsource(module._load_session_signer_only)
    assert "main_secret" not in source and "register_risex" not in source


def test_signer_only_handle_requires_derived_identity_and_zeroizes():
    from eth_account import Account
    secret = bytes.fromhex("11" * 32)
    signer = Account.from_key(secret).address.lower()
    handle = _credential_from_secret(secret, signer)
    assert handle.material == b""
    with pytest.raises(ValueError):
        _credential_from_secret(secret, "0x" + "22" * 20)
    handle.close()
    assert handle.closed and handle.material == b"" and len(handle._secret) == 0


@pytest.mark.parametrize(
    "mutation",
    ("domain", "type", "account", "signer", "message", "expiration", "nonce"),
)
def test_signer_only_handle_rejects_every_noncanonical_register_v2(mutation):
    from risex_farmer.testnet_risex_private_read_preflight import PrivateReadPreflight
    secret = bytes.fromhex("11" * 32)
    handle = SessionSignerCredential(
        "0x6274d6d9f628ba89c36de4b71efa2c602b7f783b", secret,
    )
    try:
        canonical = PrivateReadPreflight._typed_data("0x0001")
        assert handle.sign_register_v2(canonical).startswith("0x")
        altered = copy.deepcopy(canonical)
        if mutation == "domain": altered["domain"]["name"] = "other"
        elif mutation == "type": altered["types"]["RegisterV2"][0]["type"] = "bytes32"
        elif mutation == "account": altered["message"]["account"] = "0x" + "22" * 20
        elif mutation == "signer": altered["message"]["signer"] = "0x" + "22" * 20
        elif mutation == "message": altered["message"]["message"] = "other"
        elif mutation == "expiration": altered["message"]["expiration"] = 1
        else: altered["message"]["nonce"] = "01"
        with pytest.raises(ValueError, match="rejected"):
            handle.sign_register_v2(altered)
    finally:
        handle.close()


class _Content:
    def __init__(self, body): self.body = body
    async def read(self, _limit): return self.body


class _Response:
    status = 200
    content_length = None
    history = ()
    def __init__(self, url, body=b'{"data":{},"request_id":"x"}'):
        self.url = url; self.content = _Content(body)
    async def __aenter__(self): return self
    async def __aexit__(self, *_args): return None


class _Session:
    def __init__(self, response): self.response = response; self.call = None
    def get(self, *args, **kwargs): self.call = (args, kwargs); return self.response


@pytest.mark.asyncio
async def test_public_transport_rejects_redirect_final_url_and_bounds_body():
    transport = object.__new__(SealedTransport)
    redirected = _Response("https://example.invalid/")
    transport._session = _Session(redirected)
    with pytest.raises(ValueError, match="final URL"):
        await transport.public_get("/v1/system/config", ())
    huge = _Response(
        "https://api.testnet.rise.trade/v1/system/config",
        b"x" * (SealedTransport.MAX_BYTES + 1),
    )
    transport._session = _Session(huge)
    with pytest.raises(ValueError, match="bounded"):
        await transport.public_get("/v1/system/config", ())


@pytest.mark.asyncio
async def test_public_transport_owns_get_method_proxy_redirect_and_exact_query():
    target = "https://api.testnet.rise.trade/v1/orderbook?market_id=1"
    transport = object.__new__(SealedTransport)
    session = _Session(_Response(target))
    transport._session = session
    response = await transport.public_get("/v1/orderbook", (("market_id", "1"),))
    assert response.final_url == target
    assert session.call == ((target,), {"allow_redirects": False, "proxy": None})


@pytest.mark.asyncio
async def test_websocket_rejects_alternate_url_or_outbound_sequence_before_connect():
    transport = object.__new__(SealedTransport)
    transport._session = object()
    with pytest.raises(ValueError, match="sealed"):
        await transport._private_exchange("wss://other.invalid/ws", ({}, {}, {}))
    with pytest.raises(ValueError, match="sequence"):
        await transport._private_exchange(SealedTransport.WS_URL, ({"method": "auth"}, {}, {}))


@pytest.mark.asyncio
async def test_public_transport_rejects_unaccepted_endpoint_before_session():
    transport = object.__new__(SealedTransport)
    transport._session = object()
    with pytest.raises(ValueError, match="surface"):
        await transport.public_get("/v1/orders/place", ())
