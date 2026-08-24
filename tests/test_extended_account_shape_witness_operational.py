import asyncio
import hashlib
import inspect
import json
import os
from pathlib import Path
import sqlite3
import stat
import subprocess
import sys

import pytest

from risex_farmer import extended_account_shape_witness as core
from risex_farmer import extended_account_shape_witness_operational as operational


SECRET = "synthetic-account-shape-key"


def _body():
    return {
        "status": "OK",
        "data": {
            "id": 937465,
            "description": "private synthetic description",
            "accountIndex": 3,
            "status": "ACTIVE",
            "l2Key": "0xprivate-synthetic-l2-key",
            "l2Vault": 9,
            "bridgeStarknetAddress": None,
        },
        "error": None,
        "pagination": None,
    }


def _transport_metadata():
    return {
        "actual_url": core.ACCOUNT_INFO_URL,
        "method": "GET",
        "direct_tls": True,
        "trust_env": False,
        "proxy": None,
        "redirects": 0,
        "retries": 0,
    }


class _Capability:
    def __init__(self, secret=SECRET):
        self.secret = secret
        self.closed = False

    def x_api_key_header_value(self):
        return self.secret

    def close(self):
        self.secret = ""
        self.closed = True


class _Source:
    def __init__(self, capability=None):
        self.capability = capability or _Capability()
        self.calls = 0

    def open(self):
        self.calls += 1
        return self.capability


class _Transport:
    def __init__(self, *, error=None):
        self.calls = []
        self.error = error

    async def get(self, request):
        self.calls.append(request)
        if self.error is not None:
            raise self.error
        return {
            "body": _body(),
            "body_bytes": 511,
            "transport": _transport_metadata(),
        }


async def _run(tmp_path, *, source=None, transport=None, hook=None):
    source = source or _Source()
    transport = transport or _Transport()
    result = await operational._fixture_run(
        store=operational._OperationalStore(tmp_path / "witness.sqlite3"),
        credential_source=source,
        transport=transport,
        _effect_hook=hook,
    )
    return result, source, transport


def _set_fixed_home(monkeypatch, home):
    path = home / operational.STORE_BASENAME
    monkeypatch.setattr(operational, "_home", lambda: home)
    monkeypatch.setattr(
        operational,
        "EXPECTED_STORE_PATH_SHA256",
        hashlib.sha256(os.fsencode(path)).hexdigest(),
    )
    return path


def _write_key(home, value=SECRET.encode()):
    path = home / operational.API_KEY_BASENAME
    path.write_bytes(value)
    path.chmod(0o600)
    return path


def test_fixed_surface_and_normal_startup_isolation():
    assert operational.INVOCATION_ID == (
        "extended-account-shape-witness-20260824-new-op-003"
    )
    assert operational.STORE_BASENAME == (
        ".risex-funding-farmer-extended-account-shape-witness-"
        "20260824-new-op-003.sqlite3"
    )
    assert operational.API_KEY_BASENAME == (
        ".risex-funding-farmer-extended-api-key-v1"
    )
    assert operational.EXPECTED_STORE_PATH_SHA256 == (
        "c4c769e78cbfb76ae807510b5d6efbd5f393b15ac8dea4c1f468522fc68cbcdf"
    )
    assert list(inspect.signature(operational.run).parameters) == []
    assert list(inspect.signature(operational.prearm).parameters) == []
    source = inspect.getsource(operational)
    for forbidden in ("extended-identity", "stark", "private-key", "l1"):
        assert forbidden not in source.lower()
    completed = subprocess.run(
        [sys.executable, "-c", (
            "import sys,risex_farmer;"
            "assert 'risex_farmer.extended_account_shape_witness_operational' "
            "not in sys.modules"
        )],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": "src"},
    )
    assert completed.returncode == 0, completed.stderr


@pytest.mark.asyncio
async def test_success_binds_core_one_get_six_counters_and_redacted_evidence(tmp_path):
    result, source, transport = await _run(tmp_path)
    assert (result.status, result.reason) == (
        "CAPTURED", "ACCOUNT_SHAPE_CAPTURED"
    )
    assert result.schema_version == 1
    assert result.invocation_id == operational.INVOCATION_ID
    assert result.path == operational.REDACTED_STORE_PATH
    assert set(result.counters) == set(core._empty_counters())
    assert set(result.counters.values()) == {1}
    assert source.calls == 1 and source.capability.closed
    assert len(transport.calls) == 1
    request = transport.calls[0]
    assert (request.method, request.url, request.headers) == (
        "GET", core.ACCOUNT_INFO_URL, {"X-Api-Key": SECRET}
    )
    evidence = json.loads(result.evidence())
    assert set(evidence) == {
        "counters", "descriptor", "invocation_id", "path", "reason",
        "schema_version", "status",
    }
    durable = (tmp_path / "witness.sqlite3").read_bytes()
    for forbidden in (
        SECRET.encode(), b"937465", b"private synthetic description",
        b"0xprivate-synthetic-l2-key", os.fsencode(tmp_path),
    ):
        assert forbidden not in durable
    assert b"hash" not in result.evidence().encode().lower()
    assert b"size" not in result.evidence().encode().lower()


@pytest.mark.asyncio
async def test_terminal_reentry_is_identical_with_zero_effects(tmp_path):
    first, _, _ = await _run(tmp_path)
    source, transport = _Source(), _Transport()
    second = await operational._fixture_run(
        store=operational._OperationalStore(tmp_path / "witness.sqlite3"),
        credential_source=source,
        transport=transport,
    )
    assert second == first
    assert source.calls == 0 and transport.calls == []


class _ProcessDeath(BaseException):
    pass


@pytest.mark.asyncio
@pytest.mark.parametrize("effect", core.EFFECTS)
@pytest.mark.parametrize("point", ["after_attempt", "before_completion"])
async def test_crash_consumes_one_shot_and_reentry_has_zero_effects(
    tmp_path, effect, point
):
    def hook(current, current_point):
        if (current, current_point) == (effect, point):
            raise _ProcessDeath

    with pytest.raises(_ProcessDeath):
        await _run(tmp_path, hook=hook)
    source, transport = _Source(), _Transport()
    result = await operational._fixture_run(
        store=operational._OperationalStore(tmp_path / "witness.sqlite3"),
        credential_source=source,
        transport=transport,
    )
    assert result.status == "UNKNOWN"
    assert source.calls == 0 and transport.calls == []


@pytest.mark.asyncio
async def test_cancellation_is_terminal_and_exception_text_is_redacted(tmp_path):
    def hook(effect, point):
        if (effect, point) == ("account_info", "after_attempt"):
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await _run(tmp_path, hook=hook)
    source, transport = _Source(), _Transport()
    cancelled = await operational._fixture_run(
        store=operational._OperationalStore(tmp_path / "witness.sqlite3"),
        credential_source=source,
        transport=transport,
    )
    assert (cancelled.status, cancelled.reason) == ("BLOCKED", "CANCELLED")
    assert source.calls == 0 and transport.calls == []

    other = tmp_path / "other"
    other.mkdir()
    secret = "exception-secret-and-account-88331"
    blocked, _, _ = await _run(
        other, transport=_Transport(error=RuntimeError(secret))
    )
    assert (blocked.status, blocked.reason) == (
        "BLOCKED", "ACCOUNT_INFO_UNRESOLVED"
    )
    assert secret not in blocked.evidence()
    assert secret.encode() not in (other / "witness.sqlite3").read_bytes()


def test_prearm_is_metadata_only_and_creates_nothing(tmp_path, monkeypatch):
    home = tmp_path / "passwd-home"
    home.mkdir(mode=0o700)
    store_path = _set_fixed_home(monkeypatch, home)
    key_path = _write_key(home)
    original_open = operational.os.open
    opened = []

    def recording_open(path, flags, *args, **kwargs):
        opened.append((path, flags, kwargs))
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(operational.os, "open", recording_open)
    monkeypatch.setattr(
        operational.os, "readv",
        lambda *_: (_ for _ in ()).throw(AssertionError("credential read")),
    )
    before = set(home.iterdir())
    result = operational.prearm()
    assert (result.status, result.reason) == ("READY", "PREARM_READY")
    assert set(home.iterdir()) == before == {key_path}
    assert not store_path.exists()
    assert len(opened) == 1 and Path(opened[0][0]) == home


def test_prearm_rejects_used_store_and_bad_credential_metadata(tmp_path, monkeypatch):
    home = tmp_path / "passwd-home"
    home.mkdir(mode=0o700)
    store_path = _set_fixed_home(monkeypatch, home)
    _write_key(home)
    store_path.write_bytes(b"used")
    store_path.chmod(0o600)
    with pytest.raises(core.WitnessViolation, match="DURABLE_STORE_ALREADY_EXISTS"):
        operational.prearm()
    store_path.unlink()
    key_path = home / operational.API_KEY_BASENAME
    key_path.chmod(0o644)
    with pytest.raises(core.WitnessViolation, match="CREDENTIAL_CAPABILITY_INVALID"):
        operational.prearm()


@pytest.mark.parametrize(
    "defect",
    [
        "missing", "symlink", "directory", "mode", "empty", "oversize",
        "hardlink", "whitespace", "newline", "non_ascii",
    ],
)
def test_api_key_file_and_canonical_value_adverse_cases_block(
    tmp_path, monkeypatch, defect
):
    home = tmp_path / "passwd-home"
    home.mkdir(mode=0o700)
    _set_fixed_home(monkeypatch, home)
    key = _write_key(home)
    if defect == "missing":
        key.unlink()
    elif defect == "symlink":
        key.unlink()
        target = home / "target"
        target.write_bytes(SECRET.encode())
        target.chmod(0o600)
        key.symlink_to(target)
    elif defect == "directory":
        key.unlink()
        key.mkdir(mode=0o700)
    elif defect == "mode":
        key.chmod(0o644)
    elif defect == "empty":
        key.write_bytes(b"")
    elif defect == "oversize":
        key.write_bytes(b"x" * 513)
    elif defect == "hardlink":
        os.link(key, home / "second-link")
    elif defect == "whitespace":
        key.write_bytes(b"bad key")
    elif defect == "newline":
        key.write_bytes(b"bad-key\n")
    elif defect == "non_ascii":
        key.write_bytes(b"bad-\xff-key")
    if key.exists() and not key.is_symlink() and key.is_file():
        key.chmod(0o600 if defect != "mode" else 0o644)
    with pytest.raises(core.WitnessViolation):
        operational._PasswdHomeApiKeySource().open()


def test_api_key_dirfd_nofollow_and_zeroized_close(tmp_path, monkeypatch):
    home = tmp_path / "passwd-home"
    home.mkdir(mode=0o700)
    _set_fixed_home(monkeypatch, home)
    key = _write_key(home)
    original_open = operational.os.open
    opened = []

    def recording_open(path, flags, *args, **kwargs):
        opened.append((path, flags, kwargs))
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(operational.os, "open", recording_open)
    capability = operational._PasswdHomeApiKeySource().open()
    key_open = next(item for item in opened if item[0] == operational.API_KEY_BASENAME)
    assert key_open[1] & os.O_NOFOLLOW
    assert type(key_open[2]["dir_fd"]) is int
    assert capability.x_api_key_header_value() == SECRET
    buffer = capability._value
    capability.close()
    assert buffer == bytearray()
    assert capability._closed
    with pytest.raises(core.WitnessViolation, match="CREDENTIAL_CLOSED"):
        capability.x_api_key_header_value()
    assert key.read_bytes() == SECRET.encode()


def test_prearm_to_open_symlink_race_fails_nofollow(tmp_path, monkeypatch):
    home = tmp_path / "passwd-home"
    home.mkdir(mode=0o700)
    _set_fixed_home(monkeypatch, home)
    key = _write_key(home)
    assert operational.prearm().status == "READY"
    key.unlink()
    target = home / "target"
    target.write_bytes(SECRET.encode())
    target.chmod(0o600)
    key.symlink_to(target)
    with pytest.raises(core.WitnessViolation, match="CREDENTIAL_CAPABILITY_UNAVAILABLE"):
        operational._PasswdHomeApiKeySource().open()


def test_store_requires_regular_owned_exclusive_0600_single_link(tmp_path):
    path = tmp_path / "store.sqlite3"
    store = operational._OperationalStore(path)
    details = path.stat()
    assert stat.S_IMODE(details.st_mode) == 0o600 and details.st_nlink == 1
    assert store.claim() is None
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA synchronous").fetchone()[0] == 2
    mode_path = tmp_path / "mode.sqlite3"
    mode_path.write_bytes(b"")
    mode_path.chmod(0o644)
    with pytest.raises(core.WitnessViolation, match="DURABLE_FILE_INVALID"):
        operational._OperationalStore(mode_path)
    target = tmp_path / "target.sqlite3"
    target.write_bytes(b"")
    target.chmod(0o600)
    link = tmp_path / "link.sqlite3"
    link.symlink_to(target)
    with pytest.raises(core.WitnessViolation, match="DURABLE_FILE_INVALID"):
        operational._OperationalStore(link)
    hard = tmp_path / "hard.sqlite3"
    os.link(target, hard)
    with pytest.raises(core.WitnessViolation, match="DURABLE_FILE_INVALID"):
        operational._OperationalStore(target)


def test_store_path_replacement_race_is_rejected(tmp_path, monkeypatch):
    path = tmp_path / "store.sqlite3"
    store = operational._OperationalStore(path)
    replacement = tmp_path / "replacement.sqlite3"
    replacement.write_bytes(b"")
    replacement.chmod(0o600)
    displaced = tmp_path / "displaced.sqlite3"
    original_connect = operational.sqlite3.connect

    def swapping_connect(current, *args, **kwargs):
        path.rename(displaced)
        replacement.rename(path)
        return original_connect(current, *args, **kwargs)

    monkeypatch.setattr(operational.sqlite3, "connect", swapping_connect)
    with pytest.raises(core.WitnessViolation, match="DURABLE_FILE_INVALID"):
        store.claim()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "boundary",
    ["after_exclusive_create_before_schema", "after_schema_before_claim", "existing_empty"],
)
async def test_preclaim_store_boundary_is_consumed_without_effects(tmp_path, boundary):
    path = tmp_path / "preclaim.sqlite3"
    if boundary == "after_exclusive_create_before_schema":
        descriptor = os.open(
            path,
            os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        os.fsync(descriptor)
        os.close(descriptor)
    elif boundary == "after_schema_before_claim":
        operational._OperationalStore(path)
    else:
        path.write_bytes(b"")
        path.chmod(0o600)

    source, transport = _Source(), _Transport()
    first = await operational._fixture_run(
        store=operational._OperationalStore(path),
        credential_source=source,
        transport=transport,
    )
    assert (first.status, first.reason) == (
        "UNKNOWN", "INTERRUPTED_BEFORE_CLAIM"
    )
    assert set(first.counters.values()) == {0}
    assert source.calls == 0 and transport.calls == []

    replay_source, replay_transport = _Source(), _Transport()
    replay = await operational._fixture_run(
        store=operational._OperationalStore(path),
        credential_source=replay_source,
        transport=replay_transport,
    )
    assert replay == first
    assert replay_source.calls == 0 and replay_transport.calls == []


@pytest.mark.asyncio
async def test_existing_running_claim_with_zero_counters_never_resumes_effects(tmp_path):
    path = tmp_path / "running.sqlite3"
    store = operational._OperationalStore(path)
    assert store.claim() is None

    source, transport = _Source(), _Transport()
    interrupted = await operational._fixture_run(
        store=operational._OperationalStore(path),
        credential_source=source,
        transport=transport,
    )
    assert (interrupted.status, interrupted.reason) == (
        "UNKNOWN", "INTERRUPTED_RUNNING"
    )
    assert set(interrupted.counters.values()) == {0}
    assert source.calls == 0 and transport.calls == []


class _Content:
    def __init__(self, raw):
        self.raw = raw
        self.calls = []

    async def read(self, maximum):
        self.calls.append(maximum)
        return self.raw


class _Response:
    def __init__(
        self, raw, *, status=200, url=core.ACCOUNT_INFO_URL,
        history=(), content_length=None,
    ):
        self.status = status
        self.url = url
        self.history = history
        self.content_length = len(raw) if content_length is None else content_length
        self.content = _Content(raw)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None


class _Session:
    def __init__(self, response, **kwargs):
        self.response = response
        self.kwargs = kwargs
        self.calls = []
        self.closed = False
        self._retry_connection = True

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response

    async def close(self):
        self.closed = True


def _direct(monkeypatch, response):
    holder = {}

    def session_factory(**kwargs):
        holder["session"] = _Session(response, **kwargs)
        return holder["session"]

    monkeypatch.setattr(operational.aiohttp, "ClientSession", session_factory)
    monkeypatch.setattr(
        operational.aiohttp, "TCPConnector", lambda **kwargs: ("connector", kwargs)
    )
    return operational._DirectTransport(), holder


@pytest.mark.asyncio
async def test_direct_transport_exact_get_tls_timeout_bound_and_close(monkeypatch):
    raw = json.dumps(_body()).encode()
    response = _Response(raw)
    transport, holder = _direct(monkeypatch, response)
    reply = await transport.get(core.AccountInfoRequest(
        "GET", core.ACCOUNT_INFO_URL, {"X-Api-Key": SECRET}
    ))
    session = holder["session"]
    assert session.kwargs["trust_env"] is False
    assert session.kwargs["timeout"].total == 10
    assert session.kwargs["connector"][1]["ssl"] is not None
    assert session._retry_connection is False
    assert session.calls == [(core.ACCOUNT_INFO_URL, {
        "headers": {"X-Api-Key": SECRET}, "allow_redirects": False,
        "proxy": None, "ssl": True,
    })]
    assert response.content.calls == [core.BODY_MAX_BYTES + 1]
    assert reply["body"] == _body() and reply["body_bytes"] == len(raw)
    assert reply["transport"] == _transport_metadata()
    with pytest.raises(core.WitnessViolation, match="TRANSPORT_REUSE_FORBIDDEN"):
        await transport.get(core.AccountInfoRequest(
            "GET", core.ACCOUNT_INFO_URL, {"X-Api-Key": SECRET}
        ))
    await transport.close()
    assert session.closed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response,reason",
    [
        (_Response(b"{}", status=401), "HTTP_RESPONSE_INVALID"),
        (_Response(b"{}", status=302), "HTTP_RESPONSE_INVALID"),
        (_Response(b"{}", url="https://example.invalid"), "HTTP_RESPONSE_INVALID"),
        (_Response(b"{}", history=(object(),)), "HTTP_RESPONSE_INVALID"),
        (_Response(b"{}", content_length=65537), "BODY_TOO_LARGE"),
        (_Response(b"x" * 65537), "BODY_TOO_LARGE"),
        (_Response(b"not-json"), "STRICT_JSON_INVALID"),
        (_Response(b'{"a":1,"a":2}'), "STRICT_JSON_INVALID"),
        (_Response(b'{"a":NaN}'), "STRICT_JSON_INVALID"),
    ],
)
async def test_direct_transport_adverse_boundaries(monkeypatch, response, reason):
    transport, _ = _direct(monkeypatch, response)
    with pytest.raises(core.WitnessViolation, match=reason):
        await transport.get(core.AccountInfoRequest(
            "GET", core.ACCOUNT_INFO_URL, {"X-Api-Key": SECRET}
        ))
    await transport.close()
