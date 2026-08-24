import asyncio
import json
import sqlite3
from pathlib import Path

import pytest

from risex_farmer import extended_account_shape_witness as witness


FIXTURE = Path(__file__).parent / "fixtures/extended_account_shape_witness/official_contract.json"
SECRET = "synthetic-api-secret-never-persist"
ACCOUNT_ID = 7001


def _contract():
    return json.loads(FIXTURE.read_text())


def _body(**extra):
    account = {
        "id": ACCOUNT_ID,
        "description": "synthetic account label",
        "accountIndex": 3,
        "status": "ACTIVE",
        "l2Key": "0x0123456789abcdef",
        "l2Vault": 9,
        "bridgeStarknetAddress": None,
    }
    account.update(extra)
    return {"status": "OK", "data": account, "error": None, "pagination": None}


def _metadata():
    return {
        "actual_url": witness.ACCOUNT_INFO_URL,
        "method": "GET",
        "direct_tls": True,
        "trust_env": False,
        "proxy": None,
        "redirects": 0,
        "retries": 0,
    }


class _Capability:
    def __init__(self):
        self.closed = False

    def x_api_key_header_value(self):
        return SECRET

    def close(self):
        self.closed = True


class _Source:
    def __init__(self):
        self.calls = 0
        self.capability = _Capability()

    def open(self):
        self.calls += 1
        return self.capability


class _Transport:
    def __init__(self, body=None, *, body_bytes=512, metadata=None):
        self.body = _body() if body is None else body
        self.body_bytes = body_bytes
        self.metadata = _metadata() if metadata is None else metadata
        self.calls = []

    async def get(self, request):
        self.calls.append(request)
        return {
            "body": self.body,
            "body_bytes": self.body_bytes,
            "transport": self.metadata,
        }


async def _run(tmp_path, *, source=None, transport=None, hook=None):
    source = source or _Source()
    transport = transport or _Transport()
    result = await witness._run_fixture_account_shape_witness(
        store=witness._WitnessStore(tmp_path / "witness.sqlite3"),
        credential_source=source,
        transport=transport,
        _effect_hook=hook,
    )
    return result, source, transport


def test_official_contract_is_exact_and_fixture_only():
    contract = _contract()
    assert contract == {
        "schema_version": witness.SCHEMA_VERSION,
        "method": "GET",
        "url": witness.ACCOUNT_INFO_URL,
        "body_max_bytes": witness.BODY_MAX_BYTES,
        "shape_max_depth": witness.SHAPE_MAX_DEPTH,
        "shape_max_keys": witness.SHAPE_MAX_KEYS,
        "descriptor_max_bytes": witness.DESCRIPTOR_MAX_BYTES,
        "allowed_field_names": sorted(witness.ALLOWED_FIELD_NAMES),
        "allowed_type_classes": sorted(witness.ALLOWED_TYPE_CLASSES),
        "effects": list(witness.EFFECTS),
    }
    assert not hasattr(witness, "run")
    assert not hasattr(witness, "main")


def test_descriptor_contains_only_allowlisted_names_closed_types_and_unknown_count():
    body = _body(**{"futureSecretField": "value-must-not-appear"})
    descriptor = witness._describe_body(body)
    encoded = json.dumps(descriptor, sort_keys=True, separators=(",", ":"))
    assert "futureSecretField" not in encoded
    assert "value-must-not-appear" not in encoded
    assert "synthetic account label" not in encoded
    assert "0x0123456789abcdef" not in encoded
    assert str(ACCOUNT_ID) not in encoded
    assert "length" not in encoded and "hash" not in encoded and "digest" not in encoded
    assert descriptor["fields"]["data"]["unknown_fields"] == 1

    names = set()
    types = set()

    def walk(item):
        if isinstance(item, dict):
            names.update(item.get("fields", {}).keys())
            if "type" in item:
                types.add(item["type"])
            for value in item.values():
                walk(value)
        elif isinstance(item, list):
            for value in item:
                walk(value)

    walk(descriptor)
    assert names <= witness.ALLOWED_FIELD_NAMES
    assert types <= witness.ALLOWED_TYPE_CLASSES


def test_unknown_names_values_and_array_lengths_do_not_change_descriptor():
    left = _body(**{"unknownAlpha": ["one"]})
    right = _body(**{"unknownBeta": {"nested": 1}})
    assert witness._describe_body(left) == witness._describe_body(right)
    assert witness._describe_body([{"id": 1}]) == witness._describe_body(
        [{"id": 2}, {"id": 3}, {"id": 4}]
    )


@pytest.mark.parametrize(
    "body,reason",
    [
        ({"a": {"b": {"c": {"d": None}}}}, "SHAPE_DEPTH_EXCEEDED"),
        ({str(i): i for i in range(17)}, "SHAPE_KEYS_EXCEEDED"),
    ],
)
def test_shape_limits_fail_closed_without_a_partial_descriptor(body, reason):
    with pytest.raises(witness.WitnessViolation, match=reason):
        witness._describe_body(body)


@pytest.mark.asyncio
async def test_success_is_one_get_with_six_durable_counters_and_redacted_evidence(tmp_path):
    result, source, transport = await _run(tmp_path)
    assert (result.status, result.reason, result.phase) == (
        "CAPTURED", "ACCOUNT_SHAPE_CAPTURED", "TERMINAL"
    )
    assert result.schema_version == 1
    assert set(result.counters) == {
        "loader_attempts", "loader_completions",
        "account_info_attempts", "account_info_completions",
        "terminal_attempts", "terminal_completions",
    }
    assert set(result.counters.values()) == {1}
    assert source.calls == 1 and source.capability.closed
    assert len(transport.calls) == 1
    request = transport.calls[0]
    assert (request.method, request.url, request.headers) == (
        "GET", witness.ACCOUNT_INFO_URL, {"X-Api-Key": SECRET}
    )
    durable = (tmp_path / "witness.sqlite3").read_bytes()
    for forbidden in (SECRET.encode(), b"synthetic account label", b"7001", b"0x0123456789abcdef"):
        assert forbidden not in durable


@pytest.mark.asyncio
async def test_terminal_reentry_has_zero_loader_or_network_effects(tmp_path):
    first, _, _ = await _run(tmp_path)
    source, transport = _Source(), _Transport()
    second = await witness._run_fixture_account_shape_witness(
        store=witness._WitnessStore(tmp_path / "witness.sqlite3"),
        credential_source=source,
        transport=transport,
    )
    assert second == first
    assert source.calls == 0 and transport.calls == []


class _ProcessDeath(BaseException):
    pass


@pytest.mark.asyncio
@pytest.mark.parametrize("effect", ["loader", "account_info", "terminal"])
@pytest.mark.parametrize("point", ["after_attempt", "before_completion"])
async def test_interruption_never_replays_any_effect(tmp_path, effect, point):
    def hook(current, current_point):
        if (current, current_point) == (effect, point):
            raise _ProcessDeath

    with pytest.raises(_ProcessDeath):
        await _run(tmp_path, hook=hook)
    source, transport = _Source(), _Transport()
    recovered = await witness._run_fixture_account_shape_witness(
        store=witness._WitnessStore(tmp_path / "witness.sqlite3"),
        credential_source=source,
        transport=transport,
    )
    if effect == "terminal" and point == "before_completion":
        assert recovered.status == "UNKNOWN"
    else:
        assert recovered.status == "UNKNOWN"
    assert source.calls == 0 and transport.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("effect", ["loader", "account_info"])
async def test_interruption_after_external_effect_is_unknown_and_never_replayed(
    tmp_path, effect
):
    def hook(current, current_point):
        if (current, current_point) == (effect, "after_effect"):
            raise _ProcessDeath

    with pytest.raises(_ProcessDeath):
        await _run(tmp_path, hook=hook)
    source, transport = _Source(), _Transport()
    recovered = await witness._run_fixture_account_shape_witness(
        store=witness._WitnessStore(tmp_path / "witness.sqlite3"),
        credential_source=source,
        transport=transport,
    )
    assert recovered.status == "UNKNOWN"
    assert source.calls == 0 and transport.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "defect,reason",
    [
        ({"body_bytes": 65537}, "BODY_TOO_LARGE"),
        ({"body_bytes": True}, "RESPONSE_CONTRACT_INVALID"),
        ({"metadata": {**_metadata(), "actual_url": "https://example.invalid"}}, "TRANSPORT_CONTRACT_INVALID"),
        ({"metadata": {**_metadata(), "redirects": 1}}, "TRANSPORT_CONTRACT_INVALID"),
        ({"metadata": {**_metadata(), "retries": 1}}, "TRANSPORT_CONTRACT_INVALID"),
        ({"metadata": {**_metadata(), "proxy": "http://proxy.invalid"}}, "TRANSPORT_CONTRACT_INVALID"),
        ({"metadata": {**_metadata(), "direct_tls": False}}, "TRANSPORT_CONTRACT_INVALID"),
        ({"metadata": {**_metadata(), "trust_env": True}}, "TRANSPORT_CONTRACT_INVALID"),
    ],
)
async def test_body_and_direct_transport_boundaries_fail_closed(tmp_path, defect, reason):
    transport = _Transport(
        body_bytes=defect.get("body_bytes", 512),
        metadata=defect.get("metadata", _metadata()),
    )
    result, _, transport = await _run(tmp_path, transport=transport)
    assert (result.status, result.reason) == ("BLOCKED", reason)
    assert len(transport.calls) == 1
    assert result.counters["account_info_attempts"] == 1
    assert result.counters["account_info_completions"] == 0


@pytest.mark.asyncio
async def test_descriptor_byte_bound_fails_closed(monkeypatch, tmp_path):
    monkeypatch.setattr(witness, "DESCRIPTOR_MAX_BYTES", 16)
    result, _, _ = await _run(tmp_path)
    assert (result.status, result.reason) == ("BLOCKED", "DESCRIPTOR_TOO_LARGE")
    assert result.descriptor is None


@pytest.mark.asyncio
async def test_cancellation_is_durably_terminal_and_not_rearmed(tmp_path):
    def hook(effect, point):
        if (effect, point) == ("account_info", "after_attempt"):
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await _run(tmp_path, hook=hook)
    source, transport = _Source(), _Transport()
    result = await witness._run_fixture_account_shape_witness(
        store=witness._WitnessStore(tmp_path / "witness.sqlite3"),
        credential_source=source,
        transport=transport,
    )
    assert (result.status, result.reason) == ("BLOCKED", "CANCELLED")
    assert source.calls == 0 and transport.calls == []


def test_store_is_schema_one_and_only_contains_the_six_counters(tmp_path):
    store = witness._WitnessStore(tmp_path / "witness.sqlite3")
    assert store.claim() is None
    with sqlite3.connect(store.path) as connection:
        row = connection.execute(
            "SELECT schema_version,counters FROM extended_account_shape_witness"
        ).fetchone()
    assert row[0] == 1
    assert set(json.loads(row[1])) == set(witness._empty_counters())
