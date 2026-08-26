from __future__ import annotations

import base64
import copy
from decimal import Decimal
import inspect
import json
from pathlib import Path
import sqlite3

import pytest

from risex_farmer.testnet_risex_order_lifecycle import (
    AccountState, BBO, DurableIntentStore, Evidence, Lifecycle,
    LifecycleSafetyError, MarketState, OrderRecord, PlaceResponseFailure,
    PlaceResultClass, SyntheticSigner,
)
from risex_farmer.testnet_risex_order_lifecycle_operational import (
    ACCOUNT, AUTHORIZATION, CANCEL_PATH, OFFICIAL_CHAIN_ID,
    OFFICIAL_DOMAIN_NAME, OFFICIAL_DOMAIN_VERSION, PLACE_PATH, REST_ORIGIN,
    ROUTER, RUN_JOURNAL_NAME, SIGNER, OperationalBinding, RuntimeRunJournal,
    SealedWriteTransport, SessionOrderCredential, _credential_from_secret,
    _signed_cancel, _signed_place,
)


NOW = 1_800_000_000
SECRET = bytes.fromhex("11" * 32)


def composite_order_id(wide: int, block: int, log: int) -> str:
    return f"0x{wide:016x}{block:016x}{log:016x}"


OPEN_ORDER = composite_order_id(113, 100, 1)
OTHER_ORDER = composite_order_id(115, 101, 2)
OTHER_ACCOUNT = "0x" + "66" * 20


def order_record(order_id: str, client_order_id: int) -> OrderRecord:
    wide = int(order_id[2:18], 16)
    return OrderRecord(order_id, wide, wide >> 1, client_order_id)


def market() -> MarketState:
    return MarketState(
        host="api.testnet.rise.trade", chain_id=OFFICIAL_CHAIN_ID,
        domain_name=OFFICIAL_DOMAIN_NAME, domain_version=OFFICIAL_DOMAIN_VERSION,
        router=ROUTER, authorization=AUTHORIZATION, market_id=29,
        symbol="ONDO/USDC", active=True, unlocked=True, tick=Decimal("0.00001"),
        step=Decimal("0.1"), minimum=Decimal("25"), observed_at=NOW,
    )


def account(signer: str, *, position: str = "0") -> AccountState:
    value = Decimal(position)
    return AccountState(
        account=ACCOUNT, signer=signer, signer_status="ACTIVE", position=value,
        open_order_ids=(), repeated_open_order_ids=(), repeated_position=value,
        observed_at=NOW,
    )


def bbo() -> BBO:
    return BBO(
        bid=Decimal("0.79900"), ask=Decimal("0.80000"),
        bid_depth=Decimal("25"), ask_depth=Decimal("25"), observed_at=NOW,
    )


class RecordingTransport:
    def __init__(self) -> None:
        self.calls = []
        self.before = None
        self.fail = False

    def place(self, body):
        if self.before:
            self.before()
        self.calls.append((PLACE_PATH, body))
        if self.fail:
            raise TimeoutError("ambiguous")
        return OPEN_ORDER

    def cancel(self, body):
        if self.before:
            self.before()
        self.calls.append((CANCEL_PATH, body))
        if self.fail:
            raise TimeoutError("ambiguous")
        return {"data": {"success": True}}


def fixture(tmp_path):
    from eth_account import Account

    signer = Account.from_key(SECRET).address.lower()
    store = DurableIntentStore(tmp_path / "lifecycle.sqlite")
    lifecycle = Lifecycle(
        store, now=lambda: NOW, router=ROUTER, authorization=AUTHORIZATION,
        expected_account=ACCOUNT, expected_signer=signer,
    )
    transport = RecordingTransport()
    binding = OperationalBinding._fixture(
        RuntimeRunJournal._fixture(tmp_path / "runs.sqlite"), transport,
        lambda: _credential_from_secret(SECRET, signer), expected_signer=signer,
        now=lambda: NOW,
    )
    return lifecycle, binding, transport, signer


def test_production_surface_is_fixed_and_isolated_from_normal_startup():
    assert tuple(inspect.signature(OperationalBinding).parameters) == ()
    assert tuple(inspect.signature(SealedWriteTransport).parameters) == ()
    assert REST_ORIGIN == "https://api.testnet.rise.trade"
    assert (PLACE_PATH, CANCEL_PATH) == ("/v1/orders/place", "/v1/orders/cancel")
    assert OperationalBinding.ACCOUNT == ACCOUNT and OperationalBinding.SIGNER == SIGNER
    assert OperationalBinding.ROUTER == ROUTER
    assert OperationalBinding.AUTHORIZATION == AUTHORIZATION
    assert OperationalBinding.CHAIN_ID == 11_155_931
    assert OperationalBinding.DOMAIN == ("RISEx", "1")
    assert SealedWriteTransport.TRUST_ENV is False
    assert SealedWriteTransport.ALLOW_REDIRECTS is False
    assert not any(
        "testnet_risex_order_lifecycle_operational" in
        (Path(__file__).parents[1] / path).read_text()
        for path in ("src/risex_farmer/cli.py", "src/risex_farmer/__init__.py")
    )


def test_runtime_run_id_is_fresh_durable_protected_and_separate_from_write_identity(tmp_path):
    path = tmp_path / RUN_JOURNAL_NAME
    journal = RuntimeRunJournal._fixture(path)
    first = journal.begin(NOW)
    second = journal.begin(NOW + 1)
    assert first != second
    assert path.stat().st_mode & 0o777 == 0o600
    rows = sqlite3.connect(path).execute(
        "SELECT run_id, created_at, state FROM runs ORDER BY created_at"
    ).fetchall()
    assert rows == [(first, NOW, "STARTED"), (second, NOW + 1, "STARTED")]
    raw = path.read_bytes().lower()
    assert b"client_order_id" not in raw and b"nonce" not in raw


def test_runtime_journal_rejects_unsafe_or_spoofed_storage(tmp_path):
    unsafe = tmp_path / "unsafe.sqlite"
    unsafe.write_bytes(b""); unsafe.chmod(0o644)
    with pytest.raises(LifecycleSafetyError):
        RuntimeRunJournal._fixture(unsafe).begin(NOW)
    spoof = tmp_path / "spoof.sqlite"
    connection = sqlite3.connect(spoof)
    connection.execute("CREATE TABLE runs(run_id TEXT, created_at INTEGER, state TEXT)")
    connection.commit(); connection.close(); spoof.chmod(0o600)
    with pytest.raises(LifecycleSafetyError):
        RuntimeRunJournal._fixture(spoof).begin(NOW)


def test_synthetic_real_signature_is_compact_recoverable_and_zeroized():
    from eth_account import Account
    from eth_account.messages import encode_typed_data

    signer = Account.from_key(SECRET).address.lower()
    credential = _credential_from_secret(SECRET, signer)
    typed = {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            "VerifyWitness": [
                {"name": "account", "type": "address"},
                {"name": "target", "type": "address"},
                {"name": "hash", "type": "bytes32"},
                {"name": "nonceAnchor", "type": "uint48"},
                {"name": "nonceBitmap", "type": "uint8"},
                {"name": "deadline", "type": "uint32"},
            ],
        },
        "primaryType": "VerifyWitness",
        "domain": {
            "name": OFFICIAL_DOMAIN_NAME, "version": OFFICIAL_DOMAIN_VERSION,
            "chainId": OFFICIAL_CHAIN_ID, "verifyingContract": AUTHORIZATION,
        },
        "message": {
            "account": ACCOUNT, "target": ROUTER, "hash": "0x" + "ab" * 32,
            "nonceAnchor": 7, "nonceBitmap": 3, "deadline": NOW + 30,
        },
    }
    compact = base64.b64decode(credential.sign_permit(typed), validate=True)
    assert len(compact) == 64
    s = bytearray(compact[32:]); v = 28 if s[0] & 0x80 else 27; s[0] &= 0x7f
    signature = compact[:32] + bytes(s) + bytes((v,))
    recovered = Account.recover_message(
        encode_typed_data(full_message=typed), signature=signature,
    ).lower()
    assert recovered == signer
    altered = replace_domain(typed)
    with pytest.raises(LifecycleSafetyError):
        credential.sign_permit(altered)
    with pytest.raises(LifecycleSafetyError, match="permit-only"):
        credential.sign_register_v2(typed)
    credential.close()
    assert credential.closed and len(credential._secret) == 0


def replace_domain(typed):
    value = json.loads(json.dumps(typed))
    value["domain"]["chainId"] += 1
    return value


def test_place_is_signed_only_after_durable_intent_and_dispatch_claim(tmp_path):
    lifecycle, binding, transport, signer = fixture(tmp_path)
    preflight = lifecycle.preflight(market(), account(signer), bbo())
    intent = lifecycle.prepare_open(preflight, 101, 7, 3, NOW + 30)
    transport.before = lambda: (
        lifecycle.store.get(intent.intent_id).state == "DISPATCHING"
        and lifecycle.store.get(intent.intent_id).dispatch_count == 1
    ) or pytest.fail("write was not durable before POST")
    binding.dispatch_place(lifecycle, intent, market())
    persisted = lifecycle.store.get(intent.intent_id)
    assert persisted.state == "DISPATCHED" and persisted.order_id == OPEN_ORDER
    path, body = transport.calls[0]
    assert path == PLACE_PATH
    assert body["price_ticks"] == intent.price_ticks == 80240
    assert set(body) == {
        "market_id", "size_steps", "price_ticks", "side", "post_only",
        "reduce_only", "stp_mode", "order_type", "time_in_force",
        "client_order_id", "permit",
    }
    assert body["client_order_id"] == intent.client_order_id
    assert set(body["permit"]) == {
        "account", "signer", "nonce_anchor", "nonce_bitmap_index",
        "deadline", "signature",
    }
    assert body["permit"]["signer"] == signer
    assert len(base64.b64decode(body["permit"]["signature"], validate=True)) == 64
    assert binding.run_id and binding.run_id != intent.intent_id
    lifecycle.store.close()


@pytest.mark.parametrize(
    "mutation",
    (
        "body_market", "body_client", "order_data", "abi", "action_hash",
        "permit_hash", "permit_account", "permit_target", "nonce", "bitmap",
        "deadline",
    ),
)
def test_place_rejects_any_action_permit_or_body_substitution_before_transport(
    tmp_path, mutation,
):
    lifecycle, _binding, _transport, signer = fixture(tmp_path)
    intent = lifecycle.prepare_open(
        lifecycle.preflight(market(), account(signer), bbo()), 101, 7, 3, NOW + 30,
    )
    request = copy.deepcopy(lifecycle.unsigned_request(intent.intent_id, market=market()))
    if mutation == "body_market": request["body"]["market_id"] = 2
    elif mutation == "body_client": request["body"]["client_order_id"] = 102
    elif mutation == "order_data": request["order_data"] += 1
    elif mutation == "abi": request["abi_encoded"] = b"\x00" * len(request["abi_encoded"])
    elif mutation == "action_hash": request["action_hash"] = b"\x00" * 32
    elif mutation == "permit_hash": request["permit"]["message"]["hash"] = "0x" + "00" * 32
    elif mutation == "permit_account": request["permit"]["message"]["account"] = OTHER_ACCOUNT
    elif mutation == "permit_target": request["permit"]["message"]["target"] = OTHER_ACCOUNT
    elif mutation == "nonce": request["body"]["nonce_anchor"] = "8"
    elif mutation == "bitmap": request["body"]["nonce_bitmap_index"] = 4
    else: request["body"]["deadline"] += 1
    credential = _credential_from_secret(SECRET, signer)
    try:
        with pytest.raises(LifecycleSafetyError):
            _signed_place(request, credential, signer)
    finally:
        credential.close(); lifecycle.store.close()


def test_ambiguous_post_is_never_replayed_and_credential_is_zeroized(tmp_path):
    lifecycle, binding, transport, signer = fixture(tmp_path)
    intent = lifecycle.prepare_open(
        lifecycle.preflight(market(), account(signer), bbo()), 101, 7, 3, NOW + 30,
    )
    issued = []
    binding._credential_loader = lambda: issued.append(
        _credential_from_secret(SECRET, signer)
    ) or issued[-1]
    transport.fail = True
    binding.dispatch_place(lifecycle, intent, market())
    assert lifecycle.store.get(intent.intent_id).state == "AMBIGUOUS"
    assert lifecycle.store.place_result(intent.intent_id) == (
        PlaceResultClass.LOCAL_FAILURE, "UNCLASSIFIED_LOCAL_FAILURE",
    )
    with pytest.raises(LifecycleSafetyError):
        binding.dispatch_place(lifecycle, intent, market())
    assert len(transport.calls) == 1 and len(issued) == 1
    assert issued[0].closed and len(issued[0]._secret) == 0
    lifecycle.store.close()


def test_cancel_signs_resting_identity_but_posts_exact_composite_identity(tmp_path):
    lifecycle, binding, transport, signer = fixture(tmp_path)
    opening = lifecycle.prepare_open(
        lifecycle.preflight(market(), account(signer), bbo()), 101, 7, 3, NOW + 30,
    )
    binding.dispatch_place(lifecycle, opening, market())
    record = order_record(OPEN_ORDER, 101)
    lifecycle.reconcile(
        opening.intent_id,
        Evidence(
            account=ACCOUNT, signer=signer, signer_status="ACTIVE", terminal=False,
            filled_size=Decimal("0"), position=Decimal("0"), observed_at=NOW,
            position_market_id=29, by_id_order=record, open_orders=(record,),
            history_orders=(record,),
        ),
    )
    binding.cancel_known(
        lifecycle, OPEN_ORDER, market=market(), nonce_anchor=8,
        nonce_bitmap=4, expires_at=NOW + 30,
    )
    path, body = transport.calls[-1]
    assert path == CANCEL_PATH and body["order_id"] == OPEN_ORDER
    assert "resting_order_id" not in body
    assert lifecycle.store.cancel_count(OPEN_ORDER) == 1
    assert lifecycle.store.cancel_state(OPEN_ORDER) == "PENDING_RECONCILIATION"
    assert lifecycle.reconcile_cancel(OPEN_ORDER, account(signer)) is True
    lifecycle.store.close()


@pytest.mark.parametrize(
    "mutation",
    (
        "request_market", "resting", "abi", "action_hash", "body_market",
        "body_order", "permit_hash", "permit_account", "permit_target", "nonce",
        "bitmap", "deadline",
    ),
)
def test_cancel_rejects_any_action_permit_or_body_substitution_before_transport(
    tmp_path, mutation,
):
    lifecycle, _binding, _transport, signer = fixture(tmp_path)
    opening = lifecycle.prepare_open(
        lifecycle.preflight(market(), account(signer), bbo()), 101, 7, 3, NOW + 30,
    )
    lifecycle.dispatch(opening, SyntheticSigner(signer), lambda _: OPEN_ORDER)
    record = order_record(OPEN_ORDER, 101)
    lifecycle.reconcile(
        opening.intent_id,
        Evidence(
            account=ACCOUNT, signer=signer, signer_status="ACTIVE", terminal=False,
            filled_size=Decimal("0"), position=Decimal("0"), observed_at=NOW,
            position_market_id=29, by_id_order=record, open_orders=(record,),
            history_orders=(record,),
        ),
    )
    captured = []
    lifecycle.cancel_known(
        OPEN_ORDER, market=market(), nonce_anchor=8, nonce_bitmap=4,
        expires_at=NOW + 30, synthetic_signer=SyntheticSigner(signer),
        execute=lambda request: captured.append(copy.deepcopy(request)),
    )
    request = captured[0]
    if mutation == "request_market": request["market_id"] = 2
    elif mutation == "resting": request["resting_order_id"] += 1
    elif mutation == "abi": request["abi_encoded"] = b"\x00" * len(request["abi_encoded"])
    elif mutation == "action_hash": request["action_hash"] = b"\x00" * 32
    elif mutation == "body_market": request["body"]["market_id"] = 2
    elif mutation == "body_order": request["body"]["order_id"] = OTHER_ORDER
    elif mutation == "permit_hash": request["permit"]["message"]["hash"] = "0x" + "00" * 32
    elif mutation == "permit_account": request["permit"]["message"]["account"] = OTHER_ACCOUNT
    elif mutation == "permit_target": request["permit"]["message"]["target"] = OTHER_ACCOUNT
    elif mutation == "nonce": request["body"]["permit"]["nonce_anchor"] = "9"
    elif mutation == "bitmap": request["body"]["permit"]["nonce_bitmap_index"] = 5
    else: request["body"]["permit"]["deadline"] += 1
    credential = _credential_from_secret(SECRET, signer)
    try:
        with pytest.raises(LifecycleSafetyError):
            _signed_cancel(request, credential, signer)
    finally:
        credential.close(); lifecycle.store.close()


class FakeResponse:
    def __init__(self, status=200, body=b'{"data":{"order_id":"' +
                 OPEN_ORDER.encode() + b'"}}', length=None):
        self.status = status; self._body = body; self._length = length

    def getheader(self, name):
        return self._length if name == "Content-Length" else None

    def read(self, limit):
        return self._body[:limit]


class FakeConnection:
    def __init__(self, response):
        self.response = response; self.calls = []; self.closed = False

    def request(self, *args, **kwargs):
        self.calls.append((args, kwargs))

    def getresponse(self):
        return self.response

    def close(self):
        self.closed = True


def test_transport_owns_exact_post_surface_has_no_retry_and_bounds_response():
    transport = SealedWriteTransport()
    connection = FakeConnection(FakeResponse())
    transport._connection_factory = lambda: connection
    assert transport.place({"fixed": "body"}) == OPEN_ORDER
    assert connection.calls == [(('POST', PLACE_PATH), {
        "body": b'{"fixed":"body"}',
        "headers": {"Content-Type": "application/json", "Accept": "application/json"},
    })]
    assert connection.closed
    with pytest.raises(LifecycleSafetyError, match="surface"):
        transport._post("https://other.invalid/", {})
    failing = FakeConnection(FakeResponse(status=307))
    transport._connection_factory = lambda: failing
    with pytest.raises(PlaceResponseFailure) as redirect:
        transport.place({})
    assert (redirect.value.result_class, redirect.value.failure_code) == (
        PlaceResultClass.RESPONSE_AMBIGUITY, "HTTP_307",
    )
    assert len(failing.calls) == 1
    oversized = FakeConnection(FakeResponse(length=str(SealedWriteTransport.MAX_BYTES + 1)))
    transport._connection_factory = lambda: oversized
    with pytest.raises(PlaceResponseFailure) as too_large:
        transport.place({})
    assert (too_large.value.result_class, too_large.value.failure_code) == (
        PlaceResultClass.RESPONSE_AMBIGUITY, "OVERSIZED_RESPONSE",
    )
    assert len(oversized.calls) == 1


@pytest.mark.parametrize(
    ("status", "expected_class"),
    (
        (400, PlaceResultClass.TERMINAL_VENUE_REJECTION),
        (401, PlaceResultClass.TERMINAL_VENUE_REJECTION),
        (408, PlaceResultClass.RESPONSE_AMBIGUITY),
        (500, PlaceResultClass.RESPONSE_AMBIGUITY),
    ),
)
def test_place_http_result_class_is_sanitized_and_never_retains_body(
    status, expected_class,
):
    transport = SealedWriteTransport()
    response = FakeResponse(status=status, body=b'{"secret":"must-not-survive"}')
    connection = FakeConnection(response)
    transport._connection_factory = lambda: connection
    with pytest.raises(PlaceResponseFailure) as failure:
        transport.place({})
    assert failure.value.result_class is expected_class
    assert failure.value.failure_code == f"HTTP_{status}"
    assert "secret" not in str(failure.value) and connection.closed


def test_place_transport_and_malformed_success_are_distinct_ambiguities():
    class FailingConnection(FakeConnection):
        def request(self, *args, **kwargs):
            raise ConnectionResetError("sensitive transport detail")

    transport = SealedWriteTransport()
    transport._connection_factory = lambda: FailingConnection(FakeResponse())
    with pytest.raises(PlaceResponseFailure) as transport_failure:
        transport.place({})
    assert (
        transport_failure.value.result_class,
        transport_failure.value.failure_code,
    ) == (PlaceResultClass.TRANSPORT_AMBIGUITY, "TRANSPORT_FAILURE")
    assert "sensitive" not in str(transport_failure.value)

    malformed = FakeConnection(FakeResponse(body=b'{"data":{}}'))
    transport._connection_factory = lambda: malformed
    with pytest.raises(PlaceResponseFailure) as response_failure:
        transport.place({})
    assert (
        response_failure.value.result_class,
        response_failure.value.failure_code,
    ) == (PlaceResultClass.RESPONSE_AMBIGUITY, "MISSING_ORDER_ID")
