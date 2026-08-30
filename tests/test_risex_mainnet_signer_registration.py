from __future__ import annotations

import asyncio
import ast
import inspect
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from eth_account import Account
from eth_account.messages import encode_typed_data

from risex_farmer import risex_mainnet_onboarding as onboarding
from risex_farmer import risex_mainnet_signer_registration as registration


ROOT = Path(__file__).parents[1]
NOW = 1_800_000_000
MAIN_KEY = bytes.fromhex("11" * 32)
SESSION_KEY = bytes.fromhex("22" * 32)
MAIN_ADDRESS = Account.from_key(MAIN_KEY).address.lower()
SESSION_ADDRESS = Account.from_key(SESSION_KEY).address.lower()


def _envelope(data: Any) -> dict[str, Any]:
    return {"data": data, "request_id": "fixture-request"}


def _domain() -> dict[str, Any]:
    return _envelope({
        "name": registration.DOMAIN_NAME,
        "version": registration.DOMAIN_VERSION,
        "chain_id": str(registration.MAINNET_CHAIN_ID),
        "verifying_contract": registration.MAINNET_AUTH_CONTRACT,
    })


def _nonce(anchor: int = 7, index: int = 3, bitmap: int = 0) -> dict[str, Any]:
    return _envelope({
        "nonce_anchor": str(anchor),
        "current_bitmap_index": index,
        "bitmap": hex(bitmap),
    })


def _active_signer(expiration: int, signer: str = SESSION_ADDRESS) -> dict[str, Any]:
    return {"signer": signer, "expiration": str(expiration), "status": "Active"}


class FakeTransport:
    def __init__(self, scenario: "Scenario") -> None:
        self.scenario = scenario
        self.closed = False

    async def get(
        self,
        path: str,
        query: tuple[tuple[str, str], ...] = (),
    ) -> registration.HttpObservation:
        self.scenario.calls.append(("GET", path, query))
        failure = (
            self.scenario.get_failures.pop(0)
            if self.scenario.get_failures
            else None
        )
        if failure is not None:
            raise failure
        if path == registration.DOMAIN_PATH:
            body = self.scenario.domain
        elif path == registration.NONCE_STATE_PATH + self.scenario.wallet:
            body = (
                self.scenario.nonce_sequence.pop(0)
                if self.scenario.nonce_sequence
                else self.scenario.nonce
            )
        elif path == registration.SIGNERS_PATH:
            body = _envelope({"signers": list(self.scenario.signers)})
        else:
            raise AssertionError(f"unexpected read path: {path}")
        return registration.HttpObservation(
            200,
            str(registration.MAINNET_REST_ORIGIN.with_path(path)),
            body,
        )

    async def register(self, request: dict[str, Any]) -> registration.HttpObservation:
        self.scenario.post_count += 1
        self.scenario.posts.append(dict(request))
        self.scenario.events.append("post")
        if self.scenario.post_effect is not None:
            self.scenario.post_effect()
        if self.scenario.post_error is not None:
            raise self.scenario.post_error
        return registration.HttpObservation(
            self.scenario.post_status,
            str(registration.MAINNET_REST_ORIGIN.with_path(registration.REGISTER_PATH)),
            self.scenario.post_body,
        )

    async def close(self) -> None:
        self.closed = True


class Scenario:
    def __init__(self, wallet: str, signer: str, expiration: int) -> None:
        self.wallet = wallet
        self.signer = signer
        self.expiration = expiration
        self.domain = _domain()
        self.nonce = _nonce()
        self.nonce_sequence: list[dict[str, Any]] = []
        self.signers: list[dict[str, Any]] = []
        self.post_body: Any = _envelope({"success": True, "transaction_hash": ""})
        self.post_status = 200
        self.post_error: BaseException | None = None
        self.post_effect: Any = None
        self.get_failures: list[BaseException] = []
        self.calls: list[tuple[str, str, tuple[tuple[str, str], ...]]] = []
        self.posts: list[dict[str, Any]] = []
        self.post_count = 0
        self.events: list[str] = []

    def active(self) -> None:
        self.signers = [_active_signer(self.expiration, self.signer)]

    def consumed(self, observed: registration.NonceObservation | None = None) -> None:
        if observed is None:
            observed = registration._parse_nonce(self.nonce)
        current = registration._expected_post_nonce(observed)
        self.nonce = _nonce(current.anchor, current.current_bitmap_index, current.bitmap)

    def transport(self) -> FakeTransport:
        return FakeTransport(self)


@pytest.fixture
def context(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    directory = tmp_path / "home" / ".config" / "risex-farmer"
    monkeypatch.setattr(onboarding, "PROTECTED_SECRET_DIRECTORY", directory)
    monkeypatch.setattr(onboarding, "_now_unix", lambda: NOW)
    monkeypatch.setattr(onboarding, "_new_session_secret", lambda: bytearray(SESSION_KEY))
    result = onboarding.provision_mainnet_session_signer(
        lambda _prompt: "0x" + MAIN_KEY.hex()
    )
    assert result.ready
    expiration = result.expiration
    assert expiration is not None

    monkeypatch.setattr(registration, "AUTHORIZED_WALLET", MAIN_ADDRESS)
    monkeypatch.setattr(registration, "AUTHORIZED_SESSION_SIGNER", SESSION_ADDRESS)
    monkeypatch.setattr(registration, "AUTHORIZED_EXPIRATION", expiration)
    monkeypatch.setattr(
        registration,
        "AUTHORIZED_BINDING",
        registration.AuthorizationBinding(
            environment="MAINNET",
            wallet_address=MAIN_ADDRESS,
            session_signer_address=SESSION_ADDRESS,
            chain_id=registration.MAINNET_CHAIN_ID,
            verifying_contract=registration.MAINNET_AUTH_CONTRACT,
            expiration=expiration,
            message=registration.REGISTER_MESSAGE,
        ),
    )
    return SimpleNamespace(
        directory=directory,
        expiration=expiration,
        wallet=MAIN_ADDRESS,
        signer=SESSION_ADDRESS,
        main_key=MAIN_KEY,
        session_key=SESSION_KEY,
    )


def _dependencies(
    scenario: Scenario,
    *,
    input_fn: Any = lambda _prompt: "0x" + MAIN_KEY.hex(),
):
    return registration._fixture_dependencies(
        input_fn=input_fn,
        transport_factory=scenario.transport,
        clock=lambda: NOW,
    )


async def _run(context, scenario: Scenario, *, input_fn: Any = None):
    return await registration._run_fixture(
        _dependencies(
            scenario,
            input_fn=input_fn if input_fn is not None else lambda _prompt: "0x" + MAIN_KEY.hex(),
        )
    )


def test_binding_and_production_surface_are_fixed() -> None:
    assert registration.AUTHORIZED_WALLET == "0xb13c2bbe1f07f58efbbbdf86d948b49da2e0a56f"
    assert registration.AUTHORIZED_SESSION_SIGNER == "0x9c904d9145a45fbe2d5645cd9226def5efc9c5de"
    assert registration.AUTHORIZED_EXPIRATION == 1790689990
    assert registration.MAINNET_CHAIN_ID == 4153
    assert registration.MAINNET_AUTH_CONTRACT == "0x0d919daa3f12ae715744eb648c00066c5dbd66f0"
    assert str(registration.MAINNET_REST_ORIGIN) == "https://api.rise.trade"
    assert tuple(inspect.signature(registration.run).parameters) == ()
    assert tuple(inspect.signature(registration.main).parameters) == ()

    source = (ROOT / "src" / "risex_farmer" / "risex_mainnet_signer_registration.py").read_text()
    tree = ast.parse(source)
    assert "os.environ" not in source
    assert "os.getenv" not in source
    for forbidden in (
        "/v1/orders",
        "/v1/positions",
        "/v1/transfers",
        "/v1/withdrawals",
        "/v1/cancel",
        "strategy",
        "sqlite3",
        "websocket",
    ):
        assert forbidden not in source
    methods = {
        node.args[0].value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "_request"
        and node.args
        and isinstance(node.args[0], ast.Constant)
    }
    assert methods == {"POST", "GET"}


def test_cli_arguments_are_rejected_without_running_or_leaking(monkeypatch, capsys) -> None:
    monkeypatch.setattr(registration.sys, "argv", ["runner", "--wallet", "secret-value"])
    assert registration.main() == 1
    output = capsys.readouterr().out
    assert "secret-value" not in output
    assert "POST" not in output
    assert json.loads(output)["reason"] == "ARGUMENTS_REJECTED"


@pytest.mark.asyncio
async def test_exact_success_signatures_runtime_and_durable_order(
    context, monkeypatch: pytest.MonkeyPatch
) -> None:
    scenario = Scenario(context.wallet, context.signer, context.expiration)
    observed = registration._parse_nonce(scenario.nonce)
    scenario.post_effect = lambda: (scenario.active(), scenario.consumed(observed))

    events = scenario.events
    real_prepare = onboarding.prepare_registration_intent
    real_claim = onboarding.claim_registration_intent
    real_sign = registration._sign_typed_data

    def prepare(*args: Any, **kwargs: Any):
        result = real_prepare(*args, **kwargs)
        events.append("intent_durable")
        return result

    def claim(*args: Any, **kwargs: Any):
        result = real_claim(*args, **kwargs)
        events.append("claim_durable")
        return result

    def sign(*args: Any, **kwargs: Any):
        events.append("sign")
        return real_sign(*args, **kwargs)

    monkeypatch.setattr(onboarding, "prepare_registration_intent", prepare)
    monkeypatch.setattr(onboarding, "claim_registration_intent", claim)
    monkeypatch.setattr(registration, "_sign_typed_data", sign)

    result = await _run(context, scenario)

    assert result.state is registration.RegistrationState.REGISTERED
    assert result.reason == "COMPLETE"
    assert result.dispatch_count == 1
    assert result.runtime_id and result.intent_id
    assert result.runtime_id != result.intent_id
    assert events.index("intent_durable") < events.index("sign")
    assert events.index("sign") < events.index("claim_durable") < events.index("post")
    assert len(scenario.posts) == 1

    request = scenario.posts[0]
    assert set(request) == {
        "account", "signer", "message", "nonce_anchor", "expiration",
        "account_signature", "signer_signature", "nonce_bitmap_index",
    }
    assert request["account"] == context.wallet
    assert request["signer"] == context.signer
    assert request["message"] == registration.REGISTER_MESSAGE
    assert request["nonce_anchor"] == str(observed.anchor)
    assert request["nonce_bitmap_index"] == observed.current_bitmap_index
    assert request["expiration"] == str(context.expiration)
    assert len(request["account_signature"]) == len(request["signer_signature"]) == 132

    register_typed = onboarding.build_register_signer_typed_data(
        context.wallet,
        context.signer,
        context.expiration,
        observed.anchor,
        observed.current_bitmap_index,
    )
    verify_typed = onboarding.build_verify_signer_typed_data(
        context.wallet, observed.anchor, observed.current_bitmap_index
    )
    assert Account.recover_message(
        encode_typed_data(full_message=register_typed),
        signature=request["account_signature"],
    ).lower() == context.wallet
    assert Account.recover_message(
        encode_typed_data(full_message=verify_typed),
        signature=request["signer_signature"],
    ).lower() == context.signer

    persisted = json.loads(
        onboarding.protected_paths()["registration_intent"].read_text()
    )
    assert persisted["runtime_id"] == result.runtime_id
    assert persisted["intent_id"] == result.intent_id
    assert context.main_key.hex().encode() not in json.dumps(persisted).encode()
    assert context.session_key.hex().encode() not in json.dumps(persisted).encode()
    assert context.main_key.hex() not in result.evidence()
    assert context.session_key.hex() not in result.evidence()


@pytest.mark.asyncio
async def test_already_active_is_terminal_without_prompt_post_or_intent(context) -> None:
    scenario = Scenario(context.wallet, context.signer, context.expiration)
    scenario.active()
    called = 0

    def no_prompt(_prompt: str) -> str:
        nonlocal called
        called += 1
        raise AssertionError("main key must not be requested for an active signer")

    result = await _run(context, scenario, input_fn=no_prompt)
    assert result.state is registration.RegistrationState.REGISTERED
    assert result.reason == "ALREADY_REGISTERED"
    assert result.dispatch_count == 0
    assert result.runtime_id is None and result.intent_id is None
    assert called == 0
    assert scenario.post_count == 0
    assert not onboarding.protected_paths()["registration_intent"].exists()
    assert not onboarding.protected_paths()["registration_spent"].exists()


@pytest.mark.asyncio
async def test_nonce_change_after_signing_blocks_before_claim_or_post(context) -> None:
    scenario = Scenario(context.wallet, context.signer, context.expiration)
    scenario.nonce_sequence = [_nonce(), _nonce(anchor=8, index=3)]
    result = await _run(context, scenario)
    assert result.state is registration.RegistrationState.BLOCKED
    assert result.reason == "NONCE_CHANGED_BEFORE_CLAIM"
    assert result.dispatch_count == 0
    assert scenario.post_count == 0
    assert onboarding.protected_paths()["registration_intent"].exists()
    assert not onboarding.protected_paths()["registration_spent"].exists()
    persisted = onboarding.load_registration_intent()
    assert persisted.runtime_id != persisted.intent_id


@pytest.mark.parametrize(
    ("body", "reason"),
    [
        (_nonce(anchor=2**48 - 1, index=208, bitmap=(1 << 208) - 1), "NONCE_BITMAP_FULL_INVALID"),
        (_nonce(index=209), "NONCE_STATE_INVALID"),
        (_nonce(index=3, bitmap=1 << 3), "NONCE_INDEX_ALREADY_USED"),
        (_nonce(anchor=40, index=208, bitmap=0), "NONCE_BITMAP_FULL_INVALID"),
    ],
)
def test_nonce_bitmap_and_freshness_semantics_are_strict(body, reason) -> None:
    with pytest.raises(registration._RegistrationFailure) as caught:
        registration._parse_nonce(body)
    assert caught.value.reason == reason


def test_post_nonce_accepts_only_the_exact_consumed_transition() -> None:
    observed = registration._parse_nonce(_nonce(anchor=2, index=207, bitmap=(1 << 207) - 1))
    expected = registration._expected_post_nonce(observed)
    assert expected == registration.NonceObservation(2, 208, (1 << 208) - 1)
    parsed_full = registration._parse_nonce(
        _nonce(anchor=2, index=208, bitmap=(1 << 208) - 1)
    )
    assert parsed_full == expected
    assert parsed_full.signed_anchor == 3
    assert parsed_full.signed_bitmap_index == 0
    assert not registration._nonce_consumed(
        observed,
        registration._parse_nonce(
            _nonce(anchor=3, index=208, bitmap=(1 << 208) - 1)
        ),
    )


@pytest.mark.asyncio
async def test_full_nonce_cursor_registers_next_anchor_bit_zero(context) -> None:
    scenario = Scenario(context.wallet, context.signer, context.expiration)
    scenario.nonce = _nonce(anchor=11, index=208, bitmap=(1 << 208) - 1)
    observed = registration._parse_nonce(scenario.nonce)
    scenario.post_effect = lambda: (scenario.active(), scenario.consumed(observed))

    result = await _run(context, scenario)

    assert result.state is registration.RegistrationState.REGISTERED
    assert result.reason == "COMPLETE"
    assert scenario.post_count == 1
    assert result.observed_nonce_anchor == observed.anchor
    assert result.observed_nonce_bitmap_index == observed.current_bitmap_index
    request = scenario.posts[0]
    assert request["nonce_anchor"] == str(observed.signed_anchor)
    assert request["nonce_bitmap_index"] == observed.signed_bitmap_index
    assert result.post_nonce_anchor == observed.anchor + 1
    assert result.post_nonce_bitmap_index == 1
    persisted = onboarding.load_registration_intent()
    assert persisted.observed_nonce_anchor == observed.anchor
    assert persisted.observed_bitmap_index == 208
    assert persisted.nonce_anchor == observed.anchor + 1
    assert persisted.nonce_bitmap_index == 0


def test_signer_reconciliation_binds_exact_signer_status_and_expiration(context) -> None:
    unrelated = {
        "signer": "0x" + "33" * 20,
        "status": "not-an-authorized-row",
        "future_field": {"ignored": True},
    }
    parsed = registration._parse_signers(
        _envelope({"signers": [unrelated, _active_signer(context.expiration)]})
    )
    assert parsed.active and parsed.status == "Active"

    with pytest.raises(registration._RegistrationFailure) as duplicate:
        registration._parse_signers(
            _envelope({"signers": [_active_signer(context.expiration)] * 2})
        )
    assert duplicate.value.reason == "IDENTITY_BINDING_MISMATCH"

    wrong_expiration = _active_signer(context.expiration)
    wrong_expiration["expiration"] = str(context.expiration + 1)
    with pytest.raises(registration._RegistrationFailure) as mismatch:
        registration._parse_signers(_envelope({"signers": [wrong_expiration]}))
    assert mismatch.value.reason == "IDENTITY_BINDING_MISMATCH"

    malformed_status = _active_signer(context.expiration)
    malformed_status["status"] = ["Active"]
    with pytest.raises(registration._RegistrationFailure) as schema:
        registration._parse_signers(_envelope({"signers": [malformed_status]}))
    assert schema.value.reason == "POST_RESPONSE_INVALID"


@pytest.mark.asyncio
async def test_ambiguous_post_reconciles_once_and_restart_reuses_runtime_without_post(context) -> None:
    scenario = Scenario(context.wallet, context.signer, context.expiration)
    observed = registration._parse_nonce(scenario.nonce)
    scenario.post_effect = lambda: (scenario.active(), scenario.consumed(observed))
    scenario.post_error = asyncio.TimeoutError()

    first = await _run(context, scenario)
    assert first.state is registration.RegistrationState.REGISTERED
    assert first.reason == "RECONCILED_AFTER_AMBIGUOUS_POST"
    assert first.reconciliation == "PROVEN"
    assert scenario.post_count == 1

    second = await _run(context, scenario)
    assert second.state is registration.RegistrationState.REGISTERED
    assert second.runtime_id == first.runtime_id
    assert second.intent_id == first.intent_id
    assert scenario.post_count == 1
    assert len(scenario.posts) == 1


@pytest.mark.asyncio
async def test_ambiguous_post_without_proof_is_spent_unknown_and_not_retried(context) -> None:
    scenario = Scenario(context.wallet, context.signer, context.expiration)
    scenario.post_error = asyncio.TimeoutError()

    first = await _run(context, scenario)
    assert first.state is registration.RegistrationState.SPENT_UNKNOWN
    assert first.reconciliation == "FAILED"
    assert first.runtime_id and first.intent_id
    assert first.runtime_id != first.intent_id
    assert scenario.post_count == 1

    second = await _run(context, scenario)
    assert second.state is registration.RegistrationState.SPENT_UNKNOWN
    assert second.runtime_id == first.runtime_id
    assert second.intent_id == first.intent_id
    assert scenario.post_count == 1


@pytest.mark.asyncio
async def test_runtime_tampering_blocks_before_prompt_or_post(context) -> None:
    onboarding.prepare_registration_intent(
        nonce_anchor=7, current_bitmap_index=3, bitmap="0x0"
    )
    path = onboarding.protected_paths()["registration_intent"]
    data = json.loads(path.read_text())
    data["runtime_id"] = data["intent_id"]
    path.write_text(json.dumps(data, sort_keys=True, separators=(",", ":")) + "\n")

    called = 0

    def no_prompt(_prompt: str) -> str:
        nonlocal called
        called += 1
        raise AssertionError("tampered intent must stop before prompt")

    scenario = Scenario(context.wallet, context.signer, context.expiration)
    result = await _run(context, scenario, input_fn=no_prompt)
    assert result.state is registration.RegistrationState.BLOCKED
    assert result.reason == "REGISTRATION_INTENT_INVALID"
    assert result.runtime_id is None
    assert called == 0
    assert scenario.post_count == 0


@pytest.mark.asyncio
async def test_cancellation_after_claim_leaves_spent_marker_and_never_replays(context) -> None:
    scenario = Scenario(context.wallet, context.signer, context.expiration)
    scenario.post_error = asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await _run(context, scenario)
    assert scenario.post_count == 1
    assert onboarding.protected_paths()["registration_spent"].exists()
    first_intent = onboarding.load_registration_intent()

    second = await _run(context, scenario)
    assert second.state is registration.RegistrationState.SPENT_UNKNOWN
    assert second.runtime_id == first_intent.runtime_id
    assert second.intent_id == first_intent.intent_id
    assert scenario.post_count == 1


@pytest.mark.asyncio
async def test_explicit_http_failure_is_not_upgraded_by_contradictory_reads(context) -> None:
    scenario = Scenario(context.wallet, context.signer, context.expiration)
    observed = registration._parse_nonce(scenario.nonce)
    scenario.post_status = 400
    scenario.post_body = _envelope({"success": False})
    scenario.post_effect = lambda: (scenario.active(), scenario.consumed(observed))

    result = await _run(context, scenario)
    assert result.state is registration.RegistrationState.SPENT_UNKNOWN
    assert result.reason == "POST_REJECTED"
    assert result.failure_class == "HTTP"
    assert result.reconciliation == "FAILED"
    assert scenario.post_count == 1


@pytest.mark.asyncio
async def test_post_success_requires_authoritative_active_and_consumed_nonce(context) -> None:
    scenario = Scenario(context.wallet, context.signer, context.expiration)
    result = await _run(context, scenario)
    assert result.state is registration.RegistrationState.SPENT_UNKNOWN
    assert result.reason == "POST_RECONCILIATION_FAILED"
    assert result.reconciliation == "FAILED"
    assert scenario.post_count == 1


@pytest.mark.asyncio
async def test_read_transport_gets_one_bounded_retry_but_post_does_not(context) -> None:
    scenario = Scenario(context.wallet, context.signer, context.expiration)
    scenario.get_failures = [registration._RegistrationFailure(
        "POST_RECONCILIATION_UNAVAILABLE", "TRANSPORT"
    )]
    observed = registration._parse_nonce(scenario.nonce)
    scenario.post_effect = lambda: (scenario.active(), scenario.consumed(observed))
    result = await _run(context, scenario)
    assert result.state is registration.RegistrationState.REGISTERED
    assert scenario.post_count == 1
    domain_reads = [call for call in scenario.calls if call[1] == registration.DOMAIN_PATH]
    assert len(domain_reads) == 2


@pytest.mark.asyncio
async def test_input_failure_is_redacted_and_does_not_create_intent_or_post(context) -> None:
    sentinel = "synthetic-private-key-error"

    def broken(_prompt: str) -> str:
        raise RuntimeError(sentinel)

    scenario = Scenario(context.wallet, context.signer, context.expiration)
    result = await _run(context, scenario, input_fn=broken)
    assert result.state is registration.RegistrationState.BLOCKED
    assert result.reason == "PROTECTED_INPUT_UNAVAILABLE"
    assert sentinel not in result.evidence()
    assert scenario.post_count == 0
    assert not onboarding.protected_paths()["registration_intent"].exists()


@pytest.mark.asyncio
async def test_identity_binding_mismatch_blocks_before_network_or_input(context, monkeypatch) -> None:
    monkeypatch.setattr(
        registration,
        "AUTHORIZED_BINDING",
        registration.AuthorizationBinding(
            environment="MAINNET",
            wallet_address="0x" + "33" * 20,
            session_signer_address=context.signer,
            chain_id=registration.MAINNET_CHAIN_ID,
            verifying_contract=registration.MAINNET_AUTH_CONTRACT,
            expiration=context.expiration,
            message=registration.REGISTER_MESSAGE,
        ),
    )
    called = 0

    def no_prompt(_prompt: str) -> str:
        nonlocal called
        called += 1
        raise AssertionError("identity gate must precede prompt")

    scenario = Scenario(context.wallet, context.signer, context.expiration)
    result = await _run(context, scenario, input_fn=no_prompt)
    assert result.state is registration.RegistrationState.BLOCKED
    assert result.reason == "IDENTITY_BINDING_MISMATCH"
    assert called == 0
    assert scenario.calls == []


@pytest.mark.asyncio
async def test_unprotected_credential_blocks_before_network_or_prompt(context) -> None:
    session_path = onboarding.protected_paths()["session_key"]
    session_path.chmod(0o644)
    called = 0

    def no_prompt(_prompt: str) -> str:
        nonlocal called
        called += 1
        raise AssertionError("prompt must remain unreachable")

    scenario = Scenario(context.wallet, context.signer, context.expiration)
    result = await _run(context, scenario, input_fn=no_prompt)
    assert result.state is registration.RegistrationState.BLOCKED
    assert result.reason == "PROTECTED_CREDENTIALS_UNAVAILABLE"
    assert called == 0
    assert scenario.calls == []


def test_protected_files_are_fixed_and_capability_zeroizes() -> None:
    assert onboarding.SESSION_KEY_FILENAME == "risex-mainnet-session-signer-v1.key"
    assert onboarding.IDENTITY_FILENAME == "risex-mainnet-identity-v1.json"
    assert onboarding.REGISTRATION_INTENT_FILENAME == "risex-mainnet-register-signer-v1.json"
    assert onboarding.REGISTRATION_SPENT_FILENAME == "risex-mainnet-register-signer-v1.spent"
    main = bytearray(MAIN_KEY)
    session = bytearray(SESSION_KEY)
    capability = registration._RegistrationSigningCapability(main, session)
    capability.close()
    assert main == bytearray()
    assert session == bytearray()
    capability.close()
