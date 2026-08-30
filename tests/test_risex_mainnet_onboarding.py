from __future__ import annotations

import ast
from dataclasses import replace
import json
import os
import inspect
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest
from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_utils import keccak

from risex_farmer import risex_mainnet_onboarding as onboarding


ROOT = Path(__file__).parents[1]
NOW = 1_800_000_000


@pytest.fixture
def context(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    directory = tmp_path / "home" / ".config" / "risex-farmer"
    main_key = bytes(Account.create().key)
    session_key = bytes(Account.create().key)
    monkeypatch.setattr(onboarding, "PROTECTED_SECRET_DIRECTORY", directory)
    monkeypatch.setattr(onboarding, "_now_unix", lambda: NOW)
    monkeypatch.setattr(
        onboarding,
        "_new_session_secret",
        lambda: bytearray(session_key),
    )
    return SimpleNamespace(
        directory=directory,
        main_key=main_key,
        session_key=session_key,
        main_address=Account.from_key(main_key).address.lower(),
        session_address=Account.from_key(session_key).address.lower(),
        expiration=NOW + onboarding.SESSION_EXPIRATION_SECONDS,
    )


def _prompt_for(context, prompts: list[str] | None = None):
    def prompt(value: str) -> str:
        if prompts is not None:
            prompts.append(value)
        return "0x" + context.main_key.hex()

    return prompt


def _provision(context, prompts: list[str] | None = None):
    return onboarding.provision_mainnet_session_signer(
        _prompt_for(context, prompts)
    )


def _signature(account, typed_data) -> str:
    return "0x" + Account.sign_message(
        encode_typed_data(full_message=typed_data), account.key
    ).signature.hex()


def test_hidden_one_shot_provisioning_derives_only_public_identity_and_stores_session_key(
    context,
):
    prompts: list[str] = []
    result = _provision(context, prompts)

    assert result.status == onboarding.PROVISIONED
    assert result.reason == onboarding.PROVISIONED
    assert result.wallet_address == context.main_address
    assert result.session_signer_address == context.session_address
    assert result.expiration == context.expiration
    assert len(prompts) == 1
    assert "hidden" in prompts[0]
    assert "not persisted" in prompts[0]
    assert result.ready and not result.write_ready
    assert result.mainnet_write_authority == onboarding.NO_MAINNET_WRITE_AUTHORITY

    paths = onboarding.protected_paths()
    assert set(paths) == {
        "session_key",
        "identity",
        "registration_intent",
        "registration_spent",
    }
    assert paths["session_key"].read_bytes() == context.session_key
    identity_bytes = paths["identity"].read_bytes()
    identity = json.loads(identity_bytes)
    assert identity == {
        "chain_id": onboarding.MAINNET_CHAIN_ID,
        "environment": "MAINNET",
        "expiration": context.expiration,
        "registration_status": onboarding.REGISTRATION_NOT_PREPARED,
        "schema_version": 1,
        "session_signer_address": context.session_address,
        "venue": "RISEx",
        "verifying_contract": onboarding.MAINNET_AUTH_CONTRACT,
        "wallet_address": context.main_address,
    }
    assert context.main_key.hex().encode() not in identity_bytes
    assert context.main_key.hex() not in result.evidence()
    assert context.session_key.hex() not in result.evidence()
    assert onboarding.inspect_protected_files().all_required_protected
    assert os.stat(paths["session_key"]).st_mode & 0o777 == 0o600
    assert os.stat(paths["identity"]).st_mode & 0o777 == 0o600
    assert os.stat(paths["session_key"]).st_nlink == 1
    assert os.stat(paths["identity"]).st_nlink == 1

    loaded = onboarding.read_provisioned_identity()
    assert loaded == onboarding.ProvisionedIdentity(
        context.main_address, context.session_address, context.expiration
    )


def test_prompt_cancellation_is_sanitized_and_creates_no_directory(context):
    prompts: list[str] = []

    def cancelled(prompt: str):
        prompts.append(prompt)
        raise KeyboardInterrupt

    result = onboarding.provision_mainnet_session_signer(cancelled)

    assert result.status == onboarding.BLOCKED
    assert result.reason == "PROTECTED_INPUT_CANCELLED"
    assert len(prompts) == 1
    assert not context.directory.exists()


@pytest.mark.parametrize(
    "value",
    [
        "",
        "not-a-key",
        "0x1234",
        "g" * 64,
        "0x" + "a" * 63,
        "0x" + "a" * 65,
        "0x" + "a" * 63 + "\n",
        None,
    ],
)
def test_malformed_main_keys_fail_closed_without_persisting(value, context):
    calls = 0

    def prompt(_prompt: str):
        nonlocal calls
        calls += 1
        return value

    result = onboarding.provision_mainnet_session_signer(prompt)

    assert result.status == onboarding.BLOCKED
    assert result.reason == "MAIN_KEY_INVALID"
    assert calls == 1
    assert not context.directory.exists()


def test_main_and_session_identity_must_be_distinct(context, monkeypatch):
    monkeypatch.setattr(
        onboarding,
        "_new_session_secret",
        lambda: bytearray(context.main_key),
    )

    result = _provision(context)

    assert result.status == onboarding.BLOCKED
    assert result.reason == "MAIN_AND_SESSION_IDENTITIES_NOT_DISTINCT"
    assert not context.directory.exists()


def test_main_key_bytearray_is_zeroed_after_derivation(context, monkeypatch):
    observed: list[bytearray] = []
    real_derive = onboarding._derive_address

    def capture(secret):
        if isinstance(secret, bytearray):
            observed.append(secret)
        return real_derive(secret)

    monkeypatch.setattr(onboarding, "_derive_address", capture)
    assert _provision(context).ready
    assert observed
    assert all(not any(secret) for secret in observed)


def test_existing_paths_are_never_overwritten_or_reprompted(context):
    first = _provision(context)
    assert first.ready
    key_before = onboarding.protected_paths()["session_key"].read_bytes()
    calls = 0

    def forbidden_prompt(_prompt: str):
        nonlocal calls
        calls += 1
        raise AssertionError("existing protected paths must not prompt")

    second = onboarding.provision_mainnet_session_signer(forbidden_prompt)

    assert second.status == onboarding.BLOCKED
    assert second.reason == "PROTECTED_PATH_ALREADY_EXISTS"
    assert calls == 0
    assert onboarding.protected_paths()["session_key"].read_bytes() == key_before


def test_directory_mode_and_symlink_fail_closed_before_hidden_input(context):
    context.directory.mkdir(parents=True, mode=0o755)
    os.chmod(context.directory, 0o755)
    calls = 0

    def prompt(_prompt: str):
        nonlocal calls
        calls += 1
        return "not-used"

    inspected = onboarding.inspect_protected_files()
    assert inspected.identity.reason == "PROTECTED_DIRECTORY_MODE_NOT_0700"
    result = onboarding.provision_mainnet_session_signer(prompt)
    assert result.reason == "PROTECTED_DIRECTORY_MODE_NOT_0700"
    assert calls == 0

    context.directory.rmdir()
    replacement = context.directory.parent / "replacement"
    replacement.mkdir(mode=0o700)
    context.directory.symlink_to(replacement, target_is_directory=True)
    inspected = onboarding.inspect_protected_files()
    assert inspected.session_key.reason == "PROTECTED_DIRECTORY_SYMLINK"
    result = onboarding.provision_mainnet_session_signer(prompt)
    assert result.reason == "PROTECTED_DIRECTORY_SYMLINK"
    assert calls == 0


def test_file_mode_symlink_hardlink_and_foreign_owner_are_rejected(context, monkeypatch):
    assert _provision(context).ready
    paths = onboarding.protected_paths()

    os.chmod(paths["session_key"], 0o640)
    inspected = onboarding.inspect_protected_files()
    assert inspected.session_key.reason == "PROTECTED_FILE_MODE_NOT_0600"
    assert not inspected.session_key.protected

    os.chmod(paths["session_key"], 0o600)
    replacement = context.directory.parent / "replacement-key"
    replacement.write_bytes(b"replacement")
    paths["session_key"].unlink()
    paths["session_key"].symlink_to(replacement)
    inspected = onboarding.inspect_protected_files()
    assert inspected.session_key.reason == "PROTECTED_FILE_SYMLINK"
    assert not inspected.session_key.protected

    paths["session_key"].unlink()
    hardlink_source = context.directory.parent / "hardlink-key"
    hardlink_source.write_bytes(b"hardlink")
    os.chmod(hardlink_source, 0o600)
    os.link(hardlink_source, paths["session_key"])
    inspected = onboarding.inspect_protected_files()
    assert inspected.session_key.reason == "PROTECTED_FILE_HARDLINK"
    assert not inspected.session_key.protected

    current_uid = os.getuid()
    foreign_uid = current_uid + 100000
    real_stat = onboarding.os.stat

    def foreign_identity_stat(path, *args, **kwargs):
        info = real_stat(path, *args, **kwargs)
        if path == onboarding.IDENTITY_FILENAME:
            return SimpleNamespace(
                st_mode=info.st_mode,
                st_nlink=info.st_nlink,
                st_size=info.st_size,
                st_uid=foreign_uid,
            )
        return info

    monkeypatch.setattr(onboarding.os, "stat", foreign_identity_stat)
    inspected = onboarding.inspect_protected_files()
    assert inspected.identity.reason == "PROTECTED_FILE_OWNER_NOT_CURRENT_USER"


def test_foreign_directory_owner_is_rejected(context, monkeypatch):
    assert _provision(context).ready
    current_uid = os.getuid()
    monkeypatch.setattr(onboarding.os, "getuid", lambda: current_uid + 100000)
    inspected = onboarding.inspect_protected_files()
    assert inspected.session_key.reason == "PROTECTED_DIRECTORY_OWNER_NOT_CURRENT_USER"
    assert inspected.identity.reason == "PROTECTED_DIRECTORY_OWNER_NOT_CURRENT_USER"


def test_parent_component_symlink_is_rejected_before_hidden_input(context):
    config = context.directory.parent
    config.mkdir(parents=True, mode=0o700)
    target = config.parent / "config-target"
    target.mkdir(mode=0o700)
    config.rmdir()
    config.symlink_to(target, target_is_directory=True)
    calls = 0

    def prompt(_prompt: str):
        nonlocal calls
        calls += 1
        return "not-used"

    inspected = onboarding.inspect_protected_files()
    assert inspected.identity.reason == "PROTECTED_DIRECTORY_SYMLINK"
    result = onboarding.provision_mainnet_session_signer(prompt)
    assert result.reason == "PROTECTED_DIRECTORY_SYMLINK"
    assert calls == 0


def test_directory_descriptor_remains_bound_across_parent_swap(context, monkeypatch):
    assert _provision(context).ready
    parent = context.directory.parent
    moved_parent = parent.parent / "config-moved"
    real_open = onboarding._open_directory

    def open_then_swap():
        descriptor = real_open()
        parent.rename(moved_parent)
        parent.mkdir(mode=0o700)
        return descriptor

    monkeypatch.setattr(onboarding, "_open_directory", open_then_swap)
    inspected = onboarding.inspect_protected_files()

    assert inspected.session_key.protected
    assert inspected.identity.protected
    assert inspected.session_key.size == 32


def test_unexpected_prompt_exception_never_leaks_key_text(context):
    secret_text = "synthetic-secret-that-must-not-escape"

    def broken_prompt(_prompt: str):
        raise RuntimeError("prompt failure: " + secret_text)

    result = onboarding.provision_mainnet_session_signer(broken_prompt)

    assert result.reason == "PROTECTED_INPUT_UNAVAILABLE"
    assert secret_text not in repr(result)
    assert secret_text not in result.evidence()
    assert not context.directory.exists()


def test_register_signer_contract_shape_and_recovery_are_exact(context):
    account = Account.from_key(context.main_key)
    signer = Account.from_key(context.session_key)
    register = onboarding.build_register_signer_typed_data(
        account.address,
        signer.address,
        context.expiration,
        8,
        0,
    )
    verify = onboarding.build_verify_signer_typed_data(account.address, 8, 0)

    assert onboarding.MAINNET_CHAIN_ID == 4153
    assert onboarding.MAINNET_AUTH_CONTRACT == "0x0d919daa3f12ae715744eb648c00066c5dbd66f0"
    assert register["primaryType"] == "RegisterSigner"
    assert verify["primaryType"] == "VerifySigner"
    assert register["domain"] == {
        "name": "RISEx",
        "version": "1",
        "chainId": 4153,
        "verifyingContract": onboarding.MAINNET_AUTH_CONTRACT,
    }
    assert register["types"]["RegisterSigner"] == list(
        onboarding.REGISTER_SIGNER_FIELDS
    )
    assert verify["types"]["VerifySigner"] == list(
        onboarding.VERIFY_SIGNER_FIELDS
    )
    assert keccak(
        text="RegisterSigner(address account,address signer,string message,uint32 expiration,uint48 nonceAnchor,uint8 nonceBitmap)"
    ).hex() == "a526f63b3968e56ae1b177ce9b3dc29766e0891e6397a9c23cf8c53ee8fc8f62"
    assert keccak(
        text="VerifySigner(address account,uint48 nonceAnchor,uint8 nonceBitmap)"
    ).hex() == "4d298dcceb691695f582cc337308236426a0c97201a31834625e8eadc44d4230"

    account_signature = Account.sign_message(
        encode_typed_data(full_message=register), context.main_key
    ).signature
    signer_signature = Account.sign_message(
        encode_typed_data(full_message=verify), context.session_key
    ).signature
    assert Account.recover_message(
        encode_typed_data(full_message=register), signature=account_signature
    ).lower() == context.main_address
    assert Account.recover_message(
        encode_typed_data(full_message=verify), signature=signer_signature
    ).lower() == context.session_address


def test_registration_intent_binds_nonce_identity_is_durable_and_non_replayable(
    context,
):
    assert _provision(context).ready
    intent = onboarding.prepare_registration_intent(
        nonce_anchor=7,
        current_bitmap_index=3,
        bitmap="0x4",
    )

    assert intent.intent_id and len(intent.intent_id) == 32
    assert intent.wallet_address == context.main_address
    assert intent.session_signer_address == context.session_address
    assert intent.observed_nonce_anchor == 7
    assert intent.observed_bitmap_index == 3
    assert intent.observed_bitmap == 4
    assert intent.nonce_anchor == 7
    assert intent.nonce_bitmap_index == 3
    assert intent.state == onboarding.REGISTRATION_PREPARED
    assert intent.typed_register_data["message"]["nonceAnchor"] == 7
    assert intent.typed_register_data["message"]["nonceBitmap"] == 3

    intent_bytes = onboarding.protected_paths()["registration_intent"].read_bytes()
    assert context.main_key.hex().encode() not in intent_bytes
    assert context.session_key.hex().encode() not in intent_bytes
    assert os.stat(onboarding.protected_paths()["registration_intent"]).st_mode & 0o777 == 0o600

    loaded = onboarding.load_registration_intent()
    assert loaded == intent
    with pytest.raises(onboarding.OnboardingViolation) as duplicate:
        onboarding.prepare_registration_intent(
            nonce_anchor=7,
            current_bitmap_index=3,
            bitmap="4",
        )
    assert duplicate.value.reason == "REGISTRATION_INTENT_ALREADY_EXISTS"
    assert onboarding.load_registration_intent() == loaded

    claimed = onboarding.claim_registration_intent()
    assert claimed.state == onboarding.REGISTRATION_SPENT_UNKNOWN
    assert onboarding.load_registration_intent().state == onboarding.REGISTRATION_SPENT_UNKNOWN
    with pytest.raises(onboarding.OnboardingViolation) as replay:
        onboarding.claim_registration_intent()
    assert replay.value.reason == "REGISTRATION_INTENT_ALREADY_SPENT"
    with pytest.raises(onboarding.OnboardingViolation) as reuse:
        onboarding.build_register_signer_request(
            claimed,
            "0x" + "a" * 130,
            "0x" + "b" * 130,
        )
    assert reuse.value.reason == "REGISTRATION_INTENT_NOT_REUSABLE"


@pytest.mark.parametrize(
    ("field", "value"),
    [("nonce_anchor", 8), ("nonce_bitmap_index", 4)],
)
def test_persisted_nonce_identity_mismatch_is_rejected(field, value, context):
    assert _provision(context).ready
    intent = onboarding.prepare_registration_intent(
        nonce_anchor=7,
        current_bitmap_index=3,
        bitmap="0x4",
    )
    path = onboarding.protected_paths()["registration_intent"]
    persisted = json.loads(path.read_text())
    persisted[field] = value
    path.write_text(json.dumps(persisted, sort_keys=True, separators=(",", ":")) + "\n")

    with pytest.raises(onboarding.OnboardingViolation) as loaded:
        onboarding.load_registration_intent()
    assert loaded.value.reason == "REGISTRATION_INTENT_INVALID"
    with pytest.raises(onboarding.OnboardingViolation) as requested:
        onboarding.build_register_signer_request(
            intent,
            "0x" + "a" * 130,
            "0x" + "b" * 130,
        )
    assert requested.value.reason == "REGISTRATION_INTENT_INVALID"


@pytest.mark.parametrize(
    "field",
    ["nonce_anchor", "nonce_bitmap_index"],
)
def test_request_nonce_identity_mismatch_is_rejected(field, context):
    assert _provision(context).ready
    intent = onboarding.prepare_registration_intent(
        nonce_anchor=7,
        current_bitmap_index=3,
        bitmap="0x4",
    )
    replacement = replace(intent, **{field: getattr(intent, field) + 1})

    with pytest.raises(onboarding.OnboardingViolation) as requested:
        onboarding.build_register_signer_request(
            replacement,
            "0x" + "a" * 130,
            "0x" + "b" * 130,
        )
    assert requested.value.reason == "REGISTRATION_INTENT_NONCE_MISMATCH"


def test_register_request_is_exact_and_has_no_unofficial_fields(context):
    assert _provision(context).ready
    intent = onboarding.prepare_registration_intent(
        nonce_anchor=10,
        current_bitmap_index=5,
        bitmap="0x4",
    )
    request = onboarding.build_register_signer_request(
        intent,
        "0x" + "a" * 130,
        "0x" + "b" * 130,
    )

    assert set(request) == {
        "account",
        "signer",
        "message",
        "nonce_anchor",
        "nonce_bitmap_index",
        "expiration",
        "account_signature",
        "signer_signature",
    }
    assert request["account"] == context.main_address
    assert request["signer"] == context.session_address
    assert request["message"] == onboarding.REGISTER_SIGNER_MESSAGE
    assert request["nonce_anchor"] == "10"
    assert request["nonce_bitmap_index"] == 5
    assert request["expiration"] == str(context.expiration)
    assert "label" not in request


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("nonce_anchor", -1, "NONCE_ANCHOR_INVALID"),
        ("nonce_anchor", 2**48, "NONCE_ANCHOR_INVALID"),
        ("current_bitmap_index", 208, "NONCE_BITMAP_INDEX_INVALID"),
        ("current_bitmap_index", -1, "NONCE_BITMAP_INDEX_INVALID"),
        ("bitmap", "0x" + "0" * 65, "NONCE_BITMAP_INVALID"),
        ("bitmap", "not-a-bitmap", "NONCE_BITMAP_INVALID"),
    ],
)
def test_nonce_identity_is_bounded(field, value, reason, context):
    assert _provision(context).ready
    values = {
        "nonce_anchor": 1,
        "current_bitmap_index": 0,
        "bitmap": "0",
    }
    values[field] = value
    with pytest.raises(onboarding.OnboardingViolation) as caught:
        onboarding.prepare_registration_intent(**values)
    assert caught.value.reason == reason
    assert not onboarding.protected_paths()["registration_intent"].exists()


def test_module_is_offline_and_absent_from_normal_cli_imports():
    source = (
        ROOT / "src" / "risex_farmer" / "risex_mainnet_onboarding.py"
    ).read_text()
    for forbidden in (
        "aiohttp",
        "requests",
        "sqlite3",
        "websocket",
        "/v1/",
        "POST ",
        "orders",
        "positions",
        "withdraw",
        "transfer",
        "lstat",
    ):
        assert forbidden not in source
    tree = ast.parse(source)
    imported_roots = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.extend(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.append(node.module.split(".")[0])
    assert set(imported_roots) <= {
        "dataclasses",
        "getpass",
        "json",
        "os",
        "pathlib",
        "secrets",
        "stat",
        "time",
        "typing",
        "__future__",
        "eth_account",
        "errno",
    }

    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT / "src")
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import risex_farmer.cli; print('risex_farmer.risex_mainnet_onboarding' in sys.modules)",
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=True,
    )
    assert completed.stdout.strip() == "False"


def test_explicit_command_has_no_key_argument_surface():
    assert onboarding.main.__name__ == "main"
    assert onboarding.provision_mainnet_session_signer.__name__ == (
        "provision_mainnet_session_signer"
    )
    assert tuple(inspect.signature(onboarding.main).parameters) == ()
    assert tuple(
        inspect.signature(onboarding.provision_mainnet_session_signer).parameters
    ) == ("input_fn",)
