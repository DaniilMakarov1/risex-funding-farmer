from __future__ import annotations

import ast
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from eth_account import Account

from risex_farmer import nado_mainnet_onboarding as onboarding


ROOT = Path(__file__).parents[1]
MAIN_KEY = "0x" + "11" * 32
LINKED_KEY = "0x" + "22" * 32
OTHER_KEY = "0x" + "33" * 32


def _address(private_key: str) -> str:
    return str(Account.from_key(bytes.fromhex(private_key[2:])).address).lower()


def _public_identity(
    private_key: str = MAIN_KEY,
    name: str = onboarding.NADO_DEFAULT_SUBACCOUNT_NAME,
) -> tuple[str, str]:
    wallet = _address(private_key)
    subaccount = "0x" + (
        bytes.fromhex(wallet[2:]) + name.encode("ascii").ljust(12, b"\0")
    ).hex()
    return wallet, subaccount


def _set_directory(tmp_path: Path, monkeypatch) -> Path:
    directory = (tmp_path / "nado-mainnet-onboarding").resolve()
    monkeypatch.setattr(onboarding, "PROTECTED_DIRECTORY", directory)
    monkeypatch.setattr(
        onboarding,
        "LINKED_SIGNER_PROTECTED_DIRECTORY",
        (tmp_path / "nado-mainnet-signing").resolve(),
    )
    return directory


def _provision_hidden(
    tmp_path: Path,
    monkeypatch,
    value: str = MAIN_KEY,
    **kwargs: object,
) -> onboarding.OnboardingResult:
    _set_directory(tmp_path, monkeypatch)
    return onboarding.provision_nado_mainnet_identity(
        lambda _prompt: value, **kwargs
    )


def test_derive_and_export_exact_public_wallet_bytes32_identity() -> None:
    identity = onboarding.derive_public_identity(MAIN_KEY)
    wallet, subaccount = _public_identity()
    assert identity.wallet_address == wallet
    assert identity.subaccount == subaccount
    exported = onboarding.export_unsigned_read_identity(identity)
    assert exported["wallet_address"] == wallet
    assert exported["subaccount"] == subaccount
    assert exported["query_authentication"] == onboarding.NADO_UNSIGNED_QUERY_AUTHENTICATION
    assert exported["write_ready"] is False
    assert "credential" not in exported
    assert MAIN_KEY not in repr(exported)


@pytest.mark.parametrize(
    "value",
    [
        "",
        "not-a-key",
        "0x" + "00" * 32,
        "0x" + "11" * 31,
        "0x" + "gg" * 32,
        " 0x" + "11" * 32,
        "0x" + "11" * 32 + "\n",
    ],
)
def test_malformed_main_wallet_keys_fail_closed_without_files(
    tmp_path: Path, monkeypatch, value: str
) -> None:
    directory = _set_directory(tmp_path, monkeypatch)
    with pytest.raises(onboarding.OnboardingViolation):
        onboarding.derive_public_identity(value)
    result = onboarding.provision_nado_mainnet_identity(lambda _prompt: value)
    assert result.status == onboarding.BLOCKED
    assert result.reason == "PROTECTED_INPUT_INVALID:main_wallet_key"
    assert not directory.exists()


@pytest.mark.parametrize("name", ["", "a" * 13, "bad name", "né", "bad\nname"])
def test_malformed_subaccount_names_fail_closed(
    tmp_path: Path, monkeypatch, name: str
) -> None:
    _set_directory(tmp_path, monkeypatch)
    with pytest.raises(onboarding.OnboardingViolation, match="SUBACCOUNT_NAME_INVALID"):
        onboarding.derive_public_identity(MAIN_KEY, name)
    result = onboarding.provision_nado_mainnet_identity(
        lambda _prompt: MAIN_KEY, name
    )
    assert result.status == onboarding.BLOCKED
    assert result.reason == "SUBACCOUNT_NAME_INVALID"


def test_hidden_derivation_is_one_time_and_writes_no_secret_credential(
    tmp_path: Path, monkeypatch
) -> None:
    directory = _set_directory(tmp_path, monkeypatch)
    prompts: list[str] = []
    values = iter((MAIN_KEY, "unexpected-second-prompt"))

    def input_fn(prompt: str) -> str:
        prompts.append(prompt)
        return next(values)

    result = onboarding.provision_nado_mainnet_identity(input_fn)
    paths = onboarding.protected_paths()
    assert result.status == onboarding.IDENTITY_PROVISIONED
    assert result.reason == "PROTECTED_NADO_IDENTITY_CREATED"
    assert result.provisioned
    assert result.identity is not None
    assert prompts == ["Nado main wallet private key (hidden): "]
    assert directory.stat().st_mode & 0o777 == 0o700
    assert result.files.identity.protected
    assert result.files.credential.present is False
    assert result.files.read_identity_ready
    assert paths["identity"].stat().st_mode & 0o777 == 0o600
    assert not paths["credential"].exists()
    metadata = json.loads(paths["identity"].read_text())
    assert set(metadata) == {
        "chain_id",
        "environment",
        "identity_source",
        "mainnet_write_authority",
        "query_authentication",
        "read_status",
        "schema_version",
        "subaccount",
        "subaccount_name",
        "venue",
        "wallet_address",
        "write_ready",
    }
    assert MAIN_KEY not in paths["identity"].read_text()
    assert MAIN_KEY not in repr(result)
    assert MAIN_KEY not in result.evidence()
    assert result.credential_kind is None
    assert result.credential_address is None
    assert result.credential_fingerprint is None


def test_hidden_derivation_wipes_the_transient_key(tmp_path: Path, monkeypatch) -> None:
    _set_directory(tmp_path, monkeypatch)
    original_wipe = onboarding._wipe
    wiped: list[bytes] = []

    def observe(payload: bytearray | None) -> None:
        if payload is not None:
            wiped.append(bytes(payload))
        original_wipe(payload)

    monkeypatch.setattr(onboarding, "_wipe", observe)
    result = onboarding.provision_nado_mainnet_identity(lambda _prompt: MAIN_KEY)
    assert result.provisioned
    assert bytes.fromhex(MAIN_KEY[2:]) in wiped


def test_explicit_public_identity_skips_hidden_input_and_persists_only_identity(
    tmp_path: Path, monkeypatch
) -> None:
    wallet, subaccount = _public_identity(MAIN_KEY, "alpha")
    directory = _set_directory(tmp_path, monkeypatch)
    calls = 0

    def should_not_prompt(_prompt: str) -> str:
        nonlocal calls
        calls += 1
        raise AssertionError("explicit public identity must not prompt")

    def no_private_derivation() -> object:
        raise AssertionError("explicit public identity must not derive a key")

    monkeypatch.setattr(onboarding, "_crypto_account", no_private_derivation)

    result = onboarding.provision_nado_mainnet_identity(
        should_not_prompt,
        wallet_address=wallet.upper().replace("0X", "0x"),
        subaccount=subaccount.upper().replace("0X", "0x"),
    )
    assert result.status == onboarding.PROVISIONED
    assert result.identity is not None
    assert result.identity.wallet_address == wallet
    assert result.identity.subaccount_name == "alpha"
    assert calls == 0
    assert result.files.read_identity_ready
    assert onboarding.protected_paths()["identity"].is_file()
    assert not onboarding.protected_paths()["credential"].exists()
    assert directory.exists()


def test_public_identity_aliases_and_expected_bindings_are_validated(
    tmp_path: Path, monkeypatch
) -> None:
    wallet, subaccount = _public_identity()
    _set_directory(tmp_path, monkeypatch)
    result = onboarding.provision_nado_mainnet_identity(
        lambda _prompt: "bad",
        wallet_address=wallet,
        subaccount=subaccount,
        public_wallet_address=wallet.upper().replace("0X", "0x"),
        public_subaccount=subaccount.upper().replace("0X", "0x"),
        expected_wallet_address=wallet,
        expected_subaccount=subaccount,
    )
    assert result.provisioned
    assert result.identity is not None
    assert result.identity.wallet_address == wallet


@pytest.mark.parametrize(
    "kwargs,reason",
    [
        ({"wallet_address": _address(OTHER_KEY)}, "PUBLIC_IDENTITY_INPUT_INCOMPLETE"),
        ({"subaccount": _public_identity()[1]}, "PUBLIC_IDENTITY_INPUT_INCOMPLETE"),
        (
            {
                "wallet_address": _address(OTHER_KEY),
                "public_wallet_address": _public_identity()[0],
                "subaccount": _public_identity()[1],
            },
            "PUBLIC_IDENTITY_CONFLICT",
        ),
        (
            {
                "wallet_address": _public_identity()[0],
                "subaccount": _public_identity()[1],
                "expected_wallet_address": _address(OTHER_KEY),
            },
            "MAIN_WALLET_IDENTITY_CONFLICT",
        ),
        (
            {
                "wallet_address": _public_identity()[0],
                "subaccount": _public_identity()[1],
                "expected_subaccount": "0x" + "44" * 32,
            },
            "SUBACCOUNT_IDENTITY_CONFLICT",
        ),
    ],
)
def test_explicit_public_identity_conflicts_fail_before_prompt(
    tmp_path: Path, monkeypatch, kwargs: dict[str, object], reason: str
) -> None:
    directory = _set_directory(tmp_path, monkeypatch)
    called = False

    def no_prompt(_prompt: str) -> str:
        nonlocal called
        called = True
        return MAIN_KEY

    result = onboarding.provision_nado_mainnet_identity(no_prompt, **kwargs)
    assert result.status == onboarding.BLOCKED
    assert result.reason == reason
    assert called is False
    assert not directory.exists()


def test_public_subaccount_must_encode_the_exact_wallet(tmp_path: Path, monkeypatch) -> None:
    wallet, _subaccount = _public_identity()
    _other_wallet, other_subaccount = _public_identity(OTHER_KEY)
    _set_directory(tmp_path, monkeypatch)
    result = onboarding.provision_nado_mainnet_identity(
        lambda _prompt: "bad",
        wallet_address=wallet,
        subaccount=other_subaccount,
    )
    assert result.status == onboarding.BLOCKED
    assert result.reason == "SUBACCOUNT_IDENTITY_CONFLICT"


def test_restart_discovery_reads_public_identity_without_secret_file(
    tmp_path: Path, monkeypatch
) -> None:
    result = _provision_hidden(tmp_path, monkeypatch)
    assert result.identity is not None
    public = onboarding.discover_public_identity()
    assert public == result.identity
    assert onboarding.export_unsigned_read_identity(public)["subaccount"] == public.subaccount
    with pytest.raises(onboarding.OnboardingViolation, match="PROTECTED_FILE_UNAVAILABLE"):
        onboarding.discover_protected_credential()


def test_read_only_provisioning_never_invokes_or_loads_linked_signer_phase(
    tmp_path: Path, monkeypatch
) -> None:
    _set_directory(tmp_path, monkeypatch)
    future_directory = onboarding.future_linked_signer_paths()["directory"]
    future_directory.mkdir(mode=0o700, parents=True)
    future_credential = onboarding.future_linked_signer_paths()["credential"]
    future_credential.write_bytes(b"future-only-sentinel")
    os.chmod(future_credential, 0o600)
    opened_directories: list[Path | None] = []
    original_open = onboarding._open_fixed_directory

    def record_open(*, create: bool = True, directory: Path | None = None):
        opened_directories.append(directory)
        return original_open(create=create, directory=directory)

    monkeypatch.setattr(onboarding, "_open_fixed_directory", record_open)
    linked_calls = 0
    discovery_calls = 0

    def forbidden_linked(*_args: object, **_kwargs: object) -> None:
        nonlocal linked_calls
        linked_calls += 1
        raise AssertionError("linked signer phase must remain separate")

    def forbidden_discovery(*_args: object, **_kwargs: object) -> None:
        nonlocal discovery_calls
        discovery_calls += 1
        raise AssertionError("read-only identity provisioning must not load a secret")

    monkeypatch.setattr(onboarding, "provision_nado_linked_signer", forbidden_linked)
    monkeypatch.setattr(onboarding, "discover_protected_credential", forbidden_discovery)
    prompts: list[str] = []
    result = onboarding.provision_nado_mainnet_identity(
        lambda prompt: (prompts.append(prompt) or MAIN_KEY)
    )
    public = onboarding.discover_public_identity()
    assert result.provisioned
    assert public.wallet_address == _address(MAIN_KEY)
    assert prompts == ["Nado main wallet private key (hidden): "]
    assert linked_calls == 0
    assert discovery_calls == 0
    assert all(directory is None for directory in opened_directories)
    assert future_credential.read_bytes() == b"future-only-sentinel"


def test_historical_credential_shaped_identity_is_rejected(tmp_path: Path, monkeypatch) -> None:
    _set_directory(tmp_path, monkeypatch)
    wallet, subaccount = _public_identity()
    directory = onboarding.PROTECTED_DIRECTORY
    directory.mkdir(mode=0o700, parents=True)
    metadata = {
        "binding_status": "REQUIRES_AUTHORITATIVE_NADO_QUERY",
        "chain_id": onboarding.NADO_MAINNET_CHAIN_ID,
        "credential_address": _address(LINKED_KEY),
        "credential_fingerprint": hashlib.sha256(
            bytes.fromhex(LINKED_KEY[2:])
        ).hexdigest(),
        "credential_kind": onboarding.LINKED_SIGNER_CREDENTIAL,
        "environment": onboarding.NADO_MAINNET_ENVIRONMENT,
        "fallback_authorization": "NOT_APPLICABLE",
        "identity_source": onboarding.NADO_PUBLIC_IDENTITY_SOURCE,
        "mainnet_write_authority": onboarding.NO_MAINNET_WRITE_AUTHORITY,
        "query_authentication": onboarding.NADO_UNSIGNED_QUERY_AUTHENTICATION,
        "schema_version": 1,
        "subaccount": subaccount,
        "subaccount_name": "default",
        "venue": onboarding.NADO_VENUE,
        "wallet_address": wallet,
    }
    path = directory / onboarding.IDENTITY_FILENAME
    path.write_text(json.dumps(metadata, sort_keys=True, separators=(",", ":")))
    os.chmod(path, 0o600)
    with pytest.raises(onboarding.OnboardingViolation, match="METADATA_NOT_CANONICAL"):
        onboarding.discover_public_identity()


def test_linked_signer_requires_existing_identity_and_is_not_default_path(
    tmp_path: Path, monkeypatch
) -> None:
    _set_directory(tmp_path, monkeypatch)
    called = False

    def no_prompt(_prompt: str) -> str:
        nonlocal called
        called = True
        return LINKED_KEY

    result = onboarding.provision_nado_linked_signer(no_prompt)
    assert result.status == onboarding.BLOCKED
    assert result.reason == "PROTECTED_IDENTITY_REQUIRED"
    assert called is False


def test_linked_signer_existing_key_is_explicit_second_phase_only(
    tmp_path: Path, monkeypatch
) -> None:
    identity_result = _provision_hidden(tmp_path, monkeypatch)
    assert identity_result.provisioned
    prompts: list[str] = []
    result = onboarding.provision_nado_linked_signer(
        lambda prompt: (prompts.append(prompt) or LINKED_KEY),
        expected_linked_signer_address=_address(LINKED_KEY),
    )
    paths = onboarding.protected_paths()
    assert result.status == onboarding.LINKED_SIGNER_PROVISIONED
    assert result.provisioned
    assert result.credential_kind == onboarding.LINKED_SIGNER_CREDENTIAL
    assert result.credential_address == _address(LINKED_KEY)
    assert prompts == [
        "Nado linked signer private key (hidden; future explicit path): "
    ]
    assert paths["credential"].read_bytes() == bytes.fromhex(LINKED_KEY[2:])
    assert stat.S_IMODE(paths["credential"].stat().st_mode) == 0o600
    assert stat.S_IMODE(paths["identity"].stat().st_mode) == 0o600
    assert "credential" not in paths["identity"].read_text()
    assert MAIN_KEY not in repr(result)
    assert LINKED_KEY not in repr(result)
    assert LINKED_KEY not in result.evidence()
    discovered = onboarding.discover_protected_credential()
    assert discovered.identity == identity_result.identity
    assert discovered.credential_address == _address(LINKED_KEY)
    assert discovered.credential_fingerprint == hashlib.sha256(
        bytes.fromhex(LINKED_KEY[2:])
    ).hexdigest()
    assert onboarding.discover_public_identity() == identity_result.identity


def test_linked_signer_generation_is_local_and_has_no_write_authority(
    tmp_path: Path, monkeypatch
) -> None:
    identity_result = _provision_hidden(tmp_path, monkeypatch)
    generated = bytes.fromhex(LINKED_KEY[2:])
    monkeypatch.setattr(onboarding.os, "urandom", lambda size: generated)
    result = onboarding.provision_nado_mainnet_linked_signer(generate=True)
    assert result.status == onboarding.LINKED_SIGNER_PROVISIONED
    assert result.identity == identity_result.identity
    assert result.mainnet_write_authority == onboarding.NO_MAINNET_WRITE_AUTHORITY
    assert result.write_ready is False
    assert onboarding.protected_paths()["credential"].read_bytes() == generated
    assert "http" not in result.evidence().lower()


def test_main_wallet_cannot_be_persisted_as_linked_signer_or_fallback(
    tmp_path: Path, monkeypatch
) -> None:
    _provision_hidden(tmp_path, monkeypatch)
    result = onboarding.provision_nado_linked_signer(lambda _prompt: MAIN_KEY)
    assert result.status == onboarding.BLOCKED
    assert result.reason == onboarding.MAIN_WALLET_PERSISTENCE_FORBIDDEN
    assert not onboarding.protected_paths()["credential"].exists()

    legacy = onboarding.provision_nado_mainnet_credential(
        lambda _prompt: MAIN_KEY,
        fallback_proof=object(),
    )
    assert legacy.status == onboarding.BLOCKED
    assert legacy.reason == onboarding.MAIN_WALLET_PERSISTENCE_FORBIDDEN


def test_legacy_credential_name_now_has_read_only_identity_semantics(
    tmp_path: Path, monkeypatch
) -> None:
    _set_directory(tmp_path, monkeypatch)
    prompts: list[str] = []
    result = onboarding.provision_nado_mainnet_credential(
        lambda prompt: (prompts.append(prompt) or MAIN_KEY)
    )
    assert result.status == onboarding.PROVISIONED
    assert prompts == ["Nado main wallet private key (hidden): "]
    assert not onboarding.protected_paths()["credential"].exists()


def test_existing_paths_are_never_overwritten_or_reprompted(
    tmp_path: Path, monkeypatch
) -> None:
    _provision_hidden(tmp_path, monkeypatch)
    path = onboarding.protected_paths()["identity"]
    before = path.read_bytes()
    calls = 0

    def should_not_prompt(_prompt: str) -> str:
        nonlocal calls
        calls += 1
        raise AssertionError("prompted after fixed path existed")

    result = onboarding.provision_nado_mainnet_identity(should_not_prompt)
    assert result.status == onboarding.BLOCKED
    assert result.reason == "PROTECTED_PATH_ALREADY_EXISTS"
    assert calls == 0
    assert path.read_bytes() == before


def test_partial_identity_write_rolls_back_and_redacts_exception(
    tmp_path: Path, monkeypatch
) -> None:
    _set_directory(tmp_path, monkeypatch)
    original = onboarding._write_new_file

    def fail_identity(directory_fd: int, filename: str, payload: bytearray) -> None:
        if filename == onboarding.IDENTITY_FILENAME:
            raise RuntimeError("RAW_PARTIAL_SECRET")
        original(directory_fd, filename, payload)

    monkeypatch.setattr(onboarding, "_write_new_file", fail_identity)
    result = onboarding.provision_nado_mainnet_identity(lambda _prompt: MAIN_KEY)
    assert result.status == onboarding.BLOCKED
    assert result.reason == "PROTECTED_ONBOARDING_FAILED"
    assert "RAW_PARTIAL_SECRET" not in repr(result)
    assert "RAW_PARTIAL_SECRET" not in result.evidence()
    assert not onboarding.protected_paths()["identity"].exists()
    assert not onboarding.protected_paths()["credential"].exists()


def test_partial_linked_signer_write_rolls_back_only_credential(
    tmp_path: Path, monkeypatch
) -> None:
    _provision_hidden(tmp_path, monkeypatch)

    def fail_credential(_directory_fd: int, filename: str, _payload: bytearray) -> None:
        assert filename == onboarding.CREDENTIAL_FILENAME
        raise RuntimeError("RAW_LINKED_SECRET")

    monkeypatch.setattr(onboarding, "_write_new_file", fail_credential)
    result = onboarding.provision_nado_linked_signer(lambda _prompt: LINKED_KEY)
    assert result.status == onboarding.BLOCKED
    assert result.reason == "PROTECTED_ONBOARDING_FAILED"
    assert "RAW_LINKED_SECRET" not in repr(result)
    assert onboarding.protected_paths()["identity"].exists()
    assert not onboarding.protected_paths()["credential"].exists()


@pytest.mark.parametrize(
    "exception", [KeyboardInterrupt(), EOFError(), RuntimeError("RAW_SECRET")]
)
def test_prompt_cancellation_is_sanitized_and_does_not_create_paths(
    tmp_path: Path, monkeypatch, exception: BaseException
) -> None:
    directory = _set_directory(tmp_path, monkeypatch)

    def cancelled(_prompt: str) -> str:
        raise exception

    result = onboarding.provision_nado_mainnet_identity(cancelled)
    assert result.status == onboarding.BLOCKED
    assert result.reason == "PROTECTED_INPUT_CANCELLED"
    assert "RAW_SECRET" not in repr(result)
    assert "RAW_SECRET" not in result.evidence()
    assert not directory.exists()


def test_unsafe_directory_mode_and_symlink_fail_before_prompt(
    tmp_path: Path, monkeypatch
) -> None:
    directory = _set_directory(tmp_path, monkeypatch)
    directory.mkdir(mode=0o755)
    os.chmod(directory, 0o755)
    inspected = onboarding.inspect_protected_files()
    assert inspected.identity.reason == "PROTECTED_DIRECTORY_MODE_NOT_0700"
    called = False

    def no_prompt(_prompt: str) -> str:
        nonlocal called
        called = True
        return MAIN_KEY

    result = onboarding.provision_nado_mainnet_identity(no_prompt)
    assert result.status == onboarding.BLOCKED
    assert result.reason == "PROTECTED_DIRECTORY_MODE_NOT_0700"
    assert called is False

    directory.rmdir()
    target = tmp_path / "target"
    target.mkdir(mode=0o700)
    directory.symlink_to(target, target_is_directory=True)
    inspected = onboarding.inspect_protected_files()
    assert inspected.identity.reason == "PROTECTED_DIRECTORY_SYMLINK"
    result = onboarding.provision_nado_mainnet_identity(no_prompt)
    assert result.status == onboarding.BLOCKED
    assert result.reason == "PROTECTED_DIRECTORY_SYMLINK"
    assert called is False


def test_existing_parent_symlink_is_rejected_before_hidden_input(
    tmp_path: Path, monkeypatch
) -> None:
    real_parent = tmp_path / "real-parent"
    real_directory = real_parent / "nado-mainnet-onboarding"
    real_directory.mkdir(mode=0o700, parents=True)
    alias = tmp_path / "parent-alias"
    alias.symlink_to(real_parent, target_is_directory=True)
    monkeypatch.setattr(
        onboarding,
        "PROTECTED_DIRECTORY",
        alias / "nado-mainnet-onboarding",
    )
    inspected = onboarding.inspect_protected_files()
    assert inspected.identity.reason == "PROTECTED_DIRECTORY_SYMLINK"
    called = False

    def no_prompt(_prompt: str) -> str:
        nonlocal called
        called = True
        return MAIN_KEY

    result = onboarding.provision_nado_mainnet_identity(no_prompt)
    assert result.status == onboarding.BLOCKED
    assert result.reason == "PROTECTED_DIRECTORY_SYMLINK"
    assert called is False


@pytest.mark.parametrize("missing_flag", ("O_DIRECTORY", "O_NOFOLLOW"))
def test_required_directory_flags_fail_closed_before_hidden_input(
    tmp_path: Path, monkeypatch, missing_flag: str
) -> None:
    _set_directory(tmp_path, monkeypatch)
    monkeypatch.delattr(onboarding.os, missing_flag, raising=False)
    inspected = onboarding.inspect_protected_files()
    assert inspected.identity.reason == "PROTECTED_DIRECTORY_FEATURE_UNAVAILABLE"
    called = False

    def no_prompt(_prompt: str) -> str:
        nonlocal called
        called = True
        return MAIN_KEY

    result = onboarding.provision_nado_mainnet_identity(no_prompt)
    assert result.status == onboarding.BLOCKED
    assert result.reason == "PROTECTED_DIRECTORY_FEATURE_UNAVAILABLE"
    assert called is False


def test_foreign_owned_intermediate_directory_is_rejected(
    tmp_path: Path, monkeypatch
) -> None:
    _set_directory(tmp_path, monkeypatch)
    real_fstat = onboarding.os.fstat
    current_uid = os.getuid()
    foreign_uid = current_uid + 100000
    fstat_calls = 0

    def foreign_intermediate_fstat(fd: int):
        nonlocal fstat_calls
        info = real_fstat(fd)
        fstat_calls += 1
        if fstat_calls == 1:
            return SimpleNamespace(st_mode=info.st_mode, st_uid=foreign_uid)
        return info

    monkeypatch.setattr(onboarding.os, "fstat", foreign_intermediate_fstat)
    inspected = onboarding.inspect_protected_files()
    assert inspected.identity.reason == "PROTECTED_DIRECTORY_OWNER_NOT_CURRENT_USER"
    assert fstat_calls == 1


def test_provision_binds_to_opened_fd_after_parent_substitution(
    tmp_path: Path, monkeypatch
) -> None:
    parent = tmp_path / "nado-parent"
    directory = parent / "nado-mainnet-onboarding"
    directory.mkdir(mode=0o700, parents=True)
    os.chmod(directory, 0o700)
    monkeypatch.setattr(onboarding, "PROTECTED_DIRECTORY", directory)
    original_inspect = onboarding._inspect_protected_files_fd
    substituted = False

    def inspect_and_substitute(directory_fd: int) -> onboarding.ProtectedFiles:
        nonlocal substituted
        files = original_inspect(directory_fd)
        if not substituted:
            moved_parent = tmp_path / "nado-parent-original"
            parent.rename(moved_parent)
            parent.mkdir(mode=0o700)
            replacement = parent / "nado-mainnet-onboarding"
            replacement.mkdir(mode=0o700)
            os.chmod(replacement, 0o700)
            substituted = True
        return files

    monkeypatch.setattr(onboarding, "_inspect_protected_files_fd", inspect_and_substitute)
    result = onboarding.provision_nado_mainnet_identity(lambda _prompt: MAIN_KEY)
    original_directory = tmp_path / "nado-parent-original" / "nado-mainnet-onboarding"
    replacement_directory = parent / "nado-mainnet-onboarding"
    assert result.status == onboarding.PROVISIONED
    assert substituted is True
    assert (original_directory / onboarding.IDENTITY_FILENAME).is_file()
    assert not (replacement_directory / onboarding.IDENTITY_FILENAME).exists()


def test_missing_components_are_created_relative_to_trusted_directory_fds(
    tmp_path: Path, monkeypatch
) -> None:
    directory = _set_directory(tmp_path / "missing-parent", monkeypatch)
    real_mkdir = onboarding.os.mkdir
    calls: list[tuple[object, int | None]] = []

    def relative_mkdir(path: object, mode: int, *, dir_fd: int | None = None) -> None:
        calls.append((path, dir_fd))
        assert dir_fd is not None
        assert isinstance(path, str)
        assert not os.path.isabs(path)
        real_mkdir(path, mode, dir_fd=dir_fd)

    monkeypatch.setattr(onboarding.os, "mkdir", relative_mkdir)
    result = onboarding.provision_nado_mainnet_identity(lambda _prompt: MAIN_KEY)
    assert result.status == onboarding.PROVISIONED
    assert calls
    assert all(dir_fd is not None for _path, dir_fd in calls)
    assert all(
        isinstance(path, str) and not os.path.isabs(path)
        for path, _dir_fd in calls
    )
    assert directory.is_dir()


def test_relative_and_foreign_directory_paths_fail_closed(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(onboarding, "PROTECTED_DIRECTORY", Path("relative-onboarding"))
    inspected = onboarding.inspect_protected_files()
    assert inspected.identity.reason == "PROTECTED_DIRECTORY_NOT_ABSOLUTE"
    result = onboarding.provision_nado_mainnet_identity(lambda _prompt: MAIN_KEY)
    assert result.status == onboarding.BLOCKED
    assert result.reason == "PROTECTED_DIRECTORY_NOT_ABSOLUTE"

    directory = _set_directory(tmp_path / "foreign", monkeypatch)
    directory.parent.mkdir(mode=0o700)
    directory.mkdir(mode=0o700)
    os.chmod(directory, 0o700)
    current_uid = os.getuid()
    monkeypatch.setattr(onboarding.os, "getuid", lambda: current_uid + 100000)
    result = onboarding.provision_nado_mainnet_identity(lambda _prompt: MAIN_KEY)
    assert result.status == onboarding.BLOCKED
    assert result.reason == "PROTECTED_DIRECTORY_OWNER_NOT_CURRENT_USER"
    assert directory.exists()
    assert not onboarding.protected_paths()["identity"].exists()


def test_identity_file_mode_owner_symlink_and_hardlink_are_rejected(
    tmp_path: Path, monkeypatch
) -> None:
    _provision_hidden(tmp_path, monkeypatch)
    path = onboarding.protected_paths()["identity"]
    os.chmod(path, 0o644)
    inspected = onboarding.inspect_protected_files()
    assert inspected.identity.reason == "PROTECTED_FILE_MODE_NOT_0600"
    with pytest.raises(onboarding.OnboardingViolation, match="PROTECTED_FILE_MODE_NOT_0600"):
        onboarding.discover_public_identity()

    os.chmod(path, 0o600)
    replacement = tmp_path / "replacement"
    replacement.write_bytes(b"synthetic")
    path.unlink()
    path.symlink_to(replacement)
    inspected = onboarding.inspect_protected_files()
    assert inspected.identity.reason == "PROTECTED_FILE_SYMLINK"
    with pytest.raises(onboarding.OnboardingViolation, match="PROTECTED_FILE_SYMLINK"):
        onboarding.discover_public_identity()

    path.unlink()
    hardlink_source = tmp_path / "hardlink-source"
    hardlink_source.write_bytes(b"synthetic")
    os.chmod(hardlink_source, 0o600)
    os.link(hardlink_source, path)
    inspected = onboarding.inspect_protected_files()
    assert inspected.identity.reason == "PROTECTED_FILE_HARDLINK"
    with pytest.raises(onboarding.OnboardingViolation, match="PROTECTED_FILE_HARDLINK"):
        onboarding.discover_public_identity()


def test_credential_file_integrity_is_checked_only_by_explicit_phase(
    tmp_path: Path, monkeypatch
) -> None:
    _provision_hidden(tmp_path, monkeypatch)
    signer = onboarding.provision_nado_linked_signer(lambda _prompt: LINKED_KEY)
    assert signer.provisioned
    path = onboarding.protected_paths()["credential"]
    os.chmod(path, 0o644)
    inspected = onboarding.inspect_protected_files()
    assert inspected.credential.reason == "FUTURE_PHASE_NOT_INSPECTED"
    with pytest.raises(onboarding.OnboardingViolation, match="PROTECTED_FILE_MODE_NOT_0600"):
        onboarding.discover_protected_credential()


def test_persisted_identity_or_credential_conflicts_fail_closed(
    tmp_path: Path, monkeypatch
) -> None:
    _provision_hidden(tmp_path, monkeypatch)
    paths = onboarding.protected_paths()
    metadata = json.loads(paths["identity"].read_text())
    metadata["subaccount"] = "0x" + "44" * 32
    paths["identity"].write_text(
        json.dumps(metadata, sort_keys=True, separators=(",", ":"))
    )
    os.chmod(paths["identity"], 0o600)
    with pytest.raises(onboarding.OnboardingViolation, match="SUBACCOUNT_IDENTITY_CONFLICT"):
        onboarding.discover_public_identity()

    _provision_hidden(tmp_path / "credential", monkeypatch)
    onboarding.provision_nado_linked_signer(lambda _prompt: LINKED_KEY)
    paths = onboarding.protected_paths()
    paths["credential"].write_bytes(bytes.fromhex(OTHER_KEY[2:]))
    os.chmod(paths["credential"], 0o600)
    with pytest.raises(onboarding.OnboardingViolation, match="PERSISTED_CREDENTIAL_IDENTITY_CONFLICT"):
        onboarding.discover_protected_credential()


def test_legacy_credential_entry_point_rejects_linked_arguments_without_prompt(
    tmp_path: Path, monkeypatch
) -> None:
    _set_directory(tmp_path, monkeypatch)
    called = False

    def no_prompt(_prompt: str) -> str:
        nonlocal called
        called = True
        return MAIN_KEY

    result = onboarding.provision_nado_mainnet_credential(
        no_prompt,
        expected_linked_signer_address=_address(LINKED_KEY),
    )
    assert result.status == onboarding.BLOCKED
    assert result.reason == onboarding.LINKED_SIGNER_PROVISIONING_SEPARATE
    assert called is False
    assert not onboarding.protected_paths()["identity"].exists()


def test_unexpected_derivation_exception_is_sanitized(tmp_path: Path, monkeypatch) -> None:
    _set_directory(tmp_path, monkeypatch)

    def fail(_key: bytearray) -> str:
        raise RuntimeError("RAW_SECRET_DERIVATION")

    monkeypatch.setattr(onboarding, "_derive_wallet_address", fail)
    result = onboarding.provision_nado_mainnet_identity(lambda _prompt: MAIN_KEY)
    assert result.status == onboarding.BLOCKED
    assert result.reason == "PROTECTED_ONBOARDING_FAILED"
    assert "RAW_SECRET_DERIVATION" not in repr(result)
    assert "RAW_SECRET_DERIVATION" not in result.evidence()


def test_normal_cli_import_does_not_import_onboarding_module() -> None:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT / "src")
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import risex_farmer.cli; print('risex_farmer.nado_mainnet_onboarding' in sys.modules)",
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=True,
    )
    assert completed.stdout.strip() == "False"


def test_explicit_operator_main_uses_only_one_hidden_input_and_redacted_output(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    _set_directory(tmp_path, monkeypatch)
    monkeypatch.setattr(onboarding.getpass, "getpass", lambda _prompt: MAIN_KEY)
    assert onboarding.main() == 0
    output = capsys.readouterr().out
    assert MAIN_KEY not in output
    assert LINKED_KEY not in output
    parsed = json.loads(output)
    assert parsed["write_ready"] is False
    assert parsed["credential_kind"] is None
    assert parsed["files"][1]["present"] is False


def test_onboarding_module_has_only_local_derivation_and_filesystem_surface() -> None:
    source_path = ROOT / "src/risex_farmer/nado_mainnet_onboarding.py"
    source = source_path.read_text()
    for forbidden in (
        "aiohttp",
        "requests",
        "sqlite3",
        "nado_protocol",
        "urllib",
        "socket",
        "http://",
        "https://",
        "ws://",
        "wss://",
    ):
        assert forbidden not in source
    assert not re.search(r"\b(?:POST|PUT|DELETE|PATCH)\b", source)
    assert not re.search(r"\.(?:post|put|delete|patch)\s*\(", source)
    tree = ast.parse(source)
    imported_roots: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.extend(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.append(node.module.split(".")[0])
    assert set(imported_roots) <= {
        "dataclasses",
        "eth_account",
        "getpass",
        "hashlib",
        "json",
        "os",
        "pathlib",
        "stat",
        "typing",
        "__future__",
    }
    imported_nodes = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == "eth_account"
    ]
    assert len(imported_nodes) == 1
    parent_map = {
        child: parent
        for parent in ast.walk(tree)
        for child in ast.iter_child_nodes(parent)
    }
    ancestor = parent_map[imported_nodes[0]]
    inside_function = False
    while ancestor is not None:
        if isinstance(ancestor, (ast.FunctionDef, ast.AsyncFunctionDef)):
            inside_function = True
            break
        ancestor = parent_map.get(ancestor)
    assert inside_function
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT / "src")
    completed = subprocess.run(
        [
            sys.executable,
            "-S",
            "-c",
            "import sys; import risex_farmer.nado_mainnet_onboarding; print('eth_account' in sys.modules)",
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=True,
    )
    assert completed.stdout.strip() == "False"
