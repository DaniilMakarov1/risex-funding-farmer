from __future__ import annotations

import ast
import hashlib
import importlib
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


def _set_directory(tmp_path: Path, monkeypatch) -> Path:
    directory = tmp_path / "nado-mainnet-onboarding"
    monkeypatch.setattr(onboarding, "PROTECTED_DIRECTORY", directory)
    return directory


def _provision(
    tmp_path: Path,
    monkeypatch,
    values: tuple[str, str] = (MAIN_KEY, LINKED_KEY),
    **kwargs: object,
) -> onboarding.OnboardingResult:
    _set_directory(tmp_path, monkeypatch)
    supplied = iter(values)
    return onboarding.provision_nado_mainnet_credential(
        lambda _prompt: next(supplied), **kwargs
    )


def _fallback_proof(**changes: object) -> onboarding.MainWalletFallbackProof:
    values: dict[str, object] = {
        "source": onboarding.OFFICIAL_FALLBACK_SOURCE,
        "lifecycle": onboarding.REQUIRED_LIFECYCLE,
        "linked_signer_supported": False,
        "linked_signer_satisfies_lifecycle": False,
        "authoritative": True,
        "reason_code": onboarding.OFFICIAL_FALLBACK_REASON,
    }
    values.update(changes)
    return onboarding.MainWalletFallbackProof(**values)


def test_derive_and_export_exact_public_wallet_bytes32_identity() -> None:
    identity = onboarding.derive_public_identity(MAIN_KEY)
    wallet = _address(MAIN_KEY)
    assert identity.wallet_address == wallet
    assert identity.subaccount == "0x" + (
        bytes.fromhex(wallet[2:]) + b"default".ljust(12, b"\0")
    ).hex()
    exported = onboarding.export_unsigned_read_identity(identity)
    assert exported["wallet_address"] == wallet
    assert exported["subaccount"] == identity.subaccount
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
    result = onboarding.provision_nado_mainnet_credential(
        lambda _prompt: value
    )
    assert result.status == onboarding.BLOCKED
    assert result.reason == "PROTECTED_INPUT_INVALID:main_wallet_key"
    assert not directory.exists()


@pytest.mark.parametrize(
    "value",
    [
        None,
        "0x" + "00" * 32,
        "0x" + "22" * 31,
        "0x" + "gg" * 32,
        "0x" + "22" * 32 + "x",
    ],
)
def test_malformed_linked_signer_keys_fail_closed_without_files(
    tmp_path: Path, monkeypatch, value: object
) -> None:
    directory = _set_directory(tmp_path, monkeypatch)
    values = iter((MAIN_KEY, value))
    result = onboarding.provision_nado_mainnet_credential(
        lambda _prompt: next(values)
    )
    assert result.status == onboarding.BLOCKED
    assert result.reason == "PROTECTED_INPUT_INVALID:linked_signer_key"
    assert not directory.exists()


@pytest.mark.parametrize("name", ["", "a" * 13, "bad name", "né", "bad\nname"])
def test_malformed_subaccount_names_fail_closed(tmp_path: Path, monkeypatch, name: str) -> None:
    _set_directory(tmp_path, monkeypatch)
    with pytest.raises(onboarding.OnboardingViolation, match="SUBACCOUNT_NAME_INVALID"):
        onboarding.derive_public_identity(MAIN_KEY, name)
    result = onboarding.provision_nado_mainnet_credential(
        lambda _prompt: MAIN_KEY, name
    )
    assert result.status == onboarding.BLOCKED
    assert result.reason == "SUBACCOUNT_NAME_INVALID"


def test_linked_signer_is_persisted_but_main_wallet_is_not(tmp_path, monkeypatch) -> None:
    result = _provision(tmp_path, monkeypatch)
    paths = onboarding.protected_paths()
    assert result.status == onboarding.PROVISIONED
    assert result.identity is not None
    assert result.credential_kind == onboarding.LINKED_SIGNER_CREDENTIAL
    assert result.credential_address == _address(LINKED_KEY)
    assert result.files.all_protected
    assert stat.S_IMODE(paths["identity"].stat().st_mode) == 0o600
    assert stat.S_IMODE(paths["credential"].stat().st_mode) == 0o600
    assert paths["identity"].stat().st_nlink == 1
    assert paths["credential"].stat().st_nlink == 1
    assert paths["credential"].read_bytes() == bytes.fromhex(LINKED_KEY[2:])
    assert paths["credential"].read_bytes() != bytes.fromhex(MAIN_KEY[2:])
    metadata = paths["identity"].read_text()
    assert MAIN_KEY not in metadata
    assert LINKED_KEY not in metadata
    assert MAIN_KEY not in repr(result)
    assert LINKED_KEY not in repr(result)
    assert MAIN_KEY not in result.evidence()
    assert LINKED_KEY not in result.evidence()


def test_restart_discovery_validates_persisted_credential_and_public_export(
    tmp_path: Path, monkeypatch
) -> None:
    result = _provision(tmp_path, monkeypatch)
    assert result.identity is not None
    public = onboarding.discover_public_identity()
    discovered = onboarding.discover_protected_credential()
    assert public == result.identity
    assert discovered.identity == result.identity
    assert discovered.credential_kind == onboarding.LINKED_SIGNER_CREDENTIAL
    assert discovered.credential_address == _address(LINKED_KEY)
    assert discovered.credential_fingerprint == hashlib.sha256(
        bytes.fromhex(LINKED_KEY[2:])
    ).hexdigest()
    unsigned = onboarding.export_unsigned_read_identity(public)
    assert unsigned["subaccount"] == public.subaccount
    assert MAIN_KEY not in discovered.evidence()
    assert LINKED_KEY not in discovered.evidence()


def test_public_identity_discovery_does_not_require_secret_file(
    tmp_path: Path, monkeypatch
) -> None:
    _provision(tmp_path, monkeypatch)
    paths = onboarding.protected_paths()
    paths["credential"].unlink()
    identity = onboarding.discover_public_identity()
    assert identity.wallet_address == _address(MAIN_KEY)
    with pytest.raises(onboarding.OnboardingViolation, match="PROTECTED_FILE_UNAVAILABLE"):
        onboarding.discover_protected_credential()


@pytest.mark.parametrize(
    "kwargs,reason",
    [
        ({"expected_wallet_address": _address(OTHER_KEY)}, "MAIN_WALLET_IDENTITY_CONFLICT"),
        ({"expected_subaccount": "0x" + "44" * 32}, "SUBACCOUNT_IDENTITY_CONFLICT"),
        (
            {"expected_linked_signer_address": _address(OTHER_KEY)},
            "LINKED_SIGNER_IDENTITY_CONFLICT",
        ),
    ],
)
def test_expected_identity_bindings_reject_conflicts(
    tmp_path: Path, monkeypatch, kwargs: dict[str, str], reason: str
) -> None:
    result = _provision(tmp_path, monkeypatch, **kwargs)
    assert result.status == onboarding.BLOCKED
    assert result.reason == reason
    assert not onboarding.protected_paths()["identity"].exists()
    assert not onboarding.protected_paths()["credential"].exists()


def test_main_and_linked_identity_conflict_is_rejected(tmp_path: Path, monkeypatch) -> None:
    result = _provision(tmp_path, monkeypatch, values=(MAIN_KEY, MAIN_KEY))
    assert result.status == onboarding.BLOCKED
    assert result.reason == "MAIN_AND_LINKED_IDENTITY_CONFLICT"
    assert not onboarding.protected_paths()["identity"].exists()


@pytest.mark.parametrize("exception", [KeyboardInterrupt(), EOFError(), RuntimeError("RAW_SECRET")])
def test_prompt_cancellation_is_sanitized_and_does_not_create_paths(
    tmp_path: Path, monkeypatch, exception: BaseException
) -> None:
    directory = _set_directory(tmp_path, monkeypatch)

    def cancelled(_prompt: str) -> str:
        raise exception

    result = onboarding.provision_nado_mainnet_credential(cancelled)
    assert result.status == onboarding.BLOCKED
    assert result.reason == "PROTECTED_INPUT_CANCELLED"
    assert "RAW_SECRET" not in repr(result)
    assert "RAW_SECRET" not in result.evidence()
    assert not directory.exists()


def test_blank_linked_signer_requires_exact_fallback_proof(tmp_path: Path, monkeypatch) -> None:
    result = _provision(tmp_path, monkeypatch, values=(MAIN_KEY, ""))
    assert result.status == onboarding.BLOCKED
    assert result.reason == "MAIN_WALLET_FALLBACK_OFFICIAL_PROOF_REQUIRED"
    assert not onboarding.protected_paths()["identity"].exists()

    contradicted = _provision(
        tmp_path / "contradicted",
        monkeypatch,
        values=(MAIN_KEY, ""),
        fallback_proof=_fallback_proof(linked_signer_supported=True),
    )
    assert contradicted.status == onboarding.BLOCKED
    assert contradicted.reason == "MAIN_WALLET_FALLBACK_OFFICIAL_PROOF_REQUIRED"


def test_main_wallet_fallback_remains_forbidden_while_official_linked_signer_is_supported(
    tmp_path: Path, monkeypatch
) -> None:
    result = _provision(
        tmp_path,
        monkeypatch,
        values=(MAIN_KEY, ""),
        fallback_proof=_fallback_proof(),
    )
    assert result.status == onboarding.BLOCKED
    assert result.reason == "MAIN_WALLET_FALLBACK_FORBIDDEN_BY_CURRENT_OFFICIAL_CONTRACT"
    assert result.reason in result.evidence()
    assert not onboarding.protected_paths()["identity"].exists()
    assert not onboarding.protected_paths()["credential"].exists()


def test_linked_credential_wins_over_fallback_and_does_not_retain_main_key(
    tmp_path: Path, monkeypatch
) -> None:
    result = _provision(
        tmp_path,
        monkeypatch,
        fallback_proof=_fallback_proof(),
    )
    assert result.status == onboarding.PROVISIONED
    assert result.credential_kind == onboarding.LINKED_SIGNER_CREDENTIAL
    assert onboarding.protected_paths()["credential"].read_bytes() == bytes.fromhex(
        LINKED_KEY[2:]
    )


def test_existing_paths_are_never_overwritten_or_reprompted(tmp_path: Path, monkeypatch) -> None:
    _provision(tmp_path, monkeypatch)
    paths = onboarding.protected_paths()
    before_identity = paths["identity"].read_bytes()
    before_credential = paths["credential"].read_bytes()
    calls = 0

    def should_not_prompt(_prompt: str) -> str:
        nonlocal calls
        calls += 1
        raise AssertionError("prompted after fixed path existed")

    result = onboarding.provision_nado_mainnet_credential(should_not_prompt)
    assert result.status == onboarding.BLOCKED
    assert result.reason == "PROTECTED_PATH_ALREADY_EXISTS"
    assert calls == 0
    assert paths["identity"].read_bytes() == before_identity
    assert paths["credential"].read_bytes() == before_credential


def test_partial_write_rolls_back_all_created_files_and_redacts_exception(
    tmp_path: Path, monkeypatch
) -> None:
    _set_directory(tmp_path, monkeypatch)
    original = onboarding._write_new_file

    def fail_metadata(directory_fd: int, filename: str, payload: bytearray) -> None:
        if filename == onboarding.IDENTITY_FILENAME:
            raise RuntimeError("RAW_PARTIAL_SECRET")
        original(directory_fd, filename, payload)

    monkeypatch.setattr(onboarding, "_write_new_file", fail_metadata)
    result = onboarding.provision_nado_mainnet_credential(
        lambda prompt: MAIN_KEY if "main wallet" in prompt else LINKED_KEY
    )
    assert result.status == onboarding.BLOCKED
    assert result.reason == "PROTECTED_ONBOARDING_FAILED"
    assert "RAW_PARTIAL_SECRET" not in repr(result)
    assert "RAW_PARTIAL_SECRET" not in result.evidence()
    paths = onboarding.protected_paths()
    assert not paths["identity"].exists()
    assert not paths["credential"].exists()


def test_unsafe_directory_path_mode_and_symlink_fail_before_prompt(
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

    result = onboarding.provision_nado_mainnet_credential(no_prompt)
    assert result.status == onboarding.BLOCKED
    assert result.reason == "PROTECTED_DIRECTORY_MODE_NOT_0700"
    assert called is False

    directory.rmdir()
    target = tmp_path / "target"
    target.mkdir(mode=0o700)
    directory.symlink_to(target, target_is_directory=True)
    inspected = onboarding.inspect_protected_files()
    assert inspected.identity.reason == "PROTECTED_DIRECTORY_SYMLINK"
    result = onboarding.provision_nado_mainnet_credential(no_prompt)
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

    result = onboarding.provision_nado_mainnet_credential(no_prompt)
    assert result.status == onboarding.BLOCKED
    assert result.reason == "PROTECTED_DIRECTORY_SYMLINK"
    assert called is False


def test_relative_and_foreign_directory_paths_fail_closed(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(onboarding, "PROTECTED_DIRECTORY", Path("relative-onboarding"))
    inspected = onboarding.inspect_protected_files()
    assert inspected.identity.reason == "PROTECTED_DIRECTORY_NOT_ABSOLUTE"
    result = onboarding.provision_nado_mainnet_credential(lambda _prompt: MAIN_KEY)
    assert result.status == onboarding.BLOCKED
    assert result.reason == "PROTECTED_DIRECTORY_NOT_ABSOLUTE"

    directory = _set_directory(tmp_path / "foreign", monkeypatch)
    directory.parent.mkdir(mode=0o700)
    directory.mkdir(mode=0o700)
    os.chmod(directory, 0o700)
    current_uid = os.getuid()
    monkeypatch.setattr(onboarding.os, "getuid", lambda: current_uid + 100000)
    values = iter((MAIN_KEY, LINKED_KEY))
    result = onboarding.provision_nado_mainnet_credential(
        lambda _prompt: next(values)
    )
    assert result.status == onboarding.BLOCKED
    assert result.reason == "PROTECTED_DIRECTORY_OWNER_NOT_CURRENT_USER"
    assert directory.exists()
    assert not onboarding.protected_paths()["identity"].exists()
    assert not onboarding.protected_paths()["credential"].exists()


def test_file_mode_owner_symlink_and_hardlink_are_rejected(
    tmp_path: Path, monkeypatch
) -> None:
    _provision(tmp_path, monkeypatch)
    paths = onboarding.protected_paths()

    os.chmod(paths["credential"], 0o644)
    inspected = onboarding.inspect_protected_files()
    assert inspected.credential.reason == "PROTECTED_FILE_MODE_NOT_0600"
    with pytest.raises(onboarding.OnboardingViolation, match="PROTECTED_FILE_MODE_NOT_0600"):
        onboarding.discover_protected_credential()

    os.chmod(paths["credential"], 0o600)
    replacement = tmp_path / "replacement"
    replacement.write_bytes(b"synthetic")
    paths["credential"].unlink()
    paths["credential"].symlink_to(replacement)
    inspected = onboarding.inspect_protected_files()
    assert inspected.credential.reason == "PROTECTED_FILE_SYMLINK"
    with pytest.raises(onboarding.OnboardingViolation, match="PROTECTED_FILE_SYMLINK"):
        onboarding.discover_protected_credential()

    paths["credential"].unlink()
    hardlink_source = tmp_path / "hardlink-source"
    hardlink_source.write_bytes(bytes.fromhex(LINKED_KEY[2:]))
    os.chmod(hardlink_source, 0o600)
    os.link(hardlink_source, paths["credential"])
    inspected = onboarding.inspect_protected_files()
    assert inspected.credential.reason == "PROTECTED_FILE_HARDLINK"
    with pytest.raises(onboarding.OnboardingViolation, match="PROTECTED_FILE_HARDLINK"):
        onboarding.discover_protected_credential()


def test_file_owner_is_checked_independently_of_directory_owner(
    tmp_path: Path, monkeypatch
) -> None:
    _provision(tmp_path, monkeypatch)
    paths = onboarding.protected_paths()
    current_uid = os.getuid()
    foreign_uid = current_uid + 100000
    real_lstat = onboarding.os.lstat

    def foreign_file_lstat(path: Path):
        info = real_lstat(path)
        if Path(path) == paths["credential"]:
            return SimpleNamespace(
                st_mode=info.st_mode,
                st_nlink=info.st_nlink,
                st_size=info.st_size,
                st_uid=foreign_uid,
            )
        return info

    monkeypatch.setattr(onboarding.os, "lstat", foreign_file_lstat)
    inspected = onboarding.inspect_protected_files()
    assert inspected.credential.reason == "PROTECTED_FILE_OWNER_NOT_CURRENT_USER"


def test_persisted_identity_or_credential_conflicts_fail_closed(
    tmp_path: Path, monkeypatch
) -> None:
    _provision(tmp_path, monkeypatch)
    paths = onboarding.protected_paths()
    metadata = json.loads(paths["identity"].read_text())
    metadata["subaccount"] = "0x" + "44" * 32
    paths["identity"].write_text(
        json.dumps(metadata, sort_keys=True, separators=(",", ":"))
    )
    with pytest.raises(onboarding.OnboardingViolation, match="SUBACCOUNT_IDENTITY_CONFLICT"):
        onboarding.discover_public_identity()

    _provision(tmp_path / "credential", monkeypatch)
    paths = onboarding.protected_paths()
    paths["credential"].write_bytes(bytes.fromhex(OTHER_KEY[2:]))
    os.chmod(paths["credential"], 0o600)
    with pytest.raises(onboarding.OnboardingViolation, match="PERSISTED_CREDENTIAL_IDENTITY_CONFLICT"):
        onboarding.discover_protected_credential()


def test_unexpected_derivation_exception_is_sanitized(tmp_path: Path, monkeypatch) -> None:
    _set_directory(tmp_path, monkeypatch)

    def fail(_key: bytearray) -> str:
        raise RuntimeError("RAW_SECRET_DERIVATION")

    monkeypatch.setattr(onboarding, "_derive_wallet_address", fail)
    result = onboarding.provision_nado_mainnet_credential(
        lambda _prompt: MAIN_KEY
    )
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


def test_explicit_operator_main_uses_hidden_inputs_and_redacted_output(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    _set_directory(tmp_path, monkeypatch)
    values = iter((MAIN_KEY, LINKED_KEY))
    monkeypatch.setattr(onboarding.getpass, "getpass", lambda _prompt: next(values))
    assert onboarding.main() == 0
    output = capsys.readouterr().out
    assert MAIN_KEY not in output
    assert LINKED_KEY not in output
    assert json.loads(output)["write_ready"] is False


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
    assert dict(importlib.import_module("risex_farmer.nado_mainnet_onboarding").__dict__)[
        "main"
    ] is onboarding.main
