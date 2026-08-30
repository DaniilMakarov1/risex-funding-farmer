import hashlib
import ast
import json
import os
from pathlib import Path
import stat
import subprocess
import sys

import pytest

from risex_farmer import extended_mainnet_credential_onboarding as onboarding


PUBLIC_KEY = int("abc123", 16)
ACCOUNT_ID = "1001"
ACCOUNT_INDEX = "0"
L2_KEY = "0xabc123"
L2_VAULT = "321"
API_KEY = "synthetic-api-key-only"
STARK_PRIVATE_KEY = "0x1234"


def _configure(tmp_path, monkeypatch):
    directory = tmp_path / "config" / "risex-farmer" / "extended-mainnet-credentials"
    monkeypatch.setattr(onboarding, "PROTECTED_DIRECTORY", directory)
    monkeypatch.setattr(
        onboarding, "_derive_stark_public_key", lambda scalar: PUBLIC_KEY
    )
    return directory


def _values(**changes):
    values = {
        "account_id": ACCOUNT_ID,
        "account_index": ACCOUNT_INDEX,
        "l2_key": L2_KEY,
        "l2_vault": L2_VAULT,
        "api_key": API_KEY,
        "stark_private_key": STARK_PRIVATE_KEY,
    }
    values.update(changes)
    return [values[key] for key in values]


def _input(values, prompts=None):
    iterator = iter(values)

    def read(prompt):
        if prompts is not None:
            prompts.append(prompt)
        return next(iterator)

    return read


def _protected_directory(directory):
    directory.mkdir(parents=True, mode=0o700)
    directory.chmod(0o700)
    return directory


def _file(path, value=b"synthetic"):
    path.write_bytes(value)
    path.chmod(0o600)
    return path


def test_provision_persists_exact_identity_separate_credentials_and_sanitized_metadata(
    tmp_path, monkeypatch
):
    directory = _configure(tmp_path, monkeypatch)
    result = onboarding.provision_protected_credentials(_input(_values()))

    assert result.status == onboarding.PROVISIONED
    assert result.provisioned
    assert result.inspection.ready
    assert stat.S_IMODE(os.lstat(directory).st_mode) == 0o700
    assert os.lstat(directory).st_uid == os.getuid()
    assert set(path.name for path in directory.iterdir()) == {
        onboarding.IDENTITY_FILENAME,
        onboarding.API_KEY_FILENAME,
        onboarding.STARK_PRIVATE_KEY_FILENAME,
    }
    for path in directory.iterdir():
        details = os.lstat(path)
        assert stat.S_ISREG(details.st_mode)
        assert stat.S_IMODE(details.st_mode) == 0o600
        assert details.st_uid == os.getuid()
        assert details.st_nlink == 1

    metadata = json.loads(
        (directory / onboarding.IDENTITY_FILENAME).read_text(encoding="utf-8")
    )
    assert metadata["venue"] == "Extended"
    assert metadata["environment"] == "MAINNET"
    assert metadata["identity"] == {
        "account_id": 1001,
        "account_index": 0,
        "l2_key": L2_KEY,
        "l2_vault": 321,
    }
    assert metadata["credential_contract"] == {
        "api_key": "READ_ONLY_X_API_KEY",
        "stark_private_key": "WRITE_STARK_SIGNATURE_ONLY",
    }
    assert metadata["api_key_fingerprint"] == hashlib.sha256(
        API_KEY.encode("ascii")
    ).hexdigest()
    assert metadata["stark_private_key_fingerprint"] == hashlib.sha256(
        STARK_PRIVATE_KEY.encode("ascii")
    ).hexdigest()
    assert API_KEY not in (directory / onboarding.IDENTITY_FILENAME).read_text()
    assert STARK_PRIVATE_KEY not in (directory / onboarding.IDENTITY_FILENAME).read_text()
    assert API_KEY not in result.evidence()
    assert STARK_PRIVATE_KEY not in result.evidence()


def test_discovery_survives_restart_and_closes_zeroized_handle(tmp_path, monkeypatch):
    directory = _configure(tmp_path, monkeypatch)
    onboarding.provision_protected_credentials(_input(_values()))

    # A fresh discovery reads the fixed files rather than process-local state.
    inspection = onboarding.inspect_protected_credentials()
    assert inspection.ready
    assert inspection.identity == onboarding.ExtendedPublicIdentity(
        account_id=1001, account_index=0, l2_key=L2_KEY, l2_vault=321
    )
    with onboarding.discover_protected_credentials() as credentials:
        assert credentials.identity == inspection.identity
        assert credentials.api_key() == API_KEY
        assert credentials.stark_private_key() == STARK_PRIVATE_KEY
        assert API_KEY not in repr(credentials)
        assert STARK_PRIVATE_KEY not in repr(credentials)
    assert credentials.closed
    assert credentials._api_key == bytearray()
    assert credentials._stark_private_key == bytearray()
    with pytest.raises(onboarding.CredentialOnboardingError) as error:
        credentials.api_key()
    assert str(error.value) == "CREDENTIAL_HANDLE_CLOSED"
    assert API_KEY not in str(error.value)
    assert STARK_PRIVATE_KEY not in str(error.value)


def test_metadata_inspection_never_reads_secret_file_bytes(tmp_path, monkeypatch):
    _configure(tmp_path, monkeypatch)
    onboarding.provision_protected_credentials(_input(_values()))

    def forbidden(*args, **kwargs):
        raise AssertionError("secret file reader used by metadata inspection")

    monkeypatch.setattr(onboarding, "_read_secret_file", forbidden)
    inspection = onboarding.inspect_protected_credentials()
    assert inspection.ready
    assert inspection.api_key_fingerprint
    assert inspection.stark_private_key_fingerprint


def test_prompt_cancellation_is_sanitized_and_writes_nothing(tmp_path, monkeypatch):
    directory = _configure(tmp_path, monkeypatch)
    calls = []

    def cancelled(prompt):
        calls.append(prompt)
        if len(calls) == 4:
            raise KeyboardInterrupt("synthetic secret must not escape")
        return [ACCOUNT_ID, ACCOUNT_INDEX, L2_KEY][len(calls) - 1]

    result = onboarding.provision_protected_credentials(cancelled)
    assert result.status == onboarding.BLOCKED
    assert result.reason == "INPUT_CANCELLED"
    assert len(calls) == 4
    assert not directory.exists()
    assert "synthetic secret" not in result.evidence()


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("account_id", "", "ACCOUNT_ID_INVALID"),
        ("account_index", "01", "ACCOUNT_INDEX_INVALID"),
        ("l2_key", "not-hex", "L2_KEY_INVALID"),
        ("l2_vault", "-1", "L2_VAULT_INVALID"),
        ("api_key", " ", "API_KEY_INVALID"),
        ("stark_private_key", "not-a-stark-key", "STARK_PRIVATE_KEY_INVALID"),
        ("account_id", None, "INPUT_INVALID"),
    ],
)
def test_malformed_or_missing_inputs_fail_closed(tmp_path, monkeypatch, field, value, reason):
    directory = _configure(tmp_path, monkeypatch)
    result = onboarding.provision_protected_credentials(
        _input(_values(**{field: value}))
    )
    assert result.status == onboarding.BLOCKED
    assert result.reason == reason
    assert not directory.exists()
    assert value not in result.evidence() if value else True


def test_identical_credentials_are_rejected_before_key_derivation(tmp_path, monkeypatch):
    directory = _configure(tmp_path, monkeypatch)
    derived = []
    monkeypatch.setattr(
        onboarding, "_derive_stark_public_key", lambda scalar: derived.append(scalar)
    )
    result = onboarding.provision_protected_credentials(
        _input(_values(api_key=STARK_PRIVATE_KEY))
    )
    assert result.status == onboarding.BLOCKED
    assert result.reason == "CREDENTIALS_NOT_DISTINCT"
    assert derived == []
    assert not directory.exists()


def test_private_key_must_bind_to_public_identity(tmp_path, monkeypatch):
    directory = _configure(tmp_path, monkeypatch)
    monkeypatch.setattr(onboarding, "_derive_stark_public_key", lambda scalar: 999)
    result = onboarding.provision_protected_credentials(_input(_values()))
    assert result.status == onboarding.BLOCKED
    assert result.reason == "STARK_PUBLIC_IDENTITY_MISMATCH"
    assert not directory.exists()


def test_missing_official_sdk_blocks_before_persistence(tmp_path, monkeypatch):
    directory = _configure(tmp_path, monkeypatch)
    monkeypatch.setattr(
        onboarding,
        "_derive_stark_public_key",
        lambda scalar: (_ for _ in ()).throw(
            onboarding.CredentialOnboardingError("OFFICIAL_SDK_UNAVAILABLE")
        ),
    )
    result = onboarding.provision_protected_credentials(_input(_values()))
    assert result.status == onboarding.BLOCKED
    assert result.reason == "OFFICIAL_SDK_UNAVAILABLE"
    assert not directory.exists()


@pytest.mark.parametrize("defect", ["symlink", "regular_file", "mode", "owner"])
def test_bad_fixed_directory_is_rejected_without_prompt(
    tmp_path, monkeypatch, defect
):
    directory = _configure(tmp_path, monkeypatch)
    if defect == "symlink":
        target = tmp_path / "outside"
        target.mkdir()
        directory.parent.mkdir(parents=True)
        directory.symlink_to(target, target_is_directory=True)
    elif defect == "regular_file":
        directory.parent.mkdir(parents=True)
        directory.write_text("synthetic")
    else:
        _protected_directory(directory)
        if defect == "mode":
            directory.chmod(0o755)
        else:
            real_uid = os.getuid()
            parent_checks = len(onboarding._path_components(directory)) - 1
            calls = 0

            def wrong_final_owner():
                nonlocal calls
                calls += 1
                return real_uid if calls <= parent_checks else real_uid + 1

            monkeypatch.setattr(onboarding.os, "getuid", wrong_final_owner)

    prompts = []
    result = onboarding.provision_protected_credentials(_input(_values(), prompts))
    assert result.status == onboarding.BLOCKED
    assert not prompts
    assert result.reason in {
        "PROTECTED_DIRECTORY_SYMLINK",
        "PROTECTED_DIRECTORY_NOT_DIRECTORY",
        "PROTECTED_DIRECTORY_MODE_NOT_0700",
        "PROTECTED_DIRECTORY_OWNER_NOT_CURRENT_USER",
    }


@pytest.mark.parametrize("defect", ["symlink", "hardlink", "mode", "owner"])
def test_metadata_inspection_reports_file_safety_without_secret_reads(
    tmp_path, monkeypatch, defect
):
    directory = _configure(tmp_path, monkeypatch)
    _protected_directory(directory)
    identity = _file(directory / onboarding.IDENTITY_FILENAME, b"{}").resolve()
    api_key = _file(directory / onboarding.API_KEY_FILENAME, API_KEY.encode())
    stark = _file(
        directory / onboarding.STARK_PRIVATE_KEY_FILENAME,
        STARK_PRIVATE_KEY.encode(),
    )
    if defect == "symlink":
        api_key.unlink()
        api_key.symlink_to(identity)
    elif defect == "hardlink":
        api_key.unlink()
        os.link(stark, api_key)
    elif defect == "mode":
        api_key.chmod(0o644)
    else:
        real_stat = onboarding.os.stat
        bad_uid = os.getuid() + 1

        class FakeStat:
            def __init__(self, info):
                self.st_mode = info.st_mode
                self.st_uid = bad_uid
                self.st_nlink = info.st_nlink
                self.st_size = info.st_size

        def stat_with_wrong_api_owner(path, *args, **kwargs):
            info = real_stat(path, *args, **kwargs)
            if path == onboarding.API_KEY_FILENAME:
                return FakeStat(info)
            return info

        monkeypatch.setattr(onboarding.os, "stat", stat_with_wrong_api_owner)

    inspection = onboarding.inspect_protected_credentials()
    api_observation = next(
        item for item in inspection.files if item.name == onboarding.API_KEY_FILENAME
    )
    assert not inspection.ready
    assert api_observation.reason in {
        "PROTECTED_FILE_SYMLINK",
        "PROTECTED_FILE_HARDLINK",
        "PROTECTED_FILE_MODE_NOT_0600",
        "PROTECTED_FILE_OWNER_NOT_CURRENT_USER",
    }
    assert API_KEY not in inspection.evidence()
    assert STARK_PRIVATE_KEY not in inspection.evidence()


def test_parent_symlink_is_rejected_before_hidden_input(tmp_path, monkeypatch):
    directory = _configure(tmp_path, monkeypatch)
    parent_component = directory.parents[1]
    redirected = tmp_path / "redirected-parent"
    redirected.mkdir()
    parent_component.symlink_to(redirected, target_is_directory=True)

    prompts = []
    result = onboarding.provision_protected_credentials(_input(_values(), prompts))

    assert result.status == onboarding.BLOCKED
    assert result.reason == "PROTECTED_DIRECTORY_SYMLINK"
    assert prompts == []
    assert not (redirected / "risex-farmer").exists()


def test_retained_final_descriptor_survives_parent_swap(tmp_path, monkeypatch):
    directory = _configure(tmp_path, monkeypatch)
    assert onboarding.provision_protected_credentials(_input(_values())).provisioned
    descriptor = onboarding._open_directory()
    parent_component = directory.parents[1]
    moved_parent = tmp_path / "original-parent"
    redirected = tmp_path / "redirected-parent"
    redirected.mkdir()

    try:
        parent_component.rename(moved_parent)
        parent_component.symlink_to(redirected, target_is_directory=True)

        assert set(onboarding._directory_entries_from_fd(descriptor)) == {
            onboarding.IDENTITY_FILENAME,
            onboarding.API_KEY_FILENAME,
            onboarding.STARK_PRIVATE_KEY_FILENAME,
        }
        blocked = onboarding.inspect_protected_credentials()
        assert blocked.status == onboarding.INSPECTION_BLOCKED
        assert blocked.reason == "PROTECTED_DIRECTORY_SYMLINK"
    finally:
        if parent_component.is_symlink():
            parent_component.unlink()
        if moved_parent.exists():
            moved_parent.rename(parent_component)
        if redirected.exists():
            redirected.rmdir()
        os.close(descriptor)


def test_overwrite_is_rejected_before_any_new_prompt_or_write(tmp_path, monkeypatch):
    directory = _configure(tmp_path, monkeypatch)
    first = onboarding.provision_protected_credentials(_input(_values()))
    assert first.provisioned
    original = {
        path.name: path.read_bytes() for path in directory.iterdir()
    }
    prompts = []
    second = onboarding.provision_protected_credentials(_input(_values(), prompts))
    assert second.status == onboarding.BLOCKED
    assert second.reason == "PROTECTED_PATH_ALREADY_EXISTS"
    assert not prompts
    assert {path.name: path.read_bytes() for path in directory.iterdir()} == original


def test_partial_write_rolls_back_all_created_files_and_redacts_exception(
    tmp_path, monkeypatch
):
    directory = _configure(tmp_path, monkeypatch)
    original = onboarding._write_secure_file
    calls = []
    secret_in_error = "synthetic-write-exception-secret"

    def fail_on_second(fd, filename, payload, maximum):
        calls.append(filename)
        if len(calls) == 2:
            raise RuntimeError(secret_in_error)
        return original(fd, filename, payload, maximum)

    monkeypatch.setattr(onboarding, "_write_secure_file", fail_on_second)
    result = onboarding.provision_protected_credentials(_input(_values()))
    assert result.status == onboarding.BLOCKED
    assert result.reason == "PROTECTED_PROVISIONING_FAILED"
    assert calls == [onboarding.API_KEY_FILENAME, onboarding.STARK_PRIVATE_KEY_FILENAME]
    assert directory.exists()
    assert list(directory.iterdir()) == []
    assert secret_in_error not in result.evidence()
    assert secret_in_error not in repr(result)


def test_changed_secret_is_detected_on_restart_without_revealing_bytes(
    tmp_path, monkeypatch
):
    directory = _configure(tmp_path, monkeypatch)
    onboarding.provision_protected_credentials(_input(_values()))
    api_path = directory / onboarding.API_KEY_FILENAME
    api_path.write_bytes(b"different-synthetic-api-key")
    api_path.chmod(0o600)

    # Metadata inspection remains byte-free for secret files; discovery binds
    # the newly read bytes to the persisted fingerprint and rejects the change.
    assert onboarding.inspect_protected_credentials().ready
    with pytest.raises(onboarding.CredentialOnboardingError) as error:
        onboarding.discover_protected_credentials()
    assert str(error.value) == "API_KEY_FINGERPRINT_MISMATCH"
    assert API_KEY not in str(error.value)


def test_discovery_rejects_tampered_metadata_and_secret_links(tmp_path, monkeypatch):
    directory = _configure(tmp_path, monkeypatch)
    onboarding.provision_protected_credentials(_input(_values()))
    metadata_path = directory / onboarding.IDENTITY_FILENAME
    metadata = json.loads(metadata_path.read_text())
    metadata["credential_contract"]["api_key"] = "WRITE"
    metadata_path.write_text(json.dumps(metadata))
    metadata_path.chmod(0o600)
    assert not onboarding.inspect_protected_credentials().ready
    with pytest.raises(onboarding.CredentialOnboardingError) as error:
        onboarding.discover_protected_credentials()
    assert str(error.value) == "IDENTITY_METADATA_INVALID"


def test_cli_inspect_is_visible_safe_and_does_not_add_normal_cli_mode(
    tmp_path, monkeypatch, capsys
):
    _configure(tmp_path, monkeypatch)
    assert onboarding.main(["inspect"]) == 0
    output = capsys.readouterr().out
    assert "PROTECTED_FILES_MISSING" in output
    assert API_KEY not in output
    assert STARK_PRIVATE_KEY not in output

    result = onboarding.main(["provision", "--api-key", API_KEY])
    captured = capsys.readouterr()
    assert result == 2
    assert API_KEY not in captured.out
    assert API_KEY not in captured.err


def test_terminal_provision_uses_hidden_getpass_and_prints_only_sanitized_result(
    tmp_path, monkeypatch, capsys
):
    _configure(tmp_path, monkeypatch)
    prompts = []
    monkeypatch.setattr(
        onboarding.getpass,
        "getpass",
        _input(_values(), prompts),
    )
    assert onboarding.main(["provision"]) == 0
    output = capsys.readouterr().out
    assert len(prompts) == 6
    assert all("hidden" in prompt for prompt in prompts)
    assert "PROTECTED_FILES_CREATED" in output
    assert API_KEY not in output
    assert STARK_PRIVATE_KEY not in output


def test_missing_persisted_secret_fails_closed_on_inspection_and_discovery(
    tmp_path, monkeypatch
):
    directory = _configure(tmp_path, monkeypatch)
    onboarding.provision_protected_credentials(_input(_values()))
    (directory / onboarding.API_KEY_FILENAME).unlink()
    inspection = onboarding.inspect_protected_credentials()
    assert not inspection.ready
    assert inspection.reason == "PROTECTED_FILE_MISSING"
    with pytest.raises(onboarding.CredentialOnboardingError) as error:
        onboarding.discover_protected_credentials()
    assert str(error.value) == "PROTECTED_FILE_MISSING"


def test_normal_cli_import_does_not_load_onboarding_module():
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root / "src")
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import risex_farmer.cli; "
            "print('risex_farmer.extended_mainnet_credential_onboarding' in sys.modules)",
        ],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.stdout.strip() == "False"


def test_onboarding_module_has_no_runtime_or_write_surface_imports():
    tree = ast.parse(Path(onboarding.__file__).read_text(encoding="utf-8"))
    imports = {
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imports.update(
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
    )
    assert imports.isdisjoint({"aiohttp", "sqlite3", "websocket", "x10"})
    assert not any(
        isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and any(
            token in node.name.lower()
            for token in ("sign", "dispatch", "order", "withdraw", "transfer")
        )
        for node in ast.walk(tree)
    )
