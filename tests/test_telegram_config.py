import json
import os
from pathlib import Path

import pytest

import risex_farmer.cli as cli_module
import risex_farmer.telegram_config as telegram_config
from risex_farmer.telegram_config import (
    BLOCKED,
    PROVISIONED,
    TelegramConfigurationError,
    load_protected_telegram_environment,
    main as provisioning_main,
    protected_telegram_environment,
    provision_protected_telegram,
)


TOKEN = "123456:synthetic-token"
CHAT_ID = "738925112"


def _write_config(
    directory: Path,
    *,
    token: str = TOKEN,
    chat_id: str = CHAT_ID,
) -> None:
    directory.mkdir(parents=True)
    directory.chmod(0o700)
    (directory / telegram_config.BOT_TOKEN_FILENAME).write_text(
        token,
        encoding="ascii",
    )
    (directory / telegram_config.CHAT_ID_FILENAME).write_text(
        chat_id,
        encoding="ascii",
    )
    (directory / telegram_config.BOT_TOKEN_FILENAME).chmod(0o600)
    (directory / telegram_config.CHAT_ID_FILENAME).chmod(0o600)


def _reason(callable_obj, *args, **kwargs) -> str:
    with pytest.raises(TelegramConfigurationError) as caught:
        callable_obj(*args, **kwargs)
    return caught.value.reason


def test_loader_exports_only_explicit_environment_values_and_context_restores(
    tmp_path,
    monkeypatch,
):
    directory = tmp_path / "telegram"
    monkeypatch.setattr(telegram_config, "PROTECTED_TELEGRAM_DIRECTORY", directory)
    _write_config(directory)
    environment = {
        "KEEP": "unchanged",
        telegram_config.RISEX_TELEGRAM_ENABLED: "false",
        telegram_config.RISEX_TELEGRAM_BOT_TOKEN: "old-token",
        telegram_config.RISEX_TELEGRAM_CHAT_ID: "old-chat",
    }

    load_protected_telegram_environment(environment)
    assert environment == {
        "KEEP": "unchanged",
        telegram_config.RISEX_TELEGRAM_ENABLED: "true",
        telegram_config.RISEX_TELEGRAM_BOT_TOKEN: TOKEN,
        telegram_config.RISEX_TELEGRAM_CHAT_ID: CHAT_ID,
    }

    with protected_telegram_environment(environment):
        assert environment[telegram_config.RISEX_TELEGRAM_BOT_TOKEN] == TOKEN
        assert environment[telegram_config.RISEX_TELEGRAM_CHAT_ID] == CHAT_ID
    assert environment[telegram_config.RISEX_TELEGRAM_ENABLED] == "true"
    assert environment[telegram_config.RISEX_TELEGRAM_BOT_TOKEN] == TOKEN


def test_context_restores_absent_environment_keys(tmp_path, monkeypatch):
    directory = tmp_path / "telegram"
    monkeypatch.setattr(telegram_config, "PROTECTED_TELEGRAM_DIRECTORY", directory)
    _write_config(directory)
    environment = {"KEEP": "unchanged"}
    with protected_telegram_environment(environment):
        assert environment[telegram_config.RISEX_TELEGRAM_ENABLED] == "true"
    assert environment == {"KEEP": "unchanged"}


def test_missing_directory_and_file_fail_closed_without_environment_mutation(
    tmp_path,
    monkeypatch,
):
    directory = tmp_path / "telegram"
    monkeypatch.setattr(telegram_config, "PROTECTED_TELEGRAM_DIRECTORY", directory)
    environment = {"KEEP": "unchanged"}
    with pytest.raises(TelegramConfigurationError) as caught:
        load_protected_telegram_environment(environment)
    assert caught.value.reason == "TELEGRAM_DIRECTORY_MISSING"
    assert environment == {"KEEP": "unchanged"}

    directory.mkdir(mode=0o700)
    (directory / telegram_config.BOT_TOKEN_FILENAME).write_text(
        TOKEN,
        encoding="ascii",
    )
    (directory / telegram_config.BOT_TOKEN_FILENAME).chmod(0o600)
    assert _reason(
        load_protected_telegram_environment,
        environment,
    ) == "TELEGRAM_FILE_MISSING"
    assert environment == {"KEEP": "unchanged"}


@pytest.mark.parametrize(
    ("mutate", "expected"),
    (
        (lambda directory: directory.chmod(0o755), "TELEGRAM_DIRECTORY_MODE_INVALID"),
        (
            lambda directory: directory.joinpath(telegram_config.BOT_TOKEN_FILENAME).chmod(0o640),
            "TELEGRAM_FILE_MODE_INVALID",
        ),
        (
            lambda directory: os.link(
                directory / telegram_config.BOT_TOKEN_FILENAME,
                directory / "bot-token-copy",
            ),
            "TELEGRAM_FILE_HARDLINK",
        ),
    ),
)
def test_unsafe_metadata_fails_closed(tmp_path, monkeypatch, mutate, expected):
    directory = tmp_path / "telegram"
    monkeypatch.setattr(telegram_config, "PROTECTED_TELEGRAM_DIRECTORY", directory)
    _write_config(directory)
    mutate(directory)
    assert _reason(
        load_protected_telegram_environment,
        {},
    ) == expected


def test_symlink_directory_and_file_are_rejected(tmp_path, monkeypatch):
    target = tmp_path / "target"
    _write_config(target)
    directory_link = tmp_path / "telegram-link"
    directory_link.symlink_to(target, target_is_directory=True)
    monkeypatch.setattr(
        telegram_config,
        "PROTECTED_TELEGRAM_DIRECTORY",
        directory_link,
    )
    assert _reason(
        load_protected_telegram_environment,
        {},
    ) == "TELEGRAM_DIRECTORY_SYMLINK"

    directory = tmp_path / "telegram"
    directory.mkdir(mode=0o700)
    external = tmp_path / "external-token"
    external.write_text(TOKEN, encoding="ascii")
    (directory / telegram_config.BOT_TOKEN_FILENAME).symlink_to(external)
    (directory / telegram_config.CHAT_ID_FILENAME).write_text(
        CHAT_ID,
        encoding="ascii",
    )
    (directory / telegram_config.CHAT_ID_FILENAME).chmod(0o600)
    monkeypatch.setattr(telegram_config, "PROTECTED_TELEGRAM_DIRECTORY", directory)
    assert _reason(
        load_protected_telegram_environment,
        {},
    ) == "TELEGRAM_FILE_SYMLINK"


@pytest.mark.parametrize(
    ("filename", "value", "expected"),
    (
        (telegram_config.BOT_TOKEN_FILENAME, "", "TELEGRAM_FILE_EMPTY"),
        (
            telegram_config.BOT_TOKEN_FILENAME,
            "x" * (telegram_config.TELEGRAM_VALUE_MAX_BYTES + 1),
            "TELEGRAM_FILE_TOO_LARGE",
        ),
        (telegram_config.BOT_TOKEN_FILENAME, "malformed", "TELEGRAM_VALUE_INVALID"),
        (telegram_config.BOT_TOKEN_FILENAME, "123456:bad value", "TELEGRAM_VALUE_INVALID"),
        (telegram_config.CHAT_ID_FILENAME, "not-a-chat-id", "TELEGRAM_VALUE_INVALID"),
        (telegram_config.CHAT_ID_FILENAME, "123\n456", "TELEGRAM_VALUE_INVALID"),
        (telegram_config.CHAT_ID_FILENAME, "９９", "TELEGRAM_VALUE_INVALID"),
    ),
)
def test_empty_oversized_and_malformed_values_fail_closed(
    tmp_path,
    monkeypatch,
    filename,
    value,
    expected,
):
    directory = tmp_path / "telegram"
    monkeypatch.setattr(telegram_config, "PROTECTED_TELEGRAM_DIRECTORY", directory)
    _write_config(directory)
    path = directory / filename
    path.write_text(value, encoding="utf-8")
    path.chmod(0o600)
    assert _reason(
        load_protected_telegram_environment,
        {},
    ) == expected


def test_filesystem_feature_is_required(tmp_path, monkeypatch):
    directory = tmp_path / "telegram"
    monkeypatch.setattr(telegram_config, "PROTECTED_TELEGRAM_DIRECTORY", directory)
    _write_config(directory)
    monkeypatch.setattr(telegram_config.os, "O_NOFOLLOW", 0)
    assert _reason(
        load_protected_telegram_environment,
        {},
    ) == "TELEGRAM_FILESYSTEM_FEATURE_UNAVAILABLE"


def test_loader_rejects_foreign_directory_owner_before_reading_values(tmp_path, monkeypatch):
    directory = tmp_path / "telegram"
    monkeypatch.setattr(telegram_config, "PROTECTED_TELEGRAM_DIRECTORY", directory)
    _write_config(directory)
    current_uid = os.getuid()
    monkeypatch.setattr(telegram_config.os, "getuid", lambda: current_uid + 1000)
    assert _reason(
        load_protected_telegram_environment,
        {},
    ) == "TELEGRAM_DIRECTORY_OWNER_INVALID"


def test_provisioning_uses_hidden_input_and_never_overwrites(tmp_path, monkeypatch):
    directory = tmp_path / "nested" / "telegram"
    monkeypatch.setattr(telegram_config, "PROTECTED_TELEGRAM_DIRECTORY", directory)
    prompts = []

    def hidden_input(prompt: str) -> str:
        prompts.append(prompt)
        return TOKEN if len(prompts) == 1 else CHAT_ID

    result = provision_protected_telegram(hidden_input)
    assert result.status == PROVISIONED
    assert result.reason == "TELEGRAM_FILES_CREATED"
    assert all("hidden" in prompt and "echo" in prompt for prompt in prompts)
    assert TOKEN not in result.evidence()
    assert CHAT_ID not in result.evidence()
    assert directory.stat().st_mode & 0o777 == 0o700
    for filename, expected in (
        (telegram_config.BOT_TOKEN_FILENAME, TOKEN),
        (telegram_config.CHAT_ID_FILENAME, CHAT_ID),
    ):
        path = directory / filename
        assert path.stat().st_mode & 0o777 == 0o600
        assert path.stat().st_nlink == 1
        assert path.read_text(encoding="ascii") == expected

    before = (directory / telegram_config.BOT_TOKEN_FILENAME).read_bytes()
    result = provision_protected_telegram(
        lambda _prompt: (_ for _ in ()).throw(AssertionError("must not prompt")),
    )
    assert result.status == BLOCKED
    assert result.reason == "TELEGRAM_FILE_ALREADY_EXISTS"
    assert (directory / telegram_config.BOT_TOKEN_FILENAME).read_bytes() == before


def test_provisioning_invalid_input_creates_no_files_and_redacts_failures(
    tmp_path,
    monkeypatch,
):
    directory = tmp_path / "telegram"
    monkeypatch.setattr(telegram_config, "PROTECTED_TELEGRAM_DIRECTORY", directory)
    supplied = "123456:synthetic value"
    result = provision_protected_telegram(lambda _prompt: supplied)
    assert result.status == BLOCKED
    assert result.reason == "TELEGRAM_INPUT_INVALID"
    assert supplied not in result.evidence()
    assert not directory.exists()


def test_provisioning_cli_rejects_secret_bearing_arguments_without_echo(capsys):
    assert provisioning_main(["--token", TOKEN]) == 1
    output = capsys.readouterr().out
    assert TOKEN not in output
    assert json.loads(output) == {
        "reason": "TELEGRAM_ARGUMENTS_FORBIDDEN",
        "status": BLOCKED,
    }


def test_public_paper_run_loads_before_runtime_and_restores_environment(
    tmp_path,
    monkeypatch,
    capsys,
):
    directory = tmp_path / "telegram"
    _write_config(directory)
    monkeypatch.setattr(telegram_config, "PROTECTED_TELEGRAM_DIRECTORY", directory)
    monkeypatch.delenv(telegram_config.RISEX_TELEGRAM_ENABLED, raising=False)
    monkeypatch.delenv(telegram_config.RISEX_TELEGRAM_BOT_TOKEN, raising=False)
    monkeypatch.delenv(telegram_config.RISEX_TELEGRAM_CHAT_ID, raising=False)
    observed = {}

    async def fake_paper_run(repository, fixture):
        observed.update(
            {
                telegram_config.RISEX_TELEGRAM_ENABLED: os.environ.get(
                    telegram_config.RISEX_TELEGRAM_ENABLED
                ),
                telegram_config.RISEX_TELEGRAM_BOT_TOKEN: os.environ.get(
                    telegram_config.RISEX_TELEGRAM_BOT_TOKEN
                ),
                telegram_config.RISEX_TELEGRAM_CHAT_ID: os.environ.get(
                    telegram_config.RISEX_TELEGRAM_CHAT_ID
                ),
            }
        )
        return {"status": "STOPPED_SAFE", "forced_close": False}

    monkeypatch.setattr(cli_module, "_paper_run", fake_paper_run)
    database = tmp_path / "paper.db"
    assert cli_module.main(["--db", str(database), "paper-run"]) == 0
    assert observed == {
        telegram_config.RISEX_TELEGRAM_ENABLED: "true",
        telegram_config.RISEX_TELEGRAM_BOT_TOKEN: TOKEN,
        telegram_config.RISEX_TELEGRAM_CHAT_ID: CHAT_ID,
    }
    assert telegram_config.RISEX_TELEGRAM_BOT_TOKEN not in os.environ
    assert telegram_config.RISEX_TELEGRAM_CHAT_ID not in os.environ
    assert TOKEN.encode() not in database.read_bytes()
    assert CHAT_ID.encode() not in database.read_bytes()
    assert json.loads(capsys.readouterr().out)["status"] == "STOPPED_SAFE"


def test_public_paper_run_preserves_disabled_default_when_config_is_absent(
    tmp_path,
    monkeypatch,
    capsys,
):
    directory = tmp_path / "missing-telegram"
    monkeypatch.setattr(telegram_config, "PROTECTED_TELEGRAM_DIRECTORY", directory)
    observed = {}

    async def fake_paper_run(repository, fixture):
        observed.update(
            {
                key: os.environ.get(key)
                for key in (
                    telegram_config.RISEX_TELEGRAM_ENABLED,
                    telegram_config.RISEX_TELEGRAM_BOT_TOKEN,
                    telegram_config.RISEX_TELEGRAM_CHAT_ID,
                )
            }
        )
        return {"status": "STOPPED_SAFE", "forced_close": False}

    monkeypatch.setattr(cli_module, "_paper_run", fake_paper_run)
    database = tmp_path / "paper.db"
    for key in (
        telegram_config.RISEX_TELEGRAM_ENABLED,
        telegram_config.RISEX_TELEGRAM_BOT_TOKEN,
        telegram_config.RISEX_TELEGRAM_CHAT_ID,
    ):
        monkeypatch.delenv(key, raising=False)
    assert cli_module.main(["--db", str(database), "paper-run"]) == 0
    assert observed == {
        telegram_config.RISEX_TELEGRAM_ENABLED: None,
        telegram_config.RISEX_TELEGRAM_BOT_TOKEN: None,
        telegram_config.RISEX_TELEGRAM_CHAT_ID: None,
    }
    assert database.exists()
    assert json.loads(capsys.readouterr().out)["status"] == "STOPPED_SAFE"


def test_public_paper_run_preserves_explicit_environment_when_pair_is_absent(
    tmp_path,
    monkeypatch,
    capsys,
):
    directory = tmp_path / "owner-only-telegram"
    directory.mkdir(mode=0o700)
    directory.chmod(0o700)
    monkeypatch.setattr(telegram_config, "PROTECTED_TELEGRAM_DIRECTORY", directory)
    explicit = {
        telegram_config.RISEX_TELEGRAM_ENABLED: "true",
        telegram_config.RISEX_TELEGRAM_BOT_TOKEN: "synthetic-explicit-token",
        telegram_config.RISEX_TELEGRAM_CHAT_ID: "synthetic-explicit-chat",
    }
    for key, value in explicit.items():
        monkeypatch.setenv(key, value)
    observed = {}

    async def fake_paper_run(repository, fixture):
        observed.update(
            {
                key: os.environ.get(key)
                for key in explicit
            }
        )
        return {"status": "STOPPED_SAFE", "forced_close": False}

    monkeypatch.setattr(cli_module, "_paper_run", fake_paper_run)
    database = tmp_path / "paper.db"
    assert cli_module.main(["--db", str(database), "paper-run"]) == 0
    assert observed == explicit
    assert database.exists()
    assert json.loads(capsys.readouterr().out)["status"] == "STOPPED_SAFE"


@pytest.mark.parametrize(
    "setup",
    (
        "partial",
        "unsafe_directory",
        "invalid_value",
        "ambiguous_inspection",
    ),
)
def test_public_paper_run_blocks_before_database_for_incomplete_or_unsafe_pair(
    tmp_path,
    monkeypatch,
    capsys,
    setup,
):
    directory = tmp_path / "telegram"
    monkeypatch.setattr(telegram_config, "PROTECTED_TELEGRAM_DIRECTORY", directory)
    if setup == "partial":
        directory.mkdir(mode=0o700)
        directory.chmod(0o700)
        token_path = directory / telegram_config.BOT_TOKEN_FILENAME
        token_path.write_text(TOKEN, encoding="ascii")
        token_path.chmod(0o600)
        expected_reason = "TELEGRAM_FILE_PAIR_INCOMPLETE"
    elif setup == "unsafe_directory":
        _write_config(directory)
        directory.chmod(0o755)
        expected_reason = "TELEGRAM_DIRECTORY_MODE_INVALID"
    elif setup == "invalid_value":
        _write_config(directory, token="synthetic malformed token")
        expected_reason = "TELEGRAM_VALUE_INVALID"
    else:
        _write_config(directory)
        real_stat = telegram_config.os.stat

        def ambiguous_stat(*_args, **_kwargs):
            raise PermissionError("synthetic inspection failure")

        monkeypatch.setattr(telegram_config.os, "stat", ambiguous_stat)
        expected_reason = "TELEGRAM_FILE_INSPECTION_AMBIGUOUS"

    started = False

    async def fake_paper_run(*_args, **_kwargs):
        nonlocal started
        started = True
        return {"status": "STOPPED_SAFE", "forced_close": False}

    monkeypatch.setattr(cli_module, "_paper_run", fake_paper_run)
    database = tmp_path / "paper.db"
    assert cli_module.main(["--db", str(database), "paper-run"]) == 1
    assert not started
    if setup == "ambiguous_inspection":
        with pytest.raises(FileNotFoundError):
            real_stat(database, follow_symlinks=False)
    else:
        assert not database.exists()
    output = capsys.readouterr().out
    assert TOKEN not in output
    assert CHAT_ID not in output
    assert "synthetic malformed token" not in output
    assert json.loads(output) == {
        "reason": expected_reason,
        "status": "BLOCKED",
    }
