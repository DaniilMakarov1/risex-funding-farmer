"""Protected local loading for the outbound PAPER Telegram configuration.

The normal application still consumes Telegram configuration exclusively from
the ``RISEX_TELEGRAM_*`` environment variables.  This module is only the
local restart boundary that loads those values from fixed owner-only files;
it has no Telegram transport or lifecycle knowledge.
"""

from __future__ import annotations

import getpass
import errno
import json
import os
import re
import stat
import sys
from collections.abc import Callable, Iterator, MutableMapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Final


PROTECTED_TELEGRAM_DIRECTORY: Final = Path(
    "/Users/daniilmakarov/.config/risex-farmer/telegram"
)
# Descriptive alias retained for callers that refer to the local config root.
TELEGRAM_CONFIG_DIRECTORY = PROTECTED_TELEGRAM_DIRECTORY

BOT_TOKEN_FILENAME: Final = "bot-token"
CHAT_ID_FILENAME: Final = "chat-id"
TELEGRAM_BOT_TOKEN_PATH: Final = PROTECTED_TELEGRAM_DIRECTORY / BOT_TOKEN_FILENAME
TELEGRAM_CHAT_ID_PATH: Final = PROTECTED_TELEGRAM_DIRECTORY / CHAT_ID_FILENAME

RISEX_TELEGRAM_ENABLED: Final = "RISEX_TELEGRAM_ENABLED"
RISEX_TELEGRAM_BOT_TOKEN: Final = "RISEX_TELEGRAM_BOT_TOKEN"
RISEX_TELEGRAM_CHAT_ID: Final = "RISEX_TELEGRAM_CHAT_ID"

PROTECTED_DIRECTORY_MODE: Final = 0o700
PROTECTED_FILE_MODE: Final = 0o600
TELEGRAM_VALUE_MAX_BYTES: Final = 512

PROVISIONED: Final = "PROVISIONED"
BLOCKED: Final = "BLOCKED"

_BOT_TOKEN_RE = re.compile(r"[0-9]{1,16}:[A-Za-z0-9_-]{1,256}\Z")
_CHAT_ID_RE = re.compile(r"-?(?:0|[1-9][0-9]{0,19})\Z")
_MISSING = object()


class TelegramConfigurationError(RuntimeError):
    """A sanitized fail-closed protected-configuration error."""

    def __init__(self, reason: str) -> None:
        if not isinstance(reason, str) or not re.fullmatch(r"[A-Z0-9_]+", reason):
            reason = "TELEGRAM_CONFIGURATION_UNAVAILABLE"
        self.reason = reason
        super().__init__(f"Telegram protected configuration unavailable: {reason}")


@dataclass(frozen=True, slots=True)
class ProvisioningResult:
    """Sanitized result for the hidden local provisioning path."""

    status: str
    reason: str

    @property
    def provisioned(self) -> bool:
        return self.status == PROVISIONED

    def evidence(self) -> str:
        return json.dumps(
            {"reason": self.reason, "status": self.status},
            sort_keys=True,
            separators=(",", ":"),
        )


def protected_telegram_paths() -> dict[str, Path]:
    """Return fixed paths without opening or reading either value."""

    root = PROTECTED_TELEGRAM_DIRECTORY
    return {
        "bot_token": root / BOT_TOKEN_FILENAME,
        "chat_id": root / CHAT_ID_FILENAME,
    }


def _directory_open_flags() -> int:
    directory_flag = getattr(os, "O_DIRECTORY", 0)
    nofollow_flag = getattr(os, "O_NOFOLLOW", 0)
    if not directory_flag or not nofollow_flag:
        raise TelegramConfigurationError("TELEGRAM_FILESYSTEM_FEATURE_UNAVAILABLE")
    return os.O_RDONLY | directory_flag | nofollow_flag | getattr(os, "O_CLOEXEC", 0)


def _fixed_directory_components(directory: Path) -> tuple[str, ...]:
    if not isinstance(directory, Path) or not directory.is_absolute():
        raise TelegramConfigurationError("TELEGRAM_DIRECTORY_NOT_ABSOLUTE")
    parts = directory.parts
    components = parts[1:] if parts and parts[0] == os.sep else ()
    if not components or any(part in {"", ".", ".."} for part in components):
        raise TelegramConfigurationError("TELEGRAM_DIRECTORY_INVALID")
    return components


def _validate_directory_fd(fd: int, *, final: bool) -> None:
    try:
        info = os.fstat(fd)
    except OSError:
        raise TelegramConfigurationError("TELEGRAM_DIRECTORY_UNREADABLE") from None
    if not stat.S_ISDIR(info.st_mode):
        raise TelegramConfigurationError("TELEGRAM_DIRECTORY_NOT_DIRECTORY")
    current_uid = os.getuid()
    if final:
        if info.st_uid != current_uid:
            raise TelegramConfigurationError("TELEGRAM_DIRECTORY_OWNER_INVALID")
        if stat.S_IMODE(info.st_mode) != PROTECTED_DIRECTORY_MODE:
            raise TelegramConfigurationError("TELEGRAM_DIRECTORY_MODE_INVALID")
    elif info.st_uid not in {0, current_uid}:
        raise TelegramConfigurationError("TELEGRAM_DIRECTORY_OWNER_INVALID")


def _directory_open_reason(
    error: OSError,
    *,
    parent_fd: int | None = None,
    component: str | None = None,
) -> str:
    if error.errno == errno.ENOENT:
        return "TELEGRAM_DIRECTORY_MISSING"
    if error.errno == errno.ELOOP:
        return "TELEGRAM_DIRECTORY_SYMLINK"
    if error.errno == errno.ENOTDIR:
        if parent_fd is not None and component is not None:
            try:
                info = os.stat(component, dir_fd=parent_fd, follow_symlinks=False)
            except OSError:
                pass
            else:
                if stat.S_ISLNK(info.st_mode):
                    return "TELEGRAM_DIRECTORY_SYMLINK"
        return "TELEGRAM_DIRECTORY_NOT_DIRECTORY"
    return "TELEGRAM_DIRECTORY_UNREADABLE"


def _open_directory_component(
    parent_fd: int,
    component: str,
    *,
    create_missing: bool,
    final: bool,
    flags: int,
) -> int:
    created = False
    try:
        descriptor = os.open(component, flags, dir_fd=parent_fd)
    except OSError as error:
        if error.errno != errno.ENOENT or not create_missing:
            raise TelegramConfigurationError(
                _directory_open_reason(
                    error,
                    parent_fd=parent_fd,
                    component=component,
                )
            ) from None
        try:
            os.mkdir(component, PROTECTED_DIRECTORY_MODE, dir_fd=parent_fd)
            created = True
        except FileExistsError:
            pass
        except OSError:
            raise TelegramConfigurationError("TELEGRAM_DIRECTORY_CREATE_FAILED") from None
        try:
            descriptor = os.open(component, flags, dir_fd=parent_fd)
        except OSError as open_error:
            raise TelegramConfigurationError(
                _directory_open_reason(
                    open_error,
                    parent_fd=parent_fd,
                    component=component,
                )
            ) from None

    try:
        if created:
            try:
                os.fchmod(descriptor, PROTECTED_DIRECTORY_MODE)
            except OSError:
                raise TelegramConfigurationError("TELEGRAM_DIRECTORY_MODE_FAILED") from None
        _validate_directory_fd(descriptor, final=final)
        return descriptor
    except TelegramConfigurationError:
        os.close(descriptor)
        raise
    except OSError:
        os.close(descriptor)
        raise TelegramConfigurationError("TELEGRAM_DIRECTORY_UNREADABLE") from None


def _walk_fixed_directory(directory: Path, *, create_missing: bool) -> int:
    components = _fixed_directory_components(directory)
    flags = _directory_open_flags()
    try:
        current_fd = os.open(os.sep, flags)
    except OSError:
        raise TelegramConfigurationError("TELEGRAM_DIRECTORY_OPEN_FAILED") from None
    try:
        _validate_directory_fd(current_fd, final=False)
        for index, component in enumerate(components):
            child_fd = _open_directory_component(
                current_fd,
                component,
                create_missing=create_missing,
                final=index == len(components) - 1,
                flags=flags,
            )
            os.close(current_fd)
            current_fd = child_fd
        result = current_fd
        current_fd = -1
        return result
    finally:
        if current_fd != -1:
            os.close(current_fd)


def _file_reason_from_stat(info: os.stat_result) -> str | None:
    if stat.S_ISLNK(info.st_mode):
        return "TELEGRAM_FILE_SYMLINK"
    if not stat.S_ISREG(info.st_mode):
        return "TELEGRAM_FILE_NOT_REGULAR"
    if info.st_uid != os.getuid():
        return "TELEGRAM_FILE_OWNER_INVALID"
    if info.st_nlink != 1:
        return "TELEGRAM_FILE_HARDLINK"
    if stat.S_IMODE(info.st_mode) != PROTECTED_FILE_MODE:
        return "TELEGRAM_FILE_MODE_INVALID"
    if info.st_size <= 0:
        return "TELEGRAM_FILE_EMPTY"
    if info.st_size > TELEGRAM_VALUE_MAX_BYTES:
        return "TELEGRAM_FILE_TOO_LARGE"
    return None


def _read_fd_value(directory_fd: int, filename: str, field: str) -> str:
    nofollow_flag = getattr(os, "O_NOFOLLOW", 0)
    if not nofollow_flag:
        raise TelegramConfigurationError("TELEGRAM_FILESYSTEM_FEATURE_UNAVAILABLE")
    try:
        descriptor = os.open(
            filename,
            os.O_RDONLY | nofollow_flag | getattr(os, "O_CLOEXEC", 0),
            dir_fd=directory_fd,
        )
    except FileNotFoundError:
        raise TelegramConfigurationError("TELEGRAM_FILE_MISSING") from None
    except OSError as error:
        if error.errno == errno.ELOOP:
            raise TelegramConfigurationError("TELEGRAM_FILE_SYMLINK") from None
        raise TelegramConfigurationError("TELEGRAM_FILE_OPEN_FAILED") from None

    raw = bytearray()
    try:
        try:
            initial = os.fstat(descriptor)
        except OSError:
            raise TelegramConfigurationError("TELEGRAM_FILE_UNREADABLE") from None
        reason = _file_reason_from_stat(initial)
        if reason is not None:
            raise TelegramConfigurationError(reason)

        while True:
            try:
                chunk = os.read(
                    descriptor,
                    TELEGRAM_VALUE_MAX_BYTES + 1 - len(raw),
                )
            except OSError:
                raise TelegramConfigurationError("TELEGRAM_FILE_READ_FAILED") from None
            if not chunk:
                break
            raw.extend(chunk)
            if len(raw) > TELEGRAM_VALUE_MAX_BYTES:
                raise TelegramConfigurationError("TELEGRAM_FILE_TOO_LARGE")

        try:
            final = os.fstat(descriptor)
        except OSError:
            raise TelegramConfigurationError("TELEGRAM_FILE_UNREADABLE") from None
        reason = _file_reason_from_stat(final)
        if reason is not None:
            raise TelegramConfigurationError(reason)
        if final.st_size != len(raw):
            raise TelegramConfigurationError("TELEGRAM_FILE_CHANGED")

        try:
            current_path = os.stat(
                filename,
                dir_fd=directory_fd,
                follow_symlinks=False,
            )
        except OSError:
            raise TelegramConfigurationError("TELEGRAM_FILE_CHANGED") from None
        if stat.S_ISLNK(current_path.st_mode):
            raise TelegramConfigurationError("TELEGRAM_FILE_SYMLINK")
        if (
            current_path.st_dev != final.st_dev
            or current_path.st_ino != final.st_ino
        ):
            raise TelegramConfigurationError("TELEGRAM_FILE_CHANGED")

        try:
            value = raw.decode("ascii")
        except UnicodeDecodeError:
            raise TelegramConfigurationError("TELEGRAM_VALUE_INVALID") from None
        if not value or any(character.isspace() or ord(character) < 0x20 for character in value):
            raise TelegramConfigurationError("TELEGRAM_VALUE_INVALID")
        pattern = _BOT_TOKEN_RE if field == BOT_TOKEN_FILENAME else _CHAT_ID_RE
        if pattern.fullmatch(value) is None:
            raise TelegramConfigurationError("TELEGRAM_VALUE_INVALID")
        return value
    finally:
        raw[:] = b"\x00" * len(raw)
        os.close(descriptor)


def _read_protected_values() -> tuple[str, str]:
    directory_fd = _walk_fixed_directory(
        PROTECTED_TELEGRAM_DIRECTORY,
        create_missing=False,
    )
    try:
        token = _read_fd_value(directory_fd, BOT_TOKEN_FILENAME, BOT_TOKEN_FILENAME)
        chat_id = _read_fd_value(directory_fd, CHAT_ID_FILENAME, CHAT_ID_FILENAME)
        return token, chat_id
    finally:
        os.close(directory_fd)


def _protected_files_present() -> bool:
    """Inspect the fixed pair without reading values or following links.

    A missing directory or a valid directory with both entries absent means
    that the existing explicit environment configuration remains authoritative.
    Any partial, unsafe, or ambiguous pair is a startup error.
    """

    try:
        directory_fd = _walk_fixed_directory(
            PROTECTED_TELEGRAM_DIRECTORY,
            create_missing=False,
        )
    except TelegramConfigurationError as error:
        if error.reason == "TELEGRAM_DIRECTORY_MISSING":
            return False
        raise

    try:
        states: list[tuple[bool, str | None]] = []
        for filename in (BOT_TOKEN_FILENAME, CHAT_ID_FILENAME):
            try:
                info = os.stat(
                    filename,
                    dir_fd=directory_fd,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                states.append((False, None))
                continue
            except OSError:
                raise TelegramConfigurationError(
                    "TELEGRAM_FILE_INSPECTION_AMBIGUOUS"
                ) from None
            states.append((True, _file_reason_from_stat(info)))

        if states[0][0] != states[1][0]:
            raise TelegramConfigurationError("TELEGRAM_FILE_PAIR_INCOMPLETE")
        if not states[0][0]:
            return False
        for _, reason in states:
            if reason is not None:
                raise TelegramConfigurationError(reason)
        return True
    finally:
        os.close(directory_fd)


def _set_environment(
    target: MutableMapping[str, str],
    token: str,
    chat_id: str,
) -> None:
    values = {
        RISEX_TELEGRAM_ENABLED: "true",
        RISEX_TELEGRAM_BOT_TOKEN: token,
        RISEX_TELEGRAM_CHAT_ID: chat_id,
    }
    previous = {key: target.get(key, _MISSING) for key in values}
    try:
        target.update(values)
    except Exception:
        for key, old_value in previous.items():
            if old_value is _MISSING:
                target.pop(key, None)
            else:
                target[key] = old_value
        raise TelegramConfigurationError("TELEGRAM_ENVIRONMENT_EXPORT_FAILED") from None


def load_protected_telegram_environment(
    environ: MutableMapping[str, str] | None = None,
) -> None:
    """Load protected values into the explicit application environment.

    The function returns no configuration object and never prints a value.
    Tests may replace the module's fixed path constant with a temporary
    synthetic directory; normal startup always uses the production path above.
    """

    target = os.environ if environ is None else environ
    token, chat_id = _read_protected_values()
    _set_environment(target, token, chat_id)


@contextmanager
def protected_telegram_environment(
    environ: MutableMapping[str, str] | None = None,
) -> Iterator[None]:
    """Temporarily expose protected values as ``RISEX_TELEGRAM_*`` only."""

    target = os.environ if environ is None else environ
    keys = (
        RISEX_TELEGRAM_ENABLED,
        RISEX_TELEGRAM_BOT_TOKEN,
        RISEX_TELEGRAM_CHAT_ID,
    )
    previous = {key: target.get(key, _MISSING) for key in keys}
    try:
        token, chat_id = _read_protected_values()
        _set_environment(target, token, chat_id)
        yield
    finally:
        for key, old_value in previous.items():
            if old_value is _MISSING:
                target.pop(key, None)
            else:
                target[key] = old_value


@contextmanager
def paper_telegram_environment(
    environ: MutableMapping[str, str] | None = None,
) -> Iterator[None]:
    """Load protected Telegram values only when the complete pair exists.

    With both fixed files absent, this context is a no-op so the existing
    explicit environment contract—including disabled-by-default behavior—
    remains unchanged.
    """

    if not _protected_files_present():
        yield
        return
    with protected_telegram_environment(environ):
        yield


def _input_payload(
    input_fn: Callable[[str], str],
    prompt: str,
) -> bytearray:
    try:
        supplied = input_fn(prompt)
    except (KeyboardInterrupt, EOFError):
        raise TelegramConfigurationError("TELEGRAM_INPUT_CANCELLED") from None
    except Exception:
        raise TelegramConfigurationError("TELEGRAM_INPUT_UNAVAILABLE") from None
    try:
        if type(supplied) is not str:
            raise TelegramConfigurationError("TELEGRAM_INPUT_INVALID")
        payload = bytearray(supplied.encode("ascii"))
    except UnicodeEncodeError:
        raise TelegramConfigurationError("TELEGRAM_INPUT_INVALID") from None
    finally:
        # The local string is never returned, logged, or included in a result.
        supplied = ""
    if not 0 < len(payload) <= TELEGRAM_VALUE_MAX_BYTES:
        payload[:] = b"\x00" * len(payload)
        raise TelegramConfigurationError("TELEGRAM_INPUT_INVALID")
    return payload


def _validate_payload(payload: bytearray, field: str) -> None:
    try:
        value = payload.decode("ascii")
    except UnicodeDecodeError:
        raise TelegramConfigurationError("TELEGRAM_INPUT_INVALID") from None
    if not value or any(character.isspace() or ord(character) < 0x20 for character in value):
        raise TelegramConfigurationError("TELEGRAM_INPUT_INVALID")
    pattern = _BOT_TOKEN_RE if field == BOT_TOKEN_FILENAME else _CHAT_ID_RE
    if pattern.fullmatch(value) is None:
        raise TelegramConfigurationError("TELEGRAM_INPUT_INVALID")


def _existing_file_reason(directory_fd: int, filename: str) -> str | None:
    try:
        info = os.stat(filename, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    except OSError:
        return "TELEGRAM_FILE_UNREADABLE"
    reason = _file_reason_from_stat(info)
    return "TELEGRAM_FILE_ALREADY_EXISTS" if reason is None else reason


def _write_all(fd: int, payload: bytearray) -> None:
    view = memoryview(payload)
    try:
        while len(view):
            try:
                written = os.write(fd, view)
            except OSError:
                raise TelegramConfigurationError("TELEGRAM_FILE_WRITE_FAILED") from None
            if written <= 0:
                raise TelegramConfigurationError("TELEGRAM_FILE_WRITE_INCOMPLETE")
            view = view[written:]
    finally:
        view.release()


def _create_secure_file(
    directory_fd: int,
    filename: str,
    payload: bytearray,
) -> None:
    nofollow_flag = getattr(os, "O_NOFOLLOW", 0)
    if not nofollow_flag:
        raise TelegramConfigurationError("TELEGRAM_FILESYSTEM_FEATURE_UNAVAILABLE")
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | nofollow_flag
        | getattr(os, "O_CLOEXEC", 0)
    )
    try:
        descriptor = os.open(
            filename,
            flags,
            PROTECTED_FILE_MODE,
            dir_fd=directory_fd,
        )
    except FileExistsError:
        raise TelegramConfigurationError("TELEGRAM_FILE_ALREADY_EXISTS") from None
    except OSError:
        raise TelegramConfigurationError("TELEGRAM_FILE_CREATE_FAILED") from None

    completed = False
    try:
        try:
            os.fchmod(descriptor, PROTECTED_FILE_MODE)
            info = os.fstat(descriptor)
        except OSError:
            raise TelegramConfigurationError("TELEGRAM_FILE_METADATA_FAILED") from None
        reason = _file_reason_from_stat(info)
        if reason is not None and reason not in {"TELEGRAM_FILE_EMPTY"}:
            raise TelegramConfigurationError(reason)
        _write_all(descriptor, payload)
        try:
            os.fsync(descriptor)
            final = os.fstat(descriptor)
        except OSError:
            raise TelegramConfigurationError("TELEGRAM_FILE_SYNC_FAILED") from None
        if (
            stat.S_IMODE(final.st_mode) != PROTECTED_FILE_MODE
            or final.st_uid != os.getuid()
            or final.st_nlink != 1
            or final.st_size != len(payload)
        ):
            raise TelegramConfigurationError("TELEGRAM_FILE_METADATA_CHANGED")
        completed = True
    finally:
        os.close(descriptor)
        if not completed:
            try:
                os.unlink(filename, dir_fd=directory_fd)
            except OSError:
                pass


def _remove_created(directory_fd: int, filenames: Sequence[str]) -> None:
    for filename in reversed(tuple(filenames)):
        try:
            os.unlink(filename, dir_fd=directory_fd)
        except OSError:
            pass


def provision_protected_telegram(
    input_fn: Callable[[str], str] | None = None,
) -> ProvisioningResult:
    """Create the two fixed files through hidden input, never overwriting.

    The default input function is ``getpass.getpass``.  Tests may inject only
    synthetic values; the returned result contains no supplied value.
    """

    input_fn = getpass.getpass if input_fn is None else input_fn
    root = PROTECTED_TELEGRAM_DIRECTORY
    directory_fd: int | None = None
    payloads: list[bytearray] = []
    created: list[str] = []
    try:
        try:
            directory_fd = _walk_fixed_directory(root, create_missing=False)
        except TelegramConfigurationError as error:
            if error.reason != "TELEGRAM_DIRECTORY_MISSING":
                return ProvisioningResult(BLOCKED, error.reason)

        if directory_fd is not None:
            for filename in (BOT_TOKEN_FILENAME, CHAT_ID_FILENAME):
                reason = _existing_file_reason(directory_fd, filename)
                if reason is not None:
                    return ProvisioningResult(BLOCKED, reason)

        prompts = (
            "Telegram bot token (hidden; no secret echo): ",
            "Telegram chat ID (hidden; no secret echo): ",
        )
        for prompt, filename in zip(
            prompts,
            (BOT_TOKEN_FILENAME, CHAT_ID_FILENAME),
        ):
            payload = _input_payload(input_fn, prompt)
            try:
                _validate_payload(payload, filename)
            except TelegramConfigurationError:
                payload[:] = b"\x00" * len(payload)
                raise
            payloads.append(payload)

        directory_fd = (
            directory_fd
            if directory_fd is not None
            else _walk_fixed_directory(root, create_missing=True)
        )
        for filename, payload in zip(
            (BOT_TOKEN_FILENAME, CHAT_ID_FILENAME),
            payloads,
        ):
            _create_secure_file(directory_fd, filename, payload)
            created.append(filename)
        try:
            os.fsync(directory_fd)
        except OSError:
            raise TelegramConfigurationError("TELEGRAM_DIRECTORY_SYNC_FAILED") from None
    except TelegramConfigurationError as error:
        if directory_fd is not None and created:
            _remove_created(directory_fd, created)
        return ProvisioningResult(BLOCKED, error.reason)
    except OSError:
        if directory_fd is not None and created:
            _remove_created(directory_fd, created)
        return ProvisioningResult(BLOCKED, "TELEGRAM_FILESYSTEM_OPERATION_FAILED")
    finally:
        for payload in payloads:
            payload[:] = b"\x00" * len(payload)
        if directory_fd is not None:
            os.close(directory_fd)

    return ProvisioningResult(PROVISIONED, "TELEGRAM_FILES_CREATED")


# Short aliases keep the boundary discoverable without changing the app's
# environment-only notification API.
load_telegram_environment = load_protected_telegram_environment
provision_telegram_configuration = provision_protected_telegram


def main(argv: Sequence[str] | None = None) -> int:
    """Hidden-input provisioning entry point; no secret-bearing arguments."""

    supplied_args = list(sys.argv[1:] if argv is None else argv)
    if supplied_args:
        print(json.dumps({"reason": "TELEGRAM_ARGUMENTS_FORBIDDEN", "status": BLOCKED}))
        return 1
    try:
        result = provision_protected_telegram()
    except Exception:
        result = ProvisioningResult(BLOCKED, "TELEGRAM_PROVISIONING_FAILED")
    print(result.evidence())
    return 0 if result.provisioned else 1


__all__ = [
    "BLOCKED",
    "BOT_TOKEN_FILENAME",
    "CHAT_ID_FILENAME",
    "PROTECTED_DIRECTORY_MODE",
    "PROTECTED_FILE_MODE",
    "PROTECTED_TELEGRAM_DIRECTORY",
    "ProvisioningResult",
    "PROVISIONED",
    "RISEX_TELEGRAM_BOT_TOKEN",
    "RISEX_TELEGRAM_CHAT_ID",
    "RISEX_TELEGRAM_ENABLED",
    "TELEGRAM_BOT_TOKEN_PATH",
    "TELEGRAM_CHAT_ID_PATH",
    "TELEGRAM_CONFIG_DIRECTORY",
    "TELEGRAM_VALUE_MAX_BYTES",
    "TelegramConfigurationError",
    "load_protected_telegram_environment",
    "load_telegram_environment",
    "paper_telegram_environment",
    "protected_telegram_environment",
    "protected_telegram_paths",
    "provision_protected_telegram",
    "provision_telegram_configuration",
]


if __name__ == "__main__":
    raise SystemExit(main())
