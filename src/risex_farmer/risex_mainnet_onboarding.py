"""Protected, offline RISEx mainnet session-signer onboarding.

This module is deliberately outside the normal Farmer import graph.  Running
it explicitly with ``python -m risex_farmer.risex_mainnet_onboarding`` asks for
one hidden main-wallet key, derives its public address, creates a distinct
session key, and stores only the session key plus sanitized public identity
metadata.

The register-signer helpers below are contract-shape helpers only.  They build
the exact EIP-712 objects and request shape documented by RISEx, but this
module has no transport, signing entry point, or live-registration path.
Registration remains a separately authorized Chief-owned operation.  In
particular, a durable registration intent is claimable only once so a later
operational caller cannot silently replay it after a restart or ambiguity.
"""

from __future__ import annotations

import errno
import getpass
import json
import os
import secrets
import stat
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable, Mapping


# Current official mainnet register-signer contract values.  The domain and
# type fields match the RISEx integration contract; no endpoint is contacted
# by this offline module.
MAINNET_CHAIN_ID = 4153
MAINNET_AUTH_CONTRACT = "0x0d919daa3f12ae715744eb648c00066c5dbd66f0"
MAINNET_DOMAIN_NAME = "RISEx"
MAINNET_DOMAIN_VERSION = "1"
REGISTER_SIGNER_MESSAGE = "RISEx session key"
SESSION_EXPIRATION_SECONDS = 30 * 24 * 60 * 60

_REGISTER_SIGNER_FIELDS = (
    {"name": "account", "type": "address"},
    {"name": "signer", "type": "address"},
    {"name": "message", "type": "string"},
    {"name": "expiration", "type": "uint32"},
    {"name": "nonceAnchor", "type": "uint48"},
    {"name": "nonceBitmap", "type": "uint8"},
)
_VERIFY_SIGNER_FIELDS = (
    {"name": "account", "type": "address"},
    {"name": "nonceAnchor", "type": "uint48"},
    {"name": "nonceBitmap", "type": "uint8"},
)
_EIP712_DOMAIN_FIELDS = (
    {"name": "name", "type": "string"},
    {"name": "version", "type": "string"},
    {"name": "chainId", "type": "uint256"},
    {"name": "verifyingContract", "type": "address"},
)
# Public copies are evidence/reporting constants.  Builders use the private
# copies above so a caller cannot mutate the contract schema in-process.
REGISTER_SIGNER_FIELDS = tuple(dict(field) for field in _REGISTER_SIGNER_FIELDS)
VERIFY_SIGNER_FIELDS = tuple(dict(field) for field in _VERIFY_SIGNER_FIELDS)
EIP712_DOMAIN_FIELDS = tuple(dict(field) for field in _EIP712_DOMAIN_FIELDS)

REGISTER_SIGNER_TYPEHASH = bytes.fromhex(
    "a526f63b3968e56ae1b177ce9b3dc29766e0891e6397a9c23cf8c53ee8fc8f62"
)
VERIFY_SIGNER_TYPEHASH = bytes.fromhex(
    "4d298dcceb691695f582cc337308236426a0c97201a31834625e8eadc44d4230"
)

PROVISIONED = "PROTECTED_FILES_CREATED"
BLOCKED = "BLOCKED"
NO_MAINNET_WRITE_AUTHORITY = "NO_MAINNET_WRITE_AUTHORITY"

REGISTRATION_NOT_PREPARED = "NOT_PREPARED"
REGISTRATION_PREPARED = "PREPARED"
REGISTRATION_SPENT_UNKNOWN = "SPENT_UNKNOWN"

PROTECTED_SECRET_DIRECTORY = Path.home() / ".config" / "risex-farmer"
# Alias for callers that use the shorter wording; all production operations
# still resolve the one fixed constant above and accept no path argument.
PROTECTED_DIRECTORY = PROTECTED_SECRET_DIRECTORY

SESSION_KEY_FILENAME = "risex-mainnet-session-signer-v1.key"
IDENTITY_FILENAME = "risex-mainnet-identity-v1.json"
REGISTRATION_INTENT_FILENAME = "risex-mainnet-register-signer-v1.json"
REGISTRATION_SPENT_FILENAME = "risex-mainnet-register-signer-v1.spent"
_FIXED_FILENAMES = (
    SESSION_KEY_FILENAME,
    IDENTITY_FILENAME,
    REGISTRATION_INTENT_FILENAME,
    REGISTRATION_SPENT_FILENAME,
)

_SCHEMA_VERSION = 1
_DIRECTORY_MODE = 0o700
_FILE_MODE = 0o600
_KEY_BYTES = 32
_MAX_IDENTITY_BYTES = 4096
_MAX_INTENT_BYTES = 4096
_MAX_SPENT_BYTES = 1024
_MAX_INPUT_CHARS = 66
_MAX_ADDRESS_CHARS = 42
_UINT32_MAX = 2**32 - 1
_UINT48_MAX = 2**48 - 1
_MAX_NONCE_BITMAP_INDEX = 207
_PROMPT = "RISEx main-wallet private key (hidden; not persisted): "


class OnboardingViolation(ValueError):
    """A sanitized fail-closed onboarding or offline-contract rejection."""

    def __init__(self, reason: str) -> None:
        if type(reason) is not str or not reason or any(
            character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-:"
            for character in reason
        ):
            reason = "ONBOARDING_CONTRACT_REJECTED"
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True)
class ProtectedPathState:
    name: str
    path: str
    present: bool
    protected: bool
    reason: str
    mode: int | None = None
    link_count: int | None = None
    size: int | None = None


@dataclass(frozen=True)
class _CreatedFile:
    name: str
    device: int
    inode: int


@dataclass(frozen=True)
class ProtectedFiles:
    session_key: ProtectedPathState
    identity: ProtectedPathState
    registration_intent: ProtectedPathState
    registration_spent: ProtectedPathState

    @property
    def all_required_protected(self) -> bool:
        return self.session_key.protected and self.identity.protected

    @property
    def all_protected(self) -> bool:
        """Compatibility-friendly name for the two required credential files."""

        return self.all_required_protected

    def for_name(self, name: str) -> ProtectedPathState:
        return {
            SESSION_KEY_FILENAME: self.session_key,
            IDENTITY_FILENAME: self.identity,
            REGISTRATION_INTENT_FILENAME: self.registration_intent,
            REGISTRATION_SPENT_FILENAME: self.registration_spent,
        }[name]


@dataclass(frozen=True)
class ProvisionedIdentity:
    wallet_address: str
    session_signer_address: str
    expiration: int
    registration_status: str = REGISTRATION_NOT_PREPARED

    @property
    def environment(self) -> str:
        return "MAINNET"

    @property
    def chain_id(self) -> int:
        return MAINNET_CHAIN_ID

    @property
    def verifying_contract(self) -> str:
        return MAINNET_AUTH_CONTRACT


@dataclass(frozen=True)
class OnboardingResult:
    status: str
    reason: str
    wallet_address: str | None = None
    session_signer_address: str | None = None
    expiration: int | None = None
    files: ProtectedFiles | None = None
    mainnet_write_authority: str = NO_MAINNET_WRITE_AUTHORITY

    @property
    def ready(self) -> bool:
        return self.status == PROVISIONED

    @property
    def write_ready(self) -> bool:
        return False

    def as_dict(self) -> dict[str, Any]:
        # This is intentionally a public-identity-only report.  No secret,
        # credential bytes, prompt value, or filesystem payload is returned.
        return {
            "environment": "MAINNET",
            "expiration": self.expiration,
            "mainnet_write_authority": self.mainnet_write_authority,
            "reason": self.reason,
            "registration_status": REGISTRATION_NOT_PREPARED,
            "session_signer_address": self.session_signer_address,
            "status": self.status,
            "wallet_address": self.wallet_address,
            "write_ready": False,
        }

    def evidence(self) -> str:
        return json.dumps(self.as_dict(), sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class RegistrationIntent:
    intent_id: str
    wallet_address: str
    session_signer_address: str
    expiration: int
    observed_nonce_anchor: int
    observed_bitmap_index: int
    observed_bitmap: int
    nonce_anchor: int
    nonce_bitmap_index: int
    state: str = REGISTRATION_PREPARED

    @property
    def environment(self) -> str:
        return "MAINNET"

    @property
    def typed_register_data(self) -> dict[str, Any]:
        return build_register_signer_typed_data(
            self.wallet_address,
            self.session_signer_address,
            self.expiration,
            self.nonce_anchor,
            self.nonce_bitmap_index,
        )

    @property
    def typed_verify_data(self) -> dict[str, Any]:
        return build_verify_signer_typed_data(
            self.wallet_address,
            self.nonce_anchor,
            self.nonce_bitmap_index,
        )


def _blocked(reason: str, *, files: ProtectedFiles | None = None) -> OnboardingResult:
    return OnboardingResult(status=BLOCKED, reason=reason, files=files)


def _now_unix() -> int:
    import time

    return int(time.time())


def _directory_open_flags() -> int:
    directory_flag = getattr(os, "O_DIRECTORY", 0)
    nofollow_flag = getattr(os, "O_NOFOLLOW", 0)
    if not directory_flag or not nofollow_flag:
        raise OnboardingViolation("PROTECTED_FILESYSTEM_FEATURE_UNAVAILABLE")
    return (
        os.O_RDONLY
        | directory_flag
        | nofollow_flag
        | getattr(os, "O_CLOEXEC", 0)
    )


def _fixed_directory_components(directory: Path) -> tuple[str, ...]:
    if not isinstance(directory, Path) or not directory.is_absolute():
        raise OnboardingViolation("PROTECTED_DIRECTORY_NOT_ABSOLUTE")
    parts = directory.parts
    if not parts or parts[0] != os.sep:
        raise OnboardingViolation("PROTECTED_DIRECTORY_NOT_ABSOLUTE")
    components = parts[1:]
    if not components or any(part in {"", ".", ".."} for part in components):
        raise OnboardingViolation("PROTECTED_DIRECTORY_INVALID")
    return components


def _validate_directory_fd(fd: int, *, final: bool = False) -> None:
    try:
        info = os.fstat(fd)
    except OSError:
        raise OnboardingViolation("PROTECTED_DIRECTORY_UNREADABLE") from None
    if not stat.S_ISDIR(info.st_mode):
        raise OnboardingViolation("PROTECTED_DIRECTORY_NOT_DIRECTORY")
    current_uid = os.getuid()
    if final:
        if info.st_uid != current_uid:
            raise OnboardingViolation("PROTECTED_DIRECTORY_OWNER_NOT_CURRENT_USER")
        if stat.S_IMODE(info.st_mode) != _DIRECTORY_MODE:
            raise OnboardingViolation("PROTECTED_DIRECTORY_MODE_NOT_0700")
    elif info.st_uid not in {0, current_uid}:
        raise OnboardingViolation("PROTECTED_DIRECTORY_OWNER_NOT_CURRENT_USER")


def _directory_open_reason(
    error: OSError, *, parent_fd: int | None = None, component: str | None = None
) -> str:
    if error.errno == errno.ENOENT:
        return "PROTECTED_DIRECTORY_MISSING"
    if error.errno == errno.ELOOP:
        return "PROTECTED_DIRECTORY_SYMLINK"
    if error.errno == errno.ENOTDIR:
        if parent_fd is not None and component is not None:
            try:
                info = os.stat(component, dir_fd=parent_fd, follow_symlinks=False)
            except OSError:
                pass
            else:
                if stat.S_ISLNK(info.st_mode):
                    return "PROTECTED_DIRECTORY_SYMLINK"
        return "PROTECTED_DIRECTORY_NOT_DIRECTORY"
    return "PROTECTED_DIRECTORY_UNREADABLE"


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
            raise OnboardingViolation(
                _directory_open_reason(
                    error,
                    parent_fd=parent_fd,
                    component=component,
                )
            ) from None
        try:
            os.mkdir(component, _DIRECTORY_MODE, dir_fd=parent_fd)
            created = True
        except FileExistsError:
            pass
        except OSError:
            raise OnboardingViolation("PROTECTED_DIRECTORY_CREATE_FAILED") from None
        try:
            descriptor = os.open(component, flags, dir_fd=parent_fd)
        except OSError as open_error:
            raise OnboardingViolation(
                _directory_open_reason(
                    open_error,
                    parent_fd=parent_fd,
                    component=component,
                )
            ) from None

    try:
        if created:
            try:
                os.fchmod(descriptor, _DIRECTORY_MODE)
            except OSError:
                raise OnboardingViolation("PROTECTED_DIRECTORY_MODE_FAILED") from None
        _validate_directory_fd(descriptor, final=final)
        return descriptor
    except OnboardingViolation:
        os.close(descriptor)
        raise
    except OSError:
        os.close(descriptor)
        raise OnboardingViolation("PROTECTED_DIRECTORY_UNREADABLE") from None


def _walk_fixed_directory(*, create_missing: bool) -> int:
    components = _fixed_directory_components(PROTECTED_SECRET_DIRECTORY)
    flags = _directory_open_flags()
    try:
        current_fd = os.open(os.sep, flags)
    except OSError:
        raise OnboardingViolation("PROTECTED_DIRECTORY_OPEN_FAILED") from None
    try:
        _validate_directory_fd(current_fd)
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


def _directory_state(directory: Path) -> tuple[bool, str]:
    if directory != PROTECTED_SECRET_DIRECTORY:
        return False, "PROTECTED_DIRECTORY_NOT_FIXED"
    try:
        descriptor = _walk_fixed_directory(create_missing=False)
    except OnboardingViolation as error:
        return False, error.reason
    try:
        return True, "PROTECTED_DIRECTORY_OK"
    finally:
        os.close(descriptor)


def _ensure_fixed_directory() -> int:
    return _walk_fixed_directory(create_missing=True)


def _open_directory() -> int:
    return _walk_fixed_directory(create_missing=False)


def protected_paths() -> Mapping[str, Path]:
    """Return the fixed paths without opening or reading any protected file."""

    directory = PROTECTED_SECRET_DIRECTORY
    return {
        "session_key": directory / SESSION_KEY_FILENAME,
        "identity": directory / IDENTITY_FILENAME,
        "registration_intent": directory / REGISTRATION_INTENT_FILENAME,
        "registration_spent": directory / REGISTRATION_SPENT_FILENAME,
    }


def _file_size_limit(name: str) -> int:
    return {
        SESSION_KEY_FILENAME: _KEY_BYTES,
        IDENTITY_FILENAME: _MAX_IDENTITY_BYTES,
        REGISTRATION_INTENT_FILENAME: _MAX_INTENT_BYTES,
        REGISTRATION_SPENT_FILENAME: _MAX_SPENT_BYTES,
    }[name]


def _file_state(
    name: str,
    path: Path,
    directory_fd: int | None,
    directory_ok: bool,
    directory_reason: str,
) -> ProtectedPathState:
    if not directory_ok or directory_fd is None:
        return ProtectedPathState(
            name=name,
            path=str(path),
            present=False,
            protected=False,
            reason=directory_reason,
        )
    try:
        info = os.stat(path.name, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return ProtectedPathState(
            name=name,
            path=str(path),
            present=False,
            protected=False,
            reason="PROTECTED_FILE_MISSING",
        )
    except OSError:
        return ProtectedPathState(
            name=name,
            path=str(path),
            present=False,
            protected=False,
            reason="PROTECTED_FILE_UNREADABLE",
        )

    mode = stat.S_IMODE(info.st_mode)
    reason = "PROTECTED_FILE_OK"
    if stat.S_ISLNK(info.st_mode):
        reason = "PROTECTED_FILE_SYMLINK"
    elif not stat.S_ISREG(info.st_mode):
        reason = "PROTECTED_FILE_NOT_REGULAR"
    elif info.st_uid != os.getuid():
        reason = "PROTECTED_FILE_OWNER_NOT_CURRENT_USER"
    elif info.st_nlink != 1:
        reason = "PROTECTED_FILE_HARDLINK"
    elif mode != _FILE_MODE:
        reason = "PROTECTED_FILE_MODE_NOT_0600"
    elif info.st_size <= 0:
        reason = "PROTECTED_FILE_EMPTY"
    elif info.st_size > _file_size_limit(name):
        reason = "PROTECTED_FILE_TOO_LARGE"
    elif name == SESSION_KEY_FILENAME and info.st_size != _KEY_BYTES:
        reason = "PROTECTED_SESSION_KEY_SIZE_INVALID"
    return ProtectedPathState(
        name=name,
        path=str(path),
        present=True,
        protected=reason == "PROTECTED_FILE_OK",
        reason=reason,
        mode=mode,
        link_count=info.st_nlink,
        size=info.st_size,
    )


def inspect_protected_files() -> ProtectedFiles:
    """Inspect fixed metadata only; never read secret or identity bytes."""

    paths = protected_paths()
    directory_fd: int | None = None
    try:
        directory_fd = _open_directory()
    except OnboardingViolation as error:
        directory_ok, directory_reason = False, error.reason
    else:
        directory_ok, directory_reason = True, "PROTECTED_DIRECTORY_OK"
    try:
        states = {
            name: _file_state(
                name,
                path,
                directory_fd,
                directory_ok,
                directory_reason,
            )
            for name, path in (
                (SESSION_KEY_FILENAME, paths["session_key"]),
                (IDENTITY_FILENAME, paths["identity"]),
                (REGISTRATION_INTENT_FILENAME, paths["registration_intent"]),
                (REGISTRATION_SPENT_FILENAME, paths["registration_spent"]),
            )
        }
    finally:
        if directory_fd is not None:
            os.close(directory_fd)
    return ProtectedFiles(
        session_key=states[SESSION_KEY_FILENAME],
        identity=states[IDENTITY_FILENAME],
        registration_intent=states[REGISTRATION_INTENT_FILENAME],
        registration_spent=states[REGISTRATION_SPENT_FILENAME],
    )


def _validate_fd(fd: int, name: str, *, check_size: bool = True) -> None:
    try:
        info = os.fstat(fd)
    except OSError:
        raise OnboardingViolation("PROTECTED_FILE_UNREADABLE") from None
    if not stat.S_ISREG(info.st_mode):
        raise OnboardingViolation("PROTECTED_FILE_NOT_REGULAR")
    if info.st_uid != os.getuid():
        raise OnboardingViolation("PROTECTED_FILE_OWNER_NOT_CURRENT_USER")
    if info.st_nlink != 1:
        raise OnboardingViolation("PROTECTED_FILE_HARDLINK")
    if stat.S_IMODE(info.st_mode) != _FILE_MODE:
        raise OnboardingViolation("PROTECTED_FILE_MODE_NOT_0600")
    if check_size:
        if info.st_size < 0 or info.st_size > _file_size_limit(name):
            raise OnboardingViolation("PROTECTED_FILE_TOO_LARGE")
        if name == SESSION_KEY_FILENAME and info.st_size != _KEY_BYTES:
            raise OnboardingViolation("PROTECTED_SESSION_KEY_SIZE_INVALID")


def _read_secure_file(directory_fd: int, name: str) -> bytes:
    nofollow_flag = getattr(os, "O_NOFOLLOW", 0)
    if not nofollow_flag:
        raise OnboardingViolation("PROTECTED_FILESYSTEM_FEATURE_UNAVAILABLE")
    try:
        fd = os.open(
            name,
            os.O_RDONLY | nofollow_flag | getattr(os, "O_CLOEXEC", 0),
            dir_fd=directory_fd,
        )
    except FileNotFoundError:
        raise OnboardingViolation("PROTECTED_FILE_MISSING") from None
    except OSError:
        raise OnboardingViolation("PROTECTED_FILE_OPEN_FAILED") from None
    try:
        _validate_fd(fd, name)
        limit = _file_size_limit(name)
        chunks: list[bytes] = []
        total = 0
        while True:
            try:
                chunk = os.read(fd, min(4096, limit + 1 - total))
            except OSError:
                raise OnboardingViolation("PROTECTED_FILE_READ_FAILED") from None
            if not chunk:
                break
            total += len(chunk)
            if total > limit:
                raise OnboardingViolation("PROTECTED_FILE_TOO_LARGE")
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        os.close(fd)


def _write_all(fd: int, payload: bytes | bytearray) -> None:
    view = memoryview(payload)
    try:
        while len(view):
            try:
                written = os.write(fd, view)
            except OSError:
                raise OnboardingViolation("PROTECTED_FILE_WRITE_FAILED") from None
            if written <= 0:
                raise OnboardingViolation("PROTECTED_FILE_WRITE_INCOMPLETE")
            view = view[written:]
    finally:
        view.release()


def _create_secure_file(
    directory_fd: int, name: str, payload: bytes | bytearray
) -> _CreatedFile:
    nofollow_flag = getattr(os, "O_NOFOLLOW", 0)
    if not nofollow_flag:
        raise OnboardingViolation("PROTECTED_FILESYSTEM_FEATURE_UNAVAILABLE")
    if not 0 < len(payload) <= _file_size_limit(name):
        raise OnboardingViolation("PROTECTED_FILE_SIZE_INVALID")
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | nofollow_flag
        | getattr(os, "O_CLOEXEC", 0)
    )
    try:
        fd = os.open(name, flags, _FILE_MODE, dir_fd=directory_fd)
    except FileExistsError:
        raise OnboardingViolation("PROTECTED_PATH_ALREADY_EXISTS") from None
    except OSError:
        raise OnboardingViolation("PROTECTED_FILE_CREATE_FAILED") from None
    try:
        try:
            os.fchmod(fd, _FILE_MODE)
        except OSError:
            raise OnboardingViolation("PROTECTED_FILE_MODE_FAILED") from None
        _validate_fd(fd, name, check_size=False)
        _write_all(fd, payload)
        try:
            os.fsync(fd)
            info = os.fstat(fd)
        except OSError:
            raise OnboardingViolation("PROTECTED_FILE_SYNC_FAILED") from None
        expected_size = len(payload)
        if (
            stat.S_IMODE(info.st_mode) != _FILE_MODE
            or info.st_uid != os.getuid()
            or info.st_nlink != 1
            or info.st_size != expected_size
        ):
            if info.st_uid != os.getuid():
                raise OnboardingViolation("PROTECTED_FILE_OWNER_NOT_CURRENT_USER")
            raise OnboardingViolation("PROTECTED_FILE_METADATA_CHANGED")
        return _CreatedFile(name=name, device=info.st_dev, inode=info.st_ino)
    finally:
        os.close(fd)


def _sync_directory(directory_fd: int) -> None:
    try:
        os.fsync(directory_fd)
    except OSError:
        raise OnboardingViolation("PROTECTED_DIRECTORY_SYNC_FAILED") from None


def _remove_created(directory_fd: int, files: list[_CreatedFile]) -> None:
    for created in reversed(files):
        try:
            info = os.stat(
                created.name,
                dir_fd=directory_fd,
                follow_symlinks=False,
            )
        except OSError:
            continue
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) != _FILE_MODE
            or info.st_dev != created.device
            or info.st_ino != created.inode
        ):
            continue
        try:
            os.unlink(created.name, dir_fd=directory_fd)
        except FileNotFoundError:
            continue
        except OSError:
            # The operation never removes a path it did not create.  If an
            # unlink is refused, leave the fixed path for explicit recovery;
            # never replace it on a later invocation.
            continue


def _normalize_address(value: object) -> str:
    if type(value) is not str or len(value) != _MAX_ADDRESS_CHARS:
        raise OnboardingViolation("ADDRESS_INVALID")
    if not value.startswith("0x"):
        raise OnboardingViolation("ADDRESS_INVALID")
    digits = value[2:]
    if any(character not in "0123456789abcdefABCDEF" for character in digits):
        raise OnboardingViolation("ADDRESS_INVALID")
    return value.lower()


def _uint(value: object, *, maximum: int, reason: str) -> int:
    if type(value) is not int or not 0 <= value <= maximum:
        raise OnboardingViolation(reason)
    return value


def _parse_main_private_key(value: object) -> bytearray:
    if type(value) is not str or not value or len(value) > _MAX_INPUT_CHARS:
        raise OnboardingViolation("MAIN_KEY_INVALID")
    if value.startswith("0x"):
        digits = value[2:]
    else:
        digits = value
    if len(digits) != 64 or any(
        character not in "0123456789abcdefABCDEF" for character in digits
    ):
        raise OnboardingViolation("MAIN_KEY_INVALID")
    try:
        secret = bytearray.fromhex(digits)
    except ValueError:
        raise OnboardingViolation("MAIN_KEY_INVALID") from None
    if len(secret) != _KEY_BYTES:
        for index in range(len(secret)):
            secret[index] = 0
        raise OnboardingViolation("MAIN_KEY_INVALID")
    return secret


def _derive_address(secret: bytes | bytearray) -> str:
    try:
        from eth_account import Account

        address = Account.from_key(secret).address
    except OnboardingViolation:
        raise
    except Exception:
        raise OnboardingViolation("MAIN_KEY_INVALID") from None
    try:
        return _normalize_address(address)
    except OnboardingViolation:
        raise OnboardingViolation("MAIN_KEY_INVALID") from None


def _new_session_secret() -> bytearray:
    try:
        secret = bytearray(secrets.token_bytes(_KEY_BYTES))
    except Exception:
        raise OnboardingViolation("SESSION_KEY_GENERATION_FAILED") from None
    if len(secret) != _KEY_BYTES:
        for index in range(len(secret)):
            secret[index] = 0
        raise OnboardingViolation("SESSION_KEY_GENERATION_FAILED")
    return secret


def _identity_mapping(identity: ProvisionedIdentity) -> dict[str, Any]:
    return {
        "chain_id": MAINNET_CHAIN_ID,
        "environment": "MAINNET",
        "expiration": identity.expiration,
        "registration_status": identity.registration_status,
        "schema_version": _SCHEMA_VERSION,
        "session_signer_address": identity.session_signer_address,
        "venue": "RISEx",
        "verifying_contract": MAINNET_AUTH_CONTRACT,
        "wallet_address": identity.wallet_address,
    }


def _json_bytes(value: Mapping[str, Any]) -> bytes:
    try:
        return (
            json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode("ascii")
    except (UnicodeEncodeError, TypeError, ValueError):
        raise OnboardingViolation("PROTECTED_METADATA_INVALID") from None


def _parse_identity(value: bytes) -> ProvisionedIdentity:
    try:
        data = json.loads(value)
        if not isinstance(data, dict):
            raise ValueError
        expected = {
            "chain_id",
            "environment",
            "expiration",
            "registration_status",
            "schema_version",
            "session_signer_address",
            "venue",
            "verifying_contract",
            "wallet_address",
        }
        if set(data) != expected:
            raise ValueError
        if (
            data["schema_version"] != _SCHEMA_VERSION
            or data["venue"] != "RISEx"
            or data["environment"] != "MAINNET"
            or data["chain_id"] != MAINNET_CHAIN_ID
            or data["verifying_contract"] != MAINNET_AUTH_CONTRACT
            or data["registration_status"] != REGISTRATION_NOT_PREPARED
        ):
            raise ValueError
        wallet = _normalize_address(data["wallet_address"])
        signer = _normalize_address(data["session_signer_address"])
        expiration = _uint(
            data["expiration"], maximum=_UINT32_MAX, reason="EXPIRATION_INVALID"
        )
        if wallet == signer:
            raise ValueError
        return ProvisionedIdentity(wallet, signer, expiration)
    except OnboardingViolation:
        raise OnboardingViolation("IDENTITY_FILE_INVALID") from None
    except Exception:
        raise OnboardingViolation("IDENTITY_FILE_INVALID") from None


def read_provisioned_identity() -> ProvisionedIdentity:
    """Read and validate only the sanitized identity metadata."""

    directory_fd = _open_directory()
    try:
        return _parse_identity(_read_secure_file(directory_fd, IDENTITY_FILENAME))
    finally:
        os.close(directory_fd)


def _valid_signature(value: object) -> bool:
    if type(value) is not str or len(value) != 132 or not value.startswith("0x"):
        return False
    return all(character in "0123456789abcdefABCDEF" for character in value[2:])


def _domain() -> dict[str, Any]:
    return {
        "name": MAINNET_DOMAIN_NAME,
        "version": MAINNET_DOMAIN_VERSION,
        "chainId": MAINNET_CHAIN_ID,
        "verifyingContract": MAINNET_AUTH_CONTRACT,
    }


def build_register_signer_typed_data(
    account: object,
    signer: object,
    expiration: object,
    nonce_anchor: object,
    nonce_bitmap_index: object,
) -> dict[str, Any]:
    """Build exact offline EIP-712 data for ``RegisterSigner``."""

    account_address = _normalize_address(account)
    signer_address = _normalize_address(signer)
    if account_address == signer_address:
        raise OnboardingViolation("MAIN_AND_SESSION_IDENTITIES_NOT_DISTINCT")
    expiration_value = _uint(
        expiration, maximum=_UINT32_MAX, reason="EXPIRATION_INVALID"
    )
    if expiration_value == 0:
        raise OnboardingViolation("EXPIRATION_INVALID")
    anchor = _uint(nonce_anchor, maximum=_UINT48_MAX, reason="NONCE_ANCHOR_INVALID")
    bitmap_index = _uint(
        nonce_bitmap_index,
        maximum=_MAX_NONCE_BITMAP_INDEX,
        reason="NONCE_BITMAP_INDEX_INVALID",
    )
    return {
        "types": {
            "EIP712Domain": [dict(field) for field in _EIP712_DOMAIN_FIELDS],
            "RegisterSigner": [dict(field) for field in _REGISTER_SIGNER_FIELDS],
        },
        "primaryType": "RegisterSigner",
        "domain": _domain(),
        "message": {
            "account": account_address,
            "signer": signer_address,
            "message": REGISTER_SIGNER_MESSAGE,
            "expiration": expiration_value,
            "nonceAnchor": anchor,
            "nonceBitmap": bitmap_index,
        },
    }


def build_verify_signer_typed_data(
    account: object,
    nonce_anchor: object,
    nonce_bitmap_index: object,
) -> dict[str, Any]:
    """Build exact offline EIP-712 data for ``VerifySigner``."""

    account_address = _normalize_address(account)
    anchor = _uint(nonce_anchor, maximum=_UINT48_MAX, reason="NONCE_ANCHOR_INVALID")
    bitmap_index = _uint(
        nonce_bitmap_index,
        maximum=_MAX_NONCE_BITMAP_INDEX,
        reason="NONCE_BITMAP_INDEX_INVALID",
    )
    return {
        "types": {
            "EIP712Domain": [dict(field) for field in _EIP712_DOMAIN_FIELDS],
            "VerifySigner": [dict(field) for field in _VERIFY_SIGNER_FIELDS],
        },
        "primaryType": "VerifySigner",
        "domain": _domain(),
        "message": {
            "account": account_address,
            "nonceAnchor": anchor,
            "nonceBitmap": bitmap_index,
        },
    }


def build_register_signer_request(
    intent: RegistrationIntent,
    account_signature: object,
    signer_signature: object,
) -> dict[str, Any]:
    """Build the exact official request body without sending it.

    The caller must build this before claiming the intent.  A spent intent is
    intentionally rejected so an operational caller cannot recreate a request
    after an ambiguous registration attempt.
    """

    if not isinstance(intent, RegistrationIntent) or intent.state != REGISTRATION_PREPARED:
        raise OnboardingViolation("REGISTRATION_INTENT_NOT_REUSABLE")
    if not _valid_signature(account_signature) or not _valid_signature(signer_signature):
        raise OnboardingViolation("SIGNATURE_INVALID")
    if (
        intent.nonce_anchor != intent.observed_nonce_anchor
        or intent.nonce_bitmap_index != intent.observed_bitmap_index
    ):
        raise OnboardingViolation("REGISTRATION_INTENT_NONCE_MISMATCH")
    try:
        persisted = load_registration_intent()
    except OnboardingViolation:
        raise
    if persisted != intent:
        raise OnboardingViolation("REGISTRATION_INTENT_MISMATCH")
    # Revalidate the whole contract identity before returning a wire-shaped
    # object.  No optional label or extra field is accepted here.
    build_register_signer_typed_data(
        intent.wallet_address,
        intent.session_signer_address,
        intent.expiration,
        intent.nonce_anchor,
        intent.nonce_bitmap_index,
    )
    return {
        "account": intent.wallet_address,
        "signer": intent.session_signer_address,
        "message": REGISTER_SIGNER_MESSAGE,
        "nonce_anchor": str(intent.nonce_anchor),
        "nonce_bitmap_index": intent.nonce_bitmap_index,
        "expiration": str(intent.expiration),
        "account_signature": account_signature,
        "signer_signature": signer_signature,
    }


def _parse_bitmap(value: object) -> int:
    if type(value) is int:
        return _uint(value, maximum=2**256 - 1, reason="NONCE_BITMAP_INVALID")
    if type(value) is not str or not value or value != value.strip():
        raise OnboardingViolation("NONCE_BITMAP_INVALID")
    try:
        if value.startswith("0x"):
            digits = value[2:]
            if not 1 <= len(digits) <= 64:
                raise ValueError
            if any(character not in "0123456789abcdefABCDEF" for character in digits):
                raise ValueError
            parsed = int(digits, 16)
        else:
            if not value.isdigit() or len(value) > 78:
                raise ValueError
            parsed = int(value, 10)
    except ValueError:
        raise OnboardingViolation("NONCE_BITMAP_INVALID") from None
    return _uint(parsed, maximum=2**256 - 1, reason="NONCE_BITMAP_INVALID")


def _intent_mapping(intent: RegistrationIntent) -> dict[str, Any]:
    return {
        "environment": "MAINNET",
        "expiration": intent.expiration,
        "intent_id": intent.intent_id,
        "nonce_anchor": intent.nonce_anchor,
        "nonce_bitmap_index": intent.nonce_bitmap_index,
        "observed_bitmap": str(intent.observed_bitmap),
        "observed_bitmap_index": intent.observed_bitmap_index,
        "observed_nonce_anchor": intent.observed_nonce_anchor,
        "schema_version": _SCHEMA_VERSION,
        "session_signer_address": intent.session_signer_address,
        "state": REGISTRATION_PREPARED,
        "venue": "RISEx",
        "wallet_address": intent.wallet_address,
    }


def _spent_mapping(intent_id: str) -> dict[str, Any]:
    return {
        "environment": "MAINNET",
        "intent_id": intent_id,
        "schema_version": _SCHEMA_VERSION,
        "state": REGISTRATION_SPENT_UNKNOWN,
        "venue": "RISEx",
    }


def _parse_intent(value: bytes, identity: ProvisionedIdentity) -> RegistrationIntent:
    try:
        data = json.loads(value)
        expected = {
            "environment",
            "expiration",
            "intent_id",
            "nonce_anchor",
            "nonce_bitmap_index",
            "observed_bitmap",
            "observed_bitmap_index",
            "observed_nonce_anchor",
            "schema_version",
            "session_signer_address",
            "state",
            "venue",
            "wallet_address",
        }
        if not isinstance(data, dict) or set(data) != expected:
            raise ValueError
        if (
            data["environment"] != "MAINNET"
            or data["schema_version"] != _SCHEMA_VERSION
            or data["venue"] != "RISEx"
            or data["state"] != REGISTRATION_PREPARED
            or type(data["intent_id"]) is not str
            or len(data["intent_id"]) != 32
            or any(character not in "0123456789abcdef" for character in data["intent_id"])
        ):
            raise ValueError
        wallet = _normalize_address(data["wallet_address"])
        signer = _normalize_address(data["session_signer_address"])
        if (
            wallet != identity.wallet_address
            or signer != identity.session_signer_address
        ):
            raise ValueError
        expiration = _uint(
            data["expiration"], maximum=_UINT32_MAX, reason="EXPIRATION_INVALID"
        )
        if expiration == 0:
            raise ValueError
        if expiration != identity.expiration:
            raise ValueError
        observed_anchor = _uint(
            data["observed_nonce_anchor"],
            maximum=_UINT48_MAX,
            reason="NONCE_ANCHOR_INVALID",
        )
        observed_index = _uint(
            data["observed_bitmap_index"],
            maximum=_MAX_NONCE_BITMAP_INDEX,
            reason="NONCE_BITMAP_INDEX_INVALID",
        )
        observed_bitmap = _parse_bitmap(data["observed_bitmap"])
        anchor = _uint(
            data["nonce_anchor"], maximum=_UINT48_MAX, reason="NONCE_ANCHOR_INVALID"
        )
        bitmap_index = _uint(
            data["nonce_bitmap_index"],
            maximum=_MAX_NONCE_BITMAP_INDEX,
            reason="NONCE_BITMAP_INDEX_INVALID",
        )
        if anchor != observed_anchor or bitmap_index != observed_index:
            raise ValueError
        return RegistrationIntent(
            intent_id=data["intent_id"],
            wallet_address=wallet,
            session_signer_address=signer,
            expiration=expiration,
            observed_nonce_anchor=observed_anchor,
            observed_bitmap_index=observed_index,
            observed_bitmap=observed_bitmap,
            nonce_anchor=anchor,
            nonce_bitmap_index=bitmap_index,
        )
    except OnboardingViolation:
        raise OnboardingViolation("REGISTRATION_INTENT_INVALID") from None
    except Exception:
        raise OnboardingViolation("REGISTRATION_INTENT_INVALID") from None


def _parse_spent(value: bytes, intent_id: str) -> None:
    try:
        data = json.loads(value)
        if data != _spent_mapping(intent_id):
            raise ValueError
    except Exception:
        raise OnboardingViolation("REGISTRATION_SPENT_MARKER_INVALID") from None


def _read_identity_and_open() -> tuple[int, ProvisionedIdentity]:
    directory_fd = _open_directory()
    try:
        identity = _parse_identity(_read_secure_file(directory_fd, IDENTITY_FILENAME))
        return directory_fd, identity
    except Exception:
        os.close(directory_fd)
        raise


def _load_intent_state(
    directory_fd: int, identity: ProvisionedIdentity
) -> RegistrationIntent:
    intent = _parse_intent(
        _read_secure_file(directory_fd, REGISTRATION_INTENT_FILENAME), identity
    )
    try:
        spent = _read_secure_file(directory_fd, REGISTRATION_SPENT_FILENAME)
    except OnboardingViolation as error:
        if error.reason == "PROTECTED_FILE_MISSING":
            return intent
        raise
    _parse_spent(spent, intent.intent_id)
    return replace(intent, state=REGISTRATION_SPENT_UNKNOWN)


def _ensure_key_matches_identity(directory_fd: int, identity: ProvisionedIdentity) -> None:
    secret = bytearray(_read_secure_file(directory_fd, SESSION_KEY_FILENAME))
    try:
        if _derive_address(secret) != identity.session_signer_address:
            raise OnboardingViolation("SESSION_KEY_IDENTITY_MISMATCH")
    finally:
        for index in range(len(secret)):
            secret[index] = 0


def _new_intent_id() -> str:
    try:
        value = secrets.token_hex(16)
    except Exception:
        raise OnboardingViolation("REGISTRATION_INTENT_ID_FAILED") from None
    if (
        type(value) is not str
        or len(value) != 32
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise OnboardingViolation("REGISTRATION_INTENT_ID_FAILED")
    return value


def prepare_registration_intent(
    *, nonce_anchor: object, current_bitmap_index: object, bitmap: object
) -> RegistrationIntent:
    """Persist one offline register-signer intent from an observed nonce.

    ``nonce_anchor``/``current_bitmap_index``/``bitmap`` are the exact bounded
    identity fields from the official nonce-state observation.  The signed
    registration identity uses the exact observed anchor and bitmap index.  This
    function performs no key loading, signing, network access, or dispatch.
    """

    observed_anchor = _uint(
        nonce_anchor,
        maximum=_UINT48_MAX,
        reason="NONCE_ANCHOR_INVALID",
    )
    observed_index = _uint(
        current_bitmap_index,
        maximum=_MAX_NONCE_BITMAP_INDEX,
        reason="NONCE_BITMAP_INDEX_INVALID",
    )
    observed_bitmap = _parse_bitmap(bitmap)
    now = _now_unix()
    directory_fd, identity = _read_identity_and_open()
    try:
        _ensure_key_matches_identity(directory_fd, identity)
        try:
            _read_secure_file(directory_fd, REGISTRATION_INTENT_FILENAME)
            raise OnboardingViolation("REGISTRATION_INTENT_ALREADY_EXISTS")
        except OnboardingViolation as error:
            if error.reason != "PROTECTED_FILE_MISSING":
                raise
        try:
            _read_secure_file(directory_fd, REGISTRATION_SPENT_FILENAME)
            raise OnboardingViolation("REGISTRATION_INTENT_ALREADY_EXISTS")
        except OnboardingViolation as error:
            if error.reason != "PROTECTED_FILE_MISSING":
                raise
        if not 0 < now < identity.expiration <= _UINT32_MAX:
            raise OnboardingViolation("EXPIRATION_INVALID")
        intent = RegistrationIntent(
            intent_id=_new_intent_id(),
            wallet_address=identity.wallet_address,
            session_signer_address=identity.session_signer_address,
            expiration=identity.expiration,
            observed_nonce_anchor=observed_anchor,
            observed_bitmap_index=observed_index,
            observed_bitmap=observed_bitmap,
            nonce_anchor=observed_anchor,
            nonce_bitmap_index=observed_index,
        )
        _create_secure_file(
            directory_fd,
            REGISTRATION_INTENT_FILENAME,
            _json_bytes(_intent_mapping(intent)),
        )
        _sync_directory(directory_fd)
        return intent
    finally:
        os.close(directory_fd)


def load_registration_intent() -> RegistrationIntent:
    """Load the one durable registration intent and its non-replay state."""

    directory_fd, identity = _read_identity_and_open()
    try:
        return _load_intent_state(directory_fd, identity)
    finally:
        os.close(directory_fd)


def claim_registration_intent() -> RegistrationIntent:
    """Durably consume the intent before a separately owned live dispatch."""

    directory_fd, identity = _read_identity_and_open()
    try:
        intent = _load_intent_state(directory_fd, identity)
        if intent.state != REGISTRATION_PREPARED:
            raise OnboardingViolation("REGISTRATION_INTENT_ALREADY_SPENT")
        try:
            _create_secure_file(
                directory_fd,
                REGISTRATION_SPENT_FILENAME,
                _json_bytes(_spent_mapping(intent.intent_id)),
            )
        except OnboardingViolation as error:
            if error.reason == "PROTECTED_PATH_ALREADY_EXISTS":
                raise OnboardingViolation("REGISTRATION_INTENT_ALREADY_SPENT") from None
            raise
        _sync_directory(directory_fd)
        return replace(intent, state=REGISTRATION_SPENT_UNKNOWN)
    finally:
        os.close(directory_fd)


def _zeroize(secret: bytearray) -> None:
    for index in range(len(secret)):
        secret[index] = 0


def provision_mainnet_session_signer(
    input_fn: Callable[[str], str] | None = None,
) -> OnboardingResult:
    """Run the one-shot hidden-input offline onboarding flow.

    The callback exists only for synthetic tests.  Production defaults to
    ``getpass.getpass`` and accepts no command-line, environment, or path
    override for the main key.
    """

    input_fn = getpass.getpass if input_fn is None else input_fn
    before = inspect_protected_files()
    if any(before.for_name(name).present for name in _FIXED_FILENAMES):
        return _blocked("PROTECTED_PATH_ALREADY_EXISTS", files=before)
    directory_ok, directory_reason = _directory_state(PROTECTED_SECRET_DIRECTORY)
    if not directory_ok and directory_reason not in {
        "PROTECTED_DIRECTORY_MISSING",
    }:
        return _blocked(directory_reason, files=before)

    supplied: str | None = None
    main_secret = bytearray()
    session_secret = bytearray()
    try:
        try:
            supplied = input_fn(_PROMPT)
        except (EOFError, KeyboardInterrupt):
            return _blocked("PROTECTED_INPUT_CANCELLED", files=before)
        except Exception:
            return _blocked("PROTECTED_INPUT_UNAVAILABLE", files=before)
        main_secret = _parse_main_private_key(supplied)
        supplied = None
        wallet_address = _derive_address(main_secret)
        session_secret = _new_session_secret()
        session_address = _derive_address(session_secret)
        if wallet_address == session_address:
            return _blocked("MAIN_AND_SESSION_IDENTITIES_NOT_DISTINCT", files=before)
        now = _now_unix()
        expiration = now + SESSION_EXPIRATION_SECONDS
        if not 0 < now < expiration <= _UINT32_MAX:
            return _blocked("EXPIRATION_INVALID", files=before)
        identity = ProvisionedIdentity(
            wallet_address=wallet_address,
            session_signer_address=session_address,
            expiration=expiration,
        )

        directory_fd = _ensure_fixed_directory()
        created: list[_CreatedFile] = []
        try:
            # O_EXCL is the authoritative race-safe no-overwrite barrier.  The
            # preflight above only avoids prompting when paths are already
            # present; it is not relied upon for safety.
            created.append(
                _create_secure_file(directory_fd, SESSION_KEY_FILENAME, session_secret)
            )
            created.append(
                _create_secure_file(
                    directory_fd,
                    IDENTITY_FILENAME,
                    _json_bytes(_identity_mapping(identity)),
                )
            )
            _sync_directory(directory_fd)
        except Exception:
            _remove_created(directory_fd, created)
            raise
        finally:
            os.close(directory_fd)
        return OnboardingResult(
            status=PROVISIONED,
            reason=PROVISIONED,
            wallet_address=wallet_address,
            session_signer_address=session_address,
            expiration=expiration,
            files=inspect_protected_files(),
        )
    except OnboardingViolation as error:
        return _blocked(error.reason, files=inspect_protected_files())
    except Exception:
        return _blocked("ONBOARDING_OPERATION_FAILED", files=inspect_protected_files())
    finally:
        supplied = None
        _zeroize(main_secret)
        _zeroize(session_secret)


# Short explicit alias for callers that prefer the noun used by the task.
onboard_mainnet = provision_mainnet_session_signer


def main() -> int:
    """Visible Terminal entry point with hidden input and sanitized output."""

    result = provision_mainnet_session_signer()
    print(json.dumps(result.as_dict(), sort_keys=True, separators=(",", ":")))
    return 0 if result.ready else 1


__all__ = [
    "BLOCKED",
    "EIP712_DOMAIN_FIELDS",
    "IDENTITY_FILENAME",
    "MAINNET_AUTH_CONTRACT",
    "MAINNET_CHAIN_ID",
    "MAINNET_DOMAIN_NAME",
    "MAINNET_DOMAIN_VERSION",
    "NO_MAINNET_WRITE_AUTHORITY",
    "OnboardingResult",
    "OnboardingViolation",
    "PROTECTED_DIRECTORY",
    "PROTECTED_SECRET_DIRECTORY",
    "ProvisionedIdentity",
    "ProtectedFiles",
    "ProtectedPathState",
    "REGISTRATION_INTENT_FILENAME",
    "REGISTRATION_NOT_PREPARED",
    "REGISTRATION_PREPARED",
    "REGISTRATION_SPENT_UNKNOWN",
    "REGISTER_SIGNER_FIELDS",
    "REGISTER_SIGNER_MESSAGE",
    "REGISTER_SIGNER_TYPEHASH",
    "RegistrationIntent",
    "SESSION_KEY_FILENAME",
    "SESSION_EXPIRATION_SECONDS",
    "VERIFY_SIGNER_FIELDS",
    "VERIFY_SIGNER_TYPEHASH",
    "REGISTRATION_SPENT_FILENAME",
    "PROVISIONED",
    "build_register_signer_request",
    "build_register_signer_typed_data",
    "build_verify_signer_typed_data",
    "claim_registration_intent",
    "inspect_protected_files",
    "load_registration_intent",
    "main",
    "onboard_mainnet",
    "prepare_registration_intent",
    "provision_mainnet_session_signer",
    "protected_paths",
    "read_provisioned_identity",
]


if __name__ == "__main__":
    raise SystemExit(main())
