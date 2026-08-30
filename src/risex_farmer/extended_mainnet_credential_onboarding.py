"""Protected local Extended mainnet credential onboarding.

This module is an operator-only boundary, not a runtime mode.  It can be run
from a visible terminal with ``python -m risex_farmer.extended_mainnet_credential_onboarding``.
The default input function is :func:`getpass.getpass`; no credential value is
accepted through command-line arguments, environment variables, or task/chat
messages.

Extended's official API contract separates the two retained credentials:
``X-Api-Key`` is sufficient for read-only account access, while writes also
require a Stark signature made with the account's Stark private key.  This
slice only persists and discovers those credentials.  It does not create a
client, prepare a payload, sign, connect to the venue, or dispatch a request.
"""

from __future__ import annotations

import argparse
import errno
import getpass
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
from dataclasses import dataclass
from typing import Any, Callable, Mapping


VENUE = "Extended"
ENVIRONMENT = "MAINNET"
PROVISIONED = "PROVISIONED"
BLOCKED = "BLOCKED"
INSPECTION_READY = "PROTECTED_FILES_READY"
INSPECTION_MISSING = "PROTECTED_FILES_MISSING"
INSPECTION_BLOCKED = "PROTECTED_FILES_BLOCKED"

# This is the only production path.  It is intentionally venue-local and is
# not shared with paper, testnet, or the older readiness evidence module.
PROTECTED_DIRECTORY = (
    Path.home() / ".config" / "risex-farmer" / "extended-mainnet-credentials"
)
IDENTITY_FILENAME = "identity.json"
API_KEY_FILENAME = "api-key"
STARK_PRIVATE_KEY_FILENAME = "stark-private-key"
PROTECTED_FILENAMES = (
    IDENTITY_FILENAME,
    API_KEY_FILENAME,
    STARK_PRIVATE_KEY_FILENAME,
)

PROVISIONING_SCHEMA_VERSION = 1
PROTECTED_DIRECTORY_MODE = 0o700
PROTECTED_FILE_MODE = 0o600
MAX_API_KEY_BYTES = 512
MAX_STARK_PRIVATE_KEY_BYTES = 128
MAX_IDENTITY_FILE_BYTES = 4096
MAX_PUBLIC_INPUT_CHARS = 256
MAX_DECIMAL_IDENTIFIER = 2**63 - 1
MAX_ACCOUNT_INDEX = 2**31 - 1

# The current official x10 SDK delegates Stark public-key derivation to this
# native dependency.  Keep this bound local and lazy: the normal paper import
# surface never loads the SDK or any crypto library.
STARK_EC_ORDER = 0x800000000000010FFFFFFFFFFFFFFFFB781126DCAE7B2321E66A241ADC64D2F
MAX_STARK_PRIVATE_KEY_HEX_DIGITS = (STARK_EC_ORDER.bit_length() + 3) // 4


class CredentialOnboardingError(ValueError):
    """Sanitized local failure; its text is always a fixed code."""

    def __init__(self, code: str) -> None:
        if type(code) is not str or not code or any(ord(char) < 33 for char in code):
            raise ValueError("invalid onboarding error code")
        self.code = code
        super().__init__(code)


def _zeroize(value: bytearray) -> None:
    for index in range(len(value)):
        value[index] = 0
    value.clear()


def _fingerprint(value: bytearray) -> str:
    """Return sanitized metadata, never the credential itself."""

    return hashlib.sha256(bytes(value)).hexdigest()


def _canonical_decimal_identifier(value: Any, field: str, *, maximum: int) -> int:
    if type(value) is not str or not value or len(value) > MAX_PUBLIC_INPUT_CHARS:
        raise CredentialOnboardingError(f"{field.upper()}_INVALID")
    if not value.isdecimal() or (len(value) > 1 and value.startswith("0")):
        raise CredentialOnboardingError(f"{field.upper()}_INVALID")
    try:
        parsed = int(value, 10)
    except ValueError:
        raise CredentialOnboardingError(f"{field.upper()}_INVALID") from None
    if parsed < 0 or parsed > maximum:
        raise CredentialOnboardingError(f"{field.upper()}_INVALID")
    return parsed


def _canonical_l2_key(value: Any) -> str:
    if type(value) is not str or not value or len(value) > MAX_PUBLIC_INPUT_CHARS:
        raise CredentialOnboardingError("L2_KEY_INVALID")
    if not value.startswith("0x") or not value[2:]:
        raise CredentialOnboardingError("L2_KEY_INVALID")
    if any(char not in "0123456789abcdefABCDEF" for char in value[2:]):
        raise CredentialOnboardingError("L2_KEY_INVALID")
    try:
        parsed = int(value[2:], 16)
    except ValueError:
        raise CredentialOnboardingError("L2_KEY_INVALID") from None
    if parsed <= 0:
        raise CredentialOnboardingError("L2_KEY_INVALID")
    return f"0x{parsed:x}"


@dataclass(frozen=True)
class ExtendedPublicIdentity:
    """Exact public account binding returned by Extended account endpoints."""

    account_id: int
    account_index: int
    l2_key: str
    l2_vault: int

    @classmethod
    def from_inputs(
        cls,
        account_id: Any,
        account_index: Any,
        l2_key: Any,
        l2_vault: Any,
    ) -> "ExtendedPublicIdentity":
        return cls(
            account_id=_canonical_decimal_identifier(
                account_id, "account_id", maximum=MAX_DECIMAL_IDENTIFIER
            ),
            account_index=_canonical_decimal_identifier(
                account_index, "account_index", maximum=MAX_ACCOUNT_INDEX
            ),
            l2_key=_canonical_l2_key(l2_key),
            l2_vault=_canonical_decimal_identifier(
                l2_vault, "l2_vault", maximum=MAX_DECIMAL_IDENTIFIER
            ),
        )

    @classmethod
    def from_metadata(cls, value: Any) -> "ExtendedPublicIdentity":
        if not isinstance(value, Mapping) or set(value) != {
            "account_id", "account_index", "l2_key", "l2_vault"
        }:
            raise CredentialOnboardingError("IDENTITY_METADATA_INVALID")
        try:
            account_id = int(value["account_id"])
            account_index = int(value["account_index"])
            l2_vault = int(value["l2_vault"])
        except (TypeError, ValueError, OverflowError):
            raise CredentialOnboardingError("IDENTITY_METADATA_INVALID") from None
        if any(type(value[key]) is not int for key in ("account_id", "account_index", "l2_vault")):
            raise CredentialOnboardingError("IDENTITY_METADATA_INVALID")
        if (
            account_id < 0
            or account_id > MAX_DECIMAL_IDENTIFIER
            or account_index < 0
            or account_index > MAX_ACCOUNT_INDEX
            or l2_vault < 0
            or l2_vault > MAX_DECIMAL_IDENTIFIER
        ):
            raise CredentialOnboardingError("IDENTITY_METADATA_INVALID")
        l2_key = _canonical_l2_key(value["l2_key"])
        return cls(account_id, account_index, l2_key, l2_vault)

    def to_metadata(self) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "account_index": self.account_index,
            "l2_key": self.l2_key,
            "l2_vault": self.l2_vault,
        }


def _derive_stark_public_key(private_scalar: int) -> int:
    """Use the official SDK's crypto dependency at the protected boundary."""

    try:
        from fast_stark_crypto import get_public_key
    except (ImportError, ModuleNotFoundError):
        raise CredentialOnboardingError("OFFICIAL_SDK_UNAVAILABLE") from None
    try:
        derived = get_public_key(private_scalar)
    except BaseException:
        raise CredentialOnboardingError("STARK_PUBLIC_KEY_DERIVATION_FAILED") from None
    if type(derived) is not int or derived <= 0:
        raise CredentialOnboardingError("STARK_PUBLIC_KEY_DERIVATION_FAILED")
    return derived


def _validate_stark_private_key(value: bytearray, identity: ExtendedPublicIdentity) -> None:
    try:
        text = bytes(value).decode("ascii")
    except UnicodeDecodeError:
        raise CredentialOnboardingError("STARK_PRIVATE_KEY_INVALID") from None
    if (
        not text.startswith("0x")
        or not text[2:]
        or len(text[2:]) > MAX_STARK_PRIVATE_KEY_HEX_DIGITS
        or any(char not in "0123456789abcdefABCDEF" for char in text[2:])
    ):
        raise CredentialOnboardingError("STARK_PRIVATE_KEY_INVALID")
    try:
        scalar = int(text[2:], 16)
    except ValueError:
        raise CredentialOnboardingError("STARK_PRIVATE_KEY_INVALID") from None
    if not 0 < scalar < STARK_EC_ORDER:
        raise CredentialOnboardingError("STARK_PRIVATE_KEY_INVALID")
    derived = _derive_stark_public_key(scalar)
    if derived != int(identity.l2_key[2:], 16):
        raise CredentialOnboardingError("STARK_PUBLIC_IDENTITY_MISMATCH")


def _secret_bytes(value: Any, *, maximum: int, code: str) -> bytearray:
    if type(value) is not str or not value or value != value.strip():
        raise CredentialOnboardingError(code)
    if any(ord(char) < 33 or ord(char) > 126 for char in value):
        raise CredentialOnboardingError(code)
    try:
        result = bytearray(value.encode("ascii"))
    except UnicodeEncodeError:
        raise CredentialOnboardingError(code) from None
    if not 0 < len(result) <= maximum:
        _zeroize(result)
        raise CredentialOnboardingError(code)
    return result


@dataclass(frozen=True)
class ProtectedFileMetadata:
    name: str
    path: str
    present: bool
    protected: bool
    reason: str
    mode: int | None = None
    link_count: int | None = None
    size: int | None = None

    def to_metadata(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "path": self.path,
            "present": self.present,
            "protected": self.protected,
            "reason": self.reason,
            "mode": self.mode,
            "link_count": self.link_count,
            "size": self.size,
        }


@dataclass(frozen=True)
class CredentialInspection:
    status: str
    reason: str
    directory: str
    directory_present: bool
    directory_protected: bool
    directory_mode: int | None
    identity: ExtendedPublicIdentity | None
    api_key_fingerprint: str | None
    stark_private_key_fingerprint: str | None
    files: tuple[ProtectedFileMetadata, ...]

    @property
    def ready(self) -> bool:
        return self.status == INSPECTION_READY

    def to_metadata(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "reason": self.reason,
            "directory": {
                "path": self.directory,
                "present": self.directory_present,
                "protected": self.directory_protected,
                "mode": self.directory_mode,
            },
            "identity": None if self.identity is None else self.identity.to_metadata(),
            "credentials": {
                "api_key": {
                    "access": "READ_ONLY_X_API_KEY",
                    "fingerprint": self.api_key_fingerprint,
                },
                "stark_private_key": {
                    "access": "WRITE_STARK_SIGNATURE_ONLY",
                    "fingerprint": self.stark_private_key_fingerprint,
                },
            },
            "files": [item.to_metadata() for item in self.files],
        }

    def evidence(self) -> str:
        return json.dumps(self.to_metadata(), sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class ProvisioningResult:
    status: str
    reason: str
    inspection: CredentialInspection

    @property
    def provisioned(self) -> bool:
        return self.status == PROVISIONED

    def to_metadata(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "reason": self.reason,
            "inspection": self.inspection.to_metadata(),
        }

    def evidence(self) -> str:
        return json.dumps(self.to_metadata(), sort_keys=True, separators=(",", ":"))


class ProtectedExtendedCredentials:
    """In-memory handle for a discovered credential pair.

    The handle deliberately has no signing or transport methods.  A later
    bounded private-read or write slice may consume its fields and must close
    it in a ``finally`` block.
    """

    def __init__(
        self,
        identity: ExtendedPublicIdentity,
        api_key: bytearray,
        stark_private_key: bytearray,
        api_key_fingerprint: str,
        stark_private_key_fingerprint: str,
    ) -> None:
        self.identity = identity
        self._api_key = api_key
        self._stark_private_key = stark_private_key
        self.api_key_fingerprint = api_key_fingerprint
        self.stark_private_key_fingerprint = stark_private_key_fingerprint
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def api_key(self) -> str:
        if self._closed:
            raise CredentialOnboardingError("CREDENTIAL_HANDLE_CLOSED")
        try:
            return bytes(self._api_key).decode("ascii")
        except UnicodeDecodeError:
            raise CredentialOnboardingError("API_KEY_INVALID") from None

    def stark_private_key(self) -> str:
        if self._closed:
            raise CredentialOnboardingError("CREDENTIAL_HANDLE_CLOSED")
        try:
            return bytes(self._stark_private_key).decode("ascii")
        except UnicodeDecodeError:
            raise CredentialOnboardingError("STARK_PRIVATE_KEY_INVALID") from None

    def __repr__(self) -> str:
        state = "closed" if self._closed else "open"
        return f"<ProtectedExtendedCredentials {state} identity={self.identity!r}>"

    __str__ = __repr__

    def close(self) -> None:
        _zeroize(self._api_key)
        _zeroize(self._stark_private_key)
        self._closed = True

    def __enter__(self) -> "ProtectedExtendedCredentials":
        return self

    def __exit__(self, _type: Any, _value: Any, _traceback: Any) -> None:
        self.close()


def protected_paths() -> Mapping[str, Path]:
    """Return fixed paths without opening or reading any file."""

    return {
        "directory": PROTECTED_DIRECTORY,
        "identity": PROTECTED_DIRECTORY / IDENTITY_FILENAME,
        "api_key": PROTECTED_DIRECTORY / API_KEY_FILENAME,
        "stark_private_key": PROTECTED_DIRECTORY / STARK_PRIVATE_KEY_FILENAME,
    }


def _path_components(path: Path) -> tuple[str, ...]:
    if not path.is_absolute():
        raise CredentialOnboardingError("PROTECTED_PATH_NOT_ABSOLUTE")
    if path.anchor != os.sep:
        raise CredentialOnboardingError("PROTECTED_PATH_NOT_ABSOLUTE")
    components = path.parts[1:]
    if not components or any(component in {"", ".", ".."} for component in components):
        raise CredentialOnboardingError("PROTECTED_PATH_INVALID")
    return components


def _directory_open_flags() -> int:
    if not hasattr(os, "O_DIRECTORY") or not hasattr(os, "O_NOFOLLOW"):
        raise CredentialOnboardingError("PROTECTED_DIRECTORY_FLAGS_UNAVAILABLE")
    return os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW


def _directory_open_error(
    error: OSError,
    *,
    root: bool = False,
    parent_fd: int | None = None,
    component: str | None = None,
) -> CredentialOnboardingError:
    if parent_fd is not None and component is not None:
        try:
            info = os.stat(component, dir_fd=parent_fd, follow_symlinks=False)
        except OSError:
            info = None
        if info is not None:
            if stat.S_ISLNK(info.st_mode):
                return CredentialOnboardingError("PROTECTED_DIRECTORY_SYMLINK")
            if not stat.S_ISDIR(info.st_mode):
                return CredentialOnboardingError("PROTECTED_DIRECTORY_NOT_DIRECTORY")
    if error.errno == errno.ELOOP:
        return CredentialOnboardingError("PROTECTED_DIRECTORY_SYMLINK")
    if error.errno == errno.ENOTDIR:
        return CredentialOnboardingError("PROTECTED_DIRECTORY_NOT_DIRECTORY")
    if isinstance(error, FileNotFoundError):
        return CredentialOnboardingError("PROTECTED_DIRECTORY_MISSING")
    return CredentialOnboardingError(
        "PROTECTED_DIRECTORY_ROOT_UNREADABLE" if root else "PROTECTED_DIRECTORY_UNREADABLE"
    )


def _validate_directory_component(descriptor: int) -> os.stat_result:
    try:
        info = os.fstat(descriptor)
    except OSError:
        raise CredentialOnboardingError("PROTECTED_DIRECTORY_UNREADABLE") from None
    if not stat.S_ISDIR(info.st_mode):
        raise CredentialOnboardingError("PROTECTED_DIRECTORY_NOT_DIRECTORY")
    return info


def _validate_final_directory(descriptor: int) -> os.stat_result:
    info = _validate_directory_component(descriptor)
    if info.st_uid != os.getuid():
        raise CredentialOnboardingError("PROTECTED_DIRECTORY_OWNER_NOT_CURRENT_USER")
    if stat.S_IMODE(info.st_mode) != PROTECTED_DIRECTORY_MODE:
        raise CredentialOnboardingError("PROTECTED_DIRECTORY_MODE_NOT_0700")
    return info


def _walk_protected_directory(*, create: bool) -> int:
    """Open the fixed directory through retained descriptor-relative parents."""

    components = _path_components(PROTECTED_DIRECTORY)
    flags = _directory_open_flags()
    current = -1
    try:
        try:
            current = os.open(os.sep, flags)
        except OSError as exc:
            raise _directory_open_error(exc, root=True) from None
        try:
            _validate_directory_component(current)
        except CredentialOnboardingError:
            raise CredentialOnboardingError("PROTECTED_DIRECTORY_ROOT_INVALID") from None

        for index, component in enumerate(components):
            final = index == len(components) - 1
            child = -1
            created = False
            keep_child = False
            try:
                try:
                    child = os.open(component, flags, dir_fd=current)
                except FileNotFoundError as exc:
                    if not create:
                        raise _directory_open_error(
                            exc, parent_fd=current, component=component
                        ) from None
                    try:
                        os.mkdir(component, PROTECTED_DIRECTORY_MODE, dir_fd=current)
                        created = True
                    except FileExistsError:
                        pass
                    except OSError:
                        raise CredentialOnboardingError(
                            "PROTECTED_DIRECTORY_CREATE_FAILED"
                        ) from None
                    try:
                        child = os.open(component, flags, dir_fd=current)
                    except OSError as exc:
                        raise _directory_open_error(
                            exc, parent_fd=current, component=component
                        ) from None
                except OSError as exc:
                    raise _directory_open_error(
                        exc, parent_fd=current, component=component
                    ) from None

                try:
                    info = _validate_directory_component(child)
                except CredentialOnboardingError:
                    raise
                if not final and info.st_uid not in {0, os.getuid()}:
                    raise CredentialOnboardingError(
                        "PROTECTED_DIRECTORY_PARENT_OWNER_NOT_TRUSTED"
                    )
                if created:
                    try:
                        os.fchmod(child, PROTECTED_DIRECTORY_MODE)
                    except OSError:
                        raise CredentialOnboardingError(
                            "PROTECTED_DIRECTORY_MODE_SET_FAILED"
                        ) from None
                    info = _validate_directory_component(child)
                    if stat.S_IMODE(info.st_mode) != PROTECTED_DIRECTORY_MODE:
                        raise CredentialOnboardingError(
                            "PROTECTED_DIRECTORY_MODE_NOT_0700"
                        )
                keep_child = True
            finally:
                if not keep_child and child >= 0:
                    try:
                        os.close(child)
                    except OSError:
                        pass
                if current >= 0:
                    try:
                        os.close(current)
                    except OSError:
                        pass
                    current = -1
            current = child
            child = -1
        if current < 0:
            raise CredentialOnboardingError("PROTECTED_DIRECTORY_INVALID")
        return current
    except BaseException:
        if current >= 0:
            try:
                os.close(current)
            except OSError:
                pass
        raise


def _directory_observation() -> tuple[bool, bool, str, int | None]:
    descriptor = -1
    try:
        descriptor = _walk_protected_directory(create=False)
        info = os.fstat(descriptor)
        mode = stat.S_IMODE(info.st_mode)
        if info.st_uid != os.getuid():
            return True, False, "PROTECTED_DIRECTORY_OWNER_NOT_CURRENT_USER", mode
        if mode != PROTECTED_DIRECTORY_MODE:
            return True, False, "PROTECTED_DIRECTORY_MODE_NOT_0700", mode
        return True, True, "PROTECTED_DIRECTORY_OK", mode
    except CredentialOnboardingError as exc:
        return (
            False,
            False,
            exc.code,
            None,
        )
    finally:
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except OSError:
                pass


def _file_observation(
    name: str,
    path: Path,
    directory_ready: bool,
    directory_fd: int | None = None,
) -> ProtectedFileMetadata:
    if not directory_ready or directory_fd is None:
        return ProtectedFileMetadata(
            name=name,
            path=str(path),
            present=False,
            protected=False,
            reason="PROTECTED_DIRECTORY_NOT_READY",
    )
    try:
        info = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return ProtectedFileMetadata(name, str(path), False, False, "PROTECTED_FILE_MISSING")
    except OSError:
        return ProtectedFileMetadata(name, str(path), False, False, "PROTECTED_FILE_UNREADABLE")
    mode = stat.S_IMODE(info.st_mode)
    common = {
        "name": name,
        "path": str(path),
        "present": True,
        "mode": mode,
        "link_count": info.st_nlink,
        "size": info.st_size,
    }
    if stat.S_ISLNK(info.st_mode):
        reason = "PROTECTED_FILE_SYMLINK"
    elif not stat.S_ISREG(info.st_mode):
        reason = "PROTECTED_FILE_NOT_REGULAR"
    elif info.st_uid != os.getuid():
        reason = "PROTECTED_FILE_OWNER_NOT_CURRENT_USER"
    elif info.st_nlink != 1:
        reason = "PROTECTED_FILE_HARDLINK"
    elif mode != PROTECTED_FILE_MODE:
        reason = "PROTECTED_FILE_MODE_NOT_0600"
    elif info.st_size <= 0:
        reason = "PROTECTED_FILE_EMPTY"
    elif info.st_size > {
        IDENTITY_FILENAME: MAX_IDENTITY_FILE_BYTES,
        API_KEY_FILENAME: MAX_API_KEY_BYTES,
        STARK_PRIVATE_KEY_FILENAME: MAX_STARK_PRIVATE_KEY_BYTES,
    }[name]:
        reason = "PROTECTED_FILE_TOO_LARGE"
    else:
        reason = "PROTECTED_FILE_OK"
    return ProtectedFileMetadata(
        protected=reason == "PROTECTED_FILE_OK", reason=reason, **common
    )


def _directory_entries() -> tuple[str, ...]:
    descriptor = -1
    try:
        descriptor = _walk_protected_directory(create=False)
        return _directory_entries_from_fd(descriptor)
    finally:
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except OSError:
                pass


def _directory_entries_from_fd(directory_fd: int) -> tuple[str, ...]:
    try:
        return tuple(sorted(os.listdir(directory_fd)))
    except OSError:
        raise CredentialOnboardingError("PROTECTED_DIRECTORY_UNREADABLE") from None


def _read_metadata_file(directory_fd: int) -> bytearray:
    """Read only the non-secret metadata manifest."""

    descriptor = -1
    result = bytearray()
    try:
        descriptor = os.open(
            IDENTITY_FILENAME,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=directory_fd,
        )
        details = os.fstat(descriptor)
        if (
            not stat.S_ISREG(details.st_mode)
            or details.st_uid != os.getuid()
            or details.st_nlink != 1
            or stat.S_IMODE(details.st_mode) != PROTECTED_FILE_MODE
            or details.st_size <= 0
            or details.st_size > MAX_IDENTITY_FILE_BYTES
        ):
            raise CredentialOnboardingError("IDENTITY_METADATA_FILE_INVALID")
        remaining = details.st_size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1024))
            if not chunk:
                raise CredentialOnboardingError("IDENTITY_METADATA_FILE_INVALID")
            result.extend(chunk)
            remaining -= len(chunk)
        final = os.fstat(descriptor)
        if (
            not stat.S_ISREG(final.st_mode)
            or final.st_dev != details.st_dev
            or final.st_ino != details.st_ino
            or final.st_size != details.st_size
            or final.st_uid != os.getuid()
            or final.st_nlink != 1
            or stat.S_IMODE(final.st_mode) != PROTECTED_FILE_MODE
        ):
            raise CredentialOnboardingError("IDENTITY_METADATA_FILE_CHANGED")
        return result
    except CredentialOnboardingError:
        _zeroize(result)
        raise
    except BaseException:
        _zeroize(result)
        raise CredentialOnboardingError("IDENTITY_METADATA_FILE_UNAVAILABLE") from None
    finally:
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except OSError:
                pass


def _metadata_values(raw: bytearray) -> tuple[ExtendedPublicIdentity, str, str]:
    try:
        value = json.loads(bytes(raw).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise CredentialOnboardingError("IDENTITY_METADATA_INVALID") from None
    if not isinstance(value, Mapping) or set(value) != {
        "schema_version", "venue", "environment", "identity",
        "api_key_fingerprint", "stark_private_key_fingerprint",
        "credential_contract",
    }:
        raise CredentialOnboardingError("IDENTITY_METADATA_INVALID")
    if (
        type(value["schema_version"]) is not int
        or value["schema_version"] != PROVISIONING_SCHEMA_VERSION
        or value["venue"] != VENUE
        or value["environment"] != ENVIRONMENT
    ):
        raise CredentialOnboardingError("IDENTITY_METADATA_INVALID")
    identity = ExtendedPublicIdentity.from_metadata(value["identity"])
    contract = value["credential_contract"]
    if not isinstance(contract, Mapping) or set(contract) != {"api_key", "stark_private_key"}:
        raise CredentialOnboardingError("IDENTITY_METADATA_INVALID")
    if (
        contract["api_key"] != "READ_ONLY_X_API_KEY"
        or contract["stark_private_key"] != "WRITE_STARK_SIGNATURE_ONLY"
    ):
        raise CredentialOnboardingError("IDENTITY_METADATA_INVALID")
    fingerprints = []
    for key in ("api_key_fingerprint", "stark_private_key_fingerprint"):
        fingerprint = value[key]
        if (
            type(fingerprint) is not str
            or len(fingerprint) != 64
            or any(char not in "0123456789abcdef" for char in fingerprint)
        ):
            raise CredentialOnboardingError("IDENTITY_METADATA_INVALID")
        fingerprints.append(fingerprint)
    return identity, fingerprints[0], fingerprints[1]


def _inspection_failure(reason: str) -> CredentialInspection:
    path_map = protected_paths()
    files = tuple(
        ProtectedFileMetadata(
            name=name,
            path=str(path_map[key]),
            present=False,
            protected=False,
            reason=reason,
        )
        for key, name in (
            ("identity", IDENTITY_FILENAME),
            ("api_key", API_KEY_FILENAME),
            ("stark_private_key", STARK_PRIVATE_KEY_FILENAME),
        )
    )
    return CredentialInspection(
        status=INSPECTION_BLOCKED,
        reason=reason,
        directory=str(PROTECTED_DIRECTORY),
        directory_present=False,
        directory_protected=False,
        directory_mode=None,
        identity=None,
        api_key_fingerprint=None,
        stark_private_key_fingerprint=None,
        files=files,
    )


def _inspect_protected_credentials() -> CredentialInspection:
    """Inspect protected metadata only; secret file bytes are never read."""

    path_map = protected_paths()
    directory_fd = -1
    present = False
    protected = False
    mode: int | None = None
    reason = "PROTECTED_DIRECTORY_MISSING"
    files = tuple(
        _file_observation(name, path_map[key], False)
        for key, name in (
            ("identity", IDENTITY_FILENAME),
            ("api_key", API_KEY_FILENAME),
            ("stark_private_key", STARK_PRIVATE_KEY_FILENAME),
        )
    )
    identity: ExtendedPublicIdentity | None = None
    api_fingerprint: str | None = None
    stark_fingerprint: str | None = None
    inspection_reason = reason
    status = INSPECTION_MISSING
    try:
        try:
            directory_fd = _walk_protected_directory(create=False)
        except CredentialOnboardingError as exc:
            reason = exc.code
            inspection_reason = reason
            status = (
                INSPECTION_MISSING
                if reason in {"PROTECTED_DIRECTORY_MISSING", "PROTECTED_DIRECTORY_PARENT_MISSING"}
                else INSPECTION_BLOCKED
            )
        else:
            present = True
            info = _validate_directory_component(directory_fd)
            mode = stat.S_IMODE(info.st_mode)
            if info.st_uid != os.getuid():
                inspection_reason = "PROTECTED_DIRECTORY_OWNER_NOT_CURRENT_USER"
                status = INSPECTION_BLOCKED
            elif mode != PROTECTED_DIRECTORY_MODE:
                inspection_reason = "PROTECTED_DIRECTORY_MODE_NOT_0700"
                status = INSPECTION_BLOCKED
            else:
                protected = True
                reason = "PROTECTED_DIRECTORY_OK"
                inspection_reason = reason
                files = tuple(
                    _file_observation(name, path_map[key], True, directory_fd)
                    for key, name in (
                        ("identity", IDENTITY_FILENAME),
                        ("api_key", API_KEY_FILENAME),
                        ("stark_private_key", STARK_PRIVATE_KEY_FILENAME),
                    )
                )
                try:
                    entries = _directory_entries_from_fd(directory_fd)
                except CredentialOnboardingError as exc:
                    status = INSPECTION_BLOCKED
                    inspection_reason = exc.code
                else:
                    unexpected = set(entries) - set(PROTECTED_FILENAMES)
                    if unexpected:
                        status = INSPECTION_BLOCKED
                        inspection_reason = "PROTECTED_DIRECTORY_UNEXPECTED_ENTRY"
                    elif not all(item.protected for item in files):
                        status = INSPECTION_BLOCKED
                        inspection_reason = next(
                            item.reason for item in files if not item.protected
                        )
                    else:
                        raw = bytearray()
                        try:
                            raw = _read_metadata_file(directory_fd)
                            identity, api_fingerprint, stark_fingerprint = _metadata_values(raw)
                        except CredentialOnboardingError as exc:
                            status = INSPECTION_BLOCKED
                            inspection_reason = exc.code
                        finally:
                            _zeroize(raw)
                        if identity is not None:
                            status = INSPECTION_READY
                            inspection_reason = "PROTECTED_FILES_READY"
    finally:
        if directory_fd >= 0:
            try:
                os.close(directory_fd)
            except OSError:
                pass
    return CredentialInspection(
        status=status,
        reason=inspection_reason,
        directory=str(PROTECTED_DIRECTORY),
        directory_present=present,
        directory_protected=protected,
        directory_mode=mode,
        identity=identity,
        api_key_fingerprint=api_fingerprint,
        stark_private_key_fingerprint=stark_fingerprint,
        files=files,
    )


def inspect_protected_credentials() -> CredentialInspection:
    """Return sanitized inspection evidence even if local inspection fails."""

    try:
        return _inspect_protected_credentials()
    except BaseException:
        return _inspection_failure("PROTECTED_INSPECTION_FAILED")


def _ensure_protected_directory() -> None:
    descriptor = -1
    try:
        descriptor = _walk_protected_directory(create=True)
        _validate_final_directory(descriptor)
    finally:
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except OSError:
                pass


def _open_directory(*, create: bool = False) -> int:
    descriptor = -1
    try:
        descriptor = _walk_protected_directory(create=create)
        _validate_final_directory(descriptor)
        return descriptor
    except BaseException:
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except OSError:
                pass
        raise


def _write_secure_file(directory_fd: int, filename: str, payload: bytearray, maximum: int) -> None:
    descriptor = -1
    created = False
    complete = False
    try:
        try:
            descriptor = os.open(
                filename,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_NOFOLLOW", 0),
                PROTECTED_FILE_MODE,
                dir_fd=directory_fd,
            )
        except FileExistsError:
            raise CredentialOnboardingError("PROTECTED_PATH_ALREADY_EXISTS") from None
        except OSError:
            raise CredentialOnboardingError("PROTECTED_FILE_CREATE_FAILED") from None
        created = True
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_nlink != 1
        ):
            raise CredentialOnboardingError("PROTECTED_FILE_NOT_SAFE")
        try:
            os.fchmod(descriptor, PROTECTED_FILE_MODE)
        except OSError:
            raise CredentialOnboardingError("PROTECTED_FILE_MODE_SET_FAILED") from None
        info = os.fstat(descriptor)
        if stat.S_IMODE(info.st_mode) != PROTECTED_FILE_MODE:
            raise CredentialOnboardingError("PROTECTED_FILE_NOT_SAFE")
        if len(payload) <= 0 or len(payload) > maximum:
            raise CredentialOnboardingError("PROTECTED_FILE_SIZE_INVALID")
        offset = 0
        while offset < len(payload):
            written = os.write(descriptor, payload[offset:])
            if written <= 0:
                raise CredentialOnboardingError("PROTECTED_FILE_WRITE_INCOMPLETE")
            offset += written
        os.fsync(descriptor)
        final = os.fstat(descriptor)
        if (
            not stat.S_ISREG(final.st_mode)
            or final.st_uid != os.getuid()
            or final.st_nlink != 1
            or stat.S_IMODE(final.st_mode) != PROTECTED_FILE_MODE
            or final.st_size != len(payload)
        ):
            raise CredentialOnboardingError("PROTECTED_FILE_METADATA_CHANGED")
        complete = True
    except CredentialOnboardingError:
        raise
    except BaseException:
        raise CredentialOnboardingError("PROTECTED_FILE_WRITE_FAILED") from None
    finally:
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except OSError:
                pass
        if created and not complete:
            try:
                os.unlink(filename, dir_fd=directory_fd)
            except OSError:
                pass


def _sync_directory(directory_fd: int) -> None:
    try:
        os.fsync(directory_fd)
    except OSError:
        raise CredentialOnboardingError("PROTECTED_DIRECTORY_SYNC_FAILED") from None


def _cleanup_created(directory_fd: int, created: list[str]) -> None:
    for filename in reversed(created):
        try:
            os.unlink(filename, dir_fd=directory_fd)
        except OSError:
            pass
    try:
        os.fsync(directory_fd)
    except OSError:
        pass


def _metadata_payload(
    identity: ExtendedPublicIdentity,
    api_fingerprint: str,
    stark_fingerprint: str,
) -> bytearray:
    value = {
        "schema_version": PROVISIONING_SCHEMA_VERSION,
        "venue": VENUE,
        "environment": ENVIRONMENT,
        "identity": identity.to_metadata(),
        "api_key_fingerprint": api_fingerprint,
        "stark_private_key_fingerprint": stark_fingerprint,
        "credential_contract": {
            "api_key": "READ_ONLY_X_API_KEY",
            "stark_private_key": "WRITE_STARK_SIGNATURE_ONLY",
        },
    }
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    if len(encoded) > MAX_IDENTITY_FILE_BYTES:
        raise CredentialOnboardingError("IDENTITY_METADATA_TOO_LARGE")
    return bytearray(encoded)


def _input(input_fn: Callable[[str], str], prompt: str) -> str:
    try:
        value = input_fn(prompt)
    except BaseException:
        raise CredentialOnboardingError("INPUT_CANCELLED") from None
    if type(value) is not str:
        raise CredentialOnboardingError("INPUT_INVALID")
    return value


def provision_protected_credentials(
    input_fn: Callable[[str], str] | None = None,
) -> ProvisioningResult:
    """Atomically provision Extended identity/API/Stark files.

    ``input_fn`` exists only as a synthetic test seam.  Production callers
    omit it and receive hidden terminal input for every field.
    """

    input_fn = getpass.getpass if input_fn is None else input_fn
    before = inspect_protected_credentials()
    if before.reason not in {
        "PROTECTED_DIRECTORY_MISSING",
        "PROTECTED_DIRECTORY_PARENT_MISSING",
        "PROTECTED_DIRECTORY_OK",
        "PROTECTED_FILES_READY",
    }:
        return ProvisioningResult(BLOCKED, before.reason, before)
    if before.directory_present and not before.directory_protected:
        return ProvisioningResult(BLOCKED, before.reason, before)
    if before.directory_present and any(item.present for item in before.files):
        return ProvisioningResult(BLOCKED, "PROTECTED_PATH_ALREADY_EXISTS", before)

    api_key = bytearray()
    stark_private_key = bytearray()
    metadata_payload = bytearray()
    directory_fd = -1
    created: list[str] = []
    try:
        identity = ExtendedPublicIdentity.from_inputs(
            _input(input_fn, "Extended account ID (hidden public metadata): "),
            _input(input_fn, "Extended account index (hidden public metadata): "),
            _input(input_fn, "Extended l2 public key (hidden public metadata): "),
            _input(input_fn, "Extended l2 vault (hidden public metadata): "),
        )
        api_key = _secret_bytes(
            _input(input_fn, "Extended read-only API key (hidden): "),
            maximum=MAX_API_KEY_BYTES,
            code="API_KEY_INVALID",
        )
        stark_private_key = _secret_bytes(
            _input(input_fn, "Extended Stark signing key (hidden): "),
            maximum=MAX_STARK_PRIVATE_KEY_BYTES,
            code="STARK_PRIVATE_KEY_INVALID",
        )
        if api_key == stark_private_key:
            raise CredentialOnboardingError("CREDENTIALS_NOT_DISTINCT")
        _validate_stark_private_key(stark_private_key, identity)
        api_fingerprint = _fingerprint(api_key)
        stark_fingerprint = _fingerprint(stark_private_key)
        metadata_payload = _metadata_payload(
            identity, api_fingerprint, stark_fingerprint
        )
        directory_fd = _open_directory(create=True)
        if _directory_entries_from_fd(directory_fd):
            raise CredentialOnboardingError("PROTECTED_PATH_ALREADY_EXISTS")
        _write_secure_file(
            directory_fd, API_KEY_FILENAME, api_key, MAX_API_KEY_BYTES
        )
        created.append(API_KEY_FILENAME)
        _write_secure_file(
            directory_fd,
            STARK_PRIVATE_KEY_FILENAME,
            stark_private_key,
            MAX_STARK_PRIVATE_KEY_BYTES,
        )
        created.append(STARK_PRIVATE_KEY_FILENAME)
        _write_secure_file(
            directory_fd,
            IDENTITY_FILENAME,
            metadata_payload,
            MAX_IDENTITY_FILE_BYTES,
        )
        created.append(IDENTITY_FILENAME)
        _sync_directory(directory_fd)
    except CredentialOnboardingError as exc:
        if directory_fd >= 0:
            _cleanup_created(directory_fd, created)
        result = ProvisioningResult(
            BLOCKED, exc.code, inspect_protected_credentials()
        )
        return result
    except BaseException:
        if directory_fd >= 0:
            _cleanup_created(directory_fd, created)
        return ProvisioningResult(
            BLOCKED, "PROTECTED_PROVISIONING_FAILED", inspect_protected_credentials()
        )
    finally:
        if directory_fd >= 0:
            try:
                os.close(directory_fd)
            except OSError:
                pass
        _zeroize(api_key)
        _zeroize(stark_private_key)
        _zeroize(metadata_payload)
    after = inspect_protected_credentials()
    return ProvisioningResult(PROVISIONED, "PROTECTED_FILES_CREATED", after)


def _read_secret_file(directory_fd: int, filename: str, maximum: int) -> bytearray:
    descriptor = -1
    result = bytearray()
    try:
        try:
            descriptor = os.open(
                filename,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=directory_fd,
            )
        except OSError:
            raise CredentialOnboardingError("PROTECTED_FILE_UNAVAILABLE") from None
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or before.st_nlink != 1
            or stat.S_IMODE(before.st_mode) != PROTECTED_FILE_MODE
            or before.st_size <= 0
            or before.st_size > maximum
        ):
            raise CredentialOnboardingError("PROTECTED_FILE_NOT_SAFE")
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1024))
            if not chunk:
                raise CredentialOnboardingError("PROTECTED_FILE_READ_INCOMPLETE")
            result.extend(chunk)
            remaining -= len(chunk)
        after = os.fstat(descriptor)
        if (
            not stat.S_ISREG(after.st_mode)
            or after.st_dev != before.st_dev
            or after.st_ino != before.st_ino
            or after.st_size != before.st_size
            or after.st_uid != os.getuid()
            or after.st_nlink != 1
            or stat.S_IMODE(after.st_mode) != PROTECTED_FILE_MODE
        ):
            raise CredentialOnboardingError("PROTECTED_FILE_CHANGED")
        return result
    except CredentialOnboardingError:
        _zeroize(result)
        raise
    except BaseException:
        _zeroize(result)
        raise CredentialOnboardingError("PROTECTED_FILE_READ_FAILED") from None
    finally:
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except OSError:
                pass


def discover_protected_credentials() -> ProtectedExtendedCredentials:
    """Discover the persisted pair after a restart without emitting secrets."""

    inspection = inspect_protected_credentials()
    if not inspection.ready:
        raise CredentialOnboardingError(inspection.reason)
    directory_fd = -1
    api_key = bytearray()
    stark_private_key = bytearray()
    try:
        directory_fd = _open_directory()
        if set(_directory_entries_from_fd(directory_fd)) != set(PROTECTED_FILENAMES):
            raise CredentialOnboardingError("PROTECTED_DIRECTORY_CONTENT_CHANGED")
        api_key = _read_secret_file(
            directory_fd, API_KEY_FILENAME, MAX_API_KEY_BYTES
        )
        stark_private_key = _read_secret_file(
            directory_fd,
            STARK_PRIVATE_KEY_FILENAME,
            MAX_STARK_PRIVATE_KEY_BYTES,
        )
        if api_key == stark_private_key:
            raise CredentialOnboardingError("CREDENTIALS_NOT_DISTINCT")
        if _fingerprint(api_key) != inspection.api_key_fingerprint:
            raise CredentialOnboardingError("API_KEY_FINGERPRINT_MISMATCH")
        if _fingerprint(stark_private_key) != inspection.stark_private_key_fingerprint:
            raise CredentialOnboardingError("STARK_PRIVATE_KEY_FINGERPRINT_MISMATCH")
        if inspection.identity is None:
            raise CredentialOnboardingError("IDENTITY_METADATA_INVALID")
        _validate_stark_private_key(stark_private_key, inspection.identity)
        return ProtectedExtendedCredentials(
            inspection.identity,
            api_key,
            stark_private_key,
            inspection.api_key_fingerprint or "",
            inspection.stark_private_key_fingerprint or "",
        )
    except CredentialOnboardingError:
        _zeroize(api_key)
        _zeroize(stark_private_key)
        raise
    except BaseException:
        _zeroize(api_key)
        _zeroize(stark_private_key)
        raise CredentialOnboardingError("PROTECTED_DISCOVERY_FAILED") from None
    finally:
        if directory_fd >= 0:
            try:
                os.close(directory_fd)
            except OSError:
                pass


class _SafeArgumentParser(argparse.ArgumentParser):
    def error(self, _message: str) -> None:
        raise CredentialOnboardingError("CLI_ARGUMENTS_INVALID")


def _parser() -> argparse.ArgumentParser:
    parser = _SafeArgumentParser(
        prog="python -m risex_farmer.extended_mainnet_credential_onboarding",
        description="Protected local Extended mainnet credential onboarding",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("provision", help="create protected files using hidden input")
    commands.add_parser("inspect", help="show sanitized metadata and file safety only")
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
    except CredentialOnboardingError as exc:
        print(
            json.dumps({"status": BLOCKED, "reason": exc.code}, sort_keys=True),
            file=sys.stderr,
        )
        return 2
    if args.command == "inspect":
        print(inspect_protected_credentials().evidence())
        return 0
    result = provision_protected_credentials()
    print(result.evidence())
    return 0 if result.provisioned else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "API_KEY_FILENAME",
    "BLOCKED",
    "CredentialInspection",
    "CredentialOnboardingError",
    "ENVIRONMENT",
    "ExtendedPublicIdentity",
    "IDENTITY_FILENAME",
    "INSPECTION_BLOCKED",
    "INSPECTION_MISSING",
    "INSPECTION_READY",
    "MAX_API_KEY_BYTES",
    "MAX_STARK_PRIVATE_KEY_BYTES",
    "PROTECTED_DIRECTORY",
    "PROTECTED_DIRECTORY_MODE",
    "PROTECTED_FILE_MODE",
    "PROVISIONED",
    "ProtectedExtendedCredentials",
    "ProvisioningResult",
    "STARK_PRIVATE_KEY_FILENAME",
    "discover_protected_credentials",
    "inspect_protected_credentials",
    "main",
    "provision_protected_credentials",
    "protected_paths",
]
