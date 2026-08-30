"""Protected, offline Nado mainnet identity and signer onboarding.

This module is an explicitly invoked operator boundary, not part of the
paper runtime or the normal command-line application.  Its read-only path
stores only the public Nado wallet/subaccount identity in a fixed owner-only
directory.  A separately named future signer path may create or store one
linked-signer key, but neither path has transport, database, execute, order,
or signature surface.  Public account queries do not require that signer.

The representation follows the current official Nado SDK/contracts:
mainnet chain ``57073``; a sender is the wallet address followed by a
12-byte subaccount name; public queries do not require a credential; and a
linked signer is the narrower credential accepted for later execute signing.
"""

from __future__ import annotations

import getpass
import hashlib
import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping


IDENTITY_PROVISIONED = "PROTECTED_NADO_IDENTITY_CREATED"
LINKED_SIGNER_PROVISIONED = "PROTECTED_NADO_LINKED_SIGNER_CREATED"
# Kept as the stable generic success marker for callers that do not need to
# distinguish the two explicit phases.  Read-only provisioning is the only
# current operator command, so its value is intentionally identity-specific.
PROVISIONED = IDENTITY_PROVISIONED
BLOCKED = "BLOCKED"
NO_MAINNET_WRITE_AUTHORITY = "NO_MAINNET_WRITE_AUTHORITY"

NADO_VENUE = "Nado"
NADO_MAINNET_ENVIRONMENT = "MAINNET"
NADO_MAINNET_CHAIN_ID = 57073
NADO_PUBLIC_IDENTITY_SOURCE = "PUBLIC_WALLET_SUBACCOUNT"
NADO_UNSIGNED_QUERY_AUTHENTICATION = "UNSIGNED_QUERY"
NADO_UNSIGNED_READ_STATUS = "AUTHORITATIVE_UNSIGNED_QUERY"
NADO_DEFAULT_SUBACCOUNT_NAME = "default"

LINKED_SIGNER_CREDENTIAL = "LINKED_SIGNER"
LINKED_SIGNER_BINDING_PENDING = "REQUIRES_AUTHORITATIVE_NADO_QUERY"
IDENTITY_METADATA_SCHEMA_VERSION = 2
MAIN_WALLET_PERSISTENCE_FORBIDDEN = "MAIN_WALLET_PERSISTENCE_FORBIDDEN"
LINKED_SIGNER_PROVISIONING_SEPARATE = "LINKED_SIGNER_PROVISIONING_SEPARATE"

PROTECTED_DIRECTORY = (
    Path.home() / ".config" / "risex-farmer" / "nado-mainnet-onboarding"
)
# Reserved for the separately invoked future linked-signer phase.  The
# read-only identity path never opens, inspects, prompts for, or loads this
# directory.
LINKED_SIGNER_PROTECTED_DIRECTORY = (
    Path.home() / ".config" / "risex-farmer" / "nado-mainnet-signing"
)
IDENTITY_FILENAME = "nado.identity.json"
CREDENTIAL_FILENAME = "nado.credential.bin"
LINKED_SIGNER_METADATA_FILENAME = "nado.linked-signer.json"
PROTECTED_DIRECTORY_MODE = 0o700
PROTECTED_FILE_MODE = 0o600
MAX_PRIVATE_KEY_BYTES = 32
MAX_IDENTITY_BYTES = 4096
MAX_SUBACCOUNT_NAME_BYTES = 12
_HEX_DIGITS = frozenset("0123456789abcdefABCDEF")


class OnboardingViolation(ValueError):
    """A sanitized fail-closed onboarding or protected-path violation."""


@dataclass(frozen=True)
class NadoPublicIdentity:
    """Public identity sufficient for unsigned account queries."""

    wallet_address: str
    subaccount_name: str
    subaccount: str
    environment: str = NADO_MAINNET_ENVIRONMENT
    chain_id: int = NADO_MAINNET_CHAIN_ID
    identity_source: str = NADO_PUBLIC_IDENTITY_SOURCE
    query_authentication: str = NADO_UNSIGNED_QUERY_AUTHENTICATION
    read_status: str = NADO_UNSIGNED_READ_STATUS
    mainnet_write_authority: str = NO_MAINNET_WRITE_AUTHORITY

    def as_dict(self) -> dict[str, object]:
        return {
            "chain_id": self.chain_id,
            "environment": self.environment,
            "identity_source": self.identity_source,
            "mainnet_write_authority": self.mainnet_write_authority,
            "query_authentication": self.query_authentication,
            "read_status": self.read_status,
            "subaccount": self.subaccount,
            "subaccount_name": self.subaccount_name,
            "venue": NADO_VENUE,
            "wallet_address": self.wallet_address,
            "write_ready": False,
        }


@dataclass(frozen=True)
class ProtectedFileState:
    kind: str
    path: str
    present: bool
    protected: bool
    reason: str
    mode: int | None = None
    link_count: int | None = None
    size: int | None = None
    owner_uid: int | None = None


@dataclass(frozen=True)
class ProtectedFiles:
    identity: ProtectedFileState
    credential: ProtectedFileState

    @property
    def states(self) -> tuple[ProtectedFileState, ProtectedFileState]:
        return self.identity, self.credential

    @property
    def all_protected(self) -> bool:
        return all(state.protected for state in self.states)

    @property
    def identity_protected(self) -> bool:
        return self.identity.protected

    @property
    def read_identity_ready(self) -> bool:
        return (
            self.identity.protected
            and not self.credential.present
            and self.credential.reason == "FUTURE_PHASE_NOT_INSPECTED"
        )

    @property
    def any_present(self) -> bool:
        return any(state.present for state in self.states) or (
            self.credential.reason == "PROTECTED_DIRECTORY_UNEXPECTED_ENTRY"
        )


@dataclass(frozen=True)
class OnboardingResult:
    status: str
    reason: str
    files: ProtectedFiles
    identity: NadoPublicIdentity | None = None
    credential_kind: str | None = None
    credential_address: str | None = None
    credential_fingerprint: str | None = None
    mainnet_write_authority: str = NO_MAINNET_WRITE_AUTHORITY

    @property
    def provisioned(self) -> bool:
        return self.status in {IDENTITY_PROVISIONED, LINKED_SIGNER_PROVISIONED}

    @property
    def write_ready(self) -> bool:
        return False

    def evidence(self) -> str:
        return json.dumps(
            {
                "credential_address": self.credential_address,
                "credential_fingerprint": self.credential_fingerprint,
                "credential_kind": self.credential_kind,
                "files": _files_evidence(self.files),
                "identity": (
                    None if self.identity is None else self.identity.as_dict()
                ),
                "mainnet_write_authority": self.mainnet_write_authority,
                "reason": self.reason,
                "status": self.status,
                "write_ready": self.write_ready,
            },
            sort_keys=True,
            separators=(",", ":"),
        )


@dataclass(frozen=True)
class CredentialDiscovery:
    """Restart-safe metadata for a protected credential, never the secret."""

    identity: NadoPublicIdentity
    credential_kind: str
    credential_address: str
    credential_fingerprint: str
    credential_path: str
    mainnet_write_authority: str = NO_MAINNET_WRITE_AUTHORITY

    def evidence(self) -> str:
        return json.dumps(
            {
                "credential_address": self.credential_address,
                "credential_fingerprint": self.credential_fingerprint,
                "credential_kind": self.credential_kind,
                "credential_path": self.credential_path,
                "identity": self.identity.as_dict(),
                "mainnet_write_authority": self.mainnet_write_authority,
                "write_ready": False,
            },
            sort_keys=True,
            separators=(",", ":"),
        )


def protected_paths() -> Mapping[str, Path]:
    """Return the read-only path and reserved future credential path."""

    return {
        "directory": PROTECTED_DIRECTORY,
        "identity": PROTECTED_DIRECTORY / IDENTITY_FILENAME,
        "credential": LINKED_SIGNER_PROTECTED_DIRECTORY / CREDENTIAL_FILENAME,
    }


def future_linked_signer_paths() -> Mapping[str, Path]:
    """Return future signer paths without opening or inspecting them."""

    return {
        "directory": LINKED_SIGNER_PROTECTED_DIRECTORY,
        "credential": LINKED_SIGNER_PROTECTED_DIRECTORY / CREDENTIAL_FILENAME,
        "metadata": LINKED_SIGNER_PROTECTED_DIRECTORY
        / LINKED_SIGNER_METADATA_FILENAME,
    }


def _files_evidence(files: ProtectedFiles) -> list[dict[str, object]]:
    return [
        {
            "kind": state.kind,
            "link_count": state.link_count,
            "mode": state.mode,
            "owner_uid": state.owner_uid,
            "path": state.path,
            "present": state.present,
            "protected": state.protected,
            "reason": state.reason,
            "size": state.size,
        }
        for state in files.states
    ]


def _raise_violation(reason: str) -> None:
    raise OnboardingViolation(reason)


def _protected_directory_parts(directory: Path | None = None) -> tuple[str, ...]:
    directory = PROTECTED_DIRECTORY if directory is None else directory
    if not isinstance(directory, Path) or not directory.is_absolute():
        _raise_violation("PROTECTED_DIRECTORY_NOT_ABSOLUTE")
    parts = directory.parts
    if len(parts) < 2 or any(part in {"", ".", ".."} for part in parts[1:]):
        _raise_violation("PROTECTED_DIRECTORY_NOT_CANONICAL")
    return parts[1:]


def _directory_files_for_reason(reason: str) -> ProtectedFiles:
    if not reason.startswith("PROTECTED_DIRECTORY_"):
        reason = "PROTECTED_DIRECTORY_UNAVAILABLE"
    paths = protected_paths()
    return ProtectedFiles(
        identity=ProtectedFileState(
            kind="identity",
            path=str(paths["identity"]),
            present=False,
            protected=False,
            reason=reason,
        ),
        credential=ProtectedFileState(
            kind="credential",
            path=str(paths["credential"]),
            present=False,
            protected=False,
            reason=reason,
        ),
    )


def _directory_child_failure(
    parent_fd: int,
    component: str,
    *,
    missing_reason: str,
) -> None:
    try:
        info = os.lstat(component, dir_fd=parent_fd)
    except FileNotFoundError:
        _raise_violation(missing_reason)
    except OSError:
        _raise_violation("PROTECTED_DIRECTORY_UNAVAILABLE")
    if stat.S_ISLNK(info.st_mode):
        _raise_violation("PROTECTED_DIRECTORY_SYMLINK")
    if not stat.S_ISDIR(info.st_mode):
        _raise_violation("PROTECTED_DIRECTORY_NOT_DIRECTORY")
    _raise_violation("PROTECTED_DIRECTORY_UNAVAILABLE")


def _required_directory_flags() -> int:
    directory_flag = getattr(os, "O_DIRECTORY", None)
    nofollow_flag = getattr(os, "O_NOFOLLOW", None)
    if (
        type(directory_flag) is not int
        or directory_flag <= 0
        or type(nofollow_flag) is not int
        or nofollow_flag <= 0
    ):
        _raise_violation("PROTECTED_DIRECTORY_FEATURE_UNAVAILABLE")
    return os.O_RDONLY | directory_flag | nofollow_flag


def _validate_directory_component(info: os.stat_result) -> None:
    if not stat.S_ISDIR(info.st_mode):
        _raise_violation("PROTECTED_DIRECTORY_NOT_DIRECTORY")
    if info.st_uid not in (0, os.getuid()):
        _raise_violation("PROTECTED_DIRECTORY_OWNER_NOT_CURRENT_USER")


def _open_directory_child(
    parent_fd: int,
    component: str,
    flags: int,
    *,
    create: bool,
) -> tuple[int, os.stat_result]:
    created = False
    try:
        child_fd = os.open(component, flags, dir_fd=parent_fd)
    except FileNotFoundError:
        if not create:
            _raise_violation("PROTECTED_DIRECTORY_MISSING")
        try:
            os.mkdir(
                component,
                PROTECTED_DIRECTORY_MODE,
                dir_fd=parent_fd,
            )
        except FileExistsError:
            pass
        except OSError:
            _raise_violation("PROTECTED_DIRECTORY_UNAVAILABLE")
        else:
            created = True
        try:
            child_fd = os.open(component, flags, dir_fd=parent_fd)
        except FileNotFoundError:
            _raise_violation("PROTECTED_DIRECTORY_UNAVAILABLE")
        except OSError:
            _directory_child_failure(
                parent_fd,
                component,
                missing_reason="PROTECTED_DIRECTORY_UNAVAILABLE",
            )
    except OSError:
        _directory_child_failure(
            parent_fd,
            component,
            missing_reason="PROTECTED_DIRECTORY_MISSING",
        )

    try:
        info = os.fstat(child_fd)
        _validate_directory_component(info)
        if created:
            try:
                os.fchmod(child_fd, PROTECTED_DIRECTORY_MODE)
                info = os.fstat(child_fd)
                _validate_directory_component(info)
            except OSError:
                _raise_violation("PROTECTED_DIRECTORY_UNAVAILABLE")
        return child_fd, info
    except BaseException:
        try:
            os.close(child_fd)
        except OSError:
            pass
        raise


def _open_fixed_directory(
    *,
    create: bool = True,
    directory: Path | None = None,
) -> int | None:
    """Open the fixed directory through trusted descriptors only."""

    parts = _protected_directory_parts(directory)
    flags = _required_directory_flags()
    descriptor = -1
    try:
        descriptor = os.open(os.sep, flags)
        for index, component in enumerate(parts):
            next_descriptor = -1
            try:
                next_descriptor, info = _open_directory_child(
                    descriptor,
                    component,
                    flags,
                    create=create,
                )
                if index == len(parts) - 1:
                    if info.st_uid != os.getuid():
                        _raise_violation(
                            "PROTECTED_DIRECTORY_OWNER_NOT_CURRENT_USER"
                        )
                    if stat.S_IMODE(info.st_mode) != PROTECTED_DIRECTORY_MODE:
                        _raise_violation("PROTECTED_DIRECTORY_MODE_NOT_0700")
                previous_descriptor = descriptor
                descriptor = next_descriptor
                next_descriptor = -1
                try:
                    os.close(previous_descriptor)
                except OSError:
                    pass
            finally:
                if next_descriptor >= 0:
                    try:
                        os.close(next_descriptor)
                    except OSError:
                        pass
        return descriptor
    except OnboardingViolation as exc:
        try:
            os.close(descriptor)
        except OSError:
            pass
        if not create and str(exc) == "PROTECTED_DIRECTORY_MISSING":
            return None
        raise
    except OSError:
        try:
            os.close(descriptor)
        except OSError:
            pass
        _raise_violation("PROTECTED_DIRECTORY_UNAVAILABLE")
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        raise


def _file_state(
    kind: str,
    path: Path,
    directory_fd: int | None,
    directory_reason: str | None = None,
) -> ProtectedFileState:
    if directory_fd is None:
        return ProtectedFileState(
            kind=kind,
            path=str(path),
            present=False,
            protected=False,
            reason=directory_reason or "PROTECTED_DIRECTORY_MISSING",
        )
    try:
        info = os.lstat(path.name, dir_fd=directory_fd)
    except FileNotFoundError:
        return ProtectedFileState(
            kind=kind,
            path=str(path),
            present=False,
            protected=False,
            reason="PROTECTED_FILE_MISSING",
        )
    except OSError:
        return ProtectedFileState(
            kind=kind,
            path=str(path),
            present=False,
            protected=False,
            reason="PROTECTED_FILE_UNREADABLE",
        )

    mode = stat.S_IMODE(info.st_mode)
    links = info.st_nlink
    owner_uid = info.st_uid
    size = info.st_size
    if stat.S_ISLNK(info.st_mode):
        reason = "PROTECTED_FILE_SYMLINK"
    elif not stat.S_ISREG(info.st_mode):
        reason = "PROTECTED_FILE_NOT_REGULAR"
    elif owner_uid != os.getuid():
        reason = "PROTECTED_FILE_OWNER_NOT_CURRENT_USER"
    elif links != 1:
        reason = "PROTECTED_FILE_HARDLINK"
    elif mode != PROTECTED_FILE_MODE:
        reason = "PROTECTED_FILE_MODE_NOT_0600"
    elif size <= 0:
        reason = "PROTECTED_FILE_EMPTY"
    elif size > MAX_IDENTITY_BYTES:
        reason = "PROTECTED_FILE_TOO_LARGE"
    else:
        reason = "PROTECTED_FILE_OK"
    return ProtectedFileState(
        kind=kind,
        path=str(path),
        present=True,
        protected=reason == "PROTECTED_FILE_OK",
        reason=reason,
        mode=mode,
        link_count=links,
        size=size,
        owner_uid=owner_uid,
    )


def _directory_entries(directory_fd: int) -> tuple[str, ...]:
    try:
        return tuple(sorted(os.listdir(directory_fd)))
    except OSError:
        _raise_violation("PROTECTED_DIRECTORY_UNAVAILABLE")


def _future_credential_state(reason: str) -> ProtectedFileState:
    return ProtectedFileState(
        kind="credential",
        path=str(future_linked_signer_paths()["credential"]),
        present=False,
        protected=False,
        reason=reason,
    )


def _inspect_protected_files_fd(directory_fd: int) -> ProtectedFiles:
    paths = protected_paths()
    identity = _file_state("identity", paths["identity"], directory_fd)
    entries = _directory_entries(directory_fd)
    if not entries or set(entries) == {IDENTITY_FILENAME}:
        credential = _future_credential_state("FUTURE_PHASE_NOT_INSPECTED")
    elif CREDENTIAL_FILENAME in entries:
        credential = _file_state(
            "credential",
            PROTECTED_DIRECTORY / CREDENTIAL_FILENAME,
            directory_fd,
        )
    else:
        credential = _future_credential_state(
            "PROTECTED_DIRECTORY_UNEXPECTED_ENTRY"
        )
    return ProtectedFiles(identity=identity, credential=credential)


def _inspect_future_linked_signer_files_fd(
    directory_fd: int,
) -> tuple[ProtectedFileState, ProtectedFileState]:
    paths = future_linked_signer_paths()
    entries = _directory_entries(directory_fd)
    allowed = {CREDENTIAL_FILENAME, LINKED_SIGNER_METADATA_FILENAME}
    if any(entry not in allowed for entry in entries):
        _raise_violation("PROTECTED_DIRECTORY_UNEXPECTED_ENTRY")
    return (
        _file_state("credential", paths["credential"], directory_fd),
        _file_state("metadata", paths["metadata"], directory_fd),
    )


def inspect_protected_files() -> ProtectedFiles:
    """Inspect only metadata through one exact directory descriptor."""

    try:
        directory_fd = _open_fixed_directory(create=False)
    except OnboardingViolation as exc:
        return _directory_files_for_reason(str(exc))
    except OSError:
        return _directory_files_for_reason("PROTECTED_DIRECTORY_UNAVAILABLE")
    if directory_fd is None:
        return _directory_files_for_reason("PROTECTED_DIRECTORY_MISSING")
    try:
        return _inspect_protected_files_fd(directory_fd)
    finally:
        try:
            os.close(directory_fd)
        except OSError:
            pass


def _wipe(payload: bytearray | None) -> None:
    if payload is None:
        return
    for index in range(len(payload)):
        payload[index] = 0
    payload.clear()


def _private_key_bytes(value: Any, field: str, *, optional: bool = False) -> bytearray | None:
    if optional and value == "":
        return None
    if type(value) is not str or not value or value != value.strip():
        _raise_violation(f"PROTECTED_INPUT_INVALID:{field}")
    if "\x00" in value or "\n" in value or "\r" in value:
        _raise_violation(f"PROTECTED_INPUT_INVALID:{field}")
    if len(value) != 2 + (MAX_PRIVATE_KEY_BYTES * 2) or not value.startswith("0x"):
        _raise_violation(f"PROTECTED_INPUT_INVALID:{field}")
    if any(char not in _HEX_DIGITS for char in value[2:]):
        _raise_violation(f"PROTECTED_INPUT_INVALID:{field}")
    try:
        decoded = bytes.fromhex(value[2:])
    except ValueError:
        _raise_violation(f"PROTECTED_INPUT_INVALID:{field}")
    payload = bytearray(decoded)
    del decoded
    if len(payload) != MAX_PRIVATE_KEY_BYTES or not any(payload):
        _wipe(payload)
        _raise_violation(f"PROTECTED_INPUT_INVALID:{field}")
    return payload


def _crypto_account() -> Any:
    try:
        from eth_account import Account
    except (ImportError, AttributeError):
        _raise_violation("CRYPTO_DEPENDENCY_UNAVAILABLE")
    return Account


def _address(value: Any, field: str) -> str:
    if (
        type(value) is not str
        or len(value) != 42
        or not value.startswith("0x")
        or any(char not in _HEX_DIGITS for char in value[2:])
    ):
        _raise_violation(f"PUBLIC_IDENTITY_INVALID:{field}")
    raw = bytes.fromhex(value[2:])
    if len(raw) != 20 or raw == b"\0" * 20:
        _raise_violation(f"PUBLIC_IDENTITY_INVALID:{field}")
    return value.lower()


def _subaccount_name(value: Any) -> str:
    if type(value) is not str or not value or value != value.strip():
        _raise_violation("SUBACCOUNT_NAME_INVALID")
    if "\x00" in value or "\n" in value or "\r" in value:
        _raise_violation("SUBACCOUNT_NAME_INVALID")
    try:
        encoded = value.encode("ascii")
    except UnicodeEncodeError:
        _raise_violation("SUBACCOUNT_NAME_INVALID")
    if not 1 <= len(encoded) <= MAX_SUBACCOUNT_NAME_BYTES:
        _raise_violation("SUBACCOUNT_NAME_INVALID")
    if any(byte < 0x21 or byte > 0x7E for byte in encoded):
        _raise_violation("SUBACCOUNT_NAME_INVALID")
    return value


def _encode_subaccount(wallet_address: str, subaccount_name: str) -> str:
    wallet = bytes.fromhex(_address(wallet_address, "wallet_address")[2:])
    name = _subaccount_name(subaccount_name).encode("ascii")
    return "0x" + (wallet + name.ljust(MAX_SUBACCOUNT_NAME_BYTES, b"\0")).hex()


def _derive_wallet_address(private_key: bytearray) -> str:
    Account = _crypto_account()
    try:
        address = Account.from_key(bytes(private_key)).address
    except BaseException:
        _raise_violation("WALLET_IDENTITY_DERIVATION_FAILED")
    return _address(address, "wallet_address")


def _identity_from_parts(
    wallet_address: str,
    subaccount_name: str,
    *,
    expected_wallet_address: str | None = None,
    expected_subaccount: str | None = None,
) -> NadoPublicIdentity:
    wallet = _address(wallet_address, "wallet_address")
    name = _subaccount_name(subaccount_name)
    sender = _encode_subaccount(wallet, name)
    if expected_wallet_address is not None:
        if _address(expected_wallet_address, "expected_wallet_address") != wallet:
            _raise_violation("MAIN_WALLET_IDENTITY_CONFLICT")
    if expected_subaccount is not None:
        expected = _bytes32(expected_subaccount, "expected_subaccount")
        if expected != sender:
            _raise_violation("SUBACCOUNT_IDENTITY_CONFLICT")
    return NadoPublicIdentity(
        wallet_address=wallet,
        subaccount_name=name,
        subaccount=sender,
    )


def _bytes32(value: Any, field: str) -> str:
    if (
        type(value) is not str
        or len(value) != 66
        or not value.startswith("0x")
        or any(char not in _HEX_DIGITS for char in value[2:])
    ):
        _raise_violation(f"PUBLIC_IDENTITY_INVALID:{field}")
    raw = bytes.fromhex(value[2:])
    if len(raw) != 32:
        _raise_violation(f"PUBLIC_IDENTITY_INVALID:{field}")
    return value.lower()


def _subaccount_name_from_sender(
    wallet_address: str,
    sender: str,
) -> str:
    wallet = bytes.fromhex(_address(wallet_address, "wallet_address")[2:])
    raw = bytes.fromhex(_bytes32(sender, "subaccount")[2:])
    if raw[: len(wallet)] != wallet:
        _raise_violation("SUBACCOUNT_IDENTITY_CONFLICT")
    encoded_name = raw[len(wallet) :]
    try:
        first_padding = encoded_name.index(0)
    except ValueError:
        first_padding = len(encoded_name)
    if any(byte != 0 for byte in encoded_name[first_padding:]):
        _raise_violation("SUBACCOUNT_IDENTITY_CONFLICT")
    try:
        name = encoded_name[:first_padding].decode("ascii")
    except UnicodeDecodeError:
        _raise_violation("PUBLIC_IDENTITY_INVALID:subaccount")
    return _subaccount_name(name)


def _identity_from_public_parts(
    wallet_address: Any,
    subaccount: Any,
    *,
    subaccount_name: str | None = None,
    expected_wallet_address: str | None = None,
    expected_subaccount: str | None = None,
) -> NadoPublicIdentity:
    wallet = _address(wallet_address, "wallet_address")
    sender = _bytes32(subaccount, "subaccount")
    derived_name = _subaccount_name_from_sender(wallet, sender)
    if subaccount_name is not None:
        if _subaccount_name(subaccount_name) != derived_name:
            _raise_violation("SUBACCOUNT_IDENTITY_CONFLICT")
    return _identity_from_parts(
        wallet,
        derived_name,
        expected_wallet_address=expected_wallet_address,
        expected_subaccount=(
            sender if expected_subaccount is None else expected_subaccount
        ),
    )


def derive_public_identity(
    main_wallet_key: str,
    subaccount_name: str = NADO_DEFAULT_SUBACCOUNT_NAME,
    *,
    expected_wallet_address: str | None = None,
    expected_subaccount: str | None = None,
) -> NadoPublicIdentity:
    """Derive only public identity; no credential is persisted or returned."""

    key = _private_key_bytes(main_wallet_key, "main_wallet_key")
    try:
        assert key is not None
        wallet_address = _derive_wallet_address(key)
        return _identity_from_parts(
            wallet_address,
            subaccount_name,
            expected_wallet_address=expected_wallet_address,
            expected_subaccount=expected_subaccount,
        )
    finally:
        _wipe(key)


def export_unsigned_read_identity(identity: NadoPublicIdentity) -> dict[str, object]:
    """Return sanitized identity for unsigned Nado account queries."""

    if not isinstance(identity, NadoPublicIdentity):
        _raise_violation("PUBLIC_IDENTITY_OBJECT_INVALID")
    canonical = _identity_from_parts(identity.wallet_address, identity.subaccount_name)
    if identity != canonical:
        _raise_violation("SUBACCOUNT_IDENTITY_CONFLICT")
    return canonical.as_dict()


def _metadata_payload(identity: NadoPublicIdentity) -> bytearray:
    payload: dict[str, object] = identity.as_dict()
    payload["schema_version"] = IDENTITY_METADATA_SCHEMA_VERSION
    try:
        encoded = json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
    except (TypeError, UnicodeError, ValueError):
        _raise_violation("METADATA_ENCODING_FAILED")
    if len(encoded) > MAX_IDENTITY_BYTES:
        _raise_violation("METADATA_TOO_LARGE")
    return bytearray(encoded)


def _hex_fingerprint(value: Any, field: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or any(char not in "0123456789abcdef" for char in value)
    ):
        _raise_violation(f"METADATA_INVALID:{field}")
    return value


def _linked_signer_metadata_payload(
    identity: NadoPublicIdentity,
    credential_address: str,
    credential_fingerprint: str,
) -> bytearray:
    payload: dict[str, object] = {
        "chain_id": NADO_MAINNET_CHAIN_ID,
        "credential_address": credential_address,
        "credential_fingerprint": credential_fingerprint,
        "credential_kind": LINKED_SIGNER_CREDENTIAL,
        "environment": NADO_MAINNET_ENVIRONMENT,
        "identity_source": NADO_PUBLIC_IDENTITY_SOURCE,
        "identity_subaccount": identity.subaccount,
        "identity_wallet_address": identity.wallet_address,
        "mainnet_write_authority": NO_MAINNET_WRITE_AUTHORITY,
        "query_authentication": NADO_UNSIGNED_QUERY_AUTHENTICATION,
        "schema_version": 1,
        "venue": NADO_VENUE,
    }
    try:
        encoded = json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
    except (TypeError, UnicodeError, ValueError):
        _raise_violation("METADATA_ENCODING_FAILED")
    if len(encoded) > MAX_IDENTITY_BYTES:
        _raise_violation("METADATA_TOO_LARGE")
    return bytearray(encoded)


def _write_new_file(directory_fd: int, filename: str, payload: bytearray) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= os.O_NOFOLLOW
    descriptor = -1
    created = False
    completed = False
    try:
        try:
            descriptor = os.open(
                filename,
                flags,
                PROTECTED_FILE_MODE,
                dir_fd=directory_fd,
            )
        except FileExistsError:
            _raise_violation("PROTECTED_PATH_ALREADY_EXISTS")
        except OSError:
            _raise_violation("PROTECTED_FILE_UNAVAILABLE")
        created = True
        info = os.fstat(descriptor)
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            _raise_violation("PROTECTED_FILE_NOT_REGULAR")
        if info.st_uid != os.getuid():
            _raise_violation("PROTECTED_FILE_OWNER_NOT_CURRENT_USER")
        if info.st_nlink != 1:
            _raise_violation("PROTECTED_FILE_HARDLINK")
        os.fchmod(descriptor, PROTECTED_FILE_MODE)
        view = memoryview(payload)
        try:
            while len(view):
                written = os.write(descriptor, view)
                if written <= 0:
                    _raise_violation("PROTECTED_FILE_WRITE_INCOMPLETE")
                view = view[written:]
        finally:
            view.release()
        os.fsync(descriptor)
        final_info = os.fstat(descriptor)
        if final_info.st_uid != os.getuid():
            _raise_violation("PROTECTED_FILE_OWNER_NOT_CURRENT_USER")
        if final_info.st_nlink != 1:
            _raise_violation("PROTECTED_FILE_HARDLINK")
        if stat.S_IMODE(final_info.st_mode) != PROTECTED_FILE_MODE:
            _raise_violation("PROTECTED_FILE_METADATA_CHANGED")
        if final_info.st_size != len(payload):
            _raise_violation("PROTECTED_FILE_WRITE_INCOMPLETE")
        completed = True
    finally:
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except OSError:
                pass
        if created and not completed:
            try:
                os.unlink(filename, dir_fd=directory_fd)
            except BaseException:
                pass


def _rollback_created(directory_fd: int, created: list[str]) -> bool:
    rollback_ok = True
    for filename in reversed(created):
        try:
            os.unlink(filename, dir_fd=directory_fd)
        except FileNotFoundError:
            pass
        except BaseException:
            rollback_ok = False
    try:
        os.fsync(directory_fd)
    except BaseException:
        rollback_ok = False
    return rollback_ok


def _prompt(input_fn: Callable[[str], str], prompt: str) -> str:
    try:
        value = input_fn(prompt)
    except BaseException:
        _raise_violation("PROTECTED_INPUT_CANCELLED")
    return value


def _blocked(reason: str) -> OnboardingResult:
    return OnboardingResult(
        status=BLOCKED,
        reason=reason,
        files=inspect_protected_files(),
    )


def _merge_public_identity_value(
    primary: Any,
    alias: Any,
    *,
    field: str,
    normalizer: Callable[[Any, str], str],
) -> Any:
    if primary is None:
        return alias
    if alias is None:
        return primary
    first = normalizer(primary, field)
    second = normalizer(alias, field)
    if first != second:
        _raise_violation("PUBLIC_IDENTITY_CONFLICT")
    return first


def _public_identity_arguments(
    wallet_address: Any,
    subaccount: Any,
    public_wallet_address: Any,
    public_subaccount: Any,
) -> tuple[Any, Any]:
    wallet = _merge_public_identity_value(
        wallet_address,
        public_wallet_address,
        field="wallet_address",
        normalizer=_address,
    )
    sender = _merge_public_identity_value(
        subaccount,
        public_subaccount,
        field="subaccount",
        normalizer=_bytes32,
    )
    if (wallet is None) != (sender is None):
        _raise_violation("PUBLIC_IDENTITY_INPUT_INCOMPLETE")
    return wallet, sender


def provision_nado_mainnet_identity(
    input_fn: Callable[[str], str] | None = None,
    subaccount_name: str = NADO_DEFAULT_SUBACCOUNT_NAME,
    *,
    wallet_address: str | None = None,
    subaccount: str | None = None,
    public_wallet_address: str | None = None,
    public_subaccount: str | None = None,
    expected_wallet_address: str | None = None,
    expected_subaccount: str | None = None,
) -> OnboardingResult:
    """Provision only the public identity needed for unsigned Nado reads.

    With no explicit public pair, exactly one hidden main-wallet prompt is
    used for local derivation.  The derived key is held only in a wipeable
    bytearray and is never written, fingerprinted, or used as a credential.
    Supplying ``wallet_address`` and ``subaccount`` skips hidden input
    entirely.  Linked-signer provisioning is deliberately a separate future
    function and is not called or loaded here.
    """

    input_fn = getpass.getpass if input_fn is None else input_fn
    directory_fd: int | None = None
    main_key: bytearray | None = None
    metadata_payload: bytearray | None = None
    try:
        try:
            directory_fd = _open_fixed_directory(create=False)
        except OnboardingViolation as exc:
            if str(exc) != "PROTECTED_DIRECTORY_MISSING":
                return _blocked(str(exc))
        if directory_fd is not None:
            before = _inspect_protected_files_fd(directory_fd)
            if before.any_present:
                return OnboardingResult(
                    status=BLOCKED,
                    reason="PROTECTED_PATH_ALREADY_EXISTS",
                    files=before,
                )

        wallet, sender = _public_identity_arguments(
            wallet_address,
            subaccount,
            public_wallet_address,
            public_subaccount,
        )
        if wallet is None and sender is None:
            _subaccount_name(subaccount_name)
            main_key = _private_key_bytes(
                _prompt(input_fn, "Nado main wallet private key (hidden): "),
                "main_wallet_key",
            )
            assert main_key is not None
            derived_wallet = _derive_wallet_address(main_key)
            identity = _identity_from_parts(
                derived_wallet,
                subaccount_name,
                expected_wallet_address=expected_wallet_address,
                expected_subaccount=expected_subaccount,
            )
        else:
            identity = _identity_from_public_parts(
                wallet,
                sender,
                subaccount_name=(
                    None
                    if subaccount_name == NADO_DEFAULT_SUBACCOUNT_NAME
                    else subaccount_name
                ),
                expected_wallet_address=expected_wallet_address,
                expected_subaccount=expected_subaccount,
            )

        metadata_payload = _metadata_payload(identity)

        if directory_fd is None:
            directory_fd = _open_fixed_directory(create=True)
            if directory_fd is None:
                _raise_violation("PROTECTED_DIRECTORY_UNAVAILABLE")
        after_input = _inspect_protected_files_fd(directory_fd)
        if after_input.any_present:
            return OnboardingResult(
                status=BLOCKED,
                reason="PROTECTED_PATH_ALREADY_EXISTS",
                files=after_input,
            )

        created: list[str] = []
        try:
            _write_new_file(directory_fd, IDENTITY_FILENAME, metadata_payload)
            created.append(IDENTITY_FILENAME)
            os.fsync(directory_fd)
            final_files = _inspect_protected_files_fd(directory_fd)
            if not final_files.read_identity_ready:
                _raise_violation("PROTECTED_FILE_METADATA_CHANGED")
        except OnboardingViolation as exc:
            if not _rollback_created(directory_fd, created):
                raise OnboardingViolation("PROTECTED_ROLLBACK_INCOMPLETE") from None
            raise exc
        except OSError:
            if not _rollback_created(directory_fd, created):
                raise OnboardingViolation("PROTECTED_ROLLBACK_INCOMPLETE") from None
            _raise_violation("PROTECTED_FILESYSTEM_OPERATION_FAILED")
        except BaseException:
            if not _rollback_created(directory_fd, created):
                raise OnboardingViolation("PROTECTED_ROLLBACK_INCOMPLETE") from None
            raise OnboardingViolation("PROTECTED_ONBOARDING_FAILED") from None
        return OnboardingResult(
            status=PROVISIONED,
            reason="PROTECTED_NADO_IDENTITY_CREATED",
            files=final_files,
            identity=identity,
        )
    except OnboardingViolation as exc:
        return _blocked(str(exc))
    except OSError:
        return _blocked("PROTECTED_FILESYSTEM_OPERATION_FAILED")
    except BaseException:
        return _blocked("PROTECTED_ONBOARDING_FAILED")
    finally:
        _wipe(main_key)
        _wipe(metadata_payload)
        if directory_fd is not None:
            try:
                os.close(directory_fd)
            except OSError:
                pass


def provision_nado_mainnet_credential(
    input_fn: Callable[[str], str] | None = None,
    subaccount_name: str = NADO_DEFAULT_SUBACCOUNT_NAME,
    *,
    wallet_address: str | None = None,
    subaccount: str | None = None,
    public_wallet_address: str | None = None,
    public_subaccount: str | None = None,
    expected_wallet_address: str | None = None,
    expected_subaccount: str | None = None,
    expected_linked_signer_address: str | None = None,
    fallback_proof: object | None = None,
) -> OnboardingResult:
    """Compatibility entry point with read-only identity semantics.

    The historical credential-shaped name now has exactly the same
    read-only behavior as :func:`provision_nado_mainnet_identity`.  Legacy
    signer or fallback arguments are rejected before any prompt or write so
    the main wallet can never become a persistent fallback.
    """

    if expected_linked_signer_address is not None:
        return _blocked(LINKED_SIGNER_PROVISIONING_SEPARATE)
    if fallback_proof is not None:
        return _blocked(MAIN_WALLET_PERSISTENCE_FORBIDDEN)
    return provision_nado_mainnet_identity(
        input_fn,
        subaccount_name,
        wallet_address=wallet_address,
        subaccount=subaccount,
        public_wallet_address=public_wallet_address,
        public_subaccount=public_subaccount,
        expected_wallet_address=expected_wallet_address,
        expected_subaccount=expected_subaccount,
    )


# Read-only spelling for new callers; both names have identical semantics.
provision_nado_read_identity = provision_nado_mainnet_identity


def _generate_linked_signer_key() -> bytearray:
    try:
        payload = bytearray(os.urandom(MAX_PRIVATE_KEY_BYTES))
    except BaseException:
        _raise_violation("LINKED_SIGNER_GENERATION_FAILED")
    if len(payload) != MAX_PRIVATE_KEY_BYTES or not any(payload):
        _wipe(payload)
        _raise_violation("LINKED_SIGNER_GENERATION_FAILED")
    return payload


def provision_nado_linked_signer(
    input_fn: Callable[[str], str] | None = None,
    *,
    generate: bool = False,
    expected_linked_signer_address: str | None = None,
) -> OnboardingResult:
    """Explicit future-only local linked-signer provisioning.

    This path requires an already provisioned public identity.  It either
    generates a fresh local key or accepts one existing linked-signer key
    through one hidden prompt.  It never accepts the main wallet as a
    fallback, never updates the identity file, and never contacts or signs
    for Nado.  Its fixed directory is separate from the read-only identity
    directory.  The normal read-only identity path never calls this function.
    """

    input_fn = getpass.getpass if input_fn is None else input_fn
    identity_directory_fd: int | None = None
    signer_directory_fd: int | None = None
    linked_key: bytearray | None = None
    metadata_payload: bytearray | None = None
    try:
        if type(generate) is not bool:
            _raise_violation("LINKED_SIGNER_GENERATION_FLAG_INVALID")
        try:
            identity_directory_fd = _open_fixed_directory(create=False)
        except OnboardingViolation as exc:
            if str(exc) != "PROTECTED_DIRECTORY_MISSING":
                return _blocked(str(exc))
        if identity_directory_fd is None:
            return _blocked("PROTECTED_IDENTITY_REQUIRED")
        files = _inspect_protected_files_fd(identity_directory_fd)
        if files.credential.present or (
            files.credential.reason == "PROTECTED_DIRECTORY_UNEXPECTED_ENTRY"
        ):
            return OnboardingResult(
                status=BLOCKED,
                reason="PROTECTED_PATH_ALREADY_EXISTS",
                files=files,
            )
        if not files.identity.present:
            return OnboardingResult(
                status=BLOCKED,
                reason="PROTECTED_IDENTITY_REQUIRED",
                files=files,
            )
        if not files.identity.protected:
            return OnboardingResult(
                status=BLOCKED,
                reason=files.identity.reason,
                files=files,
            )
        identity = _load_metadata(identity_directory_fd)
        try:
            signer_directory_fd = _open_fixed_directory(
                create=False,
                directory=LINKED_SIGNER_PROTECTED_DIRECTORY,
            )
        except OnboardingViolation as exc:
            if str(exc) != "PROTECTED_DIRECTORY_MISSING":
                return _blocked(str(exc))
        if signer_directory_fd is not None:
            if _directory_entries(signer_directory_fd):
                return OnboardingResult(
                    status=BLOCKED,
                    reason="PROTECTED_PATH_ALREADY_EXISTS",
                    files=files,
                )
            return OnboardingResult(
                status=BLOCKED,
                reason="PROTECTED_PATH_ALREADY_EXISTS",
                files=files,
            )
        if generate:
            linked_key = _generate_linked_signer_key()
        else:
            linked_key = _private_key_bytes(
                _prompt(
                    input_fn,
                "Nado linked signer private key (hidden; future explicit path): ",
                ),
                "linked_signer_key",
            )
        assert linked_key is not None
        credential_address = _derive_wallet_address(linked_key)
        if credential_address == identity.wallet_address:
            _raise_violation(MAIN_WALLET_PERSISTENCE_FORBIDDEN)
        if expected_linked_signer_address is not None:
            expected_linked = _address(
                expected_linked_signer_address,
                "expected_linked_signer_address",
            )
            if credential_address != expected_linked:
                _raise_violation("LINKED_SIGNER_IDENTITY_CONFLICT")
        credential_fingerprint = hashlib.sha256(linked_key).hexdigest()
        metadata_payload = _linked_signer_metadata_payload(
            identity,
            credential_address,
            credential_fingerprint,
        )
        current_identity = _load_metadata(identity_directory_fd)
        if current_identity != identity:
            _raise_violation("PROTECTED_IDENTITY_CHANGED")
        current_files = _inspect_protected_files_fd(identity_directory_fd)
        if current_files.credential.present or (
            current_files.credential.reason == "PROTECTED_DIRECTORY_UNEXPECTED_ENTRY"
        ):
            return OnboardingResult(
                status=BLOCKED,
                reason="PROTECTED_PATH_ALREADY_EXISTS",
                files=current_files,
            )
        if signer_directory_fd is None:
            signer_directory_fd = _open_fixed_directory(
                create=True,
                directory=LINKED_SIGNER_PROTECTED_DIRECTORY,
            )
            if signer_directory_fd is None:
                _raise_violation("PROTECTED_DIRECTORY_UNAVAILABLE")
        if _directory_entries(signer_directory_fd):
            return OnboardingResult(
                status=BLOCKED,
                reason="PROTECTED_PATH_ALREADY_EXISTS",
                files=current_files,
            )
        created: list[str] = []
        final_identity: NadoPublicIdentity | None = None
        final_identity_state: ProtectedFileState | None = None
        try:
            _write_new_file(signer_directory_fd, CREDENTIAL_FILENAME, linked_key)
            created.append(CREDENTIAL_FILENAME)
            _write_new_file(
                signer_directory_fd,
                LINKED_SIGNER_METADATA_FILENAME,
                metadata_payload,
            )
            created.append(LINKED_SIGNER_METADATA_FILENAME)
            os.fsync(signer_directory_fd)
            final_credential, final_metadata = _inspect_future_linked_signer_files_fd(
                signer_directory_fd
            )
            if not final_credential.protected or not final_metadata.protected:
                _raise_violation("PROTECTED_FILE_METADATA_CHANGED")
            final_identity = _load_metadata(identity_directory_fd)
            if final_identity != identity:
                _raise_violation("PROTECTED_IDENTITY_CHANGED")
            final_identity_state = _inspect_protected_files_fd(
                identity_directory_fd
            ).identity
            if not final_identity_state.protected:
                _raise_violation("PROTECTED_FILE_METADATA_CHANGED")
        except OnboardingViolation as exc:
            if not _rollback_created(signer_directory_fd, created):
                raise OnboardingViolation("PROTECTED_ROLLBACK_INCOMPLETE") from None
            raise exc
        except OSError:
            if not _rollback_created(signer_directory_fd, created):
                raise OnboardingViolation("PROTECTED_ROLLBACK_INCOMPLETE") from None
            _raise_violation("PROTECTED_FILESYSTEM_OPERATION_FAILED")
        except BaseException:
            if not _rollback_created(signer_directory_fd, created):
                raise OnboardingViolation("PROTECTED_ROLLBACK_INCOMPLETE") from None
            raise OnboardingViolation("PROTECTED_ONBOARDING_FAILED") from None
        assert final_identity is not None
        assert final_identity_state is not None
        return OnboardingResult(
            status=LINKED_SIGNER_PROVISIONED,
            reason="PROTECTED_NADO_LINKED_SIGNER_CREATED",
            files=ProtectedFiles(
                identity=final_identity_state,
                credential=final_credential,
            ),
            identity=identity,
            credential_kind=LINKED_SIGNER_CREDENTIAL,
            credential_address=credential_address,
            credential_fingerprint=credential_fingerprint,
        )
    except OnboardingViolation as exc:
        return _blocked(str(exc))
    except OSError:
        return _blocked("PROTECTED_FILESYSTEM_OPERATION_FAILED")
    except BaseException:
        return _blocked("PROTECTED_ONBOARDING_FAILED")
    finally:
        _wipe(linked_key)
        _wipe(metadata_payload)
        if signer_directory_fd is not None:
            try:
                os.close(signer_directory_fd)
            except OSError:
                pass
        if identity_directory_fd is not None:
            try:
                os.close(identity_directory_fd)
            except OSError:
                pass


# Explicit name for callers that want to make the future phase visible in
# their own operator code.  The alias does not make it part of the normal
# command or the read-only identity path.
provision_nado_mainnet_linked_signer = provision_nado_linked_signer


def _read_owned_file(
    directory_fd: int,
    filename: str,
    *,
    maximum: int,
    exact_size: int | None = None,
) -> bytearray:
    flags = os.O_RDONLY | os.O_NOFOLLOW
    try:
        descriptor = os.open(filename, flags, dir_fd=directory_fd)
    except OSError:
        try:
            info = os.lstat(filename, dir_fd=directory_fd)
        except OSError:
            _raise_violation("PROTECTED_FILE_UNAVAILABLE")
        if stat.S_ISLNK(info.st_mode):
            _raise_violation("PROTECTED_FILE_SYMLINK")
        _raise_violation("PROTECTED_FILE_UNAVAILABLE")
    raw = bytearray()
    try:
        info = os.fstat(descriptor)
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            _raise_violation("PROTECTED_FILE_NOT_REGULAR")
        if info.st_uid != os.getuid():
            _raise_violation("PROTECTED_FILE_OWNER_NOT_CURRENT_USER")
        if info.st_nlink != 1:
            _raise_violation("PROTECTED_FILE_HARDLINK")
        if stat.S_IMODE(info.st_mode) != PROTECTED_FILE_MODE:
            _raise_violation("PROTECTED_FILE_MODE_NOT_0600")
        if info.st_size <= 0 or info.st_size > maximum:
            _raise_violation("PROTECTED_FILE_SIZE_INVALID")
        while len(raw) <= maximum:
            chunk = os.read(descriptor, maximum + 1 - len(raw))
            if not chunk:
                break
            raw.extend(chunk)
        final_info = os.fstat(descriptor)
        if (
            final_info.st_uid != os.getuid()
            or final_info.st_nlink != 1
            or stat.S_IMODE(final_info.st_mode) != PROTECTED_FILE_MODE
            or final_info.st_size != len(raw)
        ):
            _raise_violation("PROTECTED_FILE_METADATA_CHANGED")
        if len(raw) > maximum:
            _raise_violation("PROTECTED_FILE_TOO_LARGE")
        if exact_size is not None and len(raw) != exact_size:
            _raise_violation("PROTECTED_FILE_SIZE_INVALID")
        return raw
    except BaseException:
        _wipe(raw)
        raise
    finally:
        try:
            os.close(descriptor)
        except OSError:
            pass


def _metadata_from_bytes(
    raw: bytearray,
) -> NadoPublicIdentity:
    try:
        decoded = bytes(raw).decode("ascii")
        value = json.loads(decoded)
    except (UnicodeError, json.JSONDecodeError):
        _raise_violation("METADATA_INVALID")
    if type(value) is not dict:
        _raise_violation("METADATA_INVALID")
    expected_keys = {
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
    try:
        canonical = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError, UnicodeError):
        _raise_violation("METADATA_NOT_CANONICAL")
    if set(value) != expected_keys or decoded != canonical:
        _raise_violation("METADATA_NOT_CANONICAL")
    if (
        type(value["schema_version"]) is not int
        or value["schema_version"] != IDENTITY_METADATA_SCHEMA_VERSION
        or value["venue"] != NADO_VENUE
        or value["environment"] != NADO_MAINNET_ENVIRONMENT
        or type(value["chain_id"]) is not int
        or value["chain_id"] != NADO_MAINNET_CHAIN_ID
        or value["identity_source"] != NADO_PUBLIC_IDENTITY_SOURCE
        or value["mainnet_write_authority"] != NO_MAINNET_WRITE_AUTHORITY
        or value["query_authentication"] != NADO_UNSIGNED_QUERY_AUTHENTICATION
        or value["read_status"] != NADO_UNSIGNED_READ_STATUS
        or value["write_ready"] is not False
    ):
        _raise_violation("METADATA_INVALID")
    return _identity_from_public_parts(
        value["wallet_address"],
        value["subaccount"],
        subaccount_name=value["subaccount_name"],
    )


def _linked_signer_metadata_from_bytes(
    raw: bytearray,
    identity: NadoPublicIdentity,
) -> tuple[str, str]:
    try:
        decoded = bytes(raw).decode("ascii")
        value = json.loads(decoded)
    except (UnicodeError, json.JSONDecodeError):
        _raise_violation("METADATA_INVALID")
    if type(value) is not dict:
        _raise_violation("METADATA_INVALID")
    expected_keys = {
        "chain_id",
        "credential_address",
        "credential_fingerprint",
        "credential_kind",
        "environment",
        "identity_source",
        "identity_subaccount",
        "identity_wallet_address",
        "mainnet_write_authority",
        "query_authentication",
        "schema_version",
        "venue",
    }
    try:
        canonical = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError, UnicodeError):
        _raise_violation("METADATA_NOT_CANONICAL")
    if set(value) != expected_keys or decoded != canonical:
        _raise_violation("METADATA_NOT_CANONICAL")
    if (
        type(value["schema_version"]) is not int
        or value["schema_version"] != 1
        or value["chain_id"] != NADO_MAINNET_CHAIN_ID
        or value["venue"] != NADO_VENUE
        or value["environment"] != NADO_MAINNET_ENVIRONMENT
        or value["identity_source"] != NADO_PUBLIC_IDENTITY_SOURCE
        or value["mainnet_write_authority"] != NO_MAINNET_WRITE_AUTHORITY
        or value["query_authentication"] != NADO_UNSIGNED_QUERY_AUTHENTICATION
        or value["credential_kind"] != LINKED_SIGNER_CREDENTIAL
    ):
        _raise_violation("METADATA_INVALID")
    if (
        _address(value["identity_wallet_address"], "identity_wallet_address")
        != identity.wallet_address
        or _bytes32(value["identity_subaccount"], "identity_subaccount")
        != identity.subaccount
    ):
        _raise_violation("METADATA_IDENTITY_CONFLICT")
    return (
        _address(value["credential_address"], "credential_address"),
        _hex_fingerprint(value["credential_fingerprint"], "credential_fingerprint"),
    )


def _load_metadata(
    directory_fd: int,
) -> NadoPublicIdentity:
    raw: bytearray | None = None
    try:
        raw = _read_owned_file(
            directory_fd,
            IDENTITY_FILENAME,
            maximum=MAX_IDENTITY_BYTES,
        )
        return _metadata_from_bytes(raw)
    finally:
        _wipe(raw)


def discover_public_identity() -> NadoPublicIdentity:
    """Load only the sanitized identity file for unsigned account reads."""

    directory_fd = _open_fixed_directory(create=False)
    if directory_fd is None:
        _raise_violation("PROTECTED_DIRECTORY_MISSING")
    try:
        files = _inspect_protected_files_fd(directory_fd)
        if files.credential.present or (
            files.credential.reason == "PROTECTED_DIRECTORY_UNEXPECTED_ENTRY"
        ):
            _raise_violation("PROTECTED_DIRECTORY_UNEXPECTED_ENTRY")
        return _load_metadata(directory_fd)
    finally:
        try:
            os.close(directory_fd)
        except OSError:
            pass


def discover_protected_credential() -> CredentialDiscovery:
    """Validate restart persistence without returning secret bytes."""

    identity_directory_fd = _open_fixed_directory(create=False)
    if identity_directory_fd is None:
        _raise_violation("PROTECTED_DIRECTORY_MISSING")
    signer_directory_fd: int | None = None
    metadata_raw: bytearray | None = None
    raw: bytearray | None = None
    try:
        current_files = _inspect_protected_files_fd(identity_directory_fd)
        if current_files.credential.present or (
            current_files.credential.reason == "PROTECTED_DIRECTORY_UNEXPECTED_ENTRY"
        ):
            _raise_violation("PROTECTED_DIRECTORY_UNEXPECTED_ENTRY")
        identity = _load_metadata(identity_directory_fd)
        signer_directory_fd = _open_fixed_directory(
            create=False,
            directory=LINKED_SIGNER_PROTECTED_DIRECTORY,
        )
        if signer_directory_fd is None:
            _raise_violation("PROTECTED_FILE_UNAVAILABLE")
        entries = _directory_entries(signer_directory_fd)
        if set(entries) != {
            CREDENTIAL_FILENAME,
            LINKED_SIGNER_METADATA_FILENAME,
        }:
            _raise_violation("PROTECTED_DIRECTORY_CONTENT_CHANGED")
        metadata_raw = _read_owned_file(
            signer_directory_fd,
            LINKED_SIGNER_METADATA_FILENAME,
            maximum=MAX_IDENTITY_BYTES,
        )
        credential_address, fingerprint = _linked_signer_metadata_from_bytes(
            metadata_raw,
            identity,
        )
        raw = _read_owned_file(
            signer_directory_fd,
            CREDENTIAL_FILENAME,
            maximum=MAX_PRIVATE_KEY_BYTES,
            exact_size=MAX_PRIVATE_KEY_BYTES,
        )
        derived_address = _derive_wallet_address(raw)
        if derived_address == identity.wallet_address:
            _raise_violation(MAIN_WALLET_PERSISTENCE_FORBIDDEN)
        if derived_address != credential_address:
            _raise_violation("PERSISTED_CREDENTIAL_IDENTITY_CONFLICT")
        if hashlib.sha256(raw).hexdigest() != fingerprint:
            _raise_violation("PERSISTED_CREDENTIAL_FINGERPRINT_CONFLICT")
        return CredentialDiscovery(
            identity=identity,
            credential_kind=LINKED_SIGNER_CREDENTIAL,
            credential_address=credential_address,
            credential_fingerprint=fingerprint,
            credential_path=str(future_linked_signer_paths()["credential"]),
        )
    finally:
        _wipe(metadata_raw)
        _wipe(raw)
        if signer_directory_fd is not None:
            try:
                os.close(signer_directory_fd)
            except OSError:
                pass
        try:
            os.close(identity_directory_fd)
        except OSError:
            pass


def main() -> int:
    """Run the fixed hidden-input operator flow with no CLI arguments."""

    result = provision_nado_mainnet_identity()
    print(result.evidence())
    return 0 if result.provisioned else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "BLOCKED",
    "CREDENTIAL_FILENAME",
    "CredentialDiscovery",
    "IDENTITY_METADATA_SCHEMA_VERSION",
    "IDENTITY_FILENAME",
    "LINKED_SIGNER_CREDENTIAL",
    "LINKED_SIGNER_METADATA_FILENAME",
    "LINKED_SIGNER_PROVISIONED",
    "LINKED_SIGNER_PROTECTED_DIRECTORY",
    "LINKED_SIGNER_PROVISIONING_SEPARATE",
    "MAX_IDENTITY_BYTES",
    "MAIN_WALLET_PERSISTENCE_FORBIDDEN",
    "NADO_DEFAULT_SUBACCOUNT_NAME",
    "NADO_MAINNET_CHAIN_ID",
    "NADO_MAINNET_ENVIRONMENT",
    "NADO_PUBLIC_IDENTITY_SOURCE",
    "NADO_UNSIGNED_QUERY_AUTHENTICATION",
    "NADO_UNSIGNED_READ_STATUS",
    "NADO_VENUE",
    "NO_MAINNET_WRITE_AUTHORITY",
    "NadoPublicIdentity",
    "OnboardingResult",
    "OnboardingViolation",
    "PROTECTED_DIRECTORY",
    "PROTECTED_DIRECTORY_MODE",
    "PROTECTED_FILE_MODE",
    "PROVISIONED",
    "IDENTITY_PROVISIONED",
    "ProtectedFileState",
    "ProtectedFiles",
    "discover_protected_credential",
    "discover_public_identity",
    "derive_public_identity",
    "export_unsigned_read_identity",
    "future_linked_signer_paths",
    "inspect_protected_files",
    "main",
    "provision_nado_linked_signer",
    "provision_nado_mainnet_credential",
    "provision_nado_mainnet_identity",
    "provision_nado_mainnet_linked_signer",
    "provision_nado_read_identity",
    "protected_paths",
]
