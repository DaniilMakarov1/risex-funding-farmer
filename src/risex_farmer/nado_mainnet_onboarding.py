"""Protected, offline Nado mainnet credential onboarding.

This module is an explicitly invoked operator boundary, not part of the
paper runtime or the normal command-line application.  It derives the public
Nado wallet/subaccount identity locally and stores one opaque credential in a
fixed owner-only directory.  It deliberately has no transport, database,
execute, order, or signature surface.  A later venue-local private-read or
execution gate can consume the protected credential after independently
proving the current Nado linked-signer binding.

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


PROVISIONED = "PROTECTED_NADO_CREDENTIAL_CREATED"
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
MAIN_WALLET_FALLBACK_CREDENTIAL = "MAIN_WALLET_FALLBACK"
LINKED_SIGNER_BINDING_PENDING = "REQUIRES_AUTHORITATIVE_NADO_QUERY"
MAIN_WALLET_DIRECT_BINDING = "MAIN_WALLET_DIRECT"
REQUIRED_LIFECYCLE = "AUTHORITATIVE_RECONCILIATION_AND_REDUCE_ONLY_CLOSE"
OFFICIAL_FALLBACK_SOURCE = "OFFICIAL_NADO_CONTRACT_EVIDENCE"
OFFICIAL_FALLBACK_REASON = "LINKED_SIGNER_CANNOT_SATISFY_REQUIRED_LIFECYCLE"
# Current official Nado docs and the official Python SDK explicitly support a
# linked signer for execute signing.  Until a later source-backed contract
# correction changes this constant in a separately reviewed slice, retaining
# the main wallet is forbidden even if a caller supplies synthetic evidence.
CURRENT_OFFICIAL_LINKED_SIGNER_SUPPORT = True

PROTECTED_DIRECTORY = (
    Path.home() / ".config" / "risex-farmer" / "nado-mainnet-onboarding"
)
IDENTITY_FILENAME = "nado.identity.json"
CREDENTIAL_FILENAME = "nado.credential.bin"
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
class MainWalletFallbackProof:
    """Structured future-only proof that a linked signer is insufficient.

    The current official Nado contract/docs support linked signers, so this
    proof is not presently constructible from the current evidence.  The
    strict shape exists only to prevent a caller from silently selecting the
    main wallet as a fallback without a later authoritative contradiction.
    """

    source: str
    lifecycle: str
    linked_signer_supported: bool
    linked_signer_satisfies_lifecycle: bool
    authoritative: bool
    reason_code: str


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
    def any_present(self) -> bool:
        return any(state.present for state in self.states)


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
        return self.status == PROVISIONED

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
    """Return the fixed path pair without opening either file."""

    return {
        "identity": PROTECTED_DIRECTORY / IDENTITY_FILENAME,
        "credential": PROTECTED_DIRECTORY / CREDENTIAL_FILENAME,
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


def _protected_directory_parts() -> tuple[str, ...]:
    directory = PROTECTED_DIRECTORY
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


def _open_fixed_directory(*, create: bool = True) -> int | None:
    """Open the fixed directory through trusted descriptors only."""

    parts = _protected_directory_parts()
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


def _inspect_protected_files_fd(directory_fd: int) -> ProtectedFiles:
    paths = protected_paths()
    return ProtectedFiles(
        identity=_file_state("identity", paths["identity"], directory_fd),
        credential=_file_state("credential", paths["credential"], directory_fd),
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


def _hex_fingerprint(value: Any, field: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or any(char not in "0123456789abcdef" for char in value)
    ):
        _raise_violation(f"METADATA_INVALID:{field}")
    return value


def _validate_fallback_proof(proof: Any) -> None:
    if not isinstance(proof, MainWalletFallbackProof):
        _raise_violation("MAIN_WALLET_FALLBACK_OFFICIAL_PROOF_REQUIRED")
    if (
        proof.source != OFFICIAL_FALLBACK_SOURCE
        or proof.lifecycle != REQUIRED_LIFECYCLE
        or proof.linked_signer_supported is not False
        or proof.linked_signer_satisfies_lifecycle is not False
        or proof.authoritative is not True
        or proof.reason_code != OFFICIAL_FALLBACK_REASON
    ):
        _raise_violation("MAIN_WALLET_FALLBACK_OFFICIAL_PROOF_REQUIRED")
    if CURRENT_OFFICIAL_LINKED_SIGNER_SUPPORT:
        _raise_violation("MAIN_WALLET_FALLBACK_FORBIDDEN_BY_CURRENT_OFFICIAL_CONTRACT")


def _metadata_payload(
    identity: NadoPublicIdentity,
    credential_kind: str,
    credential_address: str,
    credential_fingerprint: str,
) -> bytearray:
    if credential_kind == LINKED_SIGNER_CREDENTIAL:
        binding = LINKED_SIGNER_BINDING_PENDING
        fallback_authorization = "NOT_APPLICABLE"
    elif credential_kind == MAIN_WALLET_FALLBACK_CREDENTIAL:
        binding = MAIN_WALLET_DIRECT_BINDING
        fallback_authorization = "OFFICIAL_LINKED_SIGNER_INSUFFICIENCY_PROVEN"
    else:
        _raise_violation("CREDENTIAL_KIND_INVALID")
    payload: dict[str, object] = {
        "binding_status": binding,
        "chain_id": NADO_MAINNET_CHAIN_ID,
        "credential_address": credential_address,
        "credential_fingerprint": credential_fingerprint,
        "credential_kind": credential_kind,
        "environment": NADO_MAINNET_ENVIRONMENT,
        "fallback_authorization": fallback_authorization,
        "identity_source": NADO_PUBLIC_IDENTITY_SOURCE,
        "mainnet_write_authority": NO_MAINNET_WRITE_AUTHORITY,
        "query_authentication": NADO_UNSIGNED_QUERY_AUTHENTICATION,
        "schema_version": 1,
        "subaccount": identity.subaccount,
        "subaccount_name": identity.subaccount_name,
        "venue": NADO_VENUE,
        "wallet_address": identity.wallet_address,
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


def provision_nado_mainnet_credential(
    input_fn: Callable[[str], str] | None = None,
    subaccount_name: str = NADO_DEFAULT_SUBACCOUNT_NAME,
    *,
    expected_wallet_address: str | None = None,
    expected_subaccount: str | None = None,
    expected_linked_signer_address: str | None = None,
    fallback_proof: MainWalletFallbackProof | None = None,
) -> OnboardingResult:
    """Provision one fixed Nado credential through hidden local input.

    The first prompt is the main wallet key and is used only to derive the
    public wallet/subaccount.  The second prompt is the optional linked signer
    key.  A non-empty second value is the default and only current path.  An
    empty second value is accepted only with the strict future fallback proof;
    the main wallet is never silently retained as a convenience fallback.

    No transport or Nado execute is performed.  In particular, this function
    does not register a linked signer; the later exact-account read gate must
    verify that the persisted linked signer address is currently bound.
    """

    input_fn = getpass.getpass if input_fn is None else input_fn
    directory_fd: int | None = None
    main_key: bytearray | None = None
    linked_key: bytearray | None = None
    metadata_payload: bytearray | None = None
    try:
        _subaccount_name(subaccount_name)
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

        main_key = _private_key_bytes(
            _prompt(input_fn, "Nado main wallet private key (hidden): "),
            "main_wallet_key",
        )
        assert main_key is not None
        wallet_address = _derive_wallet_address(main_key)
        identity = _identity_from_parts(
            wallet_address,
            subaccount_name,
            expected_wallet_address=expected_wallet_address,
            expected_subaccount=expected_subaccount,
        )
        linked_key = _private_key_bytes(
            _prompt(
                input_fn,
                "Nado linked signer private key (hidden; blank only with official fallback proof): ",
            ),
            "linked_signer_key",
            optional=True,
        )
        if linked_key is not None:
            credential_kind = LINKED_SIGNER_CREDENTIAL
            credential_address = _derive_wallet_address(linked_key)
            if credential_address == identity.wallet_address:
                _raise_violation("MAIN_AND_LINKED_IDENTITY_CONFLICT")
            if expected_linked_signer_address is not None:
                expected_linked = _address(
                    expected_linked_signer_address,
                    "expected_linked_signer_address",
                )
                if credential_address != expected_linked:
                    _raise_violation("LINKED_SIGNER_IDENTITY_CONFLICT")
        else:
            _validate_fallback_proof(fallback_proof)
            credential_kind = MAIN_WALLET_FALLBACK_CREDENTIAL
            credential_address = identity.wallet_address
            if expected_linked_signer_address is not None:
                _raise_violation("LINKED_SIGNER_IDENTITY_CONFLICT")

        credential_payload = linked_key if linked_key is not None else main_key
        credential_fingerprint = hashlib.sha256(credential_payload).hexdigest()
        metadata_payload = _metadata_payload(
            identity,
            credential_kind,
            credential_address,
            credential_fingerprint,
        )

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
            _write_new_file(directory_fd, CREDENTIAL_FILENAME, credential_payload)
            created.append(CREDENTIAL_FILENAME)
            _write_new_file(directory_fd, IDENTITY_FILENAME, metadata_payload)
            created.append(IDENTITY_FILENAME)
            os.fsync(directory_fd)
            final_files = _inspect_protected_files_fd(directory_fd)
            if not final_files.all_protected:
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
            reason="PROTECTED_NADO_CREDENTIAL_CREATED",
            files=final_files,
            identity=identity,
            credential_kind=credential_kind,
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
        _wipe(main_key)
        _wipe(linked_key)
        _wipe(metadata_payload)
        if directory_fd is not None:
            try:
                os.close(directory_fd)
            except OSError:
                pass


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
) -> tuple[NadoPublicIdentity, str, str, str]:
    try:
        decoded = bytes(raw).decode("ascii")
        value = json.loads(decoded)
    except (UnicodeError, json.JSONDecodeError):
        _raise_violation("METADATA_INVALID")
    if type(value) is not dict:
        _raise_violation("METADATA_INVALID")
    expected_keys = {
        "binding_status",
        "chain_id",
        "credential_address",
        "credential_fingerprint",
        "credential_kind",
        "environment",
        "fallback_authorization",
        "identity_source",
        "mainnet_write_authority",
        "query_authentication",
        "schema_version",
        "subaccount",
        "subaccount_name",
        "venue",
        "wallet_address",
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
        or value["venue"] != NADO_VENUE
        or value["environment"] != NADO_MAINNET_ENVIRONMENT
        or type(value["chain_id"]) is not int
        or value["chain_id"] != NADO_MAINNET_CHAIN_ID
        or value["identity_source"] != NADO_PUBLIC_IDENTITY_SOURCE
        or value["mainnet_write_authority"] != NO_MAINNET_WRITE_AUTHORITY
        or value["query_authentication"] != NADO_UNSIGNED_QUERY_AUTHENTICATION
    ):
        _raise_violation("METADATA_INVALID")
    identity = _identity_from_parts(
        _address(value["wallet_address"], "wallet_address"),
        _subaccount_name(value["subaccount_name"]),
        expected_subaccount=_bytes32(value["subaccount"], "subaccount"),
    )
    credential_kind = value["credential_kind"]
    if type(credential_kind) is not str or credential_kind not in {
        LINKED_SIGNER_CREDENTIAL,
        MAIN_WALLET_FALLBACK_CREDENTIAL,
    }:
        _raise_violation("METADATA_INVALID")
    credential_address = _address(
        value["credential_address"], "credential_address"
    )
    if credential_kind == LINKED_SIGNER_CREDENTIAL:
        if (
            credential_address == identity.wallet_address
            or value["binding_status"] != LINKED_SIGNER_BINDING_PENDING
            or value["fallback_authorization"] != "NOT_APPLICABLE"
        ):
            _raise_violation("METADATA_IDENTITY_CONFLICT")
    else:
        if (
            credential_address != identity.wallet_address
            or value["binding_status"] != MAIN_WALLET_DIRECT_BINDING
            or value["fallback_authorization"]
            != "OFFICIAL_LINKED_SIGNER_INSUFFICIENCY_PROVEN"
        ):
            _raise_violation("METADATA_IDENTITY_CONFLICT")
    fingerprint = _hex_fingerprint(
        value["credential_fingerprint"], "credential_fingerprint"
    )
    return identity, credential_kind, credential_address, fingerprint


def _load_metadata(
    directory_fd: int,
) -> tuple[NadoPublicIdentity, str, str, str]:
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
        identity, _kind, _address_value, _fingerprint = _load_metadata(
            directory_fd
        )
        return identity
    finally:
        try:
            os.close(directory_fd)
        except OSError:
            pass


def discover_protected_credential() -> CredentialDiscovery:
    """Validate restart persistence without returning secret bytes."""

    directory_fd = _open_fixed_directory(create=False)
    if directory_fd is None:
        _raise_violation("PROTECTED_DIRECTORY_MISSING")
    raw: bytearray | None = None
    try:
        identity, credential_kind, credential_address, fingerprint = _load_metadata(
            directory_fd
        )
        raw = _read_owned_file(
            directory_fd,
            CREDENTIAL_FILENAME,
            maximum=MAX_PRIVATE_KEY_BYTES,
            exact_size=MAX_PRIVATE_KEY_BYTES,
        )
        derived_address = _derive_wallet_address(raw)
        if derived_address != credential_address:
            _raise_violation("PERSISTED_CREDENTIAL_IDENTITY_CONFLICT")
        if hashlib.sha256(raw).hexdigest() != fingerprint:
            _raise_violation("PERSISTED_CREDENTIAL_FINGERPRINT_CONFLICT")
        if credential_kind == LINKED_SIGNER_CREDENTIAL:
            if derived_address == identity.wallet_address:
                _raise_violation("MAIN_AND_LINKED_IDENTITY_CONFLICT")
        elif derived_address != identity.wallet_address:
            _raise_violation("PERSISTED_CREDENTIAL_IDENTITY_CONFLICT")
        return CredentialDiscovery(
            identity=identity,
            credential_kind=credential_kind,
            credential_address=credential_address,
            credential_fingerprint=fingerprint,
            credential_path=str(protected_paths()["credential"]),
        )
    finally:
        _wipe(raw)
        try:
            os.close(directory_fd)
        except OSError:
            pass


def main() -> int:
    """Run the fixed hidden-input operator flow with no CLI arguments."""

    result = provision_nado_mainnet_credential()
    print(result.evidence())
    return 0 if result.provisioned else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "BLOCKED",
    "CREDENTIAL_FILENAME",
    "CredentialDiscovery",
    "CURRENT_OFFICIAL_LINKED_SIGNER_SUPPORT",
    "IDENTITY_FILENAME",
    "LINKED_SIGNER_BINDING_PENDING",
    "LINKED_SIGNER_CREDENTIAL",
    "MAIN_WALLET_FALLBACK_CREDENTIAL",
    "MainWalletFallbackProof",
    "MAX_IDENTITY_BYTES",
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
    "OFFICIAL_FALLBACK_REASON",
    "OFFICIAL_FALLBACK_SOURCE",
    "PROTECTED_DIRECTORY",
    "PROTECTED_DIRECTORY_MODE",
    "PROTECTED_FILE_MODE",
    "PROVISIONED",
    "ProtectedFileState",
    "ProtectedFiles",
    "REQUIRED_LIFECYCLE",
    "discover_protected_credential",
    "discover_public_identity",
    "derive_public_identity",
    "export_unsigned_read_identity",
    "inspect_protected_files",
    "main",
    "provision_nado_mainnet_credential",
    "protected_paths",
]
