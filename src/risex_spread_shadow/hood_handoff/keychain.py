"""Explicit, local macOS Keychain storage for HCR-6 credentials.

The production backend uses the macOS Security framework through a small
``ctypes`` adapter.  The framework is loaded only after an operator explicitly
selects a Keychain operation; importing this module and constructing the
default preview CLI never touches Keychain, credentials, the SDK, or a
network.  Tests can inject :class:`MemoryKeychainBackend` and therefore never
need a real Keychain or credential.
"""

from __future__ import annotations

from dataclasses import dataclass
import ctypes
import hashlib
import json
import platform
import sys
import warnings
from importlib import import_module
from typing import Any, Callable, Mapping, Protocol, Sequence
from urllib.parse import urlsplit


KEYCHAIN_SERVICE = "com.risex.spread-shadow.hood-handoff"
"""Stable service name used for generic-password records."""

KEYCHAIN_BINDING_SCHEMA = 1


class KeychainError(RuntimeError):
    """Base class for safe, non-secret Keychain failures."""


class KeychainUnavailableError(KeychainError):
    """The platform or Security framework cannot provide Keychain access."""


class KeychainAccessError(KeychainError):
    """The Keychain denied or failed an operation."""


class KeychainConflictError(KeychainError):
    """A record exists and explicit replacement was not requested."""


@dataclass(frozen=True, slots=True)
class KeychainBinding:
    """Non-secret identity of one credential in the local Keychain.

    ``api_base_url`` is reduced to an exact HTTPS origin (scheme, host and an
    optional explicit port); paths, credentials, query strings and fragments
    are rejected.  The signing environment and chain are both retained in the
    canonical binding so equal account/key indices cannot cross environments.
    """

    api_base_url: str
    environment: str
    chain_id: int
    account_index: int
    api_key_index: int

    def __post_init__(self) -> None:
        if not isinstance(self.api_base_url, str) or not self.api_base_url.strip():
            raise KeychainError("Keychain binding requires an API origin")
        raw_url = self.api_base_url.strip()
        try:
            parsed = urlsplit(raw_url)
            # Accessing ``port`` also validates malformed numeric ports.
            port = parsed.port
        except (TypeError, ValueError) as exc:
            raise KeychainError("Keychain binding API origin is malformed") from None
        if (
            parsed.scheme.lower() != "https"
            or parsed.hostname is None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            raise KeychainError("Keychain binding API origin must be an HTTPS origin")
        hostname = parsed.hostname.lower()
        normalized = f"https://{hostname}"
        if port is not None:
            normalized += f":{port}"
        object.__setattr__(self, "api_base_url", normalized)

        if not isinstance(self.environment, str) or not self.environment.strip():
            raise KeychainError("Keychain binding signing environment is required")
        environment = self.environment.strip().lower()
        object.__setattr__(self, "environment", environment)
        for value, name in (
            (self.chain_id, "chain_id"),
            (self.account_index, "account_index"),
            (self.api_key_index, "api_key_index"),
        ):
            if isinstance(value, bool) or not isinstance(value, int):
                raise KeychainError(f"Keychain binding {name} must be an integer")
        if self.chain_id < 0:
            raise KeychainError("Keychain binding chain_id must be non-negative")
        if self.account_index < 0:
            raise KeychainError("Keychain binding account_index must be non-negative")
        if not 4 <= self.api_key_index <= 254:
            raise KeychainError("Keychain binding api_key_index must be in 4..254")

    @property
    def api_origin(self) -> str:
        """Alias used by operator-facing terminology."""

        return self.api_base_url

    @property
    def signing_environment(self) -> str:
        return self.environment

    @property
    def canonical(self) -> str:
        """Stable, secret-free representation used to derive the record key."""

        return json.dumps(
            {
                "schema": KEYCHAIN_BINDING_SCHEMA,
                "api_base_url": self.api_base_url,
                "environment": self.environment,
                "chain_id": self.chain_id,
                "account_index": self.account_index,
                "api_key_index": self.api_key_index,
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    @property
    def record_account(self) -> str:
        """Opaque Keychain account attribute containing no credential data."""

        digest = hashlib.sha256(self.canonical.encode("utf-8")).hexdigest()
        return f"binding-v{KEYCHAIN_BINDING_SCHEMA}-{digest}"

    @classmethod
    def from_config(cls, config: Any, account_index: int) -> "KeychainBinding":
        """Construct a binding from an HCR-1/HCR-2/HCR-5 config object."""

        api_base_url = getattr(config, "api_base_url", None)
        environment = getattr(config, "environment", "robinhood")
        chain_id = getattr(config, "chain_id", None)
        api_key_index = getattr(config, "api_key_index", None)
        if api_base_url is None or environment is None or chain_id is None or api_key_index is None:
            raise KeychainError("Keychain binding requires API origin, environment, chain, and key index")
        return cls(
            api_base_url=api_base_url,
            environment=environment,
            chain_id=chain_id,
            account_index=account_index,
            api_key_index=api_key_index,
        )


class KeychainBackend(Protocol):
    """Minimal storage surface, injected by tests and used by the provider."""

    def get(self, binding: KeychainBinding) -> str | None: ...

    def put(self, binding: KeychainBinding, private_key: str, *, replace: bool = False) -> None: ...

    def delete(self, binding: KeychainBinding) -> bool: ...


def _validate_private_key(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise KeychainAccessError("Keychain returned an empty private key")
    if "\x00" in value:
        raise KeychainAccessError("Keychain private key contains an invalid byte")
    return value


class MemoryKeychainBackend:
    """Synthetic backend for offline tests and examples.

    This backend is intentionally in-memory only and is never selected by the
    CLI.  It makes tests able to exercise first-use, reuse, replacement and
    removal without real credentials or a real macOS Keychain.
    """

    def __init__(self, values: Mapping[KeychainBinding, str] | None = None) -> None:
        self.values: dict[KeychainBinding, str] = {}
        for binding, value in (values or {}).items():
            self.values[binding] = _validate_private_key(value)
        self.calls: list[tuple[str, KeychainBinding]] = []

    def get(self, binding: KeychainBinding) -> str | None:
        self.calls.append(("get", binding))
        return self.values.get(binding)

    def put(self, binding: KeychainBinding, private_key: str, *, replace: bool = False) -> None:
        self.calls.append(("replace" if replace else "put", binding))
        value = _validate_private_key(private_key)
        if not replace and binding in self.values:
            raise KeychainConflictError("stored Keychain credential already exists; explicit replacement is required")
        self.values[binding] = value

    def delete(self, binding: KeychainBinding) -> bool:
        self.calls.append(("delete", binding))
        return self.values.pop(binding, None) is not None

    # Friendly aliases keep the injected test backend useful to callers that
    # describe the operation as save/remove rather than put/delete.
    def save(self, binding: KeychainBinding, private_key: str, *, replace: bool = False) -> None:
        self.put(binding, private_key, replace=replace)

    def set(self, binding: KeychainBinding, private_key: str, *, replace: bool = False) -> None:
        self.put(binding, private_key, replace=replace)

    def remove(self, binding: KeychainBinding) -> bool:
        return self.delete(binding)


class _SecurityBindings:
    """Lazy ctypes bindings for CoreFoundation and Security.framework."""

    _UTF8 = 0x08000100

    def __init__(self) -> None:
        if sys.platform != "darwin" or platform.system() != "Darwin":
            raise KeychainUnavailableError("macOS Keychain is unavailable on this platform")
        try:
            ctypes_util = import_module("ctypes.util")
            security_path = ctypes_util.find_library("Security")
            core_foundation_path = ctypes_util.find_library("CoreFoundation")
            if not security_path or not core_foundation_path:
                raise RuntimeError
            self._ctypes = ctypes
            self.security = ctypes.CDLL(security_path)
            self.core_foundation = ctypes.CDLL(core_foundation_path)
            ref = ctypes.c_void_p
            self.core_foundation.CFStringCreateWithCString.argtypes = [ref, ctypes.c_char_p, ctypes.c_uint32]
            self.core_foundation.CFStringCreateWithCString.restype = ref
            self.core_foundation.CFDataCreate.argtypes = [ref, ctypes.POINTER(ctypes.c_ubyte), ctypes.c_long]
            self.core_foundation.CFDataCreate.restype = ref
            self.core_foundation.CFDataGetLength.argtypes = [ref]
            self.core_foundation.CFDataGetLength.restype = ctypes.c_long
            self.core_foundation.CFDataGetBytePtr.argtypes = [ref]
            self.core_foundation.CFDataGetBytePtr.restype = ctypes.POINTER(ctypes.c_ubyte)
            self.core_foundation.CFDictionaryCreateMutable.argtypes = [ref, ctypes.c_long, ref, ref]
            self.core_foundation.CFDictionaryCreateMutable.restype = ref
            self.core_foundation.CFDictionarySetValue.argtypes = [ref, ref, ref]
            self.core_foundation.CFDictionarySetValue.restype = None
            self.core_foundation.CFRelease.argtypes = [ref]
            self.core_foundation.CFRelease.restype = None
            self.security.SecItemCopyMatching.argtypes = [ref, ctypes.POINTER(ref)]
            self.security.SecItemCopyMatching.restype = ctypes.c_int32
            self.security.SecItemAdd.argtypes = [ref, ctypes.POINTER(ref)]
            self.security.SecItemAdd.restype = ctypes.c_int32
            self.security.SecItemUpdate.argtypes = [ref, ref]
            self.security.SecItemUpdate.restype = ctypes.c_int32
            self.security.SecItemDelete.argtypes = [ref]
            self.security.SecItemDelete.restype = ctypes.c_int32
            symbol_names = (
                "kSecClass",
                "kSecClassGenericPassword",
                "kSecAttrService",
                "kSecAttrAccount",
                "kSecValueData",
                "kSecReturnData",
                "kSecMatchLimit",
                "kSecMatchLimitOne",
                "kCFBooleanTrue",
            )
            self.symbols = {
                name: ctypes.c_void_p.in_dll(library, name).value
                for name, library in (
                    *[(name, self.security) for name in symbol_names if name != "kCFBooleanTrue"],
                    ("kCFBooleanTrue", self.core_foundation),
                )
            }
        except KeychainUnavailableError:
            raise
        except Exception:
            # Do not expose loader paths or platform exception text at the
            # credential boundary.
            raise KeychainUnavailableError("macOS Keychain Security framework is unavailable") from None

    def symbol(self, name: str) -> ctypes.c_void_p:
        value = self.symbols.get(name)
        if value is None:
            raise KeychainUnavailableError("macOS Keychain Security symbol is unavailable")
        return ctypes.c_void_p(value)

    def string(self, value: str) -> tuple[ctypes.c_void_p, ctypes.Array[ctypes.c_char]]:
        encoded = value.encode("utf-8")
        buffer = ctypes.create_string_buffer(encoded, len(encoded) + 1)
        result = self.core_foundation.CFStringCreateWithCString(None, buffer, self._UTF8)
        if not result:
            raise KeychainAccessError("macOS Keychain could not create an attribute")
        return result, buffer

    def data(self, value: str) -> tuple[ctypes.c_void_p, ctypes.Array[ctypes.c_char]]:
        encoded = value.encode("utf-8")
        buffer = ctypes.create_string_buffer(encoded, len(encoded))
        pointer = ctypes.cast(buffer, ctypes.POINTER(ctypes.c_ubyte))
        result = self.core_foundation.CFDataCreate(None, pointer, len(encoded))
        if not result:
            raise KeychainAccessError("macOS Keychain could not create credential data")
        return result, buffer

    def dictionary(self, values: Sequence[tuple[str, Any]]) -> tuple[ctypes.c_void_p, list[ctypes.c_void_p], list[Any]]:
        result = self.core_foundation.CFDictionaryCreateMutable(None, 0, None, None)
        if not result:
            raise KeychainAccessError("macOS Keychain could not create a query")
        owned: list[ctypes.c_void_p] = [result]
        keepalive: list[Any] = []
        try:
            for key_name, value in values:
                key = self.symbol(key_name)
                if isinstance(value, str):
                    value_ref, buffer = self.string(value)
                    keepalive.append(buffer)
                    owned.append(value_ref)
                else:
                    value_ref = value
                self.core_foundation.CFDictionarySetValue(result, key, value_ref)
        except Exception:
            self.release(owned)
            raise
        return result, owned, keepalive

    def release(self, values: Sequence[ctypes.c_void_p]) -> None:
        for value in reversed(tuple(values)):
            try:
                if value:
                    self.core_foundation.CFRelease(value)
            except Exception:
                pass

    def status_error(self, status: int, operation: str) -> KeychainError:
        if status in {-25293, -25308, -34018}:
            return KeychainAccessError(f"macOS Keychain {operation} was denied")
        if status == -25291:
            return KeychainUnavailableError(f"macOS Keychain {operation} is unavailable")
        return KeychainAccessError(f"macOS Keychain {operation} failed")


class MacOSKeychainBackend:
    """Native Security.framework generic-password backend.

    The object is lazy: creating it is side-effect free, while the first
    ``get``/``put``/``delete`` call loads Security.framework and performs the
    explicitly requested operation.  No ``security`` subprocess or secret
    command-line argument is used.
    """

    def __init__(self) -> None:
        self._native: _SecurityBindings | None = None

    @property
    def native(self) -> _SecurityBindings:
        if self._native is None:
            self._native = _SecurityBindings()
        return self._native

    def _query(self, binding: KeychainBinding, *, return_data: bool = False) -> tuple[_SecurityBindings, ctypes.c_void_p, list[ctypes.c_void_p], list[Any]]:
        native = self.native
        values: list[tuple[str, Any]] = [
            ("kSecClass", native.symbol("kSecClassGenericPassword")),
            ("kSecAttrService", KEYCHAIN_SERVICE),
            ("kSecAttrAccount", binding.record_account),
        ]
        if return_data:
            values.extend(
                [
                    ("kSecReturnData", native.symbol("kCFBooleanTrue")),
                    ("kSecMatchLimit", native.symbol("kSecMatchLimitOne")),
                ]
            )
        query, owned, keepalive = native.dictionary(values)
        return native, query, owned, keepalive

    def get(self, binding: KeychainBinding) -> str | None:
        native, query, owned, keepalive = self._query(binding, return_data=True)
        del keepalive
        result = ctypes.c_void_p()
        try:
            status = int(native.security.SecItemCopyMatching(query, ctypes.byref(result)))
            if status == -25300:
                return None
            if status != 0:
                raise native.status_error(status, "read")
            if not result:
                raise KeychainAccessError("macOS Keychain returned no credential data")
            length = int(native.core_foundation.CFDataGetLength(result))
            pointer = native.core_foundation.CFDataGetBytePtr(result)
            if length <= 0 or not pointer:
                raise KeychainAccessError("macOS Keychain returned empty credential data")
            try:
                value = ctypes.string_at(pointer, length).decode("utf-8")
            except (UnicodeDecodeError, ValueError):
                raise KeychainAccessError("macOS Keychain returned invalid credential data") from None
            return _validate_private_key(value)
        finally:
            if result:
                native.release([result])
            native.release(owned)

    def put(self, binding: KeychainBinding, private_key: str, *, replace: bool = False) -> None:
        value = _validate_private_key(private_key)
        native = self.native
        # SecItemAdd receives one item with value data.  The query for an
        # explicit replacement contains the same non-secret identity.
        data_ref, data_buffer = native.data(value)
        del data_buffer
        item_values: list[tuple[str, Any]] = [
            ("kSecClass", native.symbol("kSecClassGenericPassword")),
            ("kSecAttrService", KEYCHAIN_SERVICE),
            ("kSecAttrAccount", binding.record_account),
            ("kSecValueData", data_ref),
        ]
        owned: list[ctypes.c_void_p] = [data_ref]
        try:
            item, item_owned, item_keepalive = native.dictionary(item_values)
            del item_keepalive
            owned.extend(item_owned)
            if replace:
                attrs, attrs_owned, attrs_keepalive = native.dictionary([("kSecValueData", data_ref)])
                del attrs_keepalive
                owned.extend(attrs_owned)
                _native, query, query_owned, query_keepalive = self._query(binding)
                del query_keepalive
                try:
                    status = int(native.security.SecItemUpdate(query, attrs))
                finally:
                    native.release(query_owned)
                if status == -25300:
                    # A replace operation also supports the safe first-use
                    # path if no matching record exists.
                    status = int(native.security.SecItemAdd(item, None))
            else:
                status = int(native.security.SecItemAdd(item, None))
            if status == -25299:
                raise KeychainConflictError("stored Keychain credential already exists; explicit replacement is required")
            if status != 0:
                raise native.status_error(status, "write")
        finally:
            native.release(owned)

    def delete(self, binding: KeychainBinding) -> bool:
        native, query, owned, keepalive = self._query(binding)
        del keepalive
        try:
            status = int(native.security.SecItemDelete(query))
            if status == -25300:
                return False
            if status != 0:
                raise native.status_error(status, "remove")
            return True
        finally:
            native.release(owned)

    def save(self, binding: KeychainBinding, private_key: str, *, replace: bool = False) -> None:
        self.put(binding, private_key, replace=replace)

    def set(self, binding: KeychainBinding, private_key: str, *, replace: bool = False) -> None:
        self.put(binding, private_key, replace=replace)

    def remove(self, binding: KeychainBinding) -> bool:
        return self.delete(binding)


def read_hidden_secret(prompt: str) -> str:
    """Read a secret only from a TTY and reject every echo fallback."""

    if not sys.stdin.isatty() or not sys.stderr.isatty():
        raise RuntimeError("hidden API-key input requires an interactive TTY")
    try:
        with warnings.catch_warnings():
            # Python's getpass fallback warns before reading visible input;
            # turning that warning into an exception fails closed.
            warnings.simplefilter("error", getpass_warning_type())
            value = _getpass(prompt)
    except Warning:
        raise RuntimeError("hidden API-key input unavailable; echo fallback is refused") from None
    except (EOFError, KeyboardInterrupt):
        raise RuntimeError("hidden API-key input was cancelled") from None
    except Exception:
        raise RuntimeError("hidden API-key input is unavailable") from None
    if not isinstance(value, str) or not value:
        raise RuntimeError("empty API key input")
    if "\x00" in value:
        raise RuntimeError("hidden API key input is invalid")
    return value


def getpass_warning_type() -> type[Warning]:
    # Import getpass lazily so preview/help/import remain lightweight and the
    # module has no platform credential side effects.
    getpass = import_module("getpass")
    return getpass.GetPassWarning


def _getpass(prompt: str) -> str:
    getpass = import_module("getpass")
    return getpass.getpass(prompt, stream=sys.stderr)


class KeychainSecretProvider:
    """SecretProvider backed by explicit Keychain reuse and hidden first use."""

    def __init__(
        self,
        account_indices: Sequence[int],
        api_key_index: int,
        *,
        api_base_url: str,
        environment: str,
        chain_id: int,
        backend: KeychainBackend | None = None,
        replace: bool = False,
        prompt: Callable[[str], str] | None = None,
    ) -> None:
        self._account_indices = frozenset(account_indices)
        self._api_key_index = api_key_index
        self._api_base_url = api_base_url
        self._environment = environment
        self._chain_id = chain_id
        self._replace = bool(replace)
        self._prompt = prompt or read_hidden_secret
        self._values: dict[int, str] = {}
        self._bindings = {
            account_index: KeychainBinding(
                api_base_url=api_base_url,
                environment=environment,
                chain_id=chain_id,
                account_index=account_index,
                api_key_index=api_key_index,
            )
            for account_index in self._account_indices
        }
        # Validate every binding before constructing the native backend.  An
        # invalid configuration therefore cannot even initialize a Keychain
        # adapter, let alone access credentials.
        # An injected backend is authoritative even when it defines a falsey
        # truth value (for example, a minimal zero-length test double).
        self._backend = backend if backend is not None else MacOSKeychainBackend()

    @classmethod
    def from_config(
        cls,
        config: Any,
        account_indices: Sequence[int],
        *,
        backend: KeychainBackend | None = None,
        replace: bool = False,
        prompt: Callable[[str], str] | None = None,
    ) -> "KeychainSecretProvider":
        api_base_url = getattr(config, "api_base_url", None)
        environment = getattr(config, "environment", "robinhood")
        chain_id = getattr(config, "chain_id", None)
        api_key_index = getattr(config, "api_key_index", None)
        if api_base_url is None or environment is None or chain_id is None or api_key_index is None:
            raise KeychainError("Keychain provider requires API origin, environment, chain, and key index")
        return cls(
            account_indices,
            api_key_index,
            api_base_url=api_base_url,
            environment=environment,
            chain_id=chain_id,
            backend=backend,
            replace=replace,
            prompt=prompt,
        )

    @property
    def backend(self) -> KeychainBackend:
        return self._backend

    @property
    def replace(self) -> bool:
        return self._replace

    @property
    def api_key_index(self) -> int:
        return self._api_key_index

    def _check_request(self, account_index: int, api_key_index: int) -> KeychainBinding:
        if (
            isinstance(account_index, bool)
            or not isinstance(account_index, int)
            or isinstance(api_key_index, bool)
            or not isinstance(api_key_index, int)
            or api_key_index != self._api_key_index
            or account_index not in self._account_indices
        ):
            raise KeychainError("secret request does not match configured account/key index")
        try:
            return self._bindings[account_index]
        except KeyError:
            raise KeychainError("secret request does not match configured account/key index") from None

    @staticmethod
    def _backend_failure(operation: str, exc: BaseException) -> KeychainError:
        if isinstance(exc, KeychainUnavailableError):
            return KeychainUnavailableError("macOS Keychain is unavailable")
        if isinstance(exc, KeychainConflictError):
            return KeychainConflictError("stored Keychain credential already exists; explicit replacement is required")
        if isinstance(exc, KeychainAccessError):
            return KeychainAccessError("macOS Keychain access was denied or failed")
        if isinstance(exc, KeychainError):
            return KeychainAccessError(f"Keychain {operation} failed")
        # Never include arbitrary backend text: synthetic backends and OS
        # wrappers may accidentally include the credential in an exception.
        return KeychainAccessError(f"Keychain {operation} failed")

    def _get(self, binding: KeychainBinding) -> str | None:
        try:
            value = self._backend.get(binding)
        except Exception as exc:
            raise self._backend_failure("read", exc) from None
        if value is None:
            return None
        return _validate_private_key(value)

    def _put(self, binding: KeychainBinding, value: str, *, replace: bool) -> None:
        try:
            put = getattr(self._backend, "put", None)
            if not callable(put):
                put = getattr(self._backend, "save", None)
            if not callable(put):
                put = getattr(self._backend, "set", None)
            if not callable(put):
                raise TypeError("Keychain backend has no write operation")
            put(binding, value, replace=replace)
        except Exception as exc:
            raise self._backend_failure("write", exc) from None

    def _delete(self, binding: KeychainBinding) -> bool:
        try:
            delete = getattr(self._backend, "delete", None)
            if not callable(delete):
                delete = getattr(self._backend, "remove", None)
            if not callable(delete):
                raise TypeError("Keychain backend has no remove operation")
            return bool(delete(binding))
        except Exception as exc:
            raise self._backend_failure("remove", exc) from None

    def private_key(self, account_index: int, api_key_index: int) -> str:
        binding = self._check_request(account_index, api_key_index)
        cached = self._values.get(account_index)
        if cached is not None:
            return cached
        if self._replace:
            value = _validate_private_key(self._prompt(
                f"Lighter private key for account {account_index} (hidden replacement input): "
            ))
            self._put(binding, value, replace=True)
        else:
            value = self._get(binding)
            if value is None:
                value = _validate_private_key(self._prompt(
                    f"Lighter private key for account {account_index} (hidden input; saved to Keychain): "
                ))
                self._put(binding, value, replace=False)
        self._values[account_index] = value
        return value

    def remove(self, account_index: int) -> bool:
        binding = self._check_request(account_index, self._api_key_index)
        return self._delete(binding)

    def close(self) -> None:
        self._values.clear()

    def __enter__(self) -> "KeychainSecretProvider":
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()


# Descriptive aliases keep the storage boundary discoverable to callers that
# use either credential-store or backend terminology.
CredentialBinding = KeychainBinding
MacOSKeychainStore = MacOSKeychainBackend
SyntheticKeychainBackend = MemoryKeychainBackend


__all__ = [
    "CredentialBinding",
    "KEYCHAIN_BINDING_SCHEMA",
    "KEYCHAIN_SERVICE",
    "KeychainAccessError",
    "KeychainBackend",
    "KeychainBinding",
    "KeychainConflictError",
    "KeychainError",
    "KeychainSecretProvider",
    "KeychainUnavailableError",
    "MacOSKeychainBackend",
    "MacOSKeychainStore",
    "MemoryKeychainBackend",
    "SyntheticKeychainBackend",
    "read_hidden_secret",
]
