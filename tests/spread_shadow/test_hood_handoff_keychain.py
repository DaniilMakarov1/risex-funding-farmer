from __future__ import annotations

import getpass
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from risex_spread_shadow.hood_handoff import (
    HandoffConfig,
    KeychainAccessError,
    KeychainBinding,
    KeychainConflictError,
    KeychainError,
    KeychainSecretProvider,
    KeychainUnavailableError,
    LighterSdkClient,
    MacOSKeychainBackend,
    MemoryKeychainBackend,
    ReadinessConfig,
    ReadOnlyLighterSdkClient,
)
from risex_spread_shadow.hood_handoff import cli
from risex_spread_shadow.hood_handoff import keychain as keychain_module


def binding(**overrides) -> KeychainBinding:
    values = {
        "api_base_url": "https://api.rh.lighter.xyz",
        "environment": "robinhood",
        "chain_id": 466324,
        "account_index": 11,
        "api_key_index": 4,
    }
    values.update(overrides)
    return KeychainBinding(**values)


def readiness_config(**overrides) -> ReadinessConfig:
    values = {
        "market_symbol": "BTC",
        "quantity": "0.20",
        "direction": "LONG",
        "source_account_index": 11,
        "receiver_account_index": 22,
        "api_key_index": 4,
        "freshness_seconds": 10,
        "request_timeout_seconds": 1,
    }
    values.update(overrides)
    return ReadinessConfig(**values)


def handoff_config(tmp_path: Path, **overrides) -> HandoffConfig:
    values = {
        "market_id": 7,
        "direction": "LONG",
        "quantity": "0.125",
        "source_limit_price": "100.25",
        "receiver_worst_price": "101.25",
        "freshness_seconds": 10,
        "request_timeout_seconds": 1,
        "order_timeout_seconds": 1,
        "reconcile_timeout_seconds": 1,
        "poll_interval_seconds": 0.1,
        "max_poll_count": 2,
        "source_order_lifetime_seconds": 300,
        "client_order_prefix": "keychain-test",
        "journal_path": str(tmp_path / "journal.jsonl"),
        "market_symbol": "HOOD",
        "environment": "mainnet",
        "api_base_url": "https://mainnet.zklighter.elliot.ai",
        "chain_id": 304,
        "api_key_index": 4,
    }
    values.update(overrides)
    return HandoffConfig(**values)


def test_binding_is_exactly_isolated_by_origin_environment_account_and_key_index():
    first = binding()
    assert "secret" not in first.canonical.lower()
    assert first.api_origin == first.api_base_url
    assert first.signing_environment == first.environment
    variants = (
        binding(api_base_url="https://mainnet.zklighter.elliot.ai", environment="mainnet", chain_id=304),
        binding(environment="other"),
        binding(chain_id=304),
        binding(account_index=22),
        binding(api_key_index=5),
    )
    assert len({item.record_account for item in (first, *variants)}) == 6


@pytest.mark.parametrize(
    "value",
    (
        "http://api.rh.lighter.xyz",
        "https://api.rh.lighter.xyz/path",
        "https://user:password@api.rh.lighter.xyz",
        "https://api.rh.lighter.xyz?token=secret",
        "https://api.rh.lighter.xyz/#fragment",
    ),
)
def test_binding_rejects_non_origin_inputs_without_keychain_access(value):
    with pytest.raises(KeychainError, match="API origin"):
        binding(api_base_url=value)


def test_memory_backend_first_save_replacement_and_removal():
    store = MemoryKeychainBackend()
    first = binding()
    store.put(first, "synthetic-a")
    assert store.get(first) == "synthetic-a"
    with pytest.raises(KeychainConflictError):
        store.put(first, "synthetic-b")
    store.put(first, "synthetic-b", replace=True)
    assert store.get(first) == "synthetic-b"
    assert store.delete(first) is True
    assert store.delete(first) is False


def test_provider_saves_first_hidden_value_then_reuses_without_prompt():
    store = MemoryKeychainBackend()
    prompts: list[str] = []

    def prompt(value: str) -> str:
        prompts.append(value)
        return "synthetic-private-key"

    first = KeychainSecretProvider(
        (11,),
        4,
        api_base_url="https://api.rh.lighter.xyz",
        environment="robinhood",
        chain_id=466324,
        backend=store,
        prompt=prompt,
    )
    assert first.private_key(11, 4) == "synthetic-private-key"
    assert len(prompts) == 1
    first.close()

    def fail_prompt(value: str) -> str:
        raise AssertionError("stored credential should be reused")

    second = KeychainSecretProvider(
        (11,),
        4,
        api_base_url="https://api.rh.lighter.xyz",
        environment="robinhood",
        chain_id=466324,
        backend=store,
        prompt=fail_prompt,
    )
    assert second.private_key(11, 4) == "synthetic-private-key"
    assert [name for name, _ in store.calls] == ["get", "put", "get"]
    second.close()


def test_provider_replacement_is_explicit_and_remove_never_returns_the_secret():
    store = MemoryKeychainBackend({binding(): "old-key"})
    prompts: list[str] = []
    provider = KeychainSecretProvider(
        (11,),
        4,
        api_base_url="https://api.rh.lighter.xyz",
        environment="robinhood",
        chain_id=466324,
        backend=store,
        replace=True,
        prompt=lambda message: prompts.append(message) or "new-key",
    )
    assert provider.private_key(11, 4) == "new-key"
    assert store.get(binding()) == "new-key"
    assert any("replacement" in item for item in prompts)
    assert provider.remove(11) is True
    assert store.get(binding()) is None
    provider.close()


def test_provider_sanitizes_secret_bearing_backend_exceptions():
    class DeniedBackend:
        def get(self, current):
            raise RuntimeError("private_key=backend-secret-value")

        def put(self, current, private_key, *, replace=False):
            raise RuntimeError("private_key=backend-secret-value")

        def delete(self, current):
            raise RuntimeError("private_key=backend-secret-value")

    provider = KeychainSecretProvider(
        (11,),
        4,
        api_base_url="https://api.rh.lighter.xyz",
        environment="robinhood",
        chain_id=466324,
        backend=DeniedBackend(),
        prompt=lambda _: "never-used",
    )
    with pytest.raises(KeychainAccessError) as error:
        provider.private_key(11, 4)
    assert "backend-secret-value" not in str(error.value)
    provider.close()


def test_provider_sanitizes_secret_bearing_typed_backend_exceptions():
    class DeniedBackend(MemoryKeychainBackend):
        def get(self, current):
            raise KeychainUnavailableError("private_key=backend-secret-value")

    provider = KeychainSecretProvider(
        (11,),
        4,
        api_base_url="https://api.rh.lighter.xyz",
        environment="robinhood",
        chain_id=466324,
        backend=DeniedBackend(),
        prompt=lambda _: "never-used",
    )
    with pytest.raises(KeychainUnavailableError) as error:
        provider.private_key(11, 4)
    assert "backend-secret-value" not in str(error.value)
    provider.close()


def test_hidden_input_fails_closed_when_tty_is_missing(monkeypatch):
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    monkeypatch.setattr("sys.stderr.isatty", lambda: False)
    with pytest.raises(RuntimeError, match="interactive TTY"):
        keychain_module.read_hidden_secret("hidden: ")


def test_hidden_input_rejects_getpass_echo_fallback(monkeypatch):
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("sys.stderr.isatty", lambda: True)

    def visible_fallback(prompt):
        warnings = getpass.GetPassWarning("fallback")
        import warnings as warnings_module

        warnings_module.warn(warnings)
        return "visible-secret"

    monkeypatch.setattr(keychain_module, "_getpass", visible_fallback)
    with pytest.raises(RuntimeError, match="echo fallback"):
        keychain_module.read_hidden_secret("hidden: ")


def test_native_backend_is_lazy_and_unavailable_only_on_explicit_operation(monkeypatch):
    backend = MacOSKeychainBackend()
    assert backend._native is None
    monkeypatch.setattr(keychain_module.sys, "platform", "linux")
    with pytest.raises(KeychainUnavailableError, match="unavailable"):
        backend.get(binding())
    assert backend._native is None


class FakeSecurity:
    def __init__(self, *, update_status: int = 0, add_status: int = 0):
        self.update_status = update_status
        self.add_status = add_status
        self.calls: list[tuple[str, object]] = []

    def SecItemAdd(self, item, result):
        self.calls.append(("add", item))
        return self.add_status

    def SecItemUpdate(self, query, attrs):
        self.calls.append(("update", (query, attrs)))
        return self.update_status

    def SecItemDelete(self, query):
        self.calls.append(("delete", query))
        return 0


class FakeNative:
    def __init__(self, *, update_status: int = 0, add_status: int = 0):
        self.security = FakeSecurity(update_status=update_status, add_status=add_status)
        self.core_foundation = SimpleNamespace()
        self.dict_values: list[list[tuple[str, object]]] = []
        self.released: list[object] = []

    def symbol(self, name):
        return f"symbol:{name}"

    def data(self, value):
        return f"data:{value}", object()

    def dictionary(self, values):
        normalized = list(values)
        self.dict_values.append(normalized)
        dictionary = f"dict:{len(self.dict_values)}"
        return dictionary, [dictionary], []

    def release(self, values):
        self.released.extend(values)

    def status_error(self, status, operation):
        return KeychainAccessError(f"fake {operation} failed")


def test_native_backend_first_save_puts_value_data_in_secitemadd(monkeypatch):
    native = FakeNative()
    backend = MacOSKeychainBackend()
    backend._native = native
    backend.put(binding(), "synthetic-secret")
    assert [name for name, _ in native.security.calls] == ["add"]
    add_values = native.dict_values[0]
    assert any(name == "kSecValueData" and value == "data:synthetic-secret" for name, value in add_values)
    assert "synthetic-secret" in repr(add_values)
    # The secret only appears in this in-memory fake call, never in a CLI,
    # journal, file or exception path.


def test_native_backend_replace_updates_value_and_adds_when_missing():
    native = FakeNative(update_status=0)
    backend = MacOSKeychainBackend()
    backend._native = native
    backend.put(binding(), "replacement-secret", replace=True)
    assert [name for name, _ in native.security.calls] == ["update"]
    assert any(name == "kSecValueData" and value == "data:replacement-secret" for name, value in native.dict_values[1])

    missing = FakeNative(update_status=-25300, add_status=0)
    backend = MacOSKeychainBackend()
    backend._native = missing
    backend.put(binding(), "first-secret", replace=True)
    assert [name for name, _ in missing.security.calls] == ["update", "add"]
    assert any(name == "kSecValueData" and value == "data:first-secret" for name, value in missing.dict_values[0])


def test_cli_preview_with_keychain_flag_never_constructs_backend(tmp_path, monkeypatch, capsys):
    config_path = tmp_path / "config.json"
    evidence_path = tmp_path / "evidence.json"
    config_path.write_text(json.dumps({
        "market_id": 7,
        "direction": "LONG",
        "quantity": "0.125",
        "source_limit_price": "100.25",
        "receiver_worst_price": "101.25",
        "freshness_seconds": 10,
        "request_timeout_seconds": 1,
        "order_timeout_seconds": 1,
        "reconcile_timeout_seconds": 1,
        "poll_interval_seconds": 0.1,
        "max_poll_count": 2,
        "source_order_lifetime_seconds": 300,
        "client_order_prefix": "preview",
        "journal_path": str(tmp_path / "journal.jsonl"),
        "api_base_url": "https://mainnet.zklighter.elliot.ai",
        "chain_id": 304,
        "api_key_index": 4,
    }), encoding="utf-8")
    evidence_path.write_text(json.dumps({"market_id": 7}), encoding="utf-8")

    def exploding_backend():
        raise AssertionError("preview must not construct a Keychain backend")

    monkeypatch.setattr(cli, "MacOSKeychainBackend", exploding_backend)
    assert cli.main([
        "run", "--config", str(config_path), "--market-evidence", str(evidence_path),
        "--source-account-index", "11", "--receiver-account-index", "22", "--keychain",
    ]) == 0
    preview = json.loads(capsys.readouterr().out)
    assert preview["execution"] == "DISABLED"


def test_cli_invalid_config_with_keychain_flag_never_constructs_backend(tmp_path, monkeypatch):
    config_path = tmp_path / "invalid.json"
    config_path.write_text(json.dumps({"market_id": 7}), encoding="utf-8")
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_text(json.dumps({"market_id": 7}), encoding="utf-8")

    def exploding_backend():
        raise AssertionError("invalid config must not construct a Keychain backend")

    monkeypatch.setattr(cli, "MacOSKeychainBackend", exploding_backend)
    with pytest.raises(SystemExit, match="missing required fields"):
        cli.main([
            "run", "--config", str(config_path), "--market-evidence", str(evidence_path),
            "--source-account-index", "11", "--receiver-account-index", "22", "--keychain",
        ])


def test_cli_keychain_remove_uses_exact_bindings_without_evidence_or_sdk(tmp_path, monkeypatch, capsys):
    config = handoff_config(tmp_path)
    store = MemoryKeychainBackend({
        KeychainBinding.from_config(config, 11): "synthetic-source",
        KeychainBinding.from_config(config, 22): "synthetic-receiver",
    })
    monkeypatch.setattr(cli, "MacOSKeychainBackend", lambda: store)
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({
        "market_id": 7,
        "direction": "LONG",
        "quantity": "0.125",
        "source_limit_price": "100.25",
        "receiver_worst_price": "101.25",
        "freshness_seconds": 10,
        "request_timeout_seconds": 1,
        "order_timeout_seconds": 1,
        "reconcile_timeout_seconds": 1,
        "poll_interval_seconds": 0.1,
        "max_poll_count": 2,
        "source_order_lifetime_seconds": 300,
        "client_order_prefix": "remove",
        "journal_path": str(tmp_path / "journal.jsonl"),
        "market_symbol": "HOOD",
        "environment": "mainnet",
        "api_base_url": "https://mainnet.zklighter.elliot.ai",
        "chain_id": 304,
        "api_key_index": 4,
    }), encoding="utf-8")
    assert cli.main([
        "run", "--config", str(config_path), "--source-account-index", "11",
        "--receiver-account-index", "22", "--keychain-remove",
    ]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["outcome"] == "KEYCHAIN_REMOVED"
    assert all(item["removed"] for item in payload["bindings"])
    assert not store.values


def test_provider_from_readiness_and_handoff_configs_binds_origin_environment_and_chain(tmp_path):
    readiness = readiness_config()
    store = MemoryKeychainBackend()
    provider = KeychainSecretProvider.from_config(
        readiness,
        (11, 22),
        backend=store,
        prompt=lambda _: "readiness-secret",
    )
    assert provider.private_key(11, 4) == "readiness-secret"
    readiness_binding = KeychainBinding.from_config(readiness, 11)
    handoff = handoff_config(tmp_path)
    handoff_binding = KeychainBinding.from_config(handoff, 11)
    assert readiness_binding != handoff_binding
    assert readiness_binding.record_account != handoff_binding.record_account
    provider.close()


def test_keychain_provider_feeds_readiness_and_execution_signers_without_network(tmp_path):
    class Signer:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    module = SimpleNamespace(SignerClient=Signer, nonce_manager=SimpleNamespace())
    handoff = handoff_config(tmp_path)
    readiness = readiness_config()
    store = MemoryKeychainBackend({
        KeychainBinding.from_config(handoff, 11): "execution-source",
        KeychainBinding.from_config(readiness, 11): "readiness-source",
    })

    execution_provider = KeychainSecretProvider.from_config(
        handoff,
        (11,),
        backend=store,
    )
    execution_client = LighterSdkClient(
        handoff,
        source_account_index=11,
        receiver_account_index=22,
        secrets=execution_provider,
        market_evidence={},
        signer_factory=Signer,
    )
    execution_client._lighter = lambda: module
    execution_signer = execution_client._signer(11)
    assert execution_signer.kwargs["api_private_keys"] == {4: "execution-source"}
    execution_provider.close()

    readiness_provider = KeychainSecretProvider.from_config(
        readiness,
        (11,),
        backend=store,
    )
    readiness_client = ReadOnlyLighterSdkClient(
        readiness,
        source_account_index=11,
        receiver_account_index=22,
        secrets=readiness_provider,
        signer_factory=Signer,
    )
    readiness_client._lighter = lambda: module
    readiness_signer = readiness_client._auth_signer(11)
    assert readiness_signer.kwargs["api_private_keys"] == {4: "readiness-source"}
    readiness_provider.close()
