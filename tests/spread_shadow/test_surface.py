from __future__ import annotations

import ast
from dataclasses import fields
from pathlib import Path

from risex_spread_shadow import TradeEvidence


def test_spread_pure_contracts_have_no_runtime_or_persistence_surface() -> None:
    root = Path(__file__).parents[2] / "src" / "risex_spread_shadow"
    pure_paths = {
        root / "models.py",
        root / "economics.py",
        root / "evidence.py",
    }
    forbidden_tokens = {
        "scanner",
        "paper_broker",
        "lifecycle",
        "runtime",
        "storage",
        "notifications",
        "orchestrator",
        "cli",
        "config",
        "private",
        "auth",
        "credential",
        "sign",
        "dispatch",
        "socket",
        "websocket",
        "sqlite",
        "serializer",
        "json",
    }
    for path in pure_paths:
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                assert node.name not in {"serialize", "deserialize", "__hash__"}
            if isinstance(node, ast.Import):
                imported = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                imported = [node.module or ""]
            else:
                continue
            assert not any(
                any(token in name.lower() for token in forbidden_tokens) for name in imported
            ), (path, imported)
    assert "exchange_monotonic_ns" not in {field.name for field in fields(TradeEvidence)}


def test_spread_runtime_surface_excludes_legacy_strategy_and_write_paths() -> None:
    root = Path(__file__).parents[2] / "src" / "risex_spread_shadow"
    forbidden = {
        "risex_farmer.scanner",
        "risex_farmer.paper_broker",
        "risex_farmer.lifecycle",
        "risex_farmer.runtime",
        "risex_farmer.storage",
        "risex_farmer.notifications",
        "risex_farmer.orchestrator",
        "risex_farmer.testnet",
        "risex_farmer.mainnet",
        "risex_farmer.private",
        "risex_farmer.signer",
    }
    for path in root.glob("*.py"):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                imported = [node.module or ""]
            else:
                continue
            assert not any(
                any(name == token or name.startswith(token + ".") for token in forbidden)
                for name in imported
            ), (path, imported)
