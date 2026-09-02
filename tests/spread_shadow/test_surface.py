from __future__ import annotations

import ast
from dataclasses import fields
from pathlib import Path

from risex_spread_shadow import TradeEvidence


def test_spread_package_has_only_pure_allowed_imports_and_no_serializer_surface() -> None:
    root = Path(__file__).parents[2] / "src" / "risex_spread_shadow"
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
    for path in root.glob("*.py"):
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
