from __future__ import annotations

import json

from risex_spread_shadow.hood_handoff import DurableJournal, sanitize
from risex_spread_shadow.hood_handoff.cli import main


def test_journal_redacts_private_material_and_keeps_permissions(tmp_path):
    path = tmp_path / "nested" / "journal.jsonl"
    journal = DurableJournal(path, run_id="run-1", clock=lambda: 1.0)
    journal.append(
        "TEST",
        {
            "private_key": "a" * 64,
            "authorization": "token-value",
            "safe_order_id": "123",
            "error": "failed with " + "b" * 64,
        },
    )
    text = path.read_text()
    assert "aaaa" not in text
    assert "token-value" not in text
    assert "safe_order_id" in text
    assert path.stat().st_mode & 0o777 == 0o600


def test_cli_help_and_default_mode_are_offline_safe(tmp_path, capsys):
    assert main([]) == 0
    assert "offline-safe" in capsys.readouterr().out
    config = {
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
        "client_order_prefix": "safe",
        "journal_path": str(tmp_path / "journal.jsonl"),
        "api_base_url": "https://mainnet.zklighter.elliot.ai",
        "chain_id": 304,
        "api_key_index": 4,
    }
    evidence = {"market_id": 7}
    config_path = tmp_path / "config.json"
    evidence_path = tmp_path / "evidence.json"
    config_path.write_text(json.dumps(config))
    evidence_path.write_text(json.dumps(evidence))
    assert main(
        [
            "run",
            "--config",
            str(config_path),
            "--market-evidence",
            str(evidence_path),
            "--source-account-index",
            "11",
            "--receiver-account-index",
            "22",
        ]
    ) == 0
    assert "no SDK import" in capsys.readouterr().out
