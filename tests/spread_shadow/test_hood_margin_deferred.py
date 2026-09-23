from __future__ import annotations

import json
from dataclasses import replace
from decimal import Decimal

import pytest

from risex_spread_shadow.hood_handoff import (
    ContractError,
    LighterSdkClient,
    MutationReceipt,
    OperationMode,
    Outcome,
    StaticSecretProvider,
    run_handoff,
    run_series,
)
from risex_spread_shadow.hood_handoff.cli import _config, _series_config, main

from test_hood_handoff_paired_opening import (
    Clock,
    PairedClient,
    SeriesPairedClient,
    _operator_config_payload,
    _operator_series_payload,
    config as paired_config,
    series_config as paired_series_config,
)


class MissingIncrementalPairedClient(PairedClient):
    async def account_snapshot(self, account_index: int, market_id: int):
        snapshot = await super().account_snapshot(account_index, market_id)
        return replace(
            snapshot,
            incremental_margin_required=None,
            incremental_margin_evidence="",
        )


class MissingIncrementalSeriesClient(SeriesPairedClient):
    async def account_snapshot(self, account_index: int, market_id: int):
        snapshot = await super().account_snapshot(account_index, market_id)
        return replace(
            snapshot,
            incremental_margin_required=None,
            incremental_margin_evidence="",
        )


class MalformedIncrementalPairedClient(PairedClient):
    async def account_snapshot(self, account_index: int, market_id: int):
        snapshot = await super().account_snapshot(account_index, market_id)
        return replace(snapshot, incremental_margin_required=Decimal("-1"))


class RejectingMissingIncrementalClient(MissingIncrementalPairedClient):
    async def submit_order(self, plan):
        if plan.account_index == self.receiver_account_index:
            self.submissions.append(plan)
            return MutationReceipt(False, None, None, "synthetic exchange rejection")
        return await super().submit_order(plan)


class CrashAfterSourceDispatchMissingClient(MissingIncrementalPairedClient):
    async def submit_order(self, plan):
        if plan.account_index == self.source_account_index:
            self.submissions.append(plan)
            raise TimeoutError("synthetic dispatch ambiguity")
        return await super().submit_order(plan)


def test_deferred_margin_configuration_is_strict_and_exact():
    with pytest.raises(ContractError, match="defer_incremental_margin_calculation must be bool"):
        paired_config(
            "/tmp/hcr8-invalid.jsonl",
            defer_incremental_margin_calculation="true",
        )
    with pytest.raises(ContractError, match="only supported for PAIRED_OPENING"):
        paired_config(
            "/tmp/hcr8-close.jsonl",
            operation_mode=OperationMode.CLOSE_REOPEN,
            defer_incremental_margin_calculation=True,
        )

    raw = _operator_config_payload()
    raw["environment"] = "robinhood"
    assert _config(raw, execute=False).defer_incremental_margin_calculation is False
    raw["defer_incremental_margin_calculation"] = "true"
    with pytest.raises(SystemExit, match="must be bool"):
        _config(raw, execute=False)


@pytest.mark.asyncio
async def test_missing_incremental_margin_remains_strict_by_default(tmp_path):
    client = MissingIncrementalPairedClient()
    result = await run_handoff(
        paired_config(tmp_path / "strict.jsonl"),
        client,
        clock=Clock(),
    )

    assert result.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED
    assert client.submissions == []


@pytest.mark.asyncio
async def test_deferred_margin_allows_only_missing_local_estimate_and_binds_policy(tmp_path):
    path = tmp_path / "deferred.jsonl"
    client = MissingIncrementalPairedClient()
    result = await run_handoff(
        paired_config(path, defer_incremental_margin_calculation=True),
        client,
        clock=Clock(),
    )

    assert result.outcome is Outcome.SUCCESS
    assert result.plan is not None
    assert result.plan.defer_incremental_margin_calculation is True
    assert result.as_dict()["plan"]["defer_incremental_margin_calculation"] is True
    journal = path.read_text(encoding="utf-8")
    assert '"defer_incremental_margin_calculation":true' in journal
    assert len(client.submissions) == 2


@pytest.mark.asyncio
async def test_deferred_open_does_not_compare_reserved_margin_with_free_balance_and_keeps_position_gate(tmp_path):
    insufficient = MissingIncrementalPairedClient(source_margin=Decimal("0.5"))
    result = await run_handoff(
        paired_config(
            tmp_path / "current-margin.jsonl",
            defer_incremental_margin_calculation=True,
        ),
        insufficient,
        clock=Clock(),
    )
    assert result.outcome is Outcome.SUCCESS
    assert len(insufficient.submissions) == 2

    nonflat = MissingIncrementalPairedClient()
    nonflat.source_position = Decimal("0.01")
    result = await run_handoff(
        paired_config(
            tmp_path / "nonflat.jsonl",
            defer_incremental_margin_calculation=True,
        ),
        nonflat,
        clock=Clock(),
    )
    assert result.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED
    assert nonflat.submissions == []


@pytest.mark.asyncio
async def test_valid_supplied_incremental_estimate_still_blocks_when_unaffordable(tmp_path):
    client = PairedClient(source_margin=Decimal("1"))
    result = await run_handoff(
        paired_config(
            tmp_path / "supplied-estimate.jsonl",
            defer_incremental_margin_calculation=True,
        ),
        client,
        clock=Clock(),
    )
    assert result.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED
    assert client.submissions == []


@pytest.mark.asyncio
async def test_malformed_supplied_incremental_estimate_is_still_rejected(tmp_path):
    client = MalformedIncrementalPairedClient()
    result = await run_handoff(
        paired_config(
            tmp_path / "malformed-estimate.jsonl",
            defer_incremental_margin_calculation=True,
        ),
        client,
        clock=Clock(),
    )
    assert result.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED
    assert client.submissions == []


@pytest.mark.asyncio
async def test_direct_restart_rejects_margin_policy_change_without_replay(tmp_path):
    path = tmp_path / "policy-restart.jsonl"
    first = await run_handoff(
        paired_config(path, defer_incremental_margin_calculation=True),
        CrashAfterSourceDispatchMissingClient(),
        clock=Clock(),
    )
    assert first.outcome is Outcome.UNKNOWN

    resumed_client = MissingIncrementalPairedClient()
    resumed = await run_handoff(
        paired_config(path, defer_incremental_margin_calculation=False),
        resumed_client,
        clock=Clock(),
    )
    assert resumed.outcome is Outcome.UNKNOWN
    assert resumed_client.submissions == []
    assert resumed_client.cancellations == []
    assert "RESTART_BINDING_MISMATCH" in path.read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_series_propagates_deferred_policy_to_children_and_journal(tmp_path):
    path = tmp_path / "series.jsonl"
    config = paired_series_config(
        path,
        defer_incremental_margin_calculation=True,
    )
    result = await run_series(
        config,
        MissingIncrementalSeriesClient(),
        clock=Clock(),
    )

    assert result.outcome is Outcome.SUCCESS
    assert result.children
    assert all(
        child.plan is not None
        and child.plan.defer_incremental_margin_calculation is True
        for child in result.children
    )
    journal = path.read_text(encoding="utf-8")
    assert '"defer_incremental_margin_calculation":true' in journal
    assert '"defer_incremental_margin_calculation":false' not in journal


@pytest.mark.asyncio
async def test_series_without_deferral_stays_blocked_before_first_child_mutation(tmp_path):
    client = MissingIncrementalSeriesClient()
    result = await run_series(
        paired_series_config(tmp_path / "series-strict.jsonl"),
        client,
        clock=Clock(),
    )

    assert result.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED
    assert client.submissions == []


@pytest.mark.asyncio
async def test_deferred_exchange_rejection_is_not_a_fill_or_replay(tmp_path):
    path = tmp_path / "rejected.jsonl"
    client = RejectingMissingIncrementalClient()
    result = await run_handoff(
        paired_config(path, defer_incremental_margin_calculation=True),
        client,
        clock=Clock(),
    )

    assert result.outcome is not Outcome.SUCCESS
    assert result.receiver is not None
    assert result.receiver.filled_quantity == Decimal("0")
    assert result.receiver.trades == ()
    assert client.cancellations == ["source-0"]

    resumed_client = RejectingMissingIncrementalClient()
    resumed = await run_handoff(
        paired_config(path, defer_incremental_margin_calculation=True),
        resumed_client,
        clock=Clock(),
    )
    assert resumed.outcome is Outcome.UNKNOWN
    assert resumed_client.submissions == []
    assert resumed_client.cancellations == []
    assert "RESTART_RECONCILIATION_ONLY" in path.read_text(encoding="utf-8")


class AdapterSnapshotSigner:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    async def create_auth_token_with_expiry(self, **kwargs):
        return "synthetic-adapter-token"


class AdapterSnapshotAccountApi:
    def __init__(self, api_client):
        self.api_client = api_client

    async def account(self, **kwargs):
        index = int(kwargs["value"])
        return {
            "code": 200,
            "accounts": [
                {
                    "index": index,
                    "l1_address": f"synthetic-adapter-account-{index}",
                    "status": "active",
                    "positions": [{"market_id": 1, "position": "0", "sign": 1}],
                    "available_balance": "100",
                    "cross_initial_margin_requirement": "1",
                }
            ],
        }


class AdapterSnapshotOrderApi:
    def __init__(self, api_client):
        self.api_client = api_client

    async def account_active_orders(self, **kwargs):
        assert kwargs["authorization"] == "synthetic-adapter-token"
        return {"code": 200, "orders": []}


class AdapterSnapshotApiClient:
    def __init__(self, configuration):
        self.configuration = configuration


class AdapterSnapshotLighter:
    AccountApi = AdapterSnapshotAccountApi
    OrderApi = AdapterSnapshotOrderApi
    ApiClient = AdapterSnapshotApiClient

    class Configuration:
        def __init__(self, **kwargs):
            self.kwargs = kwargs


def _adapter_snapshot_client(market_evidence):
    client = LighterSdkClient(
        paired_config("/tmp/hcr8-adapter-snapshot.jsonl"),
        source_account_index=11,
        receiver_account_index=22,
        secrets=StaticSecretProvider({11: "synthetic-key-a", 22: "synthetic-key-b"}),
        market_evidence=market_evidence,
        signer_factory=AdapterSnapshotSigner,
        clock=lambda: 1_000.0,
    )
    client._lighter = lambda: AdapterSnapshotLighter
    return client


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("account_index", "incremental_key"),
    [
        (11, "source_incremental_margin_required"),
        (22, "receiver_incremental_margin_required"),
    ],
)
@pytest.mark.parametrize("raw_value", ["-1", "NaN", "malformed"])
async def test_sdk_adapter_rejects_invalid_supplied_incremental_before_missing_provenance(
    account_index, incremental_key, raw_value
):
    client = _adapter_snapshot_client({incremental_key: raw_value})

    with pytest.raises(ContractError, match="incremental_margin_required"):
        await client.account_snapshot(account_index, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("account_index", "incremental_key"),
    [
        (11, "source_incremental_margin_required"),
        (22, "receiver_incremental_margin_required"),
    ],
)
async def test_sdk_adapter_keeps_missing_incremental_unknown_for_explicit_deferral(
    account_index, incremental_key
):
    client = _adapter_snapshot_client({incremental_key: "2"})

    snapshot = await client.account_snapshot(account_index, 1)

    assert snapshot.incremental_margin_required is None
    assert snapshot.incremental_margin_evidence == ""


def test_cli_flag_is_explicit_and_preview_remains_offline(tmp_path, capsys):
    config_path = tmp_path / "config.json"
    evidence_path = tmp_path / "evidence.json"
    cli_config = _operator_config_payload()
    cli_config["environment"] = "robinhood"
    config_path.write_text(json.dumps(cli_config), encoding="utf-8")
    evidence_path.write_text(json.dumps({"market_id": 1}), encoding="utf-8")

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
            "--defer-incremental-margin-calculation",
        ]
    ) == 0
    preview = json.loads(capsys.readouterr().out)
    assert preview["defer_incremental_margin_calculation"] is True
    assert preview["execution"] == "DISABLED"

    with pytest.raises(SystemExit, match="readiness is read-only"):
        main(
            [
                "readiness",
                "--symbol",
                "BTC",
                "--quantity",
                "0.20",
                "--direction",
                "LONG",
                "--source-account-index",
                "11",
                "--receiver-account-index",
                "22",
                "--api-key-index",
                "4",
                "--freshness-seconds",
                "10",
                "--request-timeout-seconds",
                "1",
                "--defer-incremental-margin-calculation",
            ]
        )


def test_series_cli_config_accepts_exact_boolean_only():
    raw = _operator_series_payload()
    assert _series_config(raw, execute=False).defer_incremental_margin_calculation is False
    raw["defer_incremental_margin_calculation"] = 1
    with pytest.raises(SystemExit, match="must be bool"):
        _series_config(raw, execute=False)
