import ast
import dataclasses
import json
import os
import re
import stat
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

import pytest

from risex_farmer import nado_mainnet_readiness as readiness


ROOT = Path(__file__).parents[1]
FIXTURE_ROOT = ROOT / "tests/fixtures/nado_mainnet_readiness"
PUBLIC_FIXTURE = FIXTURE_ROOT / "ready_route.json"
PRIVATE_FIXTURE = FIXTURE_ROOT / "private_read.json"
APPROVAL_FIXTURE = FIXTURE_ROOT / "dispatch_approval.json"
LIFECYCLE_FIXTURE = FIXTURE_ROOT / "lifecycle.json"
SYNTHETIC_RISEX_IDENTITY = "synthetic-risex-identity-only"
NADO_WALLET = "0x1111111111111111111111111111111111111111"
NADO_SUBACCOUNT = "0x111111111111111111111111111111111111111164656661756c740000000000"


def _raw(path: Path):
    return json.loads(path.read_text())


def _public():
    return readiness.ReadinessEvidence.from_mapping(_raw(PUBLIC_FIXTURE))


def _private():
    return readiness.PrivateReadEvidence.from_mapping(_raw(PRIVATE_FIXTURE))


def _approval():
    return readiness.DispatchApprovalEvidence.from_mapping(_raw(APPROVAL_FIXTURE))


def _lifecycle():
    return readiness.LifecycleEvidence.from_mapping(_raw(LIFECYCLE_FIXTURE))


def _configure_paths(tmp_path, monkeypatch):
    canonical = tmp_path / "canonical-risex" / "risex.identity"
    monkeypatch.setattr(readiness, "CANONICAL_RISEX_IDENTITY_PATH", canonical)
    return canonical


def _install_canonical(canonical: Path):
    canonical.parent.mkdir(mode=0o700, parents=True)
    os.chmod(canonical.parent, 0o700)
    canonical.write_text(SYNTHETIC_RISEX_IDENTITY)
    os.chmod(canonical, 0o600)


def _install_canonical_for_gate(tmp_path, monkeypatch):
    canonical = _configure_paths(tmp_path, monkeypatch)
    _install_canonical(canonical)
    return canonical


def _with_venue(evidence, venue_name, **changes):
    return dataclasses.replace(
        evidence,
        venues=tuple(
            dataclasses.replace(venue, **changes)
            if venue.venue == venue_name
            else venue
            for venue in evidence.venues
        ),
    )


def _with_private_venue(private, venue_name, **changes):
    return dataclasses.replace(
        private,
        venues=tuple(
            dataclasses.replace(venue, **changes)
            if venue.venue == venue_name
            else venue
            for venue in private.venues
        ),
    )


def _with_identity(private, venue_name, **changes):
    return dataclasses.replace(
        private,
        identities=tuple(
            dataclasses.replace(identity, **changes)
            if identity.venue == venue_name
            else identity
            for identity in private.identities
        ),
    )


def _with_deposit(private, venue_name, **changes):
    return dataclasses.replace(
        private,
        planned_deposits=tuple(
            dataclasses.replace(deposit, **changes)
            if deposit.venue == venue_name
            else deposit
            for deposit in private.planned_deposits
        ),
    )


def _with_dispatch(approval, index, **changes):
    dispatches = list(approval.operational.dispatches)
    dispatches[index] = dataclasses.replace(dispatches[index], **changes)
    return dataclasses.replace(
        approval,
        operational=dataclasses.replace(
            approval.operational,
            dispatches=tuple(dispatches),
        ),
    )


def _with_leg(lifecycle, venue_name, **changes):
    field_name = venue_name.lower()
    leg = dataclasses.replace(getattr(lifecycle.execution, field_name), **changes)
    return dataclasses.replace(
        lifecycle,
        execution=dataclasses.replace(
            lifecycle.execution,
            **{field_name: leg},
        ),
    )


def _with_funding(lifecycle, index, **changes):
    funding = list(lifecycle.funding)
    funding[index] = dataclasses.replace(funding[index], **changes)
    return dataclasses.replace(lifecycle, funding=tuple(funding))


def _with_terminal(lifecycle, index, **changes):
    rounds = list(lifecycle.terminal_rounds)
    rounds[index] = dataclasses.replace(rounds[index], **changes)
    return dataclasses.replace(lifecycle, terminal_rounds=tuple(rounds))


def _post(tmp_path, monkeypatch, *, evidence=None, private=None, approval=None, lifecycle=None):
    _install_canonical_for_gate(tmp_path, monkeypatch)
    return readiness.assess_post_lifecycle(
        _public() if evidence is None else evidence,
        _private() if private is None else private,
        _approval() if approval is None else approval,
        _lifecycle() if lifecycle is None else lifecycle,
    )


def test_offline_contract_matches_current_public_nado_baseline_and_has_no_write_authority(
    tmp_path, monkeypatch
):
    _configure_paths(tmp_path, monkeypatch)
    evidence = _public()
    nado = next(item for item in evidence.venues if item.venue == "Nado")
    risex = next(item for item in evidence.venues if item.venue == "RISEx")
    assert (nado.market, nado.product_id) == ("BTC-PERP", 2)
    assert (nado.minimum_quantity, nado.quantity_step) == (
        Decimal("0.00005"),
        Decimal("0.00005"),
    )
    assert (nado.best_bid_usd, nado.best_ask_usd) == (
        Decimal("77969"),
        Decimal("77970"),
    )
    assert (risex.minimum_quantity, risex.quantity_step) == (
        Decimal("0.00015"),
        Decimal("0.000001"),
    )
    result = readiness.assess_readiness(evidence)
    assert result.status == readiness.READY_FOR_PROTECTED_PROVISIONING
    assert result.common_quantity == Decimal("0.00130")
    assert result.gross_trade_notional_usd == Decimal("202.770815")
    assert result.mainnet_write_authority == readiness.NO_MAINNET_WRITE_AUTHORITY
    assert not result.write_ready
    assert "synthetic-" not in result.evidence()


def test_official_nado_metadata_is_offline_only():
    assert readiness.NADO_MAINNET_CHAIN_ID == 57073
    assert readiness.NADO_BTC_PERP_PRODUCT_ID == 2
    assert readiness.NADO_PUBLIC_MINIMUM_NOTIONAL_USD == Decimal("100")
    assert readiness.NADO_PUBLIC_MINIMUM_FEE_NOTIONAL_USD == Decimal("100")
    assert readiness.NADO_PUBLIC_IDENTITY_SOURCE == "PUBLIC_WALLET_SUBACCOUNT"
    assert readiness.NADO_UNSIGNED_QUERY_AUTHENTICATION == "UNSIGNED_QUERY"
    assert readiness.NADO_UNSIGNED_QUERY_STATUS == "AUTHORITATIVE_UNSIGNED_QUERY"
    assert readiness.NADO_UNSIGNED_QUERY_SOURCE == "NADO_UNSIGNED_QUERY"
    assert readiness.NADO_PRIVATE_STREAM_PENDING_STATUS == "PENDING_SIGNED_AUTHENTICATE"
    assert readiness.NADO_PRIVATE_STREAM_PENDING_SOURCE == "FUTURE_SIGNED_AUTHENTICATE"
    assert readiness.NADO_REQUIRED_QUERY_KINDS == frozenset(
        {"SUBACCOUNT_INFO", "SUBACCOUNT_ORDERS", "FEE_RATES"}
    )
    assert readiness.NADO_SDK_PACKAGE == "nado-protocol"
    assert readiness.NADO_SDK_VERSION == "2.0.0"
    assert not hasattr(readiness, "NADO_READ_CREDENTIAL_KIND")
    assert not hasattr(readiness, "provision_nado_identity")
    assert not hasattr(readiness, "PROTECTED_SECRET_DIRECTORY")


@pytest.mark.parametrize("direction", sorted(readiness.OPPOSITE_DIRECTIONS))
def test_both_opposite_directions_are_valid_public_route_selections(
    tmp_path, monkeypatch, direction
):
    _configure_paths(tmp_path, monkeypatch)
    evidence = dataclasses.replace(
        _public(), route=dataclasses.replace(_public().route, direction=direction)
    )
    result = readiness.assess_readiness(evidence)
    assert result.status == readiness.READY_FOR_PROTECTED_PROVISIONING
    assert result.direction == direction


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"direction": "LONG_RISEX_SHORT_RISEX"}, "ONE_ROUTE_DIRECTION_REQUIRED"),
        ({"self_trade_free": False}, "SELF_TRADE_GUARD_NOT_PROVEN"),
        (
            {"counterparty_account_id": "synthetic-counterparty-account"},
            "PUBLIC_ROUTE_MUST_NOT_CLAIM_ACCOUNT_IDENTITY",
        ),
        ({"canonical_asset": "ETH"}, "CANONICAL_ASSET_MISMATCH:RISEx"),
    ],
)
def test_route_and_self_trade_barriers_fail_closed(tmp_path, monkeypatch, change, reason):
    _configure_paths(tmp_path, monkeypatch)
    evidence = _public()
    if "canonical_asset" in change:
        route = dataclasses.replace(evidence.route, canonical_asset=change["canonical_asset"])
        evidence = dataclasses.replace(evidence, route=route)
    else:
        evidence = dataclasses.replace(evidence, route=dataclasses.replace(evidence.route, **change))
    result = readiness.assess_readiness(evidence)
    assert result.status == readiness.BLOCKED
    assert reason in result.blockers


@pytest.mark.parametrize(
    ("venue", "change", "reason"),
    [
        ("RISEx", {"minimum_quantity": Decimal("0.000151")}, "MINIMUM_QUANTITY_NOT_EXACT:RISEx"),
        ("RISEx", {"quantity_step": Decimal("0.00001")}, "QUANTITY_STEP_NOT_EXACT:RISEx"),
        ("Nado", {"minimum_quantity": Decimal("0.0001")}, "MINIMUM_QUANTITY_NOT_EXACT:Nado"),
        ("Nado", {"quantity_step": Decimal("0.00001")}, "QUANTITY_STEP_NOT_EXACT:Nado"),
        ("Nado", {"minimum_notional_usd": Decimal("99")}, "MINIMUM_NOTIONAL_NOT_EXACT:Nado"),
        ("Nado", {"minimum_fee_notional_usd": Decimal("99")}, "MINIMUM_FEE_NOTIONAL_NOT_EXACT:Nado"),
        ("Nado", {"available_sell_quantity": Decimal("0.00129")}, "COMMON_QUANTITY_NOT_EXECUTABLE:Nado"),
        ("Nado", {"best_bid_usd": Decimal("77970")}, "BBO_OR_REFERENCE_NOT_SAFE:Nado"),
        ("Nado", {"best_ask_usd": Decimal("77970.05")}, "BBO_OR_REFERENCE_NOT_SAFE:Nado"),
        ("Nado", {"fee_status": "PUBLIC_ASSUMPTION"}, "ACCOUNT_FEE_MUST_REMAIN_PENDING:Nado"),
        ("Nado", {"private_stream_status": "READY_ACCOUNT_SCOPED"}, "PRIVATE_STREAM_MUST_REMAIN_PENDING:Nado"),
        ("Nado", {"next_funding_at": 1788087601}, "PUBLIC_FUNDING_SCHEDULE_NOT_COMMON"),
        ("Nado", {"market": "BTC-USD"}, "MARKET_CONTRACT_MISMATCH:Nado"),
    ],
)
def test_exact_minima_step_depth_bbo_fees_schedule_and_contract_are_required(
    tmp_path, monkeypatch, venue, change, reason
):
    _configure_paths(tmp_path, monkeypatch)
    result = readiness.assess_readiness(_with_venue(_public(), venue, **change))
    assert result.status == readiness.BLOCKED
    assert reason in result.blockers


def test_public_parser_tolerates_irrelevant_additive_fields_but_rejects_later_phase_claims():
    raw = _raw(PUBLIC_FIXTURE)
    raw["irrelevant_public_observation"] = {"vendor_note": "ignored"}
    assert readiness.ReadinessEvidence.from_mapping(raw).route.route_id == "RISEX-NADO-BTC-001"
    for forbidden in (
        "identities",
        "private_read",
        "approval",
        "operational",
        "execution",
        "funding",
        "terminal_rounds",
    ):
        later = _raw(PUBLIC_FIXTURE)
        later[forbidden] = {}
        with pytest.raises(readiness.ReadinessViolation, match=f"PUBLIC_EVIDENCE_MUST_NOT_CLAIM:{forbidden}"):
            readiness.ReadinessEvidence.from_mapping(later)


def test_protected_phase_reuses_only_canonical_risex_path_without_nado_secret_surface(
    tmp_path, monkeypatch
):
    canonical = _configure_paths(tmp_path, monkeypatch)
    missing = readiness.assess_readiness(_public())
    assert missing.status == readiness.READY_FOR_PROTECTED_PROVISIONING
    assert not (tmp_path / "nado-protected").exists()

    _install_canonical(canonical)
    before_content = canonical.read_bytes()
    before_inode = canonical.stat().st_ino
    result = readiness.assess_readiness(_public())
    assert result.status == readiness.READY_FOR_PRIVATE_READ_GATES
    inspected = readiness.inspect_canonical_risex_identity()
    assert inspected.path == str(canonical)
    assert inspected.protected
    assert canonical.read_bytes() == before_content
    assert canonical.stat().st_ino == before_inode
    assert "synthetic-" not in result.evidence()

    source = (ROOT / "src/risex_farmer/nado_mainnet_readiness.py").read_text()
    for forbidden in (
        "API_KEY_SECRET_READ_ONLY",
        "PROTECTED_SECRET_DIRECTORY",
        "nado.identity",
        "provision_nado_identity",
        "provision_protected_identities",
    ):
        assert forbidden not in source


def test_protected_inspection_rejects_0700_0600_ownership_symlink_and_hardlink(
    tmp_path, monkeypatch
):
    canonical = _install_canonical_for_gate(tmp_path, monkeypatch)
    assert readiness.inspect_canonical_risex_identity().protected

    os.chmod(canonical.parent, 0o755)
    assert (
        readiness.inspect_canonical_risex_identity().reason
        == "PROTECTED_DIRECTORY_MODE_NOT_0700"
    )
    os.chmod(canonical.parent, 0o700)
    os.chmod(canonical, 0o644)
    assert (
        readiness.inspect_canonical_risex_identity().reason
        == "PROTECTED_FILE_MODE_NOT_0600"
    )
    os.chmod(canonical, 0o600)

    replacement = tmp_path / "replacement"
    replacement.write_text("synthetic-replacement")
    os.chmod(replacement, 0o600)
    canonical.unlink()
    canonical.symlink_to(replacement)
    assert (
        readiness.inspect_canonical_risex_identity().reason
        == "PROTECTED_FILE_SYMLINK"
    )
    canonical.unlink()

    hardlink_source = tmp_path / "hardlink-source"
    hardlink_source.write_text("synthetic-hardlink")
    os.chmod(hardlink_source, 0o600)
    os.link(hardlink_source, canonical)
    assert (
        readiness.inspect_canonical_risex_identity().reason
        == "PROTECTED_FILE_HARDLINK"
    )

    current_uid = os.getuid()
    monkeypatch.setattr(readiness.os, "getuid", lambda: current_uid + 100000)
    foreign = readiness.inspect_canonical_risex_identity()
    assert foreign.reason == "PROTECTED_DIRECTORY_OWNER_NOT_CURRENT_USER"


def test_canonical_risex_file_unsafe_state_blocks_readiness_without_nado_secret_surface(
    tmp_path, monkeypatch
):
    canonical = _configure_paths(tmp_path, monkeypatch)
    _install_canonical(canonical)
    canonical.chmod(0o644)
    result = readiness.assess_readiness(_public())
    assert result.status == readiness.BLOCKED
    assert "PROTECTED_SECRET_FILE_NOT_SAFE:RISEx:PROTECTED_FILE_MODE_NOT_0600" in result.blockers
    assert not (tmp_path / "nado-protected").exists()


def test_safe_files_open_private_read_gate_with_unsigned_nado_queries(
    tmp_path, monkeypatch
):
    _install_canonical_for_gate(tmp_path, monkeypatch)
    public = _public()
    readiness_result = readiness.assess_readiness(public)
    assert readiness_result.status == readiness.READY_FOR_PRIVATE_READ_GATES
    private_result = readiness.assess_private_read(public, _private())
    assert private_result.status == readiness.PRIVATE_READ_GATES_COMPLETE
    assert private_result.planned_deposit_total_usd == Decimal("90")
    assert private_result.mainnet_write_authority == readiness.NO_MAINNET_WRITE_AUTHORITY
    assert not private_result.write_ready
    assert "synthetic-risex-account-001" not in private_result.evidence()
    assert NADO_SUBACCOUNT not in private_result.evidence()
    nado_identity = next(item for item in _private().identities if item.venue == "Nado")
    nado_read = next(item for item in _private().venues if item.venue == "Nado")
    assert nado_identity.wallet_address == NADO_WALLET
    assert nado_identity.subaccount == NADO_SUBACCOUNT
    assert nado_identity.identity_source == readiness.NADO_PUBLIC_IDENTITY_SOURCE
    assert nado_read.query_authentication == readiness.NADO_UNSIGNED_QUERY_AUTHENTICATION
    assert nado_read.query_status == readiness.NADO_UNSIGNED_QUERY_STATUS
    assert nado_read.query_source == readiness.NADO_UNSIGNED_QUERY_SOURCE
    assert set(nado_read.query_kinds) == readiness.NADO_REQUIRED_QUERY_KINDS
    assert nado_read.private_stream_status == readiness.NADO_PRIVATE_STREAM_PENDING_STATUS
    assert nado_read.private_stream_source == readiness.NADO_PRIVATE_STREAM_PENDING_SOURCE


@pytest.mark.parametrize(
    ("change_kind", "reason"),
    [
        ("nado_query_authentication", "NADO_QUERY_MUST_BE_UNSIGNED"),
        ("nado_query_status", "NADO_UNSIGNED_QUERY_NOT_AUTHORITATIVE"),
        ("nado_query_source", "NADO_UNSIGNED_QUERY_NOT_AUTHORITATIVE"),
        ("nado_query_kinds", "NADO_UNSIGNED_QUERY_SET_INCOMPLETE"),
        ("nado_stream", "NADO_PRIVATE_STREAM_MUST_REMAIN_PENDING"),
        ("nado_stream_pending", "NADO_PRIVATE_STREAM_MUST_REMAIN_PENDING"),
        ("nado_identity_missing", "NADO_PUBLIC_WALLET_SUBACCOUNT_NOT_EXACT"),
        ("nado_identity", "NADO_SUBACCOUNT_WALLET_MISMATCH"),
        ("nado_account_binding", "NADO_ACCOUNT_ID_NOT_SUBACCOUNT"),
        ("nado_collateral", "POSITIVE_COLLATERAL_NOT_PROVEN:Nado"),
        ("nado_zero_orders", "PRIVATE_STATE_NOT_CLEAR:Nado:zero_relevant_orders"),
        ("nado_trigger_orders", "PRIVATE_STATE_NOT_CLEAR:Nado:zero_trigger_orders"),
        ("nado_flat", "PRIVATE_STATE_NOT_CLEAR:Nado:exact_flat"),
        ("nado_unrelated", "PRIVATE_STATE_NOT_CLEAR:Nado:unrelated_state_clear"),
        ("nado_fee", "ACCOUNT_FEE_NOT_AUTHORITATIVE:Nado"),
        ("duplicate_account", "PRIVATE_ACCOUNT_IDENTITIES_MUST_BE_DISTINCT"),
        ("wrong_environment", "PRIVATE_ACCOUNT_ENVIRONMENT_NOT_MAINNET:Nado"),
        ("zero_deposit", "PLANNED_DEPOSIT_MUST_BE_POSITIVE:Nado"),
        ("deposit_account", "PLANNED_DEPOSIT_ACCOUNT_MISMATCH:Nado"),
    ],
)
def test_private_read_gates_fail_closed_on_unsigned_query_identity_fee_stream_state_and_deposit_risks(
    tmp_path, monkeypatch, change_kind, reason
):
    _install_canonical_for_gate(tmp_path, monkeypatch)
    private = _private()
    if change_kind == "nado_query_authentication":
        private = _with_private_venue(private, "Nado", query_authentication="SIGNED_QUERY")
    elif change_kind == "nado_query_status":
        private = _with_private_venue(private, "Nado", query_status="CURRENT")
    elif change_kind == "nado_query_source":
        private = _with_private_venue(private, "Nado", query_source="PUBLIC_READ")
    elif change_kind == "nado_query_kinds":
        private = _with_private_venue(
            private,
            "Nado",
            query_kinds=("SUBACCOUNT_INFO", "SUBACCOUNT_ORDERS"),
        )
    elif change_kind == "nado_stream":
        private = _with_private_venue(private, "Nado", private_stream_source="ACCOUNT_PRIVATE_STREAM")
    elif change_kind == "nado_stream_pending":
        private = _with_private_venue(private, "Nado", private_stream_status="READY_ACCOUNT_SCOPED")
    elif change_kind == "nado_identity_missing":
        private = _with_identity(
            private,
            "Nado",
            wallet_address=None,
            subaccount=None,
            identity_source=None,
        )
    elif change_kind == "nado_identity":
        private = _with_identity(private, "Nado", wallet_address="0x" + ("2" * 40))
    elif change_kind == "nado_account_binding":
        private = _with_identity(private, "Nado", account_id="0x" + ("1" * 64))
    elif change_kind == "nado_collateral":
        private = _with_private_venue(private, "Nado", collateral_usd=Decimal("0"))
    elif change_kind == "nado_zero_orders":
        private = _with_private_venue(private, "Nado", zero_relevant_orders=False)
    elif change_kind == "nado_trigger_orders":
        private = _with_private_venue(private, "Nado", zero_trigger_orders=False)
    elif change_kind == "nado_flat":
        private = _with_private_venue(private, "Nado", exact_flat=False)
    elif change_kind == "nado_unrelated":
        private = _with_private_venue(private, "Nado", unrelated_state_clear=False)
    elif change_kind == "nado_fee":
        private = _with_private_venue(private, "Nado", fee_status="PUBLIC_ASSUMPTION")
    elif change_kind == "duplicate_account":
        private = _with_identity(private, "Nado", account_id="synthetic-risex-account-001")
    elif change_kind == "wrong_environment":
        private = _with_identity(private, "Nado", environment="TESTNET")
    elif change_kind == "zero_deposit":
        private = _with_deposit(private, "Nado", amount_usd=Decimal("0"))
    elif change_kind == "deposit_account":
        private = _with_deposit(private, "Nado", account_id="synthetic-other-account-001")
    result = readiness.assess_private_read(_public(), private)
    assert result.status == readiness.BLOCKED
    assert reason in result.blockers


def test_private_read_requires_both_exact_accounts_and_positive_deposits(tmp_path, monkeypatch):
    _install_canonical_for_gate(tmp_path, monkeypatch)
    private = dataclasses.replace(_private(), identities=_private().identities[:1])
    result = readiness.assess_private_read(_public(), private)
    assert result.status == readiness.BLOCKED
    assert "PRIVATE_IDENTITIES_MUST_INCLUDE_ONE_EXACT_ACCOUNT_PER_VENUE" in result.blockers


def test_future_approval_requires_private_read_and_never_becomes_write_ready(
    tmp_path, monkeypatch
):
    _configure_paths(tmp_path, monkeypatch)
    blocked = readiness.assess_dispatch_approval(_public(), _private(), _approval())
    assert blocked.status == readiness.BLOCKED
    assert "PRIVATE_READ_REQUIRES_READY_PRIVATE_READ_GATES" in blocked.blockers
    assert not blocked.write_ready

    _install_canonical_for_gate(tmp_path, monkeypatch)
    complete = readiness.assess_dispatch_approval(_public(), _private(), _approval())
    assert complete.status == readiness.FUTURE_DISPATCH_APPROVAL_COMPLETE
    assert complete.complete
    assert not complete.write_ready
    assert complete.mainnet_write_authority == readiness.NO_MAINNET_WRITE_AUTHORITY
    assert "synthetic-runtime-001" not in complete.evidence()


@pytest.mark.parametrize(
    ("attribute", "value", "reason"),
    [
        ("deposit_cap_usd", Decimal("89"), "PLANNED_DEPOSITS_EXCEED_DEPOSIT_CAP"),
        ("deposit_cap_usd", Decimal("0"), "DEPOSIT_CAP_MUST_BE_POSITIVE"),
        ("maximum_loss_usd", Decimal("0.49"), "ROUTE_LOSS_EXCEEDS_MAXIMUM_LOSS_CAP"),
        ("maximum_loss_usd", Decimal("0"), "MAXIMUM_LOSS_CAP_MUST_BE_POSITIVE"),
        ("maximum_loss_usd", Decimal("251"), "MAXIMUM_LOSS_EXCEEDS_DEPOSIT_CAP"),
        ("approval_mode", "RELATIVE_CAP", "EXPLICIT_MANUAL_APPROVAL_REQUIRED"),
        ("scope", "UNBOUNDED", "DISPATCH_APPROVAL_SCOPE_NOT_EXACT"),
        ("manual_lifecycle_dispatch_authorized", False, "MANUAL_LIFECYCLE_DISPATCH_NOT_AUTHORIZED"),
        ("authorization_count", 2, "EXACTLY_ONE_AUTHORIZATION_REQUIRED"),
        ("nado_account_id", "synthetic-other-account-001", "DISPATCH_APPROVAL_ACCOUNT_OR_VENUE_BINDING_MISMATCH"),
    ],
)
def test_explicit_positive_deposit_and_max_loss_caps_bind_accounts_and_scope(
    tmp_path, monkeypatch, attribute, value, reason
):
    _install_canonical_for_gate(tmp_path, monkeypatch)
    base = _approval()
    approval = dataclasses.replace(base.approval, **{attribute: value})
    result = readiness.assess_dispatch_approval(
        _public(), _private(), dataclasses.replace(base, approval=approval)
    )
    assert result.status == readiness.BLOCKED
    assert reason in result.blockers


@pytest.mark.parametrize(
    ("attribute", "value", "reason"),
    [
        ("runtime_id", "", "RUNTIME_IDENTITY_INVALID"),
        ("runtime_fresh", False, "RUNTIME_IDENTITY_NOT_FRESH"),
        ("runtime_durable_before_dispatch", False, "RUNTIME_IDENTITY_NOT_DURABLE_BEFORE_DISPATCH"),
        ("captured_immediately_before_dispatch", False, "RUNTIME_WRITE_EVIDENCE_NOT_IMMEDIATELY_BEFORE_DISPATCH"),
        ("sequential_writes", False, "SEQUENTIAL_WRITE_CONTRACT_NOT_PROVEN"),
        ("no_blind_replay", False, "AMBIGUOUS_WRITE_REPLAY_NOT_BLOCKED"),
        ("restart_requires_reconciliation", False, "RESTART_RECONCILIATION_NOT_REQUIRED"),
    ],
)
def test_fresh_runtime_durable_identity_and_restart_barriers_fail_closed(
    tmp_path, monkeypatch, attribute, value, reason
):
    _install_canonical_for_gate(tmp_path, monkeypatch)
    base = _approval()
    operational = dataclasses.replace(base.operational, **{attribute: value})
    result = readiness.assess_dispatch_approval(
        _public(), _private(), dataclasses.replace(base, operational=operational)
    )
    assert result.status == readiness.BLOCKED
    assert reason in result.blockers


def test_dispatch_identity_ordering_and_distinctness_are_required(tmp_path, monkeypatch):
    _install_canonical_for_gate(tmp_path, monkeypatch)
    base = _approval()
    swapped = list(base.operational.dispatches)
    swapped[0], swapped[1] = swapped[1], swapped[0]
    result = readiness.assess_dispatch_approval(
        _public(), _private(), dataclasses.replace(
            base,
            operational=dataclasses.replace(base.operational, dispatches=tuple(swapped)),
        )
    )
    assert result.status == readiness.BLOCKED
    assert "DISPATCH_SEQUENCE_NOT_EXACT" in result.blockers

    duplicate = _with_dispatch(
        base,
        3,
        write_identity=base.operational.dispatches[0].write_identity,
        durable_before_dispatch=False,
    )
    result = readiness.assess_dispatch_approval(_public(), _private(), duplicate)
    assert result.status == readiness.BLOCKED
    assert "DISPATCH_WRITE_IDENTITIES_NOT_DISTINCT" in result.blockers
    assert "DISPATCH_WRITE_IDENTITY_NOT_DURABLE:Nado:CLOSE" in result.blockers


def test_lifecycle_requires_prior_gates_and_accepts_both_directions_with_zero_or_negative_cash(
    tmp_path, monkeypatch
):
    _configure_paths(tmp_path, monkeypatch)
    missing = readiness.assess_post_lifecycle(_public(), _private(), _approval(), _lifecycle())
    assert missing.status == readiness.BLOCKED
    assert "FUTURE_DISPATCH_APPROVAL_REQUIRED" in missing.blockers

    _install_canonical_for_gate(tmp_path, monkeypatch)
    complete = readiness.assess_post_lifecycle(_public(), _private(), _approval(), _lifecycle())
    assert complete.status == readiness.POST_LIFECYCLE_EVIDENCE_COMPLETE
    assert complete.complete
    assert not complete.write_ready
    assert complete.mainnet_write_authority == readiness.NO_MAINNET_WRITE_AUTHORITY

    public = _public()
    public = dataclasses.replace(
        public,
        route=dataclasses.replace(public.route, direction="SHORT_RISEX_LONG_NADO"),
    )
    approval = _approval()
    approval = dataclasses.replace(
        approval,
        approval=dataclasses.replace(approval.approval, direction="SHORT_RISEX_LONG_NADO"),
    )
    lifecycle = _lifecycle()
    lifecycle = _with_leg(lifecycle, "RISEx", entry_side="SELL")
    lifecycle = _with_leg(lifecycle, "Nado", entry_side="BUY")
    reverse = readiness.assess_post_lifecycle(public, _private(), approval, lifecycle)
    assert reverse.status == readiness.POST_LIFECYCLE_EVIDENCE_COMPLETE
    assert any(item.cash_usd == Decimal("-0.02") for item in lifecycle.funding)


@pytest.mark.parametrize(
    ("venue", "attribute", "value", "reason"),
    [
        ("RISEx", "entry_side", "SELL", "EXECUTION_DIRECTION_NOT_OPPOSITE"),
        ("Nado", "canonical_quantity", Decimal("0.00125"), "EXECUTION_QUANTITY_NOT_EXACT_COMMON_QUANTITY:Nado"),
        ("Nado", "reduce_only", False, "EXECUTION_EVIDENCE_NOT_AUTHORITATIVE:Nado:reduce_only"),
        ("RISEx", "close_observed_at", 1788087605, "EXECUTION_ENTRY_CLOSE_ORDER_INVALID:RISEx"),
        ("RISEx", "order_reconciled", False, "EXECUTION_EVIDENCE_NOT_AUTHORITATIVE:RISEx:order_reconciled"),
        ("Nado", "account_id", "synthetic-other-account", "EXECUTION_ACCOUNT_BINDING_MISMATCH:Nado"),
    ],
)
def test_execution_requires_opposite_exact_account_quantity_reconciliation_and_reduce_only_close(
    tmp_path, monkeypatch, venue, attribute, value, reason
):
    lifecycle = _with_leg(_lifecycle(), venue, **{attribute: value})
    result = _post(tmp_path, monkeypatch, lifecycle=lifecycle)
    assert result.status == readiness.BLOCKED
    assert reason in result.blockers


@pytest.mark.parametrize(
    ("index", "change", "reason"),
    [
        (4, {"cash_usd": None, "missing": True}, "FUNDING_MISSING_OR_CONTRADICTORY:Nado:AT"),
        (5, {"contradictory": True}, "FUNDING_MISSING_OR_CONTRADICTORY:Nado:AFTER"),
        (4, {"cash_source": "PUBLIC_AGGREGATE"}, "FUNDING_CASH_SOURCE_NOT_ACCOUNT_SCOPED:Nado:AT"),
        (4, {"settlement_at": 1788087601}, "FUNDING_SETTLEMENT_NOT_PUBLIC_SCHEDULE:Nado"),
        (4, {"status": "UNKNOWN"}, "FUNDING_STATUS_INVALID:Nado:AT"),
        (4, {"observed_at": 1788087590}, "FUNDING_PHASE_BOUNDARY_INVALID:Nado"),
    ],
)
def test_funding_requires_account_scoped_before_at_after_and_no_public_aggregate_cash(
    tmp_path, monkeypatch, index, change, reason
):
    lifecycle = _with_funding(_lifecycle(), index, **change)
    result = _post(tmp_path, monkeypatch, lifecycle=lifecycle)
    assert result.status == readiness.BLOCKED
    assert reason in result.blockers


def test_funding_requires_exact_six_observations_and_common_settlement_identity(
    tmp_path, monkeypatch
):
    short = dataclasses.replace(_lifecycle(), funding=_lifecycle().funding[:5])
    result = _post(tmp_path, monkeypatch, lifecycle=short)
    assert result.status == readiness.BLOCKED
    assert "EXACTLY_SIX_FUNDING_OBSERVATIONS_REQUIRED" in result.blockers
    assert "FUNDING_BEFORE_AT_AFTER_REQUIRED_PER_VENUE" in result.blockers

    different_id = _with_funding(_lifecycle(), 3, settlement_id="synthetic-other-settlement")
    result = _post(tmp_path / "different-settlement", monkeypatch, lifecycle=different_id)
    assert result.status == readiness.BLOCKED
    assert "FUNDING_SETTLEMENT_NOT_COMMON:Nado" in result.blockers


@pytest.mark.parametrize(
    ("index", "change", "reason"),
    [
        (1, {"signature": "synthetic-different-terminal-state"}, "TERMINAL_ROUNDS_DO_NOT_AGREE"),
        (1, {"relevant_open_orders": 1}, "TERMINAL_ROUND_HAS_ORDERS_OR_UNRELATED_STATE"),
        (1, {"trigger_orders": 1}, "TERMINAL_ROUND_HAS_ORDERS_OR_UNRELATED_STATE"),
        (1, {"unrelated_positions": 1}, "TERMINAL_ROUND_HAS_ORDERS_OR_UNRELATED_STATE"),
        (1, {"nado_net_position_quantity": Decimal("0.00001")}, "TERMINAL_ROUND_NOT_EXACTLY_FLAT"),
        (1, {"authoritative": False}, "TERMINAL_ROUND_NOT_AUTHORITATIVE"),
        (1, {"phase": "TERMINAL"}, "TERMINAL_ROUND_PHASE_NOT_EXACT"),
    ],
)
def test_terminal_rounds_require_two_agreeing_authoritative_zero_order_exact_flat_snapshots(
    tmp_path, monkeypatch, index, change, reason
):
    lifecycle = _with_terminal(_lifecycle(), index, **change)
    result = _post(tmp_path, monkeypatch, lifecycle=lifecycle)
    assert result.status == readiness.BLOCKED
    assert reason in result.blockers


def test_terminal_round_count_sequence_and_time_order_are_required(tmp_path, monkeypatch):
    one = dataclasses.replace(_lifecycle(), terminal_rounds=(_lifecycle().terminal_rounds[0],))
    result = _post(tmp_path, monkeypatch, lifecycle=one)
    assert result.status == readiness.BLOCKED
    assert "EXACTLY_TWO_TERMINAL_ROUNDS_REQUIRED" in result.blockers

    wrong_sequence = _with_terminal(_lifecycle(), 1, round_number=3)
    result = _post(tmp_path / "round-sequence", monkeypatch, lifecycle=wrong_sequence)
    assert result.status == readiness.BLOCKED
    assert "TERMINAL_ROUND_SEQUENCE_NOT_EXACT" in result.blockers

    same_time = _with_terminal(_lifecycle(), 1, observed_at=1788087900)
    result = _post(tmp_path / "round-time", monkeypatch, lifecycle=same_time)
    assert result.status == readiness.BLOCKED
    assert "TERMINAL_ROUNDS_NOT_INCREASING" in result.blockers


def test_all_result_surfaces_are_redacted_and_never_write_ready(tmp_path, monkeypatch):
    _install_canonical_for_gate(tmp_path, monkeypatch)
    public = _public()
    results = [
        readiness.assess_readiness(public),
        readiness.assess_private_read(public, _private()),
        readiness.assess_dispatch_approval(public, _private(), _approval()),
        readiness.assess_post_lifecycle(public, _private(), _approval(), _lifecycle()),
    ]
    for result in results:
        assert result.mainnet_write_authority == readiness.NO_MAINNET_WRITE_AUTHORITY
        assert not result.write_ready
        assert SYNTHETIC_RISEX_IDENTITY not in repr(result)
        assert NADO_SUBACCOUNT not in repr(result)
        assert "synthetic-risex-account-001" not in result.evidence()
        assert NADO_SUBACCOUNT not in result.evidence()


def test_readiness_module_has_no_transport_database_sdk_or_write_surface():
    source_path = ROOT / "src/risex_farmer/nado_mainnet_readiness.py"
    source = source_path.read_text()
    for forbidden in (
        "http://",
        "https://",
        "ws://",
        "wss://",
        "aiohttp",
        "requests",
        "sqlite3",
        "socket",
        "urlopen",
        "eth_account",
        "starknet",
        "nado_private_read",
        "API_KEY_SECRET_READ_ONLY",
        "PROTECTED_SECRET_DIRECTORY",
        "nado.identity",
        "provision_nado_identity",
        "provision_protected_identities",
        "NADO_ACCOUNT_PRIVATE_STREAM",
    ):
        assert forbidden not in source
    assert not re.search(r"\b(?:POST|PUT|DELETE|PATCH)\b", source)
    tree = ast.parse(source)
    imported_roots = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.extend(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.append(node.module.split(".")[0])
    assert set(imported_roots) <= {
        "dataclasses",
        "decimal",
        "json",
        "math",
        "os",
        "pathlib",
        "stat",
        "typing",
        "__future__",
    }
    assert not hasattr(readiness, "main")


def test_normal_cli_import_does_not_import_nado_readiness_module():
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT / "src")
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import risex_farmer.cli; print('risex_farmer.nado_mainnet_readiness' in sys.modules)",
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=True,
    )
    assert completed.stdout.strip() == "False"
