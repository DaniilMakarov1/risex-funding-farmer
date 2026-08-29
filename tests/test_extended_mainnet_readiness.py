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
from types import SimpleNamespace

import pytest

from risex_farmer import extended_mainnet_readiness as readiness


ROOT = Path(__file__).parents[1]
FIXTURE_ROOT = ROOT / "tests/fixtures/extended_mainnet_readiness"
PUBLIC_FIXTURE = FIXTURE_ROOT / "ready_route.json"
PRIVATE_FIXTURE = FIXTURE_ROOT / "private_read.json"
DISPATCH_FIXTURE = FIXTURE_ROOT / "dispatch_approval.json"
LIFECYCLE_FIXTURE = FIXTURE_ROOT / "lifecycle.json"
SYNTHETIC_RISEX_SECRET = "synthetic-risex-opaque-value"
SYNTHETIC_EXTENDED_SECRET = "synthetic-extended-opaque-value"


def _raw(path: Path):
    return json.loads(path.read_text())


def _public():
    return readiness.ReadinessEvidence.from_mapping(_raw(PUBLIC_FIXTURE))


def _private():
    return readiness.PrivateReadEvidence.from_mapping(_raw(PRIVATE_FIXTURE))


def _approval():
    return readiness.DispatchApprovalEvidence.from_mapping(
        _raw(DISPATCH_FIXTURE)
    )


def _lifecycle():
    return readiness.LifecycleEvidence.from_mapping(_raw(LIFECYCLE_FIXTURE))


def _provision(tmp_path, monkeypatch):
    directory = tmp_path / "fixed-provisioning-directory"
    monkeypatch.setattr(readiness, "PROTECTED_SECRET_DIRECTORY", directory)
    values = iter((SYNTHETIC_RISEX_SECRET, SYNTHETIC_EXTENDED_SECRET))
    result = readiness.provision_protected_identities(lambda _prompt: next(values))
    assert result.status == readiness.PROVISIONED
    assert result.files.all_protected
    return result


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
    leg = dataclasses.replace(
        getattr(lifecycle.execution, field_name),
        **changes,
    )
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


def _post(
    tmp_path,
    monkeypatch,
    *,
    evidence=None,
    private=None,
    approval=None,
    lifecycle=None,
):
    _provision(tmp_path, monkeypatch)
    return readiness.assess_post_lifecycle(
        _public() if evidence is None else evidence,
        _private() if private is None else private,
        _approval() if approval is None else approval,
        _lifecycle() if lifecycle is None else lifecycle,
    )


def test_phase_a_uses_only_public_offline_facts_and_keeps_later_facts_pending(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        readiness,
        "PROTECTED_SECRET_DIRECTORY",
        tmp_path / "fixed-provisioning-directory",
    )
    evidence = _public()
    raw = _raw(PUBLIC_FIXTURE)

    assert set(raw) == {"route", "venues"}
    assert not hasattr(evidence, "caps")
    assert not hasattr(evidence, "identities")
    assert not hasattr(evidence, "operational")
    assert not hasattr(evidence, "lifecycle")
    assert evidence.route.counterparty_account_id is None
    assert evidence.venues[0].fee_status == "PENDING_ACCOUNT_SCOPED"
    assert evidence.venues[0].maker_fee_rate is None
    assert evidence.venues[0].taker_fee_rate is None
    assert all(
        venue.private_stream_status == "PENDING_PRIVATE_READ"
        for venue in evidence.venues
    )
    assert evidence.venues[1].fee_status == "PUBLIC_CURRENT"

    result = readiness.assess_readiness(evidence)
    assert result.status == readiness.READY_FOR_PROTECTED_PROVISIONING
    assert result.reason == (
        "PUBLIC_OFFLINE_REQUIREMENTS_PROVEN_PROTECTED_PROVISIONING_PENDING"
    )
    assert result.ready
    assert not result.write_ready
    assert result.common_quantity == Decimal("0.00015")
    assert result.gross_trade_notional_usd == Decimal("23.465415")
    assert result.loss_bound_usd == Decimal("0.50")
    assert result.mainnet_write_authority == readiness.NO_MAINNET_WRITE_AUTHORITY
    assert "synthetic-" not in result.evidence()
    assert "identities" not in result.evidence()

    private_result = readiness.assess_private_read(evidence, _private())
    assert private_result.status == readiness.BLOCKED
    assert "PRIVATE_READ_REQUIRES_READY_PRIVATE_READ_GATES" in private_result.blockers
    approval_result = readiness.assess_dispatch_approval(
        evidence, _private(), _approval()
    )
    assert approval_result.status == readiness.BLOCKED
    assert "PRIVATE_READ_REQUIREMENTS_REQUIRED_FOR_APPROVAL" in approval_result.blockers


def test_phase_b_safe_files_only_open_private_read_gate_and_claim_no_private_facts(
    tmp_path, monkeypatch
):
    evidence = _public()
    _provision(tmp_path, monkeypatch)

    result = readiness.assess_readiness(evidence)
    assert result.status == readiness.READY_FOR_PRIVATE_READ_GATES
    assert result.reason == "PROTECTED_IDENTITIES_PRESENT_PRIVATE_READ_GATES_PENDING"
    assert result.ready
    assert not result.write_ready
    assert result.mainnet_write_authority == readiness.NO_MAINNET_WRITE_AUTHORITY
    assert "synthetic-risex-account-001" not in result.evidence()
    assert "synthetic-runtime-001" not in result.evidence()

    private_result = readiness.assess_private_read(evidence, _private())
    assert private_result.status == readiness.PRIVATE_READ_GATES_COMPLETE
    assert private_result.complete
    assert not private_result.write_ready
    assert private_result.planned_deposit_total_usd == Decimal("90.00")
    assert "synthetic-risex-account-001" not in private_result.evidence()


@pytest.mark.parametrize(
    "forbidden_key",
    [
        "caps",
        "identities",
        "private_read",
        "operational",
        "dispatch_approval",
        "lifecycle",
    ],
)
def test_public_parser_rejects_claims_from_later_phases(forbidden_key):
    raw = _raw(PUBLIC_FIXTURE)
    raw[forbidden_key] = {}
    with pytest.raises(
        readiness.ReadinessViolation,
        match=rf"PUBLIC_EVIDENCE_MUST_NOT_CLAIM:{forbidden_key}",
    ):
        readiness.ReadinessEvidence.from_mapping(raw)


def test_private_approval_and_lifecycle_fixtures_have_non_conflated_roots():
    assert set(_raw(PRIVATE_FIXTURE)) == {
        "identities",
        "venues",
        "planned_deposits",
    }
    assert set(_raw(DISPATCH_FIXTURE)) == {"approval", "operational"}
    assert set(_raw(LIFECYCLE_FIXTURE)) == {
        "execution",
        "funding",
        "terminal_rounds",
    }

    private_raw = _raw(PRIVATE_FIXTURE)
    private_raw["operational"] = {}
    with pytest.raises(
        readiness.ReadinessViolation,
        match="PRIVATE_READ_MUST_NOT_CLAIM:operational",
    ):
        readiness.PrivateReadEvidence.from_mapping(private_raw)

    approval_raw = _raw(DISPATCH_FIXTURE)
    approval_raw["lifecycle"] = {}
    with pytest.raises(
        readiness.ReadinessViolation,
        match="DISPATCH_APPROVAL_MUST_NOT_CLAIM:lifecycle",
    ):
        readiness.DispatchApprovalEvidence.from_mapping(approval_raw)

    lifecycle_raw = _raw(LIFECYCLE_FIXTURE)
    lifecycle_raw["approval"] = {}
    with pytest.raises(
        readiness.ReadinessViolation,
        match="LIFECYCLE_MUST_NOT_CLAIM:approval",
    ):
        readiness.LifecycleEvidence.from_mapping(lifecycle_raw)


@pytest.mark.parametrize(
    "direction",
    ["LONG_RISEX_SHORT_EXTENDED", "SHORT_RISEX_LONG_EXTENDED"],
)
def test_each_opposite_direction_is_one_valid_route_selection(
    tmp_path, monkeypatch, direction
):
    public = _public()
    evidence = dataclasses.replace(
        public,
        route=dataclasses.replace(public.route, direction=direction),
    )
    result = readiness.assess_readiness(evidence)
    assert result.status == readiness.READY_FOR_PROTECTED_PROVISIONING
    assert result.direction == direction

    _provision(tmp_path, monkeypatch)
    result = readiness.assess_readiness(evidence)
    assert result.status == readiness.READY_FOR_PRIVATE_READ_GATES


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        (
            {"direction": ("LONG_RISEX_SHORT_EXTENDED", "SHORT_RISEX_LONG_EXTENDED")},
            "ONE_ROUTE_DIRECTION_REQUIRED",
        ),
        ({"self_trade_free": False}, "SELF_TRADE_GUARD_NOT_PROVEN"),
        (
            {"counterparty_account_id": "synthetic-counterparty-account"},
            "PUBLIC_ROUTE_MUST_NOT_CLAIM_ACCOUNT_IDENTITY",
        ),
    ],
)
def test_public_route_direction_and_no_self_trade_barriers(
    tmp_path, monkeypatch, change, reason
):
    monkeypatch.setattr(
        readiness,
        "PROTECTED_SECRET_DIRECTORY",
        tmp_path / "fixed-provisioning-directory",
    )
    evidence = _public()
    route = dataclasses.replace(evidence.route, **change)
    result = readiness.assess_readiness(dataclasses.replace(evidence, route=route))
    assert result.status == readiness.BLOCKED
    assert reason in result.blockers


def test_phase_a_has_no_caps_and_does_not_mislabeled_gross_trade_notional_as_deposit(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        readiness,
        "PROTECTED_SECRET_DIRECTORY",
        tmp_path / "fixed-provisioning-directory",
    )
    evidence = _public()
    assert not hasattr(evidence, "caps")

    expensive_trade = dataclasses.replace(
        evidence,
        venues=tuple(
            dataclasses.replace(venue, reference_price_usd=Decimal("1000000"))
            for venue in evidence.venues
        ),
    )
    result = readiness.assess_readiness(expensive_trade)
    assert result.status == readiness.READY_FOR_PROTECTED_PROVISIONING
    assert result.gross_trade_notional_usd > Decimal("100")
    assert "deposit" not in result.evidence()


def test_public_route_loss_bound_is_only_finite_and_nonnegative_in_phase_a(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        readiness,
        "PROTECTED_SECRET_DIRECTORY",
        tmp_path / "fixed-provisioning-directory",
    )
    evidence = _public()
    route = dataclasses.replace(evidence.route, loss_bound_usd=Decimal("1000"))
    result = readiness.assess_readiness(dataclasses.replace(evidence, route=route))
    assert result.status == readiness.READY_FOR_PROTECTED_PROVISIONING
    assert result.loss_bound_usd == Decimal("1000")

    negative_route = dataclasses.replace(evidence.route, loss_bound_usd=Decimal("-1"))
    result = readiness.assess_readiness(
        dataclasses.replace(evidence, route=negative_route)
    )
    assert result.status == readiness.BLOCKED
    assert "LOSS_BOUND_MUST_NOT_BE_NEGATIVE" in result.blockers


@pytest.mark.parametrize(
    ("venue_name", "change", "reason"),
    [
        (
            "RISEx",
            {"fee_status": "PUBLIC_ASSUMPTION"},
            "RISEX_ACCOUNT_FEE_MUST_REMAIN_PENDING",
        ),
        (
            "RISEx",
            {"maker_fee_rate": Decimal("0.0001")},
            "RISEX_ACCOUNT_FEE_MUST_REMAIN_PENDING",
        ),
        (
            "Extended",
            {"fee_source": "PAPER_ASSUMPTION"},
            "EXTENDED_PUBLIC_FEE_NOT_CURRENT",
        ),
        (
            "RISEx",
            {"private_stream_status": "READY_ACCOUNT_SCOPED"},
            "PRIVATE_STREAM_MUST_REMAIN_PENDING:RISEx",
        ),
        (
            "Extended",
            {"private_stream_source": "PAPER_ASSUMPTION"},
            "PRIVATE_STREAM_MUST_REMAIN_PENDING:Extended",
        ),
        (
            "Extended",
            {"schedule_status": "ESTIMATED"},
            "PUBLIC_FUNDING_SCHEDULE_NOT_AUTHORITATIVE:Extended",
        ),
        (
            "Extended",
            {"metadata_current": False},
            "MARKET_METADATA_NOT_CURRENT:Extended",
        ),
        (
            "Extended",
            {"available_sell_quantity": Decimal("0")},
            "CURRENT_MARKET_VALUE_INVALID:Extended:available_sell_quantity",
        ),
    ],
)
def test_current_public_metadata_fee_schedule_and_pending_stream_inputs_are_required(
    tmp_path, monkeypatch, venue_name, change, reason
):
    monkeypatch.setattr(
        readiness,
        "PROTECTED_SECRET_DIRECTORY",
        tmp_path / "fixed-provisioning-directory",
    )
    result = readiness.assess_readiness(
        _with_venue(_public(), venue_name, **change)
    )
    assert result.status == readiness.BLOCKED
    assert reason in result.blockers


def test_public_funding_schedule_must_be_common_and_quantity_must_fit_both_sides(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        readiness,
        "PROTECTED_SECRET_DIRECTORY",
        tmp_path / "fixed-provisioning-directory",
    )
    schedule_result = readiness.assess_readiness(
        _with_venue(_public(), "Extended", funding_interval_seconds=1800)
    )
    assert schedule_result.status == readiness.BLOCKED
    assert "PUBLIC_FUNDING_SCHEDULE_NOT_COMMON" in schedule_result.blockers

    depth_result = readiness.assess_readiness(
        _with_venue(_public(), "Extended", available_sell_quantity=Decimal("0.00014"))
    )
    assert depth_result.status == readiness.BLOCKED
    assert "COMMON_QUANTITY_NOT_EXECUTABLE:Extended" in depth_result.blockers


def test_phase_c_requires_exact_private_facts_and_positive_planned_deposits(
    tmp_path, monkeypatch
):
    _provision(tmp_path, monkeypatch)
    result = readiness.assess_private_read(_public(), _private())
    assert result.status == readiness.PRIVATE_READ_GATES_COMPLETE
    assert result.planned_deposit_total_usd == Decimal("90.00")
    assert result.mainnet_write_authority == readiness.NO_MAINNET_WRITE_AUTHORITY
    assert not result.write_ready


@pytest.mark.parametrize(
    ("change_kind", "reason"),
    [
        ("duplicate_account", "PRIVATE_ACCOUNT_IDENTITIES_MUST_BE_DISTINCT"),
        ("wrong_environment", "PRIVATE_ACCOUNT_ENVIRONMENT_NOT_MAINNET:RISEx"),
        ("not_exact", "PRIVATE_ACCOUNT_IDENTITY_NOT_AUTHORITATIVE:RISEx"),
        ("fee_status", "ACCOUNT_FEE_NOT_AUTHORITATIVE:RISEx"),
        ("fee_value", "ACCOUNT_FEE_VALUE_INVALID:RISEx:maker"),
        ("private_stream", "PRIVATE_STREAM_NOT_READY:Extended"),
        ("unrelated_state", "UNRELATED_PRIVATE_STATE_PRESENT:RISEx"),
        ("deposit_account", "PLANNED_DEPOSIT_ACCOUNT_MISMATCH:RISEx"),
        ("negative_deposit", "PLANNED_DEPOSIT_MUST_BE_POSITIVE:Extended"),
        ("zero_deposit", "PLANNED_DEPOSIT_MUST_BE_POSITIVE:RISEx"),
        ("large_deposit", ""),
    ],
)
def test_phase_c_fails_closed_on_independent_private_gate_risks(
    tmp_path, monkeypatch, change_kind, reason
):
    _provision(tmp_path, monkeypatch)
    private = _private()
    if change_kind == "duplicate_account":
        private = _with_identity(
            private,
            "Extended",
            account_id=private.identities[0].account_id,
        )
    elif change_kind == "wrong_environment":
        private = _with_identity(private, "RISEx", environment="TESTNET")
    elif change_kind == "not_exact":
        private = _with_identity(private, "RISEx", exact=False)
    elif change_kind == "fee_status":
        private = _with_private_venue(private, "RISEx", fee_status="PAPER_ASSUMPTION")
    elif change_kind == "fee_value":
        private = _with_private_venue(private, "RISEx", maker_fee_rate="not-a-decimal")
    elif change_kind == "private_stream":
        private = _with_private_venue(
            private,
            "Extended",
            private_stream_status="PENDING_PRIVATE_READ",
        )
    elif change_kind == "unrelated_state":
        private = _with_private_venue(private, "RISEx", unrelated_state_clear=False)
    elif change_kind == "deposit_account":
        private = _with_deposit(
            private,
            "RISEx",
            account_id="synthetic-wrong-account-001",
        )
    elif change_kind == "negative_deposit":
        private = _with_deposit(private, "Extended", amount_usd=Decimal("-1"))
    elif change_kind == "zero_deposit":
        private = _with_deposit(private, "RISEx", amount_usd=Decimal("0"))
    elif change_kind == "large_deposit":
        private = _with_deposit(private, "Extended", amount_usd=Decimal("101"))
    result = readiness.assess_private_read(_public(), private)
    if change_kind == "large_deposit":
        assert result.status == readiness.PRIVATE_READ_GATES_COMPLETE
        assert result.planned_deposit_total_usd == Decimal("141.00")
    else:
        assert result.status == readiness.BLOCKED
        assert reason in result.blockers


def test_phase_c_requires_both_private_venue_reads_and_deposits(tmp_path, monkeypatch):
    _provision(tmp_path, monkeypatch)
    private = dataclasses.replace(_private(), venues=_private().venues[:1])
    result = readiness.assess_private_read(_public(), private)
    assert result.status == readiness.BLOCKED
    assert "PRIVATE_READ_REQUIRED_FOR_BOTH_VENUES" in result.blockers
    assert "PLANNED_DEPOSITS_MUST_INCLUDE_ONE_AMOUNT_PER_VENUE" not in result.blockers


@pytest.mark.parametrize(
    ("attribute", "value", "reason"),
    [
        ("route_id", "OTHER-ROUTE", "DISPATCH_APPROVAL_ROUTE_MISMATCH"),
        ("direction", "SHORT_RISEX_LONG_EXTENDED", "DISPATCH_APPROVAL_ROUTE_MISMATCH"),
        ("risex_venue", "OtherRISEx", "DISPATCH_APPROVAL_VENUE_BINDING_MISMATCH"),
        ("extended_venue", "OtherExtended", "DISPATCH_APPROVAL_VENUE_BINDING_MISMATCH"),
        ("risex_account_id", "synthetic-other-account-001", "DISPATCH_APPROVAL_RISEX_ACCOUNT_MISMATCH"),
        ("extended_account_id", "synthetic-other-account-002", "DISPATCH_APPROVAL_EXTENDED_ACCOUNT_MISMATCH"),
        ("risex_planned_deposit_usd", Decimal("41"), "DISPATCH_APPROVAL_RISEX_DEPOSIT_MISMATCH"),
        ("extended_planned_deposit_usd", Decimal("51"), "DISPATCH_APPROVAL_EXTENDED_DEPOSIT_MISMATCH"),
        ("deposit_cap_usd", Decimal("89.99"), "DISPATCH_APPROVAL_PLANNED_DEPOSITS_EXCEED_DEPOSIT_CAP"),
        ("deposit_cap_usd", Decimal("0"), "DISPATCH_APPROVAL_DEPOSIT_CAP_MUST_BE_POSITIVE"),
        ("maximum_loss_usd", Decimal("0.49"), "DISPATCH_APPROVAL_ROUTE_LOSS_EXCEEDS_MAXIMUM_LOSS_CAP"),
        ("maximum_loss_usd", Decimal("0"), "DISPATCH_APPROVAL_MAXIMUM_LOSS_CAP_MUST_BE_POSITIVE"),
        ("maximum_loss_usd", Decimal("101"), "DISPATCH_APPROVAL_MAXIMUM_LOSS_EXCEEDS_DEPOSIT_CAP"),
        ("approval_mode", "RELATIVE_PERCENT", "DISPATCH_APPROVAL_CAPS_MUST_BE_EXPLICIT_ABSOLUTE_USD"),
        ("scope", "UNBOUNDED", "DISPATCH_APPROVAL_SCOPE_INVALID"),
        ("manual_lifecycle_dispatch_authorized", False, "MANUAL_LIFECYCLE_DISPATCH_NOT_AUTHORIZED"),
        ("authorization_count", 2, "EXACTLY_ONE_MANUAL_LIFECYCLE_AUTHORIZATION_REQUIRED"),
    ],
)
def test_phase_d_approval_binds_route_accounts_planned_deposits_caps_and_one_manual_authorization(
    tmp_path, monkeypatch, attribute, value, reason
):
    _provision(tmp_path, monkeypatch)
    base = _approval()
    approval = dataclasses.replace(base.approval, **{attribute: value})
    approval_evidence = dataclasses.replace(base, approval=approval)
    result = readiness.assess_dispatch_approval(
        _public(), _private(), approval_evidence
    )
    assert result.status == readiness.BLOCKED
    assert reason in result.blockers


def test_phase_d_valid_future_approval_is_not_current_write_authority(
    tmp_path, monkeypatch
):
    _provision(tmp_path, monkeypatch)
    result = readiness.assess_dispatch_approval(_public(), _private(), _approval())
    assert result.status == readiness.FUTURE_DISPATCH_APPROVAL_COMPLETE
    assert result.complete
    assert not result.write_ready
    assert result.mainnet_write_authority == readiness.NO_MAINNET_WRITE_AUTHORITY
    evidence = result.evidence()
    assert "synthetic-risex-account-001" not in evidence
    assert "synthetic-write-risex-entry-001" not in evidence


def test_phase_d_owns_caps_and_accepts_a_valid_cap_without_phase_a_caps(
    tmp_path, monkeypatch
):
    _provision(tmp_path, monkeypatch)
    base = _approval()
    approval = dataclasses.replace(
        base.approval,
        deposit_cap_usd=Decimal("95"),
        maximum_loss_usd=Decimal("1"),
    )
    result = readiness.assess_dispatch_approval(
        _public(), _private(), dataclasses.replace(base, approval=approval)
    )
    assert result.status == readiness.FUTURE_DISPATCH_APPROVAL_COMPLETE


@pytest.mark.parametrize(
    ("attribute", "value", "reason"),
    [
        ("runtime_id", "", "RUNTIME_IDENTITY_INVALID"),
        ("runtime_fresh", False, "RUNTIME_IDENTITY_NOT_FRESH"),
        (
            "runtime_durable_before_dispatch",
            False,
            "RUNTIME_IDENTITY_NOT_DURABLE_BEFORE_DISPATCH",
        ),
        (
            "captured_immediately_before_dispatch",
            False,
            "RUNTIME_WRITE_EVIDENCE_NOT_IMMEDIATELY_BEFORE_DISPATCH",
        ),
        ("sequential_writes", False, "SEQUENTIAL_WRITE_CONTRACT_NOT_PROVEN"),
        ("no_blind_replay", False, "AMBIGUOUS_WRITE_REPLAY_NOT_BLOCKED"),
        (
            "restart_requires_reconciliation",
            False,
            "RESTART_RECONCILIATION_NOT_REQUIRED",
        ),
    ],
)
def test_phase_d_runtime_and_write_contract_barriers_fail_closed(
    tmp_path, monkeypatch, attribute, value, reason
):
    _provision(tmp_path, monkeypatch)
    base = _approval()
    operation = dataclasses.replace(base.operational, **{attribute: value})
    approval = dataclasses.replace(base, operational=operation)
    result = readiness.assess_dispatch_approval(_public(), _private(), approval)
    assert result.status == readiness.BLOCKED
    assert reason in result.blockers


def test_phase_d_requires_exact_four_ordered_venue_local_writes_and_distinct_ids(
    tmp_path, monkeypatch
):
    _provision(tmp_path, monkeypatch)
    base = _approval()

    short_operation = dataclasses.replace(
        base.operational,
        dispatches=base.operational.dispatches[:3],
    )
    short_result = readiness.assess_dispatch_approval(
        _public(), _private(), dataclasses.replace(base, operational=short_operation)
    )
    assert short_result.status == readiness.BLOCKED
    assert "EXACTLY_FOUR_DISPATCHES_REQUIRED" in short_result.blockers

    swapped = list(base.operational.dispatches)
    swapped[0], swapped[1] = swapped[1], swapped[0]
    swapped_operation = dataclasses.replace(base.operational, dispatches=tuple(swapped))
    swapped_result = readiness.assess_dispatch_approval(
        _public(), _private(), dataclasses.replace(base, operational=swapped_operation)
    )
    assert swapped_result.status == readiness.BLOCKED
    assert "DISPATCH_SEQUENCE_NOT_EXACT" in swapped_result.blockers

    duplicate = _with_dispatch(
        base,
        0,
        write_identity=base.operational.dispatches[1].write_identity,
        durable_before_dispatch=False,
    )
    duplicate_result = readiness.assess_dispatch_approval(
        _public(), _private(), duplicate
    )
    assert duplicate_result.status == readiness.BLOCKED
    assert "DISPATCH_WRITE_IDENTITIES_NOT_DISTINCT" in duplicate_result.blockers
    assert "DISPATCH_WRITE_IDENTITY_NOT_DURABLE:RISEx:ENTRY" in duplicate_result.blockers

    wrong_runtime = _with_dispatch(
        base,
        2,
        runtime_id="synthetic-other-runtime-001",
    )
    wrong_runtime_result = readiness.assess_dispatch_approval(
        _public(), _private(), wrong_runtime
    )
    assert wrong_runtime_result.status == readiness.BLOCKED
    assert "DISPATCH_RUNTIME_IDENTITY_MISMATCH" in wrong_runtime_result.blockers


def test_phase_d_requires_private_completion_before_approval(tmp_path, monkeypatch):
    result = readiness.assess_dispatch_approval(_public(), _private(), _approval())
    assert result.status == readiness.BLOCKED
    assert "PRIVATE_READ_REQUIREMENTS_REQUIRED_FOR_APPROVAL" in result.blockers
    assert not result.write_ready


def test_phase_e_is_separate_and_requires_all_prior_phases(tmp_path, monkeypatch):
    missing = readiness.assess_post_lifecycle(_public())
    assert missing.status == readiness.BLOCKED
    assert missing.reason == "POST_LIFECYCLE_EVIDENCE_REQUIRES_PRIOR_PHASES"

    _provision(tmp_path, monkeypatch)
    complete = readiness.assess_post_lifecycle(
        _public(), _private(), _approval(), _lifecycle()
    )
    assert complete.status == readiness.POST_LIFECYCLE_EVIDENCE_COMPLETE
    assert complete.complete
    assert not complete.write_ready
    assert complete.mainnet_write_authority == readiness.NO_MAINNET_WRITE_AUTHORITY


@pytest.mark.parametrize(
    "direction_and_sides",
    [
        ("LONG_RISEX_SHORT_EXTENDED", "LONG", "SHORT"),
        ("SHORT_RISEX_LONG_EXTENDED", "SHORT", "LONG"),
    ],
)
def test_post_lifecycle_accepts_both_opposite_bound_directions(
    tmp_path, monkeypatch, direction_and_sides
):
    direction, risex_side, extended_side = direction_and_sides
    public = _public()
    public = dataclasses.replace(
        public,
        route=dataclasses.replace(public.route, direction=direction),
    )
    lifecycle = _lifecycle()
    lifecycle = _with_leg(lifecycle, "RISEx", entry_side=risex_side)
    lifecycle = _with_leg(lifecycle, "Extended", entry_side=extended_side)
    base = _approval()
    approval = dataclasses.replace(
        base,
        approval=dataclasses.replace(base.approval, direction=direction),
    )
    result = _post(
        tmp_path,
        monkeypatch,
        evidence=public,
        approval=approval,
        lifecycle=lifecycle,
    )
    assert result.status == readiness.POST_LIFECYCLE_EVIDENCE_COMPLETE


@pytest.mark.parametrize(
    ("venue_name", "attribute", "value", "reason"),
    [
        (
            "RISEx",
            "order_reconciled",
            False,
            "ENTRY_ORDER_NOT_EXACTLY_RECONCILED:RISEx",
        ),
        (
            "Extended",
            "fill_reconciled",
            False,
            "ENTRY_FILL_NOT_EXACTLY_RECONCILED:Extended",
        ),
        (
            "RISEx",
            "position_reconciled",
            False,
            "ENTRY_POSITION_NOT_EXACTLY_RECONCILED:RISEx",
        ),
        (
            "Extended",
            "order_authoritative",
            False,
            "ENTRY_ORDER_NOT_AUTHORITATIVE:Extended",
        ),
        (
            "RISEx",
            "fill_authoritative",
            False,
            "ENTRY_FILL_NOT_AUTHORITATIVE:RISEx",
        ),
        (
            "Extended",
            "position_authoritative",
            False,
            "ENTRY_POSITION_NOT_AUTHORITATIVE:Extended",
        ),
        (
            "RISEx",
            "authoritative",
            False,
            "ENTRY_RECONCILIATION_NOT_AUTHORITATIVE:RISEx",
        ),
        (
            "Extended",
            "reduce_only",
            False,
            "REDUCE_ONLY_CLOSE_NOT_ACCEPTED:Extended",
        ),
        (
            "RISEx",
            "close_reconciled",
            False,
            "CLOSE_NOT_EXACTLY_RECONCILED:RISEx",
        ),
        (
            "Extended",
            "close_authoritative",
            False,
            "CLOSE_NOT_EXACTLY_RECONCILED:Extended",
        ),
    ],
)
def test_each_entry_order_fill_position_and_close_leg_is_independently_authoritative(
    tmp_path, monkeypatch, venue_name, attribute, value, reason
):
    lifecycle = _with_leg(_lifecycle(), venue_name, **{attribute: value})
    result = _post(tmp_path, monkeypatch, lifecycle=lifecycle)
    assert result.status == readiness.BLOCKED
    assert reason in result.blockers


def test_each_execution_leg_binds_its_own_venue_account_route_asset_side_and_quantity(
    tmp_path, monkeypatch
):
    private = _private()
    public = _public()
    extended = _lifecycle().execution.extended
    extended = dataclasses.replace(
        extended,
        venue="RISEx",
        account_id=private.identities[0].account_id,
        route_id="other-route",
        canonical_asset="ETH",
        entry_side="SHORT",
        canonical_quantity=Decimal("0.00016"),
    )
    lifecycle = dataclasses.replace(
        _lifecycle(),
        execution=dataclasses.replace(_lifecycle().execution, extended=extended),
    )
    result = _post(tmp_path, monkeypatch, lifecycle=lifecycle)
    assert result.status == readiness.BLOCKED
    assert "EXECUTION_VENUE_BINDING_MISMATCH:Extended" in result.blockers
    assert "EXECUTION_ACCOUNT_BINDING_MISMATCH:Extended" in result.blockers
    assert "EXECUTION_ROUTE_ID_MISMATCH:Extended" in result.blockers
    assert "EXECUTION_ASSET_MISMATCH:Extended" in result.blockers
    assert "EXECUTION_QUANTITY_NOT_EXACT_COMMON_QUANTITY:Extended" in result.blockers


def test_execution_ids_must_be_valid_and_distinct_per_venue_leg(tmp_path, monkeypatch):
    lifecycle = _with_leg(
        _lifecycle(),
        "RISEx",
        order_id="same-id",
        fill_id="same-id",
    )
    result = _post(tmp_path, monkeypatch, lifecycle=lifecycle)
    assert result.status == readiness.BLOCKED
    assert "EXECUTION_IDENTITIES_NOT_DISTINCT" in result.blockers


def test_funding_before_at_after_accepts_zero_and_negative_actual_cash(
    tmp_path, monkeypatch
):
    result = _post(tmp_path, monkeypatch)
    assert result.status == readiness.POST_LIFECYCLE_EVIDENCE_COMPLETE
    assert any(
        item.cash_usd == Decimal("-0.02") for item in _lifecycle().funding
    )


@pytest.mark.parametrize(
    ("index", "change", "reason"),
    [
        (
            4,
            {"cash_usd": None, "missing": True},
            "FUNDING_MISSING_OR_CONTRADICTORY:Extended:AT",
        ),
        (
            5,
            {"contradictory": True},
            "FUNDING_MISSING_OR_CONTRADICTORY:Extended:AFTER",
        ),
        (4, {"cash_usd": None}, "FUNDING_CASH_MISSING:Extended:AT"),
        (
            0,
            {"settlement_id": "synthetic-other-settlement"},
            "FUNDING_IDENTITY_CONTRADICTION:RISEx",
        ),
        (
            3,
            {"settlement_at": 1780000001},
            "FUNDING_SCHEDULE_IDENTITY_MISMATCH:Extended",
        ),
        (2, {"status": "UNKNOWN"}, "FUNDING_STATUS_UNKNOWN:RISEx:AFTER"),
        (
            2,
            {"status": "PROVEN_NON_ACCRUAL", "cash_usd": Decimal("0.01")},
            "FUNDING_NON_ACCRUAL_CASH_CONTRADICTION:RISEx:AFTER",
        ),
    ],
)
def test_funding_missing_contradictory_or_non_authoritative_evidence_blocks(
    tmp_path, monkeypatch, index, change, reason
):
    lifecycle = _with_funding(_lifecycle(), index, **change)
    result = _post(tmp_path, monkeypatch, lifecycle=lifecycle)
    assert result.status == readiness.BLOCKED
    assert reason in result.blockers


def test_funding_requires_all_six_observations_and_ordered_actual_observations(
    tmp_path, monkeypatch
):
    lifecycle = dataclasses.replace(_lifecycle(), funding=_lifecycle().funding[:5])
    result = _post(tmp_path, monkeypatch, lifecycle=lifecycle)
    assert result.status == readiness.BLOCKED
    assert "FUNDING_BEFORE_AT_AFTER_REQUIRED_FOR_BOTH_VENUES" in result.blockers

    lifecycle = _with_funding(_lifecycle(), 1, observed_at=1780000020)
    result = _post(tmp_path / "ordered", monkeypatch, lifecycle=lifecycle)
    assert result.status == readiness.BLOCKED
    assert "FUNDING_PHASE_ORDER_INVALID:RISEx" in result.blockers


@pytest.mark.parametrize(
    ("index", "change", "reason"),
    [
        (
            1,
            {"signature": "synthetic-different-terminal-state"},
            "TERMINAL_ROUNDS_DISAGREE",
        ),
        (1, {"relevant_open_orders": 1}, "TERMINAL_RELEVANT_ORDERS_NOT_ZERO"),
        (1, {"trigger_orders": 1}, "TERMINAL_TRIGGER_ORDERS_NOT_ZERO"),
        (1, {"unrelated_positions": 1}, "UNRELATED_ACCOUNT_STATE_PRESENT"),
        (
            1,
            {"extended_net_position_quantity": Decimal("0.00001")},
            "EXACT_FLATNESS_NOT_PROVEN",
        ),
        (1, {"authoritative": False}, "TERMINAL_ROUND_NOT_AUTHORITATIVE:2"),
        (1, {"phase": "OTHER"}, "TERMINAL_ROUND_NOT_AUTHORITATIVE:2"),
    ],
)
def test_terminal_rounds_require_two_authoritative_agreeing_zero_flat_snapshots(
    tmp_path, monkeypatch, index, change, reason
):
    lifecycle = _with_terminal(_lifecycle(), index, **change)
    result = _post(tmp_path, monkeypatch, lifecycle=lifecycle)
    assert result.status == readiness.BLOCKED
    assert reason in result.blockers


def test_terminal_round_count_and_sequence_are_required(tmp_path, monkeypatch):
    lifecycle = dataclasses.replace(
        _lifecycle(),
        terminal_rounds=(_lifecycle().terminal_rounds[0],),
    )
    result = _post(tmp_path, monkeypatch, lifecycle=lifecycle)
    assert result.status == readiness.BLOCKED
    assert "EXACTLY_TWO_TERMINAL_ROUNDS_REQUIRED" in result.blockers

    lifecycle = _with_terminal(_lifecycle(), 1, observed_at=1780000000)
    result = _post(tmp_path / "terminal-order", monkeypatch, lifecycle=lifecycle)
    assert result.status == readiness.BLOCKED
    assert "TERMINAL_ROUNDS_NOT_SEQUENTIAL" in result.blockers


def test_protected_provisioning_is_hidden_fixed_and_redacted(tmp_path, monkeypatch):
    monkeypatch.setattr(
        readiness,
        "PROTECTED_SECRET_DIRECTORY",
        tmp_path / "fixed-provisioning-directory",
    )
    prompts = []
    values = iter((SYNTHETIC_RISEX_SECRET, SYNTHETIC_EXTENDED_SECRET))

    def hidden_input(prompt):
        prompts.append(prompt)
        return next(values)

    result = readiness.provision_protected_identities(hidden_input)
    assert result.status == readiness.PROVISIONED
    assert not result.write_ready
    assert result.mainnet_write_authority == readiness.NO_MAINNET_WRITE_AUTHORITY
    assert len(prompts) == 2
    assert all("not a private key or seed phrase" in prompt for prompt in prompts)
    assert SYNTHETIC_RISEX_SECRET not in repr(result)
    assert SYNTHETIC_EXTENDED_SECRET not in repr(result)
    assert SYNTHETIC_RISEX_SECRET not in result.evidence()
    assert SYNTHETIC_EXTENDED_SECRET not in result.evidence()
    assert Path(result.files.risex.path).read_text() == SYNTHETIC_RISEX_SECRET
    assert Path(result.files.extended.path).read_text() == SYNTHETIC_EXTENDED_SECRET
    assert result.files.risex.mode == 0o600
    assert result.files.extended.mode == 0o600
    assert result.files.risex.link_count == 1
    assert result.files.extended.link_count == 1


def test_provisioning_rejects_duplicate_or_invalid_input_without_persisting(
    tmp_path, monkeypatch
):
    directory = tmp_path / "fixed-provisioning-directory"
    monkeypatch.setattr(readiness, "PROTECTED_SECRET_DIRECTORY", directory)
    duplicate = readiness.provision_protected_identities(
        lambda _prompt: "synthetic-same-opaque-value"
    )
    assert duplicate.status == readiness.BLOCKED
    assert duplicate.reason == "PROTECTED_IDENTITIES_NOT_DISTINCT"
    assert not directory.exists()

    invalid = readiness.provision_protected_identities(
        lambda prompt: "synthetic-valid" if "RISEx" in prompt else "bad\nvalue"
    )
    assert invalid.status == readiness.BLOCKED
    assert invalid.reason == "PROTECTED_INPUT_INVALID"
    assert not directory.exists()


def test_provisioning_rejects_existing_fixed_paths(tmp_path, monkeypatch):
    _provision(tmp_path, monkeypatch)
    second = readiness.provision_protected_identities(
        lambda _prompt: "synthetic-new-value"
    )
    assert second.status == readiness.BLOCKED
    assert second.reason == "PROTECTED_PATH_ALREADY_EXISTS"
    assert Path(second.files.risex.path).read_text() == SYNTHETIC_RISEX_SECRET


def test_protected_files_reject_mode_symlink_and_hardlink(tmp_path, monkeypatch):
    _provision(tmp_path, monkeypatch)
    paths = readiness.protected_secret_paths()

    os.chmod(paths["RISEx"], 0o644)
    inspected = readiness.inspect_protected_secret_files()
    assert inspected.risex.reason == "PROTECTED_FILE_MODE_NOT_0600"
    assert not inspected.risex.protected
    assert "PROTECTED_SECRET_FILE_NOT_SAFE:RISEx:PROTECTED_FILE_MODE_NOT_0600" in (
        readiness.assess_readiness(_public()).blockers
    )

    os.chmod(paths["RISEx"], 0o600)
    replacement = tmp_path / "replacement-secret"
    replacement.write_text("synthetic-replacement")
    paths["RISEx"].unlink()
    paths["RISEx"].symlink_to(replacement)
    inspected = readiness.inspect_protected_secret_files()
    assert inspected.risex.reason == "PROTECTED_FILE_SYMLINK"
    assert not inspected.risex.protected

    paths["RISEx"].unlink()
    hardlink_source = tmp_path / "hardlink-secret"
    hardlink_source.write_text("synthetic-hardlink")
    os.chmod(hardlink_source, 0o600)
    os.link(hardlink_source, paths["RISEx"])
    inspected = readiness.inspect_protected_secret_files()
    assert inspected.risex.reason == "PROTECTED_FILE_HARDLINK"
    assert not inspected.risex.protected


def test_protected_inspection_rejects_non_current_user_directory_and_file_owners(
    tmp_path, monkeypatch
):
    _provision(tmp_path, monkeypatch)
    paths = readiness.protected_secret_paths()
    current_uid = os.getuid()
    foreign_uid = current_uid + 100000

    monkeypatch.setattr(readiness.os, "getuid", lambda: foreign_uid)
    inspected = readiness.inspect_protected_secret_files()
    assert inspected.risex.reason == "PROTECTED_DIRECTORY_OWNER_NOT_CURRENT_USER"
    assert inspected.extended.reason == "PROTECTED_DIRECTORY_OWNER_NOT_CURRENT_USER"

    monkeypatch.setattr(readiness.os, "getuid", lambda: current_uid)
    real_lstat = readiness.os.lstat

    def foreign_file_lstat(path):
        info = real_lstat(path)
        if Path(path) in paths.values():
            return SimpleNamespace(
                st_mode=info.st_mode,
                st_nlink=info.st_nlink,
                st_size=info.st_size,
                st_uid=foreign_uid,
            )
        return info

    monkeypatch.setattr(readiness.os, "lstat", foreign_file_lstat)
    inspected = readiness.inspect_protected_secret_files()
    assert inspected.risex.reason == "PROTECTED_FILE_OWNER_NOT_CURRENT_USER"
    assert inspected.extended.reason == "PROTECTED_FILE_OWNER_NOT_CURRENT_USER"


def test_provisioning_rejects_foreign_owner_on_new_directory_components(
    tmp_path, monkeypatch
):
    directory = tmp_path / "new-parent" / "fixed-provisioning-directory"
    monkeypatch.setattr(readiness, "PROTECTED_SECRET_DIRECTORY", directory)
    current_uid = os.getuid()
    monkeypatch.setattr(readiness.os, "getuid", lambda: current_uid + 100000)

    result = readiness.provision_protected_identities(
        lambda prompt: "synthetic-risex" if "RISEx" in prompt else "synthetic-extended"
    )
    assert result.status == readiness.BLOCKED
    assert result.reason == "PROTECTED_DIRECTORY_OWNER_NOT_CURRENT_USER"
    assert not readiness.protected_secret_paths()["RISEx"].exists()
    assert not readiness.protected_secret_paths()["Extended"].exists()


def test_provisioning_rejects_foreign_owner_on_new_secret_files(tmp_path, monkeypatch):
    directory = tmp_path / "fixed-provisioning-directory"
    monkeypatch.setattr(readiness, "PROTECTED_SECRET_DIRECTORY", directory)
    current_uid = os.getuid()
    real_fstat = readiness.os.fstat

    def foreign_file_fstat(descriptor):
        info = real_fstat(descriptor)
        if stat.S_ISREG(info.st_mode):
            return SimpleNamespace(
                st_mode=info.st_mode,
                st_nlink=info.st_nlink,
                st_size=info.st_size,
                st_uid=current_uid + 100000,
            )
        return info

    monkeypatch.setattr(readiness.os, "fstat", foreign_file_fstat)
    result = readiness.provision_protected_identities(
        lambda prompt: "synthetic-risex" if "RISEx" in prompt else "synthetic-extended"
    )
    assert result.status == readiness.BLOCKED
    assert result.reason == "PROTECTED_FILE_OWNER_NOT_CURRENT_USER"
    assert not readiness.protected_secret_paths()["RISEx"].exists()
    assert not readiness.protected_secret_paths()["Extended"].exists()


def test_fixed_directory_must_be_private_and_not_a_symlink(tmp_path, monkeypatch):
    directory = tmp_path / "fixed-provisioning-directory"
    directory.mkdir(mode=0o755)
    monkeypatch.setattr(readiness, "PROTECTED_SECRET_DIRECTORY", directory)
    inspected = readiness.inspect_protected_secret_files()
    assert inspected.risex.reason == "PROTECTED_DIRECTORY_MODE_NOT_0700"
    assert inspected.extended.reason == "PROTECTED_DIRECTORY_MODE_NOT_0700"

    directory.rmdir()
    target = tmp_path / "real-directory"
    target.mkdir(mode=0o700)
    directory.symlink_to(target, target_is_directory=True)
    inspected = readiness.inspect_protected_secret_files()
    assert inspected.risex.reason == "PROTECTED_DIRECTORY_SYMLINK"
    failed = readiness.provision_protected_identities(lambda _prompt: "synthetic-value")
    assert failed.status == readiness.BLOCKED
    assert failed.reason == "PROTECTED_DIRECTORY_SYMLINK"


def test_malformed_nested_phase_records_fail_closed(tmp_path, monkeypatch):
    _provision(tmp_path, monkeypatch)
    approval = dataclasses.replace(_approval(), operational=object())
    approval_result = readiness.assess_dispatch_approval(
        _public(), _private(), approval
    )
    assert approval_result.status == readiness.BLOCKED
    assert "DISPATCH_APPROVAL_SCHEMA_INVALID:operational" in approval_result.blockers

    lifecycle = dataclasses.replace(_lifecycle(), execution=object())
    lifecycle_result = readiness.assess_post_lifecycle(
        _public(), _private(), _approval(), lifecycle
    )
    assert lifecycle_result.status == readiness.BLOCKED
    assert "POST_LIFECYCLE_EXECUTION_SCHEMA_INVALID" in lifecycle_result.blockers


def test_readiness_module_has_no_network_database_signing_or_write_transport_surface():
    source_path = ROOT / "src/risex_farmer/extended_mainnet_readiness.py"
    source = source_path.read_text()
    for forbidden in (
        "http://",
        "https://",
        "ws://",
        "wss://",
        "aiohttp",
        "requests",
        "sqlite3",
        "eth_account",
        "starknet",
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
        "getpass",
        "json",
        "math",
        "os",
        "pathlib",
        "stat",
        "typing",
        "__future__",
    }
    assert not hasattr(readiness, "main")


def test_normal_cli_import_does_not_import_readiness_module():
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT / "src")
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import risex_farmer.cli; print('risex_farmer.extended_mainnet_readiness' in sys.modules)",
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=True,
    )
    assert completed.stdout.strip() == "False"
