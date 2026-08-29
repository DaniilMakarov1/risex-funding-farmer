from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from pathlib import Path
import sqlite3

import pytest

from risex_farmer.nado_testnet_lifecycle import (
    COMPLETE,
    FUNDING_APPLIED,
    FUNDING_BLOCKED_CONTRADICTORY,
    FUNDING_BLOCKED_MISSING,
    FUNDING_UNRESOLVED,
    LONG,
    NADO_VENUE,
    RISEX_VENUE,
    SHORT,
    CrossRunAttestation,
    FundingBoundaryBinding,
    FundingLegBinding,
    FundingRouteBinding,
    IntentStore,
    JournalIdentity,
    NadoAccountFunding,
    NadoContractError,
    NadoFundingBaseline,
    NadoFundingEvent,
    NadoFundingExposure,
    TerminalEvidence,
    cross_run_attestation_digest,
    nado_account_funding_digest,
    nado_funding_account_amount_x18,
    nado_funding_baseline_digest,
    nado_funding_event_digest,
    nado_funding_exposure_digest,
    terminal_evidence_digest,
    validate_nado_funding_boundary,
)


OWNER = "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf"
NADO_ACCOUNT = OWNER + "64656661756c740000000000"
RISEx_JOURNAL = JournalIdentity(
    RISEX_VENUE, "risex-run-20260828-a", "risex-store-v1-immutable",
    "risex-primary-account",
)
NADO_JOURNAL = JournalIdentity(
    NADO_VENUE, "nado-run-20260828-a", "nado-store-v1-immutable", NADO_ACCOUNT,
)
SETTLEMENT = 1_700_003_600_000
EVENT_TIMESTAMP = SETTLEMENT * 1_000_000
ACCOUNT_TIMESTAMP = SETTLEMENT // 1_000
X18 = 10**18


def _terminal(journal: JournalIdentity) -> TerminalEvidence:
    unsigned = TerminalEvidence(
        journal=journal,
        status=COMPLETE,
        observed_at_ms=SETTLEMENT + 1_000,
        journal_content_sha256="0x" + "33" * 32,
        zero_regular_orders=True,
        zero_trigger_orders=True,
        exact_flat=True,
        unresolved_write_identities=(),
        evidence_digest="0x" + "00" * 32,
    )
    return replace(unsigned, evidence_digest="0x" + terminal_evidence_digest(unsigned))


def _route(
    *,
    risex_direction: str = LONG,
    nado_direction: str = SHORT,
    risex_quantity: object = Decimal("0.1"),
    nado_quantity: object = Decimal("0.1"),
) -> FundingRouteBinding:
    return FundingRouteBinding(
        canonical_asset="ETH",
        risex_leg=FundingLegBinding(
            RISEX_VENUE, "ETH-USDC", risex_direction,
            risex_quantity, Decimal("1"), risex_quantity,
        ),
        nado_leg=FundingLegBinding(
            NADO_VENUE, "ETH-PERP_USDT0", nado_direction,
            nado_quantity, Decimal("1"), nado_quantity,
        ),
        nado_product_id=44,
        settlement_at_ms=SETTLEMENT,
    )


def _binding(route: FundingRouteBinding | None = None) -> FundingBoundaryBinding:
    return FundingBoundaryBinding(route or _route(), RISEx_JOURNAL, NADO_JOURNAL)


def _attestation(binding: FundingBoundaryBinding) -> CrossRunAttestation:
    unsigned = CrossRunAttestation(
        route=binding.route,
        risex_journal=binding.risex_journal,
        nado_journal=binding.nado_journal,
        risex_terminal=_terminal(binding.risex_journal),
        nado_terminal=_terminal(binding.nado_journal),
        attestation_digest="0x" + "00" * 32,
    )
    return replace(unsigned, attestation_digest="0x" + cross_run_attestation_digest(unsigned))


def _baseline(
    binding: FundingBoundaryBinding,
    *,
    high_water: int | None = None,
    empty: bool | None = None,
    cumulative_long: int = 0,
    cumulative_short: int = 0,
) -> NadoFundingBaseline:
    unsigned = NadoFundingBaseline(
        journal=binding.nado_journal,
        owner=OWNER,
        subaccount_name="default",
        product_id=binding.route.nado_product_id,
        boundary_at_ms=binding.route.settlement_at_ms,
        history_high_water_idx=high_water,
        history_empty_terminal=(high_water is None if empty is None else empty),
        position_x18=0,
        v_quote_balance_x18=0,
        position_observed_at_ms=SETTLEMENT - 100,
        position_snapshot_id="baseline-position-1",
        cumulative_funding_long_x18=cumulative_long,
        cumulative_funding_short_x18=cumulative_short,
        open_interest_x18=0,
        public_observed_at_ms=SETTLEMENT - 100,
        baseline_digest="0x" + "00" * 32,
    )
    return replace(unsigned, baseline_digest="0x" + nado_funding_baseline_digest(unsigned))


def _event(
    binding: FundingBoundaryBinding,
    *,
    product_id: int | None = None,
    timestamp: int = EVENT_TIMESTAMP,
    payment_amount: int = 100,
    cumulative_long: int = X18,
    cumulative_short: int = X18,
    dt: int = 3_600_000_000_000,
) -> NadoFundingEvent:
    unsigned = NadoFundingEvent(
        product_id=binding.route.nado_product_id if product_id is None else product_id,
        timestamp=timestamp,
        payment_amount=payment_amount,
        open_interest=2_000,
        cumulative_funding_long_x18=cumulative_long,
        cumulative_funding_short_x18=cumulative_short,
        dt=dt,
        event_digest="0x" + "00" * 32,
    )
    return replace(unsigned, event_digest="0x" + nado_funding_event_digest(unsigned))


def _account(
    binding: FundingBoundaryBinding,
    *,
    idx: int = 0,
    timestamp: int = ACCOUNT_TIMESTAMP,
    product_id: int | None = None,
    amount: int | None = None,
    balance_amount: int | None = None,
    rate_x18: int = 125_000_000_000_000,
) -> NadoAccountFunding:
    quantity_x18 = int(binding.route.nado_leg.canonical_quantity * X18)
    position_x18 = (
        quantity_x18
        if binding.route.nado_leg.direction == LONG
        else -quantity_x18
    )
    if amount is None:
        amount = -position_x18
    if balance_amount is None:
        balance_amount = position_x18
    unsigned = NadoAccountFunding(
        journal=binding.nado_journal,
        owner=OWNER,
        subaccount_name="default",
        product_id=binding.route.nado_product_id if product_id is None else product_id,
        idx=idx,
        timestamp=timestamp,
        amount=amount,
        balance_amount=balance_amount,
        rate_x18=rate_x18,
        oracle_price_x18=2_000 * 10**18,
        evidence_digest="0x" + "00" * 32,
    )
    return replace(unsigned, evidence_digest="0x" + nado_account_funding_digest(unsigned))


def _exposure(
    binding: FundingBoundaryBinding,
    baseline: NadoFundingBaseline,
    **changes: object,
) -> NadoFundingExposure:
    quantity_x18 = int(binding.route.nado_leg.canonical_quantity * X18)
    direction = binding.route.nado_leg.direction
    position_x18 = quantity_x18 if direction == LONG else -quantity_x18
    cumulative = (
        baseline.cumulative_funding_long_x18
        if direction == LONG
        else baseline.cumulative_funding_short_x18
    )
    unsigned = NadoFundingExposure(
        journal=binding.nado_journal,
        owner=OWNER,
        subaccount_name="default",
        product_id=binding.route.nado_product_id,
        direction=direction,
        signed_position_x18=position_x18,
        route_quantity_x18=quantity_x18,
        observed_at_ms=SETTLEMENT - 50,
        snapshot_id="exposure-position-1",
        cumulative_side=direction,
        cumulative_funding_x18=cumulative,
        exposure_digest="0x" + "00" * 32,
    )
    unsigned = replace(unsigned, **changes)
    unsigned = replace(unsigned, exposure_digest="0x" + nado_funding_exposure_digest(unsigned))
    unsigned.assert_contract()
    return unsigned


def _rehash_exposure(
    exposure: NadoFundingExposure, **changes: object,
) -> NadoFundingExposure:
    unsigned = replace(
        exposure,
        **changes,
        exposure_digest="0x" + "00" * 32,
    )
    return replace(unsigned, exposure_digest="0x" + nado_funding_exposure_digest(unsigned))


def _valid_evidence() -> tuple[
    FundingBoundaryBinding, NadoFundingBaseline, CrossRunAttestation,
    NadoFundingEvent, NadoAccountFunding,
]:
    binding = _binding()
    baseline = _baseline(binding)
    event = _event(binding)
    return binding, baseline, _attestation(binding), event, _account(binding, idx=0)


def _validate(
    binding: FundingBoundaryBinding,
    baseline: NadoFundingBaseline,
    attestation: CrossRunAttestation,
    event: NadoFundingEvent | None,
    account: NadoAccountFunding | None,
    exposure: NadoFundingExposure | None = None,
):
    if exposure is None and baseline is not None:
        exposure = _exposure(binding, baseline)
    return validate_nado_funding_boundary(
        binding=binding,
        baseline=baseline,
        attestation=attestation,
        event=event,
        account_funding=account,
        exposure=exposure,
    )


def _seed_reconciled_entry(store: IntentStore) -> None:
    store._connection.execute(
        "INSERT INTO nado_intents VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "entry-for-funding-exposure", "1", SETTLEMENT - 200, "ENTRY", 44,
            b"{}", "1", "1", "1", 0, "entry-snapshot", SETTLEMENT - 200,
            "0", NADO_ACCOUNT, OWNER, "default", "RECONCILED",
        ),
    )
    store._connection.commit()


def test_empty_preentry_history_is_a_valid_baseline_and_public_cash_is_aggregate() -> None:
    binding, baseline, attestation, event, account = _valid_evidence()

    result = _validate(binding, baseline, attestation, event, account)

    assert baseline.history_empty_terminal is True
    assert baseline.history_high_water_idx is None
    assert result.status == FUNDING_APPLIED
    assert result.aggregate_payment_x18 == 100
    assert result.cash_x18 == account.amount
    assert result.cash_x18 != result.aggregate_payment_x18
    assert result.rate_x18 == account.rate_x18
    assert result.completion_eligible is True
    assert result.blocked is False
    assert not hasattr(event, "cash_x18")
    assert not hasattr(event, "status")


@pytest.mark.parametrize(
    ("cumulative_delta", "amount"),
    [(0, 0), (-X18, -(X18 // 10))],
)
def test_zero_and_negative_account_funding_are_applied(
    cumulative_delta: int, amount: int,
) -> None:
    binding, baseline, attestation, event, _ = _valid_evidence()
    event = _event(
        binding,
        cumulative_long=cumulative_delta,
        cumulative_short=cumulative_delta,
    )
    account = _account(binding, amount=amount)

    result = _validate(binding, baseline, attestation, event, account)

    assert result.cash_x18 == amount
    assert result.completion_eligible is True


def test_zero_funding_boundary_accepts_unchanged_public_cumulative_state() -> None:
    binding, baseline, attestation, _, _ = _valid_evidence()
    event = _event(
        binding,
        payment_amount=0,
        cumulative_long=baseline.cumulative_funding_long_x18,
        cumulative_short=baseline.cumulative_funding_short_x18,
    )
    account = _account(binding, amount=0, rate_x18=0)

    result = _validate(binding, baseline, attestation, event, account)

    assert result.status == FUNDING_APPLIED
    assert result.aggregate_payment_x18 == 0
    assert result.cash_x18 == 0


@pytest.mark.parametrize(
    ("nado_direction", "risex_direction"),
    [(LONG, SHORT), (SHORT, LONG)],
)
def test_signed_position_selects_the_matching_cumulative_side(
    nado_direction: str, risex_direction: str,
) -> None:
    binding = _binding(_route(
        risex_direction=risex_direction,
        nado_direction=nado_direction,
    ))
    baseline = _baseline(binding)
    event = _event(
        binding,
        cumulative_long=2 * X18,
        cumulative_short=2 * X18,
    )
    account = _account(
        binding,
        amount=(
            -2 * (X18 // 10)
            if nado_direction == LONG
            else 2 * (X18 // 10)
        ),
    )

    result = _validate(binding, baseline, _attestation(binding), event, account)

    assert result.status == FUNDING_APPLIED
    assert result.cash_x18 == account.amount


def test_on_chain_signed_division_rounds_toward_zero() -> None:
    assert nado_funding_account_amount_x18(1, -(X18 // 2)) == 0
    assert nado_funding_account_amount_x18(X18, -(X18 // 2)) == X18 // 2


def test_archive_rate_and_oracle_are_strict_metadata_not_settlement_inputs() -> None:
    binding, baseline, attestation, event, _ = _valid_evidence()
    account = _account(
        binding,
        amount=X18 // 10,
        rate_x18=-987_654_321,
    )

    result = _validate(binding, baseline, attestation, event, account)

    assert result.cash_x18 == account.amount
    assert result.rate_x18 == account.rate_x18


def test_terminal_content_digest_is_final_evidence_and_not_prebound_identity() -> None:
    binding, baseline, attestation, event, account = _valid_evidence()
    changed_terminal = replace(
        attestation.nado_terminal,
        journal_content_sha256="0x" + "44" * 32,
    )
    changed_terminal = replace(
        changed_terminal,
        evidence_digest="0x" + terminal_evidence_digest(changed_terminal),
    )
    changed_unsigned = replace(attestation, nado_terminal=changed_terminal)
    changed_attestation = replace(
        changed_unsigned,
        attestation_digest="0x" + cross_run_attestation_digest(changed_unsigned),
    )

    assert _validate(
        binding, baseline, changed_attestation, event, account,
    ).status == FUNDING_APPLIED


def test_route_rejects_quantity_not_bound_to_its_raw_leg() -> None:
    route = _route()
    malformed = replace(
        route,
        nado_leg=replace(route.nado_leg, canonical_quantity=Decimal("0.2")),
    )

    with pytest.raises(NadoContractError, match="bound to raw quantity"):
        malformed.assert_contract()


def test_attestation_requires_exact_persisted_journals_and_terminal_proofs() -> None:
    binding, baseline, attestation, event, account = _valid_evidence()
    wrong_risex_journal = JournalIdentity(
        RISEX_VENUE, "risex-run-other", "0x" + "33" * 32,
        "risex-primary-account",
    )
    wrong_binding = replace(binding, risex_journal=wrong_risex_journal)
    with pytest.raises(NadoContractError, match="RISEx journal identity"):
        _validate(wrong_binding, baseline, attestation, event, account)

    unbound_terminal = _terminal(wrong_risex_journal)
    unbound_unsigned = replace(attestation, risex_terminal=unbound_terminal)
    unbound = replace(
        unbound_unsigned,
        attestation_digest="0x" + cross_run_attestation_digest(unbound_unsigned),
    )
    with pytest.raises(NadoContractError, match="journal binding"):
        _validate(binding, baseline, unbound, event, account)


def test_attestation_digest_binds_terminal_evidence_not_just_summary() -> None:
    binding, baseline, attestation, event, account = _valid_evidence()
    tampered_terminal = replace(attestation.risex_terminal, exact_flat=False)
    tampered_unsigned = replace(attestation, risex_terminal=tampered_terminal)
    tampered = replace(
        tampered_unsigned,
        attestation_digest="0x" + cross_run_attestation_digest(tampered_unsigned),
    )

    with pytest.raises(NadoContractError, match="terminal evidence"):
        _validate(binding, baseline, tampered, event, account)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"risex_direction": LONG, "nado_direction": LONG},
        {"risex_direction": SHORT, "nado_direction": SHORT},
    ],
)
def test_route_rejects_same_direction_legs(kwargs: dict[str, str]) -> None:
    with pytest.raises(NadoContractError, match="opposite directions"):
        _route(**kwargs).assert_contract()


def test_route_rejects_different_canonical_quantities() -> None:
    with pytest.raises(NadoContractError, match="one exact canonical quantity"):
        _route(nado_quantity=Decimal("0.100000000000000001")).assert_contract()


@pytest.mark.parametrize(
    "event_kwargs",
    [
        {"product_id": 43},
        {"timestamp": EVENT_TIMESTAMP + 3_600_000_000_000},
        {"cumulative_long": 0, "cumulative_short": 0},
    ],
)
def test_wrong_product_time_or_cumulative_transition_is_rejected(
    event_kwargs: dict[str, int],
) -> None:
    binding, baseline, attestation, _, account = _valid_evidence()
    event = _event(binding, **event_kwargs)

    with pytest.raises(NadoContractError, match="product|boundary|transition|cumulative|amount"):
        _validate(binding, baseline, attestation, event, account)


def test_public_funding_interval_must_be_one_hour() -> None:
    binding, baseline, attestation, _, account = _valid_evidence()
    event = _event(binding, dt=3_599_000_000_000)

    with pytest.raises(NadoContractError, match="interval"):
        _validate(binding, baseline, attestation, event, account)


def test_stale_or_prior_account_row_is_not_attributable() -> None:
    binding, baseline, attestation, event, _ = _valid_evidence()
    stale = _account(binding, idx=0)
    nonempty = _baseline(binding, high_water=0, empty=False)

    with pytest.raises(NadoContractError, match="stale|prior"):
        _validate(binding, nonempty, attestation, event, stale)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"signed_position_x18": X18 // 10}, "sign or quantity"),
        ({"route_quantity_x18": X18 // 5}, "sign or quantity"),
        ({"signed_position_x18": 0}, "not open"),
        ({"observed_at_ms": SETTLEMENT - 100}, "fresh exact route position"),
        ({"authoritative": False}, "not authoritative"),
        ({"cumulative_side": LONG}, "cumulative side"),
    ],
)
def test_exposure_binding_rejects_wrong_sign_quantity_open_fresh_or_side(
    changes: dict[str, object], message: str,
) -> None:
    binding, baseline, attestation, event, account = _valid_evidence()
    exposure = _rehash_exposure(_exposure(binding, baseline), **changes)

    with pytest.raises(NadoContractError, match=message):
        _validate(binding, baseline, attestation, event, account, exposure)


def test_exposure_binding_rejects_stale_cumulative_state() -> None:
    binding, baseline, attestation, event, account = _valid_evidence()
    exposure = _rehash_exposure(
        _exposure(binding, baseline),
        cumulative_funding_x18=baseline.cumulative_funding_short_x18 + 1,
    )

    with pytest.raises(NadoContractError, match="cumulative state is stale"):
        _validate(binding, baseline, attestation, event, account, exposure)


def test_account_balance_must_match_exact_signed_exposure() -> None:
    binding, baseline, attestation, event, _ = _valid_evidence()
    account = _account(binding, balance_amount=X18 // 10)

    with pytest.raises(NadoContractError, match="balance does not equal"):
        _validate(binding, baseline, attestation, event, account)


def test_account_amount_must_match_exact_cumulative_settlement() -> None:
    binding, baseline, attestation, event, _ = _valid_evidence()
    account = _account(binding, amount=1)

    with pytest.raises(NadoContractError, match="exact cumulative settlement"):
        _validate(binding, baseline, attestation, event, account)


def test_public_cumulative_transition_must_agree_on_both_sides() -> None:
    binding, baseline, attestation, _, account = _valid_evidence()
    event = _event(binding, cumulative_long=X18, cumulative_short=2 * X18)

    with pytest.raises(NadoContractError, match="cumulative funding sides"):
        _validate(binding, baseline, attestation, event, account)


def test_new_account_row_must_be_strictly_after_persisted_high_water() -> None:
    binding = _binding()
    baseline = _baseline(binding, high_water=10, empty=False)
    event = _event(binding)
    account = _account(binding, idx=11)

    result = _validate(binding, baseline, _attestation(binding), event, account)

    assert result.account_idx == 11


def test_account_timestamp_must_be_in_the_target_boundary() -> None:
    binding, baseline, attestation, event, _ = _valid_evidence()
    account = _account(binding, timestamp=ACCOUNT_TIMESTAMP + 3_600)

    with pytest.raises(NadoContractError, match="outside"):
        _validate(binding, baseline, attestation, event, account)


def test_funding_evidence_requires_persisted_baseline() -> None:
    binding, _, attestation, event, account = _valid_evidence()

    with pytest.raises(NadoContractError, match="baseline"):
        _validate(binding, None, attestation, event, account)


def test_baseline_observations_must_precede_the_target_boundary() -> None:
    binding, baseline, attestation, event, account = _valid_evidence()
    unsigned = replace(
        baseline,
        position_observed_at_ms=binding.route.settlement_at_ms,
    )
    late = replace(
        unsigned,
        baseline_digest="0x" + nado_funding_baseline_digest(unsigned),
    )

    with pytest.raises(NadoContractError, match="precede the boundary"):
        _validate(binding, late, attestation, event, account)


def test_missing_post_boundary_evidence_is_durable_blocked_and_not_zero(
    tmp_path: Path,
) -> None:
    binding, baseline, attestation, _, _ = _valid_evidence()
    store = IntentStore(tmp_path / "missing.sqlite3")
    try:
        store.bind_funding_boundary(binding, baseline)
        result = store.record_nado_funding_boundary(
            attestation=attestation, event=None, account_funding=None,
        )
        assert result.status == FUNDING_UNRESOLVED
        assert result.cash_x18 is None
        assert result.completion_eligible is False
        assert result.blocked is True
        assert store.funding_boundary_blocker() == FUNDING_BLOCKED_MISSING
        assert store.nado_funding_boundary_evidence() is None
        assert store.lifecycle_status() == "HALTED"
    finally:
        store.close()


def test_valid_evidence_and_baseline_are_immutable_across_restart(tmp_path: Path) -> None:
    binding, baseline, attestation, event, account = _valid_evidence()
    path = tmp_path / "funding.sqlite3"
    store = IntentStore(path)
    try:
        store.bind_funding_boundary(binding, baseline)
        _seed_reconciled_entry(store)
        exposure = _exposure(binding, baseline)
        store.bind_funding_exposure(exposure)
        assert store.funding_boundary_binding() == binding
        assert store.funding_boundary_baseline() == baseline
        assert store.funding_boundary_exposure() == exposure
        with pytest.raises(NadoContractError, match="immutable"):
            store.bind_funding_exposure(
                _rehash_exposure(exposure, snapshot_id="different-exposure")
            )
        result = store.record_nado_funding_boundary(
            attestation=attestation, event=event, account_funding=account,
        )
        assert result.cash_x18 == account.amount
    finally:
        store.close()

    connection = sqlite3.connect(path)
    try:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        assert "nado_funding_progression" not in tables
        assert "nado_funding_progression_activation" not in tables
    finally:
        connection.close()

    reopened = IntentStore(path)
    try:
        assert reopened.funding_boundary_progression() == ()
        assert reopened.funding_boundary_binding() == binding
        assert reopened.funding_boundary_baseline() == baseline
        assert reopened.funding_boundary_exposure() == exposure
        assert reopened.nado_funding_boundary_evidence() == (
            attestation, baseline, event, account, exposure,
        )
        assert reopened.record_nado_funding_boundary(
            attestation=attestation, event=event, account_funding=account,
        ) == result
        contradictory = _account(binding, idx=0, amount=1)
        blocked = reopened.record_nado_funding_boundary(
            attestation=attestation, event=event, account_funding=contradictory,
        )
        assert blocked.status == FUNDING_UNRESOLVED
        assert reopened.funding_boundary_blocker() == FUNDING_BLOCKED_CONTRADICTORY
        assert reopened.lifecycle_status() == "HALTED"
    finally:
        reopened.close()


def test_funding_evidence_requires_a_persisted_binding(tmp_path: Path) -> None:
    binding, _, attestation, event, account = _valid_evidence()
    del binding
    store = IntentStore(tmp_path / "unbound.sqlite3")
    try:
        with pytest.raises(NadoContractError, match="baseline"):
            store.record_nado_funding_boundary(
                attestation=attestation, event=event, account_funding=account,
            )
        assert store.lifecycle_status() == "HALTED"
    finally:
        store.close()


def test_baseline_and_binding_cannot_be_added_after_intent_preparation(tmp_path: Path) -> None:
    binding, baseline, _, _, _ = _valid_evidence()
    store = IntentStore(tmp_path / "late-binding.sqlite3")
    try:
        store._connection.execute(
            "INSERT INTO nado_intents VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "late-digest", "1", 1_700_000_000_000, "ENTRY", 44, b"{}",
                "1", "1", "1", 0, None, None, "0", NADO_ACCOUNT,
                OWNER, "default", "PREPARED",
            ),
        )
        store._connection.commit()
        with pytest.raises(NadoContractError, match="before intent preparation"):
            store.bind_funding_boundary(binding)
        store._connection.execute("DELETE FROM nado_intents")
        store._connection.commit()
        store.bind_funding_boundary(binding)
        store._connection.execute(
            "INSERT INTO nado_intents VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "late-digest", "1", 1_700_000_000_000, "ENTRY", 44, b"{}",
                "1", "1", "1", 0, None, None, "0", NADO_ACCOUNT,
                OWNER, "default", "PREPARED",
            ),
        )
        store._connection.commit()
        with pytest.raises(NadoContractError, match="before intent preparation"):
            store.bind_funding_baseline(baseline)
    finally:
        store.close()
