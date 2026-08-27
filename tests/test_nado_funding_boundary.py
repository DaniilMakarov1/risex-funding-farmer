from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from risex_farmer.nado_testnet_lifecycle import (
    COMPLETE,
    FUNDING_APPLIED,
    FUNDING_SKIPPED_POSITION_CLOSED,
    FUNDING_SKIPPED_POSITION_NOT_OPEN,
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
    NadoFundingEvent,
    TerminalEvidence,
    cross_run_attestation_digest,
    nado_account_funding_digest,
    nado_funding_event_digest,
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


def _event(
    binding: FundingBoundaryBinding,
    *,
    event_id: str = "nado-funding-event-1",
    market: str | None = None,
    settlement_at_ms: int | None = None,
    status: str = FUNDING_APPLIED,
    rate_x18: int = 125_000_000_000_000,
    cash_x18: int = 0,
) -> NadoFundingEvent:
    unsigned = NadoFundingEvent(
        event_id=event_id,
        journal=binding.nado_journal,
        product_id=binding.route.nado_product_id,
        market=market or binding.route.nado_leg.market,
        settlement_at_ms=settlement_at_ms or binding.route.settlement_at_ms,
        canonical_quantity=binding.route.canonical_quantity,
        rate_x18=rate_x18,
        cash_x18=cash_x18,
        status=status,
        event_digest="0x" + "00" * 32,
    )
    return replace(unsigned, event_digest="0x" + nado_funding_event_digest(unsigned))


def _account(
    binding: FundingBoundaryBinding,
    event: NadoFundingEvent,
    *,
    cash_x18: int | None = None,
    rate_x18: int | None = None,
    status: str | None = None,
    event_id: str | None = None,
) -> NadoAccountFunding:
    unsigned = NadoAccountFunding(
        journal=binding.nado_journal,
        owner=OWNER,
        subaccount_name="default",
        event_id=event_id or event.event_id,
        product_id=event.product_id,
        market=event.market,
        settlement_at_ms=event.settlement_at_ms,
        canonical_quantity=binding.route.canonical_quantity,
        rate_x18=event.rate_x18 if rate_x18 is None else rate_x18,
        cash_x18=event.cash_x18 if cash_x18 is None else cash_x18,
        status=event.status if status is None else status,
        evidence_digest="0x" + "00" * 32,
    )
    return replace(unsigned, evidence_digest="0x" + nado_account_funding_digest(unsigned))


def _valid_evidence() -> tuple[
    FundingBoundaryBinding, CrossRunAttestation, NadoFundingEvent, NadoAccountFunding
]:
    binding = _binding()
    event = _event(binding)
    return binding, _attestation(binding), event, _account(binding, event)


def test_funding_boundary_accepts_exact_opposite_route_and_account_event_match() -> None:
    binding, attestation, event, account = _valid_evidence()

    result = validate_nado_funding_boundary(
        binding=binding,
        attestation=attestation,
        event=event,
        account_funding=account,
    )

    assert result.status == FUNDING_APPLIED
    assert result.cash_x18 == 0
    assert result.rate_x18 == event.rate_x18
    assert result.completion_eligible is True
    assert result.blocked is False


def test_applied_negative_funding_cash_remains_completion_eligible() -> None:
    binding, attestation, _, _ = _valid_evidence()
    event = _event(binding, cash_x18=-1)
    account = _account(binding, event)

    result = validate_nado_funding_boundary(
        binding=binding,
        attestation=attestation,
        event=event,
        account_funding=account,
    )

    assert result.status == FUNDING_APPLIED
    assert result.cash_x18 == -1
    assert result.completion_eligible is True
    assert result.blocked is False


def test_terminal_content_digest_is_final_evidence_and_not_prebound_identity() -> None:
    binding, attestation, event, account = _valid_evidence()

    assert binding.nado_journal.store_identity == "nado-store-v1-immutable"
    assert attestation.nado_terminal.journal == binding.nado_journal
    assert attestation.nado_terminal.journal_content_sha256 != "0x" + "22" * 32
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
    assert validate_nado_funding_boundary(
        binding=binding,
        attestation=changed_attestation,
        event=event,
        account_funding=account,
    ).status == FUNDING_APPLIED


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


def test_route_rejects_quantity_not_bound_to_its_raw_leg() -> None:
    route = _route()
    malformed = replace(
        route,
        nado_leg=replace(route.nado_leg, canonical_quantity=Decimal("0.2")),
    )
    with pytest.raises(NadoContractError, match="bound to raw quantity"):
        malformed.assert_contract()


def test_attestation_requires_exact_persisted_journals_and_terminal_proofs() -> None:
    binding, attestation, event, account = _valid_evidence()

    wrong_risex_journal = JournalIdentity(
        RISEX_VENUE, "risex-run-other", "0x" + "33" * 32,
        "risex-primary-account",
    )
    wrong_binding = replace(binding, risex_journal=wrong_risex_journal)
    with pytest.raises(NadoContractError, match="RISEx journal identity"):
        validate_nado_funding_boundary(
            binding=wrong_binding,
            attestation=attestation,
            event=event,
            account_funding=account,
        )

    unbound_terminal = _terminal(wrong_risex_journal)
    unbound_unsigned = replace(attestation, risex_terminal=unbound_terminal)
    unbound = replace(
        unbound_unsigned,
        attestation_digest="0x" + cross_run_attestation_digest(unbound_unsigned),
    )
    with pytest.raises(NadoContractError, match="journal binding"):
        validate_nado_funding_boundary(
            binding=binding,
            attestation=unbound,
            event=event,
            account_funding=account,
        )


def test_attestation_digest_binds_terminal_evidence_not_just_summary() -> None:
    binding, attestation, event, account = _valid_evidence()
    tampered_terminal = replace(attestation.risex_terminal, exact_flat=False)
    tampered_unsigned = replace(attestation, risex_terminal=tampered_terminal)
    tampered = replace(
        tampered_unsigned,
        attestation_digest="0x" + cross_run_attestation_digest(tampered_unsigned),
    )

    with pytest.raises(NadoContractError, match="terminal evidence"):
        validate_nado_funding_boundary(
            binding=binding,
            attestation=tampered,
            event=event,
            account_funding=account,
        )


@pytest.mark.parametrize(
    "changed",
    [
        {"market": "BTC-PERP_USDT0"},
        {"settlement_at_ms": SETTLEMENT + 3_600_000},
    ],
)
def test_funding_event_must_match_persisted_market_and_settlement(
    changed: dict[str, object],
) -> None:
    binding, attestation, valid_event, account = _valid_evidence()
    event = _event(binding, **changed)
    account = _account(binding, event)
    with pytest.raises(NadoContractError, match="persisted market settlement"):
        validate_nado_funding_boundary(
            binding=binding,
            attestation=attestation,
            event=event,
            account_funding=account,
        )
    assert valid_event.market == "ETH-PERP_USDT0"


def test_missing_funding_is_not_treated_as_zero() -> None:
    binding, attestation, event, account = _valid_evidence()
    with pytest.raises(NadoContractError, match="incomplete"):
        validate_nado_funding_boundary(
            binding=binding,
            attestation=attestation,
            event=None,
            account_funding=account,
        )

    unresolved_event = _event(binding, status=FUNDING_UNRESOLVED)
    unresolved_account = _account(binding, unresolved_event, status=FUNDING_UNRESOLVED)
    result = validate_nado_funding_boundary(
        binding=binding,
        attestation=attestation,
        event=unresolved_event,
        account_funding=unresolved_account,
    )
    assert result.status == FUNDING_UNRESOLVED
    assert result.cash_x18 == 0
    assert result.completion_eligible is False
    assert result.blocked is True


@pytest.mark.parametrize(
    "status",
    [FUNDING_SKIPPED_POSITION_NOT_OPEN, FUNDING_SKIPPED_POSITION_CLOSED],
)
def test_exact_skipped_funding_is_retained_as_non_accrual_not_zero(
    status: str,
) -> None:
    binding, attestation, _, _ = _valid_evidence()
    event = _event(binding, status=status)
    account = _account(binding, event, status=status)

    result = validate_nado_funding_boundary(
        binding=binding,
        attestation=attestation,
        event=event,
        account_funding=account,
    )

    assert result.status == status
    assert result.cash_x18 == 0
    assert result.completion_eligible is False
    assert result.blocked is True


def test_skipped_funding_with_applied_cash_is_contradictory() -> None:
    binding, attestation, _, _ = _valid_evidence()
    event = _event(binding, status=FUNDING_SKIPPED_POSITION_NOT_OPEN, cash_x18=1)
    account = _account(binding, event)

    with pytest.raises(NadoContractError, match="nonzero applied cash"):
        validate_nado_funding_boundary(
            binding=binding,
            attestation=attestation,
            event=event,
            account_funding=account,
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("cash_x18", 1, "agree"),
        ("rate_x18", 1, "agree"),
        ("status", FUNDING_UNRESOLVED, "agree"),
        ("event_id", "other-event", "agree"),
    ],
)
def test_account_funding_must_agree_with_authoritative_event(
    field: str, value: object, message: str,
) -> None:
    binding, attestation, event, _ = _valid_evidence()
    account = _account(binding, event, **{field: value})
    with pytest.raises(NadoContractError, match=message):
        validate_nado_funding_boundary(
            binding=binding,
            attestation=attestation,
            event=event,
            account_funding=account,
        )


def test_funding_binding_and_evidence_are_immutable_across_restart(tmp_path: Path) -> None:
    binding, attestation, event, account = _valid_evidence()
    path = tmp_path / "nado-funding.sqlite3"
    store = IntentStore(path)
    try:
        store.bind_funding_boundary(binding)
        assert store.funding_boundary_binding() == binding
        result = store.record_nado_funding_boundary(
            attestation=attestation, event=event, account_funding=account
        )
        assert result.cash_x18 == 0
    finally:
        store.close()

    reopened = IntentStore(path)
    try:
        assert reopened.funding_boundary_binding() == binding
        assert reopened.nado_funding_boundary_evidence() == (
            attestation, event, account
        )
        assert reopened.record_nado_funding_boundary(
            attestation=attestation, event=event, account_funding=account
        ) == result
        contradictory = _account(binding, event, cash_x18=1)
        with pytest.raises(NadoContractError, match="agree|contradicts"):
            reopened.record_nado_funding_boundary(
                attestation=attestation, event=event,
                account_funding=contradictory,
            )
        assert reopened.lifecycle_status() == "HALTED"
    finally:
        reopened.close()


def test_unresolved_funding_is_durable_and_blocks_the_lifecycle(tmp_path: Path) -> None:
    binding, attestation, _, _ = _valid_evidence()
    event = _event(binding, status=FUNDING_UNRESOLVED)
    account = _account(binding, event, status=FUNDING_UNRESOLVED)
    store = IntentStore(tmp_path / "unresolved.sqlite3")
    try:
        store.bind_funding_boundary(binding)
        result = store.record_nado_funding_boundary(
            attestation=attestation, event=event, account_funding=account
        )
        assert result.status == FUNDING_UNRESOLVED
        assert result.blocked is True
        assert store.lifecycle_status() == "HALTED"
        assert store.nado_funding_boundary_evidence() == (
            attestation, event, account
        )
    finally:
        store.close()


def test_binding_after_intent_preparation_is_rejected(tmp_path: Path) -> None:
    binding, _, _, _ = _valid_evidence()
    store = IntentStore(tmp_path / "late-binding.sqlite3")
    try:
        # A minimal prepared intent is unnecessary here: the store's durable
        # identity table is the exact guard used by the lifecycle before any
        # funding-boundary runner can add evidence.
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
    finally:
        store.close()


def test_funding_evidence_requires_a_persisted_binding(tmp_path: Path) -> None:
    binding, attestation, event, account = _valid_evidence()
    del binding
    store = IntentStore(tmp_path / "unbound.sqlite3")
    try:
        with pytest.raises(NadoContractError, match="not persisted"):
            store.record_nado_funding_boundary(
                attestation=attestation, event=event, account_funding=account
            )
        assert store.lifecycle_status() == "HALTED"
    finally:
        store.close()
