from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from decimal import Decimal
import inspect
from pathlib import Path
from types import SimpleNamespace
import threading

import pytest

import risex_farmer.testnet_risex_two_account_coordinator as coordinator_module
import risex_farmer.testnet_risex_nado_funding_boundary as boundary_module
from risex_farmer.nado_testnet_lifecycle import (
    FundingBoundaryBinding,
    JournalIdentity,
    NADO_VENUE,
    RisexTerminalEvidence,
)
from risex_farmer.testnet_risex_nado_funding_boundary import (
    BoundarySignalKind,
    FUNDING_BLOCKER_AUTHORITATIVE_INJECTION,
    FUNDING_BLOCKER_BOUNDARY_GATE_MISSING,
    FUNDING_BLOCKER_BOUNDARY_INTERRUPTED,
    FUNDING_BLOCKER_EVIDENCE_MISSING,
    FUNDING_BLOCKER_EVIDENCE_STALE,
    FUNDING_BLOCKER_ENTRY_AFTER_BOUNDARY,
    FUNDING_BLOCKER_GATE_CALLBACK_CANCELLED,
    FUNDING_BLOCKER_MISSING_CONTRACT,
    FUNDING_BOUNDARY_COUNTERPARTY_JOURNAL,
    FUNDING_BOUNDARY_PRIMARY_JOURNAL,
    FundingBoundaryResult,
    HoldReleaseSignal,
    RisexCoordinationLoopBridge,
    RisexFundingBoundaryCoordinator,
    RisexFundingBoundaryError,
    RisexTerminalEvidenceProvider,
    TARGET_NADO_MARKET,
    TARGET_NADO_PRODUCT_ID,
    TARGET_QUANTITY,
    TARGET_RISEX_MARKET,
    fixed_funding_route,
)

from test_testnet_risex_two_account_coordinator import (
    LifecycleVenue,
    NOW,
    identity_factory,
    lifecycle_observations,
)


NOW_AT_BOUNDARY = NOW + 1
NOW_FOR_TERMINAL = NOW + 3
SETTLEMENT_AT_MS = NOW_AT_BOUNDARY * 1_000


@dataclass(frozen=True)
class CallbackEvidence:
    observed_at_ms: int
    snapshot_id: str
    sequence: int


@dataclass(frozen=True)
class LiveObservation:
    evidence: CallbackEvidence


@dataclass
class MutableCallbackEvidence:
    observed_at_ms: int
    snapshot_id: str
    sequence: int


@dataclass
class MutableLiveObservation:
    evidence: MutableCallbackEvidence


def _retime_order(item, observed_at: int):
    return replace(item, observed_at=observed_at)


def _retime_trade(item, observed_at: int):
    return replace(item, observed_at=observed_at)


def _retime_account(account, observed_at: int):
    open_orders = tuple(_retime_order(item, observed_at) for item in account.open_orders)
    history_orders = tuple(_retime_order(item, observed_at) for item in account.history_orders)
    trades = tuple(_retime_trade(item, observed_at) for item in account.trades)
    portfolio = replace(account.portfolio, observed_at=observed_at)
    private = account.private
    if private is not None:
        private = replace(
            private,
            observed_at=observed_at,
            orders_snapshot=tuple(
                _retime_order(item, observed_at) for item in private.orders_snapshot
            ),
            orders_updates=tuple(
                _retime_order(item, observed_at) for item in private.orders_updates
            ),
        )
    return replace(
        account,
        open_orders=open_orders,
        history_orders=history_orders,
        trades=trades,
        portfolio=portfolio,
        private=private,
        observed_at=observed_at,
    )


def _retime_observation(observation, observed_at: int, *, rest_round: int):
    book = replace(observation.market.book, observed_at=observed_at)
    market = replace(observation.market, observed_at=observed_at, book=book)
    accounts = {
        role: _retime_account(account, observed_at)
        for role, account in observation.accounts.items()
    }
    return replace(observation, market=market, accounts=accounts, rest_round=rest_round)


def _lifecycle_fixture(tmp_path: Path, *, gate=None, settlement_at_ms=SETTLEMENT_AT_MS):
    observations, rounds = lifecycle_observations()
    original = LifecycleVenue(observations, rounds)
    observations = list(observations[:3]) + [observations[2]] + list(observations[3:])
    venue = LifecycleVenue(
        observations,
        rounds,
        accepted_order_ids=original.accepted_order_ids,
    )
    lifecycle = RisexFundingBoundaryCoordinator._fixture(
        venue=venue,
        primary_journal=tmp_path / "primary.sqlite3",
        counterparty_journal=tmp_path / "counterparty.sqlite3",
        now=lambda: NOW_FOR_TERMINAL,
        identity_factory=identity_factory,
    )
    route = fixed_funding_route(settlement_at_ms)
    boundary = FundingBoundaryBinding(
        route=route,
        risex_journal=lifecycle._journal_identity(coordinator_module.AccountRole.PRIMARY),
        nado_journal=JournalIdentity(
            NADO_VENUE, "nado-run-1", "nado-store-1", "nado-account-1",
        ),
    )
    lifecycle.bind_funding_boundary(boundary)
    lifecycle._hold_release_gate = gate
    return lifecycle, boundary, venue, observations, rounds


def _close_fixture(lifecycle):
    asyncio.run(lifecycle.close())
    for journal in lifecycle._journals.values():
        journal.close()


def _owner_bridge_fixture(tmp_path: Path, *, gate, rest_rounds):
    holder = {}

    def factory():
        lifecycle, boundary, venue, observations, rounds = _lifecycle_fixture(
            tmp_path, gate=gate,
        )
        venue.rest_rounds = list(rest_rounds)
        holder.update(
            lifecycle=lifecycle,
            boundary=boundary,
            venue=venue,
            observations=observations,
            rounds=rounds,
        )
        return lifecycle

    bridge = RisexCoordinationLoopBridge(
        factory,
        startup_timeout_seconds=2,
        callback_timeout_seconds=2,
        lifecycle_timeout_seconds=2,
        shutdown_timeout_seconds=2,
    )
    bridge.start()
    bridge.bind_funding_boundary(holder["boundary"])
    return bridge, holder


def _released_gate(observed_at_ms=SETTLEMENT_AT_MS):
    def gate(binding):
        return HoldReleaseSignal.released(
            binding,
            observed_at_ms=observed_at_ms,
            signal_id="release-1",
        )
    return gate


def test_fixed_route_is_exact_eth_two_venue_opposite_direction_contract():
    route = fixed_funding_route(SETTLEMENT_AT_MS)
    assert route.canonical_asset == "ETH"
    assert route.risex_leg.market == TARGET_RISEX_MARKET == "ETH/USDC"
    assert route.risex_leg.direction == "LONG"
    assert route.nado_leg.market == TARGET_NADO_MARKET == "ETH-PERP_USDT0"
    assert route.nado_leg.direction == "SHORT"
    assert route.nado_product_id == TARGET_NADO_PRODUCT_ID == 4
    assert route.canonical_quantity == TARGET_QUANTITY == Decimal("0.1")


def test_historical_zero_argument_builder_and_fresh_v4_names_are_unchanged():
    assert not inspect.signature(
        coordinator_module.build_risex_two_account_coordinator
    ).parameters
    assert not inspect.signature(
        coordinator_module.run_risex_two_account_coordinator
    ).parameters
    assert boundary_module.FUNDING_BOUNDARY_PRIMARY_JOURNAL != coordinator_module.PRIMARY_JOURNAL
    assert boundary_module.FUNDING_BOUNDARY_COUNTERPARTY_JOURNAL != coordinator_module.COUNTERPARTY_JOURNAL
    assert coordinator_module.PRIMARY_JOURNAL not in {
        FUNDING_BOUNDARY_PRIMARY_JOURNAL,
        FUNDING_BOUNDARY_COUNTERPARTY_JOURNAL,
    }
    assert FUNDING_BOUNDARY_PRIMARY_JOURNAL == (
        ".risex-funding-farmer-risex-nado-boundary-primary-v4.sqlite3"
    )
    assert FUNDING_BOUNDARY_COUNTERPARTY_JOURNAL == (
        ".risex-funding-farmer-risex-nado-boundary-counterparty-v4.sqlite3"
    )
    assert boundary_module.FUNDING_BOUNDARY_PRIMARY_STORE_IDENTITY == (
        "risex-nado-boundary-primary-v4"
    )
    assert boundary_module.FUNDING_BOUNDARY_COUNTERPARTY_STORE_IDENTITY == (
        "risex-nado-boundary-counterparty-v4"
    )
    assert FUNDING_BOUNDARY_PRIMARY_JOURNAL != FUNDING_BOUNDARY_COUNTERPARTY_JOURNAL
    assert (
        boundary_module.FUNDING_BOUNDARY_PRIMARY_STORE_IDENTITY
        != boundary_module.FUNDING_BOUNDARY_COUNTERPARTY_STORE_IDENTITY
    )
    assert all(
        all(version not in value for version in ("-v1", "-v2", "-v3"))
        for value in (
            FUNDING_BOUNDARY_PRIMARY_JOURNAL,
            FUNDING_BOUNDARY_COUNTERPARTY_JOURNAL,
            boundary_module.FUNDING_BOUNDARY_PRIMARY_STORE_IDENTITY,
            boundary_module.FUNDING_BOUNDARY_COUNTERPARTY_STORE_IDENTITY,
        )
    )


def test_funding_boundary_must_be_bound_before_first_observation(tmp_path: Path):
    observations, rounds = lifecycle_observations()
    venue = LifecycleVenue(observations, rounds)
    lifecycle = RisexFundingBoundaryCoordinator._fixture(
        venue=venue,
        primary_journal=tmp_path / "primary.sqlite3",
        counterparty_journal=tmp_path / "counterparty.sqlite3",
        now=lambda: NOW_FOR_TERMINAL,
        identity_factory=identity_factory,
        hold_release_gate=_released_gate(),
    )
    try:
        report = asyncio.run(lifecycle.run())
        assert report.result is FundingBoundaryResult.BLOCKED
        assert report.coordinator_report.failure_code == (
            "RISEx funding boundary is not bound"
        )
        assert len(venue.observations) == len(observations)
        assert venue.place_calls == []
        assert all(
            not journal.intents() for journal in lifecycle._journals.values()
        )
    finally:
        _close_fixture(lifecycle)


def test_missing_gate_blocks_before_observation_intent_or_place(tmp_path: Path):
    lifecycle, _, venue, _, _ = _lifecycle_fixture(tmp_path, gate=None)
    try:
        report = asyncio.run(lifecycle.run())
        assert report.result is FundingBoundaryResult.BLOCKED
        assert report.funding_blocker == FUNDING_BLOCKER_BOUNDARY_GATE_MISSING
        assert report.coordinator_report.primary_intents == 0
        assert report.coordinator_report.counterparty_intents == 0
        assert venue.place_calls == []
        assert lifecycle.phase is coordinator_module.Phase.HALTED
    finally:
        _close_fixture(lifecycle)


@pytest.mark.parametrize(
    "field",
    [
        "role",
        "step",
        "side",
        "order_type",
        "time_in_force",
        "reduce_only",
        "post_only",
        "size",
        "price",
        "source_position",
    ],
)
def test_fixed_preparation_mismatch_fails_before_durable_intent(
    tmp_path: Path, field: str,
):
    lifecycle, _, venue, observations, _ = _lifecycle_fixture(
        tmp_path, gate=_released_gate(),
    )
    try:
        values = {
            "role": coordinator_module.AccountRole.COUNTERPARTY,
            "observation": observations[0],
            "step": "ENTRY_MAKER",
            "side": "SELL",
            "order_type": "LIMIT",
            "time_in_force": "GTC",
            "reduce_only": False,
            "post_only": True,
            "size": TARGET_QUANTITY,
            "price": Decimal("2999.01"),
            "source_position": Decimal("0"),
        }
        values.update({
            "role": coordinator_module.AccountRole.PRIMARY
            if field == "role" else values["role"],
            "step": "EXIT_MAKER" if field == "step" else values["step"],
            "side": "BUY" if field == "side" else values["side"],
            "order_type": "MARKET" if field == "order_type" else values["order_type"],
            "time_in_force": "IOC" if field == "time_in_force" else values["time_in_force"],
            "reduce_only": True if field == "reduce_only" else values["reduce_only"],
            "post_only": False if field == "post_only" else values["post_only"],
            "size": Decimal("0.101") if field == "size" else values["size"],
            "price": Decimal("2999.015") if field == "price" else values["price"],
            "source_position": Decimal("-0.1")
            if field == "source_position" else values["source_position"],
        })
        with pytest.raises(RisexFundingBoundaryError):
            lifecycle._prepare(
                values.pop("role"),
                values.pop("observation"),
                **values,
            )
        assert lifecycle._journals[coordinator_module.AccountRole.PRIMARY].intents() == ()
        assert lifecycle._journals[coordinator_module.AccountRole.COUNTERPARTY].intents() == ()
        assert venue.place_calls == []
    finally:
        _close_fixture(lifecycle)


def test_valid_gate_reconciles_after_release_then_flattens_and_blocks_missing_contract(
    tmp_path: Path,
):
    gate_calls = []

    def gate(binding):
        gate_calls.append(binding)
        return HoldReleaseSignal.released(
            binding, observed_at_ms=SETTLEMENT_AT_MS, signal_id="release-1",
        )

    lifecycle, _, venue, _, _ = _lifecycle_fixture(tmp_path, gate=gate)
    try:
        report = asyncio.run(lifecycle.run())
        assert report.result is FundingBoundaryResult.BLOCKED
        assert report.funding_status == "UNRESOLVED"
        assert report.funding_blocker == FUNDING_BLOCKER_MISSING_CONTRACT
        assert len(gate_calls) == 1
        assert [role for role, _ in venue.place_calls] == [
            coordinator_module.AccountRole.COUNTERPARTY,
            coordinator_module.AccountRole.PRIMARY,
            coordinator_module.AccountRole.COUNTERPARTY,
            coordinator_module.AccountRole.PRIMARY,
        ]
        assert lifecycle._journals[coordinator_module.AccountRole.PRIMARY].terminal(
            "funding_entry:reconciled_at_ms"
        ) == str(NOW * 1_000)
        assert lifecycle.phase is coordinator_module.Phase.COMPLETE
    finally:
        _close_fixture(lifecycle)


@pytest.mark.parametrize(
    ("gate", "expected"),
    [
        (lambda binding: None, FUNDING_BLOCKER_EVIDENCE_MISSING),
        (
            lambda binding: HoldReleaseSignal.released(
                binding, observed_at_ms=NOW * 1_000, signal_id="early-release",
            ),
            FUNDING_BLOCKER_EVIDENCE_STALE,
        ),
        (
            lambda binding: HoldReleaseSignal.cancelled(
                binding, observed_at_ms=SETTLEMENT_AT_MS, signal_id="cancelled",
            ),
            boundary_module.FUNDING_BLOCKER_CANCELLED,
        ),
    ],
)
def test_missing_stale_cancelled_gate_evidence_flattens_and_durably_blocks(
    tmp_path: Path, gate, expected: str,
):
    lifecycle, _, venue, _, _ = _lifecycle_fixture(tmp_path, gate=gate)
    try:
        report = asyncio.run(lifecycle.run())
        assert report.result is FundingBoundaryResult.BLOCKED
        assert report.funding_blocker == expected
        assert len(venue.place_calls) == 4
        assert lifecycle.phase is coordinator_module.Phase.COMPLETE
    finally:
        _close_fixture(lifecycle)


@pytest.mark.parametrize(
    "claim",
    [
        SimpleNamespace(status="AUTHORITATIVE"),
        SimpleNamespace(cash=Decimal("1")),
        SimpleNamespace(rate=Decimal("0.1")),
        SimpleNamespace(source="engine"),
    ],
)
def test_injected_authoritative_funding_claim_is_rejected_and_flattened(
    tmp_path: Path, claim,
):
    lifecycle, _, venue, _, _ = _lifecycle_fixture(
        tmp_path, gate=lambda _binding: claim,
    )
    try:
        report = asyncio.run(lifecycle.run())
        assert report.result is FundingBoundaryResult.BLOCKED
        assert report.funding_blocker == FUNDING_BLOCKER_AUTHORITATIVE_INJECTION
        assert len(venue.place_calls) == 4
        assert lifecycle.phase is coordinator_module.Phase.COMPLETE
    finally:
        _close_fixture(lifecycle)


def test_callback_cancelled_error_is_durable_and_flattens(tmp_path: Path):
    def gate(_binding):
        raise asyncio.CancelledError

    lifecycle, _, venue, _, _ = _lifecycle_fixture(tmp_path, gate=gate)
    try:
        report = asyncio.run(lifecycle.run())
        assert report.result is FundingBoundaryResult.BLOCKED
        assert report.funding_blocker == FUNDING_BLOCKER_GATE_CALLBACK_CANCELLED
        assert len(venue.place_calls) == 4
    finally:
        _close_fixture(lifecycle)


def test_release_before_entry_boundary_is_blocked_without_callback_and_flattens(
    tmp_path: Path,
):
    lifecycle, _, venue, _, _ = _lifecycle_fixture(
        tmp_path,
        gate=_released_gate(),
        settlement_at_ms=NOW * 1_000,
    )
    try:
        report = asyncio.run(lifecycle.run())
        assert report.result is FundingBoundaryResult.BLOCKED
        assert report.funding_blocker == FUNDING_BLOCKER_ENTRY_AFTER_BOUNDARY
        assert report.gate_invocations == 0
        assert len(venue.place_calls) == 4
    finally:
        _close_fixture(lifecycle)


def test_restart_after_durable_gate_invocation_never_replays_callback(tmp_path: Path):
    lifecycle, boundary, _, observations, _ = _lifecycle_fixture(
        tmp_path, gate=_released_gate(),
    )
    try:
        async def enter_only():
            current = await lifecycle._observe()
            lifecycle._zero_state(current)
            lifecycle._set_phase(coordinator_module.Phase.INITIAL_ZERO)
            current = await lifecycle._entry_maker(current)
            return await lifecycle._entry_taker(current)

        entry = asyncio.run(enter_only())
        lifecycle._record_entry_reconciliation(entry)
        invocation = f"STARTED|{lifecycle._gate_invocation_id()}"
        lifecycle._set_paired_terminal("funding_gate:invocation", invocation)
        for journal in lifecycle._journals.values():
            journal.close()

        observations, rounds = lifecycle_observations()
        restarted_observations = [observations[2], observations[2], *observations[3:]]
        venue = LifecycleVenue(
            restarted_observations,
            rounds,
            accepted_order_ids=(
                observations[3].accounts[
                    coordinator_module.AccountRole.COUNTERPARTY
                ].open_orders[0].order_id,
                observations[4].accounts[
                    coordinator_module.AccountRole.PRIMARY
                ].history_orders[1].order_id,
            ),
        )
        callback_calls = []

        def callback(_binding):
            callback_calls.append(True)
            raise AssertionError("restart must not replay the callback")

        restarted = RisexFundingBoundaryCoordinator._fixture(
            venue=venue,
            primary_journal=tmp_path / "primary.sqlite3",
            counterparty_journal=tmp_path / "counterparty.sqlite3",
            now=lambda: NOW_FOR_TERMINAL,
            identity_factory=identity_factory,
            boundary=boundary,
            hold_release_gate=callback,
        )
        try:
            report = asyncio.run(restarted.run())
            assert report.result is FundingBoundaryResult.BLOCKED
            assert report.funding_blocker == FUNDING_BLOCKER_BOUNDARY_INTERRUPTED
            assert callback_calls == []
            assert [role for role, _ in venue.place_calls] == [
                coordinator_module.AccountRole.COUNTERPARTY,
                coordinator_module.AccountRole.PRIMARY,
            ]
        finally:
            _close_fixture(restarted)
    finally:
        asyncio.run(lifecycle.close())


def test_pair_journal_digest_has_fixed_provider_projection_only(tmp_path: Path):
    identity = coordinator_module.RoleIdentity(
        coordinator_module.AccountRole.PRIMARY,
        coordinator_module.PRIMARY_ACCOUNT,
        coordinator_module.PRIMARY_SIGNER,
        "credential",
        "record",
        "journal.sqlite3",
    )
    journal = coordinator_module.PairJournal(tmp_path / "journal.sqlite3", identity)
    try:
        before = journal.stable_content_digest()
        journal.set_terminal("risex_provider:bookkeeping", "one")
        assert journal.stable_content_digest() == before
        with pytest.raises(TypeError):
            journal.stable_content_digest(exclude_terminal_prefixes=("funding:",))
        for key in (
            "funding_boundary:payload",
            "baseline_history",
            "funding_gate:result",
            "funding:status",
        ):
            journal.set_terminal(key, "critical")
            assert journal.stable_content_digest() != before
            before = journal.stable_content_digest()
    finally:
        journal.close()


def _patched_production_identities(monkeypatch):
    primary = coordinator_module.RoleIdentity(
        coordinator_module.AccountRole.PRIMARY,
        coordinator_module.PRIMARY_ACCOUNT,
        coordinator_module.PRIMARY_SIGNER,
        "credential",
        "record",
        coordinator_module.PRIMARY_JOURNAL,
    )
    counterparty = coordinator_module.RoleIdentity(
        coordinator_module.AccountRole.COUNTERPARTY,
        coordinator_module.COUNTERPARTY_ACCOUNT,
        coordinator_module.COUNTERPARTY_SIGNER,
        "counterparty-session",
        "counterparty-marker",
        coordinator_module.COUNTERPARTY_JOURNAL,
    )
    monkeypatch.setattr(
        coordinator_module,
        "_load_primary_identity",
        lambda: (primary, lambda: None),
    )
    monkeypatch.setattr(
        coordinator_module,
        "_load_counterparty_identity",
        lambda: (counterparty, lambda: None),
    )
    return primary, counterparty


def test_actual_fresh_builder_path_reserves_and_fails_closed_on_existing_second_file(
    tmp_path: Path, monkeypatch,
):
    _patched_production_identities(monkeypatch)
    primary_path = tmp_path / "primary.sqlite3"
    counterparty_path = tmp_path / "counterparty.sqlite3"
    identity = coordinator_module.RoleIdentity(
        coordinator_module.AccountRole.COUNTERPARTY,
        coordinator_module.COUNTERPARTY_ACCOUNT,
        coordinator_module.COUNTERPARTY_SIGNER,
        "counterparty-session",
        "counterparty-marker",
        coordinator_module.COUNTERPARTY_JOURNAL,
    )
    existing = coordinator_module.PairJournal(counterparty_path, identity)
    existing.close()

    async def attempt():
        return await coordinator_module._build_risex_two_account_coordinator_at_paths(
            primary_path=primary_path,
            counterparty_path=counterparty_path,
            require_fresh=True,
        )

    with pytest.raises(coordinator_module.CoordinatorSafetyError):
        asyncio.run(attempt())
    assert primary_path.exists()
    assert counterparty_path.exists()
    with pytest.raises(coordinator_module.CoordinatorSafetyError):
        asyncio.run(attempt())


def test_actual_fresh_builder_path_has_one_winner_under_concurrent_reservation(
    tmp_path: Path, monkeypatch,
):
    _patched_production_identities(monkeypatch)
    primary_path = tmp_path / "primary.sqlite3"
    counterparty_path = tmp_path / "counterparty.sqlite3"

    async def attempt():
        try:
            built = await coordinator_module._build_risex_two_account_coordinator_at_paths(
                primary_path=primary_path,
                counterparty_path=counterparty_path,
                require_fresh=True,
            )
        except coordinator_module.CoordinatorSafetyError:
            return "failed"
        try:
            await built._venue.close()
            for journal in built._journals.values():
                journal.close()
            return "won"
        except Exception:
            return "failed"

    def worker():
        return asyncio.run(attempt())

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(lambda _: worker(), range(2)))
    assert sorted(outcomes) == ["failed", "won"]
    assert primary_path.exists() and counterparty_path.exists()


def test_new_production_builder_uses_only_new_journals(tmp_path: Path, monkeypatch):
    primary, counterparty = _patched_production_identities(monkeypatch)
    monkeypatch.setattr(
        boundary_module.pwd,
        "getpwuid",
        lambda _uid: SimpleNamespace(pw_dir=str(tmp_path)),
    )
    historical_primary_path = tmp_path / coordinator_module.PRIMARY_JOURNAL
    historical_counterparty_path = tmp_path / coordinator_module.COUNTERPARTY_JOURNAL
    historical_primary = coordinator_module.PairJournal(
        historical_primary_path, primary,
    )
    historical_counterparty = coordinator_module.PairJournal(
        historical_counterparty_path, counterparty,
    )
    historical_digests = (
        historical_primary.stable_content_digest(),
        historical_counterparty.stable_content_digest(),
    )
    historical_primary.close()
    historical_counterparty.close()

    async def build_and_close():
        built = await boundary_module._build_fresh_production_coordinator(None)
        await built.close()
        for journal in built._journals.values():
            journal.close()

    asyncio.run(build_and_close())
    assert (tmp_path / FUNDING_BOUNDARY_PRIMARY_JOURNAL).exists()
    assert (tmp_path / FUNDING_BOUNDARY_COUNTERPARTY_JOURNAL).exists()
    assert historical_primary_path.exists()
    assert historical_counterparty_path.exists()
    reopened_primary = coordinator_module.PairJournal(
        historical_primary_path, primary,
    )
    reopened_counterparty = coordinator_module.PairJournal(
        historical_counterparty_path, counterparty,
    )
    try:
        assert (
            reopened_primary.stable_content_digest(),
            reopened_counterparty.stable_content_digest(),
        ) == historical_digests
    finally:
        reopened_primary.close()
        reopened_counterparty.close()


def test_real_callback_shape_uses_evidence_timestamp_and_stable_content_token():
    first = LiveObservation(CallbackEvidence(NOW_FOR_TERMINAL * 1_000, "s1", 1))
    second = LiveObservation(CallbackEvidence(NOW_FOR_TERMINAL * 1_000, "s1", 1))
    first_timestamp, first_token = boundary_module._callback_reference(first)
    second_timestamp, second_token = boundary_module._callback_reference(second)
    assert first_timestamp == second_timestamp
    assert first_token == second_token
    mutable = MutableLiveObservation(
        MutableCallbackEvidence(NOW_FOR_TERMINAL * 1_000, "s1", 1),
    )
    with pytest.raises(RisexFundingBoundaryError):
        boundary_module._callback_reference(mutable)
    with pytest.raises(RisexFundingBoundaryError):
        boundary_module._callback_reference(SimpleNamespace(observed_at_ms=1))


def test_provider_binds_fresh_rounds_to_real_callback_references_and_rejects_replay(
    tmp_path: Path,
):
    _, rounds = lifecycle_observations()
    final_one = _retime_observation(rounds[0], NOW + 1, rest_round=1)
    final_two = _retime_observation(rounds[1], NOW + 1, rest_round=2)
    terminal_one = _retime_observation(rounds[0], NOW + 2, rest_round=3)
    terminal_two = _retime_observation(rounds[1], NOW + 3, rest_round=4)
    bridge, holder = _owner_bridge_fixture(
        tmp_path,
        gate=_released_gate(),
        rest_rounds=[final_one, final_two, terminal_one, terminal_two],
    )
    boundary = holder["boundary"]
    venue = holder["venue"]
    try:
        report = bridge.run_lifecycle(timeout_seconds=2)
        assert report.phase is coordinator_module.Phase.COMPLETE
        digest_before = bridge.call(lambda coordinator: coordinator._combined_journal_digest())
        provider = RisexTerminalEvidenceProvider(bridge)
        try:
            reference_one = LiveObservation(
                CallbackEvidence((NOW + 2) * 1_000, "nado-1", 1),
            )
            reference_two = LiveObservation(
                CallbackEvidence((NOW + 3) * 1_000, "nado-2", 2),
            )
            first = provider(boundary, reference_one, 1)
            assert type(first) is RisexTerminalEvidence
            assert first.terminal.observed_at_ms == (NOW + 2) * 1_000
            assert first.terminal.zero_regular_orders is True
            assert first.terminal.zero_trigger_orders is True
            assert first.terminal.exact_flat is True
            assert first.terminal.unresolved_write_identities == ()
            assert bridge.call(
                lambda coordinator: coordinator._combined_journal_digest()
            ) == digest_before
            with pytest.raises(RisexFundingBoundaryError):
                provider(boundary, reference_one, 1)
            with pytest.raises(RisexFundingBoundaryError):
                provider(boundary, reference_one, 2)
            second = provider(boundary, reference_two, 2)
            assert type(second) is RisexTerminalEvidence
            assert second.terminal.observed_at_ms > first.terminal.observed_at_ms
            assert bridge.call(
                lambda coordinator: coordinator._combined_journal_digest()
            ) == digest_before
            with pytest.raises(RisexFundingBoundaryError):
                provider(boundary, reference_two, 2)
        finally:
            bridge.close()
    finally:
        if bridge.lifecycle_status() != "FINISHED":
            bridge.close()


def test_provider_rejects_disagreeing_fresh_terminal_market_rounds(tmp_path: Path):
    _, rounds = lifecycle_observations()
    final_one = _retime_observation(rounds[0], NOW + 1, rest_round=1)
    final_two = _retime_observation(rounds[1], NOW + 1, rest_round=2)
    terminal_one = _retime_observation(rounds[0], NOW + 2, rest_round=3)
    terminal_two = _retime_observation(rounds[1], NOW + 3, rest_round=4)
    changed_book = replace(
        terminal_two.market.book,
        bid=Decimal("2998.99"),
        bids=(replace(terminal_two.market.book.bids[0], price=Decimal("2998.99")),),
    )
    terminal_two = replace(
        terminal_two,
        market=replace(terminal_two.market, book=changed_book),
    )
    bridge, holder = _owner_bridge_fixture(
        tmp_path,
        gate=_released_gate(),
        rest_rounds=[final_one, final_two, terminal_one, terminal_two],
    )
    boundary = holder["boundary"]
    try:
        assert bridge.run_lifecycle(timeout_seconds=2).phase is coordinator_module.Phase.COMPLETE
        provider = RisexTerminalEvidenceProvider(bridge)
        provider(
            boundary,
            LiveObservation(CallbackEvidence((NOW + 2) * 1_000, "nado-1", 1)),
            1,
        )
        with pytest.raises(RisexFundingBoundaryError):
            provider(
                boundary,
                LiveObservation(CallbackEvidence((NOW + 3) * 1_000, "nado-2", 2)),
                2,
            )
    finally:
        bridge.close()


@pytest.mark.parametrize(
    "field",
    [
        "route",
        "risex_run",
        "risex_store",
        "risex_account",
        "nado_run",
        "nado_store",
        "nado_account",
    ],
)
def test_provider_rejects_route_and_every_journal_identity_mismatch(
    tmp_path: Path, field: str,
):
    _, rounds = lifecycle_observations()
    final_one = _retime_observation(rounds[0], NOW + 1, rest_round=1)
    final_two = _retime_observation(rounds[1], NOW + 1, rest_round=2)
    provider_round = _retime_observation(rounds[0], NOW + 2, rest_round=3)
    bridge, holder = _owner_bridge_fixture(
        tmp_path,
        gate=_released_gate(),
        rest_rounds=[final_one, final_two, provider_round],
    )
    boundary = holder["boundary"]
    venue = holder["venue"]
    try:
        bridge.run_lifecycle(timeout_seconds=2)
        provider = RisexTerminalEvidenceProvider(bridge)
        try:
            if field == "route":
                bad = replace(boundary, route=replace(boundary.route, nado_product_id=3))
            elif field == "risex_run":
                bad = replace(
                    boundary,
                    risex_journal=replace(boundary.risex_journal, run_id="other-run"),
                )
            elif field == "risex_store":
                bad = replace(
                    boundary,
                    risex_journal=replace(
                        boundary.risex_journal, store_identity="other-store",
                    ),
                )
            elif field == "risex_account":
                bad = replace(
                    boundary,
                    risex_journal=replace(
                        boundary.risex_journal,
                        account_id="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    ),
                )
            elif field == "nado_run":
                bad = replace(
                    boundary,
                    nado_journal=replace(boundary.nado_journal, run_id="other-run"),
                )
            elif field == "nado_store":
                bad = replace(
                    boundary,
                    nado_journal=replace(
                        boundary.nado_journal, store_identity="other-store",
                    ),
                )
            else:
                bad = replace(
                    boundary,
                    nado_journal=replace(
                        boundary.nado_journal, account_id="other-account",
                    ),
                )
            with pytest.raises(RisexFundingBoundaryError):
                provider(
                    bad,
                    LiveObservation(CallbackEvidence((NOW + 2) * 1_000, "nado-1", 1)),
                    1,
                )
            assert len(venue.rest_rounds) == 1
        finally:
            bridge.close()
    finally:
        if bridge.lifecycle_status() != "FINISHED":
            bridge.close()


def test_provider_rejects_stale_reference_and_uses_oldest_required_timestamp(
    tmp_path: Path,
):
    _, rounds = lifecycle_observations()
    final_one = _retime_observation(rounds[0], NOW + 1, rest_round=1)
    final_two = _retime_observation(rounds[1], NOW + 1, rest_round=2)
    terminal = _retime_observation(rounds[0], NOW + 2, rest_round=3)
    primary = terminal.accounts[coordinator_module.AccountRole.PRIMARY]
    terminal = replace(
        terminal,
        accounts={
            **terminal.accounts,
            coordinator_module.AccountRole.PRIMARY: replace(
                primary,
                portfolio=replace(primary.portfolio, observed_at=NOW + 1),
            ),
        },
    )
    bridge, holder = _owner_bridge_fixture(
        tmp_path,
        gate=_released_gate(),
        rest_rounds=[final_one, final_two, terminal],
    )
    boundary = holder["boundary"]
    venue = holder["venue"]
    try:
        bridge.run_lifecycle(timeout_seconds=2)
        provider = RisexTerminalEvidenceProvider(bridge)
        try:
            with pytest.raises(RisexFundingBoundaryError):
                provider(
                    boundary,
                    LiveObservation(CallbackEvidence((NOW - 3) * 1_000, "old", 1)),
                    1,
                )
            assert len(venue.rest_rounds) == 1
            first = provider(
                boundary,
                LiveObservation(CallbackEvidence((NOW + 2) * 1_000, "nado-1", 1)),
                1,
            )
            assert first.terminal.observed_at_ms == (NOW + 1) * 1_000
        finally:
            bridge.close()
    finally:
        if bridge.lifecycle_status() != "FINISHED":
            bridge.close()


def test_provider_rejects_mutable_or_missing_reference_before_rest_read(tmp_path: Path):
    _, rounds = lifecycle_observations()
    final_one = _retime_observation(rounds[0], NOW + 1, rest_round=1)
    final_two = _retime_observation(rounds[1], NOW + 1, rest_round=2)
    terminal = _retime_observation(rounds[0], NOW + 2, rest_round=3)
    bridge, holder = _owner_bridge_fixture(
        tmp_path,
        gate=_released_gate(),
        rest_rounds=[final_one, final_two, terminal],
    )
    boundary = holder["boundary"]
    venue = holder["venue"]
    try:
        bridge.run_lifecycle(timeout_seconds=2)
        provider = RisexTerminalEvidenceProvider(bridge)
        try:
            for reference in (
                None,
                SimpleNamespace(observed_at_ms=(NOW + 2) * 1_000),
                MutableLiveObservation(
                    MutableCallbackEvidence((NOW + 2) * 1_000, "mutable", 1),
                ),
            ):
                with pytest.raises(RisexFundingBoundaryError):
                    provider(boundary, reference, 1)
            assert len(venue.rest_rounds) == 1
        finally:
            bridge.close()
    finally:
        if bridge.lifecycle_status() != "FINISHED":
            bridge.close()


def test_provider_rejects_terminal_cancel_without_exactly_one_dispatch(
    tmp_path: Path,
):
    lifecycle, boundary, venue, _, rounds = _lifecycle_fixture(
        tmp_path, gate=_released_gate(),
    )
    try:
        report = asyncio.run(lifecycle.run())
        assert report.phase is coordinator_module.Phase.COMPLETE
        journal = lifecycle._journals[coordinator_module.AccountRole.COUNTERPARTY]
        intent = journal.by_step("EXIT_MAKER")
        assert intent is not None
        cancel = journal.prepare_cancel(
            intent,
            nonce_anchor=20,
            nonce_bitmap=2,
            expires_at=NOW + 60,
        )
        with journal._db:
            journal._db.execute(
                "UPDATE cancels SET state='TERMINAL', dispatch_count=0 WHERE cancel_id=?",
                (cancel.cancel_id,),
            )
        assert journal.cancels()[0].state == "TERMINAL"
        assert journal.cancels()[0].dispatch_count == 0
        venue.rest_rounds = [_retime_observation(rounds[0], NOW + 2, rest_round=3)]
        with pytest.raises(RisexFundingBoundaryError):
            asyncio.run(
                lifecycle.terminal_evidence(
                    boundary,
                    1,
                    LiveObservation(
                        CallbackEvidence((NOW + 2) * 1_000, "nado-1", 1),
                    ),
                )
            )
    finally:
        _close_fixture(lifecycle)


def test_lifecycle_timeout_does_not_cancel_owner_task_and_later_retrieves_result(
    tmp_path: Path,
):
    released = threading.Event()

    def gate(binding):
        assert released.wait(2)
        return HoldReleaseSignal.released(
            binding, observed_at_ms=SETTLEMENT_AT_MS, signal_id="release-after-wait",
        )

    _, rounds = lifecycle_observations()
    bridge, holder = _owner_bridge_fixture(
        tmp_path, gate=gate, rest_rounds=rounds,
    )
    boundary = holder["boundary"]
    try:
        with pytest.raises(RisexFundingBoundaryError):
            bridge.run_lifecycle(timeout_seconds=0.05)
        assert bridge.lifecycle_status() == "RUNNING"
        released.set()
        report = bridge.run_lifecycle(timeout_seconds=5)
        assert type(report) is boundary_module.FundingBoundaryReport
        assert report.phase is coordinator_module.Phase.COMPLETE
        assert bridge.lifecycle_status() == "FINISHED"
    finally:
        if bridge.lifecycle_status() in {"FINISHED", "FAILED", "CANCELLED"}:
            bridge.close()
        else:
            released.set()
            bridge.run_lifecycle(timeout_seconds=5)
            bridge.close()
