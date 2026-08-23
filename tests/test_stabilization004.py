import asyncio
from dataclasses import fields, replace
from datetime import timedelta
from decimal import Decimal
import sqlite3

import pytest

from risex_farmer.config import PAPER_CONFIG
from risex_farmer.lifecycle import CloseReason, LifecycleEngine
from risex_farmer.lifecycle import ExitOrderVersion
from risex_farmer.models import (
    BookExecutionCapture,
    BookLevel,
    DataQuality,
    LifecycleState,
    MarketVolume,
    OrderBook,
    RouteDirection,
    Side,
    StreamHealth,
    TradeEvidence,
    Venue,
)
from risex_farmer.orchestrator import run_fixture
from risex_farmer.paper_broker import _taker_provenance
from risex_farmer.paper_broker import PaperOrderVersion
from risex_farmer.runtime import PublicPaperRuntime
from risex_farmer.scanner import MarketObservation
from risex_farmer.storage import PaperRepository, _load

from test_runtime import (
    FakeClock,
    NOW,
    activate_with_live_streams,
    adapters,
    maker_trade,
    _stabilization002_position_runtime,
)


D = Decimal


def _capture(
    *,
    at=NOW,
    observed_at=NOW,
    received_at=NOW,
    healthy=True,
    confirmation_at=None,
    venue=Venue.RISEX,
    market="ABC-RISEX",
    bid_depth="5",
    ask_depth="5",
    session=7,
    generation=11,
    revision=13,
):
    book = OrderBook(
        venue,
        market,
        (BookLevel(D("99"), D(bid_depth)),),
        (BookLevel(D("101"), D(ask_depth)),),
        observed_at,
        17,
    )
    health = StreamHealth(
        observed_at,
        at if confirmation_at is None else confirmation_at,
        healthy,
        healthy,
        healthy,
        DataQuality.COMPLETE if healthy else DataQuality.DEGRADED,
    )
    return BookExecutionCapture(
        book, health, received_at, at, session, generation, revision, 123
    )


@pytest.mark.parametrize(
    "capture",
    [
        _capture(observed_at=NOW + timedelta(microseconds=1)),
        _capture(received_at=NOW + timedelta(microseconds=1)),
        _capture(healthy=False),
        _capture(confirmation_at=NOW - timedelta(seconds=26)),
        _capture(venue=Venue.EXTENDED),
        _capture(market="WRONG"),
        _capture(ask_depth="4"),
    ],
    ids=("future-observation", "future-receipt", "unhealthy", "stale",
         "wrong-venue", "wrong-market", "insufficient-depth"),
)
def test_taker_capture_fails_closed_for_invalid_causal_inputs(capture):
    assert _taker_provenance(
        capture,
        Side.BUY,
        D("5"),
        venue=Venue.RISEX,
        canonical_market="ABC-RISEX",
        config=PAPER_CONFIG,
    ) is None


def test_healthy_quiet_book_retains_existing_no_book_event_ttl_semantics():
    capture = _capture(
        observed_at=NOW - timedelta(seconds=100),
        received_at=NOW - timedelta(seconds=100),
        confirmation_at=NOW,
    )
    proof = _taker_provenance(
        capture, Side.BUY, D("5"), venue=Venue.RISEX,
        canonical_market="ABC-RISEX", config=PAPER_CONFIG,
    )
    assert proof is not None and proof.vwap_price == D("101")


def test_legacy_order_version_pickle_contract_is_unchanged():
    assert "qualifying_trades" not in {
        field.name for field in fields(PaperOrderVersion)
    }
    assert "qualifying_trades" not in {
        field.name for field in fields(ExitOrderVersion)
    }


def test_runtime_rejects_obsolete_session_generation_and_revision(tmp_path):
    with PaperRepository(tmp_path / "identity.db") as repository:
        runtime = PublicPaperRuntime(repository, adapters={})
        key = (Venue.RISEX, "ABC-RISEX")
        capture = _capture(session=0, generation=0, revision=0)
        assert runtime._captures_are_current(capture)
        runtime._book_revisions[key] = 1
        assert not runtime._captures_are_current(capture)
        runtime._book_revisions[key] = 0
        runtime._book_recovery_generations[key] = 1
        assert not runtime._captures_are_current(capture)
        runtime._book_recovery_generations[key] = 0
        runtime._new_stream_session((Venue.RISEX, "*", "combined"))
        assert not runtime._captures_are_current(capture)


@pytest.mark.asyncio
async def test_post_capture_book_replacement_cannot_open_position(tmp_path):
    clock = FakeClock()
    with PaperRepository(tmp_path / "replacement.db") as repository:
        runtime = PublicPaperRuntime(
            repository,
            adapters=adapters(clock, settlement_at=NOW + timedelta(seconds=120)),
            clock=clock,
        )
        await activate_with_live_streams(runtime, clock)
        broker = runtime.broker
        before = broker.state
        order = before.order
        runtime.mark_trade_stream_connected(
            order.venue, order.canonical_market, at=clock.now()
        )
        entered = asyncio.Event()
        release = asyncio.Event()
        original = runtime._recompute_funding

        async def gated(*args, **kwargs):
            entered.set()
            await release.wait()
            return await original(*args, **kwargs)

        runtime._recompute_funding = gated
        delivery = asyncio.create_task(
            runtime.deliver_trade(maker_trade(runtime, clock.now(), "replacement"))
        )
        await entered.wait()
        plan = order.route_plan
        runtime._bump_book_revision(
            (Venue.RISEX, plan.risex_market.venue_symbol)
        )
        release.set()
        await delivery

        assert runtime.broker is broker
        assert broker.state == before
        assert runtime.lifecycle is None
        for table in ("fills", "fill_provenance", "positions",
                      "processed_trade_events"):
            assert repository.connection.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0] == 0


@pytest.mark.asyncio
async def test_detached_entry_candidates_preserve_all_cumulative_maker_evidence(tmp_path):
    clock = FakeClock()
    with PaperRepository(tmp_path / "cumulative-maker.db") as repository:
        runtime = PublicPaperRuntime(
            repository,
            adapters=adapters(clock, settlement_at=NOW + timedelta(seconds=120)),
            clock=clock,
        )
        await activate_with_live_streams(runtime, clock)
        order = runtime.broker.state.order
        runtime.mark_trade_stream_connected(
            order.venue, order.canonical_market, at=clock.now()
        )
        first = replace(
            maker_trade(runtime, clock.now(), "maker-part-1"),
            canonical_quantity=D("2"),
        )
        await runtime.deliver_trade(first)
        assert runtime.broker.state.order.active_version.cumulative_eligible_quantity == D("2")
        second = replace(
            maker_trade(runtime, clock.now(), "maker-part-2"),
            canonical_quantity=D("3"),
        )
        await runtime.deliver_trade(second)
        proof = _load(repository.connection.execute(
            "SELECT payload FROM fill_provenance WHERE provenance_kind='MAKER'"
        ).fetchone()["payload"])

    assert [trade.trade_event_key for trade in proof.qualifying_trades] == [
        "maker-part-1", "maker-part-2"
    ]


@pytest.mark.asyncio
async def test_mid_transaction_provenance_failure_rolls_back_entry_authority(tmp_path):
    clock = FakeClock()
    with PaperRepository(tmp_path / "atomic-entry.db") as repository:
        runtime = PublicPaperRuntime(
            repository,
            adapters=adapters(clock, settlement_at=NOW + timedelta(seconds=120)),
            clock=clock,
        )
        await activate_with_live_streams(runtime, clock)
        broker = runtime.broker
        before = broker.state
        order = before.order
        runtime.mark_trade_stream_connected(
            order.venue, order.canonical_market, at=clock.now()
        )
        repository.connection.execute(
            "CREATE TRIGGER reject_provenance BEFORE INSERT ON fill_provenance "
            "BEGIN SELECT RAISE(ABORT, 'synthetic provenance failure'); END"
        )

        with pytest.raises(sqlite3.IntegrityError):
            await runtime.deliver_trade(
                maker_trade(runtime, clock.now(), "atomic-entry")
            )

        assert runtime.broker is broker and broker.state == before
        assert runtime.lifecycle is None
        for table in ("fills", "fill_provenance", "positions",
                      "completed_trades", "processed_trade_events"):
            assert repository.connection.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0] == 0


@pytest.mark.asyncio
async def test_repository_rejects_new_orphan_fills_when_caller_omits_proofs(tmp_path):
    with PaperRepository(tmp_path / "orphan.db") as repository:
        original_save = repository.save_decision

        def omit_proofs(**kwargs):
            if kwargs.get("fill_provenance"):
                kwargs["fill_provenance"] = ()
            return original_save(**kwargs)

        repository.save_decision = omit_proofs
        with pytest.raises(ValueError, match="every new fill requires"):
            await run_fixture({"scenario": "open_position"}, repository)

        for table in ("fills", "fill_provenance", "positions",
                      "funding_settlements", "processed_trade_events"):
            assert repository.connection.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0] == 0


@pytest.mark.asyncio
async def test_mid_transaction_cancellation_rolls_back_entry_authority(tmp_path):
    clock = FakeClock()
    with PaperRepository(tmp_path / "cancel-entry.db") as repository:
        runtime = PublicPaperRuntime(
            repository,
            adapters=adapters(clock, settlement_at=NOW + timedelta(seconds=120)),
            clock=clock,
        )
        await activate_with_live_streams(runtime, clock)
        broker = runtime.broker
        before = broker.state
        order = before.order
        runtime.mark_trade_stream_connected(
            order.venue, order.canonical_market, at=clock.now()
        )

        def cancel_provenance(*_args, **_kwargs):
            raise asyncio.CancelledError()

        repository._save_fill_provenance = cancel_provenance
        with pytest.raises(asyncio.CancelledError):
            await runtime.deliver_trade(
                maker_trade(runtime, clock.now(), "cancel-entry")
            )

        assert runtime.broker is broker and broker.state == before
        assert runtime.lifecycle is None
        for table in ("fills", "fill_provenance", "positions",
                      "funding_settlements", "processed_trade_events"):
            assert repository.connection.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0] == 0


@pytest.mark.asyncio
async def test_mid_transaction_provenance_failure_rolls_back_close_authority(tmp_path):
    clock = FakeClock()
    with PaperRepository(tmp_path / "atomic-close.db") as repository:
        runtime = await _stabilization002_position_runtime(repository, clock)
        lifecycle = runtime.lifecycle
        before = lifecycle.snapshot
        order = before.exit_order
        active = order.active_version
        runtime.mark_trade_stream_connected(
            order.venue, order.canonical_market, at=clock.now()
        )
        counts_before = {
            table: repository.connection.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
            for table in (
                "fills", "fill_provenance", "completed_trades",
                "processed_trade_events", "funding_settlements",
            )
        }
        repository.connection.execute(
            "CREATE TRIGGER reject_close_provenance BEFORE INSERT ON fill_provenance "
            "WHEN NEW.fill_id LIKE '%-exit' "
            "BEGIN SELECT RAISE(ABORT, 'synthetic close provenance failure'); END"
        )
        trade = TradeEvidence(
            "atomic-close", order.venue, order.canonical_market,
            NOW, NOW, "atomic-close", order.canonical_quantity,
            active.limit_price - before.hedge_market.tick_size_raw
            if order.side is Side.BUY
            else active.limit_price + before.hedge_market.tick_size_raw,
            Side.SELL if order.side is Side.BUY else Side.BUY,
            True,
        )

        with pytest.raises(sqlite3.IntegrityError):
            await runtime.deliver_trade(trade, processed_at=NOW)

        assert runtime.lifecycle is lifecycle and lifecycle.snapshot == before
        counts_after = {
            table: repository.connection.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
            for table in counts_before
        }
        assert counts_after == counts_before


@pytest.mark.asyncio
async def test_mid_transaction_cancellation_rolls_back_close_authority(tmp_path):
    clock = FakeClock()
    with PaperRepository(tmp_path / "cancel-close.db") as repository:
        runtime = await _stabilization002_position_runtime(repository, clock)
        lifecycle = runtime.lifecycle
        before = lifecycle.snapshot
        order = before.exit_order
        active = order.active_version
        counts_before = {
            table: repository.connection.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
            for table in (
                "fills", "fill_provenance", "completed_trades",
                "processed_trade_events", "funding_settlements",
            )
        }

        def cancel_provenance(*_args, **_kwargs):
            raise asyncio.CancelledError()

        repository._save_fill_provenance = cancel_provenance
        trade = TradeEvidence(
            "cancel-close", order.venue, order.canonical_market,
            NOW, NOW, "cancel-close", order.canonical_quantity,
            active.limit_price - before.hedge_market.tick_size_raw
            if order.side is Side.BUY
            else active.limit_price + before.hedge_market.tick_size_raw,
            Side.SELL if order.side is Side.BUY else Side.BUY,
            True,
        )

        with pytest.raises(asyncio.CancelledError):
            await runtime.deliver_trade(trade, processed_at=NOW)

        assert runtime.lifecycle is lifecycle and lifecycle.snapshot == before
        counts_after = {
            table: repository.connection.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
            for table in counts_before
        }
        assert counts_after == counts_before


def _position_observation(market, at, *, bid, ask):
    book = OrderBook(
        market.venue, market.venue_symbol,
        (BookLevel(D(bid), D("20")),),
        (BookLevel(D(ask), D("20")),), at, 99,
    )
    health = StreamHealth(at, at, True, True, True, DataQuality.COMPLETE)
    return MarketObservation(
        market,
        MarketVolume(market.venue, market.venue_symbol, D("1000"), at, "fixture"),
        book, None, health,
    )


@pytest.mark.asyncio
async def test_hard_basis_close_persists_two_exact_taker_proofs(tmp_path):
    with PaperRepository(tmp_path / "hard-basis.db") as repository:
        await run_fixture({"scenario": "open_position"}, repository)
        before = repository.load_runtime()
        engine = LifecycleEngine.from_snapshot(before)
        position = before.position
        at = position.position_opened_at + timedelta(seconds=2)
        if position.direction is RouteDirection.LONG_RISEX_SHORT_HEDGE:
            risex = _position_observation(before.risex_market, at, bid="50", ask="51")
            hedge = _position_observation(before.hedge_market, at, bid="150", ask="151")
        else:
            risex = _position_observation(before.risex_market, at, bid="150", ask="151")
            hedge = _position_observation(before.hedge_market, at, bid="50", ask="51")
        await engine.evaluate(
            evaluated_at=at,
            risex_observation=risex,
            hedge_observation=hedge,
            risex_capture=BookExecutionCapture(
                risex.book, risex.health, at, at, 1, 0, 2
            ),
            hedge_capture=BookExecutionCapture(
                hedge.book, hedge.health, at, at, 2, 0, 2
            ),
        )
        assert engine.snapshot.closed_trade.close_reason is CloseReason.HARD_BASIS
        repository.save_decision(
            recorded_at=at,
            lifecycle_snapshot=engine.snapshot,
            fill_provenance=engine.fill_provenance,
        )
        exit_rows = repository.connection.execute(
            "SELECT p.payload FROM fill_provenance p JOIN fills f USING(fill_id) "
            "WHERE f.leg IN ('HEDGE_EXIT','RISEX_EXIT') ORDER BY f.leg"
        ).fetchall()
        proofs = tuple(_load(row["payload"]) for row in exit_rows)
        closed = repository.load_runtime().closed_trade

    assert len(proofs) == 2
    assert all(type(proof).__name__ == "TakerFillProvenance" for proof in proofs)
    assert closed.hedge_exit_fill.canonical_quantity == position.canonical_quantity
    assert closed.risex_exit_fill.canonical_quantity == position.canonical_quantity
    assert closed.actual_fees_usd == sum(
        (fill.fee.amount_usd for fill in (
            position.hedge_maker_fill, position.risex_taker_fill,
            closed.hedge_exit_fill, closed.risex_exit_fill,
        )), D("0")
    )


@pytest.mark.asyncio
async def test_normal_close_replay_conserves_fills_fees_funding_and_pnl(tmp_path):
    with PaperRepository(tmp_path / "normal-replay.db") as repository:
        first = await run_fixture({"scenario": "positive_closed"}, repository)
        counts_before = {
            table: repository.connection.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
            for table in (
                "fills", "fill_provenance", "completed_trades",
                "processed_trade_events", "funding_settlements",
            )
        }
        replay_snapshot = repository.load_runtime()
        replay_provenance = tuple(
            (row["fill_id"], _load(row["payload"]))
            for row in repository.connection.execute(
                "SELECT fill_id,payload FROM fill_provenance ORDER BY fill_id"
            )
        )
        replay_trades = tuple(
            _load(row["payload"])
            for row in repository.connection.execute(
                "SELECT payload FROM processed_trade_events "
                "WHERE payload IS NOT NULL ORDER BY trade_event_key"
            )
        )
        repository.save_decision(
            recorded_at=replay_snapshot.closed_trade.closed_at,
            lifecycle_snapshot=replay_snapshot,
            trade_events=replay_trades,
            fill_provenance=replay_provenance,
        )
        counts_after = {
            table: repository.connection.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
            for table in counts_before
        }
        fills = {
            row["leg"]: _load(row["payload"])
            for row in repository.connection.execute(
                "SELECT leg,payload FROM fills"
            )
        }
        position = _load(repository.connection.execute(
            "SELECT payload FROM positions"
        ).fetchone()["payload"])
        closed = _load(repository.connection.execute(
            "SELECT payload FROM completed_trades"
        ).fetchone()["payload"])

    assert first["status"] == "CLOSED"
    assert counts_before == counts_after == {
        "fills": 4,
        "fill_provenance": 4,
        "completed_trades": 1,
        "processed_trade_events": 2,
        "funding_settlements": 2,
    }
    assert {fill.canonical_quantity for fill in fills.values()} == {
        position.canonical_quantity
    }
    q = position.canonical_quantity
    if position.direction is RouteDirection.LONG_RISEX_SHORT_HEDGE:
        pair = (
            q * (fills["RISEX_EXIT"].canonical_price
                 - fills["RISEX_ENTRY"].canonical_price)
            + q * (fills["HEDGE_ENTRY"].canonical_price
                   - fills["HEDGE_EXIT"].canonical_price)
        )
    else:
        pair = (
            q * (fills["HEDGE_EXIT"].canonical_price
                 - fills["HEDGE_ENTRY"].canonical_price)
            + q * (fills["RISEX_ENTRY"].canonical_price
                   - fills["RISEX_EXIT"].canonical_price)
        )
    fees = sum((fill.fee.amount_usd for fill in fills.values()), D("0"))
    assert pair == closed.actual_pair_pnl_usd
    assert fees == closed.actual_fees_usd
    assert closed.simulated_closed_net_pnl_usd == (
        closed.simulated_recognized_funding_usd + pair - fees
    )
