import asyncio
from dataclasses import fields
from datetime import timedelta
from decimal import Decimal
import sqlite3

import pytest

from risex_farmer.models import Fill, Side, TradeEvidence
from risex_farmer.storage import PaperRepository, _load

from test_runtime import (
    FakeClock,
    NOW,
    _stabilization002_position_runtime,
    activate_with_live_streams,
    adapters,
    maker_trade,
)
from risex_farmer.runtime import PublicPaperRuntime


def test_new_fill_provenance_is_an_independently_auditable_relation(tmp_path):
    repository = PaperRepository(tmp_path / "paper.db")
    try:
        tables = {
            row[0]
            for row in repository.connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    finally:
        repository.close()

    assert "fill_provenance" in tables


def _exit_trade(runtime, key: str) -> TradeEvidence:
    snapshot = runtime.lifecycle.snapshot
    order = snapshot.exit_order
    version = order.active_version
    tick = snapshot.hedge_market.tick_size_raw
    return TradeEvidence(
        key,
        order.venue,
        order.canonical_market,
        NOW,
        NOW,
        key,
        order.canonical_quantity,
        (
            version.limit_price - tick
            if order.side is Side.BUY
            else version.limit_price + tick
        ),
        Side.SELL if order.side is Side.BUY else Side.BUY,
        True,
    )


@pytest.mark.asyncio
async def test_rejected_restart_shape_closes_without_causal_book_proof_on_old_main(
    tmp_path,
):
    """The inherited aggressive exit can close while persistence cannot prove its VWAP."""
    clock = FakeClock()
    repository = PaperRepository(tmp_path / "rejected-restart-shape.db")
    try:
        runtime = await _stabilization002_position_runtime(repository, clock)
        await runtime.deliver_trade(_exit_trade(runtime, "rejected-restart-exit"))

        fill_rows = repository.connection.execute(
            "SELECT leg,payload FROM fills ORDER BY fill_id"
        ).fetchall()
        completed = repository.connection.execute(
            "SELECT COUNT(*) FROM completed_trades"
        ).fetchone()[0]
        tables = {
            row[0]
            for row in repository.connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        fill_contract = {field.name for field in fields(Fill)}
        persisted_fill_contracts = {
            tuple(field.name for field in fields(type(_load(row["payload"]))))
            for row in fill_rows
        }
        provenance_rows = (
            []
            if "fill_provenance" not in tables
            else repository.connection.execute(
                "SELECT fill_id,provenance_kind,payload "
                "FROM fill_provenance ORDER BY fill_id"
            ).fetchall()
        )
    finally:
        repository.close()

    audit = {
        "fills": len(fill_rows),
        "completed_trades": completed,
        "has_fill_provenance": "fill_provenance" in tables,
        "fill_contract": fill_contract,
        "persisted_fill_contracts": persisted_fill_contracts,
    }
    assert audit["fills"] == 4
    assert audit["completed_trades"] == 1
    assert audit["has_fill_provenance"], audit
    assert len(provenance_rows) == 4
    for row in provenance_rows:
        proof = _load(row["payload"])
        if row["provenance_kind"] == "TAKER":
            quantity = sum(
                (level.canonical_quantity for level in proof.consumed_levels),
                Decimal("0"),
            )
            notional = sum(
                (level.canonical_price * level.canonical_quantity
                 for level in proof.consumed_levels),
                Decimal("0"),
            )
            assert quantity == proof.requested_quantity == proof.executed_quantity
            assert notional == proof.notional_usd
            assert notional / quantity == proof.vwap_price
            assert proof.observed_at <= proof.decision_at
            assert proof.received_at <= proof.decision_at
            assert proof.stream_session_id >= 0
            assert proof.recovery_generation >= 0
        else:
            assert proof.order_id and proof.order_version_id
            assert proof.tick_size > 0
            assert proof.qualifying_trades


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        sqlite3.OperationalError("synthetic commit failure"),
        asyncio.CancelledError(),
    ],
    ids=("exception", "cancellation"),
)
async def test_exit_close_is_not_published_in_memory_before_atomic_commit(
    tmp_path, failure,
):
    clock = FakeClock()
    repository = PaperRepository(tmp_path / f"rollback-{type(failure).__name__}.db")
    try:
        runtime = await _stabilization002_position_runtime(repository, clock)
        lifecycle = runtime.lifecycle
        before = lifecycle.snapshot
        rows_before = {
            table: repository.connection.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
            for table in ("fills", "completed_trades", "processed_trade_events")
        }

        def fail_save(**_kwargs):
            raise failure

        repository.save_decision = fail_save
        with pytest.raises(type(failure), match=None):
            await runtime.deliver_trade(
                _exit_trade(runtime, f"rollback-{type(failure).__name__}")
            )

        rows_after = {
            table: repository.connection.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
            for table in rows_before
        }
        assert rows_after == rows_before
        assert runtime.lifecycle is lifecycle
        assert lifecycle.snapshot == before
    finally:
        repository.close()


@pytest.mark.asyncio
async def test_entry_open_is_not_published_in_memory_before_atomic_commit(tmp_path):
    clock = FakeClock()
    repository = PaperRepository(tmp_path / "entry-rollback.db")
    try:
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
        rows_before = {
            table: repository.connection.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
            for table in ("fills", "positions", "processed_trade_events")
        }

        def fail_save(**_kwargs):
            raise sqlite3.OperationalError("synthetic entry commit failure")

        repository.save_decision = fail_save
        with pytest.raises(sqlite3.OperationalError):
            await runtime.deliver_trade(
                maker_trade(runtime, clock.now(), "entry-rollback")
            )

        rows_after = {
            table: repository.connection.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
            for table in rows_before
        }
        assert rows_after == rows_before
        assert runtime.broker is broker
        assert broker.state == before
        assert runtime.lifecycle is None
    finally:
        repository.close()
