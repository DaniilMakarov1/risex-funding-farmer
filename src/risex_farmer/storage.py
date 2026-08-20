"""Small atomic SQLite repository and frozen paper report queries."""

from __future__ import annotations

import pickle
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterator

from .economics import replace_funding_settlement
from .lifecycle import (
    ClosedTrade,
    LifecycleSnapshot,
    PaperExitOrder,
)
from .models import (
    DataQuality,
    Fill,
    FundingCashQuote,
    FundingSettlement,
    LifecycleState,
    SettlementStatus,
    TargetFundingCycle,
    TradeEvidence,
)
from .paper_broker import (
    PaperEntryOrder,
    PaperEntryState,
    PaperOrderStatus,
    PaperPosition,
)
from .scanner import ScanSnapshot


UNKNOWN = "UNKNOWN"


def _dump(value: object) -> bytes:
    # Runtime blobs are read only from the user-selected local paper database.
    return pickle.dumps(value, protocol=5)


def _load(payload: bytes) -> object:
    return pickle.loads(payload)


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _decimal(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _seconds(value) -> str | None:
    return None if value is None else str(Decimal(str(value.total_seconds())))


SCHEMA = """
CREATE TABLE IF NOT EXISTS scanner_snapshots (
    logical_at TEXT PRIMARY KEY,
    opportunity_count INTEGER NOT NULL,
    eligible_count INTEGER NOT NULL,
    winner_asset TEXT,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS funding_quotes (
    venue TEXT NOT NULL,
    canonical_market TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    opened_at TEXT NOT NULL,
    settlement_at TEXT NOT NULL,
    payload BLOB NOT NULL,
    PRIMARY KEY (venue, canonical_market, observed_at, opened_at, settlement_at)
);
CREATE TABLE IF NOT EXISTS funding_cycles (
    cycle_id TEXT PRIMARY KEY,
    start_at TEXT NOT NULL,
    end_at TEXT NOT NULL,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS funding_cycle_events (
    cycle_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    canonical_market TEXT NOT NULL,
    settlement_at TEXT NOT NULL,
    PRIMARY KEY (cycle_id, venue, canonical_market, settlement_at)
);
CREATE TABLE IF NOT EXISTS funding_settlements (
    venue TEXT NOT NULL,
    canonical_market TEXT NOT NULL,
    settlement_at TEXT NOT NULL,
    status TEXT NOT NULL,
    cash_usd TEXT,
    payload BLOB NOT NULL,
    PRIMARY KEY (venue, canonical_market, settlement_at)
);
CREATE TABLE IF NOT EXISTS attempts (
    attempt_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    lifecycle_state TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    attempt_id TEXT,
    order_kind TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    closed_at TEXT,
    active_seconds TEXT,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS order_versions (
    version_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    version_number INTEGER NOT NULL,
    status TEXT NOT NULL,
    cumulative_quantity TEXT NOT NULL,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS positions (
    position_id TEXT PRIMARY KEY,
    attempt_id TEXT NOT NULL,
    lifecycle_state TEXT NOT NULL,
    direction TEXT NOT NULL,
    opened_at TEXT NOT NULL,
    closed_at TEXT,
    planned_execution_pnl_usd TEXT,
    planned_maker_net_pnl_usd TEXT,
    risex_entry_notional_usd TEXT NOT NULL,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS position_cycles (
    position_id TEXT NOT NULL,
    cycle_id TEXT NOT NULL,
    PRIMARY KEY (position_id, cycle_id)
);
CREATE TABLE IF NOT EXISTS processed_trade_events (
    trade_event_key TEXT PRIMARY KEY,
    payload BLOB
);
CREATE TABLE IF NOT EXISTS fills (
    fill_id TEXT PRIMARY KEY,
    position_id TEXT NOT NULL,
    leg TEXT NOT NULL,
    venue TEXT NOT NULL,
    notional_usd TEXT NOT NULL,
    fee_usd TEXT NOT NULL,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS position_samples (
    position_id TEXT NOT NULL,
    sample_index INTEGER NOT NULL,
    sampled_at TEXT NOT NULL,
    payload BLOB NOT NULL,
    PRIMARY KEY (position_id, sample_index)
);
CREATE TABLE IF NOT EXISTS gaps (
    position_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    payload BLOB NOT NULL,
    PRIMARY KEY (position_id, started_at)
);
CREATE TABLE IF NOT EXISTS lifecycle_events (
    position_id TEXT NOT NULL,
    event_index INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    payload BLOB NOT NULL,
    PRIMARY KEY (position_id, event_index)
);
CREATE TABLE IF NOT EXISTS completed_trades (
    position_id TEXT PRIMARY KEY,
    close_reason TEXT NOT NULL,
    closed_at TEXT NOT NULL,
    actual_pair_pnl_usd TEXT NOT NULL,
    actual_fees_usd TEXT NOT NULL,
    simulated_closed_net_pnl_usd TEXT,
    applied_rate_closed_net_pnl_usd TEXT,
    recognized_funding_usd TEXT,
    applied_funding_usd TEXT,
    exit_wait_seconds TEXT,
    data_quality TEXT NOT NULL,
    primary_metrics_valid INTEGER NOT NULL,
    risex_exit_notional_usd TEXT NOT NULL,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS runtime_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    state_kind TEXT NOT NULL,
    lifecycle_state TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    payload BLOB NOT NULL
);
"""


class PaperRepository:
    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        self.connection = sqlite3.connect(self.path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.executescript(SCHEMA)

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> PaperRepository:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield self.connection
        except BaseException:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    def save_decision(
        self,
        *,
        recorded_at: datetime,
        scan_snapshot: ScanSnapshot | None = None,
        funding_quotes: tuple[FundingCashQuote, ...] = (),
        trade_events: tuple[TradeEvidence, ...] = (),
        entry_state: PaperEntryState | None = None,
        lifecycle_snapshot: LifecycleSnapshot | None = None,
    ) -> None:
        if entry_state is not None and lifecycle_snapshot is not None:
            raise ValueError("one runtime state must be authoritative per decision")
        with self.transaction():
            if scan_snapshot is not None:
                self._save_scan(scan_snapshot)
            for quote in funding_quotes:
                self._save_quote(quote)
            for trade in trade_events:
                self._save_trade_event(trade)
            if entry_state is not None:
                self._save_entry_state(entry_state, recorded_at)
            if lifecycle_snapshot is not None:
                self._save_lifecycle(lifecycle_snapshot, recorded_at)

    def _insert_exact(
        self,
        table: str,
        key_where: str,
        key_values: tuple[object, ...],
        insert_sql: str,
        insert_values: tuple[object, ...],
        value: object,
    ) -> None:
        row = self.connection.execute(
            f"SELECT payload FROM {table} WHERE {key_where}", key_values
        ).fetchone()
        if row is not None:
            if _load(row["payload"]) != value:
                raise ValueError(f"conflicting duplicate in {table}")
            return
        self.connection.execute(insert_sql, insert_values)

    def _save_scan(self, snapshot: ScanSnapshot) -> None:
        self._insert_exact(
            "scanner_snapshots",
            "logical_at = ?",
            (_iso(snapshot.logical_at),),
            "INSERT INTO scanner_snapshots VALUES (?, ?, ?, ?, ?)",
            (
                _iso(snapshot.logical_at),
                len(snapshot.evaluations),
                sum(plan.entry_allowed for plan in snapshot.evaluations),
                None if snapshot.winner is None else snapshot.winner.canonical_asset,
                _dump(snapshot),
            ),
            snapshot,
        )
        for plan in snapshot.evaluations:
            if plan.target_cycle is not None:
                self._save_cycle(plan.target_cycle)

    def _save_quote(self, quote: FundingCashQuote) -> None:
        key = (
            quote.venue.value,
            quote.canonical_market,
            _iso(quote.observed_at),
            _iso(quote.assumed_or_actual_position_opened_at),
            _iso(quote.settlement_at),
        )
        self._insert_exact(
            "funding_quotes",
            "venue=? AND canonical_market=? AND observed_at=? AND opened_at=? AND settlement_at=?",
            key,
            "INSERT INTO funding_quotes VALUES (?, ?, ?, ?, ?, ?)",
            key + (_dump(quote),),
            quote,
        )

    def _save_cycle(self, cycle: TargetFundingCycle) -> None:
        row = self.connection.execute(
            "SELECT start_at, end_at, payload FROM funding_cycles WHERE cycle_id=?",
            (cycle.cycle_id,),
        ).fetchone()
        if row is not None and (
            row["start_at"] != _iso(cycle.start_at)
            or row["end_at"] != _iso(cycle.end_at)
        ):
            raise ValueError("conflicting funding cycle identity")
        if row is not None:
            existing = _load(row["payload"])
            existing_keys = {
                (event.venue, event.canonical_market, event.settlement_at)
                for event in (existing.risex_event, existing.hedge_event)
            }
            candidate_keys = {
                (event.venue, event.canonical_market, event.settlement_at)
                for event in (cycle.risex_event, cycle.hedge_event)
            }
            if existing_keys != candidate_keys:
                raise ValueError("conflicting funding cycle event keys")
        self.connection.execute(
            """INSERT INTO funding_cycles VALUES (?, ?, ?, ?)
               ON CONFLICT(cycle_id) DO UPDATE SET payload=excluded.payload""",
            (cycle.cycle_id, _iso(cycle.start_at), _iso(cycle.end_at), _dump(cycle)),
        )
        for event in (cycle.risex_event, cycle.hedge_event):
            self.connection.execute(
                "INSERT OR IGNORE INTO funding_cycle_events VALUES (?, ?, ?, ?)",
                (
                    cycle.cycle_id,
                    event.venue.value,
                    event.canonical_market,
                    _iso(event.settlement_at),
                ),
            )

    def insert_trade_event(self, trade: TradeEvidence) -> None:
        with self.transaction():
            self._save_trade_event(trade)

    def _save_trade_event(self, trade: TradeEvidence) -> None:
        row = self.connection.execute(
            "SELECT payload FROM processed_trade_events WHERE trade_event_key=?",
            (trade.trade_event_key,),
        ).fetchone()
        if row is None:
            self.connection.execute(
                "INSERT INTO processed_trade_events VALUES (?, ?)",
                (trade.trade_event_key, _dump(trade)),
            )
        elif row["payload"] is None:
            self.connection.execute(
                "UPDATE processed_trade_events SET payload=? WHERE trade_event_key=?",
                (_dump(trade), trade.trade_event_key),
            )
        elif _load(row["payload"]) != trade:
            raise ValueError("conflicting duplicate trade event")

    def upsert_settlement(self, settlement: FundingSettlement) -> None:
        with self.transaction():
            self._upsert_settlement(settlement)

    def _upsert_settlement(self, settlement: FundingSettlement) -> None:
        row = self.connection.execute(
            "SELECT payload FROM funding_settlements WHERE venue=? AND canonical_market=? AND settlement_at=?",
            (settlement.venue.value, settlement.canonical_market, _iso(settlement.settlement_at)),
        ).fetchone()
        if row is None:
            current = settlement
        else:
            existing = _load(row["payload"])
            if existing == settlement:
                return
            if existing.status is settlement.status:
                raise ValueError("conflicting settlement authority")
            current = replace_funding_settlement(
                existing, settlement.status, settlement.cash_usd
            )
        self.connection.execute(
            """INSERT INTO funding_settlements VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(venue, canonical_market, settlement_at) DO UPDATE SET
               status=excluded.status, cash_usd=excluded.cash_usd, payload=excluded.payload""",
            (
                current.venue.value,
                current.canonical_market,
                _iso(current.settlement_at),
                current.status.value,
                _decimal(current.cash_usd),
                _dump(current),
            ),
        )

    def _save_entry_state(self, state: PaperEntryState, at: datetime) -> None:
        order = state.order
        if order is not None:
            self.connection.execute(
                """INSERT INTO attempts VALUES (?, ?, ?)
                   ON CONFLICT(attempt_id) DO UPDATE SET lifecycle_state=excluded.lifecycle_state""",
                (order.attempt_id, _iso(order.created_at), state.lifecycle_state.value),
            )
            self._save_entry_order(order)
            if state.position is not None:
                self._save_position(state.position, order, state.lifecycle_state)
        for key in state.processed_trade_keys:
            self.connection.execute(
                "INSERT OR IGNORE INTO processed_trade_events VALUES (?, NULL)", (key,)
            )
        self._save_runtime("ENTRY", state.lifecycle_state, at, state)

    def _save_entry_order(self, order: PaperEntryOrder) -> None:
        closed_at = order.cancelled_at
        if closed_at is None and order.status is PaperOrderStatus.FILLED:
            closed_at = order.active_version.closed_at
        active_seconds = (
            None if closed_at is None else _seconds(closed_at - order.created_at)
        )
        self.connection.execute(
            """INSERT INTO orders VALUES (?, ?, 'ENTRY', ?, ?, ?, ?, ?)
               ON CONFLICT(order_id) DO UPDATE SET status=excluded.status,
               closed_at=excluded.closed_at, active_seconds=excluded.active_seconds,
               payload=excluded.payload""",
            (
                order.order_id,
                order.attempt_id,
                order.status.value,
                _iso(order.created_at),
                _iso(closed_at),
                active_seconds,
                _dump(order),
            ),
        )
        for version in order.versions:
            self.connection.execute(
                """INSERT INTO order_versions VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(version_id) DO UPDATE SET status=excluded.status,
                   cumulative_quantity=excluded.cumulative_quantity,
                   payload=excluded.payload""",
                (
                    version.version_id,
                    order.order_id,
                    version.number,
                    version.status.value,
                    str(version.cumulative_eligible_quantity),
                    _dump(version),
                ),
            )
        if order.route_plan.target_cycle is not None:
            self._save_cycle(order.route_plan.target_cycle)

    def _save_position(
        self,
        position: PaperPosition,
        order: PaperEntryOrder,
        state: LifecycleState,
    ) -> None:
        self.connection.execute(
            """INSERT INTO positions VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)
               ON CONFLICT(position_id) DO UPDATE SET lifecycle_state=excluded.lifecycle_state,
               payload=excluded.payload""",
            (
                position.position_id,
                order.attempt_id,
                state.value,
                position.direction.value,
                _iso(position.position_opened_at),
                _decimal(order.route_plan.planned_execution_pnl_usd),
                _decimal(order.route_plan.planned_maker_net_pnl_usd),
                str(position.risex_taker_fill.fee.fill_notional_usd),
                _dump(position),
            ),
        )
        self._save_fill(
            f"{position.position_id}:hedge-entry",
            position.position_id,
            "HEDGE_ENTRY",
            position.hedge_maker_fill,
        )
        self._save_fill(
            f"{position.position_id}:risex-entry",
            position.position_id,
            "RISEX_ENTRY",
            position.risex_taker_fill,
        )
        for quote in position.recomputed_funding_quotes:
            self._save_quote(quote)
        evidence_cycle = position.target_cycle or order.route_plan.target_cycle
        if evidence_cycle is not None:
            self._save_cycle(evidence_cycle)
            self.connection.execute(
                "INSERT OR IGNORE INTO position_cycles VALUES (?, ?)",
                (position.position_id, evidence_cycle.cycle_id),
            )

    def _save_fill(
        self, fill_id: str, position_id: str, leg: str, fill: Fill
    ) -> None:
        self._insert_exact(
            "fills",
            "fill_id=?",
            (fill_id,),
            "INSERT INTO fills VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                fill_id,
                position_id,
                leg,
                fill.venue.value,
                str(fill.fee.fill_notional_usd),
                str(fill.fee.amount_usd),
                _dump(fill),
            ),
            fill,
        )

    def _save_lifecycle(self, snapshot: LifecycleSnapshot, at: datetime) -> None:
        position_id = (
            snapshot.position.position_id
            if snapshot.position is not None
            else snapshot.closed_trade.position_id
            if snapshot.closed_trade is not None
            else None
        )
        if snapshot.active_cycle is not None:
            self._save_cycle(snapshot.active_cycle)
            if position_id is not None:
                self.connection.execute(
                    "INSERT OR IGNORE INTO position_cycles VALUES (?, ?)",
                    (position_id, snapshot.active_cycle.cycle_id),
                )
        for settlement in snapshot.settlements:
            self._upsert_settlement(settlement)
        for key in snapshot.processed_trade_keys:
            self.connection.execute(
                "INSERT OR IGNORE INTO processed_trade_events VALUES (?, NULL)", (key,)
            )
        if snapshot.exit_order is not None:
            self._save_exit_order(snapshot.exit_order, position_id)
        if snapshot.position is not None:
            self.connection.execute(
                "UPDATE positions SET lifecycle_state=?, payload=? WHERE position_id=?",
                (
                    snapshot.lifecycle_state.value,
                    _dump(snapshot.position),
                    snapshot.position.position_id,
                ),
            )
        if position_id is not None:
            for index, sample in enumerate(snapshot.samples):
                self.connection.execute(
                    """INSERT INTO position_samples VALUES (?, ?, ?, ?)
                       ON CONFLICT(position_id, sample_index) DO UPDATE SET payload=excluded.payload""",
                    (position_id, index, _iso(sample.sampled_at), _dump(sample)),
                )
            for gap in snapshot.gaps:
                self.connection.execute(
                    """INSERT INTO gaps VALUES (?, ?, ?, ?)
                       ON CONFLICT(position_id, started_at) DO UPDATE SET
                       ended_at=excluded.ended_at, payload=excluded.payload""",
                    (position_id, _iso(gap.started_at), _iso(gap.ended_at), _dump(gap)),
                )
            for index, event in enumerate(snapshot.events):
                self.connection.execute(
                    """INSERT INTO lifecycle_events VALUES (?, ?, ?, ?, ?)
                       ON CONFLICT(position_id, event_index) DO UPDATE SET payload=excluded.payload""",
                    (
                        position_id,
                        index,
                        event.event_type.value,
                        _iso(event.occurred_at),
                        _dump(event),
                    ),
                )
        if snapshot.closed_trade is not None:
            self._save_closed(snapshot.closed_trade)
        self._save_runtime("LIFECYCLE", snapshot.lifecycle_state, at, snapshot)

    def _save_exit_order(
        self, order: PaperExitOrder, position_id: str | None
    ) -> None:
        active = order.active_version
        status = (
            "OPEN"
            if active is not None
            else "FILLED"
            if order.versions[-1].status.value == "FILLED"
            else "SUSPENDED"
        )
        closed_at = None if active is not None else order.versions[-1].closed_at
        created_at = order.versions[0].created_at
        self.connection.execute(
            """INSERT INTO orders VALUES (?, ?, 'EXIT', ?, ?, ?, ?, ?)
               ON CONFLICT(order_id) DO UPDATE SET status=excluded.status,
               closed_at=excluded.closed_at, active_seconds=excluded.active_seconds,
               payload=excluded.payload""",
            (
                order.order_id,
                position_id,
                status,
                _iso(created_at),
                _iso(closed_at),
                None if closed_at is None else _seconds(closed_at - created_at),
                _dump(order),
            ),
        )
        for version in order.versions:
            self.connection.execute(
                """INSERT INTO order_versions VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(version_id) DO UPDATE SET status=excluded.status,
                   cumulative_quantity=excluded.cumulative_quantity,
                   payload=excluded.payload""",
                (
                    version.version_id,
                    order.order_id,
                    version.number,
                    version.status.value,
                    str(version.cumulative_eligible_quantity),
                    _dump(version),
                ),
            )

    def _save_closed(self, closed: ClosedTrade) -> None:
        self._save_fill(
            f"{closed.position_id}:hedge-exit",
            closed.position_id,
            "HEDGE_EXIT",
            closed.hedge_exit_fill,
        )
        self._save_fill(
            f"{closed.position_id}:risex-exit",
            closed.position_id,
            "RISEX_EXIT",
            closed.risex_exit_fill,
        )
        self.connection.execute(
            "UPDATE positions SET lifecycle_state='FLAT', closed_at=? WHERE position_id=?",
            (_iso(closed.closed_at), closed.position_id),
        )
        self.connection.execute(
            """INSERT INTO completed_trades VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(position_id) DO UPDATE SET
               simulated_closed_net_pnl_usd=excluded.simulated_closed_net_pnl_usd,
               applied_rate_closed_net_pnl_usd=excluded.applied_rate_closed_net_pnl_usd,
               recognized_funding_usd=excluded.recognized_funding_usd,
               applied_funding_usd=excluded.applied_funding_usd,
               primary_metrics_valid=excluded.primary_metrics_valid,
               payload=excluded.payload""",
            (
                closed.position_id,
                closed.close_reason.value,
                _iso(closed.closed_at),
                str(closed.actual_pair_pnl_usd),
                str(closed.actual_fees_usd),
                _decimal(closed.simulated_closed_net_pnl_usd),
                _decimal(closed.applied_rate_closed_net_pnl_usd),
                _decimal(closed.simulated_recognized_funding_usd),
                _decimal(closed.applied_rate_funding_usd),
                _seconds(closed.exit_wait),
                closed.data_quality.value,
                int(closed.primary_metrics_valid),
                str(closed.risex_exit_fill.fee.fill_notional_usd),
                _dump(closed),
            ),
        )

    def _save_runtime(
        self,
        kind: str,
        state: LifecycleState,
        at: datetime,
        value: PaperEntryState | LifecycleSnapshot,
    ) -> None:
        self.connection.execute(
            """INSERT INTO runtime_state VALUES (1, ?, ?, ?, ?)
               ON CONFLICT(singleton) DO UPDATE SET state_kind=excluded.state_kind,
               lifecycle_state=excluded.lifecycle_state, updated_at=excluded.updated_at,
               payload=excluded.payload""",
            (kind, state.value, _iso(at), _dump(value)),
        )

    def load_runtime(self) -> PaperEntryState | LifecycleSnapshot | None:
        row = self.connection.execute(
            "SELECT payload FROM runtime_state WHERE singleton=1"
        ).fetchone()
        return None if row is None else _load(row["payload"])

    def runtime_updated_at(self) -> datetime | None:
        row = self.connection.execute(
            "SELECT updated_at FROM runtime_state WHERE singleton=1"
        ).fetchone()
        return None if row is None else datetime.fromisoformat(row["updated_at"])

    def report(self, *, as_of: datetime | None = None) -> dict[str, object]:
        as_of = as_of or datetime.now(UTC)
        scans = self.connection.execute(
            "SELECT COALESCE(SUM(opportunity_count),0) opportunities, "
            "COALESCE(SUM(eligible_count),0) eligible FROM scanner_snapshots"
        ).fetchone()
        order_rows = self.connection.execute(
            "SELECT order_kind, status, active_seconds, created_at FROM orders"
        ).fetchall()
        entry_order_rows = [row for row in order_rows if row["order_kind"] == "ENTRY"]
        paper_orders = len(entry_order_rows)
        maker_fills = sum(row["status"] == "FILLED" for row in entry_order_rows)
        maker_active_seconds = Decimal("0")
        for row in entry_order_rows:
            if row["active_seconds"] is not None:
                maker_active_seconds += Decimal(row["active_seconds"])
            elif row["status"] == "OPEN":
                maker_active_seconds += max(
                    Decimal("0"),
                    Decimal(
                        str(
                            (
                                as_of - datetime.fromisoformat(row["created_at"])
                            ).total_seconds()
                        )
                    ),
                )
        funding_counts = {
            status.value: self.connection.execute(
                "SELECT COUNT(*) count FROM funding_settlements WHERE status=?",
                (status.value,),
            ).fetchone()["count"]
            for status in SettlementStatus
        }
        closed_rows = self.connection.execute(
            "SELECT * FROM completed_trades ORDER BY closed_at, position_id"
        ).fetchall()
        position_rows = {
            row["position_id"]: row
            for row in self.connection.execute("SELECT * FROM positions").fetchall()
        }
        closed_values = [_load(row["payload"]) for row in closed_rows]
        primary = [closed for closed in closed_values if closed.primary_metrics_valid]
        applied = [
            closed
            for closed in closed_values
            if closed.primary_metrics_valid
            and closed.applied_rate_closed_net_pnl_usd is not None
        ]

        def total_decimal(values) -> Decimal:
            return sum(values, Decimal("0"))

        def known_total(values: list[Decimal | None]) -> str:
            if any(value is None for value in values):
                return UNKNOWN
            return str(total_decimal(value for value in values if value is not None))

        primary_net = total_decimal(
            closed.simulated_closed_net_pnl_usd
            for closed in primary
            if closed.simulated_closed_net_pnl_usd is not None
        )
        applied_net = total_decimal(
            closed.applied_rate_closed_net_pnl_usd
            for closed in applied
            if closed.applied_rate_closed_net_pnl_usd is not None
        )
        primary_volume = total_decimal(
            Decimal(position_rows[closed.position_id]["risex_entry_notional_usd"])
            + closed.risex_exit_fill.fee.fill_notional_usd
            for closed in primary
        )
        total_volume = total_decimal(
            Decimal(position_rows[closed.position_id]["risex_entry_notional_usd"])
            + closed.risex_exit_fill.fee.fill_notional_usd
            for closed in closed_values
        )
        equity = Decimal("0")
        peak = Decimal("0")
        drawdown = Decimal("0")
        for closed in primary:
            assert closed.simulated_closed_net_pnl_usd is not None
            equity += closed.simulated_closed_net_pnl_usd
            peak = max(peak, equity)
            drawdown = max(drawdown, peak - equity)
        total_hold_seconds = Decimal("0")
        total_exit_seconds = Decimal("0")
        for closed in closed_values:
            row = position_rows[closed.position_id]
            total_duration = Decimal(
                str(
                    (
                        closed.closed_at - datetime.fromisoformat(row["opened_at"])
                    ).total_seconds()
                )
            )
            exit_seconds = (
                Decimal("0")
                if closed.exit_wait is None
                else Decimal(str(closed.exit_wait.total_seconds()))
            )
            total_exit_seconds += exit_seconds
            total_hold_seconds += max(Decimal("0"), total_duration - exit_seconds)
        planned_execution = total_decimal(
            Decimal(row["planned_execution_pnl_usd"])
            for row in position_rows.values()
            if row["closed_at"] is not None
            and row["planned_execution_pnl_usd"] is not None
        )
        planned_error = total_decimal(
            closed.simulated_closed_net_pnl_usd
            - Decimal(position_rows[closed.position_id]["planned_maker_net_pnl_usd"])
            for closed in primary
            if closed.simulated_closed_net_pnl_usd is not None
            and position_rows[closed.position_id]["planned_maker_net_pnl_usd"] is not None
        )
        runtime = self.load_runtime()
        open_position: dict[str, object] | None = None
        if runtime is not None and runtime.lifecycle_state is not LifecycleState.FLAT:
            position = getattr(runtime, "position", None)
            open_position = {
                "state": runtime.lifecycle_state.value,
                "position_id": None if position is None else position.position_id,
            }
        partial_cycles = self._applied_partial_cycle_count()
        normal_exit_fills = sum(
            closed.close_reason.value == "NORMAL_MAKER" for closed in closed_values
        )
        aggressive_exit_fills = sum(
            closed.close_reason.value == "AGGRESSIVE_MAKER" for closed in closed_values
        )
        simulated_win_rate = (
            UNKNOWN
            if not primary
            else str(
                Decimal(
                    sum(closed.simulated_closed_net_pnl_usd > 0 for closed in primary)
                )
                / Decimal(len(primary))
            )
        )
        applied_rate_win_rate = (
            UNKNOWN
            if not applied
            else str(
                Decimal(
                    sum(closed.applied_rate_closed_net_pnl_usd > 0 for closed in applied)
                )
                / Decimal(len(applied))
            )
        )
        return {
            "opportunities": scans["opportunities"],
            "eligible_opportunities": scans["eligible"],
            "eligible_count": scans["eligible"],
            "paper_orders": paper_orders,
            "maker_fills": maker_fills,
            "orders": paper_orders,
            "filled_orders": maker_fills,
            "fills": self.connection.execute("SELECT COUNT(*) FROM fills").fetchone()[0],
            "fill_rate": (
                UNKNOWN
                if not paper_orders
                else str(Decimal(maker_fills) / Decimal(paper_orders))
            ),
            "maker_active_seconds": str(maker_active_seconds),
            "order_active_seconds": str(maker_active_seconds),
            "normal_exit_fills": normal_exit_fills,
            "aggressive_exit_fills": aggressive_exit_fills,
            "normal_exits": normal_exit_fills,
            "aggressive_exits": aggressive_exit_fills,
            "hard_basis_exits": sum(
                closed.close_reason.value == "HARD_BASIS" for closed in closed_values
            ),
            "funding_applied": funding_counts[SettlementStatus.APPLIED_RATE.value],
            "estimated_funding": funding_counts[SettlementStatus.ESTIMATED.value],
            "funding_estimated": funding_counts[SettlementStatus.ESTIMATED.value],
            "unresolved_settlements": funding_counts[SettlementStatus.UNRESOLVED.value],
            "funding_unresolved": funding_counts[SettlementStatus.UNRESOLVED.value],
            "applied_rate_funding_partial": partial_cycles,
            "funding_applied_partial_cycles": partial_cycles,
            "simulated_recognized_funding_usd": known_total(
                [closed.simulated_recognized_funding_usd for closed in closed_values]
            ),
            "applied_rate_funding_usd": known_total(
                [closed.applied_rate_funding_usd for closed in closed_values]
            ),
            "planned_execution_pnl_usd": str(planned_execution),
            "actual_pair_pnl_usd": str(
                total_decimal(closed.actual_pair_pnl_usd for closed in closed_values)
            ),
            "actual_fees_usd": str(
                total_decimal(closed.actual_fees_usd for closed in closed_values)
            ),
            "simulated_closed_net_pnl_usd": known_total(
                [closed.simulated_closed_net_pnl_usd for closed in closed_values]
            ),
            "applied_rate_closed_net_pnl_usd": known_total(
                [closed.applied_rate_closed_net_pnl_usd for closed in closed_values]
            ),
            "primary_closed_net_pnl_usd": str(primary_net),
            "simulated_win_rate": simulated_win_rate,
            "primary_win_rate": simulated_win_rate,
            "applied_rate_win_rate": applied_rate_win_rate,
            "applied_win_rate": applied_rate_win_rate,
            "hold_duration_seconds": str(total_hold_seconds),
            "exit_wait_seconds": str(total_exit_seconds),
            "exit_duration_seconds": str(total_exit_seconds),
            "funding_while_exiting_usd": known_total(
                [closed.funding_while_exiting_usd for closed in closed_values]
            ),
            "pair_pnl_change_while_exiting_usd": known_total(
                [closed.pair_pnl_change_while_exiting_usd for closed in closed_values]
            ),
            "cycles": self.connection.execute(
                "SELECT COUNT(*) FROM position_cycles"
            ).fetchone()[0],
            "max_drawdown_usd": str(drawdown),
            "virtual_risex_volume_usd": str(total_volume),
            "primary_virtual_risex_volume_usd": str(primary_volume),
            "pnl_per_1000_risex_volume_usd": (
                UNKNOWN
                if primary_volume == 0
                else str(primary_net * Decimal("1000") / primary_volume)
            ),
            "planned_vs_actual_error_usd": str(planned_error),
            "complete_trades": sum(
                closed.data_quality is DataQuality.COMPLETE for closed in closed_values
            ),
            "degraded_trades": sum(
                closed.data_quality is DataQuality.DEGRADED for closed in closed_values
            ),
            "primary_trade_count": len(primary),
            "applied_trade_count": len(applied),
            "open_position": open_position,
            "assumption_flags": {
                "paper_only": True,
                "taker_failure_and_latency_not_simulated": True,
                "partial_fills_not_simulated": True,
                "queue_position_not_simulated": True,
                "cancel_replace_latency_not_simulated": True,
                "stablecoin_depeg_not_simulated": True,
                "live_margin_and_liquidation_not_simulated": True,
                "expected_basis_convergence_pnl_usd": "0",
                "points_value_usd": "0",
                "risex_fee_tier": "USER_CONFIGURED_TIER_3",
                "nado_fees": "USER_CONFIGURED_ASSUMPTION",
            },
        }

    def _applied_partial_cycle_count(self) -> int:
        rows = self.connection.execute(
            """SELECT e.cycle_id, s.status FROM funding_cycle_events e
               JOIN position_cycles p ON p.cycle_id=e.cycle_id
               LEFT JOIN funding_settlements s ON s.venue=e.venue
               AND s.canonical_market=e.canonical_market
               AND s.settlement_at=e.settlement_at"""
        ).fetchall()
        cycles: dict[str, list[str | None]] = {}
        for row in rows:
            cycles.setdefault(row["cycle_id"], []).append(row["status"])
        terminal = {
            SettlementStatus.APPLIED_RATE.value,
            SettlementStatus.SKIPPED_POSITION_NOT_OPEN.value,
            SettlementStatus.SKIPPED_POSITION_CLOSED.value,
        }
        return sum(
            SettlementStatus.APPLIED_RATE.value in statuses
            and not all(status in terminal for status in statuses)
            for statuses in cycles.values()
        )
