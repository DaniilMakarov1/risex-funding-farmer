"""Fixture-only RISEx bounded lifecycle contract.

This module deliberately has no network client, credential implementation, CLI, or
normal Farmer import.  Its callables are synthetic boundaries for deterministic
tests; enabling real signing or transport requires a later governance gate.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from enum import Enum
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any, Callable
import uuid


OFFICIAL_HOST = "api.testnet.rise.trade"
OFFICIAL_CHAIN_ID = 11_155_931
OFFICIAL_DOMAIN_NAME = "RISEx"
OFFICIAL_DOMAIN_VERSION = "1"
MAX_NOTIONAL_USD = Decimal("500")
BOUND_BPS = Decimal("30")
MAX_AGE_SECONDS = 5


class LifecycleSafetyError(RuntimeError):
    """A bounded, identity-free fail-closed rejection."""


class Outcome(str, Enum):
    ACTIVE = "ACTIVE"
    COMPLETED_NO_FILL_FLAT = "COMPLETED_NO_FILL_FLAT"
    SUCCESS_CLOSED_FLAT = "SUCCESS_CLOSED_FLAT"
    FAILED_HALTED_MANUAL_RECOVERY = "FAILED_HALTED_MANUAL_RECOVERY"


@dataclass(frozen=True)
class MarketState:
    host: str
    chain_id: int
    domain_name: str
    domain_version: str
    router: str
    market_id: int
    symbol: str
    active: bool
    unlocked: bool
    tick: Decimal
    step: Decimal
    minimum: Decimal
    observed_at: int


@dataclass(frozen=True)
class AccountState:
    signer_status: str
    position: Decimal
    open_order_ids: tuple[str, ...]
    repeated_open_order_ids: tuple[str, ...] = ()
    repeated_position: Decimal | None = None
    unexplained: bool = False
    observed_at: int = 0


@dataclass(frozen=True)
class BBO:
    bid: Decimal
    ask: Decimal
    bid_depth: Decimal
    ask_depth: Decimal
    observed_at: int


@dataclass(frozen=True)
class Preflight:
    market: MarketState
    account: AccountState
    bbo: BBO
    size: Decimal
    buy_bound: Decimal


@dataclass(frozen=True)
class Intent:
    intent_id: str
    ordinal: int
    kind: str
    client_order_id: int
    nonce: int
    nonce_bitmap: int
    payload_digest: str
    bbo_digest: str
    state: str
    side: str
    order_type: str
    time_in_force: str
    reduce_only: bool
    post_only: bool
    market_id: int
    size: Decimal
    size_steps: int
    price: Decimal
    price_ticks: int
    source_position: Decimal
    expires_at: int
    dispatch_count: int = 0
    order_id: str | None = None


@dataclass(frozen=True)
class Evidence:
    order_id: str
    client_order_id: int
    terminal: bool
    filled_size: Decimal
    position: Decimal
    open_order_ids: tuple[str, ...]
    observed_at: int

    @classmethod
    def terminal_flat(cls, order_id: str, client_order_id: int, observed_at: int) -> "Evidence":
        return cls(order_id, client_order_id, True, Decimal("0"), Decimal("0"), (), observed_at)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    ordinal INTEGER NOT NULL,
    kind TEXT NOT NULL,
    client_order_id INTEGER NOT NULL UNIQUE,
    nonce INTEGER NOT NULL UNIQUE,
    nonce_bitmap INTEGER NOT NULL,
    payload_digest TEXT NOT NULL UNIQUE,
    bbo_digest TEXT NOT NULL,
    state TEXT NOT NULL,
    side TEXT NOT NULL,
    order_type TEXT NOT NULL,
    time_in_force TEXT NOT NULL,
    reduce_only INTEGER NOT NULL,
    post_only INTEGER NOT NULL,
    market_id INTEGER NOT NULL,
    size TEXT NOT NULL,
    size_steps INTEGER NOT NULL,
    price TEXT NOT NULL,
    price_ticks INTEGER NOT NULL,
    source_position TEXT NOT NULL,
    expires_at INTEGER NOT NULL,
    dispatch_count INTEGER NOT NULL DEFAULT 0,
    order_id TEXT UNIQUE
);
CREATE TABLE IF NOT EXISTS cancels (
    order_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    dispatch_count INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS terminal (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class DurableIntentStore:
    """Small dedicated journal; it stores canonical digests, never payloads."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.connection = sqlite3.connect(self.path)
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.executescript(_SCHEMA)
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    def add(self, intent: Intent) -> None:
        try:
            with self.connection:
                self.connection.execute(
                    "INSERT INTO intents VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        intent.intent_id, intent.ordinal, intent.kind,
                        intent.client_order_id, intent.nonce, intent.nonce_bitmap,
                        intent.payload_digest, intent.bbo_digest, intent.state,
                        intent.side, intent.order_type,
                        intent.time_in_force, int(intent.reduce_only),
                        int(intent.post_only), intent.market_id, str(intent.size),
                        intent.size_steps, str(intent.price), intent.price_ticks,
                        str(intent.source_position), intent.expires_at,
                        intent.dispatch_count, intent.order_id,
                    ),
                )
        except sqlite3.IntegrityError:
            raise LifecycleSafetyError("RISEx lifecycle identity rejected") from None

    def get(self, intent_id: str) -> Intent:
        row = self.connection.execute(
            "SELECT * FROM intents WHERE intent_id = ?", (intent_id,)
        ).fetchone()
        if row is None:
            raise LifecycleSafetyError("RISEx lifecycle intent rejected")
        return _intent(row)

    def all(self) -> list[Intent]:
        return [_intent(row) for row in self.connection.execute("SELECT * FROM intents ORDER BY ordinal")]

    def update_state(self, intent_id: str, state: str, *, increment: bool = False, order_id: str | None = None) -> None:
        with self.connection:
            if increment:
                self.connection.execute(
                    "UPDATE intents SET state=?, dispatch_count=dispatch_count+1 WHERE intent_id=?",
                    (state, intent_id),
                )
            elif order_id is not None:
                self.connection.execute(
                    "UPDATE intents SET state=?, order_id=? WHERE intent_id=?",
                    (state, order_id, intent_id),
                )
            else:
                self.connection.execute("UPDATE intents SET state=? WHERE intent_id=?", (state, intent_id))

    def known_order(self, order_id: str) -> bool:
        return self.connection.execute(
            "SELECT 1 FROM intents WHERE order_id=?", (order_id,)
        ).fetchone() is not None

    def reserve_cancel(self, order_id: str) -> None:
        try:
            with self.connection:
                self.connection.execute(
                    "INSERT INTO cancels VALUES (?, 'DISPATCHING', 1)", (order_id,)
                )
        except sqlite3.IntegrityError:
            raise LifecycleSafetyError("RISEx cancel replay rejected") from None

    def set_cancel_state(self, order_id: str, state: str) -> None:
        with self.connection:
            self.connection.execute("UPDATE cancels SET state=? WHERE order_id=?", (state, order_id))

    def cancel_count(self, order_id: str) -> int:
        row = self.connection.execute(
            "SELECT dispatch_count FROM cancels WHERE order_id=?", (order_id,)
        ).fetchone()
        return 0 if row is None else int(row[0])

    def persist_outcome(self, outcome: Outcome) -> None:
        with self.connection:
            self.connection.execute(
                "INSERT OR REPLACE INTO terminal VALUES ('outcome', ?)", (outcome.value,)
            )

    def redacted_evidence(self) -> dict[str, Any]:
        rows = self.all()
        return {
            "intent_count": len(rows),
            "states": [row.state for row in rows],
            "identities": ["[REDACTED]" for _ in rows],
        }


def _intent(row: tuple[Any, ...]) -> Intent:
    return Intent(
        intent_id=row[0], ordinal=row[1], kind=row[2], client_order_id=row[3],
        nonce=row[4], nonce_bitmap=row[5], payload_digest=row[6],
        bbo_digest=row[7], state=row[8], side=row[9], order_type=row[10],
        time_in_force=row[11], reduce_only=bool(row[12]), post_only=bool(row[13]),
        market_id=row[14], size=Decimal(row[15]), size_steps=row[16],
        price=Decimal(row[17]), price_ticks=row[18],
        source_position=Decimal(row[19]), expires_at=row[20],
        dispatch_count=row[21], order_id=row[22],
    )


def _aligned(value: Decimal, grid: Decimal) -> bool:
    return grid > 0 and value > 0 and value % grid == 0


def _bound(price: Decimal, tick: Decimal, side: str) -> Decimal:
    factor = Decimal("1") + (BOUND_BPS / Decimal("10000") if side == "BUY" else -BOUND_BPS / Decimal("10000"))
    rounding = ROUND_CEILING if side == "BUY" else ROUND_FLOOR
    return (price * factor / tick).to_integral_value(rounding=rounding) * tick


def _digest(fields: dict[str, Any]) -> str:
    canonical = json.dumps(fields, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


class Lifecycle:
    def __init__(self, store: DurableIntentStore, *, now: Callable[[], int], router: str) -> None:
        self.store = store
        self._now = now
        self._router = router.lower()
        self.outcome = Outcome.ACTIVE
        self.observed_opening_fill = False
        self._halted = False

    @property
    def close_count(self) -> int:
        return sum(intent.kind == "CLOSE" for intent in self.store.all())

    def _reject(self, *, halt: bool = False) -> None:
        if halt:
            self._halted = True
            self.outcome = Outcome.FAILED_HALTED_MANUAL_RECOVERY
            self.store.persist_outcome(self.outcome)
        raise LifecycleSafetyError("RISEx lifecycle safety check rejected")

    def _fresh(self, observed_at: int) -> bool:
        return 0 <= self._now() - observed_at <= MAX_AGE_SECONDS

    def _validate_market(self, value: MarketState) -> None:
        if (
            value.host != OFFICIAL_HOST or value.chain_id != OFFICIAL_CHAIN_ID
            or value.domain_name != OFFICIAL_DOMAIN_NAME
            or value.domain_version != OFFICIAL_DOMAIN_VERSION
            or value.router.lower() != self._router or value.market_id != 1
            or value.symbol != "BTC/USDC" or not value.active or not value.unlocked
            or not self._fresh(value.observed_at) or value.tick <= 0 or value.step <= 0
            or not _aligned(value.minimum, value.step)
        ):
            self._reject()

    def _validate_bbo(self, market: MarketState, value: BBO, side: str, size: Decimal) -> Decimal:
        if (
            not self._fresh(value.observed_at) or value.bid <= 0 or value.ask <= value.bid
            or value.bid % market.tick or value.ask % market.tick
        ):
            self._reject()
        depth = value.ask_depth if side == "BUY" else value.bid_depth
        reference = value.ask if side == "BUY" else value.bid
        if depth < size:
            self._reject()
        price = _bound(reference, market.tick, side)
        if size * price > MAX_NOTIONAL_USD:
            self._reject()
        return price

    def preflight(self, market: MarketState, account: AccountState, bbo: BBO) -> Preflight:
        if self._halted or self.store.all():
            self._reject()
        self._validate_market(market)
        if (
            account.signer_status != "ACTIVE" or account.position != 0
            or account.repeated_position != account.position
            or account.open_order_ids or account.repeated_open_order_ids
            or account.unexplained or not self._fresh(account.observed_at)
        ):
            self._reject()
        if bbo.bid_depth < market.minimum or bbo.ask_depth < market.minimum:
            self._reject()
        price = self._validate_bbo(market, bbo, "BUY", market.minimum)
        return Preflight(market, account, bbo, market.minimum, price)

    def _prepare(
        self, *, kind: str, side: str, order_type: str, time_in_force: str,
        reduce_only: bool, market: MarketState, bbo: BBO, size: Decimal, price: Decimal,
        source_position: Decimal, client_order_id: int, nonce: int, expires_at: int,
    ) -> Intent:
        if (
            self._halted or not 0 < client_order_id < 2**64
            or not 0 <= nonce < 2**48 or expires_at <= self._now()
        ):
            self._reject()
        ordinal = len(self.store.all()) + 1
        size_steps = int(size / market.step)
        price_ticks = int(price / market.tick)
        if size_steps <= 0 or price_ticks <= 0 or size_steps >= 2**88 or price_ticks >= 2**88:
            self._reject()
        bbo_digest = _digest({
            "bid": str(bbo.bid), "ask": str(bbo.ask),
            "bid_depth": str(bbo.bid_depth), "ask_depth": str(bbo.ask_depth),
            "observed_at": bbo.observed_at,
        })
        fields = {
            "action": "RISE_PERPS_PLACE_ORDER_V1", "ordinal": ordinal,
            "kind": kind, "client_order_id": client_order_id, "nonce": nonce,
            "nonce_bitmap": 0, "market_id": market.market_id,
            "side": side, "order_type": order_type, "time_in_force": time_in_force,
            "reduce_only": reduce_only, "post_only": False, "size": str(size),
            "size_steps": size_steps, "price": str(price), "price_ticks": price_ticks,
            "source_position": str(source_position), "bbo_digest": bbo_digest,
            "expires_at": expires_at,
        }
        intent = Intent(
            str(uuid.uuid4()), ordinal, kind, client_order_id, nonce, 0,
            _digest(fields), bbo_digest, "PREPARED", side, order_type,
            time_in_force, reduce_only, False, market.market_id, size, size_steps,
            price, price_ticks, source_position, expires_at,
        )
        self.store.add(intent)
        return intent

    def prepare_open(self, preflight: Preflight, client_order_id: int, nonce: int, expires_at: int) -> Intent:
        if any(item.kind == "OPEN" for item in self.store.all()):
            self._reject()
        return self._prepare(
            kind="OPEN", side="BUY", order_type="MARKET", time_in_force="FOK",
            reduce_only=False, market=preflight.market, bbo=preflight.bbo,
            size=preflight.size, price=preflight.buy_bound,
            source_position=Decimal("0"), client_order_id=client_order_id,
            nonce=nonce, expires_at=expires_at,
        )

    def prepare_close(
        self, market: MarketState, account: AccountState, bbo: BBO,
        client_order_id: int, nonce: int, expires_at: int,
    ) -> Intent:
        if self._halted or not self.observed_opening_fill or self.close_count >= 3:
            self._reject(halt=self.close_count >= 3)
        self._validate_market(market)
        position = account.position
        if not self._fresh(account.observed_at) or account.unexplained or account.open_order_ids:
            self._reject(halt=True)
        if position <= 0 or not _aligned(position, market.step):
            self._reject(halt=True)
        prior = [item for item in self.store.all() if item.kind == "CLOSE"]
        unfinished = [item for item in self.store.all() if item.state != "TERMINAL"]
        if unfinished:
            self._reject(halt=True)
        if prior and (prior[-1].state != "TERMINAL" or position > prior[-1].source_position):
            self._reject(halt=True)
        if not prior and position > market.minimum:
            self._reject(halt=True)
        side = "SELL"
        price = self._validate_bbo(market, bbo, side, position)
        number = len(prior) + 1
        if number == 3 and position == prior[-1].source_position:
            self._reject(halt=True)
        return self._prepare(
            kind="CLOSE", side=side,
            order_type="MARKET" if number == 1 else "LIMIT",
            time_in_force="FOK" if number == 1 else "IOC", reduce_only=True,
            market=market, bbo=bbo, size=position, price=price, source_position=position,
            client_order_id=client_order_id, nonce=nonce, expires_at=expires_at,
        )

    def run_open(
        self, market: MarketState, account: AccountState, bbo: BBO,
        client_order_id: int, nonce: int, expires_at: int,
        *, signer_loader: Callable[[], Any], dispatch: Callable[[dict[str, Any]], Any],
    ) -> Intent:
        intent = self.prepare_open(self.preflight(market, account, bbo), client_order_id, nonce, expires_at)
        synthetic_or_later_gated_signer = signer_loader()
        self.dispatch(intent, synthetic_or_later_gated_signer, dispatch)
        return intent

    def dispatch(
        self, intent: Intent, synthetic_signer: Any,
        execute: Callable[[dict[str, Any]], Any],
    ) -> None:
        current = self.store.get(intent.intent_id)
        if self._halted or current.state != "PREPARED" or current.dispatch_count or self._now() >= current.expires_at:
            self._reject()
        self.store.update_state(intent.intent_id, "DISPATCHING", increment=True)
        try:
            if synthetic_signer is None:
                raise LifecycleSafetyError("RISEx synthetic signer rejected")
            execute({"intent": "[REDACTED]", "digest": current.payload_digest})
        except Exception:
            self.store.update_state(intent.intent_id, "AMBIGUOUS")
            return
        self.store.update_state(intent.intent_id, "DISPATCHED")

    def unsigned_action(self, intent_id: str) -> dict[str, Any]:
        """Return the exact synthetic-boundary fields used by official place encoding."""
        value = self.store.get(intent_id)
        return {
            "action": "RISE_PERPS_PLACE_ORDER_V1",
            "market_id": value.market_id,
            "side": value.side,
            "order_type": value.order_type,
            "time_in_force": value.time_in_force,
            "reduce_only": value.reduce_only,
            "post_only": value.post_only,
            "size_steps": value.size_steps,
            "price_ticks": value.price_ticks,
            "client_order_id": value.client_order_id,
            "nonce_anchor": value.nonce,
            "nonce_bitmap": value.nonce_bitmap,
            "permit_deadline": value.expires_at,
        }

    def mark_dispatched(self, intent_id: str, *, order_id: str) -> None:
        current = self.store.get(intent_id)
        if current.state not in {"PREPARED", "DISPATCHED", "DISPATCHING"}:
            self._reject()
        self.store.update_state(intent_id, "DISPATCHED", order_id=order_id)

    def mark_terminal(self, intent_id: str) -> None:
        current = self.store.get(intent_id)
        if current.state == "AMBIGUOUS":
            self._reject()
        self.store.update_state(intent_id, "TERMINAL")

    def reconcile(self, intent_id: str, evidence: Evidence) -> Outcome:
        current = self.store.get(intent_id)
        if (
            (current.order_id is not None and current.order_id != evidence.order_id)
            or not evidence.order_id or current.client_order_id != evidence.client_order_id
            or not evidence.terminal or not self._fresh(evidence.observed_at)
            or evidence.order_id in evidence.open_order_ids
        ):
            self._reject()
        if current.order_id is None:
            self.store.update_state(intent_id, current.state, order_id=evidence.order_id)
        self.store.update_state(intent_id, "TERMINAL")
        if current.kind == "OPEN" and evidence.filled_size == 0 and evidence.position == 0 and not evidence.open_order_ids:
            self.outcome = Outcome.COMPLETED_NO_FILL_FLAT
            self.store.persist_outcome(self.outcome)
            return self.outcome
        if evidence.filled_size > 0 or evidence.position > 0:
            self.observed_opening_fill = True
        return self.outcome

    def cancel_known(self, order_id: str, execute: Callable[[str], Any]) -> None:
        if self._halted or not self.store.known_order(order_id):
            self._reject()
        self.store.reserve_cancel(order_id)
        try:
            execute(order_id)
        except Exception:
            self.store.set_cancel_state(order_id, "AMBIGUOUS")
            return
        self.store.set_cancel_state(order_id, "DISPATCHED")

    def halt_manual(self, account: AccountState, reason: str) -> dict[str, Any]:
        self._halted = True
        self.outcome = Outcome.FAILED_HALTED_MANUAL_RECOVERY
        self.store.persist_outcome(self.outcome)
        safe_reason = reason if reason in {"connectivity_lost", "attempts_exhausted", "state_conflict"} else "safety_halt"
        return {
            "outcome": self.outcome.value,
            "reason": safe_reason,
            "position": str(account.position),
            "order_ids": ["[REDACTED]" for _ in account.open_order_ids],
            "manual_recovery": (
                "Use the official RISEx testnet UI to inspect and full-close the exact "
                "remaining position, then verify zero open orders and exact flatness."
            ),
        }

    def finalize(self, account: AccountState) -> Outcome:
        intents = self.store.all()
        if (
            self.observed_opening_fill and account.position == 0
            and not account.open_order_ids and not account.unexplained
            and account.repeated_position == account.position
            and account.repeated_open_order_ids == account.open_order_ids
            and any(item.kind == "OPEN" for item in intents)
            and any(item.kind == "CLOSE" for item in intents)
            and all(item.state == "TERMINAL" for item in intents)
        ):
            self.outcome = Outcome.SUCCESS_CLOSED_FLAT
            self.store.persist_outcome(self.outcome)
            return self.outcome
        return Outcome.FAILED_HALTED_MANUAL_RECOVERY
