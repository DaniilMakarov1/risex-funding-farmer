"""Fan-out random cycle: one source LIMIT filled by k receiver MARKETs.

Owner request 2026-09-26.  The 1 -> 1 cycle (:class:`RandomCycleEngine`) is
unchanged; this engine reuses its per-account helpers and replaces only what
assumed exactly two accounts: sizing and the random split, leverage, the
pre-order preparation, the opening and closing handoffs, residual recovery and
the result.

* The source LIMIT ``Q`` holds at least ``k`` venue-minimum orders with a 10 %
  margin (``Q >= ceil(k x minimum x 1.10)`` on the size grid); ``Q`` is split
  at random into ``k`` parts, each at least one venue minimum and within its
  receiver's funding.
* One hold for every account.  An opening not matched in full between the
  source and every receiver goes straight to per-account reduce-only
  recovery (no hold), as for a 1 -> 1 cycle.
* Closing mirrors the opening with reduce-only orders; every remainder goes
  to the per-account reduce-only recovery over all ``k + 1`` accounts.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from pathlib import Path
import json
import time
from typing import Any, Callable, Mapping, Sequence

from .contracts import (
    AccountSnapshot,
    ContractError,
    Direction,
    LeverageNotSent,
    MarketMetadata,
    MutationReceipt,
    OperationMode,
    Outcome,
    Phase,
    PreflightBlocked,
)
from .engine import Clock
from .fanout_handoff import (
    FanoutHandoffConfig,
    FanoutHandoffResult,
    FanoutPreflightContext,
    FanoutReceiver,
    MAX_FANOUT_RECEIVERS,
    receiver_leg,
    run_fanout_handoff,
)
from .journal import DurableJournal, sanitize_exception
from .local_attempt import select_automatic_prices
from .random_cycle import (
    FallbackResult,
    LAUNCH_METADATA_NAME,
    MAX_CLOSING_ATTEMPTS,
    MAX_HOLD_SECONDS,
    MAX_OPENING_ATTEMPTS,
    MIN_HOLD_SECONDS,
    OWNER_STOP_RETRY_REASON,
    ROBINHOOD_MAKER_FEE_CAP,
    ROBINHOOD_TAKER_FEE_CAP,
    RandomCycleConfig,
    RandomCycleEngine,
    RandomCycleResult,
    RandomCycleSelection,
    RandomQuantityBounds,
    _AccountIdentityFailure,
    _BoundMarketClient,
    _OpeningQuantityRefresh,
    _PairAttemptBudget,
    _RateLimitedAccount,
    _RetryablePreparationFailure,
    _account_payload,
    _as_book,
    _as_market,
    _available_balance,
    _boundary_books_available,
    _child_journal_path,
    _clock_monotonic,
    _coerce_cycle_account,
    _cycle_exception_reason,
    _cycle_latency,
    _cycle_terminal_reason,
    _draw_integer,
    _exclusive_source_price_available,
    _expected_cycle_signs,
    _inventory_fallback_is_resolved,
    _inventory_leg_is_resolved,
    _inverse,
    _is_price_offset_no_room,
    _is_retryable_preparation_error,
    _metadata_payload,
    _nonnegative,
    _observed_leverage_fraction,
    _opening_budget_components,
    _opening_reason,
    _position_residual,
    _positive,
    _recovery_stop_reason,
    _stream_settle,
    _stream_settle_payload,
    _validate_account_fresh,
    _validate_market_book,
    minimal_sufficient_leverage_fraction,
    opening_config_binding,
)
from .read_errors import read_rate_limit_delay
from .series import OrderBookSnapshot

# The owner's "small margin": the LIMIT holds k venue minimums plus 10 %.
FANOUT_SIZE_MARGIN = Decimal("1.10")
MAX_SPLIT_DRAWS = 64


def _ceil(value: Decimal) -> int:
    return int(value.to_integral_value(rounding=ROUND_CEILING))


def _floor(value: Decimal) -> int:
    return int(value.to_integral_value(rounding=ROUND_FLOOR))


def fanout_part_minimum(metadata: MarketMetadata, price: Decimal) -> tuple[int, Decimal, int, int]:
    """(minimum ticks of one order, size step, base-minimum ticks, quote-minimum ticks)."""
    step = Decimal(1).scaleb(-metadata.size_decimals)
    lower_base = _ceil(metadata.minimum_base_amount / step)
    lower_quote = _ceil(metadata.minimum_quote_amount / price / step)
    return max(lower_base, lower_quote), step, lower_base, lower_quote


def fanout_quantity_floor(receiver_count: int, part_minimum_tick: int) -> int:
    """Smallest LIMIT (ticks): ``receiver_count`` venue minimums plus the owner's 10 % margin."""
    return _ceil(Decimal(receiver_count * part_minimum_tick) * FANOUT_SIZE_MARGIN)


def select_fanout_route(source: int, receivers: Sequence[int], rng: Any = None) -> tuple[dict[str, Any], list[int]]:
    """The drawn roles plus a uniform direction: (route for the slot, receivers 2..k).

    ``direction`` names the receivers' exposure, as for a 1 -> 1 route.
    """
    import random

    if len(receivers) < 2:
        raise ContractError("fan-out route needs at least two receivers")
    draw = _draw_integer(random.SystemRandom() if rng is None else rng, 0, 1, "fan-out direction")
    return ({"source_account_index": source, "receiver_account_index": receivers[0],
             "direction": "LONG" if draw == 0 else "SHORT"}, list(receivers[1:]))


def _account_cap_tick(metadata: MarketMetadata, account: AccountSnapshot, *, label: str,
                      price: Decimal, worst_price: Decimal, side: str, reserve: Decimal,
                      step: Decimal, minimum_fraction: int) -> int:
    """Largest quantity (ticks) this account funds at the owner's 4x cap and planning reserve."""
    balance = _available_balance(account, label)
    owner_cap = balance * 10000 / minimum_fraction / price
    if account.margin_evidence is not None:
        parts = _opening_budget_components(metadata, account, label=label, quantity=Decimal(1),
                                           worst_price=worst_price, side=side)
        unit = parts["mark_notional"] * Decimal(minimum_fraction) / 10000 + parts["fee_cost"] + parts["adverse_entry_loss"]
        cap = min(owner_cap, (balance - reserve) / unit)
    else:
        cap = (balance - reserve) * 10000 / minimum_fraction / price
    return max(0, _floor(cap / step))


@dataclass(frozen=True, slots=True)
class FanoutBounds:
    quantity: RandomQuantityBounds
    part_minimum_tick: int
    receiver_cap_ticks: tuple[int, ...]
    receiver_available_balances: tuple[Decimal, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            **self.quantity.as_dict(),
            "fanout": {
                "receiver_count": len(self.receiver_cap_ticks),
                "part_minimum_tick": self.part_minimum_tick,
                "receiver_cap_ticks": list(self.receiver_cap_ticks),
                "receiver_available_balances": [format(value, "f") for value in self.receiver_available_balances],
                "size_margin": format(FANOUT_SIZE_MARGIN, "f"),
            },
        }


def compute_fanout_bounds(
    metadata: MarketMetadata,
    source: AccountSnapshot,
    receivers: Sequence[AccountSnapshot],
    opening_price: Decimal,
    *,
    receiver_bound: Decimal,
    direction: Direction,
    initial_reserve_quote: Decimal = Decimal(0),
) -> FanoutBounds:
    """Legal LIMIT ticks for a fan-out: k minimum parts plus 10 %, up to what every account funds."""
    price = _positive(opening_price, "opening_price")
    bound = _positive(receiver_bound, "receiver_bound")
    count = len(receivers)
    if not 2 <= count <= MAX_FANOUT_RECEIVERS:
        raise PreflightBlocked("fan-out requires two or more receivers")
    part_minimum, step, lower_base, lower_quote = fanout_part_minimum(metadata, min(price, bound))
    if part_minimum <= 0:
        raise PreflightBlocked("venue minimums produce no positive size tick")
    reserve = _nonnegative(initial_reserve_quote, "initial_reserve_quote")
    minimum_fraction = max(2500, metadata.minimum_initial_margin_fraction or 2500)
    evidence = [account.margin_evidence is not None for account in (source, *receivers)]
    if any(evidence) and not all(evidence):
        raise PreflightBlocked("opening margin budget inputs are incomplete")
    source_side = direction.source_side
    receiver_side = "BUY" if source_side == "SELL" else "SELL"
    source_cap = _account_cap_tick(metadata, source, label="source", price=price, worst_price=price,
                                   side=source_side, reserve=reserve, step=step, minimum_fraction=minimum_fraction)
    caps = tuple(_account_cap_tick(metadata, account, label="receiver", price=price, worst_price=bound,
                                   side=receiver_side, reserve=reserve, step=step, minimum_fraction=minimum_fraction)
                 for account in receivers)
    if any(cap < part_minimum for cap in caps):
        raise PreflightBlocked("a fan-out receiver cannot fund the venue minimum order")
    lower = fanout_quantity_floor(count, part_minimum)
    upper = min(source_cap, sum(caps))
    if upper < lower:
        raise PreflightBlocked("available balances cannot fund the LIMIT of all receivers' venue minimums plus 10 %")
    return FanoutBounds(
        quantity=RandomQuantityBounds(
            opening_price=price, size_step=step, lower_tick=lower, upper_tick=upper,
            minimum_base_tick=lower_base, minimum_quote_tick=lower_quote,
            source_available_balance=_available_balance(source, "source"),
            receiver_available_balance=_available_balance(receivers[0], "receiver"),
        ),
        part_minimum_tick=part_minimum,
        receiver_cap_ticks=caps,
        receiver_available_balances=tuple(_available_balance(account, "receiver") for account in receivers),
    )


def split_fanout_ticks(total: int, caps: Sequence[int], minimum: int, rng: Any) -> tuple[int, ...]:
    """Random parts of ``total`` ticks: each at least ``minimum`` and at most its cap.

    k - 1 uniform cut points over the ticks above the minimums (a uniform
    split of the simplex); a draw that exceeds a cap is repeated, and after a
    bounded number of draws the room above the minimums is filled in order.
    """
    count = len(caps)
    extra = total - count * minimum
    room = [cap - minimum for cap in caps]
    if count < 2 or extra < 0 or any(value < 0 for value in room) or extra > sum(room):
        raise PreflightBlocked("the LIMIT cannot be split into the receivers' legal parts")
    for _ in range(MAX_SPLIT_DRAWS):
        cuts = sorted(_draw_integer(rng, 0, extra, "fan-out split") for _ in range(count - 1))
        edges = [0, *cuts, extra]
        adds = [edges[index + 1] - edges[index] for index in range(count)]
        if all(add <= limit for add, limit in zip(adds, room)):
            return tuple(minimum + add for add in adds)
    parts = [minimum] * count
    remaining = extra
    for index in range(count):
        take = min(room[index], remaining)
        parts[index] += take
        remaining -= take
    return tuple(parts)


@dataclass(frozen=True, slots=True)
class FanoutSelection(RandomCycleSelection):
    receiver_accounts: tuple[int, ...] = ()
    receiver_part_ticks: tuple[int, ...] = ()
    part_minimum_tick: int = 0
    receiver_cap_ticks: tuple[int, ...] = ()

    @property
    def receiver_parts(self) -> tuple[Decimal, ...]:
        return tuple(tick * self.bounds.size_step for tick in self.receiver_part_ticks)

    def as_dict(self) -> dict[str, Any]:
        value = RandomCycleSelection.as_dict(self)
        value["fanout"] = {
            "receiver_count": len(self.receiver_accounts),
            "receiver_account_indices": list(self.receiver_accounts),
            "receiver_part_ticks": list(self.receiver_part_ticks),
            "receiver_parts": [format(part, "f") for part in self.receiver_parts],
            "part_minimum_tick": self.part_minimum_tick,
            "receiver_cap_ticks": list(self.receiver_cap_ticks),
            "size_margin": format(FANOUT_SIZE_MARGIN, "f"),
        }
        return value


def select_fanout_quantity(bounds: FanoutBounds, rng: Any) -> tuple[Decimal, int, int, tuple[int, ...]]:
    """Draw the LIMIT ticks, the hold and the random split (in this order)."""
    tick = _draw_integer(rng, bounds.quantity.lower_tick, bounds.quantity.upper_tick, "quantity")
    hold = _draw_integer(rng, MIN_HOLD_SECONDS, MAX_HOLD_SECONDS, "hold_seconds")
    parts = split_fanout_ticks(tick, bounds.receiver_cap_ticks, bounds.part_minimum_tick, rng)
    return tick * bounds.quantity.size_step, tick, hold, parts


@dataclass(frozen=True, slots=True)
class FanoutCycleResult(RandomCycleResult):
    """``remaining_receiver_position`` is the first receiver; the others follow in order."""

    receiver_accounts: tuple[int, ...] = ()
    remaining_extra_positions: tuple[Decimal | None, ...] = ()
    remaining_extra_observed_at: tuple[float | None, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        value = RandomCycleResult.as_dict(self)
        for position, (remaining, observed) in enumerate(
                zip(self.remaining_extra_positions, self.remaining_extra_observed_at), 2):
            leg = receiver_leg(position)
            value["remaining_positions"][leg] = None if remaining is None else format(remaining, "f")
            value["remaining_position_observed_at"][leg] = observed
        value["fanout"] = {"receiver_account_indices": list(self.receiver_accounts)}
        return value


def _phase_legs(phase: Any) -> list[Any]:
    if phase is None:
        return []
    if isinstance(phase, FanoutHandoffResult):
        return [phase.source, *phase.receivers]
    return [phase.source, phase.receiver]


def _latest_positions(opening: Any, closing: Any) -> dict[int, Decimal] | None:
    """Latest causally reconciled position of every cycle account (closing overrides opening)."""
    positions: dict[int, Decimal] = {}
    for phase in (opening, closing):
        for leg in _phase_legs(phase):
            if leg is None or leg.position_after is None:
                return None
            positions[leg.account_index] = leg.position_after
    return positions


def fanout_cycle_classifications(
    opening: Any, closing: Any, fallbacks: Sequence[FallbackResult],
    remaining: Mapping[int, Decimal | None],
) -> tuple[str, str, str]:
    """The 1 -> 1 classification rules over every account of the cycle."""
    if not remaining or any(value is None for value in remaining.values()):
        inventory = "UNKNOWN"
    elif all(value == 0 for value in remaining.values()):
        phases = [phase for phase in (opening, closing) if phase is not None]
        resolved = bool(phases) and all(_inventory_leg_is_resolved(leg) for phase in phases
                                        for leg in _phase_legs(phase))
        resolved = resolved and all(_inventory_fallback_is_resolved(item) for item in fallbacks)
        expected = _latest_positions(opening, closing) if resolved else None
        if expected is not None:
            for item in fallbacks:
                if item.account_index not in expected or item.position_after is None:
                    expected = None
                    break
                expected[item.account_index] = item.position_after
        inventory = ("CONFIRMED_FLAT" if expected is not None and set(expected) == set(remaining)
                     and all(expected[index] == remaining[index] for index in remaining) else "UNKNOWN")
    else:
        inventory = "KNOWN_RESIDUAL"
    phases = [phase for phase in (opening, closing) if phase is not None]
    if not phases:
        paired = "UNKNOWN"
    elif (opening is not None and opening.source is not None and opening.source.filled_quantity == 0
          and any(leg is not None and leg.dispatched and leg.filled_quantity > 0 for leg in _phase_legs(opening)[1:])
          and opening.joint_match_status != "MATCHED"):
        paired = "FAILED"
    elif opening is None or opening.outcome is not Outcome.SUCCESS:
        if opening is not None and opening.retryable_pair:
            paired = "FAILED"
        elif opening is not None and opening.outcome is Outcome.PARTIAL:
            paired = "PARTIAL"
        elif opening is not None and opening.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED:
            paired = "FAILED"
        else:
            paired = "UNKNOWN"
    elif closing is None:
        paired = "UNKNOWN"
    elif closing.outcome is Outcome.SUCCESS:
        paired = "PARTIAL" if fallbacks else "SUCCESS"
    elif closing.retryable_pair or closing.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED:
        paired = "FAILED"
    elif closing.outcome is Outcome.PARTIAL:
        paired = "PARTIAL"
    else:
        paired = "UNKNOWN"
    if paired == "SUCCESS" and not all(phase.mutual_execution_proven for phase in phases):
        paired = ("FAILED" if any(phase.joint_match_status in {"KNOWN_ZERO", "PARTIAL", "CONFLICTING"}
                                  for phase in phases) else "UNKNOWN")
    if not phases or any(phase.economic_status == "UNKNOWN" for phase in phases):
        economics = "UNKNOWN"
    elif any(item.economic_status == "UNKNOWN" for item in fallbacks):
        economics = "UNKNOWN"
    elif all(phase.economic_status == "PROVEN" for phase in phases) and all(
            item.economic_status == "PROVEN" for item in fallbacks):
        economics = "KNOWN"
    else:
        economics = "UNKNOWN"
    return paired, inventory, economics


class _BoundFanoutClient(_BoundMarketClient):
    """The bound cycle client with every fan-out receiver's identity."""

    def __init__(self, delegate: Any, metadata: MarketMetadata, *, source_identity: str,
                 receiver_accounts: Sequence[int], receiver_identities: Sequence[str],
                 identity_failure_callback: Callable[[str], None] | None = None,
                 preflight_context: FanoutPreflightContext | None = None) -> None:
        _BoundMarketClient.__init__(self, delegate, metadata, source_identity=source_identity,
                                    receiver_identity=receiver_identities[0],
                                    identity_failure_callback=identity_failure_callback,
                                    preflight_context=None)
        # A closing may cover only the receivers still holding a legal part.
        allowed = set(getattr(delegate, "extra_receiver_account_indices", ()) or ()) | {delegate.receiver_account_index}
        if not set(receiver_accounts) <= allowed:
            raise ContractError("fan-out receivers are outside the execution client")
        self.handoff_preflight_context = preflight_context
        self.receiver_account_index = receiver_accounts[0]
        self.extra_receiver_account_indices = tuple(receiver_accounts[1:])
        self._extra_identities = dict(zip(receiver_accounts[1:], receiver_identities[1:]))

    async def account_snapshot(self, account_index: int, market_id: int) -> AccountSnapshot:
        if account_index not in self._extra_identities:
            return await _BoundMarketClient.account_snapshot(self, account_index, market_id)
        if market_id != self._metadata.market_id:
            raise ContractError("bound account market identity does not match configured market")
        try:
            raw = await self._delegate.account_snapshot(account_index, market_id)
            snapshot = _coerce_cycle_account(raw, account_index, market_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if read_rate_limit_delay(exc) is not None:
                raise
            reason = "receiver account identity/read validation failed"
            if self._identity_failure_callback is not None:
                self._identity_failure_callback(reason)
            raise PreflightBlocked(reason) from exc
        if snapshot.source_identity != self._extra_identities[account_index]:
            reason = "receiver account identity changed after it was bound"
            if self._identity_failure_callback is not None:
                self._identity_failure_callback(reason)
            raise PreflightBlocked(reason)
        return snapshot


class FanoutCycleEngine(RandomCycleEngine):
    """One fan-out cycle; every step keeps the 1 -> 1 safety rules for all accounts."""

    # ----- accounts -------------------------------------------------------

    def _roles(self, config: RandomCycleConfig) -> list[tuple[str, int]]:
        return [("source", config.source_account_index),
                *((receiver_leg(position), index) for position, index in
                  enumerate(config.receiver_account_indices, 1))]

    async def _fanout_accounts(self, config: RandomCycleConfig) -> tuple[AccountSnapshot, tuple[AccountSnapshot, ...]]:
        roles = self._roles(config)
        tasks = [asyncio.create_task(self._read_account(config, index, f"{label} account read"))
                 for label, index in roles]
        try:
            values = await asyncio.gather(*tasks)
        except BaseException as exc:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if not isinstance(exc, asyncio.CancelledError):
                for task in tasks:
                    if not task.cancelled() and isinstance(task.exception(), _AccountIdentityFailure):
                        raise task.exception()
                limits = [task.exception() for task in tasks if not task.cancelled()
                          and isinstance(task.exception(), _RateLimitedAccount)]
                if limits:
                    raise max(limits, key=lambda error: error.retry_after)
            raise
        now = self.clock.now()
        for (label, _), snapshot in zip(roles, values):
            _validate_account_fresh(config, snapshot, "source" if label == "source" else "receiver", now)
        self._last_account_observations = {snapshot.account_index: snapshot for snapshot in values}
        return values[0], tuple(values[1:])

    async def _rate_limited_fanout_accounts(self, config: RandomCycleConfig, *, journal: DurableJournal | None = None):
        return await self._recovery_read(config, lambda: self._fanout_accounts(config), "account snapshot",
                                         journal, rate_limit_only=True)

    async def _recovery_fanout_accounts(self, config: RandomCycleConfig, journal: DurableJournal | None = None):
        return await self._recovery_read(config, lambda: self._fanout_accounts(config), "recovery accounts", journal)

    async def _reserve_fanout_nonces(self, config: RandomCycleConfig) -> None:
        reserve = getattr(self.client, "reserve_order_nonce", None)
        if not callable(reserve) or not callable(getattr(self.client, "invalidate_reserved_nonce", None)):
            return
        deadline = time.monotonic() + config.freshness_seconds
        self._nonce_deadline = deadline

        async def read(index: int) -> None:
            self._pending_nonces[index] = await self._bounded(
                reserve(index, deadline=deadline), config, "pre-quote nonce reservation")

        tasks = [asyncio.create_task(read(index)) for _, index in self._roles(config)]
        try:
            await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self._release_preflight_nonces()
            raise

    async def _fanout_revalidation_context(self, config: RandomCycleConfig, *, market_read_label: str,
                                           warm_closing_transport: bool = False):
        async def read_metadata() -> MarketMetadata:
            return _as_market(await self._bounded(self.client.market_metadata(config.market_id), config,
                                                  market_read_label))

        await self._release_preflight_nonces()
        warm_task = None
        if warm_closing_transport:
            self._closing_http_warmup = {"status": "unavailable"}
            warm = getattr(self.client, "warm_mutation_http", None)
            if callable(warm):
                async def warm_once() -> None:
                    started = time.perf_counter()
                    try:
                        await asyncio.wait_for(warm(), timeout=min(1.0, config.request_timeout_seconds))
                        self._closing_http_warmup["status"] = "completed"
                    except asyncio.CancelledError:
                        self._closing_http_warmup["status"] = "unfinished_read_cancelled"
                        raise
                    except Exception:
                        self._closing_http_warmup["status"] = "failed"
                    finally:
                        self._closing_http_warmup["seconds"] = time.perf_counter() - started
                warm_task = asyncio.create_task(warm_once())
        tasks = [asyncio.create_task(read_metadata()),
                 asyncio.create_task(self._rate_limited_fanout_accounts(config)),
                 asyncio.create_task(self._reserve_fanout_nonces(config))]
        try:
            metadata, accounts, _ = await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self._release_preflight_nonces()
            raise
        finally:
            if warm_task is not None:
                if not warm_task.done():
                    warm_task.cancel()
                await asyncio.gather(warm_task, return_exceptions=True)
        source, receivers = accounts
        return metadata, source, receivers

    # ----- cycle directory -------------------------------------------------

    def _prepare_cycle_directory(self, path: Path, *, expected_client_order_prefix: str,
                                 expected_route: Mapping[str, Any] | None = None) -> None:
        launch = path / LAUNCH_METADATA_NAME
        if not path.is_symlink() and path.is_dir() and launch.is_file() and not launch.is_symlink():
            try:
                value = json.loads(launch.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                raise PreflightBlocked("cycle reservation metadata is unreadable") from exc
            reserved = value.get("fanout_receivers") if isinstance(value, Mapping) else None
            expected = list(self._config.extra_receiver_account_indices)
            if reserved != expected:
                raise PreflightBlocked("reserved fan-out receivers do not match cycle configuration")
        RandomCycleEngine._prepare_cycle_directory(self, path, expected_client_order_prefix=expected_client_order_prefix,
                                                   expected_route=expected_route)

    async def execute(self, config: RandomCycleConfig) -> RandomCycleResult:
        if not config.extra_receiver_account_indices:
            raise ContractError("fan-out cycle requires extra receivers")
        self._config = config
        return await RandomCycleEngine.execute(self, config)

    # ----- opening context, sizing and leverage ----------------------------

    async def _initial_fanout_context(self, config: RandomCycleConfig, journal: DurableJournal):
        async def observe():
            metadata = _as_market(await self._bounded(self.client.market_metadata(config.market_id), config,
                                                      "market metadata read"))
            book = _as_book(await self._bounded(self._order_book(config.market_id), config, "order book read"),
                            metadata)
            now = self.clock.now()
            _validate_market_book(config, metadata, book, now)
            source, receivers = await self._fanout_accounts(config)
            if source.signed_position != 0 or any(item.signed_position != 0 for item in receivers):
                raise PreflightBlocked("fan-out cycle requires every selected-market position to be exactly flat")
            proposal = select_automatic_prices(config.direction, metadata, book, now=self.clock.now(),
                                               freshness_seconds=config.freshness_seconds,
                                               price_improvement_ticks=config.price_improvement_ticks)
            return metadata, book, source, receivers, proposal

        try:
            return await observe()
        except ContractError as exc:
            if not _is_price_offset_no_room(exc):
                raise
        deadline = _clock_monotonic(self.clock) + config.reconcile_timeout_seconds
        for attempt in range(1, config.max_poll_count + 1):
            remaining = deadline - _clock_monotonic(self.clock)
            if attempt == config.max_poll_count or remaining <= config.poll_interval_seconds:
                raise PreflightBlocked("PRICE_OFFSET_NO_ROOM: initial spread wait exhausted; no orders sent")
            if attempt == 1:
                journal.append("INITIAL_SPREAD_WAIT", {
                    "price_improvement_ticks": config.price_improvement_ticks,
                    "maximum_reads": config.max_poll_count,
                    "timeout_seconds": config.reconcile_timeout_seconds,
                })
            await self.clock.sleep(config.poll_interval_seconds)
            remaining = deadline - _clock_monotonic(self.clock)
            if remaining <= 0:
                raise PreflightBlocked("PRICE_OFFSET_NO_ROOM: initial spread wait exhausted; no orders sent")
            try:
                context = await asyncio.wait_for(observe(), timeout=remaining)
                if _clock_monotonic(self.clock) >= deadline:
                    raise PreflightBlocked("initial spread readiness deadline exceeded")
                journal.append("INITIAL_SPREAD_READY", {"reads": attempt + 1})
                return context
            except ContractError as exc:
                if not _is_price_offset_no_room(exc):
                    raise
        raise AssertionError("initial spread wait must terminate")

    async def _configure_fanout_leverage(self, config: RandomCycleConfig, journal: DurableJournal,
                                         metadata: MarketMetadata, book: OrderBookSnapshot,
                                         selection: FanoutSelection, source: AccountSnapshot,
                                         receivers: tuple[AccountSnapshot, ...]):
        setter = getattr(self.client, "update_leverage_fraction", None)
        accounts = (source, *receivers)
        if not callable(setter):
            if any(account.margin_evidence is not None for account in accounts):
                raise PreflightBlocked("leverage setting capability is missing")
            return source, receivers
        if metadata.minimum_initial_margin_fraction is None or metadata.market_margin_mode != 0:
            raise PreflightBlocked("fresh Robinhood cross-margin leverage limits are unproved")
        if any(account.signed_position != 0 for account in accounts):
            raise PreflightBlocked("leverage can only be configured before a flat cycle")
        source_side = config.direction.source_side
        receiver_side = "BUY" if source_side == "SELL" else "SELL"
        legs = [("source", source, selection.quantity, selection.opening_source_price, source_side),
                *((receiver_leg(position), account, part, selection.opening_receiver_bound, receiver_side)
                  for position, (account, part) in enumerate(zip(receivers, selection.receiver_parts), 1))]
        original = {account.account_index: account for account in accounts}
        budgets: dict[str, Any] = {}
        targets: dict[int, int] = {}
        evidence: dict[str, Any] = {}
        for label, account, quantity, worst_price, side in legs:
            role = "source" if label == "source" else "receiver"
            if account.margin_evidence is not None:
                parts = _opening_budget_components(metadata, account, label=role, quantity=quantity,
                                                   worst_price=worst_price, side=side)
            else:
                parts = {"mark_notional": quantity * selection.opening_source_price, "fee_cost": Decimal(0),
                         "adverse_entry_loss": Decimal(0), "fee_rate": Decimal(0)}
            budgets[label] = {key: format(value, "f") for key, value in parts.items()}
            targets[account.account_index] = minimal_sufficient_leverage_fraction(
                _available_balance(account, role), parts["mark_notional"], metadata.minimum_initial_margin_fraction,
                fee_cost=parts["fee_cost"], adverse_entry_loss=parts["adverse_entry_loss"],
                initial_reserve_quote=config.margin_reserve.initial_quote if config.margin_reserve else Decimal(0))
            required = (parts["mark_notional"] * Decimal(targets[account.account_index]) / 10000
                        + parts["fee_cost"] + parts["adverse_entry_loss"])
            evidence[label] = {
                "account_index": account.account_index, "account_source_identity": account.source_identity,
                "account_observed_at": account.observed_at, "metadata_market_id": metadata.market_id,
                "metadata_symbol": metadata.symbol, "metadata_observed_at": metadata.observed_at,
                "book_market_id": book.market_id, "book_symbol": book.symbol, "book_observed_at": book.observed_at,
                "available_balance": _available_balance(account, role), "mark_price": metadata.mark_price,
                "worst_price": worst_price, "quantity": quantity, "required": required,
                "headroom": _available_balance(account, role) - required, "fee_rate": parts["fee_rate"],
            }
        current = {account.account_index: _observed_leverage_fraction(
            account, "source" if account.account_index == source.account_index else "receiver")
            for account in accounts}
        journal.append("LEVERAGE_PLAN", {
            "quantity": format(selection.quantity, "f"), "price": format(selection.opening_source_price, "f"),
            "notional": format(selection.quantity * selection.opening_source_price, "f"),
            "target_fraction_bps": targets, "observed_fraction_bps": current, "opening_budgets": budgets,
            "initial_plan_observations": evidence,
            "receiver_parts": {str(account.account_index): format(part, "f")
                               for account, part in zip(receivers, selection.receiver_parts)},
            "initial_reserve_quote": (None if config.margin_reserve is None
                                      else format(config.margin_reserve.initial_quote, "f")),
            "dispatch_reserve_quote": (None if config.margin_reserve is None
                                       else format(config.margin_reserve.dispatch_quote, "f")),
        })
        for index in [account.account_index for account in accounts]:
            if current[index] == targets[index]:
                continue
            self._stage = "LEVERAGE"
            journal.append("LEVERAGE_UPDATE_INTENT", {
                "account_index": index, "market_id": config.market_id, "fraction_bps": targets[index],
                "margin_mode": 0, "source_identity": original[index].source_identity,
            })
            not_sent_recorded = False

            def record_not_sent(index: int = index) -> None:
                nonlocal not_sent_recorded
                journal.append("LEVERAGE_UPDATE_NOT_SENT", {"account_index": index, "market_id": config.market_id,
                                                            "fraction_bps": targets[index]})
                not_sent_recorded = True

            call = (setter(index, config.market_id, targets[index], 0,
                           prepared_intent=lambda identity: journal.append("LEVERAGE_TX_PREPARED", identity),
                           cancelled_before_transport=record_not_sent)
                    if getattr(self.client, "supports_leverage_prepared_intent", False)
                    else setter(index, config.market_id, targets[index], 0))
            try:
                receipt = await self._bounded(call, config, "leverage setting")
            except LeverageNotSent as exc:
                record_not_sent()
                self._stage = "PREFLIGHT"
                raise PreflightBlocked(f"account {index} leverage setting was not sent") from exc
            except TimeoutError as exc:
                if not_sent_recorded:
                    self._stage = "PREFLIGHT"
                    raise PreflightBlocked(f"account {index} leverage setting was not sent") from exc
                raise
            if not isinstance(receipt, MutationReceipt):
                raise PreflightBlocked(f"account {index} leverage setting response is undecidable")
            if not receipt.accepted:
                journal.append("LEVERAGE_UPDATE_REJECTED", {"account_index": index, "market_id": config.market_id,
                                                            "fraction_bps": targets[index], "reason": receipt.error})
                self._stage = "PREFLIGHT"
                raise PreflightBlocked(f"account {index} leverage setting was rejected")
            effective = observed = None
            for attempt in range(min(3, config.max_poll_count)):
                source, receivers = await self._rate_limited_fanout_accounts(config, journal=journal)
                for account in (source, *receivers):
                    before = original[account.account_index]
                    if account.source_identity != before.source_identity or account.signed_position != 0:
                        raise PreflightBlocked("account identity or position changed during leverage setting")
                observed = next(account for account in (source, *receivers) if account.account_index == index)
                effective = _observed_leverage_fraction(observed, "source" if index == source.account_index else "receiver")
                if effective == targets[index]:
                    break
                if attempt + 1 < min(3, config.max_poll_count):
                    await self.clock.sleep(config.poll_interval_seconds)
            if effective != targets[index] or observed is None:
                raise PreflightBlocked(f"account {index} leverage setting is not effective or readback is stale")
            journal.append("LEVERAGE_UPDATE_CONFIRMED", {"account_index": index, "market_id": config.market_id,
                                                         "fraction_bps": effective, "observed_at": observed.observed_at})
            current[index] = effective
        self._leverage_fractions = targets
        self._opening_plan_evidence = evidence
        self._stage = "PREFLIGHT"
        return source, receivers

    # ----- preparation ------------------------------------------------------

    async def _prepare_fanout_with_retries(self, config, journal, selection, metadata, book, source, receivers,
                                           budget: _PairAttemptBudget):
        initial = (metadata, book, source, receivers)
        current = selection
        while budget.available:
            attempt = budget.consume()
            journal.append("PREPARATION_ATTEMPT", {
                "attempt": attempt, "maximum_attempts": budget.limit, "budget_used": budget.used,
                "budget_remaining": budget.limit - budget.used,
                "selected_quantity": format(current.quantity, "f"),
                "selected_quantity_tick": current.quantity_tick,
                "selected_hold_seconds": current.hold_seconds,
                "selected_parts": [format(part, "f") for part in current.receiver_parts],
            })
            try:
                prepared = await self._revalidate_fanout(config, current, journal, *initial)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                reason = _cycle_exception_reason(exc)
                retryable = _is_retryable_preparation_error(exc)
                journal.append("PREPARATION_FAILED", {"attempt": attempt, "retryable": retryable, "reason": reason})
                if retryable and budget.available:
                    if isinstance(exc, _OpeningQuantityRefresh):
                        journal.append("OPENING_QUANTITY_RECALCULATED", {
                            "attempt": attempt, "old_quantity": format(current.quantity, "f"),
                            "new_quantity": format(exc.selection.quantity, "f"),
                            "old_parts": [format(part, "f") for part in current.receiver_parts],
                            "new_parts": [format(part, "f") for part in exc.selection.receiver_parts],
                            "next_attempt": budget.used + 1, "reason": "fresh quote and confirmed margin budget",
                        })
                        current = exc.selection
                        self._selection = current
                    journal.append("PREPARATION_RETRY", {
                        "attempt": attempt, "next_attempt": budget.used + 1, "reason": reason,
                        "delay_seconds": config.poll_interval_seconds, "budget_used": budget.used,
                        "budget_remaining": budget.limit - budget.used,
                    })
                    await self.clock.sleep(config.poll_interval_seconds)
                    continue
                journal.append("PREPARATION_EXHAUSTED" if retryable else "PREPARATION_BLOCKED", {
                    "attempt": attempt, "maximum_attempts": budget.limit, "budget_used": budget.used,
                    "budget_remaining": budget.limit - budget.used, "reason": reason, "retryable": retryable,
                })
                terminal = (f"opening shared pair-attempt budget exhausted after {attempt}/{budget.limit}: {reason}"
                            if retryable else f"opening preparation blocked: {reason}")
                result = self._classified(FanoutCycleResult(
                    outcome=Outcome.FAILED_PREFLIGHT_BLOCKED, phase=Phase.PREFLIGHT, run_id=journal.run_id,
                    selection=current, reason=terminal, journal_path=str(config.journal_path),
                    receiver_accounts=config.receiver_account_indices), {})
                journal.append("CYCLE_PREFLIGHT_BLOCKED", result.as_dict())
                return result
            metadata, book, source, receivers, refreshed = prepared
            if (refreshed.opening_source_price != current.opening_source_price
                    or refreshed.opening_receiver_bound != current.opening_receiver_bound):
                journal.append("OPENING_PRICE_UPDATED", {
                    "attempt": attempt, "old_source_price": format(current.opening_source_price, "f"),
                    "new_source_price": format(refreshed.opening_source_price, "f"),
                    "old_receiver_bound": format(current.opening_receiver_bound, "f"),
                    "new_receiver_bound": format(refreshed.opening_receiver_bound, "f"),
                    "old_metadata_observed_at": current.metadata_observed_at,
                    "new_metadata_observed_at": refreshed.metadata_observed_at,
                    "old_book_observed_at": current.book_observed_at,
                    "new_book_observed_at": refreshed.book_observed_at,
                })
            if refreshed.bounds.as_dict() != current.bounds.as_dict():
                journal.append("OPENING_BOUNDS_REFRESHED", {"attempt": attempt, "old_bounds": current.bounds.as_dict(),
                                                            "new_bounds": refreshed.bounds.as_dict()})
            journal.append("PREPARATION_ACCEPTED", {"attempt": attempt, "selection": refreshed.as_dict()})
            return metadata, book, source, receivers, refreshed
        raise AssertionError("shared pair-attempt budget returned without a terminal result")

    async def _revalidate_fanout(self, config, selection: FanoutSelection, journal, initial_metadata, initial_book,
                                 initial_source: AccountSnapshot, initial_receivers: tuple[AccountSnapshot, ...]):
        metadata, source, receivers = await self._fanout_revalidation_context(
            config, market_read_label="opening revalidation market read")
        book = _as_book(await self._bounded(self._order_book(config.market_id), config,
                                            "opening revalidation order book read"), metadata)
        now = self.clock.now()
        _validate_market_book(config, metadata, book, now)
        proposal = select_automatic_prices(config.direction, metadata, book, now=now,
                                           freshness_seconds=config.freshness_seconds,
                                           price_improvement_ticks=config.price_improvement_ticks)
        dispatch_reserve = config.margin_reserve.dispatch_quote if config.margin_reserve else Decimal(0)
        planning_reserve = max(dispatch_reserve, config.margin_reserve.initial_quote if config.margin_reserve else dispatch_reserve)
        source_side = config.direction.source_side
        receiver_side = "BUY" if source_side == "SELL" else "SELL"
        legs = [("source", source, selection.quantity, proposal.source_limit_price, source_side),
                *((receiver_leg(position), account, part, proposal.receiver_worst_price, receiver_side)
                  for position, (account, part) in enumerate(zip(receivers, selection.receiver_parts), 1))]
        rows: list[dict[str, Any]] = []
        failures: list[tuple[str, int, Decimal]] = []
        errors: list[BaseException] = []
        units: dict[str, tuple[Decimal, Decimal]] = {}
        for label, account, quantity, worst, side in legs:
            role = "source" if label == "source" else "receiver"
            target = self._leverage_fractions.get(account.account_index)
            row: dict[str, Any] = {"role": label, "account_index": account.account_index,
                                   "quantity": format(quantity, "f"), "target_imf_bps": target,
                                   "observed_imf_bps": None, "worst_price": format(worst, "f"),
                                   "account_observed_at": account.observed_at, "status": "UNKNOWN"}
            if target is None:
                row["status"] = "NO_TARGET_IMF"
                rows.append(row)
                continue
            try:
                row["observed_imf_bps"] = _observed_leverage_fraction(account, role)
                balance = _available_balance(account, role)
                if account.margin_evidence is not None:
                    parts = _opening_budget_components(metadata, account, label=role, quantity=quantity,
                                                       worst_price=worst, side=side)
                else:
                    parts = {"mark_notional": quantity * proposal.source_limit_price, "fee_cost": Decimal(0),
                             "adverse_entry_loss": Decimal(0), "fee_rate": Decimal(0)}
                required = parts["mark_notional"] * Decimal(target) / 10000 + parts["fee_cost"] + parts["adverse_entry_loss"]
                headroom = balance - required
                units[label] = (balance, required / quantity)
                row.update({"available_balance": format(balance, "f"), "total_required": format(required, "f"),
                            "headroom": format(headroom, "f"), "fee_bound": format(parts["fee_cost"], "f"),
                            "status": "ADMITTED" if headroom >= dispatch_reserve else "INSUFFICIENT"})
                if headroom < dispatch_reserve:
                    failures.append((label, account.account_index, dispatch_reserve - headroom))
            except (PreflightBlocked, ContractError) as exc:
                row["calculation_error"] = _cycle_exception_reason(exc)
                errors.append(exc)
            rows.append(row)
        journal.append("FRESH_OPENING_MARGIN_BUDGET", {
            "observed_at": now, "quantity": format(selection.quantity, "f"),
            "initial_reserve_quote": (None if config.margin_reserve is None
                                      else format(config.margin_reserve.initial_quote, "f")),
            "dispatch_reserve_quote": format(dispatch_reserve, "f"), "legs": rows,
        })
        if not _exclusive_source_price_available(book, config.direction, proposal.source_limit_price):
            raise _RetryablePreparationFailure("public book does not permit an exclusive improved source price")
        if (source.signed_position != initial_source.signed_position
                or any(item.signed_position != before.signed_position
                       for item, before in zip(receivers, initial_receivers))):
            raise PreflightBlocked("account position changed before opening mutation")
        if errors:
            raise errors[0]
        for row in rows:
            if row["target_imf_bps"] is not None and row["observed_imf_bps"] != row["target_imf_bps"]:
                raise PreflightBlocked(f"{row['role']} leverage changed before opening mutation")
        for current, initial in zip((source, *receivers), (initial_source, *initial_receivers)):
            if current.source_identity != initial.source_identity:
                self._mark_identity_failure("account identity changed before opening mutation")
                raise PreflightBlocked("account identity changed before opening mutation")
        reason = "fresh available balance no longer funds the selected quantity"
        if failures:
            label, account_index, shortfall = failures[0]
            reason = (f"{label} selected quantity exceeds fresh free-balance margin model "
                      f"(account {account_index}; shortfall {format(shortfall, 'f')} quote)")
        try:
            fresh = compute_fanout_bounds(metadata, source, receivers, proposal.source_limit_price,
                                          receiver_bound=proposal.receiver_worst_price, direction=config.direction,
                                          initial_reserve_quote=dispatch_reserve)
        except PreflightBlocked:
            if failures:
                raise PreflightBlocked(reason) from None
            raise
        if fresh.quantity.size_step != selection.bounds.size_step:
            raise PreflightBlocked("opening size grid changed before mutation")
        parts = list(selection.receiver_part_ticks)
        if any(part < fresh.part_minimum_tick for part in parts):
            raise PreflightBlocked("a fan-out part no longer meets a fresh venue minimum")
        # The owner's rule holds at dispatch too: the LIMIT keeps k fresh minimums plus 10 %.
        if selection.quantity_tick < fresh.quantity.lower_tick:
            raise PreflightBlocked("the LIMIT no longer holds every receiver's venue minimum plus 10 %")
        over_cap = (selection.quantity_tick > fresh.quantity.upper_tick
                    or any(part > cap for part, cap in zip(parts, fresh.receiver_cap_ticks)))
        if failures or over_cap:
            step = fresh.quantity.size_step

            def affordable(label: str, reserve: Decimal) -> int | None:
                if label not in units:
                    return None
                balance, unit = units[label]
                return _floor((balance - reserve) / unit / step)

            resized = None
            for reserve in (planning_reserve, dispatch_reserve):
                source_limit = affordable("source", reserve)
                caps = [affordable(receiver_leg(position), reserve) for position in range(1, len(parts) + 1)]
                if source_limit is None or any(cap is None for cap in caps):
                    raise PreflightBlocked(reason)
                limit = min(source_limit, fresh.quantity.upper_tick, selection.quantity_tick)
                new_parts = [min(part, cap, fresh_cap) for part, cap, fresh_cap in
                             zip(parts, caps, fresh.receiver_cap_ticks)]
                excess = sum(new_parts) - limit
                for index in sorted(range(len(new_parts)), key=lambda i: new_parts[i], reverse=True):
                    if excess <= 0:
                        break
                    cut = min(excess, new_parts[index] - fresh.part_minimum_tick)
                    if cut > 0:
                        new_parts[index] -= cut
                        excess -= cut
                if (excess <= 0 and all(part >= fresh.part_minimum_tick for part in new_parts)
                        and sum(new_parts) >= fresh.quantity.lower_tick):
                    total = sum(new_parts)
                    if total < selection.quantity_tick:
                        resized = replace(selection, quantity=total * step, quantity_tick=total,
                                          receiver_part_ticks=tuple(new_parts), bounds=fresh.quantity,
                                          receiver_cap_ticks=fresh.receiver_cap_ticks,
                                          part_minimum_tick=fresh.part_minimum_tick)
                    break
            if resized is not None:
                raise _OpeningQuantityRefresh(reason, resized)
            raise PreflightBlocked(reason)
        refreshed = replace(
            selection, quantity=selection.quantity_tick * fresh.quantity.size_step,
            opening_source_price=proposal.source_limit_price, opening_receiver_bound=proposal.receiver_worst_price,
            bounds=fresh.quantity, receiver_cap_ticks=fresh.receiver_cap_ticks,
            part_minimum_tick=fresh.part_minimum_tick, metadata_observed_at=metadata.observed_at,
            book_observed_at=book.observed_at, best_bid_price=book.bids[0].price,
            best_bid_quantity=book.bids[0].quantity, best_ask_price=book.asks[0].price,
            best_ask_quantity=book.asks[0].quantity,
        )
        return metadata, book, source, receivers, refreshed

    # ----- handoffs -----------------------------------------------------------

    def _fanout_handoff_config(self, config: RandomCycleConfig, quantity: Decimal, parts: Sequence[Decimal],
                               receivers: Sequence[int], source_price: Decimal, receiver_bound: Decimal,
                               journal_path: Path, operation_mode: OperationMode, source_position: Decimal,
                               receiver_positions: Sequence[Decimal], *, attempt_index: int,
                               source_quote_observed_at: float | None) -> FanoutHandoffConfig:
        prefix = config.client_order_prefix
        if attempt_index != 1:
            prefix = f"{prefix}-{operation_mode.value.lower()}-{attempt_index:03d}"
        return FanoutHandoffConfig(
            market_id=config.market_id, market_symbol=config.market_symbol,
            direction=config.direction if operation_mode is OperationMode.PAIRED_OPENING else _inverse(config.direction),
            quantity=quantity, source_limit_price=source_price, receiver_worst_price=receiver_bound,
            freshness_seconds=config.freshness_seconds, request_timeout_seconds=config.request_timeout_seconds,
            order_timeout_seconds=config.order_timeout_seconds, reconcile_timeout_seconds=config.reconcile_timeout_seconds,
            poll_interval_seconds=config.poll_interval_seconds, max_poll_count=config.max_poll_count,
            source_order_lifetime_seconds=config.source_order_lifetime_seconds, client_order_prefix=prefix,
            journal_path=str(journal_path), environment=config.environment, operator_execution_opt_in=True,
            operator_plan_reviewed=True, api_base_url=config.api_base_url, api_key_index=config.api_key_index,
            chain_id=config.chain_id, auth_token_lifetime_seconds=config.auth_token_lifetime_seconds,
            operation_mode=operation_mode, attempt_index=attempt_index,
            expected_source_position=source_position, expected_receiver_position=receiver_positions[0],
            source_quote_observed_at=source_quote_observed_at, max_quote_age_seconds=config.max_quote_age_seconds,
            max_source_to_receiver_seconds=config.max_source_to_receiver_seconds,
            receiver_admission=config.receiver_admission,
            defer_incremental_margin_calculation=(config.defer_incremental_margin_calculation
                                                  if operation_mode is OperationMode.PAIRED_OPENING else False),
            receivers=tuple(FanoutReceiver(account, part, position)
                            for account, part, position in zip(receivers, parts, receiver_positions)),
        )

    async def _run_prepared_fanout(self, handoff: FanoutHandoffConfig, metadata: MarketMetadata,
                                   source: AccountSnapshot, receivers: Sequence[AccountSnapshot]):
        context = FanoutPreflightContext(handoff, metadata, source, tuple(receivers), dict(self._pending_nonces),
                                         self._nonce_deadline)
        client = _BoundFanoutClient(
            self.client, metadata, source_identity=source.source_identity,
            receiver_accounts=[item.account_index for item in receivers],
            receiver_identities=[item.source_identity for item in receivers],
            identity_failure_callback=self._mark_identity_failure, preflight_context=context)
        try:
            return await run_fanout_handoff(handoff, client, clock=self.clock)
        finally:
            await self._release_preflight_nonces()

    @staticmethod
    def _handoff_binding(handoff: FanoutHandoffConfig) -> dict[str, Any]:
        value = opening_config_binding(handoff)
        value["fanout_receivers"] = [item.as_dict() for item in handoff.receivers]
        return value

    # ----- the cycle ------------------------------------------------------------

    def _classified(self, result: FanoutCycleResult, remaining: Mapping[int, Decimal | None]) -> FanoutCycleResult:
        paired, inventory, economics = fanout_cycle_classifications(result.opening, result.closing,
                                                                     result.fallbacks, remaining)
        findings = tuple(dict.fromkeys(
            [item for phase in (result.opening, result.closing) for item in (getattr(phase, "economic_findings", ()) or ())]
            + [item for fallback in result.fallbacks for item in fallback.economic_findings]))
        return replace(result, paired_execution=paired, inventory=inventory, economics=economics,
                       economic_findings=findings,
                       boundary_books_available=(result.boundary_books_available
                                                 if result.boundary_books_available is not None
                                                 else _boundary_books_available(result.opening, result.closing)),
                       latency=_cycle_latency(result.opening, result.closing))

    def _result(self, config: RandomCycleConfig, journal: DurableJournal, *, outcome: Outcome, phase: Phase,
                selection: FanoutSelection, opening=None, closing=None, fallbacks=(),
                remaining: Mapping[int, Decimal | None] | None = None, reason: str | None = None) -> FanoutCycleResult:
        remaining = dict(remaining or {})
        receivers = config.receiver_account_indices
        result = FanoutCycleResult(
            outcome=outcome, phase=phase, run_id=journal.run_id, selection=selection, opening=opening,
            closing=closing, fallbacks=tuple(fallbacks),
            remaining_source_position=remaining.get(config.source_account_index),
            remaining_receiver_position=remaining.get(receivers[0]),
            remaining_source_position_observed_at=(None if remaining.get(config.source_account_index) is None
                                                   else self._last_observed_at(config.source_account_index)),
            remaining_receiver_position_observed_at=(None if remaining.get(receivers[0]) is None
                                                     else self._last_observed_at(receivers[0])),
            opening_reason=_opening_reason(opening), reason=reason, journal_path=str(config.journal_path),
            receiver_accounts=receivers,
            remaining_extra_positions=tuple(remaining.get(index) for index in receivers[1:]),
            remaining_extra_observed_at=tuple(None if remaining.get(index) is None else self._last_observed_at(index)
                                              for index in receivers[1:]),
        )
        return self._classified(result, remaining)

    @staticmethod
    def _residual_text(remaining: Mapping[int, Decimal | None], roles: Sequence[tuple[str, int]]) -> str | None:
        if not remaining or any(remaining.get(index) is None for _, index in roles):
            return None
        if all(remaining[index] == 0 for _, index in roles):
            return "final inventory confirmed flat"
        return "final inventory known residual: " + ", ".join(f"{label}={remaining[index]}" for label, index in roles)

    def _terminal_reason(self, config, journal, opening, closing, fallbacks, seed, remaining) -> str | None:
        base = _cycle_terminal_reason(opening, closing, fallbacks, seed, recovery_reason=_recovery_stop_reason(journal))
        residual = self._residual_text(remaining, self._roles(config))
        parts = [part for part in (base, residual) if part]
        return "; ".join(parts) if parts else None

    async def _execute_locked(self, config: RandomCycleConfig, journal: DurableJournal) -> RandomCycleResult:
        self._stage = "PREFLIGHT"
        start_stream = getattr(self.client, "start_read_stream", None)
        if callable(start_stream):
            self._read_stream_attempted = True
            await start_stream(ready_timeout=5)
        metadata, book, source, receivers, proposal = await self._initial_fanout_context(config, journal)
        bounds = compute_fanout_bounds(
            metadata, source, receivers, proposal.source_limit_price, receiver_bound=proposal.receiver_worst_price,
            direction=config.direction,
            initial_reserve_quote=config.margin_reserve.initial_quote if config.margin_reserve else Decimal(0))
        quantity, tick, hold, parts = select_fanout_quantity(bounds, self.rng)
        selection = FanoutSelection(
            quantity=quantity, quantity_tick=tick, hold_seconds=hold,
            opening_source_price=proposal.source_limit_price, opening_receiver_bound=proposal.receiver_worst_price,
            bounds=bounds.quantity, metadata_observed_at=metadata.observed_at, book_observed_at=book.observed_at,
            best_bid_price=book.bids[0].price, best_bid_quantity=book.bids[0].quantity,
            best_ask_price=book.asks[0].price, best_ask_quantity=book.asks[0].quantity,
            receiver_accounts=config.receiver_account_indices, receiver_part_ticks=parts,
            part_minimum_tick=bounds.part_minimum_tick, receiver_cap_ticks=bounds.receiver_cap_ticks,
        )
        self._selection = selection
        journal.append("SELECTION_PROVED", {"selection": selection.as_dict(), "metadata": _metadata_payload(metadata),
                                            "book_observed_at": book.observed_at})
        if self._owner_stop(journal, "BEFORE_LEVERAGE"):
            return self._owner_stopped_before_orders(config, journal, selection)
        source, receivers = await self._configure_fanout_leverage(config, journal, metadata, book, selection,
                                                                  source, receivers)
        budget = _PairAttemptBudget(limit=MAX_OPENING_ATTEMPTS)
        initial = (metadata, book, source, receivers)
        prepared = await self._prepare_fanout_with_retries(config, journal, selection, metadata, book, source,
                                                           receivers, budget)
        if isinstance(prepared, RandomCycleResult):
            journal.append("CYCLE_COMPLETE", prepared.as_dict())
            return prepared
        metadata, book, source, receivers, selection = prepared
        self._selection = selection
        if self._owner_stop(journal, "BEFORE_FIRST_ORDER"):
            return self._owner_stopped_before_orders(config, journal, selection)
        preparation_result: RandomCycleResult | None = None
        owner_stop_reason: str | None = None
        while True:
            attempt_index = budget.used
            path = _child_journal_path(config.opening_journal_path, attempt_index)
            handoff = self._fanout_handoff_config(
                config, selection.quantity, selection.receiver_parts, config.receiver_account_indices,
                selection.opening_source_price, selection.opening_receiver_bound, path, OperationMode.PAIRED_OPENING,
                source.signed_position, [item.signed_position for item in receivers], attempt_index=attempt_index,
                source_quote_observed_at=selection.book_observed_at)
            journal.append("OPENING_PLAN_READY", {
                "config": self._handoff_binding(handoff), "latency": dict(getattr(self, "_last_quote_read", {})),
                "selection": selection.as_dict(), "attempt": attempt_index,
                "lineage": {"used": budget.used, "limit": budget.limit},
            })
            journal.append("FIRST_MUTATION_BOUNDARY", {
                "message": "opening handoff admitted; later order state is authoritative only from its immutable child journal",
                "selection": selection.as_dict(), "attempt": attempt_index, "journal_path": handoff.journal_path,
            })
            self._stage = "OPENING"
            opening = await self._run_prepared_fanout(handoff, metadata, source, receivers)
            self._stage = "OPENING_RECONCILED"
            journal.append("OPENING_COMPLETE", {"result": opening.as_dict(), "attempt": attempt_index,
                                                "journal_path": handoff.journal_path})
            if not opening.retryable_pair:
                break
            if self._owner_stop(journal, "BEFORE_OPENING_RETRY"):
                owner_stop_reason = OWNER_STOP_RETRY_REASON
                break
            if not budget.available:
                journal.append("PAIR_ATTEMPT_EXHAUSTED", {"phase": "PAIRED_OPENING", "attempt": attempt_index,
                                                          "maximum_attempts": budget.limit,
                                                          "reason": "safe zero-fill guard retry exhausted"})
                break
            journal.append("PAIR_ATTEMPT_RETRY", {
                "phase": "PAIRED_OPENING", "from_attempt": attempt_index, "next_attempt": budget.used + 1,
                "reason": opening.reason, "guard": opening.priority_guard,
                "lineage": {"used": budget.used, "limit": budget.limit}, **_stream_settle_payload(opening, config)})
            await _stream_settle(self.clock, opening, config)
            retry = await self._prepare_fanout_with_retries(config, journal, selection, *initial, budget)
            if isinstance(retry, RandomCycleResult):
                preparation_result = retry
                break
            metadata, book, source, receivers, selection = retry
            self._selection = selection
        if not opening.mutual_execution_proven:
            fallbacks, remaining = await self._fanout_fallback_residuals(config, journal, selection, opening, None)
            unknown = any(value is None for value in remaining.values()) or any(
                item.outcome is Outcome.UNKNOWN for item in fallbacks)
            seed = (preparation_result.reason if preparation_result is not None
                    else owner_stop_reason or "paired opening did not prove a complete cycle")
            result = self._result(config, journal, outcome=Outcome.UNKNOWN if unknown else opening.outcome,
                                  phase=Phase.COMPLETE, selection=selection, opening=opening, fallbacks=fallbacks,
                                  remaining=remaining,
                                  reason=self._terminal_reason(config, journal, opening, None, fallbacks, seed, remaining))
            journal.append("CYCLE_COMPLETE", result.as_dict())
            return result

        anchor_wall = self.clock.now()
        anchor_mono = _clock_monotonic(self.clock)
        journal.append("HOLD_ANCHORED", {"hold_seconds": selection.hold_seconds, "anchor_wall": anchor_wall,
                                         "anchor_monotonic": anchor_mono})
        try:
            self._stage = "HOLD"
            held = await self._wait_hold(selection.hold_seconds, anchor_mono)
        except Exception as exc:
            result = self._result(config, journal, outcome=Outcome.UNKNOWN, phase=Phase.RECONCILIATION,
                                  selection=selection, opening=opening,
                                  reason=f"hold timer could not reach its persisted deadline: {sanitize_exception(exc)}")
            journal.append("CYCLE_COMPLETE", result.as_dict())
            return result
        if held is not None:
            journal.append("HOLD_ENDED_BY_OWNER_STOP", {"hold_seconds": selection.hold_seconds, "held_seconds": held})

        self._stage = "CLOSING"
        closing, seed = await self._run_fanout_close(config, journal, selection, opening)
        self._stage = "FALLBACK"
        fallbacks, remaining = await self._fanout_fallback_residuals(config, journal, selection, opening, closing)
        reconciled = bool(fallbacks) and all(item.outcome in {Outcome.PARTIAL, Outcome.SUCCESS}
                                             and item.position_after is not None for item in fallbacks)
        flat = bool(remaining) and all(value == 0 for value in remaining.values())
        if self._identity_barrier is not None:
            outcome = Outcome.UNKNOWN
        elif closing is not None and closing.mutual_execution_proven and not fallbacks and flat:
            outcome = Outcome.SUCCESS
        elif closing is not None and closing.outcome is Outcome.SUCCESS and reconciled and flat:
            outcome = Outcome.PARTIAL
        elif fallbacks and reconciled and flat:
            outcome = Outcome.PARTIAL
        elif any(value is None for value in remaining.values()) or not remaining:
            outcome = Outcome.UNKNOWN
        elif closing is not None and closing.outcome in {Outcome.UNKNOWN, Outcome.FAILED_PREFLIGHT_BLOCKED}:
            outcome = Outcome.UNKNOWN
        elif any(item.outcome is Outcome.UNKNOWN for item in fallbacks) or closing is None:
            outcome = Outcome.UNKNOWN
        else:
            outcome = Outcome.PARTIAL
        recovered = bool(fallbacks) or (closing is not None and closing.outcome is not Outcome.SUCCESS)
        reason = None
        if outcome is not Outcome.SUCCESS or recovered:
            reason = (self._identity_barrier
                      or self._terminal_reason(config, journal, opening, closing, fallbacks, seed, remaining)
                      or "cycle closure did not prove exact flat positions")
        result = self._result(config, journal, outcome=outcome, phase=Phase.COMPLETE, selection=selection,
                              opening=opening, closing=closing, fallbacks=fallbacks, remaining=remaining, reason=reason)
        journal.append("CYCLE_COMPLETE", result.as_dict())
        return result

    async def _run_fanout_close(self, config: RandomCycleConfig, journal: DurableJournal, selection: FanoutSelection,
                                opening: FanoutHandoffResult):
        def blocked(reason: str):
            journal.append("CLOSING_BLOCKED", {"reason": reason})
            return None, reason

        try:
            plan = opening.plan
            legs = [opening.source, *opening.receivers]
            if not opening.mutual_execution_proven or plan is None or any(leg is None for leg in legs):
                return blocked("opening reconciliation did not prove close lineage")
            if any(leg.position_after is None or not leg.history_complete or leg.unknown_reasons for leg in legs):
                return blocked("opening reconciliation is incomplete for a dependent close")
            budget = _PairAttemptBudget(limit=MAX_CLOSING_ATTEMPTS)
            source_sign, receiver_sign = _expected_cycle_signs(config.direction)
            while budget.available:
                attempt_index = budget.consume()
                started = time.perf_counter()
                try:
                    try:
                        metadata, source, receivers = await self._fanout_revalidation_context(
                            config, market_read_label="closing market read", warm_closing_transport=True)
                    except _AccountIdentityFailure:
                        self._mark_identity_failure("closing account identity/read validation failed")
                        raise
                    book = _as_book(await self._bounded(self._order_book(config.market_id), config,
                                                        "closing order book read"), metadata)
                    now = self.clock.now()
                    _validate_market_book(config, metadata, book, now)
                    if source.source_identity != plan.source_identity or any(
                            item.source_identity != identity for item, identity in zip(receivers, plan.receiver_identities)):
                        self._mark_identity_failure("account identity changed during the holding period")
                        return blocked("account identity changed during the holding period")
                    if any(item.signed_position != leg.position_after for item, leg in zip((source, *receivers), legs)):
                        return blocked("account position changed during the holding period")
                    source_residual = _position_residual(source.signed_position, source_sign, selection.quantity)
                    residuals = [_position_residual(item.signed_position, receiver_sign, part)
                                 for item, part in zip(receivers, selection.receiver_parts)]
                    if source_residual is None or any(value is None for value in residuals):
                        return blocked("cycle position changed direction or exceeded the selected quantity before paired close")
                    minimum = selection.part_minimum_tick * selection.bounds.size_step
                    members = [(item, value) for item, value in zip(receivers, residuals) if value >= minimum]
                    total = sum((value for _, value in members), Decimal(0))
                    if len(members) < 2 or total <= 0 or total > source_residual:
                        return blocked("fan-out close has no two-receiver confirmed residual")
                    close_direction = _inverse(config.direction)
                    proposal = select_automatic_prices(close_direction, metadata, book, quantity=total, now=now,
                                                       freshness_seconds=config.freshness_seconds,
                                                       price_improvement_ticks=config.price_improvement_ticks)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    await self._release_preflight_nonces()
                    reason = _cycle_exception_reason(exc)
                    retryable = self._identity_barrier is None and _is_retryable_preparation_error(exc)
                    journal.append("CLOSING_PREPARATION_FAILED", {
                        "attempt": attempt_index, "reason": reason, "retryable": retryable,
                        "latency_seconds": max(0.0, time.perf_counter() - started),
                        "lineage": {"used": budget.used, "limit": budget.limit}})
                    if not retryable:
                        return blocked(reason)
                    if not budget.available:
                        journal.append("PAIR_ATTEMPT_EXHAUSTED", {"phase": "PAIRED_CLOSING", "attempt": attempt_index,
                                                                  "maximum_attempts": budget.limit, "reason": reason})
                        return None, f"paired closing shared pair-attempt budget exhausted ({budget.used}/{budget.limit}): {reason}"
                    journal.append("CLOSING_PREPARATION_RETRY", {
                        "attempt": attempt_index, "next_attempt": budget.used + 1, "reason": reason,
                        "delay_seconds": config.poll_interval_seconds,
                        "lineage": {"used": budget.used, "limit": budget.limit}})
                    await self.clock.sleep(config.poll_interval_seconds)
                    continue
                if not _exclusive_source_price_available(book, close_direction, proposal.source_limit_price):
                    reason = "public book does not permit an exclusive improved closing source price"
                    journal.append("CLOSING_PREPARATION_FAILED", {
                        "attempt": attempt_index, "reason": reason, "retryable": True,
                        "source_price": format(proposal.source_limit_price, "f"), "book_observed_at": book.observed_at,
                        "lineage": {"used": budget.used, "limit": budget.limit}})
                    if budget.available:
                        journal.append("CLOSING_PREPARATION_RETRY", {
                            "attempt": attempt_index, "next_attempt": budget.used + 1, "reason": reason,
                            "delay_seconds": config.poll_interval_seconds,
                            "lineage": {"used": budget.used, "limit": budget.limit}})
                        await self.clock.sleep(config.poll_interval_seconds)
                        continue
                    journal.append("PAIR_ATTEMPT_EXHAUSTED", {"phase": "PAIRED_CLOSING", "attempt": attempt_index,
                                                              "maximum_attempts": budget.limit, "reason": reason})
                    return None, f"paired closing shared pair-attempt budget exhausted ({budget.used}/{budget.limit})"
                handoff = self._fanout_handoff_config(
                    config, total, [value for _, value in members], [item.account_index for item, _ in members],
                    proposal.source_limit_price, proposal.receiver_worst_price,
                    _child_journal_path(config.closing_journal_path, attempt_index), OperationMode.PAIRED_CLOSING,
                    source.signed_position, [item.signed_position for item, _ in members],
                    attempt_index=attempt_index, source_quote_observed_at=book.observed_at)
                journal.append("CLOSING_PLAN_READY", {
                    "config": self._handoff_binding(handoff),
                    "http_warmup": dict(getattr(self, "_closing_http_warmup", {})),
                    "latency": {**dict(getattr(self, "_last_quote_read", {})),
                                "preparation_seconds": max(0.0, time.perf_counter() - started)},
                    "paired_quantity": format(total, "f"), "attempt": attempt_index,
                    "lineage": {"used": budget.used, "limit": budget.limit}})
                closing = await self._run_prepared_fanout(handoff, metadata, source, [item for item, _ in members])
                journal.append("CLOSING_COMPLETE", {"result": closing.as_dict(), "attempt": attempt_index,
                                                    "journal_path": handoff.journal_path})
                if not closing.retryable_pair:
                    return closing, None
                if not budget.available:
                    reason = f"paired closing shared pair-attempt budget exhausted ({budget.used}/{budget.limit})"
                    journal.append("PAIR_ATTEMPT_EXHAUSTED", {"phase": "PAIRED_CLOSING", "attempt": attempt_index,
                                                              "maximum_attempts": budget.limit, "reason": reason})
                    return closing, reason
                journal.append("PAIR_ATTEMPT_RETRY", {
                    "phase": "PAIRED_CLOSING", "from_attempt": attempt_index, "next_attempt": budget.used + 1,
                    "reason": closing.reason, "guard": closing.priority_guard,
                    "lineage": {"used": budget.used, "limit": budget.limit}, **_stream_settle_payload(closing, config)})
                await _stream_settle(self.clock, closing, config)
            return blocked(f"paired closing shared pair-attempt budget exhausted ({budget.used}/{budget.limit})")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            reason = _cycle_exception_reason(exc)
            journal.append("CLOSING_BLOCKED", {"reason": reason})
            return None, reason
        finally:
            # Every exit, including an exhausted price guard, precedes recovery.
            await self._release_preflight_nonces()

    # ----- residual recovery ----------------------------------------------------------

    def _positions(self, source: AccountSnapshot, receivers: Sequence[AccountSnapshot]) -> dict[int, Decimal]:
        return {item.account_index: item.signed_position for item in (source, *receivers)}

    async def _fanout_fallback_residuals(self, config: RandomCycleConfig, journal: DurableJournal,
                                         selection: FanoutSelection, opening: Any, closing: Any):
        roles = self._roles(config)
        unknown = {index: None for _, index in roles}
        try:
            source, receivers = await self._recovery_fanout_accounts(config, journal)
        except _AccountIdentityFailure:
            self._mark_identity_failure("fallback starting account identity/read validation failed")
            journal.append("FALLBACK_RECONCILIATION_UNKNOWN", {"reason": self._identity_barrier})
            return [], unknown
        except Exception as exc:
            journal.append("FALLBACK_RECONCILIATION_UNKNOWN", {
                "reason": f"fallback starting account state is unknown: {_cycle_exception_reason(exc)}"})
            return [], unknown
        positions = self._positions(source, receivers)
        if self._identity_barrier is not None:
            journal.append("FALLBACK_BLOCKED_IDENTITY_BARRIER", {"reason": self._identity_barrier})
            return [], positions
        if opening.outcome not in {Outcome.PARTIAL, Outcome.SUCCESS}:
            return [], positions
        bound = closing
        if bound is not None and bound.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED:
            bound = None
        if bound is not None and bound.outcome is Outcome.UNKNOWN:
            return [], positions
        plan = opening.plan
        identities = {} if plan is None else {plan.source.account_index: plan.source_identity,
                                              **dict(zip((order.account_index for order in plan.receivers),
                                                         plan.receiver_identities))}
        for snapshot in (source, *receivers):
            if identities.get(snapshot.account_index) not in (None, snapshot.source_identity):
                self._mark_identity_failure("account identity changed during fallback admission")
        if self._identity_barrier is not None:
            journal.append("FALLBACK_BLOCKED_IDENTITY_BARRIER", {"reason": self._identity_barrier})
            return [], positions
        legs: dict[int, Any] = {}
        for phase in (opening, bound):
            for leg in _phase_legs(phase):
                if leg is not None:
                    legs[leg.account_index] = leg
        if (plan is None or set(legs) != set(positions)
                or any(leg.position_after is None or not leg.history_complete or leg.unknown_reasons
                       for leg in legs.values())
                or any(positions[index] != leg.position_after for index, leg in legs.items())):
            journal.append("FALLBACK_STOPPED_STATE_CHANGED", {
                "reason": "fresh account state is not causally bound to the reconciled cycle position"})
            return [], positions
        source_sign, receiver_sign = _expected_cycle_signs(config.direction)
        caps = {config.source_account_index: selection.quantity,
                **dict(zip(config.receiver_account_indices, selection.receiver_parts))}
        signs = {config.source_account_index: source_sign,
                 **{index: receiver_sign for index in config.receiver_account_indices}}
        residuals = {index: _position_residual(positions[index], signs[index], caps[index]) for index in positions}
        if closing is None and opening.mutual_execution_proven and all(value == 0 for value in residuals.values()):
            return [], {index: Decimal(0) for index in positions}
        if any(value is None for value in residuals.values()):
            journal.append("FALLBACK_STOPPED_STATE_CHANGED", {
                "reason": "cycle residual changed direction or exceeded the selected quantity"})
            return [], positions
        snapshots = {item.account_index: item for item in (source, *receivers)}
        return await self._close_fanout_accounts(config, journal, snapshots, caps)

    async def _close_fanout_accounts(self, config: RandomCycleConfig, journal: DurableJournal,
                                     current: dict[int, AccountSnapshot], caps: Mapping[int, Decimal]):
        """The 1 -> 1 residual loop over every account of the cycle."""
        roles = self._roles(config)
        order = [index for _, index in roles]
        for label, index in roles:
            _validate_account_fresh(config, current[index], "source" if label == "source" else "receiver", self.clock.now())
        signs = {index: 1 if current[index].signed_position >= 0 else -1 for index in order}
        results: list[FallbackResult] = []
        pending = [index for index in order if abs(current[index].signed_position) > 0]
        blocked: set[int] = set()
        ready_at = {index: _clock_monotonic(self.clock) for index in pending}
        attempt_ordinal = 0
        unknown = {index: None for index in order}

        async def fresh_accounts() -> dict[int, AccountSnapshot] | None:
            try:
                source, receivers = await self._recovery_fanout_accounts(config, journal)
                return {item.account_index: item for item in (source, *receivers)}
            except asyncio.CancelledError:
                raise
            except _AccountIdentityFailure:
                self._mark_identity_failure("fallback account identity/read validation failed")
                journal.append("FALLBACK_RECONCILIATION_UNKNOWN", {"reason": self._identity_barrier})
                return None
            except Exception as exc:
                journal.append("FALLBACK_RECONCILIATION_UNKNOWN", {
                    "reason": f"fallback account state is unknown: {_cycle_exception_reason(exc)}"})
                return None

        def positions(snapshots: Mapping[int, AccountSnapshot]) -> dict[int, Decimal]:
            return {index: snapshots[index].signed_position for index in order}

        while pending:
            account_index = pending.pop(0)
            if account_index in blocked:
                continue
            before = current[account_index]
            residual = _position_residual(before.signed_position, signs[account_index], caps[account_index])
            if residual is None:
                journal.append("FALLBACK_STOPPED_STATE_CHANGED", {
                    "account_index": account_index,
                    "reason": "cycle residual changed direction or exceeded the selected quantity"})
                return results, positions(current)
            if residual <= 0:
                continue
            now_monotonic = _clock_monotonic(self.clock)
            if now_monotonic < ready_at.get(account_index, now_monotonic):
                pending.append(account_index)
                next_ready = min(ready_at.get(candidate, now_monotonic) for candidate in pending)
                if all(_clock_monotonic(self.clock) < ready_at.get(candidate, now_monotonic) for candidate in pending):
                    await self.clock.sleep(max(0.0, next_ready - now_monotonic))
                continue
            attempt_ordinal += 1
            reserve = getattr(self.client, "reserve_order_nonce", None)
            invalidate = getattr(self.client, "invalidate_reserved_nonce", None)
            prepared_capable = (callable(reserve) and callable(invalidate)
                                and callable(getattr(self.client, "prepare_order", None))
                                and callable(getattr(self.client, "submit_prepared_order", None))
                                and callable(getattr(self.client, "invalidate_prepared_order", None)))
            reserved_nonce = None
            result = None
            if prepared_capable:
                try:
                    reserved_nonce = await self._bounded(
                        reserve(account_index, deadline=time.monotonic() + config.freshness_seconds),
                        config, "fallback pre-quote nonce reservation")
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    reason = f"fallback pre-quote nonce reservation failed: {sanitize_exception(exc)}"
                    journal.append("FALLBACK_PREPARATION_FAILED", {"account_index": account_index,
                                                                   "attempt": attempt_ordinal, "reason": reason,
                                                                   "sent": False})
                    result = FallbackResult(account_index, "SELL" if before.signed_position > 0 else "BUY",
                                            residual, False, Outcome.PARTIAL, reason=reason, attempt=attempt_ordinal)
            if result is None:
                try:
                    result = await self._fallback_one(config, journal, before, residual,
                                                      attempt_ordinal=attempt_ordinal, reserved_nonce=reserved_nonce)
                finally:
                    if reserved_nonce is not None:
                        await invalidate(reserved_nonce)
            results.append(result)
            if self._identity_barrier is not None and result.outcome is not Outcome.UNKNOWN:
                observed = await fresh_accounts()
                if observed is None:
                    return results, unknown
                journal.append("FALLBACK_BLOCKED_IDENTITY_BARRIER", {"reason": self._identity_barrier})
                return results, positions(observed)
            if result.outcome is Outcome.UNKNOWN:
                observed = await fresh_accounts()
                return results, (unknown if observed is None else positions(observed))
            observed = await fresh_accounts()
            if observed is None:
                return results, unknown
            journal.append("FALLBACK_POST_ATTEMPT_ACCOUNT_OBSERVATION", {
                "attempt": attempt_ordinal, "account_index": account_index,
                "reconciliation_state": result.reconciliation_state,
                "source": _account_payload(observed[config.source_account_index]),
                "receiver": _account_payload(observed[config.receiver_account_index]),
                "accounts": {str(index): _account_payload(observed[index]) for index in order},
            })
            if any(observed[index].source_identity != current[index].source_identity for index in order):
                self._mark_identity_failure("fresh account identity changed after the fallback reconciliation")
                journal.append("FALLBACK_STOPPED_STATE_CHANGED", {
                    "attempt": attempt_ordinal, "account_index": account_index,
                    "reason": "fresh account identity changed after the fallback reconciliation"})
                return results, unknown
            expected = {index: current[index].signed_position for index in order}
            if result.position_after is not None:
                expected[account_index] = result.position_after
            if any(observed[index].signed_position != expected[index] for index in order):
                journal.append("FALLBACK_STOPPED_STATE_CHANGED", {
                    "attempt": attempt_ordinal, "account_index": account_index,
                    "reason": "fresh account positions disagree with the reconciled fallback result"})
                return results, unknown
            if result.reconciliation_state == "REJECTED" and result.position_after is None:
                result = replace(result, position_after=observed[account_index].signed_position,
                                 position_observed_at=observed[account_index].observed_at)
                results[-1] = result
            current = observed
            fresh_residuals = {index: _position_residual(current[index].signed_position, signs[index], caps[index])
                               for index in order}
            if any(value is None for value in fresh_residuals.values()):
                journal.append("FALLBACK_STOPPED_STATE_CHANGED", {
                    "attempt": attempt_ordinal, "account_index": account_index,
                    "reason": "fresh account position is outside the cycle direction or quantity cap"})
                return results, unknown
            next_residual = fresh_residuals[account_index]
            if next_residual is None or next_residual <= 0:
                continue
            if result.reconciliation_state == "REJECTED" or result.position_after is None:
                blocked.add(account_index)
                continue
            ready_at[account_index] = _clock_monotonic(self.clock) + (
                config.poll_interval_seconds if result.filled_quantity == 0 else 0.0)
            if account_index not in pending:
                pending.append(account_index)
        try:
            source, receivers = await self._recovery_fanout_accounts(config, journal)
            final = {item.account_index: item for item in (source, *receivers)}
            if any(final[index].source_identity != current[index].source_identity for index in order):
                self._mark_identity_failure("final fallback account identity changed after reconciliation")
                journal.append("FALLBACK_STOPPED_STATE_CHANGED", {
                    "reason": "final fallback account identity changed after reconciliation"})
                return results, unknown
            if any(final[index].signed_position != current[index].signed_position for index in order):
                journal.append("FALLBACK_STOPPED_STATE_CHANGED", {
                    "reason": "final fallback account position changed after reconciliation"})
                return results, unknown
            return results, positions(final)
        except asyncio.CancelledError:
            raise
        except _AccountIdentityFailure:
            self._mark_identity_failure("final fallback account identity/read validation failed")
            journal.append("FALLBACK_RECONCILIATION_UNKNOWN", {"reason": self._identity_barrier})
            return results, unknown
        except Exception as exc:
            journal.append("FALLBACK_RECONCILIATION_UNKNOWN", {
                "reason": f"fallback final account state is unknown: {_cycle_exception_reason(exc)}"})
            return results, unknown


__all__ = [
    "FANOUT_SIZE_MARGIN",
    "FanoutBounds",
    "FanoutCycleEngine",
    "FanoutCycleResult",
    "FanoutSelection",
    "compute_fanout_bounds",
    "fanout_cycle_classifications",
    "fanout_part_minimum",
    "fanout_quantity_floor",
    "select_fanout_quantity",
    "split_fanout_ticks",
]
