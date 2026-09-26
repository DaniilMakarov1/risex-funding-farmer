"""Owner-managed wallet pool and the per-cycle uniform wallet-pair draw.

The pool lives in ``wallets.json`` next to the operator configuration.  When
that file is absent the pool is exactly the configured source/receiver pair,
so existing two-account operation is unchanged.  The file is re-read and fully
validated by every readiness check and every cycle; it holds only public
account indices, never credentials.

Every cycle draws two distinct wallets uniformly at random from the *eligible*
active wallets (readable active account, exactly flat on the configured
market, a stored credential for the configured API key index, and free
balance for the venue-minimum order at the owner's 4x cap plus the planning
reserve).  Ineligible wallets are skipped and recorded; with fewer than two
eligible wallets no cycle starts.  Paused wallets are never drawn but remain
covered by readiness checks and position closing.  Roles and direction are
then drawn exactly as before by ``select_random_route``.

A fan-out draw (owner request 2026-09-26: one LIMIT filled by several
MARKETs) instead picks one source and 2..(eligible - 1) receivers; see
``draw_fanout``.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING
import json
import math
import os
from pathlib import Path
import random
import stat
import time
from typing import Any, Callable, Mapping, Sequence
import uuid

from .contracts import ContractError, PreflightBlocked

POOL_FILE_NAME = "wallets.json"
POOL_SCHEMA = "hood-wallet-pool-v1"
SELECTION_SCHEMA = "hood-wallet-selection-v1"
# Finite technical bound: every readiness check reads each wallet.
MAX_POOL_WALLETS = 1000
MAX_POOL_FILE_BYTES = 256 * 1024
# Concurrent public account reads during selection.
SELECTION_READ_CONCURRENCY = 4

SKIP_REASONS = {
    "LOW_BALANCE": "мало баланса",
    "NOT_FLAT": "есть позиция",
    "KEY_MISSING": "нет ключа",
    "UNAVAILABLE": "счёт недоступен",
}


class WalletPoolError(PreflightBlocked):
    """The wallet pool file is missing required structure or is unsafe."""


class WalletsUnavailable(PreflightBlocked):
    """Fewer than two wallets (three for a fan-out cycle) are eligible for the next cycle."""


@dataclass(frozen=True, slots=True)
class WalletPool:
    active: tuple[int, ...]
    paused: tuple[int, ...] = ()
    from_file: bool = False

    @property
    def all(self) -> tuple[int, ...]:
        return self.active + self.paused

    def as_dict(self) -> dict[str, Any]:
        return {"active": list(self.active), "paused": list(self.paused), "from_file": self.from_file}


@dataclass(frozen=True, slots=True)
class WalletState:
    """Public account state used only to decide eligibility, never as proof."""

    account_index: int
    available_balance: Decimal
    signed_position: Decimal
    ready: bool
    observed_at: float


def _index(value: Any, name: str = "wallet account index") -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0 or value > 2 ** 48:
        raise WalletPoolError(f"{name} must be a positive integer")
    return value


def pool_path(operator_dir: Path | str) -> Path:
    return Path(operator_dir) / POOL_FILE_NAME


def _configured_pair(config: Any) -> tuple[int, int]:
    return (_index(config.source_account_index, "configured source account"),
            _index(config.receiver_account_index, "configured receiver account"))


def load_wallet_pool(operator_dir: Path | str, config: Any) -> WalletPool:
    """Validated pool; the configured pair when no pool file exists."""

    path = pool_path(operator_dir)
    if not path.is_symlink() and not path.exists():
        # Legacy two-account operation: the configured pair, unchanged.
        return WalletPool(active=(config.source_account_index, config.receiver_account_index),
                          paused=(), from_file=False)
    pair = _configured_pair(config)
    if path.is_symlink():
        raise WalletPoolError("wallet pool file must not be a symlink")
    try:
        info = path.stat()
    except OSError:
        raise WalletPoolError("wallet pool file is unreadable") from None
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_mode & 0o077 or info.st_size > MAX_POOL_FILE_BYTES):
        raise WalletPoolError("wallet pool file must be a small owner-only regular file")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, UnicodeDecodeError):
        raise WalletPoolError("wallet pool file is not valid JSON") from None
    if (not isinstance(value, dict) or value.get("schema") != POOL_SCHEMA
            or not set(value) <= {"schema", "wallets", "paused"}
            or not isinstance(value.get("wallets"), list)
            or not isinstance(value.get("paused", []), list)):
        raise WalletPoolError("wallet pool file has an unsupported structure")
    active = tuple(_index(item) for item in value["wallets"])
    paused = tuple(_index(item) for item in value.get("paused", []))
    every = active + paused
    if len(set(every)) != len(every):
        raise WalletPoolError("wallet pool lists a wallet twice")
    if len(active) < 2:
        raise WalletPoolError("wallet pool needs at least two active wallets")
    if len(every) > MAX_POOL_WALLETS:
        raise WalletPoolError("wallet pool exceeds its bound")
    if any(index not in every for index in pair):
        # The configured accounts carry the existing history and credentials.
        raise WalletPoolError("configured source/receiver accounts must stay in the wallet pool")
    return WalletPool(active=active, paused=paused, from_file=True)


def save_wallet_pool(operator_dir: Path | str, pool: WalletPool) -> None:
    """Atomic owner-only replacement; callers validate the new pool first."""

    directory = Path(operator_dir)
    target = pool_path(directory)
    if target.is_symlink():
        raise WalletPoolError("wallet pool file must not be a symlink")
    payload = {"schema": POOL_SCHEMA, "wallets": list(pool.active)}
    if pool.paused:
        payload["paused"] = list(pool.paused)
    temporary = directory / f".wallets-{uuid.uuid4().hex}.tmp"
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=1)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    except BaseException:
        try:
            temporary.unlink()
        except OSError:
            pass
        raise
    directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def updated_pool(pool: WalletPool, *, add: int | None = None, pause: int | None = None,
                 resume: int | None = None) -> WalletPool:
    """Return one validated membership change; the file is written separately."""

    active, paused = list(pool.active), list(pool.paused)
    if add is not None:
        index = _index(add)
        if index in active or index in paused:
            raise WalletPoolError("wallet is already in the pool")
        active.append(index)
    if pause is not None:
        index = _index(pause)
        if index not in active:
            raise WalletPoolError("only an active wallet can be paused")
        if len(active) <= 2:
            raise WalletPoolError("at least two active wallets must remain")
        active.remove(index)
        paused.append(index)
    if resume is not None:
        index = _index(resume)
        if index not in paused:
            raise WalletPoolError("only a paused wallet can be resumed")
        paused.remove(index)
        active.append(index)
    if len(active) + len(paused) > MAX_POOL_WALLETS:
        raise WalletPoolError("wallet pool exceeds its bound")
    return WalletPool(active=tuple(active), paused=tuple(paused), from_file=True)


# Eligibility is decided before the cycle's own fresh sizing; keep a small
# margin so a borderline wallet is skipped instead of failing its cycle.
REQUIREMENT_SAFETY = Decimal("1.05")


def wallet_requirement(metadata: Any, book: Any, config: Any, *,
                       fanout_parts: int | None = None) -> Decimal:
    """Conservative quote balance for one venue-minimum order in either role.

    With ``fanout_parts`` the order is instead the fan-out LIMIT floor for that
    many receivers (``ceil(parts x minimum x 1.10)``): ``1`` is one receiver's
    minimum part plus the margin, ``k`` is the source LIMIT for ``k`` receivers.

    The cycle prices both legs inside the spread, so the best ask bounds the
    notional/fee price and the best bid bounds the venue quote minimum.  Uses
    the owner's 4x cap (or a stricter venue minimum margin fraction), the
    larger published/observed fee rate, the worse adverse distance to mark on
    either side, a 5% margin and the planning reserve.  The drawn pair is
    still fully sized and checked by the cycle itself.
    """

    from .random_cycle import ROBINHOOD_MAKER_FEE_CAP, ROBINHOOD_TAKER_FEE_CAP

    ask = Decimal(book.asks[0].price)
    bid = Decimal(book.bids[0].price)
    if not ask.is_finite() or not bid.is_finite() or ask <= 0 or bid <= 0:
        raise ContractError("book prices are invalid for wallet eligibility")
    high, low = max(ask, bid), min(ask, bid)
    mark = Decimal(metadata.mark_price) if getattr(metadata, "mark_price", None) is not None else high
    step = Decimal(1).scaleb(-int(metadata.size_decimals))
    lower_base = int((Decimal(metadata.minimum_base_amount) / step).to_integral_value(rounding=ROUND_CEILING))
    lower_quote = int((Decimal(metadata.minimum_quote_amount) / low / step).to_integral_value(rounding=ROUND_CEILING))
    minimum_tick = max(lower_base, lower_quote, 1)
    if fanout_parts is not None:
        from .fanout_cycle import fanout_quantity_floor
        if isinstance(fanout_parts, bool) or not isinstance(fanout_parts, int) or fanout_parts < 1:
            raise ContractError("fan-out part count must be a positive integer")
        minimum_tick = fanout_quantity_floor(fanout_parts, minimum_tick)
    quantity = minimum_tick * step
    fraction = max(2500, int(getattr(metadata, "minimum_initial_margin_fraction", None) or 2500))
    rates = [ROBINHOOD_MAKER_FEE_CAP, ROBINHOOD_TAKER_FEE_CAP]
    for name in ("source_fee_rate", "receiver_fee_rate"):
        rate = getattr(metadata, name, None)
        if rate is not None:
            rates.append(Decimal(rate))
    adverse = max(Decimal(0), high - mark, mark - low)
    reserve = Decimal(0)
    margin_reserve = getattr(config, "margin_reserve", None)
    if margin_reserve is not None:
        reserve = Decimal(margin_reserve.initial_quote)
    variable = quantity * (mark * Decimal(fraction) / 10000 + high * max(rates) + adverse)
    return variable * REQUIREMENT_SAFETY + reserve


def draw_pair(eligible: Sequence[int], rng: Any = None) -> tuple[int, int]:
    """Uniform unordered pair of distinct wallets (order is drawn separately)."""

    from .random_cycle import _draw_integer

    ordered = sorted(set(eligible))
    if len(ordered) < 2:
        raise WalletsUnavailable("fewer than two eligible wallets")
    source = random.SystemRandom() if rng is None else rng
    first = _draw_integer(source, 0, len(ordered) - 1, "first wallet")
    second = _draw_integer(source, 0, len(ordered) - 2, "second wallet")
    if second >= first:
        second += 1
    return ordered[first], ordered[second]


def draw_fanout(receiver_ready: Sequence[int], capacity: Mapping[int, int],
                rng: Any = None) -> tuple[int, tuple[int, ...]] | None:
    """Uniform fan-out draw: one source and ``k`` distinct receivers (owner request 2026-09-26).

    ``receiver_ready`` are the eligible wallets that fund one receiver part (a
    venue minimum plus the 10 % margin); ``capacity[w]`` is the largest
    receiver count whose LIMIT wallet ``w`` funds as the source.  The source is
    uniform among wallets with capacity >= 2 and at least two other ready
    wallets; ``k`` is uniform in [2, min(other ready wallets, capacity, 16)];
    the receivers are uniform without replacement, in drawn order.  None when
    no such source exists.
    """

    from .fanout_handoff import MAX_FANOUT_RECEIVERS
    from .random_cycle import _draw_integer

    ordered = sorted(set(receiver_ready))
    sources = [index for index in ordered if int(capacity.get(index, 0)) >= 2]
    if len(ordered) < 3 or not sources:
        return None
    draw = random.SystemRandom() if rng is None else rng
    source = sources[_draw_integer(draw, 0, len(sources) - 1, "fan-out source")]
    top = min(len(ordered) - 1, int(capacity[source]), MAX_FANOUT_RECEIVERS)
    count = _draw_integer(draw, 2, top, "fan-out receiver count")
    candidates = [index for index in ordered if index != source]
    receivers = tuple(candidates.pop(_draw_integer(draw, 0, len(candidates) - 1, "fan-out receiver"))
                      for _ in range(count))
    return source, receivers


def _fanout_selection(eligible: Sequence[int], available: Mapping[int, Decimal],
                      metadata: Any, book: Any, config: Any, rng: Any) -> dict[str, Any]:
    """The fan-out part of the durable selection record (display/audit; the route stays authoritative)."""

    from .fanout_handoff import MAX_FANOUT_RECEIVERS

    receiver_requirement = wallet_requirement(metadata, book, config, fanout_parts=1)
    ready = [index for index in eligible if available[index] >= receiver_requirement]
    # The source LIMIT for k receivers; the requirement grows with k.
    source_requirements = {count: wallet_requirement(metadata, book, config, fanout_parts=count)
                           for count in range(2, min(len(ready) - 1, MAX_FANOUT_RECEIVERS) + 1)}
    capacity = {index: max((count for count, need in source_requirements.items() if available[index] >= need),
                           default=0)
                for index in ready}
    drawn = draw_fanout(ready, capacity, rng)
    return {
        "receiver_ready": ready,
        "receiver_requirement_quote": format(receiver_requirement, "f"),
        "source_requirement_quote": {str(count): format(need, "f") for count, need in source_requirements.items()},
        "source_capacity": {str(index): capacity[index] for index in ready},
        "source": None if drawn is None else drawn[0],
        "receivers": None if drawn is None else list(drawn[1]),
    }


async def _rate_limited_read(read: Callable[[], Any], config: Any, sleep: Callable[[float], Any]) -> Any:
    """One public read; only a typed HTTP 429 is retried, within the configured bounds."""

    from .read_errors import read_rate_limit_delay

    timeout = float(config.request_timeout_seconds)
    deadline = time.monotonic() + float(config.reconcile_timeout_seconds)
    attempt = 0
    while True:
        attempt += 1
        try:
            return await asyncio.wait_for(read(), timeout)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            delay = read_rate_limit_delay(exc)
            if delay is None or attempt >= int(config.max_poll_count):
                raise
            wait = max(float(config.poll_interval_seconds), min(8.0, 2.0 ** min(attempt - 1, 3)), float(delay))
            if time.monotonic() + wait > deadline:
                raise
            await sleep(wait)


async def select_wallet_pair(
    config: Any,
    pool: WalletPool,
    client: Any,
    *,
    has_key: Callable[[int], bool],
    rng: Any = None,
    clock: Callable[[], float] = time.time,
    sleep: Callable[[float], Any] = asyncio.sleep,
    fanout: bool = False,
) -> dict[str, Any]:
    """Read public state for each active wallet and draw one eligible pair.

    Returns the durable selection record; ``pair`` is None when fewer than two
    wallets are eligible.  Market reads failing propagate (no cycle starts).
    A typed HTTP 429 on any read is retried with the engine's bounded cooldown.
    With ``fanout`` the record instead carries ``mode: "fanout"`` and a
    ``fanout`` draw (source and receivers, None when infeasible); ``pair`` is None.
    """

    from .random_cycle import _as_book, _as_market

    metadata = _as_market(await _rate_limited_read(
        lambda: client.market_metadata(config.market_id), config, sleep))
    book = _as_book(await _rate_limited_read(lambda: client.order_book(config.market_id), config, sleep), metadata)
    requirement = wallet_requirement(metadata, book, config)
    semaphore = asyncio.Semaphore(SELECTION_READ_CONCURRENCY)

    async def state(index: int) -> WalletState | None:
        async with semaphore:
            try:
                value = await _rate_limited_read(
                    lambda: client.public_account_state(index, config.market_id), config, sleep)
            except asyncio.CancelledError:
                raise
            except Exception:
                return None  # Never surface SDK/transport text; skip the wallet.
            if not isinstance(value, WalletState) or value.account_index != index:
                return None
            return value

    states = await asyncio.gather(*(state(index) for index in pool.active))
    eligible: list[int] = []
    skipped: list[dict[str, Any]] = []
    balances: dict[str, str] = {}
    available: dict[int, Decimal] = {}
    for index, observed in zip(pool.active, states):
        reason = None
        if observed is None or not observed.ready:
            reason = "UNAVAILABLE"
        elif observed.signed_position != 0:
            reason = "NOT_FLAT"
        else:
            try:
                present = bool(has_key(index))
            except Exception:
                present = False
            if not present:
                reason = "KEY_MISSING"
            elif observed.available_balance < requirement:
                reason = "LOW_BALANCE"
        if observed is not None:
            balances[str(index)] = format(observed.available_balance, "f")
        if reason is None:
            eligible.append(index)
            available[index] = observed.available_balance
        else:
            skipped.append({"account_index": index, "reason": reason})
    if fanout:
        return {
            "schema": SELECTION_SCHEMA,
            "at": clock(),
            "mode": "fanout",
            "pool_size": len(pool.active),
            "paused": list(pool.paused),
            "eligible": eligible,
            "skipped": skipped,
            "available_balances": balances,
            "requirement_quote": format(requirement, "f"),
            "pair": None,
            "fanout": _fanout_selection(eligible, available, metadata, book, config, rng),
        }
    pair = list(draw_pair(eligible, rng)) if len(eligible) >= 2 else None
    return {
        "schema": SELECTION_SCHEMA,
        "at": clock(),
        "pool_size": len(pool.active),
        "paused": list(pool.paused),
        "eligible": eligible,
        "skipped": skipped,
        "available_balances": balances,
        "requirement_quote": format(requirement, "f"),
        "pair": pair,
    }


def read_selection(cycle_dir: Path | str) -> dict[str, Any] | None:
    """Bounded display-only read of the draw kept in the slot's launch.json."""

    path = Path(cycle_dir) / "launch.json"
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 512 * 1024:
            return None
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, UnicodeDecodeError):
        return None
    record = value.get("wallet_selection") if isinstance(value, dict) else None
    if not isinstance(record, dict) or record.get("schema") != SELECTION_SCHEMA:
        return None
    at = record.get("at")
    if isinstance(at, bool) or not isinstance(at, (int, float)) or not math.isfinite(at):
        return None
    return record


__all__ = [
    "MAX_POOL_WALLETS",
    "POOL_FILE_NAME",
    "SKIP_REASONS",
    "WalletPool",
    "WalletPoolError",
    "WalletState",
    "WalletsUnavailable",
    "draw_fanout",
    "draw_pair",
    "load_wallet_pool",
    "pool_path",
    "read_selection",
    "save_wallet_pool",
    "select_wallet_pair",
    "updated_pool",
    "wallet_requirement",
]
