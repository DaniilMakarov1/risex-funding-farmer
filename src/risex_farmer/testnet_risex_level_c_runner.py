"""Sealed orchestration for the first bounded RISEx Level C lifecycle.

The module is deliberately absent from normal startup.  It composes the
accepted venue-local lifecycle and write binding with one narrow authoritative
read capability.  The capability owns transport/parsing; this runner owns the
write order, durable identities, reconciliation, attempt ceiling, and terminal
barrier.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
import json
from pathlib import Path
import pwd
import os
import secrets
import sys
import time
from typing import Any, Callable, Mapping

from .testnet_risex_order_lifecycle import (
    AccountState, BBO, DurableIntentStore, Evidence, FillRecord, Intent,
    Lifecycle, LifecycleSafetyError, MarketState, OrderRecord, Outcome,
    _valid_order_id,
)
from .testnet_risex_order_lifecycle_operational import OperationalBinding
from .risex_private_read_operational import (
    FixedRisexPrivateReadTransport, PasswdHomeSessionSignerCapabilitySource,
    _open_exact_capability, _parse_auth_v2, _require_auth_v2_success,
    _validate_auth_v2_schema,
    _PUBLIC_REQUESTS,
)
from .testnet_risex_private_read_operational import (
    LifecycleClearBinding, _LIFECYCLE, _canonical_lifecycle_database,
    _safe_file,
)
from .testnet_risex_private_read_preflight import (
    ACCOUNT, AUTHORIZATION, MAX_BOUND_FRACTION, MINIMUM, REST_ORIGIN,
    ROUTER, SIGNER, PrivateReadPreflight,
)


class RunnerResult(str, Enum):
    COMPLETED_NO_FILL_FLAT = "COMPLETED_NO_FILL_FLAT"
    SUCCESS_CLOSED_FLAT = "SUCCESS_CLOSED_FLAT"
    BLOCKED_BEFORE_WRITE = "BLOCKED_BEFORE_WRITE"
    FAILED_HALTED_MANUAL_RECOVERY = "FAILED_HALTED_MANUAL_RECOVERY"


@dataclass(frozen=True)
class AuthoritativeState:
    market: MarketState
    account: AccountState
    bbo: BBO
    nonce_anchor: int = 0
    nonce_bitmap: int = 0


@dataclass(frozen=True)
class RunnerReport:
    run_id: str
    result: RunnerResult
    intent_count: int
    dispatch_count: int
    close_attempts: int
    manual_recovery: bool

    def sanitized(self) -> dict[str, Any]:
        return {
            "result": self.result.value,
            "run_id": self.run_id,
            "intent_count": self.intent_count,
            "dispatch_count": self.dispatch_count,
            "close_attempts": self.close_attempts,
            "manual_recovery": self.manual_recovery,
        }


def _capability_is_exact(value: Any) -> bool:
    expected = {"state", "evidence", "close"}
    exposed = {name for name in vars(type(value)) if not name.startswith("_")}
    return exposed == expected and all(
        callable(getattr(value, name, None)) for name in expected
    )


async def _maybe_await(value: Any) -> Any:
    if asyncio.iscoroutine(value) or isinstance(value, asyncio.Future):
        return await value
    return value


class _ProductionReadCapability:
    """Fixed RISEx testnet reads built from the accepted Level B machinery."""

    def __init__(
        self, source: Any, credential: Any, transport: Any,
        clock: Callable[[], float],
    ) -> None:
        self._source = source
        self._credential = credential
        self._transport = transport
        self._clock = clock
        self._updates: dict[int, Mapping[str, Any]] = {}
        self._subscribed = False
        self._subscribe_ack_count = 0
        self._named_subscribe_acks: set[str] = set()
        self._orders: tuple[Mapping[str, Any], ...] | None = None
        self._position: tuple[Decimal, bool, int, int] | None = None
        self._orders_snapshot_received = False
        self._position_snapshot_received = False
        self._stream_sequence = 0
        self._closed = False
        self._prestate_verified = False
        self._validator = PrivateReadPreflight(
            object(), clock=self._clock, public_get=lambda *_: None,
            lifecycle_clear=lambda: True,
        )

    @classmethod
    async def _create(
        cls, *,
        source_factory: Callable[[], Any] = PasswdHomeSessionSignerCapabilitySource,
        transport_factory: Callable[[], Any] = FixedRisexPrivateReadTransport,
        clock: Callable[[], float] = time.time,
    ) -> "_ProductionReadCapability":
        source = source_factory()
        transport = transport_factory()
        credential = None
        try:
            source.load()
            credential = _open_exact_capability(source)
            if credential.derive_signer_address().lower() != SIGNER:
                raise LifecycleSafetyError("RISEx Level C signer rejected")
            validator = PrivateReadPreflight(
                object(), clock=clock, public_get=lambda *_: None,
                lifecycle_clear=lambda: True,
            )
            nonce_response = await transport.nonce_get()
            nonce_data = validator._validate_response(
                "/v1/auth/nonce", (("account", ACCOUNT),), nonce_response,
            )
            if not isinstance(nonce_data, Mapping) or set(nonce_data) != {"nonce"}:
                raise LifecycleSafetyError("RISEx Level C nonce rejected")
            nonce = PrivateReadPreflight._nonce(nonce_data["nonce"])
            typed = PrivateReadPreflight._typed_data(nonce)
            signature = credential.sign_register_v2(typed)
            await transport.auth_v2_dispatch({
                "method": "auth_v2",
                "params": {
                    "account": ACCOUNT, "signer": SIGNER,
                    "message": "sign in with RISEx", "nonce": nonce,
                    "expiration": int(clock()) + 365 * 24 * 60 * 60,
                    "signature": signature,
                },
            })
            auth = _parse_auth_v2(await transport.auth_v2_receive())
            _require_auth_v2_success(_validate_auth_v2_schema(auth))
            return cls(source, credential, transport, clock)
        except BaseException:
            if credential is not None:
                credential.close()
            source.close()
            await transport.close()
            raise

    async def _market_state(self) -> tuple[MarketState, BBO]:
        market_response = await self._transport.public_get(4)
        book_response = await self._transport.public_get(5)
        market_data = self._validator._validate_response(
            "/v1/markets", (("force_refresh", "true"), ("market_ids", "1")),
            market_response,
        )
        book_data = self._validator._validate_response(
            "/v1/orderbook", (("market_id", "1"),), book_response,
        )
        parsed_market = PrivateReadPreflight._validate_market(
            market_data, self._clock(),
        )
        bids, asks = PrivateReadPreflight._validate_book(book_data, self._clock())
        bid, ask = bids[0][0], asks[0][0]
        if bid >= ask:
            raise LifecycleSafetyError("RISEx Level C book rejected")
        lower = bid * (Decimal(1) - MAX_BOUND_FRACTION)
        upper = ask * (Decimal(1) + MAX_BOUND_FRACTION)
        observed_at = int(min(market_response.observed_at, book_response.observed_at))
        market = MarketState(
            host=REST_ORIGIN.removeprefix("https://"), chain_id=11_155_931,
            domain_name="RISEx", domain_version="1", router=ROUTER,
            authorization=AUTHORIZATION, market_id=1, symbol="BTC/USDC",
            active=parsed_market["active"],
            unlocked=parsed_market["config"]["unlocked"],
            tick=Decimal(parsed_market["config"]["step_price"]),
            step=Decimal(parsed_market["config"]["step_size"]),
            minimum=Decimal(parsed_market["config"]["min_order_size"]),
            observed_at=observed_at,
        )
        bbo = BBO(
            bid=bid, ask=ask,
            bid_depth=sum(size for price, size in bids if price >= lower),
            ask_depth=sum(size for price, size in asks if price <= upper),
            observed_at=int(book_response.observed_at),
        )
        if market.minimum != MINIMUM:
            raise LifecycleSafetyError("RISEx Level C minimum rejected")
        return market, bbo

    async def _full_public_prestate(self) -> int:
        last_sweep_observed: list[float] = []
        for _ in range(2):
            responses: dict[str, Any] = {}
            observed: list[float] = []
            for index, (path, query) in enumerate(_PUBLIC_REQUESTS):
                response = await self._transport.public_get(index)
                responses[path] = self._validator._validate_response(
                    path, query, response,
                )
                observed.append(response.observed_at)
            self._validator._validate_sweep(responses)
            now = self._clock()
            if any(not 0 <= now - item <= 5 for item in observed):
                raise LifecycleSafetyError("RISEx Level C public prestate stale")
            last_sweep_observed = observed
        return int(min(last_sweep_observed))

    def _allow_recovered_lifecycle(self) -> None:
        self._prestate_verified = True

    @staticmethod
    def _order_row(value: Any) -> Mapping[str, Any]:
        required = {
            "id", "wide_order_id", "resting_order_id", "client_order_id",
            "market_id", "sender", "side", "type", "time_in_force",
            "status", "size", "filled_size", "post_only", "reduce_only",
            "is_liquidation",
        }
        if not isinstance(value, Mapping) or not required <= set(value):
            raise LifecycleSafetyError("RISEx Level C order schema rejected")
        if (
            not _valid_order_id(value["id"])
            or str(value["market_id"]) != "1"
            or str(value["sender"]).lower() != ACCOUNT
            or value["status"] not in {
                "ORDER_STATUS_OPEN", "ORDER_STATUS_FILLED",
                "ORDER_STATUS_CANCELLED",
            }
            or type(value["post_only"]) is not bool
            or type(value["reduce_only"]) is not bool
            or value["is_liquidation"] is not False
        ):
            raise LifecycleSafetyError("RISEx Level C order rejected")
        try:
            client = int(value["client_order_id"])
            wide = int(value["wide_order_id"])
            resting = int(value["resting_order_id"])
            size = Decimal(str(value["size"]))
            filled = Decimal(str(value["filled_size"]))
        except Exception:
            raise LifecycleSafetyError("RISEx Level C order rejected") from None
        if (
            not 0 < client < 2**64 or not 0 <= wide < 2**64
            or resting != wide >> 1 or size <= 0 or not 0 <= filled <= size
        ):
            raise LifecycleSafetyError("RISEx Level C order rejected")
        return value

    def _orders_frame(
        self, value: Any,
    ) -> tuple[tuple[Mapping[str, Any], ...], int]:
        try:
            data, timestamp = PrivateReadPreflight._decode_private_snapshot(
                value, channel="orders", count_field="order_count",
            )
            PrivateReadPreflight._validate_snapshot_freshness(timestamp, self._clock())
        except Exception:
            raise LifecycleSafetyError("RISEx Level C orders snapshot rejected") from None
        return (
            tuple(self._order_row(item) for item in data),
            int(timestamp),
        )

    def _order_update(
        self, value: Any,
    ) -> tuple[tuple[Mapping[str, Any], ...], int]:
        if (
            not isinstance(value, Mapping) or value.get("channel") != "orders"
            or value.get("type") != "update" or str(value.get("market_id")) != "1"
            or not isinstance(value.get("data"), list)
            or not isinstance(value.get("worker_timestamp"), str)
        ):
            raise LifecycleSafetyError("RISEx Level C order update rejected")
        PrivateReadPreflight._validate_snapshot_freshness(
            value["worker_timestamp"], self._clock(),
        )
        return tuple(self._order_row(item) for item in value["data"]), int(
            value["worker_timestamp"],
        )

    def _positions_frame(self, value: Any) -> tuple[Decimal, bool, int]:
        if isinstance(value, Mapping) and value.get("type") == "snapshot":
            try:
                data, timestamp = PrivateReadPreflight._decode_private_snapshot(
                    value, channel="positions", count_field="position_count",
                )
            except Exception:
                raise LifecycleSafetyError(
                    "RISEx Level C positions snapshot rejected"
                ) from None
        elif isinstance(value, Mapping) and value.get("type") == "update":
            if (
                set(value) != {
                    "channel", "type", "market_id", "data", "block_number",
                    "log_index", "worker_timestamp",
                }
                or value.get("channel") != "positions"
                or str(value.get("market_id")) != "1"
                or not isinstance(value.get("data"), list)
                or not isinstance(value.get("block_number"), int)
                or not isinstance(value.get("log_index"), int)
                or not isinstance(value.get("worker_timestamp"), str)
            ):
                raise LifecycleSafetyError(
                    "RISEx Level C positions update rejected"
                )
            data = tuple(value["data"])
            timestamp = value["worker_timestamp"]
        else:
            raise LifecycleSafetyError("RISEx Level C positions frame rejected")
        try:
            PrivateReadPreflight._validate_snapshot_freshness(
                timestamp, self._clock(),
            )
        except Exception:
            raise LifecycleSafetyError("RISEx Level C positions stale") from None
        position = Decimal("0")
        unexplained = False
        markets: set[int] = set()
        for item in data:
            if not isinstance(item, Mapping) or not {
                "account", "market_id", "size",
            } <= set(item):
                raise LifecycleSafetyError("RISEx Level C position rejected")
            try:
                market_id = int(str(item["market_id"]))
                size = Decimal(str(item["size"]))
            except Exception:
                raise LifecycleSafetyError("RISEx Level C position rejected") from None
            if str(item["account"]).lower() != ACCOUNT:
                raise LifecycleSafetyError("RISEx Level C position identity rejected")
            if market_id <= 0 or market_id in markets or not size.is_finite():
                raise LifecycleSafetyError("RISEx Level C position rejected")
            markets.add(market_id)
            if market_id == 1:
                position = size
            elif size != 0:
                unexplained = True
        return position, unexplained, int(timestamp)

    async def _ensure_subscribed(self) -> None:
        if self._subscribed:
            return
        await self._transport.orders_subscribe()
        await self._transport.positions_subscribe()
        self._subscribed = True

    def _apply_order_update(self, row: Mapping[str, Any]) -> None:
        client = int(row["client_order_id"])
        previous = self._updates.get(client)
        stable = (
            "id", "wide_order_id", "resting_order_id", "client_order_id",
            "market_id", "sender", "side", "type", "time_in_force", "size",
            "post_only", "reduce_only", "is_liquidation",
        )
        if previous is not None and any(previous[key] != row[key] for key in stable):
            raise LifecycleSafetyError("RISEx Level C order contradiction")
        self._updates[client] = row
        current = {
            int(item["client_order_id"]): item for item in (self._orders or ())
        }
        if row["status"] == "ORDER_STATUS_OPEN":
            current[client] = row
        else:
            current.pop(client, None)
        self._orders = tuple(current[key] for key in sorted(current))

    async def _demux_once(self, expected_client: int | None = None) -> None:
        value = await self._transport._receive()
        if isinstance(value, Mapping) and value.get("method") == "subscribe":
            try:
                if (
                    value.get("status") != "success"
                    or not isinstance(value.get("data"), Mapping)
                    or not isinstance(value.get("channel"), str)
                    or not isinstance(value.get("type"), str)
                ):
                    raise ValueError
            except Exception:
                raise LifecycleSafetyError("RISEx Level C subscribe ack rejected") from None
            self._subscribe_ack_count += 1
            if self._subscribe_ack_count > 2:
                raise LifecycleSafetyError("RISEx Level C duplicate subscribe ack")
            channel = value["channel"]
            if channel in {"orders", "positions"}:
                if channel in self._named_subscribe_acks:
                    raise LifecycleSafetyError("RISEx Level C duplicate subscribe ack")
                self._named_subscribe_acks.add(channel)
            return
        if isinstance(value, Mapping) and value.get("channel") == "orders":
            if value.get("type") == "snapshot":
                if self._orders is not None:
                    raise LifecycleSafetyError("RISEx Level C duplicate orders snapshot")
                self._orders, _ = self._orders_frame(value)
                self._orders_snapshot_received = True
            elif value.get("type") == "update":
                rows, _ = self._order_update(value)
                if (
                    expected_client is None or len(rows) != 1
                    or int(rows[0]["client_order_id"]) != expected_client
                ):
                    raise LifecycleSafetyError("RISEx Level C unrelated order update")
                for row in rows:
                    self._apply_order_update(row)
            else:
                raise LifecycleSafetyError("RISEx Level C orders frame rejected")
        elif isinstance(value, Mapping) and value.get("channel") == "positions":
            position, unexplained, timestamp = self._positions_frame(value)
            if (
                expected_client is None and self._position is not None
                and self._position[:2] != (position, unexplained)
            ):
                raise LifecycleSafetyError("RISEx Level C position contradiction")
            self._stream_sequence += 1
            self._position = (
                position, unexplained, timestamp, self._stream_sequence,
            )
            if value.get("type") == "snapshot":
                self._position_snapshot_received = True
        else:
            raise LifecycleSafetyError("RISEx Level C unrelated stream frame")

    async def _pump_until(
        self, condition: Callable[[], bool], *, expected_client: int | None = None,
        allow_absence: bool = False,
    ) -> bool:
        await self._ensure_subscribed()
        for _ in range(8):
            if condition():
                return True
            await self._demux_once(expected_client)
        if condition():
            return True
        if allow_absence:
            return False
        raise LifecycleSafetyError("RISEx Level C stream observation unavailable")

    async def _account(self) -> AccountState:
        await self._pump_until(
            lambda: self._orders is not None and self._position is not None,
        )
        assert self._orders is not None and self._position is not None
        position, unexplained, timestamp, _ = self._position
        order_ids = tuple(str(item["id"]) for item in self._orders)
        return AccountState(
            account=ACCOUNT, signer=SIGNER, signer_status="ACTIVE",
            position=position, open_order_ids=order_ids,
            repeated_open_order_ids=order_ids, repeated_position=position,
            unexplained=unexplained,
            observed_at=int(Decimal(timestamp) / Decimal(1_000_000_000)),
        )

    async def state(self) -> AuthoritativeState:
        initial_prestate = not self._prestate_verified
        if initial_prestate:
            await self._full_public_prestate()
        market, bbo = await self._market_state()
        account = await self._account()
        if (
            not initial_prestate and account.position == 0
            and not account.open_order_ids and not account.unexplained
        ):
            await self._full_public_prestate()
        response = await self._transport._get(
            f"/v1/nonce-state/{ACCOUNT}", (),
        )
        if response.status != 200 or not isinstance(response.body, Mapping):
            raise LifecycleSafetyError("RISEx Level C nonce state rejected")
        envelope = response.body
        if set(envelope) != {"data", "request_id"} or not isinstance(
            envelope["request_id"], str,
        ):
            raise LifecycleSafetyError("RISEx Level C nonce state rejected")
        data = envelope["data"]
        if not isinstance(data, Mapping) or set(data) != {
            "nonce_anchor", "current_bitmap_index", "bitmap",
        }:
            raise LifecycleSafetyError("RISEx Level C nonce state rejected")
        try:
            anchor = int(data["nonce_anchor"])
            index = data["current_bitmap_index"]
            bitmap_text = data["bitmap"]
            if (
                type(index) is not int or not isinstance(bitmap_text, str)
                or not bitmap_text.startswith("0x")
            ):
                raise ValueError
            bitmap = int(bitmap_text[2:], 16)
        except Exception:
            raise LifecycleSafetyError("RISEx Level C nonce state rejected") from None
        if (
            not 0 <= anchor < 2**48 - 1 or not 0 <= index <= 208
            or not 0 <= bitmap < 2**256
            or (index < 208 and (bitmap >> index) & 1)
        ):
            raise LifecycleSafetyError("RISEx Level C nonce state rejected")
        nonce_anchor, nonce_bitmap = (anchor, index) if index < 208 else (anchor + 1, 0)
        result = AuthoritativeState(
            market, account, bbo, nonce_anchor, nonce_bitmap,
        )
        if initial_prestate:
            if account.position != 0 or account.open_order_ids or account.unexplained:
                raise LifecycleSafetyError("RISEx Level C private prestate rejected")
            self._prestate_verified = True
        return result

    async def evidence(self, intent: Intent) -> Evidence:
        position_sequence = self._position[3] if self._position is not None else 0
        row = self._updates.pop(intent.client_order_id, None)
        if row is None:
            exact_no_identity_open = (
                intent.kind == "OPEN" and intent.order_type == "LIMIT"
                and intent.time_in_force == "IOC"
                and not intent.reduce_only and not intent.post_only
                and intent.order_id is None and intent.dispatch_count == 1
                and intent.state in {"DISPATCHING", "DISPATCHED", "AMBIGUOUS"}
            )
            try:
                observed = await self._pump_until(
                    lambda: intent.client_order_id in self._updates,
                    expected_client=intent.client_order_id, allow_absence=True,
                )
            except TimeoutError:
                initial_zero_flat_snapshots = (
                    self._orders_snapshot_received
                    and self._position_snapshot_received
                    and self._orders == () and self._position is not None
                    and self._position[:2] == (Decimal("0"), False)
                )
                if not exact_no_identity_open or not initial_zero_flat_snapshots:
                    raise
                observed = False
            if not observed:
                if not exact_no_identity_open:
                    raise LifecycleSafetyError(
                        "RISEx Level C stream observation unavailable"
                    )
                wait_seconds = intent.expires_at + 1 - self._clock()
                if wait_seconds > 61:
                    raise LifecycleSafetyError(
                        "RISEx Level C intent expiry unavailable"
                    )
                if wait_seconds > 0:
                    await asyncio.sleep(wait_seconds)
                if self._clock() <= intent.expires_at:
                    raise LifecycleSafetyError(
                        "RISEx Level C intent not expired"
                    )
                observed_at = await self._full_public_prestate()
                return Evidence(
                    account=ACCOUNT, signer=SIGNER, signer_status="ACTIVE",
                    terminal=True, filled_size=Decimal("0"),
                    position=Decimal("0"), observed_at=observed_at,
                    position_market_id=1,
                )
            row = self._updates.pop(intent.client_order_id, None)
        if row is None:
            raise LifecycleSafetyError("RISEx Level C order outcome unavailable")
        if (
            row["side"] != intent.side or row["type"] != intent.order_type
            or row["time_in_force"] != intent.time_in_force
            or row["reduce_only"] is not intent.reduce_only
            or row["post_only"] is not intent.post_only
            or Decimal(str(row["size"])) != intent.size
        ):
            raise LifecycleSafetyError("RISEx Level C order binding rejected")
        order = OrderRecord(
            str(row["id"]), int(row["wide_order_id"]),
            int(row["resting_order_id"]), intent.client_order_id,
        )
        filled = Decimal(str(row["filled_size"]))
        if filled > 0:
            await self._pump_until(
                lambda: self._position is not None
                and self._position[3] > position_sequence,
                expected_client=intent.client_order_id,
            )
        if self._position is None:
            raise LifecycleSafetyError("RISEx Level C position unavailable")
        position, unexplained, observed_ns, _ = self._position
        if unexplained:
            raise LifecycleSafetyError("RISEx Level C position contradiction")
        terminal = row["status"] != "ORDER_STATUS_OPEN"
        return Evidence(
            account=ACCOUNT, signer=SIGNER, signer_status="ACTIVE",
            terminal=terminal, filled_size=filled, position=position,
            observed_at=int(Decimal(observed_ns) / Decimal(1_000_000_000)),
            position_market_id=1, by_id_order=order,
            open_orders=() if terminal else (order,), history_orders=(order,),
            fills=(FillRecord(order.order_id, intent.client_order_id),)
            if filled > 0 else (),
        )

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._credential.close()
        self._source.close()
        await self._transport.close()


class RisexLevelCRunner:
    """One fixed-account lifecycle.  Construction itself dispatches no write."""

    def __init__(self) -> None:
        home = Path(pwd.getpwuid(os.getuid()).pw_dir)
        lifecycle_path = home / _LIFECYCLE
        if lifecycle_path != home / ".risex-funding-farmer-testnet-order-lifecycle-v1.sqlite":
            raise LifecycleSafetyError("RISEx Level C fixed storage rejected")
        if not lifecycle_path.exists() and LifecycleClearBinding()() is not True:
            raise LifecycleSafetyError("RISEx Level C lifecycle store rejected")
        if not _safe_file(lifecycle_path):
            raise LifecycleSafetyError("RISEx Level C lifecycle store rejected")
        self._store = DurableIntentStore(lifecycle_path)
        if not _canonical_lifecycle_database(self._store.connection):
            self._store.close()
            raise LifecycleSafetyError("RISEx Level C lifecycle store rejected")
        self._binding = OperationalBinding()
        self._clock: Callable[[], int] = lambda: int(time.time())
        self._identity: Callable[[], tuple[int, int, int]] | None = None

    @classmethod
    def _fixture(
        cls, *, store: DurableIntentStore, binding: Any,
        clock: Callable[[], int], identity: Callable[[], tuple[int, int, int]],
    ) -> "RisexLevelCRunner":
        value = object.__new__(cls)
        value._store = store
        value._binding = binding
        value._clock = clock
        value._identity = identity
        return value

    def _lifecycle(self) -> Lifecycle:
        return Lifecycle(
            self._store, now=self._clock, router=ROUTER,
            authorization=AUTHORIZATION, expected_account=ACCOUNT,
            expected_signer=SIGNER,
        )

    def _new_identity(
        self, state: AuthoritativeState,
    ) -> tuple[int, int, int, int]:
        if self._identity is None:
            client_order_id = secrets.randbelow(2**64 - 1) + 1
            nonce_anchor = state.nonce_anchor
            nonce_bitmap = state.nonce_bitmap
        else:
            client_order_id, nonce_anchor, nonce_bitmap = self._identity()
        if (
            type(client_order_id) is not int or not 0 < client_order_id < 2**64
            or type(nonce_anchor) is not int or not 0 <= nonce_anchor < 2**48
            or type(nonce_bitmap) is not int or not 0 <= nonce_bitmap <= 207
        ):
            raise LifecycleSafetyError("RISEx Level C identity rejected")
        return client_order_id, nonce_anchor, nonce_bitmap, self._clock() + 60

    def _report(self, result: RunnerResult, *, manual: bool = False) -> RunnerReport:
        intents = self._store.all()
        return RunnerReport(
            run_id=self._binding.run_id,
            result=result,
            intent_count=len(intents),
            dispatch_count=sum(item.dispatch_count for item in intents),
            close_attempts=sum(item.kind == "CLOSE" for item in intents),
            manual_recovery=manual,
        )

    def _dispatch_prepared(
        self, lifecycle: Lifecycle, intent: Intent, state: AuthoritativeState,
    ) -> None:
        if intent.state != "PREPARED" or intent.dispatch_count != 0:
            raise LifecycleSafetyError("RISEx Level C replay rejected")
        if self._identity is None and (
            intent.nonce != state.nonce_anchor
            or intent.nonce_bitmap != state.nonce_bitmap
        ):
            raise LifecycleSafetyError("RISEx Level C prepared nonce consumed")
        self._binding.dispatch_place(lifecycle, intent, state.market)

    async def run(self, capability: Any) -> RunnerReport:
        if not _capability_is_exact(capability):
            raise LifecycleSafetyError("RISEx Level C read capability rejected")
        lifecycle = self._lifecycle()
        last_account: AccountState | None = None
        try:
            if lifecycle.outcome is Outcome.COMPLETED_NO_FILL_FLAT:
                return self._report(RunnerResult.COMPLETED_NO_FILL_FLAT)
            if lifecycle.outcome is Outcome.SUCCESS_CLOSED_FLAT:
                return self._report(RunnerResult.SUCCESS_CLOSED_FLAT)
            if lifecycle.outcome is Outcome.FAILED_HALTED_MANUAL_RECOVERY:
                return self._report(RunnerResult.FAILED_HALTED_MANUAL_RECOVERY, manual=True)

            intents = self._store.all()
            if not intents:
                state = await _maybe_await(capability.state())
                if not isinstance(state, AuthoritativeState):
                    raise LifecycleSafetyError("RISEx Level C state rejected")
                last_account = state.account
                preflight = lifecycle.preflight(state.market, state.account, state.bbo)
                intent = lifecycle.prepare_open(preflight, *self._new_identity(state))
                self._dispatch_prepared(lifecycle, intent, state)

            while lifecycle.outcome is Outcome.ACTIVE:
                intents = self._store.all()
                current = intents[-1]
                if current.state == "PREPARED":
                    state = await _maybe_await(capability.state())
                    if not isinstance(state, AuthoritativeState):
                        raise LifecycleSafetyError("RISEx Level C state rejected")
                    last_account = state.account
                    self._dispatch_prepared(lifecycle, current, state)

                evidence = await _maybe_await(
                    capability.evidence(self._store.get(current.intent_id))
                )
                if not isinstance(evidence, Evidence):
                    raise LifecycleSafetyError("RISEx Level C evidence rejected")
                lifecycle.reconcile(current.intent_id, evidence)
                last_account = AccountState(
                    account=evidence.account, signer=evidence.signer,
                    signer_status=evidence.signer_status,
                    position=evidence.position,
                    open_order_ids=tuple(item.order_id for item in evidence.open_orders),
                    repeated_open_order_ids=tuple(item.order_id for item in evidence.open_orders),
                    repeated_position=evidence.position,
                    observed_at=evidence.observed_at,
                )

                current = self._store.get(current.intent_id)
                if current.state == "OPEN_KNOWN":
                    if current.order_id is None:
                        raise LifecycleSafetyError("RISEx Level C order identity rejected")
                    state = await _maybe_await(capability.state())
                    if not isinstance(state, AuthoritativeState):
                        raise LifecycleSafetyError("RISEx Level C state rejected")
                    last_account = state.account
                    _client, nonce, bitmap, deadline = self._new_identity(state)
                    self._binding.cancel_known(
                        lifecycle, current.order_id, market=state.market,
                        nonce_anchor=nonce, nonce_bitmap=bitmap,
                        expires_at=deadline,
                    )
                    terminal = await _maybe_await(capability.evidence(current))
                    if not isinstance(terminal, Evidence) or not terminal.terminal:
                        raise LifecycleSafetyError("RISEx Level C cancel evidence rejected")
                    cancelled = await _maybe_await(capability.state())
                    if not isinstance(cancelled, AuthoritativeState):
                        raise LifecycleSafetyError("RISEx Level C state rejected")
                    last_account = cancelled.account
                    if not lifecycle.reconcile_cancel(current.order_id, cancelled.account):
                        raise LifecycleSafetyError("RISEx Level C cancel unresolved")
                    lifecycle.reconcile(current.intent_id, terminal)
                    evidence = terminal

                if lifecycle.outcome is Outcome.COMPLETED_NO_FILL_FLAT:
                    final = await _maybe_await(capability.state())
                    if not isinstance(final, AuthoritativeState):
                        raise LifecycleSafetyError("RISEx Level C final state rejected")
                    if (
                        not lifecycle._account_valid(final.account)
                        or final.account.position != 0
                        or final.account.open_order_ids
                    ):
                        raise LifecycleSafetyError("RISEx Level C final barrier rejected")
                    return self._report(RunnerResult.COMPLETED_NO_FILL_FLAT)

                if evidence.position == 0:
                    final = await _maybe_await(capability.state())
                    if not isinstance(final, AuthoritativeState):
                        raise LifecycleSafetyError("RISEx Level C final state rejected")
                    last_account = final.account
                    outcome = lifecycle.finalize(final.account)
                    if outcome is not Outcome.SUCCESS_CLOSED_FLAT:
                        raise LifecycleSafetyError("RISEx Level C final barrier rejected")
                    return self._report(RunnerResult.SUCCESS_CLOSED_FLAT)

                if lifecycle.close_count >= 3:
                    lifecycle.halt_manual(last_account, "attempts_exhausted")
                    return self._report(
                        RunnerResult.FAILED_HALTED_MANUAL_RECOVERY, manual=True,
                    )
                state = await _maybe_await(capability.state())
                if not isinstance(state, AuthoritativeState):
                    raise LifecycleSafetyError("RISEx Level C state rejected")
                last_account = state.account
                close = lifecycle.prepare_close(
                    state.market, state.account, state.bbo,
                    *self._new_identity(state),
                )
                self._dispatch_prepared(lifecycle, close, state)
        except Exception:
            if self._store.all():
                if last_account is None:
                    self._store.persist_outcome(Outcome.FAILED_HALTED_MANUAL_RECOVERY)
                else:
                    lifecycle.halt_manual(last_account, "state_conflict")
                return self._report(
                    RunnerResult.FAILED_HALTED_MANUAL_RECOVERY, manual=True,
                )
            return self._report(RunnerResult.BLOCKED_BEFORE_WRITE)
        finally:
            await _maybe_await(capability.close())


async def _run_production() -> RunnerReport:
    runner = RisexLevelCRunner()
    try:
        capability = await _ProductionReadCapability._create()
    except Exception:
        return runner._report(RunnerResult.BLOCKED_BEFORE_WRITE)
    if runner._store.all():
        capability._allow_recovered_lifecycle()
    return await runner.run(capability)


def run_risex_level_c() -> RunnerReport:
    """Run the one fixed RISEx testnet Level C operation."""
    return asyncio.run(_run_production())


def main() -> int:
    if len(sys.argv) != 1:
        print(json.dumps({"result": "BLOCKED_BEFORE_WRITE"}, sort_keys=True))
        return 1
    try:
        report = run_risex_level_c()
    except BaseException:
        print(json.dumps({"result": "FAILED_HALTED_MANUAL_RECOVERY"}, sort_keys=True))
        return 1
    print(json.dumps(report.sanitized(), sort_keys=True, separators=(",", ":")))
    return 0 if report.result in {
        RunnerResult.COMPLETED_NO_FILL_FLAT, RunnerResult.SUCCESS_CLOSED_FLAT,
    } else 1


if __name__ == "__main__":
    raise SystemExit(main())
