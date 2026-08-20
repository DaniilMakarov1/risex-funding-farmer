"""Small public-only venue adapter contract."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_FLOOR
from enum import StrEnum
import json
from typing import Any, Mapping

import aiohttp

from risex_farmer.models import CanonicalMarket, FundingCashQuote, OrderBook, TradeEvidence


JsonObject = Mapping[str, Any]
# SYSTEM_SPEC polling cadence; venue wire-heartbeat intervals remain venue-specific.
HEALTH_CHECK_CADENCE_SECONDS = 10


class WebSocketFrameAction(StrEnum):
    PING = "PING"
    PONG = "PONG"
    NONE = "NONE"


@dataclass(frozen=True, slots=True)
class PublicHeartbeatAction:
    frame_action: WebSocketFrameAction
    payload: bytes
    connection_confirmed: bool


class PublicDataUnavailable(RuntimeError):
    """The venue rejected access to a supposedly public surface."""


class PublicAdapter(ABC):
    """REST parsing and WebSocket message construction without authentication."""

    def __init__(self, session: aiohttp.ClientSession, rest_base: str, ws_base: str) -> None:
        self._session = session
        self.rest_base = rest_base.rstrip("/")
        self.ws_base = ws_base
        self.public_data_available: bool | None = None

    async def _get_json(
        self, path: str, *, params: Mapping[str, str] | None = None
    ) -> JsonObject:
        return await self._get_json_at(self.rest_base, path, params=params)

    async def _get_json_at(
        self,
        base: str,
        path: str,
        *,
        params: Mapping[str, str] | None = None,
    ) -> JsonObject:
        try:
            async with self._session.get(
                f"{base.rstrip('/')}{path}", params=params
            ) as response:
                response.raise_for_status()
                payload = json.loads(await response.text(), parse_float=Decimal)
        except aiohttp.ClientResponseError as exc:
            if exc.status in {401, 403}:
                self.public_data_available = False
                raise PublicDataUnavailable(
                    "venue rejected unauthenticated public market data"
                ) from exc
            raise
        self.public_data_available = True
        if not isinstance(payload, Mapping):
            raise ValueError("public endpoint returned a non-object payload")
        return payload

    async def _post_json_at(
        self, base: str, path: str, *, body: Mapping[str, Any]
    ) -> JsonObject:
        try:
            async with self._session.post(
                f"{base.rstrip('/')}{path}", json=body
            ) as response:
                response.raise_for_status()
                payload = json.loads(await response.text(), parse_float=Decimal)
        except aiohttp.ClientResponseError as exc:
            if exc.status in {401, 403}:
                self.public_data_available = False
                raise PublicDataUnavailable(
                    "venue rejected unauthenticated public market data"
                ) from exc
            raise
        self.public_data_available = True
        if not isinstance(payload, Mapping):
            raise ValueError("public endpoint returned a non-object payload")
        return payload

    @abstractmethod
    async def fetch_markets(self) -> tuple[CanonicalMarket, ...]: ...

    @abstractmethod
    async def fetch_book(self, venue_symbol: str) -> OrderBook: ...

    @abstractmethod
    async def fetch_funding_quote(
        self, market: CanonicalMarket, *, assumed_open_at: datetime
    ) -> FundingCashQuote: ...

    @abstractmethod
    def normalize_trade(
        self, payload: JsonObject, *, received_at: datetime, session_id: str, ordinal: int
    ) -> TradeEvidence: ...

    @abstractmethod
    def unknown_funding_quote(
        self, market: CanonicalMarket, *, observed_at: datetime, assumed_open_at: datetime
    ) -> FundingCashQuote: ...


def require_mapping(value: Any, field: str) -> JsonObject:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be an object")
    return value


def require_list(value: Any, field: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{field} must be an array")
    return value


def decimal_value(value: Any, field: str, *, scale: Decimal | None = None) -> Decimal:
    if (
        isinstance(value, bool)
        or isinstance(value, float)
        or not isinstance(value, (str, int, Decimal))
    ):
        raise TypeError(f"{field} must be a decimal string or integer")
    parsed = Decimal(value)
    if not parsed.is_finite():
        raise ValueError(f"{field} must be finite")
    return parsed if scale is None else parsed / scale


def timestamp(value: Any, unit: str) -> datetime:
    raw = decimal_value(value, "timestamp")
    divisors = {
        "seconds": Decimal("1"),
        "milliseconds": Decimal("1000"),
        "nanoseconds": Decimal("1000000000"),
    }
    try:
        divisor = divisors[unit]
    except KeyError as exc:
        raise ValueError(f"unsupported timestamp unit: {unit}") from exc
    total_microseconds = (raw * Decimal("1000000") / divisor).to_integral_value(
        rounding=ROUND_FLOOR
    )
    whole_seconds, microseconds = divmod(int(total_microseconds), 1_000_000)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    return epoch + timedelta(seconds=whole_seconds, microseconds=microseconds)


def synthetic_trade_key(
    venue: str,
    session_id: str,
    raw_timestamp: str | int | None,
    product: str,
    price: Decimal,
    quantity: Decimal,
    aggressor: str,
    ordinal: int,
) -> str:
    return "|".join(
        (venue, session_id, str(raw_timestamp), product, str(price), str(quantity), aggressor, str(ordinal))
    )
