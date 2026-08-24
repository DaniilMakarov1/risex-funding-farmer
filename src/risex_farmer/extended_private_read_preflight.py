"""Isolated fixture contract for a sealed Extended v1 private-read preflight.

The module owns no live transport.  A later operational gate must inject the
credential loader and direct transport; normal Farmer startup never imports it.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import sqlite3
from decimal import Decimal, InvalidOperation
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


REST_BASE_URL = "https://api.starknet.sepolia.extended.exchange/api/v1"
STREAM_URL = (
    "wss://api.starknet.sepolia.extended.exchange/"
    "stream.extended.exchange/v1/account"
)
ACCOUNT_ID = 7001
ACCOUNT_INDEX = 3
L2_KEY = "0x12345"
L2_VAULT = 7001003

_API_HEADER = "X-Api-Key"
_REST_PATHS = ("/user/account/info", "/user/orders", "/user/positions")
_ACCOUNT_KEYS = {
    "id", "description", "accountIndex", "status", "l2Key", "l2Vault",
    "bridgeStarknetAddress",
}
_STREAM_TYPES = {
    "BALANCE", "SPOT_BALANCE", "ORDER", "POSITION", "TRADE",
}
_STREAM_DATA_KEYS = {"orders", "positions", "trades", "balance", "spotBalances"}
_MAX_OBSERVATION_AGE_MS = 5_000
_TRANSPORT_KEYS = {
    "actual_url", "method", "header_names", "direct_tls", "trust_env", "proxy",
    "redirects", "retries", "fallbacks", "api_key_header_count",
    "authorization_present", "credential_in_query", "credential_in_body",
    "application_frames_sent",
}
_BALANCE_KEYS = {
    "collateralName", "balance", "equity", "availableForTrade",
    "availableForWithdrawal", "unrealisedPnl", "initialMargin", "marginRatio",
    "updatedTime", "spotEquity", "spotEquityForAvailableForTrade",
    "collateralReservedForSpotOrders",
}
_SPOT_KEYS = {
    "accountId", "asset", "balance", "indexPrice", "notionalValue",
    "contributionFactor", "equityContribution", "availableToWithdraw", "absolutePnl",
    "pnlPercentage", "averageEntryPrice", "updatedAt",
}


class PreflightViolation(Exception):
    """A bounded, redaction-safe fail-closed reason."""


@dataclass(frozen=True)
class RestRequest:
    method: str
    url: str
    path: str
    round_name: str
    headers: Mapping[str, str]
    direct_tls: bool = True
    trust_env: bool = False
    allow_redirects: bool = False
    retry_count: int = 0
    timeout_seconds: int = 10


@dataclass(frozen=True)
class StreamRequest:
    url: str
    headers: Mapping[str, str]
    direct_tls: bool = True
    trust_env: bool = False
    allow_redirects: bool = False
    retry_count: int = 0
    reconnect: bool = False
    timeout_seconds: int = 10


@dataclass(frozen=True)
class PreflightResult:
    status: str
    reason: str
    rest_calls: int
    stream_frames: int
    identity_verified: bool

    def evidence(self) -> str:
        return json.dumps(
            {
                "identity_verified": self.identity_verified,
                "reason": self.reason,
                "rest_calls": self.rest_calls,
                "status": self.status,
                "stream_frames": self.stream_frames,
            },
            sort_keys=True,
            separators=(",", ":"),
        )


@dataclass
class _Counters:
    rest_calls: int = 0
    stream_frames: int = 0


@dataclass
class _StreamState:
    previous_sequence: int | None = None


class PreflightStore:
    """A single-use SQLite terminal record containing redacted evidence only."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS extended_private_read_preflight (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    state TEXT NOT NULL,
                    evidence TEXT
                )
                """
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @staticmethod
    def _decode(raw: str) -> PreflightResult:
        value = json.loads(raw)
        if set(value) != {
            "identity_verified", "reason", "rest_calls", "status", "stream_frames"
        }:
            raise PreflightViolation("DURABLE_EVIDENCE_INVALID")
        if (
            type(value["status"]) is not str
            or value["status"] not in {"READY_FIXTURE", "BLOCKED"}
            or type(value["reason"]) is not str or not value["reason"]
            or type(value["rest_calls"]) is not int or value["rest_calls"] < 0
            or type(value["stream_frames"]) is not int or value["stream_frames"] < 0
            or type(value["identity_verified"]) is not bool
            or (value["status"] == "READY_FIXTURE" and (
                value["reason"] != "FIXTURE_CONTRACT_PROVED"
                or value["rest_calls"] != 6 or not value["identity_verified"]
            ))
            or (value["status"] == "BLOCKED" and value["identity_verified"])
        ):
            raise PreflightViolation("DURABLE_EVIDENCE_INVALID")
        return PreflightResult(
            status=value["status"],
            reason=value["reason"],
            rest_calls=value["rest_calls"],
            stream_frames=value["stream_frames"],
            identity_verified=value["identity_verified"],
        )

    def claim(self) -> PreflightResult | None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT state, evidence FROM extended_private_read_preflight WHERE singleton=1"
            ).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO extended_private_read_preflight VALUES (1, 'RUNNING', NULL)"
                )
                return None
            state, evidence = row
            if state == "TERMINAL" and type(evidence) is str:
                return self._decode(evidence)
            raise PreflightViolation("ONE_SHOT_ALREADY_CONSUMED")

    def terminal(self, result: PreflightResult) -> None:
        with self._connect() as connection:
            changed = connection.execute(
                """
                UPDATE extended_private_read_preflight
                SET state='TERMINAL', evidence=?
                WHERE singleton=1 AND state='RUNNING'
                """,
                (result.evidence(),),
            ).rowcount
            if changed != 1:
                raise PreflightViolation("DURABLE_STATE_CONFLICT")


def _exact_object(value: Any, keys: set[str], reason: str) -> dict[str, Any]:
    if type(value) is not dict or set(value) != keys:
        raise PreflightViolation(reason)
    return value


def _integer(value: Any) -> bool:
    return type(value) is int and value >= 0


def _decimal_string(value: Any) -> bool:
    if type(value) is not str:
        return False
    try:
        return Decimal(value).is_finite()
    except InvalidOperation:
        return False


def _validate_transport(value: Any, *, url: str) -> None:
    meta = _exact_object(value, _TRANSPORT_KEYS, "TRANSPORT_METADATA_MALFORMED")
    expected_headers = (
        ["User-Agent", _API_HEADER]
        if url == STREAM_URL
        else ["Accept", "Content-Type", "User-Agent", _API_HEADER]
    )
    if (
        meta["actual_url"] != url or meta["method"] != "GET"
        or meta["header_names"] != expected_headers
        or meta["api_key_header_count"] != 1
        or type(meta["api_key_header_count"]) is not int
        or meta["authorization_present"] is not False
        or meta["credential_in_query"] is not False
        or meta["credential_in_body"] is not False
        or meta["application_frames_sent"] is not False
        or meta["direct_tls"] is not True or meta["trust_env"] is not False
        or meta["proxy"] is not None or meta["redirects"] != 0
        or meta["retries"] != 0 or meta["fallbacks"] != 0
        or any(type(meta[key]) is not int for key in ("redirects", "retries", "fallbacks"))
    ):
        raise PreflightViolation("TRANSPORT_CONTRACT_MISMATCH")


def _validate_account(value: Any) -> None:
    account = _exact_object(value, _ACCOUNT_KEYS, "REST_ACCOUNT_MALFORMED")
    if (
        account["id"] != ACCOUNT_ID
        or account["accountIndex"] != ACCOUNT_INDEX
        or account["l2Key"] != L2_KEY
        or account["l2Vault"] != L2_VAULT
    ):
        raise PreflightViolation("ACCOUNT_IDENTITY_MISMATCH")
    if account["status"] != "ACTIVE":
        raise PreflightViolation("ACCOUNT_INACTIVE")
    if type(account["description"]) is not str or account["bridgeStarknetAddress"] is not None:
        raise PreflightViolation("REST_ACCOUNT_MALFORMED")


def _validate_wrapper(body: Any, path: str) -> None:
    wrapper = _exact_object(
        body, {"status", "data", "error", "pagination"}, "REST_WRAPPER_MALFORMED"
    )
    if wrapper["status"] != "OK" or wrapper["error"] is not None:
        raise PreflightViolation("REST_RESPONSE_ERROR")
    if path == "/user/account/info":
        if wrapper["pagination"] is not None:
            raise PreflightViolation("REST_PAGINATION_INVALID")
        _validate_account(wrapper["data"])
        return
    if type(wrapper["data"]) is not list:
        raise PreflightViolation("REST_LIST_MALFORMED")
    page = _exact_object(
        wrapper["pagination"], {"cursor", "count"}, "REST_PAGINATION_INVALID"
    )
    if page["cursor"] is not None:
        raise PreflightViolation("REST_PAGINATION_INCOMPLETE")
    if type(page["count"]) is not int or page["count"] != len(wrapper["data"]):
        raise PreflightViolation("REST_PAGINATION_INVALID")
    if wrapper["data"]:
        reason = "REST_OPEN_ORDER_PRESENT" if path == "/user/orders" else "REST_POSITION_PRESENT"
        raise PreflightViolation(reason)


async def _rest_round(
    transport: Any, api_key: str, round_name: str, now_ms: int, counters: _Counters
) -> list[int]:
    observations: list[int] = []
    for path in _REST_PATHS:
        request = RestRequest(
            method="GET",
            url=f"{REST_BASE_URL}{path}",
            path=path,
            round_name=round_name,
            headers={_API_HEADER: api_key},
        )
        counters.rest_calls += 1
        reply = await transport.get(request)
        reply = _exact_object(
            reply, {"status", "final_url", "observed_at_ms", "body", "transport"}, "REST_REPLY_MALFORMED"
        )
        _validate_transport(reply["transport"], url=request.url)
        if reply["status"] != 200:
            raise PreflightViolation("REST_HTTP_STATUS")
        if reply["final_url"] != request.url:
            raise PreflightViolation("REST_REDIRECT_FORBIDDEN")
        observed = reply["observed_at_ms"]
        if not _integer(observed) or observed > now_ms or now_ms - observed > _MAX_OBSERVATION_AGE_MS:
            raise PreflightViolation("REST_EVIDENCE_STALE")
        _validate_wrapper(reply["body"], path)
        observations.append(observed)
    return observations


def _validate_stream_frame(raw: Any, previous: int | None) -> int:
    frame = _exact_object(
        raw, {"type", "data", "error", "ts", "seq"}, "STREAM_MALFORMED_FRAME"
    )
    if frame["type"] not in _STREAM_TYPES:
        raise PreflightViolation("STREAM_UNKNOWN_FRAME")
    if frame["error"] is not None or not _integer(frame["ts"]) or not _integer(frame["seq"]):
        raise PreflightViolation("STREAM_MALFORMED_FRAME")
    data = _exact_object(frame["data"], _STREAM_DATA_KEYS, "STREAM_MALFORMED_FRAME")
    matching = {
        "ORDER": "orders",
        "POSITION": "positions",
        "TRADE": "trades",
        "BALANCE": "balance",
        "SPOT_BALANCE": "spotBalances",
    }[frame["type"]]
    for key in _STREAM_DATA_KEYS:
        value = data[key]
        if key == matching:
            required_type = dict if key == "balance" else list
            if type(value) is not required_type:
                raise PreflightViolation("STREAM_TYPE_PAYLOAD_MISMATCH")
        elif value is not None:
            raise PreflightViolation("STREAM_TYPE_PAYLOAD_MISMATCH")
    if frame["type"] == "BALANCE":
        balance = _exact_object(data["balance"], _BALANCE_KEYS, "STREAM_BALANCE_MALFORMED")
        if (
            type(balance["collateralName"]) is not str
            or not _integer(balance["updatedTime"])
            or any(not _decimal_string(balance[key]) for key in _BALANCE_KEYS - {"collateralName", "updatedTime"})
        ):
            raise PreflightViolation("STREAM_BALANCE_MALFORMED")
    if frame["type"] == "SPOT_BALANCE":
        for row in data["spotBalances"]:
            spot = _exact_object(row, _SPOT_KEYS, "STREAM_SPOT_BALANCE_MALFORMED")
            if (
                spot["accountId"] != ACCOUNT_ID or type(spot["asset"]) is not str
                or not _integer(spot["updatedAt"])
                or any(
                    spot[key] is not None and not _decimal_string(spot[key])
                    for key in _SPOT_KEYS - {"accountId", "asset", "updatedAt"}
                )
            ):
                raise PreflightViolation("STREAM_SPOT_BALANCE_MALFORMED")
    if frame["type"] == "ORDER" and data["orders"]:
        raise PreflightViolation("STREAM_ORDER_ACTIVITY")
    if frame["type"] == "POSITION" and data["positions"]:
        raise PreflightViolation("STREAM_POSITION_ACTIVITY")
    if frame["type"] == "TRADE" and data["trades"]:
        raise PreflightViolation("STREAM_TRADE_ACTIVITY")
    sequence = frame["seq"]
    if previous is not None:
        if sequence == previous:
            raise PreflightViolation("STREAM_SEQUENCE_DUPLICATE")
        if sequence < previous:
            raise PreflightViolation("STREAM_SEQUENCE_REGRESSION")
        if sequence != previous + 1:
            raise PreflightViolation("STREAM_SEQUENCE_GAP")
    return sequence


async def _consume_stream(
    stream: Any, counters: _Counters, state: _StreamState
) -> None:
    while True:
        try:
            raw = await stream.recv()
        except StopAsyncIteration as exc:
            if getattr(stream, "barrier_complete", False) is True:
                return
            raise PreflightViolation("STREAM_ENDED_EARLY") from exc
        except (ConnectionError, OSError) as exc:
            raise PreflightViolation("STREAM_DISCONNECTED") from exc
        counters.stream_frames += 1
        state.previous_sequence = _validate_stream_frame(raw, state.previous_sequence)


async def _load_api_key(loader: Any) -> str:
    loaded = loader()
    if inspect.isawaitable(loaded):
        loaded = await loaded
    if type(loaded) is not str or not loaded:
        raise PreflightViolation("CREDENTIAL_INVALID")
    return loaded


async def _execute(
    credential_loader: Any, transport: Any, now_ms: int, counters: _Counters
) -> PreflightResult:
    api_key = await _load_api_key(credential_loader)
    round_a = await _rest_round(transport, api_key, "A", now_ms, counters)
    stream = await transport.open_stream(
        StreamRequest(url=STREAM_URL, headers={_API_HEADER: api_key})
    )
    state = _StreamState()
    consumer: asyncio.Task[None] | None = None
    failure: PreflightViolation | None = None
    try:
        _validate_transport(getattr(stream, "upgrade_metadata", None), url=STREAM_URL)
        if getattr(stream, "outbound_frames", None) != []:
            raise PreflightViolation("STREAM_OUTBOUND_FORBIDDEN")
        if getattr(stream, "reconnect_count", 0) != 0:
            raise PreflightViolation("STREAM_RECONNECT_FORBIDDEN")
        consumer = asyncio.create_task(_consume_stream(stream, counters, state))
        round_b_task = asyncio.create_task(
            _rest_round(transport, api_key, "B", now_ms, counters)
        )
        done, _ = await asyncio.wait(
            {consumer, round_b_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if consumer in done:
            round_b_task.cancel()
            try:
                await round_b_task
            except asyncio.CancelledError:
                pass
            await consumer
            raise PreflightViolation("STREAM_ENDED_EARLY")
        round_b = await round_b_task
        if min(round_b) <= max(round_a):
            raise PreflightViolation("REST_ROUNDS_NOT_ORDERED")
        try:
            barrier = await stream.final_barrier()
        except StopAsyncIteration as exc:
            raise PreflightViolation("STREAM_ENDED_EARLY") from exc
        except (ConnectionError, OSError) as exc:
            raise PreflightViolation("STREAM_DISCONNECTED") from exc
        barrier = _exact_object(
            barrier,
            {"connected", "same_connection", "outbound_frames", "reconnect_count", "frames", "transport"},
            "STREAM_BARRIER_UNVERIFIABLE",
        )
        _validate_transport(barrier["transport"], url=STREAM_URL)
        if barrier["connected"] is not True or barrier["same_connection"] is not True:
            raise PreflightViolation("STREAM_ENDED_EARLY")
        if barrier["outbound_frames"] != []:
            raise PreflightViolation("STREAM_OUTBOUND_FORBIDDEN")
        if barrier["reconnect_count"] != 0:
            raise PreflightViolation("STREAM_RECONNECT_FORBIDDEN")
        if type(barrier["frames"]) is not list:
            raise PreflightViolation("STREAM_BARRIER_UNVERIFIABLE")
        await consumer
        for raw in barrier["frames"]:
            counters.stream_frames += 1
            state.previous_sequence = _validate_stream_frame(raw, state.previous_sequence)
        if getattr(stream, "closed", False):
            raise PreflightViolation("STREAM_ENDED_EARLY")
        if getattr(stream, "outbound_frames", None) != []:
            raise PreflightViolation("STREAM_OUTBOUND_FORBIDDEN")
        if getattr(stream, "reconnect_count", 0) != 0:
            raise PreflightViolation("STREAM_RECONNECT_FORBIDDEN")
    except PreflightViolation as exc:
        failure = exc
    finally:
        if consumer is not None and not consumer.done():
            consumer.cancel()
            try:
                await consumer
            except asyncio.CancelledError:
                pass
        try:
            await stream.close()
        except Exception:
            if failure is None:
                failure = PreflightViolation("STREAM_CLOSE_FAILED")
    if failure is not None:
        raise failure
    return PreflightResult(
        "READY_FIXTURE", "FIXTURE_CONTRACT_PROVED",
        counters.rest_calls, counters.stream_frames, True,
    )


async def run_preflight(
    *, store: PreflightStore, credential_loader: Any, transport: Any, now_ms: int
) -> PreflightResult:
    """Consume the fixture one-shot and persist one redacted terminal verdict."""

    existing = store.claim()
    if existing is not None:
        return existing
    counters = _Counters()
    try:
        result = await _execute(credential_loader, transport, now_ms, counters)
    except asyncio.CancelledError:
        result = PreflightResult(
            "BLOCKED", "CANCELLED", counters.rest_calls, counters.stream_frames, False
        )
        store.terminal(result)
        raise
    except PreflightViolation as exc:
        result = PreflightResult(
            "BLOCKED", str(exc), counters.rest_calls, counters.stream_frames, False
        )
    except Exception:
        result = PreflightResult(
            "BLOCKED", "UNEXPECTED_FAILURE", counters.rest_calls,
            counters.stream_frames, False,
        )
    store.terminal(result)
    return result


__all__ = [
    "ACCOUNT_ID", "REST_BASE_URL", "STREAM_URL", "PreflightResult",
    "PreflightStore", "RestRequest", "StreamRequest", "run_preflight",
]
