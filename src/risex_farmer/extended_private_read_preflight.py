"""Isolated fixture contract for a sealed Extended v1 private-read preflight.

The module owns no live transport.  A later operational gate must inject the
credential loader and direct transport; normal Farmer startup never imports it.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import sqlite3
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
    "BALANCE", "SPOT_BALANCE", "DEPOSIT", "ORDER", "POSITION", "TRADE",
    "TRANSFER", "WITHDRAWAL",
}
_STREAM_DATA_KEYS = {"orders", "positions", "trades", "balance", "spotBalances"}
_MAX_OBSERVATION_AGE_MS = 5_000


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


async def _rest_round(transport: Any, api_key: str, round_name: str, now_ms: int) -> list[int]:
    observations: list[int] = []
    for path in _REST_PATHS:
        request = RestRequest(
            method="GET",
            url=f"{REST_BASE_URL}{path}",
            path=path,
            round_name=round_name,
            headers={_API_HEADER: api_key},
        )
        reply = await transport.get(request)
        reply = _exact_object(
            reply, {"status", "final_url", "observed_at_ms", "body"}, "REST_REPLY_MALFORMED"
        )
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
    if any(
        data[key] is not None and type(data[key]) is not list
        for key in ("orders", "positions", "trades")
    ):
        raise PreflightViolation("STREAM_MALFORMED_FRAME")
    if data["orders"]:
        raise PreflightViolation("STREAM_ORDER_ACTIVITY")
    if data["positions"]:
        raise PreflightViolation("STREAM_POSITION_ACTIVITY")
    if data["trades"]:
        raise PreflightViolation("STREAM_TRADE_ACTIVITY")
    if data["balance"] is not None and type(data["balance"]) is not dict:
        raise PreflightViolation("STREAM_MALFORMED_FRAME")
    if data["spotBalances"] is not None and type(data["spotBalances"]) is not list:
        raise PreflightViolation("STREAM_MALFORMED_FRAME")
    sequence = frame["seq"]
    if previous is not None:
        if sequence == previous:
            raise PreflightViolation("STREAM_SEQUENCE_DUPLICATE")
        if sequence < previous:
            raise PreflightViolation("STREAM_SEQUENCE_REGRESSION")
        if sequence != previous + 1:
            raise PreflightViolation("STREAM_SEQUENCE_GAP")
    return sequence


async def _receive(stream: Any, previous: int | None) -> int:
    if getattr(stream, "closed", False):
        raise PreflightViolation("STREAM_ENDED_EARLY")
    try:
        raw = await stream.recv()
    except StopAsyncIteration as exc:
        raise PreflightViolation("STREAM_ENDED_EARLY") from exc
    except (ConnectionError, OSError) as exc:
        raise PreflightViolation("STREAM_DISCONNECTED") from exc
    return _validate_stream_frame(raw, previous)


async def _load_api_key(loader: Any) -> str:
    loaded = loader()
    if inspect.isawaitable(loaded):
        loaded = await loaded
    if type(loaded) is not str or not loaded:
        raise PreflightViolation("CREDENTIAL_INVALID")
    return loaded


async def _execute(credential_loader: Any, transport: Any, now_ms: int) -> PreflightResult:
    api_key = await _load_api_key(credential_loader)
    round_a = await _rest_round(transport, api_key, "A", now_ms)
    stream = await transport.open_stream(
        StreamRequest(url=STREAM_URL, headers={_API_HEADER: api_key})
    )
    frames = 0
    previous: int | None = None
    failure: PreflightViolation | None = None
    try:
        if getattr(stream, "outbound_frames", None) != []:
            raise PreflightViolation("STREAM_OUTBOUND_FORBIDDEN")
        if getattr(stream, "reconnect_count", 0) != 0:
            raise PreflightViolation("STREAM_RECONNECT_FORBIDDEN")
        previous = await _receive(stream, previous)
        frames += 1

        round_b: list[int] = []
        for path in _REST_PATHS:
            request = RestRequest(
                method="GET", url=f"{REST_BASE_URL}{path}", path=path,
                round_name="B", headers={_API_HEADER: api_key},
            )
            reply = await transport.get(request)
            reply = _exact_object(
                reply, {"status", "final_url", "observed_at_ms", "body"},
                "REST_REPLY_MALFORMED",
            )
            if reply["status"] != 200 or reply["final_url"] != request.url:
                raise PreflightViolation(
                    "REST_HTTP_STATUS" if reply["status"] != 200 else "REST_REDIRECT_FORBIDDEN"
                )
            observed = reply["observed_at_ms"]
            if not _integer(observed) or observed > now_ms or now_ms - observed > _MAX_OBSERVATION_AGE_MS:
                raise PreflightViolation("REST_EVIDENCE_STALE")
            _validate_wrapper(reply["body"], path)
            round_b.append(observed)
            previous = await _receive(stream, previous)
            frames += 1

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
            {"connected", "same_connection", "outbound_frames", "reconnect_count", "frames"},
            "STREAM_BARRIER_UNVERIFIABLE",
        )
        if barrier["connected"] is not True or barrier["same_connection"] is not True:
            raise PreflightViolation("STREAM_ENDED_EARLY")
        if barrier["outbound_frames"] != []:
            raise PreflightViolation("STREAM_OUTBOUND_FORBIDDEN")
        if barrier["reconnect_count"] != 0:
            raise PreflightViolation("STREAM_RECONNECT_FORBIDDEN")
        if type(barrier["frames"]) is not list or not barrier["frames"]:
            raise PreflightViolation("STREAM_ENDED_EARLY")
        for raw in barrier["frames"]:
            previous = _validate_stream_frame(raw, previous)
            frames += 1
        if getattr(stream, "closed", False):
            raise PreflightViolation("STREAM_ENDED_EARLY")
        if getattr(stream, "outbound_frames", None) != []:
            raise PreflightViolation("STREAM_OUTBOUND_FORBIDDEN")
        if getattr(stream, "reconnect_count", 0) != 0:
            raise PreflightViolation("STREAM_RECONNECT_FORBIDDEN")
    except PreflightViolation as exc:
        failure = exc
    finally:
        try:
            await stream.close()
        except Exception:
            if failure is None:
                failure = PreflightViolation("STREAM_CLOSE_FAILED")
    if failure is not None:
        raise failure
    return PreflightResult("READY_FIXTURE", "FIXTURE_CONTRACT_PROVED", 6, frames, True)


async def run_preflight(
    *, store: PreflightStore, credential_loader: Any, transport: Any, now_ms: int
) -> PreflightResult:
    """Consume the fixture one-shot and persist one redacted terminal verdict."""

    existing = store.claim()
    if existing is not None:
        return existing
    try:
        result = await _execute(credential_loader, transport, now_ms)
    except asyncio.CancelledError:
        result = PreflightResult("BLOCKED", "CANCELLED", 0, 0, False)
        store.terminal(result)
        raise
    except PreflightViolation as exc:
        result = PreflightResult("BLOCKED", str(exc), 0, 0, False)
    except Exception:
        result = PreflightResult("BLOCKED", "UNEXPECTED_FAILURE", 0, 0, False)
    store.terminal(result)
    return result


__all__ = [
    "ACCOUNT_ID", "REST_BASE_URL", "STREAM_URL", "PreflightResult",
    "PreflightStore", "RestRequest", "StreamRequest", "run_preflight",
]
