"""Sealed operational binding for the accepted RISEx testnet lifecycle.

This venue-local module is absent from normal startup.  It adds only a durable
runtime run identity, the provisioned session-signer operation, and the two
official write transports consumed by ``testnet_risex_order_lifecycle``.

Construction is not write authority.  A separate operational gate must first
refresh and verify the official config/domain/router/authorization identities.
"""

from __future__ import annotations

import base64
import http.client
import json
import os
from pathlib import Path
import sqlite3
import ssl
import time
from typing import Any, Callable
import uuid

from . import testnet_risex_signer as _signer
from .testnet_risex_order_lifecycle import (
    CANCEL_ACTION, HEADER_FLAGS, OFFICIAL_CHAIN_ID, OFFICIAL_DOMAIN_NAME,
    OFFICIAL_DOMAIN_VERSION, Intent, Lifecycle,
    LifecycleSafetyError, MarketState, SyntheticSigner, _address,
    _composite_wide_order_id, _valid_order_id, encode_cancel_action,
    encode_place_action, pack_order_data,
)
from .testnet_risex_private_read_operational import (
    SessionSignerCredential, _fsync_file_and_parent, _home,
    _prepare_sqlite_file, _strict_json,
)
from .testnet_risex_private_read_preflight import (
    ACCOUNT, AUTHORIZATION, REST_ORIGIN, ROUTER, SIGNER,
)


# Official direct-integration contract: developer.rise.trade/reference/integration
PLACE_PATH = "/v1/orders/place"
CANCEL_PATH = "/v1/orders/cancel"
RUN_JOURNAL_NAME = ".risex-funding-farmer-risex-level-c-runs-v1.sqlite"
_OFFICIAL_HOST = "api.testnet.rise.trade"
_MAX_RESPONSE_BYTES = 1_048_576
_DEADLINE_SECONDS = 5

_RUN_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    created_at INTEGER NOT NULL,
    state TEXT NOT NULL CHECK(state = 'STARTED')
);
"""

_DOMAIN_FIELDS = [
    {"name": "name", "type": "string"},
    {"name": "version", "type": "string"},
    {"name": "chainId", "type": "uint256"},
    {"name": "verifyingContract", "type": "address"},
]
_WITNESS_FIELDS = [
    {"name": "account", "type": "address"},
    {"name": "target", "type": "address"},
    {"name": "hash", "type": "bytes32"},
    {"name": "nonceAnchor", "type": "uint48"},
    {"name": "nonceBitmap", "type": "uint8"},
    {"name": "deadline", "type": "uint32"},
]


def _catalog(connection: sqlite3.Connection) -> tuple[tuple[Any, ...], ...]:
    return tuple(connection.execute(
        "SELECT type,name,tbl_name,COALESCE(sql,'') FROM sqlite_master "
        "ORDER BY type,name,tbl_name,sql"
    ))


def _expected_catalog() -> tuple[tuple[Any, ...], ...]:
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(_RUN_SCHEMA)
        return _catalog(connection)
    finally:
        connection.close()


class RuntimeRunJournal:
    """Protected append-only run identities, intentionally separate from intents."""

    def __init__(self) -> None:
        self._path = _home() / RUN_JOURNAL_NAME

    @classmethod
    def _fixture(cls, path: Path) -> "RuntimeRunJournal":
        value = object.__new__(cls)
        value._path = Path(path)
        return value

    def begin(self, created_at: int) -> str:
        if not isinstance(created_at, int) or isinstance(created_at, bool) or created_at <= 0:
            raise LifecycleSafetyError("RISEx runtime journal rejected")
        if not _prepare_sqlite_file(self._path):
            raise LifecycleSafetyError("RISEx runtime journal rejected")
        run_id = str(uuid.uuid4())
        connection = sqlite3.connect(self._path)
        try:
            connection.execute("PRAGMA synchronous=FULL")
            connection.executescript(_RUN_SCHEMA)
            if (
                connection.execute("PRAGMA integrity_check").fetchall() != [("ok",)]
                or _catalog(connection) != _expected_catalog()
            ):
                raise sqlite3.DatabaseError
            with connection:
                connection.execute(
                    "INSERT INTO runs(run_id, created_at, state) VALUES (?, ?, 'STARTED')",
                    (run_id, created_at),
                )
        except (sqlite3.DatabaseError, OSError):
            raise LifecycleSafetyError("RISEx runtime journal rejected") from None
        finally:
            connection.close()
        _fsync_file_and_parent(self._path)
        return run_id


class SealedWriteTransport:
    """Two exact official POST surfaces with no retry or redirect behavior."""

    REST_ORIGIN = REST_ORIGIN
    TRUST_ENV = False
    ALLOW_REDIRECTS = False
    MAX_BYTES = _MAX_RESPONSE_BYTES
    DEADLINE_SECONDS = _DEADLINE_SECONDS

    def __init__(self) -> None:
        self._connection_factory: Callable[[], Any] = lambda: http.client.HTTPSConnection(
            _OFFICIAL_HOST, timeout=self.DEADLINE_SECONDS,
            context=ssl.create_default_context(),
        )

    def _post(self, path: str, body: dict[str, Any]) -> Any:
        if path not in {PLACE_PATH, CANCEL_PATH}:
            raise LifecycleSafetyError("RISEx write surface rejected")
        try:
            encoded = json.dumps(
                body, sort_keys=True, separators=(",", ":"), allow_nan=False,
            ).encode("utf-8")
        except Exception:
            raise LifecycleSafetyError("RISEx write body rejected") from None
        connection = self._connection_factory()
        try:
            connection.request(
                "POST", path, body=encoded,
                headers={"Content-Type": "application/json", "Accept": "application/json"},
            )
            response = connection.getresponse()
            declared = response.getheader("Content-Length")
            if declared is not None and int(declared) > self.MAX_BYTES:
                raise LifecycleSafetyError("RISEx write response rejected")
            raw = response.read(self.MAX_BYTES + 1)
            if len(raw) > self.MAX_BYTES or not 200 <= response.status < 300:
                raise LifecycleSafetyError("RISEx write outcome requires reconciliation")
            return _strict_json(raw.decode("utf-8", errors="strict"))
        except LifecycleSafetyError:
            raise
        except Exception:
            raise LifecycleSafetyError("RISEx write outcome requires reconciliation") from None
        finally:
            connection.close()

    def place(self, body: dict[str, Any]) -> str:
        response = self._post(PLACE_PATH, body)
        try:
            order_id = response["data"]["order_id"]
        except (KeyError, TypeError):
            raise LifecycleSafetyError("RISEx place response requires reconciliation") from None
        if not _valid_order_id(order_id):
            raise LifecycleSafetyError("RISEx place response requires reconciliation")
        return order_id

    def cancel(self, body: dict[str, Any]) -> Any:
        response = self._post(CANCEL_PATH, body)
        if not isinstance(response, dict) or not isinstance(response.get("data"), dict):
            raise LifecycleSafetyError("RISEx cancel response requires reconciliation")
        return response


class SessionOrderCredential(SessionSignerCredential):
    """Dedicated session key handle; only canonical VerifyWitness is signable."""

    def __init__(self, signer: str, secret: bytes) -> None:
        super().__init__(_address(signer).lower(), secret)

    def sign_permit(self, typed_data: dict[str, Any]) -> str:
        if self.closed or typed_data != _canonical_typed_data(typed_data):
            raise LifecycleSafetyError("RISEx permit signing rejected")
        try:
            signature = _signer._sign_typed_data(bytes(self._secret), typed_data)
            raw = bytes.fromhex(signature[2:])
            if len(raw) != 65 or raw[64] not in {27, 28}:
                raise ValueError
            compact_s = bytearray(raw[32:64])
            if raw[64] == 28:
                compact_s[0] |= 0x80
            return base64.b64encode(raw[:32] + bytes(compact_s)).decode("ascii")
        except LifecycleSafetyError:
            raise
        except Exception:
            raise LifecycleSafetyError("RISEx permit signing rejected") from None

    def sign_register_v2(self, typed_data: dict[str, Any]) -> str:
        raise LifecycleSafetyError("RISEx permit-only credential rejected")


def _canonical_typed_data(value: Any) -> dict[str, Any]:
    try:
        if set(value) != {"types", "primaryType", "domain", "message"}:
            raise ValueError
        types = value["types"]
        domain = value["domain"]
        message = value["message"]
        if (
            types != {"EIP712Domain": _DOMAIN_FIELDS, "VerifyWitness": _WITNESS_FIELDS}
            or value["primaryType"] != "VerifyWitness"
            or set(domain) != {"name", "version", "chainId", "verifyingContract"}
            or domain["name"] != OFFICIAL_DOMAIN_NAME
            or domain["version"] != OFFICIAL_DOMAIN_VERSION
            or domain["chainId"] != OFFICIAL_CHAIN_ID
            or _address(domain["verifyingContract"]).lower() != AUTHORIZATION
            or set(message) != {
                "account", "target", "hash", "nonceAnchor", "nonceBitmap", "deadline",
            }
            or _address(message["account"]).lower() != ACCOUNT
            or _address(message["target"]).lower() != ROUTER
            or not isinstance(message["hash"], str) or len(message["hash"]) != 66
            or not message["hash"].startswith("0x")
        ):
            raise ValueError
        int(message["hash"][2:], 16)
        widths = (("nonceAnchor", 48), ("nonceBitmap", 8), ("deadline", 32))
        for key, bits in widths:
            item = message[key]
            if not isinstance(item, int) or isinstance(item, bool) or not 0 <= item < 2**bits:
                raise ValueError
        if message["nonceBitmap"] > 207:
            raise ValueError
        return value
    except Exception:
        raise LifecycleSafetyError("RISEx permit signing rejected") from None


def _credential_from_secret(secret: bytes, expected: str) -> SessionOrderCredential:
    if not isinstance(secret, bytes) or len(secret) != 32:
        raise LifecycleSafetyError("RISEx session signer rejected")
    try:
        if _signer._derive_address(secret) != _address(expected).lower():
            raise LifecycleSafetyError("RISEx session signer rejected")
        return SessionOrderCredential(expected, secret)
    except LifecycleSafetyError:
        raise
    except Exception:
        raise LifecycleSafetyError("RISEx session signer rejected") from None


def _load_session_signer_only() -> SessionOrderCredential:
    home_fd = _signer._open_home()
    material = bytearray()
    try:
        record = _signer._load_record(home_fd)
        material.extend(_signer._load_credential(home_fd))
    finally:
        os.close(home_fd)
    try:
        if (
            record.state is not _signer.SignerState.ACTIVE
            or record.signer != SIGNER or record.expiration <= int(time.time())
        ):
            raise LifecycleSafetyError("RISEx session signer rejected")
        return _credential_from_secret(bytes(material), SIGNER)
    finally:
        for index in range(len(material)):
            material[index] = 0
        material.clear()


def _permit_matches(
    typed: dict[str, Any], *, account: str, signer: str, nonce_anchor: str,
    nonce_bitmap: int, deadline: int, action_hash: bytes, expected_signer: str,
) -> bool:
    canonical = _canonical_typed_data(typed)
    message = canonical["message"]
    return (
        account == ACCOUNT and signer == expected_signer
        and message["account"] == account and message["target"] == ROUTER
        and message["hash"] == "0x" + action_hash.hex()
        and nonce_anchor == str(message["nonceAnchor"])
        and nonce_bitmap == message["nonceBitmap"]
        and deadline == message["deadline"]
    )


def _signed_place(
    request: dict[str, Any], credential: SessionOrderCredential,
    expected_signer: str,
) -> dict[str, Any]:
    expected_keys = {
        "header_flags", "order_data", "abi_encoded", "action_hash", "permit",
        "body", "signature", "dispatchable",
    }
    body_keys = {
        "market_id", "size_steps", "price_ticks", "side", "order_type",
        "time_in_force", "post_only", "reduce_only", "stp_mode",
        "client_order_id", "account", "signer", "nonce_anchor",
        "nonce_bitmap_index", "deadline",
    }
    try:
        body = request["body"]
        if (
            type(body.get("side")) is not int
            or type(body.get("order_type")) is not int
            or type(body.get("time_in_force")) is not int
            or type(body.get("stp_mode")) is not int
            or type(body.get("post_only")) is not bool
            or type(body.get("reduce_only")) is not bool
        ):
            raise ValueError
        order_data = pack_order_data(
            market_id=body["market_id"], size_steps=body["size_steps"],
            price_ticks=body["price_ticks"], side={0: "BUY", 1: "SELL"}[body["side"]],
            post_only=body["post_only"], reduce_only=body["reduce_only"],
            order_type={0: "MARKET", 1: "LIMIT"}[body["order_type"]],
            time_in_force={0: "GTC", 1: "GTT", 2: "FOK", 3: "IOC"}[
                body["time_in_force"]
            ],
        )
        encoded, action_hash = encode_place_action(
            order_data=order_data, client_order_id=body["client_order_id"],
        )
        if (
            set(request) != expected_keys or set(body) != body_keys
            or request["signature"] is not None or request["dispatchable"] is not False
            or body["stp_mode"] != 0 or request["header_flags"] != HEADER_FLAGS
            or request["order_data"] != order_data or request["abi_encoded"] != encoded
            or request["action_hash"] != action_hash
            or credential.signer != expected_signer
            or not _permit_matches(
                request["permit"], account=body["account"], signer=body["signer"],
                nonce_anchor=body["nonce_anchor"],
                nonce_bitmap=body["nonce_bitmap_index"], deadline=body["deadline"],
                action_hash=action_hash, expected_signer=expected_signer,
            )
        ):
            raise ValueError
        signature = credential.sign_permit(request["permit"])
        permit = {
            "account": body["account"], "signer": body["signer"],
            "nonce_anchor": body["nonce_anchor"],
            "nonce_bitmap_index": body["nonce_bitmap_index"],
            "deadline": body["deadline"], "signature": signature,
        }
        return {
            key: body[key] for key in (
                "market_id", "size_steps", "price_ticks", "side", "post_only",
                "reduce_only", "stp_mode", "order_type", "time_in_force",
                "client_order_id",
            )
        } | {"permit": permit}
    except LifecycleSafetyError:
        raise
    except Exception:
        raise LifecycleSafetyError("RISEx place binding rejected") from None


def _signed_cancel(
    request: dict[str, Any], credential: SessionOrderCredential,
    expected_signer: str,
) -> dict[str, Any]:
    try:
        if set(request) != {
            "action", "market_id", "resting_order_id", "abi_encoded",
            "action_hash", "permit", "body", "signature", "dispatchable",
        } or request["action"] != CANCEL_ACTION:
            raise ValueError
        body = request["body"]
        permit = body["permit"]
        encoded, action_hash = encode_cancel_action(
            market_id=request["market_id"],
            resting_order_id=request["resting_order_id"],
        )
        if (
            set(body) != {"market_id", "order_id", "permit"}
            or set(permit) != {
                "account", "signer", "nonce_anchor", "nonce_bitmap_index",
                "deadline", "signature",
            }
            or permit["signature"] is not None or request["signature"] is not None
            or request["dispatchable"] is not False
            or body["market_id"] != request["market_id"]
            or _composite_wide_order_id(body["order_id"]) >> 1
            != request["resting_order_id"]
            or request["abi_encoded"] != encoded or request["action_hash"] != action_hash
            or credential.signer != expected_signer
            or not _permit_matches(
                request["permit"], account=permit["account"],
                signer=permit["signer"], nonce_anchor=permit["nonce_anchor"],
                nonce_bitmap=permit["nonce_bitmap_index"], deadline=permit["deadline"],
                action_hash=action_hash, expected_signer=expected_signer,
            )
        ):
            raise ValueError
        signature = credential.sign_permit(request["permit"])
        return {
            "market_id": body["market_id"], "order_id": body["order_id"],
            "permit": dict(permit, signature=signature),
        }
    except LifecycleSafetyError:
        raise
    except Exception:
        raise LifecycleSafetyError("RISEx cancel binding rejected") from None


class OperationalBinding:
    """One fresh runtime identity bound to exact production dependencies."""

    ACCOUNT = ACCOUNT
    SIGNER = SIGNER
    ROUTER = ROUTER
    AUTHORIZATION = AUTHORIZATION
    CHAIN_ID = OFFICIAL_CHAIN_ID
    DOMAIN = (OFFICIAL_DOMAIN_NAME, OFFICIAL_DOMAIN_VERSION)

    def __init__(self) -> None:
        self._journal = RuntimeRunJournal()
        self._transport = SealedWriteTransport()
        self._credential_loader: Callable[[], SessionOrderCredential] = (
            _load_session_signer_only
        )
        self._expected_signer = SIGNER
        self.run_id = self._journal.begin(int(time.time()))

    @classmethod
    def _fixture(
        cls, journal: RuntimeRunJournal, transport: Any,
        credential_loader: Callable[[], SessionOrderCredential], *,
        expected_signer: str, now: Callable[[], int],
    ) -> "OperationalBinding":
        value = object.__new__(cls)
        value._journal = journal
        value._transport = transport
        value._credential_loader = credential_loader
        value._expected_signer = _address(expected_signer).lower()
        value.run_id = journal.begin(now())
        return value

    def dispatch_place(
        self, lifecycle: Lifecycle, intent: Intent, market: MarketState,
    ) -> None:
        marker = SyntheticSigner(self._expected_signer)

        def execute(_redacted: dict[str, Any]) -> str:
            credential = self._credential_loader()
            try:
                request = lifecycle.unsigned_request(intent.intent_id, market=market)
                body = _signed_place(request, credential, self._expected_signer)
                return self._transport.place(body)
            finally:
                credential.close()

        lifecycle.dispatch(intent, marker, execute)

    def cancel_known(
        self, lifecycle: Lifecycle, order_id: str, *, market: MarketState,
        nonce_anchor: int, nonce_bitmap: int, expires_at: int,
    ) -> None:
        marker = SyntheticSigner(self._expected_signer)

        def execute(request: dict[str, Any]) -> Any:
            credential = self._credential_loader()
            try:
                body = _signed_cancel(request, credential, self._expected_signer)
                return self._transport.cancel(body)
            finally:
                credential.close()

        lifecycle.cancel_known(
            order_id, market=market, nonce_anchor=nonce_anchor,
            nonce_bitmap=nonce_bitmap, expires_at=expires_at,
            synthetic_signer=marker, execute=execute,
        )
