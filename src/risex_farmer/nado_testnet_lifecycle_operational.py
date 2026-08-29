"""Sealed zero-argument Nado Ink Sepolia Level-C lifecycle runner.

The normal Farmer never imports this module.  Production construction fixes the
account files, journal, environment, product and transports; only the private
``_fixture_run`` seam accepts injected observations and never opens credentials
or sockets.
"""

from __future__ import annotations

import asyncio
import aiohttp
import brotli
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
import hashlib
import http.client
import inspect
import json
import os
from pathlib import Path
import pwd
import secrets
import sqlite3
import ssl
import stat
import time
from typing import Callable, Protocol
import uuid
import zlib

from eth_keys import keys

from .nado_private_read_operational import (
    KEY_BASENAME, SUBACCOUNT_NAME, _load_owner_capability, _recover_owner,
    _strict_identity, run as _accepted_private_read,
)
from .nado_private_read_preflight import (
    ALL_PRODUCTS_MAX_RESPONSE_BYTES, FAILURE_CLASSES, MAX_FRESHNESS_MS,
    MAX_RESPONSE_BYTES, SUBACCOUNT_INFO_MAX_RESPONSE_BYTES,
    FixedPreflightIdentity, NadoPreflightError, ObservedResponse,
    _sanitized_failure_class as _preflight_failure_class,
    _server_time_observation, list_trigger_orders_typed_data,
)
from .nado_testnet_lifecycle import (
    ACTIVE_PERP, CANCEL_ALL, CLOSE, COMPLETE, ENTRY,
    EXECUTE_RESPONSE_AMBIGUITY, EXECUTE_TRANSPORT_AMBIGUITY,
    EXECUTE_VENUE_REJECTION, FUNDING_BLOCKED_CONTRADICTORY,
    FUNDING_BLOCKED_MISSING, FUNDING_BOUNDARY_INTERVAL_MS, FUNDING_UNRESOLVED,
    IOC_APPENDIX, MAX_CLOSE_ATTEMPTS, UINT32_MAX,
    LONG, NADO_VENUE, RISEX_VENUE, SHORT,
    AccountSnapshot, CatalogSnapshot, CrossRunAttestation, EngineEvidence,
    FundingBoundaryBinding, FundingRouteBinding, IntentStore, ExecuteFailure,
    JournalIdentity, LifecycleCore, NadoAccountFunding, NadoContractError,
    NadoFundingBaseline, NadoFundingEvent, NadoFundingExposure,
    OrderEvidence, OrderIntent,
    Product, Reconciliation, RisexTerminalEvidence, TerminalEvidence,
    SyntheticOrderVector, TriggerSnapshot, build_order_nonce,
    decode_subaccount, encode_subaccount,
    canonical_payload, verify_signed_validation, _notional_x18,
    completion_barrier, order_digest, smallest_executable_amount,
    nado_account_funding_digest, nado_funding_event_digest,
    nado_funding_exposure_digest,
    cross_run_attestation_digest, terminal_evidence_digest,
    _assert_authoritative_account,
    validate_entry_preflight,
)


RUN_STORE_BASENAME = ".risex-funding-farmer-nado-level-c-v1.sqlite3"
REDACTED_STORE_PATH = "<passwd-home>/" + RUN_STORE_BASENAME
# The completed historical Level-C runner above is deliberately not the
# funding-boundary writer's store.  A new fixed basename gives this fresh
# route its own protected SQLite identity and keeps the historical SKR store
# untouched.
FUNDING_BOUNDARY_RUN_STORE_BASENAME = (
    ".risex-funding-farmer-nado-funding-boundary-eth-v7.sqlite3"
)
FUNDING_BOUNDARY_REDACTED_STORE_PATH = (
    "<passwd-home>/" + FUNDING_BOUNDARY_RUN_STORE_BASENAME
)
# These are the historical Level-C constants.  ``run()`` continues to open
# RUN_STORE_BASENAME and therefore must continue to interpret that database as
# the completed SKR route.
TARGET_PRODUCT_ID = 44
TARGET_TICKER_ID = "SKR-PERP_USDT0"
FUNDING_BOUNDARY_TARGET_CANONICAL_ASSET = "ETH"
FUNDING_BOUNDARY_TARGET_PRODUCT_ID = 4
FUNDING_BOUNDARY_TARGET_TICKER_ID = "ETH-PERP_USDT0"
# Official Nado Python SDK 2.0.0's nado_protocol.utils.gen_order_nonce(
# recv_time_ms=None) fences an order at the current UTC receive timestamp plus
# 90 seconds.
RECV_WINDOW_MS = 90_000
HTTP_TIMEOUT_SECONDS = 5.0
RECONCILE_READ_ATTEMPTS = 5
RECONCILE_READ_INTERVAL_SECONDS = 1.0
NADO_CLOSE_AGGRESSIVE_PERCENT = 2
_GATEWAY_HOST = "gateway.test.nado.xyz"
_ARCHIVE_HOST = "archive.test.nado.xyz"
_TRIGGER_HOST = "trigger.test.nado.xyz"
_FUNDING_SUBSCRIBE_URL = "wss://gateway.test.nado.xyz/v1/subscribe"
NADO_FUNDING_PAGE_LIMIT = 100
NADO_FUNDING_MAX_PAGES = 8
NADO_FUNDING_WAIT_GRACE_SECONDS = 30.0
UNEXPECTED_FAILURE = "UNEXPECTED_FAILURE"

# Official Nado gateway weights observed for the accepted 94-product catalog:
# contracts(1) + status(1) + linked signer(5) + all products(5) + orders
# (2 * 92) + subaccount(2) + isolated positions(10) + market price(1) = 209.
# The accepted private-read round B is 29 + (2 * 92) = 213.  These are fixed
# venue-local admission facts, not a reusable rate limiter.  Admission tracks
# only this process's own gateway windows; unrelated same-IP traffic remains
# outside its control and can still consume the official IP-scoped quota.
NADO_QUERY_WEIGHT_LIMIT_10S = 400
NADO_QUERY_WINDOW_SECONDS = 10.0
NADO_OBSERVED_CATALOG_PRODUCT_COUNT = 94
NADO_NON_ORDERABLE_PRODUCT_COUNT = 2
NADO_OBSERVED_ORDERABLE_PRODUCT_COUNT = (
    NADO_OBSERVED_CATALOG_PRODUCT_COUNT - NADO_NON_ORDERABLE_PRODUCT_COUNT
)
NADO_FULL_OBSERVE_FIXED_WEIGHT = 1 + 1 + 5 + 5 + 2 + 10 + 1
NADO_FULL_OBSERVE_GATEWAY_WEIGHT = (
    NADO_FULL_OBSERVE_FIXED_WEIGHT + 2 * NADO_OBSERVED_ORDERABLE_PRODUCT_COUNT
)
NADO_PRIVATE_READ_ROUND_B_GATEWAY_WEIGHT = 29 + (
    2 * NADO_OBSERVED_ORDERABLE_PRODUCT_COUNT
)
NADO_BARRIER_TO_OBSERVE_WEIGHT = (
    NADO_PRIVATE_READ_ROUND_B_GATEWAY_WEIGHT + NADO_FULL_OBSERVE_GATEWAY_WEIGHT
)
NADO_QUERY_ADMISSION_LIMITATION = (
    "Admission pacing controls only this runner's own gateway query window; "
    "unrelated same-IP traffic is not controlled."
)


class OperationalSafetyError(RuntimeError):
    """Sanitized terminal operational failure."""


class DurableOperationalFailure(OperationalSafetyError):
    """Sanitized failure persisted after the runtime journal began."""

    def __init__(self, failure_class: str, stage: str) -> None:
        if (
            type(failure_class) is not str
            or failure_class not in FAILURE_CLASSES | {UNEXPECTED_FAILURE}
        ):
            raise ValueError("unsupported operational failure class")
        if type(stage) is not str or stage not in _RUNTIME_STAGES:
            raise ValueError("unsupported operational stage")
        self.failure_class = failure_class
        self.stage = stage
        super().__init__(failure_class)


class DurableExecuteFailure(OperationalSafetyError):
    """Sanitized execute failure recovered from the durable intent journal."""

    def __init__(self, failure_class: str, venue_code: int | None) -> None:
        self.failure_class = failure_class
        self.venue_code = venue_code
        super().__init__(failure_class)


@dataclass(frozen=True)
class LiveObservation:
    catalog: CatalogSnapshot
    evidence: EngineEvidence
    product: Product
    bid_x18: int
    ask_x18: int


# A Chief-owned RISEx implementation supplies one immutable terminal artifact
# and its route/journal/round provenance.  The Nado runner builds the Nado
# terminal and CrossRunAttestation itself; a caller can never supply either as
# authority through this seam.
RisexTerminalEvidenceProvider = Callable[
    [FundingBoundaryBinding, LiveObservation, int], RisexTerminalEvidence
]

# These callbacks are deliberately expressed in terms of the contract module's
# immutable binding and bounded coordination values.  The Nado writer surface
# never imports the RISEx orchestration module or receives funding cash, rate,
# status, or any other venue result through this hook.
BoundaryProgressionSink = Callable[[FundingBoundaryBinding, str, int], None]
PreparationGate = Callable[[FundingBoundaryBinding], object]
BOUNDARY_RELEASED = "RELEASED"
BOUNDARY_CANCELLED = "CANCELLED"


@dataclass(frozen=True)
class OperationalReport:
    schema_version: int
    status: str
    run_tag: str
    writes: int
    close_attempts: int
    final_zero_regular: bool
    final_zero_trigger: bool
    final_exact_flat: bool
    reason: str | None = None

    def sanitized(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "run_tag": self.run_tag,
            "writes": self.writes,
            "close_attempts": self.close_attempts,
            "final_zero_regular": self.final_zero_regular,
            "final_zero_trigger": self.final_zero_trigger,
            "final_exact_flat": self.final_exact_flat,
            "reason": self.reason,
            "path": REDACTED_STORE_PATH,
        }


@dataclass(frozen=True)
class FundingBoundaryReport:
    """Sanitized report for the Nado leg of one funding-boundary route."""

    schema_version: int
    status: str
    run_tag: str
    writes: int
    close_attempts: int
    funding_status: str | None
    funding_rate_x18: int | None
    funding_cash_x18: int | None
    final_rounds_agree: bool
    final_zero_regular: bool
    final_zero_trigger: bool
    final_exact_flat: bool
    reason: str | None = None
    funding_aggregate_payment_x18: int | None = None
    funding_account_idx: int | None = None

    def sanitized(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "run_tag": self.run_tag,
            "writes": self.writes,
            "close_attempts": self.close_attempts,
            "funding_status": self.funding_status,
            "funding_rate_x18": self.funding_rate_x18,
            "funding_cash_x18": self.funding_cash_x18,
            "final_rounds_agree": self.final_rounds_agree,
            "final_zero_regular": self.final_zero_regular,
            "final_zero_trigger": self.final_zero_trigger,
            "final_exact_flat": self.final_exact_flat,
            "reason": self.reason,
            "funding_aggregate_payment_x18": self.funding_aggregate_payment_x18,
            "funding_account_idx": self.funding_account_idx,
            "path": FUNDING_BOUNDARY_REDACTED_STORE_PATH,
        }


_RUNTIME_FAILURE_CLASSES = frozenset(FAILURE_CLASSES) | frozenset({
    EXECUTE_RESPONSE_AMBIGUITY,
    EXECUTE_TRANSPORT_AMBIGUITY,
    EXECUTE_VENUE_REJECTION,
    UNEXPECTED_FAILURE,
})

_RUNTIME_STAGES = frozenset({
    "PRIVATE_READ_BARRIER", "LIVE_OBSERVATION", "ORDER_DERIVATION",
    "ENTRY_PREFLIGHT", "ENTRY_SIGNATURE", "ENTRY_VALIDATION",
    "ENTRY_PREPARATION", "DISPATCH", "RECONCILIATION",
    "CANCEL_PREPARATION", "CLOSE_PREPARATION", "FINAL_BARRIER",
    "FUNDING_BOUNDARY", "RUNNER_STARTUP", "OUTER",
})

_PREPARATION_FAILURE_CODES = frozenset({
    "STARTUP_FAILED",
    "STARTUP_INTERRUPTED",
    "BINDING_MISMATCH",
    "BARRIER_ABORTED",
    "NADO_FAILED",
    "RISEX_FAILED",
    "ASYMMETRIC_PARTIAL_EXPOSURE",
    "MANUAL_RECOVERY_REQUIRED",
    "CLOSE_FAILED",
})
_PREPARATION_FAILURE_CLASS = "SAFETY"
_PREPARATION_FAILURE_STAGE = "ENTRY_PREPARATION"


class _PreparationGateFailure(OperationalSafetyError):
    """Sanitized allowlisted failure from the owner preparation barrier."""

    def __init__(self, code: str) -> None:
        if type(code) is not str or code not in _PREPARATION_FAILURE_CODES:
            raise ValueError("unsupported preparation failure")
        self.code = code
        self.failure_class = _PREPARATION_FAILURE_CLASS
        self.stage = _PREPARATION_FAILURE_STAGE
        super().__init__(code)


def _preparation_gate_failure(error: object) -> _PreparationGateFailure | None:
    """Accept only the fixed code/class/stage tuple from the owner barrier."""
    try:
        code = getattr(error, "code", None)
        failure_class = getattr(error, "failure_class", None)
        stage = getattr(error, "stage", None)
    except BaseException:
        return None
    if (
        type(code) is not str
        or code not in _PREPARATION_FAILURE_CODES
        or failure_class != _PREPARATION_FAILURE_CLASS
        or stage != _PREPARATION_FAILURE_STAGE
    ):
        return None
    return _PreparationGateFailure(code)


def _failure_class(error: BaseException | object) -> str:
    """Return only a bounded class; never persist or report exception text."""
    explicit = getattr(error, "failure_class", None)
    if type(explicit) is str and explicit in _RUNTIME_FAILURE_CLASSES:
        return explicit
    if isinstance(error, NadoPreflightError):
        classified = _preflight_failure_class(error)
        return classified if classified in FAILURE_CLASSES else UNEXPECTED_FAILURE
    if isinstance(error, NadoContractError):
        return "SAFETY"
    if isinstance(error, OperationalSafetyError):
        message = str(error).lower()
        if "http status" in message:
            return "HTTP"
        if any(term in message for term in (
            "schema", "strict json", "canonical json", "response size",
            "content encoding",
        )):
            return "SCHEMA"
        if any(term in message for term in (
            "identity", "subaccount", "linked signer", "owner mismatch",
        )):
            return "IDENTITY"
        if any(term in message for term in (
            "credential", "capability", "sign", "signature",
        )):
            return "AUTH"
        if "transport" in message:
            return "TRANSPORT"
    return UNEXPECTED_FAILURE


def _report_failure_class(report: object) -> str:
    if type(report) is dict:
        failure_class = report.get("failure_class")
        if type(failure_class) is str and failure_class in FAILURE_CLASSES:
            return failure_class
    return UNEXPECTED_FAILURE


class VenueIO(Protocol):
    def now_ms(self) -> int: ...
    def observe(self, digests: tuple[str, ...]) -> LiveObservation: ...
    def validate_order(self, order: SyntheticOrderVector, signature: str) -> bool: ...
    def dispatch(self, intent: OrderIntent, signature: str) -> str: ...


class FundingBoundaryVenueIO(VenueIO, Protocol):
    """Nado IO extension for exact, fixture-injected funding observations."""

    def capture_funding_baseline(
        self, binding: FundingBoundaryBinding, observation: LiveObservation,
    ) -> NadoFundingBaseline: ...

    def capture_funding_exposure(
        self, binding: FundingBoundaryBinding, observation: LiveObservation,
    ) -> NadoFundingExposure: ...

    def await_funding_boundary(self, binding: FundingBoundaryBinding) -> None: ...

    def read_funding_boundary(
        self, binding: FundingBoundaryBinding,
    ) -> tuple[NadoFundingEvent | None, NadoAccountFunding | None]: ...


class RuntimeRunJournal:
    """Append-only runtime identity in the same protected operational DB."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    @staticmethod
    def _ensure_schema(connection: sqlite3.Connection) -> None:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS nado_runtime_runs ("
            "run_id TEXT PRIMARY KEY, created_at_ms INTEGER NOT NULL, "
            "state TEXT NOT NULL, failure_class TEXT, stage TEXT)"
        )
        columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(nado_runtime_runs)")
        }
        if "failure_class" not in columns:
            connection.execute(
                "ALTER TABLE nado_runtime_runs ADD COLUMN failure_class TEXT"
            )
        if "stage" not in columns:
            connection.execute(
                "ALTER TABLE nado_runtime_runs ADD COLUMN stage TEXT"
            )

    def begin(self, created_at_ms: int) -> str:
        if type(created_at_ms) is not int or created_at_ms <= 0:
            raise OperationalSafetyError("runtime journal rejected")
        _prepare_file(self.path)
        run_id = str(uuid.uuid4())
        connection = sqlite3.connect(self.path)
        try:
            connection.execute("PRAGMA synchronous=FULL")
            with connection:
                self._ensure_schema(connection)
                connection.execute(
                    "INSERT INTO nado_runtime_runs "
                    "(run_id, created_at_ms, state, failure_class, stage) "
                    "VALUES (?, ?, 'STARTED', NULL, NULL)",
                    (run_id, created_at_ms),
                )
        except sqlite3.DatabaseError:
            raise OperationalSafetyError("runtime journal rejected") from None
        finally:
            connection.close()
        _fsync(self.path)
        return run_id

    def terminalize(self, run_id: str, failure_class: str, stage: str) -> None:
        if (
            type(run_id) is not str
            or not run_id
            or type(failure_class) is not str
            or type(stage) is not str
            or failure_class not in _RUNTIME_FAILURE_CLASSES
            or stage not in _RUNTIME_STAGES
        ):
            raise OperationalSafetyError("runtime journal rejected")
        _prepare_file(self.path)
        connection = sqlite3.connect(self.path)
        try:
            connection.execute("PRAGMA synchronous=FULL")
            with connection:
                self._ensure_schema(connection)
                row = connection.execute(
                    "SELECT state, failure_class, stage FROM nado_runtime_runs "
                    "WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                if row is None:
                    raise OperationalSafetyError("runtime journal rejected")
                if row[0] == "BLOCKED":
                    if row[1] != failure_class or row[2] != stage:
                        raise OperationalSafetyError("runtime journal terminal conflict")
                    return
                if row[0] != "STARTED":
                    raise OperationalSafetyError("runtime journal terminal conflict")
                changed = connection.execute(
                    "UPDATE nado_runtime_runs SET state = 'BLOCKED', "
                    "failure_class = ?, stage = ? WHERE run_id = ? AND state = 'STARTED'",
                    (failure_class, stage, run_id),
                )
                if changed.rowcount != 1:
                    raise OperationalSafetyError("runtime journal terminal conflict")
        except sqlite3.DatabaseError:
            raise OperationalSafetyError("runtime journal rejected") from None
        finally:
            connection.close()
        _fsync(self.path)


def _home() -> Path:
    return Path(pwd.getpwuid(os.getuid()).pw_dir)


def _production_store_path() -> Path:
    return _home() / RUN_STORE_BASENAME


def _funding_boundary_store_path() -> Path:
    """Return the separate fixed store for the fresh ETH funding route."""
    return _home() / FUNDING_BOUNDARY_RUN_STORE_BASENAME


def _prepare_file(path: Path) -> None:
    try:
        descriptor = os.open(
            path, os.O_CREAT | os.O_EXCL | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
    except FileExistsError:
        details = path.lstat()
        if (
            path.is_symlink() or not stat.S_ISREG(details.st_mode)
            or details.st_uid != os.getuid() or stat.S_IMODE(details.st_mode) != 0o600
            or details.st_nlink != 1
        ):
            raise OperationalSafetyError("operational store rejected") from None
    except OSError:
        raise OperationalSafetyError("operational store unavailable") from None
    else:
        os.fchmod(descriptor, 0o600)
        os.fsync(descriptor)
        os.close(descriptor)
        _fsync(path)


def _fsync(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _journal_store_identity(path: Path) -> str:
    """Return a non-secret identity for the protected path and schema."""
    try:
        resolved = Path(path).resolve()
        material = f"nado-level-c-v1:{resolved}".encode("utf-8")
    except (OSError, UnicodeError):
        raise OperationalSafetyError("operational journal identity unavailable") from None
    return "nado-level-c-v1-" + hashlib.sha256(material).hexdigest()


def _full_observe_gateway_weight(
    catalog_product_count: int = NADO_OBSERVED_CATALOG_PRODUCT_COUNT,
    orderable_product_count: int = NADO_OBSERVED_ORDERABLE_PRODUCT_COUNT,
) -> int:
    """Return the fixed current-catalog weight reserved before ``observe``."""
    if (
        type(catalog_product_count) is not int
        or type(orderable_product_count) is not int
        or catalog_product_count != NADO_OBSERVED_CATALOG_PRODUCT_COUNT
        or orderable_product_count != (
            catalog_product_count - NADO_NON_ORDERABLE_PRODUCT_COUNT
        )
    ):
        raise OperationalSafetyError("Nado observed catalog weight is unavailable")
    return NADO_FULL_OBSERVE_FIXED_WEIGHT + 2 * orderable_product_count


class _NadoSnapshotAdmission:
    """Deterministic admission whose sleeper interruption fails closed."""

    def __init__(
        self,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self._monotonic = monotonic
        self._sleeper = sleeper
        self._last_gateway_window_at: float | None = None
        self._snapshot_admitted = False

    def note_gateway_window(self) -> None:
        """Record completion of an external or own Nado gateway window."""
        self._last_gateway_window_at = self._monotonic()

    def before_snapshot(self) -> None:
        """Wait for a safe start; interruption leaves the operation fail-closed."""
        if self._snapshot_admitted:
            raise OperationalSafetyError("Nado snapshot admission is already in flight")
        anchor = self._last_gateway_window_at
        if anchor is not None:
            deadline = anchor + NADO_QUERY_WINDOW_SECONDS
            remaining = deadline - self._monotonic()
            if remaining > 0:
                self._sleeper(remaining)
                now = self._monotonic()
                if now < deadline:
                    raise OperationalSafetyError(
                        "Nado query admission did not reach a safe window"
                    )
            else:
                now = self._monotonic()
        else:
            now = self._monotonic()
        del now
        self._snapshot_admitted = True

    def complete_snapshot(self) -> None:
        """Anchor the next window at conservative snapshot completion."""
        if not self._snapshot_admitted:
            raise OperationalSafetyError("Nado snapshot admission was not started")
        self._last_gateway_window_at = self._monotonic()
        self._snapshot_admitted = False


class OwnerOrderCapability:
    """Opaque owner key handle restricted to already-prepared Nado digests."""

    def __init__(self, secret: bytes, owner: str) -> None:
        if type(secret) is not bytes or len(secret) != 32:
            raise OperationalSafetyError("owner capability rejected")
        self._secret = bytearray(secret)
        self.owner = owner.lower()
        try:
            derived = keys.PrivateKey(secret).public_key.to_canonical_address()
        except BaseException:
            raise OperationalSafetyError("owner capability rejected") from None
        if derived.hex() != self.owner[2:]:
            self.close()
            raise OperationalSafetyError("owner capability identity mismatch")

    def sign(self, intent: OrderIntent) -> str:
        if not self._secret or intent.owner.lower() != self.owner:
            raise OperationalSafetyError("order signing rejected")
        try:
            signature = keys.PrivateKey(bytes(self._secret)).sign_msg_hash(
                bytes.fromhex(intent.digest[2:])
            )
            raw = signature.r.to_bytes(32, "big") + signature.s.to_bytes(32, "big")
            return "0x" + (raw + bytes((signature.v + 27,))).hex()
        except BaseException:
            raise OperationalSafetyError("order signing rejected") from None

    def close(self) -> None:
        for index in range(len(self._secret)):
            self._secret[index] = 0
        self._secret.clear()


def _load_capability(owner: str) -> OwnerOrderCapability:
    home = _home()
    directory = os.open(home, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    material = bytearray()
    try:
        descriptor = os.open(
            KEY_BASENAME, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0), dir_fd=directory,
        )
        try:
            details = os.fstat(descriptor)
            if (
                not stat.S_ISREG(details.st_mode) or details.st_uid != os.getuid()
                or stat.S_IMODE(details.st_mode) != 0o600 or details.st_nlink != 1
            ):
                raise OperationalSafetyError("owner capability rejected")
            material.extend(os.read(descriptor, 33))
        finally:
            os.close(descriptor)
    except OSError:
        raise OperationalSafetyError("owner capability unavailable") from None
    finally:
        os.close(directory)
    try:
        if len(material) != 32:
            raise OperationalSafetyError("owner capability rejected")
        return OwnerOrderCapability(bytes(material), owner)
    finally:
        for index in range(len(material)):
            material[index] = 0
        material.clear()


def _salt() -> int:
    return secrets.randbelow(2**20)


def _canonical_quantity_x18(value: object) -> int:
    if isinstance(value, bool) or isinstance(value, float) or not isinstance(
        value, (str, int, Decimal)
    ):
        raise OperationalSafetyError("funding quantity is not exact")
    try:
        scaled = Decimal(str(value)) * Decimal(10**18)
    except (InvalidOperation, ValueError):
        raise OperationalSafetyError("funding quantity is not exact") from None
    if not scaled.is_finite() or scaled <= 0 or scaled != scaled.to_integral_value():
        raise OperationalSafetyError("funding quantity is not an exact x18 amount")
    return int(scaled)


def _entry_order(
    observation: LiveObservation,
    owner: str,
    sender: str,
    recv: int,
    *,
    direction: str = LONG,
    expected_amount_x18: int | None = None,
    target_product_id: int = TARGET_PRODUCT_ID,
    target_ticker_id: str = TARGET_TICKER_ID,
) -> SyntheticOrderVector:
    product = observation.product
    if (
        product.product_id != target_product_id
        or product.symbol != target_ticker_id
        or product.product_type != ACTIVE_PERP
    ):
        raise OperationalSafetyError("fixed target product identity unavailable")
    bid, ask = observation.bid_x18, observation.ask_x18
    if bid <= 0 or ask <= bid or bid % product.tick_x18 or ask % product.tick_x18:
        raise OperationalSafetyError("fresh non-crossed tick-aligned BBO required")
    if direction not in {LONG, SHORT}:
        raise OperationalSafetyError("funding route direction is invalid")
    salt = _salt()
    minimum_amount = smallest_executable_amount(product, prices_x18=(bid, ask))
    if expected_amount_x18 is None:
        amount = minimum_amount
    else:
        if (
            type(expected_amount_x18) is not int
            or expected_amount_x18 < minimum_amount
            or expected_amount_x18 % product.step_x18
        ):
            raise OperationalSafetyError(
                "funding route quantity differs from executable amount"
            )
        amount = expected_amount_x18
    if direction == LONG:
        buffered_ask = (ask * 110 + 99) // 100
        price = (
            (buffered_ask + product.tick_x18 - 1) // product.tick_x18
        ) * product.tick_x18
        signed_amount = amount
    else:
        buffered_bid = (bid * 90) // 100
        price = (buffered_bid // product.tick_x18) * product.tick_x18
        if price <= 0:
            raise OperationalSafetyError("funding short entry price is invalid")
        signed_amount = -amount
    return SyntheticOrderVector(
        owner, SUBACCOUNT_NAME, sender, target_product_id, price,
        signed_amount, UINT32_MAX, recv, salt,
        build_order_nonce(recv, salt), IOC_APPENDIX,
    )


def _funding_boundary_close_price(
    observation: LiveObservation, position_direction: str,
) -> int:
    """Build the official Nado aggressive limit for a funding-boundary close."""
    product = observation.product
    product.assert_contract()
    bid, ask = observation.bid_x18, observation.ask_x18
    if (
        bid <= 0
        or ask <= bid
        or bid % product.tick_x18
        or ask % product.tick_x18
    ):
        raise OperationalSafetyError("fresh non-crossed tick-aligned BBO required")
    if position_direction == SHORT:
        # A short position closes with a BUY.  The official CLI derives its
        # aggressive limit from the safe bid and rounds upward to a tick.
        buffered_bid = (
            bid * (100 + NADO_CLOSE_AGGRESSIVE_PERCENT) + 99
        ) // 100
        price = (
            (buffered_bid + product.tick_x18 - 1) // product.tick_x18
        ) * product.tick_x18
    elif position_direction == LONG:
        # A long position closes with a SELL.  Mirror the official CLI by
        # deriving from the safe ask and rounding downward to a tick.
        buffered_ask = (
            ask * (100 - NADO_CLOSE_AGGRESSIVE_PERCENT)
        ) // 100
        price = (buffered_ask // product.tick_x18) * product.tick_x18
    else:
        raise OperationalSafetyError("funding route direction is invalid")
    if price <= 0:
        raise OperationalSafetyError("funding close price is invalid")
    return price


def _terminal_flags(observation: LiveObservation) -> tuple[bool, bool, bool]:
    account = observation.evidence.account
    regular = not any(account.regular_orders_by_product.values())
    trigger = not observation.evidence.triggers.active_digests
    flat = not any(account.cross_perp_amounts_x18.values()) and not account.isolated_positions
    return regular, trigger, flat


def _terminal_fingerprint(evidence: TerminalEvidence) -> tuple[object, ...]:
    """Compare final rounds while allowing their observation timestamps to differ."""
    return (
        evidence.journal,
        evidence.status,
        evidence.journal_content_sha256,
        evidence.zero_regular_orders,
        evidence.zero_trigger_orders,
        evidence.exact_flat,
        evidence.unresolved_write_identities,
        evidence.authoritative,
    )


def _nado_terminal_journal_content_projection(
    store: IntentStore,
) -> dict[str, object]:
    """Project only immutable terminal-relevant Nado journal content.

    Lifecycle/runtime state and the later funding evidence/blocker rows are
    deliberately excluded.  The latter would make this digest circular with
    the attestation that stores it.  The included binding, baseline, exposure,
    durable intent payloads, and fill identities are immutable once written.
    """
    binding = store.funding_boundary_binding()
    baseline = store.funding_boundary_baseline()
    exposure = store.funding_boundary_exposure()
    if binding is None or baseline is None or exposure is None:
        raise OperationalSafetyError(
            "Nado terminal journal content is unavailable"
        )
    try:
        binding.assert_contract()
        nado_journal = binding.nado_journal
        route = binding.route
        intents = tuple(
            {
                "digest": intent.digest,
                "kind": intent.kind,
                "product_id": intent.product_id,
                "nonce": intent.nonce,
                "recv_time": intent.recv_time,
                "payload": intent.payload.decode("ascii"),
                "amount_x18": intent.amount_x18,
                "appendix": intent.appendix,
                "notional_x18": intent.notional_x18,
                "clamp_expected": intent.clamp_expected,
                "snapshot_id": intent.snapshot_id,
                "snapshot_observed_at_ms": intent.snapshot_observed_at_ms,
                "starting_position_x18": intent.starting_position_x18,
                "sender": intent.sender,
                "owner": intent.owner,
                "subaccount_name": intent.subaccount_name,
            }
            for intent, _state in store.intents()
        )
        fills = tuple(
            {
                "digest": digest,
                "submission_idx": submission_idx,
                "product_id": product_id,
                "amount_x18": amount_x18,
            }
            for (digest, submission_idx), (product_id, amount_x18)
            in sorted(store.persisted_fill_map().items())
        )
        return {
            "binding": {
                "route": {
                    "canonical_asset": route.canonical_asset,
                    "risex_leg": {
                        "venue": route.risex_leg.venue,
                        "market": route.risex_leg.market,
                        "direction": route.risex_leg.direction,
                        "raw_quantity": format(route.risex_leg.raw_quantity, "f"),
                        "base_multiplier": format(
                            route.risex_leg.base_multiplier, "f"
                        ),
                        "canonical_quantity": format(
                            route.risex_leg.canonical_quantity, "f"
                        ),
                    },
                    "nado_leg": {
                        "venue": route.nado_leg.venue,
                        "market": route.nado_leg.market,
                        "direction": route.nado_leg.direction,
                        "raw_quantity": format(route.nado_leg.raw_quantity, "f"),
                        "base_multiplier": format(
                            route.nado_leg.base_multiplier, "f"
                        ),
                        "canonical_quantity": format(
                            route.nado_leg.canonical_quantity, "f"
                        ),
                    },
                    "nado_product_id": route.nado_product_id,
                    "settlement_at_ms": route.settlement_at_ms,
                },
                "risex_journal": {
                    "venue": binding.risex_journal.venue,
                    "run_id": binding.risex_journal.run_id,
                    "store_identity": binding.risex_journal.store_identity,
                    "account_id": binding.risex_journal.account_id,
                },
                "nado_journal": {
                    "venue": nado_journal.venue,
                    "run_id": nado_journal.run_id,
                    "store_identity": nado_journal.store_identity,
                    "account_id": nado_journal.account_id,
                },
            },
            "baseline_digest": baseline.baseline_digest,
            "exposure_digest": exposure.exposure_digest,
            "intents": intents,
            "fills": fills,
        }
    except (UnicodeDecodeError, TypeError, ValueError):
        raise OperationalSafetyError(
            "Nado terminal journal content is unavailable"
        ) from None


def _nado_terminal_journal_content_sha256(store: IntentStore) -> str:
    try:
        material = canonical_payload(
            _nado_terminal_journal_content_projection(store)
        )
    except OperationalSafetyError:
        raise
    except (NadoContractError, TypeError, ValueError):
        raise OperationalSafetyError(
            "Nado terminal journal content is unavailable"
        ) from None
    return "0x" + hashlib.sha256(material).hexdigest()


class SealedLifecycleRunner:
    def __init__(
        self, *, store: IntentStore, journal: RuntimeRunJournal, io: VenueIO,
        capability_loader: Callable[[str], OwnerOrderCapability], owner: str,
        sender: str,
    ) -> None:
        self.store = store
        self.core = LifecycleCore(store)
        self.io = io
        self.capability_loader = capability_loader
        self.owner = owner.lower()
        self.sender = sender.lower()
        self.journal = journal
        self.run_id = journal.begin(io.now_ms())
        self.stage = "RUNNER_STARTUP"
        self.writes = 0
        # Set immediately before the durable one-way dispatch call.  An owner
        # may use this conservative flag to distinguish a prewrite failure
        # from a task that may already have reached the venue.
        self.potential_write = False

    def terminalize(self, failure_class: str, stage: str | None = None) -> None:
        selected_stage = self.stage if stage is None else stage
        if selected_stage not in _RUNTIME_STAGES:
            selected_stage = "OUTER"
        self.store.halt()
        self.journal.terminalize(self.run_id, failure_class, selected_stage)

    def _dispatch(self, intent: OrderIntent) -> None:
        capability = self.capability_loader(self.owner)
        try:
            signature = capability.sign(intent)
            self.potential_write = True
            try:
                returned = self.store.dispatch_prepared(
                    intent.digest, lambda durable: self.io.dispatch(durable, signature)
                )
            except ExecuteFailure as error:
                persisted = self.store.execute_failure(intent.digest)
                expected = (error.failure_class, error.venue_code)
                if persisted != expected:
                    raise OperationalSafetyError(
                        "durable execute failure evidence mismatch"
                    ) from None
                raise DurableExecuteFailure(*persisted) from None
            self.writes += 1
            if returned.lower() != intent.digest.lower():
                self.store.halt()
                raise OperationalSafetyError("write response identity mismatch")
        finally:
            capability.close()

    def _observe(self) -> LiveObservation:
        return self.io.observe(tuple(intent.digest for intent, _ in self.store.intents()))

    def _reconcile(self, intent: OrderIntent) -> tuple[Reconciliation, LiveObservation]:
        while self.io.now_ms() <= intent.recv_time:
            time.sleep(0.01)
        terminal = getattr(self.io, "terminal_status", lambda _digest: None)(intent.digest)
        observed = self._observe()
        for _ in range(RECONCILE_READ_ATTEMPTS - 1):
            visible = (
                terminal is not None
                or any(order.digest.lower() == intent.digest.lower()
                       for order in observed.evidence.orders)
                or any(fill.digest.lower() == intent.digest.lower()
                       for fill in observed.evidence.fills)
                or observed.evidence.exact_rejection_digest == intent.digest
                or intent.kind == CANCEL_ALL
            )
            if visible:
                break
            time.sleep(RECONCILE_READ_INTERVAL_SECONDS)
            observed = self._observe()
        if terminal is not None:
            from dataclasses import replace
            observed = LiveObservation(
                observed.catalog,
                replace(
                    observed.evidence, terminal_digest=intent.digest,
                    terminal_status=terminal,
                    exact_cancel_digest=(
                        intent.digest if intent.kind == CANCEL_ALL
                        else observed.evidence.exact_cancel_digest
                    ),
                ),
                observed.product, observed.bid_x18, observed.ask_x18,
            )
        result = self.store.reconcile(
            intent.digest, catalog=observed.catalog, evidence=observed.evidence,
        )
        if result in {Reconciliation.AMBIGUOUS, Reconciliation.CONTRADICTORY}:
            raise OperationalSafetyError("manual recovery required")
        return result, observed

    def run(self) -> OperationalReport:
        return self._run_order_lifecycle()

    def _run_order_lifecycle(
        self,
        *,
        route: FundingRouteBinding | None = None,
        before_entry: Callable[[LiveObservation], None] | None = None,
        before_close: Callable[[LiveObservation], None] | None = None,
        finalizer: Callable[[tuple[LiveObservation, ...]], object] | None = None,
    ) -> OperationalReport | object:
        if self.store.intents() or self.store.lifecycle_status() != "RUNNING":
            raise OperationalSafetyError("existing lifecycle requires manual recovery")
        entry_direction = LONG
        expected_amount_x18: int | None = None
        target_product_id = TARGET_PRODUCT_ID
        target_ticker_id = TARGET_TICKER_ID
        if route is not None:
            route.assert_contract()
            if (
                route.canonical_asset != FUNDING_BOUNDARY_TARGET_CANONICAL_ASSET
                or route.nado_leg.venue != NADO_VENUE
                or route.nado_product_id != FUNDING_BOUNDARY_TARGET_PRODUCT_ID
                or route.nado_leg.market != FUNDING_BOUNDARY_TARGET_TICKER_ID
            ):
                raise OperationalSafetyError("funding route target product identity unavailable")
            entry_direction = route.nado_leg.direction
            expected_amount_x18 = _canonical_quantity_x18(route.canonical_quantity)
            target_product_id = FUNDING_BOUNDARY_TARGET_PRODUCT_ID
            target_ticker_id = FUNDING_BOUNDARY_TARGET_TICKER_ID
        self.stage = "LIVE_OBSERVATION"
        initial = self._observe()
        if before_entry is not None:
            self.stage = "FUNDING_BOUNDARY"
            before_entry(initial)
        issued_at = self.io.now_ms()
        recv = issued_at + RECV_WINDOW_MS
        self.stage = "ORDER_DERIVATION"
        order = _entry_order(
            initial,
            self.owner,
            self.sender,
            recv,
            direction=entry_direction,
            expected_amount_x18=expected_amount_x18,
            target_product_id=target_product_id,
            target_ticker_id=target_ticker_id,
        )
        worst_close_price = (
            initial.bid_x18 if entry_direction == LONG else initial.ask_x18
        )
        self.stage = "ENTRY_PREFLIGHT"
        validate_entry_preflight(
            catalog=initial.catalog, account=initial.evidence.account,
            triggers=initial.evidence.triggers, product_id=target_product_id,
            entry_price_x18=order.price_x18,
            worst_close_price_x18=worst_close_price, now_ms=issued_at,
        )
        self.stage = "ENTRY_SIGNATURE"
        capability = self.capability_loader(self.owner)
        try:
            signature = capability.sign(OrderIntent(
                ENTRY, order.product_id, order.nonce, order.recv_time,
                order_digest(order), json.dumps(order.as_payload(), sort_keys=True,
                separators=(",", ":")).encode("ascii"), order.amount_x18,
                order.appendix, sender=order.sender, owner=order.owner,
                subaccount_name=order.subaccount_name,
            ))
            self.stage = "ENTRY_VALIDATION"
            valid = self.io.validate_order(order, signature)
        finally:
            capability.close()
        self.stage = "ENTRY_PREPARATION"
        if route is None:
            entry = self.core.prepare_entry(
                order=order, catalog=initial.catalog, account=initial.evidence.account,
                triggers=initial.evidence.triggers,
                worst_close_price_x18=worst_close_price, signature=signature,
                validation_product_id=order.product_id, validation_valid=valid,
                now_ms=issued_at, direction=entry_direction,
            )
        else:
            entry = self._prepare_funding_entry(
                order=order, catalog=initial.catalog,
                account=initial.evidence.account,
                triggers=initial.evidence.triggers,
                worst_close_price_x18=worst_close_price,
                signature=signature, validation_product_id=order.product_id,
                validation_valid=valid, now_ms=issued_at,
                direction=entry_direction,
            )
        self.stage = "DISPATCH"
        self._dispatch(entry)
        self.stage = "RECONCILIATION"
        outcome, observed = self._reconcile(entry)
        if outcome is Reconciliation.RESTING or outcome is Reconciliation.PARTIAL:
            issued_at = self.io.now_ms()
            recv = issued_at + RECV_WINDOW_MS
            self.stage = "CANCEL_PREPARATION"
            cancel = self.core.prepare_cancel_all(
                catalog=observed.catalog, account=observed.evidence.account,
                triggers=observed.evidence.triggers, sender=self.sender,
                recv_time=recv, salt=_salt(), now_ms=issued_at,
            )
            self.stage = "DISPATCH"
            self._dispatch(cancel)
            self.stage = "RECONCILIATION"
            outcome, observed = self._reconcile(cancel)
            if outcome is not Reconciliation.CANCELLED:
                raise OperationalSafetyError("exact entry cancellation unresolved")
            outcome, observed = self._reconcile(entry)
        if outcome not in {Reconciliation.FILLED, Reconciliation.CANCELLED, Reconciliation.EXPIRED}:
            raise OperationalSafetyError("entry outcome requires manual recovery")
        if before_close is not None:
            self.stage = "FUNDING_BOUNDARY"
            before_close(observed)
        while any(observed.evidence.account.cross_perp_amounts_x18.values()):
            if self.store.count_kind(CLOSE) >= MAX_CLOSE_ATTEMPTS:
                self.store.halt()
                raise OperationalSafetyError("three close attempts exhausted")
            issued_at = self.io.now_ms()
            recv = issued_at + RECV_WINDOW_MS
            self.stage = "CLOSE_PREPARATION"
            close = self.core.prepare_close(
                catalog=observed.catalog, product=observed.product,
                account=observed.evidence.account, triggers=observed.evidence.triggers,
                worst_price_x18=(
                    _funding_boundary_close_price(observed, entry_direction)
                    if route is not None
                    else (
                        observed.bid_x18
                        if entry_direction == LONG
                        else observed.ask_x18
                    )
                ), recv_time=recv, salt=_salt(),
                now_ms=issued_at,
            )
            self.stage = "DISPATCH"
            self._dispatch(close)
            self.stage = "RECONCILIATION"
            close_outcome, observed = self._reconcile(close)
            if close_outcome in {Reconciliation.PARTIAL, Reconciliation.REJECTED}:
                raise OperationalSafetyError("close requires manual recovery")
            if close_outcome not in {
                Reconciliation.FILLED, Reconciliation.CANCELLED, Reconciliation.EXPIRED,
            }:
                raise OperationalSafetyError("close outcome unresolved")
        self.stage = "FINAL_BARRIER"
        if finalizer is not None:
            finals: list[LiveObservation] = []
            for _ in range(2):
                final = self._observe()
                complete = completion_barrier(
                    store=self.store, catalog=final.catalog, evidence=final.evidence,
                    now_ms=self.io.now_ms(), mark_complete=False,
                )
                regular, trigger, flat = _terminal_flags(final)
                if not complete or not (regular and trigger and flat):
                    raise OperationalSafetyError(
                        "terminal zero-order exact-flat barrier failed"
                    )
                finals.append(final)
            return finalizer(tuple(finals))
        final = self._observe()
        complete = completion_barrier(
            store=self.store, catalog=final.catalog, evidence=final.evidence,
            now_ms=self.io.now_ms(),
        )
        regular, trigger, flat = _terminal_flags(final)
        if not complete or not (regular and trigger and flat):
            raise OperationalSafetyError("terminal zero-order exact-flat barrier failed")
        return OperationalReport(
            1, COMPLETE, hashlib.sha256(self.run_id.encode()).hexdigest()[:16],
            self.writes, self.store.count_kind(CLOSE), regular, trigger, flat,
        )


class SealedFundingBoundaryRunner(SealedLifecycleRunner):
    """Nado order runner that consumes an exact cross-run funding contract."""

    def __init__(
        self,
        *,
        store: IntentStore,
        journal: RuntimeRunJournal,
        io: FundingBoundaryVenueIO,
        capability_loader: Callable[[str], OwnerOrderCapability],
        owner: str,
        sender: str,
        route: FundingRouteBinding,
        risex_journal: JournalIdentity,
        risex_attestation_provider: RisexTerminalEvidenceProvider,
        preparation_gate: PreparationGate | None = None,
        boundary_progression_sink: BoundaryProgressionSink | None = None,
    ) -> None:
        try:
            route.assert_contract()
            risex_journal.assert_contract()
            if risex_journal.venue != RISEX_VENUE:
                raise NadoContractError("funding counterpart journal is not RISEx")
        except NadoContractError:
            raise OperationalSafetyError("funding boundary contract rejected") from None
        if not callable(risex_attestation_provider):
            raise OperationalSafetyError("RISEx attestation provider unavailable")
        if preparation_gate is not None and not callable(preparation_gate):
            raise OperationalSafetyError("Nado preparation gate unavailable")
        if (
            boundary_progression_sink is not None
            and not callable(boundary_progression_sink)
        ):
            raise OperationalSafetyError("Nado boundary progression sink unavailable")
        if any(
            not callable(getattr(io, method, None))
            for method in (
                "capture_funding_baseline",
                "capture_funding_exposure",
                "await_funding_boundary",
                "read_funding_boundary",
            )
        ):
            raise OperationalSafetyError("funding boundary adapter unavailable")
        super().__init__(
            store=store, journal=journal, io=io,
            capability_loader=capability_loader, owner=owner, sender=sender,
        )
        nado_journal = JournalIdentity(
            NADO_VENUE,
            self.run_id,
            _journal_store_identity(self.journal.path),
            self.sender,
        )
        try:
            self.funding_binding = FundingBoundaryBinding(
                route, risex_journal, nado_journal,
            )
            self.funding_binding.assert_contract()
            self.store.bind_funding_boundary(self.funding_binding)
        except BaseException:
            self.store.halt()
            try:
                self.journal.terminalize(self.run_id, "SAFETY", "RUNNER_STARTUP")
            except BaseException:
                pass
            raise
        self.funding_io = io
        self.risex_attestation_provider = risex_attestation_provider
        self._preparation_gate = preparation_gate
        self._preparation_gate_used = False
        self._boundary_progression_sink = boundary_progression_sink
        self._boundary_progression_emitted = False
        self.funding_baseline: NadoFundingBaseline | None = None
        self.funding_exposure: NadoFundingExposure | None = None
        self.funding_event: NadoFundingEvent | None = None
        self.account_funding: NadoAccountFunding | None = None
        self.funding_blocker_reason: str | None = None

    def _await_preparation_gate(self) -> None:
        """Wait for the owner before the first durable Nado intent prepare."""
        if self._preparation_gate is None or self._preparation_gate_used:
            return
        try:
            result = self._preparation_gate(self.funding_binding)
            if inspect.isawaitable(result):
                raise OperationalSafetyError(
                    "Nado preparation gate must be synchronous"
                )
        except OperationalSafetyError:
            raise
        except BaseException as error:
            preserved = _preparation_gate_failure(error)
            if preserved is not None:
                raise preserved from None
            raise OperationalSafetyError("Nado preparation gate rejected") from None
        self._preparation_gate_used = True

    def _emit_boundary_progression(self, kind: str) -> None:
        """Publish only the accepted Nado boundary progression to the owner."""
        if self._boundary_progression_sink is None:
            return
        if kind not in {BOUNDARY_RELEASED, BOUNDARY_CANCELLED}:
            raise OperationalSafetyError("Nado boundary progression kind rejected")
        if self._boundary_progression_emitted:
            raise OperationalSafetyError("Nado boundary progression replay rejected")
        observed_at_ms = self.io.now_ms()
        if type(observed_at_ms) is not int or observed_at_ms <= 0:
            raise OperationalSafetyError(
                "Nado boundary progression timestamp rejected"
            )
        try:
            self._boundary_progression_sink(
                self.funding_binding, kind, observed_at_ms,
            )
        except OperationalSafetyError:
            raise
        except BaseException:
            raise OperationalSafetyError(
                "Nado boundary progression sink rejected"
            ) from None
        self._boundary_progression_emitted = True

    def cancel_boundary_progression(self) -> None:
        """Publish Nado-owned cancellation after an interrupted lifecycle."""
        if not self._boundary_progression_emitted:
            self._emit_boundary_progression(BOUNDARY_CANCELLED)

    def _prepare_funding_entry(
        self,
        *,
        order: SyntheticOrderVector,
        catalog: CatalogSnapshot,
        account: AccountSnapshot,
        triggers: TriggerSnapshot,
        worst_close_price_x18: int,
        signature: str,
        validation_product_id: int,
        validation_valid: bool,
        now_ms: int,
        direction: str,
    ) -> OrderIntent:
        """Prepare the exact route quantity after minimum-size validation."""
        if order.appendix != IOC_APPENDIX or direction not in {LONG, SHORT}:
            raise NadoContractError("funding entry order contract is invalid")
        if (
            order.sender.lower() != self.sender
            or order.owner.lower() != account.owner.lower()
            or order.subaccount_name != account.subaccount_name
            or order.sender.lower() != encode_subaccount(
                account.owner, account.subaccount_name
            ).lower()
        ):
            raise NadoContractError("signed order and preflight subaccount identity mismatch")
        self.core._assert_next_state_write(recv_time=order.recv_time, now_ms=now_ms)
        verify_signed_validation(
            order,
            signature=signature,
            validation_product_id=validation_product_id,
            validation_valid=validation_valid,
        )
        plan = validate_entry_preflight(
            catalog=catalog,
            account=account,
            triggers=triggers,
            product_id=FUNDING_BOUNDARY_TARGET_PRODUCT_ID,
            entry_price_x18=order.price_x18,
            worst_close_price_x18=worst_close_price_x18,
            now_ms=now_ms,
        )
        product = catalog.by_id()[FUNDING_BOUNDARY_TARGET_PRODUCT_ID]
        amount = abs(order.amount_x18)
        expected_amount_x18 = _canonical_quantity_x18(
            self.funding_binding.route.canonical_quantity
        )
        expected_direction = self.funding_binding.route.nado_leg.direction
        if (
            order.product_id != FUNDING_BOUNDARY_TARGET_PRODUCT_ID
            or amount != expected_amount_x18
            or expected_direction not in {LONG, SHORT}
            or direction != expected_direction
            or amount < plan.amount_x18
            or amount % product.step_x18
            or (order.amount_x18 > 0) != (direction == LONG)
        ):
            raise NadoContractError("funding entry quantity or direction is invalid")
        entry_notional = _notional_x18(order.price_x18, amount)
        close_notional = _notional_x18(worst_close_price_x18, amount)
        if min(entry_notional, close_notional) < product.minimum_notional_x18:
            raise NadoContractError("funding entry is below product minimum notional")
        intent = OrderIntent(
            kind=ENTRY,
            product_id=order.product_id,
            nonce=order.nonce,
            recv_time=order.recv_time,
            digest=order_digest(order),
            payload=canonical_payload(order.as_payload()),
            amount_x18=order.amount_x18,
            appendix=order.appendix,
            notional_x18=entry_notional,
            sender=order.sender.lower(),
            owner=order.owner.lower(),
            subaccount_name=order.subaccount_name,
        )
        self._await_preparation_gate()
        self.store.prepare(intent)
        return intent

    def _nado_journal_content_sha256(self) -> str:
        return _nado_terminal_journal_content_sha256(self.store)

    def _local_nado_terminal(self, observation: LiveObservation) -> TerminalEvidence:
        persisted = self.store.funding_boundary_binding()
        if persisted != self.funding_binding:
            raise NadoContractError("persisted Nado funding binding changed")
        regular, trigger, flat = _terminal_flags(observation)
        unresolved = tuple(
            intent.digest
            for intent, state in self.store.intents()
            if state != "RECONCILED"
        )
        unsigned = TerminalEvidence(
            self.funding_binding.nado_journal,
            COMPLETE,
            observation.evidence.observed_at_ms,
            self._nado_journal_content_sha256(),
            regular,
            trigger,
            flat,
            unresolved,
            "0x" + "00" * 32,
        )
        terminal = replace(
            unsigned,
            evidence_digest="0x" + terminal_evidence_digest(unsigned),
        )
        terminal.assert_contract()
        return terminal

    def _external_risex_terminal(
        self,
        observation: LiveObservation,
        round_index: int,
        previous: CrossRunAttestation | None,
    ) -> TerminalEvidence:
        try:
            result = self.risex_attestation_provider(
                self.funding_binding, observation, round_index,
            )
        except Exception:
            raise OperationalSafetyError("RISEx terminal provider failed") from None
        if type(result) is not RisexTerminalEvidence:
            raise OperationalSafetyError(
                "RISEx terminal provider returned invalid evidence"
            )
        try:
            result.assert_contract()
        except NadoContractError:
            raise OperationalSafetyError(
                "RISEx terminal provider returned invalid evidence"
            ) from None
        if (
            result.route != self.funding_binding.route
            or result.journal != self.funding_binding.risex_journal
            or result.terminal.journal != self.funding_binding.risex_journal
            or result.round_index != round_index
        ):
            raise OperationalSafetyError(
                "RISEx terminal is not bound to the persisted route and journal"
            )
        reference_ms = observation.evidence.observed_at_ms
        if abs(result.terminal.observed_at_ms - reference_ms) > MAX_FRESHNESS_MS:
            raise OperationalSafetyError("RISEx terminal evidence is stale")
        if previous is not None and (
            result.terminal.observed_at_ms
            <= previous.risex_terminal.observed_at_ms
        ):
            raise OperationalSafetyError(
                "RISEx terminal rounds were reused or are not fresh"
            )
        return result.terminal

    def _final_attestation(
        self,
        observation: LiveObservation,
        round_index: int,
        previous: CrossRunAttestation | None,
    ) -> CrossRunAttestation:
        # Build Nado's terminal proof before invoking the external provider so
        # that provider code cannot influence the local terminal observation.
        nado_terminal = self._local_nado_terminal(observation)
        risex_terminal = self._external_risex_terminal(
            observation, round_index, previous,
        )
        unsigned = CrossRunAttestation(
            self.funding_binding.route,
            self.funding_binding.risex_journal,
            self.funding_binding.nado_journal,
            risex_terminal,
            nado_terminal,
            "0x" + "00" * 32,
        )
        attestation = replace(
            unsigned,
            attestation_digest="0x" + cross_run_attestation_digest(unsigned),
        )
        attestation.assert_contract()
        if previous is not None:
            if (
                attestation.nado_terminal.observed_at_ms
                <= previous.nado_terminal.observed_at_ms
            ):
                raise NadoContractError(
                    "Nado terminal rounds were reused or are not fresh"
                )
            if (
                _terminal_fingerprint(attestation.risex_terminal)
                != _terminal_fingerprint(previous.risex_terminal)
                or _terminal_fingerprint(attestation.nado_terminal)
                != _terminal_fingerprint(previous.nado_terminal)
            ):
                raise NadoContractError("final funding terminal rounds disagree")
        return attestation

    def _capture_funding_baseline(self, observed: LiveObservation) -> None:
        baseline = self.funding_io.capture_funding_baseline(
            self.funding_binding, observed,
        )
        if not isinstance(baseline, NadoFundingBaseline):
            raise OperationalSafetyError("funding baseline adapter returned invalid evidence")
        try:
            baseline.assert_contract()
            self.store.bind_funding_baseline(baseline)
            self.funding_baseline = baseline
        except NadoContractError:
            raise OperationalSafetyError("funding baseline binding rejected") from None

    def _await_and_read_funding(self, observed: LiveObservation) -> None:
        try:
            exposure = self.funding_io.capture_funding_exposure(
                self.funding_binding, observed,
            )
            if not isinstance(exposure, NadoFundingExposure):
                raise NadoContractError(
                    "funding exposure adapter returned invalid evidence"
                )
            exposure.assert_contract()
            self.store.bind_funding_exposure(exposure)
            self.funding_exposure = exposure
            self.funding_io.await_funding_boundary(self.funding_binding)
            # This is the only RELEASED signal source.  It is emitted from
            # the accepted Nado boundary progression, before the venue-local
            # account-funding read, and carries no funding claim.
            self._emit_boundary_progression(BOUNDARY_RELEASED)
            event, account_funding = self.funding_io.read_funding_boundary(
                self.funding_binding
            )
            if event is not None and not isinstance(event, NadoFundingEvent):
                raise NadoContractError("funding event adapter returned invalid evidence")
            if account_funding is not None and not isinstance(
                account_funding, NadoAccountFunding
            ):
                raise NadoContractError(
                    "account funding adapter returned invalid evidence"
                )
            self.funding_event, self.account_funding = event, account_funding
            self.funding_blocker_reason = (
                None
                if event is not None and account_funding is not None
                else FUNDING_BLOCKED_MISSING
            )
        except BaseException as error:
            # Funding evidence failure must not skip the already-authorized
            # reduce-only close.  The finalizer durably records the blocker.
            if not self._boundary_progression_emitted:
                try:
                    # A failed/interrupted Nado progression must unblock a
                    # waiting RISEx gate with a coordination-only CANCELLED
                    # signal; it never claims a funding outcome.
                    self._emit_boundary_progression(BOUNDARY_CANCELLED)
                except BaseException:
                    pass
            self.funding_event = None
            self.account_funding = None
            self.funding_exposure = None
            if isinstance(error, NadoContractError):
                self.funding_blocker_reason = FUNDING_BLOCKED_CONTRADICTORY
                return
            if isinstance(error, OperationalSafetyError):
                message = str(error)
                transport_failure = any(
                    marker in message
                    for marker in (
                        "transport",
                        "HTTP status",
                        "stream ended",
                        "deadline exhausted",
                        "baseline or event is unavailable",
                    )
                )
                self.funding_blocker_reason = (
                    FUNDING_BLOCKED_MISSING
                    if transport_failure
                    else FUNDING_BLOCKED_CONTRADICTORY
                )
                return
            self.funding_blocker_reason = FUNDING_BLOCKED_MISSING

    def _finalize_funding(
        self, finals: tuple[LiveObservation, ...],
    ) -> FundingBoundaryReport:
        if len(finals) != 2:
            raise OperationalSafetyError("two final funding rounds are required")
        attestations: list[CrossRunAttestation] = []
        previous: CrossRunAttestation | None = None
        for round_index, observation in enumerate(finals, start=1):
            attestation = self._final_attestation(
                observation, round_index, previous,
            )
            attestation.assert_contract()
            attestations.append(attestation)
            previous = attestation
        first, second = attestations
        if (
            first.route != second.route
            or first.risex_journal != second.risex_journal
            or first.nado_journal != second.nado_journal
            or _terminal_fingerprint(first.risex_terminal)
            != _terminal_fingerprint(second.risex_terminal)
            or _terminal_fingerprint(first.nado_terminal)
            != _terminal_fingerprint(second.nado_terminal)
        ):
            raise NadoContractError("final funding terminal rounds disagree")
        if self.funding_blocker_reason is not None:
            result = self.store.record_nado_funding_blocker(
                attestation=second,
                reason=self.funding_blocker_reason,
            )
        else:
            try:
                result = self.store.record_nado_funding_boundary(
                    attestation=second,
                    event=self.funding_event,
                    account_funding=self.account_funding,
                )
            except NadoContractError:
                result = self.store.record_nado_funding_blocker(
                    attestation=second,
                    reason=FUNDING_BLOCKED_CONTRADICTORY,
                )
        regular, trigger, flat = _terminal_flags(finals[-1])
        if result.completion_eligible:
            self.store._mark_complete()
        return FundingBoundaryReport(
            1,
            COMPLETE if result.completion_eligible else "BLOCKED",
            hashlib.sha256(self.run_id.encode()).hexdigest()[:16],
            self.writes,
            self.store.count_kind(CLOSE),
            result.status,
            result.rate_x18,
            result.cash_x18,
            True,
            regular,
            trigger,
            flat,
            None if result.completion_eligible else result.status,
            result.aggregate_payment_x18,
            result.account_idx,
        )

    def run(self) -> FundingBoundaryReport:
        result = self._run_order_lifecycle(
            route=self.funding_binding.route,
            before_entry=self._capture_funding_baseline,
            before_close=self._await_and_read_funding,
            finalizer=self._finalize_funding,
        )
        if not isinstance(result, FundingBoundaryReport):
            raise OperationalSafetyError("funding boundary report was not produced")
        return result


def _latest_execute_failure(
    runner: SealedLifecycleRunner,
) -> tuple[str, int | None] | None:
    try:
        for intent, _state in reversed(runner.store.intents()):
            failure = runner.store.execute_failure(intent.digest)
            if failure is not None:
                return failure
    except BaseException:
        return None
    return None


def _persist_runner_failure(
    runner: SealedLifecycleRunner, error: BaseException,
) -> tuple[str, tuple[str, int | None] | None]:
    persisted_execute = _latest_execute_failure(runner)
    if isinstance(error, DurableExecuteFailure):
        failure_class = error.failure_class
    elif persisted_execute is not None:
        failure_class = persisted_execute[0]
    elif isinstance(error, DurableOperationalFailure):
        failure_class = error.failure_class
    else:
        failure_class = _failure_class(error)
    stage = (
        error.stage
        if isinstance(error, DurableOperationalFailure)
        else error.stage
        if isinstance(error, _PreparationGateFailure)
        else runner.stage
    )
    runner.terminalize(failure_class, stage)
    return failure_class, persisted_execute


def _fixture_run(
    *, path: Path, io: VenueIO,
    capability_loader: Callable[[str], OwnerOrderCapability], owner: str, sender: str,
) -> OperationalReport:
    _prepare_file(path)
    store = IntentStore(path)
    try:
        # Fixture-only observability for PREPARED-before-dispatch assertions.
        try:
            setattr(io, "store", store)
        except BaseException:
            pass
        runner = SealedLifecycleRunner(
            store=store, journal=RuntimeRunJournal(path), io=io,
            capability_loader=capability_loader, owner=owner, sender=sender,
        )
        try:
            return runner.run()
        except DurableOperationalFailure as error:
            _persist_runner_failure(runner, error)
            raise
        except DurableExecuteFailure as error:
            _persist_runner_failure(runner, error)
            raise
        except BaseException as error:
            failure_class, persisted_execute = _persist_runner_failure(runner, error)
            if not runner.store.intents():
                raise DurableOperationalFailure(failure_class, runner.stage) from None
            if persisted_execute is not None:
                raise error
            raise
    finally:
        store.close()


def _blocked_funding_report(
    runner: SealedFundingBoundaryRunner, reason: str,
) -> FundingBoundaryReport:
    return FundingBoundaryReport(
        1,
        "BLOCKED",
        hashlib.sha256(runner.run_id.encode()).hexdigest()[:16],
        runner.writes,
        runner.store.count_kind(CLOSE),
        None,
        None,
        None,
        False,
        False,
        False,
        False,
        reason,
    )


def _run_funding_boundary_fixture_or_operational(
    *,
    path: Path,
    io: FundingBoundaryVenueIO,
    capability_loader: Callable[[str], OwnerOrderCapability],
    owner: str,
    sender: str,
    route: FundingRouteBinding,
    risex_journal: JournalIdentity,
    risex_attestation_provider: RisexTerminalEvidenceProvider,
    private_read: bool,
) -> FundingBoundaryReport:
    _prepare_file(path)
    store = IntentStore(path)
    try:
        if isinstance(io, OperationalVenueIO):
            io._enable_funding_boundary_target()
        try:
            setattr(io, "store", store)
        except BaseException:
            pass
        runner: SealedFundingBoundaryRunner | None = None
        try:
            runner = SealedFundingBoundaryRunner(
                store=store,
                journal=RuntimeRunJournal(path),
                io=io,
                capability_loader=capability_loader,
                owner=owner,
                sender=sender,
                route=route,
                risex_journal=risex_journal,
                risex_attestation_provider=risex_attestation_provider,
            )
            if private_read:
                runner.stage = "PRIVATE_READ_BARRIER"
                preflight = asyncio.run(_accepted_private_read())
                if preflight.get("status") != "FINALIZED":
                    raise DurableOperationalFailure(
                        _report_failure_class(preflight), "PRIVATE_READ_BARRIER",
                    )
                note_barrier = getattr(io, "note_private_read_barrier", None)
                if callable(note_barrier):
                    note_barrier()
            report = runner.run()
            if report.status == "BLOCKED":
                runner.terminalize("SAFETY", "FINAL_BARRIER")
            return report
        except BaseException as error:
            if runner is None:
                try:
                    store.halt()
                except BaseException:
                    pass
                return FundingBoundaryReport(
                    1, "BLOCKED", "UNBOUND", 0, 0, None, None, None,
                    False, False, False, False, _failure_class(error),
                )
            failure_class, _persisted_execute = _persist_runner_failure(runner, error)
            return _blocked_funding_report(runner, failure_class)
    finally:
        store.close()


def _fixture_funding_boundary_run(
    *,
    path: Path,
    io: FundingBoundaryVenueIO,
    capability_loader: Callable[[str], OwnerOrderCapability],
    owner: str,
    sender: str,
    route: FundingRouteBinding,
    risex_journal: JournalIdentity,
    risex_attestation_provider: RisexTerminalEvidenceProvider,
) -> FundingBoundaryReport:
    """Run the full funding contract against an injected fixture IO only."""
    return _run_funding_boundary_fixture_or_operational(
        path=path,
        io=io,
        capability_loader=capability_loader,
        owner=owner,
        sender=sender,
        route=route,
        risex_journal=risex_journal,
        risex_attestation_provider=risex_attestation_provider,
        private_read=False,
    )


def run_funding_boundary(
    *,
    route: FundingRouteBinding,
    risex_journal: JournalIdentity,
    io: FundingBoundaryVenueIO,
    risex_attestation_provider: RisexTerminalEvidenceProvider,
) -> dict[str, object]:
    """Run one production Nado funding-boundary route with fixed credentials."""
    owner, sender = _strict_identity()
    report = _run_funding_boundary_fixture_or_operational(
        path=_funding_boundary_store_path(),
        io=io,
        capability_loader=_load_capability,
        owner=owner,
        sender=sender,
        route=route,
        risex_journal=risex_journal,
        risex_attestation_provider=risex_attestation_provider,
        private_read=True,
    )
    return report.sanitized()


def run() -> dict[str, object]:
    """Run the sealed production operation; accepts no runtime parameters."""
    owner, sender = _strict_identity()
    path = _production_store_path()
    _prepare_file(path)
    store = IntentStore(path)
    runner: SealedLifecycleRunner | None = None
    try:
        # The network observer is deliberately constructed here so importing
        # this module remains inert and normal startup cannot reach Level C.
        io = OperationalVenueIO(owner, sender)
        runner = SealedLifecycleRunner(
            store=store, journal=RuntimeRunJournal(path), io=io,
            capability_loader=_load_capability, owner=owner, sender=sender,
        )
        runner.stage = "PRIVATE_READ_BARRIER"
        preflight = asyncio.run(_accepted_private_read())
        if preflight.get("status") != "FINALIZED":
            raise DurableOperationalFailure(
                _report_failure_class(preflight), "PRIVATE_READ_BARRIER",
            )
        note_barrier = getattr(io, "note_private_read_barrier", None)
        if callable(note_barrier):
            note_barrier()
        report = runner.run()
        return report.sanitized()
    except DurableOperationalFailure as error:
        if runner is None:
            raise
        _persist_runner_failure(runner, error)
        raise
    except DurableExecuteFailure as error:
        if runner is None:
            raise
        _persist_runner_failure(runner, error)
        raise
    except BaseException as error:
        if runner is None:
            raise
        failure_class, persisted_execute = _persist_runner_failure(runner, error)
        if not store.intents():
            raise DurableOperationalFailure(failure_class, runner.stage) from None
        if persisted_execute is not None:
            raise DurableExecuteFailure(*persisted_execute) from None
        raise
    finally:
        store.close()


_PUBLIC_FUNDING_FIELDS = frozenset({
    "type", "timestamp", "product_id", "payment_amount", "open_interest",
    "cumulative_funding_long_x18", "cumulative_funding_short_x18", "dt",
})
_ACCOUNT_PAYMENT_FIELDS = frozenset({
    "product_id", "idx", "timestamp", "amount", "balance_amount", "rate_x18",
    "oracle_price_x18",
})


def _official_integer_text(
    value: object, label: str, *, positive: bool = False, nonnegative: bool = False,
) -> int:
    if type(value) is not str or not value:
        raise OperationalSafetyError(f"{label} schema mismatch")
    if value.startswith("-"):
        digits = value[1:]
    else:
        digits = value
    if not digits.isdigit():
        raise OperationalSafetyError(f"{label} schema mismatch")
    try:
        parsed = int(value)
    except ValueError:
        raise OperationalSafetyError(f"{label} schema mismatch") from None
    if str(parsed) != value:
        raise OperationalSafetyError(f"{label} schema mismatch")
    if positive and parsed <= 0:
        raise OperationalSafetyError(f"{label} schema mismatch")
    if nonnegative and parsed < 0:
        raise OperationalSafetyError(f"{label} schema mismatch")
    return parsed


def parse_nado_public_funding_event(raw: object) -> NadoFundingEvent:
    """Parse only the documented public product-level funding event."""
    if type(raw) is not dict or not _PUBLIC_FUNDING_FIELDS <= set(raw):
        raise OperationalSafetyError("public funding event schema mismatch")
    if raw["type"] != "funding_payment" or type(raw["product_id"]) is not int:
        raise OperationalSafetyError("public funding event schema mismatch")
    unsigned = NadoFundingEvent(
        raw["product_id"],
        _official_integer_text(raw["timestamp"], "funding event timestamp", positive=True),
        _official_integer_text(raw["payment_amount"], "funding aggregate payment"),
        _official_integer_text(raw["open_interest"], "funding open interest", nonnegative=True),
        _official_integer_text(
            raw["cumulative_funding_long_x18"], "funding cumulative long value"
        ),
        _official_integer_text(
            raw["cumulative_funding_short_x18"], "funding cumulative short value"
        ),
        _official_integer_text(raw["dt"], "funding event dt", positive=True),
        "0x" + "00" * 32,
    )
    try:
        event = replace(
            unsigned,
            event_digest="0x" + nado_funding_event_digest(unsigned),
        )
        event.assert_contract()
        return event
    except NadoContractError:
        raise OperationalSafetyError("public funding event contract mismatch") from None


def parse_nado_account_payment_row(
    raw: object,
    binding: FundingBoundaryBinding,
    *,
    payment_kind: str = "funding",
) -> NadoAccountFunding:
    """Parse one official SDK ``IndexerPayment`` row.

    The archive response may add irrelevant fields.  Required fields remain a
    closed semantic contract, and the caller selects ``funding_payments`` so an
    interest row can never be mistaken for account funding.
    """
    if type(raw) is not dict or not _ACCOUNT_PAYMENT_FIELDS <= set(raw):
        raise OperationalSafetyError("account payment row schema mismatch")
    if type(raw["product_id"]) is not int:
        raise OperationalSafetyError("account payment row schema mismatch")
    parsed = _parse_account_payment_fields(raw)
    owner, subaccount_name = decode_subaccount(binding.nado_journal.account_id)
    unsigned = NadoAccountFunding(
        binding.nado_journal,
        owner,
        subaccount_name,
        raw["product_id"],
        parsed["idx"], parsed["timestamp"], parsed["amount"],
        parsed["balance_amount"], parsed["rate_x18"], parsed["oracle_price_x18"],
        "0x" + "00" * 32,
        payment_kind,
    )
    try:
        account = replace(
            unsigned,
            evidence_digest="0x" + nado_account_funding_digest(unsigned),
        )
        account.assert_contract()
        return account
    except NadoContractError:
        raise OperationalSafetyError("account payment row contract mismatch") from None


def parse_nado_account_funding_row(
    raw: object, binding: FundingBoundaryBinding,
) -> NadoAccountFunding:
    return parse_nado_account_payment_row(raw, binding, payment_kind="funding")


def _parse_account_payment_fields(raw: dict[str, object]) -> dict[str, int]:
    return {
        "idx": _official_integer_text(raw["idx"], "account payment index", nonnegative=True),
        "timestamp": _official_integer_text(
            raw["timestamp"], "account payment timestamp", positive=True
        ),
        "amount": _official_integer_text(raw["amount"], "account payment amount"),
        "balance_amount": _official_integer_text(
            raw["balance_amount"], "account payment balance"
        ),
        "rate_x18": _official_integer_text(raw["rate_x18"], "account payment rate"),
        "oracle_price_x18": _official_integer_text(
            raw["oracle_price_x18"], "account payment oracle", positive=True
        ),
    }


class OperationalVenueIO:
    """Fixed-host production surface. Response semantics fail closed.

    The first accepted operational invocation supplies the venue observations;
    this class intentionally has no configurable URL/account/market surface.
    """

    def __init__(self, owner: str, sender: str) -> None:
        self.owner, self.sender = owner.lower(), sender.lower()
        self._target_product_id = TARGET_PRODUCT_ID
        self._target_ticker_id = TARGET_TICKER_ID
        self._query_admission = _NadoSnapshotAdmission()
        self._terminal: dict[str, str] = {}
        self._cancelled_entry: str | None = None
        self._resting_orders: dict[str, OrderEvidence] = {}
        self._v_quote_balances: dict[int, int] = {}
        self._perp_last_cumulative_funding_x18: dict[int, int] = {}
        self._funding_states: dict[int, tuple[int, int, int]] = {}
        self._funding_baseline: NadoFundingBaseline | None = None
        self._funding_exposure: NadoFundingExposure | None = None
        self._funding_event: NadoFundingEvent | None = None
        self._connection_factory = lambda host: http.client.HTTPSConnection(
            host, timeout=HTTP_TIMEOUT_SECONDS, context=ssl.create_default_context(),
        )

    def now_ms(self) -> int:
        return time.time_ns() // 1_000_000

    def terminal_status(self, digest: str) -> str | None:
        return self._terminal.get(digest.lower())

    def _enable_funding_boundary_target(self) -> None:
        """Switch this fixed IO to ETH only under the funding entrypoint."""
        self._target_product_id = FUNDING_BOUNDARY_TARGET_PRODUCT_ID
        self._target_ticker_id = FUNDING_BOUNDARY_TARGET_TICKER_ID

    def note_private_read_barrier(self) -> None:
        """Anchor admission after the accepted private-read gateway window."""
        self._query_admission.note_gateway_window()

    def observe(self, digests: tuple[str, ...]) -> LiveObservation:
        self._query_admission.before_snapshot()
        try:
            return self._observe_unpaced(digests)
        finally:
            self._query_admission.complete_snapshot()

    def _observe_unpaced(self, digests: tuple[str, ...]) -> LiveObservation:
        contracts = self._gateway({"type": "contracts"}, "query_contracts")
        if (
            set(contracts) != {"chain_id", "endpoint_addr"}
            or int(contracts["chain_id"]) != 763373
            or str(contracts["endpoint_addr"]).lower()
            != FixedPreflightIdentity.endpoint.lower()
        ):
            raise OperationalSafetyError("environment identity mismatch")
        if self._gateway({"type": "status"}, "query_status") != "active":
            raise OperationalSafetyError("engine is not active")
        linked = self._gateway(
            {"type": "linked_signer", "subaccount": self.sender},
            "query_linked_signer",
        )
        if set(linked) != {"linked_signer"} or linked["linked_signer"] != "0x" + "00" * 20:
            raise OperationalSafetyError("unrelated linked signer state")
        raw_pairs = self._get(_GATEWAY_HOST, "/v2/pairs")
        pairs = self._pairs(raw_pairs)
        raw_products = self._gateway({"type": "all_products"}, "query_all_products")
        products = self._products(raw_products, pairs)
        target = products.get(self._target_product_id)
        if (
            target is None or target.product_type != ACTIVE_PERP
            or target.symbol != self._target_ticker_id
        ):
            raise OperationalSafetyError("fixed target product identity unavailable")
        product_ids = tuple(sorted(products))
        raw_orders = self._gateway(
            {"type": "orders", "sender": self.sender, "product_ids": list(product_ids)},
            "query_orders",
        )
        regular, open_orders = self._orders(raw_orders, products)
        self._resting_orders = {order.digest.lower(): order for order in open_orders}
        raw_account = self._gateway(
            {"type": "subaccount_info", "subaccount": self.sender},
            "query_subaccount_info",
        )
        positions = self._positions(raw_account, products)
        isolated = self._gateway(
            {"type": "isolated_positions", "subaccount": self.sender},
            "query_isolated_positions",
        )
        if set(isolated) != {"isolated_positions"} or isolated["isolated_positions"]:
            raise OperationalSafetyError("unrelated isolated position state")
        market = self._gateway(
            {"type": "market_price", "product_id": self._target_product_id},
            "query_market_price",
        )
        if set(market) != {"product_id", "bid_x18", "ask_x18"}:
            raise OperationalSafetyError("market price schema mismatch")
        bid, ask = int(market["bid_x18"]), int(market["ask_x18"])
        triggers = self._triggers()
        fills = self._fills(digests, products)
        observed = self.now_ms()
        account = AccountSnapshot(
            chain_id=763373, domain_name="Nado", domain_version="0.0.1",
            endpoint=FixedPreflightIdentity.endpoint,
            gateway="https://gateway.test.nado.xyz/v1",
            gateway_ws="wss://gateway.test.nado.xyz/v1/ws",
            archive="https://archive.test.nado.xyz/v1",
            trigger="https://trigger.test.nado.xyz/v1", owner=self.owner,
            subaccount_name=SUBACCOUNT_NAME, observed_at_ms=observed,
            fresh=True, authoritative_source="engine",
            regular_orders_by_product=regular,
            cross_perp_amounts_x18=positions, isolated_positions=(),
            snapshot_id=str(uuid.uuid4()),
            perp_last_cumulative_funding_x18=self._perp_last_cumulative_funding_x18,
        )
        trigger_snapshot = TriggerSnapshot(
            self.owner, SUBACCOUNT_NAME, observed, True, "trigger", triggers,
            snapshot_id=str(uuid.uuid4()),
        )
        terminal_digest = None
        terminal_status = None
        for digest in reversed(digests):
            if digest.lower() in self._terminal:
                terminal_digest = digest
                terminal_status = self._terminal[digest.lower()]
                break
        exact_cancel = None
        if self._cancelled_entry is not None:
            exact_cancel = next(
                (digest for digest in reversed(digests)
                 if self._terminal.get(digest.lower()) == "CANCELLED"), None,
            )
        evidence = EngineEvidence(
            account, trigger_snapshot, tuple(open_orders), tuple(fills), observed,
            exact_cancel_digest=exact_cancel,
            terminal_digest=terminal_digest, terminal_status=terminal_status,
            archive_digests=tuple(fill.digest for fill in fills),
        )
        return LiveObservation(
            CatalogSnapshot(tuple(products.values()), True, observed, True, "engine"),
            evidence, target, bid, ask,
        )

    @staticmethod
    def _funding_interval(binding: FundingBoundaryBinding) -> tuple[int, int]:
        return (
            binding.route.settlement_at_ms,
            binding.route.settlement_at_ms + FUNDING_BOUNDARY_INTERVAL_MS,
        )

    def _assert_funding_binding(self, binding: FundingBoundaryBinding) -> None:
        try:
            binding.assert_contract()
            if (
                binding.route.nado_product_id != FUNDING_BOUNDARY_TARGET_PRODUCT_ID
                or binding.route.nado_leg.market != FUNDING_BOUNDARY_TARGET_TICKER_ID
                or binding.nado_journal.account_id.lower() != self.sender
            ):
                raise NadoContractError("funding adapter identity mismatch")
            owner, subaccount_name = decode_subaccount(binding.nado_journal.account_id)
            if owner.lower() != self.owner or subaccount_name != SUBACCOUNT_NAME:
                raise NadoContractError("funding adapter subaccount mismatch")
        except NadoContractError:
            raise OperationalSafetyError("funding adapter identity mismatch") from None

    def capture_funding_baseline(
        self, binding: FundingBoundaryBinding, observation: LiveObservation,
    ) -> NadoFundingBaseline:
        """Read and return the immutable pre-entry archive/product baseline."""
        self._assert_funding_binding(binding)
        try:
            _assert_authoritative_account(
                observation.catalog,
                observation.evidence.account,
                now_ms=self.now_ms(),
                require_flat=True,
            )
        except NadoContractError:
            raise OperationalSafetyError("funding baseline account evidence rejected") from None
        if (
            observation.product.product_id != FUNDING_BOUNDARY_TARGET_PRODUCT_ID
            or observation.product.symbol != FUNDING_BOUNDARY_TARGET_TICKER_ID
            or observation.evidence.account.owner.lower() != self.owner
            or observation.evidence.account.subaccount_name != SUBACCOUNT_NAME
        ):
            raise OperationalSafetyError("funding baseline product or account mismatch")
        position = observation.evidence.account.cross_perp_amounts_x18.get(
            FUNDING_BOUNDARY_TARGET_PRODUCT_ID
        )
        if position is None or position != 0:
            raise OperationalSafetyError("funding baseline position is not exactly flat")
        v_quote = self._v_quote_balances.get(FUNDING_BOUNDARY_TARGET_PRODUCT_ID)
        if v_quote is None or v_quote != 0:
            raise OperationalSafetyError("funding baseline v_quote is not exactly flat")
        state = self._funding_states.get(FUNDING_BOUNDARY_TARGET_PRODUCT_ID)
        if state is None:
            raise OperationalSafetyError("funding baseline public state is unavailable")
        cumulative_long, cumulative_short, open_interest = state
        if open_interest < 0:
            raise OperationalSafetyError("funding baseline open interest is invalid")
        try:
            funding_rows, all_indices = self._read_funding_history(binding)
        except OperationalSafetyError:
            raise
        high_water = max(all_indices) if all_indices else None
        empty_terminal = not all_indices
        baseline = NadoFundingBaseline(
            binding.nado_journal,
            self.owner,
            SUBACCOUNT_NAME,
            FUNDING_BOUNDARY_TARGET_PRODUCT_ID,
            binding.route.settlement_at_ms,
            high_water,
            empty_terminal,
            position,
            v_quote,
            observation.evidence.account.observed_at_ms,
            observation.evidence.account.snapshot_id,
            cumulative_long,
            cumulative_short,
            open_interest,
            observation.catalog.observed_at_ms,
            "0x" + "00" * 32,
        )
        try:
            baseline = replace(
                baseline,
                baseline_digest="0x" + self._baseline_digest(baseline),
            )
            baseline.assert_contract()
        except NadoContractError:
            raise OperationalSafetyError("funding baseline contract rejected") from None
        self._funding_baseline = baseline
        return baseline

    def capture_funding_exposure(
        self, binding: FundingBoundaryBinding, observation: LiveObservation,
    ) -> NadoFundingExposure:
        """Capture the exact fresh signed post-entry exposure before waiting."""
        self._assert_funding_binding(binding)
        baseline = self._funding_baseline
        if baseline is None:
            raise OperationalSafetyError("funding exposure baseline is unavailable")
        try:
            _assert_authoritative_account(
                observation.catalog,
                observation.evidence.account,
                now_ms=self.now_ms(),
                require_flat=False,
                after_ms=baseline.position_observed_at_ms,
            )
        except NadoContractError:
            raise OperationalSafetyError("funding exposure account evidence rejected") from None
        if (
            observation.product.product_id != FUNDING_BOUNDARY_TARGET_PRODUCT_ID
            or observation.product.symbol != FUNDING_BOUNDARY_TARGET_TICKER_ID
            or observation.evidence.account.owner.lower() != self.owner
            or observation.evidence.account.subaccount_name != SUBACCOUNT_NAME
        ):
            raise OperationalSafetyError("funding exposure product or account mismatch")
        position = observation.evidence.account.cross_perp_amounts_x18.get(
            FUNDING_BOUNDARY_TARGET_PRODUCT_ID
        )
        route_quantity = _canonical_quantity_x18(binding.route.nado_leg.canonical_quantity)
        expected_position = (
            route_quantity
            if binding.route.nado_leg.direction == LONG
            else -route_quantity
        )
        if position != expected_position:
            raise OperationalSafetyError("funding exposure position is not the exact route fill")
        cumulative = observation.evidence.account.perp_last_cumulative_funding_x18.get(
            FUNDING_BOUNDARY_TARGET_PRODUCT_ID
        )
        if cumulative is None:
            raise OperationalSafetyError("funding exposure cumulative state is unavailable")
        side = LONG if position > 0 else SHORT
        baseline_cumulative = (
            baseline.cumulative_funding_long_x18
            if side == LONG
            else baseline.cumulative_funding_short_x18
        )
        if cumulative != baseline_cumulative:
            raise OperationalSafetyError("funding exposure cumulative state mismatch")
        exposure = NadoFundingExposure(
            binding.nado_journal,
            self.owner,
            SUBACCOUNT_NAME,
            FUNDING_BOUNDARY_TARGET_PRODUCT_ID,
            binding.route.nado_leg.direction,
            position,
            route_quantity,
            observation.evidence.account.observed_at_ms,
            observation.evidence.account.snapshot_id,
            side,
            cumulative,
            "0x" + "00" * 32,
        )
        try:
            exposure = replace(
                exposure,
                exposure_digest="0x" + nado_funding_exposure_digest(exposure),
            )
            exposure.assert_contract()
        except NadoContractError:
            raise OperationalSafetyError("funding exposure contract rejected") from None
        self._funding_exposure = exposure
        return exposure

    @staticmethod
    def _baseline_digest(baseline: NadoFundingBaseline) -> str:
        from .nado_testnet_lifecycle import nado_funding_baseline_digest
        return nado_funding_baseline_digest(baseline)

    def _read_funding_page(
        self, binding: FundingBoundaryBinding, max_idx: int | None,
    ) -> tuple[list[NadoAccountFunding], list[int], int | None]:
        body = {
            "interest_and_funding": {
                "subaccount": self.sender,
                "product_ids": [FUNDING_BOUNDARY_TARGET_PRODUCT_ID],
                "max_idx": None if max_idx is None else str(max_idx),
                "limit": NADO_FUNDING_PAGE_LIMIT,
            }
        }
        raw = self._post(_ARCHIVE_HOST, "/v1", body)
        required = {"interest_payments", "funding_payments", "next_idx"}
        if type(raw) is not dict or not required <= set(raw):
            raise OperationalSafetyError("archive funding response schema mismatch")
        interest_rows, funding_rows = raw["interest_payments"], raw["funding_payments"]
        if type(interest_rows) is not list or type(funding_rows) is not list:
            raise OperationalSafetyError("archive funding response schema mismatch")
        if len(interest_rows) > NADO_FUNDING_PAGE_LIMIT or len(funding_rows) > NADO_FUNDING_PAGE_LIMIT:
            raise OperationalSafetyError("archive funding page is not bounded")
        all_indices: list[int] = []
        for row in interest_rows:
            if type(row) is not dict or type(row.get("product_id")) is not int:
                raise OperationalSafetyError("archive interest row schema mismatch")
            if row["product_id"] != FUNDING_BOUNDARY_TARGET_PRODUCT_ID:
                raise OperationalSafetyError("archive funding product mismatch")
            parsed = _parse_account_payment_fields(row)
            all_indices.append(parsed["idx"])
        parsed_funding: list[NadoAccountFunding] = []
        for row in funding_rows:
            if (
                type(row) is not dict
                or row.get("product_id") != FUNDING_BOUNDARY_TARGET_PRODUCT_ID
            ):
                raise OperationalSafetyError("archive funding product mismatch")
            parsed_funding.append(parse_nado_account_funding_row(row, binding))
            all_indices.append(parsed_funding[-1].idx)
        next_raw = raw["next_idx"]
        if next_raw is None:
            next_idx = None
        else:
            next_idx = _official_integer_text(
                next_raw, "archive funding next index", nonnegative=True
            )
        if max_idx is not None and any(idx > max_idx for idx in all_indices):
            raise OperationalSafetyError("archive funding cursor was not respected")
        return parsed_funding, all_indices, next_idx

    def _read_funding_history(
        self, binding: FundingBoundaryBinding, *, stop_idx: int | None = None,
    ) -> tuple[list[NadoAccountFunding], list[int]]:
        cursor: int | None = None
        pages = 0
        funding_rows: list[NadoAccountFunding] = []
        all_indices: list[int] = []
        seen_rows: dict[tuple[str, int], NadoAccountFunding] = {}
        seen_indices: set[int] = set()
        while pages < NADO_FUNDING_MAX_PAGES:
            page_rows, page_indices, next_idx = self._read_funding_page(binding, cursor)
            pages += 1
            if not page_indices and next_idx is not None:
                raise OperationalSafetyError("archive funding pagination is incomplete")
            for idx in page_indices:
                if idx not in seen_indices:
                    seen_indices.add(idx)
                    all_indices.append(idx)
            for row in page_rows:
                identity = ("funding", row.idx)
                previous = seen_rows.get(identity)
                if previous is not None and previous != row:
                    raise OperationalSafetyError("archive funding row is contradictory")
                if previous is None:
                    seen_rows[identity] = row
                    funding_rows.append(row)
            if stop_idx is not None and page_indices and max(page_indices) <= stop_idx:
                return funding_rows, all_indices
            if next_idx is None:
                return funding_rows, all_indices
            if cursor is not None and next_idx >= cursor:
                raise OperationalSafetyError("archive funding cursor did not advance")
            cursor = next_idx
        raise OperationalSafetyError("archive funding pagination is not bounded")

    async def _await_funding_boundary_async(
        self, binding: FundingBoundaryBinding,
    ) -> None:
        now_ms = self.now_ms()
        deadline = time.monotonic() + max(
            0.0,
            (binding.route.settlement_at_ms - now_ms) / 1_000,
        ) + NADO_FUNDING_WAIT_GRACE_SECONDS
        try:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise OperationalSafetyError(
                    "public funding stream deadline exhausted"
                )
            connect_timeout = min(HTTP_TIMEOUT_SECONDS, remaining)
            timeout = aiohttp.ClientTimeout(total=None, sock_connect=connect_timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.ws_connect(
                    _FUNDING_SUBSCRIBE_URL,
                    heartbeat=30,
                    timeout=connect_timeout,
                ) as websocket:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise OperationalSafetyError(
                            "public funding stream deadline exhausted"
                        )
                    await asyncio.wait_for(websocket.send_json({
                        "method": "subscribe",
                        "stream": {
                            "type": "funding_payment",
                            "product_id": FUNDING_BOUNDARY_TARGET_PRODUCT_ID,
                        },
                    }), timeout=remaining)
                    while True:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise OperationalSafetyError(
                                "public funding stream deadline exhausted"
                            )
                        message = await websocket.receive(timeout=remaining)
                        if time.monotonic() >= deadline:
                            raise OperationalSafetyError(
                                "public funding stream deadline exhausted"
                            )
                        if message.type is aiohttp.WSMsgType.TEXT:
                            try:
                                payload = json.loads(message.data)
                            except (TypeError, json.JSONDecodeError):
                                raise OperationalSafetyError(
                                    "public funding event schema mismatch"
                                ) from None
                            if type(payload) is not dict or payload.get("type") != "funding_payment":
                                continue
                            event = parse_nado_public_funding_event(payload)
                            if event.product_id != binding.route.nado_product_id:
                                raise OperationalSafetyError(
                                    "public funding event product mismatch"
                                )
                            start_ms, end_ms = self._funding_interval(binding)
                            event_ms = event.timestamp // 1_000_000
                            if not start_ms <= event_ms < end_ms:
                                raise OperationalSafetyError(
                                    "public funding event boundary mismatch"
                                )
                            self._funding_event = event
                            return
                        if message.type in {
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.CLOSE,
                            aiohttp.WSMsgType.ERROR,
                        }:
                            raise OperationalSafetyError(
                                "public funding stream ended before settlement"
                            )
        except OperationalSafetyError:
            raise
        except asyncio.TimeoutError:
            if time.monotonic() >= deadline:
                raise OperationalSafetyError(
                    "public funding stream deadline exhausted"
                ) from None
            raise OperationalSafetyError(
                "public funding stream transport failed"
            ) from None
        except (aiohttp.ClientError, OSError):
            raise OperationalSafetyError(
                "public funding stream transport failed"
            ) from None

    def await_funding_boundary(self, binding: FundingBoundaryBinding) -> None:
        self._assert_funding_binding(binding)
        if self._funding_event is not None:
            return
        try:
            asyncio.run(self._await_funding_boundary_async(binding))
        except RuntimeError as error:
            if "cannot be called from a running event loop" in str(error):
                raise OperationalSafetyError(
                    "public funding stream requires a synchronous boundary"
                ) from None
            raise

    def read_funding_boundary(
        self, binding: FundingBoundaryBinding,
    ) -> tuple[NadoFundingEvent | None, NadoAccountFunding | None]:
        self._assert_funding_binding(binding)
        baseline = self._funding_baseline
        event = self._funding_event
        if baseline is None or event is None:
            raise OperationalSafetyError("funding baseline or event is unavailable")
        funding_rows, _all_indices = self._read_funding_history(
            binding, stop_idx=baseline.history_high_water_idx,
        )
        new_rows = [
            row for row in funding_rows
            if baseline.history_high_water_idx is None
            or row.idx > baseline.history_high_water_idx
        ]
        if not new_rows:
            return event, None
        start_ms, end_ms = self._funding_interval(binding)
        for row in new_rows:
            row_ms = row.timestamp * 1_000
            if not start_ms <= row_ms < end_ms:
                raise OperationalSafetyError(
                    "account funding row boundary mismatch"
                )
        if len(new_rows) != 1:
            raise OperationalSafetyError("multiple post-boundary funding rows")
        return event, new_rows[0]

    def validate_order(self, order: SyntheticOrderVector, signature: str) -> bool:
        try:
            raw = bytes.fromhex(signature[2:])
            recovered = keys.Signature(
                vrs=(raw[64] - 27, int.from_bytes(raw[:32], "big"),
                     int.from_bytes(raw[32:64], "big"))
            ).recover_public_key_from_msg_hash(bytes.fromhex(order_digest(order)[2:]))
        except BaseException:
            raise OperationalSafetyError("signed order validation failed") from None
        return recovered.to_canonical_address().hex() == self.owner[2:]

    def dispatch(self, intent: OrderIntent, signature: str) -> str:
        try:
            payload = json.loads(intent.payload)
            if intent.kind == CANCEL_ALL:
                operation = payload["cancel_product_orders"]
                if set(operation) != {"tx"}:
                    raise ValueError
                operation["signature"] = signature
            elif intent.kind in {ENTRY, CLOSE}:
                operation = payload["place_order"]
                if set(operation) != {"product_id", "order"}:
                    raise ValueError
                operation.update({
                    "signature": signature,
                    "digest": intent.digest,
                    "id": int.from_bytes(bytes.fromhex(intent.digest[2:10]), "big"),
                })
            else:
                raise ValueError
        except BaseException:
            raise OperationalSafetyError("write request binding rejected") from None
        expected = (
            "execute_cancel_product_orders" if intent.kind == CANCEL_ALL
            else "execute_place_order"
        )
        try:
            response = self._post(_GATEWAY_HOST, "/v1/execute", payload)
        except BaseException:
            raise ExecuteFailure(EXECUTE_TRANSPORT_AMBIGUITY) from None
        if (
            type(response) is dict
            and response.get("status") == "failure"
            and response.get("request_type") == expected
            and type(response.get("error_code")) is int
            and response["error_code"] >= 0
            and type(response.get("error")) is str
            and bool(response["error"])
        ):
            raise ExecuteFailure(
                EXECUTE_VENUE_REJECTION, response["error_code"]
            )
        if (
            type(response) is not dict or response.get("status") != "success"
            or response.get("request_type") != expected or type(response.get("data")) is not dict
        ):
            raise ExecuteFailure(EXECUTE_RESPONSE_AMBIGUITY)
        if intent.kind == CANCEL_ALL:
            cancelled = response["data"].get("cancelled_orders")
            if (
                type(cancelled) is not list or len(cancelled) != 1
                or len(self._resting_orders) != 1 or type(cancelled[0]) is not dict
            ):
                raise ExecuteFailure(EXECUTE_RESPONSE_AMBIGUITY)
            expected_entry = next(iter(self._resting_orders.values()))
            cancelled_order = cancelled[0]
            cancelled_digest = cancelled_order.get("digest")
            try:
                cancelled_nonce = self._integer(
                    cancelled_order.get("nonce"), "cancelled order nonce"
                )
                cancelled_remaining = self._integer(
                    cancelled_order.get("unfilled_amount"),
                    "cancelled order remaining amount",
                )
            except OperationalSafetyError:
                raise ExecuteFailure(EXECUTE_RESPONSE_AMBIGUITY) from None
            if (
                type(cancelled_digest) is not str
                or cancelled_digest.lower() != expected_entry.digest.lower()
                or cancelled_order.get("product_id") != self._target_product_id
                or type(cancelled_order.get("sender")) is not str
                or cancelled_order["sender"].lower() != self.sender
                or cancelled_nonce != expected_entry.nonce
                or cancelled_remaining != expected_entry.amount_x18
            ):
                raise ExecuteFailure(EXECUTE_RESPONSE_AMBIGUITY)
            self._cancelled_entry = cancelled_digest.lower()
            self._terminal[intent.digest.lower()] = "CANCELLED"
            self._terminal[cancelled_digest.lower()] = "CANCELLED"
        else:
            returned = response["data"].get("digest")
            if type(returned) is not str or returned.lower() != intent.digest.lower():
                raise ExecuteFailure(EXECUTE_RESPONSE_AMBIGUITY)
            if intent.kind == CLOSE:
                self._terminal[intent.digest.lower()] = "CANCELLED"
        return intent.digest

    @staticmethod
    def _integer(value: object, label: str, *, positive: bool = False) -> int:
        if type(value) is not str or not value or not value.lstrip("-").isdigit():
            raise OperationalSafetyError(f"{label} schema mismatch")
        parsed = int(value)
        if str(parsed) != value or (positive and parsed <= 0):
            raise OperationalSafetyError(f"{label} schema mismatch")
        return parsed

    @staticmethod
    def _post_response_limit(
        host: str, path: str, body: dict[str, object],
    ) -> int:
        if host == _GATEWAY_HOST and path == "/v1/query":
            request_type = body.get("type")
            if request_type == "all_products":
                return ALL_PRODUCTS_MAX_RESPONSE_BYTES
            if request_type == "subaccount_info":
                return SUBACCOUNT_INFO_MAX_RESPONSE_BYTES
        return MAX_RESPONSE_BYTES

    def _post(self, host: str, path: str, body: dict[str, object]) -> object:
        encoded = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("ascii")
        return self._request(
            "POST", host, path, encoded,
            response_limit=self._post_response_limit(host, path, body),
        )

    def _get(self, host: str, path: str) -> object:
        return self._request("GET", host, path, None)

    def _request(
        self, method: str, host: str, path: str, body: bytes | None,
        *, response_limit: int = MAX_RESPONSE_BYTES,
    ) -> object:
        try:
            connection = self._connection_factory(host)
            try:
                connection.request(method, path, body, {
                    "Content-Type": "application/json", "Accept": "application/json",
                    "Accept-Encoding": "gzip, br, deflate",
                })
                response = connection.getresponse()
                declared = response.getheader("Content-Length")
                if declared is not None and (
                    not declared.isdigit() or int(declared) > response_limit
                ):
                    raise OperationalSafetyError("transport response schema rejected")
                raw = response.read(response_limit + 1)
                if len(raw) > response_limit:
                    raise OperationalSafetyError("transport response size exceeded")
                if not 200 <= response.status < 300:
                    raise OperationalSafetyError("HTTP status rejected")
                content_encoding = response.getheader("Content-Encoding")
            finally:
                connection.close()
            decoded = self._decode_response(raw, content_encoding, response_limit)
            try:
                return json.loads(decoded.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                raise OperationalSafetyError("response schema rejected") from None
        except OperationalSafetyError:
            raise
        except BaseException:
            raise OperationalSafetyError("transport outcome requires manual recovery") from None

    @staticmethod
    def _decode_response(
        raw: bytes, content_encoding: str | None,
        response_limit: int = MAX_RESPONSE_BYTES,
    ) -> bytes:
        encoding = "identity" if content_encoding is None else content_encoding.strip().lower()
        if encoding in {"", "identity"}:
            decoded = raw
        elif encoding in {"gzip", "deflate"}:
            window = zlib.MAX_WBITS | (16 if encoding == "gzip" else 0)
            decoder = zlib.decompressobj(window)
            decoded = decoder.decompress(raw, response_limit + 1)
            if (
                len(decoded) > response_limit or not decoder.eof
                or decoder.unconsumed_tail or decoder.unused_data
            ):
                raise OperationalSafetyError("transport content encoding rejected")
        elif encoding == "br":
            try:
                decoder = brotli.Decompressor()
                parts: list[bytes] = []
                total = 0
                for offset in range(0, len(raw), 1024):
                    part = decoder.process(raw[offset:offset + 1024])
                    total += len(part)
                    if total > response_limit:
                        raise ValueError
                    parts.append(part)
                if not decoder.is_finished():
                    raise ValueError
                decoded = b"".join(parts)
            except BaseException:
                raise OperationalSafetyError("transport content encoding rejected") from None
        else:
            raise OperationalSafetyError("transport content encoding rejected")
        if len(decoded) > response_limit:
            raise OperationalSafetyError("transport response size exceeded")
        return decoded

    def _gateway(self, request: dict[str, object], request_type: str) -> object:
        envelope = self._post(_GATEWAY_HOST, "/v1/query", request)
        if (
            type(envelope) is not dict
            or set(envelope) != {"status", "data", "request_type"}
            or envelope["status"] != "success" or envelope["request_type"] != request_type
        ):
            raise OperationalSafetyError("gateway response schema mismatch")
        return envelope["data"]

    def _pairs(self, raw: object) -> dict[int, str]:
        if type(raw) is not list or not raw:
            raise OperationalSafetyError("V2 pair identity schema mismatch")
        result: dict[int, str] = {}
        for pair in raw:
            if type(pair) is not dict or not {
                "product_id", "ticker_id", "base", "quote",
            } <= set(pair):
                raise OperationalSafetyError("V2 pair identity schema mismatch")
            product_id = pair["product_id"]
            ticker, base, quote = pair["ticker_id"], pair["base"], pair["quote"]
            if (
                type(product_id) is not int or product_id < 0
                or type(ticker) is not str or not ticker
                or type(base) is not str or not base
                or type(quote) is not str or not quote
                or ticker != f"{base}_{quote}"
                or product_id in result
            ):
                raise OperationalSafetyError("V2 pair identity schema mismatch")
            result[product_id] = ticker
        return result

    def _products(self, raw: object, pairs: dict[int, str]) -> dict[int, Product]:
        if type(raw) is not dict or set(raw) != {"spot_products", "perp_products"}:
            raise OperationalSafetyError("catalog schema mismatch")
        result: dict[int, Product] = {}
        self._funding_states = {}
        catalog_ids: set[int] = set()
        for kind, field in (("SPOT", "spot_products"), (ACTIVE_PERP, "perp_products")):
            if type(raw[field]) is not list:
                raise OperationalSafetyError("catalog schema mismatch")
            for item in raw[field]:
                if type(item) is not dict or type(item.get("product_id")) is not int:
                    raise OperationalSafetyError("catalog product schema mismatch")
                product_id = item["product_id"]
                if product_id in catalog_ids:
                    raise OperationalSafetyError("duplicate catalog product")
                catalog_ids.add(product_id)
                if product_id in {0, 11}:
                    continue
                if kind == ACTIVE_PERP and "state" in item:
                    state = item["state"]
                    if type(state) is not dict:
                        raise OperationalSafetyError("funding state schema mismatch")
                    required_state = {
                        "cumulative_funding_long_x18",
                        "cumulative_funding_short_x18",
                        "open_interest",
                    }
                    if not required_state <= set(state):
                        raise OperationalSafetyError("funding state schema mismatch")
                    self._funding_states[product_id] = (
                        self._integer(
                            state["cumulative_funding_long_x18"],
                            "cumulative funding long",
                        ),
                        self._integer(
                            state["cumulative_funding_short_x18"],
                            "cumulative funding short",
                        ),
                        self._integer(state["open_interest"], "funding open interest"),
                    )
                symbol = pairs.get(product_id)
                if symbol is None:
                    raise OperationalSafetyError("catalog V2 identity coverage mismatch")
                book = item.get("book_info")
                if type(book) is not dict:
                    raise OperationalSafetyError("catalog grid schema mismatch")
                tick = self._integer(book.get("price_increment_x18"), "price tick", positive=True)
                step = self._integer(book.get("size_increment"), "amount step", positive=True)
                minimum_notional = self._integer(
                    book.get("min_size"), "minimum notional", positive=True
                )
                if product_id in result:
                    raise OperationalSafetyError("duplicate catalog product")
                result[product_id] = Product(
                    product_id, symbol, kind, True, tick, step,
                    step, minimum_notional,
                )
        if set(pairs) != catalog_ids - {0}:
            raise OperationalSafetyError("catalog V2 identity coverage mismatch")
        return result

    def _orders(self, raw: object, products: dict[int, Product]):
        if type(raw) is not dict or set(raw) != {"sender", "product_orders"}:
            raise OperationalSafetyError("orders schema mismatch")
        if raw["sender"] != self.sender or type(raw["product_orders"]) is not list:
            raise OperationalSafetyError("orders identity mismatch")
        regular: dict[int, tuple[str, ...]] = {}
        evidence: list[OrderEvidence] = []
        for group in raw["product_orders"]:
            if type(group) is not dict or set(group) != {"product_id", "orders"}:
                raise OperationalSafetyError("orders schema mismatch")
            product_id, orders = group["product_id"], group["orders"]
            if product_id not in products or product_id in regular or type(orders) is not list:
                raise OperationalSafetyError("orders coverage mismatch")
            digests: list[str] = []
            for order in orders:
                if type(order) is not dict:
                    raise OperationalSafetyError("order schema mismatch")
                digest = order.get("digest")
                if type(digest) is not str or order.get("sender") != self.sender:
                    raise OperationalSafetyError("order identity mismatch")
                digests.append(digest)
                evidence.append(OrderEvidence(
                    digest, product_id, self._integer(order.get("nonce"), "order nonce"),
                    self._integer(order.get("unfilled_amount"), "unfilled amount"), "OPEN",
                ))
            regular[product_id] = tuple(digests)
        if set(regular) != set(products):
            raise OperationalSafetyError("orders coverage mismatch")
        return regular, evidence

    def _positions(self, raw: object, products: dict[int, Product]) -> dict[int, int]:
        if type(raw) is not dict or raw.get("subaccount") != self.sender:
            raise OperationalSafetyError("account identity mismatch")
        spots, perps = raw.get("spot_balances"), raw.get("perp_balances")
        if type(spots) is not list or type(perps) is not list:
            raise OperationalSafetyError("account balance schema mismatch")
        for item in spots:
            if type(item) is not dict or type(item.get("balance")) is not dict:
                raise OperationalSafetyError("spot balance schema mismatch")
            amount = self._integer(item["balance"].get("amount"), "spot amount")
            if item.get("product_id") != 0 and amount:
                raise OperationalSafetyError("unrelated spot exposure")
        result = {pid: 0 for pid, product in products.items() if product.product_type == ACTIVE_PERP}
        self._v_quote_balances = {pid: 0 for pid in result}
        self._perp_last_cumulative_funding_x18 = {}
        for item in perps:
            if type(item) is not dict or type(item.get("balance")) is not dict:
                raise OperationalSafetyError("perp balance schema mismatch")
            product_id = item.get("product_id")
            amount = self._integer(item["balance"].get("amount"), "perp amount")
            quote = self._integer(item["balance"].get("v_quote_balance"), "v_quote")
            cumulative = self._integer(
                item["balance"].get("last_cumulative_funding_x18"),
                "last cumulative funding",
            )
            if product_id in result:
                result[product_id] = amount
                self._v_quote_balances[product_id] = quote
                self._perp_last_cumulative_funding_x18[product_id] = cumulative
            elif amount or quote:
                raise OperationalSafetyError("unrelated perpetual exposure")
            if amount == 0 and quote != 0:
                raise OperationalSafetyError("flat position has nonzero v_quote")
        if any(
            amount for pid, amount in result.items()
            if pid != self._target_product_id
        ):
            raise OperationalSafetyError("unrelated perpetual exposure")
        return result

    def _triggers(self) -> tuple[str, ...]:
        time_payload = self._post(
            _GATEWAY_HOST, "/v1/edge/query", {"type": "time"},
        )
        observed_at_ms = self.now_ms()
        try:
            server_ms = _server_time_observation(
                ObservedResponse(
                    url=FixedPreflightIdentity.gateway_edge_query,
                    final_url=FixedPreflightIdentity.gateway_edge_query,
                    http_status=200,
                    observed_at_ms=observed_at_ms,
                    payload=time_payload,
                ),
                self.now_ms,
            )
        except NadoPreflightError:
            raise OperationalSafetyError("server time response rejected") from None
        capability = _load_owner_capability(self.sender)
        try:
            recv = str(server_ms + MAX_FRESHNESS_MS)
            typed = list_trigger_orders_typed_data(self.sender, recv)
            signature = capability.sign_list_trigger_orders(typed)
            if _recover_owner(typed, signature) != self.owner:
                raise OperationalSafetyError("trigger signature identity mismatch")
        finally:
            capability.close()
        envelope = self._post(_TRIGGER_HOST, "/v1/query", {
            "type": "list_trigger_orders", "tx": {"sender": self.sender, "recvTime": recv},
            "signature": signature, "limit": 500,
        })
        if (
            type(envelope) is not dict or envelope.get("status") != "success"
            or envelope.get("request_type") != "query_list_trigger_orders"
            or type(envelope.get("data")) is not dict
            or type(envelope["data"].get("orders")) is not list
        ):
            raise OperationalSafetyError("trigger response schema mismatch")
        orders = envelope["data"]["orders"]
        if len(orders) == 500:
            raise OperationalSafetyError("trigger history is not bounded")
        active: list[str] = []
        for item in orders:
            try:
                status = item["status"]
                digest = item["order"]["digest"]
            except (KeyError, TypeError):
                raise OperationalSafetyError("trigger order schema mismatch") from None
            if status not in {"cancelled", "triggered", "internal_error", "twap_completed"}:
                active.append(digest)
        return tuple(active)

    def _fills(self, digests: tuple[str, ...], products: dict[int, Product]):
        from .nado_testnet_lifecycle import FillEvidence
        if not digests:
            return []
        raw = self._post(_ARCHIVE_HOST, "/v1", {
            "matches": {"subaccounts": [self.sender], "limit": 500, "isolated": False},
        })
        if type(raw) is not dict or type(raw.get("matches")) is not list:
            raise OperationalSafetyError("archive matches schema mismatch")
        if len(raw["matches"]) == 500:
            raise OperationalSafetyError("archive matches are not bounded")
        wanted = {digest.lower() for digest in digests}
        result: list[FillEvidence] = []
        for match in raw["matches"]:
            if type(match) is not dict or str(match.get("digest", "")).lower() not in wanted:
                continue
            try:
                base = match["pre_balance"]["base"]["perp"]
                product_id = base["product_id"]
            except (KeyError, TypeError):
                raise OperationalSafetyError("archive fill product mismatch") from None
            if product_id not in products:
                raise OperationalSafetyError("archive fill product mismatch")
            result.append(FillEvidence(
                match["digest"], product_id,
                self._integer(match.get("base_filled"), "fill amount"),
                self._integer(match.get("submission_idx"), "submission index"),
            ))
        return result


def main() -> None:
    try:
        report = run()
    except DurableExecuteFailure as error:
        report = {
            "schema_version": 1, "status": "BLOCKED", "path": REDACTED_STORE_PATH,
            "reason": error.failure_class, "venue_code": error.venue_code,
        }
    except DurableOperationalFailure as error:
        report = {
            "schema_version": 1, "status": "BLOCKED", "path": REDACTED_STORE_PATH,
            "reason": error.failure_class, "stage": error.stage,
        }
    except BaseException as error:
        report = {
            "schema_version": 1, "status": "BLOCKED", "path": REDACTED_STORE_PATH,
            "reason": _failure_class(error), "stage": "OUTER",
        }
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
