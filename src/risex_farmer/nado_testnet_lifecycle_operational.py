"""Sealed zero-argument Nado Ink Sepolia Level-C lifecycle runner.

The normal Farmer never imports this module.  Production construction fixes the
account files, journal, environment, product and transports; only the private
``_fixture_run`` seam accepts injected observations and never opens credentials
or sockets.
"""

from __future__ import annotations

import asyncio
import brotli
from dataclasses import dataclass
import hashlib
import http.client
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
    MAX_FRESHNESS_MS, FixedPreflightIdentity, NadoPreflightError,
    ObservedResponse, _server_time_observation, list_trigger_orders_typed_data,
)
from .nado_testnet_lifecycle import (
    ACTIVE_PERP, CANCEL_ALL, CLOSE, COMPLETE, ENTRY,
    MAX_CLOSE_ATTEMPTS, POST_ONLY_APPENDIX, UINT32_MAX,
    AccountSnapshot, CatalogSnapshot, EngineEvidence, IntentStore,
    LifecycleCore, OrderEvidence, OrderIntent, Product, Reconciliation,
    SyntheticOrderVector, TriggerSnapshot, build_order_nonce,
    completion_barrier, order_digest, smallest_executable_amount,
    validate_entry_preflight,
)


RUN_STORE_BASENAME = ".risex-funding-farmer-nado-level-c-v1.sqlite3"
REDACTED_STORE_PATH = "<passwd-home>/" + RUN_STORE_BASENAME
TARGET_PRODUCT_ID = 44
TARGET_TICKER_ID = "SKR-PERP_USDT0"
RECV_WINDOW_MS = 100
HTTP_TIMEOUT_SECONDS = 5.0
MAX_RESPONSE_BYTES = 1_048_576
RECONCILE_READ_ATTEMPTS = 5
RECONCILE_READ_INTERVAL_SECONDS = 1.0
_GATEWAY_HOST = "gateway.test.nado.xyz"
_ARCHIVE_HOST = "archive.test.nado.xyz"
_TRIGGER_HOST = "trigger.test.nado.xyz"


class OperationalSafetyError(RuntimeError):
    """Sanitized terminal operational failure."""


@dataclass(frozen=True)
class LiveObservation:
    catalog: CatalogSnapshot
    evidence: EngineEvidence
    product: Product
    bid_x18: int
    ask_x18: int


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


class VenueIO(Protocol):
    def now_ms(self) -> int: ...
    def observe(self, digests: tuple[str, ...]) -> LiveObservation: ...
    def validate_order(self, order: SyntheticOrderVector, signature: str) -> bool: ...
    def dispatch(self, intent: OrderIntent, signature: str) -> str: ...


class RuntimeRunJournal:
    """Append-only runtime identity in the same protected operational DB."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def begin(self, created_at_ms: int) -> str:
        if type(created_at_ms) is not int or created_at_ms <= 0:
            raise OperationalSafetyError("runtime journal rejected")
        _prepare_file(self.path)
        run_id = str(uuid.uuid4())
        connection = sqlite3.connect(self.path)
        try:
            connection.execute("PRAGMA synchronous=FULL")
            with connection:
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS nado_runtime_runs ("
                    "run_id TEXT PRIMARY KEY, created_at_ms INTEGER NOT NULL, "
                    "state TEXT NOT NULL)"
                )
                connection.execute(
                    "INSERT INTO nado_runtime_runs VALUES (?, ?, 'STARTED')",
                    (run_id, created_at_ms),
                )
        except sqlite3.DatabaseError:
            raise OperationalSafetyError("runtime journal rejected") from None
        finally:
            connection.close()
        _fsync(self.path)
        return run_id


def _home() -> Path:
    return Path(pwd.getpwuid(os.getuid()).pw_dir)


def _production_store_path() -> Path:
    return _home() / RUN_STORE_BASENAME


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


def _entry_order(observation: LiveObservation, owner: str, sender: str, recv: int) -> SyntheticOrderVector:
    product = observation.product
    if (
        product.product_id != TARGET_PRODUCT_ID
        or product.symbol != TARGET_TICKER_ID
        or product.product_type != ACTIVE_PERP
    ):
        raise OperationalSafetyError("fixed target product identity unavailable")
    bid, ask = observation.bid_x18, observation.ask_x18
    if bid <= 0 or ask <= bid or bid % product.tick_x18 or ask % product.tick_x18:
        raise OperationalSafetyError("fresh non-crossed tick-aligned BBO required")
    salt = _salt()
    amount = smallest_executable_amount(product, prices_x18=(bid, ask))
    return SyntheticOrderVector(
        owner, SUBACCOUNT_NAME, sender, TARGET_PRODUCT_ID, bid,
        amount, UINT32_MAX, recv, salt,
        build_order_nonce(recv, salt), POST_ONLY_APPENDIX,
    )


def _terminal_flags(observation: LiveObservation) -> tuple[bool, bool, bool]:
    account = observation.evidence.account
    regular = not any(account.regular_orders_by_product.values())
    trigger = not observation.evidence.triggers.active_digests
    flat = not any(account.cross_perp_amounts_x18.values()) and not account.isolated_positions
    return regular, trigger, flat


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
        self.run_id = journal.begin(io.now_ms())
        self.writes = 0

    def _dispatch(self, intent: OrderIntent) -> None:
        capability = self.capability_loader(self.owner)
        try:
            signature = capability.sign(intent)
            returned = self.store.dispatch_prepared(
                intent.digest, lambda durable: self.io.dispatch(durable, signature)
            )
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
        if self.store.intents() or self.store.lifecycle_status() != "RUNNING":
            raise OperationalSafetyError("existing lifecycle requires manual recovery")
        initial = self._observe()
        issued_at = self.io.now_ms()
        recv = issued_at + RECV_WINDOW_MS
        order = _entry_order(initial, self.owner, self.sender, recv)
        validate_entry_preflight(
            catalog=initial.catalog, account=initial.evidence.account,
            triggers=initial.evidence.triggers, product_id=TARGET_PRODUCT_ID,
            entry_price_x18=order.price_x18,
            worst_close_price_x18=initial.ask_x18, now_ms=issued_at,
        )
        capability = self.capability_loader(self.owner)
        try:
            signature = capability.sign(OrderIntent(
                ENTRY, order.product_id, order.nonce, order.recv_time,
                order_digest(order), json.dumps(order.as_payload(), sort_keys=True,
                separators=(",", ":")).encode("ascii"), order.amount_x18,
                order.appendix, sender=order.sender, owner=order.owner,
                subaccount_name=order.subaccount_name,
            ))
            valid = self.io.validate_order(order, signature)
        finally:
            capability.close()
        entry = self.core.prepare_entry(
            order=order, catalog=initial.catalog, account=initial.evidence.account,
            triggers=initial.evidence.triggers,
            worst_close_price_x18=initial.ask_x18, signature=signature,
            validation_product_id=order.product_id, validation_valid=valid,
            now_ms=issued_at,
        )
        self._dispatch(entry)
        outcome, observed = self._reconcile(entry)
        if outcome is Reconciliation.RESTING or outcome is Reconciliation.PARTIAL:
            issued_at = self.io.now_ms()
            recv = issued_at + RECV_WINDOW_MS
            cancel = self.core.prepare_cancel_all(
                catalog=observed.catalog, account=observed.evidence.account,
                triggers=observed.evidence.triggers, sender=self.sender,
                recv_time=recv, salt=_salt(), now_ms=issued_at,
            )
            self._dispatch(cancel)
            outcome, observed = self._reconcile(cancel)
            if outcome is not Reconciliation.CANCELLED:
                raise OperationalSafetyError("exact entry cancellation unresolved")
            outcome, observed = self._reconcile(entry)
        if outcome not in {Reconciliation.FILLED, Reconciliation.CANCELLED, Reconciliation.EXPIRED}:
            raise OperationalSafetyError("entry outcome requires manual recovery")
        while any(observed.evidence.account.cross_perp_amounts_x18.values()):
            if self.store.count_kind(CLOSE) >= MAX_CLOSE_ATTEMPTS:
                self.store.halt()
                raise OperationalSafetyError("three close attempts exhausted")
            issued_at = self.io.now_ms()
            recv = issued_at + RECV_WINDOW_MS
            close = self.core.prepare_close(
                catalog=observed.catalog, product=observed.product,
                account=observed.evidence.account, triggers=observed.evidence.triggers,
                worst_price_x18=observed.bid_x18, recv_time=recv, salt=_salt(),
                now_ms=issued_at,
            )
            self._dispatch(close)
            close_outcome, observed = self._reconcile(close)
            if close_outcome in {Reconciliation.PARTIAL, Reconciliation.REJECTED}:
                raise OperationalSafetyError("close requires manual recovery")
            if close_outcome not in {
                Reconciliation.FILLED, Reconciliation.CANCELLED, Reconciliation.EXPIRED,
            }:
                raise OperationalSafetyError("close outcome unresolved")
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
        return SealedLifecycleRunner(
            store=store, journal=RuntimeRunJournal(path), io=io,
            capability_loader=capability_loader, owner=owner, sender=sender,
        ).run()
    finally:
        store.close()


def run() -> dict[str, object]:
    """Run the sealed production operation; accepts no runtime parameters."""
    owner, sender = _strict_identity()
    path = _production_store_path()
    _prepare_file(path)
    store = IntentStore(path)
    try:
        # The network observer is deliberately constructed here so importing
        # this module remains inert and normal startup cannot reach Level C.
        io = OperationalVenueIO(owner, sender)
        runner = SealedLifecycleRunner(
            store=store, journal=RuntimeRunJournal(path), io=io,
            capability_loader=_load_capability, owner=owner, sender=sender,
        )
        preflight = asyncio.run(_accepted_private_read())
        if preflight.get("status") != "FINALIZED":
            raise OperationalSafetyError("accepted private-read barrier failed")
        report = runner.run()
        return report.sanitized()
    finally:
        store.close()


class OperationalVenueIO:
    """Fixed-host production surface. Response semantics fail closed.

    The first accepted operational invocation supplies the venue observations;
    this class intentionally has no configurable URL/account/market surface.
    """

    def __init__(self, owner: str, sender: str) -> None:
        self.owner, self.sender = owner.lower(), sender.lower()
        self._terminal: dict[str, str] = {}
        self._cancelled_entry: str | None = None
        self._resting_orders: dict[str, OrderEvidence] = {}
        self._connection_factory = lambda host: http.client.HTTPSConnection(
            host, timeout=HTTP_TIMEOUT_SECONDS, context=ssl.create_default_context(),
        )

    def now_ms(self) -> int:
        return time.time_ns() // 1_000_000

    def terminal_status(self, digest: str) -> str | None:
        return self._terminal.get(digest.lower())

    def observe(self, digests: tuple[str, ...]) -> LiveObservation:
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
        target = products.get(TARGET_PRODUCT_ID)
        if (
            target is None or target.product_type != ACTIVE_PERP
            or target.symbol != TARGET_TICKER_ID
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
            {"type": "market_price", "product_id": TARGET_PRODUCT_ID},
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
        response = self._post(_GATEWAY_HOST, "/v1/execute", payload)
        expected = (
            "execute_cancel_product_orders" if intent.kind == CANCEL_ALL
            else "execute_place_order"
        )
        if (
            type(response) is not dict or response.get("status") != "success"
            or response.get("request_type") != expected or type(response.get("data")) is not dict
        ):
            raise OperationalSafetyError("write outcome requires reconciliation")
        if intent.kind == CANCEL_ALL:
            cancelled = response["data"].get("cancelled_orders")
            if (
                type(cancelled) is not list or len(cancelled) != 1
                or len(self._resting_orders) != 1 or type(cancelled[0]) is not dict
            ):
                raise OperationalSafetyError("exact cancellation response mismatch")
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
                raise OperationalSafetyError("exact cancellation response mismatch") from None
            if (
                type(cancelled_digest) is not str
                or cancelled_digest.lower() != expected_entry.digest.lower()
                or cancelled_order.get("product_id") != TARGET_PRODUCT_ID
                or type(cancelled_order.get("sender")) is not str
                or cancelled_order["sender"].lower() != self.sender
                or cancelled_nonce != expected_entry.nonce
                or cancelled_remaining != expected_entry.amount_x18
            ):
                raise OperationalSafetyError("exact cancellation response mismatch")
            self._cancelled_entry = cancelled_digest.lower()
            self._terminal[intent.digest.lower()] = "CANCELLED"
            self._terminal[cancelled_digest.lower()] = "CANCELLED"
        else:
            returned = response["data"].get("digest")
            if type(returned) is not str or returned.lower() != intent.digest.lower():
                raise OperationalSafetyError("write response identity mismatch")
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

    def _post(self, host: str, path: str, body: dict[str, object]) -> object:
        encoded = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("ascii")
        return self._request("POST", host, path, encoded)

    def _get(self, host: str, path: str) -> object:
        return self._request("GET", host, path, None)

    def _request(
        self, method: str, host: str, path: str, body: bytes | None
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
                    not declared.isdigit() or int(declared) > MAX_RESPONSE_BYTES
                ):
                    raise ValueError
                raw = response.read(MAX_RESPONSE_BYTES + 1)
                if not 200 <= response.status < 300 or len(raw) > MAX_RESPONSE_BYTES:
                    raise ValueError
                content_encoding = response.getheader("Content-Encoding")
            finally:
                connection.close()
            decoded = self._decode_response(raw, content_encoding)
            return json.loads(decoded.decode("utf-8"))
        except OperationalSafetyError:
            raise
        except BaseException:
            raise OperationalSafetyError("transport outcome requires manual recovery") from None

    @staticmethod
    def _decode_response(raw: bytes, content_encoding: str | None) -> bytes:
        encoding = "identity" if content_encoding is None else content_encoding.strip().lower()
        if encoding in {"", "identity"}:
            decoded = raw
        elif encoding in {"gzip", "deflate"}:
            window = zlib.MAX_WBITS | (16 if encoding == "gzip" else 0)
            decoder = zlib.decompressobj(window)
            decoded = decoder.decompress(raw, MAX_RESPONSE_BYTES + 1)
            if (
                len(decoded) > MAX_RESPONSE_BYTES or not decoder.eof
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
                    if total > MAX_RESPONSE_BYTES:
                        raise ValueError
                    parts.append(part)
                if not decoder.is_finished():
                    raise ValueError
                decoded = b"".join(parts)
            except BaseException:
                raise OperationalSafetyError("transport content encoding rejected") from None
        else:
            raise OperationalSafetyError("transport content encoding rejected")
        if len(decoded) > MAX_RESPONSE_BYTES:
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
                symbol = pairs.get(product_id)
                if symbol is None:
                    raise OperationalSafetyError("catalog V2 identity coverage mismatch")
                book = item.get("book_info")
                if type(book) is not dict:
                    raise OperationalSafetyError("catalog grid schema mismatch")
                tick = self._integer(book.get("price_increment_x18"), "price tick", positive=True)
                step = self._integer(book.get("size_increment"), "amount step", positive=True)
                minimum = self._integer(book.get("min_size"), "minimum amount", positive=True)
                if product_id in result:
                    raise OperationalSafetyError("duplicate catalog product")
                result[product_id] = Product(
                    product_id, symbol, kind, True, tick, step,
                    minimum, 5 * 10**18,
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
        for item in perps:
            if type(item) is not dict or type(item.get("balance")) is not dict:
                raise OperationalSafetyError("perp balance schema mismatch")
            product_id = item.get("product_id")
            amount = self._integer(item["balance"].get("amount"), "perp amount")
            quote = self._integer(item["balance"].get("v_quote_balance"), "v_quote")
            if product_id in result:
                result[product_id] = amount
            elif amount or quote:
                raise OperationalSafetyError("unrelated perpetual exposure")
            if amount == 0 and quote != 0:
                raise OperationalSafetyError("flat position has nonzero v_quote")
        if any(amount for pid, amount in result.items() if pid != TARGET_PRODUCT_ID):
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
    except BaseException:
        report = {
            "schema_version": 1, "status": "BLOCKED", "path": REDACTED_STORE_PATH,
            "reason": "OPERATIONAL_PREREQUISITE_FAILED",
        }
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
