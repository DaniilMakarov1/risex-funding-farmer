"""Thin offline S3 complete-cycle driver, evidence path, and report.

This module is intentionally narrower than the historical public observer.  It
feeds the accepted :class:`~risex_spread_shadow.cycle.CycleKernel` in the
exact producer order supplied by a fixture or a future bounded collector.  It
does not sort ingress by timestamp, open a network connection, or expose any
private or write-capable surface.

The runtime evidence is a normal append-only JSONL store.  The small typed
codec below is only for the accepted public models used by the S2 kernel; it
is not a generic serializer or persistence framework.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import Enum
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import time
from typing import Any

import aiohttp

from risex_farmer.exchanges.lighter import LighterAdapter
from risex_farmer.exchanges.risex import RisexAdapter

from risex_farmer.models import (
    BookLevel,
    CanonicalMarket,
    ContractType,
    ExactVwap,
    LiquidityRole,
    MarketType,
    Side,
    Venue,
)

from .causal import (
    CausalEvent,
    CausalEventKind,
    CausalSourceIdentity,
)
from .cycle import (
    CycleAdmission,
    CycleAttempt,
    CycleClock,
    CycleKernel,
    CycleKernelState,
    CyclePolicy,
    CycleResult,
    CycleScenario,
    CycleTerminalState,
    s2_cycle_policy,
)
from .config import ShadowConfig
from .economics import build_hypothetical_maker_quote
from .feed import (
    FeedBookEvent,
    FeedGapEvent,
    FeedTradeEvent,
    IngressItem,
    IngressQueue,
    MarketPair,
    PublicFeedRunner,
    select_public_market_pairs,
)
from .models import (
    BookEvidence,
    DataGapEvidence,
    EntryViabilityOutcome,
    FeeEvidence,
    HedgeHorizonCapture,
    HypotheticalMakerQuote,
    QuotePolicy,
    QuoteVersion,
    SizingEvidence,
    SpreadDirection,
    TradeEvidence,
)
from .scanner import ScannerPreconditionError, validate_loaded_release
from .store import (
    AppendOnlyEvidenceStore,
    TERMINAL_FAILURE_BYTES_RESERVE,
    TERMINAL_FAILURE_RECORD_RESERVE,
    iter_records,
    new_run_id,
)


S3_SCHEMA_VERSION = 1
S3_EXPERIMENT_KIND = "S3_COMPLETE_CYCLE"
S3_WINDOW_SECONDS = 45 * 60
S3_ENTRY_CUTOFF_SECONDS = 42 * 60 + 45
S3_MARKET_DEADLINE_SECONDS = 45 * 60
S3_CLOSING_TAIL_SECONDS = 135
S3_MAX_RECORDS = 1_000_000
S3_RECORD_RESERVE = 100_000
S3_MAX_BYTES = 4 * 1024 * 1024 * 1024
S3_BYTES_RESERVE = 512 * 1024 * 1024
S3_KERNEL_RETENTION_CAPACITY = 256
S3_REQUIRED_COMPLETE_CYCLES = 20
S3_REQUIRED_FILLED_GROUPS = 20
S3_REQUIRED_WINDOWS_WITH_CYCLES = 3
S3_REQUIRED_CYCLES_PER_QUALIFYING_WINDOW = 5

_SHA256_RE = re.compile(r"^[0-9a-f]{40}$")
_HEX256_RE = re.compile(r"^[0-9a-f]{64}$")
_ZERO = Decimal("0")
_RESOURCE_LIMIT_REASONS = frozenset(
    {"S3_ENVELOPE_LIMIT_RECORDS", "S3_ENVELOPE_LIMIT_BYTES"}
)
_RESOURCE_INCOMPLETE_MARKERS = frozenset(
    {
        "CYCLE_FINALIZATION_PREFIX",
        "FINAL_RESULT_PREFIX",
        "STREAM_FINALIZATION_PREFIX",
    }
)
_UNAVAILABLE_RESULT_METRICS = (
    "total_pnl_usd",
    "mean_pnl_usd",
    "gross_profit_usd",
    "gross_loss_usd",
    "worst_cycle",
    "turnover_usd",
    "holding_duration_seconds",
    "occupancy_holding_duration_seconds",
    "unmatched_exposure_duration_seconds",
    "forced_or_unmatched_pnl_usd",
    "forced_unmatched_exit_contribution_usd",
    "total_without_best_dependence_group_usd",
)


class CycleEvidenceError(ValueError):
    """Base class for fail-closed S3 evidence errors."""


class CycleEvidenceIntegrityError(CycleEvidenceError):
    """Raised when physical or semantic evidence integrity is not provable."""


class CycleWindowClaimError(RuntimeError):
    """Raised when a prospective campaign window was already consumed."""


class CycleEnvelopeLimitError(CycleEvidenceError):
    """Raised before an append would consume a reserved closing resource."""

    def __init__(self, resource: str) -> None:
        self.resource = resource
        super().__init__(f"S3 envelope {resource} limit reached before closing reserve")


class CycleManifestError(CycleEvidenceError):
    """Raised when a prospective S3 campaign manifest is not exact."""


class CyclePublicPreconditionError(CycleEvidenceError):
    """Raised before a public request when the S3 gate is not satisfied."""


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _non_negative_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _positive_int(value: Any, name: str) -> int:
    result = _non_negative_int(value, name)
    if result <= 0:
        raise ValueError(f"{name} must be positive")
    return result


def _decimal(value: Any, name: str) -> Decimal:
    if isinstance(value, Decimal):
        result = value
    elif isinstance(value, str):
        result = Decimal(value)
    else:
        raise TypeError(f"{name} must be Decimal or its serialized string")
    if not result.is_finite():
        raise ValueError(f"{name} must be finite")
    return result


def _optional_decimal(value: Any, name: str) -> Decimal | None:
    return None if value is None else _decimal(value, name)


def _utc(value: datetime, name: str = "timestamp") -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    if value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must use UTC")
    return value


def _parse_utc(value: Any, name: str = "timestamp") -> datetime:
    if not isinstance(value, str):
        raise CycleEvidenceIntegrityError(f"{name} is missing or not text")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CycleEvidenceIntegrityError(f"{name} is not valid ISO-8601 UTC") from exc
    return _utc(parsed, name)


def _primitive(value: Any) -> Any:
    """Convert accepted immutable models to JSON-compatible primitives."""

    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("non-finite Decimal cannot be encoded")
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return {field.name: _primitive(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Mapping):
        return {str(key): _primitive(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_primitive(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    return value


def _evidence_primitive(value: Any) -> Any:
    """Use safe wire names for the existing store's secret-key guard."""

    primitive = _primitive(value)
    if isinstance(primitive, dict):
        return {
            ("position_state_proven" if key == "authoritative" else key): _evidence_primitive(item)
            for key, item in primitive.items()
        }
    if isinstance(primitive, list):
        return [_evidence_primitive(item) for item in primitive]
    return primitive


def _digest(value: Any) -> str:
    encoded = json.dumps(_primitive(value), sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _require(mapping: Mapping[str, Any], key: str, *, context: str) -> Any:
    if key not in mapping:
        raise CycleEvidenceIntegrityError(f"{context} is missing {key}")
    return mapping[key]


@dataclass(frozen=True, slots=True)
class CycleEnvelope:
    """The immutable four-window resource and timing envelope."""

    window_seconds: int = S3_WINDOW_SECONDS
    entry_cutoff_seconds: int = S3_ENTRY_CUTOFF_SECONDS
    market_deadline_seconds: int = S3_MARKET_DEADLINE_SECONDS
    closing_tail_seconds: int = S3_CLOSING_TAIL_SECONDS
    max_records: int = S3_MAX_RECORDS
    record_reserve: int = S3_RECORD_RESERVE
    max_bytes: int = S3_MAX_BYTES
    bytes_reserve: int = S3_BYTES_RESERVE
    kernel_retention_capacity: int = S3_KERNEL_RETENTION_CAPACITY

    def __post_init__(self) -> None:
        for value, name in (
            (self.window_seconds, "window_seconds"),
            (self.entry_cutoff_seconds, "entry_cutoff_seconds"),
            (self.market_deadline_seconds, "market_deadline_seconds"),
            (self.closing_tail_seconds, "closing_tail_seconds"),
            (self.max_records, "max_records"),
            (self.record_reserve, "record_reserve"),
            (self.max_bytes, "max_bytes"),
            (self.bytes_reserve, "bytes_reserve"),
            (self.kernel_retention_capacity, "kernel_retention_capacity"),
        ):
            _positive_int(value, name)
        if self.entry_cutoff_seconds >= self.market_deadline_seconds:
            raise ValueError("entry cutoff must precede hard market deadline")
        if self.window_seconds != self.market_deadline_seconds:
            raise ValueError("S3 window and hard market deadline are fixed at 45 minutes")
        if self.market_deadline_seconds - self.entry_cutoff_seconds != self.closing_tail_seconds:
            raise ValueError("closing tail must equal deadline minus entry cutoff")
        if self.record_reserve >= self.max_records:
            raise ValueError("record reserve must leave usable record capacity")
        if self.bytes_reserve >= self.max_bytes:
            raise ValueError("byte reserve must leave usable byte capacity")

    @property
    def entry_cutoff_ns(self) -> int:
        return self.entry_cutoff_seconds * 1_000_000_000

    @property
    def market_deadline_ns(self) -> int:
        return self.market_deadline_seconds * 1_000_000_000

    @property
    def closing_tail_ns(self) -> int:
        return self.closing_tail_seconds * 1_000_000_000

    def worst_configured_tail_ns(self, policy: CyclePolicy | None = None) -> int:
        """Return a conservative configured stress timeline to flatness.

        The sum intentionally keeps every activation, cancellation, taker,
        and holding component explicit.  It does not assume that missing
        books or processing time resolve themselves.
        """

        selected = s2_cycle_policy() if policy is None else policy
        delays = selected.delays(CycleScenario.STRESS)
        return (
            delays.activation_delay_ns
            + selected.entry_cancel_after_activation_ns
            + delays.cancel_delay_ns
            + delays.taker_delay_ns
            + delays.activation_delay_ns
            + selected.max_hold_ns
            + delays.cancel_delay_ns
            + delays.taker_delay_ns
        )

    @property
    def worst_configured_tail_seconds(self) -> Decimal:
        return Decimal(self.worst_configured_tail_ns()) / Decimal(1_000_000_000)

    def assert_tail_sufficient(self, policy: CyclePolicy | None = None) -> None:
        required = self.worst_configured_tail_ns(policy)
        if self.closing_tail_ns < required:
            raise CycleEvidenceIntegrityError(
                f"closing tail {self.closing_tail_seconds}s is shorter than configured worst {Decimal(required) / Decimal(1_000_000_000)}s"
            )


@dataclass(frozen=True, slots=True)
class CycleWindow:
    """One immutable prospective or fixture window."""

    campaign_id: str
    window_id: str
    start_utc: datetime
    end_utc: datetime
    ordinal: int = 0
    monotonic_start_ns: int = 0

    def __post_init__(self) -> None:
        _path_safe(self.campaign_id, "campaign_id")
        _path_safe(self.window_id, "window_id")
        _utc(self.start_utc, "start_utc")
        _utc(self.end_utc, "end_utc")
        _non_negative_int(self.ordinal, "ordinal")
        _non_negative_int(self.monotonic_start_ns, "monotonic_start_ns")
        if self.end_utc - self.start_utc != timedelta(seconds=S3_WINDOW_SECONDS):
            raise ValueError("S3 windows are exactly 45 minutes")

    @classmethod
    def from_text(
        cls,
        *,
        campaign_id: str,
        window_id: str,
        start_utc: str,
        end_utc: str,
        ordinal: int = 0,
        monotonic_start_ns: int = 0,
    ) -> "CycleWindow":
        return cls(
            campaign_id=campaign_id,
            window_id=window_id,
            start_utc=_parse_utc(start_utc, "window_start_utc"),
            end_utc=_parse_utc(end_utc, "window_end_utc"),
            ordinal=ordinal,
            monotonic_start_ns=monotonic_start_ns,
        )

    @property
    def cutoff_utc(self) -> datetime:
        return self.start_utc + timedelta(seconds=S3_ENTRY_CUTOFF_SECONDS)

    @property
    def deadline_utc(self) -> datetime:
        return self.end_utc

    @property
    def cutoff_monotonic_ns(self) -> int:
        return self.monotonic_start_ns + S3_ENTRY_CUTOFF_SECONDS * 1_000_000_000

    @property
    def deadline_monotonic_ns(self) -> int:
        return self.monotonic_start_ns + S3_MARKET_DEADLINE_SECONDS * 1_000_000_000

    def to_metadata(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "window_id": self.window_id,
            "window_start_utc": self.start_utc,
            "window_end_utc": self.end_utc,
            "window_ordinal": self.ordinal,
            "monotonic_start_ns": self.monotonic_start_ns,
        }


def validate_cycle_windows(
    windows: Sequence[CycleWindow],
    *,
    envelope: CycleEnvelope | None = None,
) -> None:
    """Validate the exact four-window campaign shape."""

    selected = CycleEnvelope() if envelope is None else envelope
    selected.assert_tail_sufficient()
    if len(windows) != 4:
        raise CycleEvidenceIntegrityError("S3 campaign requires exactly four windows")
    if len({window.window_id for window in windows}) != len(windows):
        raise CycleEvidenceIntegrityError("S3 window identities must be unique")
    if len({window.ordinal for window in windows}) != len(windows):
        raise CycleEvidenceIntegrityError("S3 window ordinals must be unique")
    if len({window.campaign_id for window in windows}) != 1:
        raise CycleEvidenceIntegrityError("S3 windows must share one campaign identity")
    ordered = tuple(sorted(windows, key=lambda window: window.start_utc))
    for previous, current in zip(ordered, ordered[1:]):
        if current.start_utc < previous.end_utc:
            raise CycleEvidenceIntegrityError("S3 windows must not overlap")
    days: defaultdict[Any, int] = defaultdict(int)
    for window in windows:
        days[window.start_utc.date()] += 1
    if len(days) < 2 or any(count != 2 for count in days.values()):
        raise CycleEvidenceIntegrityError("S3 campaign requires two windows per day over at least two days")


@dataclass(frozen=True, slots=True)
class CycleCampaignManifest:
    """Prospective, immutable identity for one four-window S3 campaign."""

    campaign_id: str
    accepted_release: str
    policy_fingerprint: str
    windows: tuple[CycleWindow, ...]
    envelope: CycleEnvelope
    created_utc: datetime

    def __post_init__(self) -> None:
        _path_safe(self.campaign_id, "campaign_id")
        if not _SHA256_RE.fullmatch(self.accepted_release):
            raise ValueError("accepted_release must be a full lowercase 40-character Git SHA")
        if not _HEX256_RE.fullmatch(self.policy_fingerprint):
            raise ValueError("policy_fingerprint must be a SHA-256 digest")
        _utc(self.created_utc, "created_utc")
        if not isinstance(self.windows, tuple):
            raise TypeError("manifest windows must be a tuple")
        validate_cycle_windows(self.windows, envelope=self.envelope)
        if any(window.campaign_id != self.campaign_id for window in self.windows):
            raise ValueError("manifest windows must use the manifest campaign identity")
        expected = cycle_policy_fingerprint(self.accepted_release)
        if self.policy_fingerprint != expected:
            raise ValueError("manifest policy fingerprint does not match accepted release")

    def window(self, window_id: str) -> CycleWindow:
        for window in self.windows:
            if window.window_id == window_id:
                return window
        raise CycleManifestError(f"S3 manifest has no window {window_id!r}")

    def _core_payload(self) -> dict[str, Any]:
        return {
            "schema_version": S3_SCHEMA_VERSION,
            "experiment_kind": S3_EXPERIMENT_KIND,
            "campaign_id": self.campaign_id,
            "accepted_release": self.accepted_release,
            "policy_fingerprint": self.policy_fingerprint,
            "policy": _primitive(s2_cycle_policy()),
            "envelope": _primitive(self.envelope),
            "windows": [window.to_metadata() for window in self.windows],
            "created_utc": self.created_utc,
        }

    def to_payload(self) -> dict[str, Any]:
        core = _primitive(self._core_payload())
        return {**core, "manifest_sha256": _digest(core)}


def _cycle_manifest_path(root: str | os.PathLike[str], campaign_id: str) -> Path:
    _path_safe(campaign_id, "campaign_id")
    return Path(root) / ".s3-cycle" / campaign_id / "manifest.json"


def freeze_cycle_manifest(
    root: str | os.PathLike[str],
    *,
    accepted_release: str,
    windows: Sequence[CycleWindow],
    envelope: CycleEnvelope | None = None,
    created_utc: datetime | None = None,
) -> Path:
    """Create one owner-only prospective manifest exactly once."""

    selected_envelope = CycleEnvelope() if envelope is None else envelope
    policy = cycle_policy_fingerprint(accepted_release)
    selected = CycleCampaignManifest(
        campaign_id=windows[0].campaign_id if windows else "",
        accepted_release=accepted_release,
        policy_fingerprint=policy,
        windows=tuple(windows),
        envelope=selected_envelope,
        created_utc=datetime.now(UTC) if created_utc is None else created_utc,
    )
    path = _cycle_manifest_path(root, selected.campaign_id)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path.parent, stat.S_IRWXU)
    encoded = json.dumps(selected.to_payload(), sort_keys=True, separators=(",", ":")) + "\n"
    try:
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            stat.S_IRUSR | stat.S_IWUSR,
        )
    except FileExistsError as exc:
        raise CycleManifestError(
            f"S3 campaign manifest already exists: {selected.campaign_id}"
        ) from exc
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
        directory_descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    except BaseException:
        raise
    return path


def read_cycle_manifest(path: str | os.PathLike[str]) -> CycleCampaignManifest:
    """Read and validate one exact prospective campaign manifest."""

    selected_path = Path(path)
    try:
        payload = json.loads(selected_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CycleManifestError("S3 campaign manifest cannot be read") from exc
    if not isinstance(payload, Mapping):
        raise CycleManifestError("S3 campaign manifest is not an object")
    required = (
        "schema_version",
        "experiment_kind",
        "campaign_id",
        "accepted_release",
        "policy_fingerprint",
        "policy",
        "envelope",
        "windows",
        "created_utc",
        "manifest_sha256",
    )
    for key in required:
        _require(payload, key, context="S3 campaign manifest")
    if payload["schema_version"] != S3_SCHEMA_VERSION or payload["experiment_kind"] != S3_EXPERIMENT_KIND:
        raise CycleManifestError("unsupported S3 campaign manifest schema")
    if payload["policy"] != _primitive(s2_cycle_policy()):
        raise CycleManifestError("S3 campaign manifest policy is not the fixed S2 policy")
    core = {key: payload[key] for key in required if key != "manifest_sha256"}
    if payload["manifest_sha256"] != _digest(core):
        raise CycleManifestError("S3 campaign manifest digest mismatch")
    envelope_data = payload["envelope"]
    if not isinstance(envelope_data, Mapping):
        raise CycleManifestError("S3 campaign manifest envelope is malformed")
    try:
        envelope = CycleEnvelope(
            **{field.name: envelope_data[field.name] for field in fields(CycleEnvelope)}
        )
        raw_windows = payload["windows"]
        if not isinstance(raw_windows, list):
            raise TypeError("windows must be a list")
        windows = tuple(
            CycleWindow.from_text(
                campaign_id=payload["campaign_id"],
                window_id=_require(item, "window_id", context="manifest window"),
                start_utc=_require(item, "window_start_utc", context="manifest window"),
                end_utc=_require(item, "window_end_utc", context="manifest window"),
                ordinal=item.get("window_ordinal", index),
                monotonic_start_ns=item.get("monotonic_start_ns", 0),
            )
            for index, item in enumerate(raw_windows)
            if isinstance(item, Mapping)
        )
        if len(windows) != len(raw_windows):
            raise TypeError("manifest window is malformed")
        manifest = CycleCampaignManifest(
            campaign_id=payload["campaign_id"],
            accepted_release=payload["accepted_release"],
            policy_fingerprint=payload["policy_fingerprint"],
            windows=windows,
            envelope=envelope,
            created_utc=_parse_utc(payload["created_utc"], "manifest.created_utc"),
        )
    except (TypeError, ValueError, KeyError, CycleEvidenceIntegrityError) as exc:
        raise CycleManifestError("S3 campaign manifest fields are invalid") from exc
    if _primitive(manifest._core_payload()) != core:
        raise CycleManifestError("S3 campaign manifest canonical fields changed")
    return manifest


def cycle_policy_fingerprint(accepted_release: str) -> str:
    """Hash the fixed policy, envelope, and accepted source release."""

    if not _SHA256_RE.fullmatch(accepted_release):
        raise ValueError("accepted_release must be a full lowercase 40-character Git SHA")
    payload = {
        "accepted_release": accepted_release,
        "policy": _primitive(s2_cycle_policy()),
        "envelope": _primitive(CycleEnvelope()),
        "funding_status": "UNKNOWN",
        "source_scope": (Venue.RISEX.value, Venue.LIGHTER.value),
    }
    return _digest(payload)


def cycle_window_fingerprint(
    *,
    accepted_release: str,
    window: CycleWindow,
) -> str:
    return _digest(
        {
            "policy_fingerprint": cycle_policy_fingerprint(accepted_release),
            "campaign_id": window.campaign_id,
            "window_id": window.window_id,
            "window_start_utc": window.start_utc,
            "window_end_utc": window.end_utc,
            "window_ordinal": window.ordinal,
        }
    )


def _path_safe(value: str, name: str) -> None:
    _text(value, name)
    if "/" in value or "\\" in value or value in {".", ".."}:
        raise ValueError(f"{name} must be path-safe")


def reserve_cycle_window(
    root: str | os.PathLike[str],
    *,
    accepted_release: str,
    window: CycleWindow,
    policy_fingerprint: str | None = None,
    claimed_utc: datetime | None = None,
) -> Path:
    """Create the owner-only, create-once local window claim."""

    _path_safe(window.campaign_id, "campaign_id")
    _path_safe(window.window_id, "window_id")
    expected_policy = cycle_policy_fingerprint(accepted_release)
    policy = expected_policy if policy_fingerprint is None else policy_fingerprint
    if not _HEX256_RE.fullmatch(policy):
        raise ValueError("policy_fingerprint must be a SHA-256 digest")
    if policy != expected_policy:
        raise ValueError("policy_fingerprint does not match the fixed S3 policy and release")
    selected_claimed_utc = _utc(
        datetime.now(UTC) if claimed_utc is None else claimed_utc,
        "claimed_utc",
    )
    root_path = Path(root)
    root_path.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root_path, stat.S_IRWXU)
    claim_dir = root_path / ".s3-cycle" / window.campaign_id
    claim_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(claim_dir, stat.S_IRWXU)
    claim_path = claim_dir / f"{window.window_id}.claim"
    payload = {
        "schema_version": S3_SCHEMA_VERSION,
        "experiment_kind": S3_EXPERIMENT_KIND,
        "campaign_id": window.campaign_id,
        "window_id": window.window_id,
        "accepted_release": accepted_release,
        "policy_fingerprint": policy,
        "window_fingerprint": cycle_window_fingerprint(
            accepted_release=accepted_release,
            window=window,
        ),
        "window_start_utc": window.start_utc,
        "window_end_utc": window.end_utc,
        "claimed_utc": selected_claimed_utc,
    }
    encoded = json.dumps(_primitive(payload), sort_keys=True, separators=(",", ":")) + "\n"
    try:
        descriptor = os.open(
            claim_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            stat.S_IRUSR | stat.S_IWUSR,
        )
    except FileExistsError as exc:
        raise CycleWindowClaimError(
            f"S3 campaign window already claimed: {window.campaign_id}/{window.window_id}"
        ) from exc
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(claim_path, stat.S_IRUSR | stat.S_IWUSR)
        directory_descriptor = os.open(claim_dir, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    except BaseException:
        # An ambiguous claim remains consumed.  The caller must inspect the
        # local attempt instead of replacing it.
        raise
    return claim_path


def _market_to_dict(market: CanonicalMarket | None) -> Any:
    return None if market is None else _primitive(market)


def _market_from_dict(value: Any, *, context: str) -> CanonicalMarket | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise CycleEvidenceIntegrityError(f"{context} market is malformed")
    return CanonicalMarket(
        canonical_asset=_text(_require(value, "canonical_asset", context=context), "canonical_asset"),
        venue=Venue(_require(value, "venue", context=context)),
        venue_symbol=_text(_require(value, "venue_symbol", context=context), "venue_symbol"),
        market_type=MarketType(_require(value, "market_type", context=context)),
        contract_type=ContractType(_require(value, "contract_type", context=context)),
        base_multiplier=_optional_decimal(value.get("base_multiplier"), "base_multiplier"),
        quote_asset=_text(_require(value, "quote_asset", context=context), "quote_asset"),
        settlement_asset=_text(_require(value, "settlement_asset", context=context), "settlement_asset"),
        tick_size_raw=_decimal(_require(value, "tick_size_raw", context=context), "tick_size_raw"),
        quantity_step_raw=_decimal(_require(value, "quantity_step_raw", context=context), "quantity_step_raw"),
        minimum_quantity_raw=_decimal(_require(value, "minimum_quantity_raw", context=context), "minimum_quantity_raw"),
        minimum_notional_usd=_decimal(_require(value, "minimum_notional_usd", context=context), "minimum_notional_usd"),
        minimum_fee_notional_usd=_optional_decimal(value.get("minimum_fee_notional_usd"), "minimum_fee_notional_usd"),
        is_active=bool(_require(value, "is_active", context=context)),
        is_rfq=bool(_require(value, "is_rfq", context=context)),
        is_off_hours=bool(_require(value, "is_off_hours", context=context)),
        evidence_blockers=tuple(value.get("evidence_blockers", ())),
    )


def _book_to_dict(book: BookEvidence) -> dict[str, Any]:
    return _primitive(book)


def _book_from_dict(value: Any, *, context: str = "book") -> BookEvidence:
    if not isinstance(value, Mapping):
        raise CycleEvidenceIntegrityError(f"{context} is malformed")
    def levels(name: str) -> tuple[BookLevel, ...]:
        raw = _require(value, name, context=context)
        if not isinstance(raw, list):
            raise CycleEvidenceIntegrityError(f"{context}.{name} is malformed")
        result: list[BookLevel] = []
        for item in raw:
            if not isinstance(item, Mapping):
                raise CycleEvidenceIntegrityError(f"{context}.{name} has malformed level")
            result.append(
                BookLevel(
                    _decimal(_require(item, "canonical_price", context=context), "canonical_price"),
                    _decimal(_require(item, "canonical_quantity", context=context), "canonical_quantity"),
                )
            )
        return tuple(result)
    received_utc = value.get("received_utc")
    return BookEvidence(
        venue=Venue(_require(value, "venue", context=context)),
        canonical_market=_text(_require(value, "canonical_market", context=context), "canonical_market"),
        bids=levels("bids"),
        asks=levels("asks"),
        received_monotonic_ns=_non_negative_int(_require(value, "received_monotonic_ns", context=context), "received_monotonic_ns"),
        stream_session_id=_require(value, "stream_session_id", context=context),
        recovery_generation=_non_negative_int(_require(value, "recovery_generation", context=context), "recovery_generation"),
        book_revision=_non_negative_int(_require(value, "book_revision", context=context), "book_revision"),
        sequence=value.get("sequence"),
        checksum=value.get("checksum"),
        sequence_valid=bool(value.get("sequence_valid", True)),
        checksum_valid=bool(value.get("checksum_valid", True)),
        received_utc=None if received_utc is None else _parse_utc(received_utc, f"{context}.received_utc"),
        fresh=bool(value.get("fresh", True)),
        ingress_received_monotonic_ns=value.get("ingress_received_monotonic_ns"),
        normalized_ready_monotonic_ns=value.get("normalized_ready_monotonic_ns"),
        decision_ready_monotonic_ns=value.get("decision_ready_monotonic_ns"),
        tx_hash=value.get("tx_hash"),
        block_number=value.get("block_number"),
        log_index=value.get("log_index"),
        worker_timestamp=value.get("worker_timestamp"),
    )


def _trade_to_dict(trade: TradeEvidence) -> dict[str, Any]:
    return _primitive(trade)


def _trade_from_dict(value: Any, *, context: str = "trade") -> TradeEvidence:
    if not isinstance(value, Mapping):
        raise CycleEvidenceIntegrityError(f"{context} is malformed")
    return TradeEvidence(
        trade_event_key=_text(_require(value, "trade_event_key", context=context), "trade_event_key"),
        venue=Venue(_require(value, "venue", context=context)),
        canonical_market=_text(_require(value, "canonical_market", context=context), "canonical_market"),
        canonical_price=_decimal(_require(value, "canonical_price", context=context), "canonical_price"),
        canonical_quantity=_decimal(_require(value, "canonical_quantity", context=context), "canonical_quantity"),
        aggressor_side=Side(_require(value, "aggressor_side", context=context)),
        received_utc=_parse_utc(_require(value, "received_utc", context=context), f"{context}.received_utc"),
        received_monotonic_ns=_non_negative_int(_require(value, "received_monotonic_ns", context=context), "received_monotonic_ns"),
        stream_session_id=_require(value, "stream_session_id", context=context),
        recovery_generation=_non_negative_int(_require(value, "recovery_generation", context=context), "recovery_generation"),
        exchange_event_utc=None if value.get("exchange_event_utc") is None else _parse_utc(value["exchange_event_utc"], f"{context}.exchange_event_utc"),
        exchange_event_time_provenance=value.get("exchange_event_time_provenance"),
        ingress_received_monotonic_ns=value.get("ingress_received_monotonic_ns"),
        normalized_ready_monotonic_ns=value.get("normalized_ready_monotonic_ns"),
        decision_ready_monotonic_ns=value.get("decision_ready_monotonic_ns"),
        source_trade_id=value.get("source_trade_id"),
        maker_order_id=value.get("maker_order_id"),
        taker_order_id=value.get("taker_order_id"),
        maker=value.get("maker"),
        taker=value.get("taker"),
        tx_hash=value.get("tx_hash"),
        block_number=value.get("block_number"),
        log_index=value.get("log_index"),
        worker_timestamp=value.get("worker_timestamp"),
        venue_symbol=value.get("venue_symbol"),
    )


def _gap_to_dict(gap: DataGapEvidence) -> dict[str, Any]:
    return _primitive(gap)


def _gap_from_dict(value: Any, *, context: str = "gap") -> DataGapEvidence:
    if not isinstance(value, Mapping):
        raise CycleEvidenceIntegrityError(f"{context} is malformed")
    return DataGapEvidence(
        source_venue=Venue(_require(value, "source_venue", context=context)),
        canonical_market=_text(_require(value, "canonical_market", context=context), "canonical_market"),
        stream_session_id=_require(value, "stream_session_id", context=context),
        recovery_generation=_non_negative_int(_require(value, "recovery_generation", context=context), "recovery_generation"),
        gap_start_monotonic_ns=_non_negative_int(_require(value, "gap_start_monotonic_ns", context=context), "gap_start_monotonic_ns"),
        gap_end_monotonic_ns=value.get("gap_end_monotonic_ns"),
        reason=_text(_require(value, "reason", context=context), "reason"),
        protocol_frame_kind=value.get("protocol_frame_kind"),
        protocol_frame_category=value.get("protocol_frame_category"),
        protocol_frame_length=value.get("protocol_frame_length"),
        protocol_frame_sha256=value.get("protocol_frame_sha256"),
        transport_event=value.get("transport_event"),
        transport_failure_class=value.get("transport_failure_class"),
        transport_exception_type=value.get("transport_exception_type"),
    )


def _identity_from_dict(value: Any) -> CausalSourceIdentity | str | None:
    if value is None or value == "":
        return value
    if not isinstance(value, Mapping):
        raise CycleEvidenceIntegrityError("causal source identity is malformed")
    return CausalSourceIdentity(
        venue=Venue(_require(value, "venue", context="source_identity")),
        canonical_market=_text(_require(value, "canonical_market", context="source_identity"), "canonical_market"),
        stream_session_id=_require(value, "stream_session_id", context="source_identity"),
        recovery_generation=_non_negative_int(_require(value, "recovery_generation", context="source_identity"), "recovery_generation"),
        source_event_id=value.get("source_event_id"),
        block_number=value.get("block_number"),
        sequence=value.get("sequence"),
        revision=value.get("revision"),
        match_id=value.get("match_id"),
        source_kind=value.get("source_kind"),
        source_trade_id=value.get("source_trade_id"),
        maker_order_id=value.get("maker_order_id"),
        taker_order_id=value.get("taker_order_id"),
        maker=value.get("maker"),
        taker=value.get("taker"),
        tx_hash=value.get("tx_hash"),
        log_index=value.get("log_index"),
        worker_timestamp=value.get("worker_timestamp"),
        venue_symbol=value.get("venue_symbol"),
    )


def _event_to_dict(event: CausalEvent) -> dict[str, Any]:
    return {
        "kind": event.kind.value,
        "payload": _primitive(event.payload),
        "source_identity": _primitive(event.source_identity),
        "source_event_monotonic_ns": event.source_event_monotonic_ns,
        "block_number": event.block_number,
        "sequence": event.sequence,
        "revision": event.revision,
        "match_id": event.match_id,
        "ingress_received_monotonic_ns": event.ingress_received_monotonic_ns,
        "normalized_ready_monotonic_ns": event.normalized_ready_monotonic_ns,
        "decision_ready_monotonic_ns": event.decision_ready_monotonic_ns,
        "tx_hash": event.tx_hash,
        "log_index": event.log_index,
        "worker_timestamp": event.worker_timestamp,
    }


def _as_causal_event(
    event: CausalEvent | TradeEvidence | BookEvidence | DataGapEvidence,
) -> CausalEvent:
    if isinstance(event, CausalEvent):
        return event
    if isinstance(event, TradeEvidence):
        return CausalEvent.from_trade(event)
    if isinstance(event, BookEvidence):
        return CausalEvent.from_book(event)
    if isinstance(event, DataGapEvidence):
        return CausalEvent.from_gap(event)
    raise TypeError("cycle attempt events must be accepted public evidence")


def _input_to_dict(
    value: CausalEvent | TradeEvidence | BookEvidence | DataGapEvidence | CycleClock,
) -> dict[str, Any]:
    if isinstance(value, CycleClock):
        return {"kind": "CLOCK", "at_monotonic_ns": value.at_monotonic_ns}
    return {"kind": "EVENT", "event": _event_to_dict(_as_causal_event(value))}


def _event_from_dict(value: Any, *, context: str = "event") -> CausalEvent:
    if not isinstance(value, Mapping):
        raise CycleEvidenceIntegrityError(f"{context} is malformed")
    kind = CausalEventKind(_require(value, "kind", context=context))
    raw_payload = _require(value, "payload", context=context)
    if kind is CausalEventKind.TRADE:
        payload = _trade_from_dict(raw_payload, context=f"{context}.payload")
    elif kind is CausalEventKind.BOOK:
        payload = _book_from_dict(raw_payload, context=f"{context}.payload")
    else:
        payload = _gap_from_dict(raw_payload, context=f"{context}.payload")
    return CausalEvent(
        payload=payload,
        kind=kind,
        source_identity=_identity_from_dict(_require(value, "source_identity", context=context)),
        source_event_monotonic_ns=value.get("source_event_monotonic_ns"),
        block_number=value.get("block_number"),
        sequence=value.get("sequence"),
        revision=value.get("revision"),
        match_id=value.get("match_id"),
        ingress_received_monotonic_ns=value.get("ingress_received_monotonic_ns"),
        normalized_ready_monotonic_ns=value.get("normalized_ready_monotonic_ns"),
        decision_ready_monotonic_ns=value.get("decision_ready_monotonic_ns"),
        tx_hash=value.get("tx_hash"),
        log_index=value.get("log_index"),
        worker_timestamp=value.get("worker_timestamp"),
    )


def _input_from_dict(
    value: Any,
    *,
    context: str = "input",
) -> CausalEvent | CycleClock:
    if not isinstance(value, Mapping):
        raise CycleEvidenceIntegrityError(f"{context} is malformed")
    kind = _require(value, "kind", context=context)
    if kind == "CLOCK":
        return CycleClock(_non_negative_int(_require(value, "at_monotonic_ns", context=context), "at_monotonic_ns"))
    if kind == "EVENT":
        return _event_from_dict(_require(value, "event", context=context), context=f"{context}.event")
    raise CycleEvidenceIntegrityError(f"{context} has unsupported kind {kind}")


def _sizing_from_dict(value: Any, *, context: str) -> SizingEvidence | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise CycleEvidenceIntegrityError(f"{context} is malformed")
    kwargs = dict(value)
    for name in (
        "target_notional_usd", "reference_price", "risex_validation_price", "q_raw",
        "common_quantity_step", "floored_quantity", "risex_raw_quantity", "lighter_raw_quantity",
        "risex_quantity_step_raw", "lighter_quantity_step_raw", "risex_base_multiplier",
        "lighter_base_multiplier", "risex_minimum_quantity_raw", "lighter_minimum_quantity_raw",
        "risex_minimum_notional_usd", "lighter_minimum_notional_usd",
    ):
        kwargs[name] = _decimal(_require(kwargs, name, context=context), name)
    kwargs["direction"] = SpreadDirection(_require(kwargs, "direction", context=context))
    kwargs["risex_market"] = _market_from_dict(kwargs.get("risex_market"), context=f"{context}.risex_market")
    kwargs["lighter_market"] = _market_from_dict(kwargs.get("lighter_market"), context=f"{context}.lighter_market")
    return SizingEvidence(**kwargs)


def _quote_version_to_dict(version: QuoteVersion) -> dict[str, Any]:
    return _primitive(version)


def _quote_version_from_dict(value: Any) -> QuoteVersion:
    context = "quote_version"
    if not isinstance(value, Mapping):
        raise CycleEvidenceIntegrityError("quote_version is malformed")
    policy_data = _require(value.get("quote"), "policy", context="quote")
    if not isinstance(policy_data, Mapping):
        raise CycleEvidenceIntegrityError("quote.policy is malformed")
    policy = QuotePolicy(
        canonical_market=_text(_require(policy_data, "canonical_market", context="quote.policy"), "canonical_market"),
        direction=SpreadDirection(_require(policy_data, "direction", context="quote.policy")),
        target_notional_usd=_decimal(_require(policy_data, "target_notional_usd", context="quote.policy"), "target_notional_usd"),
        target_margin_bps=_decimal(_require(policy_data, "target_margin_bps", context="quote.policy"), "target_margin_bps"),
        risex_maker_fee_rate=_decimal(_require(policy_data, "risex_maker_fee_rate", context="quote.policy"), "risex_maker_fee_rate"),
        lighter_taker_fee_rate=_decimal(_require(policy_data, "lighter_taker_fee_rate", context="quote.policy"), "lighter_taker_fee_rate"),
        risex_fee_source=_text(_require(policy_data, "risex_fee_source", context="quote.policy"), "risex_fee_source"),
        lighter_fee_source=_text(_require(policy_data, "lighter_fee_source", context="quote.policy"), "lighter_fee_source"),
        risex_market=_market_from_dict(policy_data.get("risex_market"), context="quote.policy.risex_market"),
        lighter_market=_market_from_dict(policy_data.get("lighter_market"), context="quote.policy.lighter_market"),
        risex_best_bid=_optional_decimal(policy_data.get("risex_best_bid"), "risex_best_bid"),
        risex_best_ask=_optional_decimal(policy_data.get("risex_best_ask"), "risex_best_ask"),
        risex_tick_size=_optional_decimal(policy_data.get("risex_tick_size"), "risex_tick_size"),
        fee_observed_or_configured_at=(
            None
            if policy_data.get("fee_observed_or_configured_at") is None
            else _parse_utc(policy_data["fee_observed_or_configured_at"], "fee_observed_or_configured_at")
        ),
        quote_venue=Venue(policy_data.get("quote_venue", Venue.RISEX.value)),
        hedge_venue=Venue(policy_data.get("hedge_venue", Venue.LIGHTER.value)),
    )
    quote_data = _require(value, "quote", context=context)
    if not isinstance(quote_data, Mapping):
        raise CycleEvidenceIntegrityError("quote is malformed")
    fee_values: list[FeeEvidence] = []
    for raw_fee in _require(quote_data, "fee_components", context="quote"):
        if not isinstance(raw_fee, Mapping):
            raise CycleEvidenceIntegrityError("quote fee component is malformed")
        fee_values.append(
            FeeEvidence(
                venue=Venue(_require(raw_fee, "venue", context="quote.fee")),
                liquidity_role=LiquidityRole(_require(raw_fee, "liquidity_role", context="quote.fee")),
                fill_notional_usd=_decimal(_require(raw_fee, "fill_notional_usd", context="quote.fee"), "fill_notional_usd"),
                fee_base_notional_usd=_decimal(_require(raw_fee, "fee_base_notional_usd", context="quote.fee"), "fee_base_notional_usd"),
                rate=_decimal(_require(raw_fee, "rate", context="quote.fee"), "rate"),
                amount_usd=_decimal(_require(raw_fee, "amount_usd", context="quote.fee"), "amount_usd"),
                source=_text(_require(raw_fee, "source", context="quote.fee"), "source"),
                observed_or_configured_at=(
                    None
                    if raw_fee.get("observed_or_configured_at") is None
                    else _parse_utc(raw_fee["observed_or_configured_at"], "quote.fee.observed_or_configured_at")
                ),
            )
        )
    raw_vwap = quote_data.get("exact_hedge_vwap")
    exact_vwap = None
    if raw_vwap is not None:
        if not isinstance(raw_vwap, Mapping):
            raise CycleEvidenceIntegrityError("quote exact hedge VWAP is malformed")
        exact_vwap = ExactVwap(
            requested_quantity=_decimal(_require(raw_vwap, "requested_quantity", context="quote.vwap"), "requested_quantity"),
            filled_quantity=_decimal(_require(raw_vwap, "filled_quantity", context="quote.vwap"), "filled_quantity"),
            notional_usd=_decimal(_require(raw_vwap, "notional_usd", context="quote.vwap"), "notional_usd"),
            price=_optional_decimal(raw_vwap.get("price"), "price"),
        )
    quote = HypotheticalMakerQuote(
        policy=policy,
        outcome=EntryViabilityOutcome(_require(quote_data, "outcome", context="quote")),
        maker_side=Side(_require(quote_data, "maker_side", context="quote")),
        lighter_side=Side(_require(quote_data, "lighter_side", context="quote")),
        canonical_quantity=_optional_decimal(quote_data.get("canonical_quantity"), "canonical_quantity"),
        maker_price=_optional_decimal(quote_data.get("maker_price"), "maker_price"),
        lighter_vwap_price=_optional_decimal(quote_data.get("lighter_vwap_price"), "lighter_vwap_price"),
        lighter_filled_quantity=_optional_decimal(quote_data.get("lighter_filled_quantity"), "lighter_filled_quantity"),
        lighter_notional_usd=_optional_decimal(quote_data.get("lighter_notional_usd"), "lighter_notional_usd"),
        maker_notional_usd=_optional_decimal(quote_data.get("maker_notional_usd"), "maker_notional_usd"),
        fee_components=tuple(fee_values),
        total_entry_fees_usd=_optional_decimal(quote_data.get("total_entry_fees_usd"), "total_entry_fees_usd"),
        target_edge_usd=_optional_decimal(quote_data.get("target_edge_usd"), "target_edge_usd"),
        actual_edge_usd=_optional_decimal(quote_data.get("actual_edge_usd"), "actual_edge_usd"),
        raw_risex_price_bound=_optional_decimal(quote_data.get("raw_risex_price_bound"), "raw_risex_price_bound"),
        post_only_bound_price=_optional_decimal(quote_data.get("post_only_bound_price"), "post_only_bound_price"),
        sizing_evidence=_sizing_from_dict(quote_data.get("sizing_evidence"), context="quote.sizing_evidence"),
        exact_hedge_vwap=exact_vwap,
        risex_tick_size=_optional_decimal(quote_data.get("risex_tick_size"), "risex_tick_size"),
    )
    return QuoteVersion(
        version_id=_text(_require(value, "version_id", context=context), "version_id"),
        quote=quote,
        quote_created_utc=_parse_utc(_require(value, "quote_created_utc", context=context), "quote_created_utc"),
        quote_created_monotonic_ns=_non_negative_int(_require(value, "quote_created_monotonic_ns", context=context), "quote_created_monotonic_ns"),
        stream_session_id=_require(value, "stream_session_id", context=context),
        recovery_generation=_non_negative_int(_require(value, "recovery_generation", context=context), "recovery_generation"),
        quote_expires_monotonic_ns=value.get("quote_expires_monotonic_ns"),
        hedge_stream_session_id=value.get("hedge_stream_session_id"),
        hedge_recovery_generation=value.get("hedge_recovery_generation"),
        risex_book_revision=value.get("risex_book_revision"),
        lighter_book_revision=value.get("lighter_book_revision"),
        risex_book_revision_id=value.get("risex_book_revision_id"),
        lighter_book_revision_id=value.get("lighter_book_revision_id"),
        ingress_received_monotonic_ns=value.get("ingress_received_monotonic_ns"),
        normalized_ready_monotonic_ns=value.get("normalized_ready_monotonic_ns"),
        decision_ready_monotonic_ns=value.get("decision_ready_monotonic_ns"),
    )


def cycle_attempt_to_dict(attempt: CycleAttempt) -> dict[str, Any]:
    return {
        "schema_version": S3_SCHEMA_VERSION,
        "quote_version": _quote_version_to_dict(attempt.quote_version),
        "events": [_input_to_dict(event) for event in attempt.events],
        "source_books": [_book_to_dict(book) for book in attempt.source_books],
        "end_monotonic_ns": attempt.end_monotonic_ns,
    }


def cycle_attempt_from_dict(value: Any) -> CycleAttempt:
    if not isinstance(value, Mapping):
        raise CycleEvidenceIntegrityError("cycle attempt is malformed")
    events_raw = _require(value, "events", context="cycle attempt")
    books_raw = _require(value, "source_books", context="cycle attempt")
    if not isinstance(events_raw, list) or not isinstance(books_raw, list):
        raise CycleEvidenceIntegrityError("cycle attempt event/source-book arrays are malformed")
    events = tuple(_input_from_dict(item, context=f"cycle attempt input {index}") for index, item in enumerate(events_raw))
    books = tuple(_book_from_dict(item, context=f"cycle attempt source book {index}") for index, item in enumerate(books_raw))
    return CycleAttempt(
        quote_version=_quote_version_from_dict(_require(value, "quote_version", context="cycle attempt")),
        events=events,
        source_books=books,
        end_monotonic_ns=value.get("end_monotonic_ns"),
    )


def cycle_result_payload(result: CycleResult) -> dict[str, Any]:
    return _evidence_primitive(result)


def cycle_result_digest(result: CycleResult) -> str:
    return _digest(cycle_result_payload(result))


@dataclass(slots=True)
class _CycleCampaignBudget:
    """Aggregate S3 cap view reconstructed from the durable campaign runs."""

    campaign_root: Path
    campaign_id: str
    max_records: int
    max_bytes: int
    record_reserve: int
    bytes_reserve: int
    record_count: int = 0
    byte_count: int = 0

    @classmethod
    def load(
        cls,
        campaign_root: Path,
        *,
        campaign_id: str,
        max_records: int,
        max_bytes: int,
        record_reserve: int,
        bytes_reserve: int,
    ) -> "_CycleCampaignBudget":
        budget = cls(
            campaign_root=campaign_root,
            campaign_id=campaign_id,
            max_records=max_records,
            max_bytes=max_bytes,
            record_reserve=record_reserve,
            bytes_reserve=bytes_reserve,
        )
        if not campaign_root.exists():
            return budget
        for path in sorted(campaign_root.glob("run-*/evidence.jsonl")):
            try:
                records = list(iter_records(path))
            except Exception as exc:
                raise CycleEvidenceIntegrityError(
                    "S3 campaign budget cannot read an existing run"
                ) from exc
            if not records:
                raise CycleEvidenceIntegrityError("S3 campaign contains an empty run")
            metadata = records[0].get("metadata")
            if not isinstance(metadata, Mapping):
                raise CycleEvidenceIntegrityError("S3 campaign run has malformed metadata")
            if metadata.get("campaign_id") != campaign_id:
                continue
            budget.record_count += len(records)
            budget.byte_count += path.stat().st_size
        return budget

    def check(
        self,
        *,
        encoded_bytes: int,
        closing: bool,
        terminal: bool = False,
    ) -> None:
        """Check one record while retaining a durable terminal-marker slot.

        The S3 caps include the physically-last ``RUN_STOP``/``RUN_FAILED``
        record.  A normal stop is not special-cased by the shared append-only
        store, so the S3 guard must reserve one record and a bounded terminal
        marker's bytes until that record is written.  Closing evidence may
        consume the configured closing reserves, but it may not consume the
        terminal slot.
        """

        if terminal:
            record_limit = self.max_records
            byte_limit = self.max_bytes
        else:
            record_limit = self.max_records - self.record_reserve
            byte_limit = self.max_bytes - self.bytes_reserve
            if closing:
                # The terminal marker itself is part of the closing reserve;
                # keep a bounded slot for it after closing evidence.
                record_limit = self.max_records - 1
                byte_limit = self.max_bytes - TERMINAL_FAILURE_BYTES_RESERVE
        if self.record_count + 1 > record_limit:
            raise CycleEnvelopeLimitError("records")
        if self.byte_count + encoded_bytes > byte_limit:
            raise CycleEnvelopeLimitError("bytes")

    def commit(self, *, encoded_bytes: int) -> None:
        self.record_count += 1
        self.byte_count += encoded_bytes


def _preflight_campaign_store(
    root: str | os.PathLike[str],
    *,
    campaign_id: str,
    envelope: CycleEnvelope,
    metadata: Mapping[str, Any],
    run_id: str,
) -> None:
    """Reserve the first metadata record before creating a fresh run file."""

    payload = dict(metadata)
    payload["run_id"] = run_id
    payload["evidence_mode"] = payload.get("evidence_mode", "OBSERVATIONAL")
    encoded = json.dumps(
        _primitive(
            {
                "kind": "RUN_METADATA",
                "metadata": payload,
                "run_id": run_id,
                "record_index": 0,
            }
        ),
        sort_keys=True,
        separators=(",", ":"),
    ) + "\n"
    budget = _CycleCampaignBudget.load(
        Path(root),
        campaign_id=campaign_id,
        max_records=envelope.max_records,
        max_bytes=envelope.max_bytes,
        record_reserve=envelope.record_reserve,
        bytes_reserve=envelope.bytes_reserve,
    )
    budget.check(encoded_bytes=len(encoded.encode("utf-8")), closing=False)


class CycleEvidenceWriter:
    """S3-specific cap/reserve guard around the accepted append-only store."""

    def __init__(
        self,
        store: AppendOnlyEvidenceStore,
        envelope: CycleEnvelope,
        *,
        window: CycleWindow | None = None,
        campaign_root: str | os.PathLike[str] | None = None,
    ) -> None:
        self.store = store
        self.envelope = envelope
        self.window = window
        self._terminal_written = False
        root = (
            Path(campaign_root)
            if campaign_root is not None
            else store.path.parent.parent
        )
        campaign_id = None if window is None else window.campaign_id
        self._campaign_budget = (
            None
            if campaign_id is None
            else _CycleCampaignBudget.load(
                root,
                campaign_id=campaign_id,
                max_records=envelope.max_records,
                max_bytes=envelope.max_bytes,
                record_reserve=envelope.record_reserve,
                bytes_reserve=envelope.bytes_reserve,
            )
        )

    @property
    def terminal_written(self) -> bool:
        return self._terminal_written

    def _is_closing_record(self, record: Mapping[str, Any], *, terminal: bool) -> bool:
        if terminal or record.get("resource_phase") == "CLOSING" or record.get("closing") is True:
            return True
        if self.window is None:
            return False
        observed = record.get("observed_monotonic_ns")
        return isinstance(observed, int) and observed >= self.window.cutoff_monotonic_ns

    def append(self, record: Mapping[str, Any]) -> int:
        if self._terminal_written:
            raise CycleEvidenceIntegrityError("evidence was offered after terminal")
        kind = record.get("kind")
        terminal = kind in {"RUN_STOP", "RUN_FAILED"}
        closing = self._is_closing_record(record, terminal=terminal)
        if terminal:
            record_limit = self.envelope.max_records
            byte_limit = self.envelope.max_bytes
        else:
            record_limit = self.envelope.max_records - self.envelope.record_reserve
            byte_limit = self.envelope.max_bytes - self.envelope.bytes_reserve
            if closing:
                record_limit = self.envelope.max_records - 1
                byte_limit = self.envelope.max_bytes - TERMINAL_FAILURE_BYTES_RESERVE
        if self.store.record_count + 1 > record_limit:
            raise CycleEnvelopeLimitError("records")
        payload = dict(record)
        payload["run_id"] = self.store.run_id
        payload["record_index"] = self.store.record_count
        encoded = json.dumps(_primitive(payload), sort_keys=True, separators=(",", ":")) + "\n"
        if self.store.byte_count + len(encoded.encode("utf-8")) > byte_limit:
            raise CycleEnvelopeLimitError("bytes")
        encoded_bytes = len(encoded.encode("utf-8"))
        if self._campaign_budget is not None:
            self._campaign_budget.check(
                encoded_bytes=encoded_bytes,
                closing=closing,
                terminal=terminal,
            )
        assigned = self.store.append_batch((record,), sync=True)
        if len(assigned) != 1 or assigned[0] != payload["record_index"]:
            raise CycleEvidenceIntegrityError("append store returned a non-contiguous S3 index")
        if self._campaign_budget is not None:
            self._campaign_budget.commit(encoded_bytes=encoded_bytes)
        if terminal:
            self._terminal_written = True
        return assigned[0]

    def append_terminal(
        self,
        *,
        failed: bool = False,
        reason: str | None = None,
        incomplete_evidence: str | None = None,
    ) -> int:
        if self._terminal_written:
            raise CycleEvidenceIntegrityError("S3 run has more than one terminal")
        record: dict[str, Any] = {
            "kind": "RUN_FAILED" if failed else "RUN_STOP",
            "observed_monotonic_ns": (
                self.window.deadline_monotonic_ns
                if self.window is not None
                else self.envelope.market_deadline_ns
            ),
            "fatal_reason": reason,
        }
        if incomplete_evidence is not None:
            record["incomplete_evidence"] = _text(incomplete_evidence, "incomplete_evidence")
        return self.append(record)


@dataclass(frozen=True, slots=True)
class CycleRunOutput:
    run_id: str
    store_path: Path
    claim_path: Path | None
    admissions: tuple[CycleAdmission, ...]
    results: tuple[CycleResult, ...]


@dataclass(slots=True)
class _DecisionState:
    attempt_index: int
    quote_version: QuoteVersion
    source_books: tuple[BookEvidence, ...]
    admissions: dict[CycleScenario, CycleAdmission]
    inputs: list[CausalEvent | CycleClock]
    finished: bool = False


class CycleRunDriver:
    """Feed one persistent dual-lane S2 kernel without reordering evidence.

    The public boundary is intentionally incremental: ``admit_decision`` is
    called once the normalized quote and its witnesses are ready, then each
    producer event or explicit clock is offered through ``accept_input``.
    ``run`` is only a convenience adapter for a pre-recorded fixture; it uses
    the same one-input methods and never looks ahead at future inputs.
    """

    def __init__(
        self,
        window: CycleWindow,
        *,
        envelope: CycleEnvelope | None = None,
        policy: CyclePolicy | None = None,
        writer: CycleEvidenceWriter | None = None,
        persist: bool = True,
        streaming: bool = False,
    ) -> None:
        self.window = window
        self.envelope = CycleEnvelope() if envelope is None else envelope
        self.envelope.assert_tail_sufficient(policy)
        self.policy = s2_cycle_policy() if policy is None else policy
        self.kernel = CycleKernel(
            self.policy,
            terminal_retention_capacity=self.envelope.kernel_retention_capacity,
        )
        self.writer = writer
        self.persist = persist
        self.streaming = streaming
        self.admissions: list[CycleAdmission] = []
        self.results: list[CycleResult] = []
        self.skipped: list[dict[str, Any]] = []
        self._decisions: dict[int, _DecisionState] = {}
        self._stream_inputs: list[CausalEvent | CycleClock] = []
        self._stream_ended = False
        self._finalized = False
        self._finalize_error: BaseException | None = None

    def _write(self, record: Mapping[str, Any]) -> None:
        if self.persist and self.writer is not None:
            payload = dict(record)
            observed = payload.get("observed_monotonic_ns")
            if (
                payload.get("kind") not in {"RUN_STOP", "RUN_FAILED"}
                and isinstance(observed, int)
                and observed >= self.window.cutoff_monotonic_ns
            ):
                payload.setdefault("resource_phase", "CLOSING")
            self.writer.append(payload)

    @property
    def decision_count(self) -> int:
        return len(self._decisions)

    def decision_finished(self, attempt_index: int) -> bool:
        return self._state(attempt_index).finished

    def lanes_flat(self) -> bool:
        return all(
            self.kernel.state(scenario) is CycleKernelState.FLAT
            for scenario in CycleScenario
        )

    def lanes_pending(self) -> bool:
        return any(
            self.kernel.state(scenario) is CycleKernelState.PENDING
            for scenario in CycleScenario
        )

    def lanes_halted(self) -> bool:
        return any(
            self.kernel.state(scenario) is CycleKernelState.UNRESOLVED_HALTED
            for scenario in CycleScenario
        )

    def eligible_scenarios(self) -> tuple[CycleScenario, ...]:
        """Return lanes that can accept a fresh decision now."""

        return tuple(
            scenario
            for scenario in CycleScenario
            if self.kernel.state(scenario) is CycleKernelState.FLAT
        )

    def accept_global_input(
        self,
        value: CausalEvent | TradeEvidence | BookEvidence | DataGapEvidence | CycleClock,
    ) -> None:
        """Deliver one ordered input to every applicable persistent lane."""

        if not self.streaming:
            raise CycleEvidenceIntegrityError("global S3 inputs require streaming mode")
        if self._stream_ended:
            raise CycleEvidenceIntegrityError("S3 stream input arrived after stream end")
        if isinstance(value, CycleClock):
            item: CausalEvent | CycleClock = value
            observed = value.at_monotonic_ns
        else:
            item = _as_causal_event(value)
            observed = item.causal_monotonic_ns
        if observed > self.window.deadline_monotonic_ns:
            raise CycleEvidenceIntegrityError("S3 input arrived after hard market deadline")
        self._write(
            {
                "kind": "CYCLE_STREAM_INPUT",
                "input_index": len(self._stream_inputs),
                "input": _input_to_dict(item),
                "observed_monotonic_ns": observed,
            }
        )
        self._stream_inputs.append(item)
        for scenario in CycleScenario:
            state = self.kernel.state(scenario)
            if isinstance(item, CycleClock):
                if state is CycleKernelState.PENDING:
                    self.kernel.advance(item, scenario=scenario)
                continue
            # An event is also the trailing audit surface for a flat lane.
            # A lane with no prior result has nothing to audit yet.
            if state is CycleKernelState.PENDING or self.kernel.last_result(scenario) is not None:
                self.kernel.advance(item, scenario=scenario)

    def finish_stream(self, *, end_monotonic_ns: int | None = None) -> None:
        if not self.streaming:
            raise CycleEvidenceIntegrityError("stream end requires streaming mode")
        if self._stream_ended:
            return
        end_ns = self.window.deadline_monotonic_ns if end_monotonic_ns is None else _non_negative_int(end_monotonic_ns, "end_monotonic_ns")
        if end_ns > self.window.deadline_monotonic_ns:
            raise CycleEvidenceIntegrityError("S3 stream end is after hard market deadline")
        if any(self.kernel.state(scenario) is CycleKernelState.PENDING for scenario in CycleScenario):
            self.accept_global_input(CycleClock(end_ns))
        self._write(
            {
                "kind": "CYCLE_STREAM_END",
                "end_monotonic_ns": end_ns,
                "observed_monotonic_ns": end_ns,
            }
        )
        self._stream_ended = True

    def _admission_payload(self, attempt_index: int, admission: CycleAdmission) -> dict[str, Any]:
        return {
            "kind": "CYCLE_ADMISSION",
            "attempt_index": attempt_index,
            "scenario": admission.scenario.value,
            "accepted": admission.accepted,
            "quote_version_id": admission.quote_version_id,
            "decision_monotonic_ns": admission.decision_monotonic_ns,
            "reason": admission.reason,
            "observed_monotonic_ns": admission.decision_monotonic_ns or self.window.monotonic_start_ns,
        }

    def record_signal_skip(self, *, reason: str, observed_monotonic_ns: int) -> None:
        """Persist an inadmissible producer signal without entering the kernel."""

        _text(reason, "reason")
        observed = _non_negative_int(observed_monotonic_ns, "observed_monotonic_ns")
        self.skipped.append({"reason": reason, "observed_monotonic_ns": observed})
        self._write(
            {
                "kind": "CYCLE_SIGNAL_SKIPPED",
                "reason": reason,
                "observed_monotonic_ns": observed,
            }
        )

    def _skip_reason(self, version: QuoteVersion, source_books: tuple[BookEvidence, ...]) -> str | None:
        decision = version.decision_ready_monotonic_ns
        if decision is None or version.ingress_received_monotonic_ns is None or version.normalized_ready_monotonic_ns is None:
            return "MISSING_CAUSAL_METADATA"
        if decision < self.window.monotonic_start_ns:
            return "DECISION_BEFORE_WINDOW"
        if decision > self.window.cutoff_monotonic_ns:
            return "ENTRY_CUTOFF"
        if not version.is_active or version.quote.sizing_evidence is None:
            return "QUOTE_NOT_ADMISSIBLE"
        required = {version.risex_book_revision_id, version.lighter_book_revision_id}
        if None in required or len(required) != 2:
            return "MISSING_SOURCE_WITNESS"
        by_id = {book.book_revision_id: book for book in source_books}
        if not required.issubset(by_id):
            return "MISSING_SOURCE_WITNESS"
        for venue, revision_id in (
            (Venue.RISEX, version.risex_book_revision_id),
            (Venue.LIGHTER, version.lighter_book_revision_id),
        ):
            assert revision_id is not None
            book = by_id[revision_id]
            if book.venue is not venue or book.canonical_market != version.canonical_market:
                return "SOURCE_WITNESS_IDENTITY"
            if book.received_monotonic_ns > decision:
                return "SOURCE_BOOK_AFTER_DECISION"
            if decision - book.received_monotonic_ns > self.policy.input_freshness_max_age_ns:
                return "SOURCE_BOOK_STALE"
            if not book.fresh or not book.is_sequence_healthy:
                return "SOURCE_BOOK_UNHEALTHY"
            if not CausalEvent.from_book(book).source_identity_complete:
                return "SOURCE_WITNESS_IDENTITY"
        return None

    def _record_decision(self, attempt_index: int, version: QuoteVersion, source_books: tuple[BookEvidence, ...]) -> None:
        self._write(
            {
                "kind": "CYCLE_DECISION",
                "attempt_index": attempt_index,
                "quote_version": _quote_version_to_dict(version),
                "source_books": [_book_to_dict(book) for book in source_books],
                "observed_monotonic_ns": version.decision_ready_monotonic_ns or self.window.monotonic_start_ns,
            }
        )

    def admit_decision(
        self,
        attempt_index: int,
        quote_version: QuoteVersion,
        *,
        source_books: Iterable[BookEvidence] = (),
    ) -> tuple[CycleAdmission, ...]:
        if self._finalized:
            raise CycleEvidenceIntegrityError("S3 driver is already finalized")
        if isinstance(attempt_index, bool) or not isinstance(attempt_index, int) or attempt_index < 0:
            raise ValueError("attempt_index must be a non-negative integer")
        if attempt_index in self._decisions:
            raise CycleEvidenceIntegrityError("duplicate S3 decision index")
        if attempt_index != len(self._decisions):
            raise CycleEvidenceIntegrityError("S3 decisions must be admitted in order")
        if not isinstance(quote_version, QuoteVersion):
            raise TypeError("quote_version must be QuoteVersion")
        books = tuple(source_books)
        if any(not isinstance(book, BookEvidence) for book in books):
            raise TypeError("source_books must contain BookEvidence")
        self._record_decision(attempt_index, quote_version, books)
        skip = self._skip_reason(quote_version, books)
        admissions: dict[CycleScenario, CycleAdmission] = {}
        if skip is None:
            for scenario in CycleScenario:
                admission = self.kernel.admit(
                    quote_version,
                    scenario=scenario,
                    source_books=books,
                )
                admissions[scenario] = admission
                self.admissions.append(admission)
                self._write(self._admission_payload(attempt_index, admission))
                if not admission.accepted:
                    self.skipped.append({"attempt_index": attempt_index, "scenario": scenario.value, "reason": admission.reason})
        else:
            self.skipped.append({"attempt_index": attempt_index, "reason": skip})
            for scenario in CycleScenario:
                admission = CycleAdmission(False, scenario, quote_version.version_id, quote_version.decision_ready_monotonic_ns, skip)
                admissions[scenario] = admission
                self.admissions.append(admission)
                self._write(self._admission_payload(attempt_index, admission))
        accepted_any = any(admission.accepted for admission in admissions.values())
        self._decisions[attempt_index] = _DecisionState(
            attempt_index=attempt_index,
            quote_version=quote_version,
            source_books=books,
            admissions=admissions,
            inputs=[],
            # A rejected alternative must not close a decision whose sibling
            # was admitted.  In streaming mode the accepted lane continues
            # through the global input path while the rejected lane remains
            # auditable as a per-scenario admission.  The old aggregate
            # ``rejected`` check incorrectly made that healthy lane look
            # finished and could also emit an attempt end into stream
            # evidence, making a physical replay impossible.
            finished=skip is not None or not accepted_any,
        )
        # A streaming run has one physical stream end, not one attempt end
        # per signal.  Even an entirely rejected late signal is represented
        # by its decision/admission records; emitting ``CYCLE_END`` here
        # would mix attempt-scoped records into the stream replay contract.
        if not self.streaming and (skip is not None or not accepted_any):
            self._write(
                {
                    "kind": "CYCLE_END",
                    "attempt_index": attempt_index,
                    "skipped": True,
                    "observed_monotonic_ns": quote_version.decision_ready_monotonic_ns
                    or self.window.monotonic_start_ns,
                }
            )
        return tuple(admissions[scenario] for scenario in CycleScenario)

    def _state(self, attempt_index: int) -> _DecisionState:
        try:
            return self._decisions[attempt_index]
        except KeyError as exc:
            raise CycleEvidenceIntegrityError("S3 input arrived before its decision") from exc

    def accept_input(
        self,
        attempt_index: int,
        value: CausalEvent | TradeEvidence | BookEvidence | DataGapEvidence | CycleClock,
    ) -> None:
        state = self._state(attempt_index)
        if state.finished:
            raise CycleEvidenceIntegrityError("S3 input arrived after cycle end")
        item: CausalEvent | CycleClock
        if isinstance(value, CycleClock):
            item = value
            observed = value.at_monotonic_ns
        else:
            item = _as_causal_event(value)
            observed = item.causal_monotonic_ns
        if observed > self.window.deadline_monotonic_ns:
            raise CycleEvidenceIntegrityError("S3 input arrived after hard market deadline")
        self._write(
            {
                "kind": "CYCLE_INPUT",
                "attempt_index": attempt_index,
                "input_index": len(state.inputs),
                "input": _input_to_dict(item),
                "observed_monotonic_ns": observed,
            }
        )
        state.inputs.append(item)
        for scenario, admission in state.admissions.items():
            if admission.accepted:
                if isinstance(item, CycleClock) and self.kernel.state(scenario) is not CycleKernelState.PENDING:
                    # An explicit producer clock after a lane has already
                    # serialized its terminal is retained as input evidence,
                    # but cannot be applied to a non-pending kernel lane.
                    continue
                self.kernel.advance(item, scenario=scenario)

    def advance_clock(self, attempt_index: int, at_monotonic_ns: int) -> None:
        self.accept_input(attempt_index, CycleClock(_non_negative_int(at_monotonic_ns, "at_monotonic_ns")))

    def finish_decision(self, attempt_index: int, *, end_monotonic_ns: int | None = None) -> None:
        state = self._state(attempt_index)
        if state.finished:
            return
        end_ns = self.window.deadline_monotonic_ns if end_monotonic_ns is None else _non_negative_int(end_monotonic_ns, "end_monotonic_ns")
        if end_ns > self.window.deadline_monotonic_ns:
            raise CycleEvidenceIntegrityError("cycle end is after hard market deadline")
        if not state.inputs or not isinstance(state.inputs[-1], CycleClock) or state.inputs[-1].at_monotonic_ns != end_ns:
            self.advance_clock(attempt_index, end_ns)
        state.finished = True
        self._write({"kind": "CYCLE_END", "attempt_index": attempt_index, "skipped": False, "end_monotonic_ns": end_ns, "observed_monotonic_ns": end_ns})

    def finalize(
        self,
        *,
        failed: bool = False,
        reason: str | None = None,
    ) -> CycleRunOutput | tuple[CycleResult, ...]:
        if self._finalized:
            if self._finalize_error is not None:
                raise self._finalize_error
            return tuple(self.results) if self.writer is None else CycleRunOutput(self.writer.store.run_id, self.writer.store.path, None, tuple(self.admissions), tuple(self.results))
        try:
            if self.streaming:
                if not self._stream_ended:
                    self.finish_stream()
            else:
                for index, state in self._decisions.items():
                    if not state.finished:
                        self.finish_decision(index)
        except CycleEnvelopeLimitError as exc:
            # Closing the stream is itself evidence.  If its final clock or
            # CYCLE_STREAM_END cannot fit, preserve the already-written
            # prefix and use the reserved terminal slot immediately.
            if self.writer is not None and not self.writer.terminal_written:
                self.writer.append_terminal(
                    failed=True,
                    reason=f"S3_ENVELOPE_LIMIT_{exc.resource.upper()}",
                    incomplete_evidence="STREAM_FINALIZATION_PREFIX" if self.streaming else "CYCLE_FINALIZATION_PREFIX",
                )
            self._finalize_error = exc
            self._finalized = True
            raise
        self.results = []
        by_version = {state.quote_version.version_id: index for index, state in self._decisions.items()}
        try:
            for scenario in CycleScenario:
                for result in self.kernel.retained_results(scenario, include_active=True):
                    if result.quote_version_id not in by_version:
                        raise CycleEvidenceIntegrityError("kernel returned an unknown S3 cycle identity")
                    self.results.append(result)
                    self._write(
                        {
                            "kind": "CYCLE_FINAL_RESULT",
                            "attempt_index": by_version[result.quote_version_id],
                            "scenario": result.scenario.value,
                            "quote_version_id": result.quote_version_id,
                            "deadline_reached": result.status is CycleTerminalState.PENDING,
                            "result_sha256": cycle_result_digest(result),
                            "result": cycle_result_payload(result),
                            # Final results are emitted during finalization,
                            # after the stream has reached its bounded close.
                            # Keep them in the closing resource accounting
                            # even when the cycle itself completed earlier.
                            "observed_monotonic_ns": self.window.deadline_monotonic_ns,
                            "resource_phase": "CLOSING",
                        }
                    )
        except CycleEnvelopeLimitError as exc:
            # A failed final-result append must not consume the sole terminal
            # slot or leave an evidence file without a replayable failure
            # marker.  The result list is a durable prefix; replay can derive
            # the omitted suffix from the physical stream, but the failed
            # terminal keeps the run explicitly insufficient.
            if self.writer is not None and not self.writer.terminal_written:
                self.writer.append_terminal(
                    failed=True,
                    reason=f"S3_ENVELOPE_LIMIT_{exc.resource.upper()}",
                    incomplete_evidence="FINAL_RESULT_PREFIX",
                )
            self._finalize_error = exc
            self._finalized = True
            raise
        self._finalized = True
        if self.writer is None:
            return tuple(self.results)
        if not self.writer.terminal_written:
            self.writer.append_terminal(failed=failed, reason=reason)
        return CycleRunOutput(
            run_id=self.writer.store.run_id,
            store_path=self.writer.store.path,
            claim_path=None,
            admissions=tuple(self.admissions),
            results=tuple(self.results),
        )

    def run(self, attempts: Iterable[CycleAttempt]) -> CycleRunOutput | tuple[CycleResult, ...]:
        for attempt_index, attempt in enumerate(attempts):
            if not isinstance(attempt, CycleAttempt):
                raise TypeError("cycle driver expects CycleAttempt values")
            self.admit_decision(
                attempt_index,
                attempt.quote_version,
                source_books=attempt.source_books,
            )
            state = self._state(attempt_index)
            if not state.finished:
                for item in attempt.events:
                    self.accept_input(attempt_index, item)  # type: ignore[arg-type]
                self.finish_decision(attempt_index, end_monotonic_ns=attempt.end_monotonic_ns)
        return self.finalize()


class PublicCycleProducer:
    """Turn ordered public feed items into one persistent S3 cycle stream.

    The producer is deliberately synchronous at the decision boundary.  A
    book item is handled, its quote is calculated, and only then is the
    version's decision-ready timestamp captured.  Every subsequent item is
    delivered to the same driver in queue order; no timestamp sort or future
    attempt batch is constructed.
    """

    def __init__(
        self,
        driver: CycleRunDriver,
        market_pair: MarketPair,
        *,
        ingress: IngressQueue | None = None,
        now_utc: Callable[[], datetime] | None = None,
        monotonic_ns: Callable[[], int] | None = None,
    ) -> None:
        if not isinstance(driver, CycleRunDriver):
            raise TypeError("driver must be CycleRunDriver")
        if not isinstance(market_pair, MarketPair):
            raise TypeError("market_pair must be MarketPair")
        if market_pair.canonical_market != driver.policy.canonical_market:
            raise ValueError("public cycle pair does not match the S2 policy")
        self.driver = driver
        self.market_pair = market_pair
        self.ingress = ingress
        self._now_utc = now_utc or (lambda: datetime.now(UTC))
        self._monotonic_ns = monotonic_ns or time.monotonic_ns
        self._latest: dict[Venue, BookEvidence] = {}
        self._attempt_index: int | None = None
        self._last_decision_ns: int | None = None
        self._last_input_ns = driver.window.monotonic_start_ns
        self._decision_serial = 0
        self.processed_items: list[str] = []
        self._closed = False

    @property
    def latest_books(self) -> tuple[BookEvidence, ...]:
        return tuple(
            book
            for venue in (Venue.RISEX, Venue.LIGHTER)
            if (book := self._latest.get(venue)) is not None
        )

    @property
    def attempt_index(self) -> int | None:
        return self._attempt_index

    def _record_skip(self, reason: str, observed_ns: int) -> None:
        self.driver.record_signal_skip(reason=reason, observed_monotonic_ns=observed_ns)

    @staticmethod
    def _processing_ready(value: Any) -> int:
        if isinstance(value, CycleClock):
            return value.at_monotonic_ns
        event = _as_causal_event(value)
        values = [event.causal_monotonic_ns]
        if event.normalized_ready_monotonic_ns is not None:
            values.append(event.normalized_ready_monotonic_ns)
        if event.decision_ready_monotonic_ns is not None:
            values.append(event.decision_ready_monotonic_ns)
        return max(values)

    def _deliver(self, value: Any) -> int:
        observed_ns = self._processing_ready(value)
        if observed_ns > self.driver.window.deadline_monotonic_ns:
            return observed_ns
        if not self.driver.streaming:
            attempt_index = self._attempt_index
            if attempt_index is None or self.driver.decision_finished(attempt_index):
                return observed_ns
            self.driver.accept_input(attempt_index, value)
        else:
            self.driver.accept_global_input(value)
        self._last_input_ns = max(self._last_input_ns, observed_ns)
        # The event transition and the explicit clock transition are separate
        # evidence.  The latter is what advances delayed actions when no
        # exchange event happens exactly at their due boundary.
        if self.driver.streaming and not isinstance(value, CycleClock):
            self.driver.accept_global_input(CycleClock(observed_ns))
        elif not self.driver.streaming:
            assert self._attempt_index is not None
            if not self.driver.decision_finished(self._attempt_index):
                self.driver.advance_clock(self._attempt_index, observed_ns)
        return observed_ns

    def _policy(self, risex_book: BookEvidence, lighter_book: BookEvidence) -> QuotePolicy:
        if not risex_book.bids or not risex_book.asks:
            raise CyclePublicPreconditionError("RISEX_BOOK_BBO_MISSING")
        tick = self.market_pair.risex_market.tick_size_raw
        if tick is None or tick <= 0:
            raise CyclePublicPreconditionError("RISEX_TICK_MISSING")
        configured_at = max(risex_book.received_utc, lighter_book.received_utc)
        return QuotePolicy(
            canonical_market=self.driver.policy.canonical_market,
            direction=self.driver.policy.direction,
            target_notional_usd=self.driver.policy.target_notional_usd,
            target_margin_bps=self.driver.policy.target_margin_bps,
            risex_maker_fee_rate=self.driver.policy.risex_maker_fee_rate,
            lighter_taker_fee_rate=self.driver.policy.lighter_taker_fee_rate,
            risex_fee_source=self.driver.policy.risex_fee_source,
            lighter_fee_source=self.driver.policy.lighter_fee_source,
            risex_market=self.market_pair.risex_market,
            lighter_market=self.market_pair.lighter_market,
            risex_best_bid=risex_book.bids[0].canonical_price,
            risex_best_ask=risex_book.asks[0].canonical_price,
            risex_tick_size=tick,
            fee_observed_or_configured_at=configured_at,
        )

    def _build_version(
        self,
        risex_book: BookEvidence,
        lighter_book: BookEvidence,
    ) -> QuoteVersion:
        calculation_started = _non_negative_int(
            self._monotonic_ns(), "quote_calculation_started_monotonic_ns"
        )
        policy = self._policy(risex_book, lighter_book)
        quote = build_hypothetical_maker_quote(
            policy,
            lighter_book,
            risex_market=self.market_pair.risex_market,
            lighter_market=self.market_pair.lighter_market,
            risex_best_bid=risex_book.bids[0].canonical_price,
            risex_best_ask=risex_book.asks[0].canonical_price,
            risex_tick_size=policy.risex_tick_size,
        )
        # This call is intentionally after quote arithmetic.  A producer
        # timestamp on the input book is not a decision-ready timestamp.
        calculation_finished = _non_negative_int(
            self._monotonic_ns(), "decision_ready_monotonic_ns"
        )
        ingress_values = tuple(
            book.ingress_received_monotonic_ns
            if book.ingress_received_monotonic_ns is not None
            else book.received_monotonic_ns
            for book in (risex_book, lighter_book)
        )
        normalized_values = tuple(
            book.normalized_ready_monotonic_ns
            if book.normalized_ready_monotonic_ns is not None
            else book.received_monotonic_ns
            for book in (risex_book, lighter_book)
        )
        ingress = max(ingress_values)
        normalized = max(normalized_values)
        decision = max(calculation_started, calculation_finished, normalized)
        self._decision_serial += 1
        version_id = f"{self.driver.window.window_id}-public-{self._decision_serial}"
        return QuoteVersion(
            version_id=version_id,
            quote=quote,
            quote_created_utc=max(risex_book.received_utc, lighter_book.received_utc),
            quote_created_monotonic_ns=calculation_started,
            stream_session_id=risex_book.stream_session_id,
            recovery_generation=risex_book.recovery_generation,
            hedge_stream_session_id=lighter_book.stream_session_id,
            hedge_recovery_generation=lighter_book.recovery_generation,
            risex_book_revision=risex_book.book_revision,
            lighter_book_revision=lighter_book.book_revision,
            risex_book_revision_id=risex_book.book_revision_id,
            lighter_book_revision_id=lighter_book.book_revision_id,
            ingress_received_monotonic_ns=ingress,
            normalized_ready_monotonic_ns=normalized,
            decision_ready_monotonic_ns=decision,
        )

    def _maybe_decide(self, observed_ns: int) -> None:
        if self._closed or observed_ns > self.driver.window.deadline_monotonic_ns:
            return
        if self.driver.streaming:
            if not self.driver.eligible_scenarios():
                return
        elif self.driver.lanes_halted() or self.driver.lanes_pending():
            return
        books = self._latest
        risex_book = books.get(Venue.RISEX)
        lighter_book = books.get(Venue.LIGHTER)
        if risex_book is None or lighter_book is None:
            return
        try:
            version = self._build_version(risex_book, lighter_book)
        except (CyclePublicPreconditionError, TypeError, ValueError, ArithmeticError) as exc:
            self._record_skip(str(exc), observed_ns)
            return
        decision = version.decision_ready_monotonic_ns
        if decision is None:
            self._record_skip("MISSING_DECISION_READY", observed_ns)
            return
        if self._last_decision_ns is not None and decision < self._last_decision_ns + 1_000_000_000:
            return
        if (
            not self.driver.streaming
            and self._attempt_index is not None
            and not self.driver.decision_finished(self._attempt_index)
        ):
            if not self.driver.lanes_flat():
                return
            self.driver.finish_decision(
                self._attempt_index,
                end_monotonic_ns=max(decision, self._last_input_ns),
            )
        if not self.driver.streaming and self.driver.lanes_halted():
            self._record_skip("UNRESOLVED_HALTED", decision)
            return
        self._attempt_index = self.driver.decision_count
        self.driver.admit_decision(
            self._attempt_index,
            version,
            source_books=(risex_book, lighter_book),
        )
        self._last_decision_ns = decision

    def handle_item(self, item: IngressItem) -> None:
        """Process one feed item in exactly the supplied delivery order."""

        if isinstance(item, FeedBookEvent):
            if item.market_pair is not self.market_pair and item.market_pair != self.market_pair:
                raise CycleEvidenceIntegrityError("public cycle received an unrelated market pair")
            book = item.book
            self.processed_items.append(f"BOOK:{book.venue.value}:{book.book_revision}")
            ready_ns = self._deliver(book)
            self._latest[book.venue] = book
            self._maybe_decide(ready_ns)
            return
        if isinstance(item, FeedTradeEvent):
            if item.market_pair is not self.market_pair and item.market_pair != self.market_pair:
                raise CycleEvidenceIntegrityError("public cycle received an unrelated market pair")
            trade = item.trade
            self.processed_items.append(f"TRADE:{trade.trade_event_key}")
            self._deliver(trade)
            return
        if isinstance(item, FeedGapEvent):
            gap = item.gap
            self.processed_items.append(f"GAP:{gap.reason}:{gap.gap_start_monotonic_ns}")
            self._deliver(gap)
            latest = self._latest.get(gap.source_venue)
            if latest is not None and gap.matches(
                latest.venue,
                latest.canonical_market,
                latest.stream_session_id,
                latest.recovery_generation,
            ):
                self._latest.pop(gap.source_venue, None)
            return
        raise TypeError("unsupported public cycle producer item")

    def accept_clock(self, at_monotonic_ns: int) -> None:
        at_ns = _non_negative_int(at_monotonic_ns, "at_monotonic_ns")
        self.processed_items.append(f"CLOCK:{at_ns}")
        self._deliver(CycleClock(at_ns))

    async def consume(self) -> None:
        if self.ingress is None:
            raise ValueError("consume requires an ingress queue")
        while True:
            item = await self.ingress.next_item()
            if item is None:
                return
            succeeded = False
            try:
                self.handle_item(item)
                succeeded = True
            finally:
                self.ingress.complete_item(success=succeeded)

    async def run_items(self, items: Iterable[IngressItem | CycleClock]) -> None:
        """Offline fake-producer path; it intentionally does not sort items."""

        for item in items:
            if isinstance(item, CycleClock):
                self.accept_clock(item.at_monotonic_ns)
            else:
                self.handle_item(item)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self.driver.streaming:
            self.driver.finish_stream(end_monotonic_ns=self.driver.window.deadline_monotonic_ns)
        elif self._attempt_index is not None and not self.driver.decision_finished(self._attempt_index):
            self.accept_clock(self.driver.window.deadline_monotonic_ns)
            self.driver.finish_decision(
                self._attempt_index,
                end_monotonic_ns=self.driver.window.deadline_monotonic_ns,
            )

    def finalize(self, *, failed: bool = False, reason: str | None = None) -> CycleRunOutput | tuple[CycleResult, ...]:
        try:
            self.close()
        except CycleEnvelopeLimitError:
            # ``close`` performs the stream-end write before delegating to the
            # driver.  Route a close-time resource failure through the same
            # terminalizing path immediately; callers must not need a retry to
            # obtain the explicit incomplete-prefix marker.
            return self.driver.finalize(failed=failed, reason=reason)
        return self.driver.finalize(failed=failed, reason=reason)


# A descriptive alias keeps the public boundary discoverable without adding a
# second implementation or engine.
CyclePublicProducer = PublicCycleProducer


def _fixture_market(venue: Venue, symbol: str) -> CanonicalMarket:
    return CanonicalMarket(
        canonical_asset="BTC",
        venue=venue,
        venue_symbol=symbol,
        market_type=MarketType.PERPETUAL,
        contract_type=ContractType.LINEAR,
        base_multiplier=Decimal("1"),
        quote_asset="USDC",
        settlement_asset="USDC",
        tick_size_raw=Decimal("1"),
        quantity_step_raw=Decimal("0.01"),
        minimum_quantity_raw=Decimal("0.01"),
        minimum_notional_usd=Decimal("0"),
        minimum_fee_notional_usd=None,
        is_active=True,
        is_rfq=False,
        is_off_hours=False,
    )


def _fixture_book(
    venue: Venue,
    received: int,
    revision: int,
    *,
    bids: tuple[tuple[str, str], ...] = (("99", "10"),),
    asks: tuple[tuple[str, str], ...] = (("101", "10"),),
) -> BookEvidence:
    return BookEvidence(
        venue=venue,
        canonical_market="BTC",
        bids=tuple(BookLevel(Decimal(price), Decimal(quantity)) for price, quantity in bids),
        asks=tuple(BookLevel(Decimal(price), Decimal(quantity)) for price, quantity in asks),
        received_monotonic_ns=received,
        stream_session_id="fixture-risex" if venue is Venue.RISEX else "fixture-lighter",
        recovery_generation=0,
        book_revision=revision,
        sequence=revision,
        checksum=revision,
        sequence_valid=True,
        checksum_valid=True,
        received_utc=datetime(2026, 1, 1, tzinfo=UTC),
        fresh=True,
        ingress_received_monotonic_ns=received,
        normalized_ready_monotonic_ns=received,
        decision_ready_monotonic_ns=received,
        block_number=10_000 + revision if venue is Venue.RISEX else None,
        log_index=revision if venue is Venue.RISEX else None,
        worker_timestamp=received if venue is Venue.RISEX else None,
    )


def _fixture_trade(
    key: str,
    received: int,
    quantity: str,
    price: str,
    *,
    aggressor: Side,
) -> TradeEvidence:
    digest = hashlib.sha256(key.encode()).hexdigest()
    maker_order_id = "0x" + digest[:48]
    taker_order_id = "0x" + hashlib.sha256(("taker:" + key).encode()).hexdigest()[:48]
    source_trade_id = f"{maker_order_id}-{taker_order_id}"
    return TradeEvidence(
        trade_event_key=f"RISEX|BTC|{source_trade_id}",
        venue=Venue.RISEX,
        canonical_market="BTC",
        canonical_price=Decimal(price),
        canonical_quantity=Decimal(quantity),
        aggressor_side=aggressor,
        received_utc=datetime(2026, 1, 1, tzinfo=UTC),
        received_monotonic_ns=received,
        stream_session_id="fixture-risex",
        recovery_generation=0,
        ingress_received_monotonic_ns=received,
        normalized_ready_monotonic_ns=received,
        decision_ready_monotonic_ns=received,
        source_trade_id=source_trade_id,
        maker_order_id=maker_order_id,
        taker_order_id=taker_order_id,
        maker="0x" + "11" * 32,
        taker="0x" + "22" * 32,
        tx_hash="0x" + hashlib.sha256(("tx:" + key).encode()).hexdigest(),
        block_number=20_000 + received,
        log_index=received,
        worker_timestamp=received,
    )


def _fixture_attempt(
    *,
    window: CycleWindow,
    index: int,
    kind: str,
) -> CycleAttempt:
    base = window.monotonic_start_ns + index * 400_000_000_000 + 500_000_000
    risex_market = _fixture_market(Venue.RISEX, "BTC/USDC")
    lighter_market = _fixture_market(Venue.LIGHTER, "BTC")
    source_risex = _fixture_book(Venue.RISEX, base - 100_000_000, 1)
    source_lighter = _fixture_book(
        Venue.LIGHTER,
        base - 100_000_000,
        1,
        bids=(("99", "10"),),
        asks=(("100", "10"),),
    )
    policy = QuotePolicy(
        canonical_market="BTC",
        direction=SpreadDirection.RISEX_SELL_LIGHTER_BUY,
        target_notional_usd=Decimal("100"),
        target_margin_bps=Decimal("1"),
        risex_maker_fee_rate=Decimal("0.0001"),
        lighter_taker_fee_rate=Decimal("0"),
        risex_fee_source="SS-001Q",
        lighter_fee_source="OFFICIAL_LIGHTER_STANDARD_RESEARCH_INPUT",
        risex_market=risex_market,
        lighter_market=lighter_market,
        risex_best_bid=Decimal("99"),
        risex_best_ask=Decimal("101"),
        risex_tick_size=Decimal("1"),
        fee_observed_or_configured_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    from .economics import build_hypothetical_maker_quote

    quote = build_hypothetical_maker_quote(policy, source_lighter)
    if not quote.is_active:
        raise RuntimeError("S3 fixture quote did not satisfy the accepted fixed policy")
    version = QuoteVersion(
        version_id=f"{window.window_id}-cycle-{index}",
        quote=quote,
        quote_created_utc=datetime(2026, 1, 1, tzinfo=UTC),
        quote_created_monotonic_ns=base - 200_000_000,
        stream_session_id="fixture-risex",
        recovery_generation=0,
        hedge_stream_session_id="fixture-lighter",
        hedge_recovery_generation=0,
        risex_book_revision=1,
        lighter_book_revision=1,
        risex_book_revision_id=source_risex.book_revision_id,
        lighter_book_revision_id=source_lighter.book_revision_id,
        ingress_received_monotonic_ns=base - 200_000_000,
        normalized_ready_monotonic_ns=base - 150_000_000,
        decision_ready_monotonic_ns=base,
    )
    common = (
        _fixture_book(Venue.RISEX, base + 1_400_000_000, 2, asks=(("105", "10"),)),
        _fixture_book(Venue.LIGHTER, base + 1_400_000_000, 2, bids=(("99", "10"),), asks=(("100", "10"),)),
        _fixture_book(Venue.RISEX, base + 1_900_000_000, 3, asks=(("105", "10"),)),
        _fixture_book(Venue.LIGHTER, base + 1_900_000_000, 3, bids=(("99", "10"),), asks=(("100", "10"),)),
    )
    if kind == "forced":
        events = (
            CausalEvent.from_trade(_fixture_trade(f"{version.version_id}-partial", base + 1_100_000_000, "0.50", "102", aggressor=Side.BUY)),
            *common,
            _fixture_book(Venue.RISEX, base + 123_000_000_000, 4, asks=(("105", "10"),)),
            _fixture_book(Venue.LIGHTER, base + 123_000_000_000, 4, bids=(("99", "10"),), asks=(("100", "10"),)),
        )
        end = base + 125_000_000_000
    elif kind == "unresolved":
        events = (
            CausalEvent.from_trade(_fixture_trade(f"{version.version_id}-residue", base + 1_100_000_000, "0.50", "102", aggressor=Side.BUY)),
            *common[:2],
            _fixture_book(Venue.RISEX, base + 3_000_000_000, 3, asks=(("105", "10"),)),
            _fixture_book(Venue.LIGHTER, base + 3_000_000_000, 3, bids=(("99", "0"),), asks=(("100", "0"),)),
        )
        end = base + 125_000_000_000
    else:
        close_bid = "97" if kind == "negative" else "99"
        events = (
            CausalEvent.from_trade(_fixture_trade(f"{version.version_id}-entry", base + 1_100_000_000, "1.00", "102", aggressor=Side.BUY)),
            *common,
            CausalEvent.from_trade(_fixture_trade(f"{version.version_id}-exit", base + 4_600_000_000, "1.00", "97", aggressor=Side.SELL)),
            _fixture_book(Venue.RISEX, base + 4_800_000_000, 4, asks=(("105", "10"),)),
            _fixture_book(Venue.LIGHTER, base + 4_800_000_000, 4, bids=((close_bid, "10"),), asks=((str(Decimal(close_bid) + 1), "10"),)),
            _fixture_book(Venue.RISEX, base + 5_300_000_000, 5, asks=(("105", "10"),)),
            _fixture_book(Venue.LIGHTER, base + 5_300_000_000, 5, bids=((close_bid, "10"),), asks=((str(Decimal(close_bid) + 1), "10"),)),
        )
        end = base + 7_000_000_000
    return CycleAttempt(
        quote_version=version,
        events=tuple(events),
        source_books=(source_risex, source_lighter),
        end_monotonic_ns=end,
    )


def fixture_cycle_attempts(
    window: CycleWindow,
    *,
    count: int = 6,
    profile: str = "mixed",
) -> tuple[CycleAttempt, ...]:
    """Return deterministic typed attempts used by offline S3 evidence tests."""

    _positive_int(count, "count")
    if profile not in {"mixed", "normal", "negative", "forced", "unresolved", "terminal_conflict"}:
        raise ValueError("fixture profile must be mixed, normal, negative, forced, unresolved, or terminal_conflict")
    kinds = {
        "normal": "normal",
        "negative": "negative",
        "forced": "forced",
        "unresolved": "unresolved",
    }
    result: list[CycleAttempt] = []
    for index in range(count):
        if profile == "mixed":
            kind = ("normal", "negative", "forced", "unresolved")[index % 4]
        elif profile == "terminal_conflict":
            kind = "normal"
        else:
            kind = kinds[profile]
        result.append(_fixture_attempt(window=window, index=index, kind=kind))
    if profile == "terminal_conflict":
        if count < 2:
            raise ValueError("terminal_conflict fixture requires at least two cycles")
        first_event = result[0].events[0]
        if not isinstance(first_event, CausalEvent) or first_event.trade is None:
            raise RuntimeError("terminal_conflict fixture requires a trade event")
        conflicting_trade = replace(first_event.trade, canonical_quantity=Decimal("0.50"))
        conflicting_event = replace(first_event, payload=conflicting_trade)
        result[1] = replace(result[1], events=(conflicting_event, *result[1].events))
    return tuple(result)


def fixture_campaign_windows(*, campaign_id: str = "fixture-campaign") -> tuple[CycleWindow, ...]:
    day_one = datetime(2026, 1, 1, tzinfo=UTC)
    day_two = datetime(2026, 1, 2, tzinfo=UTC)
    starts = (day_one, day_one + timedelta(hours=1), day_two, day_two + timedelta(hours=1))
    return tuple(
        CycleWindow(
            campaign_id=campaign_id,
            window_id=f"window-{index + 1}",
            start_utc=start,
            end_utc=start + timedelta(seconds=S3_WINDOW_SECONDS),
            ordinal=index,
            monotonic_start_ns=0,
        )
        for index, start in enumerate(starts)
    )


def run_fixture_window(
    root: str | os.PathLike[str],
    *,
    accepted_release: str,
    window: CycleWindow,
    fixture_profile: str = "mixed",
    count: int = 6,
    envelope: CycleEnvelope | None = None,
    claim: bool = True,
) -> CycleRunOutput:
    """Produce one complete offline run through the real S3 path."""

    selected = CycleEnvelope() if envelope is None else envelope
    policy_fingerprint = cycle_policy_fingerprint(accepted_release)
    claim_path = (
        reserve_cycle_window(
            root,
            accepted_release=accepted_release,
            window=window,
            policy_fingerprint=policy_fingerprint,
        )
        if claim
        else None
    )
    attempts = fixture_cycle_attempts(window, count=count, profile=fixture_profile)
    metadata = {
        "schema_version": S3_SCHEMA_VERSION,
        "experiment_kind": S3_EXPERIMENT_KIND,
        "evidence_mode": "FIXTURE",
        "accepted_release": accepted_release,
        "campaign_id": window.campaign_id,
        "window_id": window.window_id,
        "window_fingerprint": cycle_window_fingerprint(accepted_release=accepted_release, window=window),
        "policy_fingerprint": policy_fingerprint,
        "source_scope": (Venue.RISEX.value, Venue.LIGHTER.value),
        "policy": _primitive(s2_cycle_policy()),
        "envelope": _primitive(selected),
        "fixture_profile": fixture_profile,
        "fixture_count": count,
        **window.to_metadata(),
        "tail_required_ns": selected.worst_configured_tail_ns(),
        "tail_sufficient": selected.closing_tail_ns >= selected.worst_configured_tail_ns(),
        "funding_status": "UNKNOWN",
        "created_utc": datetime.now(UTC),
    }
    run_id = new_run_id()
    _preflight_campaign_store(
        root,
        campaign_id=window.campaign_id,
        envelope=selected,
        metadata=metadata,
        run_id=run_id,
    )
    store = AppendOnlyEvidenceStore.create(
        root,
        metadata=metadata,
        run_id=run_id,
        max_records=selected.max_records + TERMINAL_FAILURE_RECORD_RESERVE,
        max_bytes=selected.max_bytes + TERMINAL_FAILURE_BYTES_RESERVE,
    )
    writer = CycleEvidenceWriter(store, selected, window=window, campaign_root=root)
    driver = CycleRunDriver(window, envelope=selected, writer=writer)
    try:
        driver.run(attempts)
    except BaseException as exc:
        if not writer.terminal_written:
            try:
                writer.append_terminal(failed=True, reason=type(exc).__name__)
            except BaseException as marker_exc:
                exc.add_note(f"unable to persist S3 failure marker: {type(marker_exc).__name__}")
        raise
    finally:
        store.close()
    return CycleRunOutput(
        run_id=store.run_id,
        store_path=store.path,
        claim_path=claim_path,
        admissions=tuple(driver.admissions),
        results=tuple(driver.results),
    )


def run_fixture_campaign(
    root: str | os.PathLike[str],
    *,
    accepted_release: str,
    campaign_id: str = "fixture-campaign",
    envelope: CycleEnvelope | None = None,
) -> tuple[CycleRunOutput, ...]:
    windows = fixture_campaign_windows(campaign_id=campaign_id)
    validate_cycle_windows(windows, envelope=envelope)
    return tuple(
        run_fixture_window(
            root,
            accepted_release=accepted_release,
            window=window,
            envelope=envelope,
        )
        for window in windows
    )


def _runtime_window(window: CycleWindow, *, started_utc: datetime, started_ns: int) -> CycleWindow:
    elapsed_ns = int((started_utc - window.start_utc).total_seconds() * 1_000_000_000)
    runtime_start_ns = started_ns - elapsed_ns
    if runtime_start_ns < 0:
        raise CyclePublicPreconditionError("monotonic clock cannot bind the prospective window")
    return replace(window, monotonic_start_ns=runtime_start_ns)


def _public_cycle_metadata(
    *,
    manifest: CycleCampaignManifest,
    prospective_window: CycleWindow,
    runtime_window: CycleWindow,
    run_id: str,
    started_utc: datetime,
) -> dict[str, Any]:
    return {
        "schema_version": S3_SCHEMA_VERSION,
        "experiment_kind": S3_EXPERIMENT_KIND,
        "evidence_mode": "OBSERVATIONAL",
        "accepted_release": manifest.accepted_release,
        "campaign_id": manifest.campaign_id,
        "window_id": prospective_window.window_id,
        "window_fingerprint": cycle_window_fingerprint(
            accepted_release=manifest.accepted_release,
            window=runtime_window,
        ),
        "manifest_window_fingerprint": cycle_window_fingerprint(
            accepted_release=manifest.accepted_release,
            window=prospective_window,
        ),
        "manifest_sha256": _digest(manifest._core_payload()),
        "policy_fingerprint": manifest.policy_fingerprint,
        "source_scope": (Venue.RISEX.value, Venue.LIGHTER.value),
        "policy": _primitive(s2_cycle_policy()),
        "envelope": _primitive(manifest.envelope),
        "run_id": run_id,
        "prospective": True,
        "funding_status": "UNKNOWN",
        "tail_required_ns": manifest.envelope.worst_configured_tail_ns(),
        "tail_sufficient": True,
        "created_utc": started_utc,
        **runtime_window.to_metadata(),
    }


async def run_public_cycle_collection(
    store_root: str | os.PathLike[str],
    *,
    manifest_path: str | os.PathLike[str] | None = None,
    manifest: CycleCampaignManifest | None = None,
    window_id: str,
    accepted_release: str | None = None,
    requested_markets: tuple[str, ...] = ("BTC",),
    now_utc: Callable[[], datetime] | None = None,
    monotonic_ns: Callable[[], int] | None = None,
    source_root: str | os.PathLike[str] | None = None,
    market_selector: Callable[..., Awaitable[tuple[MarketPair, ...]]] | None = None,
    feed_factory: Callable[..., PublicFeedRunner] | None = None,
) -> CycleRunOutput:
    """Run one exact prospective S3 window through public feeds only.

    The clean-release check, manifest read, create-once claim, and UTC-window
    check all happen before adapter construction or a public request.  The
    function is callable for the later Chief-frozen experiment but is never
    invoked by module import or help rendering.
    """

    if manifest is not None and manifest_path is not None:
        raise CyclePublicPreconditionError("supply manifest or manifest_path, not both")
    selected_manifest = manifest
    if selected_manifest is None:
        if manifest_path is None:
            raise CyclePublicPreconditionError("S3 public collection requires a prospective manifest")
        selected_manifest = read_cycle_manifest(manifest_path)
    if not isinstance(selected_manifest, CycleCampaignManifest):
        raise CyclePublicPreconditionError("S3 manifest is invalid")
    release = selected_manifest.accepted_release if accepted_release is None else accepted_release
    if release != selected_manifest.accepted_release:
        raise CyclePublicPreconditionError("accepted release does not match the S3 manifest")
    if requested_markets and {item.strip().upper() for item in requested_markets if item.strip()} != {"BTC"}:
        raise CyclePublicPreconditionError("S3 public collection is fixed to BTC")
    try:
        accepted_root = validate_loaded_release(release, source_root=source_root)
    except (ScannerPreconditionError, ValueError) as exc:
        raise CyclePublicPreconditionError(str(exc)) from exc
    prospective_window = selected_manifest.window(window_id)
    clock_utc = now_utc or (lambda: datetime.now(UTC))
    clock_ns = monotonic_ns or time.monotonic_ns
    claimed_utc = _utc(clock_utc(), "claimed_utc")
    claim_path = reserve_cycle_window(
        store_root,
        accepted_release=release,
        window=prospective_window,
        policy_fingerprint=selected_manifest.policy_fingerprint,
        claimed_utc=claimed_utc,
    )
    started_utc = _utc(clock_utc(), "sample_start_utc")
    if not prospective_window.start_utc <= started_utc < prospective_window.end_utc:
        raise CyclePublicPreconditionError(
            "S3 public collection attempt is outside its supplied prospective UTC window"
        )
    started_ns = _non_negative_int(clock_ns(), "sample_start_monotonic_ns")
    runtime_window = _runtime_window(
        prospective_window,
        started_utc=started_utc,
        started_ns=started_ns,
    )
    run_id = new_run_id()
    metadata = _public_cycle_metadata(
        manifest=selected_manifest,
        prospective_window=prospective_window,
        runtime_window=runtime_window,
        run_id=run_id,
        started_utc=started_utc,
    )
    _preflight_campaign_store(
        store_root,
        campaign_id=selected_manifest.campaign_id,
        envelope=selected_manifest.envelope,
        metadata=metadata,
        run_id=run_id,
    )
    store = AppendOnlyEvidenceStore.create(
        store_root,
        metadata=metadata,
        run_id=run_id,
        max_records=selected_manifest.envelope.max_records + TERMINAL_FAILURE_RECORD_RESERVE,
        max_bytes=selected_manifest.envelope.max_bytes + TERMINAL_FAILURE_BYTES_RESERVE,
    )
    writer = CycleEvidenceWriter(
        store,
        selected_manifest.envelope,
        window=runtime_window,
        campaign_root=store_root,
    )
    driver = CycleRunDriver(
        runtime_window,
        envelope=selected_manifest.envelope,
        writer=writer,
        streaming=True,
    )
    # S3 stream replay is a physical producer-order contract.  Keep the
    # historical queue default unchanged for the other public pipelines and
    # opt this bounded stream into offer-order gap delivery explicitly.
    ingress = IngressQueue(capacity=4096, preserve_offer_order=True)
    producer: PublicCycleProducer | None = None
    feed: PublicFeedRunner | None = None
    terminal_written = False
    try:
        writer.append(
            {
                "kind": "RUN_START",
                "run_id": run_id,
                "window_id": prospective_window.window_id,
                "accepted_release": release,
                "observed_monotonic_ns": started_ns,
            }
        )
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            risex = RisexAdapter(session)
            lighter = LighterAdapter(session)
            selector = select_public_market_pairs if market_selector is None else market_selector
            setup_started_utc = _utc(clock_utc(), "catalog_setup_utc")
            setup_remaining_seconds = (
                prospective_window.end_utc - setup_started_utc
            ).total_seconds()
            if setup_remaining_seconds <= 0:
                raise CyclePublicPreconditionError(
                    "S3 prospective window elapsed before public catalog setup"
                )
            try:
                pairs = await asyncio.wait_for(
                    selector(
                        risex,
                        lighter,
                        requested_markets=("BTC",),
                        max_markets=1,
                    ),
                    timeout=setup_remaining_seconds,
                )
            except asyncio.TimeoutError as exc:
                raise CyclePublicPreconditionError(
                    "S3 public catalog setup exceeded the hard deadline"
                ) from exc
            if len(pairs) != 1 or pairs[0].canonical_market != "BTC":
                raise CyclePublicPreconditionError("public catalog did not admit exactly BTC")
            feed_started_utc = _utc(clock_utc(), "feed_start_utc")
            remaining_seconds_float = (
                prospective_window.end_utc - feed_started_utc
            ).total_seconds()
            if remaining_seconds_float <= 0:
                raise CyclePublicPreconditionError(
                    "S3 prospective window elapsed before public feed startup"
                )
            producer = PublicCycleProducer(
                driver,
                pairs[0],
                ingress=ingress,
                now_utc=clock_utc,
                monotonic_ns=clock_ns,
            )
            config = ShadowConfig(
                max_markets=1,
                duration_seconds=1,
                sample_wall_clock_seconds=S3_WINDOW_SECONDS,
                ingress_queue_capacity=ingress.capacity,
            )
            factory = PublicFeedRunner if feed_factory is None else feed_factory
            feed = factory(
                session,
                pairs,
                ingress,
                config=config,
                risex_adapter=risex,
                lighter_adapter=lighter,
                now_utc=clock_utc,
                monotonic_ns=clock_ns,
            )
            consumer_stop = asyncio.Event()
            # The S3 hard deadline is the sample stop, not the start of an
            # unbounded post-sample drain.  The closing tail is already inside
            # the prospective 45-minute window.
            drain_complete = asyncio.Event()
            drain_complete.set()
            deadline_timer = asyncio.create_task(
                asyncio.sleep(remaining_seconds_float)
            )

            async def signal_deadline() -> None:
                await deadline_timer
                consumer_stop.set()

            deadline_signal = asyncio.create_task(signal_deadline())

            async def consume_with_failure_signal() -> None:
                try:
                    await producer.consume()
                except asyncio.CancelledError:
                    raise
                except BaseException:
                    # PublicFeedRunner observes this marker and exits its
                    # transport loops immediately when the consumer fails.
                    setattr(feed, "fatal_reason", "S3_CONSUMER_FAILURE")
                    consumer_stop.set()
                    raise

            consumer = asyncio.create_task(consume_with_failure_signal())
            feed_task = asyncio.create_task(
                feed.run(
                    duration_seconds=max(1, math.ceil(remaining_seconds_float)),
                    stop_event=consumer_stop,
                    drain_event=drain_complete,
                    duration_limit_seconds=S3_WINDOW_SECONDS,
                )
            )
            try:
                done, _ = await asyncio.wait(
                    (feed_task, consumer),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if consumer in done:
                    if consumer.exception() is None:
                        raise CycleEvidenceIntegrityError(
                            "S3 public consumer exited before feed shutdown"
                        )
                    if not feed_task.done():
                        feed_task.cancel()
                    await asyncio.gather(feed_task, return_exceptions=True)
                    await consumer
                await feed_task
                ingress.close()
                await consumer
            finally:
                ingress.close()
                for task in (feed_task, consumer, deadline_signal, deadline_timer):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(
                    feed_task,
                    consumer,
                    deadline_signal,
                    deadline_timer,
                    return_exceptions=True,
                )
            producer.close()
            failed_reason = feed.fatal_reason
            producer.finalize(failed=failed_reason is not None, reason=failed_reason)
            terminal_written = writer.terminal_written
            if failed_reason is not None:
                raise CycleEvidenceError(failed_reason)
    except BaseException as exc:
        if not terminal_written and not writer.terminal_written:
            try:
                if producer is not None:
                    producer.finalize(failed=True, reason=type(exc).__name__)
                else:
                    writer.append_terminal(failed=True, reason=type(exc).__name__)
                terminal_written = True
            except BaseException as marker_exc:
                exc.add_note(f"unable to persist S3 failure marker: {type(marker_exc).__name__}")
        raise
    finally:
        store.close()
    output = CycleRunOutput(
        run_id=store.run_id,
        store_path=store.path,
        claim_path=claim_path,
        admissions=tuple(driver.admissions),
        results=tuple(driver.results),
    )
    # Retain this local evidence in the returned object only; the clean source
    # root and exact claim/manifest identities remain in the persisted records.
    _ = accepted_root
    return output


@dataclass(frozen=True, slots=True)
class _RunBundle:
    path: Path
    metadata: dict[str, Any]
    attempts: tuple[CycleAttempt, ...]
    streaming: bool
    stream_inputs: tuple[CausalEvent | CycleClock, ...]
    admissions: tuple[dict[str, Any], ...]
    signal_skips: tuple[dict[str, Any], ...]
    results: tuple[dict[str, Any], ...]
    replay_admissions: tuple[CycleAdmission, ...]
    replay_results: tuple[CycleResult, ...]
    report_results: tuple[CycleResult, ...]
    resource_incomplete: bool
    terminal: str
    terminal_reason: str | None
    incomplete_evidence: str | None
    record_count: int
    byte_count: int


def _read_run(path: Path) -> _RunBundle:
    records = list(iter_records(path))
    if not records:
        raise CycleEvidenceIntegrityError("S3 evidence is empty")
    run_id = records[0].get("run_id")
    if not isinstance(run_id, str) or not run_id:
        raise CycleEvidenceIntegrityError("S3 evidence has no run identity")
    for expected_index, record in enumerate(records):
        if record.get("run_id") != run_id or record.get("record_index") != expected_index:
            raise CycleEvidenceIntegrityError("S3 evidence record indices are not contiguous")
    terminals = [record for record in records if record.get("kind") in {"RUN_STOP", "RUN_FAILED"}]
    if len(terminals) != 1 or records[-1] is not terminals[0]:
        raise CycleEvidenceIntegrityError("S3 evidence must have one physically-last terminal")
    terminal_kind = terminals[0].get("kind")
    terminal_reason = terminals[0].get("fatal_reason")
    if records[0].get("kind") != "RUN_METADATA" or not isinstance(records[0].get("metadata"), Mapping):
        raise CycleEvidenceIntegrityError("S3 evidence must begin with RUN_METADATA")
    metadata = dict(records[0]["metadata"])
    if metadata.get("run_id") != run_id:
        raise CycleEvidenceIntegrityError("S3 metadata run identity does not match the evidence file")
    for key in (
        "schema_version", "experiment_kind", "accepted_release", "campaign_id", "window_id",
        "window_fingerprint", "policy_fingerprint", "window_start_utc", "window_end_utc",
        "envelope", "monotonic_start_ns", "evidence_mode", "source_scope", "policy",
        "funding_status",
    ):
        _require(metadata, key, context="S3 metadata")
    if metadata["schema_version"] != S3_SCHEMA_VERSION or metadata["experiment_kind"] != S3_EXPERIMENT_KIND:
        raise CycleEvidenceIntegrityError("unsupported S3 evidence schema or experiment")
    if metadata["evidence_mode"] not in {"FIXTURE", "OBSERVATIONAL"}:
        raise CycleEvidenceIntegrityError("S3 evidence provenance is invalid")
    if metadata["evidence_mode"] == "OBSERVATIONAL":
        for key in ("manifest_sha256", "manifest_window_fingerprint", "prospective"):
            _require(metadata, key, context="observed-public S3 metadata")
        if not isinstance(metadata["manifest_sha256"], str) or not _HEX256_RE.fullmatch(
            metadata["manifest_sha256"]
        ):
            raise CycleEvidenceIntegrityError("observed-public S3 manifest identity is invalid")
        if metadata["prospective"] is not True:
            raise CycleEvidenceIntegrityError("observed-public S3 evidence is not prospective")
    if metadata["source_scope"] not in (
        [Venue.RISEX.value, Venue.LIGHTER.value],
        (Venue.RISEX.value, Venue.LIGHTER.value),
    ):
        raise CycleEvidenceIntegrityError("S3 evidence source scope is invalid")
    if metadata["policy"] != _primitive(s2_cycle_policy()):
        raise CycleEvidenceIntegrityError("S3 evidence policy metadata is not the fixed S2 policy")
    if metadata["funding_status"] != "UNKNOWN":
        raise CycleEvidenceIntegrityError("S3 evidence funding status is not UNKNOWN")
    accepted_release = metadata["accepted_release"]
    try:
        window = CycleWindow.from_text(
            campaign_id=metadata["campaign_id"],
            window_id=metadata["window_id"],
            start_utc=metadata["window_start_utc"],
            end_utc=metadata["window_end_utc"],
            ordinal=metadata.get("window_ordinal", 0),
            monotonic_start_ns=metadata["monotonic_start_ns"],
        )
        if metadata["policy_fingerprint"] != cycle_policy_fingerprint(accepted_release):
            raise CycleEvidenceIntegrityError("S3 policy/release fingerprint mismatch")
        if metadata["window_fingerprint"] != cycle_window_fingerprint(accepted_release=accepted_release, window=window):
            raise CycleEvidenceIntegrityError("S3 window fingerprint mismatch")
        manifest_window_fingerprint = metadata.get("manifest_window_fingerprint")
        if (
            manifest_window_fingerprint is not None
            and manifest_window_fingerprint
            != cycle_window_fingerprint(accepted_release=accepted_release, window=window)
        ):
            raise CycleEvidenceIntegrityError("S3 manifest/window fingerprint mismatch")
        envelope_data = metadata["envelope"]
        if not isinstance(envelope_data, Mapping):
            raise CycleEvidenceIntegrityError("S3 envelope metadata is malformed")
        envelope = CycleEnvelope(
            **{field.name: envelope_data[field.name] for field in fields(CycleEnvelope)}
        )
        envelope.assert_tail_sufficient()
        if metadata.get("tail_required_ns") != envelope.worst_configured_tail_ns():
            raise CycleEvidenceIntegrityError("S3 envelope tail metadata is inconsistent")
        if metadata.get("tail_sufficient") is not True:
            raise CycleEvidenceIntegrityError("S3 envelope closing tail is not sufficient")
    except CycleEvidenceIntegrityError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise CycleEvidenceIntegrityError("S3 evidence metadata is malformed") from exc
    decisions_raw: dict[int, tuple[QuoteVersion, tuple[BookEvidence, ...]]] = {}
    inputs_raw: defaultdict[int, list[CausalEvent | CycleClock]] = defaultdict(list)
    ends_raw: dict[int, int | None] = {}
    stream_inputs_raw: list[tuple[int, CausalEvent | CycleClock]] = []
    stream_end: int | None = None
    admissions: list[dict[str, Any]] = []
    signal_skips: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    for record in records[1:-1]:
        kind = record.get("kind")
        if kind == "CYCLE_DECISION":
            index = _non_negative_int(_require(record, "attempt_index", context="CYCLE_DECISION"), "attempt_index")
            if index in decisions_raw:
                raise CycleEvidenceIntegrityError("duplicate S3 decision index")
            raw_books = _require(record, "source_books", context="CYCLE_DECISION")
            if not isinstance(raw_books, list):
                raise CycleEvidenceIntegrityError("CYCLE_DECISION source books are malformed")
            decisions_raw[index] = (
                _quote_version_from_dict(_require(record, "quote_version", context="CYCLE_DECISION")),
                tuple(_book_from_dict(item, context=f"CYCLE_DECISION source book {book_index}") for book_index, item in enumerate(raw_books)),
            )
        elif kind == "CYCLE_INPUT":
            index = _non_negative_int(_require(record, "attempt_index", context="CYCLE_INPUT"), "attempt_index")
            input_index = _non_negative_int(_require(record, "input_index", context="CYCLE_INPUT"), "input_index")
            if input_index != len(inputs_raw[index]):
                raise CycleEvidenceIntegrityError("S3 input delivery indices are not contiguous")
            inputs_raw[index].append(_input_from_dict(_require(record, "input", context="CYCLE_INPUT"), context="CYCLE_INPUT.input"))
        elif kind == "CYCLE_STREAM_INPUT":
            input_index = _non_negative_int(
                _require(record, "input_index", context="CYCLE_STREAM_INPUT"),
                "input_index",
            )
            stream_inputs_raw.append(
                (
                    input_index,
                    _input_from_dict(
                        _require(record, "input", context="CYCLE_STREAM_INPUT"),
                        context="CYCLE_STREAM_INPUT.input",
                    ),
                )
            )
        elif kind == "CYCLE_STREAM_END":
            if stream_end is not None:
                raise CycleEvidenceIntegrityError("duplicate S3 stream end")
            stream_end = _non_negative_int(
                _require(record, "end_monotonic_ns", context="CYCLE_STREAM_END"),
                "end_monotonic_ns",
            )
        elif kind == "CYCLE_END":
            index = _non_negative_int(_require(record, "attempt_index", context="CYCLE_END"), "attempt_index")
            if index in ends_raw:
                raise CycleEvidenceIntegrityError("duplicate S3 cycle end")
            ends_raw[index] = record.get("end_monotonic_ns")
        elif kind == "CYCLE_ADMISSION":
            admissions.append(record)
        elif kind == "CYCLE_SIGNAL_SKIPPED":
            signal_skips.append(record)
        elif kind == "CYCLE_FINAL_RESULT":
            result_payload = _require(record, "result", context="CYCLE_FINAL_RESULT")
            digest = _require(record, "result_sha256", context="CYCLE_FINAL_RESULT")
            if digest != _digest(result_payload):
                raise CycleEvidenceIntegrityError("CYCLE_FINAL_RESULT payload digest mismatch")
            results.append(record)
        elif kind in {"RUN_METADATA", "REPLAY_MODE"}:
            raise CycleEvidenceIntegrityError(f"unexpected S3 record kind: {kind}")
    if stream_inputs_raw and (inputs_raw or ends_raw):
        raise CycleEvidenceIntegrityError("S3 evidence mixes stream and attempt inputs")
    expected_indices = tuple(range(len(decisions_raw)))
    if tuple(sorted(decisions_raw)) != expected_indices:
        raise CycleEvidenceIntegrityError("S3 decision indices are not contiguous")
    if not stream_inputs_raw and set(ends_raw) != set(decisions_raw):
        missing_cycle_end_marker = (
            terminal_kind == "RUN_FAILED"
            and isinstance(terminal_reason, str)
            and terminal_reason in _RESOURCE_LIMIT_REASONS
            and terminals[0].get("incomplete_evidence") == "CYCLE_FINALIZATION_PREFIX"
        )
        if not missing_cycle_end_marker:
            raise CycleEvidenceIntegrityError("S3 evidence is missing a cycle end")
    if stream_inputs_raw and ends_raw:
        raise CycleEvidenceIntegrityError("S3 stream evidence contains attempt cycle ends")
    ordered_attempts = tuple(
        CycleAttempt(
            quote_version=decisions_raw[index][0],
            source_books=decisions_raw[index][1],
            events=tuple(inputs_raw.get(index, ())),
        end_monotonic_ns=ends_raw.get(index),
        )
        for index in expected_indices
    )
    streaming = bool(stream_inputs_raw or stream_end is not None)
    incomplete_evidence = terminals[0].get("incomplete_evidence")
    if incomplete_evidence is not None and (
        not isinstance(incomplete_evidence, str)
        or incomplete_evidence not in _RESOURCE_INCOMPLETE_MARKERS
    ):
        raise CycleEvidenceIntegrityError("S3 terminal incomplete evidence marker is invalid")
    resource_limit_terminal = (
        terminal_kind == "RUN_FAILED"
        and isinstance(terminal_reason, str)
        and terminal_reason in _RESOURCE_LIMIT_REASONS
    )
    if incomplete_evidence is not None and not resource_limit_terminal:
        raise CycleEvidenceIntegrityError("S3 incomplete evidence marker lacks a resource-limit terminal")
    stream_finalization_incomplete = (
        resource_limit_terminal
        and incomplete_evidence == "STREAM_FINALIZATION_PREFIX"
        and stream_end is None
    )
    if incomplete_evidence == "STREAM_FINALIZATION_PREFIX" and not stream_finalization_incomplete:
        raise CycleEvidenceIntegrityError("S3 stream-finalization marker is inconsistent")
    cycle_finalization_incomplete = (
        resource_limit_terminal
        and incomplete_evidence == "CYCLE_FINALIZATION_PREFIX"
        and not streaming
        and set(ends_raw) != set(decisions_raw)
    )
    if incomplete_evidence == "CYCLE_FINALIZATION_PREFIX" and not cycle_finalization_incomplete:
        raise CycleEvidenceIntegrityError("S3 cycle-finalization marker is inconsistent")
    if streaming:
        if stream_end is None:
            if not stream_finalization_incomplete:
                raise CycleEvidenceIntegrityError("S3 stream evidence is missing its end")
        if tuple(index for index, _ in stream_inputs_raw) != tuple(range(len(stream_inputs_raw))):
            raise CycleEvidenceIntegrityError("S3 stream input indices are not contiguous")
        driver = CycleRunDriver(window, envelope=envelope, persist=False, streaming=True)
        # Replay the physical producer order.  Admitting every decision before
        # replaying the stream would let a later decision bypass an active
        # lane, and would therefore turn a valid persisted stream into a
        # different kernel history.
        stream_input_by_index = {
            index: item for index, item in stream_inputs_raw
        }
        stream_input_cursor = 0
        for record in records[1:-1]:
            kind = record.get("kind")
            if kind == "CYCLE_DECISION":
                index = _non_negative_int(
                    _require(record, "attempt_index", context="CYCLE_DECISION"),
                    "attempt_index",
                )
                version, books = decisions_raw[index]
                driver.admit_decision(index, version, source_books=books)
            elif kind == "CYCLE_STREAM_INPUT":
                item = stream_input_by_index[stream_input_cursor]
                driver.accept_global_input(item)
                stream_input_cursor += 1
            elif kind == "CYCLE_STREAM_END":
                driver.finish_stream(end_monotonic_ns=stream_end)
        if stream_input_cursor != len(stream_inputs_raw):
            raise CycleEvidenceIntegrityError("S3 stream replay did not consume every input")
        if not stream_finalization_incomplete and not driver._stream_ended:
            driver.finish_stream(end_monotonic_ns=stream_end)
        if not stream_finalization_incomplete:
            driver.finalize()
    else:
        driver = CycleRunDriver(window, envelope=envelope, persist=False)
        driver.run(ordered_attempts)
    expected_admissions = tuple(
        {
            "attempt_index": record.get("attempt_index"),
            "scenario": record.get("scenario"),
            "accepted": record.get("accepted"),
            "quote_version_id": record.get("quote_version_id"),
            "decision_monotonic_ns": record.get("decision_monotonic_ns"),
            "reason": record.get("reason"),
        }
        for record in admissions
    )
    actual_admissions = tuple(
        {
            "attempt_index": record.get("attempt_index"),
            "scenario": admission.scenario.value,
            "accepted": admission.accepted,
            "quote_version_id": admission.quote_version_id,
            "decision_monotonic_ns": admission.decision_monotonic_ns,
            "reason": admission.reason,
        }
        for record, admission in zip(admissions, driver.admissions)
    )
    if len(admissions) != len(driver.admissions) or expected_admissions != actual_admissions:
        raise CycleEvidenceIntegrityError("persisted S3 admissions do not replay identically")
    expected_result_digests = tuple(record.get("result_sha256") for record in results)
    actual_result_digests = tuple(cycle_result_digest(result) for result in driver.results)
    prefix_incomplete = (
        resource_limit_terminal
        and incomplete_evidence in {"CYCLE_FINALIZATION_PREFIX", "FINAL_RESULT_PREFIX"}
    )
    if prefix_incomplete:
        result_prefix = actual_result_digests[: len(expected_result_digests)]
        if len(expected_result_digests) >= len(actual_result_digests):
            raise CycleEvidenceIntegrityError("S3 final-result prefix marker has no omitted suffix")
        results_match = (
            len(expected_result_digests) <= len(actual_result_digests)
            and expected_result_digests == result_prefix
        )
    else:
        results_match = (
            len(results) == len(actual_result_digests)
            and expected_result_digests == actual_result_digests
        )
    if not results_match:
        raise CycleEvidenceIntegrityError("persisted S3 cycle results do not replay identically")
    resource_incomplete = (
        prefix_incomplete
        or stream_finalization_incomplete
        or cycle_finalization_incomplete
    )
    report_results = (
        ()
        if resource_incomplete
        else tuple(driver.results)
    )
    return _RunBundle(
        path=path,
        metadata=metadata,
        attempts=ordered_attempts,
        streaming=streaming,
        stream_inputs=tuple(item for _, item in stream_inputs_raw),
        admissions=tuple(admissions),
        signal_skips=tuple(signal_skips),
        results=tuple(results),
        replay_admissions=tuple(driver.admissions),
        replay_results=tuple(driver.results),
        report_results=report_results,
        resource_incomplete=resource_incomplete,
        terminal=terminals[0]["kind"],
        terminal_reason=terminal_reason,
        incomplete_evidence=incomplete_evidence,
        record_count=len(records),
        byte_count=path.stat().st_size,
    )


def _decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _identity_tokens(result: CycleResult) -> frozenset[str]:
    measurement = result.entry_measurement
    if measurement is None or not measurement.is_proven_fill or not measurement.fills:
        return frozenset()
    identities: set[str] = set()
    for fill in measurement.fills:
        identity = fill.source_identity
        if not isinstance(identity, CausalSourceIdentity):
            continue
        order_ids = (identity.maker_order_id, identity.taker_order_id)
        available_orders = {value for value in order_ids if value is not None}
        if available_orders:
            identities.update(available_orders)
        elif identity.source_event_id is not None:
            identities.add(identity.source_event_id)
        elif identity.source_trade_id is not None:
            identities.add(identity.source_trade_id)
    return frozenset(identities)


def _dependence_groups(
    results: Sequence[CycleResult],
) -> tuple[dict[int, str], int]:
    """Merge overlapping exact identities transitively within one lane."""

    parent = list(range(len(results)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    owners: dict[str, int] = {}
    tokens_by_index: dict[int, frozenset[str]] = {}
    for index, result in enumerate(results):
        tokens = _identity_tokens(result)
        tokens_by_index[index] = tokens
        for token in tokens:
            previous = owners.get(token)
            if previous is not None:
                union(previous, index)
            else:
                owners[token] = index
    component_tokens: defaultdict[int, set[str]] = defaultdict(set)
    for index, tokens in tokens_by_index.items():
        component_tokens[find(index)].update(tokens)
    keys = {
        root: "|".join(sorted(tokens))
        for root, tokens in component_tokens.items()
        if tokens
    }
    result_keys = {
        index: keys[find(index)]
        for index, tokens in tokens_by_index.items()
        if tokens and find(index) in keys
    }
    unresolved = sum(
        result.entry_measurement is not None
        and result.entry_measurement.is_proven_fill
        and bool(result.entry_measurement.fills)
        and not tokens_by_index[index]
        for index, result in enumerate(results)
    )
    return result_keys, unresolved


def _group_key(result: CycleResult) -> str | None:
    """Return the local identity key for compatibility with prefix callers."""

    tokens = _identity_tokens(result)
    return "|".join(sorted(tokens)) if tokens else None


def _result_is_unresolved(result: CycleResult) -> bool:
    return result.status is CycleTerminalState.UNRESOLVED or result.status is CycleTerminalState.PENDING or not result.is_flat


def _scenario_report(results: Sequence[CycleResult], scenario: CycleScenario) -> dict[str, Any]:
    selected = tuple(result for result in results if result.scenario is scenario)
    complete = tuple(result for result in selected if result.pnl_usd is not None and result.is_flat)
    normal = sum(result.status is CycleTerminalState.NORMAL for result in selected)
    forced = sum(result.status is CycleTerminalState.FORCED for result in selected)
    aborted = sum(result.status is CycleTerminalState.ABORTED for result in selected)
    unresolved = sum(_result_is_unresolved(result) for result in selected)
    pnls = tuple(result.pnl_usd for result in complete if result.pnl_usd is not None)
    total = sum(pnls, _ZERO)
    group_keys, unresolved_identity_count = _dependence_groups(selected)
    groups: dict[str, Decimal] = defaultdict(lambda: _ZERO)
    for index, result in enumerate(selected):
        if result not in complete:
            continue
        group = group_keys.get(index)
        if group is not None:
            groups[group] += result.pnl_usd or _ZERO
    best_group = max(groups, key=lambda value: (groups[value], value)) if groups else None
    without_best = total - groups[best_group] if best_group is not None else None
    worst = min(complete, key=lambda result: (result.pnl_usd or _ZERO, result.quote_version_id)) if complete else None
    forced_results = tuple(
        result
        for result in complete
        if result.forced or result.unmatched_entry_quantity > 0
    )
    holding = sum((result.holding_duration_ns or 0 for result in selected), 0)
    unmatched = sum((result.unmatched_exposure_duration_ns or 0 for result in selected), 0)
    return {
        "scenario": scenario.value,
        "cycle_count": len(selected),
        "complete_cycle_count": len(complete),
        "normal_count": normal,
        "forced_count": forced,
        "aborted_count": aborted,
        "unresolved_count": unresolved,
        "pnl_count": len(pnls),
        "total_pnl_usd": _decimal_text(total) if pnls else None,
        "mean_pnl_usd": _decimal_text(total / len(pnls)) if pnls else None,
        "gross_profit_usd": _decimal_text(sum((value for value in pnls if value > 0), _ZERO)),
        "gross_loss_usd": _decimal_text(sum((value for value in pnls if value < 0), _ZERO)),
        "negative_cycle_count": sum(value < 0 for value in pnls),
        "worst_cycle": None if worst is None else {"quote_version_id": worst.quote_version_id, "pnl_usd": str(worst.pnl_usd)},
        "turnover_usd": _decimal_text(sum((result.turnover_usd for result in selected), _ZERO)),
        "holding_duration_seconds": _decimal_text(Decimal(holding) / Decimal(1_000_000_000)),
        "occupancy_holding_duration_seconds": _decimal_text(Decimal(holding) / Decimal(1_000_000_000)),
        "unmatched_exposure_duration_seconds": _decimal_text(Decimal(unmatched) / Decimal(1_000_000_000)),
        "filled_entry_dependence_group_count": len(groups),
        "filled_entry_identity_unresolved_count": unresolved_identity_count,
        "forced_or_unmatched_pnl_usd": _decimal_text(sum((result.pnl_usd or _ZERO for result in forced_results), _ZERO)),
        "forced_unmatched_exit_contribution_usd": _decimal_text(sum((result.pnl_usd or _ZERO for result in forced_results), _ZERO)),
        "best_dependence_group": best_group,
        "total_without_best_dependence_group_usd": _decimal_text(without_best),
        "funding_status": "UNKNOWN_EXECUTION_ONLY",
        "positive_is_hypothetical_only": True,
    }


def _annotate_result_metrics(
    report: dict[str, Any],
    *,
    resource_incomplete: bool,
    terminal_reason: str | None,
    incomplete_evidence: str | None,
) -> dict[str, Any]:
    if not resource_incomplete:
        report.update(
            {
                "cycle_result_metrics_complete": True,
                "cycle_result_metrics_status": "COMPLETE",
                "cycle_result_metrics_note": None,
            }
        )
        return report
    report.update(
        {
            "cycle_result_metrics_complete": False,
            "cycle_result_metrics_status": "UNAVAILABLE_RESOURCE_LIMIT",
            "cycle_result_metrics_note": (
                "Cycle-result metrics are unavailable: the persisted evidence is an "
                f"explicit {incomplete_evidence} after {terminal_reason}. Zero-valued "
                "counts and empty-prefix aggregates do not mean observed zero exposure."
            ),
        }
    )
    for field in _UNAVAILABLE_RESULT_METRICS:
        report[field] = None
    return report


def _window_summary(bundle: _RunBundle) -> dict[str, Any]:
    primary = _annotate_result_metrics(
        _scenario_report(bundle.report_results, CycleScenario.PRIMARY),
        resource_incomplete=bundle.resource_incomplete,
        terminal_reason=bundle.terminal_reason,
        incomplete_evidence=bundle.incomplete_evidence,
    )
    stress = _annotate_result_metrics(
        _scenario_report(bundle.report_results, CycleScenario.STRESS),
        resource_incomplete=bundle.resource_incomplete,
        terminal_reason=bundle.terminal_reason,
        incomplete_evidence=bundle.incomplete_evidence,
    )
    metadata = bundle.metadata
    return {
        "run_id": next(iter_records(bundle.path)).get("run_id"),
        "campaign_id": metadata["campaign_id"],
        "window_id": metadata["window_id"],
        "window_start_utc": metadata["window_start_utc"],
        "window_end_utc": metadata["window_end_utc"],
        "day": metadata["window_start_utc"][:10],
        "terminal": bundle.terminal,
        "terminal_reason": bundle.terminal_reason,
        "incomplete_evidence": bundle.incomplete_evidence,
        "cycle_result_metrics_complete": not bundle.resource_incomplete,
        "cycle_result_metrics_status": (
            "UNAVAILABLE_RESOURCE_LIMIT" if bundle.resource_incomplete else "COMPLETE"
        ),
        "persisted_final_result_count": len(bundle.results),
        "replayed_final_result_count": len(bundle.replay_results),
        "record_count": bundle.record_count,
        "byte_count": bundle.byte_count,
        "primary": primary,
        "stress": stress,
        "skipped_signal_count": sum(not bool(record.get("accepted")) for record in bundle.admissions)
        + len(bundle.signal_skips),
        "skipped_signal_reasons": sorted(
            {
                str(record.get("reason"))
                for record in bundle.admissions
                if not bool(record.get("accepted"))
            }
            | {str(record.get("reason")) for record in bundle.signal_skips}
        ),
    }


def _paths_for_report(path: str | os.PathLike[str]) -> tuple[Path, ...]:
    selected = Path(path)
    if selected.is_file():
        return (selected,)
    if selected.is_dir():
        paths = tuple(sorted(selected.glob("run-*/evidence.jsonl")))
        if paths:
            return paths
    raise CycleEvidenceIntegrityError(f"no S3 evidence JSONL found at {selected}")


def _validate_report_window_set(
    windows: Sequence[CycleWindow],
    *,
    exact_campaign: bool,
    envelope: CycleEnvelope | None = None,
) -> None:
    """Validate identities for both partial descriptive reports and campaigns."""

    if not windows:
        raise CycleEvidenceIntegrityError("S3 report has no windows")
    if len({window.window_id for window in windows}) != len(windows):
        raise CycleEvidenceIntegrityError("S3 report contains duplicate window identities")
    if len({window.campaign_id for window in windows}) != 1:
        raise CycleEvidenceIntegrityError("S3 report windows use different campaign identities")
    ordered = tuple(sorted(windows, key=lambda window: window.start_utc))
    if any(current.start_utc < previous.end_utc for previous, current in zip(ordered, ordered[1:])):
        raise CycleEvidenceIntegrityError("S3 report windows overlap")
    if exact_campaign:
        validate_cycle_windows(windows, envelope=envelope)


def build_cycle_report(path: str | os.PathLike[str]) -> dict[str, Any]:
    """Replay one run or a campaign root and return deterministic metrics."""

    paths = _paths_for_report(path)
    if len(paths) > 4:
        raise CycleEvidenceIntegrityError("S3 campaign report contains more than four windows")
    bundles = tuple(_read_run(item) for item in paths)
    run_ids = [next(iter_records(bundle.path)).get("run_id") for bundle in bundles]
    if len(set(run_ids)) != len(run_ids):
        raise CycleEvidenceIntegrityError("S3 report contains duplicate run identities")
    metadata = bundles[0].metadata
    campaign_id = metadata["campaign_id"]
    policy_fingerprint = metadata["policy_fingerprint"]
    accepted_release = metadata["accepted_release"]
    evidence_mode = metadata["evidence_mode"]
    if any(bundle.metadata["evidence_mode"] != evidence_mode for bundle in bundles[1:]):
        raise CycleEvidenceIntegrityError("S3 report mixes fixture and observed-public provenance")
    envelope_data = metadata["envelope"]
    if not isinstance(envelope_data, Mapping):
        raise CycleEvidenceIntegrityError("S3 report envelope metadata is malformed")
    envelope = CycleEnvelope(
        **{field.name: envelope_data[field.name] for field in fields(CycleEnvelope)}
    )
    for bundle in bundles[1:]:
        if (
            bundle.metadata["campaign_id"] != campaign_id
            or bundle.metadata["policy_fingerprint"] != policy_fingerprint
            or bundle.metadata["accepted_release"] != accepted_release
        ):
            raise CycleEvidenceIntegrityError("S3 campaign or policy identity mismatch across windows")
        if bundle.metadata.get("envelope") != envelope_data:
            raise CycleEvidenceIntegrityError("S3 campaign envelope mismatch across windows")
    manifest_hashes = {
        bundle.metadata.get("manifest_sha256") for bundle in bundles
    }
    if evidence_mode == "OBSERVATIONAL":
        if None in manifest_hashes or len(manifest_hashes) != 1:
            raise CycleEvidenceIntegrityError(
                "observed-public S3 report lacks one immutable manifest identity"
            )
    windows = tuple(
        CycleWindow.from_text(
            campaign_id=bundle.metadata["campaign_id"],
            window_id=bundle.metadata["window_id"],
            start_utc=bundle.metadata["window_start_utc"],
            end_utc=bundle.metadata["window_end_utc"],
            ordinal=bundle.metadata.get("window_ordinal", 0),
            monotonic_start_ns=bundle.metadata["monotonic_start_ns"],
        )
        for bundle in bundles
    )
    campaign_shape_valid = len(windows) == 4
    _validate_report_window_set(
        windows,
        exact_campaign=campaign_shape_valid,
        envelope=envelope,
    )
    observed_public = evidence_mode == "OBSERVATIONAL"
    prospective_public = observed_public and all(
        bundle.metadata.get("prospective") is True for bundle in bundles
    )
    manifest_complete = evidence_mode == "FIXTURE" or (
        len(manifest_hashes) == 1 and None not in manifest_hashes
    )
    campaign_complete = campaign_shape_valid and prospective_public and manifest_complete
    resource_incomplete_run_count = sum(bundle.resource_incomplete for bundle in bundles)
    if resource_incomplete_run_count == 0:
        cycle_result_metrics_status = "COMPLETE"
    elif resource_incomplete_run_count == len(bundles):
        cycle_result_metrics_status = "UNAVAILABLE_RESOURCE_LIMIT"
    else:
        cycle_result_metrics_status = "PARTIAL_RESOURCE_LIMIT"
    summaries = tuple(_window_summary(bundle) for bundle in bundles)
    all_primary = tuple(result for bundle in bundles for result in bundle.report_results if result.scenario is CycleScenario.PRIMARY)
    all_stress = tuple(result for bundle in bundles for result in bundle.report_results if result.scenario is CycleScenario.STRESS)
    primary = _scenario_report(all_primary, CycleScenario.PRIMARY)
    stress = _scenario_report(all_stress, CycleScenario.STRESS)
    if resource_incomplete_run_count == len(bundles):
        primary = _annotate_result_metrics(
            primary,
            resource_incomplete=True,
            terminal_reason="S3_ENVELOPE_LIMIT",
            incomplete_evidence="RESOURCE_LIMITED_PREFIX",
        )
        stress = _annotate_result_metrics(
            stress,
            resource_incomplete=True,
            terminal_reason="S3_ENVELOPE_LIMIT",
            incomplete_evidence="RESOURCE_LIMITED_PREFIX",
        )
    elif resource_incomplete_run_count:
        partial_note = (
            "Cycle-result aggregates are partial: complete runs are reported, while "
            "explicit resource-limited prefixes are excluded."
        )
        for scenario_report in (primary, stress):
            scenario_report.update(
                {
                    "cycle_result_metrics_complete": False,
                    "cycle_result_metrics_status": "PARTIAL_RESOURCE_LIMIT",
                    "cycle_result_metrics_note": partial_note,
                }
            )
    qualifying_windows = tuple(
        summary
        for summary in summaries
        if summary["primary"]["complete_cycle_count"] >= S3_REQUIRED_CYCLES_PER_QUALIFYING_WINDOW
    )
    days: defaultdict[str, Decimal] = defaultdict(lambda: _ZERO)
    for summary in summaries:
        value = summary["primary"]["total_pnl_usd"]
        if value is not None:
            days[summary["day"]] += Decimal(value)
    primary_without_best = primary["total_without_best_dependence_group_usd"]
    floors_pass = (
        campaign_complete
        and primary["complete_cycle_count"] >= S3_REQUIRED_COMPLETE_CYCLES
        and primary["filled_entry_dependence_group_count"] >= S3_REQUIRED_FILLED_GROUPS
        and len(qualifying_windows) >= S3_REQUIRED_WINDOWS_WITH_CYCLES
        and len({summary["day"] for summary in qualifying_windows}) >= 2
    )
    robustness_pass = (
        floors_pass
        and len(days) >= 2
        and all(value > 0 for value in days.values())
        and stress["total_pnl_usd"] is not None
        and Decimal(stress["total_pnl_usd"]) > 0
        and primary_without_best is not None
        and Decimal(primary_without_best) > 0
    )
    aggregate_record_count = sum(bundle.record_count for bundle in bundles)
    aggregate_byte_count = sum(bundle.byte_count for bundle in bundles)
    aggregate_within_caps = (
        aggregate_record_count <= envelope.max_records
        and aggregate_byte_count <= envelope.max_bytes
    )
    structurally_valid = all(bundle.terminal == "RUN_STOP" for bundle in bundles) and aggregate_within_caps
    unresolved_count = primary["unresolved_count"] + stress["unresolved_count"]
    sufficient = (
        structurally_valid
        and unresolved_count == 0
        and floors_pass
        and campaign_complete
    )
    if not structurally_valid or unresolved_count:
        sufficiency_label = "INSUFFICIENT"
    elif not floors_pass:
        sufficiency_label = "INSUFFICIENT"
    else:
        sufficiency_label = "SUFFICIENT"
    if evidence_mode == "FIXTURE":
        usefulness = "FIXTURE_ONLY"
    elif len(bundles) == 1:
        usefulness = "SINGLE_WINDOW_DESCRIPTIVE_ONLY"
    elif not campaign_complete:
        usefulness = "INCOMPLETE_CAMPAIGN"
    elif robustness_pass:
        usefulness = "DESCRIPTIVE_CAMPAIGN_SCREEN_PASS"
    else:
        usefulness = "DESCRIPTIVE_CAMPAIGN_SCREEN_FAIL"
    return {
        "schema_version": S3_SCHEMA_VERSION,
        "experiment_kind": S3_EXPERIMENT_KIND,
        "campaign_id": campaign_id,
        "accepted_release": accepted_release,
        "policy_fingerprint": policy_fingerprint,
        "window_count": len(bundles),
        "campaign_complete": campaign_complete,
        "provenance": {
            "evidence_mode": evidence_mode,
            "fixture_only": evidence_mode == "FIXTURE",
            "observed_public": observed_public,
            "prospective_public": prospective_public,
            "manifest_sha256": next(iter(manifest_hashes)) if manifest_complete and evidence_mode == "OBSERVATIONAL" else None,
            "campaign_eligible": campaign_complete,
        },
        "measurement_validity": "VALID" if structurally_valid else "DATA_INSUFFICIENT",
        "evidence_sufficiency": sufficiency_label,
        "data_quality": {
            "cycle_result_metrics_complete": resource_incomplete_run_count == 0,
            "cycle_result_metrics_status": cycle_result_metrics_status,
            "resource_incomplete_run_count": resource_incomplete_run_count,
            "note": (
                None
                if resource_incomplete_run_count == 0
                else "Cycle-result aggregates exclude explicit resource-limited prefixes; "
                "zero-valued aggregates must not be interpreted as observed zero exposure."
            ),
        },
        "aggregate_caps": {
            "record_count": aggregate_record_count,
            "max_records": envelope.max_records,
            "record_reserve": envelope.record_reserve,
            "byte_count": aggregate_byte_count,
            "max_bytes": envelope.max_bytes,
            "bytes_reserve": envelope.bytes_reserve,
            "within_caps": aggregate_within_caps,
        },
        "economics": {
            "primary": primary,
            "stress": stress,
            "cycle_result_metrics_complete": resource_incomplete_run_count == 0,
            "cycle_result_metrics_status": cycle_result_metrics_status,
            "days_primary_total_pnl_usd": {day: str(value) for day, value in sorted(days.items())},
            "funding_status": "UNKNOWN_EXECUTION_ONLY",
            "pnl_is_hypothetical": True,
        },
        "usefulness": {
            "label": usefulness,
            "campaign_qualification": (
                "FIXTURE_ONLY"
                if evidence_mode == "FIXTURE"
                else "QUALIFIED_DESCRIPTIVE_ONLY"
                if sufficient and robustness_pass
                else "INSUFFICIENT"
            ),
            "complete_cycle_floor": S3_REQUIRED_COMPLETE_CYCLES,
            "filled_group_floor": S3_REQUIRED_FILLED_GROUPS,
            "qualifying_window_floor": S3_REQUIRED_WINDOWS_WITH_CYCLES,
            "cycles_per_window_floor": S3_REQUIRED_CYCLES_PER_QUALIFYING_WINDOW,
            "no_trading_authority": True,
        },
        "envelope": _primitive(envelope),
        "windows": summaries,
    }


def render_cycle_report(path: str | os.PathLike[str], *, format: str = "json") -> str:
    report = build_cycle_report(path)
    if format == "json":
        return json.dumps(report, sort_keys=True, separators=(",", ":"))
    if format != "table":
        raise ValueError("cycle report format must be json or table")
    primary = report["economics"]["primary"]
    stress = report["economics"]["stress"]
    lines = [
        f"campaign={report['campaign_id']} windows={report['window_count']}",
        f"validity={report['measurement_validity']} sufficiency={report['evidence_sufficiency']} usefulness={report['usefulness']['label']}",
        f"cycle_result_metrics={report['data_quality']['cycle_result_metrics_status']}",
        f"primary cycles={primary['complete_cycle_count']} normal={primary['normal_count']} forced={primary['forced_count']} unresolved={primary['unresolved_count']} total_pnl={primary['total_pnl_usd']}",
        f"stress cycles={stress['complete_cycle_count']} normal={stress['normal_count']} forced={stress['forced_count']} unresolved={stress['unresolved_count']} total_pnl={stress['total_pnl_usd']}",
        "funding=UNKNOWN_EXECUTION_ONLY positive_results=hypothetical_only trading_authority=NONE",
    ]
    for window in report["windows"]:
        lines.append(
            f"window={window['window_id']} day={window['day']} primary_pnl={window['primary']['total_pnl_usd']} stress_pnl={window['stress']['total_pnl_usd']} skipped={window['skipped_signal_count']}"
        )
    return "\n".join(lines)


__all__ = [
    "CycleEnvelope",
    "CycleEvidenceError",
    "CycleEvidenceIntegrityError",
    "CycleCampaignManifest",
    "CycleManifestError",
    "CyclePublicPreconditionError",
    "CycleEvidenceWriter",
    "CycleEnvelopeLimitError",
    "CycleRunDriver",
    "CycleRunOutput",
    "PublicCycleProducer",
    "CyclePublicProducer",
    "CycleWindow",
    "CycleWindowClaimError",
    "S3_BYTES_RESERVE",
    "S3_CLOSING_TAIL_SECONDS",
    "S3_ENTRY_CUTOFF_SECONDS",
    "S3_MARKET_DEADLINE_SECONDS",
    "S3_MAX_BYTES",
    "S3_MAX_RECORDS",
    "S3_RECORD_RESERVE",
    "S3_SCHEMA_VERSION",
    "S3_WINDOW_SECONDS",
    "build_cycle_report",
    "cycle_attempt_from_dict",
    "cycle_attempt_to_dict",
    "freeze_cycle_manifest",
    "cycle_policy_fingerprint",
    "cycle_result_digest",
    "cycle_result_payload",
    "cycle_window_fingerprint",
    "fixture_campaign_windows",
    "fixture_cycle_attempts",
    "render_cycle_report",
    "read_cycle_manifest",
    "reserve_cycle_window",
    "run_fixture_campaign",
    "run_fixture_window",
    "run_public_cycle_collection",
    "validate_cycle_windows",
]
