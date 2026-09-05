"""The bounded SCAN-003 fixed public scanner command and report."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
import hashlib
import json
import platform
from pathlib import Path
import subprocess
import time
from typing import Any

import aiohttp

from risex_farmer.exchanges.lighter import LighterAdapter
from risex_farmer.exchanges.risex import RisexAdapter
from risex_farmer.models import Venue

from .config import (
    FIXED_SCANNER_BYTE_CAP,
    FIXED_SCANNER_DIRECTION,
    FIXED_SCANNER_ELIGIBLE_TRADE_LIMIT,
    FIXED_SCANNER_HORIZONS_MS,
    FIXED_SCANNER_LIGHTER_FEE_PROVENANCE,
    FIXED_SCANNER_MARKET,
    FIXED_SCANNER_MARGINS_BPS,
    FIXED_SCANNER_NOTIONAL_USD,
    FIXED_SCANNER_RECORD_CAP,
    FIXED_SCANNER_RISEX_FEE_PROVENANCE,
    FIXED_SCANNER_STAGE_NAMES,
    FIXED_SCANNER_TERMINAL_DRAIN_ALLOWANCE_NS,
    FIXED_SCANNER_WALL_CLOCK_SECONDS,
    fixed_scanner_config,
    fixed_scanner_policy_fields,
    fixed_scanner_policy_fingerprint,
    fixed_scanner_stage_fingerprint,
    is_exact_release,
)
from .feed import PublicFeedRunner, select_public_market_pairs
from .models import FillabilityModel, SampleStopReason, SpreadDirection
from .offline_evaluation import build_fixed_offline_evaluation
from .runner import SpreadObserver, SpreadShadowRunner
from .store import (
    AppendOnlyEvidenceStore,
    ScannerStageClaimError,
    new_run_id,
    reserve_scanner_stage,
)


class ScannerPreconditionError(ValueError):
    """Raised before a fixed scanner can make a public request."""


_REFERENCE_REPORT_MAX_BYTES = 8 * 1024 * 1024
_HASH_CHUNK_BYTES = 1024 * 1024


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(_HASH_CHUNK_BYTES)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _utc(value: str | datetime, *, name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ScannerPreconditionError(f"{name} must be an ISO-8601 UTC timestamp") from exc
    else:
        raise ScannerPreconditionError(f"{name} must be an ISO-8601 UTC timestamp")
    if parsed.tzinfo is None:
        raise ScannerPreconditionError(f"{name} must include a UTC offset")
    return parsed.astimezone(UTC)


def loaded_source_root() -> Path:
    """Return the checkout containing this loaded Spread package."""

    return Path(__file__).resolve().parents[2]


def validate_loaded_release(
    accepted_release: str,
    *,
    source_root: str | Path | None = None,
) -> Path:
    """Require the loaded package to come from one exact clean Git release."""

    if not is_exact_release(accepted_release):
        raise ScannerPreconditionError(
            "--accepted-release must be a full lowercase 40-character Git SHA"
        )
    root = loaded_source_root() if source_root is None else Path(source_root).resolve()
    package_path = Path(__file__).resolve()
    try:
        package_path.relative_to(root)
    except ValueError as exc:
        raise ScannerPreconditionError("loaded Spread package is outside the accepted checkout") from exc
    try:
        head = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--verify", "HEAD^{commit}"],
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        ).stdout.strip()
        status = subprocess.run(
            ["git", "-C", str(root), "status", "--porcelain", "--untracked-files=all"],
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        ).stdout
    except (OSError, subprocess.SubprocessError) as exc:
        raise ScannerPreconditionError("accepted checkout Git identity could not be verified") from exc
    if head != accepted_release:
        raise ScannerPreconditionError("loaded checkout HEAD does not equal --accepted-release")
    if status:
        raise ScannerPreconditionError("accepted checkout is not clean")
    return root


def _fixed_limits() -> dict[str, Any]:
    return {
        "eligible_trade_limit": FIXED_SCANNER_ELIGIBLE_TRADE_LIMIT,
        "wall_clock_seconds": FIXED_SCANNER_WALL_CLOCK_SECONDS,
        "record_cap": FIXED_SCANNER_RECORD_CAP,
        "byte_cap": FIXED_SCANNER_BYTE_CAP,
        "terminal_drain_allowance_ns": FIXED_SCANNER_TERMINAL_DRAIN_ALLOWANCE_NS,
        "fill_count_stop": None,
    }


def _stage_metadata(
    *,
    stage_name: str,
    run_id: str,
    accepted_release: str,
    window_start: datetime,
    window_end: datetime,
    sample_start_utc: datetime,
    sample_start_ns: int,
    cal_reference: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    policy = fixed_scanner_policy_fields(accepted_release)
    stage = {
        "stage_name": stage_name,
        "stage_kind": "PUBLIC",
        "run_id": run_id,
        "accepted_release": accepted_release,
        "policy": policy,
        "policy_fingerprint": fixed_scanner_policy_fingerprint(accepted_release),
        "requested_window_utc": {
            "start_utc": window_start.isoformat(),
            "end_utc": window_end.isoformat(),
        },
        "sample_start": {
            "monotonic_ns": sample_start_ns,
            "utc": sample_start_utc.isoformat(),
        },
        "limits": _fixed_limits(),
    }
    if stage_name == "HOLDOUT-001":
        stage["cal_reference"] = dict(cal_reference or {})
    return stage


def _terminal_record(
    *,
    kind: str,
    stage: Mapping[str, Any],
    observer: SpreadObserver | None,
    terminal_utc: datetime,
    terminal_ns: int,
    fatal_reason: str | None,
    failure_class: str | None = None,
) -> dict[str, Any]:
    if kind not in {"RUN_STOP", "RUN_FAILED"}:
        raise ValueError("fixed scanner terminal kind must be RUN_STOP or RUN_FAILED")
    signal = None if observer is None else observer.sample_stop_signal
    if signal is None:
        stop = {
            "reason": SampleStopReason.INTEGRITY_FAILURE.value,
            "strict_episode_count": 0 if observer is None else observer.strict_episode_count,
            "eligible_trade_count": 0 if observer is None else observer.eligible_trade_count,
            "optimistic_episode_count": 0 if observer is None else observer.optimistic_episode_count,
            "integrity_reason": fatal_reason or "TERMINAL_WITHOUT_SAMPLE_STOP",
            "observed_monotonic_ns": terminal_ns,
        }
    else:
        stop = {
            "reason": signal.reason.value,
            "strict_episode_count": signal.strict_episode_count,
            "eligible_trade_count": signal.eligible_trade_count,
            "optimistic_episode_count": signal.optimistic_episode_count,
            "integrity_reason": signal.integrity_reason,
            "observed_monotonic_ns": signal.observed_monotonic_ns,
        }
    sample_start = stage["sample_start"]
    interval = {
        "start_monotonic_ns": sample_start["monotonic_ns"],
        "end_monotonic_ns": terminal_ns,
        "start_utc": sample_start["utc"],
        "end_utc": terminal_utc.isoformat(),
    }
    policy_fingerprint = stage["policy_fingerprint"]
    stage_payload = {
        "stage_name": stage["stage_name"],
        "stage_kind": stage["stage_kind"],
        "run_id": stage["run_id"],
        "accepted_release": stage["accepted_release"],
        "sample_interval": interval,
        "policy_fingerprint": policy_fingerprint,
    }
    return {
        "kind": kind,
        "failure_class": failure_class,
        "fatal_reason": fatal_reason,
        "stopped_utc" if kind == "RUN_STOP" else "failed_utc": terminal_utc,
        "observed_monotonic_ns": terminal_ns,
        "scan_003": {
            "stage_name": stage["stage_name"],
            "stage_kind": stage["stage_kind"],
            "run_id": stage["run_id"],
            "accepted_release": stage["accepted_release"],
            "policy_fingerprint": policy_fingerprint,
            "sample_interval": interval,
            "stop": stop,
            "stage_fingerprint": fixed_scanner_stage_fingerprint(**stage_payload),
        },
    }


def _reference_report(reference: str | Path | None) -> tuple[Mapping[str, Any], str]:
    if reference is None:
        raise ScannerPreconditionError(
            "HOLDOUT-001 requires --cal-report from an accepted CAL-001 result"
        )
    path = Path(reference)
    if not path.is_file():
        raise ScannerPreconditionError("--cal-report does not identify a readable file")
    try:
        size = path.stat().st_size
        reference_sha256 = _sha256_file(path)
    except OSError as exc:
        raise ScannerPreconditionError("--cal-report is not readable") from exc
    if size > _REFERENCE_REPORT_MAX_BYTES:
        try:
            report = build_fixed_offline_evaluation(path)
        except (OSError, TypeError, ValueError) as exc:
            raise ScannerPreconditionError(
                "--cal-report is not a valid fixed CAL report or evidence stream"
            ) from exc
        return report, reference_sha256
    try:
        with path.open("rb") as handle:
            content = handle.read(_REFERENCE_REPORT_MAX_BYTES + 1)
    except OSError as exc:
        raise ScannerPreconditionError("--cal-report is not readable") from exc
    if len(content) > _REFERENCE_REPORT_MAX_BYTES:
        try:
            report = build_fixed_offline_evaluation(path)
        except (OSError, TypeError, ValueError) as exc:
            raise ScannerPreconditionError(
                "--cal-report is not a valid fixed CAL report or evidence stream"
            ) from exc
        return report, reference_sha256
    try:
        decoded = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        try:
            report = build_fixed_offline_evaluation(path)
        except (OSError, TypeError, ValueError) as exc:
            raise ScannerPreconditionError(
                "--cal-report is not a valid fixed CAL report or evidence stream"
            ) from exc
        return report, reference_sha256
    if not isinstance(decoded, Mapping):
        raise ScannerPreconditionError("--cal-report must contain one report object")
    report = decoded.get("offline_evaluation", decoded)
    if not isinstance(report, Mapping):
        raise ScannerPreconditionError("--cal-report does not contain an offline evaluation")
    return report, reference_sha256


def _validate_holdout_reference(
    reference: str | Path | None,
    *,
    accepted_release: str,
    holdout_window_start: datetime,
) -> dict[str, Any]:
    """Validate the CAL admission contract before any HOLDOUT public request."""

    if not is_exact_release(accepted_release):
        raise ScannerPreconditionError(
            "--accepted-release must be a full lowercase 40-character Git SHA"
        )
    report, reference_sha256 = _reference_report(reference)
    provenance = report.get("provenance")
    selector = report.get("selector")
    issues: list[str] = []
    if report.get("section") != "SCAN_003_FIXED_OFFLINE_EVALUATION":
        issues.append("section")
    for name in (
        "descriptive_only",
        "conditional_entry_edge_only",
        "no_executable_pnl",
        "no_confidence_estimate",
    ):
        if report.get(name) is not True:
            issues.append(name)
    if report.get("stage_verdict") != "CAL_PASS_PROVISIONAL":
        issues.append("stage_verdict")
    if report.get("stage_qualified") is not True or report.get("evidence_outcome") != "POSITIVE":
        issues.append("stage_qualified")
    if not isinstance(provenance, Mapping):
        issues.append("provenance")
        provenance = {}
    if provenance.get("stage_name") != "CAL-001":
        issues.append("provenance.stage_name")
    if provenance.get("stage_kind") != "PUBLIC" or provenance.get("synthetic") is not False:
        issues.append("provenance.public")
    if provenance.get("valid") is not True:
        issues.append("provenance.valid")
    if provenance.get("missing_fields") not in ([], ()) or provenance.get("invalid_fields") not in ([], ()):
        issues.append("provenance.fields")
    run_id = provenance.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        issues.append("provenance.run_id")
    if provenance.get("accepted_release") != accepted_release:
        issues.append("provenance.accepted_release")
    expected_policy = fixed_scanner_policy_fields(accepted_release)
    if provenance.get("policy") != expected_policy:
        issues.append("provenance.policy")
    policy_fingerprint = fixed_scanner_policy_fingerprint(accepted_release)
    if provenance.get("policy_fingerprint") != policy_fingerprint:
        issues.append("provenance.policy_fingerprint")
    terminal = provenance.get("terminal")
    interval = terminal.get("sample_interval") if isinstance(terminal, Mapping) else None
    terminal_end = None
    if not isinstance(interval, Mapping):
        issues.append("provenance.terminal.sample_interval")
    else:
        try:
            terminal_start = _utc(interval["start_utc"], name="CAL sample start")
            terminal_end = _utc(interval["end_utc"], name="CAL sample end")
        except (KeyError, ScannerPreconditionError):
            issues.append("provenance.terminal.sample_interval")
        else:
            if terminal_end < terminal_start:
                issues.append("provenance.terminal.sample_interval.order")
    stage_fingerprint = provenance.get("stage_fingerprint")
    if (
        not isinstance(stage_fingerprint, str)
        or len(stage_fingerprint) != 64
        or any(character not in "0123456789abcdef" for character in stage_fingerprint)
    ):
        issues.append("provenance.stage_fingerprint")
    elif isinstance(run_id, str) and isinstance(interval, Mapping):
        computed = fixed_scanner_stage_fingerprint(
            stage_name="CAL-001",
            stage_kind="PUBLIC",
            run_id=run_id,
            accepted_release=accepted_release,
            sample_interval=interval,
            policy_fingerprint=policy_fingerprint,
        )
        if stage_fingerprint != computed or provenance.get("computed_stage_fingerprint") != computed:
            issues.append("provenance.stage_fingerprint")
    if provenance.get("terminal_kind") != "RUN_STOP":
        issues.append("provenance.terminal_kind")
    if not isinstance(selector, Mapping):
        issues.append("selector")
        selector = {}
    selected_arm = selector.get("selected_margin_bps")
    arm_qualifies = selector.get("arm_qualifies")
    if (
        selected_arm not in {"1", "2"}
        or selector.get("selection_pass") is not True
        or not isinstance(arm_qualifies, Mapping)
        or arm_qualifies.get(selected_arm) is not True
    ):
        issues.append("selector.selected_arm")
    if terminal_end is None or terminal_end > holdout_window_start:
        issues.append("cal_completion_before_holdout_window")
    if issues:
        raise ScannerPreconditionError(
            "--cal-report is not an accepted public CAL-001 result: "
            + ",".join(sorted(set(issues)))
        )
    return {
        "stage_name": "CAL-001",
        "run_id": run_id,
        "accepted_release": accepted_release,
        "policy_fingerprint": policy_fingerprint,
        "stage_fingerprint": stage_fingerprint,
        "reference_sha256": reference_sha256,
        "selected_margin_bps": selected_arm,
        "terminal_end_utc": terminal_end.isoformat(),
    }


async def run_fixed_scanner(
    store_root: str | Path,
    *,
    stage_name: str,
    accepted_release: str,
    window_start_utc: str | datetime,
    window_end_utc: str | datetime,
    cal_report: str | Path | None = None,
    now_utc: Callable[[], datetime] | None = None,
    monotonic_ns: Callable[[], int] | None = None,
    source_root: str | Path | None = None,
) -> dict[str, Any]:
    """Run exactly one CAL/HOLDOUT public stage and return its offline report."""

    if stage_name not in FIXED_SCANNER_STAGE_NAMES:
        raise ScannerPreconditionError("stage_name must be CAL-001 or HOLDOUT-001")
    window_start = _utc(window_start_utc, name="window_start_utc")
    window_end = _utc(window_end_utc, name="window_end_utc")
    if window_end <= window_start:
        raise ScannerPreconditionError("window_end_utc must be after window_start_utc")
    cal_binding = None
    if stage_name == "HOLDOUT-001":
        cal_binding = _validate_holdout_reference(
            cal_report,
            accepted_release=accepted_release,
            holdout_window_start=window_start,
        )
    accepted_root = validate_loaded_release(accepted_release, source_root=source_root)
    clock_utc = now_utc or (lambda: datetime.now(UTC))
    clock_ns = monotonic_ns or time.monotonic_ns

    run_id = new_run_id()
    claimed_utc = clock_utc()
    claim_path = reserve_scanner_stage(
        store_root,
        stage_name=stage_name,
        run_id=run_id,
        accepted_release=accepted_release,
        window_start_utc=window_start.isoformat(),
        window_end_utc=window_end.isoformat(),
        claimed_utc=claimed_utc.isoformat(),
    )
    sample_start_utc = clock_utc()
    sample_start_ns = clock_ns()
    if not window_start <= sample_start_utc < window_end:
        raise ScannerPreconditionError(
            "fixed scanner attempt is outside its supplied prospective UTC window"
        )

    stage = _stage_metadata(
        stage_name=stage_name,
        run_id=run_id,
        accepted_release=accepted_release,
        window_start=window_start,
        window_end=window_end,
        sample_start_utc=sample_start_utc,
        sample_start_ns=sample_start_ns,
        cal_reference=cal_binding,
    )
    metadata = {
        "schema_version": 1,
        "source_commit": accepted_release,
        "accepted_release": accepted_release,
        "python_version": platform.python_version(),
        "evidence_mode": "OBSERVATIONAL",
        "feed_scope": (Venue.RISEX.value, Venue.LIGHTER.value),
        "scanner": "SCAN_003_FIXED",
        "scan_003": stage,
        "created_utc": sample_start_utc,
    }
    store = AppendOnlyEvidenceStore.create(
        store_root,
        metadata=metadata,
        run_id=run_id,
        max_records=FIXED_SCANNER_RECORD_CAP,
        max_bytes=FIXED_SCANNER_BYTE_CAP,
    )
    observer: SpreadObserver | None = None
    feed: PublicFeedRunner | None = None
    terminal_written = False
    terminal_kind: str | None = None
    terminal_error: str | None = None
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            session_risex = RisexAdapter(session)
            session_lighter = LighterAdapter(session)
            pairs = await select_public_market_pairs(
                session_risex,
                session_lighter,
                requested_markets=(FIXED_SCANNER_MARKET,),
                max_markets=1,
            )
            if len(pairs) != 1 or pairs[0].canonical_market != FIXED_SCANNER_MARKET:
                raise ScannerPreconditionError("public catalog did not admit exactly BTC")
            observer = SpreadObserver(
                fixed_scanner_config(),
                pairs,
                store,
                now_utc=clock_utc,
                monotonic_ns=clock_ns,
                sample_started_monotonic_ns=sample_start_ns,
                directions=(SpreadDirection.RISEX_SELL_LIGHTER_BUY,),
                material_stop_enabled=False,
                enforce_sample_deadline=True,
            )
            await observer._append(
                (
                    {
                        "kind": "RUN_START",
                        "markets": (FIXED_SCANNER_MARKET,),
                        "scan_003": {
                            "stage_name": stage_name,
                            "stage_kind": "PUBLIC",
                            "run_id": run_id,
                            "accepted_release": accepted_release,
                            "sample_start": dict(stage["sample_start"]),
                        },
                        "observed_monotonic_ns": sample_start_ns,
                    },
                )
            )
            await observer.flush_pending()
            # Construct the feed only after all fixed preflight and the run
            # start marker are durable.  The feed has public unauthenticated
            # RISEx/Lighter surfaces only.
            feed = PublicFeedRunner(
                session,
                pairs,
                observer.ingress,
                config=fixed_scanner_config(),
                risex_adapter=session_risex,
                lighter_adapter=session_lighter,
                now_utc=clock_utc,
                monotonic_ns=clock_ns,
            )
            await SpreadShadowRunner(feed, observer).run(
                duration_seconds=FIXED_SCANNER_WALL_CLOCK_SECONDS
            )
            fatal_reason = feed.fatal_reason or observer.fatal_reason
            terminal_kind = "RUN_FAILED" if fatal_reason is not None else "RUN_STOP"
            await observer.append_terminal(
                _terminal_record(
                    kind=terminal_kind,
                    stage=stage,
                    observer=observer,
                    terminal_utc=clock_utc(),
                    terminal_ns=clock_ns(),
                    fatal_reason=fatal_reason,
                )
            )
            terminal_written = True
    except BaseException as exc:
        terminal_error = type(exc).__name__
        if not terminal_written:
            fatal_reason = (
                None if feed is None else feed.fatal_reason
            ) or (None if observer is None else observer.fatal_reason)
            terminal_kind = "RUN_FAILED"
            failure_record = _terminal_record(
                kind="RUN_FAILED",
                stage=stage,
                observer=observer,
                terminal_utc=clock_utc(),
                terminal_ns=clock_ns(),
                fatal_reason=fatal_reason or type(exc).__name__,
                failure_class=type(exc).__name__,
            )
            try:
                if observer is None:
                    store.append_batch((failure_record,))
                else:
                    await observer.append_terminal(failure_record)
                terminal_written = True
            except BaseException as marker_exc:
                terminal_error = type(marker_exc).__name__
    finally:
        store.close()

    result = build_fixed_offline_evaluation(store.path, cal_reference=cal_report)
    result["run"] = {
        "run_id": run_id,
        "stage_name": stage_name,
        "accepted_release": accepted_release,
        "store_path": str(store.path),
        "claim_path": str(claim_path),
        "terminal_kind": terminal_kind,
        "terminal_written": terminal_written,
        "runtime_error_class": terminal_error,
        "source_root": str(accepted_root),
    }
    result["store_path"] = str(store.path)
    return result


def render_fixed_evaluation(result: Mapping[str, Any], *, format: str = "json") -> str:
    if format == "json":
        return json.dumps(result, sort_keys=True, separators=(",", ":"), default=str)
    if format != "table":
        raise ValueError("fixed scanner format must be json or table")
    provenance = result.get("provenance")
    stage_name = provenance.get("stage_name") if isinstance(provenance, Mapping) else None
    lines = [
        "RISEx Spread Shadow fixed scanner (SCAN-003)",
        f"stage={stage_name} verdict={result.get('stage_verdict')} evidence={result.get('evidence_outcome')} candidate={result.get('candidate_eligible')}",
        f"market={FIXED_SCANNER_MARKET} direction={FIXED_SCANNER_DIRECTION} notional=${FIXED_SCANNER_NOTIONAL_USD} margins=1/2bps horizons=0/300/500/1000ms",
        "fees=risex_maker_rate=0.0001 "
        f"provenance={FIXED_SCANNER_RISEX_FEE_PROVENANCE}; "
        "lighter_standard_taker_rate=0 "
        f"provenance={FIXED_SCANNER_LIGHTER_FEE_PROVENANCE} latency_ms=300",
        "conditional entry-unit scores; not executable PnL",
    ]
    coverage = result.get("coverage")
    if isinstance(coverage, Mapping):
        raw_units = coverage.get("raw_unit_count")
        clean_units = coverage.get("clean_unit_count")
        raw_strict_complete = (
            raw_units == clean_units
            and coverage.get("contaminated_unit_count") == 0
            and coverage.get("inactive_unit_count") == 0
            and coverage.get("unresolved_unit_count") == 0
        )
        lines.append(
            "coverage "
            f"raw_trades={coverage.get('raw_eligible_trade_count')} "
            f"raw_units={raw_units} clean={clean_units} "
            f"contaminated={coverage.get('contaminated_unit_count')} "
            f"inactive={coverage.get('inactive_unit_count')} "
            f"unresolved={coverage.get('unresolved_unit_count')} "
            f"common={coverage.get('common_eligible_unit_count')} "
            f"raw_strict_complete={raw_strict_complete}"
        )
    arms = result.get("arms")
    if isinstance(arms, Mapping):
        for arm in ("1", "2"):
            row = arms.get(arm)
            if isinstance(row, Mapping):
                lines.append(
                    f"arm={arm} clean_filled_units={row.get('clean_filled_unit_count')} "
                    f"clusters={row.get('distinct_venue_cluster_count')} "
                    f"detection_timestamps={row.get('distinct_detection_timestamp_count')}"
                )
    lines.append(
        "arm horizon filled_units sum mean minimum p05 gross_positive gross_negative negative_count clean_filled_full_hedge_share"
    )
    if isinstance(arms, Mapping):
        for arm in ("1", "2"):
            row = arms.get(arm)
            if not isinstance(row, Mapping):
                continue
            scores = row.get("horizon_scores")
            shares = row.get("full_hedge_shares")
            if not isinstance(scores, Mapping) or not isinstance(shares, Mapping):
                continue
            for horizon in FIXED_SCANNER_HORIZONS_MS:
                stats = scores.get(str(horizon), {})
                share = shares.get(str(horizon), {}).get("share") if isinstance(shares.get(str(horizon)), Mapping) else None
                lines.append(
                    " ".join(
                        (
                            arm,
                            str(horizon),
                            str(row.get("clean_filled_unit_count")),
                            str(stats.get("sum")),
                            str(stats.get("mean")),
                            str(stats.get("minimum")),
                            str(stats.get("p05")),
                            str(stats.get("gross_positive")),
                            str(stats.get("gross_negative")),
                            str(stats.get("negative_count")),
                            str(share),
                        )
                    )
                )
    failed = result.get("failed_gates", ())
    lines.append(f"failed_gates={','.join(str(value) for value in failed) or 'none'}")
    return "\n".join(lines)


def render_fixed_report(
    path: str | Path,
    *,
    format: str = "json",
    cal_reference: str | Path | Mapping[str, Any] | None = None,
) -> str:
    return render_fixed_evaluation(
        build_fixed_offline_evaluation(path, cal_reference=cal_reference),
        format=format,
    )


__all__ = [
    "ScannerPreconditionError",
    "ScannerStageClaimError",
    "loaded_source_root",
    "render_fixed_evaluation",
    "render_fixed_report",
    "run_fixed_scanner",
    "validate_loaded_release",
]
