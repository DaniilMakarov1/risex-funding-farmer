"""Safe command line entry point for the future operator-run HCR-1 adapter."""

from __future__ import annotations

import argparse
import asyncio
from contextlib import nullcontext
from dataclasses import replace
from decimal import Decimal, InvalidOperation
import importlib
import json
import os
from pathlib import Path
from typing import Any, Mapping

from .contracts import Direction, HandoffConfig, OperationMode, PreflightBlocked
from .engine import run_handoff
from .journal import sanitize_exception
from .keychain import (
    KeychainAccessError,
    KeychainConflictError,
    KeychainError,
    KeychainSecretProvider,
    KeychainUnavailableError,
    MacOSKeychainBackend,
    read_hidden_secret,
)
from .local_attempt import (
    LAUNCH_TOKEN,
    LocalAttemptInputError,
    collect_local_attempt_inputs,
    format_local_attempt_result,
    run_local_attempt,
)
from .offline_report import format_report, report_saved_paths, load_saved_cycle_report
from .operator_view import read_lifecycle, lifecycle_lines, result_lines, read_execution_notices, read_launch_failure, opening_margin_refusal
from .random_cycle import (
    MAX_OPENING_ATTEMPTS,
    OpeningMarginReserve,
    RandomCycleConfig,
    RandomCycleEngine,
    _atomic_launch_metadata,
    allocate_cycle_slot,
    select_random_route,
    run_random_cycle,
    terminal_cycle_facts,
)
from .readiness import (
    ReadinessCheck,
    ReadinessConfig,
    ReadinessResult,
    ReadinessSecretProvider,
    ReadOnlyLighterSdkClient,
    run_readiness,
)
from .sdk import LighterSdkClient, MissingSdkError, SdkVersionError
from .series import RobinhoodSeriesConfig, run_series


# Owner-selected opening policy. The operator file must state these values
# explicitly; this constant validates the binding and is not a silent default.
OWNER_OPENING_MARGIN_RESERVE = OpeningMarginReserve(Decimal("0.10"), Decimal("0.02"))


def _parse_opening_margin_reserve(raw: Any) -> OpeningMarginReserve:
    if not isinstance(raw, Mapping) or set(raw) != {"initial_quote", "dispatch_quote"}:
        raise SystemExit("margin_reserve requires initial_quote and dispatch_quote")
    if any(not isinstance(raw[name], str) for name in ("initial_quote", "dispatch_quote")):
        raise SystemExit("margin_reserve amounts must be exact decimal strings")
    try:
        return OpeningMarginReserve(Decimal(raw["initial_quote"]), Decimal(raw["dispatch_quote"]))
    except (InvalidOperation, ValueError) as exc:
        raise SystemExit(f"invalid margin_reserve: {exc}") from exc


def _require_owner_opening_margin_reserve(config: RandomCycleConfig) -> None:
    if config.margin_reserve != OWNER_OPENING_MARGIN_RESERVE:
        raise SystemExit("opening requires explicit per-account margin_reserve "
                         "initial_quote=0.10 and dispatch_quote=0.02 in operator configuration")


class PromptSecretProvider:
    """Read each key from hidden local input only when --execute is explicit."""

    def __init__(self, account_indices: tuple[int, ...], api_key_index: int) -> None:
        self._account_indices = account_indices
        self._api_key_index = api_key_index
        self._values: dict[int, str] = {}

    def private_key(self, account_index: int, api_key_index: int) -> str:
        if api_key_index != self._api_key_index or account_index not in self._account_indices:
            raise ValueError("secret request does not match configured account/key index")
        if account_index not in self._values:
            self._values[account_index] = read_hidden_secret(
                f"Lighter private key for account {account_index} (hidden input): "
            )
        return self._values[account_index]

    def close(self) -> None:
        self._values.clear()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="risex-hood-handoff",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=(
            "Prepare one explicit HOOD close/reopen attempt. Default mode is offline-safe "
            "and never imports the Lighter SDK, prompts for keys, or makes a request."
        ),
        epilog=(
            "HCR-12 local-attempt: omit both price flags to review the fixed automatic one-tick rule "
            "before LAUNCH; keys then load and one fresh public metadata/book observation selects "
            "the exact source price and equal receiver bound. Omitted timing "
            "bounds use finite defaults: freshness 10s, request 5s, order 10s, reconciliation "
            "20s, polling 0.5s, 40 polls, source expiry 300s. HCR-17 random-cycle "
            "previews one random integer size tick and a 20..300 second hold; LAUNCH "
            "then runs exactly one finite opening/close cycle."
        ),
    )
    parser.add_argument('--no-progress', action='store_true',
                        help='disable live terminal rendering; execution and final output are unchanged')
    parser.add_argument(
        "run",
        nargs="?",
        choices=("run", "readiness", "local-attempt", "random-cycle", "simple", "close-positions", "report", "offline-report"),
        help="run the configured utility or the explicit read-only readiness check",
    )
    parser.add_argument(
        "report_inputs",
        nargs="*",
        help="offline report cycle directory or cycle.jsonl (report/offline-report only)",
    )
    parser.add_argument(
        "--path",
        "--report-path",
        "--cycle-dir",
        dest="report_paths",
        action="append",
        type=Path,
        help="saved cycle directory or cycle.jsonl for the offline report; repeat for separate reports",
    )
    parser.add_argument(
        "--format",
        dest="report_format",
        choices=("human", "json", "both"),
        default="human",
        help="offline report output format",
    )
    parser.add_argument(
        "--json",
        dest="report_json",
        action="store_true",
        help="emit only machine-readable offline report JSON",
    )
    parser.add_argument("--receiver-admission", choices=("ws_confirmed", "ack"), help="simple: MARKET after exact WS or experimental positive ACK")
    parser.add_argument("--price-improvement-ticks", type=int, choices=range(1, 6), help="simple: exact improvement, skip when spread is too narrow")
    parser.add_argument("--fanout", action="store_true",
                        help="simple: one LIMIT filled by 2..(ready wallets - 1) MARKETs from the wallet pool")
    parser.add_argument("--config", type=Path, help="JSON operator configuration; all numerical bounds are required")
    parser.add_argument("--market-evidence", type=Path, help="JSON current orderBookDetails/fee/margin evidence")
    parser.add_argument("--source-account-index", type=int)
    parser.add_argument("--receiver-account-index", type=int)
    parser.add_argument("--api-key-index", type=int)
    parser.add_argument("--symbol", "--market-symbol", dest="readiness_symbol")
    parser.add_argument("--quantity", dest="readiness_quantity")
    parser.add_argument("--direction", dest="readiness_direction")
    parser.add_argument("--freshness-seconds", dest="readiness_freshness_seconds", type=float)
    parser.add_argument(
        "--request-timeout-seconds",
        "--read-timeout-seconds",
        dest="readiness_request_timeout_seconds",
        type=float,
    )
    parser.add_argument(
        "--source-limit-price",
        dest="readiness_source_limit_price",
        help="exact explicit source bound; omit together with receiver bound for HCR-12 post-LAUNCH automatic selection",
    )
    parser.add_argument(
        "--receiver-worst-price",
        dest="readiness_receiver_worst_price",
        help="exact explicit receiver bound; one-sided omission is rejected",
    )
    parser.add_argument("--order-timeout-seconds", dest="local_order_timeout_seconds", type=float)
    parser.add_argument("--reconcile-timeout-seconds", dest="local_reconcile_timeout_seconds", type=float)
    parser.add_argument("--poll-interval-seconds", dest="local_poll_interval_seconds", type=float)
    parser.add_argument("--max-poll-count", dest="local_max_poll_count", type=int)
    parser.add_argument("--source-order-lifetime-seconds", dest="local_source_order_lifetime_seconds", type=int)
    parser.add_argument("--client-order-prefix", dest="local_client_order_prefix")
    parser.add_argument(
        "--attempt-dir",
        dest="local_attempt_dir",
        type=Path,
        help="new owner-only directory for the fixed local diagnostic packet and intent journal",
    )
    parser.add_argument("--api-base-url", dest="readiness_api_base_url")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="explicitly opt in to future user-operated SDK signing and one-attempt dispatch",
    )
    parser.add_argument(
        "--i-understand-one-attempt-live-operation",
        action="store_true",
        help="required together with --execute; live verification is otherwise not performed",
    )
    parser.add_argument(
        "--i-understand-series-live-operation",
        action="store_true",
        help="required together with --execute for the explicit Robinhood sequential series path",
    )
    parser.add_argument(
        "--confirm-plan",
        action="store_true",
        help="confirm that the printed exact plan, price bounds and readiness/margin requirements were reviewed",
    )
    parser.add_argument(
        "--defer-incremental-margin-calculation",
        dest="defer_incremental_margin_calculation",
        action="store_true",
        default=None,
        help="explicitly defer only the local incremental opening-margin calculation",
    )
    parser.add_argument(
        "--keychain",
        "--use-keychain",
        "--keychain-reuse",
        dest="keychain",
        action="store_true",
        help="explicitly reuse a matching macOS Keychain credential and save first hidden use",
    )
    parser.add_argument(
        "--keychain-replace",
        dest="keychain_replace",
        action="store_true",
        help="replace the matching Keychain credential with a new hidden value before the run",
    )
    parser.add_argument(
        "--keychain-remove",
        dest="keychain_remove",
        action="store_true",
        help="remove matching local Keychain credentials without creating an SDK client",
    )
    return parser


def _load_json(path: Path, label: str) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"cannot read {label}: {exc}") from exc
    if not isinstance(value, Mapping):
        raise SystemExit(f"{label} must contain one JSON object")
    return value


def _reject_operator_position_overrides(value: Mapping[str, Any], label: str) -> None:
    """Keep exact pre-position fields internal to validated series continuity.

    A direct operator JSON object must always enter paired opening from the
    live flat-account preflight.  The series coordinator supplies these fields
    only after it has proved the previous child and bound the next child to its
    cumulative signed positions.  Reject the canonical names and their small
    input aliases before dataclass construction so an operator cannot use them
    to authorize a non-flat initial run.
    """

    reserved = {
        "expected_source_position",
        "expected_receiver_position",
        "expected_source",
        "expected_receiver",
        "source_position_before",
        "receiver_position_before",
    }
    compact_reserved = {item.replace("_", "") for item in reserved}
    for raw_name in value:
        if not isinstance(raw_name, str):
            continue
        normalized = raw_name.strip().lower().replace("-", "_").replace(" ", "_")
        if normalized in reserved or normalized.replace("_", "") in compact_reserved:
            raise SystemExit(
                f"{label} position override {raw_name!r} is reserved for validated series continuity"
            )


def _config(
    value: Mapping[str, Any],
    *,
    execute: bool,
    defer_incremental_margin_calculation: bool | None = None,
) -> HandoffConfig:
    required = (
        "market_id",
        "direction",
        "quantity",
        "source_limit_price",
        "receiver_worst_price",
        "freshness_seconds",
        "request_timeout_seconds",
        "order_timeout_seconds",
        "reconcile_timeout_seconds",
        "poll_interval_seconds",
        "max_poll_count",
        "source_order_lifetime_seconds",
        "client_order_prefix",
        "journal_path",
        "api_base_url",
        "api_key_index",
    )
    missing = [name for name in required if name not in value]
    if missing:
        raise SystemExit("config is missing required fields: " + ", ".join(missing))
    _reject_operator_position_overrides(value, "config")
    kwargs = dict(value)
    if "defer_incremental_margin_calculation" in kwargs and not isinstance(
        kwargs["defer_incremental_margin_calculation"], bool
    ):
        raise SystemExit(
            "invalid HCR-8 configuration: defer_incremental_margin_calculation must be bool"
        )
    if defer_incremental_margin_calculation is not None:
        kwargs["defer_incremental_margin_calculation"] = defer_incremental_margin_calculation
    # A plan review is an interactive act, never a JSON configuration flag.
    # The only way to set it for this process is the explicit --confirm-plan.
    kwargs.pop("operator_plan_reviewed", None)
    decimal_fields = (
        "quantity",
        "source_limit_price",
        "receiver_worst_price",
        # These four fields are accepted below only for legacy config/journal
        # compatibility; they are no longer HCR-1 admission gates.
        "receiver_price_cap",
        "max_gross_notional",
        "source_fee_budget",
        "receiver_fee_budget",
    )
    for name in decimal_fields:
        if name not in kwargs or kwargs[name] is None:
            continue
        if isinstance(kwargs[name], float):
            raise SystemExit(f"invalid HCR-1 configuration: {name} must be an exact decimal string")
        try:
            parsed = Decimal(str(kwargs[name]))
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise SystemExit(f"invalid HCR-1 configuration: {name} is not a decimal") from exc
        if not parsed.is_finite():
            raise SystemExit(f"invalid HCR-1 configuration: {name} must be finite")
        kwargs[name] = parsed
    integer_fields = (
        "market_id",
        "max_poll_count",
        "source_order_lifetime_seconds",
        "api_key_index",
        "chain_id",
        "auth_token_lifetime_seconds",
    )
    for name in integer_fields:
        if name not in kwargs or kwargs[name] is None:
            continue
        if isinstance(kwargs[name], bool):
            raise SystemExit(f"invalid HCR-1 configuration: {name} must be an integer")
        try:
            parsed = int(str(kwargs[name]))
        except (TypeError, ValueError) as exc:
            raise SystemExit(f"invalid HCR-1 configuration: {name} must be an integer") from exc
        if not isinstance(kwargs[name], int) and str(kwargs[name]).strip() not in {str(parsed), f"+{parsed}"}:
            raise SystemExit(f"invalid HCR-1 configuration: {name} must be an exact integer")
        kwargs[name] = parsed
    for name in (
        "freshness_seconds",
        "request_timeout_seconds",
        "order_timeout_seconds",
        "reconcile_timeout_seconds",
        "poll_interval_seconds",
    ):
        if name in kwargs and not isinstance(kwargs[name], bool):
            try:
                kwargs[name] = float(str(kwargs[name]))
            except (TypeError, ValueError) as exc:
                raise SystemExit(f"invalid HCR-1 configuration: {name} must be numeric") from exc
    try:
        kwargs["direction"] = Direction(str(kwargs["direction"]).upper())
        if "operation_mode" in kwargs:
            kwargs["operation_mode"] = OperationMode.parse(kwargs["operation_mode"])
        kwargs["operator_execution_opt_in"] = execute
        return HandoffConfig(**kwargs)
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"invalid HCR-1 configuration: {exc}") from exc


def _series_config(
    value: Mapping[str, Any],
    *,
    execute: bool,
    defer_incremental_margin_calculation: bool | None = None,
) -> RobinhoodSeriesConfig:
    """Parse the finite HCR-2 config without inventing numerical defaults."""

    required = (
        "market_symbol",
        "direction",
        "total_quantity",
        "desired_slice_quantity",
        "allowed_price_deviation",
        "source_limit_price",
        "receiver_worst_price",
        "freshness_seconds",
        "request_timeout_seconds",
        "order_timeout_seconds",
        "reconcile_timeout_seconds",
        "poll_interval_seconds",
        "max_poll_count",
        "source_order_lifetime_seconds",
        "client_order_prefix",
        "journal_path",
    )
    missing = [name for name in required if name not in value]
    if missing:
        raise SystemExit("series config is missing required fields: " + ", ".join(missing))
    _reject_operator_position_overrides(value, "series config")
    kwargs = dict(value)
    if "defer_incremental_margin_calculation" in kwargs and not isinstance(
        kwargs["defer_incremental_margin_calculation"], bool
    ):
        raise SystemExit(
            "invalid HCR-8 configuration: defer_incremental_margin_calculation must be bool"
        )
    if defer_incremental_margin_calculation is not None:
        kwargs["defer_incremental_margin_calculation"] = defer_incremental_margin_calculation
    if str(kwargs.get("mode", "")).strip().lower() in {"series", "hcr-2"}:
        kwargs.pop("mode", None)
    kwargs.pop("series", None)
    kwargs.pop("operator_plan_reviewed", None)
    decimal_fields = (
        "total_quantity",
        "desired_slice_quantity",
        "allowed_price_deviation",
        "source_limit_price",
        "receiver_worst_price",
    )
    for name in decimal_fields:
        if isinstance(kwargs[name], float):
            raise SystemExit(f"invalid HCR-2 configuration: {name} must be an exact decimal string")
        try:
            parsed = Decimal(str(kwargs[name]))
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise SystemExit(f"invalid HCR-2 configuration: {name} is not a decimal") from exc
        if not parsed.is_finite():
            raise SystemExit(f"invalid HCR-2 configuration: {name} must be finite")
        kwargs[name] = parsed
    for name in ("market_id", "max_poll_count", "source_order_lifetime_seconds", "api_key_index", "chain_id", "auth_token_lifetime_seconds"):
        if name not in kwargs or kwargs[name] is None:
            continue
        if isinstance(kwargs[name], bool):
            raise SystemExit(f"invalid HCR-2 configuration: {name} must be an integer")
        try:
            parsed = int(str(kwargs[name]))
        except (TypeError, ValueError) as exc:
            raise SystemExit(f"invalid HCR-2 configuration: {name} must be an integer") from exc
        if not isinstance(kwargs[name], int) and str(kwargs[name]).strip() not in {str(parsed), f"+{parsed}"}:
            raise SystemExit(f"invalid HCR-2 configuration: {name} must be an exact integer")
        kwargs[name] = parsed
    for name in (
        "freshness_seconds",
        "request_timeout_seconds",
        "order_timeout_seconds",
        "reconcile_timeout_seconds",
        "poll_interval_seconds",
    ):
        if name in kwargs and not isinstance(kwargs[name], bool):
            try:
                kwargs[name] = float(str(kwargs[name]))
            except (TypeError, ValueError) as exc:
                raise SystemExit(f"invalid HCR-2 configuration: {name} must be numeric") from exc
    try:
        kwargs["direction"] = Direction(str(kwargs["direction"]).upper())
        if "operation_mode" in kwargs:
            kwargs["operation_mode"] = OperationMode.parse(kwargs["operation_mode"])
        kwargs["operator_execution_opt_in"] = execute
        return RobinhoodSeriesConfig(**kwargs)
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"invalid HCR-2 configuration: {exc}") from exc


def _readiness_config(args: argparse.Namespace) -> ReadinessConfig:
    readiness_symbol = args.readiness_symbol
    required = {
        "symbol": readiness_symbol,
        "quantity": args.readiness_quantity,
        "direction": args.readiness_direction,
        "source account": args.source_account_index,
        "receiver account": args.receiver_account_index,
        "api key index": args.api_key_index,
        "freshness seconds": args.readiness_freshness_seconds,
        "request timeout seconds": args.readiness_request_timeout_seconds,
    }
    missing = [name for name, value in required.items() if value is None]
    if missing:
        raise SystemExit("readiness is missing required inputs: " + ", ".join(missing))
    try:
        return ReadinessConfig(
            market_symbol=readiness_symbol,
            quantity=args.readiness_quantity,
            direction=args.readiness_direction,
            source_account_index=args.source_account_index,
            receiver_account_index=args.receiver_account_index,
            api_key_index=args.api_key_index,
            freshness_seconds=args.readiness_freshness_seconds,
            request_timeout_seconds=args.readiness_request_timeout_seconds,
            source_limit_price=args.readiness_source_limit_price,
            receiver_worst_price=args.readiness_receiver_worst_price,
            api_base_url=args.readiness_api_base_url or "https://api.rh.lighter.xyz",
        )
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"invalid readiness input: {exc}") from exc


def _random_cycle_config(
    value: Mapping[str, Any],
    *,
    execute: bool,
    plan_reviewed: bool,
    defer_incremental_margin_calculation: bool | None = None,
) -> RandomCycleConfig:
    """Parse the finite HCR-17 operator binding without sampling or reads."""

    required = (
        "market_id",
        "market_symbol",
        "direction",
        "source_account_index",
        "receiver_account_index",
    )
    missing = [name for name in required if name not in value]
    if missing:
        raise SystemExit("random-cycle config is missing required fields: " + ", ".join(missing))
    if "cycle_dir" not in value and "journal_path" not in value:
        raise SystemExit("random-cycle config requires cycle_dir or journal_path")
    _reject_operator_position_overrides(value, "random-cycle config")
    kwargs = dict(value)
    mode = kwargs.pop("mode", None)
    if mode is not None and str(mode).strip().lower() not in {"random-cycle", "hcr-17", "random"}:
        raise SystemExit("random-cycle config mode must be random-cycle")
    kwargs.pop("operator_execution_opt_in", None)
    kwargs.pop("operator_plan_reviewed", None)
    if "margin_reserve" in kwargs:
        kwargs["margin_reserve"] = _parse_opening_margin_reserve(kwargs["margin_reserve"])
    if defer_incremental_margin_calculation is not None:
        kwargs["defer_incremental_margin_calculation"] = defer_incremental_margin_calculation
    for name in (
        "market_id",
        "source_account_index",
        "receiver_account_index",
        "max_poll_count",
        "source_order_lifetime_seconds",
        "api_key_index",
        "chain_id",
        "auth_token_lifetime_seconds",
    ):
        if name not in kwargs or kwargs[name] is None:
            continue
        raw = kwargs[name]
        if isinstance(raw, bool):
            raise SystemExit(f"invalid random-cycle configuration: {name} must be an integer")
        try:
            parsed = int(str(raw))
        except (TypeError, ValueError) as exc:
            raise SystemExit(f"invalid random-cycle configuration: {name} must be an integer") from exc
        if not isinstance(raw, int) and str(raw).strip() not in {str(parsed), f"+{parsed}"}:
            raise SystemExit(f"invalid random-cycle configuration: {name} must be an exact integer")
        kwargs[name] = parsed
    kwargs["operator_execution_opt_in"] = execute
    kwargs["operator_plan_reviewed"] = plan_reviewed
    try:
        kwargs["direction"] = Direction(str(kwargs["direction"]).upper())
        return RandomCycleConfig(**kwargs)
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"invalid random-cycle configuration: {exc}") from exc


def _keychain_provider(
    config: Any,
    account_indices: tuple[int, ...],
    *,
    replace: bool,
    prompt: Any = None,
) -> KeychainSecretProvider:
    """Construct the explicit Keychain provider after config validation."""

    try:
        return KeychainSecretProvider.from_config(
            config,
            account_indices,
            backend=MacOSKeychainBackend(),
            replace=replace,
            prompt=prompt,
        )
    except KeychainError as exc:
        raise SystemExit(f"keychain configuration failed: {_keychain_error_text(exc)}") from None
    except Exception:
        # A native loader or injected adapter must never expose its exception
        # text at this CLI boundary.
        raise SystemExit("keychain configuration failed: Keychain operation failed") from None


def _keychain_error_text(exc: KeychainError) -> str:
    """Describe a Keychain failure without trusting backend exception text."""

    if isinstance(exc, KeychainUnavailableError):
        return "macOS Keychain is unavailable"
    if isinstance(exc, KeychainConflictError):
        return "stored credential exists; explicit replacement is required"
    if isinstance(exc, KeychainAccessError):
        return "macOS Keychain access was denied or failed"
    return "Keychain operation failed"


def _prime_keychain(provider: KeychainSecretProvider, account_indices: tuple[int, ...]) -> None:
    """Resolve all selected credentials before SDK/network construction."""

    try:
        for account_index in account_indices:
            provider.private_key(account_index, provider.api_key_index)
    except KeychainError as exc:
        # Provider and hidden-input errors contain fixed, non-secret text.  A
        # backend's arbitrary exception text is sanitized inside the provider.
        raise SystemExit(f"keychain credential operation failed: {_keychain_error_text(exc)}") from None
    except RuntimeError:
        # Keep prompt/terminal failures fixed-text even if a platform wrapper
        # unexpectedly includes implementation or credential details.
        raise SystemExit("keychain credential operation failed: hidden input unavailable") from None


def _remove_keychain(config: Any, account_indices: tuple[int, ...]) -> int:
    provider = _keychain_provider(config, account_indices, replace=False)
    removed: list[dict[str, Any]] = []
    try:
        for account_index in account_indices:
            try:
                did_remove = provider.remove(account_index)
            except KeychainError as exc:
                raise SystemExit(f"keychain removal failed: {_keychain_error_text(exc)}") from None
            removed.append({"account_index": account_index, "removed": did_remove})
    finally:
        provider.close()
    print(
        json.dumps(
            {
                "outcome": "KEYCHAIN_REMOVED",
                "execution": "LOCAL_KEYCHAIN",
                "bindings": removed,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


async def _run_local_attempt(args: argparse.Namespace) -> int:
    """Collect one local plan, then invoke the existing engine once."""

    if args.config is not None or args.market_evidence is not None:
        raise SystemExit("local-attempt takes direct operator inputs and does not accept config or market-evidence JSON")
    if args.keychain_remove:
        raise SystemExit("local-attempt cannot remove Keychain credentials; use the existing run/readiness removal path")
    if args.confirm_plan:
        raise SystemExit("local-attempt requires its interactive launch confirmation")
    if args.i_understand_series_live_operation:
        raise SystemExit("local-attempt is one PAIRED_OPENING attempt and cannot use the series confirmation")
    if args.i_understand_one_attempt_live_operation and not args.execute:
        raise SystemExit("--i-understand-one-attempt-live-operation requires --execute")
    if args.execute and not args.i_understand_one_attempt_live_operation:
        raise SystemExit("--execute also requires --i-understand-one-attempt-live-operation")
    if args.local_attempt_dir is None:
        raise SystemExit("local-attempt requires --attempt-dir")
    if args.readiness_api_base_url is not None:
        raise SystemExit("local-attempt uses the fixed Robinhood Chain endpoint")

    try:
        inputs = collect_local_attempt_inputs(
            market_symbol=args.readiness_symbol,
            quantity=args.readiness_quantity,
            direction=args.readiness_direction,
            source_account_index=args.source_account_index,
            receiver_account_index=args.receiver_account_index,
            api_key_index=args.api_key_index,
            attempt_dir=args.local_attempt_dir,
            source_limit_price=args.readiness_source_limit_price,
            receiver_worst_price=args.readiness_receiver_worst_price,
            freshness_seconds=args.readiness_freshness_seconds,
            request_timeout_seconds=args.readiness_request_timeout_seconds,
            order_timeout_seconds=args.local_order_timeout_seconds,
            reconcile_timeout_seconds=args.local_reconcile_timeout_seconds,
            poll_interval_seconds=args.local_poll_interval_seconds,
            max_poll_count=args.local_max_poll_count,
            source_order_lifetime_seconds=args.local_source_order_lifetime_seconds,
            client_order_prefix=args.local_client_order_prefix,
            defer_incremental_margin_calculation=bool(args.defer_incremental_margin_calculation),
            automatic_price_selection=(
                args.readiness_source_limit_price is None
                and args.readiness_receiver_worst_price is None
            ),
        )
    except LocalAttemptInputError as exc:
        raise SystemExit(str(exc)) from None
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"invalid local-attempt input: {exc}") from None

    account_indices = (inputs.source_account_index, inputs.receiver_account_index)

    def secret_provider_factory(local_inputs: Any) -> Any:
        if args.keychain or args.keychain_replace:
            try:
                return _keychain_provider(
                    local_inputs.readiness_config(),
                    account_indices,
                    replace=args.keychain_replace,
                )
            except SystemExit as exc:
                raise RuntimeError("keychain credential operation failed") from exc
        return PromptSecretProvider(account_indices, local_inputs.api_key_index)

    result = await run_local_attempt(
        inputs,
        execute=args.execute,
        output_fn=print,
        secret_provider_factory=secret_provider_factory,
    )
    print(format_local_attempt_result(result))
    return result.exit_code


_DEFAULT_SIMPLE_OPERATOR_RELATIVE = Path(
    "spread-shadow-runs/hood-cycle-race-latency-20260920/operator-v1"
)


def _simple_operator_dir(config_path: Path) -> Path:
    configured = os.environ.get("RISEX_HOOD_OPERATOR_DIR")
    if configured:
        return Path(configured)
    return config_path.parent


def _simple_config_path(explicit: Path | None) -> Path:
    if explicit is not None:
        return explicit
    configured = os.environ.get("RISEX_HOOD_CONFIG")
    if configured:
        return Path(configured)
    root = Path(os.environ.get("RISEX_SPREAD_SHADOW_ROOT", Path.cwd()))
    return root / _DEFAULT_SIMPLE_OPERATOR_RELATIVE / "random-cycle.json"


def _simple_evidence_path(value: Mapping[str, Any], operator_dir: Path, explicit: Path | None) -> Path:
    if explicit is not None:
        return explicit
    for key in ("market_evidence", "market_evidence_path"):
        raw = value.get(key)
        if raw is not None:
            return Path(str(raw))
    return operator_dir / "market-contract.json"


def _simple_config_value(value: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(value)
    for key in ("market_evidence", "market_evidence_path", "operator_dir"):
        result.pop(key, None)
    return result


def _validate_simple_sdk() -> None:
    """Prove the local SDK distribution and module before claiming a slot.

    The simple launcher is deliberately the only path that reaches the
    protected credential/client boundary.  Keeping this check here means a
    system interpreter with no ``lighter`` module fails locally, before a
    cycle directory can become consumed.
    """

    try:
        LighterSdkClient.verify_sdk()
    except MissingSdkError:
        raise SystemExit(
            "Локальная ошибка SDK: требуется установленный lighter-sdk==1.1.2. "
            "Запустите ./start из проекта после установки зависимостей в .venv-hood."
        ) from None
    except SdkVersionError as exc:
        raise SystemExit(
            f"Локальная ошибка SDK: {exc}. Установите ровно lighter-sdk==1.1.2 в .venv-hood."
        ) from None
    except Exception:
        raise SystemExit(
            "Локальная ошибка SDK: не удалось проверить distribution lighter-sdk. "
            "Переустановите зависимости в .venv-hood."
        ) from None

    try:
        importlib.import_module("lighter")
    except Exception:
        raise SystemExit(
            "Локальная ошибка SDK: lighter-sdk==1.1.2 найден, но модуль lighter не импортируется. "
            "Переустановите зависимости в .venv-hood."
        ) from None


def _validate_simple_local_inputs(
    value: Mapping[str, Any],
    *,
    config_path: Path,
    operator_dir: Path,
    evidence_path: Path,
    defer_incremental_margin_calculation: bool | None,
) -> tuple[RandomCycleConfig, Mapping[str, Any]]:
    """Validate all local inputs before the first durable slot mutation."""

    if config_path.is_symlink() or not config_path.is_file():
        raise SystemExit(f"Локальная ошибка конфигурации: файл недоступен: {config_path}")
    if operator_dir.is_symlink() or not operator_dir.exists() or not operator_dir.is_dir():
        raise SystemExit(
            f"Локальная ошибка каталога циклов: каталог недоступен или не является папкой: {operator_dir}"
        )
    try:
        operator_info = operator_dir.stat()
    except OSError:
        raise SystemExit(f"Локальная ошибка каталога циклов: каталог нельзя прочитать: {operator_dir}") from None
    if operator_info.st_uid != os.geteuid() or operator_info.st_mode & 0o077:
        raise SystemExit(
            f"Локальная ошибка каталога циклов: каталог должен быть доступен только владельцу: {operator_dir}"
        )

    try:
        config = _random_cycle_config(
            _simple_config_value(value),
            execute=True,
            plan_reviewed=True,
            defer_incremental_margin_calculation=defer_incremental_margin_calculation,
        )
    except SystemExit as exc:
        detail = str(exc) or "неизвестная ошибка"
        raise SystemExit(f"Локальная ошибка конфигурации: {detail}") from None
    if config.api_key_index is None:
        raise SystemExit("Локальная ошибка конфигурации: требуется api_key_index")

    try:
        evidence = _load_json(evidence_path, "market evidence")
    except SystemExit as exc:
        detail = str(exc) or "файл нельзя прочитать"
        raise SystemExit(f"Локальная ошибка market evidence: {detail}") from None
    return config, evidence


def _pool_summary_line(value: Mapping[str, Any], operator_dir: Path) -> str | None:
    """Local, secret-free description of wallets.json; None without a pool file."""

    from types import SimpleNamespace
    from .wallet_pool import WalletPoolError, load_wallet_pool, pool_path

    if not pool_path(operator_dir).exists() and not pool_path(operator_dir).is_symlink():
        return None
    try:
        pool = load_wallet_pool(operator_dir, SimpleNamespace(
            source_account_index=value.get("source_account_index"),
            receiver_account_index=value.get("receiver_account_index")))
    except WalletPoolError as exc:
        return f"Пул кошельков: файл некорректен ({exc}); цикл не начнётся."
    paused = f", на паузе {len(pool.paused)}" if pool.paused else ""
    return f"Пул кошельков: активных {len(pool.active)}{paused}."


def _print_simple_summary(
    value: Mapping[str, Any],
    config_path: Path,
    operator_dir: Path,
    *,
    use_keychain: bool,
    fanout: bool = False,
) -> None:
    symbol = str(value.get("market_symbol", value.get("symbol", "?"))).upper()
    source = value.get("source_account_index", "?")
    receiver = value.get("receiver_account_index", "?")
    credential_route = "сохранённый Keychain" if use_keychain else "скрытый локальный ввод ключей"
    print(f"\n══ НОВЫЙ ЦИКЛ · {symbol} · Robinhood Chain Mainnet ══")
    print(f"Один реальный Robinhood Chain Mainnet цикл: {symbol}; первый счёт и сторона лимитки случайны.")
    pool_line = _pool_summary_line(value, operator_dir)
    if fanout:
        print(f"{pool_line or 'Пул кошельков не настроен; режим недоступен.'} Режим 1 LIMIT → несколько MARKET: "
              "источник лимитки и 2..(готовые − 1) получателей выбираются случайно; объём лимитки — не меньше "
              "минимального ордера на каждого получателя +10%, делится между ними случайно.")
    elif pool_line is None:
        print(f"Счета: {source} и {receiver}; каждый может первым выставить BUY или SELL (четыре равновероятных варианта).")
    else:
        print(f"{pool_line} Два разных кошелька выбираются случайно из готовых (баланса хватает, позиции нет); "
              "затем первый счёт и сторона — четыре равновероятных варианта.")
    print(f"Улучшение цены: {value.get('price_improvement_ticks', '1 (старое правило)')} тиков.")
    if value.get("receiver_admission") == "ack":
        print("Режим ACK: MARKET без ожидания WS лимитки; LIMIT может отсутствовать или уже исполниться. Проверка стакана сохранена.")
    if value.get("receiver_admission") == "ws_confirmed":
        print("Режим: MARKET после WS-подтверждения лимитки; приоритет перед чужими заявками не гарантирован.")
    print("Для запуска требуется резерв на каждом счёте: 0.10 при выборе объёма и плеча, 0.02 перед ордерами (валюта баланса).")
    print(f"После Enter будет использован {credential_route}; до Enter нет чтения рынка или доступа к ключам.")
    print(f"Конфигурация: {config_path}; каталог результатов: {operator_dir}.")
    print("Нажмите Enter, чтобы запустить один цикл. Введите C или CANCEL для отмены.")


def _simple_confirmation() -> bool:
    try:
        response = input("Подтверждение (Enter = запуск, C/CANCEL = отмена): ")
    except (EOFError, KeyboardInterrupt):
        print("Отменено до запуска: ключи, чтение рынка и ордера не использовались.")
        return False
    if not isinstance(response, str) or response.strip().upper() in {"C", "CANCEL", "N", "NO"}:
        print("Отменено до запуска: ключи, чтение рынка и ордера не использовались.")
        return False
    if response.strip():
        print("Отменено до запуска: для запуска нужно нажать Enter без текста.")
        return False
    return True


def _read_simple_events(path: str | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    try:
        stream = Path(path).open(encoding="utf-8")
    except OSError:
        return []
    events: list[dict[str, Any]] = []
    with stream:
        for line in stream:
            try:
                value = json.loads(line)
            except (TypeError, json.JSONDecodeError):
                continue
            if isinstance(value, Mapping) and isinstance(value.get("event"), str):
                events.append(dict(value))
    return events


def _simple_event_key(row: Mapping[str, Any]) -> tuple[str, int | None, int | None]:
    payload = row.get("payload")
    if not isinstance(payload, Mapping):
        payload = {}
    attempt = payload.get("attempt")
    account_index = payload.get("account_index")
    return (
        str(row.get("event")),
        attempt if isinstance(attempt, int) else None,
        account_index if isinstance(account_index, int) else None,
    )


def _simple_event_line(row: Mapping[str, Any]) -> str | None:
    event = row.get("event")
    payload = row.get("payload")
    if not isinstance(payload, Mapping):
        payload = {}
    attempt = payload.get("attempt")
    if event == "SELECTION_PROVED":
        selection = payload.get("selection")
        if isinstance(selection, Mapping):
            return f"Выбрано: {selection.get('quantity')} единиц, удержание {selection.get('hold_seconds')} с."
    if event == "PREPARATION_ATTEMPT":
        return f"Подготовка: попытка {attempt}/{payload.get('maximum_attempts', MAX_OPENING_ATTEMPTS)}."
    if event == "PREPARATION_RETRY":
        return f"Подготовка не завершена; повтор через {payload.get('delay_seconds')} с."
    if event == "PREPARATION_FAILED":
        return "Подготовка не прошла; проверяю свежий полный снимок."
    if event == "PREPARATION_BLOCKED":
        return opening_margin_refusal(payload.get("reason")) or "Подготовка остановлена: безопасный повтор запрещён."
    if event == "PREPARATION_EXHAUSTED":
        return f"Подготовка остановлена: исчерпан лимит {payload.get('maximum_attempts', MAX_OPENING_ATTEMPTS)} попыток до отправки ордера."
    if event == "PAIR_ATTEMPT_RETRY":
        phase = payload.get("phase") or "paired"
        return f"Парная попытка {phase}: безопасная отмена подтверждена; следующая попытка использует свежие данные и новый identity."
    if event == "PAIR_ATTEMPT_EXHAUSTED":
        return "Парная операция остановлена: общий бюджет из трёх попыток исчерпан."
    if event == "OPENING_PRICE_UPDATED":
        return (
            "Котировка обновлена: "
            f"{payload.get('old_source_price')} → {payload.get('new_source_price')} "
            f"(попытка {attempt})."
        )
    if event == "PREPARATION_ACCEPTED":
        return f"Подготовка принята (попытка {attempt}); запись ещё не отправлялась."
    if event == "OPENING_BOUNDS_REFRESHED":
        return "Свежие минимумы и балансы проверены для сохранённого объёма."
    if event == "OPENING_PLAN_READY":
        return "План открытия готов; до следующей границы ордера не отправляются."
    if event == "FIRST_MUTATION_BOUNDARY":
        return "Граница первой записи пройдена; дальнейшее состояние определяется журналом открытия."
    if event == "OPENING_COMPLETE":
        result = payload.get("result")
        if isinstance(result, Mapping):
            receiver = result.get("receiver")
            if isinstance(receiver, Mapping) and receiver.get("dispatched") is False:
                source = result.get("source")
                source_order = source.get("order") if isinstance(source, Mapping) else None
                source_trades = source.get("trades") if isinstance(source, Mapping) else None

                def _decimal(value: Any) -> Decimal | None:
                    try:
                        parsed = Decimal(str(value))
                    except (InvalidOperation, TypeError, ValueError):
                        return None
                    return parsed if parsed.is_finite() else None

                source_fill = (
                    _decimal(source.get("filled_quantity"))
                    if isinstance(source, Mapping)
                    else None
                )
                order_fill = (
                    _decimal(source_order.get("filled_quantity"))
                    if isinstance(source_order, Mapping)
                    else None
                )
                source_order_id = (
                    source.get("order_id")
                    if isinstance(source, Mapping)
                    else None
                )
                receipt_quantities: list[Decimal] = []
                if isinstance(source_trades, list):
                    for trade in source_trades:
                        if not isinstance(trade, Mapping):
                            receipt_quantities = []
                            break
                        quantity = _decimal(trade.get("quantity"))
                        if quantity is None or quantity <= 0:
                            receipt_quantities = []
                            break
                        receipt_quantities.append(quantity)
                receipt_total = sum(receipt_quantities, Decimal(0))
                exact_receipts = (
                    isinstance(source_trades, list)
                    and bool(source_trades)
                    and isinstance(source_order, Mapping)
                    and isinstance(source_order_id, str)
                    and source_fill is not None
                    and source_fill > 0
                    and order_fill is not None
                    and order_fill > 0
                    and source_order.get("order_id") == source_order_id
                    and receipt_total == source_fill == order_fill
                    and all(
                        isinstance(trade, Mapping)
                        and trade.get("order_id") == source_order_id
                        for trade in source_trades
                    )
                )
                if exact_receipts:
                    return "Источник получил подтверждённое исполнение; ордер приёмника не отправлялся."
                zero_fill_cancel = (
                    isinstance(source, Mapping)
                    and isinstance(source_order, Mapping)
                    and str(source_order.get("status", "")).lower() in {
                        "canceled",
                        "canceled-post-only",
                        "canceled-reduce-only",
                        "canceled-invalid-balance",
                        "canceled-position-not-allowed",
                        "canceled-margin-not-allowed",
                        "canceled-too-much-slippage",
                        "canceled-not-enough-liquidity",
                        "canceled-self-trade",
                        "canceled-expired",
                        "canceled-oco",
                        "canceled-child",
                        "canceled-liquidation",
                    }
                    and source_fill == 0
                    and order_fill == 0
                    and not source_trades
                )
                if zero_fill_cancel:
                    if str(source_order.get("status", "")).lower() == "canceled-post-only":
                        return (
                            "Источник отменён как canceled-post-only без исполнения; "
                            "цикл не открыт, ордер приёмника не отправлялся."
                        )
                    return "Источник подтверждён как zero-fill/cancel; ордер приёмника не отправлялся."
                return "Источник: исполнение не подтверждено; ордер приёмника не отправлялся."
        return "Открытие и его сверка завершены."
    if event == "PRE_RECEIVER_GUARD":
        status = payload.get("status") or payload.get("priority_status") or "UNKNOWN"
        reason = payload.get("priority_reason") or "приоритет не подтверждён"
        if status == "PROVED":
            return "Приоритет source подтверждён свежей книгой; receiver допущен."
        return f"Guard до receiver: {status}; {reason}. Receiver не допускается."
    if event == "HOLD_ANCHORED":
        return f"Удержание начато: {payload.get('hold_seconds')} с от подтверждённого открытия."
    if event == "CLOSING_PLAN_READY":
        return "Закрытие подготовлено по фактическим позициям."
    if event == "CLOSING_COMPLETE":
        return "Закрытие и его сверка завершены."
    if event == "FALLBACK_DISPATCH_RESULT":
        if payload.get("accepted") is True:
            return f"Резервный ордер принят (попытка {attempt}, счёт {payload.get('account_index')})."
        return f"Резервный ордер отклонён (попытка {attempt}, счёт {payload.get('account_index')})."
    if event == "FALLBACK_ORDER_OBSERVATION_REJECTED":
        return "Наблюдение резервного ордера отклонено по конфликту идентичности; сохранена карта несовпадений."
    if event == "FALLBACK_RECONCILED":
        state = payload.get("reconciliation_state") or "UNKNOWN"
        order = payload.get("order")
        status = order.get("status") if isinstance(order, Mapping) else None
        observed_at = payload.get("position_observed_at")
        if state == "TERMINAL_ZERO_FILL":
            detail = f"известный нулевой fill/cancel ({status or 'terminal'})"
        elif state == "FULL_FILL":
            detail = "известный полный fill"
        elif state == "PARTIAL_FILL":
            detail = "известный partial fill"
        else:
            detail = str(state)
        suffix = "" if observed_at is None else f"; позиция наблюдалась в {observed_at}"
        return f"Резервная сверка подтверждена: {detail}{suffix}."
    if event == "FALLBACK_RECONCILIATION_UNKNOWN":
        return "Сверка резервного ордера неизвестна; дальнейшие записи остановлены."
    if event == "FALLBACK_POST_ATTEMPT_ACCOUNT_OBSERVATION":
        source = payload.get("source")
        receiver = payload.get("receiver")
        source_at = source.get("observed_at") if isinstance(source, Mapping) else None
        receiver_at = receiver.get("observed_at") if isinstance(receiver, Mapping) else None
        return (
            "Свежие позиции после fallback: "
            f"источник={source.get('signed_position') if isinstance(source, Mapping) else 'UNKNOWN'} "
            f"(время {source_at}); "
            f"приёмник={receiver.get('signed_position') if isinstance(receiver, Mapping) else 'UNKNOWN'} "
            f"(время {receiver_at})."
        )
    if event == "FALLBACK_ATTEMPT_EVIDENCE":
        return None
    return None


def _simple_durable_state(cycle_dir: Path) -> tuple[bool, str, str, Any, Any]:
    cycle_events = _read_simple_events(str(cycle_dir / "cycle.jsonl"))
    opening_events = _read_simple_events(str(cycle_dir / "opening.jsonl"))
    all_events = [*cycle_events, *opening_events]
    boundary = any(
        row.get("event") == "FIRST_MUTATION_BOUNDARY"
        or str(row.get("event", "")).endswith("_DISPATCH_INTENT")
        for row in all_events
    )

    # CYCLE_COMPLETE is the durable terminal snapshot.  It may be the only
    # source available after a result object was lost, so keep its positions
    # and observation times authoritative for the final renderer.
    source_text = receiver_text = "UNKNOWN"
    source_observed_at = receiver_observed_at = None
    for row in reversed(cycle_events):
        if row.get("event") != "CYCLE_COMPLETE":
            continue
        payload = row.get("payload")
        if not isinstance(payload, Mapping):
            continue
        remaining = payload.get("remaining_positions")
        observed_at = payload.get("remaining_position_observed_at")
        if isinstance(remaining, Mapping) and remaining.get("source") is not None:
            source_text = str(remaining["source"])
        if isinstance(remaining, Mapping) and remaining.get("receiver") is not None:
            receiver_text = str(remaining["receiver"])
        if isinstance(observed_at, Mapping):
            source_observed_at = observed_at.get("source")
            receiver_observed_at = observed_at.get("receiver")
        break
    else:
        # If the process stopped before CYCLE_COMPLETE, retain the latest
        # paired post-attempt account observation.  This is sufficient to
        # report a known durable position without implying that an absent
        # terminal result was successful.
        for row in cycle_events:
            if row.get("event") != "FALLBACK_POST_ATTEMPT_ACCOUNT_OBSERVATION":
                continue
            payload = row.get("payload")
            if not isinstance(payload, Mapping):
                continue
            source = payload.get("source")
            receiver = payload.get("receiver")
            if isinstance(source, Mapping):
                if source.get("signed_position") is not None:
                    source_text = str(source["signed_position"])
                if source.get("observed_at") is not None:
                    source_observed_at = source["observed_at"]
            if isinstance(receiver, Mapping):
                if receiver.get("signed_position") is not None:
                    receiver_text = str(receiver["signed_position"])
                if receiver.get("observed_at") is not None:
                    receiver_observed_at = receiver["observed_at"]
    return boundary, source_text, receiver_text, source_observed_at, receiver_observed_at


async def _stream_simple_progress(
    journal_path: Path,
    emitted_keys: set[tuple[str, int | None, int | None]],
    stop_event: asyncio.Event,
) -> None:
    """Print each durable progress event once while the finite cycle runs."""

    while True:
        rows = await asyncio.to_thread(_read_simple_events, str(journal_path))
        progress = await asyncio.to_thread(read_lifecycle, journal_path)
        for notice_key, notice in await asyncio.to_thread(read_execution_notices, journal_path):
            key = (notice_key, None, None)
            if key not in emitted_keys:
                try:
                    await asyncio.to_thread(print, '\n── ' + notice, flush=True)
                except OSError:
                    return
                emitted_keys.add(key)
        for row in rows:
            key = _simple_event_key(row)
            if key in emitted_keys:
                continue
            line = _simple_event_line(row)
            if row.get('event') in {'OPENING_COMPLETE', 'CLOSING_COMPLETE', 'PREPARATION_ACCEPTED',
                                    'FIRST_MUTATION_BOUNDARY', 'OPENING_BOUNDS_REFRESHED', 'OPENING_PLAN_READY'}:
                continue
            if row.get("event") == "HOLD_ANCHORED" and progress and progress.get("stage") == "HOLD":
                line = "\n══ УДЕРЖАНИЕ ══\n" + "\n".join(lifecycle_lines(progress))
            if line is None:
                continue
            try:
                await asyncio.to_thread(print, line, flush=True)
            except OSError:
                return  # A closed terminal must not replace the trading result.
            emitted_keys.add(key)
        if stop_event.is_set():
            return
        await asyncio.sleep(0.05)


def format_random_cycle_result_ru(
    result: Any,
    *,
    emitted_keys: set[tuple[str, int | None, int | None]] | None = None,
) -> str:
    """Render the simple launch result without replacing the durable JSONL."""

    journal = getattr(result, "journal_path", None)
    if journal:
        try:
            report = load_saved_cycle_report(Path(journal).parent)
            if report["status"] == "COMPLETE":
                outcome = report["cycle"].get("outcome", "UNKNOWN")
                return "\n".join([f"\n══ ИТОГ ЦИКЛА ══\nИтог: {outcome}", *result_lines(report, detailed=True), f"Журнал: {journal}"])
        except Exception:
            pass  # Preserve the existing incomplete-result diagnostic path.

    events = _read_simple_events(getattr(result, "journal_path", None))
    lines: list[str] = []
    emitted = emitted_keys if emitted_keys is not None else set()
    for row in events:
        key = _simple_event_key(row)
        if key in emitted:
            continue
        line = _simple_event_line(row)
        if line is not None:
            lines.append(line)
            emitted.add(key)

    selection = getattr(result, "selection", None)
    if selection is not None and not any(line.startswith("Выбрано:") for line in lines) and not any(
        key[0] == "SELECTION_PROVED" for key in emitted
    ):
        lines.insert(
            0,
            f"Выбрано: {selection.quantity} единиц, удержание {selection.hold_seconds} с.",
        )

    outcome = getattr(getattr(result, "outcome", None), "value", str(getattr(result, "outcome", "UNKNOWN")))
    source = getattr(result, "remaining_source_position", None)
    receiver = getattr(result, "remaining_receiver_position", None)
    journal = getattr(result, "journal_path", None)
    mutation_boundary = False
    durable_source = durable_receiver = "UNKNOWN"
    durable_source_observed_at = durable_receiver_observed_at = None
    if journal:
        (
            mutation_boundary,
            durable_source,
            durable_receiver,
            durable_source_observed_at,
            durable_receiver_observed_at,
        ) = _simple_durable_state(Path(journal).parent)
    source_display = source if source is not None else (
        None if durable_source == "UNKNOWN" else durable_source
    )
    receiver_display = receiver if receiver is not None else (
        None if durable_receiver == "UNKNOWN" else durable_receiver
    )

    def position_text(value: Any) -> str:
        if value is None:
            return "UNKNOWN"
        if isinstance(value, Decimal):
            return format(value, "f")
        return str(value)

    source_text = position_text(source_display)
    receiver_text = position_text(receiver_display)

    source_observed_at = getattr(result, "remaining_source_position_observed_at", None)
    receiver_observed_at = getattr(result, "remaining_receiver_position_observed_at", None)
    if source_observed_at is None:
        source_observed_at = durable_source_observed_at
    if receiver_observed_at is None:
        receiver_observed_at = durable_receiver_observed_at
    fallbacks = tuple(getattr(result, "fallbacks", ()) or ())
    opening = getattr(result, "opening", None)
    closing = getattr(result, "closing", None)
    receiver_phase = closing if closing is not None else opening
    source_account_index = (
        None
        if receiver_phase is None or getattr(receiver_phase, "source", None) is None
        else getattr(receiver_phase.source, "account_index", None)
    )
    receiver_account_index = (
        None
        if receiver_phase is None or getattr(receiver_phase, "receiver", None) is None
        else getattr(receiver_phase.receiver, "account_index", None)
    )
    for fallback in reversed(fallbacks):
        if getattr(fallback, "position_observed_at", None) is None:
            continue
        if getattr(fallback, "account_index", None) == source_account_index:
            if source_observed_at is None:
                source_observed_at = fallback.position_observed_at
        elif getattr(fallback, "account_index", None) == receiver_account_index:
            if receiver_observed_at is None:
                receiver_observed_at = fallback.position_observed_at

    receiver_leg = None if receiver_phase is None else getattr(receiver_phase, "receiver", None)
    receiver_not_dispatched = receiver_leg is not None and getattr(receiver_leg, "dispatched", True) is False
    dispatch_rows = [
        row
        for row in events
        if row.get("event") == "FALLBACK_DISPATCH_RESULT"
        and isinstance(row.get("payload"), Mapping)
    ]
    accepted_fallback = any(row["payload"].get("accepted") is True for row in dispatch_rows)

    def position_line(label: str, value: Any, observed_at: Any) -> str:
        text = position_text(value)
        if observed_at is None:
            return f"{label}={text} (время наблюдения UNKNOWN)"
        return f"{label}={text} (время наблюдения {observed_at})"

    inventory = getattr(result, "inventory", "UNKNOWN")
    if outcome == "SUCCESS" and inventory == "CONFIRMED_FLAT":
        lines.append("Итог: SUCCESS — обе позиции подтверждённо закрыты.")
    elif outcome == "SUCCESS":
        lines.append("Итог: SUCCESS — исполнение завершено, но flat inventory не подтверждён.")
    else:
        may_have_sent = "да или неизвестно" if mutation_boundary else "нет"
        reason = getattr(result, "reason", None) or "результат не подтверждён"
        lines.append(f"Итог: {outcome} — {reason}.")
        lines.append(
            f"Ордер мог быть отправлен: {may_have_sent}; остаток: источник={source_text}, приёмник={receiver_text}."
        )
        if journal:
            lines.append(f"Действие: сохранён журнал {journal}; UNKNOWN нельзя трактовать как flat.")
    paired_execution = getattr(result, "paired_execution", "UNKNOWN")
    economics = getattr(result, "economics", "UNKNOWN")
    lines.append(
        "Классификация: "
        f"paired_execution={paired_execution}; inventory={inventory}; economics={economics}."
    )
    terminal_facts = terminal_cycle_facts(result)
    opening_canceled = False
    closing_canceled = False
    if terminal_facts:
        lines.append("Терминальные факты: " + "; ".join(terminal_facts) + ".")
        opening_canceled = any("cycle not opened" in fact for fact in terminal_facts)
        closing_canceled = any("paired close not completed" in fact for fact in terminal_facts)
        if opening_canceled:
            lines.append(
                "Источник: canceled-post-only без исполнения; цикл не открыт, "
                "ордер приёмника не отправлялся."
            )
        elif closing_canceled:
            recovery = "; восстановление продолжено через fallback" if fallbacks else ""
            lines.append(
                "Закрытие: source canceled-post-only без исполнения; "
                f"парное закрытие не завершено{recovery}."
            )

    opening_receiver_leg = None if opening is None else getattr(opening, "receiver", None)
    if closing_canceled and opening_receiver_leg is not None:
        opening_receiver_order = getattr(opening_receiver_leg, "order", None)
        opening_receiver_status = getattr(opening_receiver_order, "status", None)
        if opening_receiver_status is None and isinstance(opening_receiver_order, Mapping):
            opening_receiver_status = opening_receiver_order.get("status")
        if getattr(opening_receiver_leg, "dispatched", True) is False:
            lines.append(
                "Приёмник открытия: ордер не отправлялся; это не отмена уже отправленного ордера."
            )
        elif opening_receiver_status:
            lines.append(
                f"Приёмник открытия: ордер отправлен; известное состояние {opening_receiver_status}."
            )
        else:
            lines.append("Приёмник открытия: ордер отправлен; конечное состояние не подтверждено.")
    receiver_label = "Приёмник закрытия" if closing_canceled else "Приёмник"
    if receiver_not_dispatched:
        label = receiver_label
        lines.append(f"{label}: ордер не отправлялся; это не отмена уже отправленного ордера.")
    elif receiver_leg is not None:
        receiver_order = getattr(receiver_leg, "order", None)
        receiver_status = getattr(receiver_order, "status", None)
        if receiver_status is None and isinstance(receiver_order, Mapping):
            receiver_status = receiver_order.get("status")
        label = receiver_label
        if receiver_status:
            lines.append(f"{label}: ордер отправлен; известное состояние {receiver_status}.")
        else:
            lines.append(f"{label}: ордер отправлен; конечное состояние не подтверждено.")
    if accepted_fallback:
        lines.append("Fallback: dispatch принят.")
    if fallbacks:
        last_fallback = fallbacks[-1]
        state = getattr(last_fallback, "reconciliation_state", "UNKNOWN")
        if state == "TERMINAL_ZERO_FILL":
            state_text = "известный terminal zero-fill/cancel"
        elif state == "FULL_FILL":
            state_text = "известный полный fill"
        elif state == "PARTIAL_FILL":
            state_text = "известный partial fill"
        elif state == "REJECTED":
            state_text = "известный rejected"
        else:
            state_text = "UNKNOWN"
        lines.append(f"Fallback: последнее состояние — {state_text}.")
    lines.append(
        "Последние позиции: "
        f"{position_line('источник', source_display, source_observed_at)}; "
        f"{position_line('приёмник', receiver_display, receiver_observed_at)}."
    )
    return "\n".join(lines)


async def _run_simple(args: argparse.Namespace) -> int:
    """Run the concise Russian HCR-19 launcher after one Enter confirmation."""

    if args.execute or args.confirm_plan or args.i_understand_one_attempt_live_operation:
        raise SystemExit("simple launcher accepts its single Enter confirmation instead of execution flags")
    config_path = _simple_config_path(args.config)
    value = _load_json(config_path, "simple launcher configuration")
    for name in ("receiver_admission", "price_improvement_ticks"):
        override = getattr(args, name, None)
        if override is not None:
            value[name] = override
    operator_dir = _simple_operator_dir(config_path)
    _print_simple_summary(
        value,
        config_path,
        operator_dir,
        use_keychain=args.keychain or args.keychain_replace,
        fanout=getattr(args, "fanout", False),
    )
    if not _simple_confirmation():
        return 0

    from .operator_control import exclusive_lock
    with exclusive_lock(operator_dir / ".operator-launch.lock"):
        return await _run_simple_confirmed(args, value, config_path, operator_dir)


def _persist_prejournal_launch_failure(cycle_dir: Path, exc: BaseException) -> str | None:
    """Persist only an allowlisted cause; never copy an exception or key text."""
    if (cycle_dir / 'cycle.jsonl').exists():
        return None
    from .operator_recovery import HistoryBoundExceeded, WalletKeyMissing
    from .wallet_pool import WalletPoolError, WalletsUnavailable
    if isinstance(exc, PreflightBlocked) and 'leverage setting is unresolved' in str(exc):
        code = 'PRIOR_LEVERAGE_UNRESOLVED'
    elif isinstance(exc, PreflightBlocked) and 'previous order is unresolved' in str(exc):
        code = 'PRIOR_ORDER_UNRESOLVED'
    elif isinstance(exc, (KeychainError, WalletKeyMissing)):
        code = 'CREDENTIAL_UNAVAILABLE'
    elif isinstance(exc, HistoryBoundExceeded):
        code = 'HISTORY_LIMIT'
    elif isinstance(exc, WalletsUnavailable):
        code = 'WALLETS_UNAVAILABLE'
    elif isinstance(exc, WalletPoolError):
        code = 'WALLET_POOL'
    elif isinstance(exc, PreflightBlocked):
        code = 'PREFLIGHT_REFUSED'
    else:
        code = 'PREPARATION_UNAVAILABLE'
    _atomic_launch_metadata(cycle_dir / 'launch-failure.json', {
        'schema': 'hcr-41-launch-failure-v1',
        'code': code,
        'cycle_dir': str(cycle_dir),
        'inventory': 'UNKNOWN',
        'execution': 'UNKNOWN',
        'cycle_journal_present': False,
    })
    return code


class _NoSigningSecrets:
    """Secret provider for the public wallet-selection reads: it never returns a key."""

    def private_key(self, account_index: int, api_key_index: int) -> str:
        raise KeychainError("wallet selection never reads a private key")

    def close(self) -> None:
        return None


async def _select_pool_wallets(base_config: Any, evidence: Mapping[str, Any], pool: Any,
                              *, fanout: bool = False) -> dict[str, Any]:
    """Read each active wallet publicly and draw one uniform eligible pair (or a fan-out)."""

    from .telegram_accounts import missing_key
    from .wallet_pool import select_wallet_pair

    keys = _keychain_provider(base_config, tuple(pool.active), replace=False, prompt=missing_key)
    reader = None
    try:
        reader = LighterSdkClient(
            base_config,
            source_account_index=base_config.source_account_index,
            receiver_account_index=base_config.receiver_account_index,
            secrets=_NoSigningSecrets(),
            market_evidence=evidence,
        )
        return await select_wallet_pair(base_config, pool, reader, has_key=keys.has_stored_credential,
                                        **({"fanout": True} if fanout else {}))
    finally:
        try:
            if reader is not None:
                await reader.aclose()
        finally:
            keys.close()


def _pool_failure_text(exc: BaseException, selection: Mapping[str, Any] | None) -> str:
    from .wallet_pool import SKIP_REASONS, WalletPoolError, WalletsUnavailable

    if isinstance(exc, WalletsUnavailable) and isinstance(selection, Mapping):
        skipped = ", ".join(f"{item.get('account_index')} — {SKIP_REASONS.get(item.get('reason'), '?')}"
                            for item in selection.get("skipped", [])[:20])
        if selection.get("mode") == "fanout":
            fanout = selection.get("fanout") if isinstance(selection.get("fanout"), Mapping) else {}
            return (f"для режима «несколько MARKET» нужен источник с балансом на лимитку из двух минимальных "
                    f"ордеров +10% и ещё минимум два готовых кошелька (готовы {len(fanout.get('receiver_ready', []))} "
                    f"из {selection.get('pool_size')})" + (f"; пропущены: {skipped}" if skipped else ""))
        return (f"готовых кошельков меньше двух ({len(selection.get('eligible', []))} из "
                f"{selection.get('pool_size')})" + (f"; пропущены: {skipped}" if skipped else ""))
    if isinstance(exc, WalletPoolError):
        return f"файл пула кошельков некорректен: {exc}"
    if isinstance(exc, KeychainError):
        return "доступ к Keychain для проверки ключей не подтверждён"
    return f"не удалось прочитать рынок или кошельки: {sanitize_exception(exc)}"


async def _run_simple_confirmed(args, value, config_path, operator_dir) -> int:
    from .operator_control import stop_requested
    if stop_requested(operator_dir):
        # An owner /stop is pending: no slot is claimed and nothing is read or sent.
        print("Цикл не начат: действует команда /stop (файл stop-request.json рядом с конфигурацией). "
              "Ордера не отправлялись; новая /run в Telegram снимает остановку.")
        return 2
    # Validation is local and secret-free.  The SDK distribution/import,
    # configuration and evidence must all be valid before the new slot can be
    # durably claimed.  No Keychain, client, account reader or network path is
    # touched by these checks.
    evidence_path = _simple_evidence_path(value, operator_dir, args.market_evidence)
    base_config, evidence = _validate_simple_local_inputs(
        value,
        config_path=config_path,
        operator_dir=operator_dir,
        evidence_path=evidence_path,
        defer_incremental_margin_calculation=args.defer_incremental_margin_calculation,
    )
    if base_config.confirmed_pilot:
        raise SystemExit("confirmed pilot uses the dedicated one-cycle command")
    if getattr(args, "fanout", False) and base_config.receiver_admission not in ("ack", "ws_confirmed"):
        # Checked before a slot is claimed: the fan-out cycle refuses any other admission.
        raise SystemExit("режим «несколько MARKET» работает только с приёмом ACK или WS "
                         "(--receiver-admission ack); слот не занят, ордера не отправлялись")
    _require_owner_opening_margin_reserve(base_config)
    _validate_simple_sdk()
    from .wallet_pool import WalletPoolError, WalletsUnavailable, load_wallet_pool
    pool = selection = pool_failure = None
    try:
        pool = load_wallet_pool(operator_dir, base_config)
    except WalletPoolError as exc:
        pool_failure = exc
    pooled = pool is not None and pool.from_file
    fanout = bool(getattr(args, "fanout", False))
    if fanout and pool is not None and not pooled:
        pool_failure = WalletPoolError("режим «несколько MARKET» требует файл пула кошельков wallets.json")
    if pooled and (args.keychain_replace or not args.keychain):
        raise SystemExit("пул кошельков работает только с сохранёнными ключами (--keychain); "
                         "ключ кошелька добавляется командой ./wallet add")
    if pooled:
        # Public balance/position reads of every active wallet; no key value is
        # read and nothing is signed before the pair is drawn and the slot claimed.
        try:
            selection = await _select_pool_wallets(base_config, evidence, pool, fanout=fanout)
        except Exception as exc:
            pool_failure = exc
    extras: list[int] | None = None
    if pool_failure is not None:
        route = None
    elif fanout:
        from .fanout_cycle import select_fanout_route
        drawn = selection["fanout"]
        if drawn["source"] is None:
            route = None
            pool_failure = WalletsUnavailable("no fan-out source with two other ready wallets")
        else:
            route, extras = select_fanout_route(drawn["source"], drawn["receivers"])
    elif pooled:
        pair = selection["pair"]
        route = None if pair is None else select_random_route(
            replace(base_config, source_account_index=pair[0], receiver_account_index=pair[1]))
        if pair is None:
            pool_failure = WalletsUnavailable("fewer than two eligible wallets")
    else:
        route = select_random_route(base_config)
    try:
        cycle_dir, client_order_prefix = allocate_cycle_slot(
            operator_dir,
            client_order_prefix=base_config.client_order_prefix,
            random_route=route,
            wallet_selection=selection,
            fanout_receivers=extras,
        )
    except PreflightBlocked as exc:
        raise SystemExit(f"не удалось занять новый слот цикла: {exc}") from None
    if pool_failure is not None:
        try:
            code = _persist_prejournal_launch_failure(cycle_dir, pool_failure)
        except PreflightBlocked:
            code = None
        print(f"Цикл не начат: {_pool_failure_text(pool_failure, selection)}. "
              f"Ордера не отправлялись; слот {cycle_dir} сохранён" + (f" с причиной {code}." if code else "."))
        return 2
    if extras is not None:
        receivers = ", ".join(str(index) for index in (route["receiver_account_index"], *extras))
        print(f"Кошельки: LIMIT — {route['source_account_index']}, MARKET — {receivers} "
              f"из {len(selection['eligible'])} готовых (всего активных {selection['pool_size']}).")
    elif pooled:
        print(f"Кошельки: выбрана пара {route['source_account_index']} и {route['receiver_account_index']} "
              f"из {len(selection['eligible'])} готовых (всего активных {selection['pool_size']}).")
    launch_value = _simple_config_value(value)
    launch_value.update(route)
    # Fan-out receivers are drawn per cycle; a 1 -> 1 cycle never inherits them.
    launch_value["extra_receiver_account_indices"] = extras or []
    launch_value["cycle_dir"] = str(cycle_dir)
    launch_value.pop("journal_path", None)
    launch_value["client_order_prefix"] = client_order_prefix
    config = _random_cycle_config(
        launch_value,
        execute=True,
        plan_reviewed=True,
        defer_incremental_margin_calculation=args.defer_incremental_margin_calculation,
    )
    _require_owner_opening_margin_reserve(config)
    print(
        f"Новый слот создан и зарезервирован: {cycle_dir}; "
        f"уникальный префикс сохранён в {cycle_dir / 'launch.json'}."
    )
    if config.extra_receiver_account_indices:
        print(f"Выбрано: счёт {config.source_account_index} — LIMIT {config.direction.source_side}; счета "
              f"{', '.join(str(index) for index in config.receiver_account_indices)} — MARKET "
              f"{config.direction.receiver_side}. Выбор сохранён в launch.json.")
    else:
        print(f"Выбрано: первый счёт {config.source_account_index}, лимитный {config.direction.source_side}; "
              f"второй счёт {config.receiver_account_index}, {config.direction.receiver_side}. Выбор сохранён в launch.json.")
    secrets: Any | None = None
    client: LighterSdkClient | None = None
    emitted_keys: set[tuple[str, int | None, int | None]] = set()
    try:
        account_indices = (config.source_account_index, *config.receiver_account_indices)
        if pooled:
            # Every pool wallet's key stays readable for exact history lookups;
            # only the drawn pair is loaded now.  Keys are never prompted here.
            from .telegram_accounts import missing_key
            secrets = _keychain_provider(config, tuple(pool.all), replace=False, prompt=missing_key)
            _prime_keychain(secrets, account_indices)
        elif args.keychain or args.keychain_replace:
            secrets = _keychain_provider(config, account_indices, replace=args.keychain_replace)
            _prime_keychain(secrets, account_indices)
        else:
            secrets = PromptSecretProvider(account_indices, config.api_key_index)
        client = LighterSdkClient(
            config,
            source_account_index=config.source_account_index,
            receiver_account_index=config.receiver_account_index,
            secrets=secrets,
            market_evidence=evidence,
            # Only a fan-out cycle signs for receivers 2..k; 1 -> 1 construction is unchanged.
            **({"extra_receiver_account_indices": config.extra_receiver_account_indices}
               if config.extra_receiver_account_indices else {}),
        )
        from .operator_recovery import resolve_prior
        if pooled:
            client.read_account_indices = tuple(pool.all)
            await resolve_prior(config, client, operator_dir, pool=pool)
        else:
            await resolve_prior(config, client, operator_dir)
        stop_progress = asyncio.Event()
        progress_task = None if getattr(args, 'no_progress', False) else asyncio.create_task(
            _stream_simple_progress(Path(config.journal_path), emitted_keys, stop_progress))
        run_task = asyncio.create_task(run_random_cycle(
            config, client, stop_requested=lambda: stop_requested(operator_dir)))
        await asyncio.sleep(0)
        try:
            result = await run_task
        finally:
            stop_progress.set()
            if progress_task is not None:
                await progress_task
    except asyncio.CancelledError:
        (
            may_have_sent,
            source_text,
            receiver_text,
            source_observed_at,
            receiver_observed_at,
        ) = _simple_durable_state(cycle_dir)
        send_state = "да или неизвестно" if may_have_sent else "нет"
        print(
            f"Прервано: ордер мог быть отправлен — {send_state}; остаток: "
            f"источник={source_text}, приёмник={receiver_text}; сохранён журнал {cycle_dir / 'cycle.jsonl'}. "
            f"Времена наблюдения: источник={source_observed_at if source_observed_at is not None else 'UNKNOWN'}, "
            f"приёмник={receiver_observed_at if receiver_observed_at is not None else 'UNKNOWN'}. "
            "Не повторяйте этот слот и сначала выполните read-only сверку."
        )
        return 2
    except BaseException as exc:
        reason = str(exc) if isinstance(exc, SystemExit) and str(exc) else sanitize_exception(exc)
        try:
            _persist_prejournal_launch_failure(cycle_dir, exc)
        except PreflightBlocked:
            print('Причину отказа не удалось сохранить; исход остаётся UNKNOWN.')
        (
            may_have_sent,
            source_text,
            receiver_text,
            source_observed_at,
            receiver_observed_at,
        ) = _simple_durable_state(cycle_dir)
        send_state = "да или неизвестно" if may_have_sent else "нет"
        print(
            f"Ошибка запуска: {reason}. Ордер мог быть отправлен: {send_state}; "
            f"позиции: источник={source_text}, приёмник={receiver_text}. Действие: сохранён слот {cycle_dir} и журнал "
            f"{cycle_dir / 'cycle.jsonl'}. "
            f"Времена наблюдения: источник={source_observed_at if source_observed_at is not None else 'UNKNOWN'}, "
            f"приёмник={receiver_observed_at if receiver_observed_at is not None else 'UNKNOWN'}. "
            "Не повторяйте его автоматически."
        )
        return 2
    finally:
        if client is not None:
            await client.aclose()
        if secrets is not None:
            secrets.close()
    print(format_random_cycle_result_ru(result, emitted_keys=emitted_keys))
    return 0 if result.outcome.value in {"SUCCESS", "PARTIAL", "PREVIEW"} else 2


async def _run_close_positions(args):
    """Same operator/credential boundary, explicit adoption of current inventory."""
    from dataclasses import replace
    from .operator_control import exclusive_lock
    from .operator_recovery import allocate_close_slot, close_positions
    from .operator_view import close_result_lines

    if args.execute or args.confirm_plan or args.i_understand_one_attempt_live_operation:
        raise SystemExit("close-positions accepts its single Enter confirmation")
    config_path = _simple_config_path(args.config)
    value = _load_json(config_path, "operator configuration")
    operator_dir = _simple_operator_dir(config_path)
    print("\n══ ЗАКРЫТИЕ ПОЗИЦИЙ ══")
    pool_line = _pool_summary_line(value, operator_dir)
    if pool_line is None:
        print(f"Рынок {value.get('market_symbol', '?')}; счета {value.get('source_account_index')} и {value.get('receiver_account_index')}.")
    else:
        print(f"Рынок {value.get('market_symbol', '?')}; все кошельки пула, включая приостановленные. {pool_line}")
    print("Проверить позиции и закрыть имеющиеся ограниченными MARKET/IOC reduce-only ордерами.")
    print("Enter подтверждает реальную операцию; C/CANCEL — отмена.")
    if not _simple_confirmation():
        return 0
    with exclusive_lock(operator_dir / '.operator-launch.lock'):
        evidence_path = _simple_evidence_path(value, operator_dir, args.market_evidence)
        config, evidence = _validate_simple_local_inputs(
            value, config_path=config_path, operator_dir=operator_dir, evidence_path=evidence_path,
            defer_incremental_margin_calculation=args.defer_incremental_margin_calculation)
        _validate_simple_sdk()
        config = replace(config, operator_execution_opt_in=True, operator_plan_reviewed=True)
        from .operator_recovery import WalletKeyMissing, require_pool_credentials
        from .wallet_pool import WalletPoolError, load_wallet_pool
        try:
            pool = load_wallet_pool(operator_dir, config)
        except WalletPoolError as exc:
            raise SystemExit(f"файл пула кошельков некорректен: {exc}") from None
        indices = (config.source_account_index, config.receiver_account_index)
        pool_secrets = None
        if pool.from_file:
            if args.keychain_replace or not args.keychain:
                raise SystemExit("пул кошельков работает только с сохранёнными ключами (--keychain)")
            from .telegram_accounts import missing_key
            pool_secrets = _keychain_provider(config, tuple(pool.all), replace=False, prompt=missing_key)
            try:
                require_pool_credentials(pool_secrets, pool)
            except (WalletKeyMissing, KeychainError) as exc:
                pool_secrets.close()
                raise SystemExit(f"закрытие не начато: {exc}; ключ добавляется командой ./wallet add") from None
        slot = allocate_close_slot(operator_dir)
        secrets = client = None
        try:
            if pool_secrets is not None:
                secrets = pool_secrets
            elif args.keychain or args.keychain_replace:
                secrets = _keychain_provider(config, indices, replace=args.keychain_replace)
                _prime_keychain(secrets, indices)
            else:
                secrets = PromptSecretProvider(indices, config.api_key_index)
            client = LighterSdkClient(config, source_account_index=indices[0], receiver_account_index=indices[1],
                                      secrets=secrets, market_evidence=evidence)
            if pool.from_file:
                client.read_account_indices = tuple(pool.all)

                def pair_client(pair_config):
                    # Mutations stay bound to exactly the pair being closed.
                    pair = LighterSdkClient(pair_config, source_account_index=pair_config.source_account_index,
                                            receiver_account_index=pair_config.receiver_account_index,
                                            secrets=secrets, market_evidence=evidence)
                    pair.read_account_indices = tuple(pool.all)
                    return pair

                result = await close_positions(config, client, operator_dir, slot, pool=pool,
                                               pair_client_factory=pair_client)
            else:
                result = await close_positions(config, client, operator_dir, slot)
        finally:
            try:
                if client is not None:
                    await client.aclose()
            finally:
                if secrets is not None:
                    secrets.close()
        print('\n'.join(close_result_lines(result)))
        print(f"Журнал: {slot / 'close.jsonl'}")
        return 0 if result['status'] == 'CONFIRMED_FLAT' else 2


def _prompt_random_cycle_launch(config: RandomCycleConfig) -> bool:
    """Require the one interactive launch boundary before any secret access."""

    if config.confirmed_pilot:
        print(
            "Confirmed one-cycle BTC pilot pending LAUNCH: "
            f"source={config.source_account_index} receiver={config.receiver_account_index} "
            f"direction={config.direction.value}; exact 0.00020 BTC, at most 40.00 quote "
            "per account, 20-second proved hold, one paired attempt per phase, "
            "read-only stream required; "
            + ("at most one exact 1x setting per account after fresh proof."
               if config.pilot_allow_leverage_update else "no leverage-setting write.")
        )
    else:
        print(
            "HCR-17 random-cycle pending LAUNCH: "
            f"market={config.market_symbol} direction={config.direction.value} "
            f"source={config.source_account_index} receiver={config.receiver_account_index}; "
            "one random legal quantity tick, one hold in [20,300] seconds, then paired reduce-only close."
        )
    try:
        response = input(f"Type {LAUNCH_TOKEN} to launch this one random cycle: ")
    except (EOFError, KeyboardInterrupt):
        print("random-cycle cancelled before LAUNCH; no credentials or orders were used")
        return False
    if not isinstance(response, str) or response.strip() != LAUNCH_TOKEN:
        print("random-cycle cancelled before LAUNCH; no credentials or orders were used")
        return False
    return True


def _claim_confirmed_pilot(config: RandomCycleConfig, config_path: Path) -> None:
    """Consume this one owner authorization before credentials or network use."""
    operator_dir = Path(config.cycle_dir).parent
    if config_path.is_symlink() or config_path.parent.resolve() != operator_dir.resolve():
        raise SystemExit("confirmed pilot config must be in the protected operator directory")
    marker = operator_dir / ".confirmed-pilot-20260924.claim.json"
    try:
        fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    except FileExistsError:
        raise SystemExit("confirmed pilot authorization was already consumed") from None
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as sink:
            json.dump({"schema": "confirmed-pilot-20260924-v1", "cycle_dir": str(config.cycle_dir),
                       "market_id": config.market_id,
                       "accounts": [config.source_account_index, config.receiver_account_index]}, sink)
            sink.write("\n")
            sink.flush()
            os.fsync(sink.fileno())
        parent_fd = os.open(operator_dir, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
    except BaseException:
        # The exclusive claim remains even after a local failure: replay is unsafe.
        raise


async def _run_random_cycle(args: argparse.Namespace, value: Mapping[str, Any]) -> int:
    """Preview or launch exactly one HCR-17 random cycle."""

    if args.keychain_remove:
        raise SystemExit("random-cycle cannot remove Keychain credentials; use the existing run/readiness removal path")
    if args.i_understand_series_live_operation:
        raise SystemExit("random-cycle is one finite cycle and cannot use the series confirmation")
    if args.i_understand_one_attempt_live_operation and not args.execute:
        raise SystemExit("--i-understand-one-attempt-live-operation requires --execute")
    if args.execute and not args.i_understand_one_attempt_live_operation:
        raise SystemExit("--execute also requires --i-understand-one-attempt-live-operation")
    if args.execute and not args.confirm_plan:
        raise SystemExit("random-cycle execution requires --confirm-plan after the offline preview")
    config = _random_cycle_config(
        value,
        execute=args.execute,
        plan_reviewed=args.confirm_plan,
        defer_incremental_margin_calculation=args.defer_incremental_margin_calculation,
    )
    if args.execute and config.confirmed_pilot and (not args.keychain or args.keychain_replace):
        raise SystemExit("confirmed pilot requires existing Keychain credentials without replacement")
    if args.execute:
        _require_owner_opening_margin_reserve(config)
    if not args.execute:
        inverse = Direction.SHORT if config.direction is Direction.LONG else Direction.LONG
        print(
            json.dumps(
                {
                    "outcome": "PREVIEW",
                    "execution": "DISABLED",
                    "message": "offline-safe mode: no SDK import, key prompt, signing, or network request",
                    "mode": "random-cycle",
                    "operation_mode": "PAIRED_OPENING",
                    "venue": "robinhood-chain",
                    "website_url": "https://robinhoodchain.lighter.xyz",
                    "api_base_url": config.api_base_url,
                    "chain_id": config.chain_id,
                    "market_id": config.market_id,
                    "market_symbol": config.market_symbol,
                    "direction": config.direction.value,
                    "source_account_index": config.source_account_index,
                    "receiver_account_index": config.receiver_account_index,
                    "source_side": config.direction.source_side,
                    "receiver_side": config.direction.receiver_side,
                    "source_order_type": "LIMIT",
                    "source_time_in_force": "POST_ONLY",
                    "source_reduce_only": False,
                    "receiver_order_type": "MARKET",
                    "receiver_time_in_force": "IOC",
                    "receiver_reduce_only": False,
                    "close_direction": inverse.value,
                    "close_source_side": inverse.source_side,
                    "close_receiver_side": inverse.receiver_side,
                    "close_source_reduce_only": True,
                    "close_receiver_reduce_only": True,
                    "quantity_policy": ("exact 0.00020 BTC and at most 40.00 quote per account"
                                        if config.confirmed_pilot else
                                        "uniform integer legal size tick after fresh metadata/book/accounts"),
                    "hold_policy": ("exact 20 seconds after proved mutual opening"
                                    if config.confirmed_pilot else
                                    "uniform integer seconds in [20,300] after both opening legs are fully reconciled"),
                    "confirmed_pilot": config.confirmed_pilot,
                    "pilot_allow_leverage_update": config.pilot_allow_leverage_update,
                    "fallback_policy": (
                        "at most one exact reduce-only recovery attempt per account; preserve a proved residual"
                        if config.confirmed_pilot else
                        "repeat reduce-only market attempts for each fresh confirmed residual until exact zero; reconcile, refresh the executable bound and use a unique attempt ID each time; no widening"
                    ),
                    "freshness_seconds": config.freshness_seconds,
                    "request_timeout_seconds": config.request_timeout_seconds,
                    "order_timeout_seconds": config.order_timeout_seconds,
                    "reconcile_timeout_seconds": config.reconcile_timeout_seconds,
                    "poll_interval_seconds": config.poll_interval_seconds,
                    "max_poll_count": config.max_poll_count,
                    "source_order_lifetime_seconds": config.source_order_lifetime_seconds,
                    "auth_token_lifetime_seconds": config.auth_token_lifetime_seconds,
                    "defer_incremental_margin_calculation": config.defer_incremental_margin_calculation,
                    "plan_reviewed": config.operator_plan_reviewed,
                    "journal_path": str(config.journal_path),
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 0
    if args.market_evidence is None:
        raise SystemExit("random-cycle execution requires --market-evidence after the offline preview")
    if config.api_key_index is None:
        raise SystemExit("random-cycle execution requires api_key_index")
    evidence = _load_json(args.market_evidence, "market evidence")
    # This check is deliberately read-only.  The engine creates and claims the
    # slot only after the explicit prompt, while this early validation avoids
    # touching credentials when an old/occupied attempt would be refused.
    try:
        RandomCycleEngine.validate_cycle_directory(config.cycle_dir)
    except PreflightBlocked as exc:
        raise SystemExit(f"random-cycle launch refused: {exc}") from None
    if not _prompt_random_cycle_launch(config):
        return 0
    from .operator_control import exclusive_lock
    lock = (exclusive_lock(Path(config.cycle_dir).parent / '.operator-launch.lock')
            if config.confirmed_pilot else nullcontext())
    with lock:
        if config.confirmed_pilot:
            _claim_confirmed_pilot(config, Path(args.config))
        account_indices = (config.source_account_index, config.receiver_account_index)
        if args.keychain or args.keychain_replace:
            secrets: Any = _keychain_provider(config, account_indices, replace=args.keychain_replace)
            try:
                _prime_keychain(secrets, account_indices)
            except SystemExit:
                secrets.close()
                raise
        else:
            secrets = PromptSecretProvider(account_indices, config.api_key_index)
        client: LighterSdkClient | None = None
        try:
            # Pilot and ordinary cycle share the exact execution engine.
            client = LighterSdkClient(
                config,  # type: ignore[arg-type]
                source_account_index=config.source_account_index,
                receiver_account_index=config.receiver_account_index,
                secrets=secrets,
                market_evidence=evidence,
            )
            if config.confirmed_pilot:
                from .operator_recovery import inspect_current
                await inspect_current(config, client, Path(config.cycle_dir).parent,
                                      require_flat=True)
            result = await run_random_cycle(config, client)
        finally:
            if client is not None:
                await client.aclose()
            secrets.close()
    print(json.dumps(result.as_dict(), sort_keys=True, separators=(",", ":")))
    return 0 if result.outcome.value in {"SUCCESS", "PARTIAL", "PREVIEW"} else 2


async def _run_offline_report(args: argparse.Namespace) -> int:
    """Read saved cycle journals without constructing a client or touching a network."""

    requested: list[Path] = list(args.report_paths or [])
    requested.extend(Path(item) for item in (args.report_inputs or []))
    if not requested:
        raise SystemExit("report requires a cycle directory or cycle.jsonl via --path or a positional path")
    if args.execute or args.keychain or args.keychain_replace or args.keychain_remove:
        raise SystemExit("report is offline-only and cannot use execution or Keychain flags")
    reports = report_saved_paths(requested)
    output_format = "json" if args.report_json else args.report_format
    if len(reports) == 1:
        value: Mapping[str, Any] = reports[0]
    else:
        value = {
            "schema": "hcr-27-offline-cycle-report-batch-v1",
            "status": "COMPLETE" if all(item.get("status") == "COMPLETE" for item in reports) else "INCOMPLETE",
            "reports": reports,
        }
    def human(item):
        path = Path(item['input_path'])
        slot = path.parent if path.name == 'cycle.jsonl' else path
        code = read_launch_failure(slot)
        suffix = '' if code is None else f'\nДо журнала цикла сохранён отказ запуска: {code}; исполнение и позиции UNKNOWN.'
        return format_report(item, output_format='human') + suffix

    if len(reports) > 1 and output_format in {"human", "both"}:
        print("\n\n".join(human(item) for item in reports))
        if output_format == "both":
            print(format_report(value, output_format="json"))
    elif len(reports) == 1 and output_format in {"human", "both"}:
        print(human(reports[0]))
        if output_format == "both":
            print(format_report(reports[0], output_format="json"))
    else:
        print(format_report(value, output_format=output_format))
    # The report itself carries the incomplete/unknown state.  A diagnostic
    # reader remains composable in shell pipelines even when evidence is
    # partial; it never converts that state into a successful result.
    return 0


async def _run_readiness(args: argparse.Namespace) -> int:
    # Reject every execution/configuration flag before config parsing, client
    # creation, SDK import, or hidden key input.  Readiness is a distinct path.
    forbidden = []
    if args.execute:
        forbidden.append("--execute")
    if args.i_understand_one_attempt_live_operation:
        forbidden.append("--i-understand-one-attempt-live-operation")
    if args.i_understand_series_live_operation:
        forbidden.append("--i-understand-series-live-operation")
    if args.confirm_plan:
        forbidden.append("--confirm-plan")
    if args.defer_incremental_margin_calculation:
        forbidden.append("--defer-incremental-margin-calculation")
    if args.config is not None:
        forbidden.append("--config")
    if args.market_evidence is not None:
        forbidden.append("--market-evidence")
    if args.keychain_remove and (args.keychain or args.keychain_replace):
        forbidden.append("--keychain-remove with --keychain/--keychain-replace")
    if forbidden:
        raise SystemExit(
            "readiness is read-only and cannot be combined with execution/configuration flags: "
            + ", ".join(forbidden)
        )
    config = _readiness_config(args)
    account_indices = (config.source_account_index, config.receiver_account_index)
    if args.keychain_remove:
        return _remove_keychain(config, account_indices)
    if args.keychain or args.keychain_replace:
        secrets: Any = _keychain_provider(
            config,
            account_indices,
            replace=args.keychain_replace,
        )
        try:
            _prime_keychain(secrets, account_indices)
        except SystemExit:
            secrets.close()
            raise
    else:
        secrets = ReadinessSecretProvider(account_indices, config.api_key_index)
    client = ReadOnlyLighterSdkClient(
        config,
        source_account_index=config.source_account_index,
        receiver_account_index=config.receiver_account_index,
        secrets=secrets,
    )
    try:
        result = await run_readiness(config, client)
    except Exception as exc:
        result = ReadinessResult(
            outcome="UNKNOWN",
            config=config,
            checks=(
                ReadinessCheck(
                    "readiness",
                    "UNKNOWN",
                    "READINESS_FAILED",
                    "read-only readiness check failed before completion",
                    {"reason": sanitize_exception(exc)},
                ),
            ),
        )
    finally:
        try:
            await client.aclose()
        finally:
            secrets.close()
    print(json.dumps(result.as_dict(), sort_keys=True, separators=(",", ":")))
    return 0 if result.outcome == "READY" else 2


async def _run(args: argparse.Namespace) -> int:
    if args.run != "simple" and any(getattr(args, name, None) is not None for name in ("receiver_admission", "price_improvement_ticks")):
        raise SystemExit("admission/tick overrides are supported only by simple")
    if args.run != "simple" and getattr(args, "fanout", False):
        raise SystemExit("--fanout is supported only by simple")
    if args.run in {"report", "offline-report"}:
        return await _run_offline_report(args)
    if args.run == "simple":
        return await _run_simple(args)
    if args.run == "close-positions":
        return await _run_close_positions(args)
    if args.run == "local-attempt":
        return await _run_local_attempt(args)
    if args.run == "readiness":
        return await _run_readiness(args)
    if args.config is None:
        raise SystemExit("run requires --config")
    config_data = _load_json(args.config, "config")
    is_random_cycle = args.run == "random-cycle" or str(config_data.get("mode", "")).strip().lower() in {
        "random-cycle",
        "hcr-17",
        "random",
    }
    if is_random_cycle:
        return await _run_random_cycle(args, config_data)
    is_series = config_data.get("mode") in {"series", "hcr-2"} or config_data.get("series") is True
    if args.keychain_remove:
        if (
            args.execute
            or args.i_understand_one_attempt_live_operation
            or args.i_understand_series_live_operation
            or args.confirm_plan
            or args.defer_incremental_margin_calculation
        ):
            raise SystemExit(
                "--keychain-remove cannot be combined with execution, plan-review, or deferral flags"
            )
        if args.keychain or args.keychain_replace:
            raise SystemExit("--keychain-remove cannot be combined with --keychain/--keychain-replace")
        if args.source_account_index is None or args.receiver_account_index is None:
            raise SystemExit("keychain removal requires explicit source and receiver account indices")
        if args.source_account_index == args.receiver_account_index:
            raise SystemExit("source and receiver account indices must differ")
        config = (
            _series_config(
                config_data,
                execute=False,
                defer_incremental_margin_calculation=args.defer_incremental_margin_calculation,
            )
            if is_series
            else _config(
                config_data,
                execute=False,
                defer_incremental_margin_calculation=args.defer_incremental_margin_calculation,
            )
        )
        return _remove_keychain(
            config,
            (args.source_account_index, args.receiver_account_index),
        )
    if args.market_evidence is None:
        raise SystemExit("run requires --config and --market-evidence")
    evidence = _load_json(args.market_evidence, "market evidence")
    if is_series:
        series_config = _series_config(
            config_data,
            execute=args.execute,
            defer_incremental_margin_calculation=args.defer_incremental_margin_calculation,
        )
        if args.confirm_plan:
            object.__setattr__(series_config, "operator_plan_reviewed", True)
        if args.source_account_index is None or args.receiver_account_index is None:
            raise SystemExit("series run requires explicit source and receiver account indices")
        if args.source_account_index == args.receiver_account_index:
            raise SystemExit("source and receiver account indices must differ")
        if not args.execute:
            print(
                json.dumps(
                    {
                        "outcome": "PREVIEW",
                        "execution": "DISABLED",
                        "message": "offline-safe mode: no SDK import, key prompt, signing, or network request",
                        "mode": "series",
                        "operation_mode": series_config.operation_mode.value,
                        "venue": "robinhood-chain",
                        "website_url": "https://robinhoodchain.lighter.xyz",
                        "api_base_url": series_config.api_base_url,
                        "chain_id": series_config.chain_id,
                        "market_symbol": series_config.market_symbol,
                        "market_id": series_config.market_id,
                        "direction": series_config.direction.value,
                        "total_quantity": str(series_config.total_quantity),
                        "desired_slice_quantity": str(series_config.desired_slice_quantity),
                        "allowed_price_deviation": str(series_config.allowed_price_deviation),
                        "source_limit_price": str(series_config.source_limit_price),
                        "receiver_worst_price": str(series_config.receiver_worst_price),
                        "plan_reviewed": series_config.operator_plan_reviewed,
                        "defer_incremental_margin_calculation": series_config.defer_incremental_margin_calculation,
                        "journal_path": series_config.journal_path,
                    },
                    sort_keys=True,
                )
            )
            return 0
        if not args.i_understand_series_live_operation:
            raise SystemExit("--execute also requires --i-understand-series-live-operation for HCR-2")
        if series_config.api_key_index is None:
            raise SystemExit("series execution requires api_key_index")
        base_config = series_config.attempt_config(
            market_id=series_config.market_id or 0,
            quantity=min(series_config.total_quantity, series_config.desired_slice_quantity),
            journal_path=series_config.journal_path + ".client",
        )
        account_indices = (args.source_account_index, args.receiver_account_index)
        if args.keychain or args.keychain_replace:
            secrets: Any = _keychain_provider(
                series_config,
                account_indices,
                replace=args.keychain_replace,
            )
            try:
                _prime_keychain(secrets, account_indices)
            except SystemExit:
                secrets.close()
                raise
        else:
            secrets = PromptSecretProvider(account_indices, series_config.api_key_index)
        try:
            client = LighterSdkClient(
                base_config,
                source_account_index=args.source_account_index,
                receiver_account_index=args.receiver_account_index,
                secrets=secrets,
                market_evidence=evidence,
            )
            result = await run_series(series_config, client)
        finally:
            secrets.close()
        print(json.dumps(result.as_dict(), sort_keys=True, separators=(",", ":")))
        return 0 if result.outcome.value in {"SUCCESS", "PARTIAL", "PREVIEW"} else 2
    config = _config(
        config_data,
        execute=args.execute,
        defer_incremental_margin_calculation=args.defer_incremental_margin_calculation,
    )
    if args.confirm_plan:
        object.__setattr__(config, "operator_plan_reviewed", True)
    if args.source_account_index is None or args.receiver_account_index is None:
        raise SystemExit("run requires explicit source and receiver account indices")
    if args.source_account_index == args.receiver_account_index:
        raise SystemExit("source and receiver account indices must differ")
    if not args.execute:
        print(
            json.dumps(
                {
                    "outcome": "PREVIEW",
                    "execution": "DISABLED",
                    "message": "offline-safe mode: no SDK import, key prompt, signing, or network request",
                    "market_id": config.market_id,
                    "market_symbol": config.market_symbol,
                    "direction": config.direction.value,
                    "operation_mode": config.operation_mode.value,
                    "source_side": config.direction.source_side,
                    "receiver_side": config.direction.receiver_side,
                    "quantity": str(config.quantity),
                    "source_limit_price": str(config.source_limit_price),
                    "receiver_worst_price": str(config.receiver_worst_price),
                    "source_order_type": "LIMIT",
                    "source_time_in_force": "POST_ONLY",
                    "source_reduce_only": config.operation_mode is OperationMode.CLOSE_REOPEN,
                    "receiver_order_type": "MARKET",
                    "receiver_time_in_force": "IOC",
                    "receiver_reduce_only": False,
                    "receiver_bound_semantics": (
                        "BUY price is a ceiling"
                        if config.direction is Direction.LONG
                        else "SELL price is a floor"
                    ),
                    "plan_reviewed": config.operator_plan_reviewed,
                    "defer_incremental_margin_calculation": config.defer_incremental_margin_calculation,
                    "journal_path": config.journal_path,
                },
                sort_keys=True,
            )
        )
        return 0
    if not args.i_understand_one_attempt_live_operation:
        raise SystemExit("--execute also requires --i-understand-one-attempt-live-operation")
    assert config.api_key_index is not None
    account_indices = (args.source_account_index, args.receiver_account_index)
    if args.keychain or args.keychain_replace:
        secrets = _keychain_provider(
            config,
            account_indices,
            replace=args.keychain_replace,
        )
        try:
            _prime_keychain(secrets, account_indices)
        except SystemExit:
            secrets.close()
            raise
    else:
        secrets = PromptSecretProvider(account_indices, config.api_key_index)
    try:
        client = LighterSdkClient(
            config,
            source_account_index=args.source_account_index,
            receiver_account_index=args.receiver_account_index,
            secrets=secrets,
            market_evidence=evidence,
        )
        result = await run_handoff(config, client)
    finally:
        secrets.close()
    print(json.dumps(result.as_dict(), sort_keys=True, separators=(",", ":")))
    return 0 if result.outcome.value in {"SUCCESS", "PARTIAL", "PREVIEW"} else 2


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.run is None:
        _parser().print_help()
        return 0
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["PromptSecretProvider", "format_random_cycle_result_ru", "main"]
