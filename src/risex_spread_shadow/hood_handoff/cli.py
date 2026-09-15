"""Safe command line entry point for the future operator-run HCR-1 adapter."""

from __future__ import annotations

import argparse
import asyncio
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
from typing import Any, Mapping

from .contracts import Direction, HandoffConfig, OperationMode
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
from .readiness import (
    ReadinessCheck,
    ReadinessConfig,
    ReadinessResult,
    ReadinessSecretProvider,
    ReadOnlyLighterSdkClient,
    run_readiness,
)
from .sdk import LighterSdkClient
from .series import RobinhoodSeriesConfig, run_series


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
        description=(
            "Prepare one explicit HOOD close/reopen attempt. Default mode is offline-safe "
            "and never imports the Lighter SDK, prompts for keys, or makes a request."
        ),
    )
    parser.add_argument(
        "run",
        nargs="?",
        choices=("run", "readiness"),
        help="run the configured utility or the explicit read-only readiness check",
    )
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
    parser.add_argument("--source-limit-price", dest="readiness_source_limit_price")
    parser.add_argument("--receiver-worst-price", dest="readiness_receiver_worst_price")
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


def _config(value: Mapping[str, Any], *, execute: bool) -> HandoffConfig:
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


def _series_config(value: Mapping[str, Any], *, execute: bool) -> RobinhoodSeriesConfig:
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
    required = {
        "symbol": args.readiness_symbol,
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
            market_symbol=args.readiness_symbol,
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


def _keychain_provider(
    config: Any,
    account_indices: tuple[int, ...],
    *,
    replace: bool,
) -> KeychainSecretProvider:
    """Construct the explicit Keychain provider after config validation."""

    try:
        return KeychainSecretProvider.from_config(
            config,
            account_indices,
            backend=MacOSKeychainBackend(),
            replace=replace,
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
    if args.run == "readiness":
        return await _run_readiness(args)
    if args.config is None:
        raise SystemExit("run requires --config")
    config_data = _load_json(args.config, "config")
    is_series = config_data.get("mode") in {"series", "hcr-2"} or config_data.get("series") is True
    if args.keychain_remove:
        if args.execute or args.i_understand_one_attempt_live_operation or args.i_understand_series_live_operation or args.confirm_plan:
            raise SystemExit("--keychain-remove cannot be combined with execution or plan-review flags")
        if args.keychain or args.keychain_replace:
            raise SystemExit("--keychain-remove cannot be combined with --keychain/--keychain-replace")
        if args.source_account_index is None or args.receiver_account_index is None:
            raise SystemExit("keychain removal requires explicit source and receiver account indices")
        if args.source_account_index == args.receiver_account_index:
            raise SystemExit("source and receiver account indices must differ")
        config = (
            _series_config(config_data, execute=False)
            if is_series
            else _config(config_data, execute=False)
        )
        return _remove_keychain(
            config,
            (args.source_account_index, args.receiver_account_index),
        )
    if args.market_evidence is None:
        raise SystemExit("run requires --config and --market-evidence")
    evidence = _load_json(args.market_evidence, "market evidence")
    if is_series:
        series_config = _series_config(config_data, execute=args.execute)
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
    config = _config(config_data, execute=args.execute)
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


__all__ = ["PromptSecretProvider", "main"]
