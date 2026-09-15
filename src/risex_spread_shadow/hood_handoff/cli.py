"""Safe command line entry point for the future operator-run HCR-1 adapter."""

from __future__ import annotations

import argparse
import asyncio
from decimal import Decimal, InvalidOperation
import getpass
import json
from pathlib import Path
import sys
from typing import Any, Mapping

from .contracts import Direction, HandoffConfig
from .engine import run_handoff
from .sdk import LighterSdkClient, SecretProvider
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
            if not sys.stdin.isatty() or not sys.stderr.isatty():
                raise RuntimeError("hidden private-key input requires an interactive TTY")
            self._values[account_index] = getpass.getpass(
                f"Lighter private key for account {account_index} (hidden input): "
            )
        return self._values[account_index]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="risex-hood-handoff",
        description=(
            "Prepare one explicit HOOD close/reopen attempt. Default mode is offline-safe "
            "and never imports the Lighter SDK, prompts for keys, or makes a request."
        ),
    )
    parser.add_argument("run", nargs="?", choices=("run",), help="run the configured one-attempt utility")
    parser.add_argument("--config", type=Path, help="JSON operator configuration; all numerical bounds are required")
    parser.add_argument("--market-evidence", type=Path, help="JSON current orderBookDetails/fee/margin evidence")
    parser.add_argument("--source-account-index", type=int)
    parser.add_argument("--receiver-account-index", type=int)
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
    return parser


def _load_json(path: Path, label: str) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"cannot read {label}: {exc}") from exc
    if not isinstance(value, Mapping):
        raise SystemExit(f"{label} must contain one JSON object")
    return value


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
    kwargs = dict(value)
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
        kwargs["operator_execution_opt_in"] = execute
        return RobinhoodSeriesConfig(**kwargs)
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"invalid HCR-2 configuration: {exc}") from exc


async def _run(args: argparse.Namespace) -> int:
    if args.config is None or args.market_evidence is None:
        raise SystemExit("run requires --config and --market-evidence")
    config_data = _load_json(args.config, "config")
    evidence = _load_json(args.market_evidence, "market evidence")
    if config_data.get("mode") in {"series", "hcr-2"} or config_data.get("series") is True:
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
        secrets: SecretProvider = PromptSecretProvider(
            (args.source_account_index, args.receiver_account_index), series_config.api_key_index
        )
        client = LighterSdkClient(
            base_config,
            source_account_index=args.source_account_index,
            receiver_account_index=args.receiver_account_index,
            secrets=secrets,
            market_evidence=evidence,
        )
        result = await run_series(series_config, client)
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
                    "source_side": config.direction.source_side,
                    "receiver_side": config.direction.receiver_side,
                    "quantity": str(config.quantity),
                    "source_limit_price": str(config.source_limit_price),
                    "receiver_worst_price": str(config.receiver_worst_price),
                    "source_order_type": "LIMIT",
                    "source_time_in_force": "POST_ONLY",
                    "source_reduce_only": True,
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
    secrets: SecretProvider = PromptSecretProvider(
        (args.source_account_index, args.receiver_account_index), config.api_key_index
    )
    client = LighterSdkClient(
        config,
        source_account_index=args.source_account_index,
        receiver_account_index=args.receiver_account_index,
        secrets=secrets,
        market_evidence=evidence,
    )
    result = await run_handoff(config, client)
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
