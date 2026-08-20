"""Paper-only command-line entry point."""

from __future__ import annotations

import argparse
import asyncio
import json
import shutil
from collections.abc import Sequence
from datetime import UTC
from decimal import Decimal, ROUND_HALF_UP

from .orchestrator import fixture_scan, load_fixture, run_fixture
from .runtime import public_paper_run, public_scan_once
from .storage import PaperRepository


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="risex-farmer")
    parser.add_argument("--db", default="risex-farmer.db", help="paper SQLite path")
    commands = parser.add_subparsers(dest="command", required=True)
    scan = commands.add_parser("scan-once")
    scan.add_argument("--fixture", help="deterministic JSON fixture")
    scan.add_argument(
        "--format", choices=("json", "table"), default="json",
        help="output format (default: json)",
    )
    run = commands.add_parser("paper-run")
    run.add_argument("--fixture", help="deterministic JSON fixture")
    commands.add_parser("report")
    return parser


_COLUMNS = (
    ("Rank", "rank"), ("Asset", "canonical_asset"), ("Route", "direction"),
    ("Hedge", "hedge_venue"), ("Qty", "canonical_quantity"),
    ("Funding At / T-", "funding"), ("RISEx Funding $", "risex_funding_usd"),
    ("Hedge Funding $", "hedge_funding_usd"), ("Net Funding $", "net_funding_usd"),
    ("Entry Fee $", "planned_entry_fees_usd"), ("Exit Fee $", "planned_exit_fees_usd"),
    ("Entry Execution PnL $", "entry_execution_pnl_usd"),
    ("Exit Execution PnL $", "exit_execution_pnl_usd"),
    ("Expected Net PnL $", "planned_maker_net_pnl_usd"), ("Status", "status"),
)

_REASONS = {
    "MARKET_INELIGIBLE": "MARKET INELIGIBLE",
    "PLANNED_NET_PNL_NEGATIVE": "NEGATIVE PNL",
    "FUNDING_STALE": "STALE DATA",
    "BOOK_UNHEALTHY": "STALE DATA",
    "INVALID_BBO": "STALE DATA",
    "INSUFFICIENT_EXACT_DEPTH": "INSUFFICIENT DEPTH",
    "FUNDING_UNKNOWN": "FUNDING UNKNOWN",
    "FUNDING_ELIGIBILITY_UNKNOWN": "FUNDING UNKNOWN",
    "FUNDING_OPEN_TIME_MISMATCH": "FUNDING TIMING UNKNOWN",
    "PARITY_OR_MULTIPLIER_UNKNOWN": "FUNDING UNKNOWN",
    "NO_COMMON_EXECUTABLE_QUANTITY": "NO COMMON SIZE",
    "MINIMUM_ORDER": "BELOW MINIMUM",
}


def _money(value: object) -> str:
    if value is None:
        return "—"
    number = Decimal(str(value))
    if number and abs(number) < Decimal("0.0001"):
        return "<0.0001" if number > 0 else "-<0.0001"
    rendered = format(number.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP), "f")
    return rendered.rstrip("0").rstrip(".") or "0"


def _quantity(value: object) -> str:
    if value is None:
        return "—"
    number = Decimal(str(value)).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
    return format(number, "f").rstrip("0").rstrip(".")


def _countdown(value: object) -> str:
    if value is None:
        return "—"
    seconds = int(Decimal(str(value)))
    prefix = "T-" if seconds > 0 else ("T+" if seconds < 0 else "")
    seconds = abs(seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return "DUE" if not prefix else f"{prefix}{hours:02d}:{minutes:02d}:{seconds:02d}"


def _reason(row: dict[str, object]) -> str:
    blockers = row.get("blockers") or []
    labels = [
        _REASONS.get(str(reason), str(reason).replace("_", " ").title().upper())
        for reason in blockers
    ]
    return labels[0] if labels else "NOT ELIGIBLE"


def _route_values(row: dict[str, object]) -> list[str]:
    direction = str(row.get("direction", ""))
    route = "R↑ / H↓" if direction == "LONG_RISEX_SHORT_HEDGE" else (
        "R↓ / H↑" if direction == "SHORT_RISEX_LONG_HEDGE" else "—"
    )
    funding_at = row.get("target_cycle_start")
    funding = "—" if funding_at is None else f"{funding_at} / {_countdown(row.get('seconds_to_earliest_funding'))}"
    status = "TRADE" if row.get("entry_allowed") else f"NO TRADE — {_reason(row)}"
    values = {
        "rank": "—" if row.get("rank") is None else str(row["rank"]),
        "canonical_asset": str(row.get("canonical_asset") or "—"),
        "direction": route,
        "hedge_venue": str(row.get("hedge_venue") or "—"),
        "canonical_quantity": _quantity(row.get("canonical_quantity")),
        "funding": funding,
        "status": status,
    }
    for key in (
        "risex_funding_usd", "hedge_funding_usd", "net_funding_usd",
        "planned_entry_fees_usd", "planned_exit_fees_usd",
        "entry_execution_pnl_usd", "exit_execution_pnl_usd",
        "planned_maker_net_pnl_usd",
    ):
        values[key] = _money(row.get(key))
    return [values[key] for _, key in _COLUMNS]


def _scan_table(output: dict[str, object], *, width: int | None = None) -> str:
    width = shutil.get_terminal_size((180, 24)).columns if width is None else width
    routes = list(output.get("routes") or [])
    scan_at = str(output.get("scan_at") or "UNKNOWN")
    status = str(output.get("status") or "NO_TRADE").replace("_", " ")
    nearest = min(
        (row for row in routes if row.get("target_cycle_start") is not None),
        key=lambda row: str(row["target_cycle_start"]), default=None,
    )
    nearest_text = "UNKNOWN" if nearest is None else (
        f"{nearest['target_cycle_start']} / {_countdown(nearest.get('seconds_to_earliest_funding'))}"
    )
    readiness = output.get("venue_readiness") or {}
    ready_parts = []
    for venue, label in (("RISEX", "RISEx"), ("EXTENDED", "Extended"), ("NADO", "Nado")):
        state = readiness.get(venue, {}) if isinstance(readiness, dict) else {}
        ready_parts.append(f"{label}: {'READY' if state.get('available') else 'UNAVAILABLE'}")
    lines = [
        f"Scan UTC: {scan_at} | {status} | Eligible: {output.get('eligible_count', 0)}",
        f"Nearest funding UTC: {nearest_text}",
        "Readiness: " + " | ".join(ready_parts),
    ]
    headers = [header for header, _ in _COLUMNS]
    rows = [_route_values(row) for row in routes]
    caps = {"Funding At / T-": 48, "Status": 34}
    widths = [
        min(max([len(header), *(len(row[index]) for row in rows)]), caps.get(header, 24))
        for index, header in enumerate(headers)
    ]
    table_width = sum(widths) + 3 * (len(widths) - 1)
    if width < table_width:
        if not rows:
            lines.append(" | ".join(headers))
        for index, row in enumerate(rows, 1):
            lines.append(f"-- Route {index} --")
            current = ""
            for header, value in zip(headers, row):
                item = f"{header}: {value}"
                if current and len(current) + 3 + len(item) > width:
                    lines.append(current)
                    current = item
                else:
                    current = item if not current else f"{current} | {item}"
            if current:
                lines.append(current)
        return "\n".join(lines)

    def line(values: list[str]) -> str:
        return " | ".join(
            (value if len(value) <= size else value[: size - 1] + "…").ljust(size)
            for value, size in zip(values, widths)
        )
    lines.extend((line(headers), "-+-".join("-" * size for size in widths)))
    lines.extend(line(row) for row in rows)
    return "\n".join(lines)


async def _scan_once(repository: PaperRepository, fixture: str | None) -> dict[str, object]:
    if fixture is None:
        return await public_scan_once(repository)
    snapshot, observations = await fixture_scan(load_fixture(fixture))
    repository.save_decision(
        recorded_at=snapshot.logical_at,
        scan_snapshot=snapshot,
        funding_quotes=tuple(
            row.funding for row in observations if row.funding is not None
        ),
    )
    return {
        "scan_at": snapshot.logical_at.astimezone(UTC).isoformat(),
        "status": "OPPORTUNITY" if snapshot.winner is not None else "NO_TRADE",
        "eligible_count": sum(plan.entry_allowed for plan in snapshot.evaluations),
        "winner": None if snapshot.winner is None else snapshot.winner.canonical_asset,
    }


async def _paper_run(
    repository: PaperRepository, fixture: str | None
) -> dict[str, object]:
    if fixture is None:
        return await public_paper_run(repository)
    return await run_fixture(load_fixture(fixture), repository)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    with PaperRepository(args.db) as repository:
        try:
            if args.command == "scan-once":
                output = asyncio.run(_scan_once(repository, args.fixture))
            elif args.command == "paper-run":
                output = asyncio.run(_paper_run(repository, args.fixture))
            else:
                output = repository.report()
        except KeyboardInterrupt:
            output = {"status": "STOPPED_SAFE", "forced_close": False}
    if args.command == "scan-once" and args.format == "table":
        print(_scan_table(output))
    else:
        print(json.dumps(output, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
