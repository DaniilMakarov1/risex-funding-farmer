"""Paper-only command-line entry point."""

from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Sequence

from .orchestrator import fail_closed_scan, fixture_scan, load_fixture, run_fixture
from .storage import PaperRepository


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="risex-farmer")
    parser.add_argument("--db", default="risex-farmer.db", help="paper SQLite path")
    commands = parser.add_subparsers(dest="command", required=True)
    scan = commands.add_parser("scan-once")
    scan.add_argument("--fixture", help="deterministic JSON fixture")
    run = commands.add_parser("paper-run")
    run.add_argument("--fixture", help="deterministic JSON fixture")
    commands.add_parser("report")
    return parser


async def _scan_once(repository: PaperRepository, fixture: str | None) -> dict[str, object]:
    if fixture is None:
        snapshot = await fail_closed_scan()
        repository.save_decision(
            recorded_at=snapshot.logical_at, scan_snapshot=snapshot
        )
        return {
            "status": "NO_TRADE",
            "reason": "LIVE_PUBLIC_DATA_NOT_CONFIGURED_OR_RISEX_SEMANTICS_UNKNOWN",
            "eligible_count": 0,
        }
    snapshot, observations = await fixture_scan(load_fixture(fixture))
    repository.save_decision(
        recorded_at=snapshot.logical_at,
        scan_snapshot=snapshot,
        funding_quotes=tuple(
            row.funding for row in observations if row.funding is not None
        ),
    )
    return {
        "status": "OPPORTUNITY" if snapshot.winner is not None else "NO_TRADE",
        "eligible_count": sum(plan.entry_allowed for plan in snapshot.evaluations),
        "winner": None if snapshot.winner is None else snapshot.winner.canonical_asset,
    }


async def _paper_run(
    repository: PaperRepository, fixture: str | None
) -> dict[str, object]:
    if fixture is None:
        snapshot = await fail_closed_scan()
        repository.save_decision(
            recorded_at=snapshot.logical_at, scan_snapshot=snapshot
        )
        return {
            "status": "STOPPED_SAFE",
            "reason": "LIVE_PUBLIC_DATA_NOT_CONFIGURED_OR_RISEX_SEMANTICS_UNKNOWN",
            "forced_close": False,
        }
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
    print(json.dumps(output, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
