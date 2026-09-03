"""One public-only SS-001D command and one offline report mode."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import subprocess

from .config import MAX_PUBLIC_DURATION_SECONDS, ShadowConfig
from .report import render_report
from .runner import run_public_smoke


def _source_commit() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return "UNKNOWN"
    value = result.stdout.strip()
    return value if value else "UNKNOWN"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="risex-spread-shadow")
    subparsers = parser.add_subparsers(dest="command", required=True)
    smoke = subparsers.add_parser("smoke", help="bounded public-only RISEx/Lighter smoke")
    smoke.add_argument("--store-root", default="./spread-shadow-runs")
    smoke.add_argument("--market", action="append", default=[])
    smoke.add_argument("--duration-seconds", type=int, default=60)
    smoke.add_argument("--max-markets", type=int, default=3)
    report = subparsers.add_parser("report", help="offline deterministic evidence report")
    report.add_argument("path")
    report.add_argument("--format", choices=("json", "table"), default="json")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "report":
        print(render_report(Path(args.path), format=args.format))
        return 0
    if args.duration_seconds <= 0 or args.duration_seconds > MAX_PUBLIC_DURATION_SECONDS:
        raise SystemExit(
            f"--duration-seconds must be between 1 and {MAX_PUBLIC_DURATION_SECONDS}"
        )
    if args.max_markets <= 0 or args.max_markets > 3:
        raise SystemExit("--max-markets must be between 1 and 3")
    result = asyncio.run(
        run_public_smoke(
            args.store_root,
            requested_markets=tuple(args.market),
            source_commit=_source_commit(),
            config=ShadowConfig(
                max_markets=args.max_markets,
                duration_seconds=args.duration_seconds,
            ),
            duration_seconds=args.duration_seconds,
        )
    )
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["main"]
