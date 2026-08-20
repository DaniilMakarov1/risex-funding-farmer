# PAPER-006 — SQLite, CLI and E2E

## Goal

Complete the paper application with minimal SQLite persistence, atomic/idempotent repository operations, command-line wiring for `scan-once`, `paper-run`, and `report`, deterministic fixture-driven E2E orchestration, and the frozen report metrics. Do not start PAPER-007, connect live smoke by default, or add live/authenticated trading.

## Deliverables

- SQLite schema with unique IDs/keys for attempts, order/version, position, processed trade events, settlement keys, and one runtime state; persist scanner snapshots, quotes/cycles/settlements, orders/versions/cumulative evidence, fills/VWAP, samples, gaps/events, open state, and completed trades without raw-stream/book history.
- Repository transaction atomically writes a complete application decision/evidence snapshot; duplicate trade/settlement inserts are idempotent and conflicting authority rejects. Restart loading cannot reapply events.
- `scan-once`: obtain/provide public observations, evaluate once, persist snapshot, print structured no-trade/opportunity output. Default execution must fail closed safely when live public data is unavailable or RISEx semantics remain UNKNOWN.
- `paper-run`: ordinary async Python loop/orchestrator over adapters/scanner/broker/lifecycle/storage; no LLM calls. Provide deterministic fixture mode for CI/E2E and safe graceful stop without force-closing an open position.
- `report`: query SQLite and emit all frozen counts, PnL/funding/fee/volume/win/drawdown/duration/error/quality/open-position fields plus assumption flags. Use `UNKNOWN` where applied or unresolved evidence is incomplete.
- Restore all five states with PAPER-004/005 restart rules, preserving sticky timestamps/mode and recording open-position offline gaps.
- Wire the `risex-farmer` console script and document actual setup/commands in README.

## Acceptance tests

- No opportunity and maker-never-fills runs.
- Positive and negative closed trades, long exit, unresolved funding, and simulated/applied report divergence.
- Restart with open position and open-position report without artificial close.
- Duplicate DB event/settlement and conflicting-authority behavior.
- Persist/load state for FLAT, ENTRY_MAKER_OPEN, HOLDING, EXITING_NORMAL, EXITING_AGGRESSIVE.
- Report primary filtering for complete/resolved trades and exclusion of degraded/unresolved trades.
- Virtual RISEx volume, PnL per $1,000 volume, drawdown, win rates, planned-vs-actual error, and all paper-assumption flags.
- Full fixture-only E2E runs; no CI network dependency.

## Constraints

- Work on `codex/paper-006` from accepted `main`; no subagents or product-rule changes.
- Keep schema/orchestration small; use stdlib `sqlite3`, no ORM/event sourcing/framework.
- No PAPER-007 execution, real orders, auth/private endpoints, secrets, or forced end-of-run close.
- Run focused tests and full `pytest`, review the diff, commit, then report in at most 20 lines.
