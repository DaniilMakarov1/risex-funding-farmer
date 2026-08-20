# PAPER-006-FIX — Real Public Scanner and Paper Runtime

## Expected baseline

- Repository: `/Users/daniilmakarov/Desktop/risex-funding-farmer`
- Accepted implementation baseline: PAPER-006 @ `e993064f98525331298578f1de3b2999705bf1f7`
- Builder branch: `codex/paper-006-fix-real-public-runtime`
- PAPER-007 and live trading are not authorized.

## Objective

Make ordinary `scan-once` consume official public RISEx, Extended, and Nado data and make ordinary `paper-run` a long-lived real-public-data paper runtime, while retaining the existing Scanner, economics, broker, lifecycle, SQLite repository, reports, and fixture paths.

## Design checkpoint

Before editing implementation files, return a DESIGN CHECKPOINT of at most 25 lines covering: the fixture-only ordinary path; changed files; minimal runtime architecture; REST bootstrap; WebSocket lifecycle; normalized observations; public-trade delivery to `PaperEntryBroker`/`LifecycleEngine`; full/focused scheduling; SQLite persistence; shutdown/restart; fixture preservation; paper-only RISEx assumptions; and consciously excluded scope. Wait for Architect approval.

## Allowed scope

- Existing public adapters, `MarketDataCoordinator`/`BookStream`, Scanner, economics, `PaperEntryBroker`, `LifecycleEngine`, `PaperRepository`, CLI, models, tests, README, STATUS and NEXT_TASK.
- At most one small runtime module such as `src/risex_farmer/runtime.py`.
- One async process using `aiohttp`, `asyncio`, public REST/WebSocket data, SQLite, and injectable clock/sleep for deterministic tests.
- Config `RISEX_PAPER_FALLBACK_ASSUMPTIONS_ENABLED = true` and only the explicitly authorized contract/quantity, funding-eligibility, next-rate-estimate, and reporting assumptions. Every assumption must be visibly marked and must fail closed when its stated consistency checks fail.

## Forbidden scope

- Real orders, authenticated/private/account endpoints, credentials/API keys, live execution, LLM calls, PAPER-007, or any live-trading preparation.
- A second Scanner/economics/broker/lifecycle/storage path; event bus, microservices, Redis, Celery, generic scheduler/plugin/DI frameworks, or per-venue processes.
- Guessing beyond the explicitly authorized paper-only RISEx fallbacks or representing an assumption as official fact.

## Acceptance criteria

1. `scan-once` without `--fixture` creates an `aiohttp.ClientSession`, bootstraps all three official public adapters, obtains metadata/volume/books/funding, builds real normalized observations/common markets/Top-5/routes, invokes the existing Scanner/economics, persists evidence, and prints up to 15 ranked routes including negative/blocked rows with detailed economics, blockers, source quality, and assumption flags. It must not call an empty `fail_closed_scan` path.
2. `paper-run` without `--fixture` performs immediate REST bootstrap/full scan, maintains public WebSockets/books/trades/funding/heartbeat and per-venue stale/unavailable state, recovers disconnects and sequence/checksum gaps with capped reconnect plus full snapshot, and continues until Ctrl+C/SIGTERM even on `NO_TRADE`.
3. Scheduling is deterministic and injectable: full scan 120s; focused window T-300; focused recalculation 10s; exact activation T-120; strict unfilled-entry cancellation T-5 exchange time; position monitoring 10s; normal-to-aggressive Exit after exact 10s; Hard Basis event-driven.
4. Normalized public trades reach the existing versioned, deduplicated, cumulative one-tick trade-through maker-fill model only when the public trade stream is healthy. Fill triggers fresh exact-q RISEx paper-taker VWAP and the existing lifecycle.
5. Runtime evidence, route diagnostics, venue readiness, assumption flags, settlements, restart/idempotency state, and shutdown evidence use the existing SQLite/report path. Official applied settlement replaces—not duplicates—an assumption estimate.
6. SIGINT/SIGTERM stops new entries, cancels active virtual Entry maker, preserves open paper positions, closes public transports/session/SQLite, and prints `STOPPED_SAFE` plus `forced_close = false`.
7. Fixture `scan-once`, fixture `paper-run`, and `report` remain functional through the same models and production paths. README documents real and fixture commands.
8. Venue failure never kills the process: affected routes are specifically blocked while other venues continue; RISEx failure blocks Entry but not Scanner/runtime.
9. No real-order path, private endpoint, credentials, or runtime LLM call exists.

## Required tests

All existing tests plus deterministic fake-adapter/FakeClock coverage for: ordinary injected public scan; no empty fail-closed scan; real observation delivery; visible blocked/top routes and detailed blockers; persistent `paper-run` on `NO_TRADE`; immediate/120s/10s/T-120/T-5 scheduling; trade-to-broker delivery; disconnect cancellation; gap snapshot recovery; reconnect; open-position lifecycle; settlement replacement/reconciliation; safe Ctrl+C without forced close; restart recovery; fixture preservation; event deduplication; no authenticated/private endpoints; no real-order path; explicit paper assumptions; and report evidence. CI must not require external connectivity. Public smoke is opt-in.

## Architect acceptance

After focused and full deterministic tests, run read-only real smoke against a disposable `/tmp/risex-public-smoke.db`: ordinary `scan-once`; continuous `paper-run` for 60 seconds then SIGINT; and `report`. Temporary public API outage may yield venue-specific blockers, but never a generic empty scan or process exit on `NO_TRADE`.
