# Active bounded task

## Legacy central economics/funding task

Status: `FROZEN / REPLACED`.

The former Funding Farmer central profitability, funding-boundary, PAPER lifecycle, testnet, and mainnet tasks are not active. Do not resume an old process, database, worktree, candidate, credential path, or operational authority.

## SS-001A — pure entry observer domain

Status: `AUTHORIZED FOR ONE VISIBLE SPREAD BUILDER`.

Base: exact accepted governance `main` selected by the Chief when the Builder is created.

Objective: implement only deterministic pure RISEx Spread Shadow domain and evidence contracts under `src/risex_spread_shadow/`, with tests under `tests/spread_shadow/`. No network runtime, CLI, database, report, position, exit, funding lifecycle, or operational process belongs in this slice.

### Fixed research grid

- Directions: RISEx maker BUY → Lighter taker SELL; RISEx maker SELL → Lighter taker BUY.
- Discovery notionals: `$100`, `$250`, `$500`.
- Target margins: `1`, `2`, `3`, `5` basis points.
- Horizons: `0`, `300`, `500`, `1000` milliseconds. An optional `2000 ms` stress horizon is allowed only if it adds no architecture.

### Required immutable contracts

- `QuotePolicy`
- `HypotheticalMakerQuote`
- `QuoteVersion`
- `WouldFillEvidence`
- `HedgeHorizonCapture`
- `EntryViabilityEpisode`
- `EntryViabilityOutcome`

Required outcome values include:

- `QUOTE_NOT_POST_ONLY`
- `QUOTE_NOT_ECONOMIC`
- `QUOTE_ACTIVE`
- `NO_WOULD_FILL`
- `WOULD_FILL`
- `HEDGE_FULL`
- `HEDGE_PARTIAL`
- `HEDGE_DEPTH_UNAVAILABLE`
- `HEDGE_DATA_UNAVAILABLE`
- `HEDGE_DATA_STALE`
- `HEDGE_OUTCOME_UNKNOWN`

`HEDGE_PARTIAL` means a valid book supplies a positive quantity smaller than exact `q`. `HEDGE_DEPTH_UNAVAILABLE` means the valid book has no executable quantity on the required side. Missing book, invalid/current-session evidence, or an overlapping data gap is `HEDGE_DATA_UNAVAILABLE`; stale but otherwise valid evidence is `HEDGE_DATA_STALE`. None becomes `NO TRADE`.

### Required behavior

- Exact BUY/SELL entry-edge signs and fees applied once.
- Exact-q multi-level VWAP; visible spread/depth impact is never subtracted a second time.
- Strict would-fill: quote version predates evidence; exact RISEx venue/market/direction; correct aggressor; trade-through by at least one tick; version-local eligible quantity reaches exact `q`; duplicate rejection; replacement resets evidence; expiry or data gap forbids fill.
- One optional fill model may exist only as an explicitly named optimistic upper bound, not a queue simulator.
- Persistable provenance contracts include UTC and monotonic quote/trade/detection timestamps, absolute monotonic horizon deadline, book receipt monotonic time, stream session, recovery generation, book revision, and sequence/checksum when applicable.
- Horizon selection accepts only an exact-market, sequence-valid, current-session, gap-free, fresh book received at or before the deadline. No interpolation or later-book look-ahead is permitted.
- Pure deterministic replay of the same immutable inputs produces byte-equivalent canonical evidence.

### Focused acceptance evidence

One regression per material risk, including:

1. Exact BUY and SELL edge signs.
2. Exact-q multi-level VWAP.
3. No double counting of spread/slippage.
4. RISEx fee applied once.
5. Quote predates qualifying trade.
6. Wrong aggressor does not fill.
7. One-tick boundary.
8. Duplicate trade rejection.
9. Replacement resets version-local evidence.
10. A book received one nanosecond after deadline is rejected.
11. Monotonic-clock deadline construction.
12. Stale/displaced Lighter session rejection.
13. Exact partial hedge quantity.
14. Missing hedge does not become `NO TRADE`.
15. Queue-overflow input creates explicit gap evidence contract.
16. Deterministic replay.
17. No imports from old scanner/broker/lifecycle/runtime or old strategy persistence/reporting.
18. No private/auth/write surface is reachable from the package.

Builder must report preflight root/branch/HEAD/status, exact scope/diff, focused/adverse tests, dependency/import surface, compile evidence, and one clean full Python 3.11 suite on the final committed SHA. Builder never self-accepts, merges, or pushes `main`.

### Forbidden scope

- No SS-001B feed integration, sockets, persistence, CLI, report, or discovery run.
- No old scanner, broker, lifecycle, RoutePlan, funding activation/cutoff, old state machine, Telegram, or operational module.
- No private/authenticated endpoint, credential, signing, order preparation, dispatch, testnet/mainnet write, real fund, transfer, withdrawal, or strategy execution.
- No position/exit/funding lifecycle, two-sided active quotes, multi-lot inventory, batching, OMS, generic event bus, dashboard, new venue adapter, ML, or automatic optimization.

Completion: Chief independently reviews scope, diff, contracts, tests, dependency surface, Git, and final suite. Only Chief may accept and integrate. SS-001B remains closed after SS-001A unless separately opened by accepted governance.
