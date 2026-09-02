# Active bounded task

## Legacy central economics/funding task

Status: `FROZEN / REPLACED`.

The former Funding Farmer central profitability, funding-boundary, PAPER lifecycle, testnet, and mainnet tasks are not active. Do not resume an old process, database, worktree, candidate, credential path, or operational authority.

## SS-001A — pure entry observer domain

Status: `AUTHORIZED FOR ONE FRESH VISIBLE SPREAD BUILDER AFTER REJECTED CANDIDATE a3ac545`.

Base: exact accepted governance `main` selected by the Chief when the Builder is created.

Objective: implement only deterministic pure RISEx Spread Shadow domain and evidence contracts under `src/risex_spread_shadow/`, with tests under `tests/spread_shadow/`. No network runtime, CLI, database, report, position, exit, funding lifecycle, or operational process belongs in this slice.

### Fixed research grid

- Directions: RISEx maker BUY → Lighter taker SELL; RISEx maker SELL → Lighter taker BUY.
- Discovery notionals: `$100`, `$250`, `$500`.
- Target margins: `1`, `2`, `3`, `5` basis points.
- Horizons: `0`, `300`, `500`, `1000` milliseconds. An optional `2000 ms` stress horizon is allowed only if it adds no architecture.

### Frozen quote and quantity semantics

`target_margin_bps` is minimum net entry-execution edge after exact configured RISEx maker and Lighter taker entry fees, measured against actual Lighter hedge notional and before any latency/markout haircut. It is not BBO distance, gross spread, funding, exit income, or latency reserve.

For `m = target_margin_bps / 10000`, RISEx maker fee `fR`, and Lighter taker fee `fL`:

```text
max_risex_buy = Hsell(q) * (1 - fL - m) / (1 + fR)
buy_quote = min(round_down_to_tick(max_risex_buy), risex_best_ask - tick)

min_risex_sell = Hbuy(q) * (1 + fL + m) / (1 - fR)
sell_quote = max(round_up_to_tick(min_risex_sell), risex_best_bid + tick)
```

Recompute and retain the actual exact entry edge after rounding; the rounded quote is economic only if that edge still satisfies the requested target.

For each target notional, size exactly once from the current required-side Lighter top price: `q_raw = target_notional / reference_price`; floor to the common RISEx/Lighter canonical quantity step; validate both venues' minimum quantity and notional; calculate exact-q Lighter VWAP; then derive the RISEx quote. Never optimize or resize `q` from later books.

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
- `HEDGE_DATA_MISSING`
- `HEDGE_DATA_STALE`
- `HEDGE_SESSION_DISPLACED`
- `HEDGE_DATA_GAP`
- `HEDGE_OUTCOME_UNKNOWN`

`HEDGE_PARTIAL` means a valid book supplies a positive quantity smaller than exact `q`. `HEDGE_DEPTH_UNAVAILABLE` means an otherwise valid required-side book supplies zero executable quantity. Missing book, stale book, displaced session, and an overlapping data gap retain their exact named classifications. `HEDGE_OUTCOME_UNKNOWN` is only for a genuinely unclassified or incomplete terminal state; it is never a catch-all. None becomes `NO TRADE`.

### Required behavior

- Exact BUY/SELL entry-edge signs and fees applied once.
- Exact target-margin formula, post-only cap/floor, and post-rounding actual-edge revalidation.
- Deterministic required-side top-price sizing, common-step floor, venue-minimum validation, and no later resizing.
- Exact-q multi-level VWAP; visible spread/depth impact is never subtracted a second time.
- Strict would-fill: quote version predates evidence; exact RISEx venue/market/direction; correct aggressor; trade-through by at least one tick; version-local eligible quantity reaches exact `q`; duplicate rejection; replacement resets evidence; expiry or data gap forbids fill.
- One optional fill model may exist only as an explicitly named optimistic upper bound, not a queue simulator.
- Persistable provenance contracts include UTC and monotonic quote/trade/detection timestamps, absolute monotonic horizon deadline, book receipt monotonic time, stream session, recovery generation, book revision, and sequence/checksum when applicable.
- Local monotonic timestamps belong only to the local process. Do not invent or compare an exchange monotonic timestamp. Authoritative exchange/event UTC is optional and carries explicit authority/provenance; quote-before-trade ordering and would-fill detection use local receipt monotonic time. `WouldFillEvidence` explicitly persists `would_fill_detected_monotonic_ns`.
- Horizon selection accepts only an exact-market, sequence-valid, current-session, gap-free, fresh book received at or before the deadline. No interpolation or later-book look-ahead is permitted.
- Horizon selection requires both the expected stream session ID and expected recovery generation. A same-session book from another recovery generation is displaced evidence and cannot become a hedge.
- A quote cannot be `QUOTE_ACTIVE` unless its recomputed actual edge, target edge, hedge notional, fee evidence, and sizing evidence are present and internally consistent. Missing economics fails closed as `QUOTE_NOT_ECONOMIC`.
- Pure deterministic replay of the same immutable inputs produces equivalent canonical evidence using ordinary deterministic value contracts; do not build a custom serializer subsystem.
- Strict would-fill is a conservative lower bound. The optional optimistic model is an upper bound. Absence of strict fills alone is not a no-go: near-zero strict and near-zero optimistic evidence supports `PROFITABLE_QUOTES_UNFILLABLE`, while near-zero strict and materially positive optimistic evidence supports `FILLABILITY_INSUFFICIENT_EVIDENCE`; repeated strict fills support delayed-edge evaluation. SS-001A does not invent the numeric thresholds for “near-zero” or “materially positive”; SS-001B must freeze them before discovery.

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

- No SS-001B feed integration, sockets, serializer framework, persistence abstraction, generic engine, CLI, database, report, or discovery run.
- No old scanner, broker, lifecycle, RoutePlan, funding activation/cutoff, old state machine, Telegram, or operational module.
- No private/authenticated endpoint, credential, signing, order preparation, dispatch, testnet/mainnet write, real fund, transfer, withdrawal, or strategy execution.
- No position/exit/funding lifecycle, two-sided active quotes, multi-lot inventory, batching, OMS, generic event bus, dashboard, new venue adapter, ML, or automatic optimization.

Completion: Chief independently reviews scope, diff, contracts, tests, dependency surface, Git, and final suite. Only Chief may accept and integrate. SS-001B remains closed after SS-001A unless separately opened by accepted governance.

Rejected evidence: candidate `a3ac545a78a687788595470c1e1e1e91a501ec74` is not accepted and must not be merged or amended. It invented a cross-clock `trade_exchange_monotonic_ns`, admitted quotes with absent economics, omitted explicit would-fill detection monotonic evidence, and did not bind horizon books to the expected recovery generation. A fresh correction candidate starts from the current accepted `main`; it may use the rejected diff only as non-authoritative reference.
