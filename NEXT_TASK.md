# Active bounded task

## Two-route testnet funding-boundary lifecycles

Status: `BLOCKED — FRESH NADO READ-ONLY ROUND TERMINATED HTTP 403; EXTENDED ETH-USD HAS ZERO ASKS; NO WRITE IN FLIGHT`.

Objective: prove how the accepted system behaves before, during, and after an actual testnet funding settlement on exactly two hedged routes: one RISEx–Nado route and one RISEx–Extended route. Open the smallest venue-executable matched testnet positions, observe and reconcile actual venue funding semantics at the boundary, then close every leg to authoritative zero relevant orders and exact flatness.

Exact starting point:

- Published `main` is the only authorized source base; the latest accepted checkpoint includes the Nado all-products boundary, outbound-only lifecycle notifications, and the corrected RISEx completed-history/nonce-scope read gate.
- All earlier RISEx, Nado, and Extended Level C lifecycles are complete historical evidence; this is a fresh funding-accrual objective, not permission to replay their intents.
- The corrected public all-route implementation and its paper measurements remain accepted, but the `chief11` statistical window was intentionally ended for this rotation. Its database has integrity `ok` and zero orders/fills/positions; its obsolete automation was deleted.
- Fresh authenticated read-only gates are accepted for RISEx, Nado, and Extended. The first staged RISEx, Nado, and Extended candidates are not accepted: exact opposite-direction matching, authoritative cross-run journal binding, and Nado event-to-account funding agreement must be corrected and independently reverified before any dispatch. Their bounded correction worktrees are preserved and idle at the operational blocker; none is integrated or accepted. Extended ETH-USD currently has no ask-side liquidity. No venue write is in flight.
- On the fresh `2026-08-27` operational checkpoint, RISEx again passed its two-barrier authenticated read with authoritative zero-order/flat evidence, and Extended again reached `READY` through the accepted strict REST fallback. Nado round A and its signed trigger-order observation completed, but the fifth request of authoritative round B (`contracts`) returned terminal `HTTP 403`; one separate public-only diagnostic reproduced the same `HTTP 403` on its first request. This semantic HTTP failure is not retryable inside the gate. Together with Extended ETH-USD remaining `OK` but `4 bids / 0 asks`, these are exact fail-closed blockers for both authorized writes.

Authorized routes:

1. One testnet `RISEx–Nado` matched route.
2. One testnet `RISEx–Extended` matched route.

The new Chief must discover currently tradable common testnet markets, authoritative funding schedules, minimum quantities, and account state before choosing assets or directions. Prefer distinct RISEx markets when both routes overlap in time so venue-level netting cannot make route attribution ambiguous. If safe isolation or a common funding window cannot be proven, run the two routes sequentially across their next actual boundaries rather than combining ambiguous exposure.

Allowed scope:

- Existing protected testnet-only credentials, accounts, wallets, and accepted venue-local operational runners for RISEx, Nado, and Extended.
- Public and authenticated testnet reads required to prove current market metadata, funding schedule/rate/source quality, identity, balances/collateral sufficiency, orders, fills, positions, and applied funding.
- The smallest currently venue-executable matched quantity per route; modeled negative economics, test-asset spreads, and testnet minimum notionals are not blockers for this explicitly authorized lifecycle.
- Durable fresh runtime and write-intent identities before every dispatch, sequential venue writes, exact canonical quantity matching, and the accepted no-replay/ambiguity barriers.
- Hold the matched legs across the authoritative funding boundary; persist evidence immediately before, at/around, and after settlement. Reconcile actual funding cash/rate/status on both legs from authoritative venue evidence. Never substitute an assumed positive rate for the actual operational verdict.
- Close each leg through the accepted venue-specific reduce-only path and finish with two fresh agreeing authoritative rounds proving zero relevant open/trigger orders, exact flatness, no unrelated state, and no unresolved write identity.
- Read-only error monitoring throughout. On any concrete code defect, use one fresh visible GPT-5.6 Luna-max Builder in a separate worktree from the exact published `main`; Chief independently reviews, integrates, tests, and alone pushes `main`. Keep venue-local signing and execution semantics separate.

Required evidence per route:

- Exact environment, sanitized account/wallet identities, market, direction, canonical/raw quantity, funding timestamp, rate/source quality, entry and close identities, dispatch counts, fills, fees, and before/during/after position snapshots.
- Authoritative funding result for each leg: applied cash when exposed and eligible, or an exact venue-proven skipped/unresolved reason. A missing or contradictory funding record is a blocker, not zero funding.
- Timing/freshness/latency and every disconnect, retry, ambiguity, reconciliation, or manual recovery event, with secrets and raw private payloads excluded.
- Final SQLite integrity `ok`, terminal lifecycle status, zero relevant orders, exact flatness, and a sanitized route report. `COMPLETE` requires authoritative funding semantics and final flatness; otherwise report `BLOCKED` or `DEGRADED` precisely.

Forbidden scope:

- No mainnet credential, private mainnet endpoint, production wallet/account, real asset, real-money order, or mainnet strategy execution.
- No blind replay, parallel venue writes, ambiguous route attribution, unrelated account-state mutation, fabricated funding, assumed applied cash, or weakening of identity/freshness/reconciliation gates.
- No generic OMS, new venue, dashboard/service, cross-venue signing abstraction, or product/economics change unrelated to an observed testnet defect.
- No reuse of rejected branch `codex/strategy-measurement-foundation` or commit `300362d840141d9ed599d8189ed1d10801fc5256`.

Completion:

Both authorized routes must independently finish with authoritative funding-boundary evidence, zero relevant orders, and exact flatness. Actual zero/negative funding or a venue-proven non-accrual is valid evidence only when eligibility and exposure are authoritative. Even two successful testnet lifecycles do not authorize mainnet Level D, real funds, or production strategy execution.

## Parallel bounded public observation and Telegram delivery correction

The authorized public-only all-route paper run on `mainnet-shadow-all-routes-20260827-chief12-2h.db` is complete. It ran from `2026-08-27T15:35:24Z` to the exact authorized `17:35:24Z` stop, reached durable `STOPPED_SAFE` at `17:36:14Z`, and ended with SQLite integrity `ok` and orders/fills/positions `0/0/0`. It is a bounded completed observation, not an active process and not permission to resume either this database or the old `chief11` database.

The outbound-only Telegram boundary may add sanitized, semantically deduplicated PAPER versus TESTNET lifecycle actions for maker activation, authoritative two-leg open, actual funding status, exit start, close, final flatness, and paired fail-closed error/recovery. It must remain non-blocking and must not affect cadence, economics, decisions, signing, reconciliation, or venue writes; inbound polling and commands remain forbidden. No secret may enter Git, process arguments, logs, reports, or task text. The previously exposed bot token must be rotated and supplied through protected runtime configuration before Telegram delivery is re-enabled.

Accepted published main `c8bcef0118eb4c613b0c5b8ffb63c87b22dceb1e` contains the bounded correction for the Extended heartbeat persistence storm observed during the two-hour run. Telegram remains operationally blocked only by the absent rotated protected token/chat configuration; do not reuse the exposed historical token or start an inbound bot.
