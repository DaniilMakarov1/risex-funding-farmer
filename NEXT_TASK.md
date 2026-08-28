# Active bounded task

## Two-route testnet funding-boundary lifecycles

Status: `BLOCKED — NADO VENUE-LOCAL LIVE FUNDING ADAPTER CORRECTION IS REQUIRED AND NO CANDIDATE IS ACCEPTED; EXTENDED TESTNET BOOKS HAVE ZERO ASKS AND A STALE FUNDING TIMESTAMP; NO WRITE IN FLIGHT`.

Objective: prove how the accepted system behaves before, during, and after an actual testnet funding settlement on exactly two hedged routes: one RISEx–Nado route and one RISEx–Extended route. Open the smallest venue-executable matched testnet positions, observe and reconcile actual venue funding semantics at the boundary, then close every leg to authoritative zero relevant orders and exact flatness.

Exact starting point:

- Published `main` is the only authorized source base; the latest accepted checkpoint includes the Nado all-products boundary, outbound-only lifecycle notifications, and the corrected RISEx completed-history/nonce-scope read gate.
- All earlier RISEx, Nado, and Extended Level C lifecycles are complete historical evidence; this is a fresh funding-accrual objective, not permission to replay their intents.
- The corrected public all-route implementation and its paper measurements remain accepted, but the `chief11` statistical window was intentionally ended for this rotation. Its database has integrity `ok` and zero orders/fills/positions; its obsolete automation was deleted.
- Fresh authenticated read-only gates are accepted for RISEx, Nado, and Extended. The first staged RISEx, Nado, and Extended candidates are not accepted: exact opposite-direction matching, authoritative cross-run journal binding, and Nado event-to-account funding agreement must be corrected and independently reverified before any dispatch. Their bounded correction worktrees are preserved and idle at the operational blocker; none is integrated or accepted. Extended ETH-USD currently has no ask-side liquidity. No venue write is in flight.
- On the fresh `2026-08-28` checkpoint, RISEx again `PASSED`, Extended again reached `READY` through the accepted strict REST fallback, and Nado `FINALIZED` on 94 products with two agreeing rounds; the earlier Nado `contracts` HTTP 403 no longer reproduced. Accepted main `62fad2fbb27d937db2faa9d95feca10b21d63b0e` adds the Nado funding-boundary safety and operational contract: exact opposite directions and canonical quantity, immutable pre-dispatch route/run/store/account binding, exact event-to-account funding agreement, durable skipped/unresolved blockers, applied-only completion, and two agreeing final zero-order/exact-flat rounds. Chief verification passed `2542` tests with `3` skipped. Nado remains blocked before dispatch because no accepted authoritative live funding-event wire adapter exists; do not inject or guess that schema. Extended public `BTC-USD` and `ETH-USD` books returned respectively `1 bid / 0 asks` and `4 bids / 0 asks`, so its write remains blocked too.
- The next fresh Nado Level-B funding-wire discovery made exactly one attempt per official testnet read and no writes. Protected owner/key identity agreed, but the live gate ended terminal `SCHEMA`; do not retry that semantic failure inside the same gate, create a Builder, inject a schema, or prepare an order. A separate bounded Extended public observation again returned `1 bid / 0 asks` for `BTC-USD` and `4 bids / 0 asks` for `ETH-USD`; its documented next-funding timestamp was `2026-08-28T00:00:00Z`, stale at the `05:13Z` observation. Do not open the Extended route until both executable ask-side liquidity and a fresh authoritative funding schedule are observed.
- Separate bounded diagnostics established that the preceding Nado `SCHEMA` results came from local diagnostic decoder mistakes, not a venue response contradiction. The accepted catalog decoder observed 94 products and 79 perps; the official archive exposed the account-history cursor contract and two realized hourly product-level funding rows for product 44. The exact protected subaccount returned an authoritative empty baseline with terminal `next_idx=null`. Independent review of the current official Nado docs and Python SDK proves that this endpoint is historical and account-scoped, so an empty pre-entry history means no prior observed payment and must not block the first experiment that can create one. The same review proves that public `funding_payment.payment_amount` is aggregate product-level cash, while account-history `amount` is the individual subaccount payment; they must not be required equal. This corrects the pre-entry gate without treating missing funding as zero. One fresh venue-local live-adapter Builder is authorized; order preparation and dispatch remain forbidden until its candidate is independently accepted.

Corrected Nado funding evidence gate:

- Before dispatch, durably bind the exact subaccount, product, target boundary, account-history high-water cursor/empty baseline, fresh exact position state, and the product's public cumulative long/short funding state. Official account-row fields are `product_id`, `idx`, `timestamp`, `amount`, `balance_amount`, `rate_x18`, and `oracle_price_x18`; tolerate additive irrelevant fields but fail closed on any required semantic mismatch.
- Preserve the public hourly event as product-level evidence: exact product, timestamp, `payment_amount`, open interest, cumulative long/short values, and `dt`. Never relabel aggregate `payment_amount` as account cash or attach a fabricated account quantity/status to the public event.
- After authoritative exposure through the target boundary, query bounded complete account-history pages strictly after the persisted high-water mark and require a newly attributable funding row for the exact subaccount/product/boundary. Bind it to the public event through product, boundary interval, before/after cumulative state, and official rate/payment semantics; use account `amount` as individual cash. Missing or contradictory post-boundary account evidence is `BLOCKED`, never zero, but must not prevent the accepted reduce-only close and terminal flat reconciliation.
- The live adapter must remain venue-local, use the official Nado SDK/contract first, perform no write during implementation or acceptance, and preserve the existing durable identity, no-replay, opposite-direction, exact-quantity, applied-only completion, and two-round terminal barriers.

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
- Authoritative funding result for each leg: applied account cash when exposed and eligible, or an exact venue-proven skipped/unresolved reason. For Nado, public aggregate product cash is never substituted for the account payment; a missing or contradictory post-boundary account record is a blocker, not zero funding.
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

The outbound-only Telegram boundary may add sanitized, semantically deduplicated PAPER versus TESTNET lifecycle actions for maker activation, authoritative two-leg open, actual funding status, exit start, close, final flatness, and paired fail-closed error/recovery. It must remain non-blocking and must not affect cadence, economics, decisions, signing, reconciliation, or venue writes; inbound polling and commands remain forbidden. No secret may enter Git, process arguments, logs, reports, or source files. On `2026-08-28`, one user-authorized bounded operator discovery read resolved the pending `/start`, and a sanitized outbound smoke message reached private chat `738925112`; the test-only token/chat configuration is protected outside Git with mode `0600`. Continuous delivery remains inactive until a future PAPER runtime is separately started.

Accepted published main `c8bcef0118eb4c613b0c5b8ffb63c87b22dceb1e` contains the bounded correction for the Extended heartbeat persistence storm observed during the two-hour run. Telegram credentials and outbound delivery are now operationally proven; do not start an inbound bot or a new PAPER window without a separate bounded runtime decision.
