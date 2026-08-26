# Active bounded tasks

One slice per venue. Apply the A/B/C levels and safety core from `AGENTS.md`; operational run IDs are data, not source milestones.

## RISEx — fill-seeking taker lifecycle

Status: `ETH ZERO-FILL CLOSED; BLOCKED ON CURRENT TESTNET MATCHING`.

- The accepted BUY `MARKET+IOC` now sends the exact validated ask-side adverse bound and the venue accepted its authoritative identity, but the BTC/USDC order still reconciled terminal zero-fill/flat. Two independent final observations and protected local recovery completed with no replay or venue recovery write. Do not repeat BTC unchanged.
- The accepted fixed ONDO attempt dispatched once, received an authoritative order ID, reconciled terminal zero-fill, and ended with two independent zero-order/exact-flat/consumed-nonce observations plus protected local outcome recovery. Do not repeat ONDO unchanged. Its displayed book was stale as execution evidence: latest public trade was about 31 hours old.
- The accepted fixed ETH/USDC market-2 correction passed its full verification. One fresh pre-state proved zero orders/exact flatness; exactly one minimum `0.1` `MARKET+IOC` was accepted with an authoritative order ID, reconciled terminal zero-fill/flat, and consumed its nonce. Two independent final observations again proved zero orders, exact flatness, and no unexplained state; protected local outcome recovery completed with no replay or venue recovery write.
- Do not repeat BTC, ONDO, ETH, or select another RISEx product without materially new execution evidence. The remaining blocker is current testnet matching, not signing, bound encoding, identity, or reconciliation. Keep this lane idle while Extended advances.

## Nado — sequential Level C lifecycle

Status: `LEVEL C COMPLETE`.

- A bounded route/status diagnostic proved materially changed endpoint evidence with eleven HTTP 200 responses. One fresh lifecycle then filled its entry, and one guarded manual-recovery close used a new durable reduce-only identity after an unrelated protective local halt. The final database is `COMPLETE`; ENTRY/CLOSE are both `RECONCILED`, and independent authoritative evidence proves zero regular/trigger orders and exact flatness. Do not run another Nado lifecycle before the post-3/3 commonality review.

## Extended — wallet/API Management setup and lifecycle

Status: `LEVEL C BLOCKED BEFORE WRITE ON PRICE-RATIO INTERPRETATION`.

- Existing local owner/Stark identities and the sole subaccount match; testnet claim completed, balance is readable, and zero orders/positions are authoritative. A fresh REST-valid API key reproduced v1 HTTP 503 and v2 RPC HTTP 404, excluding wallet, collateral, stale key, and quota causes.
- The local Level B runner now durably classifies pre-upgrade failures as sanitized `HTTP` or `TRANSPORT`; automated stream retries remain stopped after the observed provider v1 HTTP 503 and v2 HTTP 404.
- Credential-free probes prove the whole testnet WebSocket ingress fails before authentication (official API host HTTP 503; documented CDN host HTTP 403), while the same mainnet public stream works. Authenticated testnet REST account, orders, positions, trades, and history all return HTTP 200/`OK` and authoritative zero state.
- The accepted testnet-only REST fallback opens no stream, permits missing pagination only for exact empty lists, and preserves strict two-round identity/reconciliation barriers. One fresh production Level B completed six authenticated reads with verified identity, agreeing zero orders/positions, zero stream effects, and durable terminal `READY`.
- The accepted count-only empty-pagination correction is published at `dceed3ba67f79f67b96fb0c2f39d5ca23183e395`; fresh REST Level B passed. The following single sealed Level C invocation halted in `INITIAL_REST` with zero intents and zero dispatch because ten sequential reads take about 14.0 seconds and leave the order book about 8.8 seconds old against the honest 5-second freshness bound.
- The accepted parallel bounded observer records every required response receipt, rejects any stale mutable state, uses independent direct-TLS connections, and rechecks the complete observation both before durable claim and immediately before dispatch. The first launch environment lacked the pinned optional SDK and produced only an archived zero-byte file; after SDK 2.5.0 provisioning and a fresh READY Level B, the only sealed invocation stopped at `ENTRY_PREPARATION` with `PRICE_BOUND_INVALID`, zero intents, and zero dispatch.
- Current official evidence defines `limitPriceCap` and `limitPriceFloor` as ratios against mark price for limit orders; the current values are both `0.05`. The runner incorrectly compares the absolute BTC ask directly to those ratio fields. Open one fresh Luna-max Builder from current `main` to correct only this Extended price-ratio validation for the existing `LIMIT+IOC` entry/close contract, using the fresh mark price already present in `marketStats`. Preserve the five-second freshness, exact tick/depth/minimum/notional/balance/leverage, signing/write identity, no-replay, reconciliation, and final zero-order/exact-flat barriers; do not change product, order type, shared core, or other venues.
- After Chief review/integration, archive the zero-intent price-bound database byte-identically, require one fresh REST Level B, then run exactly one new sequential sealed Level C lifecycle. Any ambiguous write halts for authoritative reconciliation; success requires final authoritative zero orders and exact flatness.

## Completion

- Each venue receives one separate smallest-executable Level C lifecycle without a fixed USD ceiling. Strategy work begins only after all three finish with authoritative zero orders and exact flatness.
