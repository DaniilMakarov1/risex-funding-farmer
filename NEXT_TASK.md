# Active bounded tasks

One slice per venue. Apply the A/B/C levels and safety core from `AGENTS.md`; operational run IDs are data, not source milestones.

## RISEx — fill-seeking taker lifecycle

Status: `LOCAL MARKET-PRICE-BOUND CORRECTION READY`.

- Current official documentation states that market-order `price_ticks` is the slippage bound. The accepted BUY `MARKET+IOC` runner sends zero ticks, which explains its accepted zero fills against a positive ask. Open one fresh Luna-max Builder from current `main` to bind `price_ticks` to the already-validated adverse ask bound and add focused/adverse regressions without changing signing, sizing, spread policy, durable identity, reconciliation, close, or terminal barriers.
- After Chief review/integration, require a fresh authoritative zero-order/exact-flat pre-state and exactly one sequential sealed Level C lifecycle. Never repeat the zero-price vector or replay any historical intent.

## Nado — sequential Level C lifecycle

Status: `LEVEL C COMPLETE`.

- A bounded route/status diagnostic proved materially changed endpoint evidence with eleven HTTP 200 responses. One fresh lifecycle then filled its entry, and one guarded manual-recovery close used a new durable reduce-only identity after an unrelated protective local halt. The final database is `COMPLETE`; ENTRY/CLOSE are both `RECONCILED`, and independent authoritative evidence proves zero regular/trigger orders and exact flatness. Do not run another Nado lifecycle before the post-3/3 commonality review.

## Extended — wallet/API Management setup and lifecycle

Status: `REST FALLBACK CORRECTION READY`.

- Existing local owner/Stark identities and the sole subaccount match; testnet claim completed, balance is readable, and zero orders/positions are authoritative. A fresh REST-valid API key reproduced v1 HTTP 503 and v2 RPC HTTP 404, excluding wallet, collateral, stale key, and quota causes.
- The local Level B runner now durably classifies pre-upgrade failures as sanitized `HTTP` or `TRANSPORT`; automated stream retries remain stopped after the observed provider v1 HTTP 503 and v2 HTTP 404.
- Credential-free probes prove the whole testnet WebSocket ingress fails before authentication (official API host HTTP 503; documented CDN host HTTP 403), while the same mainnet public stream works. Authenticated testnet REST account, orders, positions, trades, and history all return HTTP 200/`OK` and authoritative zero state.
- Open one fresh Luna-max Extended Builder from current `main` to replace only the testnet stream barrier with two bounded agreeing strict REST rounds. Bind the durable external ID, returned Extended order ID, exact order/history rows, matching trades carrying both identities, zero open orders, and exact flatness; require complete bounded pagination and reject unrelated state. Preserve credential isolation, signing, nonce/expiry, no replay, IOC/reduce-only lifecycle, and normal-startup isolation. Do not claim mainnet readiness from this fallback.
- After Chief acceptance, run a fresh REST-only Level B. Only then authorize one sequential Extended Level C lifecycle.

## Completion

- Each venue receives one separate smallest-executable Level C lifecycle without a fixed USD ceiling. Strategy work begins only after all three finish with authoritative zero orders and exact flatness.
