# Current status

## Central baseline

- Published `main == origin/main`; Git is the exact accepted-history authority.
- All three fixture lifecycle cores are integrated. Paper remains default; no venue is strategy-ready.

## Venue readiness

### RISEx

- Private-read op011 passed with complete counters, authoritative zero orders, and exact flatness; all invocations through op011 are consumed.
- The accepted fixture lifecycle now binds `client_order_id` to composite, wide, and resting order identities, matching order/fill records, the exact cancel action/body identity split, and a fresh authoritative market position. It preserves durable no-replay and terminal zero-order/exact-flat barriers.
- The next slice is an isolated fixture-tested Tier C operational adapter candidate. No credential access, signature, request dispatch, or live write is authorized before that candidate is independently accepted and receives its own one-shot Chief operational gate.

### Nado

- The fixed identity is securely provisioned; Ink Sepolia gas and test USDT0 collateral are available, with exactly 10 test USDT0 deposited.
- Private-read op003 stopped before credentials after a public catalog mismatch. A later single official public diagnosis received HTTP 403 and made no code change. The next action waits for public gateway availability; consumed invocations are never reused.

### Extended

- The fixed identity and API-key capability are securely provisioned. Account-shape witness op003 completed `CAPTURED` with every counter exactly `1/1` and no write.
- The exact `{status, data}` parser correction is accepted. Sealed private-read op004 is consumed and durably `BLOCKED`: all three first REST reads completed, then the first stream-open attempt failed before completion; no stream frame, second REST pass, barrier, or write occurred.
- A single credential-free diagnosis of the same current official testnet stream endpoint returned HTTP 503 from its load balancer. The lane waits for a demonstrated external recovery before a fresh invocation/store binding; op004 is never reused or retried.

## Exit condition

- Each venue must independently pass one bounded testnet place/reconcile/cancel/close lifecycle ending in authoritative zero open orders and exact flatness. Only then may a separate strategy-testnet task begin.
