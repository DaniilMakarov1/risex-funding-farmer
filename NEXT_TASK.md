# Active bounded tasks

One slice per venue. Apply the A/B/C levels and safety core from `AGENTS.md`; operational run IDs are data, not source milestones.

## RISEx — wait for public book normalization

Status: `WAITING FOR EXTERNAL MARKET STATE`.

- The local lifecycle database is recovered and repeatedly passes `LifecycleClearBinding`; its exact protected pre-recovery backup and the immutable consumed runtime row remain preserved.
- The latest public BTC/USDC book was not crossed but had an approximately 18.83% spread against the accepted 0.30% safety bound. Wait for a later external market-state change; do not weaken the bound or repeat the authenticated gate while this blocker persists.
- After a credential-free book observation passes the existing bound, perform one fresh Level B run with another durable runtime ID. Do not dispatch an order, cancel, close, account mutation, or any other write.

## Nado — resume authenticated read-only readiness

Status: `READY FOR FRESH LEVEL B GATE`.

- Perform one fresh authenticated read-only observation with the accepted sparse product-set, runtime-run-ID, failure-class, counter, identity, redaction, and bounded-transport contracts. Allow only the initial attempt plus one retry after a qualifying transport failure before a valid observation.
- Validate exact account state, zero regular/trigger orders, and exact flatness. Do not mutate account state or dispatch an order, cancel, close, or any other write.

## Extended — resume authenticated read-only readiness

Status: `READY FOR FRESH LEVEL B GATE`.

- Perform one fresh authenticated read-only observation with the accepted current official stream host, fresh durable runtime run ID, header-only API key, counter, identity, redaction, and bounded-transport contracts. Allow only the initial attempt plus one retry after a qualifying transport failure before a valid observation.
- Validate required account/order/position semantics and authoritative zero orders/exact flatness. Do not sign, mutate account state, or dispatch an order, cancel, close, or any other write.

## Completion

- After private readiness, each venue receives one separate minimum-notional Level C lifecycle (`<= USD 500`). Strategy work begins only after all three finish with authoritative zero orders and exact flatness.
