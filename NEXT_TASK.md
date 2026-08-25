# Active bounded tasks

One slice per venue. Apply the A/B/C levels and safety core from `AGENTS.md`; operational run IDs are data, not source milestones.

## RISEx — wait for public book normalization

Status: `WAITING FOR EXTERNAL MARKET STATE`.

- The local lifecycle database is recovered and repeatedly passes `LifecycleClearBinding`; its exact protected pre-recovery backup and the immutable consumed runtime row remain preserved.
- The latest public BTC/USDC book was not crossed but had an approximately 18.83% spread against the accepted 0.30% safety bound. Wait for a later external market-state change; do not weaken the bound or repeat the authenticated gate while this blocker persists.
- After a credential-free book observation passes the existing bound, perform one fresh Level B run with another durable runtime ID. Do not dispatch an order, cancel, close, account mutation, or any other write.

## Nado — fresh Level B after normalized public snapshots

Status: `AUTHORIZED LEVEL B READ-ONLY GATE`.

- Current separate public evidence now passes the complete account validator, constituting an external-state change. Perform one fresh operational Level B run with a new durable runtime ID; public checks must complete before credential load, and the signed trigger request remains read-only.
- Preserve the initial-attempt-plus-one-transport-retry rule and sanitized failure classes. Do not mutate account state or dispatch any order, cancel, close, deposit, withdrawal, or other write. A complete failure is terminal and must not be replayed.

## Extended — wait for account-stream access resolution

Status: `BLOCKED ON EXTERNAL ACCESS / USER AUTHORITY`.

- The official-SDK-conformant authenticated account-stream handshake still returns HTTP 403 while the same API key succeeds on authenticated REST. Do not repeat the gate or change code speculatively. Resolution now requires provider-side stream access normalization or separately authorized API-key reprovisioning/rotation through Extended API Management.
- After that external change, perform one fresh Level B run with another durable runtime ID. Do not send application frames beyond the accepted passive protocol/barrier behavior, sign, mutate account state, or dispatch any write.

## Completion

- After private readiness, each venue receives one separate minimum-notional Level C lifecycle (`<= USD 500`). Strategy work begins only after all three finish with authoritative zero orders and exact flatness.
