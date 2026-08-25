# Active bounded tasks

One slice per venue. Apply the A/B/C levels and safety core from `AGENTS.md`; operational run IDs are data, not source milestones.

## RISEx — wait for public book normalization

Status: `WAITING FOR EXTERNAL MARKET STATE`.

- The local lifecycle database is recovered and repeatedly passes `LifecycleClearBinding`; its exact protected pre-recovery backup and the immutable consumed runtime row remain preserved.
- The latest public BTC/USDC book was not crossed but had an approximately 18.83% spread against the accepted 0.30% safety bound. Wait for a later external market-state change; do not weaken the bound or repeat the authenticated gate while this blocker persists.
- After a credential-free book observation passes the existing bound, perform one fresh Level B run with another durable runtime ID. Do not dispatch an order, cancel, close, account mutation, or any other write.

## Nado — remove live risk price from stable hash

Status: `AUTHORIZED FIXTURE CORRECTION`.

- From exact published `main`, make the smallest Nado-local change that continues to require and strictly validate a positive canonical `risk.price_x18` in every product snapshot but excludes that observed live price from cross-snapshot stable comparison. Preserve stable risk weights, spot config, identity, kind, coverage, sparse contributions, and every account-risk gate.
- Add focused regressions accepting only cross-snapshot `risk.price_x18` drift while rejecting malformed/nonpositive per-snapshot prices and stable risk-weight/config contradictions. Do not load credentials, derive/sign, dispatch private operations, mutate account state, or write during development. A later Level B run requires a fresh runtime ID after independent acceptance.

## Extended — wait for account-stream access resolution

Status: `BLOCKED ON EXTERNAL ACCESS / USER AUTHORITY`.

- The official-SDK-conformant authenticated account-stream handshake still returns HTTP 403 while the same API key succeeds on authenticated REST. Do not repeat the gate or change code speculatively. Resolution now requires provider-side stream access normalization or separately authorized API-key reprovisioning/rotation through Extended API Management.
- After that external change, perform one fresh Level B run with another durable runtime ID. Do not send application frames beyond the accepted passive protocol/barrier behavior, sign, mutate account state, or dispatch any write.

## Completion

- After private readiness, each venue receives one separate minimum-notional Level C lifecycle (`<= USD 500`). Strategy work begins only after all three finish with authoritative zero orders and exact flatness.
