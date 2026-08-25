# Active bounded tasks

One slice per venue. Apply the A/B/C levels and safety core from `AGENTS.md`; operational run IDs are data, not source milestones.

## RISEx — wait for public book normalization

Status: `WAITING FOR EXTERNAL MARKET STATE`.

- The local lifecycle database is recovered and repeatedly passes `LifecycleClearBinding`; its exact protected pre-recovery backup and the immutable consumed runtime row remain preserved.
- The latest public BTC/USDC book was not crossed but had an approximately 18.83% spread against the accepted 0.30% safety bound. Wait for a later external market-state change; do not weaken the bound or repeat the authenticated gate while this blocker persists.
- After a credential-free book observation passes the existing bound, perform one fresh Level B run with another durable runtime ID. Do not dispatch an order, cancel, close, account mutation, or any other write.

## Nado — identify live field in full public sequence

Status: `AUTHORIZED CREDENTIAL-FREE DIAGNOSTIC GATE`.

- Reproduce the exact credential-free public prefix through `linked_signer` and `subaccount_info` once. Retain only the sanitized first predicate and bounded product section/field names whose values differ between the complete catalog snapshots; never retain values, bodies, or account identity. Treat the complete observation as terminal.
- Do not load credentials, derive/sign, dispatch a private trigger request, mutate account state, or write. Open a fresh visible Nado Builder only if this proves another concrete local contract defect.

## Extended — wait for account-stream access resolution

Status: `BLOCKED ON EXTERNAL ACCESS / USER AUTHORITY`.

- The official-SDK-conformant authenticated account-stream handshake still returns HTTP 403 while the same API key succeeds on authenticated REST. Do not repeat the gate or change code speculatively. Resolution now requires provider-side stream access normalization or separately authorized API-key reprovisioning/rotation through Extended API Management.
- After that external change, perform one fresh Level B run with another durable runtime ID. Do not send application frames beyond the accepted passive protocol/barrier behavior, sign, mutate account state, or dispatch any write.

## Completion

- After private readiness, each venue receives one separate minimum-notional Level C lifecycle (`<= USD 500`). Strategy work begins only after all three finish with authoritative zero orders and exact flatness.
