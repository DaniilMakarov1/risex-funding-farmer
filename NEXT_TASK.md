# Active bounded tasks

One slice per venue. Apply the A/B/C levels and safety core from `AGENTS.md`; operational run IDs are data, not source milestones.

## RISEx — remove the testnet-only maximum-spread rejection

Status: `AUTHORIZED FIXTURE CORRECTION`.

- The local lifecycle database is recovered and repeatedly passes `LifecycleClearBinding`; its exact protected pre-recovery backup and the immutable consumed runtime row remain preserved.
- In a fresh visible RISEx Builder worktree, remove only the maximum-spread rejection from the isolated RISEx testnet public/private-read and lifecycle path. Preserve fresh authoritative BBO, bid below ask, positive exact tick alignment, exact quantity/depth, price bounds, notional, durable identities, no-replay, reconciliation, and terminal zero-order/exact-flat checks; do not change paper or mainnet behavior.
- After accepted implementation and one fresh Level B run, proceed sequentially to the already-defined minimum-notional Level C lifecycle if and only if Level B is ready and pre-state remains safe.

## Nado — identify authoritative market-bearing products

Status: `AUTHORIZED CREDENTIAL-FREE DIAGNOSTIC GATE`.

- Do not retry the terminal Level B run or infer further non-market IDs from individual error responses. Using official Nado documentation and one bounded credential-free current observation, identify the authoritative endpoint/field that distinguishes market-bearing product IDs from account-only products; retain only aggregate counts and public IDs needed for contract evidence.
- Do not load credentials, derive/sign, dispatch private operations, mutate account state, or write. Any implementation must be a fresh visible Nado Builder slice and must preserve complete account-product safety while binding zero-order queries to the authoritative market set.

## Extended — wait for account-stream access resolution

Status: `BLOCKED ON EXTERNAL ACCESS / USER AUTHORITY`.

- The official-SDK-conformant authenticated account-stream handshake still returns HTTP 403 while the same API key succeeds on authenticated REST. Do not repeat the gate or change code speculatively. Resolution now requires provider-side stream access normalization or separately authorized API-key reprovisioning/rotation through Extended API Management.
- After that external change, perform one fresh Level B run with another durable runtime ID. Do not send application frames beyond the accepted passive protocol/barrier behavior, sign, mutate account state, or dispatch any write.

## Completion

- After private readiness, each venue receives one separate minimum-notional Level C lifecycle (`<= USD 500`). Strategy work begins only after all three finish with authoritative zero orders and exact flatness.
