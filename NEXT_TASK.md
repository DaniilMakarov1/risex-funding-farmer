# Active bounded tasks

One slice per venue. Apply the A/B/C levels and safety core from `AGENTS.md`; operational run IDs are data, not source milestones.

## RISEx — remove the testnet-only maximum-spread rejection

Status: `AUTHORIZED FIXTURE CORRECTION`.

- The local lifecycle database is recovered and repeatedly passes `LifecycleClearBinding`; its exact protected pre-recovery backup and the immutable consumed runtime row remain preserved.
- In a fresh visible RISEx Builder worktree, remove only the maximum-spread rejection from the isolated RISEx testnet public/private-read and lifecycle path. Preserve fresh authoritative BBO, bid below ask, positive exact tick alignment, exact quantity/depth, price bounds, notional, durable identities, no-replay, reconciliation, and terminal zero-order/exact-flat checks; do not change paper or mainnet behavior.
- After accepted implementation and one fresh Level B run, proceed sequentially to the already-defined minimum-notional Level C lifecycle if and only if Level B is ready and pre-state remains safe.

## Nado — exclude the special NLP vault token from orderbook queries

Status: `AUTHORIZED FIXTURE CORRECTION`.

- In a fresh visible Nado Builder worktree, preserve product 11 (`NLP_USDT0`) in catalog/account/balance/health safety coverage but exclude it only from regular `subaccount_orders` orderbook queries, alongside collateral product 0. Bind this narrowly to the fixed testnet contract and add distinct regression/adverse evidence; do not generalize from ticker guesses or relax any other product coverage.
- After accepted implementation, perform one fresh Level B run. Do not dispatch a write unless it reaches full readiness; then the already-authorized minimum-notional Level C lifecycle may proceed sequentially under the existing Nado contract.

## Extended — provider/API Management resolution required

Status: `BLOCKED ON EXTERNAL ACCOUNT-STREAM ACCESS`.

- Stop automated retries: published SDK 2.5.0 plus compatible `websockets` reaches its source-configured old account-stream host and receives HTTP 503, while current official source/accepted runner reaches the new host and receives HTTP 403 with the same REST-valid key. No local transport correction is evidenced.
- User/provider action: in Extended Testnet API Management confirm the key was generated for the current Starknet Sepolia account/environment and is enabled for private account WebSocket streams; if the UI exposes no stream permission, ask Extended support to reconcile the PyPI 2.5.0 testnet stream host with current source and explain/clear the new-host 403 for this REST-valid key. Key rotation/reprovisioning remains unauthorized until the user chooses it. After provider-side change, rerun one fresh Level B before any write.

## Completion

- After private readiness, each venue receives one separate minimum-notional Level C lifecycle (`<= USD 500`). Strategy work begins only after all three finish with authoritative zero orders and exact flatness.
