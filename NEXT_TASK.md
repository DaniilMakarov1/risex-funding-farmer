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

## Extended — reproduce account-stream access with the exact official SDK

Status: `AUTHORIZED FINAL AUTHENTICATED DIAGNOSTIC`.

- In one isolated temporary Python environment, use the published official Extended Python SDK 2.5.0 and its exact testnet `StreamClient` account subscription with the existing protected REST-valid API key. Retain only package version, fixed host/path, sanitized HTTP/error class, and whether the upgrade/first passive frame succeeded; do not print the key or account data, rotate/reprovision credentials, sign, mutate account state, or write.
- If the official SDK also receives HTTP 403, stop retries and report provider-side key/account-stream entitlement, environment/key generation, or ingress policy as the remaining causes for the user to check in Extended API Management/support. If it succeeds, open a fresh visible Extended Builder only for the concrete local transport difference, then rerun Level B.

## Completion

- After private readiness, each venue receives one separate minimum-notional Level C lifecycle (`<= USD 500`). Strategy work begins only after all three finish with authoritative zero orders and exact flatness.
