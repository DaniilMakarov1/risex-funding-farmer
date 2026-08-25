# Active bounded tasks

One slice per venue. Apply the A/B/C levels and safety core from `AGENTS.md`; operational run IDs are data, not source milestones.

## RISEx — wait for public book normalization

Status: `WAITING FOR EXTERNAL MARKET STATE`.

- The local lifecycle database is recovered and repeatedly passes `LifecycleClearBinding`; its exact protected pre-recovery backup and the immutable consumed runtime row remain preserved.
- The latest public BTC/USDC book was not crossed but had an approximately 18.83% spread against the accepted 0.30% safety bound. Wait for a later external market-state change; do not weaken the bound or repeat the authenticated gate while this blocker persists.
- After a credential-free book observation passes the existing bound, perform one fresh Level B run with another durable runtime ID. Do not dispatch an order, cancel, close, account mutation, or any other write.

## Nado — accept bounded current `subaccount_info` size

Status: `AUTHORIZED FIXTURE CORRECTION`.

- From exact published `main`, make the smallest Nado-local change that gives current complete public `subaccount_info` responses a finite defensible ceiling above 65,536 bytes while retaining strict JSON, timeout, redirect, host, freshness, and fail-closed checks. Keep unrelated public responses bounded and add focused boundary/oversize regressions for the distinct risk.
- Do not load credentials, derive/sign, dispatch a private trigger request, mutate account state, or write during development. After independent acceptance, a later Level B run must use another fresh durable runtime ID.

## Extended — make stream headers truthful and SDK-conformant

Status: `AUTHORIZED FIXTURE CORRECTION`.

- From exact published `main`, make the smallest Extended-local change so the actual account-stream handshake explicitly sends the official SDK user-agent form together with `X-Api-Key`, and the reported header-name evidence exactly reflects the actual request. Add focused regressions for exact headers and secret redaction; do not change endpoint, account identity, frame behavior, retry policy, or REST semantics.
- The SDK-conformant diagnostic still returned HTTP 403, so this correction must not claim operational readiness. Do not send application frames, sign, mutate account state, or dispatch an order, cancel, close, or any other write. A later full read requires a separate external-state/account-access gate and fresh durable runtime ID.

## Completion

- After private readiness, each venue receives one separate minimum-notional Level C lifecycle (`<= USD 500`). Strategy work begins only after all three finish with authoritative zero orders and exact flatness.
