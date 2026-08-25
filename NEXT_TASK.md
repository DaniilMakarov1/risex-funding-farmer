# Active bounded tasks

One slice per venue. Apply the A/B/C levels and safety core from `AGENTS.md`; operational run IDs are data, not source milestones.

## RISEx — wait for public book normalization

Status: `WAITING FOR EXTERNAL MARKET STATE`.

- The local lifecycle database is recovered and repeatedly passes `LifecycleClearBinding`; its exact protected pre-recovery backup and the immutable consumed runtime row remain preserved.
- The latest public BTC/USDC book was not crossed but had an approximately 18.83% spread against the accepted 0.30% safety bound. Wait for a later external market-state change; do not weaken the bound or repeat the authenticated gate while this blocker persists.
- After a credential-free book observation passes the existing bound, perform one fresh Level B run with another durable runtime ID. Do not dispatch an order, cancel, close, account mutation, or any other write.

## Nado — isolate `subaccount_info` envelope mismatch

Status: `AUTHORIZED CREDENTIAL-FREE DIAGNOSTIC GATE`.

- The prior schema diagnostic is terminal and must not be replayed. Make one new, narrower credential-free request to public `subaccount_info` and retain only top-level key names, bounded value types, response status/request-type class, and presence/absence flags needed to locate the envelope mismatch; never retain values from `data`, a raw body, or account identity. Allow one retry only for a qualifying transport failure before a valid observation; a complete HTTP/schema/safety result is terminal.
- Do not load credentials, derive/sign, dispatch a private trigger request, mutate account state, or write. Add code/tests only for a concrete observed contract defect; a later authenticated run must use another fresh durable runtime ID.

## Extended — test official-SDK handshake conformance

Status: `AUTHORIZED LEVEL B DIAGNOSTIC GATE`.

- The prior authenticated HTTP 403 diagnostic is terminal and must not be replayed. Make one new authenticated read-only handshake against the accepted current official account-stream URL using the official SDK's explicit `User-Agent` form together with the existing header-only API key. Retain only sanitized transport/HTTP/status classification and bounded endpoint identity, never the API key or response body. Allow one retry only for a qualifying DNS/TLS/connection failure before a valid HTTP observation; a complete HTTP/auth/safety result is terminal.
- Do not send application frames, sign, mutate account state, or dispatch an order, cancel, close, or any other write. Add code/tests only for a concrete observed contract defect; a later full read must use another fresh durable runtime ID.

## Completion

- After private readiness, each venue receives one separate minimum-notional Level C lifecycle (`<= USD 500`). Strategy work begins only after all three finish with authoritative zero orders and exact flatness.
