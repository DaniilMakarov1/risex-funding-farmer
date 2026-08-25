# Active bounded tasks

One slice per venue. Apply the A/B/C levels and safety core from `AGENTS.md`; operational run IDs are data, not source milestones.

## RISEx — wait for public book normalization

Status: `WAITING FOR EXTERNAL MARKET STATE`.

- The local lifecycle database is recovered and repeatedly passes `LifecycleClearBinding`; its exact protected pre-recovery backup and the immutable consumed runtime row remain preserved.
- The latest public BTC/USDC book was not crossed but had an approximately 18.83% spread against the accepted 0.30% safety bound. Wait for a later external market-state change; do not weaken the bound or repeat the authenticated gate while this blocker persists.
- After a credential-free book observation passes the existing bound, perform one fresh Level B run with another durable runtime ID. Do not dispatch an order, cancel, close, account mutation, or any other write.

## Nado — identify `subaccount_info` schema defect

Status: `AUTHORIZED CREDENTIAL-FREE DIAGNOSTIC GATE`.

- Make one separate credential-free reproduction through the exact public sequence ending at `subaccount_info` and retain only the sanitized failing schema phase/predicate plus bounded aggregate field/product coverage, never a raw body or account identity. Allow one retry only for a qualifying transport failure before a valid observation; a complete HTTP/schema/safety result is terminal.
- Do not load credentials, derive/sign, dispatch a private trigger request, mutate account state, or write. Add code/tests only for a concrete observed contract defect; a later authenticated run must use another fresh durable runtime ID.

## Extended — classify authenticated stream-open failure

Status: `AUTHORIZED LEVEL B DIAGNOSTIC GATE`.

- Make one separate authenticated read-only handshake diagnostic against the accepted current official account-stream URL and retain only sanitized transport/HTTP/status classification and bounded endpoint identity, never the API key, headers, or response body. Allow one retry only for a qualifying DNS/TLS/connection failure before a valid HTTP observation; a complete HTTP/auth/safety result is terminal.
- Do not send application frames, sign, mutate account state, or dispatch an order, cancel, close, or any other write. Add code/tests only for a concrete observed contract defect; a later full read must use another fresh durable runtime ID.

## Completion

- After private readiness, each venue receives one separate minimum-notional Level C lifecycle (`<= USD 500`). Strategy work begins only after all three finish with authoritative zero orders and exact flatness.
