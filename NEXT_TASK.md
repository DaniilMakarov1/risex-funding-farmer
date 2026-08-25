# Active bounded tasks

One slice per venue. Apply the A/B/C levels and safety core from `AGENTS.md`; operational run IDs are data, not source milestones.

## RISEx — await separate local recovery authority

Status: `WAITING FOR USER AUTHORITY`.

- The distinct sanitized lifecycle-clear safety classification is accepted. Preserve the consumed runtime row and do not repair, delete, migrate, or replace the noncanonical operational lifecycle database without a separately authorized recovery gate.
- Do not authorize a fresh private-read runtime row, credential access, signing, network request, or write. The next bounded action must be an explicit user decision between continued halt and a separately specified local recovery path; any later Level B observation is another gate.

## Nado — diagnose public `all_products`

Status: `READY FOR CREDENTIAL-FREE DIAGNOSTIC GATE`.

- Make one bounded credential-free observation of the official `all_products` response through the accepted Nado parser and sanitized failure classifier. Allow only the initial transport attempt plus one retry, and only for a transport failure before a valid observation; HTTP, schema, identity, or safety failure is terminal.
- Do not load credentials, sign, dispatch a private request, mutate account state, or write. Preserve only the bounded sanitized result and required semantic evidence, never a raw body. Authenticated access remains unauthorized until this gate succeeds; add code/tests only for a newly observed contract defect.

## Extended — resume authenticated read-only readiness

Status: `WAITING FOR OFFICIAL STREAM AVAILABILITY`.

- Wait for a later external-state change; the latest credential-free handshake gate exhausted its transport allowance and ended on HTTP 503. After recovery, if the accepted runner is still source-bound, first make one minimal Level B runtime-run-ID decoupling candidate; then perform the fresh authenticated read under a separate operational gate.
- Validate required account/order/position semantics and authoritative zero orders/exact flatness. Add code/tests only for an observed contract defect.

## Completion

- After private readiness, each venue receives one separate minimum-notional Level C lifecycle (`<= USD 500`). Strategy work begins only after all three finish with authoritative zero orders and exact flatness.
