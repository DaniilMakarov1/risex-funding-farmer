# Active bounded tasks

One slice per venue. Apply the A/B/C levels and safety core from `AGENTS.md`; operational run IDs are data, not source milestones.

## RISEx — recover local lifecycle database

Status: `AUTHORIZED LOCAL RECOVERY GATE`.

- Inspect the noncanonical lifecycle database read-only, identify the exact canonicality failure, and preserve the consumed runtime row as immutable evidence. Before any mutation, create and verify a protected recoverable backup; then perform only the smallest venue-local repair, migration, or replacement needed for a fresh canonical runtime database.
- This gate performs no network request, credential access, signing, private read, or write. Prove the recovered database passes `LifecycleClearBinding` and that the historical failed database and runtime row remain recoverable. A fresh Level B observation is a later separate gate.

## Nado — identify public safety predicate

Status: `AUTHORIZED CREDENTIAL-FREE DIAGNOSTIC GATE`.

- Make one new bounded credential-free `all_products` diagnostic that records only the sanitized failing predicate/phase and required aggregate semantic evidence, never a raw body. Allow the initial attempt plus one retry only for a transport failure before a valid observation; any HTTP, schema, identity, or safety result is terminal.
- Do not load credentials, sign, dispatch a private request, mutate account state, or write. If the public contract passes, a later authenticated Level B gate may proceed under the standing read-only authority; add code/tests only for a concrete observed contract defect.

## Extended — diagnose official stream availability

Status: `AUTHORIZED CREDENTIAL-FREE AVAILABILITY DIAGNOSTIC`.

- Make one fresh credential-free DNS/TLS/WebSocket availability diagnostic against the exact official testnet account-stream endpoint. Allow the initial attempt plus one retry only after a transport failure before a valid HTTP observation; HTTP 503 or another complete HTTP/semantic result is terminal. Preserve only sanitized DNS/TLS/HTTP class and bounded endpoint identity, never credentials or response bodies.
- If the endpoint is available, first make the smallest Extended-local runtime-run-ID decoupling candidate if the accepted runner remains source-bound, then perform a fresh authenticated read under the standing Level B authority. Add code/tests only for an observed contract defect.

## Completion

- After private readiness, each venue receives one separate minimum-notional Level C lifecycle (`<= USD 500`). Strategy work begins only after all three finish with authoritative zero orders and exact flatness.
