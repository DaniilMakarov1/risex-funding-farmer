# Active bounded tasks

One slice per venue. Apply the A/B/C levels and safety core from `AGENTS.md`; operational run IDs are data, not source milestones.

## RISEx — classify local lifecycle-clear rejection

Status: `READY FOR LEVEL A FIXTURE START GATE`.

- Make one fixture-only candidate that records the observed lifecycle-clear rejection as a distinct sanitized safety failure instead of generic `validation_failed`. Preserve the consumed runtime row, no-rearm boundary, counter ordering, fail-closed predicate, and every transport/signing/write contract.
- Prove the classified terminal result occurs before every public, credential, signing, private, and write effect. Do not inspect secrets, access the network, repair/delete/migrate the operational lifecycle database, or authorize a new private-read runtime row. Local recovery and any fresh Level B observation remain separate gates after acceptance.

## Nado — classify Level B public failure

Status: `READY FOR LEVEL B FIXTURE START GATE`.

- Make only the smallest Nado-local correction that gives the durable private-read report a sanitized failure class sufficient to distinguish transport, HTTP, catalog schema, authentication, identity, and safety failures. Preserve the existing terminal row, no-rearm boundary, request sequence, parser, signing contract, and runtime/write identities; store no raw body, secret, signature, or account identity.
- Candidate work is fixture-only and must prove distinct transport-versus-`all_products` schema outcomes. It performs no network request, credential load, signing, private dispatch, account mutation, or write. After acceptance, a separate credential-free diagnostic gate may observe `all_products`; authenticated access remains unauthorized until that public gate succeeds.

## Extended — resume authenticated read-only readiness

Status: `WAITING FOR OFFICIAL STREAM AVAILABILITY`.

- Wait for a later external-state change; the latest credential-free handshake gate exhausted its transport allowance and ended on HTTP 503. After recovery, if the accepted runner is still source-bound, first make one minimal Level B runtime-run-ID decoupling candidate; then perform the fresh authenticated read under a separate operational gate.
- Validate required account/order/position semantics and authoritative zero orders/exact flatness. Add code/tests only for an observed contract defect.

## Completion

- After private readiness, each venue receives one separate minimum-notional Level C lifecycle (`<= USD 500`). Strategy work begins only after all three finish with authoritative zero orders and exact flatness.
