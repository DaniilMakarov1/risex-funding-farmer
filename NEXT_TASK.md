# Active bounded tasks

One slice per venue. Apply the A/B/C levels and safety core from `AGENTS.md`; operational run IDs are data, not source milestones.

## RISEx — fresh authoritative pre-write barrier

Status: `READY FOR LEVEL B RUNTIME-RUN-ID START GATE`.

- Make only the smallest RISEx-local correction that replaces the consumed source-bound private-read invocation/store identity with fresh runtime run identities in one protected durable journal. Preserve the accepted read sequence, counters, parsers, credential confinement, lifecycle-clear barrier, and every write-intent identity; do not touch the accepted Level C binding or lifecycle semantics.
- Candidate work is fixture-only with no credential load, signature, network/private request, nonce consumption, or write. After acceptance, a separate Level B gate must freshly verify official config/domain/router/authorization, active signer, zero orders, and exact flatness before any later Level C write gate.

## Nado — classify Level B public failure

Status: `READY FOR LEVEL B FIXTURE START GATE`.

- Make only the smallest Nado-local correction that gives the durable private-read report a sanitized failure class sufficient to distinguish transport, HTTP, catalog schema, authentication, identity, and safety failures. Preserve the existing terminal row, no-rearm boundary, request sequence, parser, signing contract, and runtime/write identities; store no raw body, secret, signature, or account identity.
- Candidate work is fixture-only and must prove distinct transport-versus-`all_products` schema outcomes. It performs no network request, credential load, signing, private dispatch, account mutation, or write. After acceptance, a separate credential-free diagnostic gate may observe `all_products`; authenticated access remains unauthorized until that public gate succeeds.

## Extended — resume authenticated read-only readiness

Status: `WAITING FOR OFFICIAL STREAM AVAILABILITY`.

- Check the official stream endpoint with one credential-free bounded handshake. After recovery, if the accepted runner is still source-bound, first make one minimal Level B runtime-run-ID decoupling candidate; then perform the fresh authenticated read under a separate operational gate.
- Validate required account/order/position semantics and authoritative zero orders/exact flatness. Add code/tests only for an observed contract defect.

## Completion

- After private readiness, each venue receives one separate minimum-notional Level C lifecycle (`<= USD 500`). Strategy work begins only after all three finish with authoritative zero orders and exact flatness.
