# Active bounded tasks

One slice per venue. Apply the A/B/C levels and safety core from `AGENTS.md`; operational run IDs are data, not source milestones.

## RISEx — fresh authenticated pre-write barrier

Status: `READY FOR LEVEL B OPERATIONAL GATE`.

- Run one fresh sealed RISEx authenticated private-read operation with its runtime journal identity. It may load only the fixed provisioned signer capability for the accepted auth contract and must freshly verify official config/domain/router/authorization, active signer, zero orders, exact flatness, and lifecycle-clear state; no order/cancel/close write is authorized.
- Require a complete durable terminal report. Any semantic, authentication, identity, safety, unclassified, or ambiguous outcome stops without rearm or write; only a separately classified transport-only failure may use the Level B retry ceiling. A successful read enables a later separate Level C operational gate but never dispatches it automatically.

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
