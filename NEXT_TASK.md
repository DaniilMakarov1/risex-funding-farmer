# Active bounded tasks

One slice per venue. Apply the A/B/C levels and safety core from `AGENTS.md`; operational run IDs are data, not source milestones.

## RISEx — resume authenticated read-only readiness

Status: `READY FOR FRESH LEVEL B GATE`.

- The local lifecycle database is recovered and repeatedly passes `LifecycleClearBinding`; its exact protected pre-recovery backup and the immutable consumed runtime row remain preserved.
- Perform one fresh authenticated read-only observation with the accepted runtime-run-ID, counter, identity, redaction, and bounded-transport contracts. Allow only the initial attempt plus one retry after a qualifying transport failure before a valid observation. Do not dispatch an order, cancel, close, account mutation, or any other write.

## Nado — accept sparse official product IDs

Status: `READY FOR LEVEL A FIXTURE CORRECTION`.

- Make the smallest Nado-local fixture candidate that accepts unique non-negative product IDs as the authoritative set returned by official `all_products`, without requiring `set(range(len(products)))`. Preserve strict product schemas, unique IDs, product 0 collateral identity, exact account/product coverage, request ordering, failure classes, signing, counters, no-rearm, and every runtime/write identity.
- Prove the observed sparse-ID shape is accepted while duplicate, negative, malformed, missing-collateral, and cross-response product mismatches still fail closed. Candidate work performs no network, credential load, signing, private dispatch, account mutation, or write. After acceptance, perform a fresh Level B read-only gate.

## Extended — bind current official testnet stream

Status: `READY FOR LEVEL A FIXTURE CORRECTION`.

- Make one Extended-local fixture candidate that replaces the obsolete testnet stream host with the current official SDK testnet `stream_url` while preserving the exact `/account` path, direct TLS, `X-Api-Key` header-only authentication, no redirects/fallbacks, response semantics, and normal-startup isolation. Update only affected Extended fixtures/tests/source evidence.
- In the same bounded readiness candidate, replace the source-bound private-read invocation/store milestone with a fresh durable runtime run ID without changing account identity or any write identity. Candidate work performs no network, credential load, authenticated dispatch, signing, account mutation, or write. After acceptance, perform a fresh Level B read-only gate.

## Completion

- After private readiness, each venue receives one separate minimum-notional Level C lifecycle (`<= USD 500`). Strategy work begins only after all three finish with authoritative zero orders and exact flatness.
