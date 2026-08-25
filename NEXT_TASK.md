# Active bounded tasks

One slice per venue. Apply the A/B/C levels and safety core from `AGENTS.md`; operational run IDs are data, not source milestones.

## RISEx — distinguish order-book safety predicate

Status: `AUTHORIZED CREDENTIAL-FREE DIAGNOSTIC GATE`.

- The local lifecycle database is recovered and repeatedly passes `LifecycleClearBinding`; its exact protected pre-recovery backup and the immutable consumed runtime row remain preserved.
- Make one separate credential-free observation of the exact accepted BTC/USDC public orderbook and retain only bounded aggregate evidence sufficient to distinguish `best_bid >= best_ask` from `best_ask > best_bid * (1 + MAX_BOUND_FRACTION)`, never raw levels or account identity. Allow one retry only for a qualifying transport failure before a valid observation; a complete HTTP/schema/safety result is terminal.
- Do not load the credential source, derive/sign, dispatch a private request, mutate account state, or write. Add code/tests only for a concrete observed contract defect; a later authenticated run must use another fresh durable runtime ID.

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
