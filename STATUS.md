# Current status

## Central baseline

- Paper remains the default product. Strategy execution, mainnet, real funds, and ungated private or write traffic are prohibited.
- The central baseline includes the accepted Extended private-read governance and the compact active-governance state. Its post-integration Python 3.11 suite passed with 1062 tests and `pip check` passed.
- The RISEx, Nado, and Extended deterministic lifecycle cores are centrally accepted. Infrastructure is frozen except for corrections strictly necessary to finish the three minimal operational lifecycles safely.

## RISEx

- The signer prerequisite, fixture lifecycle core, deterministic private-read path, and operational adapter are accepted.
- The single authorized operational private-read invocation stopped fail closed during its public phase after exactly nine public GETs. Durable evidence reports `PREFLIGHT_BLOCKED`; no credential load, nonce, signature, private request, order, cancel, close, or other write occurred.
- One bounded public-only diagnostic sweep proved all nine official endpoints available with HTTP 200 and identified three concrete stale fixture/decoder contracts: signer-row fields and expiration representation, full market symbol/config fields, and orderbook level fields including official `order_count`. Existing public normalization independently corroborates the market and book shape mismatch.
- Official OpenAPI enumerates candidate fields and types but does not define exact emitted/required key sets. Exactly three narrowly evidence-driven public schema captures established the affected closed key/type structures without retaining identities or values.
- The consumed one-shot remains immutable and must not be reset, rearmed, or retried. The strict decoder correction is Architect-accepted and independently Chief-accepted at `6c530eb9a0b13050b66e385325f767f6d45f2c10`; 1087 full tests and the dependency check pass. It is integrated into local `main`, but exact publication is blocked until local GitHub CLI authentication is completed. The connected GitHub contents API cannot preserve the accepted local commit object.
- RISEx has not proved an operational order lifecycle, authoritative zero open orders, or exact flatness.

## Nado

- The fixture lifecycle core and deterministic sealed private-read preflight are accepted and published through `bf6271797919f98ff77c7fe59e1b680ab6bcb3b1`; accepted implementation tip is `d193689c630ee9a1fadf2b032cf49ad96d0e3fb4`.
- The last operational private-read turn returned no authoritative redacted verdict. A bounded read-only search of the repository, Nado worktrees, project-named hidden home artifacts, and project/Nado-named temporary evidence found no operational store. The only SQLite hit was an explicitly synthetic cancellation reproduction and was excluded.
- The authoritative classification is `UNKNOWN — OPERATIONAL DURABLE EVIDENCE MISSING`. Absence of a discovered store does not prove that nothing was dispatched. No repeat network request, key load, signature, replay, or write is authorized unless the original injected store path and counters are recovered and prove an undispatched state.
- Nado has not proved an operational order lifecycle, authoritative zero open orders, or exact flatness.

## Extended

- Phase 0 and the fixture lifecycle core are accepted and published.
- The first isolated private-read implementation candidate is rejected and immutable. Independent official-source review resolved its apparent stream contradiction: official v1 uses exactly `/stream.extended.exchange/v1/account` with `X-Api-Key` during the WebSocket upgrade and has no application-level subscribe/ack exchange; JSON-RPC subscribe/ack belongs to the distinct v2 RPC protocol. The active correction must keep the v1 stream gap-free through REST round B and use it only as an intervening-activity barrier. No credential load, authenticated request, stream connection, signature, or write is currently authorized.
- Extended has not proved an operational order lifecycle, authoritative zero open orders, or exact flatness.

## Exit condition

- This phase ends only after all three venues independently pass their separately gated minimal place/reconcile/cancel/close lifecycle and finish with authoritative zero open orders and exact flatness. The accepted narrow RISEx manual-recovery terminal remains failure, not readiness.
- Once that condition is met, infrastructure work stops and the next task is a separate strategy-testnet measurement using the already accepted strategy. Required evidence is opportunity frequency, planned-versus-actual execution, fees, resolved funding, and complete net PnL; degraded or unresolved trades are excluded from profitability claims.
