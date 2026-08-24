# Current status

## Central baseline

- Paper remains the default product. Strategy execution, mainnet, real funds, and ungated private or write traffic are prohibited.
- The central baseline includes the accepted deterministic Extended sealed private-read implementation at `1551cb328ca1cf41bc4c4a49541c7e5f301ec5e6`. Its post-integration Python 3.11 suite passed with 1159 tests and `pip check` passed.
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
- The old invocation remains permanently `UNKNOWN — OPERATIONAL DURABLE EVIDENCE MISSING`. Absence of its store does not prove that nothing was dispatched, and its invocation ID, store, signature, and `recvTime` must never be reused or treated as retry authority.
- An Architect-accepted governance candidate now defines one genuinely new read-only `list_trigger_orders` Query with a new invocation ID and a new durable store. Official pinned Nado SDK sources separate Trigger Query at `POST [TRIGGER_ENDPOINT]/query` from Execute at `POST [TRIGGER_ENDPOINT]/execute`; the accepted module's fresh server-time-derived 30-second `recvTime` is stricter than the 100-second gate maximum. The existing sealed implementation is sufficient, so no Builder or implementation change is active.
- The new operation still requires a separate Chief Coordinator operational gate before its store creation, public traffic, owner-key load, signature, or private Query. This candidate performs none of those actions and does not alter the old `UNKNOWN` classification.
- Nado has not proved an operational order lifecycle, authoritative zero open orders, or exact flatness.

## Extended

- Phase 0 and the fixture lifecycle core are accepted and published.
- The deterministic sealed private-read implementation is Architect-accepted, independently Chief-accepted, and integrated into local `main` at `1551cb328ca1cf41bc4c4a49541c7e5f301ec5e6` on exact base `dc83bb209bb8c51194e2e0eda3c42166db9a59ba`. Acceptance evidence is 72 focused tests, 1159 full clean Python 3.11 tests, and a clean dependency check. It pins official v1 `/stream.extended.exchange/v1/account`, uses `X-Api-Key` only in the upgrade header with no application-level subscribe/ack, keeps one gap-free activity barrier active through exhaustive REST round B and the final barrier, applies response-time freshness checks, and persists redacted one-shot terminal evidence.
- Publication is blocked solely because the local HTTPS Git transport lacks GitHub credentials; `origin/main` remains `ed6b0f076200b4f5316cd2341e8d8a3e0e16c8b1`. No real credential load, private REST request, WebSocket connection, signature, or write was performed or is authorized. Any later real private REST plus v1 stream invocation requires a separate Chief operational gate with a new durable one-shot identity.
- Extended has not proved an operational order lifecycle, authoritative zero open orders, or exact flatness.

## Exit condition

- This phase ends only after all three venues independently pass their separately gated minimal place/reconcile/cancel/close lifecycle and finish with authoritative zero open orders and exact flatness. The accepted narrow RISEx manual-recovery terminal remains failure, not readiness.
- Once that condition is met, infrastructure work stops and the next task is a separate strategy-testnet measurement using the already accepted strategy. Required evidence is opportunity frequency, planned-versus-actual execution, fees, resolved funding, and complete net PnL; degraded or unresolved trades are excluded from profitability claims.
