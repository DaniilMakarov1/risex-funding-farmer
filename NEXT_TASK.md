# PAPER-007-STABILIZATION-004 — Atomic Causal Fill Provenance

Status: `AUTHORIZED — ONE BOUNDED CORRECTIVE SLICE`.

Builder starts from accepted `origin/main` `b73fd3c8dac7cba7dc434475d3eefa77d64b0880` on `codex/paper-007-stabilization-004`. The quarantined `bbb7c4ec397842531c30afef64793076a491b05a` refactor branch and its stashed `tests/test_scanner.py` work are rejected inputs: do not inspect further, copy, merge, rebase, or otherwise use them. Implement only atomic causal public-market provenance for every newly simulated paper fill and position close. Do not begin another slice.

## Ownership and minimal design boundary

- Runtime/lifecycle remains the sole decision owner; adapters retain venue normalization; Scanner retains route blockers and economics; repository persists only; Telegram remains delivery-only.
- Carry the same immutable trade or order-book input used by the decision through fill construction into one repository transaction. Do not reread mutable book state, recompute later, or add a cache, service, event bus, state machine, or parallel owner.
- Use one small additive provenance contract and additive SQLite persistence compatible with archived databases and pickles. Never mutate an archive. Legacy fills may lack provenance; every fill newly created by this implementation must have it.
- A maker proof records venue/market/side, owned order/version/limit, all qualifying public trade identities needed to prove cumulative trade-through, exchange and receipt/observation timestamps, price, quantity, aggressor and order-book-match semantics, plus causal decision/fill time.
- A taker proof records venue/market/side, current public book stream session and recovery generation, observed and received timestamps, logical/decision time, sequence and checksum when available, exact consumed depth levels, requested and executed quantity, notional, and exact VWAP/fill price.
- The existing book owner must capture identity and immutable depth together. A later mutable-book replacement must not alter or validate an earlier decision. Quiet books remain usable under the existing heartbeat rule; this slice does not invent a book-event age TTL.
- Fill, provenance, processed trade key, owned order/version, lifecycle state, position or completed trade, and close settlements affected by the decision persist in one atomic transaction. Publish the in-memory decision only after persistence succeeds. No member of that decision may survive repository failure or cancellation alone.
- Fail closed before a fill when provenance is missing, future-dated, disconnected/stale by existing health rules, owned by an obsolete session or recovery generation, for the wrong venue/market/side/version, or lacks exact requested depth. Enforce `observed_at <= causal_decision_at` and exact requested = executed quantity.
- Preserve fill dedupe, settlement uniqueness, exact two-leg quantity conservation, Decimal fees/funding/PnL formulas, cadence, routes, thresholds, Scanner `UNKNOWN`, refresh/socket behavior, Telegram behavior, public-only paper boundary, and all frozen `SYSTEM_SPEC.md` economics.

## Mandatory RED and acceptance matrix

A. On exact old main, a fixture shaped like rejected restart/close evidence fails an independent audit because maker and taker causal provenance is absent.
B. Candidate persistence alone independently proves maker eligibility and exact taker consumed depth, requested/executed quantity, notional, and VWAP for entry, normal/aggressive exit, and Hard Basis fills.
C. Focused production-path tests prove future observation, unhealthy/stale ownership, obsolete session, obsolete recovery generation, wrong venue/market/side/version, missing/insufficient depth, and post-decision mutable-book replacement cannot fill; a healthy quiet unchanged book retains existing semantics.
D. Injected repository exception and cancellation roll back the complete entry or close decision: no fill, provenance, processed key, order/version transition, position/lifecycle transition, settlement mutation, or completed trade survives alone, and in-memory authority is not published.
E. Replay of the same trade, fill identity, or settlement cannot duplicate a fill, provenance row, close, or authoritative settlement.
F. Entry and close legs conserve the exact canonical quantity, and persisted fills alone reproduce unchanged pair PnL, fees, recognized funding, and closed net PnL.
G. Preserve scanner/`UNKNOWN`, refresh, socket/recovery, settlement, reporting, safe-stop, and Telegram deterministic suites.
H. Run focused RED on old main and candidate, then full `pytest`, `compileall`, diff review, secret scan, and async task/quiescence checks. Builder reviews the diff and creates exactly one implementation commit; report is at most 20 lines.
I. R16 uses a fresh disposable copy of the original archive whose SHA-256 is `93e9b6793e76cec227d0fe40799a70d0416518568b0c228fc0808a681497df80`. The original hash must remain unchanged. The inherited `EXITING_AGGRESSIVE` position may close only when every new fill has complete causal provenance; otherwise it remains safely open and fail-closed.

## Post-deterministic operational gate

Only after deterministic acceptance, use a fresh disposable copy of the original archive with Telegram disabled and public paper data only. Run until the inherited position closes or a bounded materially informative window ends. Independently reconstruct every new fill identity, causal time, depth consumption, VWAP, quantities, fees, funding, and PnL from persistence; then safe-stop and prove SQLite integrity, no post-stop writes, and task/process quiescence. Diagnose the prior external SIGINT and possible Mac sleep only as a separate operational observation; do not add a speculative daemon/service. No Stage B or Telegram restart, merge, or push before independent Chief Reviewer approval.
