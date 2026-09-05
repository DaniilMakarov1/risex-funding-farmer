# S1 — Causal quote measurement, offline-only

Status: AUTHORIZED — fresh completion Builder after owner-requested context rotation; implementation not yet accepted.
Venue: central SPREAD, RISEx/Lighter public data only.
Objective: honest timing and exact partial-volume measurement for one immutable hypothetical resting quote, suitable for the subsequent minimal cycle kernel.
Accepted implementation baseline: `b8e9415e648543fd3435bc683eb649934c9dd0d5`.
Pre-gate accepted main: `2070c8bce43fbec22e71478591fc2fd4bc46611a`. Builder starts from the exact published main containing this gate; Chief dispatch records its full SHA.
Verification: Level A, no new public market requests or authenticated/venue-write work.
Authority: owner "разрешаю все" on 2026-09-05 accepts the supplied S1/S2/S3 public-only plan. Only S1 is active now; Chief independently accepts and publishes its checkpoint before opening S2. Role/model rules remain unchanged.

## Allowed and forbidden scope

- Source identity and separate ingress_received / normalized_ready / decision_ready timestamps; ingress captured immediately after frame receive before parsing or another await.
- Small explicit one-quote execution model: hypothetical activation, fixed price/quantity, delayed effective cancellation, no overlapping replacement, exact cumulative partial quantity and dedup, and explicit uncertain outcomes.
- Separate source-book receipt freshness, receipt skew, normalization/decision delay and resting-quote age diagnostics; compatible evidence plumbing only. Preserve historical evaluator semantics and canonical CAL/DG output; old evidence missing new fields cannot be upgraded into new causal evidence.
- Existing Spread package/tests plus only necessary additive changes to shared public models/adapters for preserving already-available identity. A concrete venue-contract conflict stops and returns to Chief before any broader venue-local implementation.
- A small independent numerical checker in focused tests, without importing production sizing/VWAP/fee/reconstruction helpers; independently specified adverse FULL/DELTA terminal books.
- No cycle positions, ledger/PnL engine, CLI campaign, market observation, private/credential/fee-reader/write paths, legacy strategy imports, dependency/framework expansion or governance edits by Builder. No undocumented server expiry or inferred cross-channel event order. Documentation study, if needed, is official-source read-only; market feeds remain closed.

## Acceptance and required evidence

- Trade before decision-ready and before activation cannot fill; input book update from the same match cannot retroactively make that match a fill. Late older events, missing/stale identity and causal ambiguity are distinguished from proven no-fill.
- Fills during cancel delay count; first partial may request cancellation, subsequent eligible partials accumulate before its effect; exact remaining quantity cannot go negative or be reset on book updates. Duplicate volume is consumed at most once; conflicting duplicate or recovery transition cannot produce false positive or false clean no-fill.
- Explicit deterministic ordering/equal-time boundary rules; activation/cancel delays are hypothetical assumptions. Use a later-block watermark filter only to the extent official/retained venue evidence establishes its semantics; never claim actual order activation.
- Independent checker recomputes exact common-step quantity, level notional, fees, entry edge and per-episode markout, including adverse arithmetic cases. A few FULL/DELTA chains have independently specified end state.
- Focused/adverse regressions for distinct risks and one final clean isolated Python 3.11 full suite on final candidate SHA, with dependency/import/compile, private/write-surface, diff/scope and clean Git evidence. Do not rerun full historical CAL replay without a specific compatibility risk.
- Before edits Builder reports root/branch/HEAD/status. Final report supplies exact SHA, changed files, tests, identity/causal evidence and limits. Chief reviews independently; Builder never self-accepts or integrates/pushes main.

## Stop and next action

Use one fresh visible GPT-5.6 Luna max Builder on `codex/spread-s1-finish` in a separate worktree from the exact published main containing this rotation gate. The predecessor has stopped at clean immutable checkpoint `b2e3c9cbfe3bef7a5300c1dd4a363cf3ccb337f3` (candidate commits `20535f6`, `9d131019`, `b2e3c9c`, based on `92c3086`). These in-project changes may be reused on the fresh branch as unaccepted implementation; do not merge them into main or rewrite the predecessor. This is owner-requested rotation, not formal REJECT.

The bounded completion is to independently inspect and finish the four Chief-reproduced defects: (1) pre-cancel received trades remain eligible despite later processing, while first-partial cancellation starts at actual processing-ready; (2) both exact QuoteVersion input witnesses retain original venue/session/recovery/revision bindings, including an actual factory-to-measurement regression and missing legacy evidence; (3) known equal/older RISEx blocks cannot become causal fills when an optional watermark is omitted, without invented cross-channel order; (4) freshness diagnostics evaluate each boolean condition, including a stale-input regression. The checkpoint reports fixes and 24 passing causal tests, but factory-path and stale-flag regressions and final full-suite validation remain incomplete. Check the complete resulting S1 diff for simple coherent behavior; do not add another execution model or broaden scope. One final isolated Python 3.11 full suite is required on the final SHA, followed by independent Chief review.

Prefer another fresh Builder at a substantive clean checkpoint rather than prolonged context-heavy correction loops. Stop for required scope expansion, unsupported venue semantics or non-convergence. After acceptance Chief publishes checkpoint; only then may separately bounded S2 begin under the owner's authorization. No market launch follows S1.

Historical CAL-001 remains DATA_INSUFFICIENT / INSUFFICIENT and HOLDOUT remains closed. Preserve `.scan-003/CAL-001.claim` and every evidence/report/audit file under `spread-shadow-runs/run-04NlPq5s8cSalaTngkOpSz6H/`; hashes and historical result remain in STATUS/Git.
