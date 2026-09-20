# HCR-21 — COMPLETE; cycle-003 fallback reconciliation and truthful terminal explanation

## Completed finite objective

HCR-21 is complete offline and installed. Accepted Builder candidate `d3f98e6b21a6059a2dfbb62cb8e2e507cc1819bf` was independently reviewed and integrated by merge `0c886856b484e0a662ddb9f850be9219da75d271`. Preserve all branches, worktrees, commits, journals and evidence exactly.

The accepted implementation strictly recognizes the observed terminal canceled IOC without treating zero filled plus zero remaining as an identity conflict; preserves exact mismatch evidence; distinguishes full, partial, terminal zero-fill/cancel, rejected and UNKNOWN fallback states; continues only known residuals through fresh account/book reconciliation; and reports receiver dispatch state plus durable positions and observation times in Russian output. Empty or malformed order identity remains decoder UNKNOWN and blocks later mutation. Source fill before receiver admission still forbids receiver dispatch. Price, quantity, minimum, freshness, pacing and slippage policies are unchanged.

Chief focused verification passed `144` tests plus exact saved-response and durable-output probes. The final clean candidate full suite completed with `4601 passed, 3 skipped, 4 failed`; the same four unrelated S3 envelope tests reproduced identically on accepted `main` (`1 passed, 4 failed`) because the current host monotonic clock is below their synthetic `2700`-second evidence timestamp. HCR-21 changes no S3 source/tests; no skip, masking or unrelated repair was added. Installed help and CANCEL passed, no `cycle-004` was created, and all six `cycle-003` hashes remained unchanged. No live mutation or new cycle occurred.

No additional live read or financial action is authorized. A future cycle or another policy change requires a new owner decision.

## Completed authorization record

On 2026-09-20 the owner explicitly authorized implementation of the complete correction plan for the latest random-cycle incident. Diagnose the exact read-only state of cycle-003's fallback order, then correct fallback reconciliation, retained mismatch evidence and the operator-facing terminal explanation. Install and publish only after independent Chief review. The owner authorizes exact read-only Robinhood Mainnet account/order/trade inspection for this incident using the existing protected Keychain credentials. No signing mutation, order, cancellation, transfer, leverage/margin change, historical-position adoption or new cycle is authorized.

## Immutable incident and known boundary

Preserve `operator-v1/cycle-003` exactly. Opening source account 27331 SELL POST_ONLY `0.00023 BTC @ 80366.8`, client index `11900171426322`, order `562950033842620`, filled fully against external account 9235. Receiver account 27337 MARKET BUY was never dispatched because the source fill was observed before receiver admission. The cycle then dispatched source account 27331 BUY MARKET IOC reduce-only `0.00023 BTC @ 80387.9`, client index `111033094521616`; dispatch response was accepted without order id. Pre-fix code found an order but discarded it after `fallback order identity or parameters conflict with the plan`, then stopped UNKNOWN. The original journal alone did not prove cancellation, zero fill or current inventory; the separate authorized read-only packet supplied those later facts.

## Ownership

Sole Chief task `01a0aafe-23fa-7c90-87a6-e807b3f6450a` completed live read-only diagnosis, contract, independent review, integration and publication. Fresh GPT-5.6 Luna max Builder task `01a0bde6-a393-79e1-b2b7-9f65a4259c88` delivered production changes/tests from isolated branch `codex/spread-v1-fallback-reconcile` without credentials/network/live accounts, governing-document edits, merge or push of main. Its completed worktree and candidates remain preserved.

## Required behavior

- Retain and journal every safely sanitized fallback order observation rejected by matching, together with an exact field-by-field mismatch map. Include account, market, order/client identity, side, type, TIF, reduce-only, quantities, price, status and observation time. Never retain secrets or raw unbounded responses.
- Align fallback MARKET/IOC comparison with official SDK/venue semantics proven by the cycle-003 response. Preserve strict account, market and exact client-order identity. Do not relax a field without observed/official evidence. Distinguish representation differences from a true identity conflict.
- Reconcile a known terminal fallback into exactly one of: full fill and zero residual; partial fill with fresh residual; terminal zero-fill/cancel; rejected; or UNKNOWN. A known partial or terminal zero-fill may continue only through the existing fresh-account/residual loop. UNKNOWN forbids dependent mutation. Never infer zero position from an accepted dispatch.
- Preserve a fresh account observation after fallback reconciliation, bind it to observation time and report it separately from order outcome. Continue to stop if identity, history or causal state is ambiguous.
- Make cycle terminal reason reflect the deepest decisive event. For this incident it must explain source external fill, receiver not dispatched, fallback attempted, and exact fallback reconciliation result. Preserve the opening reason separately. Russian output must state whether the receiver was never sent versus canceled, whether fallback dispatch was accepted, whether its fill is known, and the last known positions with observation time.
- Keep the current safety policy: a source fill before receiver admission does not authorize receiver dispatch. Do not add price widening, slippage percentages, new retry caps, matching guarantees or other trading-policy changes. Existing fresh executable bound and repeat-residual behavior remain unchanged.
- Preserve cycle-001/002/003 and all journals. No replay or new live cycle during development/verification.

## Acceptance

The live read-only packet must record exact order/account/trade facts or an explicit unavailable boundary without credentials. Builder tests the observed cycle-003 response plus full/partial/zero-fill/canceled/rejected/identity-conflict/malformed/stale cases, exact mismatch evidence, continuation only after known reconciliation, final reason precedence, Russian output and legacy behavior. Chief reviews the complete diff, independently replays the saved response and adverse cases, then runs one final clean isolated Python 3.11 full suite. Installed verification is offline only and consumes no new cycle slot.
