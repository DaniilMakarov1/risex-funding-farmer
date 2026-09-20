# HCR-21 — cycle-003 fallback reconciliation and truthful terminal explanation

## Active owner-authorized objective

On 2026-09-20 the owner explicitly authorized implementation of the complete correction plan for the latest random-cycle incident. Diagnose the exact read-only state of cycle-003's fallback order, then correct fallback reconciliation, retained mismatch evidence and the operator-facing terminal explanation. Install and publish only after independent Chief review. The owner authorizes exact read-only Robinhood Mainnet account/order/trade inspection for this incident using the existing protected Keychain credentials. No signing mutation, order, cancellation, transfer, leverage/margin change, historical-position adoption or new cycle is authorized.

## Immutable incident and known boundary

Preserve `operator-v1/cycle-003` exactly. Opening source account 27331 SELL POST_ONLY `0.00023 BTC @ 80366.8`, client index `11900171426322`, order `562950033842620`, filled fully against external account 9235. Receiver account 27337 MARKET BUY was never dispatched because the source fill was observed before receiver admission. The cycle then dispatched source account 27331 BUY MARKET IOC reduce-only `0.00023 BTC @ 80387.9`, client index `111033094521616`; dispatch response was accepted without order id. Current code found an order but discarded it after `fallback order identity or parameters conflict with the plan`, then stopped UNKNOWN. The journal does not prove cancellation, zero fill or current inventory.

## Ownership

Sole Chief task `01a0aafe-23fa-7c90-87a6-e807b3f6450a` owns live read-only diagnosis, contract, independent review, integration and publication. One fresh GPT-5.6 Luna max Builder owns production changes/tests in isolated branch `codex/spread-v1-fallback-reconcile`. Builder must not access credentials/network/live accounts, edit governing documents, merge or push main. No fixed Builder concurrency cap exists; each distinct objective still requires a fresh Builder and parallel writers require non-overlapping ownership.

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
