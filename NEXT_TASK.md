# HCR-22 — fresh-book source-priority admission and bounded pair retry

## Finite objective and authority

Correct the race between source POST_ONLY LIMIT placement and receiver MARKET/IOC dispatch for `PAIRED_OPENING` and `PAIRED_CLOSING`. The receiver may be sent only when a fresh public-book observation and the exact source-order observation prove that the intended source quantity is still resting with admissible price-time priority. If priority is lost or cannot be proved, do not send the receiver; reconcile the exact source order and actual positions, and permit a fresh pair attempt only from a proven zero-fill, confirmed terminal/cancel state and the original positions.

This is an offline implementation, test, independent review, integration and publication task. It authorizes no Mainnet/testnet account read, signing, order, cancellation, Keychain access, new cycle or other financial mutation. Preserve all historical journals and run evidence immutably. The existing fee, quantity sampling, hold sampling, price-selection formula, minimum, balance, slippage, polling, freshness, resource and risk policies remain unchanged unless this contract records a later explicit owner decision.

Accepted base before contract work is `1cc1dc2e456413abe701ae84f41328f80fe5d9c9`, equal to fetched `origin/main` on 2026-09-20. Sole Chief owns this contract, independent review, integration and publication. A fresh visible GPT-5.6 Luna max Builder must own production implementation and tests in an isolated `codex/spread-v1-priority-guard-builder` worktree/branch; Builder must not edit governing documents, merge or push `main`, use credentials/network/live accounts, or spawn agents.

## Immutable incident evidence

Use only the saved records below; do not invent missing book levels, venue timestamps, versions or FIFO facts.

- `spread-shadow-runs/hood-cycle-race-latency-20260920/operator-v1/cycle-004/cycle.jsonl`: 20 lines, 31,295 bytes, SHA-256 `efbda6822eadf397c07b3e37c45585b97178f6587ff0481c4a00e419f15cbb42`.
- `spread-shadow-runs/hood-cycle-race-latency-20260920/operator-v1/cycle-004/opening.jsonl`: 16 lines, 16,443 bytes, SHA-256 `b70f67810076fdf918018edc460f3ef6a95a415db26f82a5e312f93d8dae355a`.

Cycle-004 proves source account 27331 submitted SELL LIMIT POST_ONLY `0.00026 BTC @ 80468.4`, order `562950034029918`; receiver account 27337 submitted BUY MARKET IOC with bound `80468.4`; receiver instead filled all `0.00026 @ 80467.5` against external account 16969, order `562950034029051`, trade `839100826`; source filled zero and was canceled. Fallback attempt 1 is known zero-fill/cancel `canceled-too-much-slippage`; attempt 2 fully closed the residual; final positions of both accounts were observed as zero. Official receipt fee absence leaves economics UNKNOWN. The last saved selection book preceded receiver dispatch by about 4.34 seconds, and the saved record does not contain a complete book snapshot at every critical boundary.

## Required behavior

1. Preserve the current first-stage automatic price selection: source SELL uses best ask minus one tick only when still strictly above best bid, otherwise best ask; source BUY mirrors this with best bid plus one tick only when still strictly below best ask, otherwise best bid. Receiver bound remains the source price.
2. Immediately before receiver dispatch, obtain a fresh public book and exact source order as close together as possible. Run independent safety reads concurrently where causal independence permits so the guard does not add avoidable sequential latency. Admission still requires every existing account, identity, readiness, margin, freshness, position and active-order check.
3. Match the exact source order by order id, owner account, market, client identity, side, LIMIT/POST_ONLY, price and expected remaining quantity. A missing, stale, terminal, changed, foreign or malformed observation cannot pass.
4. For source SELL, receiver BUY is admissible only when no external ask is below the source price and the exact source order's priority at its price is proved. Source BUY mirrors this: no external bid above the source price and own same-price priority proved.
5. API element order is not FIFO evidence. Unless official semantics or the available observation explicitly proves queue priority, an external order at the same price makes priority `UNKNOWN`; `LOST` and `UNKNOWN` both forbid receiver dispatch. Do not claim that a public market order is guaranteed to match a chosen counterparty.
6. If the exact source order is active and zero-filled but priority is `LOST` or `UNKNOWN`, cancel only that exact order, then fully reconcile order, trades and both account positions. A fresh pair attempt is admissible only after proven zero fill, confirmed terminal/canceled state, no ambiguous mutation and positions equal to the original pre-attempt positions.
7. Every pair attempt uses a fresh book/selected price and new client/order identities with durable attempt lineage. Never reuse an identity or blindly replay an ambiguous submit/cancel. Revalidate the preserved selected quantity against the fresh price, current grid/minimums and balances.
8. If source partially or fully fills externally during guard, cancellation or reconciliation, do not retry the original quantity. Use actual proven positions and the existing safe residual/fallback handling. For closing, confirmed reduce-only residual safety outranks waiting for an internal match and cannot wait forever.
9. Sample random quantity and hold duration once per cycle and preserve them across pair attempts. Start the hold timer only after a fully proved successful paired opening, never after source placement alone or an incomplete/unknown pair.
10. Apply the same guard, retry eligibility, lineage and reconciliation symmetrically to `PAIRED_CLOSING`, including BUY/SELL direction symmetry. An actual one-sided residual proceeds through the existing bounded, fresh-price, unique-identity reduce-only fallback rules.
11. Add compact durable diagnostics at selection, source placement and the pre-receiver boundary: attempt id/lineage; request start/end and local `observed_at`; venue timestamp/version only when actually supplied; best bid/ask and quantities; selected price/tick; exact own order id, client identity, owner, side, price and remaining quantity; external better-price volume; same-price evidence and priority status/reason. Bound and sanitize retained payloads.
12. Measure durations for public-book read, source submit/ack/visibility, concurrent pre-receiver checks, receiver submit/ack/fill observation and reconciliation. Optimize only evidenced avoidable latency through concurrency, connection reuse and removal of unnecessary immutable metadata reads. Do not change existing polling or freshness thresholds.
13. Operator and durable terminal results must independently classify `paired_execution` as `SUCCESS|PARTIAL|FAILED|UNKNOWN`, `inventory` as `CONFIRMED_FLAT|KNOWN_RESIDUAL|UNKNOWN`, and `economics` as `KNOWN|UNKNOWN`. Preserve the original cause and deepest fallback result. The cycle-004 replay must say that receiver filled against a better-priced external maker, source was zero-filled then canceled, the residual was later closed, final inventory is confirmed flat, and fee economics remains unknown. A successful later fallback must not leave a stale residual claim from an earlier zero-fill attempt.

## Pair-attempt budget decision

The owner authorized one finite budget and explicitly prohibited multiplying preparation retries by post-placement retries, but did not choose a numeric mutation-attempt cap. No new number may be selected silently. Chief proposal awaiting explicit owner decision: **maximum 3 complete pair attempts total per opening or closing operation**, using the existing `MAX_PREPARATION_ATTEMPTS = 3` as one shared budget. Each consumed preparation attempt or placed source order advances the same lineage; no Cartesian product of preflight and post-placement attempts is allowed. Exhaustion ends with a specific terminal reason and honest inventory state.

Until the owner accepts or replaces that proposal, production implementation of post-placement retry count is blocked. Offline diagnosis, contract work and non-policy design may proceed; no Builder may silently encode a different number or reinterpret `max_poll_count`, timeouts or freshness as the attempt budget.

## Mandatory adverse verification

- External better-price order appears after source placement: receiver is not sent.
- External older same-price order or unproved FIFO: priority is UNKNOWN and receiver is not sent.
- Exact source is absent from the fresh snapshot, the snapshot is stale/future, or identity/remaining quantity differs: no admission by assumption.
- Source partially or fully fills during guard/cancel: no replay of original quantity; actual positions/fills drive fallback.
- Confirmed zero-fill cancel from original positions: retry uses a fresh book/price and new identities after the approved shared budget is available.
- Ambiguous submit or cancel: no blind replay and no dependent receiver mutation.
- Book changes after a passed guard: actual fills/orders/positions are reconciled safely; no matching guarantee is claimed.
- Mirrored source BUY and `PAIRED_CLOSING` cases, including a one-sided closing residual and reduce-only fallback.
- Fallback attempt 1 known zero-fill then attempt 2 full fill: final inventory is flat and the operator result is not stale or misleading.
- Random quantity/hold are not resampled; hold starts only after proven paired opening.
- Budget exhaustion returns a clear terminal reason and truthful paired-execution/inventory/economics classifications.
- Offline regression reproduces only the known cycle-004 facts and explicitly marks missing historical boundary books as unavailable.
- Existing credential containment, exact-flat, no-replay, fallback identity/reconciliation, opening and closing regressions remain intact.

## Acceptance and publication

Builder must commit a reviewable candidate and deliver exact base/candidate SHAs, full changed-file list, focused/adverse commands and results, final clean isolated Python 3.11 suite result, measured synthetic latency evidence and remaining limitations to the Chief task. Code/test changes require focused adverse regressions, relevant integration checks and one clean isolated Python 3.11 full suite bound to the exact candidate. No skip or unrelated cleanup may hide base failures.

Chief must verify candidate/base identity, read the complete diff and relevant surroundings, independently assess the adverse cases and evidence provenance, and either issue one consolidated `CHANGES_REQUESTED` or accept. After acceptance, Chief alone integrates, updates `SYSTEM_SPEC.md`, `STATUS.md`, `NEXT_TASK.md` and `README.md` consistently, publishes `main`, and verifies remote `main`. Final owner report must include accepted SHA, behavior, checks, measured latency, limitations and an explicit statement that live validation was not run without new confirmation.
