# HCR-23 — COMPLETE: offline execution audit and bounded corrections

## Accepted result and next boundary

SDK candidate `c589f888699139935b69ccf9844b307141905087` and state correction `629501331aebc19998127a890b23fb81ea2b74aa` passed independent full-diff review and six adverse Chief probes. Final clean isolated Python3.11 suite:4645 passed,3 skipped, exit0. Current finite implementation objective is complete. Remaining delivery is accepted publication/operator identity verification; evidence is `spread-shadow-runs/hood-offline-audit-20260921/chief-v1`. There is no active Builder assignment or market process. Future live validation requires a new explicit owner action; no automated continuation, real read/sign/order/cancel or new cycle is authorized by this completed audit.

Two extra causes found during independent candidate review were corrected in the same healthy state Builder session: unexplained account-only zeros after partial close could still certify flatness, and failed journal sequence reread leaked its acquired lock. Final per-account evidence must agree with observed positions; lock cleanup now covers every failed acquisition path. No policy changes were made.

## Objective and authority

On 2026-09-21 the owner appointed this task Chief for a broader independent audit and correction of the currently used Robinhood random-cycle system before future live testing. Accepted base is `9d1ce057b331b1e5dc77e4fd38fbe83c330b3764`. The previous Chief task is idle/completed. Scope is the existing hood_handoff launcher, SDK boundary, paired execution, retries, reconciliation, residual closure, immutable journal and operator explanation. Legacy scanner/paper/other venue modules are out of scope.

This authorizes implementation through fresh Builders, offline probes, synthetic integration/latency measurements, independent review, clean Python 3.11 full-suite verification, accepted integration/publication and offline operator installation. No account/Keychain access, public market collection, real signing/orders/cancellations or new live cycles. Existing accounts, prices, quantity/hold sampling, fees, freshness/request/polling thresholds, retry limits and residual policies remain unchanged. Perfect matching or live performance is not promised.

## Finite audit and correction slices

1. SDK uncertainty and observation age: a transport timeout, malformed response or undecidable send result must not become an authoritative exchange rejection. An ambiguous fallback must stop dependent writes across both accounts. Preserve explicit rejection and known local pre-send failure distinctly where supported. Check submit/cancel, engine and fallback end-to-end using synthetic signing/transport. Account position/balance observations must not receive a newer timestamp merely because a later active-orders read finishes. Reject conflicting selected-market position identities rather than choosing a duplicate.
2. Measure avoidable SDK transport overhead. Reuse explicit HTTP connections only with owned cleanup, request-local authentication, unchanged timeouts and exactly one mutation request without redirect or retry. No increase in account-read fan-out or causal changes. Synthetic measurements must distinguish eliminated setup from unknown live latency.
3. Chief independently reviews remaining admission/retry/close/fallback, restart/no-replay, sizing and output. Fix concrete counterexamples within established behavior only. Include inaccurate cycle-004 fixture account/side/position and fabricated fee evidence; preserve original journals. Record further bounded findings here before assignment. No generic infrastructure rewrite.

## Additional independently reproduced findings

- Journal `append` ignores a short `os.write`: with an injected half-write it returns SOURCE_DISPATCH_INTENT successfully while the journal cannot be decoded. Require complete durable intent before any mutation; short writes must be completed or fail closed, with no hidden destructive repair or replay. Audit the same bounded write primitive for lock/launch records where directly relevant.
- A source submit raising TimeoutError, missing terminal order/incomplete trade history, and fresh observed zero positions currently produces overall UNKNOWN but inventory CONFIRMED_FLAT. Preserve observed zero positions separately; confirmed-flat inventory requires no unresolved submitted order and complete causal evidence. No account-only zero observation may upgrade an ambiguous write.
- Account payload with two selected-market position rows (0 and +0.2) is silently reduced to the first row; an active-orders delay of 3 seconds restamps the earlier position from time1000 to1003. SDK slice must reject duplicate/conflicting identity and preserve original age.
- Cycle-004 fixture's first fallback incorrectly uses source27331 BUY/-0.00026 instead of receiver27337 SELL/+0.00026; its second fee is fabricated. Correct the fixture from preserved known facts, keeping unavailable fee UNKNOWN.

## Ownership and completion

Chief task `01a0aafe-23fa-7c90-87a6-e807b3f6450a` owns contract, audit, review and sole integration/push. Fresh Builder sessions use configured Builder role GPT-5.6 Luna max with explicit isolated named worktrees/file ownership; historical Builders are not reused. Standard processing requested; no speed verification claim absent client evidence. Builders must not spawn agents, edit governing files, merge/push main, access credentials or make live requests. Completion returns through collaboration delivery; no management polling loops.

Acceptance requires independently reproduced before/after adverse cases, targeted SDK/engine/cycle integration, actual full diff review, preserved no-replay/exact-flat/credential/fee semantics, measured transport improvement and one final clean isolated Python 3.11 full suite on the integrated candidate. Preserve candidates/evidence outside tracked source; no skips/masking or unrelated repairs. Update SYSTEM_SPEC/STATUS/README, verify remote main and installed import identity. State live-validation boundaries explicitly.
