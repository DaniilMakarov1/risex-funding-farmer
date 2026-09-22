# HCR-27 — completeness audit

## Objective and authority

On 2026-09-22 the owner asked to verify and finish all five previously proposed improvements without Builders or repeat routine confirmations: stage latency, a unified offline run report, reproducible real incidents, crash-boundary verification and current concise documentation. Accepted base: `46ed54ee0d2d2e8762f8449374ca02e1f10d76e0`.

The owner's explicit sole-Chief instruction overrides the default role split for this objective. Chief owns implementation, tests, documents, self-review and integration/publication. Self-authored changes are not independently reviewed. Routine authorized edits, offline tests, dependencies, evidence and Git operations are pre-approved within tool restrictions; no global security/settings changes.

Scope is offline only. No live market/account reads, credentials/Keychain, real signing, orders/cancellations, new slots or monitoring. Preserve all original evidence and 42 recorded cycle hashes. No changes to fees, sizing/hold, minimums, slippage, freshness/delay, retry budgets, residual cleanup, exact priority, inventory/PnL semantics or trading authority. No other repository, new infrastructure or deferred campaign work.

## Acceptance and result

Candidate: `0b6be39a325e350618c25ccc746a65704b81e4de`. Final verification: **4749 passed, 3 existing skips, exit 0** in a clean isolated Python 3.11.5 checkout of the exact candidate (134.23 seconds).

- Added only measured gaps: successful quote read, preparation lock wait, nonce acquisition, signing-call elapsed and transport roundtrip. Distinguish private lookup from exact owner-bound public observation and preserve original quote/plan/intent timing. Old missing metrics remain unavailable; pure CPU/network/exchange fractions remain UNKNOWN and are not claimed achieved. No policy tuning.
- Unified report now exposes latest saved orders with timestamps and unresolved orders/intents, alongside existing plans, dispatch evidence, fills, stop reasons, separate pair/inventory/fee/funding conclusions. It remains streaming, offline, read-only, bounded and conservative on missing/conflicting evidence.
- Added sanitized hash-bound cycle-003/004/007 fixtures and distinguishing action/provenance assertions. Reused existing adverse execution regressions for source disappearance, external fill before receiver, delayed history, canceled zero-fill IOC and ambiguous transport; corrected inaccurate coverage pointers.
- Rechecked five existing actual-engine subprocess crash boundaries and restart no-replay/UNKNOWN inventory behavior. No duplicate test campaign or new recovery service.
- Updated STATUS/README and durable reporting semantics in SYSTEM_SPEC. Prior broad completion claim is corrected. Historical state is not current authority.

Acceptance evidence is stored in owner-only ignored `spread-shadow-runs/hood-diagnostics-20260922/audit-v1/`: exact candidate/import identity, focused/full test outputs, final saved reports and `final-audit.json`. Focused profile passed 459 tests. All 42 original hashes and seven consumed slots are preserved. Real-incident facts matched independently stated report expectations; execution/action barriers use existing regression assertions. Review is Chief self-review.

## Completion boundary

The clean isolated Python 3.11 full suite passed. Integration must preserve the exact tested source; verify main source identity and remote publication before reporting completion. No unchanged-code duplicate full suite is needed. Preserve all candidates/worktrees/evidence. No new live run, real latency measurement, owned-counterparty success or profit is established by completion. New live work requires a concrete owner decision, not reuse of historical permissions.
