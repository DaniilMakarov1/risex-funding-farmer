# HCR-28 — global offline regression and bounded bug fixes

## Current objective

Owner requested global tests and bug correction on 2026-09-22. Accepted base: `c880e2f79ae39c9dd5fb2f01d4041dbe2f12b824`. Run the complete Python 3.11 offline suite, inspect adverse execution/restart/report boundaries and fix reproduced defects within existing behavior. Preserve historical input hashes and existing regression coverage. No speculative refactoring or policy changes.

The owner's instruction to work personally without Builders and repeated routine confirmations remains in force. Chief owns source/tests/docs/integration and self-review; no independent review is claimed. Worktree: `/Users/daniilmakarov/.codex/worktrees/hcr28-global`, branch `codex/spread-v1-hcr28-global`. No other writer is assigned.

## Authority and acceptance

Offline tests, bounded implementation corrections, dependency setup, local evidence and accepted Git publication are authorized. No live requests, credentials, real signatures/orders/cancellations, new cycle slots or collection. Fees, size/hold, freshness/deadlines, retries, cleanup, ownership/priority and inventory/PnL policy stay unchanged. Other repositories and frozen legacy functionality are outside correction scope; full-suite failures there must be classified, not silently repaired.

Reproduce each concrete defect before correction and add a distinguishing regression when needed. Review actual diff, run affected checks and one final clean isolated Python 3.11 full suite on the exact candidate after code/test changes. Preserve initial failures and all 42 original cycle hashes. Evidence goes in ignored owner-only `spread-shadow-runs/hood-global-tests-20260922/`. Verify integrated source equals tested candidate and remote main before claiming publication. No test result establishes live performance or successful real trading.

## Result

Candidate `64006758d15ad48cd6fafef04a950f922a3c2431` corrects malformed fallback-account and dispatch-plan crashes in the offline report. Evidence: 416 malformed-payload variants, 10 crashes before and zero after; 12 adverse regressions reproduced failure before correction; 65 focused tests passed afterward. Baseline full suite passed 4749 tests with 3 existing skips. Final verification: **4761 passed, 3 existing skips, exit 0**, Python 3.11.5, 135.36 seconds. The skips concern optional frozen Extended testnet dependencies (`x10`, `fast_stark_crypto`); no new skips were added.

All seven historical reports render; 42 original hashes and cycle slots are unchanged. Review covers the complete two-file implementation/test diff and surrounding validation; this is Chief self-review. Publish only the exact tested source after final suite success and verify remote main. No automatic follow-up, trading campaign or Builder is assigned.
