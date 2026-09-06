# Current status

## Active work — 2026-09-06

The owner-directed objective is to correct observed defects and independently verify public research-scanner data handling, causal modeling, bounded resources and reproducible reports, with explicit limits on what the evidence proves. No administrative or Builder-coordination blocker remains. The Goal card is confirmed `active`; its prior administrative block is resolved.

S3-B2 Builder task `01a0759e-98d6-7c82-86ff-ae53d713bca8` is confirmed completed and idle. It is released; no B2 implementation remains authorized. Candidate `419ba491549a820fb94e0fcb867c62dfeab362a0` on `codex/spread-s3-b2` is clean and preserved as UNACCEPTED evidence, not integrated. Only `s3_cycle.py` and its load tests changed. Source hash `b2231e335ba4b121953c02697954d790e616038106c599ad2e145db662f3b830` matches Chief mutation, 18 aggregation and constant-retention driver checks. Builder's exact-SHA suite passed 4046 / 3 skipped, with 95 focused tests and dependency/import/public-surface checks passing. A non-failing pending-task warning remains in unchanged legacy runtime. Chief checked final logs, scope, clean Git and byte-identical D1 output. Timed replay: 184.07 seconds, peak RSS 1,186,758,656 bytes; evidence is in `s3-b2-builder-final-20260906` and `s3-b2-builder-memory-20260906` under the campaign root.

The active next slice is S3-B3: one fresh visible Luna max Builder completes S3 plus kernel memory correction, reusing the exact B2 candidate only as unaccepted evidence. NEXT_TASK defines scope and acceptance. Chief independently reviews and alone integrates; no second Spread Builder may overlap.

A newly confirmed core-memory defect prevents whole-path acceptance. Accepted `cycle.py` SHA-256 `b91a33378855c8e00daff43aacfc54a7322033360ad4584bbe904d54fce59b4d` retains every unique full book in `_MutableCycle.books` and retains mutable terminal cycles. Chief direct probe retained 102/1002/2002 books after 100/1000/2000 additions, with about 80KB/831KB/1.58MB object overhead even when book levels are shared. Final measured candidate replay reached 1,186,758,656 bytes maximum RSS. The observed plateau after this campaign's early lane halts does not prove boundedness for longer active cycles. Evidence is in `B2-chief-core-book-retention.json` and the two `B2-chief-*-b2231e335ba4.json` runtime artifacts. B2 did not authorize kernel edits; its completed checkpoint is preserved and a fresh centrally authorized kernel-memory successor is now open. The goal remains active; this scope conflict has an available next correction, not an administrative impasse.

## Accepted implementation

S1 `05b46eb7a76a87509b4cb3f7d020f9c263772e82`, S2 `d9595420a24281fe9d0d2bc496d4b89c96ca8c80` and S3 `5e226dcd1da6627358cede3dc7b688567663d979` are accepted hypothetical public-research implementation. Historical acceptance details remain in Git. They do not establish executable profit or trading readiness.

S3-B1 `c0af376efc76f61a874ac01f241acf7342186964` is ACCEPTED and integrated at `004669d18872f00a06c0aee6a9e6346a1cd6d91b`. Campaign-budget accounting now streams records. Chief verified exact accounting on the real 3.38GB file with maximum RSS 46,415,872 bytes, 35 focused/reserve tests, old-source regression failure, final-SHA isolated Python 3.11 suite (4044 passed / 3 skipped), dependencies, public CLI import and Git checks. One initial permission-fixture failure under Chief-imposed umask 077 disappears under normal test umask 022; both full-run logs are retained. Builder final completion agrees with Chief evidence. No B1 work or shutdown wait remains.

## Independent audit — open correction work

A1 auditor completed its single verdict and is released. Verdict: REVISE before a new campaign or trading-readiness claim; the current DATA_INSUFFICIENT / INSUFFICIENT result is correct.

- Active B2: live driver retains full stream inputs; offline reading materializes the entire evidence file; reporting retains multiple run bundles. Require bounded memory while preserving existing report and evidence semantics.
- Subsequent bounded slices: full-book serialization cannot fit comparable original four-window traffic within the 4GiB envelope; failure terminal uses the scheduled deadline rather than observed failure time; halted-lane skips are omitted; NO_ENTRY and INSUFFICIENT_DEPTH reasons can contradict later fills or minimum-residue failures.
- Both lanes halted about 81 seconds after start: primary had an unexecutable 0.000047 BTC partial, stress had causal touch uncertainty. These are current policy outcomes, requiring a separate owner policy decision if changed. No successful hedge, exit or completed-cycle PnL was observed. Transport-close root cause remains unproven.

These findings are work to resolve, not a reason to pause the authorized offline corrections. New collection and trading remain separate gates.

## Closed campaign and durable evidence

Owner closed CYCLE-001-20260906 early: CLOSED_EARLY_BY_OWNER / DATA_INSUFFICIENT / INSUFFICIENT. D1-W1 failed with PUBLIC_SOCKET_TRANSPORT_FAILURE; D1-W2 was missed; D2-W1 and D2-W2 are OWNER_CANCELLED_UNRUN. Campaign heartbeat is PAUSED. No further market collection is authorized by this correction work.

Root: `/Users/daniilmakarov/Desktop/RISEx Spread Shadow/spread-shadow-runs/cycle-001-20260906`. Frozen release `23c70f79a7914bd82dc55fa94f1e699734258f46`; immutable manifest and consumed claim remain retained. Run `uupT4CQaE0cRywb4PH9wvOfJ`: 54,692 records / 3,380,019,851 bytes, SHA-256 `c0ffd6a5e5fe55160485f39c4b7b95d284e40cea79067ddfdaec1bc1523c21cb`.

Chief independently verified contiguous identities, sole physically-last failure terminal, nested event/gap accounting and Decimal fill arithmetic. Two offline reports are byte-identical (8,561 bytes; SHA-256 `c42abbf91b00c1e5360dda5ef73c427f7a76ff218c423e50bce0fab1e2b9cfd9`) and reproduce all 17 final results. Primary: 8 aborted, 1 unresolved, 0 complete; stress: 7 aborted, 1 unresolved, 0 complete. Primary net cashflow $3.75831283113 is not PnL; modeled RISEx position -0.000047 BTC remains unclosed. Remaining aggregate bytes: 914,947,445, including 536,870,912 closing reserve. Observed inputs span about 1540 seconds; failure terminal is 1137.707 seconds later than the first unexpected-close gap.

Historical CAL-001 remains DATA_INSUFFICIENT / INSUFFICIENT; HOLDOUT-001 is unrun and closed. Historical DG-007 remains NO_SNAPSHOT_EDGE. Their immutable details are in Git and runtime evidence. No threshold retuning, replacement interval, private access, signing, dispatch or trading is opened by removing stale administrative blockers.
