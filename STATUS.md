# Current status

## Active work — 2026-09-06

The owner-directed objective is to correct observed defects and independently verify public research-scanner data handling, causal modeling, bounded resources and reproducible reports, with explicit limits on what the evidence proves. No administrative or Builder-coordination blocker remains. The Goal card is confirmed `active`; its prior administrative block is resolved.

S3-B3 is ACCEPTED at `f5d4b7d511bd579178333565cdf0e5e02f1eb3ec` and fast-forward integrated into main. Its Builder task `01a075c6-5aef-7541-b56b-09b74767f060` is confirmed completed/idle and released. No candidate or implementation is in flight. B2's unaccepted checkpoint was incorporated only through the independently reviewed combined B3 correction.

Chief verified scope/clean Git, two-pass replay integrity, bounded driver/report retention, duplicate/conflict/source-identity and temporal/rescheduled-boundary regressions, and 86 exact cycle-result digests across nine fixture profiles. Final isolated Python 3.11 suite: 4055 passed / 3 skipped in 100.45 seconds; dependency and import checks pass. An initial umask077 run reproduced the known permission-fixture harness failure; unchanged code passes under required test umask022, with both logs retained owner-only.

Final D1 replay on the accepted SHA reproduces all 17 results and the exact 8561-byte report, SHA-256 `c42abbf91b00c1e5360dda5ef73c427f7a76ff218c423e50bce0fab1e2b9cfd9`: 178.21 seconds, peak RSS 53,166,080 bytes, down from B2's 1,186,758,656 bytes. Active cycles retain at most three full-book witnesses per expected venue; compact identity digests still grow with admitted event count, and terminal latching releases book payloads/identity maps. Deep-book and 100/1000/2000-update probes pass. This proves bounded full-book retention within the record/byte envelope, not constant total memory for arbitrary inputs. Runtime evidence: `s3-b3-builder-20260906` and `B3-chief-*-d93a5f4a5130.json` / `B3-chief-final-surface-f5d4b7d.json` under the campaign root. The first transient harness hash discrepancy remains unexplained; the preserved final exact-output gate passes.

S3-T1 original candidate `15e14abcfe97f3f8e77eb3796bf134a597c8e199` remains REJECTED and immutable. S3-T1-R2 rotation checkpoint `4c6e1cd95de41d72a0b9134e5614d5dfd893cdf2` is UNACCEPTED, not formally rejected and not integrated. Its branch is `codex/spread-s3-t1-r2`, exact base `074bf956618fe4b3bc3e7499cf5f7ff2077f5fa2`, worktree `/Users/daniilmakarov/.codex/worktrees/0685/RISEx Spread Shadow`. Builder `01a07651-6665-7801-bad4-7a9c04b75d80` is confirmed completed/idle and released; the checkpoint is clean and no candidate/write is active.

Chief independently verified exact-tip scope/Git and isolated Python 3.11.5 imports, then ran the two bounded envelope/load test files with child umask022 and owner-only evidence: 26 passed / 3 failed in 13.49 seconds. Resource-failure terminal now lacks the required `FINAL_RESULT_PREFIX` marker; aggregate record accounting gets 22 rather than 28 rows; the aggregate byte-cap fixture does not raise. The globally shifted 12-second fixture clocks still fail to preserve intended fresh admissions near 11.1 seconds. Evidence: `s3-t1-r2-chief-checkpoint-4c6e1cd/focused.log` and `summary.json` under the campaign root; log SHA-256 `7b7ef44490e8fb3e9a59fb4b84b60cbb04aacab72131b9c5bb615e905f32a7de`. Builder reports compile pass. No full suite or D1 replay was run for this checkpoint.

Four additional Chief-confirmed prior-candidate defects and exact reproduction evidence are retained in `s3-t1-r2-chief-prefinal-findings.json`: post-deadline raw failure accepted with an earlier invalid model boundary; normal scheduled stream end followed by a final-result cap creates an unreadable resource prefix; a resource-prefix terminal can precede persisted stream inputs without rejection; pending unmatched duration uses first fill rather than the later unmatched-action start (1.1 seconds rather than 0.1). R2 contains partial corrections, but the reader max-observation guard and unmatched duration remain unfinished per Builder, and the corrected normal-close/resource path still needs exact-tip independent evidence. Preserve first transport failure across later storage errors and all prior pending/legacy checks. The source diff is 1322 lines plus 254 test lines; review necessity and duplication before acceptance.

Chief handoff: fresh successor `01a07669-9577-7d12-a012-5eaf0c50f05e` is designated for read-only orientation and receives sole coordination authority only after explicit activation. Predecessor `01a0764d-98d0-7af0-8c67-a91381a8771d` then stops project work. On activation the successor must create its own active Goal with the full original owner objective, unchanged success criteria and no token budget, and continue S3-T1-R3 using a fresh visible Builder. The predecessor goal is unachieved; stale old-task continuations are handoff-only. Rotation is not acceptance or mission completion.

## Accepted implementation

S1 `05b46eb7a76a87509b4cb3f7d020f9c263772e82`, S2 `d9595420a24281fe9d0d2bc496d4b89c96ca8c80` and S3 `5e226dcd1da6627358cede3dc7b688567663d979` are accepted hypothetical public-research implementation. Historical acceptance details remain in Git. They do not establish executable profit or trading readiness.

S3-B1 `c0af376efc76f61a874ac01f241acf7342186964` is ACCEPTED and integrated at `004669d18872f00a06c0aee6a9e6346a1cd6d91b`. Campaign-budget accounting now streams records. Chief verified exact accounting on the real 3.38GB file with maximum RSS 46,415,872 bytes, 35 focused/reserve tests, old-source regression failure, final-SHA isolated Python 3.11 suite (4044 passed / 3 skipped), dependencies, public CLI import and Git checks. One initial permission-fixture failure under Chief-imposed umask 077 disappears under normal test umask 022; both full-run logs are retained. Builder final completion agrees with Chief evidence. No B1 work or shutdown wait remains.

## Independent audit — open correction work

A1 auditor completed its single verdict and is released. Verdict: REVISE before a new campaign or trading-readiness claim; the current DATA_INSUFFICIENT / INSUFFICIENT result is correct.

- Memory correction is closed by accepted S3-B3 above.
- Subsequent bounded slices: full-book serialization cannot fit comparable original four-window traffic within the 4GiB envelope; failure terminal uses the scheduled deadline rather than observed failure time; halted-lane skips are omitted; NO_ENTRY and INSUFFICIENT_DEPTH reasons can contradict later fills or minimum-residue failures.
- Both lanes halted about 81 seconds after start: primary had an unexecutable 0.000047 BTC partial, stress had causal touch uncertainty. These are current policy outcomes, requiring a separate owner policy decision if changed. No successful hedge, exit or completed-cycle PnL was observed. Transport-close root cause remains unproven.

These findings are work to resolve, not a reason to pause the authorized offline corrections. New collection and trading remain separate gates.

## Closed campaign and durable evidence

Owner closed CYCLE-001-20260906 early: CLOSED_EARLY_BY_OWNER / DATA_INSUFFICIENT / INSUFFICIENT. D1-W1 failed with PUBLIC_SOCKET_TRANSPORT_FAILURE; D1-W2 was missed; D2-W1 and D2-W2 are OWNER_CANCELLED_UNRUN. Campaign heartbeat is PAUSED. No further market collection is authorized by this correction work.

Root: `/Users/daniilmakarov/Desktop/RISEx Spread Shadow/spread-shadow-runs/cycle-001-20260906`. Frozen release `23c70f79a7914bd82dc55fa94f1e699734258f46`; immutable manifest and consumed claim remain retained. Run `uupT4CQaE0cRywb4PH9wvOfJ`: 54,692 records / 3,380,019,851 bytes, SHA-256 `c0ffd6a5e5fe55160485f39c4b7b95d284e40cea79067ddfdaec1bc1523c21cb`.

Chief independently verified contiguous identities, sole physically-last failure terminal, nested event/gap accounting and Decimal fill arithmetic. Two offline reports are byte-identical (8,561 bytes; SHA-256 `c42abbf91b00c1e5360dda5ef73c427f7a76ff218c423e50bce0fab1e2b9cfd9`) and reproduce all 17 final results. Primary: 8 aborted, 1 unresolved, 0 complete; stress: 7 aborted, 1 unresolved, 0 complete. Primary net cashflow $3.75831283113 is not PnL; modeled RISEx position -0.000047 BTC remains unclosed. Remaining aggregate bytes: 914,947,445, including 536,870,912 closing reserve. Observed inputs span about 1540 seconds; failure terminal is 1137.707 seconds later than the first unexpected-close gap.

Historical CAL-001 remains DATA_INSUFFICIENT / INSUFFICIENT; HOLDOUT-001 is unrun and closed. Historical DG-007 remains NO_SNAPSHOT_EDGE. Their immutable details are in Git and runtime evidence. No threshold retuning, replacement interval, private access, signing, dispatch or trading is opened by removing stale administrative blockers.
