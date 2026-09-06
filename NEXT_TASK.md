# S4 — Prospective complete-cycle public campaign freeze

Status: Chief-only preparation under the owner's 2026-09-05 full-plan authorization. S1/S2/S3 are accepted. Exact accepted implementation: `5e226dcd1da6627358cede3dc7b688567663d979`; use a clean published release containing this implementation and current operator docs. No Builder active. D1-W1 terminated with PUBLIC_SOCKET_TRANSPORT_FAILURE; D1-W2 is missed. No observer is active. Remaining D2 windows and aggregate caps are unchanged. See STATUS for the offline replay checkpoint. The prospective schedule below is frozen before any market request.

Venue: central SPREAD, RISEx/Lighter public-only. Objective: obtain one bounded, reproducible execution-only observation of the fixed complete-cycle policy in SYSTEM_SPEC 0.21. Positive entry edge is not full-cycle PnL. Chief alone owns the operational gate, launches, evidence review and final verdict.

## Exact prospective campaign

- Campaign ID: `CYCLE-001-20260906`.
- Clean operational release: `23c70f79a7914bd82dc55fa94f1e699734258f46` (accepted S3 implementation plus operator docs).
- Operational checkout: `/Users/daniilmakarov/.codex/worktrees/cycle-001-release/RISEx Spread Shadow`.
- Absolute owner-only runtime root: `/Users/daniilmakarov/Desktop/RISEx Spread Shadow/spread-shadow-runs/cycle-001-20260906`.
- Manifest: runtime root plus `.s3-cycle/CYCLE-001-20260906/manifest.json`.
- Window `D1-W1`: 2026-09-06T02:15:00Z through 2026-09-06T03:00:00Z.
- Window `D1-W2`: 2026-09-06T03:15:00Z through 2026-09-06T04:00:00Z.
- Window `D2-W1`: 2026-09-07T02:15:00Z through 2026-09-07T03:00:00Z.
- Window `D2-W2`: 2026-09-07T03:15:00Z through 2026-09-07T04:00:00Z.

Moscow time is 05:15–06:00 and 06:15–07:00 on each day. All four intervals, policy and aggregate envelope are fixed now, not selected from outcomes. Launch each at its scheduled start with the accepted CLI; a delayed start cannot move the deadline. Missed/failed windows are not replaced. Check the retained claim and actual process before every launch; never duplicate a consumed attempt. The sole current Chief may schedule follow-ups through Codex. At an owner-authorized clean Chief rotation, transfer the existing heartbeat to the successor and stop the predecessor; never duplicate coordination.

## Required before any public request

Record and publish one exact campaign ID, absolute owner-only excluded storage root, clean release SHA, and four future 45-minute UTC start/end intervals over two calendar days, two per day. Then create the immutable manifest with `cycle-freeze` and verify exact parameters and create-once identity. No historical CAL/HOLDOUT/DG reuse, retrospective selection or fixture promotion. Do not launch before this record exists.

## Frozen policy and limits

Use accepted BTC/$100/1-bp RISEx maker SELL / Lighter Standard taker BUY cycle; accepted delays, sizing, fees, partials, exits and maximum holding time remain unchanged. Primary and stress are alternative evaluations, never additive. Funding stays UNKNOWN_EXECUTION_ONLY. Public unauthenticated data only; private/credential/fee-reader/signing/order/dispatch/trading paths remain closed.

Four fixed 45-minute windows; entry cutoff 42:45, hard market deadline 45:00, closing tail 135 seconds. Aggregate maximum 1,000,000 records and 4 GiB includes 100,000 records and 512 MiB closing reserves. No PnL/count-based stop, tuning, replacement, extension or retry-to-PASS. Retain consumed claims even after failure/missed launch. A resource/integrity/closing-data failure remains insufficient/unresolved.

## Evidence and completion

## Offline correction S3-B1 — streaming campaign budget

Owner authorized concrete bug fixes on 2026-09-06. One fresh visible Spread Builder (Luna max), Level A only, starts from the exact published main containing this gate. Observed defect: `_CycleCampaignBudget.load` in `src/risex_spread_shadow/s3_cycle.py` materializes `list(iter_records(path))`; the retained D1 file is 3,380,019,851 bytes. Replace only this whole-file retention with bounded streaming accounting, preserving campaign identity validation, exact record/byte totals, error classifications and existing cap/reserve semantics. Allowed files: this module and directly relevant Spread tests. No other refactor, economic/report schema change, public request, manifest/claim mutation or operational release replacement. Chief owns governance; Builder must not edit it, self-accept, merge, push main or spawn agents.

Acceptance: demonstrate the old materialization risk with a bounded adverse test; prove streaming consumption, exact multi-run accounting, malformed/empty stream and wrong-campaign behavior, and unchanged reserve boundaries. Run focused tests and one final-SHA isolated Python 3.11 full suite plus dependency/import/public-surface/Git checks. Report exact base/tip, root/branch/status before edits, evidence and limitations. This correction can be reviewed independently of CYCLE-001 but cannot replace its frozen release. Public collection remains idle until the recorded D2 windows; a launch-blocking defect must be escalated, never bypassed.

## Campaign evidence and completion

Preserve each exact run identity, manifest and serialized terminal; verify bounds, public-only surface and deterministic offline replay/report. Report turnover, holding/occupancy, unmatched duration, skips, normal/forced/aborted/unresolved counts, fees/cashflows, total/mean PnL, gross profit/loss, worst cycle, forced exit contribution and without-best-group result separately for primary/stress. Incomplete cashflow is not completed PnL.

Descriptive floors: 20 complete cycles and 20 filled dependence groups; five cycles in at least three windows spanning both days. Screens: primary positive each day, aggregate stress positive, primary without best dependence group positive. Groups are not proven independent. Separate measurement validity, sufficiency, economics and usefulness; any positive verdict stays hypothetical and confers no trading authority.

After exactly this campaign STOP, preserve evidence and state positive, negative or insufficiently established modeled economics under these rules. Propose at most one substantive policy change tied to an observed cause; no new campaign without a new owner decision. Implementation changes require a fresh bounded Builder gate, not ad hoc Chief code.
