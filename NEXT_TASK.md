# HCR-38 — Accurate execution reports and measured critical-path latency

Work alone with self-review. Accepted base: a006abf8afeaac9218a4da74aff9eb3b75fca368. Branch: codex/spread-v1-hcr38-report-latency.

## Authorized work

- Audit the latest four completed cycles at assignment (cycle-020…023), with cycle-019 and existing immutable evidence as bounded comparison. Preserve original inputs and bind findings to actual runtime/configuration. Distinguish saved facts from current positions and channel attribution from inference.
- Correct Telegram/terminal saved and live messages that fail to disclose proven external fills. Keep exact order/account/market/trade/position validation and truthful unknowns; an unsuccessful pair or missing fee must not erase independently proven execution. Preserve original outcomes; do not invent own matching, flatness, fees or PnL.
- Compare Telegram and terminal dispatch paths, source visibility/admission, preparation/signing and local request intervals. Fix demonstrated avoidable latency while preserving existing observation, no-replay, causal-ordering, price/quantity/minimum, expiry/freshness and reconciliation invariants. No timing threshold or strategy policy relaxation, speculative orders, unrequested market/size, new execution campaign or transport migration.
- Add bounded measurements and distinguishing offline regressions needed to substantiate the fix. Give evidence-backed remaining improvements separately from implemented and verified behavior.
- Integrate/publish the tested result and refresh the idle Telegram controller; do not interrupt a trading child. The controller's existing startup read-only reconciliation is permitted as part of activation. No agent-initiated order, cancellation, /run, /close, extra market collection or private diagnostic campaign.

## Acceptance

Explain the exact discrepancy between recent trade receipts and owner-visible messages. Verify source-only external, receiver-only external, reciprocal own, mixed, zero and truly unknown cases without weakening result/fee/flatness proof. Compare measured opening/closing intervals and explain limits of channel comparison. For execution/timing/result preservation changes, run adverse focused/integration checks and one final clean isolated Python 3.11 full suite; review the complete actual diff and source fingerprints. Preserve original journal hashes, safe process ownership, tested deployment identity and remote main verification. No live strategy success claim from offline checks.

Evidence: spread-shadow-runs/hood-report-latency-20260922/solo-v1/.
Status: IN_PROGRESS.
