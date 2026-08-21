# PAPER-007-FIX-005 — First Full-Scan Funding Freshness

## Objective

Prevent the first post-restart `FULL` scan and its Telegram digest from becoming deterministically `FUNDING_STALE` merely because the initial REST bootstrap completed later than its RISEx funding observations. Seed one non-blocking public refresh after runtime readiness so the existing 120-second freshness rule can be satisfied without changing economics or scan deadlines.

## Scope

- Branch: `codex/paper-007-fix-005-first-full-freshness` from current main.
- Start at most one existing single-flight background public refresh immediately after streams are started and `PAPER_RUN_READY` is persisted.
- Reuse the accepted 30-second shared public HTTP timeout and existing refresh path; do not add Telegram calculations or substitute stale values.
- Keep every `FULL`, `FOCUSED`, activation, cutoff, and lifecycle deadline non-blocking and unchanged.
- The active Stage B process and database remain untouched during implementation and review.

## Acceptance

- A deterministic regression reproduces an initial funding observation older than the initial scan completion and proves the first `FULL` scan uses the completed seeded refresh rather than reporting avoidable `FUNDING_STALE`.
- Another regression proves runtime readiness and scan deadlines do not wait for a gated or timed-out seeded refresh.
- Existing focused and full tests pass; compileall and `git diff --check` pass.
- No funding-age threshold, Scanner formula, fees, sizing, ranking, lifecycle, Telegram rendering/delivery, adapter endpoint, private/authenticated, real-order, or live-trading changes.

After Architect acceptance, safely restart the flat Stage B on the same database so the correction becomes active. Do not force-close a position.
