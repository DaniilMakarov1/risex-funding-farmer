# PAPER-005 — Lifecycle and PnL

## Goal

Implement the complete in-memory open-position lifecycle: authoritative funding reconciliation, HOLD decisions, normal/aggressive hedge-maker exit, Hard Basis taker-close, gaps/recovery, restart contracts, actual closed PnL, and data-quality evidence. No SQLite persistence, report aggregation, final CLI, or live execution.

## Mandatory Design Checkpoint

Before code changes, report the proposed minimal contracts and atomic flows for settlement authority/replacement, lifecycle-recognized vs applied funding, HOLD/EXIT, normal→aggressive timing, exit version fill/reprice, missing RISEx depth, gaps/recovery, Hard Basis in both directions, complete close, and restart from all five states. Show exact formulas, sticky fields, event/reason mappings, and fixture/test cases. Do not implement until Architect explicitly approves.

## Deliverables

- One authoritative settlement per key with only frozen transitions; APPLIED_RATE replaces ESTIMATED and all PnL derives from rows/fills/fees.
- LifecycleRecognizedFunding includes relevant elapsed estimates/applied values and controls HOLD/EXIT; applied-rate closed PnL is reporting-only and UNKNOWN until complete.
- Before target resolution hold only for strict remaining-funding improvement; after it, construct only latest next cycle; unknown exits. EXITING never returns to HOLDING.
- Normal exit maker uses normal pricing; exactly 10 seconds unfilled transitions to sticky aggressive pricing. Reprice every 10 seconds; same trade-through/dedup/version rules as entry. No timed taker fallback.
- Lost RISEx reverse depth cancels exit version, preserves EXITING mode/timer/position, and recreates in same mode after recovery.
- Event-driven Hard Basis uses exact-q taker unwind on both legs at 4% BTC/ETH or 6% other Top‑5; unavailable quote keeps position open and degrades metrics.
- Gap start/end evidence pauses HOLD/normal exit, cancels active exit version, preserves sticky timing, and makes primary metrics invalid.
- Full exit records actual pair PnL/fees, simulated closed net, applied-rate closed net or UNKNOWN, exit wait/funding/pair change evidence, then FLAT.
- Restart contracts for FLAT, ENTRY_MAKER_OPEN, HOLDING, EXITING_NORMAL, and EXITING_AGGRESSIVE exactly follow `SYSTEM_SPEC.md` without fill reconstruction.

## Acceptance tests

- Funding replacement, partial applied set, deterministic skipped, lifecycle-recognized funding, and funding during EXITING.
- Normal→aggressive exactly 10 seconds, sticky aggressive, and no taker timeout.
- Exit maker cancellation without RISEx depth and recreation after recovery.
- Hard Basis in both route directions and unavailable unwind quote.
- Funding-inclusive simulated closed PnL and applied-rate UNKNOWN/completeness.
- Degraded open-position data gap, overlap flags, preserved exit timer/mode, and no reconstructed fills.
- Restart from every state with entry cancellation, open-position gap/reconcile, and sticky exit recovery.

## Constraints

- Work on `codex/paper-005` from accepted `main`; no subagents or product-rule changes.
- Use deterministic in-memory evidence only; persistence/reporting arrive in PAPER-006.
- Do not add new states, timed taker fallback, partial positions, route switching, or live behavior.
- Run focused tests and full `pytest`, review the diff, commit, then report in at most 20 lines.
