# PAPER-007-FIX-008 — Public Evidence, Extended Catalog/Heartbeat, Full Digest, and Stop Semantics

Start from the current accepted `main` on `codex/paper-007-fix-008`. Preserve all experiment databases and implement only this bounded correction.

## Required scope

- RISEx public trade/book unit evidence accepts positive grid-aligned below-minimum observations; planned paper orders still enforce minimum quantity. Preserve all other parity blockers and add exact bounded unit diagnostics.
- Split Extended full-universe background catalog (600-second cadence, 1200-second TTL, 60-second dedicated timeout) from repeated-`market` required-market refresh (every normal refresh, 300-second TTL). Validate complete responses atomically, preserve fresh last-good state, and expose exact catalog/metadata blockers separately from book health.
- Give each Extended book/trade/funding socket one 10-second client heartbeat; handle server PING/client PONG and client PING/server PONG; confirm only the matching socket and cancel/await heartbeat on every exit.
- Persist physical socket lifecycle only for observed transport events. Persist confirmation-stale/watchdog restart separately. Preserve isolated streams and separate book-resync lifecycle.
- Persist and deliver all 20 ordered authoritative FULL routes. UNKNOWN includes a short exact blocker in the existing third field. Split deterministically into <=4096-character messages without route loss/duplication; Telegram performs no economics.
- Persist bounded SIGINT/SIGTERM/STOP_EVENT/RUNTIME_FATAL/UNKNOWN_EXTERNAL_STOP cause and request time. Runtime has no elapsed-time stop; safe shutdown awaits owned tasks without false lifecycle evidence.

## Verification

Add deterministic failing-before/passing-after coverage for every boundary in the user-authorized FIX-008 brief, including TTL edges, atomic cache replacement, non-blocking cadence, heartbeat/watchdog/physical lifecycle cardinality, all-20 digest splitting, stop causes, and unchanged funding/PnL/minimum-order invariants. Run focused tests, full pytest, compileall, diff-check, and secret scan. One Builder, no subagents, one implementation commit, at most two same-branch fix cycles. No real orders, private/authenticated endpoints, live trading, economics changes, timeout wrappers, or Stage B launch.
