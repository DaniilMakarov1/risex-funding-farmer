# Active bounded task

## SS-001G — Lossless Book-Delta Evidence

Status: `AUTHORIZED / BUILDER NOT YET OPENED`.

Objective: eliminate the proven repeated-full-book evidence bottleneck with the smallest lossless revision-chain representation that preserves deterministic audit and all accepted research semantics.

Exact base: the accepted published `main` after this governance record. Create exactly one fresh visible Spread Builder and worktree from that base.

Allowed: one full normalized BOOK snapshot at the start of each venue/market/session/recovery chain; subsequent exact normalized level deltas with predecessor/revision identity; deterministic bounded-memory reconstruction/audit; exact RISEx/Lighter revision references on QUOTE and existing exact Lighter reference on HEDGE_HORIZON; realistic deep-book serialization tests.

Acceptance: full books reconstruct exactly and deterministically across snapshots, deltas, gaps, session/recovery changes, and restart; missing/ambiguous/out-of-order predecessors fail closed; quote/horizon revision references resolve; a three-market `1,900`-level small-delta workload above DG-005 rate uses actual owner-only JSON serialization with zero overflow/loss/reordering, bounded memory, clean terminal, and material byte headroom under unchanged caps; legacy full-BOOK reports remain deterministic; focused/adverse and full Python 3.11 suites pass.

Forbidden: database/compression/message-bus/framework work; queue/cap/timeout increase; raw-payload archive; economic, fee, quote, fill, eligibility, stop, horizon, protocol, venue, strategy, private/auth/credential/signing/write/testnet/mainnet, `SS-002`, or `SS-003` change.

After candidate delivery, Chief independently reviews and alone accepts/integrates. Freeze no replacement discovery gate until acceptance. `SS-002` and `SS-003` remain closed.
