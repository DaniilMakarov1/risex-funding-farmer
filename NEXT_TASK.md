# Active bounded task

## SS-001F — Terminal Serialization and Protocol-Failure Evidence

Status: `AUTHORIZED / BUILDER NOT YET OPENED`.

Objective: correct the proven DG-004 evidence-terminal race and preserve bounded sanitized public-protocol failure classification before any replacement economic sample.

Exact base: the accepted published `main` after this governance record. Create exactly one fresh visible Spread Builder and worktree from that exact base.

Allowed: serialize all store writes and terminal emission against in-flight thread-backed appends; guarantee one physically-last terminal with unique contiguous increasing indices; make offline reporting reject index/terminal corruption; preserve bounded sanitized venue/frame-kind/category/length-or-hash protocol-failure evidence before terminal even when ingress is full or closing; add focused/adverse regressions.

Acceptance: a deterministic test reproduces cancellation while a thread-backed append is still running; full/closing-ingress protocol failures remain durably ordered before the terminal; corrupt DG-004-style replay is explicitly rejected; clean fixtures retain one last terminal and ordered indices; immutable DG-002B/DG-003/DG-004 reports remain deterministic; focused/adverse tests and one clean Python 3.11 full suite pass; dependency, compile, import, private/write-surface, diff, scope, and Git checks are clean.

Forbidden: changing store representation/caps, queue size, shutdown timeout, fill semantics, eligibility, economics, fees, quote grid, horizons, stop rules, public protocol acceptance, retry behavior, venue, strategy, private/auth/credential/signing/write/testnet/mainnet surface, `SS-002`, or `SS-003`.

After candidate delivery, Chief independently reviews and alone accepts/integrates. Freeze no replacement economic gate until the correction is accepted and the unsupported Lighter frame class is resolved from official or sanitized observed public evidence. `SS-002` and `SS-003` remain closed.
