# PAPER-007-FIX-011 — Recovery Generation and Lifecycle Causality

Status: explicitly user-authorized and active. This is the only implementation task. PAPER-007 Stage B and Telegram remain stopped.

Correct the bounded recovery-generation and open-position lifecycle causality/atomicity defects proven by the post-FIX-010 operational challenge. Do not change strategy economics, thresholds, cadence, public API semantics, funding arithmetic, trade-through rules, or paper/live boundaries.

## Required implementation contract

- Model each market recovery as one explicit bounded episode with generation, phase, current buffer, task identity, and attempt/overflow counters. `START` occurs once per episode; `OVERFLOW` occurs at most once per generation; exactly one accepted `COMPLETE` or terminal `FAIL` occurs per episode. Obsolete generations cannot modify current buffers, book, health, readiness, tasks, or evidence.
- Nado REST recovery atomically activates a fresh generation buffer before its snapshot request can observe deltas. Every current-socket delta received after that boundary is buffered. Snapshot-covered deltas are skipped and all newer deltas replay continuously without yielding. Any buffered discontinuity rejects completion and triggers bounded fail-closed recovery. Keep the 2048 cap and existing bounded attempt budget; no tight loop, task leak, unbounded memory, or raw per-delta evidence.
- Preserve distinct RISEx unsubscribe/resubscribe WebSocket snapshot semantics, including discard of pre-boundary old-subscription deltas and no REST/false physical-socket evidence. Preserve distinct Extended dedicated reconnect/WebSocket snapshot semantics.
- Use one runtime serialization/commit frontier for tick, recovery, gap/disconnect, relevant book evaluation, exit trade, funding settlement/reconciliation, restart, and shutdown persistence. Never hold it across network I/O, sleeps, Telegram, or adapter calls. Re-read lifecycle identity/version/observations inside it, reject stale work, and derive causal processing time at commit.
- Causal recorded time must be no earlier than commit-boundary clock, persisted `runtime_state.updated_at`, latest event/sample, all gap timestamps, and relevant exit-version timestamps. Keep scheduled, exchange, and observation timestamps as separate unchanged evidence.
- Repository runtime saves reject backward `recorded_at` before any transaction write. Events, samples, gaps, and versions remain chronological; closed versions satisfy `created_at <= last_checked_at <= closed_at`.
- Lifecycle transitions validate/build a candidate before publication. Exception or cancellation before success leaves the snapshot exactly unchanged, covering gap start/end/recover, close, aggressive/reprice/replace/cancel, settlement reconciliation, and trade close.
- Exit trade processing re-reads current lifecycle/version under the runtime frontier. A stale v1 trade after v2 cannot fill, consume the lifecycle key for the current version, notify, or move checkpoint time backward. Separate append-only trade evidence may remain idempotent and causally recorded.
- For an active-position settlement, normalized settlement row and lifecycle checkpoint commit atomically in one SQLite transaction (or an equivalently strong tested protocol). Cancellation before commit and repository failure leave DB and in-memory lifecycle unchanged; success advances both exactly once. Exact duplicates stay idempotent, conflicts fail closed, and flat/no-position behavior remains explicit.

## Mandatory deterministic acceptance

Add production-shaped regressions first and prove each fails on code baseline `53704869047e32b0357eaf4d3d32955fa0cf8b65` and passes on the candidate:

- R1 Nado overflow, new gated snapshot N, then N+1/N+2: final N+2 with levels applied and one terminal completion.
- R2 N+1/N+3 discontinuity: never COMPLETE; unhealthy with bounded retry/failure.
- R3 obsolete generation snapshot completion cannot overwrite any current state/readiness/evidence.
- R4 10,000 realistically scheduled deltas: cap respected, zero raw delta evidence, bounded tasks/evidence, no false COMPLETE or tight loop.
- R5 RISEx resubscribe boundary retains pre-boundary discard and creates no REST/false socket rows.
- R6 Extended dedicated reconnect/snapshot generation remains correct.
- R7 gated tick t1 plus GAP_STARTED t2: runtime time >= t2, chronology valid, at most one scheduled sample for the slot.
- R8 gated recovery t1 plus later settlement/lifecycle t2: causally current or rejected stale, with no regression.
- R9 every direct backward-time engine failure leaves the complete snapshot unchanged.
- R10 stale exit v1 trade after v2: no fill/key consumption/time regression/duplicate notification.
- R11 settlement cancellation while awaiting serialization leaves DB and lifecycle unchanged.
- R12 repository failure after settlement candidate leaves DB and in-memory lifecycle unchanged.
- R13 settlement success is atomic and exactly once; restart preserves identical state/cash.
- R14 repeated concurrent tick/recovery/trade/settlement stress preserves all invariants without duplicates or exceptions.
- R15 production `run()` safe-stop under gated lifecycle I/O and high-rate unrelated frames completes within 2 seconds, ends `STOPPED_SAFE`, performs no post-stop writes, and preserves the position.
- R16 a disposable copy of the preserved open-position DB loads/restarts without invented funding/fill/close; the original hash remains unchanged.

Retain all existing recovery, FIX-003 socket evidence, FIX-006 RISEx checksum, FIX-007 funding, FIX-008 timeout/heartbeat, FIX-009 startup, and FIX-010 persistence/safe-stop tests. Use tmp/disposable databases only; never mutate the authoritative production DB or preserved evidence. Telegram stays disabled. No private/authenticated endpoints, API keys, real orders, live trading, or LLM runtime calls.

Builder must work only on `codex/paper-007-fix-011`, must not spawn agents, and must produce one implementation commit after focused tests, full `pytest`, compileall, diff review, and tracked-secret scan. Stop after this milestone; no further milestone is authorized.
