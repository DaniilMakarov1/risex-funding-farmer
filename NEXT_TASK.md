# PAPER-007-STABILIZATION-001 — Single-Owner Stream and Lifecycle Causality

Status: user-authorized and active. This is the only implementation slice. It is a corrective audit label under the existing PAPER-007 paper scope, not new product functionality. Stage B and Telegram remain stopped.

Reconstruct the smallest independently reviewed correction from accepted baseline `53704869047e32b0357eaf4d3d32955fa0cf8b65`. Do not merge or cherry-pick rejected PAPER-007-FIX-011 commits `fd0a014`, `86566e0`, or `802fec2`. Remove or consolidate duplicate ownership before adding state. Do not change economics, thresholds, cadence, formulas, official API semantics, route selection, trade-through rules, Telegram behavior, or paper/live boundaries.

## Required implementation

- Give every Extended physical book reader/session a captured, explicit, non-null monotonically unique `StreamSessionId` from startup through every reconnect. Every snapshot/delta and every mutation of book health, readiness, recovery, or evidence must prove that reader still owns the current session. `StreamSessionId` must be semantically distinct from `RecoveryEpisodeId` and recovery attempt generation; optional identity or fallback paths are prohibited.
- Give each `(venue, market)` recovery exactly one owner containing its `RecoveryEpisodeId`, current attempt generation, phase, buffer, attempt/overflow counters, terminal result, and task ownership. This object replaces the existing parallel `_recovery_buffers`, `_recovery_overflowed`, `_recovery_attempts`, `_recovery_generations`, and `_recovery_tasks` episode state rather than coordinating them as a sixth owner. Keep only a minimal separate task registry if shutdown genuinely requires it, with explicit ownership and cleanup tests. For Extended, the episode explicitly owns one exact `StreamSessionId`; `None`, older, newer-unowned, startup-late, and obsolete-reconnect frames are inert.
- A recovery restart task only transfers/establishes reader ownership. It cannot complete snapshot recovery. Exactly one START and one terminal COMPLETE or FAIL are recorded per episode; obsolete work cannot change book/readiness/evidence. Two sequential gap/reconnect cycles use distinct episode and session identities.
- Preserve venue-specific behavior unchanged: Nado activates and replays its bounded REST snapshot buffer; RISEx waits for official unsubscribe/resubscribe WebSocket snapshots; Extended waits for its owned dedicated WebSocket snapshot.
- Cancel and await current and displaced stream/recovery owners on replacement and shutdown. After the stop boundary, no task may write readiness, book, lifecycle, or evidence.
- Preserve only the lifecycle corrections independently proven necessary: all-or-nothing lifecycle transitions, causal non-regressing commit time, stale exit-version rejection before key consumption, and atomic active-position settlement plus lifecycle checkpoint persistence. One runtime serialization frontier orders lifecycle commits; no network I/O, sleep, Telegram, or adapter call occurs while held.

## Mandatory evidence

Builder writes production-shaped RED tests first and demonstrates the exact Extended late untagged/startup reader failure on rejected `802fec2` or accepted baseline as appropriate, then PASS on the new branch.

Acceptance requires R1–R15 from the rejected FIX-011 contract, including the exact external Extended reproduction, plus:

- every physical Extended session has non-null identity and only the current/episode-owned identity can mutate state;
- two sequential real-shaped Extended recovery cycles have distinct sessions/episodes and one START plus one terminal COMPLETE each;
- restart-task completion is not recovery completion;
- shutdown completes within two seconds with no post-stop writes;
- Nado and RISEx recovery boundaries are unchanged;
- all existing FIX-003, FIX-006, FIX-007, FIX-008, FIX-009, FIX-010, recovery, repository failure/cancellation, full-suite, compileall, diff, and tracked-secret checks pass;
- Builder and Architect diff reviews list every duplicate recovery structure deleted or collapsed, justify any remaining task registry, and report whether production `runtime.py` recovery code/state count decreased. A net-new flag, coordinator, or optional fallback is rejection.
- R16 runs only on a disposable copy of `/Users/daniilmakarov/Desktop/risex-paper007-archives/paper-007-stage-b-fix003-accepted.pre-fix010-operational.db`; the original archive must remain untouched and its verified SHA-256 `93e9b6793e76cec227d0fe40799a70d0416518568b0c228fc0808a681497df80` unchanged. The different root DB with SHA-256 `60b6c82a...` must not be opened, mutated, or treated as this archive.

The Builder starts from current local `main` on `codex/paper-007-stabilization-001`, reports root/branch/HEAD/status before edits, must not spawn agents, and produces one bounded implementation commit. At most two Architect-requested fix cycles are allowed. Telegram stays disabled; use tmp/disposable databases only; no private/authenticated endpoints, keys, real orders, or live trading.

Stop after this slice. The global Scanner/UNKNOWN/PnL ownership and blocker matrix is a separate strictly corrective slice only after this implementation is accepted; do not mix it into this branch.
