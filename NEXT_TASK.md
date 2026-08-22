# PAPER-007-STABILIZATION-002 — Venue-Complete Recovery and Persisted Lifecycle Causality

Status: proposed and held for Chief Reviewer plan review. No Builder may start until the Chief Reviewer accepts this bounded contract. PAPER-007-STABILIZATION-001 is `BLOCKED — TASK DID NOT CONVERGE`; rejected commits `e6c0bcc2415bb2b4b7b9b3d0026fd435b9db29c4` and `fc47b8a111503dc91d1347929ba32b7038f03d99` must not be merged, cherry-picked wholesale, or called accepted.

This is one strictly corrective slice under the existing PAPER-007 stabilization authorization. It adds no product behavior, economics, formula, cadence, Telegram behavior, private endpoint, live capability, service, compatibility layer, flag, cache, or parallel state owner. Stage B and Telegram remain stopped.

## Reconstruction boundary

Builder starts from current local `main`, writes production-shaped RED tests against that baseline and, where the defect exists only in the rejected candidate, proves RED on a disposable detached checkout of `fc47b8a`. Builder reconstructs only independently proven useful code; rejected commits are evidence, not an implementation base.

- Replace the five parallel recovery episode maps with one existing-owner `RecoveryEpisode` per `(venue, market)`, using distinct non-null `StreamSessionId`, `RecoveryEpisodeId`, and attempt generation types. A minimal displaced-task registry is allowed only for cancellation/await ownership and must clean itself and be empty after shutdown.
- Make terminal `FAILED` inert but live for Extended, Nado, and RISEx. Each venue's next genuine physical startup/reconnect creates exactly one fresh episode and START under its existing venue-specific recovery semantics. Nado remains REST snapshot plus bounded replay; RISEx remains unsubscribe/resubscribe WebSocket snapshot; Extended remains its owned dedicated WebSocket snapshot.
- In combined RISEx/Nado startup, treat `apply_book_event=False` as fail-closed: do not mark book, trade, funding, or combined connection data ready from the rejected snapshot. No frame, confirmation, readiness, book, evidence, or terminal mutation may cross a lost physical `StreamSessionId` boundary.
- Under the existing position serialization frontier, periodic evaluation, relevant-book hard-basis evaluation, and disconnect gap opening must evaluate a detached `LifecycleEngine` candidate, persist the exact candidate checkpoint, then publish memory synchronously. Repository failure leaves lifecycle memory, persisted snapshot, notifications, and scheduler-visible state consistent with their pre-operation values. Do not hold the lock across adapter/network I/O, sleep, or Telegram.
- Adversarially audit queued RISEx/Nado combined trade and funding work. If cancellation does not make obsolete work inert, carry the existing typed combined `StreamSessionId` to the current commit boundary and revalidate there; do not add another coordinator or owner. Obsolete queued work emits no post-replacement receipt evidence.
- Reconstruct only the previously proven Extended session checks, detached recovery publication, stale exit-version rejection, settlement atomicity, causal commit time, and bounded shutdown behavior that are necessary for this contract. Remove redundant wrappers or branches; report production LOC and recovery state-owner count against `main`.

## Mandatory evidence

- RED on `fc47b8a`: real Nado three-attempt REST failure and real RISEx overflow terminalize `FAILED`; a new combined physical session currently creates no fresh episode/terminal, publishes no book, and falsely marks readiness.
- RED on `fc47b8a`: injected SQLite failure after periodic evaluate, relevant-book hard-basis evaluate, and disconnect gap opening currently leaves live lifecycle changed while persistence remains old.
- GREEN must prove both venue recovery cases produce ordered START/FAILED/START/COMPLETED with distinct episode/generation/session identity and no premature readiness.
- GREEN must prove repository failure and cancellation leave lifecycle memory, SQLite, notifications, readiness, scheduling, fills, settlements, and evidence consistent at every affected commit boundary.
- Re-run the exact external Extended untagged/startup reproduction, all prior stabilization R1–R16 preservation cases, FIX-003/006/007/008/009/010 tests, repository races, full `pytest`, compileall, diff check, and tracked-secret scan.
- R16 uses only a disposable copy of `/Users/daniilmakarov/Desktop/risex-paper007-archives/paper-007-stage-b-fix003-accepted.pre-fix010-operational.db`. The original must remain untouched with SHA-256 `93e9b6793e76cec227d0fe40799a70d0416518568b0c228fc0808a681497df80`; the distinct root DB must not be opened or mutated.

Exactly one Builder may work after Chief Reviewer approval, on a new `codex/paper-007-stabilization-002` branch from local `main`, without spawning agents. Maximum two fix cycles. No merge, push, public endpoints, operational run, Stage B, Telegram, or global Scanner/UNKNOWN/PnL implementation before deterministic acceptance and independent Chief Review.
