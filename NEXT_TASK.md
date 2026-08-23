# PAPER-007-STABILIZATION-005 — Public Data Instability Causal Diagnosis

Status: `PHASE 0 ONLY — IMPLEMENTATION NOT AUTHORIZED`.

STABILIZATION-004 is accepted at exact commits `59c87bd26496ea15c75104b37a8f05ac6d20f0a1`, `63413b046233b185cbc60f03e19649218632d527`, and `c18161675b104627a8364deff9f3efbae0f403dc`. Continue as one bounded administrative corrective slice only to determine whether the accepted operational run proves a local public-data stability defect. Do not create a Builder, edit production code, or start RED/GREEN until the Architect presents Phase 0 evidence and the Chief Reviewer explicitly authorizes implementation. If the evidence supports only venue, network, host, sleep/wake, or otherwise external behavior, stop and report without inventing a correction.

## Evidence boundary

- The accepted 24-minute operational run produced 45 new Extended physical `SOCKET_CLOSED/EOF` disconnects: book 11, funding 17, trade 17. Every exact `episode_id` eventually had one persisted reconnect; unmatched episodes at safe stop were zero.
- The same run produced 30 new public `TimeoutError` rows: Extended 14, Nado 14, and RISEx 2. Scanner persisted one INITIAL and eight FULL scans, 206 evaluations, zero eligible routes, four `VOLUME_UNKNOWN`, and transient book/trade/funding unhealthy blockers. Final venue readiness was available for all venues.
- These are observations, not a diagnosis. Do not assume a client reconnect storm, heartbeat error, timeout ownership defect, server policy, official API behavior, Mac sleep, local network failure, or event-loop stall.
- Use only this repository, its accepted Git history, the preserved accepted operational evidence, locally available host/network/sleep signals, and official RISEx, Extended, or Nado contracts. Do not inspect or reuse the quarantined refactor branch/stash, other repositories, Radar, or old projects. Any validation must prove imports resolve to the intended clean worktree rather than the global editable install.

## Phase 0 ownership map and required evidence

- Physical socket owner: runtime public-stream connection loop. Establish exact connection-open, confirmation, heartbeat, disconnect, backoff, reconnect, session, and episode timelines without adding another owner.
- Public request owner: runtime refresh/single-flight request orchestration; adapters own venue request semantics. Establish endpoint/component, attempt, start/end, timeout duration, retry/backoff, concurrent request count, refresh ownership, and terminal outcome for every timeout.
- Cadence owner: runtime. Correlate refreshes, scans, deadline lateness, socket episodes, recovery work, and any event-loop scheduling stalls without changing absolute cadence.
- Scanner remains the sole blocker/`UNKNOWN` owner. Report every per-scan blocker distribution exactly; genuine unavailable official data must remain fail-closed and may remain `UNKNOWN`.
- Host/environment evidence is diagnostic only. Correlate locally available sleep/wake, process signals, network transitions, and wall-clock/monotonic discontinuities with explicit confidence and gaps; absence of evidence is not proof of absence.
- Compare captured behavior only with current official venue public contracts. Distinguish documented server close/heartbeat/reconnect behavior from a client/protocol mismatch; cite exact official evidence and do not infer undocumented requirements.

## Falsifiable hypotheses

1. The client creates a reconnect storm through duplicate connection owners, premature heartbeat closure, stale-session interference, or incorrect reconnect pairing. Falsifier: one owner per stream/session, server/transport EOF precedes each retry, official heartbeat deadlines are met, and no overlapping client-created reconnect exists.
2. Refresh orchestration causes request timeouts or event-loop starvation through unbounded concurrency, serial coupling, duplicate single-flight ownership, or work that blocks socket heartbeats/scans. Falsifier: bounded independent tasks, expected timeout duration, responsive event loop, and no causal alignment beyond external latency.
3. Retry/backoff implementation contradicts the official venue contract or collapses into burst retries. Falsifier: persisted attempt spacing and connection lifetimes match the accepted code and official limits, with no client-side burst.
4. Host sleep/wake or network transitions caused the observed clusters. Falsifier: continuous host/network evidence across each episode with no matching discontinuity; confirmation requires positive local evidence, not the user's hypothesis alone.
5. The observations are expected external/server/network behavior correctly isolated by existing fail-closed recovery. Confirmation requires exact episode pairing, bounded retries, preserved cadence, no duplicate state owner, and recovery without invented freshness.

## Mandatory Phase 0 output and proposed RED boundary

- Reconstruct every new disconnect/reconnect episode in chronological order, including venue, stream, market, session, lifetime, cause/classification, heartbeat or PING/PONG evidence, retry delay, reconnect time, and unmatched/overlapping ownership checks.
- Reconstruct every new request timeout with endpoint/component, attempt, duration, concurrent refresh/socket context, configured timeout/backoff, later recovery, and correlation to scanner deadline or event-loop delay.
- Inspect the accepted reconnect, heartbeat, refresh, timeout, safe-stop, and Scanner tests and production owners for a specific contract mismatch. Record exact production files that would be unavoidable; prefer zero files when no defect is proven.
- Present a concise causal timeline, ownership map, evidence gaps, hypothesis verdicts, and exact production-shaped RED tests that fail on accepted main only if a local defect is established. A RED plan must identify the current incorrect behavior and invariant, not merely assert fewer disconnects or zero `UNKNOWN`.
- Any later correction must preserve economics, Decimal formulas, routes, thresholds, Scanner semantics, absolute cadence, Telegram boundaries, public/paper-only safety, and single ownership. Prefer removing duplicated state over adding a cache, flag, task, or service.
- Mandatory later operational outcome, only if Chief authorizes GREEN: a bounded awake-machine public soak with exact episode pairing, no client-caused reconnect storm, timeout isolation, safe stop/quiescence, and complete per-scan blocker/`UNKNOWN` distribution. Genuine official-data unavailability remains visible.

## Stop gates

- Stop after Phase 0 and request Chief review before creating the single allowed Builder.
- Stop without implementation if causality is external, environmental, undocumented, ambiguous, or not reproducible as a local invariant violation.
- Do not restart long-running Stage B or Telegram, mutate archives, commit databases/logs/secrets, merge speculative work, or begin another stabilization slice.
