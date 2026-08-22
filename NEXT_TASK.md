# PAPER-007-STABILIZATION-003 — Authoritative Refresh→Snapshot→FULL Scan Ordering and Global UNKNOWN Matrix

Status: `DRAFT — AWAITING CHIEF REVIEW`. This is the only proposed next strictly corrective slice under the existing PAPER-007 stabilization authorization. No Builder or implementation is active. PAPER-007-STABILIZATION-002 is scoped-accepted only for session/recovery/lifecycle causality, persistence atomicity, recovery ownership/evidence, shutdown, and public/paper safety at unchanged commit `ab9c4e9b04438d59c7897b8ed4b2972b2b811d5f`. Its overall cadence is inconclusive and whole-system Scanner/UNKNOWN/PnL readiness is not accepted.

This slice adds no product functionality. Stage B and Telegram remain stopped. No runtime restart, public operational run, or Telegram run is authorized before deterministic implementation acceptance and independent Chief Review.

## Product and ownership invariants

- Scanner remains the sole owner of planned PnL evaluation and exact no-trade reasons. Telegram only relays the persisted authoritative Scanner value/reason and performs no calculation.
- Runtime and public venue adapters remain the sole owners of observations, freshness inputs, refresh cadence, and lifecycle. SQLite remains persistence, not a scan read model or decision owner; acceptance monitoring must not continuously poll SQLite.
- Do not increase TTLs or change PnL, economics, fees, thresholds, formulas, eligibility, parity, route selection, trade-through, cadence, paper/live boundaries, or official-source semantics.
- Add no service, framework, cache, read model, alternate coordinator, parallel owner, scheduler, endpoint, compatibility layer, or new product state. Preserve the existing single-flight refresh owner and remove/simplify duplication before considering any new branch or flag.
- Focused settlement cadence must remain independent of catalog REST refresh and must not be delayed by FULL refresh ordering.
- A FULL digest evaluates exactly one immutable authoritative observation snapshot. Its `logical_at` is the actual capture instant after every refresh commit included in that snapshot, and no included observation may be later than `logical_at`.
- When a FULL slot and public refresh are due together, the owned refresh must be scheduled and committed before the FULL snapshot, or FULL must boundedly await the already-owned refresh task. FULL must not knowingly evaluate a just-expired pre-refresh snapshot and then start the due refresh immediately afterward.
- Refresh remains single-flight. Concurrent FULL/focused work must not duplicate or burst public REST calls.
- Refresh failure, timeout, or an official venue continuing to advertise an elapsed/old funding cycle preserves the exact authoritative UNKNOWN. Never invent a next settlement, funding cash, eligibility, or numeric PnL.
- After settlement, when the due refresh has authoritative next-cycle quotes for RISEx, Extended, and Nado, the first post-refresh FULL must use them and be numeric subject only to other honest blockers, not `TARGET_CYCLE_ELAPSED` from the superseded snapshot.
- Operational cadence acceptance uses PID-only continuous supervision plus bounded, measured, short-lived read-only checkpoints. No reader transaction may span sleeps or scan deadlines.

## Mandatory RED and proof matrix

### A. Same-slot settlement rollover

Construct a production-shaped clock sequence with an old 19:00 snapshot, Nado already advertising 20:00, and the due RISEx/Extended refresh returning authoritative 20:00 quotes seconds after the FULL slot. Prove RED on accepted `ab9c4e9b`: old ordering emits `TARGET_CYCLE_ELAPSED`. Corrected FULL must run only after the owned refresh commit and use the 20:00 snapshot.

### B. Funding TTL edge

At the frozen 120-second funding TTL and 120-second FULL cadence, make a successful due refresh available while the prior quote reaches approximately 123 seconds. Corrected FULL must consume the committed fresh quote, not produce avoidable `FUNDING_STALE`. TTL and cadence remain unchanged.

### C. In-flight refresh atomicity

Gate individual venue refresh completions and FULL snapshot capture. FULL must observe all-old or all-new authoritative inputs, never mixed venue generations and never an observation later than snapshot `logical_at`.

### D. Refresh failure and recovery

For timeout and official failure, last-good data under TTL remains valid; last-good data over TTL yields exact `FUNDING_STALE`. The scan scheduler remains live, no future value is invented, and the next successful owned refresh restores numeric evaluation when every other input is valid.

### E. Focused cadence independence

Gate a slow catalog/metadata REST refresh while focused scans cross their settlement deadlines. Focused scans remain on the frozen schedule and do not await REST; only the affected FULL waits or defers under the bounded owned-refresh rule.

### F. Single-flight and call counts

Prove overlapping FULL, seed, catalog, and ordinary refresh triggers share the existing owner. Official endpoint call counts remain bounded with no duplicate request burst or second refresh coordinator.

### G. Global authoritative blocker matrix

Cover RISEx×Extended and RISEx×Nado for every authoritative symbol and both directions. For each route component prove its source, owner, timestamp/freshness semantics, and exact Scanner result:

- metadata and parity;
- BBO, exact depth, book health, and trade freshness;
- funding quote freshness, settlement cycle, eligibility, predicted/estimated/applied semantics;
- execution grids, quantity/minimums, fees, and exact execution inputs;
- planned maker net PnL when all mandatory inputs are valid.

Catalog, funding, trade, metadata, parity, and book failures must not masquerade as another component's blocker. UNKNOWN must contain the shortest exact authoritative reason. Numeric PnL appears whenever all required inputs are valid, fresh, parity-proven, and executable. No Scanner formula or economics change is permitted.

### H. Telegram preservation

Tracked presentation-only tests prove the FULL digest relays the exact persisted Scanner number or UNKNOWN reason. Telegram performs no freshness, blocker, funding, fee, or PnL calculation and gains no new behavior.

## Implementation and acceptance boundary

- Before production edits, reconstruct each RED test independently against current `main`; rejected branches may be read only as evidence and must not be cherry-picked or copied wholesale.
- Exactly one Builder may work only after Chief activation, on `codex/paper-007-stabilization-003`, without spawning agents. Maximum two fix cycles.
- Keep the production diff bounded to existing runtime/Scanner/adapter/repository/Telegram preservation paths strictly required by the matrix. Every changed production hunk must map to one invariant above; remove unrelated or future-phase code.
- Preserve the direct R1-R16 tests, the accepted Phase A/B and recovery/session/lifecycle evidence, the 263-test accepted suite, repository failure/cancellation races, compileall, diff review, non-null identity/owner/provenance audit, and secret scan.
- Deterministic acceptance requires exact RED-on-`ab9c4e9b`/GREEN-on-candidate mapping for A-H, focused tests, and full pytest with no warnings, pending tasks, skipped causality checks, or weakened preservation assertions.
- Operational acceptance requires one clean 60-90 minute public-only paper run spanning a settlement boundary, Telegram disabled, no continuous SQLite reader, and no root/archive DB mutation. Audit FULL/focused cadence, refresh start/commit/snapshot ordering, endpoint call counts, numeric/UNKNOWN streaks, funding ages/cycles, future observations, recovery/socket evidence, paper lifecycle, safe stop, integrity, and post-stop writes.
- Telegram and authoritative Stage B remain stopped until deterministic and operational acceptance plus independent Chief Review. Only then may a separate decision consider restart.

No Builder may begin from this draft. Architect must first receive Chief approval or correction of this exact bounded contract.
