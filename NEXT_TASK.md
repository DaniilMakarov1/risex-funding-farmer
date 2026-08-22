# PAPER-007-STABILIZATION-003 — Authoritative Refresh→Snapshot→FULL Scan Ordering and Global UNKNOWN Matrix

Status: `DRAFT — AWAITING CHIEF REVIEW`. This is the only proposed next strictly corrective slice under the existing PAPER-007 stabilization authorization. No Builder or implementation is active. PAPER-007-STABILIZATION-002 is scoped-accepted only for session/recovery/lifecycle causality, persistence atomicity, recovery ownership/evidence, shutdown, and public/paper safety at unchanged commit `ab9c4e9b04438d59c7897b8ed4b2972b2b811d5f`. Its overall cadence is inconclusive and whole-system Scanner/UNKNOWN/PnL readiness is not accepted.

This slice adds no product functionality. Stage B and Telegram remain stopped. No runtime restart, public operational run, or Telegram run is authorized before deterministic implementation acceptance and independent Chief Review.

## Frozen product and ownership invariants

- Scanner remains the sole owner of `planned_maker_net_pnl_usd`, the complete authoritative blocker tuple, and entry eligibility. Telegram only relays the persisted authoritative Scanner value or selected display blocker and performs no calculation.
- Runtime and public venue adapters remain the sole owners of observations, freshness inputs, refresh cadence, and lifecycle. Repository/SQLite remains persistence, not a scan read model, event source, or decision owner.
- Do not increase TTLs or change PnL, economics, fees, thresholds, formulas, eligibility, parity, route selection, trade-through, cadence, paper/live boundaries, official endpoints, or official-source semantics.
- Add no service, framework, cache, read model, alternate coordinator, parallel owner, scheduler, endpoint, compatibility layer, or product state. Preserve existing runtime ownership and single-flight behavior; remove or separate existing work only where the frozen deadline boundary requires it.

## SYSTEM_SPEC §6 catalog and deadline boundary

- Extended full-universe catalog refresh is always background work. Its existing 600-second cadence, 60-second timeout, and 1200-second last-good TTL are unchanged. It never delays FULL, FOCUSED, health, or lifecycle deadlines.
- Required-market metadata is catalog work for the §6 deadline boundary. Its existing filtered official `market` queries, 30-second public timeout, atomic validated replacement, and 300-second per-market TTL are unchanged. A due FULL or FOCUSED scan must not await `_refresh_extended_required`, `fetch_required_catalog`, full-universe catalog, `fetch_markets`, or `fetch_volumes`.
- Fresh last-good catalog/metadata within its frozen TTL remains usable. Missing or expired evidence produces its existing exact `CATALOG_UNAVAILABLE`, `CATALOG_STALE`, or `MARKET_METADATA_STALE` blocker. It must not masquerade as book or funding failure.
- The only deadline-critical public completion that a due FULL may defer for is the existing `_market_observation` work for already selected candidate markets: venue `fetch_funding_quote`, plus only the existing `fetch_book` and RISEx `prime_recent_trade_evidence` fallback when that same required observation lacks usable stream data. No catalog/universe/volume discovery call is part of the pending FULL completion condition.
- If the current `_refresh_public_data` single flight combines catalog/metadata and market-observation work, minimally separate its deadline-critical observation completion from background catalog completion inside the existing Runtime owner. Do not add a service, cache, coordinator, or second scheduler owner; background catalog work must not become a prerequisite for the pending FULL.

## Non-blocking scheduler and FULL-slot ownership

- The main tick must never await slow public network refresh in a way that prevents health checks, position monitoring, T−120/T−5 activation, or 10-second FOCUSED scans.
- When a FULL absolute slot is due, Runtime starts or joins the one owned deadline-critical observation refresh and defers that FULL until the refresh terminally succeeds or fails. Tick continues servicing FOCUSED, lifecycle, and health work while FULL is pending; those deadlines take precedence.
- If unavoidable, one minimal internal pending FULL-slot/task-consumption marker is permitted inside the existing Runtime scheduler. It is not product state or a new scheduler owner. It must contain one auditable absolute slot identity, be uniquely Runtime-owned and bounded, clear after the resulting scan, terminal refresh failure handling, cancellation, or stop, restore to none on restart, and create neither duplicate FULL nor duplicate refresh.
- Missed FULL slots follow the existing absolute-slot coalescing policy and are never replayed in a burst. The persisted FULL evidence retains the original scheduled slot even when execution is deferred.
- Unexpected internal/programmer errors remain governed by the existing fatal safety contract. They must not be converted into ordinary venue failure, stale cache, or UNKNOWN.

## Single immutable FULL snapshot boundary

- FULL must not capture observations while its owned due refresh is partially in progress. After terminal completion, materialize one immutable tuple from the finished Runtime state: new observations for successful components and the existing honest last-good or unavailable state for failed components under frozen TTL/readiness rules. Legitimate component-level partial success is allowed and is not cross-venue corruption.
- Capture `logical_at` only after the due deadline-critical refresh has terminally completed and immediately after materializing that immutable observation tuple. Every included `observed_at` must be less than or equal to this `logical_at`.
- Scanner evaluation, `ScanSnapshot`, persisted funding quotes and public route rows, blocker/source-quality fields, and Telegram FULL payload all derive from that exact tuple and Scanner snapshot.
- `_route_row` or equivalent rendering must not reread mutable `self.observations` after `scan_once`. A concurrent stream or refresh mutation between Scanner evaluation and route rendering must not alter the persisted row or Telegram payload for that FULL.
- Preserve existing repository ownership and transactions. Do not create a SQLite read model, event sourcing, batch observation cache, or cross-venue publication transaction.

## Mandatory RED and proof matrix

### A. Same-slot settlement rollover

Construct a production-shaped sequence with an old 19:00 snapshot, Nado already advertising 20:00, and the due RISEx/Extended observation refresh returning authoritative 20:00 quotes seconds after the FULL slot. Prove RED on accepted `ab9c4e9b`: old ordering emits `TARGET_CYCLE_ELAPSED`. Corrected FULL is deferred until the owned observation refresh terminally completes and then uses the finished 20:00 state.

### B. Funding TTL edge

At the frozen 120-second funding TTL and 120-second FULL cadence, make a successful due observation refresh available while the prior quote reaches approximately 123 seconds. Corrected FULL must use the newly committed quote rather than knowingly consume the just-expired quote. TTL and cadence remain unchanged.

### C. In-flight refresh and legitimate partial success

Gate individual market-observation components and FULL capture. No FULL may capture halfway through the same owned refresh. After terminal completion it may contain new successful observations and honest last-good/unavailable failed components according to existing TTL/readiness rules. Prove no observation is later than `logical_at`; do not require cross-venue all-or-nothing publication or introduce a batch cache/transaction.

### D. Failure, timeout, and recovery

For an expected component timeout or official failure, prove no scan starvation and no invented value. Successful components remain fresh; a failed component uses last-good only within its existing TTL and otherwise exposes its exact existing blocker, including `FUNDING_STALE` where applicable. The next successful owned refresh restores numeric PnL when every mandatory input is valid. Programmer errors remain fatal rather than being masked.

### E. Scheduler priority under a slow refresh

Hold the deadline-critical refresh for more than 25-30 seconds across multiple health, lifecycle, T−120/T−5, and 10-second FOCUSED deadlines. Tick must continue servicing them on the frozen schedule while one FULL slot remains pending. On release, exactly one FULL consumes the terminal result; missed slots are coalesced under existing policy and no burst occurs.

### F. Catalog isolation and single-flight call counts

Gate Extended full-universe and required-market metadata independently from observation refresh. Neither catalog path delays FULL/FOCUSED; fresh TTL cache or exact catalog/metadata blocker is used. Overlapping seed, FULL, catalog, and ordinary triggers preserve existing Runtime ownership and bounded endpoint call counts with no duplicate observation refresh or REST burst.

### G. One tuple through Scanner, persistence, rows, and Telegram

After the immutable tuple is passed to `scan_once`, mutate live `self.observations` and stream state before route-row rendering. Scanner snapshot, persisted funding quotes, all public route fields including blocker/source quality, and Telegram FULL payload must remain derived from the captured tuple/snapshot. No later mutable reread may change that FULL.

### H. Actual top-5 route-component matrix

Using deterministic official-shaped fixtures, cover the configured current Top-5 matched markets, Extended and Nado hedge venues, and both directions: the existing up-to-20 FULL route rows, not an exchange-wide dynamic-universe feature. For every route component prove its authoritative source, owner, timestamp/freshness semantics, and exact result:

- catalog/required metadata and parity;
- BBO, exact depth, book health, and trade readiness/freshness;
- funding quote freshness, settlement cycle, eligibility, and predicted/estimated/applied semantics;
- execution grids, quantity/minimums, fees, and exact execution inputs;
- unchanged Scanner computation of `planned_maker_net_pnl_usd` when all mandatory inputs are valid.

Catalog, metadata, parity, funding, trade, and book failures must retain their own blocker and not masquerade as another component. Scanner retains its complete blocker tuple. The persisted/Telegram third column selects the existing deterministic shortest precise blocker from that exact tuple; do not replace Scanner's set with one reason or invent a new priority/economics rule. Numeric planned PnL may coexist with `entry_allowed=false` solely under the unchanged negative-PnL threshold.

### I. Telegram delivery-only preservation

Tracked presentation tests prove the FULL digest relays the exact persisted Scanner number or existing selected display blocker. Telegram performs no freshness, blocker, funding, fee, eligibility, or PnL calculation and gains no new behavior.

## Implementation and deterministic acceptance boundary

- Before production edits, reconstruct every RED independently against accepted `ab9c4e9b`; rejected branches are evidence only and must not be cherry-picked or copied wholesale.
- Exactly one Builder may work only after Chief activation, on `codex/paper-007-stabilization-003`, without spawning agents. Maximum two fix cycles.
- Keep every production hunk mapped to one invariant above. No unrelated cleanup or future-phase code. A minimal pending FULL marker is permitted only under the ownership, lifecycle, and boundedness rules above.
- Preserve direct R1-R16, accepted Phase A/B recovery/session/lifecycle evidence, the accepted 263-test suite, repository failure/cancellation races, compileall, diff review, non-null identity/owner/provenance audit, and secret scan.
- Deterministic acceptance requires exact RED-on-`ab9c4e9b`/GREEN-on-candidate mapping for A-I, focused and full pytest with no warnings, pending tasks, skipped causality checks, or weakened preservation assertions.

## Operational acceptance boundary

- Use a new dedicated disposable empty DB outside Git for the Scanner/UNKNOWN/PnL run. Keep the root DB and preserved open-position archive untouched. R16 open-position preservation remains a separate deterministic disposable-copy audit, not part of this empty-DB public run.
- Run 60-90 minutes across an hourly settlement boundary, Telegram disabled, with PID-only continuous supervision and only short measured read-only DB checkpoints. No reader transaction spans a sleep or deadline.
- Persist and audit refresh due/start/terminal timestamps, the pending FULL absolute slot, and the exact resulting FULL snapshot `logical_at`, so refresh→snapshot causality is proved directly rather than inferred from nearby events.
- Audit FULL/FOCUSED cadence, health/lifecycle deadlines, endpoint call counts, single-flight ownership, component successes/failures, numeric/UNKNOWN streaks, funding ages/cycles, future observations, route-row/Telegram-source identity, recovery/socket evidence, safe stop, integrity, and post-stop writes.
- A settlement-boundary FULL may be deferred for its one owned observation refresh. The next completed FULL must be numeric when every official mandatory input is available and valid; an exact honest UNKNOWN remains acceptable for a real component failure or official old/elapsed cycle.
- Telegram and authoritative Stage B remain stopped until deterministic and operational acceptance plus independent Chief Review. Only then may a separate decision consider restart.

No Builder may begin from this draft. Architect must first receive Chief approval or correction of this exact bounded contract.
