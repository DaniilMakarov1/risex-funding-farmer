# Active bounded task

## Chief mission and completion condition

The current Chief mission is to finish the Entry Viability Stage with an evidence-backed product verdict about whether a practically available, repeatable positive RISEx-maker to Lighter-taker entry edge exists. Accepted SS-001B plus smoke is an intermediate technical milestone, not mission completion.

Required sequence: accept and integrate the minimal SS-001B measurement pipeline; complete its bounded public-only smoke; before examining the full discovery sample, freeze the exact discovery universe, freshness policy, public fee inputs and sources, numeric strict/optimistic fillability thresholds, minimum completeness, fatal/incomplete stop rules, maximum duration/evidence count, and verdict rules; run bounded real-public discovery; then issue exactly one predeclared Entry Viability verdict. Do not open SS-002 before that verdict.

## Legacy central economics/funding task

Status: `FROZEN / REPLACED`.

Do not resume an old Funding Farmer process, database, worktree, candidate, credential path, or operational authority.

## SS-001A — pure entry observer domain

Status: `ACCEPTED` at `f7251387f0ffe684ed5f3d61f4ea4601b6156183`.

The accepted pure contracts are the only Spread-domain base for SS-001B. Rejected candidates `a3ac545` and `7b3e370` remain non-authoritative history.

## SS-001B — limited public integration, evidence store, and report

Status: `AUTHORIZED FOR ONE FRESH VISIBLE SPREAD BUILDER`. The first Builder attempt was rejected without a commit or acceptance evidence; a replacement must start from the current accepted `main`.

Base: the exact accepted governance `main` selected by the Chief when the Builder is created.

Objective: connect the accepted SS-001A contracts to a deliberately limited RISEx/Lighter public feed runner, prospectively capture hypothetical quote/fill/hedge evidence, persist it append-only, and expose one bounded CLI report. This slice may run only a short 1–3 market public pipeline smoke. It does not authorize the discovery run, private access, or venue writes.

### Required architecture

- RISEx and Lighter only. Reuse the accepted public adapters, normalizers, `BookStream`, checksum/sequence logic, exact math, and neutral timestamp/provenance primitives; do not copy them.
- Do not import the legacy runtime, scanner, paper broker, lifecycle, route ranking/admission, funding activation, position state, persistence, Telegram/reporting, testnet/mainnet operations, private adapters, credentials, signing, or dispatch.
- Use a separate limited feed runner because the legacy runtime directly imports forbidden strategy modules. Do not create a general public runtime, generic event bus, generic recovery framework, service, or daemon.
- The observer boundary receives immutable accepted RISEx books/trades, accepted Lighter books, explicit public-stream health/gap changes, and diagnostic public funding only. It never receives old scanner output, `RoutePlan`, `PaperEntryState`, `LifecycleSnapshot`, route winners, or old PnL.
- One bounded non-blocking ingress queue may be used. It must never block the WebSocket critical path; overflow is an explicit venue/market/session/recovery `DATA_GAP`, never a silent drop. Stale, displaced, duplicate, gapped, or recovery-ambiguous evidence fails closed.
- Maintain the fixed research grid for both directions, notionals `$100/$250/$500`, margins `1/2/3/5 bps`, and horizons `0/300/500/1000 ms`. These are simultaneous counterfactual observations, never venue orders.
- Quote refresh cadence is independent of fill-to-hedge reaction. A strict hypothetical would-fill immediately creates monotonic deadlines; no periodic scan tick may delay detection or capture scheduling.
- At every deadline, use only the latest eligible Lighter book actually received no later than that deadline. No later-book replay, interpolation, retrospective quantity change, or assumed execution is permitted.
- Funding and points cannot enter entry edge. Public funding may be persisted only as separately labelled diagnostic evidence; points remain `$0`.

### Run identity and append-only evidence

- Every smoke or later discovery run uses a fresh unpredictable run ID and a fresh owner-only store. Never open or migrate legacy PAPER state.
- The store is append-only for observations. Corrections are new records linked to the superseded record; evidence rows are not updated or deleted during a run.
- Persist at minimum: run/config identity; market and venue identities; quote policy/version and exact sizing/economics; quote UTC/monotonic creation; RISEx trade exchange UTC plus authority when present; trade receipt UTC/monotonic; strict/optimistic fill-model label and evidence keys; would-fill detection monotonic; every horizon/deadline; exact hedge outcome; selected or conflicting book receipt/session/recovery/revision/sequence/checksum; exact requested/filled quantity and accumulated notional; VWAP when defined; gap/health provenance; and deterministic reason codes.
- Store metadata records the exact source commit, Python version, configured fee sources/rates, fixed grid, freshness policy, markets, and start/stop times. Secrets and raw credentials have no schema field and must never be accepted.
- A forced stop, queue overflow, disconnect, recovery ambiguity, or write failure to the evidence store must be explicit and fail closed. Missing evidence never becomes `NO TRADE`.

### CLI and report

- Provide separate public-only entrypoints for the bounded observer smoke and offline report. Importing or invoking them must expose no private/auth/write-capable path.
- The report groups every result by market, direction, size, target margin, and latency horizon. It reports opportunity count, quoteable-time share, median quote lifetime, RISEx-BBO distance in ticks, strict would-fill count, optimistic upper-bound count when implemented, full-hedge rate, partial/missing rate, mean/median/p05 exact entry edge, mean/median/p05 conditional markout, positive-edge share, maximum adverse markout, hypothetical RISEx filled notional, concentration, and data completeness.
- Show the complete latency curve and component metrics. Do not emit one profitability score, select a historically best route, treat funding as entry edge, or claim the smoke/discovery proves profitability.
- `HEDGE_OUTCOME_UNKNOWN` remains reserved for genuinely unclassified/incomplete evidence; named missing, stale, displaced, gap, partial, and zero-depth outcomes remain distinct in storage and reports.

### Bounded smoke and acceptance evidence

- Builder may perform one public-only smoke covering 1–3 markets for at most 15 minutes, with a fresh owner-only store and no legacy state. The purpose is pipeline/provenance validation, not strategy evaluation.
- Smoke must demonstrate accepted RISEx/Lighter event ingestion, event-driven fill reaction when an eligible fixture/replay or naturally observed public event occurs, all four deadlines, append-only persistence, deterministic offline replay/report, explicit gap handling, and clean shutdown. It need not wait for a natural strict fill; injected deterministic public-event fixtures must be labelled as fixtures and kept separate from observational results.
- Focused/adverse tests cover queue overflow/no silent drop, stale/displaced/recovery rejection, one-nanosecond no-lookahead, event-driven deadlines independent of quote refresh, deterministic replay, append-only/no-overwrite behavior, exact outcome preservation, report grouping/latency curve, fresh run/store identity, owner-only permissions, and unreachable private/auth/write imports.
- Builder runs one final clean Python 3.11 full suite on the committed candidate and reports exact preflight, branch/base, diff/scope, tests, dependency/import surface, smoke identity/duration/markets/outcome, sanitized evidence counts, store permissions, and clean Git status. Builder never self-accepts, merges, pushes `main`, or starts the full discovery run.
- Production implementation is an upper-bounded slice, not a line target: stop and escalate before exceeding `3200` new SS-001B production lines or introducing more than the limited runner, queue, append-only store, CLI, and report surfaces described here.

### Closed gates

- The full discovery run (up to 72 hours or 50 strict episodes) remains closed. Before it can start, SS-001B must be independently accepted and governance must freeze numeric interpretations of “approximately zero” and “materially positive”, the exact discovery market universe, freshness policy, configured public fee inputs/sources, and fatal/incomplete-evidence stop thresholds.
- `SS-002` and `SS-003` remain closed. No position/exit/funding lifecycle, real or prepared order, private connection, credential, signing, dispatch, testnet/mainnet write, transfer, withdrawal, strategy execution, multiple lots, inventory, batching, OMS, dashboard, Telegram, new venue adapter, ML, or optimization is authorized.

Completion: Chief independently reviews scope, diff, contracts, tests, public smoke evidence, dependency surface, Git, and final suite. Only Chief may accept and integrate.
