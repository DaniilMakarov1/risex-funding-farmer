# Active bounded task

## SS-001C — Measurement Reliability Correction

Status: `AUTHORIZED / BUILDER NOT YET OPENED`.

Objective: correct only the three measurement defects proven by the immutable `DG-001` evidence and accepted source: RISEx `PING` fall-through to fatal invalid-frame, ingress close failing to wake an already blocked consumer, and report completeness that globally degrades unrelated same-market evidence and treats zero strict fills as data corruption.

Exact base: current accepted and published `main`. Use one fresh visible Spread Builder and a fresh `codex/spread-ss-001c-measurement-reliability` branch/worktree. Verification is Level A plus, after independent acceptance and integration, a separately frozen public-only `DG-002A` stability run.

Allowed implementation scope: the minimum feed/ingress/runner/report surfaces required for correct RISEx ping handling, wake-up-safe draining shutdown, bounded consumer termination, exactly one clean terminal `RUN_STOP`, overlap-aware completeness, and separation of data quality from zero-fill evidence. Preserve explicit partial/stale/missing/gap outcomes and terminal-marker semantics.

Required adverse regressions: blocked empty consumer plus close; queued drain; double close; already-stopped transport; consumer exception; evidence-store failure; pending horizon at shutdown; no required queued-record loss; no synthetic planned-shutdown transport gap; exactly one clean `RUN_STOP`; overlapping gap invalidates only contaminated evidence; unrelated same-market evidence remains clean; zero strict fills is not automatically degraded; missing horizon rows remain incomplete; named partial/stale/missing/gap outcomes are never imputed as zero.

Forbidden: strategy, fee, quote-economics, quantity, fill-model, universe, storage architecture, generic lifecycle/recovery, private/auth/credential/signing/write/testnet/mainnet, venue, `SS-002`, or `SS-003` changes. Heavy persistence is observed but not proven causal, so no storage optimization is authorized in this slice.

Acceptance: Builder never self-accepts. Chief verifies exact base, root-cause-aligned narrow diff, focused adverse tests, one clean isolated Python 3.11 full suite on final SHA, dependency/import/private/write surfaces, Git cleanliness, and no strategy expansion before fast-forward integration and push.

After acceptance, freeze `DG-002A` prospectively before running it. `DG-002B` must not run or be frozen from observed stability/economic results before `DG-002A` passes. The immutable `DG-001` verdict remains `DATA_INSUFFICIENT` and is never renamed or reused as economic evidence. `SS-002` and `SS-003` remain closed.
