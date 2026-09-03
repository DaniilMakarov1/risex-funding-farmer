# Active bounded task

## SS-001E — Evidence Throughput Recovery

Status: `AUTHORIZED / FRESH POST-REJECTION BUILDER REQUIRED`.

Objective: correct the proven DG-003 public evidence backpressure and terminal-drain failure with the smallest lossless change, without changing quote economics, fillability semantics, the evidence caps, or any strategy behavior.

Exact base: current accepted and published `main`. Use one fresh visible Spread Builder and fresh `codex/spread-ss-001e-throughput-recovery` branch/worktree. Verification is Level A. Builder performs no public/live run.

Allowed scope: the minimum `risex_spread_shadow` store/observer/runner/report and focused tests needed to make the existing `store_batch_size=128` and `store_batch_interval_seconds=0.25` contract real, preserve deterministic ordering, and drain a frozen sample cleanly under the observed bounded three-market load. Diagnose with immutable DG-003 evidence and deterministic fixtures only. Compact quote-to-book references are allowed only if measured fixture evidence proves batching/sync correction alone insufficient.

Required adverse evidence: at-or-above-observed burst/sustained fixture load has zero `QUEUE_OVERFLOW`; records retain deterministic append order and unique indices; periodic/batch sync has a strict maximum unsynced interval/count; stop flushes all pre-stop evidence, freezes later RISEx economics, retains only required Lighter horizon evidence, and completes within the accepted shutdown bound; write/sync/cap failures remain fail-closed with the reserved terminal marker; legacy DG-002B and failed DG-003 stores replay deterministically and remain historically unchanged.

Acceptance: Chief independently verifies the immutable failure diagnosis, scope, adverse load/flush/failure tests, deterministic replay of both historical stores, focused tests, one clean isolated Python 3.11 full suite on final SHA, dependency/import/private/write surfaces, Git cleanliness, and no economics/storage-platform expansion. Builder never self-accepts or merges/pushes `main`.

Forbidden: changing fees, quote grid, maker pricing, strict/optimistic definitions, eligibility, thresholds, stop logic, horizons, venue, strategy, funding, inventory, exits, or evidence caps; queue-size/timeout-only masking; compression/database/message-bus/generic persistence architecture; private/auth/credential/signing/write/testnet/mainnet; SS-002 or SS-003.

After acceptance only, Chief may freeze one fresh replacement public discovery gate prospectively. No repeat run is authorized before that freeze.
