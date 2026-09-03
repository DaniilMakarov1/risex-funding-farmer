# Active bounded task

## SS-001D — Fillability Bounds

Status: `AUTHORIZED / BUILDER NOT YET OPENED`.

Objective: add one explicitly optimistic at-or-through public fill upper bound beside the unchanged strict lower bound, so a later prospective sample can distinguish unreachable profitable quotes from strict-model conservatism and capture delayed hedge evidence whenever either model fills.

Exact base: current accepted and published `main`. Use one fresh visible Spread Builder and fresh `codex/spread-ss-001d-fillability-bounds` branch/worktree. Verification is Level A. Builder performs no public/live run.

Allowed scope: the minimum `risex_spread_shadow` evidence, observer/runner, report, CLI/config, and focused test changes required for the System Specification 2.6 fillability-bound contract. Preserve the strict detector behavior byte-for-byte at its public boundary. Add separately named `OPTIMISTIC_UPPER_BOUND` cumulative at-or-through detection, model-scoped episode identity/horizons, unique eligible-trade and sample-stop counters, and bounded streaming/multi-pass per-policy reporting.

Required adverse evidence: quote must predate trade; correct market/direction/aggressor/session/recovery; equality qualifies optimistic but not strict; sub-tick through qualifies optimistic but not strict; cumulative exact-q threshold and time-to-fill; quote replacement/expiry resets eligibility; duplicates/conflicts never add volume; overlapping gap rejects; strict and optimistic episodes cannot suppress or duplicate one another; one eligible trade increments the stop counter once across policies; first-stop-wins for strict/eligible/time/integrity; model-separated no-lookahead horizons; named non-full outcomes remain non-imputed; report is deterministic and memory-bounded for a synthetic large stream.

Forbidden: queue-position/FIFO/L3/hidden-liquidity/probability/ML modeling; strict-rule relaxation; fees, venue, quote grid, maker pricing, strategy, funding, inventory, exits, storage representation/compression, generic architecture, legacy runtime, private/auth/credential/signing/write/testnet/mainnet, `SS-002`, or `SS-003` changes.

Acceptance: Chief independently verifies root-cause-aligned scope, focused/adverse tests, deterministic large-stream report evidence, one clean isolated Python 3.11 full suite on final SHA, dependency/import/private/write surfaces, Git cleanliness, and no strategy/storage expansion. Builder never self-accepts or merges/pushes `main`.

After acceptance only, Chief freezes `DG-003` prospectively. Proposed—not yet frozen—stop rule is the first of `50` strict episodes, `500` unique eligible trades, `20 minutes`, or integrity/fatal, with prospective record/byte caps and unchanged economic thresholds.
