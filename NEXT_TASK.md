# Active bounded task

## SS-001H — Episode-Local Completeness and Material Stop

Status: `AUTHORIZED / BUILDER NOT YET OPENED`.

Objective: correct the proven DG-006 mismatch so a gap invalidates only its overlapping episode/horizon and a future sample stops on the already-frozen material per-policy strict threshold rather than an insufficient aggregate count.

Exact base: accepted published `main` after this governance record. Create one fresh visible Spread Builder and worktree from that exact base.

Allowed: raw/valid/contaminated episode and horizon attribution with named reasons; valid-only fill/edge verdict distributions; graceful CLOSE versus unexpected transport-failure evidence; exact session/recovery/full-snapshot barriers; online future stop at `10` valid strict episodes for one exact policy spanning `5` detection timestamps after all four horizons complete; deterministic DG-006 and legacy replay tests.

Acceptance: only overlapping episodes/horizons are invalid; contaminated evidence remains visible and cannot enter valid statistics; graceful CLOSE remains a bounded gap while transport exception fails closed; material stop is exact and first; focused/adverse, realistic-load, deterministic replay, and fresh isolated Python 3.11 full suites pass.

Forbidden: changing economics, fees, quote-margin grid, maker pricing, strict or optimistic fill semantics, eligible-trade semantics, horizons, venue contracts, queue/cap/timeout values, storage representation, strategy, private/auth/credential/signing/order preparation/dispatch/testnet/mainnet/write activity, `SS-002`, or `SS-003`.

After candidate delivery, Chief independently reviews and alone accepts/integrates. Freeze no replacement discovery gate until acceptance.
