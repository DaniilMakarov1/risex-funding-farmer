# Active bounded task

## DG-002B — Corrected Entry Viability Discovery

Status: `FROZEN / READY TO RUN`.

Objective: obtain one valid evidence-backed product verdict for RISEx-maker → Lighter-taker entry viability on the accepted measurement path.

Exact source: accepted and published `b4f2822327fc0f7b50a02d7aabfc2d6e61b453a4`. No Builder or code change is authorized. Verification is the prospective public-only gate frozen in System Specification 2.5.

Run exactly once for `60 seconds` in a fresh owner-only store with a `250,000`-record cap and exact `BTC/ETH/SOL`. Use both directions, `$100/$250/$500`, `1/2/3/5 bps`, `0/300/500/1000 ms`, `25 s` freshness, the frozen fees, and at most the first `50` strict episodes by record index.

Require the accepted source/surface, admission, terminal, fatal/integrity, permissions, deterministic-report, completeness, fillability, depth, edge, and exact seven-verdict precedence rules. Produce one verdict with exact evidence identity and the complete report.

Forbidden: any code, strategy, fee, quote-economics, quantity, fill-model, storage, private/auth/credential/signing/write/testnet/mainnet, venue, `SS-002`, or `SS-003` change.

Acceptance: the result is not caused by an implementation or integrity failure. A proven objective public-data limitation may still support `DATA_INSUFFICIENT`; another measurement-path failure is not mission success.

After the verdict, record it and stop the current Entry Viability Stage. Only `ENTRY_EDGE_CANDIDATE` permits a later proposal for `SS-002`; every other verdict leaves `SS-002` and `SS-003` closed.
