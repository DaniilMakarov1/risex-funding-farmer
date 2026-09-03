# Active bounded task

## DG-006 — Fillability Bounds Lossless Discovery

Status: `FROZEN PROSPECTIVELY / SAMPLE NOT OPENED`.

Objective: run exactly one clean public RISEx–Lighter fillability-bounds sample on accepted measurement source `4f83f8dea9f7a5deea4902f0c5cc6443e28004c1`, using the accepted lossless book-delta evidence path, and issue the section-0.9 terminal result without tuning after sample opening.

Frozen scope: exact public `BTC/ETH/SOL`; both directions; `$100/$250/$500`; `1/2/3/5 bps`; `0/300/500/1000 ms`; `25 s` freshness; unchanged configured fees/provenance, quote construction, strict lower bound, optimistic at-or-through upper bound, exact-q accumulation, no-lookahead capture, and per-policy/concentration report.

First-stop rule: `50` aggregate strict episodes, `500` unique eligible RISEx trades with relevant active quotes, `1,200 s` wall clock, or any integrity/fatal condition. Use one fresh owner-only store; unchanged `2,500,000`-record/`12 GiB` caps and `24 GiB` free-space prerequisite; exact source/universe admission; one physically-last terminal; deterministic repeated reports; all completeness and verdict rules in System Specification 3.3 sections 0.9 and 0.15.

Forbidden: retry, extension, early manual stop, threshold or sample-dependent tuning; economics, fees, margin grid, quote/fill/eligibility/horizon/protocol/venue/strategy change; private/auth/credential/signing/order preparation/dispatch/testnet/mainnet/write activity; `SS-002` or `SS-003` work.

After the immutable run, Chief records either case A, B, or C, or a concrete measurement failure. `SS-002` and `SS-003` remain closed unless a later separate owner decision follows a valid `ENTRY_EDGE_CANDIDATE`.
