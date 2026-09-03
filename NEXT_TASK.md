# Active bounded task

## DG-007 — Fillability Bounds Resolution Discovery

Status: `FROZEN PROSPECTIVELY / NOT YET OPENED`.

Objective: run exactly one public unauthenticated RISEx–Lighter fillability-bounds sample that can resolve the active mission through the frozen valid strict/optimistic bracket and delayed-edge rules.

Exact source: `6e03195fe1c45e076cbe4cd20a2a02b178cc40e1`.

Frozen sample: `BTC/ETH/SOL`; both directions; `$100/$250/$500`; `1/2/3/5 bps`; `0/300/500/1000 ms`; configured fees; `25 s` freshness; unchanged strict lower bound and optimistic upper bound. Stop on the first exact policy with `10` valid all-horizon strict episodes across `5` detection timestamps, or `500` eligible trades, or `1,200 s`, or integrity/fatal failure.

Admission and evidence: exact source/universe match; owner-only append-only full-plus-delta store; at least `24 GiB` free; unchanged `2,500,000`-record and `12 GiB` caps; contiguous indices and valid chains/references; all required model-scoped horizons; raw/valid/contaminated named attribution; one physically-last terminal; deterministic repeated reports. Run exactly once and do not tune, extend, or stop from observed economics.

Verdict: apply System Specification 0.9 and 0.17 precedence using valid episode/edge evidence only. Complete the active mission only with case A, B, or C as defined there; in case C finish the prospective entry-edge verdict and stop before `SS-002`.

Forbidden: any implementation change, sample-dependent tuning, private/auth/credential/signing/order preparation/dispatch/testnet/mainnet/write activity, venue or strategy change, `SS-002`, or `SS-003`.
