# PAPER-003 — Scanner

## Goal

Implement deterministic universe construction, route economics, target-cycle scheduling, activation, ranking, and `scan-once` service behavior over PAPER-001/002 contracts. Do not implement paper orders/fills, position lifecycle, SQLite trading storage, or final CLI wiring.

## Deliverables

- Eligible RISEx↔Extended/Nado route construction with canonical parity, stablecoin, market-type/status, BBO/grid/minimum, funding freshness/eligibility, volume, and exact-depth gates.
- Top-5 assets by max route liquidity and at most 20 routes.
- TargetFundingCycle construction from both venue events.
- Exact activation/cutoff scheduling: one-shot T−120, immediate startup evaluation only inside `5 < seconds_to_T < 120`, then 10-second focused cadence.
- Planned entry/exit prices, exact common-step quantity, exact taker VWAP, fees, funding, planned execution/net PnL, executable unwind net PnL, and no-trade reasons.
- Deterministic route ranking and one winner at a shared logical timestamp.
- A small async `scan_once` interface returning a deterministic snapshot; no persistence or long-running CLI loop.

## Acceptance tests

- Exact T−120 activation and startup at T−87.
- Strict T−5 cutoff: exchange timestamp before counts even if received after; exact cutoff/after do not.
- Simultaneous routes evaluated at one logical timestamp.
- Deterministic tie-break in the frozen order.
- Stale funding makes planned PnL UNKNOWN and entry forbidden.
- Unknown parity/multiplier/funding eligibility and insufficient exact depth are ineligible.
- Top-5/route-liquidity selection and maximum 20 routes.
- Minimum quantity/notional and no common executable quantity produce NO TRADE.
- Negative planned net PnL produces NO TRADE; non-negative permits eligibility.

## Constraints

- Work on `codex/paper-003` from accepted `main`; no subagents or product-rule changes.
- Use synthetic fixture/domain inputs only; do not weaken PAPER-002 UNKNOWN blockers.
- Run focused tests and full `pytest`, review the diff, commit, then report in at most 20 lines.
