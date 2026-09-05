# CAL-FREEZE-001 — Prospective Profitability Threshold Freeze

Status: `ACTIVE / CHIEF-CONTROLLED HIGH-RISK RESEARCH GATE`.

Objective: freeze exact quantitative continuation and stop criteria before CAL-001, so neither its calibration observations nor the later untouched holdout can move the profitability goalposts.

Fixed design inputs: BTC only; `RISEX_SELL_LIGHTER_BUY`; `$100`; nominal `1/2 bps`; exact verified RISEx tier-1 maker fee `1 bps`; official Lighter Standard taker fee `0 bps`; horizons `0/300/500/1000 ms`; limits `250` unique eligible BTC trade keys, `1,200 s`, `1,000,000` records, `4 GiB`, or any fatal/integrity/completeness failure. No fill-count stop, extension, retry, or parameter change is permitted.

Required decision contract: define exact sample-validity requirements; the minimum independent venue-cluster and effective-level evidence; permitted concentration; exact-q hedge-completeness floors; conservative delayed-edge/markout statistics at the primary `300 ms` Lighter taker latency and stress horizons; how the paired nominal `1/2 bps` arms select one unchanged holdout policy without treating collisions or repeated quote versions as independent; and exact calibration-pass, holdout-pass, profitability-candidate, and stop precedence. The criteria must be computable from accepted report fields and must not rely on fitted probabilities, retrospective thresholds, time-window session proxies, funding, points, future exit value, or real execution.

Process: one temporary independent non-implementing auditor returns a single `ACCEPT` or `REVISE` verdict with concrete thresholds and failure modes. The Chief independently evaluates that advice, freezes the final contract in `SYSTEM_SPEC.md`, then opens one fresh visible Builder slice only for any bounded runner/report support required to execute the frozen design. Auditor and Chief must not open or inspect any CAL-001 sample because none may exist yet.

Forbidden: public sample launch; live/private reads; credentials; signing; source edits outside governance; sample-dependent thresholds; adding markets, directions, notionals, margins, horizons, or retries; orders, write payloads, dispatch, positions, balances, collateral, deposits, transfers, withdrawals, or strategy execution.

Acceptance: every threshold is explicit, internally consistent, computable from accepted evidence, unchanged between CAL-001 and HOLDOUT-001, and conservative enough that agreement can support only a public-paper profitability candidate, never a live-trading claim. Any ambiguity or unavailable report field opens a fresh bounded implementation slice before CAL-001.
