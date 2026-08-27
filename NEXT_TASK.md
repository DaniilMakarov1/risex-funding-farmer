# Active bounded task

## Mainnet public shadow — all-route liquidity measurement

Status: `IMPLEMENTATION CANDIDATE REQUIRED`.

Objective: use the accepted normal paper product against real unauthenticated RISEx, Nado, and Extended mainnet public data, evaluating every currently eligible venue-asset direction in `RISEx ∩ (Extended ∪ Nado)` without Top-5 or fixed route-count truncation. Measure whether opportunity frequency, duration, and conservative economics vary with authoritative route liquidity.

Exact starting point:

- Published `main` after the current governance checkpoint; normal commands remain `scan-once`, `paper-run`, and `report`.
- Two bounded Top-5 public runs on 2026-08-27 ended safely with SQLite integrity `ok`, zero orders/fills/positions/fatal events, and consistently negative planned net PnL. They are preserved as comparison evidence but do not satisfy the all-route window.
- Current public catalogs observed 15 unique eligible assets in the union: 15 RISEx/Extended pairs and 14 RISEx/Nado pairs, producing 58 directions. This is an observation, not a hard-coded universe; future catalog changes must be reflected dynamically.
- Rejected branch `codex/strategy-measurement-foundation` at `300362d840141d9ed599d8189ed1d10801fc5256` is not a candidate and must not be merged or copied. Open a fresh Builder only if observed evidence proves a bounded defect or a measurement field genuinely missing from the accepted paper path.

Allowed scope:

- Public unauthenticated mainnet REST/WebSocket reads from the existing fixed venue adapters.
- A fresh central Builder from the exact published main may remove only the Top-5/fixed-route truncation, make runtime subscriptions follow every currently eligible RISEx/hedge intersection, preserve all evaluated route rows, and add the liquidity-conditioned report fields defined in `SYSTEM_SPEC.md`. Existing route eligibility, exact-size depth/VWAP, economics, lifecycle, and fail-closed semantics remain unchanged.
- After Chief acceptance, use a fresh isolated paper SQLite database, one preflight `scan-once`, then a bounded 24-hour `paper-run` unless a fail-closed performance/data blocker ends it earlier. Outbound Telegram remains authoritative delivery-only/non-blocking.
- Existing conservative paper semantics: exact Decimal arithmetic, canonical units, exact-size depth/VWAP, fee and execution PnL, funding timestamps, trade-through maker evidence, data-gap degradation, restart behavior, and `NO_TRADE` as a valid result.
- Report opportunity count/duration, COMPLETE versus DEGRADED paper lifecycles, planned and executable-unwind net PnL, fee/spread/slippage/funding components, funding source quality, latency/freshness failures, leg-risk proxies, and every assumption or blocker, both overall and in the fixed liquidity buckets.
- Read-only diagnostics and a fresh Builder correction only when a concrete mainnet-public observation contradicts accepted code. Any candidate requires focused/adverse tests and one clean Python 3.11 full suite on its final SHA.

Forbidden scope:

- No API key, wallet/session/Stark key, authenticated/private endpoint, account creation, collateral, signing, nonce, order construction, order/cancel/close dispatch, testnet or mainnet write, real funds, or live strategy execution.
- No generic OMS, parallel execution engine, service/dashboard, new venue, or duplicate measurement framework. Do not modify isolated accepted Level C runners merely to support shadow measurement.
- No hard-coded current asset list, fixed Top-15 substitute, liquidity-based route exclusion, causal profitability claim, or unbounded raw market-message persistence.
- Do not treat displayed depth as proof of fill, estimated RISEx funding as authoritative applied funding, DEGRADED/unresolved trades as profitability evidence, or a single positive snapshot as strategy validation.

Acceptance for the first checkpoint:

- Every currently eligible public venue-asset direction is evaluated; current catalog size is evidence rather than configuration, and catalog additions/removals reconcile without stale subscriptions or silent route loss.
- The bounded run uses only public mainnet data and leaves verifiable zero credential/signing/write effects.
- Telegram delivery neither supplies market evidence nor changes scan cadence, economics, lifecycle decisions, or acceptance; delivery failure cannot block the runtime.
- The stored/reportable evidence distinguishes official values from paper assumptions and returns either quantified conservative paper opportunities or exact fail-closed/negative-economics reasons, including the predeclared liquidity buckets and enough history to compute frequency and consecutive duration.
- Chief defines the next statistical observation window and predeclared profitability/risk thresholds from the first evidence; no mainnet Level D or real-funds claim follows automatically.

Only after a sufficiently broad mainnet-public shadow sample shows durable conservative profitability may Chief open a separate Level D hardening task. Level D must still prove current mainnet contracts/endpoints, protected production identities, Extended private WebSocket, notional/loss/leg-risk limits, restart and ambiguous-write recovery, monitoring/manual recovery, a no-dispatch shadow run, and a separately authorized smallest real-funds canary.
