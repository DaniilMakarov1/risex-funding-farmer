# Active bounded task

## Mainnet public shadow — first real-data measurement window

Status: `READY FOR CHIEF OPERATIONAL GATE`.

Objective: use the already accepted normal paper product against real unauthenticated RISEx, Nado, and Extended mainnet public data. Establish whether any route remains economically positive under the existing conservative execution model and collect the first bounded evidence for opportunity frequency, funding, fees, executable depth, spread/slippage, timing, stale-data rejection, leg risk, reconciliation health, and kill-switch reasons.

Exact starting point:

- Published `main` after the current governance checkpoint; normal commands remain `scan-once`, `paper-run`, and `report`.
- A fresh one-shot public mainnet scan on 2026-08-27 completed with all three venues `PUBLIC_MARKET_READY`, produced real routes, and correctly returned `NO_TRADE` because every route had negative planned net PnL.
- Rejected branch `codex/strategy-measurement-foundation` at `300362d840141d9ed599d8189ed1d10801fc5256` is not a candidate and must not be merged or copied. Open a fresh Builder only if observed evidence proves a bounded defect or a measurement field genuinely missing from the accepted paper path.

Allowed scope:

- Public unauthenticated mainnet REST/WebSocket reads from the existing fixed venue adapters.
- A fresh isolated paper SQLite database, one preflight `scan-once`, then a bounded 24-hour `paper-run` observation window unless a fail-closed blocker ends it earlier; run with outbound Telegram disabled and preserve the database and sanitized report as operational evidence.
- Existing conservative paper semantics: exact Decimal arithmetic, canonical units, exact-size depth/VWAP, fee and execution PnL, funding timestamps, trade-through maker evidence, data-gap degradation, restart behavior, and `NO_TRADE` as a valid result.
- Report opportunity count/duration, COMPLETE versus DEGRADED paper lifecycles, planned and executable-unwind net PnL, fee/slippage components, funding source quality, latency/freshness failures, leg-risk proxies, and every assumption or blocker.
- Read-only diagnostics and a fresh Builder correction only when a concrete mainnet-public observation contradicts accepted code. Any candidate requires focused/adverse tests and one clean Python 3.11 full suite on its final SHA.

Forbidden scope:

- No API key, wallet/session/Stark key, authenticated/private endpoint, account creation, collateral, signing, nonce, order construction, order/cancel/close dispatch, testnet or mainnet write, real funds, or live strategy execution.
- No generic OMS, parallel execution engine, service/dashboard, new venue, or duplicate measurement framework. Do not modify isolated accepted Level C runners merely to support shadow measurement.
- Do not treat displayed depth as proof of fill, estimated RISEx funding as authoritative applied funding, DEGRADED/unresolved trades as profitability evidence, or a single positive snapshot as strategy validation.

Acceptance for the first checkpoint:

- The bounded run uses only public mainnet data and leaves verifiable zero credential/signing/write effects.
- The stored/reportable evidence distinguishes official values from paper assumptions and returns either quantified conservative paper opportunities or exact fail-closed/negative-economics reasons.
- Chief defines the next statistical observation window and predeclared profitability/risk thresholds from the first evidence; no mainnet Level D or real-funds claim follows automatically.

Only after a sufficiently broad mainnet-public shadow sample shows durable conservative profitability may Chief open a separate Level D hardening task. Level D must still prove current mainnet contracts/endpoints, protected production identities, Extended private WebSocket, notional/loss/leg-risk limits, restart and ambiguous-write recovery, monitoring/manual recovery, a no-dispatch shadow run, and a separately authorized smallest real-funds canary.
