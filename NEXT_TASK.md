# Active bounded task

## Mainnet public shadow — all-route liquidity measurement

Status: `BLOCKED — EXTENDED STREAM-HEALTH STARVES FULL CADENCE`.

Objective: use the accepted normal paper product against real unauthenticated RISEx, Nado, and Extended mainnet public data, evaluating every currently eligible venue-asset direction in `RISEx ∩ (Extended ∪ Nado)` without Top-5 or fixed route-count truncation. Measure whether opportunity frequency, duration, and conservative economics vary with authoritative route liquidity.

Exact starting point:

- Published `main` after the current governance checkpoint; normal commands remain `scan-once`, `paper-run`, and `report`.
- Two bounded Top-5 public runs on 2026-08-27 ended safely with SQLite integrity `ok`, zero orders/fills/positions/fatal events, and consistently negative planned net PnL. They are preserved as comparison evidence but do not satisfy the all-route window.
- Current public catalogs observed 15 unique eligible assets in the union: 15 RISEx/Extended pairs and 14 RISEx/Nado pairs, producing 58 directions. This is an observation, not a hard-coded universe; future catalog changes must be reflected dynamically.
- Accepted all-route preflight on 2026-08-27 evaluated all 58 directions and found two positive VVV/Nado planned routes in `< $250k`; immediate executable unwind for both was negative. The first paper-run then failed before any FULL scan: its deadline was 87.5 seconds late, concurrent public observations timed out for 37–60 seconds across venues, and shutdown required an exact-process force stop after a 30-second SIGTERM bound. Database integrity remained `ok` with zero orders, fills, and positions.
- Accepted `f856a9931c5fa17e385102038d21ab97b0b582c2` removed that first startup/fan-out blocker. A fresh preflight again covered 58 directions and a new paper-run persisted all 58 startup rows before READY. The first scheduled FULL was then starved by a synchronized Extended disconnect/staleness wave across its 45 per-market book/trade/funding tasks: sequential recovery remained inside the main tick, so no FULL deadline or fail-closed blocker was persisted. Intentional SIGINT still reached durable `STOPPED_SAFE` in 21.25 seconds; integrity is `ok` and orders/fills/positions are zero.
- Rejected branch `codex/strategy-measurement-foundation` at `300362d840141d9ed599d8189ed1d10801fc5256` is not a candidate and must not be merged or copied. Open a fresh Builder only if observed evidence proves a bounded defect or a measurement field genuinely missing from the accepted paper path.

Allowed scope:

- Public unauthenticated mainnet REST/WebSocket reads from the existing fixed venue adapters.
- A fresh central Builder from the exact published main may correct only the observed Extended stream-health scheduling defect. Health detection/restart work must not block the cadence scheduler: the scheduled FULL must either complete over the entire current universe or persist its existing bounded `PUBLIC_SCAN_BLOCKED` result. Extended book/trade/funding failures must remain fail-closed and recover without unbounded task creation or silent route loss. Existing startup catalog, per-venue REST bound, route eligibility, exact-size depth/VWAP, economics, lifecycle, Telegram, and shutdown semantics remain unchanged.
- After Chief acceptance, use a fresh isolated paper SQLite database, one preflight `scan-once`, then a bounded 24-hour `paper-run` unless a fail-closed performance/data blocker ends it earlier. Outbound Telegram remains authoritative delivery-only/non-blocking.
- Existing conservative paper semantics: exact Decimal arithmetic, canonical units, exact-size depth/VWAP, fee and execution PnL, funding timestamps, trade-through maker evidence, data-gap degradation, restart behavior, and `NO_TRADE` as a valid result.
- Report opportunity count/duration, COMPLETE versus DEGRADED paper lifecycles, planned and executable-unwind net PnL, fee/spread/slippage/funding components, funding source quality, latency/freshness failures, leg-risk proxies, and every assumption or blocker, both overall and in the fixed liquidity buckets.
- Read-only diagnostics and a fresh Builder correction only when a concrete mainnet-public observation contradicts accepted code. Any candidate requires focused/adverse tests and one clean Python 3.11 full suite on its final SHA.
- Required regressions cover a synchronized 45-task Extended disconnect/staleness wave while the first FULL becomes due, non-blocking/coalesced recovery ownership, complete FULL or precise bounded `PUBLIC_SCAN_BLOCKED`, no fabricated fresh evidence, no task leak, and bounded SIGTERM-style cancellation during recovery.

Forbidden scope:

- No API key, wallet/session/Stark key, authenticated/private endpoint, account creation, collateral, signing, nonce, order construction, order/cancel/close dispatch, testnet or mainnet write, real funds, or live strategy execution.
- No generic OMS, parallel execution engine, service/dashboard, new venue, or duplicate measurement framework. Do not modify isolated accepted Level C runners merely to support shadow measurement.
- No hard-coded current asset list, fixed Top-15 substitute, liquidity-based route exclusion, causal profitability claim, or unbounded raw market-message persistence.
- Do not raise request/scan/staleness/shutdown limits to hide the observed failure, reduce the universe, stagger away a required direction without explicit blocker evidence, or treat cached/stale data as fresh.
- Do not treat displayed depth as proof of fill, estimated RISEx funding as authoritative applied funding, DEGRADED/unresolved trades as profitability evidence, or a single positive snapshot as strategy validation.

Acceptance for the first checkpoint:

- Every currently eligible public venue-asset direction is evaluated; current catalog size is evidence rather than configuration, and catalog additions/removals reconcile without stale subscriptions or silent route loss.
- Startup and each FULL cadence either evaluate the complete current all-route universe from fresh authoritative evidence within the existing bounded schedule or persist an exact fail-closed blocker without hanging; intentional shutdown completes within 30 seconds and preserves SQLite integrity.
- The bounded run uses only public mainnet data and leaves verifiable zero credential/signing/write effects.
- Telegram delivery neither supplies market evidence nor changes scan cadence, economics, lifecycle decisions, or acceptance; delivery failure cannot block the runtime.
- The stored/reportable evidence distinguishes official values from paper assumptions and returns either quantified conservative paper opportunities or exact fail-closed/negative-economics reasons, including the predeclared liquidity buckets and enough history to compute frequency and consecutive duration.
- Chief defines the next statistical observation window and predeclared profitability/risk thresholds from the first evidence; no mainnet Level D or real-funds claim follows automatically.

Only after a sufficiently broad mainnet-public shadow sample shows durable conservative profitability may Chief open a separate Level D hardening task. Level D must still prove current mainnet contracts/endpoints, protected production identities, Extended private WebSocket, notional/loss/leg-risk limits, restart and ambiguous-write recovery, monitoring/manual recovery, a no-dispatch shadow run, and a separately authorized smallest real-funds canary.
