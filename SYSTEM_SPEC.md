# RISEx Funding Farmer — Paper System Specification

SYSTEM_SPEC_VERSION = 1.0
SPEC_STATUS = FROZEN_FOR_PAPER_IMPLEMENTATION

## 1. Purpose and boundary

Research whether a delta-neutral funding strategy used to farm RISEx points can have non-negative trading PnL after configured fees and this paper execution model.

Phase one is PAPER ONLY. Use official public RISEx, Extended, and Nado data. Never use real money, authenticated/private/account endpoints, trading keys, real orders or positions, collateral management, or live execution. Live is a separately specified future phase, not a switch.

Build a small Python 3.11 application in one async process. Do not build a generic platform, event bus, plugin/DI framework, microservices, separate venue processes, Redis, Celery, dashboard, general alerting framework, or LLM calls from `paper-run`. The only notification exception is the bounded outbound Telegram delivery in section 20.

## 2. Fixed configuration

```text
PAPER_BALANCE_USD = 10000
TARGET_NOTIONAL_PER_LEG_USD = 500
MAX_OPEN_POSITIONS = 1
TOP_MARKETS = 5
NORMAL_SCAN_SECONDS = 120
FOCUSED_WINDOW_SECONDS = 300
FOCUSED_SCAN_SECONDS = 10
ENTRY_MAKER_START_BEFORE_FUNDING_SECONDS = 120
ENTRY_MAKER_CANCEL_BEFORE_FUNDING_SECONDS = 5
ENTRY_ORDER_REPRICE_SECONDS = 10
OPEN_POSITION_MONITOR_SECONDS = 10
NORMAL_EXIT_AGGRESSIVE_AFTER_SECONDS = 10
WEBSOCKET_HEALTH_CHECK_SECONDS = 10
MAX_MARKET_STREAM_SILENCE_SECONDS = 25
DEFAULT_MAX_FUNDING_DATA_AGE_SECONDS = 120
RISEX_MAKER_FEE_RATE = 0.00005
RISEX_TAKER_FEE_RATE = 0.00021
EXTENDED_MAKER_FEE_RATE = 0
EXTENDED_TAKER_FEE_RATE = 0.00025
NADO_MAKER_FEE_RATE = 0.0001
NADO_TAKER_FEE_RATE = 0.00035
EXPECTED_BASIS_CONVERGENCE_PNL_USD = 0
POINTS_VALUE_USD = 0
PAPER_ENTRY_MIN_PLANNED_NET_PNL_USD = 0
BTC_ETH_HARD_BASIS_EXPANSION_RATE = 0.04
OTHER_TOP5_HARD_BASIS_EXPANSION_RATE = 0.06
```

RISEx fees are user-configured Tier 3; Extended fees are documented public values; Nado fees are user-configured assumptions. Do not create `MAKER_IMPROVEMENT_RATE`. Maker prices derive from ticks. Values are paper experiment parameters, not live risk controls.

## 3. Official evidence and unknowns

Use only official public RISEx, Extended, and Nado APIs and documentation. Do not use aggregators, scraped UI, manually copied market values, or other projects.

If official evidence cannot establish units, multiplier, funding semantics/eligibility, applied funding, or instrument parity, set the value `UNKNOWN` and `EntryAllowed = false`. Never guess. PAPER-002 may stop with `BLOCKED — RISEX OFFICIAL FUNDING SEMANTICS INSUFFICIENT`.

## 4. Canonical markets and parity

Each adapter normalizes:

```text
canonical_asset, venue_symbol, market_type, contract_type,
base_multiplier, quote_asset, settlement_asset, tick_size_raw,
quantity_step_raw, minimum_quantity_raw, minimum_notional_usd,
minimum_fee_notional_usd (when defined), is_active, is_rfq, is_off_hours
```

Eligible instruments are active, non-RFQ, non-off-hours, linear perpetuals only. Exclude spot, stocks/synthetic equities, indices, commodities, FX, and non-crypto markets.

A route requires matching canonical asset, known multiplier, equal normalized base exposure, linear perpetuals on both legs, and parity proven by official metadata. Adapters produce canonical USD price per base, base quantity, tick, and quantity step. `canonical_quantity = raw_venue_quantity × base_multiplier`; normalize contract-denominated prices too. Core never infers multiplier semantics.

For paper v1 only, 1 USD = 1 USDC = 1 USDT = 1 USDT0 for linear-perpetual parity, notional, fees, and PnL. Other quote/settlement assets are ineligible. Stablecoin depeg is not modeled.

## 5. Universe and routes

RISEx is one leg of every route. Hedge venue is Extended or Nado. Directions are:

- LONG RISEx / SHORT Extended
- SHORT RISEx / LONG Extended
- LONG RISEx / SHORT Nado
- SHORT RISEx / LONG Nado

`route_liquidity = min(risex_24h_quote_volume_usd, hedge_24h_quote_volume_usd)`. For each asset, `asset_liquidity` is the maximum eligible route liquidity. Select Top-5 assets by this value, at most 20 routes. Convert official base volume using that venue's official current price; if unreliable, exclude the market.

A route also needs valid BBO, canonical grids and minimums, a fresh funding quote and next funding timestamp, known eligibility, and exact-quantity taker depth in both directions on both venues.

Sort routes deterministically by:

1. PlannedMakerNetPnLUSD descending
2. route_liquidity descending
3. target_cycle_start ascending
4. canonical_asset ascending
5. hedge_venue ascending
6. route_direction ascending

Evaluate simultaneous activations at one logical timestamp and choose one top route. Once an entry maker order exists, lock the route. Falling out of Top-5 forbids new entry but does not exit an existing position. Route switching does not exist.

## 6. Market data health

WebSocket supplies available BBO, book deltas, public trades, funding, connection state, sequence, and heartbeat/ping. REST supplies markets/metadata, volume, initial and recovery snapshots, missing stream data, and official applied-funding history when available.

Track per stream:

```text
last_market_event_at, last_connection_confirmation_at,
stream_connected, book_initialized, book_sequence_valid
```

Perform documented heartbeat/ping every 10 seconds. A market is usable only with healthy connection, initialized book, and valid sequence. No trades/price motion alone is not stale. Data is stale when connection confirmation is older than 25 seconds, or immediately on disconnect, gap, uninitialized/incomplete recovery, or invalid BBO.

Default funding maximum age is 120 seconds. A longer adapter cadence needs explicit official evidence, local comment, and test.

Before entry, stale data makes planned PnL unknown and forbids entry. Cancel an active maker with `PAPER_ORDER_CANCELLED_DATA_STALE`; do not reconstruct missed fills.

During an open position, a gap emits `MARKET_DATA_GAP_STARTED`, pauses normal HOLD/EXIT, preserves the position, and invents no price/VWAP. Recover a snapshot, emit `MARKET_DATA_GAP_ENDED`, then continue. Track `data_quality` COMPLETE/DEGRADED, gap flag/count/maximum duration, overlap with funding/exit, and primary-metric validity. Any open-position gap makes the trade DEGRADED and invalid for primary metrics.

In EXITING, cancel the current exit version during a gap. After recovery create a new version; aggressive mode and `exiting_normal_started_at` are sticky and downtime counts in exit wait.

## 7. Funding contract

Funding math belongs to adapters; core has no universal rate formula. `FundingAccrualMethod` is `SNAPSHOT_AT_SETTLEMENT`, `TIME_WEIGHTED`, `OTHER_DOCUMENTED`, or `UNKNOWN`.

Adapter output `FundingCashQuote` contains venue, canonical market, observation/open assumption/settlement times, quality (`PREDICTED`, `ESTIMATED`, `APPLIED_RATE`, `UNKNOWN`), accrual method, eligibility-known flag, long/short cash per canonical base USD, and source.

Adapter converts venue semantics to cash per canonical base. Core calculates `leg_funding_usd = canonical_base_quantity × cash_per_canonical_base_usd` and never multiplies by price again. Unknown eligibility/semantics forbids entry with `FUNDING_ELIGIBILITY_UNKNOWN`.

Before maker fill, `assumed_position_opened_at = current_evaluation_timestamp`: estimate full-position funding if opened now. Never use order creation as an open position, forecast a T−5 fill, or freeze the T−120 estimate. After full two-leg entry, `position_opened_at = risex_taker_fill_at` and funding is recomputed from actual open time.

## 8. Target cycle and settlement reconciliation

`TargetFundingCycle` includes ID, start/end/span, RISEx event, and hedge event. Start is the minimum and end the maximum event settlement timestamp; `T = target_cycle_start`.

Each event includes venue, market, settlement time, expected cash, eligibility, and status: `PENDING`, `ESTIMATED`, `APPLIED_RATE`, `UNRESOLVED`, `SKIPPED_POSITION_NOT_OPEN`, or `SKIPPED_POSITION_CLOSED`.

Funding enters PnL only when both legs are already open, eligibility is known, and the accrual method applies. Otherwise use deterministic skipped status when position was not open or was already closed.

Exactly one authoritative row exists for `venue + canonical_market + settlement_timestamp`. Allowed transitions:

```text
PENDING -> ESTIMATED -> APPLIED_RATE
PENDING -> APPLIED_RATE
PENDING -> deterministic SKIPPED
PENDING/ESTIMATED -> UNRESOLVED -> APPLIED_RATE
```

APPLIED_RATE replaces ESTIMATED; it is not another cash flow. Authoritative cash is applied cash if present, otherwise estimated cash, zero for deterministic skipped/PENDING, and unknown for unresolved without estimate.

`LifecycleRecognizedFundingUSD` sums best authoritative values for relevant elapsed settlements and controls HOLD/EXIT. Applied-rate PnL is reporting only. `AppliedRateClosedNetPnLUSD` is known only when every relevant event is APPLIED_RATE or deterministically skipped; otherwise UNKNOWN. Recompute PnL from authoritative rows, fills, and fees, never `pnl += event`.

## 9. Exact scheduling

`entry_activation_at = T - 120 seconds`; `entry_fill_cutoff_at = T - 5 seconds`. Create one exact activation event once the cycle is known and perform focused evaluation exactly then, independent of a periodic tick. Starting with `5 < seconds_to_T < 120` permits immediate focused evaluation. Recalculate every 10 seconds thereafter.

A trade participates only if `exchange_timestamp < cutoff`. A trade exactly at or after cutoff never counts, regardless of receipt time or coroutine ordering. Persist exchange, receipt, and raw timestamps. At T−5 cancel an unfilled order and skip the opportunity.

## 10. Price and quantity grids

`spread = best_ask - best_bid`; `spread_ticks = spread / tick_size`. Snapshot is invalid if bid >= ask, BBO is off tick, or spread is not a positive integer tick count.

Maker entry/normal exit price:

- one tick: BUY at best bid, SELL at best ask;
- two or more: BUY at bid + tick, SELL at ask − tick.

Target raw canonical quantity is `500 / planned_hedge_maker_canonical_price`. Compute each canonical step as raw step × multiplier, derive a common canonical step with integer-scaled LCM, and floor the target to it. Adapters convert canonical quantity back to raw quantity. Enforce both venues' minimum quantity and notional.

Exact taker VWAP walks asks for BUY and bids for SELL for exactly canonical quantity. Insufficient depth is not executable. Never use last/mark as execution, fixed slippage, spread reserve, hidden buffer, percentage improvement, progressive entry chasing, or fill probability.

## 11. Fees and planned entry economics

`fill_notional_usd = abs(q × canonical_fill_price)` and `fee_usd = fee_base_notional_usd × fee_rate`. Normally fee base equals fill notional. Nado taker fee base is max(fill notional, venue minimum fee notional). Persist rate, source, and observed/configured time.

Planned entry assumes hedge maker entry, RISEx taker entry VWAP, reverse hedge maker exit, and reverse RISEx taker exit VWAP.

For LONG, price PnL is `q × (exit - entry)`; for SHORT, `q × (entry - exit)`. `PlannedExecutionPnLUSD` is both legs' planned price PnL. `PlannedMakerNetPnLUSD = ExpectedTargetCycleFundingUSD + PlannedExecutionPnLUSD - all planned entry/exit fees`.

Expected basis convergence and points value are zero. Entry requires PlannedMakerNetPnLUSD >= 0. Scanner separately shows funding PnL, entry/exit execution PnL, fees, planned maker net PnL, and executable unwind net PnL. Spread appears only through actual quoted prices. NO TRADE is valid.

## 12. Paper maker entry

At activation create hedge-venue LIMIT POST_ONLY only for the first ranked route, non-negative plan, no position/order, fresh data, and executable RISEx exact-q entry VWAP. Lock the route.

Cancel only when its own planned PnL turns negative, data is stale, route becomes invalid/non-executable, or cutoff arrives. Every 10 seconds recalculate quote/PnL; price change cancels/replaces with a new version and resets old cumulative volume.

Trade evidence contains key, exchange/receipt time, canonical quantity/price, aggressor side, and `is_orderbook_match` true/false/unknown. Only true matches count. With an official realtime trade ID, key is venue + market + ID. Otherwise key adds connection session, exchange time, product, price, quantity, aggressor, and event ordinal.

BUY maker requires SELL aggressor and `trade_price <= buy_limit - tick`; SELL maker requires BUY aggressor and `trade_price >= sell_limit + tick`. Also require same venue/market/current version, pre-cutoff time, new key, and tick alignment. Accumulate eligible canonical quantity. Full fill occurs at cumulative quantity >= order quantity, at paper limit price. Do not model partial-position lifecycle, queue, or hidden liquidity.

## 13. Global paper taker assumption

Every virtual taker signal fills immediately and fully at fresh exact-q VWAP: RISEx entry hedge, RISEx normal exit, both Hard Basis legs, and executable-unwind calculations. Do not model latency, rejection, retry, failure, or partial taker. Book must be fresh, sequence-valid, and deep enough.

While entry maker is active, lost RISEx entry depth cancels it as non-executable. While exit maker is active, lost RISEx reverse depth cancels the version as `PAPER_EXIT_ORDER_CANCELLED_UNWIND_UNAVAILABLE`, preserves position/state, and recreates in the same mode after recovery.

Reports set true: taker failure/latency not simulated, partial fills not simulated, queue position not simulated, cancel/replace latency not simulated, stablecoin depeg not simulated, live margin/liquidation not simulated.

## 14. Entry completion and PnL

Persist maker fill exchange timestamp, maker fill receipt time, and local RISEx taker processing time. Position open time is RISEx taker fill time.

Immediately after full entry: persist actual prices/fees; recompute funding from actual open; quote current maker exit; compute PlannedMakerExitNetPnLUSD, PlannedHoldToTargetNetPnLUSD, and ExecutableUnwindNetPnLUSD; decide HOLD/EXIT without waiting. HOLD only when planned hold is strictly greater than planned maker exit; otherwise EXITING_NORMAL.

Planned exit price PnL uses actual entries and planned exits with normal long/short signs. Planned maker exit net equals recognized funding + both price PnLs − actual entry fees − planned exit fees. Executable unwind substitutes taker unwind PnL/fees.

Closed pair PnL is actual long PnL + actual short PnL. Actual paper fees are all actual entry and exit fees. Simulated closed net is simulated recognized funding + pair PnL − fees. Applied-rate closed net substitutes applied funding only when all events are applied/skipped; otherwise UNKNOWN. Always derive from authoritative evidence.

## 15. HOLD, exit, and next cycle

Every 10 seconds compute executable pair PnL, informational/executable basis, lifecycle funding, remaining target funding, maker exit net, executable unwind net, fees, and Hard Basis. Recognize eligible funding through actual close, including while EXITING.

Before target-cycle resolution, planned hold-to-target equals maker exit net + remaining target funding. Strict positive improvement means HOLDING; otherwise or unknown funding means EXITING_NORMAL. After first resolved cycle, build only the latest next cycle and apply the same comparison using expected next-cycle funding. Future basis EV is zero. EXITING never returns to HOLDING.

Normal exit maker placement follows normal maker rules. Exactly 10 seconds after normal exit begins without fill, transition to EXITING_AGGRESSIVE. Aggressive one-tick pricing is unchanged; at two+ ticks BUY at ask − tick and SELL at bid + tick. Aggressive is sticky. Reprice every 10 seconds.

Exit fill uses entry maker's aggressor, one-tick trade-through, cumulative-volume, and dedup rules. There is no timed/pre-funding/waiting taker fallback. Track exit wait, funding received while exiting, and pair PnL change while exiting. After hedge maker closes, reverse RISEx taker then FLAT.

## 16. Basis and hard exit

`InformationalMidBasis = short_mid / long_mid - 1`. `EntryExecutableBasis = short_entry_fill / long_entry_fill - 1`. `CurrentExecutableBasis = short_close_buy_vwap / long_close_sell_vwap - 1`. Adverse expansion is current executable minus entry executable.

Recheck event-driven after relevant book updates on both legs. Threshold is 4% for BTC/ETH, 6% for other Top-5 assets. On trigger, skip maker waiting and taker-close both legs at exact-q VWAP with taker fees, reason HARD_BASIS, then FLAT.

If either unwind quote is unavailable, basis and unwind PnL are UNKNOWN; record `UNWIND_QUOTE_UNAVAILABLE`, mark DEGRADED/invalid for primary metrics, and keep position open. Recalculate after recovery.

## 17. State, restart, and SQLite

Only states: `FLAT`, `ENTRY_MAKER_OPEN`, `HOLDING`, `EXITING_NORMAL`, `EXITING_AGGRESSIVE`. Do not add states without a product decision.

Identifiers include attempt, order, order version, position, trade event key, and funding settlement key. SQLite unique constraints enforce idempotency. Process a trade once, apply fill only to current version, maintain one authoritative settlement, and atomically persist state transition plus evidence/values. Do not build event sourcing.

Restart from entry maker cancels virtual order as `PAPER_ORDER_CANCELLED_PROCESS_RESTART`, reconstructs no fills, and returns FLAT. Restart from HOLDING restores the position, treats offline time as a degrading gap, and reconciles funding from official history. Restart from EXITING restores position, cancels old exit version without fill reconstruction, and creates a fresh version after snapshot; mode/timing remain sticky.

Persist scanner snapshots, funding quotes/cycles/settlements, orders/versions/cumulative trade-through/processed keys, fills/VWAP evidence, position samples, gaps, completed trades, and runtime state. Do not persist every raw stream message, every full raw book, or unbounded raw ticks.

## 18. Commands and reporting

Commands are `scan-once`, `paper-run`, and `report`. Report opportunities/eligible count, orders/fills/fill rate/active time, normal/aggressive exits, applied partial/estimated/unresolved funding, planned execution PnL, actual pair PnL, fees, simulated and applied-rate closed net (or UNKNOWN), both win rates, hold/exit duration, cycles, drawdown, virtual RISEx volume, PnL per $1,000 RISEx volume, planned-vs-actual error, complete/degraded trades, open position, and all assumption flags.

RISEx volume is absolute entry notional + absolute exit notional. Primary metrics include only closed, COMPLETE trades with all required funding resolved. Applied metrics additionally require complete applied/skipped settlements. Never force-close an open position at run end.

## 19. Non-goals and delivery

No live/authenticated trading, route switching, clips, partial-position lifecycle, queue/taker-failure simulation, dynamic sizing, leverage, basis convergence EV/forecasting, ML, long-range funding forecasts, points valuation, separate spread capture, stablecoin depeg, inbound Telegram commands, Telegram-triggered scans, dashboard, general alerts, or generic infrastructure.

Fixed milestones are BOOTSTRAP-000 and PAPER-001 through PAPER-006. PAPER-002, PAPER-004, and PAPER-005 require a design checkpoint in their `NEXT_TASK.md` before implementation. Tests follow the milestone matrix supplied by the product specification. After accepted PAPER-006 stop with `PAPER TRADER READY`. PAPER-007 staged execution requires a separate user decision and must not start automatically.

## 20. Outbound Telegram notifications

TELEGRAM-001 is an explicit user-authorized exception to the original Telegram non-goal. It is delivery-only and disabled by default.

`PublicPaperRuntime` remains the sole owner of exchange data, Scanner results, economics, orders, positions, funding reconciliation, lifecycle decisions, and cadence. Telegram code must not import or invoke Scanner, exchange adapters, `scan-once`, SQLite/reporting, broker, or lifecycle decision logic. It must not expose inbound commands, `getUpdates`, a scheduler, web server, dashboard, or additional formulas.

The runtime may enqueue an immutable notification only after an authoritative completed scan or persisted authoritative runtime/lifecycle transition. Minimum notifications are: paper-run started/ready; new eligible opportunity; material best-route change or disappearance; paper entry activation; paper position opened; funding received/reconciled; exit started; position closed with final PnL; critical data loss; data recovery; safe stop.

Opportunity payloads carry authoritative values without recalculation: UTC scan/event time, ticker, both-venue route, and the selected plan's `planned_maker_net_pnl_usd`. Repeated focused scans with unchanged semantic state produce no notification. Event notifications deduplicate by stable authoritative event ID. Opportunity notifications deduplicate by route, target cycle, and displayed-cent PnL state; appearance, disappearance, route/cycle change, or a displayed-cent change may notify once.

TELEGRAM-002 explicitly adds one digest after every completed and persisted `FULL` scan, but never after `INITIAL`, `FOCUSED`, or `RECOVERY` scans. The digest renders up to the same 15 already ordered authoritative route rows as three columns: `Ticker | Route | Expected PnL`. Route names both venues and sides; PnL is copied from `planned_maker_net_pnl_usd`, with `UNKNOWN` when absent. Rendering does not invoke Scanner or recalculate economics.

Delivery uses a bounded in-memory asyncio queue and a separate worker with finite timeout and finite attempts. Runtime enqueue is non-blocking; queue saturation or Telegram outage may drop a notification but must never delay scans, exact deadlines, lifecycle, or safe stop. A notification `event_id` is accepted into the queue at most once per process. Because Telegram `sendMessage` has no idempotency key, an ambiguous timeout is not retried; only an unambiguous pre-acceptance failure or explicit flood-control response may use the bounded retry allowance.

Telegram is enabled only when an explicit environment flag is true and both a bot token and destination chat ID are present. Secrets must never enter Git, SQLite, runtime evidence, notification payloads, logs, exceptions, rendered messages, CLI arguments, or process titles. For the single PAPER-007 Stage B restart authorized with TELEGRAM-001-FIX-001, the user explicitly accepted reuse of the previously disclosed token; its value is never recorded. This one-run risk acceptance does not weaken any secret-handling boundary or authorize future reuse.

`paper-run` and the Telegram delivery module never poll `getUpdates` or process inbound commands. The user separately authorized Architect-only diagnostic `getUpdates` calls to discover or verify a destination chat after the user initiates the bot. Such diagnostics do not enter runtime, persist message content, or authorize inbound product behavior.
