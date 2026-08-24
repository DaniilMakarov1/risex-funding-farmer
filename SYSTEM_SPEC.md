# RISEx Funding Farmer — Paper System Specification

SYSTEM_SPEC_VERSION = 1.0
SPEC_STATUS = FROZEN_FOR_PAPER_IMPLEMENTATION

## 1. Purpose and boundary

Research whether a delta-neutral funding strategy used to farm RISEx points can have non-negative trading PnL after configured fees and this paper execution model.

The normal product is PAPER ONLY and uses official public RISEx, Extended, and Nado data without private endpoints, trading keys, real orders, or collateral management. Section 21 defines a separate isolated testnet program; it is not a runtime switch and never authorizes mainnet or real funds.

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
EXTENDED_REQUIRED_MARKETS_MAX_AGE_SECONDS = 300
EXTENDED_UNIVERSE_REFRESH_SECONDS = 600
EXTENDED_UNIVERSE_MAX_AGE_SECONDS = 1200
EXTENDED_UNIVERSE_REQUEST_TIMEOUT_SECONDS = 60
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

For the explicitly authorized RISEx paper fallback, public trade and nonzero order-book quantities prove units when they are strictly positive and exactly step-aligned, and prices are strictly positive and exactly tick-aligned. Public fills and residual book levels need not meet `minimum_quantity_raw`. Minimum quantity remains mandatory for every planned paper order and entry eligibility. Empty evidence, zero/negative values, off-grid values, synthetic products, multiplier mismatch, or inconsistent metadata fail closed with a precise unit-evidence blocker; assumption markers remain paper-only.

For paper v1 only, 1 USD = 1 USDC = 1 USDT = 1 USDT0 for linear-perpetual parity, notional, fees, and PnL. Other quote/settlement assets are ineligible. Stablecoin depeg is not modeled.

## 5. Universe and routes

RISEx is one leg of every route. Hedge venue is Extended or Nado. Directions are:

- LONG RISEx / SHORT Extended
- SHORT RISEx / LONG Extended
- LONG RISEx / SHORT Nado
- SHORT RISEx / LONG Nado

`route_liquidity = min(risex_24h_quote_volume_usd, hedge_24h_quote_volume_usd)`. For each asset, `asset_liquidity` is the maximum eligible route liquidity. Select Top-5 assets by this value, exactly the available four directions per selected asset and at most 20 routes. Persist and deliver all evaluated directions; ranking does not truncate the evidence set. Convert official base volume using that venue's official current price; if unreliable, exclude the market.

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

Extended book, trade, and funding WebSockets are separate physical connections. Track connection confirmation and data readiness independently for each `(market, stream_kind)`; a ping, pong, or valid message confirms only its own socket, and `connection_combined` is not an Extended component. Each physical socket owns one deterministic client heartbeat at 10-second intervals; server ping is answered within its documented deadline. Quiet market data is not stale while heartbeat confirmation is fresh.

Extended trade `seq` is monotonic evidence, not an order-book delta chain: accept increasing sequences even when values are skipped, ignore duplicate or decreasing sequences, and reset the comparison at each physical WebSocket session. Persist at most one bounded trade-sequence discontinuity event per session; never restart or degrade the book because an Extended trade sequence was skipped. Physical reconnect backoff resets only after a validated frame or heartbeat confirmation, not merely after opening a socket.

For Nado predicted funding, retain the official event timestamp when it is not ahead of local receipt time. If the official public event timestamp is ahead because of venue/host clock skew, use the local receipt time as the quote freshness timestamp. Applied funding settlement timestamps remain official venue timestamps and are never clamped.

Only observed transport EOF/CLOSE/ERROR or a connection exception creates one ordered `PUBLIC_SOCKET_DISCONNECTED` / `PUBLIC_SOCKET_RECONNECTED` episode. A watchdog decision caused solely by confirmation age instead creates one deduplicated `PUBLIC_STREAM_CONFIRMATION_STALE` / `PUBLIC_STREAM_RESTARTED` episode and restarts only that stream. Sequence gaps retain only book-resync lifecycle evidence unless a physical transport event is separately observed. Shutdown/cancellation creates neither lifecycle.

Extended catalog health is independent from book, trade, and funding health. A validated full universe catalog is refreshed in a non-blocking background task every 600 seconds with its own 60-second total timeout and may be used for at most 1200 seconds. Each normal public refresh requests only already-authoritatively-mapped required markets through repeated official `market` query parameters; required metadata may be used for at most 300 seconds. Replacement is atomic after complete validation. Transient failures within TTL persist explicit cached-last-good evidence and do not degrade healthy streams or funding. Startup without a validated universe fails Extended closed as `CATALOG_UNAVAILABLE`; expired universe or per-market metadata fails closed as `CATALOG_STALE` or `MARKET_METADATA_STALE`, never `BOOK_UNHEALTHY`. All other public requests retain the shared 30-second timeout, and catalog work never delays FULL/focused deadlines.

Default funding maximum age is 120 seconds. A longer adapter cadence needs explicit official evidence, local comment, and test.

Before entry, stale data makes planned PnL unknown and forbids entry. Cancel an active maker with `PAPER_ORDER_CANCELLED_DATA_STALE`; do not reconstruct missed fills.

RISEx orderbook checksum mismatch follows the official public WebSocket contract: keep the book unusable, unsubscribe and resubscribe the public `orderbook` channel, and accept the new ordered WebSocket snapshots before recovery. Do not combine a RISEx REST snapshot with buffered WebSocket deltas because the REST response has no ordering marker comparable to the stream's block/log position. This logical book resubscribe must not be persisted as a physical socket disconnect/reconnect.

During an open position, a gap emits `MARKET_DATA_GAP_STARTED`, pauses normal HOLD/EXIT, preserves the position, and invents no price/VWAP. Recover a snapshot, emit `MARKET_DATA_GAP_ENDED`, then continue. Track `data_quality` COMPLETE/DEGRADED, gap flag/count/maximum duration, overlap with funding/exit, and primary-metric validity. Any open-position gap makes the trade DEGRADED and invalid for primary metrics.

In EXITING, cancel the current exit version during a gap. After recovery create a new version; aggressive mode and `exiting_normal_started_at` are sticky and downtime counts in exit wait.

## 7. Funding contract

Funding math belongs to adapters; core has no universal rate formula. `FundingAccrualMethod` is `SNAPSHOT_AT_SETTLEMENT`, `TIME_WEIGHTED`, `OTHER_DOCUMENTED`, or `UNKNOWN`.

Adapter output `FundingCashQuote` contains venue, canonical market, observation/open assumption/settlement times, quality (`PREDICTED`, `ESTIMATED`, `APPLIED_RATE`, `UNKNOWN`), accrual method, eligibility-known flag, long/short cash per canonical base USD, and source.

Adapter converts venue semantics to cash per canonical base. Core calculates `leg_funding_usd = canonical_base_quantity × cash_per_canonical_base_usd` and never multiplies by price again. Unknown eligibility/semantics forbids entry with `FUNDING_ELIGIBILITY_UNKNOWN`.

For Extended, the future Scanner quote comes only from the official REST market stats (`fundingRate`, `markPrice`, and future `nextFundingRate`). A funding WebSocket record is applied/history evidence: it never replaces the future `MarketObservation.funding`, never turns its event timestamp into a future settlement, and reconciles an open lifecycle only against the exact persisted venue/market/settlement identity. Keep REST quote readiness as `funding`, applied-record data readiness as `applied_funding`, and physical connection readiness as `connection_funding`; these components must not overwrite one another. The public funding stream contains only hourly applied records, so a heartbeat-confirmed funding connection is sufficient stream readiness for a future Scanner quote; absence of the first applied record after startup is not a blocker and does not make the venue unavailable. When public evidence cannot establish applied cash, retain `UNRESOLVED`; do not infer cash from a book midpoint or carry an applied rate into the next cycle.

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

## 19. Non-goals

No mainnet/real-money trading, route switching, clips, partial-position lifecycle, queue/taker-failure simulation, dynamic sizing, leverage, basis-convergence forecasting, ML, points valuation, stablecoin depeg, inbound Telegram commands, dashboards, or generic infrastructure. Separately governed testnet work never changes paper behavior.

## 20. Outbound Telegram notifications

Telegram is delivery-only, disabled by default, and never owns data, economics, lifecycle decisions, or cadence. It accepts only authoritative runtime events/results and exposes no inbound commands, polling, scheduler, or additional formulas.

Notify bounded authoritative lifecycle/data events and every persisted `FULL` scan digest, never `INITIAL`, `FOCUSED`, or `RECOVERY`. Deduplicate by stable event/route/cycle/displayed-value identity and split messages without changing or duplicating route rows.

Delivery uses a bounded non-blocking queue, finite timeout, and finite attempts; outage or saturation never delays runtime. An ambiguous `sendMessage` timeout is not retried. Credentials come only from explicit environment configuration and never enter persistence, logs, messages, arguments, or process titles.

Runtime has no elapsed-time stop. It distinguishes intentional signals from fatal failures, preserves open positions, persists bounded safe-stop evidence, and cancels/awaits background tasks without fabricating transport events.

## 21. Isolated three-venue testnet program

RISEx, Extended, and Nado testnet modules are opt-in and isolated from normal Farmer startup, Scanner, paper runtime/economics, Telegram, and paper persistence. They use only official environments and accounts, never mainnet or real funds, and keep every potential notional `<= USD 500`.

Authenticated read-only readiness requires correct environment/account identity, secret isolation, bounded transport, critical response semantics, and authoritative account state. Proven read-only failures may be retried as fresh bounded observations; operational run IDs are durable runtime data rather than source-code milestones.

Before every order/cancel/close dispatch, persist a unique venue-correct write identity and bounded intent. Never blindly replay an ambiguous write. Reconcile exact order/fill/position identity from authoritative venue state, stop automated writes on contradiction or unrelated account state, and permit manual testnet recovery as a failure terminal.

First-lifecycle success requires correct authenticated pre-state, one minimum bounded real order path, authoritative outcome reconciliation, any necessary cancel/close, and final authoritative zero relevant open orders plus exact flatness. Venue-specific authentication, signing, wire, nonce, and lifecycle semantics remain separate and must follow official or observed evidence.

### Current first-lifecycle write contracts

- **RISEx:** one minimum-size price-bounded `MARKET+FOK` opening; close from fresh authoritative exact position size, first as reduce-only price-bounded `MARKET+FOK` and later accepted state-based fallback as reduce-only price-bounded `LIMIT+IOC`; at most three automatic close attempts; one exact cancel only for an exact known experiment order, never account-wide cancellation. Every write has a new durable identity and ambiguous writes are never blindly replayed; success is zero relevant open orders plus exact flatness.
- **Extended:** persist an explicit unique nonce, external/settlement identity, and expiry before dispatch; entry and close use the accepted price-bounded short-expiry `IOC` contract, with reduce-only close for fresh authoritative exact position size. Never blindly replay ambiguous place/cancel/close; success requires reconciled lifecycle identities and fresh agreeing authoritative zero-order/exact-flat stream/REST barrier evidence.
- **Nado:** one minimum-size price-bounded post-only entry with unique durable signed identity; preserve accepted `recv_time`/nonce fencing, no digest replay, and exact regular-order cancel-all semantics. State-based close uses fresh authoritative position and the accepted reduce-only aggressive `IOC` contract; at most three automatic close attempts before halt/manual recovery. Success requires reconciled identities, zero regular/trigger orders, and authoritative exact flatness.

After all three first lifecycles pass, stop infrastructure expansion, perform one bounded commonality review, then open a separate strategy-testnet measurement phase. Mainnet hardening remains a later explicit product decision.
