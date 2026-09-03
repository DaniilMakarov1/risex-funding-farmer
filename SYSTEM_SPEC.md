# RISEx Spread Shadow — System Specification

SYSTEM_SPEC_VERSION = 2.4
SPEC_STATUS = ACTIVE_SPREAD_SHADOW__FROZEN_LEGACY_FUNDING_FARMER

## 0. Active product domain: RISEx Spread Shadow

RISEx Spread Shadow is the active public-only research contour. Its question is whether a reproducible positive entry-execution edge exists when a hypothetical RISEx maker fill is followed by a delayed executable exact-quantity Lighter Standard taker hedge. Points are worth zero. Funding is diagnostic and separate. A future maker fill, future maker exit, or basis convergence is never recognized as earned entry income.

The fixed directions are:

- Direction A: RISEx maker BUY, then Lighter taker SELL.
- Direction B: RISEx maker SELL, then Lighter taker BUY.

For exact canonical quantity `q`:

```text
EntryEdgeA(h) = q * (LighterSellVWAP(T_detect + h) - RISExMakerBuyPrice)
                - exact configured entry fees
EntryEdgeB(h) = q * (RISExMakerSellPrice - LighterBuyVWAP(T_detect + h))
                - exact configured entry fees
ConditionalMarkout(h) = EntryEdge(h) - EntryEdge(0)
```

Exact-q VWAP already includes visible spread and depth impact. No second spread or slippage deduction is permitted. The configured RISEx fee is applied exactly once with its provenance; public-only evidence must not be described as an observed account-specific fee. Lighter Standard fee and published latency are frozen research inputs with source metadata, not private-account observations or end-to-end execution guarantees.

### 0.1 Quote economics and deterministic quantity

`target_margin_bps` is the minimum net entry-execution edge after the exact configured entry fees, measured against actual Lighter hedge notional, before any empirical latency or markout haircut. It is not distance from RISEx BBO, gross pre-fee spread, future funding, future exit income, or a latency reserve.

For `m = target_margin_bps / 10000`, configured RISEx maker fee `fR`, configured Lighter taker fee `fL`, exact-q Lighter sell VWAP `Hsell(q)`, and exact-q Lighter buy VWAP `Hbuy(q)`:

```text
max_risex_buy = Hsell(q) * (1 - fL - m) / (1 + fR)
risex_buy_quote = min(
    round_down_to_risex_tick(max_risex_buy),
    risex_best_ask - risex_tick,
)

min_risex_sell = Hbuy(q) * (1 + fL + m) / (1 - fR)
risex_sell_quote = max(
    round_up_to_risex_tick(min_risex_sell),
    risex_best_bid + risex_tick,
)
```

After tick rounding, the domain recomputes and records the actual exact entry edge. A rounded quote is not assumed to retain the target edge; a quote whose recomputed edge is below the requested target is not economic.

For each configured target notional, the current required-side Lighter top price is the deterministic sizing reference. Compute `q_raw = target_notional / reference_price`, floor it to the exact common RISEx/Lighter canonical quantity step, validate both venues' minimum quantity and minimum notional, then calculate exact-q Lighter VWAP and derive the RISEx quote. Quantity is not optimized or resized retrospectively from later books.

### 0.2 Fill and hedge evidence semantics

Strict would-fill is a conservative lower bound, not ground truth. An optional optimistic model is only an explicitly labelled upper bound. The SS-001 product interpretations are:

- strict approximately zero and optimistic approximately zero: `PROFITABLE_QUOTES_UNFILLABLE` / no-go;
- strict approximately zero but optimistic materially positive: `FILLABILITY_INSUFFICIENT_EVIDENCE`;
- repeated strict fills: delayed-edge evaluation is supported.

Absence of strict fills alone does not kill the strategy. The numeric meanings of `approximately zero` and `materially positive` must be explicit frozen SS-001B configuration selected before the discovery sample; SS-001A must not hide or infer those thresholds.

`HEDGE_OUTCOME_UNKNOWN` is reserved for a genuinely unclassified or incomplete terminal state and is not a catch-all. Missing book, stale book, displaced session, overlapping data gap, partial depth, and zero executable depth retain distinct outcomes. `HEDGE_PARTIAL` means positive executable quantity below exact `q`; `HEDGE_DEPTH_UNAVAILABLE` means zero executable quantity in an otherwise valid required-side book. Known data failures use exact missing, stale, displaced-session, or gap-overlap classifications and never become `NO TRADE` or `HEDGE_OUTCOME_UNKNOWN`.

Exact accumulated Lighter notional is authoritative. VWAP is derived evidence and may be a repeating decimal; neither entry-edge nor partial/full hedge validation may reconstruct exact notional by requiring `notional == quantity * rounded_vwap`.

Sizing evidence is internally valid only when it binds the quote direction and policy target notional and deterministically recomputes `q_raw`, the common canonical step, the floored quantity, both raw venue quantities, and every minimum flag from its retained inputs. A quote with mismatched or unverifiable sizing evidence is not economic.

Every data-gap contract identifies its source venue in addition to market, session, and recovery generation. RISEx trade-evidence gaps may invalidate would-fill evidence; Lighter book-evidence gaps may invalidate hedge horizons. A gap from one venue must never invalidate the other venue's evidence.

Sensitivity horizons are `0`, `300`, `500`, and `1000` milliseconds after local monotonic would-fill detection. `2000` milliseconds is permitted only as a cheap stress horizon. The complete latency curve is primary. In particular, `500 ms` is diagnostic and is not a prediction, actual execution latency, SLA, admission guarantee, or fill claim.

Every horizon uses only the latest current-session, sequence-valid Lighter book actually received at or before its absolute monotonic deadline. Later books are never applied retroactively and interpolation is forbidden. Missing, stale, displaced-session, gap-overlapping, or insufficient-depth evidence remains an explicit hedge outcome and never becomes `NO TRADE`.

The fill-to-hedge observation path is event driven from the hypothetical maker-would-fill detection event. Periodic quote refresh or legacy scan cadence may not delay it.

### 0.3 Stages and authority

- `SS-001`: entry observer research. `SS-001A` contains only deterministic pure domain/evidence contracts. It contains no serializer framework, persistence abstraction, generic engine, event bus, sockets, CLI, database, or reporting. Canonical evidence is deterministic without introducing a custom serialization subsystem. After independent acceptance, `SS-001B` may integrate public RISEx/Lighter feeds, prospective horizon captures, append-only evidence, and one bounded report.
- `SS-002`: one-lot complete-cycle shadow trader. Closed until SS-001 produces repeated `ENTRY_EDGE_CANDIDATE` evidence and the user authorizes the next slice.
- `SS-003`: frozen-policy holdout. Closed until SS-002 is accepted and its policy is frozen before an untouched interval.

No private endpoint, credential, signing, order preparation, dispatch, testnet/mainnet write, real fund, transfer, withdrawal, or strategy execution is authorized. The active entrypoint must have no reachable dependency on private/auth/write surfaces.

### 0.4 Package and dependency boundary

New code lives under `src/risex_spread_shadow/`; tests live under `tests/spread_shadow/`. It has separate CLI, configuration, run identity, persistence, and report surfaces.

It may reuse only venue-neutral/public contracts and exact pure math: normalized public market models, RISEx and Lighter public adapters, accepted checksum/sequence handling, exact-q VWAP, tick math, common quantity-grid math, fee math, and timestamp/provenance primitives. It must not copy public adapters or create a second venue-contract implementation.

The following legacy strategy dependencies are forbidden from every new entrypoint-reachable path: `risex_farmer.scanner`, `risex_farmer.paper_broker`, `risex_farmer.lifecycle`, legacy `RoutePlan` admission/ranking, funding activation/cutoff policy, position state machine, persistence, Telegram/reporting, operational testnet/mainnet modules, authenticated adapters, credentials, signing, and write code. The legacy public `risex_farmer.runtime` is also not a Spread strategy dependency because it directly imports those forbidden modules.

The limited Spread feed runner may reuse the accepted public adapters and venue-neutral `BookStream`, but must cover only RISEx and Lighter, expose immutable accepted events through one bounded non-blocking queue, emit explicit `DATA_GAP` on overflow, preserve session/recovery/book-revision/sequence/checksum provenance, reject stale or displaced events, and fail closed on ambiguity. It must not copy normalizers, import strategy modules, or grow a generic recovery/event framework or general public runtime. SS-001B must first pass a short 1–3 market end-to-end public pipeline smoke; only the unchanged accepted runner may then expand to the full discovery universe.

### 0.5 Frozen Entry Viability discovery gate `DG-001`

This gate was frozen on `2026-09-03` after SS-001B acceptance and its bounded smoke, and before examining any discovery run. It applies only to the next single real-public discovery run on unchanged accepted source `9ac7b73941b9f0217cfa6a2ef68b21d6040fd015`.

- Universe: exact public RISEx/Lighter intersections `BTC`, `ETH`, and `SOL`; both fixed directions; exact `$100/$250/$500` notionals, `1/2/3/5 bps` target margins, and `0/300/500/1000 ms` horizons. Failure to admit all three markets makes the run `DATA_INSUFFICIENT`.
- Freshness: the paired quote books and every selected Lighter hedge book must be current-session, sequence-valid, gap-free for the relevant identity, and at most `25,000,000,000 ns` old by local monotonic receipt time. A book received even `1 ns` after a horizon deadline is ineligible. Missing, stale, displaced, partial, zero-depth, and gap outcomes remain distinct.
- Fees: RISEx maker `0.00005`, labelled `CONFIGURED_RISEX_RESEARCH_INPUT` and never described as a public or account-specific observation; Lighter Standard taker `0`, sourced from `https://docs.lighter.xyz/trading/trading-fees` as checked on `2026-09-03`. Points are `$0`; funding and future exit value are excluded.
- Run bound: one fresh owner-only observational store, maximum `60 seconds`, maximum `250,000` persisted records, and at most the first `50` `WOULD_FILL` records by append-only `record_index` enter the verdict sample. Any later in-flight records remain immutable but are excluded. Crossing the record cap, source mismatch, missing clean `RUN_STOP`, any `RUN_FAILED`, non-null fatal reason, store/queue/history-capacity failure, unclassified schema failure, or secret/private/write surface makes the result `DATA_INSUFFICIENT` and ends the gate.
- Completeness: terminal `PUBLIC_SMOKE_STOPPED` markers after a clean bounded stop do not invalidate earlier completed observations. Every other gap remains visible and invalidates overlapping evidence. A non-data verdict requires no `HEDGE_OUTCOME_UNKNOWN`, all four horizon rows for at least `95%` of sampled strict episodes globally and for every policy used by the verdict, and at least `95%` full-or-explicitly-classified hedge outcomes. Partial, missing, stale, displaced, gap, and zero-depth outcomes are never imputed as zero edge.
- Fillability thresholds: `approximately zero` means `0` or `1` sampled strict episodes across the run; the same numeric threshold applies to an implemented optimistic model. `Materially positive` fillability means at least `10` strict episodes for one exact market/direction/size/margin policy, spanning at least `5` distinct would-fill detection timestamps. Because SS-001B has no optimistic model, low strict fillability cannot be called optimistic zero.
- Edge materiality: for one full hedge, materially positive means exact entry edge at least `max($0.01, 1 bp of hypothetical RISEx filled notional)`. A candidate policy must have at least `90%` positive-edge share and a materially positive `p05` at `300 ms`, a strictly positive median at `500 ms`, at least `95%` full-hedge rate at every horizon, and the complete latency curve reported. The published Lighter Standard `300 ms` taker latency is a research input, not an execution guarantee; `500 ms` remains diagnostic.
- Snapshot availability: quoteable-time share of at least `1%` for any policy is materially present. Below that for every policy is snapshot absence.

Verdicts are evaluated in this fixed precedence and exactly one is emitted:

1. `DATA_INSUFFICIENT` for a failed run bound, missing universe, fatal/integrity condition, or insufficient completeness.
2. `NO_SNAPSHOT_EDGE` when every policy has quoteable-time share below `1%`, or repeated complete strict evidence exists but no policy has materially positive `0 ms` edge.
3. `LIGHTER_DEPTH_UNSUITABLE` when a materially fillable policy exists but at least `50%` of its valid `0 ms` outcomes are `HEDGE_PARTIAL` or `HEDGE_DEPTH_UNAVAILABLE`.
4. `PROFITABLE_QUOTES_UNFILLABLE` only when snapshot edge is materially present, both strict and implemented optimistic counts are approximately zero, and completeness passes.
5. `FILLABILITY_INSUFFICIENT_EVIDENCE` when snapshot edge is materially present but strict evidence is below the materially-positive threshold and the optimistic model is absent or materially positive.
6. `LATENCY_DESTROYS_EDGE` when repeated complete strict evidence has materially positive `0 ms` edge but no policy satisfies the frozen `300/500 ms` edge requirements.
7. `ENTRY_EDGE_CANDIDATE` when at least one exact policy satisfies every frozen fillability, completeness, full-hedge, `300 ms`, and `500 ms` condition above.

No discovery result proves profitability. `SS-002` remains closed through this gate and can only be proposed after the terminal verdict is recorded; it requires `ENTRY_EDGE_CANDIDATE` rather than another verdict.

### 0.6 Prospective measurement-stability gate `DG-002A`

This gate was frozen on `2026-09-03` after independent acceptance of SS-001C and before any corrected public sample. It validates only measurement-path stability; economic observations from this run cannot select, tune, or constitute the later discovery verdict.

- Source and surface: unchanged accepted source `b4f2822327fc0f7b50a02d7aabfc2d6e61b453a4`; public unauthenticated RISEx and Lighter data only; exact `BTC/ETH/SOL` admission; the SS-001C private/write dependency scan must remain clean.
- Bound: exactly one fresh owner-only observational store, `60 seconds`, at most `250,000` persisted records, and no manual process intervention. The command must return within `10 seconds` after its configured duration.
- Required success evidence: all three requested markets are admitted; source metadata matches; exactly one clean `RUN_STOP` is the sole terminal marker; there is no `RUN_FAILED`, non-null fatal reason, `RISEX_PUBLIC_FRAME_INVALID`, unexpected `PUBLIC_SOCKET_DISCONNECTED`, queue overflow, history-capacity failure, store failure, or unclassified schema failure. Planned terminal `PUBLIC_SMOKE_STOPPED` gaps are allowed only after the bounded stop and must not contaminate earlier completed evidence.
- The retained store must remain below the record cap, have owner-only directory/file permissions, and produce byte-identical canonical offline JSON reports in two consecutive reads. Record counts, strict fills, horizons, and edges are reported as diagnostics only and have no pass threshold here.
- Any failed condition blocks `DG-002B`. There is no automatic retry or threshold change from observed output; a new run requires a separately recorded diagnostic result and prospective gate.

## Legacy benchmark domain: RISEx Funding Farmer

Sections 1–21 below are preserved as the historical Funding Farmer specification. That strategy and its profitability path are frozen legacy benchmark material. They remain in the repository but are not active product behavior and must not supply strategy logic to RISEx Spread Shadow.

## 1. Legacy purpose and boundary

Historically, this domain researched whether a delta-neutral funding strategy used to farm RISEx points could have non-negative trading PnL after configured fees and its paper execution model. It is now frozen as a legacy benchmark.

The normal product is PAPER ONLY and uses official public RISEx, Extended, Nado, and Lighter data without private endpoints, trading keys, real orders, or collateral management. Section 21 defines separate isolated testnet programs; they are not runtime switches and never authorize mainnet or real funds.

Build a small Python 3.11 application in one async process. Do not build a generic platform, event bus, plugin/DI framework, microservices, separate venue processes, Redis, Celery, dashboard, general alerting framework, or LLM calls from `paper-run`. The only notification exception is the bounded outbound Telegram delivery in section 20.

## 2. Fixed configuration

```text
PAPER_BALANCE_USD = 10000
TARGET_NOTIONAL_PER_LEG_USD = 500
MAX_OPEN_POSITIONS = 1
NORMAL_SCAN_SECONDS = 120
FOCUSED_WINDOW_SECONDS = 300
FOCUSED_SCAN_SECONDS = 10
ENTRY_MAKER_START_BEFORE_FUNDING_SECONDS = 180
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
LIGHTER_MAKER_FEE_RATE = 0
LIGHTER_TAKER_FEE_RATE = 0
EXPECTED_BASIS_CONVERGENCE_PNL_USD = 0
POINTS_VALUE_USD = 0
PAPER_ENTRY_MIN_PLANNED_NET_PNL_USD = 0
BTC_ETH_HARD_BASIS_EXPANSION_RATE = 0.04
OTHER_ASSET_HARD_BASIS_EXPANSION_RATE = 0.06
```

RISEx fees are user-configured Tier 3; Extended fees are documented public values; Nado fees are user-configured assumptions. Lighter PAPER and testnet are explicitly configured for the official Standard account tier with zero maker and taker fees; do not query or re-prove the account tier per scan or lifecycle. A future Lighter mainnet gate must prove the configured tier once at provisioning/startup before relying on it. Do not create `MAKER_IMPROVEMENT_RATE`. Maker prices derive from ticks. Values are paper experiment parameters, not live risk controls.

## 3. Official evidence and unknowns

Use only official public RISEx, Extended, Nado, and Lighter APIs and documentation. Do not use aggregators, scraped UI, manually copied market values, or other projects.

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

RISEx is one leg of every route. Hedge venue is Extended, Nado, or Lighter. The public shadow universe is the dynamic union `RISEx ∩ (Extended ∪ Nado ∪ Lighter)` after the existing active linear-perpetual, canonical-parity, volume, metadata, and safety eligibility checks. Include every currently eligible RISEx/hedge venue-asset pair independently; an asset available on only one hedge venue still contributes that pair. Do not truncate the universe by liquidity rank or by a fixed route count. Directions are:

- LONG RISEx / SHORT Extended
- SHORT RISEx / LONG Extended
- LONG RISEx / SHORT Nado
- SHORT RISEx / LONG Nado
- LONG RISEx / SHORT Lighter
- SHORT RISEx / LONG Lighter

`route_liquidity = min(risex_24h_quote_volume_usd, hedge_24h_quote_volume_usd)`. Liquidity is a measurement dimension and deterministic ranking input, not a universe-selection filter. Persist every evaluated venue-asset direction; ranking never truncates the evidence set. Telegram delivers only the first 10 rows of that same authoritative deterministic ranking. Convert official base volume using that venue's official current price; if unreliable, retain the evaluation with a precise fail-closed blocker and exclude it from entry eligibility.

A route also needs valid BBO, canonical grids and minimums, a fresh funding quote and next funding timestamp, known eligibility, and exact-quantity taker depth in both directions on both venues.

Sort routes deterministically by:

1. PlannedMakerNetPnLUSD descending
2. route_liquidity descending
3. target_cycle_start ascending
4. canonical_asset ascending
5. hedge_venue ascending
6. route_direction ascending

Evaluate simultaneous activations at one logical timestamp and choose one top route. Once an entry maker order exists, lock the route. Falling out of the current eligible catalog forbids new entry but does not exit an existing position. Route switching does not exist.

## 6. Market data health

WebSocket supplies available BBO, book deltas, maker-leg public trades, funding, connection state, sequence, and heartbeat/ping. REST supplies markets/metadata, volume, missing stream data, and official applied-funding history when available. Initial and recovery book snapshots may use REST only where the venue snapshot preserves the sequence contract. Lighter is the fixed exception: its REST book has no nonce, so only a fresh per-market WebSocket subscription snapshot may establish or recover a sequence-valid book. Lighter is always the virtual taker leg, so its public trade channel is neither subscribed nor required for route readiness; RISEx maker-trade evidence remains mandatory.

Lighter public `market_stats` funding values are percentage-number units: divide `current_funding_rate` and `funding_rate` by `100` exactly once at the venue boundary. Funding cash per canonical base uses the validated `mark_price`, not `index_price`. `current_funding_rate` is only the next hourly prediction; when `funding_timestamp` advances contiguously to the exact registered lifecycle settlement, apply that frame's `funding_rate` once and retain `current_funding_rate` for the next boundary. Stale, duplicate, gapped, mismatched-session, or unregistered boundaries do not create an applied settlement and fail closed without replay.

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

Before maker fill, `assumed_position_opened_at = current_evaluation_timestamp`: estimate full-position funding if opened now. Never use order creation as an open position, forecast a T−5 fill, or freeze the T−180 estimate. After full two-leg entry, `position_opened_at = route_taker_fill_at` and funding is recomputed from actual open time.

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

`entry_activation_at = T - 180 seconds`; `entry_fill_cutoff_at = T - 5 seconds`. Create one exact activation event once the cycle is known and perform focused evaluation exactly then, independent of a periodic tick. Starting with `5 < seconds_to_T < 180` permits immediate focused evaluation. Recalculate every 10 seconds thereafter.

A trade participates only if `exchange_timestamp < cutoff`. A trade exactly at or after cutoff never counts, regardless of receipt time or coroutine ordering. Persist exchange, receipt, and raw timestamps. At T−5 cancel an unfilled order and skip the opportunity.

## 10. Price and quantity grids

`spread = best_ask - best_bid`; `spread_ticks = spread / tick_size`. Snapshot is invalid if bid >= ask, BBO is off tick, or spread is not a positive integer tick count.

Maker entry/normal exit price:

- one tick: BUY at best bid, SELL at best ask;
- two or more: BUY at bid + tick, SELL at ask − tick.

Target raw canonical quantity is `500 / planned_route_maker_canonical_price`. Compute each canonical step as raw step × multiplier, derive a common canonical step with integer-scaled LCM, and floor the target to it. Adapters convert canonical quantity back to raw quantity. Enforce both venues' minimum quantity and notional.

At maker-entry activation, persist that exact canonical quantity as the attempt's `locked_quantity`. The quantity is immutable for the entire attempt. Later 10-second Scanner plans may calculate a different target quantity, but target-quantity drift alone does not invalidate or replace the active attempt. Every subsequent executability and economics check uses `locked_quantity`, including both venues' grids and minimum quantity/notional, exact-quantity route-taker depth/VWAP, fees, funding, planned entry/exit execution, and planned maker net PnL. Clips, resizing, partial-position entry, and transfer of maker evidence between quantities remain prohibited.

Exact taker VWAP walks asks for BUY and bids for SELL for exactly canonical quantity. Insufficient depth is not executable. Never use last/mark as execution, fixed slippage, spread reserve, hidden buffer, percentage improvement, progressive entry chasing, or fill probability.

## 11. Fees and planned entry economics

`fill_notional_usd = abs(q × canonical_fill_price)` and `fee_usd = fee_base_notional_usd × fee_rate`. Normally fee base equals fill notional. Nado taker fee base is max(fill notional, venue minimum fee notional). Persist rate, source, and observed/configured time.

There are exactly two paper execution profiles. Extended and Nado routes retain hedge-maker entry/exit plus RISEx-taker entry/exit. Lighter routes use RISEx-maker entry/exit plus Lighter-taker entry/exit. This is a bounded route choice, not a generic order-management or execution framework.

For LONG, price PnL is `q × (exit - entry)`; for SHORT, `q × (entry - exit)`. `PlannedExecutionPnLUSD` is both legs' planned price PnL. `PlannedMakerNetPnLUSD = ExpectedTargetCycleFundingUSD + PlannedExecutionPnLUSD - all planned entry/exit fees`.

Expected basis convergence and points value are zero. Entry requires PlannedMakerNetPnLUSD >= 0. Scanner separately shows funding PnL, entry/exit execution PnL, fees, planned maker net PnL, and executable unwind net PnL. Spread appears only through actual quoted prices. NO TRADE is valid.

## 12. Paper maker entry

At activation create a route-maker LIMIT POST_ONLY only for the first ranked route, non-negative plan, no position/order, fresh data, and executable route-taker exact-q entry VWAP. The route maker is the hedge venue for Extended/Nado and RISEx for Lighter. Lock the route.

Cancel only when its locked-quantity planned PnL turns negative or unknown, data is stale, the locked route/direction/target funding cycle changes, `locked_quantity` becomes invalid or non-executable under current venue grids/minimums or exact-q taker depth, the maker quote is no longer valid post-only, or cutoff arrives. A newly optimized target quantity differing from `locked_quantity` is not itself a cancellation reason. Every 10 seconds recalculate the maker quote and all economics at `locked_quantity`; price change cancels/replaces with a new version and resets old cumulative volume without changing quantity or transferring maker evidence.

Trade evidence contains key, exchange/receipt time, canonical quantity/price, aggressor side, and `is_orderbook_match` true/false/unknown. Only true matches count. With an official realtime trade ID, key is venue + market + ID. Otherwise key adds connection session, exchange time, product, price, quantity, aggressor, and event ordinal.

BUY maker requires SELL aggressor and `trade_price <= buy_limit - tick`; SELL maker requires BUY aggressor and `trade_price >= sell_limit + tick`. Also require same venue/market/current version, pre-cutoff time, new key, and tick alignment. Accumulate eligible canonical quantity. Full fill occurs at cumulative quantity >= order quantity, at paper limit price. Do not model partial-position lifecycle, queue, or hidden liquidity.

## 13. Global paper taker assumption

Every virtual taker signal fills immediately and fully at fresh exact-q VWAP: the selected route taker on entry and normal exit, both Hard Basis legs, and executable-unwind calculations. Do not model latency, including Lighter's documented Standard-account transaction delay, rejection, retry, failure, or partial taker. Book must be fresh, sequence-valid, and deep enough.

While entry maker is active, lost route-taker entry depth cancels it as non-executable. While exit maker is active, lost route-taker reverse depth cancels the version as `PAPER_EXIT_ORDER_CANCELLED_UNWIND_UNAVAILABLE`, preserves position/state, and recreates in the same mode after recovery.

Reports set true: taker failure/latency not simulated, partial fills not simulated, queue position not simulated, cancel/replace latency not simulated, stablecoin depeg not simulated, live margin/liquidation not simulated.

## 14. Entry completion and PnL

Persist maker fill exchange timestamp, maker fill receipt time, and local route-taker processing time. Position open time is route-taker fill time.

Immediately after full entry: persist actual prices/fees; recompute funding from actual open; quote current maker exit; compute PlannedMakerExitNetPnLUSD, PlannedHoldToTargetNetPnLUSD, and ExecutableUnwindNetPnLUSD; decide HOLD/EXIT without waiting. HOLD only when planned hold is strictly greater than planned maker exit; otherwise EXITING_NORMAL.

Planned exit price PnL uses actual entries and planned exits with normal long/short signs. Planned maker exit net equals recognized funding + both price PnLs − actual entry fees − planned exit fees. Executable unwind substitutes taker unwind PnL/fees.

Closed pair PnL is actual long PnL + actual short PnL. Actual paper fees are all actual entry and exit fees. Simulated closed net is simulated recognized funding + pair PnL − fees. Applied-rate closed net substitutes applied funding only when all events are applied/skipped; otherwise UNKNOWN. Always derive from authoritative evidence.

## 15. HOLD, exit, and next cycle

Every 10 seconds compute executable pair PnL, informational/executable basis, lifecycle funding, remaining target funding, maker exit net, executable unwind net, fees, and Hard Basis. Recognize eligible funding through actual close, including while EXITING.

Before target-cycle resolution, planned hold-to-target equals maker exit net + remaining target funding. Strict positive improvement means HOLDING; otherwise or unknown funding means EXITING_NORMAL. After first resolved cycle, build only the latest next cycle and apply the same comparison using expected next-cycle funding. Future basis EV is zero. EXITING never returns to HOLDING.

Normal exit maker placement follows normal maker rules. Exactly 10 seconds after normal exit begins without fill, transition to EXITING_AGGRESSIVE. Aggressive one-tick pricing is unchanged; at two+ ticks BUY at ask − tick and SELL at bid + tick. Aggressive is sticky. Reprice every 10 seconds.

Exit fill uses entry maker's aggressor, one-tick trade-through, cumulative-volume, and dedup rules. There is no timed/pre-funding/waiting taker fallback. Track exit wait, funding received while exiting, and pair PnL change while exiting. After the route maker closes, execute the reverse route taker then FLAT.

## 16. Basis and hard exit

`InformationalMidBasis = short_mid / long_mid - 1`. `EntryExecutableBasis = short_entry_fill / long_entry_fill - 1`. `CurrentExecutableBasis = short_close_buy_vwap / long_close_sell_vwap - 1`. Adverse expansion is current executable minus entry executable.

Recheck event-driven after relevant book updates on both legs. Threshold is 4% for BTC/ETH, 6% for other eligible assets. On trigger, skip maker waiting and taker-close both legs at exact-q VWAP with taker fees, reason HARD_BASIS, then FLAT.

If either unwind quote is unavailable, basis and unwind PnL are UNKNOWN; record `UNWIND_QUOTE_UNAVAILABLE`, mark DEGRADED/invalid for primary metrics, and keep position open. Recalculate after recovery.

## 17. State, restart, and SQLite

Only states: `FLAT`, `ENTRY_MAKER_OPEN`, `HOLDING`, `EXITING_NORMAL`, `EXITING_AGGRESSIVE`. Do not add states without a product decision.

Identifiers include attempt, order, order version, position, trade event key, and funding settlement key. SQLite unique constraints enforce idempotency. Process a trade once, apply fill only to current version, maintain one authoritative settlement, and atomically persist state transition plus evidence/values. Do not build event sourcing.

Restart from entry maker cancels virtual order as `PAPER_ORDER_CANCELLED_PROCESS_RESTART`, reconstructs no fills, and returns FLAT. Restart from HOLDING restores the position, treats offline time as a degrading gap, and reconciles funding from official history. Restart from EXITING restores position, cancels old exit version without fill reconstruction, and creates a fresh version after snapshot; mode/timing remain sticky.

Persist scanner snapshots, funding quotes/cycles/settlements, orders/versions/cumulative trade-through/processed keys, fills/VWAP evidence, position samples, gaps, completed trades, and runtime state. Do not persist every raw stream message, every full raw book, or unbounded raw ticks.

## 18. Commands and reporting

Commands are `scan-once`, `paper-run`, and `report`. Report opportunities/eligible count, orders/fills/fill rate/active time, normal/aggressive exits, applied partial/estimated/unresolved funding, planned execution PnL, actual pair PnL, fees, simulated and applied-rate closed net (or UNKNOWN), both win rates, hold/exit duration, cycles, drawdown, virtual RISEx volume, PnL per $1,000 RISEx volume, planned-vs-actual error, complete/degraded trades, open position, and all assumption flags.

For public-shadow route observations, additionally report liquidity-conditioned evidence using fixed route-liquidity buckets `< $250k`, `$250k–< $1m`, `$1m–< $10m`, and `>= $10m`, plus `UNKNOWN` when authoritative volume is unavailable. For each bucket report route-observation count, distinct venue-asset directions, eligible/opportunity count and frequency, consecutive opportunity duration, and planned versus executable-unwind net PnL with funding, fee, spread/slippage, freshness, and blocker components. Never infer that liquidity causes profitability; this is descriptive dependence evidence only.

RISEx volume is absolute entry notional + absolute exit notional. Primary metrics include only closed, COMPLETE trades with all required funding resolved. Applied metrics additionally require complete applied/skipped settlements. Never force-close an open position at run end.

## 19. Non-goals

No mainnet/real-money trading, route switching, clips, partial-position lifecycle, queue/taker-failure simulation, dynamic sizing, leverage, basis-convergence forecasting, ML, points valuation, stablecoin depeg, inbound Telegram commands, dashboards, or generic infrastructure. Separately governed testnet work never changes paper behavior.

## 20. Outbound Telegram notifications

Telegram is delivery-only, disabled by default, and never owns data, economics, lifecycle decisions, or cadence. It accepts only authoritative runtime events/results and exposes no inbound commands, polling, scheduler, or additional formulas.

Notify bounded authoritative lifecycle/data events and every persisted `FULL` scan digest, never `INITIAL`, `FOCUSED`, or `RECOVERY`. Each FULL digest contains at most the first 10 rows of the persisted deterministic ranking; this presentation limit never truncates scanning, persistence, reporting, or opportunity measurement. Deduplicate by stable event/route/cycle/displayed-value identity and split messages without changing or duplicating delivered route rows.

Persist raw public socket disconnect/reconnect and resync lifecycle evidence, but do not label or deliver a self-recovered transient socket episode as critical data loss. A critical Telegram data-loss alert requires an unresolved/persistent semantic failure such as confirmation staleness beyond the existing health threshold, failed snapshot recovery, blocked authoritative scan, or fatal runtime state. Deliver recovery only when it closes a previously delivered critical episode; routine reconnect churn remains reportable operational evidence without outbound alert spam.

Delivery uses a bounded non-blocking queue, finite timeout, and finite attempts; outage or saturation never delays runtime. An ambiguous `sendMessage` timeout is not retried. Credentials come only from explicit environment configuration and never enter persistence, logs, messages, arguments, or process titles.

Runtime has no elapsed-time stop. It distinguishes intentional signals from fatal failures, preserves open positions, persists bounded safe-stop evidence, and cancels/awaits background tasks without fabricating transport events.

## 21. Isolated venue testnet programs

RISEx, Extended, Nado, and future Lighter testnet modules are opt-in and isolated from normal Farmer startup, Scanner, paper runtime/economics, Telegram, and paper persistence. They use only official environments and separate test-only accounts, never mainnet or real funds. The historical three-venue program keeps every potential notional `<= USD 500`; the future Lighter slice uses the smallest venue-executable test quantity and receives its own bounded gate after public PAPER acceptance.

Authenticated read-only readiness requires correct environment/account identity, secret isolation, bounded transport, critical response semantics, and authoritative account state. Proven read-only failures may be retried as fresh bounded observations; operational run IDs are durable runtime data rather than source-code milestones.

Before every order/cancel/close dispatch, persist a unique venue-correct write identity and bounded intent. Never blindly replay an ambiguous write. Reconcile exact order/fill/position identity from authoritative venue state, stop automated writes on contradiction or unrelated account state, and permit manual testnet recovery as a failure terminal.

First-lifecycle success requires correct authenticated pre-state, one minimum bounded real order path, authoritative outcome reconciliation, any necessary cancel/close, and final authoritative zero relevant open orders plus exact flatness. Venue-specific authentication, signing, wire, nonce, and lifecycle semantics remain separate and must follow official or observed evidence.

For the first bounded RISEx testnet lifecycle, current testnet liquidity may have an arbitrarily wide positive spread. A maximum-spread threshold is not a write-safety gate there: use only a fresh authoritative non-crossed BBO with positive tick-aligned prices and sufficient step-aligned depth for the exact price-bounded order. This exception is testnet-only and does not change paper execution economics or authorize any mainnet behavior.

### Current first-lifecycle write contracts

- **RISEx:** one minimum-size price-bounded `MARKET+IOC` opening; close from fresh authoritative exact position size through the accepted reduce-only price-bounded lifecycle, with at most three automatic close attempts; one exact cancel only for an exact known experiment order, never account-wide cancellation. Every write has a new durable identity and ambiguous writes are never blindly replayed; success is zero relevant open orders plus exact flatness. The first proof may use one Chief-selected fixed unlocked testnet product with materially better observed executable depth; this does not authorize dynamic product selection or change paper/mainnet behavior.
- **Extended:** persist an explicit unique nonce, external/settlement identity, and expiry before dispatch; entry and close use the accepted price-bounded short-expiry `IOC` contract, with reduce-only close for fresh authoritative exact position size. Never blindly replay ambiguous place/cancel/close. Normally success requires reconciled lifecycle identities and a fresh agreeing zero-order/exact-flat stream/REST barrier. For testnet only, when bounded credential-free probes prove the entire official testnet WebSocket ingress unavailable before authentication while authenticated REST identity and account reads remain valid, the stream half may be replaced by two fresh agreeing strict REST rounds. Each round must bind the exact durable external ID and any returned Extended order ID through the exact-order/history endpoints, bind matching trades by both identities, reject unrelated orders/trades/positions, use bounded complete pagination, and prove zero open orders plus exact flatness after every signed expiry. Current official empty-list responses omit pagination entirely; absence or null is accepted only when the exact list is empty, while every nonempty list still requires bounded complete pagination metadata. This fallback never proves mainnet stream readiness and is removed if REST cannot resolve an exact write outcome.
- **Nado:** one minimum-size price-bounded post-only entry with unique durable signed identity; preserve accepted `recv_time`/nonce fencing, no digest replay, and exact regular-order cancel-all semantics. State-based close uses fresh authoritative position and the accepted reduce-only aggressive `IOC` contract; at most three automatic close attempts before halt/manual recovery. Success requires reconciled identities, zero regular/trigger orders, and authoritative exact flatness.

After all three first lifecycles pass, stop infrastructure expansion, perform one bounded commonality review, then use the normal paper product for a separate mainnet-public shadow-measurement phase. This phase may consume real unauthenticated mainnet REST/WebSocket market data but has no credentials, signing, order preparation, dispatch, private account access, collateral, or real funds. Authenticated mainnet hardening and every mainnet write remain later explicit product decisions.
