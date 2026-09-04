# RISEx Spread Shadow — System Specification

SYSTEM_SPEC_VERSION = 3.4
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

### 0.7 Prospective corrected discovery gate `DG-002B`

This gate was frozen on `2026-09-03` only after `DG-002A` passed and before opening a new economic sample. It reuses the original `DG-001` economics and verdict rules unchanged; no threshold or bound is selected from the stability run's economic observations.

- Run unchanged accepted source `b4f2822327fc0f7b50a02d7aabfc2d6e61b453a4` once into a fresh owner-only observational store, with exact public `BTC/ETH/SOL`, both directions, `$100/$250/$500`, `1/2/3/5 bps`, `0/300/500/1000 ms`, `25 s` freshness, the exact fees/provenance in section 0.5, a `60 second` duration, `250,000`-record cap, and first `50` strict episodes by `record_index`.
- The full source/surface, admission, terminal, fatal/integrity, planned-stop, permissions, deterministic-report, completeness, fillability, depth, edge-materiality, concentration, and seven-verdict precedence rules in sections 0.5 and 0.6 apply. A clean run with zero strict fills is complete data, not corruption; it is interpreted only through the unchanged frozen fillability and verdict rules.
- Emit exactly one of the seven section-0.5 verdicts with the complete diagnostic report and exact evidence identity. A measurement-path failure is not mission success; objective public-data limitations may support `DATA_INSUFFICIENT` only when the accepted path remains proven correct.
- No automatic retry, threshold change, strategy expansion, `SS-002`, or `SS-003` follows from the observation. `SS-002` can only be proposed after a recorded `ENTRY_EDGE_CANDIDATE`; every other verdict ends the current Entry Viability Stage without implementation expansion.

### 0.8 Fillability-bound research contract

The post-DG-002B mission is to resolve whether profitable hedge-anchored RISEx maker quotes are genuinely unreachable or whether zero strict fills result from the conservative public-trade lower bound. `DG-002B` and its `FILLABILITY_INSUFFICIENT_EVIDENCE` verdict remain immutable historical evidence.

- The accepted strict would-fill definition is unchanged and remains the conservative lower bound: correct immutable quote identity and local ordering, correct market/direction/aggressor, at least one full tick through the maker price, cumulative non-duplicated public quantity at or beyond that strict price reaching exact `q`, unexpired/unreplaced quote, and no overlapping invalid gap.
- One optimistic upper bound may classify a quote only when the quote version existed before the evidence; market, direction, aggressor, session, recovery, and local ordering agree; the trade is exactly at or through the maker price; cumulative non-duplicated public quantity at-or-through reaches exact `q`; the quote remains active and unexpired; and no invalid gap overlaps. It assumes zero queue ahead, no hidden liquidity ahead, and allocation of all eligible public volume. It must be labelled `OPTIMISTIC_UPPER_BOUND`, never realistic or expected fillability.
- The intended public research bracket is `StrictWouldFill <= RealMakerFillability <= OptimisticWouldFill`, with the explicit caveat that public data cannot guarantee every venue-microstructure detail. Strict and optimistic episode identity, quantity, time-to-fill, filled notional, and all `0/300/500/1000 ms` no-lookahead hedge captures remain separately attributable; the same trade quantity is never duplicated within one model/version.
- An eligible RISEx trade is one accepted, deduplicated public trade for which at least one correct-side relevant quote version is active immediately before the trade by local receipt ordering. Stop counters count each eligible trade event once, not once per policy.
- Per-policy reporting must include snapshot quoteable time, actual snapshot edge, quote distance from the RISEx post-only BBO bound in ticks/bps; eligible/touch/at-or-through/strict counts; cumulative qualifying volume, fill counts/notional/time-to-fill for both bounds; model-separated full/partial/missing hedge rates and edge/markout curves; and market/direction/size/margin concentration. Named missing/stale/session/gap/depth outcomes are not zero edge.
- The existing append-only evidence representation remains unchanged unless implementation proves it cannot preserve the contract. Long-run reports must use bounded streaming or multi-pass processing rather than retaining the full evidence file in memory. No generic storage, compression, queue, execution, or lifecycle framework is authorized.
- The mission ends only with strong optimistic-unreachability evidence, a materially separated strict/optimistic public bracket requiring a different calibration decision, or conservative fill episodes with a prospective delayed-edge verdict. `SS-002` and `SS-003` remain closed throughout.

### 0.9 Frozen fillability-bounds discovery gate `DG-003`

This gate was frozen on `2026-09-03` only after independent acceptance of SS-001D and before opening any new economic sample. It applies to exactly one public unauthenticated run on source `10cc7be7b58c536fc8edf65309b13e9a9d8d819b`.

- Universe and economics: exact public RISEx/Lighter `BTC`, `ETH`, and `SOL`; both fixed directions; `$100/$250/$500`; `1/2/3/5 bps`; `0/300/500/1000 ms`; `25,000,000,000 ns` freshness; RISEx maker fee `0.00005` labelled `CONFIGURED_RISEX_RESEARCH_INPUT`; Lighter Standard taker fee `0` with the section-0.5 provenance; points `$0`; no funding or future-exit value. The quote grid, maker pricing, strict definition, fees, and venues are unchanged.
- Bounds: `STRICT_LOWER_BOUND` retains the one-full-tick-through exact-`q` definition. `OPTIMISTIC_UPPER_BOUND` uses the section-0.8 at-or-through exact-`q` definition and must always be reported as the zero-queue/no-hidden-liquidity upper bound. One accepted deduplicated RISEx trade increments the eligible counter once even when it is relevant to multiple policies.
- Stop rule: freeze the economic sample at the first of `50` strict episodes, `500` unique eligible RISEx trades, `1,200 seconds` from sample start, or any integrity/fatal condition. After a non-fatal sample stop, accept no later RISEx economics or episodes; retain only the bounded Lighter tail required to finish already-pending horizons, for no longer than the largest configured horizon plus the accepted scheduling allowance. Emit exactly one `SAMPLE_STOP` and exactly one terminal marker.
- Storage and environment: one fresh owner-only observational store; maximum `2,500,000` total records and `12 GiB` for the evidence file, with a reserved terminal failure marker. A cap breach is an integrity failure, never a truncated economic verdict. Before launch, require at least `24 GiB` free on the target filesystem, exact source identity, clean accepted surfaces, and no other Spread observer. The append-only JSONL representation remains unchanged; final record and byte counts are reported.
- Completeness: exact three-market admission; source match; exactly one clean `RUN_STOP`; no `RUN_FAILED`, fatal reason, queue/history/store failure, unexpected disconnect, unclassified schema failure, or secret/private/write surface; deterministic repeated offline reports; owner-only permissions; and all four model-scoped horizons for every fill episode. Named partial/depth/missing/stale/session/gap outcomes remain explicit and are never imputed. A non-terminal gap invalidates only overlapping matching evidence. A wall-clock stop below `500` eligible trades and below material strict fillability is insufficient for an unfillability or public-bracket conclusion.
- Fillability thresholds: `approximately zero` is `0` or `1` episodes across the run. `Materially positive` is at least `10` episodes for one exact market/direction/size/margin policy spanning at least `5` distinct detection timestamps. Counts `2..9`, or concentration in fewer than five timestamps, are sparse rather than materially positive. The full per-policy and concentration report in section 0.8 is mandatory; no aggregate-only verdict is valid.
- Edge thresholds: for one full hedge, material positive edge is at least `max($0.01, 1 bp of hypothetical RISEx filled notional)`. A candidate policy requires material strict fillability, at least `95%` full-hedge rate at every horizon, at least `90%` positive-edge share and materially positive `p05` at `300 ms`, and a strictly positive median at `500 ms`. The complete `0/300/500/1000 ms` curve is reported; `500 ms` remains diagnostic.

Verdicts are evaluated in this fixed precedence and exactly one is recorded:

1. `DATA_INSUFFICIENT` for any source, universe, stop, cap, terminal, integrity, completeness, or deterministic-report failure; also for a wall-clock stop below both `500` eligible trades and material strict fillability.
2. `NO_SNAPSHOT_EDGE` when every policy has quoteable-time share below `1%`, or material complete strict fills exist but no policy has materially positive `0 ms` edge.
3. `LIGHTER_DEPTH_UNSUITABLE` when a materially strict-fillable policy exists but at least `50%` of its valid `0 ms` outcomes are `HEDGE_PARTIAL` or `HEDGE_DEPTH_UNAVAILABLE`.
4. `PROFITABLE_QUOTES_UNFILLABLE` only after `500` eligible trades with materially present snapshot opportunities, complete evidence, strict count at most `1`, and optimistic count at most `1`.
5. `FILLABILITY_INSUFFICIENT_EVIDENCE` after `500` eligible trades when strict evidence is not materially positive but optimistic evidence exceeds the approximately-zero bound. Sparse or concentrated optimistic evidence is reported separately from materially positive optimistic evidence. This ends further public simulation and presents the smallest controlled fill-calibration choices to the owner.
6. `LATENCY_DESTROYS_EDGE` when material complete strict fills have materially positive `0 ms` edge but no policy satisfies the frozen delayed-edge thresholds.
7. `ENTRY_EDGE_CANDIDATE` when at least one exact policy satisfies every material strict-fillability, completeness, hedge-depth, `300 ms`, and `500 ms` condition.

Any terminal result is evidence, not permission to change strategy or trade. `SS-002` and `SS-003` remain closed. Only `ENTRY_EDGE_CANDIDATE` may support a later separate proposal for SS-002; it does not open SS-002 automatically.

### 0.10 Post-DG-003 measurement-throughput correction

The one frozen DG-003 run is immutable `DATA_INSUFFICIENT / MEASUREMENT_THROUGHPUT_FAILURE`. Its source, stop counters, fills, and horizons remain diagnostic only: the run recorded queue-overflow gaps and ended with `INGRESS_DRAIN_TIMEOUT`, so it cannot issue any fillability or entry-edge verdict and must not be reinterpreted after a correction.

- The observed failure requires the smallest lossless correction to public evidence throughput and terminal draining. Prefer activating the already-configured bounded store batching/synchronization contract when that is sufficient. A compact quote representation referencing already-persisted immutable book revisions is permitted only if deterministic stress evidence proves batching alone cannot keep up with the observed bounded surface. Either path must preserve exact quote economics, active-version timing, trade interaction, both fill bounds, exact-`q` horizons, gap/session/recovery identity, append ordering, caps, terminal durability, and deterministic legacy/new replay.
- Increasing queue capacity or shutdown timeout alone is not acceptance because it does not remove sustained backpressure. No compression platform, database, message bus, generic persistence framework, strategy/economic change, private access, or write path is authorized.
- Acceptance requires an adverse high-rate fixture at or above the observed three-market event/quote workload with zero queue overflow, bounded memory, deterministic evidence/report output, and a clean stop plus horizon drain inside the accepted shutdown bound. Failure injection must prove ordered record indices, bounded unsynced exposure, final sync, cap reserve, and exactly one terminal marker.
- A replacement prospective economic gate may be frozen only after this correction is independently accepted. It must use a fresh store and exact accepted source and must not reuse DG-003 economic output to tune quote economics or fill definitions. `SS-002` and `SS-003` remain closed.

### 0.11 Frozen replacement fillability-bounds gate `DG-004`

`DG-004 — Fillability Bounds Recovery Discovery` is frozen before its sample on exact accepted measurement source `cd741e2a46e874f1e77feebac2aba5c80a96455d`. It is one fresh public-only observational run in a fresh owner-only store. No DG-003 economic count or edge value may tune its parameters.

- Universe and economics remain exact: `BTC/ETH/SOL`; both directions; target notionals `$100/$250/$500`; target margins `1/2/3/5 bps`; horizons `0/300/500/1000 ms`; configured RISEx Tier-3 maker fee and Lighter Standard taker fee with the already accepted provenance; `25 s` freshness; unchanged quote construction, strict lower bound, optimistic at-or-through upper bound, eligibility, exact-q accumulation, no-lookahead capture, and concentration dimensions.
- The economic sample stops on the first of: `50` aggregate strict would-fill episodes; `500` unique eligible RISEx public trades while relevant quotes are active; `1200 s` wall clock; or any integrity/fatal condition. The already accepted frozen-sample/Lighter-horizon tail must complete without later RISEx economics. No manual early stop, extension, retry, or parameter change is allowed.
- Storage remains append-only JSONL with the accepted lossless batching path, maximum `2,500,000` records and `12 GiB`, at least `24 GiB` free before start, deterministic offline replay, owner-only permissions, exact terminal uniqueness, and all evidence needed by section 0.8. Crossing a cap is `DATA_INSUFFICIENT`, never a partial economic verdict.
- Completeness, thresholds, reporting dimensions, and seven-verdict precedence are exactly section 0.9. In particular, `PROFITABLE_QUOTES_UNFILLABLE` and the public-bracket form of `FILLABILITY_INSUFFICIENT_EVIDENCE` require `500` eligible trades; a valid strict-stop sample instead reports the complete per-policy delayed-edge evidence and may reach `LATENCY_DESTROYS_EDGE` or `ENTRY_EDGE_CANDIDATE` only through the frozen material-policy thresholds. Sparse/concentrated strict evidence cannot be promoted to an entry candidate.
- Any terminal result is evidence, not trading permission. `SS-002` and `SS-003` remain closed. Only a separately recorded `ENTRY_EDGE_CANDIDATE` may support a later proposal for SS-002; it does not open it automatically.

### 0.12 Post-DG-004 terminal-integrity correction

The one frozen DG-004 run is immutable `DATA_INSUFFICIENT / TERMINAL_SERIALIZATION_FAILURE`. It stopped on the first fatal condition, `LIGHTER_PUBLIC_FRAME_INVALID`, but timed out draining ingress and did not produce a serially valid terminal evidence stream. Its `49` eligible trades, `24` strict episodes, `57` optimistic episodes, and `324` horizons are diagnostic only and may not support any fillability or delayed-edge verdict.

- The retained owner-only evidence file has `166,291` physical records and `727,561,746` bytes, SHA-256 `77f795be487224634e806e3b7c546de8c4378b2c98334f21f59c623ba3ecebfa`. A cancelled asynchronous drain left an in-flight thread-backed append able to overlap the direct `RUN_FAILED` append: record indices `166287` through `166289` were duplicated or physically out of order, records followed the terminal marker, and no `DATA_GAP` row preserved the protocol failure. This is an evidence-integrity defect, not venue economics.
- `SS-001F — Terminal Serialization and Protocol-Failure Evidence` is the only authorized correction. It must serialize every store append, including terminal markers, against any in-flight background append; guarantee one terminal marker is physically last with unique strictly increasing contiguous record indices; and close only after the append worker is known quiescent. Cancellation, timeout, or ambiguous file-worker completion must fail closed without concurrent direct use of the store.
- Offline reporting must independently detect and explicitly reject duplicate, missing, decreasing, or otherwise non-contiguous record indices, more than one terminal marker, any record after a terminal marker, a missing terminal marker, or a terminal marker that is not physically last. A deterministic regression must reproduce the observed cancellation-during-threaded-append race and fail on the pre-correction implementation.
- A fatal public-protocol observation must retain bounded sanitized evidence sufficient to distinguish venue, WebSocket frame kind/category, and a non-secret bounded length/hash or equivalent classification without retaining raw payloads. Its `DATA_GAP`/fatal evidence must survive a full or closing ingress path and remain ordered before the terminal marker. This slice does not authorize accepting binary or otherwise unsupported frames, changing the official public protocol contract, retrying the economic run, or weakening fatal handling.
- Acceptance requires focused cancellation/race, full-ingress protocol-failure, terminal-order, store-cap-reserve, and corrupt-replay tests; deterministic replay of immutable DG-002B, DG-003, and DG-004 evidence; one clean Python 3.11 full suite; and clean dependency, compile, import, private/write-surface, diff, scope, and Git checks. Store representation, economics, fill bounds, eligibility, stop rules, horizons, fees, markets, queue capacity, and shutdown timeout remain unchanged.
- No replacement economic gate may be frozen until this correction is independently accepted and the unsupported Lighter frame class is resolved by official or sanitized observed public evidence. `SS-002` and `SS-003` remain closed. No private, authenticated, signing, or write activity is authorized.

### 0.13 Accepted terminal correction and frozen `DG-005`

`SS-001F` is independently accepted on exact source `cdbc95c67adaf9df120c3ff07bb990dc37542ae3`. Every observer-owned append and terminal marker now crosses one serialized boundary that waits for any thread-backed file operation to become quiescent; a sole terminal marker is physically last on a successful terminal path. Offline reporting rejects invalid, duplicate, decreasing, missing, or non-contiguous indices, missing/multiple/non-last terminals, and records after a terminal. Public protocol failures retain bounded sanitized venue, frame kind/category, length, and SHA-256 evidence without raw payloads, including when ingress is full or closing.

Current official Lighter WebSocket documentation defines JSON subscription and response messages, and the current official Python SDK parses received asynchronous messages as JSON and uses JSON application-level ping/pong. A separate bounded `600 s` public transport diagnostic observed `34,309` text frames, four ordinary WebSocket CLOSE frames over five connections, and no binary, continuation, or otherwise unsupported frame. The exact historical DG-004 frame kind is unrecoverable because the old evidence omitted it; that omission is not permission to accept an unknown frame. Ordinary CLOSE remains supported, all data messages remain JSON text, and any future unsupported frame remains fatal with the new sanitized classification. No protocol-acceptance change is authorized.

`DG-005 — Fillability Bounds Integrity Recovery Discovery` is frozen before its sample on exact accepted measurement source `cdbc95c67adaf9df120c3ff07bb990dc37542ae3`. It is one fresh public-only observational run in a fresh owner-only store.

- Universe, economics, directions, sizes, margins, horizons, fees/provenance, `25 s` freshness, quote construction, strict lower bound, optimistic at-or-through upper bound, eligibility, exact-q accumulation, no-lookahead capture, and concentration dimensions are exactly sections 0.8–0.11.
- Stop on the first of `50` aggregate strict episodes, `500` unique eligible RISEx trades with relevant active quotes, `1,200 s`, or any integrity/fatal condition. After sample stop retain only the bounded Lighter horizon tail already authorized. There is no manual early stop, extension, retry, or parameter change.
- Enforce `2,500,000` records, `12 GiB`, at least `24 GiB` free before launch, exact source/universe admission, one physically-last terminal, contiguous unique indices, owner-only permissions, no private/write surface, and two byte-identical canonical reports. Any unsupported public frame remains terminal and its sanitized evidence must precede `RUN_FAILED`.
- Completeness, materiality thresholds, required per-policy reporting, and seven-verdict precedence are exactly section 0.9. `PROFITABLE_QUOTES_UNFILLABLE` and the public-bracket form of `FILLABILITY_INSUFFICIENT_EVIDENCE` still require `500` eligible trades. A strict-stop sample may reach `LATENCY_DESTROYS_EDGE` or `ENTRY_EDGE_CANDIDATE` only through the frozen per-policy thresholds.
- This gate may resolve the active mission only as case A, B, or C in section 0.8. A measurement failure is not mission completion. `SS-002` and `SS-003` remain closed; no private, authenticated, signing, or write activity follows any result.

### 0.14 Post-DG-005 lossless book-evidence correction

The one frozen DG-005 run is immutable `DATA_INSUFFICIENT / MEASUREMENT_THROUGHPUT_FAILURE`. Its terminal stream is physically valid and clean, but its `1,117` gaps include `921` Lighter and `166` RISEx `QUEUE_OVERFLOW` gaps plus `27` unexpected Lighter disconnect gaps. It stopped prospectively on `WALL_CLOCK_LIMIT` at `40` unique eligible trades with diagnostic `17` strict and `22` optimistic episodes. All `288` policy/horizon groups per model are degraded; no fillability or delayed-edge verdict may be inferred.

- The owner-only evidence file has `130,877` records and `4,986,247,063` bytes, SHA-256 `5de295aaf7b8b7f63c71a518e3da3e718c89b154faa1efc4143735c61a8c3611`; its repeated canonical report is byte-identical at SHA-256 `a9721dc76ad96286bed1e97af7bd7db40168983844e96cab2c1e1e15ba54555f`. Indices are exactly contiguous `0..130876` and the sole final marker is clean `RUN_STOP`.
- Full `BOOK` rows account for `4,831,137,744` bytes, or `96.89%` of the evidence. Lighter contributed `64,494` BOOK rows with median `74,410` bytes; the median normalized state repeated `1,081` bid and `751` ask levels per revision. This prospectively proves that the accepted batching path alone does not safely sustain the deep-book public stream and that a bounded lossless representation correction is necessary.
- `SS-001G — Lossless Book-Delta Evidence` is the only authorized correction. For each venue/market/session/recovery chain, persist one full normalized starting snapshot and then exact normalized level changes with an explicit predecessor/revision identity sufficient to reconstruct every accepted immutable full book byte-for-byte in canonical numeric form. A new or displaced chain must start with a full snapshot. Missing, ambiguous, duplicate, or out-of-order predecessor evidence fails closed; it never guesses or silently skips a revision.
- Quote evidence must bind the exact RISEx and Lighter book revision identities used for BBO, sizing, hedge VWAP, and maker-price construction. Horizon evidence retains its exact referenced Lighter revision. An offline bounded-memory reconstruction/audit path must prove revision chains and referenced calculation witnesses without changing quote, fill, eligibility, or horizon semantics. Existing full-BOOK historical evidence remains readable and deterministic.
- Acceptance requires realistic deep-book evidence: at least the observed `1,900`-level contour, repeated small deltas, all three markets, an offered event rate above DG-005 without queue-capacity feedback, actual JSON serialization and owner-only durable storage, zero overflow/loss/reordering, bounded memory, clean terminal drain, deterministic reconstruction equal to the source full books, and a material byte reduction sufficient to remain below the existing caps with headroom. Focused failure/recovery/cap/legacy-replay tests and one clean Python 3.11 full suite are mandatory.
- No generic database, compression framework, message bus, storage service, queue/cap/timeout increase, raw-payload archive, venue/protocol/economic/fill/stop/horizon change, private/authenticated access, or write path is authorized. No replacement discovery gate may be frozen until SS-001G is independently accepted. `SS-002` and `SS-003` remain closed.

### 0.15 Accepted book-delta evidence and frozen `DG-006`

`SS-001G` is independently accepted on exact implementation source `4f83f8dea9f7a5deea4902f0c5cc6443e28004c1`. Each venue/market/session/recovery chain now persists one full normalized starting snapshot and exact canonical level deltas with explicit predecessor, revision, chain, and state-digest identity. QUOTE records bind the exact RISEx and Lighter revisions used by their calculation; HEDGE_HORIZON records retain the exact Lighter revision witness. Reconstruction and report audit are bounded, deterministic, legacy-full compatible, and fail closed on broken chains or references.

Chief acceptance on the exact committed tree includes `120` focused Spread tests, an isolated Python 3.11 full suite of `3780 passed, 3 skipped`, clean compile/import/dependency/scope surfaces, deterministic legacy replay of DG-002B, DG-003, and DG-005, and continued explicit rejection of corrupt DG-004 as `DECREASING_RECORD_INDEX`. The independent three-market, six-chain, `1,900`-level stress persisted `720` ordered revisions with `714` deltas and one clean terminal in `920,592` bytes; its complete end-to-end test duration was `10.13 s`, establishing at least `71` books/s including two offline reports, above DG-005's approximately `54.8` books/s accepted rate, without changing queue, timeout, or storage caps.

`DG-006 — Fillability Bounds Lossless Discovery` is frozen before its sample on exact accepted measurement source `4f83f8dea9f7a5deea4902f0c5cc6443e28004c1`. It authorizes exactly one fresh public-only observational run in a fresh owner-only store.

- Universe and economics are unchanged: exact public RISEx/Lighter `BTC/ETH/SOL`; both directions; `$100/$250/$500`; `1/2/3/5 bps`; `0/300/500/1000 ms`; `25 s` freshness; the configured fee inputs and provenance; points `$0`; no funding or future-exit value; unchanged maker pricing, strict lower bound, optimistic at-or-through upper bound, eligible-trade definition, exact-q accumulation, no-lookahead capture, and concentration dimensions.
- Stop on the first of `50` aggregate strict episodes, `500` unique eligible RISEx public trades while relevant quotes are active, `1,200 s` wall clock, or any integrity/fatal condition. After a non-fatal sample stop, retain only the already-authorized bounded Lighter horizon tail. There is no manual early stop, extension, retry, or parameter change.
- Use the accepted full-plus-delta BOOK representation and exact calculation witnesses in one fresh owner-only append-only JSONL store. Enforce the unchanged `2,500,000`-record and `12 GiB` caps, at least `24 GiB` free before launch, exact source/universe admission, contiguous unique record indices, exactly one physically-last terminal, deterministic repeated reports, and no private/authenticated/write surface. Any cap, chain, reference, queue, history, store, protocol, terminal, or completeness failure is `DATA_INSUFFICIENT`, never an economic verdict.
- Completeness, materiality thresholds, all required per-policy dimensions, and verdict precedence are exactly section 0.9. `PROFITABLE_QUOTES_UNFILLABLE` and the public-bracket form of `FILLABILITY_INSUFFICIENT_EVIDENCE` require `500` eligible trades. A strict-stop sample may reach `LATENCY_DESTROYS_EDGE` or `ENTRY_EDGE_CANDIDATE` only through the frozen per-policy strict-fillability, exact-q hedge, and delayed-edge thresholds.
- This gate may resolve the active mission only as case A, B, or C in section 0.8. A measurement failure is not mission completion. `SS-002` and `SS-003` remain closed, and no private, authenticated, credential, signing, order-preparation, dispatch, testnet, mainnet, or other write activity is authorized.

### 0.16 Post-DG-006 episode-local completeness correction

The one frozen DG-006 run is immutable `DATA_INSUFFICIENT / EPISODE_LOCAL_COMPLETENESS_AND_STOP_MISMATCH`. It ended cleanly on `STRICT_EPISODE_LIMIT` with `305` unique eligible trades, `56` strict episodes, `87` optimistic episodes, all `572` required model-scoped horizons, contiguous indices, and one physically-last `RUN_STOP`; nevertheless no fill-bearing policy is report-complete and no section-0.9 entry-edge verdict is valid.

- Run `KNuynAYhIMdXkAx38OXIaRri` used exact source `4f83f8dea9f7a5deea4902f0c5cc6443e28004c1` and exact `BTC/ETH/SOL`. Its owner-only evidence has `860,952` records, `2,485,056,934` bytes, and SHA-256 `dc6f5a9727ef82a4905f402cbf79daf99cd8672628841a102f286199b4643eee`. Two canonical reports are byte-identical at SHA-256 `c8f3d1d88422ba3e67b5babaf3fdec315b32bd8aaa12123c087cd03c537d5376`. BOOK evidence contains `15` full anchors and `34,827` deltas across `15` chains.
- Nine non-terminal gaps are three synchronized Lighter disconnect rounds, approximately `121 s` apart, one gap per market per connection; three additional gaps are planned terminal `PUBLIC_SMOKE_STOPPED`. This matches the separately observed public `600 s` diagnostic in which ordinary CLOSE ended four successive connections. The gap intervals remain real and must invalidate overlapping matching episodes; they are not permission to impute a hedge or erase a transport interruption.
- The current report marks an entire policy/horizon degraded when any one episode overlaps a matching gap. Read-only episode-local reconstruction finds `31` strict episodes with all four clean horizons and `25` strict episodes overlapping a disconnect. The largest exact policy has `9` raw strict episodes but only `5` all-horizon-clean strict episodes; no policy reaches the already-frozen material threshold of `10` valid episodes spanning `5` distinct detection timestamps. Aggregate `50` strict episodes therefore stopped the sample before the per-policy decision threshold it was intended to support.
- `SS-001H — Episode-Local Completeness and Material Stop` is the only authorized correction. It must preserve every raw episode and gap while separately counting valid and contaminated episodes per model/policy/horizon; a matching gap invalidates only the overlapping episode/horizon, and valid fill/edge distributions must never include contaminated evidence. Reports must expose raw, valid, contaminated, and reason-attributed counts and retain deterministic legacy replay.
- Graceful public WebSocket CLOSE must be distinguished in sanitized evidence from timeout/reset/error/exception transport failure. A graceful CLOSE/reconnect remains an explicit bounded gap and invalidates only matching overlap. Unexpected transport failure remains fail-closed for a product verdict. No data may be treated as continuous across sessions, and every new session/recovery chain still requires a full snapshot.
- Replace the future prospective strict stop with the pre-existing material-policy condition: the first exact policy to accumulate `10` valid strict episodes spanning at least `5` distinct detection timestamps after all four horizons are complete. The other first-stop conditions remain `500` unique eligible trades, `1,200 s`, or integrity/fatal failure. This aligns collection with the already-frozen section-0.9 decision threshold; it does not change that threshold, extend DG-006, or authorize sample-dependent economics.
- Acceptance requires adverse mixed clean/contaminated policies, all four horizon boundaries, late gap, graceful CLOSE versus transport exception, reconnect/new-snapshot identity, online material-policy stop, deterministic DG-006 replay, realistic bounded load, and one clean isolated Python 3.11 full suite. Economics, fees, quote construction, strict/optimistic fill definitions, eligibility, margins, horizons, venues, queue/cap/timeout values, private/authenticated access, and writes remain unchanged. No replacement discovery gate may be frozen before independent acceptance. `SS-002` and `SS-003` remain closed.

### 0.17 DG-007 — Fillability Bounds Resolution Discovery

This gate is frozen prospectively on `2026-09-04`, after independent acceptance of `SS-001H` and before viewing any DG-007 sample. The exact measurement source is `6e03195fe1c45e076cbe4cd20a2a02b178cc40e1`.

- Universe and economics remain exactly section 0.9: `BTC/ETH/SOL`; both directions; target notionals `$100/$250/$500`; target margins `1/2/3/5 bps`; horizons `0/300/500/1000 ms`; `25 s` freshness; configured fee inputs and provenance; unchanged hedge-anchored maker pricing, exact-q sizing, strict lower bound, optimistic upper bound, eligible-trade definition, quote lifetime, and no-lookahead hedge capture.
- The sample stops on the first of: one exact policy reaching `10` valid strict episodes across at least `5` distinct detection timestamps after all four model-scoped horizons are complete and clean; `500` unique eligible RISEx public trades while relevant quotes are active; `1,200 s` wall clock; or the first integrity/fatal condition. A stable ingress watermark must precede a material strict stop. Results may neither extend nor shorten the frozen run.
- Evidence uses the accepted owner-only full-plus-delta append-only representation with unchanged `2,500,000`-record and `12 GiB` caps and at least `24 GiB` free before launch. Exact source/universe admission, contiguous unique record indices, chain/reference integrity, all required model-scoped horizons, one physically-last terminal, and deterministic repeated reports are mandatory. A graceful WebSocket close is a named local gap that invalidates only matching overlap; timeout, reset, error, exception, abnormal/unknown close, queue/store/history/protocol failure, or incomplete terminal fails closed.
- Reports retain raw, valid, contaminated, and named-reason counts per model/policy/horizon. An episode is valid for fillability and edge only when its maker-fill interval and all four required horizons are present and uncontaminated; individual horizon outcome completeness remains local. Contaminated observations remain visible but never enter valid fill, time-to-fill, edge, markout, hedge-rate, or material-stop distributions.
- Verdict thresholds and precedence remain exactly section 0.9. `Approximately zero` is `0..1`; material fillability is at least `10` valid episodes for one exact policy across at least `5` detection timestamps; counts `2..9` or fewer timestamps are sparse. Public unfillability or public-bracket conclusions require `500` eligible trades. `PROFITABLE_QUOTES_UNFILLABLE` requires materially present snapshot opportunities and both valid strict and valid optimistic fillability approximately zero. `FILLABILITY_INSUFFICIENT_EVIDENCE` applies when valid strict remains approximately zero while a valid optimistic policy is materially positive. `LATENCY_DESTROYS_EDGE` or `ENTRY_EDGE_CANDIDATE` requires a materially valid strict policy and the frozen exact-q delayed-edge thresholds. Any source, stop, cap, terminal, integrity, completeness, or deterministic-report failure is `DATA_INSUFFICIENT` first.
- DG-007 is public unauthenticated read-only measurement only. It does not authorize private/authenticated data, credentials, signing, order preparation, dispatch, testnet/mainnet execution, strategy changes, another venue, `SS-002`, or `SS-003`.

### 0.18 Post-DG-007 open-ended gap replay correction

The one frozen DG-007 run `TBP1G0fJZWW29zh0B7vZ2n9d` on exact source `6e03195fe1c45e076cbe4cd20a2a02b178cc40e1` is immutable evidence. It ended cleanly on the prospective material-policy stop with `181` eligible trades, `79` raw strict episodes, `81` raw optimistic episodes, and all `640` model-scoped horizons. The online stop recorded `10` valid strict episodes across `10` timestamps for `BTC|RISEX_SELL_LIGHTER_BUY|100|1`, but the accepted offline report classified only `1` of those `10` as valid. No economic verdict may be issued while those authorities disagree.

- The mismatch is localized to offline replay of a valid gap with `gap_end_monotonic_ns = null`. Runtime `DataGapEvidence.overlaps` correctly treats it as an open interval beginning at `gap_start_monotonic_ns`: it cannot overlap evidence whose interval ended before the gap began, and it overlaps later matching evidence until recovery changes the session/generation identity. Offline `_gap_contaminates` instead substitutes the evidence interval end, then treats the resulting `gap_end < gap_start` relation as malformed; this falsely contaminates every earlier episode from a session that later closes. In the DG-007 stop policy, exactly the nine episodes from the first three sessions are falsely contaminated while the one episode in the still-open fourth session remains valid.
- `SS-001I — Open-Ended Gap Replay Parity` is the only authorized correction. It may change only offline gap-overlap classification and focused replay tests so null-ended gaps exactly match the already-accepted runtime interval/identity semantics. Explicit finite gaps, malformed/missing timestamps, venue/market/session/recovery matching, raw/valid/contaminated ledgers, economics, fill models, online stop logic, storage, queues, caps, timeouts, and protocols remain unchanged.
- Acceptance requires adverse before/at/after-boundary cases for null-ended and finite gaps, exact identity mismatch cases, mixed same-policy episodes across recovery generations, deterministic immutable DG-006 and DG-007 reports, and one fresh isolated Python 3.11 full suite. The corrected reports must preserve every raw count and horizon and must explain every changed valid/contaminated count from exact interval identity. Builder evidence is not acceptance.
- DG-007 is provisionally `DATA_INSUFFICIENT / ONLINE_OFFLINE_VALIDITY_MISMATCH` until SS-001I is independently accepted. Because the immutable store contains the complete raw episodes, horizons, gaps, identities, and terminal stream, a corrected deterministic offline report may apply the already-frozen DG-007 verdict rules without collecting, extending, tuning, or reopening the sample. Any residual online/offline disagreement keeps the verdict `DATA_INSUFFICIENT`.
- This correction and replay authorize no new public run, private/authenticated access, credential, signing, order preparation, dispatch, testnet/mainnet execution, strategy change, `SS-002`, or `SS-003`.

SS-001I is independently accepted and published at `6ec15a68e2ce07b890025801dec39e0c9881d72a`. It changes only offline null-ended gap overlap and adverse tests. Chief verification passed `37` focused tests and a fresh isolated Python 3.11 full suite of `3807 passed, 3 skipped`; dependencies, compile/import, diff, scope, private/write-surface, worktree, and Git checks were clean.

- Corrected deterministic DG-006 replay is `3,482,046` bytes, SHA-256 `a449180f31b7f90ca9cf283f9257da344d9925b49afc9dc2e11f504db358ac32`, with strict `56` raw / `56` valid / `0` contaminated and optimistic `87 / 87 / 0`; all `572` horizons are preserved. Every matching horizon ended at least `1,152,587,666 ns` before its later graceful CLOSE and no embedded horizon gap exists. This corrects the historical report split but does not change immutable DG-006's terminal verdict.
- Corrected deterministic DG-007 replay is `3,482,806` bytes, SHA-256 `6bb99c99150c9ca12e93a478ac976606968a37bac00c23b1aef5468859a23ed4`, with strict `79` raw / `76` valid / `3` contaminated and optimistic `81 / 78 / 3`; all `640` horizons are preserved. The six model episodes classified contaminated are the same three ETH sell/buy `$100`, `1/2/3 bps` quote versions in each model: only their `1000 ms` horizons cross the matching third-session graceful CLOSE, by `243,827,333 ns`. The material BTC sell/buy `$100`, `1 bp` policy exactly agrees with the online stop at `10` valid strict episodes across `10` timestamps and is complete at all four horizons.
- DG-007 therefore receives the terminal frozen-precedence verdict `NO_SNAPSHOT_EDGE`. The material policy has `100%` full-hedge and positive-edge shares at every horizon, but its valid p05 entry edge is `$0.00999295275` at `0 ms`, below the frozen `$0.01` material threshold. Its `0/300/500/1000 ms` mean edges are `$0.01126212150 / $0.01155092150 / $0.01180842150 / $0.01181992150`; medians are `$0.010017608925 / $0.01009423750 / $0.010287738725 / $0.010287738725`; p05 values are `$0.00999295275 / $0.00999295275 / $0.00999295275 / $0.00987915570`. This is a prospective case-C resolution: conservative fills and delayed exact-q hedge evidence exist, but the entry fails before latency under verdict precedence 2. It is not `LATENCY_DESTROYS_EDGE` or `ENTRY_EDGE_CANDIDATE`.
- The fillability-resolution mission is complete. No further public simulation is authorized. `SS-002` and `SS-003` remain closed; any maker-pricing, margin-grid, fee, venue, private calibration, authenticated access, testnet/mainnet execution, or other strategy change requires a separate explicit owner decision.

### 0.19 Owner-authorized profitability-resolution calibration

On `2026-09-04` the owner explicitly authorized the Chief to continue autonomously, use fresh visible Builders, verify applicable fees, test and calibrate the system, and drive the RISEx Spread Shadow hypothesis to an evidence-backed profitability or stop decision. This opens only bounded research and the minimum authenticated read-only fee verification needed for that research. It does not authorize credentials in task/chat, signing, order preparation, dispatch, positions, collateral, transfers, withdrawals, or live/testnet/mainnet trading. A profitable conclusion must be earned prospectively; the Chief must stop and report honestly if the fixed calibration and untouched holdout do not support it.

`SS-001J — Effective-Level and Cluster-Aware Calibration Evidence` is the only active implementation slice. It starts from exact accepted `main` and may change only the Spread offline report and focused tests. It must:

- preserve every existing DG-006/DG-007 canonical field, raw/valid/contaminated ledger, verdict, and deterministic replay while adding a separately labelled calibration-evidence section;
- compare actual tick-aligned maker prices, never nominal margin labels, and classify a cross-arm observation as `DISTINCT_EFFECTIVE_LEVEL` only when the wider arm's actual price is strictly farther from the hedge in the policy direction; equal actual prices are one `EFFECTIVE_PRICE_COLLISION` and never independent wider-level evidence;
- retain exact raw maker bounds, actual maker prices, signed price/tick separation, quote/version identity, detection timestamp, RISEx trade-event key, maker order ID, taker order ID, transaction hash, block number, and log index when present; missing or malformed venue identity must remain explicit and may not be guessed from time proximity;
- group venue execution evidence by `(market, aggressor_side, taker_order_id)`, report repeated quote versions inside the same group, and present nominal arms as paired evidence from the same venue cluster rather than independent Bernoulli trials;
- report descriptive fill/event, fill/venue-cluster, fill/quoteable-hour, and filled-notional/hour rates; inter-cluster intervals; fixed one-minute and five-minute concentration; effective-price collision rates; actual tick separations; and the complete `0/300/500/1000 ms` edge/markout/hedge-outcome curves for distinct wider-level evidence;
- use no fitted probability, confidence interval, significance claim, profitability claim, time-window session proxy, retrospective threshold, or new market sample.

Acceptance requires adversarial fixtures for price collisions, opposite policy direction, repeated quote versions, shared cross-arm taker orders, missing/malformed order identity, and deterministic ordering; exact corrected DG-006/DG-007 replay; the known DG-007 BTC `$100` sell/buy `1/2 bps` effective-level audit; one fresh isolated Python 3.11 full suite; and clean dependency, compile/import, diff, scope, private/write-surface, worktree, and Git checks. Builder evidence is not acceptance.

After `SS-001J` acceptance, the Chief may perform one Level-B RISEx mainnet fee read for the exact owner account through an already-valid protected local credential or a local no-echo owner-only login boundary. The request is limited to official `GET /v1/user/fees`, the caller's own account, an initial attempt plus at most one transport retry, strict auth/schema/identity checks, and sanitized fee-tier/rate/provenance output. Secret bytes and raw protected payloads may not enter Git, task/chat, arguments, logs, reports, databases, fixtures, or process titles. Any semantic, auth, identity, or safety failure is terminal; no write-capable endpoint may be called. Lighter research inputs must be refreshed from current official account-type documentation and stored with the observation date; published venue latency is a tier component, never an end-to-end guarantee.

Only after the report slice and fee gate pass may the Chief freeze and open one fresh `CAL-001` public calibration sample. Its immutable design is BTC only, `RISEX_SELL_LIGHTER_BUY`, `$100`, nominal `1/2 bps`, configured exact verified RISEx maker fee, current official Lighter Standard taker fee, and `0/300/500/1000 ms` horizons. Stop on the first of `250` unique eligible BTC trade keys, `1,200 s`, `1,000,000` records, `4 GiB`, or any fatal/integrity/completeness condition; there is no fill-count stop, manual extension, retry, or parameter change. After stop, drain only already-pending horizons. The sample is calibration, not confirmation, and cannot by itself authorize `SS-002`, `SS-003`, or a profitability claim.

Before `CAL-001`, the Chief must freeze quantitative continuation/stop thresholds without using its observations. If calibration passes, one separate untouched `HOLDOUT-001` interval is required under the identical frozen policy and thresholds. Only agreement across calibration and holdout after verified fees, conservative latency/markout treatment, effective-level de-duplication, cluster dependence, concentration, and exact-q hedge completeness may support a profitability-candidate decision. Failure of either stage closes this configuration unless the owner later authorizes a genuinely new hypothesis; real execution remains a separate decision.

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
