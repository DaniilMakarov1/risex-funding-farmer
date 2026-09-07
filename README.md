# RISEx Spread Shadow and legacy Funding Farmer

## Current and planned behavior — Scanner v1

FINAL-ASTRA-01 (2026-09-06) authorizes Scanner v1; **S1a and S1b are accepted; the full scanner is not yet complete**. The accepted versioned kernel subset adds venue-local quantity/minimum rules, retained partials and exact notional, four fill-model/delay alternatives and activation post-only checks. Historical default behavior remains separate. Exact SHA/evidence and limitations are in STATUS. Accepted S1b adds fixed-cap episodes, pending reservations, entry cancellation barriers, partial exits and retained inventory marks. Its first recomputed four-alternative D1 report remains DATA_INSUFFICIENT from the original transport failure, with no observed fills and funding UNKNOWN. The owner revision of 2026-09-07 has completed RP1 diagnostic acceptance (stale activation book; no proven kernel/replay defect) and now has working RP2 minimal recording/readback; the next task connects the saved stream to existing S1b reporting; no new collection is open before independent offline acceptance. Full former S2 and four long windows are deferred, not accepted. Sequential offline alternatives are permitted. SYSTEM_SPEC's SCV1-1 is the new versioned policy, not a claim that the commands below already implement it.

The planned public-only BTC CLI models RISEx maker SELL -> Lighter Standard taker BUY, full entry/hedge/exit episodes and exact residues. It uses four alternatives: TRADE_THROUGH_ONLY and TOUCH_ALLOWED, each with primary 500 ms delays or stress 1000 ms delays plus 1 bp on RISEx fills. Touch is an explicit conditional fill assumption, not actual execution evidence. New policy preserves partials, uses venue-local operation minimums/grids, accumulates within fixed Q_cap, respects 5-second quote cancellation and a 120-second first-fill completion deadline, and reports closed execution-only PnL separately from cashflows, marked open inventory and unknown funding. See SCV1-1 for the binding details.

MODEL_POSITIVE/MODEL_NEGATIVE are conditional, MODEL_SENSITIVE identifies assumption sensitivity, POLICY_BLOCKED identifies policy feasibility, DATA_INSUFFICIENT identifies missing/corrupt inputs and NO_EXECUTION_OBSERVED means no fills. Alternatives are not additive or independent; positive open marks are not closed profit. Funding UNKNOWN forbids all-in profitability claims. The first research pilot need not produce profit, but must have functioning saved-data/core/report paths, actual write/read validation, enforced resource limits and explicit loss/overflow status. Full former combined online capacity proof is deferred. Points = $0; legacy Funding Farmer stays frozen.

By owner exception2026-09-07, current Chief GPT-6 Astra Medium / Standard personally completes implementation/tests and integrates main; RP2 Builder is stopped/archived. Self-authored fixes are not described as independently Builder-reviewed. No management polling, infinite Goal or cancelled campaign restart. Actual selected model/effort/speed verification is recorded in current operational evidence; prose alone does not configure a session.

Until staged independent acceptance, use offline development/checks only. Later authority is limited to one prospectively gated 60-second technical public smoke and one new 15-minute pilot under SCV1-1.5/NEXT_TASK, with entry/requote cutoff at12:45 and135seconds for modeled completion. Exact verified recording/report commands will be documented after implementation acceptance; current historical commands do not implement this pilot. Private/account/fee-reader endpoints, credentials, signing/order preparation/dispatch, testnet/mainnet orders, real funds, transfers/withdrawals and strategy execution remain prohibited. Transport WebSocket heartbeat stays enabled; there is no LLM in market collection. No new command is promised before accepted implementation; exact user commands will be updated at release.

The accepted Python fixture interfaces are `run_scv1_s1a` and `run_scv1_s1a_alternatives` in `risex_spread_shadow.scv1`; they reuse the existing cycle kernel and require saved/fixture inputs. The additional accepted offline interfaces `run_scv1_s1b`, `run_scv1_s1b_alternatives`, and `Scv1S1bKernel` implement S1b accumulation/marked inventory; `build_scv1_s1b_d1_report` and `render_scv1_s1b_d1_report` recompute saved inputs. These are not a released public collection CLI. Focused offline examples and assertions are in `tests/spread_shadow/test_scv1_s1a.py` (`python -m pytest -q tests/spread_shadow/test_scv1_s1a.py`).

## Saved recording to the first research table

`research-report` reads only a saved RP2 file and runs the accepted S1b kernel sequentially for all four alternatives. It makes no exchange requests. It does not use historical `cycle-report` decisions or the wider legacy observer policy.

```bash
risex-spread-shadow research-report /absolute/run/evidence.jsonl --output-json /absolute/run/research-report.json
risex-spread-shadow research-report /absolute/run/evidence.jsonl --format json
```

The default output is a readable Markdown table; `--output-json` additionally saves the complete report once (exclusive creation, mode0600). JSON includes compact source-book and fill references, decisions, positions, separate open marks, reasons, input SHA256 and implementation fingerprints. Repeat runs reproduce economic results; only offline timing changes. Invalid/corrupt input fails explicitly. An intact but incomplete recording is reported as INCOMPLETE, never silently promoted to an economic result.

Model decisions occur once per second from collection start. At an equal timestamp, already processed records precede the decision; their physical order is preserved. A saved BOOK becomes usable after its linked CHANGED processing result; trades require an explicit application processing timestamp. Old trade recordings without this field cannot establish availability and fail with TRADE_PROCESSING_TIME_UNPROVEN. No-change frames do not refresh economic timestamps. Public metadata setup time is separate from the new RUN_START collection clock.

For a recording planned for900seconds, new admissions and entry requotes stop at765seconds even if collection fails early. Existing orders retain their accepted cancellation rules; the135-second tail allows completion without fabricating fills or writing off residues. All four lanes share one input file and frozen SCV1-1 policy. The table separates data-eligible duration, scenario activity until a terminal block, blocked duration, and offline runtime. Activity here means the elapsed model interval from first admission until terminal block/end; it is not continuous fresh-data time or proof of execution. JSON also reports episode-active durations and exact eligibility reasons. Funding stays UNKNOWN, points$0. When less than half the observed interval meets requirements, the report explicitly says the model is not evaluated by this stream; this descriptive label changes no trading guard.

## RP2 recording and readback

The existing CLI now implements `record` and `record-readback`. Recording is fixed to public BTC on RISEx/Lighter, one market,60or900seconds; it does not run a strategy. Live invocation remains gated until the saved-input S1b/report path is accepted and the specific smoke/pilot is prospectively frozen. No public collection has yet been performed with this version.

After that gate, recording syntax is:

```bash
risex-spread-shadow record --market BTC --duration-seconds 60 --store-root /absolute/owner-only-run-directory
```

The command prints the saved path and immediate readback. Use900seconds only for the later frozen15-minute pilot. `record-readback` makes no exchange requests. This exact fixture sample was written through both actual public adapters and the real store, then checked with the CLI:

```bash
risex-spread-shadow record-readback '/Users/daniilmakarov/Desktop/RISEx Spread Shadow/spread-shadow-runs/scanner-v1-20260906/rp2-chief-takeover/samples/normal/run-WtCyqsgb_FYBtIYRg8n8Bf6h/evidence.jsonl' --format table
```

Use `--format json` for exact `data_valid_duration_ns`, `data_ineligible_duration_ns`, eligibility-reason totals, receipt/outcome/book links and diagnostic intervals. The normal FIXTURE_ONLY sample has3changed messages,2unchanged, no loss; a two-second observation contains499999990ns eligible and1500000010ns ineligible. Unchanged messages preserve the old economic timestamp; timer/pong never refreshes it. COMPLETE means recording/readback completed, not that data was always fresh or a trading model was profitable. Queue loss/limits are INCOMPLETE; corrupt/missing links or terminals produce explicit readback errors. Exact duration totals survive truncation of the bounded detailed interval sample. Unknown processing/source/network causes remain UNKNOWN.

Other saved samples and their source/hash bindings are in `spread-shadow-runs/scanner-v1-20260906/rp2-chief-takeover/sample-manifest.json`. These are fixtures, not market observations or trades. The new saved-stream S1b economics report and smoke/pilot still require their next gates.

## Requirements

- Python 3.11
- `aiohttp`
- `pytest` and `pytest-asyncio` for tests

## Setup

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[test]'
```

## Tests

```bash
pytest
```

## Entrypoints

The legacy benchmark retains the `risex-farmer` entrypoint documented below. It is not the active Spread Shadow strategy and must not be resumed as an operational process.

The active contour uses the separate `risex-spread-shadow` entrypoint. It remains
public-only and does not import the opt-in authenticated fee boundary below.

### Historical accepted complete-cycle commands (pre-Scanner v1)

These existing commands implement the historical §0.21 policy, not SCV1-1. Offline historical replay is permitted; the collection examples below are reference syntax only and do not authorize a run or reuse of CYCLE-001.

The accepted historical cycle path uses public BTC data only: hypothetical RISEx maker
SELL entry, delayed Lighter Standard BUY hedge, and explicit paired exits.
Target notional is $100 and entry threshold is 1 bp. RISEx modeled fees are
1 bp maker / 3 bps taker; Lighter Standard is 0. Primary delays are 500 ms;
stress uses 1000 ms and an additional 1 bp cost on each RISEx fill. Maximum
holding time is 120 seconds. `SYSTEM_SPEC.md` historical §0.21 defines that old policy; SCV1-1 supersedes it for future new-policy runs.

Offline commands make no network request:

```bash
risex-spread-shadow cycle-report /absolute/run/evidence.jsonl --format table
risex-spread-shadow cycle-report /absolute/campaign-root --format json
risex-spread-shadow cycle-freeze --help
risex-spread-shadow cycle-collect --help
```

`cycle-report` replays saved causal events through the accepted kernel and
checks persisted results. It separates validity, sufficiency, economics and
usefulness. Primary and stress are alternatives: never add their turnover or
PnL. Completed PnL includes modeled fees and costs; an unfinished cash-flow
subtotal is not profit. Funding remains `UNKNOWN_EXECUTION_ONLY`.
`forced_or_unmatched_full_cycle_pnl_usd` is full-cycle PnL;
`forced_unmatched_exit_cashflow_usd` is only the executed forced/unmatched
exit cashflow after its costs. Holding/occupancy runs from first maker fill
to the terminal observation boundary. An unresolved exposure's observed
duration does not establish its eventual closing time.

The historical campaign interface required Chief-recorded prospective parameters in
`NEXT_TASK.md` before any market request. Reference syntax for four 45-minute UTC
windows over two days, two per day, using one clean accepted release:

```bash
risex-spread-shadow cycle-freeze --store-root /absolute/campaign-root \
  --campaign-id CAMPAIGN_ID --accepted-release FULL_ACCEPTED_GIT_SHA \
  --window WINDOW_1,START_1_UTC,END_1_UTC \
  --window WINDOW_2,START_2_UTC,END_2_UTC \
  --window WINDOW_3,START_3_UTC,END_3_UTC \
  --window WINDOW_4,START_4_UTC,END_4_UTC
risex-spread-shadow cycle-collect --store-root /absolute/campaign-root \
  --manifest /absolute/campaign-root/.s3-cycle/CAMPAIGN_ID/manifest.json \
  --window-id WINDOW_1 --format table
```

Replace placeholders only with the recorded campaign parameters. Use the same
absolute root and release for every window. The manifest and create-once
window claims are retained under `.s3-cycle`; never delete claims or change
roots to retry a consumed window. Runtime storage is owner-only and excluded
from Git. Collection admits entries until 42:45 and stops market processing
at 45:00; the 135-second tail is reserved for closing. Campaign-wide caps
are 1,000,000 records and 4 GiB, including reserves of 100,000 records and
512 MiB. Missing closing evidence, resource failure or unresolved exposure
cannot be upgraded to a complete profitable result.

The historical descriptive screen required 20 completed cycles and 20 filled dependence
groups, with five cycles in at least three windows spanning both days. Primary
PnL must be positive on both days, aggregate stress positive, and primary
positive after removing its best dependence group. Groups are not proven
independent; even a passing screen is hypothetical evidence, not trading
authority. Fixtures remain `FIXTURE_ONLY`. No result-based stop, tuning,
replacement window, extension or retry-to-pass is allowed.

### Historical fixed CAL/HOLDOUT scanner (closed)

`scan` is the fixed CAL-001/HOLDOUT-001 route; `scan-report` evaluates saved
evidence offline. Its profile is BTC, RISEx sell / Lighter buy, $100, nominal
1/2 bps, recorded RISEx maker fee 1 bps and Lighter Standard taker fee 0 bps,
with exact-q hedges at 0/300/500/1000 ms. Legacy `smoke`/`report` commands do
not implement this fixed admission contract.

Offline usage (no network):

```bash
risex-spread-shadow scan-report /absolute/run/evidence.jsonl --format table
risex-spread-shadow scan-report /absolute/holdout/evidence.jsonl --cal-report /absolute/cal/evidence.jsonl --format json
risex-spread-shadow scan --help
```

The public command requires `--stage`, `--accepted-release` (full Git SHA),
`--window-start-utc`, `--window-end-utc`, and the Chief-recorded `--store-root`.
HOLDOUT also requires `--cal-report`. The window permits starting the sample;
collection stops at the first fixed limit and then drains pending horizons.
The loaded source checkout must be clean and exactly match the accepted release.
Use the same release and absolute store root for both stages. The create-once
`.scan-003/<stage>.claim` is retained on failed/missed attempts; never delete it
or change roots to retry. Every new public stage requires its prospective operational gate in
NEXT_TASK.md. CAL-001 is now consumed and HOLDOUT is closed; the current
configuration must not be rerun.

The report distinguishes `POSITIVE`, `NEGATIVE`, `NOT_CONFIRMED` and
`INSUFFICIENT`, while fixture evidence remains `FIXTURE_ONLY`. CAL passing is
provisional; only two passing stages can yield the public candidate label.
Sums, means and tails are conditional dependence-unit entry scores, not
executable PnL, profit per hour or full-cycle profitability. The first bounded public CAL-001 sample verified the source-to-report path
and returned `INSUFFICIENT`: observed conditional entry scores were positive,
but the frozen qualification thresholds failed. Exact results, evidence paths
and the stopped configuration are recorded in `STATUS.md` and `NEXT_TASK.md`.

### Opt-in RISEx owner-fee read

This is a historical interface, currently quarantined for separate audited
no-echo/partial-body defects. It must not be invoked during scanner work.
The scanner uses recorded SS-001Q fee provenance. Historical invocation:

```bash
python -m pip install -e '.[risex-fee-read]'
risex-spread-shadow-fee-read
# or: python -m risex_spread_shadow.risex_fee_read
```

The runner reads only the existing owner-only RISEx identity and session-signer
files under `~/.config/risex-farmer`, validates the fixed official RISEx domain
first, then checks the exact mainnet identity and registered signer through the
public readiness endpoint. It then asks for the owner wallet key through hidden
local input. The key is used once to sign the official login message and is
never persisted, placed in an environment variable, passed as an argument, or
included in output. The only authenticated request is the caller-owned
`GET /v1/user/fees` read. Output is sanitized fee tier/rate/provenance evidence
or one classified terminal failure. No order, position, balance, collateral,
transfer, withdrawal, deposit, or strategy path is available from this
entrypoint. The signing dependency is intentionally opt-in; without
`.[risex-fee-read]`, the runner fails closed.

## Frozen legacy commands (reference only; no operational authorization)

The shared public HTTP runtime session uses a 30-second total request timeout so
large, slow official responses can complete without changing scan cadence or
retry scheduling.

The commands use a local SQLite paper database. With no fixture, `scan-once`
performs a read-only public REST scan and `paper-run` maintains read-only public
market-data streams until Ctrl+C or SIGTERM:

```bash
risex-farmer --db paper.db scan-once
risex-farmer --db paper.db scan-once --format json
risex-farmer --db paper.db scan-once --format table
risex-farmer --db paper.db paper-run
risex-farmer --db paper.db report
```

`scan-once` defaults to backward-compatible JSON. Add `--format table` for a
human-readable view of the same ordered routes (up to 15), including funding
countdown, RISEx/hedge/net funding, entry/exit fee and execution components,
expected net PnL, plain-language trade status, and public venue readiness.
`paper-run` continues through `NO_TRADE` and venue outages, reconnecting public
streams and failing affected routes closed. `report` summarizes persisted paper
and runtime evidence. An open position is never force-closed merely because a
run ends.

On a RISEx orderbook checksum mismatch, `paper-run` follows the official public
WebSocket recovery contract: it keeps affected books unusable, unsubscribes and
resubscribes the orderbook channel, and accepts the new stream snapshots before
resuming calculations. It does not combine unordered REST and WebSocket states.

### Optional outbound Telegram notifications

Telegram delivery is disabled by default and does not change scanning or paper
trading. To enable outbound `sendMessage` notifications for `paper-run`, choose
a bot token and destination chat, and set all three
environment variables before starting a new run:

```bash
export RISEX_TELEGRAM_ENABLED=true
export RISEX_TELEGRAM_BOT_TOKEN='newly-rotated-token'
export RISEX_TELEGRAM_CHAT_ID='destination-chat-id'
risex-farmer --db paper.db paper-run
```

Credentials are read only from the environment and must not be committed,
logged, persisted, or placed in CLI arguments. Any explicit risk acceptance for
a disclosed token is recorded without the token value. This integration is outbound-only: the
application does not poll `getUpdates`, accept commands, trigger scans, or place orders.
Delivery is best effort; a full queue or Telegram outage can drop messages so it
cannot delay market-data processing, strategy deadlines, or safe shutdown.
Every completed authoritative `FULL` scan sends all 20 existing ordered route
rows in `Ticker | Route | Expected PnL` form. Long messages are split into
bounded numbered parts without splitting or duplicating a route. `UNKNOWN`
includes a short authoritative blocker in the same third field. The values come
directly from the runtime's scanner result; Telegram does not recalculate
economics. Monetary values in Telegram text are displayed with exactly two
fractional digits while authoritative Decimal values retain full precision.
INITIAL, FOCUSED, and RECOVERY scans do not send this digest.

Extended maintains a validated full-universe catalog in a non-blocking
background task and refreshes only the five required official market mappings
on normal public refreshes. Fresh last-good metadata survives transient catalog
timeouts; expired metadata fails closed with catalog/metadata blockers rather
than `BOOK_UNHEALTHY`. Dedicated book, trade, and funding sockets have isolated
10-second heartbeat and readiness state. Physical transport lifecycle,
watchdog restart, and logical book resync evidence remain distinct.

RISEx contract quantity and forecast funding use visibly reported paper-only
fallback assumptions. They are enabled only for this experiment and fail closed
when public metadata, grids, stable-quote identity, price, rate, or schedule
checks are inconsistent. They are never represented as official applied funding.

Deterministic fixture mode is intended for CI and local paper verification and
never accesses the network:

```bash
risex-farmer --db paper.db scan-once --fixture tests/fixtures/paper_006/no_opportunity.json
risex-farmer --db paper.db paper-run --fixture tests/fixtures/paper_006/positive_closed.json
risex-farmer --db paper.db report
```

See `SYSTEM_SPEC.md` for the active Spread Shadow contract and preserved legacy specification, `STATUS.md` for the accepted baseline, and `NEXT_TASK.md` for the current authorization boundary.

The RISEx, Extended, and Nado lifecycle modules are not CLI modes and must not be imported by normal Farmer startup. Their frozen verification levels and safety gates are preserved in the historical Git version referenced by `AGENTS.md`; they grant no current execution authority. Current accepted state and work live only in `STATUS.md` and `NEXT_TASK.md`.

The historical testnet lifecycle/strategy roadmap is frozen and grants no current authority. Scanner v1 does not require its resumption. A new private or execution program requires a separate owner decision.
