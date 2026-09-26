## Telegram messages: one card per cycle

A series produces one short acceptance, a brief message for every step (account checks and positions found, launch, selection, leverage, each LIMIT/MARKET attempt, hold end, residual closure, stops), one card after each cycle and one final summary. The card shows the result (✅ paired, 🟡 closed but pair incomplete, ⛔ needs checking), BTC size and two-account turnover in USD, whether each phase was filled between our own accounts (🤝), by external accounts (👥, with their IDs) or mixed (🔀), LIMIT→MARKET time, how long preparation/hold/closing took, PnL and fees, and the pause before the next cycle. Robinhood charges 0% (owner-confirmed) and its trade receipts carry no fee, so when every fill of a settled cycle has an empty fee the card shows fees 0 and PnL after fees equal to gross, marked "тариф биржи 0%". Any nonzero fee value keeps fees unknown. While a cycle runs, brief steps arrive in real time: start, "открываю" (LIMIT accepted), "открыто" (own or external fills and LIMIT→MARKET time), "закрываю", "закрыто", plus waiting for spread, HTTP 429 cooldown, size reduction and residual closure. Use `/status` for live progress and `/report` for details; both are unchanged. Messages are rendered by the controller in the background and add no work to order sending.

## Why /run was refused after 16:57 on 2026-09-25, and what changed

The shared check log `recovery-checks.jsonl` outgrew a 32 MiB safety limit, so every `/run` stopped at the READY check (Telegram: PREFLIGHT_REFUSED); `/close` still worked. The shared log now has its own larger finite limit and each check writes a short record (count + hash of inspected journals) instead of every file hash. If a history limit is ever reached again, Telegram shows HISTORY_LIMIT instead of a generic refusal; repeating `/run` will not help.

If fresh prices or margin no longer fit the selected size at the confirmed leverage, the cycle reduces the size (keeping the 0.10 planning reserve where possible and never below the venue minimum) and rechecks before sending; Telegram shows old → new size. Leverage and hold time do not change. If the private stream is briefly behind (for example it still shows a just-cancelled order), a refused attempt that sent nothing is retried after a short pause within the usual 6/15 attempts; if the stream is unusable, the cycle closes the proved positions with reduce-only orders instead of leaving them open.

These changes take effect only after the controller restarts on the new code. Run, when no cycle is active:

```bash
cd "/Users/daniilmakarov/Desktop/RISEx Spread Shadow"
.venv-hood/bin/python spread-shadow-runs/hood-margin-resize-20260925/claude-v1/verify_and_activate.py
```

It runs the full test suite in a clean temporary environment first and stops if anything fails; then it stops the idle controller, updates `main`, checks both accounts read-only and starts the controller again. It sends no orders. Afterwards send `/run` in Telegram as usual.

## Cycle spacing and temporary read limits

Finite Telegram series wait a newly sampled 5–30 seconds after each safely completed cycle before checking accounts for the next cycle. The first cycle starts normally; no extra pause follows the final cycle. Telegram announces the chosen pause and `/status` displays it. Fresh readiness checks follow the pause, so actual spacing can be longer. A restart never resumes the old series automatically.

Temporary HTTP 429 during final reconciliation triggers bounded read-only cooldown and a short background notice. Orders are not resent. If terminal execution or exact flatness still cannot be proved within the configured bounds, the series stops with an honest incomplete result. Existing USD PnL and volume reporting continues to include proved external executions.

# Accepted command but no new cycle

If readiness refuses a command, /status now shows the rejected command ID, check stage, time and safe failure category even when the original Telegram reply was not delivered or the controller restarted. Acceptance means the command was received, not that an order was sent. A fresh /run performs new checks; a consumed command is never replayed. Detailed historic series results remain available through /report. Transport diagnostics deliberately omit tokens, request URLs and response bodies.

# Waiting for room inside the spread

With an explicit tick offset, a temporarily narrow spread now triggers bounded initial waiting instead of an immediate one-snapshot refusal. Telegram may show that the system is waiting for a spread that fits the requested price offset; orders have not yet been sent. Current production bounds are 20 seconds after the first no-room observation and at most 40 total observations, with fresh validation on each. If the market never permits that price or another check fails, the series still stops. The offset is never silently reduced and a stopped series is never replayed automatically.

# Recovery after an interrupted leverage confirmation

Fresh /run checks can now read the shared recovery journal across multiple checks. For Robinhood, recovery binds the original prepared transaction to its execution event, consumed nonce and fresh matching flat accounts. It does not wait for nonexistent L1 commit timestamps or reinterpret millisecond execution time as seconds. Missing/conflicting proof still blocks. Old cycles and their reported outcomes remain unchanged; a resolved administrative barrier does not resume an old series.

# Temporary account rate limits

HTTP 429 during leverage readback or paired preparation now triggers a bounded cooldown and fresh account reads, without resending a leverage setting. The same cooldown applies to residual-recovery account reads. If the exchange remains rate limited, requests cannot fit the existing deadline, or account evidence conflicts, the operation stops. Diagnostics identify HTTP 429; failed series are never automatically replayed. Ordinary successful reads have no added request or sleep.

# Delayed order visibility during cleanup

If an accepted LIMIT is temporarily absent from exact order reads after the receiver is refused, cleanup waits within the existing observation limits instead of abandoning it after one read. A discovered active order is canceled once; a filled order is reconciled and any proved residual uses the existing reduce-only closure. Unknown/conflicting state still stops the series. This cannot guarantee completion during a venue outage and does not resume previously blocked series or send a command at restart.

# USD PnL and executed series volume — 2026-09-25

For the configured Robinhood BTC market, cycle and series PnL are displayed in nominal USD (USDG denomination), without applying or claiming a live USDG/USD conversion. Series totals include executed USD turnover: the sum of actual fill quantity times actual execution price across both configured accounts, including opening, closing and in-cycle residual fills. Each account side counts: a $50 match between our accounts contributes $100 turnover. Unfilled orders contribute nothing, and a repeated account/trade receipt never adds turnover twice.

Missing/incomplete volume evidence remains unknown, with any proven subtotal labelled separately. Fees are not needed to prove turnover; missing fees still leave net PnL unknown. Funding and pre-cycle closure of old inventory remain excluded. Unknown foreign denominations are not relabelled USD. The final report uses the existing background reader/sender and adds no venue or FX calls to trading. Robinhood describes the settlement collateral in [its Wallet perpetual futures documentation](https://robinhood.com/us/en/support/articles/robinhood-wallet-perpetual-futures/).

# Finite series length and final results — 2026-09-25

The five-cycle cap is removed: `/run 20` requests twenty cycles with saved settings; `/run ack 1 20` uses ACK admission and one tick for each. Counts must be positive decimal integers (no signs, fractions or leading zeroes) within the Telegram command length. Tick offsets remain 1–5. This is a finite request, never an endless loop or a restart-resumable campaign.

At completion or early stop the bot reports each recorded cycle: paired, external, mixed, unfilled or unproved opening/closing, plus residual execution when present. The final page sums execution PnL across both accounts and all recorded cycles, including in-cycle residual closures. Gross and net are separate; missing fees leave net unknown, incomplete cycles leave the aggregate unknown, and any known subtotal is labelled as a partial sum. Funding and separately initiated pre-cycle closures of old positions are excluded. Long reports paginate outside the trading task; `/report` repeats the latest saved series summary after restart. Old commands without a saved membership list have no reconstructed series total.

# Cycle and close notices — 2026-09-25

`/close` acknowledges only a reduce-only closure and never promises a new cycle. Cycle notices identify the step/total and command ID, announce completion and the next-step check, and distinguish opening, holding, paired closure and residual recovery. A completed flat cycle can proceed to the next requested step even when pairing failed; the existing finite-series checks still apply.

Progress and final notifications use a bounded 64-message FIFO in the separate controller, with a 10-second delivery deadline. Cycle sequencing never awaits delivery. When full, the oldest queued notice is discarded; delayed/missing messages are possible, and saved reports plus `/status` remain authoritative. This adds no notification I/O to the trading child; it does not claim zero shared-host CPU cost or measured exchange latency improvement.

# Receipt reconciliation and controller restarts

A briefly future-dated trade receipt is reread within the existing reconciliation bounds; only an exact reread at a valid time resolves that observation. The same rule applies to residual closing, without resending an uncertain order. Historical cycle journals remain unchanged.

The current manually started Telegram controller does not survive a laptop reboot. Start it again with the existing controller command after login. Startup does not replay queued trading commands or automatically open positions; inspect `/status` and `/accounts` first.

# Telegram responsiveness

Small clock differences of up to 5 seconds ahead of the host no longer silently discard owner commands. Commands outside the permitted time window receive a refusal; send a new command rather than replaying old updates. Delivery errors appear in the private controller log without tokens or message contents.

# Operator request timeout

The working operator configuration `spread-shadow-runs/hood-cycle-race-latency-20260920/operator-v1/random-cycle.json` sets `request_timeout_seconds` to 15 seconds. This is an upper wait bound, not a mandatory delay. Other deadlines and freshness checks retain their own bounds. Configuration files omitting this field retain the code default.

# Recovery correction — 2026-09-24

ACK cancellation now consumes the already reserved nonce after locating the exact LIMIT, including a refusal before private WS publication. Residual closure waits within configured bounds for delayed trade history and position propagation; it never resends an uncertain order. Reports distinguish known external fills from overall UNKNOWN, record preflight refusals as completed refusals, and use the final cycle reason rather than an earlier retry. Trading modes, ticks, size, leverage and fees policy are unchanged. Activation and verification status are in STATUS.md.

## Price and receiver timing switches

The configured default is shown before each cycle. Telegram: `/run ws 5` waits for exact private LIMIT state and improves price by five ticks; `/run ack 5` sends MARKET after a positive application ACK, without waiting for that state. `/run ws 1` and `/run ack 1` compare one tick. Append any positive integer count for a finite series: `/run ack 1 5` requests five cycles with ACK and one tick, while `/run 5` uses saved defaults for five cycles. Without a count, `/run` still requests one cycle. The selected mode/ticks apply to each cycle and its paired close. These commands initiate real trading. Before each cycle, the controller checks the configured BTC accounts and may perform one bounded reduce-only close of proved inventory; a terminal flat close and fresh exact zero positions are required before opening. Each later cycle also requires the previous cycle's complete terminal report, confirmed flat inventory, no unresolved order state and a new current readiness check. Unknown or incomplete evidence stops the series; startup never resumes it. `/close` remains a separate reduce-only command.

Terminal: append `--receiver-admission ack --price-improvement-ticks 5` to the existing `python -m risex_spread_shadow.hood_handoff.cli simple --keychain --config ...` command; use `ws_confirmed` to restore the wait. JSON fields are `receiver_admission` and `price_improvement_ticks`. No restart is needed for per-run overrides; manual file edits still require the existing controller restart/binding procedure.

ACK mode still checks the locally cached public book and known adverse private events. It does not prove LIMIT acceptance into the book: MARKET may fill externally while LIMIT is absent, rejected or already filled. Five ticks cannot cross the spread; insufficient room produces PRICE_OFFSET_NO_ROOM instead of quietly changing the offset. Normal recovery/reconciliation remains mandatory and may report unresolved residual inventory. Inspect /report and /accounts; no speed or own-fill improvement is promised before real measurements. A bounded `book_top` trace in stream-events.jsonl records up to32 changed best-price/volume snapshots for2s per source binding.

## Closing connection warmup

Paired closing automatically warms its existing HTTP send pool with one public read during mandatory revalidation, before choosing the final quote. It adds no periodic background activity or order. Failure or an unfinished warmup does not block closing; reconnect remains possible if the peer closes the connection. CLOSING_PLAN_READY.http_warmup records its result. This moves potential connection setup before source exposure; it does not guarantee faster matching after a LIMIT rests. ACK-only MARKET dispatch is not enabled.

## Race timing evidence

Owner-triggered cycles also collect passive `account_tx` events into the existing protected `stream-events.jsonl`, alongside order/local milestones. Match transaction hashes to mutation receipts for analysis; account transaction events can concern other markets and do not prove a selected-market resting order. Raw venue times retain their original units and must not be compared with local wall time without verifying units/clock offset. Missing events remain unknown. No extra command is required; transaction writes still use warmed HTTP until successful Robinhood WS responses are verified. Current measurements show about20ms median WS advantage only on unsigned validation failures, not a demonstrated speedup of live trading.

# Current fast admission mode

Set `"receiver_admission": "ws_confirmed"` in the existing random-cycle configuration to prepare before LIMIT and admit MARKET from fresh exact private WS state plus a local L2 veto. The same mode covers paired opening and closing. Telegram and terminal name it explicitly. Orders still use the existing transaction transport; preflight, cancellation recovery and final accounts/trades/fees may use REST. Omit the setting or use `"strict"` to retain repeated REST admission checks. A controller config change requires an idle restart and a matching persisted config binding; never delete history to change modes.

The faster mode accepts missing full account/owner/FIFO proof. It cannot guarantee that our counterpart receives our limit. Missing, stale, conflicting or disconnected private stream evidence stops MARKET; cancellation and residual recovery remain required. Public publication may lag the exact private source; a worse public best price is not itself a veto. Better price, extra same-price volume or a contradictory smaller level stop MARKET and are explained in Telegram/terminal. Proved zero-fill canceled liquidity vetoes can use up to 6 opening or 15 paired-closing attempts; uncertainty or fills cannot. Owner-triggered sequential tests have no one-run quota. Compare actual LIMIT→MARKET timing and both reciprocal phases in saved reports; an offline speedup is not a live fill-rate result. This section supersedes older candidate/REST-admission descriptions below for the selected fast mode. See STATUS for activation and verification.

The post-run correction accepts stream cleanup diagnostics after a completed cycle, so they no longer cause a false Telegram BLOCKED. A proved canceled zero-fill source after an early missing active-order observation is a known failed attempt, not unknown execution. Real uncertainty and concurrent-operation checks remain. New cycles save HTTP_READ_TIMINGS at cleanup for read latency diagnosis; they include only bounded numeric timings and allowlisted endpoint names. Exact ready WS observations avoid starting redundant REST reads. No live end-to-end acceleration is established by the public read benchmark; see STATUS for results and activation.

Recovery accepts cycles containing `stream-events.jsonl`: this diagnostic file is hashed separately from authoritative trading journals and cannot itself prove execution. After a failed `/close`, send a new command once the updated controller is active; old Telegram updates are never replayed.

Cycle-075 correction: an early missing active-order snapshot is distinct from a conflicting order. A subsequently proved source-only residual enters existing reduce-only recovery. WS may win the first exact observation while REST is pending; cancellation nonce preparation starts at source ACK. Final active-source/public priority checks remain. Owner Telegram tests have no one-run quota. A historical known residual is not reset by deployment; use `/accounts` to inspect it. A fresh owner `/run` may now close proved inventory before opening, as described above.

> Runtime update, 2026-09-24: the WS read version is activated in the local Telegram controller. The next owner `/run` starts its stream before order placement; final active-source and public owner/queue checks remain REST, transaction submission remains HTTP. Older candidate-only descriptions below are historical; see the latest STATUS entry.

# RISEx Spread Shadow

The WS read acceleration candidate is on `codex/ws-read-acceleration`; it is not active in the running controller. Ordinary random cycles in this candidate start a bounded read stream automatically. Stream depth serves price calculations; exact complete order events serve discovery and terminal observation. REST remains the fallback and the source for final active-order checks, public owner/queue proof, account risk and final trade/position/fee reconciliation. Human-facing Telegram delivery stays outside the order transport. A cycle saves `stream-events.jsonl` with causal timing, not raw private frames. A faster cached read is not a measured faster trade. See STATUS and NEXT_TASK for tested versions and evidence.


The owner confirmed one bounded BTC pilot for the HCR-44 development branch. Its dedicated one-cycle CLI configuration fixes 0.00020 BTC, a 40.00 quote maximum per account, a 20-second proved hold, one paired attempt per phase and one exact residual recovery per account. It requires existing Keychain credentials, a fresh read-only stream and the shared operator lock. The current pilot configuration forbids leverage-setting writes; a separately guarded exact 1x opt-in is inactive until the owner authorizes it. A durable claim consumes the single pilot authorization before credentials or network use. The protected Telegram controller configuration remains unchanged. The pilot has not been launched because the current leverage settings differ from the required 1x target; see NEXT_TASK and STATUS.

The active tool runs one owner-requested two-account Robinhood Chain BTC cycle. One account posts a limit and the other sends a price-bounded market order through the public book. Other participants can still trade first; matching our own accounts is proved afterward from exact receipts. Offline tests establish software behavior, not live execution probability.

Read STATUS.md for accepted versions/results, SYSTEM_SPEC.md for behavior, NEXT_TASK.md for current work/authority and AGENTS.md for process/safety. Historical experiments remain in Git and immutable local evidence.

HCR-41 is published and the idle Telegram controller has been updated. It improves residual-close preparation and timing, terminal reports, pre-journal failures and public-book diagnostics. Opening size uses mark-based margin, evidenced fees and adverse entry loss for both accounts while preserving the original absolute 4x cap. A leverage-setting cancellation during local preparation records a durable NOT_SENT and retains cancellation behavior; cancellation after possible transport remains unresolved. An uncertain sent setting can clear after exact terminal sequencer status, consumed nonce and fresh account-state proof; pending or incomplete evidence still blocks a new run. This is sequencer soft finality. Venue-specific reserved margin and final order admission remain unproved. See STATUS.md and NEXT_TASK.md.

HCR-42 is accepted and the owner selected an exact opening reserve of 0.10/0.02 quote per account, bound through the operator configuration. The protected controller was restarted after that release as recorded in STATUS. HCR-43 on a separate branch is an offline candidate: it keeps HTTP order submission and the existing paired admission proof, adds local numeric send/cancel timing diagnostics, and reuses the exact source observation before residual cancellation after receiver terminal status. It is not active in the controller. A separate offline-only frame parser now records bounded, redacted synthetic observations; it has no connected feed or trading role. No live latency gain or mutual-fill guarantee is established.

HCR-44 on its isolated branch adds a finite read-only Robinhood stream measurement command. It reads the protected operator configuration and existing Keychain records, then subscribes to configured BTC book and both configured account order channels for one bounded session. It saves only structured projections in an existing owner-only directory, without raw private frames or credentials. This command has no order, cancellation or controller path. The earlier 64 KiB per-frame limit caused a locally sent close code 1009; the larger chief-v4/v5 sessions received book snapshots and continuous updates, and chief-v5 recognized initial private-order snapshots for both accounts. Neither a live own-order event nor trading admission is established. The following command form requires an unused gate and a new owner-only output path; it is not a trading command:

The branch also contains an optional in-memory read stream for one bounded cycle. A book gap removes book readiness, and disconnection clears all channel and order hints. An exact terminal event can only trigger an earlier REST lookup; REST remains the source of execution proof. One bounded read-only validation reached book and both private subscription readiness. This socket is not enabled by terminal or Telegram, and no own-order event or trading admission has been established.

```bash
python -m risex_spread_shadow.hood_handoff.stream_measurement \
  --config "/Users/daniilmakarov/Desktop/RISEx Spread Shadow/spread-shadow-runs/hood-cycle-race-latency-20260920/operator-v1/random-cycle.json" \
  --output "/Users/daniilmakarov/Desktop/RISEx Spread Shadow/spread-shadow-runs/hood-hcr44-stream-measurement-20260923/chief-v1/events.jsonl"
```

## Setup

Use Python 3.11 and pinned `lighter-sdk==1.1.2`. For a new installation:

```bash
python3.11 -m venv .venv-hood
.venv-hood/bin/python -m pip install -e '.[hood-handoff,test]'
```

Do not recreate an existing installation. If tests are needed in a runtime-only environment, install `'.[test]'` there first. `./start` always selects `.venv-hood/bin/python` and this checkout's source, independently of shell PATH. Help is offline:

```bash
./start --help
.venv-hood/bin/python -m risex_spread_shadow.hood_handoff.cli --help
.venv-hood/bin/python -m pytest -q
```

## Start one real cycle

```bash
cd "/Users/daniilmakarov/Desktop/RISEx Spread Shadow"
./start
```

Enter confirms a real Mainnet cycle; `C` or `CANCEL` cancels. Before confirmation there is no Keychain/network access or slot reservation. Keep the terminal running through completion.

The configured directory is `spread-shadow-runs/hood-cycle-race-latency-20260920/operator-v1/`. Its `random-cycle.json` selects BTC market 1, accounts 27331/27337, key index 4, Robinhood signing domain 466324, existing timing defaults and explicit incremental opening-margin deferral. For opening, the owner-selected `margin_reserve` must be explicitly present as `{"initial_quote":"0.10","dispatch_quote":"0.02"}`. The same file is used by terminal `./start` and Telegram `/run`; missing or different values refuse opening before a slot is claimed. Each amount is in quote/balance currency **per account**: 0.10 must remain at quantity and leverage planning, and 0.02 at fresh pre-send admission after fee and adverse-price bounds. Equality at 0.02 is allowed. These amounts are an initial owner choice, not a proved optimum or a fill guarantee. Reduce-only `/close` does not apply an opening reserve. `market-contract.json` binds market/environment evidence; current grids, minimums, balances and timestamps still require validated observations. Do not copy old quotes into current evidence.

Each run reserves a new immutable `cycle-NNN` directory and client prefix. It chooses the first account and BUY/SELL uniformly (four combinations), one legal BTC quantity up to four times the smaller fresh available balance in quote notional, and one 20–180 second hold. Roles/quantity/hold never redraw on retry. Both accounts' opening budgets further limit quantity using fresh mark-based margin, fee and adverse entry-loss evidence. For the selected size, it computes the lowest modeled 1x–4x leverage independently for each account, using venue integer margin-fraction precision; 1x is selected when sufficient. If a setting differs, the run submits it once and requires a fresh exact readback before placing an order. A setting rejection stops the cycle; an ambiguous send/readback prevents another `/run` until proved terminal through recovery. A confirmed setting on one account is not automatically rolled back when the other fails. Venue risk/admission rules may still reject a selected order. Both positions must begin exactly flat with no conflicting orders. The first order must rest and pass exact owner/price-priority checks before the other account sends its order. Preparation and safe zero-fill retries share up to 6 opening attempts and 15 paired-closing attempts. These are total attempts including the first, not extra retries. The separately bounded residual-close procedure is unchanged.

HOLD starts only after the full quantity is proved matched between our own orders. Closing reverses the sides and is reduce-only. Known residuals can be closed separately only after complete reconciliation. Unresolved execution stops dependent actions. Normal completion ends a single-cycle command. In an explicitly requested bounded Telegram series, the controller checks the completed flat result and fresh readiness before the next requested cycle.

## Close current positions explicitly

In Telegram send `/close`. In the terminal:

```bash
.venv-hood/bin/python -m risex_spread_shadow.hood_handoff.cli close-positions --keychain \
  --config spread-shadow-runs/hood-cycle-race-latency-20260920/operator-v1/random-cycle.json
```

Enter confirms this one real recovery operation. It checks both configured BTC accounts and prior intents, records the current baseline, and closes only actual positions using the existing reduce-only MARKET/IOC residual logic. It shares the normal operator lock with `./start`; wait for an active operation to finish. Active or unresolved old orders must be reconciled first; this command does not cancel unrelated orders. Known partial/zero fills are reconciled before another attempt; ambiguous execution stops further writes. The result is saved separately in `close-NNN/close.jsonl`. It reports residuals and closure fees when proved; PnL for a manually opened/adopted position remains unknown without entry history. A standalone terminal `close-positions` operation does not start a cycle.

Already reserved cross margin can exceed the remaining available balance. That comparison does not block reduce-only closure; account identity, active orders, exact position and venue order acceptance still matter. A fresh `/run` with a proved existing BTC position now invokes one bounded reduce-only closure and requires a separate exact flatness check before its new cycle. It never resumes an old HOLD or treats an old position as a successful new cycle. An unresolved leverage-setting intent blocks opening; proved positions may still be reduced.

BTC market 1 recovery also submits exact on-grid residuals below the ordinary opening minimum using reduce-only MARKET/IOC. This narrow rule follows observed filled closure orders of 0.00004 and 0.00007 BTC; it does not increase the residual or guarantee venue acceptance. Other markets and opening orders keep normal minimum checks. Temporary recovery-account read failures are retried within existing bounds; ambiguous sends, conflicting identities and unresolved histories are never replayed. A delayed account read cannot authorize an order against an expired quote.

## Read the messages

Terminal and Telegram progress shows the two confirmed opening positions, hold duration/start, and **planned closing start in Moscow time**. That time is not a guarantee that all orders and reconciliation finish then. Closing/recovery messages mark opening positions as historical.

The final result keeps these facts separate:

| Field | Meaning |
| --- | --- |
| Paired execution | Full mutual own-order execution must succeed in both opening and closing. Recovery to flat does not make the strategy successful. |
| Historical position | CONFIRMED_FLAT requires terminal order/trade proof and causal zero positions, not just exit code or a zero snapshot. |
| Own / external / unproved volume | Exact receipt evidence per opening/closing; A is the first-limit account, B the other. |
| Fees | Sum of actual proved trade fees, including residual closure; missing fees remain unknown. |
| Gross execution PnL | SELL notionals minus BUY notionals, only for a complete initial-flat to final-flat cycle. |
| Net execution PnL | Gross minus all proved commissions, in quote currency. Missing commissions leave net unknown. |
| Funding | Excluded from execution PnL; the funding-inclusive total remains unknown without its own evidence. |

The final message states who filled each LIMIT and whether our MARKET filled our LIMIT, external orders, nothing, or was never sent. Proven external fills include account IDs even when the opposite order filled zero. A phase that never ran is marked explicitly. Overall strategy failure, successful residual closure and unknown fees can all be true in the same cycle.

If residual closure stops, the final view also shows its saved reason, including a failure before any closing order was sent. Old generic `contract_error` records are explicitly described as lacking a precise cause; the report does not invent one. A source order ID remains bound to reconciliation even if a later lookup temporarily omits the order.

The SDK now preserves the account's maker/taker venue and integrator fee fields. Both explicitly zero components prove zero. Nonzero integer units are not yet verified for this venue and are retained as raw evidence, not guessed amounts. Old journals missing these fields remain unchanged. Small PnL values are not rounded to cents.

After interruption preserve journals/state. A new `/run` checks current positions, old creation intents and leverage-setting outcomes again; a failed historical result does not permanently lock the bot. A fresh `/run` may close proved current selected-market inventory once before opening its new cycle. A missing terminal or unknown order is not a closed position and cannot be cleared by deleting a lock/state file. Startup never resumes the prior command; lower-level reconciliation does not authorize replay. `/accounts` is the way to request current selected-market account observations.

## Telegram

Only a fresh exact `/run` or `/close` from the configured numeric owner in their private chat confirms one real operation. `/run` accepts only the documented mode, tick and bounded cycle count; credentials and shell commands are never accepted in chat.

| Command | Action |
| --- | --- |
| `/run` | Recheck current readiness; close proved BTC inventory once if present, verify exact flatness, then start one real cycle under the same local configuration. |
| `/run ack 1 5` | Request five sequential real cycles with ACK admission and one tick. Each next cycle requires a complete, flat prior result and fresh account checks; any uncertainty stops the series. |
| `/run 5` | Request five sequential cycles with the saved mode and tick settings. Any positive integer count is accepted; for example `/run 20`. |
| `/close` | Check both configured accounts and close existing positions of this market with bounded reduce-only MARKET/IOC orders. Zero positions send no orders. |
| `/status` | Short progress or latest saved result, including a terminal-launched cycle when idle. |
| `/report` | Saved result plus last position times, per-account fees/PnL and bounded diagnostics. |
| `/accounts` | Current read-only available balance, selected-market position/orders and observation time for fixed A/B accounts. Other markets are not checked. |
| `/help` | Explain the commands. |

Automatic progress notices identify accepted LIMITs and then who filled each LIMIT/MARKET: the exact paired own order, external accounts or unproved counterparties. Opening and closing are separate; dispatch intervals are shown when measured. Notices also cover confirmed HOLD, closing and separate residual recovery while a controller-owned cycle runs. Delivery failures/slowness do not cancel or replay trading. Short stages can finish between observations; the final saved report is authoritative. `/status` and `/report` make no venue requests; `/accounts` does not unlock execution or treat missing/stale observations as zero.

If the bot is not already provisioned, run locally with the owner's numeric ID:

```bash
.venv-hood/bin/python -m risex_spread_shadow.hood_handoff.telegram_control provision \
  --owner-id YOUR_NUMERIC_USER_ID \
  --config spread-shadow-runs/hood-cycle-race-latency-20260920/operator-v1/random-cycle.json
```

Hidden input stores the bot token in native Keychain (`hood-telegram-control-v1`) without network access. Skip provisioning when already stored; replacement requires `--replace-token`. Trading keys must already be stored in their separately bound records. The detached runner cannot prompt for missing keys.

Start the controller locally:

```bash
.venv-hood/bin/python -m risex_spread_shadow.hood_handoff.telegram_control run \
  --owner-id YOUR_NUMERIC_USER_ID \
  --config spread-shadow-runs/hood-cycle-race-latency-20260920/operator-v1/random-cycle.json
```

Old queued commands are discarded on startup. Private identity/freshness checks, consumed-update state and active intent prevent duplicate launches. The detached child inherits an instance lock and takes the normal operator lock, excluding another controller/normal launcher. Do not run lower-level interfaces concurrently on these accounts.

Stopping the controller does not close positions or kill its child. Restart is blocked while the child retains the lock. Afterward a fresh read-only check can remove an old administrative barrier: a previous `/run` requires both accounts flat, while a previous `/close` may retain proved residual positions for a new owner-triggered command. All previous creation intents must be terminal; a new `/run` first checks close readiness and requires separate fresh exact-flat proof before opening. The exact order lookup may omit older orders; bounded inactive history provides terminal proof. Missing evidence still prevents new orders, and a later fresh command retries the check. Owner-only state is under `~/.config/risex-spread-shadow/telegram-control/`, bound to owner/config/evidence. Do not erase it to replay commands. Configuration changes need local review/restart; mismatched binding fails closed. Update an idle controller after code changes; never interrupt an active cycle to deploy.

## Offline diagnosis and other interfaces

Read a saved cycle without credentials, network or mutation:

```bash
.venv-hood/bin/python -m risex_spread_shadow.hood_handoff.cli report \
  --path spread-shadow-runs/hood-cycle-race-latency-20260920/operator-v1/cycle-011
```

Add `--json` for hashes, exact receipts, orders/intents, reasons, accounting and latency. New runs separately time the initial source lookup, account/book checks, propagation refresh and final source lookup. Launchers record the interface as a diagnostic label; older missing labels cannot prove a channel comparison. Overlapping durations cannot be added; unavailable time is not zero. Saved facts do not establish current account state. Telegram disables the hidden child's terminal progress with `--no-progress`; notifications remain in the separate controller and orders use the same cycle path.

Keys use native Keychain bound to API/signing environment, account and key index, or hidden local input. No plaintext fallback, arguments, environment variables, project files or logs. `--keychain-replace`/`--keychain-remove` affect the exact local record; removal does not revoke a venue key. Help/previews stay offline.

`readiness` is an explicitly invoked read-only diagnostic, never permission to trade. Missing incremental opening-margin proof remains UNKNOWN; the configured opening deferral skips only that missing estimate, not other checks or known insufficiency. `local-attempt` is fixed-quantity paired opening without automatic cycle closure. `run` retains explicit handoff/series and their non-reusable claims. Use their `--help` and SYSTEM_SPEC; none guarantees atomic execution.

Existing series uses a new paired operation per slice. One standing limit consumed by several market orders is a future task; every fragment must satisfy minimum quantity/notional, and balance has not been proved to be the sole constraint.

Public/paper scanners and legacy Funding Farmer/Telegram/testnet modules remain isolated and frozen. Saved readback/report tools remain offline; new collection or campaigns require a new prospective NEXT_TASK contract. Historical details are preserved in Git at the revision linked in SYSTEM_SPEC.
