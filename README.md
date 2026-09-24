The post-run correction accepts stream cleanup diagnostics after a completed cycle, so they no longer cause a false Telegram BLOCKED. A proved canceled zero-fill source after an early missing active-order observation is a known failed attempt, not unknown execution. Real uncertainty and concurrent-operation checks remain. New cycles save HTTP_READ_TIMINGS at cleanup for read latency diagnosis; they include only bounded numeric timings and allowlisted endpoint names. Exact ready WS observations avoid starting redundant REST reads. No live end-to-end acceleration is established by the public read benchmark; see STATUS for results and activation.

Recovery accepts cycles containing `stream-events.jsonl`: this diagnostic file is hashed separately from authoritative trading journals and cannot itself prove execution. After a failed `/close`, send a new command once the updated controller is active; old Telegram updates are never replayed.

Cycle-075 correction: an early missing active-order snapshot is distinct from a conflicting order. A subsequently proved source-only residual enters existing reduce-only recovery. WS may win the first exact observation while REST is pending; cancellation nonce preparation starts at source ACK. Final active-source/public priority checks remain. Owner Telegram tests have no one-run quota. A historical known residual is not reset by deployment; use `/accounts` to inspect it and `/close` to explicitly request recovery before a new `/run`.

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

Each run reserves a new immutable `cycle-NNN` directory and client prefix. It chooses the first account and BUY/SELL uniformly (four combinations), one legal BTC quantity up to four times the smaller fresh available balance in quote notional, and one 20–180 second hold. Roles/quantity/hold never redraw on retry. Both accounts' opening budgets further limit quantity using fresh mark-based margin, fee and adverse entry-loss evidence. For the selected size, it computes the lowest modeled 1x–4x leverage independently for each account, using venue integer margin-fraction precision; 1x is selected when sufficient. If a setting differs, the run submits it once and requires a fresh exact readback before placing an order. A setting rejection stops the cycle; an ambiguous send/readback prevents another `/run` until proved terminal through recovery. A confirmed setting on one account is not automatically rolled back when the other fails. Venue risk/admission rules may still reject a selected order. Both positions must begin exactly flat with no conflicting orders. The first order must rest and pass exact owner/price-priority checks before the other account sends its order. Preparation and safe zero-fill retries share three attempts per phase.

HOLD starts only after the full quantity is proved matched between our own orders. Closing reverses the sides and is reduce-only. Known residuals can be closed separately only after complete reconciliation. Unresolved execution stops dependent actions. Normal completion ends the cycle; it never starts the next one automatically.

## Close current positions explicitly

In Telegram send `/close`. In the terminal:

```bash
.venv-hood/bin/python -m risex_spread_shadow.hood_handoff.cli close-positions --keychain \
  --config spread-shadow-runs/hood-cycle-race-latency-20260920/operator-v1/random-cycle.json
```

Enter confirms this one real recovery operation. It checks both configured BTC accounts and prior intents, records the current baseline, and closes only actual positions using the existing reduce-only MARKET/IOC residual logic. It shares the normal operator lock with `./start`; wait for an active operation to finish. Active or unresolved old orders must be reconciled first; this command does not cancel unrelated orders. Known partial/zero fills are reconciled before another attempt; ambiguous execution stops further writes. The result is saved separately in `close-NNN/close.jsonl`. It reports residuals and closure fees when proved; PnL for a manually opened/adopted position remains unknown without entry history. No command starts another cycle automatically.

Already reserved cross margin can exceed the remaining available balance. That comparison does not block reduce-only closure; account identity, active orders, exact position and venue order acceptance still matter. `/run` with an existing BTC position ends with an instruction to use `/close`, then requires a separate fresh `/run` after flatness is proved. It never resumes an old HOLD or treats an old position as a successful new cycle. An unresolved leverage-setting intent blocks `/run`; `/close` can still reduce proved positions.

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

After interruption preserve journals/state. A new `/run` checks current positions, old creation intents and leverage-setting outcomes again; a failed historical result does not permanently lock the bot. After manually closing positions, send `/run` again. If positions remain, use `/close`. A missing terminal or unknown order is not a closed position and cannot be cleared by deleting a lock/state file. The normal launcher never resumes or closes historical inventory automatically; lower-level reconciliation does not authorize replay. `/accounts` is the way to request current selected-market account observations.

## Telegram

Only a fresh exact `/run` or `/close` from the configured numeric owner in their private chat confirms one real operation. No parameters, credentials or shell commands are accepted in chat.

| Command | Action |
| --- | --- |
| `/run` | Recheck current readiness, then start one real cycle under the same local configuration. |
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

Stopping the controller does not close positions or kill its child. Restart is blocked while the child retains the lock; afterward a fresh read-only check can remove an old administrative barrier when all previous creation intents are terminal and both accounts are flat without active orders. The exact order lookup may omit older orders; bounded inactive history provides terminal proof. Missing evidence still prevents new orders, and a later fresh command retries the check. Owner-only state is under `~/.config/risex-spread-shadow/telegram-control/`, bound to owner/config/evidence. Do not erase it to replay commands. Configuration changes need local review/restart; mismatched binding fails closed. Update an idle controller after code changes; never interrupt an active cycle to deploy.

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
