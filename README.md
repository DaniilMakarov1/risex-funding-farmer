# RISEx Spread Shadow

The active tool runs one owner-requested two-account Robinhood Chain BTC cycle. One account posts a limit and the other sends a price-bounded market order through the public book. Other participants can still trade first; matching our own accounts is proved afterward from exact receipts. Offline tests establish software behavior, not live execution probability.

Read STATUS.md for accepted versions/results, SYSTEM_SPEC.md for behavior, NEXT_TASK.md for current work/authority and AGENTS.md for process/safety. Historical experiments remain in Git and immutable local evidence.

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

The configured directory is `spread-shadow-runs/hood-cycle-race-latency-20260920/operator-v1/`. Its `random-cycle.json` selects BTC market 1, accounts 27331/27337, key index 4, Robinhood signing domain 466324, existing timing defaults and explicit incremental opening-margin deferral. `market-contract.json` binds market/environment evidence; current grids, minimums, balances and timestamps still require validated observations. Do not copy old quotes into current evidence.

Each run reserves a new immutable `cycle-NNN` directory and client prefix. It chooses the first account and BUY/SELL uniformly (four combinations), one legal quantity capped by the smaller free balance without leverage multiplication, and one 20–300 second hold. Roles/quantity/hold never redraw on retry. Both positions must begin exactly flat with no conflicting orders. The first order must rest and pass exact owner/price-priority checks before the other account sends its order. Preparation and safe zero-fill retries share three attempts per phase.

HOLD starts only after the full quantity is proved matched between our own orders. Closing reverses the sides and is reduce-only. Known residuals can be closed separately only after complete reconciliation. Unresolved execution stops dependent actions. Normal completion ends the cycle; it never starts the next one automatically.

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

The SDK now preserves the account's maker/taker venue and integrator fee fields. Both explicitly zero components prove zero. Nonzero integer units are not yet verified for this venue and are retained as raw evidence, not guessed amounts. Old journals missing these fields remain unchanged. Small PnL values are not rounded to cents.

After interruption preserve journals/state. A missing terminal or unknown order is not a closed position and cannot be cleared by deleting a lock/state file. The normal launcher never resumes or closes historical inventory automatically; lower-level reconciliation does not authorize replay. `/accounts` is the way to request current selected-market account observations.

## Telegram

Only a fresh exact `/run` from the configured numeric owner in their private chat confirms one real cycle. No parameters, credentials or shell commands are accepted in chat.

| Command | Action |
| --- | --- |
| `/run` | Start one real cycle under the same local configuration. |
| `/status` | Short progress or latest saved result, including a terminal-launched cycle when idle. |
| `/report` | Saved result plus last position times, per-account fees/PnL and bounded diagnostics. |
| `/accounts` | Current read-only available balance, selected-market position/orders and observation time for fixed A/B accounts. Other markets are not checked. |
| `/help` | Explain the commands. |

Automatic progress notices cover confirmed HOLD, closing and separate residual recovery while a controller-owned cycle runs. Delivery failures/slowness do not cancel or replay trading. Short stages can finish between observations; the final saved report is authoritative. `/status` and `/report` make no venue requests; `/accounts` does not unlock execution or treat missing/stale observations as zero.

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

Stopping the controller does not close positions or kill its child. Restart is blocked while the child retains the lock; afterward complete saved flatness and resolved orders/intents must permit another launch. Owner-only state is under `~/.config/risex-spread-shadow/telegram-control/`, bound to owner/config/evidence. Do not erase it to replay commands. Configuration changes need local review/restart; mismatched binding fails closed. Update an idle controller after code changes; never interrupt an active cycle to deploy.

## Offline diagnosis and other interfaces

Read a saved cycle without credentials, network or mutation:

```bash
.venv-hood/bin/python -m risex_spread_shadow.hood_handoff.cli report \
  --path spread-shadow-runs/hood-cycle-race-latency-20260920/operator-v1/cycle-011
```

Add `--json` for hashes, exact receipts, orders/intents, reasons, accounting and latency. Overlapping durations cannot be added; unavailable time is not zero. Saved facts do not establish current account state.

Keys use native Keychain bound to API/signing environment, account and key index, or hidden local input. No plaintext fallback, arguments, environment variables, project files or logs. `--keychain-replace`/`--keychain-remove` affect the exact local record; removal does not revoke a venue key. Help/previews stay offline.

`readiness` is an explicitly invoked read-only diagnostic, never permission to trade. Missing incremental opening-margin proof remains UNKNOWN; the configured opening deferral skips only that missing estimate, not other checks or known insufficiency. `local-attempt` is fixed-quantity paired opening without automatic cycle closure. `run` retains explicit handoff/series and their non-reusable claims. Use their `--help` and SYSTEM_SPEC; none guarantees atomic execution.

Existing series uses a new paired operation per slice. One standing limit consumed by several market orders is a future task; every fragment must satisfy minimum quantity/notional, and balance has not been proved to be the sole constraint.

Public/paper scanners and legacy Funding Farmer/Telegram/testnet modules remain isolated and frozen. Saved readback/report tools remain offline; new collection or campaigns require a new prospective NEXT_TASK contract. Historical details are preserved in Git at the revision linked in SYSTEM_SPEC.
