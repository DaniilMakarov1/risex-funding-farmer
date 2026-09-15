# RISEx Spread Shadow and legacy Funding Farmer

## One bounded agent-capable Mainnet attempt — HCR-9/HCR-10

The launcher is under implementation and independent review; no HCR-9 live attempt has been verified. HCR-10 permits the assigned execution agent to access the required API keys and run this one bounded operation after the launcher is accepted, all exact inputs are fixed, the full plan is shown and the owner gives a separate explicit launch instruction. The owner may still choose local launch. This authority does not extend to another attempt, series execution or autonomous trading.

The selected operation is one BTC paired opening: account27331 posts SELL LIMIT POST_ONLY and account27337 submits BUY MARKET IOC, API key index4, Robinhood deployment. The owner targets approximately USD16 of exposure per leg, with small deviation allowed. Quantity0.00020BTC is an exact reviewable input, not a verified current dollar value. The utility must not automatically resize or choose prices. The source limit price and receiver worst price, together with finite timing bounds, are selected explicitly by the operator before launch.

Before confirming, review both actions, quantity, price bounds, every timing setting and the resulting positions. Full paired opening leaves a SHORT at the source and a LONG at the receiver. It does not automatically close them or guarantee that the two accounts match each other. The limit order's exchange lifetime and the program's waiting deadline are separate settings.

After the program returns, preserve its attempt directory and all files, including the intent journal. A missing terminal result is an incomplete attempt. A timeout, rejection or unknown response does not establish that both accounts stayed flat. Do not remove or rename evidence to retry, and do not launch another attempt while earlier positions or orders remain unresolved. The same-directory launcher must refuse new submissions.

For diagnosis, tell the Chief that the attempt has ended and provide its directory path. The Chief can read the sanitized packet in the shared workspace, inspect recorded phases and known/unknown positions, and prepare an offline-tested correction. Never place API keys, authentication tokens or signed payloads in that packet. The required API keys may be exchanged only with the assigned execution agent in the private owner-agent task/chat or through the protected local credential boundary; seed phrases, recovery material and withdrawal credentials remain prohibited. The Chief does not promise continuous monitoring and must not edit or restart an active financial process. Any later financial launch requires a new explicit owner decision after the prior state is resolved.


## Optional local API-key storage — HCR-6

Accepted offline candidate `11bde5bd68177b2476d784f34e3ea9babccf63ed`: final clean isolated Python3.11 suite **4352 passed, 3 skipped**.

Use `--keychain` to save each API key in macOS Keychain on first hidden entry and reuse it on subsequent invocations. This option is supported by readiness and the existing explicitly operator-run execution command. No extra Python package is needed. Stored credentials are bound to the exact HTTPS API origin, signing environment/chain, account index and API key index. Saving a key does not verify that it is correct or grant trading permission.

The first invocation still requires input for each missing key: earlier in-memory runs did not retain keys. Later invocations with `--keychain` read the matching Keychain entries without requesting the keys again; macOS may still request Keychain access/unlock permission. Keychain denial or failure stops before client/network construction, with no plaintext fallback. Omitting `--keychain` preserves hidden in-memory input each time. No key may be placed in a command, environment variable, JSON file, Git/GitHub, log or evidence packet. HCR-10 permits the private owner-agent task/chat only for exchange with the assigned execution agent.

To replace stored keys, use `--keychain-replace` instead of `--keychain`; this requests fresh hidden input for both configured accounts and then continues the selected operation. To remove only the matching local records, use `--keychain-remove` instead; it exits after removal without constructing an SDK client or making network requests. For example, take the readiness command below and replace its final `--keychain` with `--keychain-remove`. Removal must not be combined with execution flags or either other Keychain option. Removal does not revoke API keys on the venue.

Help and execution preview remain offline and do not access Keychain even when `--keychain` is present. Builder verification uses synthetic keys/backends; real user keys and actual OS Keychain storage are not accessed in those tests. HCR-10 separately permits the assigned execution agent to use real credentials for the single accepted bounded Mainnet attempt after all live gates and explicit launch confirmation are satisfied. The current configured Robinhood endpoint is real Mainnet, not a valueless test environment.

## Explicit local margin-calculation deferral — HCR-8

For operator-run `PAIRED_OPENING`, set `"defer_incremental_margin_calculation": true` in the existing run/series JSON configuration, or add `--defer-incremental-margin-calculation` to the existing `run` invocation. The default is false. The flag is rejected for `CLOSE_REOPEN` and for `readiness`; it does not make the readiness diagnostic READY.

This option skips only a missing local incremental opening-margin estimate/provenance. Missing values stay uncalculated; never insert a fabricated zero. Supplied estimates must still be finite and nonnegative, and an evidenced estimate exceeding available margin still blocks. Current account margin/balance checks and all quantity, minimum, price, time, position, identity and reconciliation checks remain. The exact deferral choice is shown in preview/plan and bound to parent/child journals; changing it on restart is rejected. An exchange rejection does not prove a fill and is not automatically retried.

For an existing operator-prepared configuration/evidence pair, the following is an **offline preview command template**. Replace both paths with real files first; it does not send orders or access keys:

```bash
.venv-hood/bin/risex-hood-handoff run \
  --config /absolute/path/to/paired-opening-config.json \
  --market-evidence /absolute/path/to/current-market-evidence.json \
  --source-account-index 27331 --receiver-account-index 27337 \
  --defer-incremental-margin-calculation
```

With deferral enabled, the four source/receiver incremental-margin estimate/provenance fields may be omitted from market evidence. Current market identity/grid/minimum evidence and its original timestamp remain required. This command does not choose missing prices or deadlines. Existing execution/plan-review switches and optional `--keychain` remain separate; the new flag alone never enables execution. Under HCR-10 the assigned execution agent may run the single bounded Mainnet attempt only after accepted implementation, exact bounds, final plan review and the owner's separate explicit launch instruction. The owner's test budget is on Mainnet; no testnet migration is intended.

## Local read-only readiness check — HCR-5

A separate `readiness` command checks current Robinhood market/account state without sending orders. Accepted offline candidate `3de9e91ac16bd0faa2caeed2efa15c7e5ae82ae4` passed the final clean isolated Python3.11 suite with pinned SDK1.1.2: 4331 passed,3 skipped. This is an operator-run account inspection; agents have tested synthetic reads only. It uses the pinned Lighter SDK1.1.2 and read authentication, with an explicit no-op nonce manager. It never invokes create/cancel/sendTx/transfer/withdraw or mutation nonce methods.

The dedicated `.venv-hood` environment is already installed in the project checkout. From your own interactive terminal, run:

```bash
cd "/Users/daniilmakarov/Desktop/RISEx Spread Shadow"
.venv-hood/bin/risex-hood-handoff readiness \
  --symbol BTC --quantity 0.00020 --direction LONG \
  --source-account-index 27331 --receiver-account-index 27337 \
  --api-key-index 4 \
  --freshness-seconds 120 --request-timeout-seconds 10 \
  --keychain
```

Here `LONG` means receiver LONG and source SHORT. Quantity0.00020BTC is the owner-confirmed amount; the command rechecks current minimums rather than assuming the saved dollar estimate. The120-second freshness bound and10-second per-request timeout are explicit **diagnostic** settings allowing time for manual input; they do not set trading deadlines, select prices or grant execution. Age is measured at the final check, and key-entry delay does not renew old data. Change diagnostic bounds explicitly if needed and interpret a stale result accordingly.

Wait for the actual hidden-key prompt for the relevant account. With `--keychain`, a missing record prompts `Lighter private key for account … (hidden input; saved to Keychain):`. Enter the corresponding local API private key and press Enter, then repeat for the other account if prompted. Characters are not echoed. Do not paste keys into the shell before this prompt. With `--keychain`, API keys are persisted in protected macOS Keychain and loaded into process memory when needed; authentication tokens remain in memory. Without that option, keys also remain in memory only. Neither path stores keys in JSON configuration, arguments, environment variables or report files. HCR-10 also permits delivery to the assigned agent in the private owner-agent task/chat and an explicit return to the owner there; masked confirmation is the default. Rotate any key exposed outside that private boundary. Non-interactive input remains rejected by the existing CLI. Normal `--help` and old offline preview do not ask for keys or make requests.

This diagnostic does not need fabricated `market-evidence.json`, quantity-to-margin numbers, or trade-execution flags. Combining readiness with `--execute`, either live-operation acknowledgment, `--confirm-plan`, `--config` or `--market-evidence` is rejected before client/key creation. Optional `--source-limit-price` and `--receiver-worst-price` check prices you have already selected; this example intentionally leaves them UNSET. No launch-ready trading file is created.

Read the final JSON `checks` entries. `PASS` means the named condition was established, `BLOCKED` identifies a failed condition, `UNKNOWN` means missing proof/read failure, and `UNSET` identifies an unchosen trade parameter. The overall exit is0 only for READY, otherwise2. READY is read-only diagnostic completion, not a promise of execution or permission to trade; the report always has `execution_authorized:false`.

HCR-7B adds `market.margin_evidence` and `accounts.source/receiver.margin_evidence` to the same command's JSON. These retain returned margin settings, selected-row presence, account-wide order counts and provenance. `OBSERVED` means fields were retained, not that opening margin is sufficient. `INCOMPLETE`, `POSITION_ROW_ABSENT`, `UNAVAILABLE` and `INVALID` distinguish missing or conflicting evidence. Numeric margin units remain `unverified`; other-market rows and arbitrary account fields are not dumped. No extra command flags or key enrollment are required for this update.

The current official account fields expose balance and existing cross-margin requirement but do not establish the incremental margin of these planned orders. Therefore the real adapter reports `MARGIN_EVIDENCE_REQUIRED` / UNKNOWN for that calculation; available balance is not substituted for proof. Prices omitted above also remain UNSET. These are expected explicit limitations, not a failed installation. A separate justified margin calculation/evidence and operator-selected trading bounds are still needed before using the trading path. No live account/auth/order check has been performed by agents.

## Open opposite positions from balances — HCR-3

The new `PAIRED_OPENING` operation opens opposite perpetual positions from initially flat selected-market positions. It is separate from the default `CLOSE_REOPEN` operation, which still requires an existing source position. It does not transfer collateral, guarantee counterparty matching, or automatically close the resulting positions. The receiver needs its own collateral, and both legs require current opening-margin evidence. Accepted offline candidate `2b06a7b09109c2101e722103b93dc007e31fcdb9`: final clean isolated Python3.11 suite4312 passed,3 skipped; real account/signing/order execution remains NOT_RUN.

Use the complete Robinhood series configuration below and set these fields explicitly:

```json
{
  "mode": "series",
  "operation_mode": "PAIRED_OPENING",
  "market_symbol": "BTC",
  "direction": "LONG"
}
```

This is a configuration fragment, not a complete runnable file. Keep all required total/slice units, prices, deviation, timing bounds and unique private journal path from the full template. Use `operation_mode` for the operation and `mode: "series"` for sequential slicing; do not confuse these two fields. Omit `operation_mode` or set `CLOSE_REOPEN` to retain the older behavior.

| Receiver direction | Account A: maker limit | Account B: market IOC | Final positions after full Q |
| --- | --- | --- | --- |
| LONG | SELL, POST_ONLY, reduce-only false | BUY, reduce-only false | A SHORT Q; B LONG Q |
| SHORT | BUY, POST_ONLY, reduce-only false | SELL, reduce-only false | A LONG Q; B SHORT Q |

The owner's selected orientation is receiver LONG: A27331 opens SHORT and B27337 opens LONG. Both use API key index4 with separate keys. Keys may be entered through hidden local input/Keychain or supplied to the assigned agent in the private owner-agent task/chat under HCR-10; never place them in this file, Git/GitHub, configs, logs or evidence. Before the first attempt both selected-market positions must be exactly zero and readiness must be proven. HCR-10 authorizes one bounded attempt, not later series slices. Internal `expected_source_position`/`expected_receiver_position` fields are not operator settings and are rejected in JSON.

The requested first test is roughly10–15USD of BTC exposure per leg, not margin or transferred balance. No current executable BTC quantity or prices have been selected. Quantity inputs are BTC units; check current price, size step and minimums before converting the desired notional. Do not round up beyond the chosen exposure simply to satisfy a minimum. The slice logic, price bounds and stop/reconciliation behavior described below still apply. Source filling before B's admission blocks B; market matching and cancellation can race, leaving unequal positions that require operator attention. The program does not automatically compensate or close those positions.

Preview uses the same offline command with source account27331 and receiver account27337; it reports `operation_mode: PAIRED_OPENING`. Series live execution still requires the three explicit flags documented below. An ordinary terminal prompt is not a key-entry form. Hidden key input appears only when the operator-run program actually requests each key from an interactive TTY. There is no running secret-input process and no need to paste keys before that prompt exists.

Existing journals remain bound to their original source/configuration and must be preserved. A journal from a previous release cannot be assumed restart-compatible after this source change; resolve it with its original implementation rather than changing/removing bindings. Mode changes block reuse. Known recovered fills and parent completed-slice quantity can differ after a crash; remaining quantity is not a retry instruction.

## Robinhood Chain configurable perpetual series — HCR-2

Accepted offline and public-adapter verified (2026-09-15), candidate `393a17c01c73aced1735656c87ef9b4653b96653`: clean isolated Python 3.11 suite 4303 passed, 3 skipped. Real account/signing/order execution remains NOT_RUN. The older HCR-1 command below targets ordinary Lighter mainnet; use the Robinhood series configuration in this section for the requested deployment.

The requested website is https://robinhoodchain.lighter.xyz. Its official API is https://api.rh.lighter.xyz, and Lighter SDK 1.1.2 uses signing domain `466324` for it. This signing identifier is not the EVM wallet chain ID. The deployment must never silently fall back to ordinary Lighter mainnet. Official references: [Lighter deployment mapping](https://github.com/elliottech/lighter-agent-kit/blob/main/install.sh) and [pinned SDK signing domains](https://github.com/elliottech/lighter-python/blob/v1.1.2/lighter/signer_client.py).

HCR-2 selects a perpetual instrument by symbol, with BTC as the initial target and ETH also checked. The program resolves the current market identity rather than treating a historical numeric ID as permanent. Spot pairs and unknown symbols are rejected. Changing the symbol also requires reviewing quantity units, prices and market/account evidence; changing `BTC` to `ETH` does not convert the same numerical quantity into equivalent dollar exposure.

The total is split into sequential pairs. A reduces its existing position with a post-only limit and B opens the same direction with a market IOC. The desired slice is reduced when fresh observed depth within the configured price bounds is insufficient. Venue minimums and quantity increments apply to each slice. Sizes may differ as liquidity changes; random sizes alone do not improve execution. One pair must fully finish and reconcile before the next starts. Partial, unknown or conflicting execution stops the series. The series cannot guarantee direct matching, atomicity, price stability or lower total execution costs.

This closes and reopens exposure at a new entry price, not collateral or historical PnL. B needs its own adequate collateral. API private keys normally use hidden local input/Keychain; HCR-10 permits sharing only the required API keys with the assigned agent in the private owner-agent task/chat. Seed phrases, wallet recovery material and withdrawal credentials must never be shared. Public API checks have confirmed BTC/ETH availability and book responses on the requested deployment; real account readiness, signatures, order execution and receipts remain NOT_RUN.

### Configure the Robinhood series

Install with Python 3.11 and `pip install -e '.[hood-handoff]'` in a dedicated local virtual environment as shown below. The executable retains the name `risex-hood-handoff` for compatibility; `mode: "series"` selects the new configurable-market path. Some legacy help descriptions still say HOOD/one attempt; the series configuration and series execution flag below determine this mode.

Create a local `series-config.json`. The following is a **template, not a runnable trading configuration**: replace every angle-bracket placeholder with your explicitly chosen value. Quantities are units of the selected instrument (BTC for BTC, ETH for ETH), not dollars. Decimal quantities, prices and deviation remain JSON strings; time bounds and integer fields must be JSON numbers.

```json
{
  "mode": "series",
  "environment": "robinhood",
  "api_base_url": "https://api.rh.lighter.xyz",
  "chain_id": 466324,
  "market_symbol": "BTC",
  "direction": "LONG",
  "total_quantity": "<total units to close and reopen>",
  "desired_slice_quantity": "<maximum desired units per pair>",
  "allowed_price_deviation": "<relative fraction>",
  "source_limit_price": "<A limit price>",
  "receiver_worst_price": "<B worst acceptable price>",
  "freshness_seconds": "<replace with a positive number>",
  "request_timeout_seconds": "<replace with a positive number>",
  "order_timeout_seconds": "<replace with a positive number>",
  "reconcile_timeout_seconds": "<replace with a positive number>",
  "poll_interval_seconds": "<replace with a positive number>",
  "max_poll_count": "<replace with a positive integer>",
  "source_order_lifetime_seconds": "<replace with an integer, 300 through 2592000>",
  "api_key_index": "<replace with your integer key index, 4 through 254>",
  "client_order_prefix": "<unique operation label>",
  "journal_path": "<absolute path in a private local directory>"
}
```

`direction` accepts LONG or SHORT. Set `market_symbol` to ETH or another currently listed perpetual symbol to change instruments; review quantities, both prices, and evidence at the same time. `market_id` is optional: the adapter resolves the symbol from the current perpetual catalog; if an ID is supplied it must agree. No hardcoded BTC/ETH-only allowlist exists.

`allowed_price_deviation` is a fraction relative to A's configured limit price: the notation `"0.01"` means 1%, not a recommended setting. For LONG, B's buy ceiling is the tighter of the explicit worst price and A's price multiplied by `(1 + deviation)`, rounded down to the price step. For SHORT, B's sell floor is the stricter of the explicit worst price and A's price multiplied by `(1 - deviation)`, rounded up. A's price stays fixed throughout the series. The program does not automatically chase the market.

Each slice is the minimum of remaining total, desired slice, and observed receiver-side depth inside that bound, rounded down to the venue size step. The public read is bounded to 250 book records per side; it does not claim the complete available liquidity. The timestamp is the local response observation time, not a proven venue publication time. Quantities can differ with changing depth or the final remainder; randomization is not implemented. An insufficient minimum or unusable remainder stops visibly, without rounding upward. Splitting does not guarantee lower slippage.

Create `market-evidence.json` using the evidence fields documented below, with the selected symbol and current market ID instead of HOOD. Evidence must be current and applicable to both accounts and all planned slice sizes. Incremental margin requirements and provenance still require operator-provided evidence; this version does not automatically derive them. Do not use an invented zero or renew a timestamp without renewing its evidence. A long series can stop when this evidence becomes stale. The receiver needs its own collateral.

### Preview, local keys, and later operator execution

A and B below are distinct **integer Lighter account indices**, not wallet addresses. Each account uses its corresponding Lighter API private key; the configured key index is shared but the keys are separate. Use hidden local input/Keychain or the HCR-10 private owner-agent credential boundary; never place keys in Git/GitHub, files, logs, command arguments or evidence. An address alone cannot authorize this operation.

```bash
.venv-hood/bin/risex-hood-handoff run \
  --config series-config.json --market-evidence market-evidence.json \
  --source-account-index A --receiver-account-index B
```

The default preview makes no network request and asks for no keys. It checks configuration and prints the series inputs, but does not prove account readiness, full evidence validity, or executable slices. Review the source/receiver arguments and chosen direction and bounds separately.

Only for a later operator-run execution, add these three flags to that command:

```text
--execute --i-understand-series-live-operation --confirm-plan
```

These flags permit real signing and orders; agents have not used them. The one-attempt acknowledgment from the older HOOD example is not the series acknowledgment. For a first operator test, choose the total and desired slice explicitly after checking the current market and accounts. Even when total equals desired slice, thinner depth can split it into several sequential pairs. This version has no separate one-pair cap for series mode. No live size is preselected here.

### Series results and interruption

Read `outcome` and every child receipt, not only the process exit code. `completed_quantity` counts fully validated completed children. `remaining_quantity` is total minus that count; it is **not an instruction to blindly retry**. `actual_source_filled_quantity` and `actual_receiver_filled_quantity` separately report known cumulative leg fills, and can differ after a partial child. `actual_filled_quantity` is a paired quantity summary, not proof the two accounts matched each other. Economic fees/PnL remain separate in child results.

Keep the parent journal, every `.child-NNNN` journal, their locks, and the exact code/configuration/account inputs. Restart is read-only reconciliation with respect to orders: it never submits or cancels an order or starts the next child. It can authenticate/read accounts and append journal evidence. A previously completed series returns a blocked/UNKNOWN rerun result while retaining its original completed totals; a recovered interrupted child can have known fills while the overall series remains UNKNOWN. There is no automatic resume switch. Resolve any outstanding orders and position differences before separately choosing a new operation; do not remove journals to force replay. Account receipt/signing/execution compatibility remains live-unverified.


Current status (2026-09-15): HCR-1 BOTH/no-monetary-caps implementation `74fbbbf9f298648b643ce9a69353e3abcdcf6c27` passed independent review, 12 adverse fake-SDK probes and the clean isolated Python 3.11 suite (4289 passed, 3 skipped). Operator instructions follow. Live execution remains NOT_RUN. HCR-10 now grants the assigned execution agent narrowly bounded authority for the separate active HCR-9 Robinhood paired-opening attempt after its remaining acceptance/input/launch gates; it does not retroactively authorize this older HOOD example.

## HOOD close/reopen: operator instructions

The isolated `risex-hood-handoff` utility closes an agreed Q on source A and opens the same Q in the same direction on receiver B at a new entry price. Both LONG and SHORT are implemented. There are no upper gross-notional or fee-budget gates and no mandatory reference-price field. Live operation is NOT_RUN; acceptance covers offline code and fake SDK verification only. This is a two-account operation without guaranteed matching or atomicity. A can fill between the last resting check and B submission. Original entry price, realized PnL, funding and total economic PnL are not transferred or established by exposure SUCCESS.

### Install and prepare files

Use Python 3.11 in a dedicated environment from this checkout:

```bash
python3.11 -m venv .venv-hood
.venv-hood/bin/python -m pip install -e '.[hood-handoff]'
.venv-hood/bin/risex-hood-handoff --help
```

The adapter requires exactly `lighter-sdk==1.1.2` and fails closed if the SDK is missing or has another version. This optional dependency is separate from normal scanner startup. Use an owner-only journal directory (mode 0700). Keep every original journal and its lock file. Each new, independently chosen attempt uses a unique journal path.

Create `config.json` using this **offline fixture example**, replacing all market, account-key, quantity, price and timing choices with the operator's explicit values. These numbers are not recommended trading settings and market ID 7 is not a current HOOD identification claim. Quantity and prices must be decimal strings on the current venue grid.

```json
{
  "market_id": 7,
  "direction": "LONG",
  "quantity": "0.125",
  "source_limit_price": "100.25",
  "receiver_worst_price": "101.25",
  "freshness_seconds": 10,
  "request_timeout_seconds": 1,
  "order_timeout_seconds": 1,
  "reconcile_timeout_seconds": 1,
  "poll_interval_seconds": 0.1,
  "max_poll_count": 2,
  "source_order_lifetime_seconds": 300,
  "client_order_prefix": "owner-chosen-attempt",
  "journal_path": "/absolute/owner-only/unique-attempt.jsonl",
  "api_base_url": "https://mainnet.zklighter.elliot.ai",
  "api_key_index": 4,
  "chain_id": 304
}
```

`direction` is LONG or SHORT. `market_symbol` is HOOD and `environment` is mainnet by default. The CLI uses the same explicit API key index (4–254) for both accounts; each account has its own corresponding Lighter API private key. `source_order_lifetime_seconds` must be 300–2592000 seconds; the adapter converts the absolute expiry to milliseconds. All request/freshness/polling intervals must be finite and positive. Optional `auth_token_lifetime_seconds` defaults to 600 and supports 60–28800 seconds. These protocol ranges do not replace the operator's chosen finite deadlines.

Create a separate `market-evidence.json` containing current, non-secret evidence for this exact market and planned Q. It is a plain JSON object with these fields:

| Fields | Meaning |
| --- | --- |
| `market_id`, `symbol` | Exact current perpetual market identity; symbol HOOD. |
| `observed_at` | Original evidence observation time as Unix seconds; do not refresh the timestamp without refreshing the evidence. |
| `status`, `price_decimals`, `size_decimals` | Current active status and integer grid precision; the adapter cross-checks supplied values against `orderBookDetails`. |
| `minimum_base_amount`, `minimum_quote_amount` | Current documented minimums as exact decimal strings; no assumed minimum exemption. |
| `margin_evidence` | Non-empty provenance for the minimum/margin evidence. |
| `source_incremental_margin_required`, `receiver_incremental_margin_required` | Exact decimal strings for each planned operation's required margin, with current evidence; do not substitute current cross margin or an invented zero. |
| `source_incremental_margin_evidence`, `receiver_incremental_margin_evidence` | Non-empty provenance specific to each account and planned operation. |
| `source_fee_rate`, `receiver_fee_rate` | Optional evidenced decimal rates; omit or use null when unavailable. No fee-budget admission gate. |

The later operator-run adapter reads current account/position/active-order data and requires authorized, ready accounts, adequate margin, source exposure at least Q, and receiver flat or already in the chosen direction. It verifies metadata freshness and identity. The offline preview only checks configuration and prints the plan; it does **not** establish live readiness or validate the entire evidence object.

### Preview and one attempt

Replace A and B below with distinct integer account indices and use the same reviewed files throughout the attempt:

```bash
.venv-hood/bin/risex-hood-handoff run \
  --config config.json --market-evidence market-evidence.json \
  --source-account-index A --receiver-account-index B
```

This command makes no network request, imports no Lighter SDK and prompts for no keys. Review the account arguments as well as the printed direction, exact Q, prices and journal path.

| Direction | A: close source Q | B: open receiver Q |
| --- | --- | --- |
| LONG | SELL LIMIT, POST_ONLY, reduce-only | BUY MARKET, IOC; price is a ceiling |
| SHORT | BUY LIMIT, POST_ONLY, reduce-only | SELL MARKET, IOC; price is a floor |

For a later operator-authorized execution, add all three flags to the same command:

```text
--execute --i-understand-one-attempt-live-operation --confirm-plan
```

The CLI requests the two Lighter API private keys through hidden interactive TTY input. Never put keys in JSON, shell arguments, environment variables, receipts, logs or Git/GitHub. HCR-10 permits the required API keys only in the private owner-agent credential exchange for its separately bounded active attempt; that exception does not authorize this older command. The flags permit a real attempt; they are not a simulation mode. Agents did not run this historical command. No replacement, repricing, compensating trade, repeated completion or multi-wallet loop exists.

### Read the result and reconcile

Inspect `outcome`, both legs' exact fills, final positions/order statuses, `unknown_reasons`, `economic_status`, and the durable journal. Exit 0 also covers PARTIAL and PREVIEW, so it alone does not mean success. SUCCESS establishes the specified exposure change only. Missing fee receipts produce economic UNKNOWN while preserving proven exposure; PROVEN economics means observed gross/fees, not all-in PnL. Joint counterparty matching is a separate finding and may be UNKNOWN, KNOWN_ZERO or CONFLICTING.

After an interrupted or UNKNOWN attempt, retain the **same implementation, configuration, account arguments and journal path**. The same explicitly enabled command enters reconciliation-only for an unresolved journal; it may read/authenticate but never replays create or cancel operations. A completed journal rejects another attempt. A binding mismatch, missing evidence or active order can leave UNKNOWN. Do not delete/edit the journal, change its path to retry an unresolved attempt, or assume cancel acknowledgment proves cancellation. An unresolved resting source order may remain until separately handled by its operator or expired.

Old configuration fields `max_gross_notional`, `source_fee_budget`, `receiver_fee_budget` and `receiver_price_cap` are accepted only as validated legacy inputs and have no gating effect. Omit them in new configurations. **Journals from the earlier capped implementation do not migrate automatically:** their implementation/configuration binding differs, so this release returns UNKNOWN before reconciliation reads or mutations. Preserve them and use their exact original implementation/configuration for a separately authorized read-only resolution. Compatibility never permits replay.

The adapter follows the preserved official SDK/API evidence for version 1.1.2. Actual venue availability, receipt completeness, timestamp interpretation, incremental margin evidence and fees were not live-validated. Absent or conflicting evidence must remain visible; offline acceptance is no live execution guarantee.

Historical completed research (2026-09-09): the finite saved-pilot A/B/C comparison and independent review are complete. A (120-second fixed exit) closed 53 conditional episodes, B (300 seconds) closed 43, all net negative. C (120-second causal break-even repricing) has zero proven closures and undefined closed PnL; stale-data activation uncertainty and exact subminimum residues explain the recorded outcomes. Accepted C source f8b11ee37b8435ab11ad4118d3ebebd6662478a4 passed the clean isolated Python 3.11 suite: 4241 passed, 3 skipped. Historical baseline audit qualifications remain in STATUS and original evidence. This confirms bounded calculation correctness, not profitability. Final report/package: `spread-shadow-runs/scanner-v1-20260906/abc-comparison-20260909/chief/ABC-report-ru.md` and `abc-final-package-20260909-v1.tar.gz`. No further calculation or collection is authorized; see NEXT_TASK.

C is an opt-in offline kernel mode via `Scv1S1bKernel(..., exit_variant=S1bExitVariant.C_BE_REPRICE_V1)`; the default remains `A_B_FIXED_EXIT`. The evidence wrapper supplies the frozen causal decision/cancellation schedule and version exports. This is not a new public CLI switch. SYSTEM_SPEC contains the binding formula and timing; the reproduction package contains exact wrappers, commands, source archives and saved-output audit scripts. Historical commands bind original paths and may overwrite original log destinations: reproduce only with fresh output paths after checking the package instructions.

## Current and planned behavior — Scanner v1

The raw research path `record` -> `record-readback` -> `research-report` and one 60-second smoke/15-minute public pilot are complete. The recording is technically complete but only 42.7% data-eligible; the original report's early halts and zero closures do not establish strategy profitability. The later conditional evaluation and its independent audit are complete, with explicit limitations. Accepted corrections distinguish temporary taker-data gaps from uncertain maker execution and allow valid paired-position reduction while preserving exact non-executable residues. SYSTEM_SPEC defines this accepted behavior; NEXT_TASK records the completed audit and the next owner decision boundary.

The public-only BTC research CLI models RISEx maker SELL -> Lighter Standard taker BUY, full entry/hedge/exit episodes and exact residues. It uses four alternatives: TRADE_THROUGH_ONLY and TOUCH_ALLOWED, each with primary 500 ms delays or stress 1000 ms delays plus 1 bp on RISEx fills. Touch is an explicit conditional fill assumption, not actual execution evidence. New policy preserves partials, uses venue-local operation minimums/grids, accumulates within fixed Q_cap, respects 5-second quote cancellation and a 120-second first-fill completion deadline, and reports closed execution-only PnL separately from cashflows, marked open inventory and unknown funding. See SCV1-1 for the binding details.

MODEL_POSITIVE/MODEL_NEGATIVE are conditional, MODEL_SENSITIVE identifies assumption sensitivity, POLICY_BLOCKED identifies policy feasibility, DATA_INSUFFICIENT identifies missing/corrupt inputs and NO_EXECUTION_OBSERVED means no fills. Alternatives are not additive or independent; positive open marks are not closed profit. Funding UNKNOWN forbids all-in profitability claims. The first research pilot need not produce profit, but must have functioning saved-data/core/report paths, actual write/read validation, enforced resource limits and explicit loss/overflow status. Full former combined online capacity proof is deferred. Points = $0; legacy Funding Farmer stays frozen.

One Chief GPT-6 Astra medium independently reviews and integrates main; the visible GPT-5.6 Luna max Builder completed the separately owned utility in an isolated branch/worktree. No Builder assignment remains active. AGENTS defines stable process and safety, and NEXT_TASK defines current ownership and acceptance. Updates to main preserve active Builder checkouts and historical evidence. Actual model/effort/speed comes from client evidence, not prose.

The completed public authority was limited to one prospectively frozen60-second smoke and one15-minute pilot, with entry/requote cutoff at12:45 and135seconds for completion. Do not automatically repeat collection based on a zero/negative result. Further runs require owner direction. Public-only recording performs no strategy execution; private/account/fee-reader endpoints, credentials, signing/order preparation/dispatch, orders and real funds remain prohibited. WebSocket transport heartbeat remains enabled and never refreshes economic book timestamps.

The accepted Python fixture interfaces are `run_scv1_s1a` and `run_scv1_s1a_alternatives` in `risex_spread_shadow.scv1`; they reuse the existing cycle kernel and require saved/fixture inputs. The additional accepted offline interfaces `run_scv1_s1b`, `run_scv1_s1b_alternatives`, and `Scv1S1bKernel` implement S1b accumulation/marked inventory; `build_scv1_s1b_d1_report` and `render_scv1_s1b_d1_report` recompute saved inputs. The current recording/report CLI wraps the saved-data S1b path; historical D1 utilities remain offline development tools. Focused offline examples and assertions are in `tests/spread_shadow/test_scv1_s1a.py` (`python -m pytest -q tests/spread_shadow/test_scv1_s1a.py`).

## Saved recording to the first research table

`research-report` reads only a saved RP2 file and runs the accepted S1b kernel sequentially for all four alternatives. It makes no exchange requests. It does not use historical `cycle-report` decisions or the wider legacy observer policy.

```bash
risex-spread-shadow research-report /absolute/run/evidence.jsonl --output-json /absolute/run/research-report.json
risex-spread-shadow research-report /absolute/run/evidence.jsonl --format json
```

The default output is a readable Markdown table; `--output-json` additionally saves the complete report once (exclusive creation, mode0600). JSON includes compact source-book and fill references, decisions, positions, separate open marks, reasons, input SHA256 and implementation fingerprints. Repeat runs reproduce economic results; only offline timing changes. Invalid/corrupt input fails explicitly. An intact but incomplete recording is reported as INCOMPLETE, never silently promoted to an economic result.

Model decisions occur once per second from collection start. At an equal timestamp, already processed records precede the decision; their physical order is preserved. A saved BOOK becomes usable after its linked CHANGED processing result; trades require an explicit application processing timestamp. Old trade recordings without this field cannot establish availability and fail with TRADE_PROCESSING_TIME_UNPROVEN. No-change frames do not refresh economic timestamps. Public metadata setup time is separate from the new RUN_START collection clock.

For a recording planned for900seconds, new admissions and entry requotes stop at765seconds even if collection fails early. Existing orders retain their accepted cancellation rules; the135-second tail allows completion without fabricating fills or writing off residues. All four lanes share one input file and frozen SCV1-1 policy. The table separates data-eligible duration, scenario activity until a terminal block, blocked duration, and offline runtime. Activity here means the elapsed model interval from first admission until terminal block/end; it is not continuous fresh-data time or proof of execution. JSON also reports episode-active durations and exact eligibility reasons. Nested legacy readback coverage describes the normalized BOOK timeline; the research top-level eligible duration additionally requires the actual processing outcome and complete causal evidence. Funding stays UNKNOWN, points$0. When less than half the observed interval meets requirements, the report explicitly says the model is not evaluated by this stream; this descriptive label changes no trading guard.

## RP2 recording and readback

The existing CLI now implements `record` and `record-readback`. Recording is fixed to public BTC on RISEx/Lighter, one market,60or900seconds; it does not run a strategy. The saved-input S1b/report path and the single frozen smoke/pilot are complete. The syntax below remains available; further public runs require new owner direction.

Recording syntax:

```bash
risex-spread-shadow record --market BTC --duration-seconds 60 --store-root /absolute/owner-only-run-directory
```

The command prints the saved path and immediate readback. Use900seconds only for the later frozen15-minute pilot. `record-readback` makes no exchange requests. This exact fixture sample was written through both actual public adapters and the real store, then checked with the CLI:

```bash
risex-spread-shadow record-readback '/Users/daniilmakarov/Desktop/RISEx Spread Shadow/spread-shadow-runs/scanner-v1-20260906/rp2-chief-takeover/samples/normal/run-WtCyqsgb_FYBtIYRg8n8Bf6h/evidence.jsonl' --format table
```

Use `--format json` for exact `data_valid_duration_ns`, `data_ineligible_duration_ns`, eligibility-reason totals, receipt/outcome/book links and diagnostic intervals. The normal FIXTURE_ONLY sample has3changed messages,2unchanged, no loss; a two-second observation contains499999990ns eligible and1500000010ns ineligible. Unchanged messages preserve the old economic timestamp; timer/pong never refreshes it. COMPLETE means recording/readback completed, not that data was always fresh or a trading model was profitable. Queue loss/limits are INCOMPLETE; corrupt/missing links or terminals produce explicit readback errors. Exact duration totals survive truncation of the bounded detailed interval sample. Unknown processing/source/network causes remain UNKNOWN.

Other saved samples and their source/hash bindings are in `spread-shadow-runs/scanner-v1-20260906/rp2-chief-takeover/sample-manifest.json`. These are fixtures, not market observations or trades. Those fixture records are historical; current saved-pilot correction and evaluation gates are defined in NEXT_TASK.

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
