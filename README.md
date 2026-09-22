# RISEx Spread Shadow

This standalone project contains the operator-run Robinhood Chain BTC cycle utility and frozen public/paper research tools. HCR-27 offline diagnostics and its completeness audit are implemented; STATUS.md records the exact accepted candidate and verification. Live validation was not part of that assignment. `NEXT_TASK.md` defines the finite authority and acceptance criteria, `STATUS.md` records accepted state, `SYSTEM_SPEC.md` defines behavior, and `AGENTS.md` defines process and safety. Historical experiments and incident narratives are retained in Git and immutable owner-only evidence, not operational permissions.

## Runtime and setup

Use Python 3.11. The cycle utility pins `lighter-sdk==1.1.2` and uses the project-local `.venv-hood`. For a new installation:

```bash
python3.11 -m venv .venv-hood
.venv-hood/bin/python -m pip install -e '.[hood-handoff,test]'
```

An existing installation does not need to be recreated. If it contains only runtime dependencies, install the test extras before using pytest: `.venv-hood/bin/python -m pip install -e '.[test]'`. The root `start` script selects `.venv-hood/bin/python` regardless of shell PATH and imports this checkout's source. Before reserving a cycle it validates local configuration, owner-only storage, market evidence and the pinned SDK. Help and pre-launch cancellation are offline and consume no cycle slot.

```bash
./start --help
.venv-hood/bin/risex-hood-handoff --help
.venv-hood/bin/python -m pytest -q
```

## Configured operator cycle

The existing operator directory is:

`spread-shadow-runs/hood-cycle-race-latency-20260920/operator-v1/`

`random-cycle.json` selects BTC market 1, source account 27331, receiver account 27337, API key index 4, Robinhood API/signing domain 466324, existing timing defaults and the explicitly selected incremental opening-margin deferral. `market-contract.json` binds identity/provenance requirements; current grid, minimums, balances and timestamps must come from actual validated observations. Do not invent missing fees or refresh saved timestamps.

The owner-operated command is:

```bash
cd "/Users/daniilmakarov/Desktop/RISEx Spread Shadow"
./start
```

The prompt describes a real Mainnet cycle. Enter starts it; `C` or `CANCEL` cancels. Before confirmation there is no Keychain/client/network access or cycle reservation. After confirmation the launcher loads protected credentials, atomically reserves the next unused `cycle-NNN` directory and prepares one cycle. Keep the terminal running through completion. Existing cycle-001 through cycle-007 are consumed and must never be cleared or reused; this list is historical, and the allocator checks actual directories.

The receiver opens LONG and the source opens SHORT with equal BTC quantity. Both selected-market positions must initially be exactly zero, with no pending BTC orders. Quantity is sampled once uniformly in legal integer size ticks, capped by the smaller fresh free balance without leverage multiplication. Hold is one sampled integer from 20 through 300 seconds; elapsed holding starts only after independently confirmed paired opening. Actual closure may take longer than the hold.

Opening and closing obtain their final quote after concurrent account/metadata reads, then revalidate the original observation ages. Random-cycle pricing requires an exclusive one-tick improvement; a one-tick spread that would merely join existing volume does not qualify. Preparation and safely reconciled paired retries share a maximum of three attempts per opening or closing. Price updates never redraw quantity or hold or loosen bounds.

Both exact orders may be prepared before source exposure. The source LIMIT/POST_ONLY must be confirmed resting before the receiver MARKET/IOC can be transmitted. Receiver admission requires fresh accounts, the exact zero-fill source order and its owner-bound public level, with no better-priced external volume or unproved same-price priority. An external fill can still race the final check; these checks do not guarantee an owned counterparty or atomic execution.

Closing reverses both sides with reduce-only orders. Proven cycle residuals use the existing reduce-only fallback only after own pending orders and cancellation-race fills are reconciled. Each residual attempt needs fresh bounds and a unique identity. Reconciled zero-fill attempts are paced; there is no fixed total count for fully reconciled residual attempts and no guaranteed completion time. Uncertain execution blocks dependent writes. The system does not adopt historical positions or start another opening automatically.

## Results and interruption

Preserve `cycle.jsonl`, child opening/closing journals, fallback receipts, configuration, claims and lock files. Never delete a failed slot, edit a journal or change paths to replay an uncertain operation. A missing terminal result is incomplete. A send timeout may mean the order reached the venue; it is not proof of rejection or zero fill.

Interpret these facts independently:

| Fact | Meaning |
| --- | --- |
| Paired execution | Whether opening/closing and required trade evidence proved the intended paired operation. |
| Confirmed flat inventory | Resolved terminal execution and causally agreeing final zero positions; isolated zero snapshots are insufficient. |
| Counterparty matching | Established only by exact trade receipts; flat inventory does not prove it. |
| Fees/economics | Missing actual-trade fees remain UNKNOWN, including fallback trades. Proven no execution can establish zero execution fees. |
| Funding and PnL | Unknown funding is not zero. Execution fees or cashflow alone do not establish total profit. |
| Observation time | Saved account facts are historical, never a current account check. |

After interruption, preserve all evidence and inspect it before any later operation. The random-cycle/simple launcher does not resume trading or automatically close historical inventory. Lower-level handoff/series restart interfaces permit only their existing bound read-only reconciliation; they do not authorize replay or a new cycle. A completed process or exit code 0 alone proves neither paired success nor profit.

## Offline saved-cycle report

The report reads a saved cycle directory or its `cycle.jsonl`; it makes no requests and does not resume execution. For example:

```bash
.venv-hood/bin/risex-hood-handoff report --path spread-shadow-runs/hood-cycle-race-latency-20260920/operator-v1/cycle-007
.venv-hood/bin/risex-hood-handoff report --path spread-shadow-runs/hood-cycle-race-latency-20260920/operator-v1/cycle-007 --json
```

Use `--format both` for human and JSON output; repeat `--path` to report separate cycles without adding their results. `paired_execution.direct_counterparty_match` reports optional exact mutual matching separately from completed exposure. The reader hashes and counts every input record while retaining bounded details; if required detail is omitted, the report is INCOMPLETE and aggregate execution/inventory/fee proofs remain UNKNOWN. Read the report's completeness and issues as well as its individual execution, inventory and fee conclusions. Process exit alone is not trading success. Saved observation times are historical. Latency stages can overlap; unavailable values and uncertain transport are explicit. Do not sum overlapping windows or infer network/exchange/signing time from an uninstrumented interval.

New journals measure quote-request elapsed time and per-order preparation lock wait, nonce acquisition, SDK signing-call elapsed time and transport roundtrip. Private source lookup and owner-bound public-book observation are separate: the first saved qualifying public snapshot gives an observation bound, not the exact exchange appearance time. Quote age is split at the plan and dispatch-intent boundaries when timestamps exist. Old journals cannot supply new measurements retroactively. Pure CPU, network-only and exchange-processing fractions remain UNKNOWN; signing-call elapsed time is not CPU time.

`order_state` lists latest saved order observations with their times, unresolved observed orders and unresolved mutation intents. These are historical facts; an empty list does not establish current flatness. Sanitized cycle-003/004/007 fixtures under `tests/fixtures/hood_handoff/` retain source and projection hashes. The report coverage map points to the corresponding incident/action-barrier and crash-boundary regressions.

## Credentials and account diagnostics

The launcher uses macOS Keychain. Matching entries are bound to API origin, signing environment/domain, account and key index. A missing key is requested through hidden interactive input; Keychain failure stops without plaintext fallback. Credentials never belong in arguments, environment variables, project files, reports or Git. Do not paste a key into an ordinary shell prompt.

The lower-level CLI supports `--keychain`, `--keychain-replace` and `--keychain-remove` as mutually exclusive options. Removal deletes only the matching local record, exits without an SDK client or requests, and does not revoke a venue key. Help and offline previews do not access Keychain.

The separate owner-run `readiness` command authenticates read-only account/market requests, never order submission or cancellation:

```bash
.venv-hood/bin/risex-hood-handoff readiness \
  --symbol BTC --quantity 0.00020 --direction LONG \
  --source-account-index 27331 --receiver-account-index 27337 \
  --api-key-index 4 --freshness-seconds 120 --request-timeout-seconds 10 \
  --keychain
```

These are diagnostic bounds, not trading thresholds. Read `checks`: PASS establishes the named condition; BLOCKED is a failed condition; UNKNOWN lacks proof; UNSET is an unchosen parameter. Exit 0 means diagnostic READY, not permission or a promise to trade. `execution_authorized` remains false. Current incremental opening-margin estimates may remain `MARGIN_EVIDENCE_REQUIRED`; available balance is not substituted for that proof.

The configured paired-opening margin deferral skips only missing local incremental-margin calculation/provenance. Missing values remain uncalculated; supplied invalid or insufficient estimates still block. It preserves current account margin/balance, identity, position, quantity, minimum and freshness checks. Readiness and CLOSE_REOPEN do not accept the deferral.

## Other utility interfaces

`local-attempt` is a separate fixed-quantity paired opening and does not gain random-cycle automatic closure. Its automatic proposal is selected after `LAUNCH`; explicit source and receiver bounds must be supplied together. The diagnostic packet and non-reusable attempt claim are retained even after a failure. Ordinary quote movement is not stale data; missing engine/storage completion remains incomplete.

`run` retains explicit configured single handoff and series interfaces. Default preview is offline; exact quantities, prices, timing, identities and market evidence must be supplied. No historical example grants new execution authority. Use `--help` and `SYSTEM_SPEC.md` for the existing interface contract; preserve the same binding for reconciliation. None of these interfaces provides two-account atomicity or native position transfer.

## Saved research tools and frozen boundaries

The public/paper scanner remains separate from `risex-hood-handoff`. Existing saved-input commands are offline:

```bash
.venv-hood/bin/risex-spread-shadow record-readback /absolute/run/evidence.jsonl --format table
.venv-hood/bin/risex-spread-shadow research-report /absolute/run/evidence.jsonl --format json
.venv-hood/bin/risex-spread-shadow cycle-report /absolute/campaign-root --format json
.venv-hood/bin/risex-spread-shadow scan-report /absolute/run/evidence.jsonl --format table
```

`research-report --output-json /absolute/new-report.json` additionally writes an exclusive owner-only report. Research scenarios are conditional alternatives, never additive or independent realized trades. Open marks and unfinished cashflows are not closed profit; funding remains separately unknown. Recording completeness is distinct from data eligibility and economic sufficiency.

Public recording/collection requires a new prospective gate in NEXT_TASK; completed smoke/pilot/CAL windows and claims cannot be reused. The RISEx authenticated fee reader remains quarantined. Legacy Funding Farmer, Telegram, and isolated venue testnet/private modules are frozen and grant no operational authority. This task does not reopen capacity research, old campaigns or another repository.
