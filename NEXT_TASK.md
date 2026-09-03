# Active bounded task

## DG-005 — Fillability Bounds Integrity Recovery Discovery

Status: `FROZEN / READY TO RUN ONCE`.

Objective: obtain one valid prospective public fillability/delayed-entry-edge verdict on the accepted terminal-integrity path.

Exact measurement source: `cdbc95c67adaf9df120c3ff07bb990dc37542ae3`. No Builder or code change is authorized. Use one fresh owner-only store and exact `BTC/ETH/SOL`, both directions, `$100/$250/$500`, `1/2/3/5 bps`, `0/300/500/1000 ms`, frozen fees, and `25 s` freshness.

Stop on the first of `50` aggregate strict episodes, `500` unique eligible RISEx public trades with relevant active quotes, `1200 s`, or integrity/fatal. Complete only the frozen Lighter horizon tail. Enforce `2,500,000` records, `12 GiB`, and at least `24 GiB` free before start. Do not inspect economic output before terminal stop.

Acceptance: exact source/universe/config, unique contiguous indices, exactly one physically-last clean `RUN_STOP`, no `RUN_FAILED`, fatal/integrity/non-terminal transport gap, complete model-scoped horizons, deterministic repeated report, owner-only permissions, full per-policy snapshot/fillability/hedge/concentration report, and exact System Specification 3.1 verdict precedence.

Forbidden: any code, fee, quote-grid, maker-pricing, fill, eligibility, stop, horizon, venue, strategy, protocol-acceptance, storage-representation/cap, private/auth/credential/signing/write/testnet/mainnet, `SS-002`, or `SS-003` change; manual stop/extension or automatic retry.

After the terminal verdict, record it and stop. `SS-002` and `SS-003` remain closed; even `ENTRY_EDGE_CANDIDATE` permits only a later separate proposal.
