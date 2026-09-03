# Active bounded task

## DG-004 — Fillability Bounds Recovery Discovery

Status: `FROZEN / READY TO RUN ONCE`.

Objective: obtain one valid prospective public fillability/delayed-entry-edge verdict on the independently accepted lossless evidence path.

Exact measurement source: `cd741e2a46e874f1e77feebac2aba5c80a96455d`. No Builder or code change is authorized. Use one fresh owner-only store and exact `BTC/ETH/SOL`, both directions, `$100/$250/$500`, `1/2/3/5 bps`, `0/300/500/1000 ms`, frozen fee inputs, `25 s` freshness, and unchanged strict/optimistic semantics.

Stop on the first of `50` aggregate strict episodes, `500` unique eligible RISEx trades with relevant active quotes, `1200 s`, or integrity/fatal. Complete only the frozen Lighter horizon tail after sample stop. Enforce `2,500,000` records, `12 GiB`, and at least `24 GiB` free before start. Do not inspect economic output before terminal stop.

Acceptance: exact source/universe/config, clean unique terminal, no fatal/integrity/non-terminal transport gap, all model-scoped horizons, deterministic repeated report, owner-only permissions, complete per-policy snapshot/fillability/hedge/concentration report, and exact System Specification 2.9 verdict precedence.

Forbidden: any code, fee, quote grid, maker pricing, fill definition, eligibility, stop rule, horizon, venue, strategy, storage representation/cap, private/auth/credential/signing/write/testnet/mainnet, `SS-002`, or `SS-003` change; manual stop/extension or automatic retry.

After the terminal verdict, record it and stop. `SS-002` and `SS-003` remain closed; even `ENTRY_EDGE_CANDIDATE` permits only a later separate proposal.
