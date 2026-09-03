# Active bounded task

## DG-003 — Fillability Bounds Discovery

Status: `FROZEN / READY FOR CHIEF PUBLIC-ONLY RUN`.

Objective: run exactly one prospectively frozen public-only sample that resolves whether the accepted profitable hedge-anchored RISEx quotes are unreachable even under the optimistic upper bound, remain bracketed by public-data fill uncertainty, or produce material strict fills with delayed Lighter hedge evidence.

Exact source: `10cc7be7b58c536fc8edf65309b13e9a9d8d819b`. Chief executes the accepted public unauthenticated runner directly; no Builder, code edit, or sample tuning is authorized.

Frozen surface: exact `BTC/ETH/SOL`, both directions, `$100/$250/$500`, `1/2/3/5 bps`, `0/300/500/1000 ms`, `25 s` freshness, RISEx maker `0.00005`, Lighter taker `0`, strict lower bound unchanged, optimistic at-or-through upper bound, and the full per-policy/concentration report in System Specification 2.7.

Frozen stop and storage rule: first of `50` strict episodes, `500` unique eligible trades, `1,200 s`, or integrity/fatal. Freeze RISEx economics at stop and retain only the bounded Lighter tail for pending horizons. Maximum `2,500,000` records and `12 GiB`; require at least `24 GiB` free before launch. One fresh owner-only store, exact source metadata, exactly one terminal marker, and two byte-identical canonical reports are required.

Verdict: apply the exact System Specification 2.7 precedence. `PROFITABLE_QUOTES_UNFILLABLE` requires `500` eligible trades and both bounds at most `1`; `FILLABILITY_INSUFFICIENT_EVIDENCE` requires `500` eligible trades, non-material strict evidence, and optimistic evidence above `1`; delayed entry verdicts require at least `10` strict episodes for one exact policy across at least `5` detection timestamps plus the frozen hedge/edge thresholds. Any failed operational/completeness gate is `DATA_INSUFFICIENT`, not an economic verdict.

Required output: exact run/store identity and hashes; stop reason; record/byte counts; integrity/completeness; per-policy snapshot, distance, fillability, both model curves, and concentration; exactly one frozen verdict; durable status update. Do not extend or rerun because results look favorable or unfavorable.

Forbidden: any code/config/threshold tuning after sample open; another venue; private/authenticated data; credentials/signing/write/testnet/mainnet; queue/FIFO/L3/probability/ML; fees, quote grid, maker pricing, strategy, funding, inventory, exits, SS-002, or SS-003. A second public run requires a newly recorded diagnostic reason and separately frozen gate; it is not automatic.
