# Active bounded task

## `DG-001` — real-public Entry Viability discovery and verdict

Status: `AUTHORIZED / RULES FROZEN BEFORE SAMPLE`.

Exact accepted source: `9ac7b73941b9f0217cfa6a2ef68b21d6040fd015` plus the governance-only commit that freezes this gate. The measurement code must remain byte-for-byte unchanged during discovery.

Objective: run one public-only observational discovery on the exact `BTC/ETH/SOL` universe and issue exactly one terminal Entry Viability verdict under System Specification 2.3 section `0.5`. This is an operational evidence task, not an implementation slice.

### Required run

- Use the accepted `risex-spread-shadow` CLI, both directions, all `24` policies per market, and all `0/300/500/1000 ms` horizons.
- Use one fresh unpredictable run ID and fresh owner-only append-only store. No smoke, fixture, replay, legacy state, private/authenticated endpoint, credential, order, signing, dispatch, testnet/mainnet write, or strategy execution is permitted.
- Run for at most `60 seconds`; admit exactly `BTC`, `ETH`, and `SOL`; stop and classify per `DG-001` if a fatal/integrity rule occurs. Persist at most `250,000` records. At most the first `50` strict `WOULD_FILL` records by `record_index` enter the verdict sample; later in-flight records are retained but excluded.
- Preserve the frozen fees, `25 s` freshness, exact sizing/economics, no-lookahead deadlines, named hedge outcomes, and terminal stop semantics. Do not change thresholds after observing any discovery record.

### Acceptance and terminal output

- Verify exact source identity, clean Git, run/store identity and permissions, all three market identities, metadata, record count, clean stop/fatal state, gap reasons, strict episode cap, four-horizon completeness, deterministic offline report, and exact per-policy component metrics.
- Apply the fixed verdict precedence in `SYSTEM_SPEC.md` without optimization or retrospective route selection. Emit exactly one of `ENTRY_EDGE_CANDIDATE`, `NO_SNAPSHOT_EDGE`, `LATENCY_DESTROYS_EDGE`, `PROFITABLE_QUOTES_UNFILLABLE`, `FILLABILITY_INSUFFICIENT_EVIDENCE`, `LIGHTER_DEPTH_UNSUITABLE`, or `DATA_INSUFFICIENT`.
- Record the evidence-backed verdict and concise accepted state in `STATUS.md`/`NEXT_TASK.md`; commit and publish governance. Do not open or start `SS-002` in this task.

Completion: the Chief independently validates the immutable evidence, records one exact verdict, confirms `SS-002` remains closed, and ends the Entry Viability mission.
