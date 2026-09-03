# Active bounded task

## DG-002A — Corrected Measurement Stability

Status: `FROZEN / READY TO RUN`.

Objective: prove the accepted SS-001C public measurement path can admit exact `BTC/ETH/SOL`, run for the bounded duration, terminate without intervention, retain intact evidence, and render deterministically.

Exact source: accepted and published `b4f2822327fc0f7b50a02d7aabfc2d6e61b453a4`. No Builder or code change is authorized. Verification is the prospective public-only gate frozen in System Specification 2.4.

Run exactly once for `60 seconds` in a fresh owner-only store with a `250,000`-record cap and requested universe `BTC/ETH/SOL`. Require unassisted return within `10 seconds` after duration, exact source metadata, all three markets, one clean sole `RUN_STOP`, no failure/fatal/integrity class named by the frozen gate, owner-only permissions, and two byte-identical canonical offline JSON reports.

The run is stability evidence only. Preserve and report economic observations, but do not interpret them as the discovery verdict and do not tune the later gate from them.

Forbidden: any code, strategy, fee, quote-economics, quantity, fill-model, storage, private/auth/credential/signing/write/testnet/mainnet, venue, `SS-002`, or `SS-003` change.

Acceptance: every frozen success condition passes. Any failure blocks `DG-002B` and is recorded before a separately frozen diagnostic or rerun.

After a pass, record the result, then freeze `DG-002B` prospectively before its economic sample. The immutable `DG-001` verdict remains `DATA_INSUFFICIENT` and is never renamed or reused as economic evidence.
