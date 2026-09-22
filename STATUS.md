# Current status

## Accepted baseline

Main before HCR-36 is `699e0a68791aa0cbd409cb5e200c588a9d056a10`; runtime implementation is `22453a1a42c5571826b1e511136c6c478ebb3f72` (HCR-35). Preparation reserves nonces/loads accounts before the final quote, overlaps SDK account/orders reads, and consumes exact validated context once. Exact source-priority admission remains mandatory. HOLD and paired SUCCESS require full reciprocal own-order matching; residual recovery to flat remains distinct from strategy success.

That implementation passed the final clean isolated Python 3.11.5 suite: 4906 passed, 3 existing optional Extended dependency skips. It was implemented and self-reviewed alone, not independently reviewed. Evidence: `spread-shadow-runs/hood-fast-execution-20260922/chief-v1/`. Offline controlled timings are not live fill-probability measurements.

## Latest saved operator evidence

The owner independently ran cycle-011 on HCR-35 (runtime provenance 699e0a6). Its second opening attempt proves mutual own-account execution of 0.00023 BTC. The confirmed hold was 56 seconds. Closing executed the receiver against an external participant; source residual recovery followed. The complete-cycle strategy outcome is PARTIAL, while final historical inventory is CONFIRMED_FLAT. This proves the successful opening, not reliable exclusion of external participants over a whole cycle.

Historical receipts omit actual fees, so net execution PnL remains unproved. These are saved observations, not a current account inspection. Cycle directories remain consumed/immutable; no run is authorized by this description. Prior incidents and verification history remain in Git and their original evidence packets.

## HCR-36 in progress

Owner requests solo documentation consolidation and clearer terminal/Telegram lifecycle and financial reports. Work is on `codex/spread-v1-hcr36-operator-clarity`; NEXT_TASK holds acceptance. New evidence: `spread-shadow-runs/hood-operator-clarity-20260922/chief-v1/`. No new agent-initiated trading, account/market collection or trading-key access is authorized/performed.

Current changes preserve actual role-specific fee fields, distinguish known zero from unverified nonzero units, calculate closed gross/net execution PnL separately from funding, and show Moscow hold/closing times. Focused verification: 396 passed in Python 3.11.5. Full diff self-review is complete; the final clean isolated suite and deployment are still pending. Candidate is not yet accepted/deployed. Historical inputs are unchanged.

At the last verified baseline deployment the idle controller was PID 95899 with preserved owner-only state. This is a historical process observation, not a current liveness guarantee; verify idle state and locks before any refresh. Do not kill an active cycle or issue a test `/run` to validate deployment.

## Remaining limits and deferred work

- A public-book check cannot reserve the counterparty. External fills/races remain possible; optimized REST stays active until equivalent owner/order/continuity stream evidence exists.
- Missing fees cannot be reconstructed by adding fields to new code. Nonzero official integer fee units need venue-specific proof before conversion. Funding-inclusive PnL remains unknown without attributable funding evidence.
- One standing LIMIT consumed by multiple MARKET orders is deferred. Existing series creates separate paired slices; venue minimums apply to each fragment. Balance alone is not a proved blocker.
- Frozen scanner/research/Funding Farmer/old Telegram/testnet modules and quarantined fee reader remain outside current scope. No continuing campaign, automatic agent follow-up or new trading run is assigned.
