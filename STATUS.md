# Current status

## Accepted implementation — HCR-36

Implementation `7fee0570921f7844a0846f0ce589c150b0daf6e7`, based on accepted main `699e0a68791aa0cbd409cb5e200c588a9d056a10`, consolidates the five governing files and adds shared Russian terminal/Telegram lifecycle and financial views. One agent implemented and self-reviewed it; no Builder or independent review is claimed.

Messages show confirmed opening positions, hold start/duration and planned closing start in Moscow time; closing/recovery is separate from completion. Saved reports show exact fees and per-account/combined gross and net execution PnL when proved, with funding separate. Actual SDK maker/taker venue/integrator fields are preserved; two explicit zero components prove zero, unverified nonzero units remain UNKNOWN. Idle reports include newer terminal-launched cycles without clearing older unresolved execution barriers.

HCR-35 execution guards, route/size/hold selection, order policy, timing limits and operator configuration are unchanged. Before final quote, nonce/account preparation still overlaps and exact one-use context is retained; HOLD/SUCCESS still require full reciprocal own-order matching. Recovery to flat never establishes strategy success.

Verification: 396 focused checks, then 74 focused presentation/barrier checks for the later local-cycle finding. Final clean isolated Python 3.11.5 suite: **4956 passed, 3 existing optional Extended dependency skips, exit 0**, 149.39 seconds. Actual imports/package hashes, complete diff self-review and independent Fraction accounting cross-check are recorded. All 81 original input files and 68 additional owner-cycle files match their captured hashes. No new agent venue/account collection, trading-key access or orders occurred. Evidence: `spread-shadow-runs/hood-operator-clarity-20260922/chief-v1/`.

Deployment: The finished controller was refreshed only after taking the operator lock, proving no child process, and acquiring the instance lock after old PID 95899 exited. PID 3051 runs the tested source, holds the instance lock, has an established Telegram HTTPS connection and an empty startup log. The exact active intent remains unchanged. Existing restart logic sees multiple later slots and records BLOCKED without a single last-cycle pointer; the new view still shows the latest saved cycle. No state reset or `/run` was sent. Real lifecycle delivery awaits a future owner-requested run after the unresolved execution is separately resolved.

## Saved live evidence and current blocker

Cycle-011 on HCR-35 proved own-account opening of 0.00023 BTC on its second attempt and a 56-second hold. Closing used an external counterparty and source residual recovery. Strategy outcome remains PARTIAL; historical inventory is CONFIRMED_FLAT. Reconciled fill cash flows independently give gross execution PnL -0.001035 in quote currency (-0.000138 on 27337, -0.000897 on 27331). Fees/net PnL and funding remain unknown in the old receipts.

During development the owner independently ran cycles 012–018 on previous runtime 699e0a6. The last saved cycle-018 is UNKNOWN/INCOMPLETE: at 2026-09-22 21:14:38 MSK, source account 27331 was observed LONG 0.00020 BTC and receiver 27337 zero. These are historical observations, not a current account read. Later zero snapshots or local cycles cannot resolve older ambiguous intents automatically.

The controller retains the unresolved active intent originally created for cycle-012; multiple later local slots do not resolve it. New `/run` is blocked. Updating presentation does not reconcile execution, close inventory or clear this barrier. Any current account inspection/recovery is a separate explicitly authorized action. Preserve all original cycle directories and state; never delete them to enable another run.

## Remaining limits and deferred work

- Public-book checks cannot reserve counterparties. External fills/races remain possible; optimized REST stays active until equivalent owner/order/continuity stream proof exists. Offline tests do not establish live reliability.
- Nonzero official integer fee units need venue-specific proof before conversion. Old missing commissions cannot be reconstructed by new fields. Funding-inclusive PnL stays unknown without attributable funding evidence.
- One standing LIMIT consumed by multiple MARKET orders is deferred. Existing series creates separate paired slices; each fragment must meet venue minimums. Balance alone is not a proved constraint.
- Frozen scanner/research/Funding Farmer/old Telegram/testnet programs and quarantined fee reader remain outside scope. No continuing campaign, automatic agent follow-up or new trading run is assigned. History and earlier verification remain in Git and immutable evidence packets.
