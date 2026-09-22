# Current status

## HCR-37 — candidate verification in progress

Accepted base is main `235ca9c352a20a59bdc405e830906e5de922307b` (HCR-36). The solo HCR-37 candidate fixes guard/cancellation-race residual cleanup, transient paired-close preparation retries, fresh operator rearm and explicit `/close`, and adds concise per-order counterparty/timing messages. Self-review is in progress; no independent review is claimed. Final isolated Python 3.11 full suite and idle controller deployment remain pending.

Focused verification so far: 363 checks across engine, residual closure, prepared SDK/timing, recovery, Telegram and presentation, followed by 107 operator checks covering the final checkpoint view and local close entry point. No real orders were sent by the agent. Evidence: `spread-shadow-runs/hood-recovery-closing-20260922/chief-v1/`.

## Observed incidents

- Saved cycle-012 (source 27331 +0.00023 BTC), cycle-016 (27337 -0.00020), and cycle-018 (27331 +0.00020) each have a complete terminal external source fill and an undispatched unchanged receiver. A failed priority guard still made the phase UNKNOWN, so residual cleanup never ran. The candidate separates failed admission from completely proved execution, without overriding transport, identity, cancellation or history uncertainty.
- Opening and closing already sign both exact plans before source exposure. HCR-35 close dispatch gaps were 0.9103 s in cycle-011 and 0.8528 s in cycle-013; source-ack-to-receiver gaps were 0.4805/0.5448 s. Local receiver signing was under 1 ms in those samples. Cycle-011's receiver hit external account 39; cycle-013's source was filled by external account 24587 while the receiver filled zero. These are saved local intervals, not calibrated network/exchange-only timings.
- Cycle-014 lost closing priority on all three pair attempts and used separate residual closure. Cycle-017 skipped paired closure after one preparation timeout, unlike opening's transient retries. The candidate shares the existing three-attempt preparation budget on closing, only before a possible mutation.
- The old controller kept an administrative barrier from cycle-012 even after later runs/manual recovery. A fresh authorized read-only check proved both configured accounts flat with no active BTC orders and resolved all 74 old creation intents. The exact lookup omitted the old cycle-003 IOC, but bounded inactive history proved order 844424849590873 terminal `canceled-too-much-slippage`. No old journal or outcome was rewritten. The running HCR-36 controller still holds the old barrier until verified deployment.

## Remaining limits

The public book cannot reserve counterparties. External matches remain possible after the latest observation, and separate residual MARKET closure normally uses public liquidity. Offline tests do not establish live strategy success. Size, route, hold, price bounds, fee assumptions, minima and risk thresholds are unchanged.

Fee completeness remains separate from execution. Missing old fees and unverified nonzero venue integer units remain unknown; net PnL and funding cannot be invented. Explicit recovery closure lacks attributable entry history and does not claim full position PnL.

One standing LIMIT consumed by several MARKET fragments remains deferred. Frozen scanner/research/Funding Farmer/old Telegram/testnet modules remain outside this task. No campaign, autonomous follow-up or agent trading run is assigned.
