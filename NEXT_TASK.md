# Active bounded task

## Cross-venue strategy testnet — read-only measurement foundation

Status: `READY FOR FRESH BUILDER`.

Objective: implement one isolated, paper/read-only measurement slice across the already accepted RISEx, Nado, and Extended venue boundaries. It must collect comparable evidence for funding, explicit fees, executable liquidity and spread, modeled slippage, observation/execution timing, stale-data rejection, and reconciliation health, then produce a deterministic paper decision with explicit leg-risk and kill-switch reasons.

Allowed scope:

- Reuse accepted venue read-only contracts and fixtures; add only the smallest venue-local adapters and a normalized immutable measurement/report boundary needed for comparison.
- Measure gross and fee-adjusted funding opportunity, executable size at bounded book depth, entry/exit slippage assumptions, timestamp age/skew, and a two-leg timing budget.
- Fail closed on stale, missing, contradictory, unrelated, non-finite, or non-comparable evidence. Report why a candidate is rejected without exposing secrets or raw authenticated payloads.
- Define deterministic paper-only gates for maximum leg imbalance, maximum observation age/skew, reconciliation health, and a kill switch. Tests must cover funding sign/direction, fee arithmetic, depth/slippage, stale data, partial-leg simulation, reconciliation contradiction, and kill-switch activation.
- Keep normal startup and all operational Level C runners unchanged. Use Python 3.11 focused/adverse tests and one clean full suite on the final candidate SHA.

Forbidden scope:

- No venue write, order preparation/signing/dispatch, credential use unless a separately bounded read-only gate requires it, real funds, mainnet endpoint, live strategy execution, scheduler/service/dashboard, generic OMS, automatic recovery, or new venue.
- Do not merge venue authentication, signing, nonce, wire identity, order/cancel/close, pagination, or private-event implementations. The accepted testnet REST-only Extended fallback is not a mainnet capability.
- Do not change product economics or safety invariants without a separate Chief gate based on official or observed evidence.

Acceptance:

- The report is reproducible from fixtures and sanitized captured observations, makes no write-capable object reachable, and returns either a fully quantified paper candidate or a specific fail-closed reason.
- All three venue adapters prove unit normalization and timestamp semantics independently; cross-venue arithmetic has exact Decimal behavior and no float path.
- Kill-switch and leg-risk simulation stop before a second paper leg whenever the first-leg or reconciliation evidence is partial, stale, ambiguous, or outside the declared bounds.

After acceptance, Chief may run a separate bounded read-only testnet measurement. Any strategy write, even on testnet, requires a new explicit authorization gate; mainnet remains Level D and unauthorized.
