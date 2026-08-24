# Current status

## Central baseline

- Published `main == origin/main == 867ac006c7f92bbbf9529ff4385b7ae1f08036fe`.
- RISEx, Nado, and Extended fixture lifecycle cores and isolated private-read operational adapters are integrated on published `main`.
- Paper remains the default product. No strategy execution, mainnet, real funds, deposits, or ungated private/write traffic is authorized.

## Venue readiness

### RISEx

- The one-shot op011 private-read gate is operationally accepted on the exact published tree: schema 8, `PASSED — complete`, all 43 counters complete, both strict public sweeps complete, and the authenticated orders plus fresh official positions snapshots prove zero orders and exact flatness. Both redacted public fingerprints are canonical and may differ; all structural witnesses are null.
- Every RISEx private-read invocation/store through op011 is consumed and immutable. None may be reset, rearmed, retried, or used as authority for a write.
- Private-read readiness is accepted. The fixture-only bounded RISEx lifecycle is integrated, but no production live-write candidate, Builder, credential access, signature, nonce consumption, order, cancel, or close is authorized yet.
- The sole next RISEx gate is a separate Tier C candidate for the already-approved minimum-size testnet lifecycle. Success still requires authoritative zero open orders and exact flatness; `FAILED_HALTED_MANUAL_RECOVERY` is failure and never readiness.

### Nado

- Deterministic private-read and operational adapter are integrated.
- The earlier invocation without durable evidence remains permanently `UNKNOWN`; its identity, signature, timing, and any associated artifacts must never be reused as retry authority.
- The fixed invocation/store remain unused. Operation is blocked because the fixed owner identity and 32-byte sign-only credential capability are not securely provisioned as owned regular `0600` files.

### Extended

- Deterministic private-read and operational adapter are integrated.
- The fixed invocation/store remain unused. Operation is blocked because the fixed API-key capability and account/subaccount identity are not securely provisioned as owned regular `0600` files.

## Exit condition

- No venue is strategy-ready yet: none has passed its real bounded testnet place/reconcile/cancel/close gate with authoritative zero open orders and exact flatness.
- After all three venues pass independently, infrastructure work stops and a separate strategy-testnet measurement task may begin.
