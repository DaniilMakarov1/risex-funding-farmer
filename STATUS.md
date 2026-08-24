# Current status

## Central baseline

- Local `main` is `e6bb562cc22750554ec0c69d67e6fc425a99cbe5`; Python 3.11 full suite: `1537 passed`; isolated dependency check: clean.
- `origin/main` is still `ed6b0f076200b4f5316cd2341e8d8a3e0e16c8b1`. Publication is blocked only by missing local GitHub authentication.
- RISEx, Nado, and Extended fixture lifecycle cores and isolated private-read operational adapters are integrated locally.
- Paper remains the default product. No strategy execution, mainnet, real funds, deposits, or ungated private/write traffic is authorized.

## Venue readiness

### RISEx

- The first operational private-read invocation ended fail closed in public barrier A: `BLOCKED — validation_failed` after exactly nine public GETs.
- Durable counters prove zero credential loads, signatures, private requests, orders, cancels, closes, or other writes.
- Public-only diagnosis found an official canonical zero `open_interest_limit`; the strict bounded decoder correction is accepted and integrated at `e6bb562`.
- The consumed invocation and store are immutable and must never be reset, rearmed, or retried. A fresh identity needs a new pre-arm and operational gate.

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
