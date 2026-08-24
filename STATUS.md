# Current status

## Central baseline

- Published `main == origin/main`; Git is the exact accepted-history authority.
- All three fixture lifecycle cores are integrated. Paper remains default; no venue is strategy-ready.

## Venue readiness

### RISEx

- Private-read op011 passed with complete counters, authoritative zero orders, and exact flatness; all invocations through op011 are consumed.
- Tier C remains blocked until official sources prove the exact place/cancel identity mapping needed for deterministic reconciliation. No live write or retry is authorized.

### Nado

- The fixed identity is securely provisioned; Ink Sepolia gas and test USDT0 collateral are available, with exactly 10 test USDT0 deposited.
- Private-read op003 stopped before credentials after a public catalog mismatch. A later single official public diagnosis received HTTP 403 and made no code change. The next action waits for public gateway availability; consumed invocations are never reused.

### Extended

- The fixed identity and API-key capability are securely provisioned. Account-shape witness op003 completed `CAPTURED` with every counter exactly `1/1` and no write.
- The official account response is `{status, data}`, with the accepted account fields inside `data`. The next candidate must correct only this fixture/private-read parsing boundary before a fresh one-shot private-read gate.

## Exit condition

- Each venue must independently pass one bounded testnet place/reconcile/cancel/close lifecycle ending in authoritative zero open orders and exact flatness. Only then may a separate strategy-testnet task begin.
