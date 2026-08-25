# Active bounded tasks

One slice per venue. Apply the A/B/C levels and safety core from `AGENTS.md`; operational run IDs are data, not source milestones.

## RISEx — automate observed no-event/no-fill reconciliation

Status: `AUTHORIZED OBSERVED-DEFECT CORRECTION`.

- The first live runner invocation dispatched one intent once and then received no terminal order event. Post-expiry authenticated/private and full public evidence proved no order identity, zero open orders, exact flatness, and no unrelated state; local recovery completed through the accepted safe-no-identity lifecycle rule.
- In a fresh visible RISEx Builder worktree, make only the production runner consume that same authoritative post-expiry evidence after bounded event absence and reconcile `COMPLETED_NO_FILL_FLAT` without replay. Any pre-expiry, open-order, nonflat, unexplained, stale, or contradictory observation remains manual recovery. Preserve every write, close, and safety contract.
- After acceptance, Chief may run one further sequential RISEx testnet lifecycle to seek evidence of the filled-position close path; no repeat is allowed from ambiguous state.

## Nado — correct signed trigger-read freshness window

Status: `AUTHORIZED OBSERVED-DEFECT CORRECTION`.

- The accepted sealed runner's first live invocation stopped before any intent or venue write because its signed trigger query used the 100 ms write receive window. A fresh standalone Level B with the same credentials immediately finalized; its accepted signed read uses server time plus 30,000 ms.
- In a fresh visible Nado Builder worktree, reuse the accepted signed trigger-read time semantics and keep the 100 ms value only where the existing write contract requires it. Preserve strict response validation, no retry, no trigger write, and the empty pre-write runtime history; an invocation with zero intents is safe to retry without deleting its journal row.
- After acceptance, Chief runs the sealed runner once sequentially under the already-authorized `<= USD 500` Level C gate.

## Extended — provider/API Management resolution required

Status: `BLOCKED ON EXTERNAL ACCOUNT-STREAM ACCESS`.

- Stop automated retries: published SDK 2.5.0 plus compatible `websockets` reaches its source-configured old account-stream host and receives HTTP 503, while current official source/accepted runner reaches the new host and receives HTTP 403 with the same REST-valid key. No local transport correction is evidenced.
- User/provider action: in Extended Testnet API Management confirm the key was generated for the current Starknet Sepolia account/environment and is enabled for private account WebSocket streams; if the UI exposes no stream permission, ask Extended support to reconcile the PyPI 2.5.0 testnet stream host with current source and explain/clear the new-host 403 for this REST-valid key. Key rotation/reprovisioning remains unauthorized until the user chooses it. After provider-side change, rerun one fresh Level B before any write.

## Completion

- After private readiness, each venue receives one separate minimum-notional Level C lifecycle (`<= USD 500`). Strategy work begins only after all three finish with authoritative zero orders and exact flatness.
