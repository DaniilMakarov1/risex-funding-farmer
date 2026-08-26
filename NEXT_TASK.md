# Active bounded tasks

One slice per venue. Apply the A/B/C levels and safety core from `AGENTS.md`; operational run IDs are data, not source milestones.

## RISEx — fill-seeking taker lifecycle

Status: `AUTHORIZED OBSERVED-WRITE CORRECTION`.

- The accepted bounded crossing `LIMIT+IOC` opening dispatched once but again terminated with zero fill and exact flatness; its local terminal outcome was recovered only after two fresh authoritative zero-order/flat/consumed-nonce observations. Do not repeat it unchanged.
- In a fresh visible RISEx Builder worktree, preserve sanitized explicit place-response failure evidence and select the smallest officially/observably supported taker opening actually capable of filling. The testnet spread is not a blocker. Preserve minimum/grid/depth checks, durable identity before dispatch, no replay, authoritative reconciliation, bounded close, and final zero relevant orders plus exact flatness.
- After acceptance, Chief archives only the exact recovered terminal database and runs one fresh sequential lifecycle.

## Nado — sequential Level C lifecycle

Status: `AUTHORIZED OBSERVED-WRITE CORRECTION`.

- The signed trigger read now uses the accepted 30,000 ms window; the actual write retains its 100 ms contract. Current catalog minima are accepted as published, while the actual target opening and clamped closing amounts remain exactly step-aligned and fully safety-checked.
- All three invocations stopped with zero intents and zero venue writes. Fixed BTC product 2 now has a minimum notional far above USD 500; credentials/private access and signing remain valid.
- Product 44 `SKR-PERP_USDT0` is accepted at the current smallest executable 650-SKR amount. Its first post-only execute halted ambiguous; exact post-window order query returned code 2020/not found and repeated authoritative zero-order/zero-fill/exact-flat evidence allowed protected local completion recovery without replay.
- In a fresh visible Nado Builder worktree, make only the smallest correction that (a) uses a bounded taker opening capable of filling and (b) preserves a sanitized explicit execute failure class/evidence needed to distinguish terminal venue rejection from transport ambiguity without retaining raw bodies or weakening no-replay. Preserve exact product identity, signing, durable intent-before-dispatch, bounded cancel/close, and final zero-order/exact-flat barriers.
- After acceptance, Chief archives only the exact recovered terminal database and runs one fresh sequential lifecycle.

## Extended — wallet/API Management setup and lifecycle

Status: `PROVIDER STREAM BLOCKED; REPORTING CORRECTION AUTHORIZED`.

- Existing local owner/Stark identities and the sole subaccount match; testnet claim completed, balance is readable, and zero orders/positions are authoritative. A fresh REST-valid API key reproduced v1 HTTP 503 and v2 RPC HTTP 404, excluding wallet, collateral, stale key, and quota causes.
- The fresh Level B completed its first three REST reads but failed before stream upgrade/frame and persisted only `UNEXPECTED_FAILURE`. Automated stream retries are stopped. In a fresh visible Extended Builder worktree, make only the local Level B reporting correction that classifies WebSocket handshake/HTTP and pre-upgrade transport failures into the required sanitized failure classes; do not make authenticated calls or change the accepted host/parser/account semantics.
- Extended Level C remains prohibited until a future authenticated private stream succeeds.

## Completion

- Each venue receives one separate smallest-executable Level C lifecycle without a fixed USD ceiling. Strategy work begins only after all three finish with authoritative zero orders and exact flatness.
