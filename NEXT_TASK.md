# Active bounded tasks

One slice per venue. Apply the A/B/C levels and safety core from `AGENTS.md`; operational run IDs are data, not source milestones.

## RISEx — fill-seeking taker lifecycle

Status: `OBSERVED TESTNET ZERO-FILL BLOCKER`.

- The official minimum-size `MARKET+IOC` opening was accepted with an order ID and reconciled terminal, but filled zero and remained exactly flat; two fresh authoritative observations confirmed zero orders, consumed nonce, and exact flatness before local outcome recovery. Place/signing/taker encoding is proved. Do not repeat the same vector without materially changed public/testnet liquidity or new official execution evidence.
- RISEx remains short of the required filled-position close proof; no infrastructure expansion is authorized solely to work around absent testnet matching.

## Nado — sequential Level C lifecycle

Status: `AUTHORIZED OBSERVED-WRITE CORRECTION`.

- The signed trigger read now uses the accepted 30,000 ms window; the actual write retains its 100 ms contract. Current catalog minima are accepted as published, while the actual target opening and clamped closing amounts remain exactly step-aligned and fully safety-checked.
- All three invocations stopped with zero intents and zero venue writes. Fixed BTC product 2 now has a minimum notional far above USD 500; credentials/private access and signing remain valid.
- Product 44 `SKR-PERP_USDT0` is accepted at the current smallest executable 650-SKR amount. Its first post-only execute halted ambiguous; exact post-window order query returned code 2020/not found and repeated authoritative zero-order/zero-fill/exact-flat evidence allowed protected local completion recovery without replay.
- The accepted exact-ask IOC correction dispatched once and received complete venue rejection code 2011; the durable `REJECTED/HALTED` state prevents replay. Account collateral/health is about 10 USDT0 and all orders/positions remain zero. Official current quickstart guidance for a market buy uses IOC with a price 10% above the current price.
- In a fresh visible Nado Builder worktree, use the smallest tick-aligned official 10%-buffered IOC buy bound and make the outer sealed report preserve the already-durable sanitized venue-rejection class/code instead of collapsing it into `OPERATIONAL_PREREQUISITE_FAILED`. Preserve exact product identity, signing, durable intent-before-dispatch, ambiguity no-replay, bounded close, and final zero-order/exact-flat barriers.
- After acceptance, Chief archives only the exact recovered terminal database and runs one fresh sequential lifecycle.

## Extended — wallet/API Management setup and lifecycle

Status: `PROVIDER STREAM BLOCKED`.

- Existing local owner/Stark identities and the sole subaccount match; testnet claim completed, balance is readable, and zero orders/positions are authoritative. A fresh REST-valid API key reproduced v1 HTTP 503 and v2 RPC HTTP 404, excluding wallet, collateral, stale key, and quota causes.
- The local Level B runner now durably classifies pre-upgrade failures as sanitized `HTTP` or `TRANSPORT`; automated stream retries remain stopped after the observed provider v1 HTTP 503 and v2 HTTP 404.
- Extended Level C remains prohibited until a future authenticated private stream succeeds.

## Completion

- Each venue receives one separate smallest-executable Level C lifecycle without a fixed USD ceiling. Strategy work begins only after all three finish with authoritative zero orders and exact flatness.
