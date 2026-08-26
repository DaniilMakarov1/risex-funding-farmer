# Active bounded tasks

One slice per venue. Apply the A/B/C levels and safety core from `AGENTS.md`; operational run IDs are data, not source milestones.

## RISEx — fill-seeking taker lifecycle

Status: `OBSERVED TESTNET ZERO-FILL BLOCKER`.

- The official minimum-size `MARKET+IOC` opening was accepted with an order ID and reconciled terminal, but filled zero and remained exactly flat; two fresh authoritative observations confirmed zero orders, consumed nonce, and exact flatness before local outcome recovery. Place/signing/taker encoding is proved. Do not repeat the same vector without materially changed public/testnet liquidity or new official execution evidence.
- RISEx remains short of the required filled-position close proof; no infrastructure expansion is authorized solely to work around absent testnet matching.

## Nado — sequential Level C lifecycle

Status: `AUTHORIZED MINIMUM-SEMANTICS/COLLATERAL CORRECTION`.

- The signed trigger read now uses the accepted 30,000 ms window; the actual write retains its 100 ms contract. Current catalog minima are accepted as published, while the actual target opening and clamped closing amounts remain exactly step-aligned and fully safety-checked.
- All three invocations stopped with zero intents and zero venue writes. Fixed BTC product 2 now has a minimum notional far above USD 500; credentials/private access and signing remain valid.
- Product 44 `SKR-PERP_USDT0` is accepted at the current smallest executable 650-SKR amount. Its first post-only execute halted ambiguous; exact post-window order query returned code 2020/not found and repeated authoritative zero-order/zero-fill/exact-flat evidence allowed protected local completion recovery without replay.
- The accepted exact-ask and tick-aligned 10%-buffered IOC attempts each dispatched once and received complete venue rejection code 2011. The latest durable `REJECTED/HALTED` state prevents replay; exact post-window reads prove zero orders, positions, and fills plus `query_order` code 2020/not found.
- Official current semantics denominate `min_size` in USDT0. Every current live perpetual publishes a 100-USDT0 minimum, while the runner misreads that field as a base amount, separately hard-codes USD 5, and submitted about USD 5.55 against only about 10 USDT0 collateral. Product 44 is currently `live`, so spread and market status are excluded.
- In a fresh visible Nado Builder worktree, correct only the current USDT0-denominated minimum-size binding and its focused regressions while preserving the accepted 10%-buffered IOC, exact product identity, signing, durable intent-before-dispatch, ambiguity no-replay, bounded close, and final zero-order/exact-flat barriers. Do not perform a write.
- After acceptance, Chief obtains and deposits enough free testnet USDT0 through the official Nado testnet path, archives the exact terminal database, proves authoritative zero/flat pre-state, and runs one fresh sequential lifecycle.

## Extended — wallet/API Management setup and lifecycle

Status: `PROVIDER STREAM BLOCKED`.

- Existing local owner/Stark identities and the sole subaccount match; testnet claim completed, balance is readable, and zero orders/positions are authoritative. A fresh REST-valid API key reproduced v1 HTTP 503 and v2 RPC HTTP 404, excluding wallet, collateral, stale key, and quota causes.
- The local Level B runner now durably classifies pre-upgrade failures as sanitized `HTTP` or `TRANSPORT`; automated stream retries remain stopped after the observed provider v1 HTTP 503 and v2 HTTP 404.
- Extended Level C remains prohibited until a future authenticated private stream succeeds.

## Completion

- Each venue receives one separate smallest-executable Level C lifecycle without a fixed USD ceiling. Strategy work begins only after all three finish with authoritative zero orders and exact flatness.
