# Current status

## Central baseline

- Git `main` is the accepted-history authority and is published to `origin/main` after every accepted Chief checkpoint. Paper remains the default; public unauthenticated mainnet shadow reads and bounded outbound-only Telegram delivery are authorized, while authenticated/private venue access, writes, real funds, inbound Telegram control, and strategy execution are not.
- RISEx, Nado, and Extended have each completed one bounded Level C testnet lifecycle ending with authoritative zero open orders and exact flatness. No additional venue lifecycle is authorized; the next gate is mainnet-public read-only shadow measurement through the normal paper product.
- The accepted tree keeps normal startup isolated from testnet operational runners. Protected credentials and operational journals remain runtime data outside Git.
- The user ended the initial Top-5 public-shadow baseline early after a safe zero-write run and authorized the next public-shadow window over every currently eligible route in `RISEx ∩ (Extended ∪ Nado)`, with liquidity-conditioned measurement rather than liquidity-based truncation. The accepted implementation still has the old Top-5/20-route limit, so a fresh bounded central Builder candidate is required before that window starts.

## Venue completion

### RISEx — Level C complete

- The accepted sealed ETH/USDC two-account coordinator uses separate durable identity domains for the primary and the one authorized isolated counterparty. Signed place/cancel writes use REST; public and authoritative read-backs use REST; authenticated private WebSocket evidence supplements but does not replace REST correctness.
- Entry completed with one counterparty `SELL LIMIT+GTC+postOnly` of `0.1 @ 2363.04` and one primary `BUY MARKET+IOC` of `0.1`; exact mutual order, trade, fill, and position evidence agreed. Exit completed with one counterparty reduce-only `BUY LIMIT+GTC+postOnly` of `0.1 @ 2599.85` and one primary reduce-only `SELL MARKET+IOC` of `0.1`; exact mutual evidence again agreed.
- Both journals are `COMPLETE`. Each contains two terminal intents with dispatch count one, total dispatch count two, and zero cancels. Two ordered final REST rounds have the same digest on both accounts. A separate fresh REST plus private-WebSocket observation confirmed zero open orders, exact flatness, no unexplained state, and nonce `(1,7)` on both accounts.
- Active journal SHA-256 values are `04a2957758a6fab646a0b213bd42eb2b4b16304ca6b55c9cec5a9ac5b6353776` for primary and `0f273ed5116e01d9ba9eabf02f5e8d435371e952daaa58726b3ea243c5c4d3fa` for counterparty; both pass SQLite integrity checks. Protected byte-identical pre-recovery backups preserve the post-exit halted state. No trade order was replayed and no recovery venue write occurred.

### Nado — Level C complete

- One entry and one guarded reduce-only close filled. Independent authoritative evidence proves zero regular orders, zero trigger orders, and exact flatness. The durable lifecycle is complete; no new Nado lifecycle is authorized before the strategy gate.

### Extended — Level C complete

- Entry order `2092620936462331904` and reduce-only close `2092632701983932416` filled and reconciled. Two authoritative final rounds prove zero orders, exact flatness, and no unrelated state. The active database is `COMPLETE` with SHA-256 `4c5dacadd83a222cd8cde7297aa4f54cc6f3bda80bd8018d6acfa7adf3e105d1`.
- The accepted strict REST-only fallback is testnet-only while the entire testnet WebSocket ingress is unavailable. Extended private WebSocket proof remains mandatory for any future mainnet Level D claim.

## Bounded 3/3 commonality review

- Common proven safety semantics are: durable unique intent identity before dispatch, one-way dispatch state, no blind replay, exact authoritative order/fill/position reconciliation, unrelated-state rejection, reduce-only flattening, and two ordered agreeing terminal zero-order/exact-flat rounds.
- Common measurement inputs are funding, fees, executable top-of-book depth, spread/slippage, response and execution timestamps, stale-data status, position/order state, and reconciliation outcome. These can share a normalized observation/report boundary without sharing venue signing or write engines.
- Venue authentication, signing, nonce/wire identity, order/cancel/close encoding, pagination, private-event transport, and recovery rules remain materially different and must stay venue-specific. The 3/3 evidence does not justify a generic OMS or a shared execution framework.
- Infrastructure expansion stops here. The next work uses real unauthenticated mainnet market data in the existing read-only paper runtime to establish opportunity frequency, conservative economics, timing, stale-data, leg-risk, reconciliation, and kill-switch evidence before any separately authorized strategy execution.
- Candidate `300362d840141d9ed599d8189ed1d10801fc5256` on `codex/strategy-measurement-foundation` is formally rejected and immutable: it adds 3,359 lines of parallel measurement infrastructure for the superseded testnet-measurement objective without a visible Builder evidence report. None of it enters `main`; the existing accepted mainnet-public paper path is the authorized starting point.

## Remaining gate

- Mainnet readiness is a separate Level D program: current mainnet contracts/endpoints, protected production identities, Extended private WebSocket ingress, explicit notional/loss/leg-risk limits, restart and ambiguous-write recovery, monitoring/manual recovery, shadow operation, and separately authorized smallest real-funds canary.
