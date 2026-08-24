# Active bounded tasks

At most one slice is active per venue. These slices may proceed in parallel, but central `main` integration and all testnet write lifecycles remain sequential. No task below authorizes strategy work, mainnet, real funds, deposits, or an order/cancel/close write.

## RISEx — strict public decoder/fixture correction

Status: `READY FOR SEPARATE BUILDER GATE`.

Objective after that gate: correct only the three stale public response contracts that caused the consumed private-read preflight's public-phase `PREFLIGHT_BLOCKED` result.

Required deterministic contract:

- Update only the pinned official fixtures and strict decoding for `/v1/auth/signers`, `/v1/markets`, and `/v1/orderbook`.
- Normalize and validate the current official signer expiration representation while retaining exact signer identity, active status, registered time, and unexpired interval.
- Validate the full official BTC/USDC market symbol and the exact current documented config fields while retaining market identity, active/unlocked state, price/size grids, and minimum size.
- Validate the exact current documented orderbook level fields, including `order_count` when present in the official contract, while retaining nonempty depth, ordering, grid, spread, freshness, and notional gates.
- Unknown or undocumented fields remain fail closed; do not replace exact contracts with permissive arbitrary-key acceptance.
- When pinned official sources do not define required/emitted key sets, the Architect may authorize only evidence-driven public schema capture for these same three endpoints. Each request must correspond to one concrete missing contract and persist only redacted key/type structure; account/signer identities, prices, quantities, request IDs, and values are not retained. Stop when the missing contract is established; repeated or exploratory sweeps are prohibited.
- Add genuine RED-before-GREEN regressions for all three observed shape classes, run the focused private-preflight suite, full clean Python 3.11 suite, dependency check, diff review, and secret scan.

Forbidden:

- Reset, rearm, or retry of the consumed operational invocation; any private or write request presented as schema capture.
- Signer or credential loading, nonce/signature creation, private traffic, or any write.
- Add fallbacks, alternate hosts, browser/support workarounds, strategy behavior, or shared-runtime expansion.

Acceptance: Architect acceptance followed by independent Chief Coordinator review, sequential central integration, full suite, and publication. The correction proves deterministic contract parity only. Any later private-read invocation requires a new explicit operational gate and a new one-shot identity; the consumed invocation is never reused.

## Nado — durable-result reconciliation

Status: `BLOCKED — ORIGINAL OPERATIONAL STORE/EVIDENCE NOT FOUND; NO NEW CALL`.

Objective: determine what the prior operational turn actually dispatched and observed before deciding whether any later gate is possible.

Authorized work:

- Read only existing redacted evidence, durable state, and request/loader/signature counters.
- Classify the invocation as not armed, armed but undispatched, publicly blocked, privately ambiguous, or finalized, with exact supporting counters and terminal state.
- Report missing evidence as unknown rather than inferring success or permission to retry.

Forbidden:

- Any network request, credential load, signature, replay, rearm, state deletion, repair, or write.
- Proxy/VPN circumvention, alternate hosts, browser/support fallback, or shared-core edits.

Acceptance: recover the exact original injected store/evidence path and report its terminal state plus loader/public/time/signature/trigger/round-B counters. Only proof that the sensitive request was not dispatched can permit formulation of a new bounded gate. Without that evidence the lane remains fail-closed `UNKNOWN`; there is no retry authority.

## Extended — sealed private-read fixture implementation

Status: `READY FOR SEPARATE BUILDER GATE`.

Prerequisite: authorize the sole Extended Architect to create one Builder from the exact published `main`.

Objective after that gate: implement only an isolated deterministic private-read preflight for one fixed Extended Sepolia subaccount.

Required deterministic contract:

- Exact account/subaccount identity and API-key-only opaque loader behind a durable one-shot boundary.
- Verified direct TLS, fixed official host, no proxy inheritance, redirects, fallback, retry, reconnect, or replay.
- Complete private REST round A, one gap-free account-stream snapshot, then complete REST round B.
- Strict exhaustive decoding and pagination; fresh agreeing observations must independently prove zero open orders and no open positions.
- Durable terminal state and fully redacted evidence; restart after terminal state makes zero loader or network calls.
- Genuine RED-before-GREEN fixtures, adverse-path coverage, focused tests, full clean Python 3.11 suite, dependency check, and isolated imports only.

Forbidden even after the Builder gate: real credentials, authenticated traffic, WebSocket connection, signing, POST, order, cancel, close, deposit, shared Scanner/runtime/economics/strategy/Telegram changes, or operational-readiness claims.

Acceptance: Architect acceptance followed by independent Chief Coordinator review, sequential central integration, full suite, and publication. A later real private-read invocation requires another explicit operational gate.

## Infrastructure freeze and transition

- Implement only corrections proven necessary to complete the three accepted minimal lifecycles safely. Do not build generalized execution infrastructure or new product behavior.
- After each venue passes private-read readiness, formulate a separate minimum-notional one-shot live-write gate for that venue. Execute venues sequentially; never blind-retry an ambiguous place, cancel, or close.
- Success for every venue is verified official order identity plus place/reconcile/cancel/close evidence and a final authoritative barrier proving zero open orders and exact flatness. Each potential notional remains `<= USD 500`.
- After all three venues pass, stop this phase and open a separate strategy-testnet measurement task. Do not implement that strategy in the current venue slices.
