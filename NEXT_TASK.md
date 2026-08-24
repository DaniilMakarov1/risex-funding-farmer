# Active bounded tasks

At most one slice is active per venue. These slices may proceed in parallel, but central `main` integration and all testnet write lifecycles remain sequential. No task below authorizes strategy work, mainnet, real funds, deposits, or an order/cancel/close write.

## RISEx — publish accepted strict public decoder correction

Status: `ACCEPTED AND INTEGRATED LOCALLY — PUBLICATION BLOCKED BY GITHUB CLI AUTH`.

Accepted implementation commit: `6c530eb9a0b13050b66e385325f767f6d45f2c10` on exact base `ed6b0f076200b4f5316cd2341e8d8a3e0e16c8b1`. Independent Chief gates passed: genuine three-case old-base RED, 136 focused tests, 1087 full tests, clean dependency check, exact diff and secret/import/network-surface review.

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

Remaining action: authenticate the local Git transport and fast-forward `origin/main` to the exact accepted Builder commit without recreating or rewriting it. The correction proves deterministic contract parity only. Any later private-read invocation requires a new explicit operational gate and a new one-shot identity; the consumed invocation is never reused.

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

## Extended — one-shot operational private-read gate

Status: `DETERMINISTIC IMPLEMENTATION ACCEPTED AND INTEGRATED LOCALLY — PUBLICATION BLOCKED; NO OPERATIONAL CALL AUTHORIZED`.

Accepted implementation: `1551cb328ca1cf41bc4c4a49541c7e5f301ec5e6` on exact base `dc83bb209bb8c51194e2e0eda3c42166db9a59ba`. Acceptance evidence is 72 focused tests, 1159 full clean Python 3.11 tests, a clean dependency check, and clean scope, diff, secret, import, and network-surface review. Publication is blocked because local HTTPS Git transport lacks GitHub credentials; `origin/main` remains `ed6b0f076200b4f5316cd2341e8d8a3e0e16c8b1`.

Next boundary: before any real credential or private traffic, the Chief Coordinator must separately authorize exactly one read-only invocation against one fixed Extended Sepolia subaccount using the accepted implementation and a new unique durable one-shot identity. This task records the gate boundary only; it does not authorize the invocation.

Required preflight for that later gate:

- Verify the exact accepted and published implementation commit, fixed account/subaccount identity, injected durable store path, and new unused one-shot identity without exposing credential values.
- Authorize only the API-key loader, exactly six authenticated GETs (the three pinned exhaustive REST resources once in each round), and one official v1 account stream connection required by the accepted contract.
- Preserve direct TLS, exact official hosts and paths, API key solely in the `X-Api-Key` upgrade header, no application-level subscribe/ack, and no proxy, redirect, fallback, retry, reconnect, or replay.
- Keep the stream active from complete REST round A through complete round B and the final barrier; fail closed on any activity, sequence defect, malformed frame, disconnect, early end, stale/future REST evidence, disagreement, nonzero order, or non-flat position.
- Persist only redacted durable terminal evidence and exact loader/REST/stream counters. Any terminal, blocked, or ambiguous result is not rearmed or retried; restart after terminal state must make zero loader or network calls.

Forbidden before and during gate formulation:

- Credential access, private REST traffic, WebSocket connection, or any other operational network action.
- Signing, POST, order, cancel, close, deposit, or any write.
- A Builder, implementation change, shared Scanner/runtime/economics/strategy/Telegram change, or operational-readiness claim.

Acceptance for the later operational gate: Chief Coordinator independently verifies publication, the exact injected identities and store path, redaction, counter expectations, and the absence of any broader authority, then explicitly authorizes one invocation. Deterministic fixture acceptance alone does not prove operational private-read readiness or permit a call.

## Infrastructure freeze and transition

- Implement only corrections proven necessary to complete the three accepted minimal lifecycles safely. Do not build generalized execution infrastructure or new product behavior.
- After each venue passes private-read readiness, formulate a separate minimum-notional one-shot live-write gate for that venue. Execute venues sequentially; never blind-retry an ambiguous place, cancel, or close.
- Success for every venue is verified official order identity plus place/reconcile/cancel/close evidence and a final authoritative barrier proving zero open orders and exact flatness. Each potential notional remains `<= USD 500`.
- After all three venues pass, stop this phase and open a separate strategy-testnet measurement task. Do not implement that strategy in the current venue slices.
