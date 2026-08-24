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

## Nado — new one-shot read-only Trigger Query governance

Status: `ARCHITECT ACCEPTED GOVERNANCE CANDIDATE — AWAITS SEPARATE CHIEF OPERATIONAL GATE; NO CALL AUTHORIZED`.

Objective: perform one new Nado private-read preflight as an operation separate from the permanently unknown prior invocation. This is not a retry and does not reclassify, recover, replay, or reuse any old action or evidence.

Fixed new operation identity:

- Invocation ID: `nado-private-read-20260824-new-op-001`.
- Absolute durable SQLite path: `/Users/daniilmakarov/.risex-funding-farmer-nado-private-read-20260824-new-op-001.sqlite3`.
- Redacted report path: `<passwd-home>/.risex-funding-farmer-nado-private-read-20260824-new-op-001.sqlite3`; SHA-256 of the exact absolute path: `ec98ed1b3781034e0436b37a634f4f87164510d077df1c3a5e7dc4a0e4d35b2d`.
- The path was absent and the invocation ID was absent from the repository during this governance review. A later operational gate must recheck both facts, open only this explicit non-temporary path, verify `PRAGMA synchronous=FULL` and durable state `NEW`, and record only the redacted path/hash plus `NEW` before any loader or network call. Any existing, mismatched, uncertain, or non-`NEW` state blocks the operation; it is never reset, repaired, deleted, or rearmed.

Required one-shot sequence after, and only after, the separate Chief operational gate:

1. Run complete public Round A through the fixed official test gateway before the secret boundary. Require the exact chain/domain/Endpoint, active engine, stable complete catalog, zero linked signer, adequate collateral and health, zero regular orders for every catalog product, every complete cross-perpetual amount and `v_quote_balance` exactly zero, and no isolated position.
2. Durably transition `NEW -> CLAIMED` with the fixed invocation identity and Round-A fingerprint. Then perform exactly one opaque owner-key load, one owner derivation and exact match, one new official server-time Query, one fresh `recvTime`, one EIP-712 `ListTriggerOrders` sign, and one exact recover-to-owner check. The accepted implementation uses `recvTime = fresh server time + 30 seconds`, within the gate maximum of 100 seconds; no old time, signature, or typed data may be reused.
3. Send exactly one read-only `list_trigger_orders` request to `POST https://trigger.test.nado.xyz/v1/query`, unfiltered except `limit=1`. It is Trigger Query, not Execute. `POST /execute`, order, cancel, close, deposit, faucet, account mutation, and every other write are prohibited.
4. A successful exact response with `orders=[]` durably transitions `CLAIMED -> OBSERVED`. Then complete public Round B and require it to agree with Round A while independently proving zero regular orders, complete exact cross-perpetual flatness, no isolated position, zero linked signer, stable identity/catalog, and active engine. Only that agreement may transition `OBSERVED -> FINALIZED`.

Failure and acceptance boundary:

- Verified TLS, exact fixed hosts and final URLs, `trust_env=False`, `proxy=None`, redirects disabled, finite deadlines, bounded strict JSON, and direct ownership remain mandatory. There is no retry, replay, re-sign, fallback, alternate host, proxy/VPN circumvention, browser workaround, repair, or second invocation under this gate.
- Any timeout, disconnect, cancellation, HTTP/schema/identity/time failure, nonempty or contradictory state, or ambiguity at or after the sole signed Query is terminal `UNKNOWN`; preserve the store and stop. `CLAIMED` is never re-entered. An incomplete `OBSERVED` is failure under this gate and is not resumed without a new explicit Chief decision; the signed Query is never repeated.
- Success is only durable `FINALIZED` plus agreeing fresh public rounds, zero regular orders for the complete catalog, zero trigger orders, every cross-perpetual amount and `v_quote_balance` exactly zero, and no isolated position. Report the redacted path/hash, invocation ID, terminal state, and redacted public/time/loader/derive/sign/recover/trigger/Round-B counters; retain no key, signature, full identity, signed body, or raw private response.
- Existing accepted code is unchanged and no Builder is needed. This governance candidate authorizes no store creation, network request, credential load, real signature, private Query, `/execute`, order, cancel, close, or other write. A passing read is not lifecycle completion, live-write authorization, exact-flat operational acceptance, strategy readiness, mainnet permission, or real-funds permission.

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
