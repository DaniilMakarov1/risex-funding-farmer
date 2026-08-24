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

## Nado — operational adapter and durable counter correction

Status: `ARCHITECT GOVERNANCE CANDIDATE — AWAITS CHIEF ACCEPTANCE AND SEPARATE BUILDER AUTHORIZATION; NO OPERATIONAL CALL AUTHORIZED`.

Objective: correct exactly two reproduced blockers inside existing `NADO-TESTNET-001`: add a truthful isolated production binding/one-command opt-in launcher for the accepted sealed read-only function, and make every required attempt/completion counter authoritatively recoverable after interruption. This is corrective infrastructure only, not a new lifecycle or product behavior.

Authorized implementation scope after a separate Chief Builder gate:

- Add one isolated Nado operational-adapter module and focused tests, plus only the minimum Nado private-read store/controller changes needed for the counter ledger. The module is never imported by package startup or `risex-farmer`; the sole production entry is an explicit opt-in module launcher. Its production constructor accepts no caller-selected store path, invocation ID, owner/subaccount, URL, method, transport, proxy, timeout, retry, or fallback.
- The adapter accepts exactly one explicitly injected opaque local credential-source capability. The Builder gate must supply the already-approved non-secret fixed owner/subaccount identity for pinning; absence of that identity or the single source class is a blocker, not authority to invent alternatives. The source may yield only a closeable handle supporting owner derivation and the exact EIP-712 sign operation. Load once, derive and match the fixed owner, encode and match the fixed subaccount, sign once, recover and match once, then close/zeroize in `finally`. Raw key material, source name/path/value, full identity, signature, typed data, and private body/response never enter Git, SQLite, CLI arguments, environment dumps, logs, exceptions, or reports.
- Bind only the already accepted fixed gateway `/query`, gateway `/edge/query`, and trigger `/query` transports with verified TLS, exact final URLs, `trust_env=False`, `proxy=None`, redirects disabled, five-second deadlines, strict bounded JSON, and no retry/reconnect/replay. The launcher exposes no `/execute`, order, cancel, close, deposit, account mutation, or generic request method. Official pinned Nado source identities and the exact `ListTriggerOrders(sender, recvTime)` request remain unchanged.
- The production binding owns the fixed invocation ID `nado-private-read-20260824-new-op-001` and absolute store path `/Users/daniilmakarov/.risex-funding-farmer-nado-private-read-20260824-new-op-001.sqlite3`; the redacted report path and exact absolute-path SHA-256 remain `<passwd-home>/.risex-funding-farmer-nado-private-read-20260824-new-op-001.sqlite3` and `ec98ed1b3781034e0436b37a634f4f87164510d077df1c3a5e7dc4a0e4d35b2d`. Production exposes no override. Builder tests use synthetic identities, synthetic handles, and disposable paths through a private fixture seam only and must never create or inspect the fixed production store.
- Extend the durable `PRAGMA synchronous=FULL` one-shot ledger with schema-versioned, nonnegative attempt/completion counters for public Round A, loader, derive, server-time Query, sign, recover, trigger dispatch claim, trigger response observation, and public Round B; also retain catalog product count, redacted identity/path hashes, bounded timestamps/reason, round fingerprints, and state. Commit each attempt before its external effect and each completion only after exact validation. The trigger dispatch claim is durable before the sole POST; any missing completion is ambiguous, never evidence of no dispatch.
- The adapter runs only `NEW -> public A -> CLAIMED -> one loader/derive/time/sign/recover/trigger Query -> OBSERVED -> public B -> FINALIZED`. Public A performs `P+6` gateway Queries and public B `2P+7` for the durable complete catalog size `P`. Success requires all sensitive counters exactly one, trigger dispatch/observation exactly one, both public-round counts exact, agreeing round fingerprints, zero regular and trigger orders, complete cross-perpetual exact flatness, and no isolated position.
- Persist a terminal redacted report on every caught failure or cancellation. After process death, any nonterminal `CLAIMED`, any trigger dispatch claim without observation, or incomplete `OBSERVED` is reported `UNKNOWN` with the exact durable counters and permits zero loader/network effects. Terminal restart returns the same report with zero effects. No state is reset, deleted, repaired, resumed, or rearmed; in particular the signed Query and Round B are never repeated by this invocation.

Deterministic acceptance:

- Genuine old-base RED must prove both missing surfaces: no production opt-in binding and no durable authoritative counter recovery. GREEN fixtures must cover the exact successful sequence/counter formulas, source load/close and identity mismatch, failures and cancellation immediately before and after every external effect, process-death recovery at every durable phase, ambiguous trigger dispatch, incomplete Round B, terminal restart, store/schema corruption, path/permission/symlink rejection, secret redaction, and absence of `/execute` or generic transport selection.
- Run the focused Nado lifecycle/private-read/adapter suites, the full clean Python 3.11 suite, dependency check, exact diff review, and secret/import/network-surface scans. CI and Builder verification remain fixture-only with synthetic keys and disposable storage.
- Builder scope is limited to the isolated Nado adapter, minimum Nado counter-ledger correction, and focused fixtures/tests. No governance file change belongs in the Builder commit; the Architect records accepted state only after acceptance. No shared Scanner/runtime/repository/economics/strategy/Telegram, RISEx, Extended, paper CLI, lifecycle order logic, framework, or generalized credential/execution abstraction may change.

This governance candidate creates no Builder and authorizes no fixed-store creation, credential access, network, real signature, private Query, `/execute`, order, cancel, close, or other write. The fixed invocation/path remain unused until implementation acceptance, sequential central integration/publication, and a later separate Chief operational gate.

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
