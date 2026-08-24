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

## Extended — operational adapter and durable counter correction

Status: `ARCHITECT GOVERNANCE CANDIDATE — AWAITS CHIEF ACCEPTANCE AND SEPARATE BUILDER AUTHORIZATION; NO OPERATIONAL CALL AUTHORIZED`.

Objective: correct exactly two reproduced blockers inside existing `EXTENDED-TESTNET-001`: add a truthful isolated production binding and one-command opt-in launcher for the accepted sealed private-read function, and make every required attempt/completion counter authoritatively recoverable after interruption. This is corrective infrastructure only, not a new lifecycle or product behavior.

Authorized implementation scope after a separate Chief Builder gate:

- Add one isolated Extended operational-adapter module and focused tests, plus only the minimum Extended private-read store/controller changes required for the counter ledger. The module is never imported by package startup or `risex-farmer`; its sole production entry is the explicit opt-in module launcher `python -m risex_farmer.extended_private_read_operational`. The production binding exposes no caller-selected store path, invocation ID, identity, URL, method, transport, proxy, timeout, retry, reconnect, fallback, or write surface.
- Accept exactly one Chief-designated opaque local API-key source capability. The later Builder gate must supply the source class and the already-approved fixed non-secret account/subaccount identity for production pinning, without placing actual API-key or account identity values in governance. Absence or mismatch fails closed. The API key is loaded once and may appear only in the `X-Api-Key` request/upgrade header; the source class, source location, key value, full identity, private response, and headers never enter Git, SQLite, CLI arguments, environment dumps, logs, exceptions, or reports. Tests use only a synthetic source and synthetic identity through a private fixture seam.
- The production binding owns invocation ID `extended-private-read-20260824-new-op-001` and absolute store path `/Users/daniilmakarov/.risex-funding-farmer-extended-private-read-20260824-new-op-001.sqlite3`; production exposes no override and the Builder must never create that store. These identifiers remain unused until implementation acceptance, integration/publication, and a later operational gate.
- Bind only the accepted Sepolia REST origin and exact `/user/account/info`, `/user/orders`, and `/user/positions` paths once in round A and once in round B, plus one connection to official v1 `/stream.extended.exchange/v1/account`. Use verified direct TLS, exact final URLs, `trust_env=False`, `proxy=None`, redirects disabled, fixed bounded deadlines, and no retry, reconnect, fallback, replay, application-level subscribe/ack, POST, generic request method, order, cancel, close-position, deposit, or other write surface.
- Preserve the accepted sequence: loader once, complete three-GET round A, one `X-Api-Key` upgrade stream, complete three-GET round B while that same gap-free stream remains active, final barrier, then stream close. Existing strict identity, pagination, freshness, zero-order, exact-flat, activity, sequence, disconnect, and redaction gates remain unchanged.
- Extend the durable `PRAGMA synchronous=FULL` one-shot record with a schema version, fixed invocation/config identity evidence, phase, and nonnegative attempt/completion counters for loader, each of the six REST requests and validated responses, stream open and validated upgrade, final barrier request and validation, stream close, and terminal persistence. Commit each attempt before its external effect and each completion only after exact validation; never infer completion from an attempt.
- A later launcher encountering interrupted `RUNNING` reports redacted `UNKNOWN` with the exact durable phase/counters and performs zero loader, clock, REST, or stream effects. Terminal restart returns the same redacted result with zero effects. No state is reset, deleted, repaired, resumed, rearmed, or replayed.

Deterministic acceptance:

- Genuine old-base RED must independently prove the absence of the production opt-in launcher and the absence of authoritative counter recovery from interrupted `RUNNING`.
- GREEN fixtures must prove the exact six-GET/one-stream sequence and counter totals, fixed binding with no production overrides, source load-once and identity mismatch, interruption/cancellation immediately before and after every external effect, process-death classification at every durable phase, incomplete round B, ambiguous stream open/barrier/close, terminal restart, store/schema/path/permission/symlink corruption, and complete secret/identity/private-response redaction.
- Run the focused Extended lifecycle/private-read/adapter suites, full clean Python 3.11 suite, dependency check, exact diff review, and secret/import/network-surface scans. CI and Builder verification remain fixture-only with synthetic credentials, identities, transports, and disposable paths.
- Builder scope is limited to the isolated Extended adapter, minimum Extended private-read durable-ledger correction, and focused fixtures/tests. No governance file change belongs in the Builder commit; no shared Scanner/runtime/repository/economics/strategy/Telegram, RISEx, Nado, paper CLI, lifecycle order logic, framework, or generalized credential/execution abstraction may change.

This governance candidate creates no Builder and authorizes no fixed-store creation, credential access, private REST traffic, WebSocket connection, signing, POST, order, cancel, close, deposit, or other write. A later real invocation requires implementation acceptance, sequential central integration/publication, and a separate Chief operational gate.

## Infrastructure freeze and transition

- Implement only corrections proven necessary to complete the three accepted minimal lifecycles safely. Do not build generalized execution infrastructure or new product behavior.
- After each venue passes private-read readiness, formulate a separate minimum-notional one-shot live-write gate for that venue. Execute venues sequentially; never blind-retry an ambiguous place, cancel, or close.
- Success for every venue is verified official order identity plus place/reconcile/cancel/close evidence and a final authoritative barrier proving zero open orders and exact flatness. Each potential notional remains `<= USD 500`.
- After all three venues pass, stop this phase and open a separate strategy-testnet measurement task. Do not implement that strategy in the current venue slices.
