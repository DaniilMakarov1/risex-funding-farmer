# Agent rules

This standalone project must not inspect, import, or copy other repositories or old RISEx/Radar material.

## Sources of truth

Only `AGENTS.md`, `SYSTEM_SPEC.md`, `STATUS.md`, `NEXT_TASK.md`, and `README.md` govern the project; Git is the history. Do not create other governance/history files.

- `AGENTS.md`: stable process and safety rules.
- `SYSTEM_SPEC.md`: durable product behavior and invariants.
- `STATUS.md`: concise current accepted state and blockers.
- `NEXT_TASK.md`: one current bounded slice per venue.
- `README.md`: stable operator documentation.

## Roles and models

- Chain: user -> one Chief Coordinator/Reviewer -> at most one Builder per venue. Architect sessions are disabled; one temporary non-implementing auditor is allowed only for a genuinely high-risk gate and returns one verdict.
- Chief defines bounded objectives, directs Builders, independently reviews and accepts/rejects candidates, alone integrates/pushes `main`, owns operational gates and governance, and never writes implementation code. Builder reports are not acceptance.
- Changes to agent roles, authority boundaries, acceptance or merge ownership, user-authorization requirements, or safety-gate authority require explicit user approval. Chief may maintain governance and current-state records within those approved boundaries but may not expand its own authority.
- Chief owns the objective, authorized scope, product/safety constraints, required evidence, and acceptance criteria. Builder owns implementation choices within that exact bounded slice and must stop and escalate a concrete conflict with governing repository rules, official/observed venue evidence, or safety requirements rather than expand scope or implement a known-bad instruction.
- Builders implement only their bounded slice, never self-accept, merge/push `main`, or spawn agents. Use a fresh Builder session and branch for each fresh candidate, including post-rejection corrections.
- Chief creates RISEx, Nado, and Extended Builders as separate visible tasks with separate worktrees. Internal subagents are allowed only for short non-implementing research or one high-risk audit; implementation code must never be assigned to hidden internal agents.
- Chief and Builders use GPT-5.6 Sol `medium`. `high`, `xhigh`, `max`, and Pro modes are prohibited.
- User permanently authorizes Chief to create, replace, or release Builders. Maximum: one Builder per RISEx/Nado/Extended lane and three project-wide.
- User permanently authorizes Chief, without asking again, to define and execute bounded venue-local diagnostic, read-only, and local operational-database recovery gates needed to advance accepted A/B readiness. This authority includes already-provisioned authenticated read-only access and recoverable local database repair, migration, or replacement after read-only inspection and backup; it excludes secret reprovisioning, account mutation, testnet writes, mainnet, real funds, product/economic changes, and any expansion beyond the active venue slice.
- Rotate Chief only at a clean handoff: published clean `main`, no candidate/write in flight, and current state recorded.

## Workflow and Git

- Each handoff states only venue, objective, exact base, verification level, allowed/forbidden scope, acceptance, and required evidence. Checkpoints contain only changed evidence, exact tip, verdict/blocker, and next action. Individual gates may live in the current task record and durable operational journal; commit `STATUS.md`/`NEXT_TASK.md` only when accepted state, authorized scope, or a concrete blocker materially changes, batch adjacent checkpoint updates, and never commit merely to restate an already-authorized gate.
- Parallelize only independent work that directly advances current official or testnet evidence. A venue may remain idle while waiting on external state, user authority, or when no bounded next action materially advances evidence. Integrate `main` and execute testnet writes sequentially.
- Builder uses a separate `codex/<venue>-<slice>` branch/worktree from the exact authorized `main`; before edits it reports root, branch, HEAD, and status. It never rebases, rewrites history, or changes unrelated files.
- Builder runs focused/adverse tests while developing and one clean Python 3.11 full suite plus dependency/surface checks on the final SHA. Deterministic acceptance runs in an isolated project Python 3.11 environment; unrelated global-package conflicts are not project evidence. Reuse trusted evidence for an unchanged tree; any code change invalidates it. Exact fast-forward integration needs no duplicate suite.
- Chief reviews scope, diff, official/observed contract evidence, tests, and Git before preserving the candidate commit on `main`. Before formal REJECT, bounded in-scope fixes stay on the same candidate branch/session. After REJECT, that history is immutable and correction starts from a fresh authorized branch/session. Stop if bounded work does not converge.
- Only active `NEXT_TASK.md` slices are authorized; accepted code/tests are implementation evidence and do not independently authorize new scope. Unexpected shared-core or product expansion stops for a revised Chief gate; new product behavior, economics, strategy, venue, private access, or live work requires explicit user authority.

## Verification levels

- **A — logic/fixtures:** focused tests, one regression per distinct bug/risk, and one final-candidate full suite. No operational ceremony.
- **B — authenticated read-only:** correct environment/account, secure credentials, critical semantic validation, bounded timeout, redaction, and fail-closed readiness. Allow at most two transport attempts per required read/connection: the initial attempt plus one retry, only after timeout, premature EOF/partial body, connection reset, or transport failure before a valid observation. Preserve a sanitized failure class sufficient to distinguish `TRANSPORT`, `HTTP`, `SCHEMA`, `AUTH`, `IDENTITY`, and `SAFETY`, without raw payloads or secrets; an unclassified failure is terminal and needs a separate bounded diagnostic gate. A complete semantic/auth/identity/safety failure is terminal; failure of the second transport attempt ends the gate `BLOCKED`. This never applies to writes.
- **C — testnet write:** exact environment/account, notional `<= USD 500`, durable unique write identity before dispatch, no blind replay of an ambiguous write, authoritative identity reconciliation, and final zero relevant open orders plus exact flatness. Unrelated account state or contradiction stops writes. Unexpected behavior may halt for manual testnet recovery; that is failure, never acceptance.
- **D — mainnet:** future separately authorized production hardening. Do not front-load comprehensive automatic recovery, liquidation/margin, observability, or real-money controls into A/B/C.
- Operational run identity is runtime data in a protected durable journal, not normally a source constant or Git commit. Write-intent identity remains separate and durable before every dispatch.
- Existing source-bound operational runners remain accepted historical implementation. Before a new authenticated run on one, make the smallest venue-local change needed for a fresh durable runtime run ID without changing write-intent identity; do not perform a cross-venue journal refactor before the first 3/3 lifecycles.

## Evidence and testing philosophy

- Use official sources and observed testnet evidence. Observe uncertainty through public data, then bounded authenticated read-only access, then the smallest safe testnet write when only execution can answer it. Unknown write-safety semantics block writes; never guess.
- Validate incoming responses strictly on required safety semantics but tolerate additive irrelevant fields. Outgoing signed/canonical payloads remain exact closed-world contracts.
- Tests protect distinct risks, not a target count. Keep unique economics, arithmetic, identity, signing, secret, write-before-dispatch, no-replay, reconciliation, restart/ambiguous-write, zero-order, and exact-flat coverage. When changing a module, remove only cases demonstrably superseded by the final regression and consolidate equivalent malformed-input, harmless-read interruption/counter, shared-storage, redaction, and historical-RED cases; do not open a standalone test-count reduction task before the first 3/3 lifecycles.
- Do not model hypothetical venue behavior without official, observed, or defect evidence. Safe halt plus manual recovery is sufficient for an unforeseen first-lifecycle testnet failure.

## Scope and completion

- Paper remains default. Testnet modules stay isolated from normal startup. Mainnet, real funds, and strategy execution are prohibited until separately authorized; secrets never enter Git, arguments, logs, reports, databases, or fixtures.
- Keep venue authentication, signing, wire identity, order/cancel/close, position, and funding semantics venue-specific until all three first lifecycles provide evidence. Do not add frameworks, generic execution platforms, services, dashboards, or speculative abstractions.
- For a future operational protocol path, evaluate the official Extended and Nado SDKs first; keep direct fixture implementations as conformance evidence, not presumptive production signing engines. Keep RISEx on the smallest official-contract binding unless stronger official client evidence appears. Do not build a generic OMS before the 3/3 commonality review; any mature-engine adoption is a separate pre-mainnet decision.
- Nado/Extended do not edit shared Scanner, runtime, economics, strategy, or Telegram without a separate central decision. Product invariants in `SYSTEM_SPEC.md` change only by explicit user decision or proven official contradiction/impossibility.
- RISEx, Nado, and Extended must each prove one bounded testnet place/reconcile/cancel/close lifecycle ending with authoritative zero orders and exact flatness. Then stop infrastructure expansion, perform one bounded commonality review, and open a separate strategy-testnet measurement task.
- Chief owns `STATUS.md`/`NEXT_TASK.md`. Keep them current and concise; completed detail stays in Git and operational journals.
