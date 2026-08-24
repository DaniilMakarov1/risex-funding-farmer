# Agent rules

This is a new standalone paper-only research project. Do not inspect, import, or copy other repositories, local projects, old RISEx/Radar code, or old architecture documents.

## Sources of truth

Only `AGENTS.md`, `SYSTEM_SPEC.md`, `STATUS.md`, `NEXT_TASK.md`, and `README.md` govern the project. Git history is the task history. Do not create other governance or task-history documents.

- `AGENTS.md` contains only stable process, role, safety, and acceptance rules.
- `SYSTEM_SPEC.md` contains frozen product behavior and invariants.
- `STATUS.md` is a concise current accepted-state and blocker snapshot, not a chronology.
- `NEXT_TASK.md` contains only the currently authorized bounded slice for each venue plus its acceptance boundary.
- `README.md` contains stable operator documentation, not current SHAs, task state, or acceptance history.
- Completed and rejected task detail remains in Git history and must not be copied forward into active governance unless it is still material to a current safety gate.

## Roles

- The permanent role chain is user -> one Chief Coordinator -> one Architect per RISEx, Nado, or Extended lane -> at most one Builder per lane. There are no permanent venue Reviewer sessions.
- Chief Coordinator formulates bounded follow-up directly for each venue Architect and independently verifies the required gates. Architects perform ordinary lane work autonomously without routine Coordinator participation. Chief Coordinator may observe lane progress and, between mandatory gates, intervene only to correct scope or safety drift, respond to an Architect-requested escalation, help resolve a genuine blocker, or address a cross-lane/shared-state conflict. Chief Coordinator does not write implementation code and does not direct or manage a Builder; each Architect directs only that lane's Builder.
- Completion of an Architect turn is a checkpoint, not completion or suspension of that venue lane. After every checkpoint the same Architect remains the lane owner unless explicitly replaced. Chief Coordinator promptly records the accepted result, supplies the next strictly necessary bounded action, or records a concrete blocker; an idle task, exhausted turn, missing final message, or Builder/Architect report alone is never acceptance and never a reason to leave the lane without a next action.
- Routine in-scope lane work does not require repeated Chief Coordinator participation. The lane Architect may continue ordinary research, Builder direction, defect reproduction, and same-scope correction autonomously between the mandatory gates listed below. Independent Chief Coordinator re-verification is required at those gates and when concrete evidence reveals safety, scope, shared-state, or official-contract risk; it is not an extra approval layer for every internal step.
- Operate in token-efficient accelerated mode. Do not repeat accepted research, history, source summaries, tests, or explanations unless new contradictory evidence makes them material to the current gate. Checkpoint reports contain only the exact base/branch/tip when relevant, changed scope, verdict, essential test counts, concrete acceptance-breaking defects or blocker, and the single next necessary action. Chief Coordinator immediately accepts, rejects with a reproduced defect, or advances the lane; it does not request ceremonial rewrites or duplicate reports.
- Parallelize every safe independent research, fixture, review, and private-read preparation action across venue lanes. Keep sequential only the operations explicitly required to be sequential: central `main` integrations and testnet order/write lifecycles. Prefer one bounded correction that closes all currently reproduced in-scope defects over serial micro-cycles, while preserving RED-before-GREEN evidence and immutable rejected history.
- Verification is proportional to the gate. Focused tests and targeted adverse reproduction are sufficient for ordinary internal correction checkpoints. Run the full clean Python 3.11 suite and dependency check for implementation acceptance and after each central integration, and retain all credential/private/write/exact-flat checks required below. Acceleration and token efficiency never weaken secret isolation, official-contract proof, no-blind-retry behavior, notional limits, authoritative zero-open-orders/exact-flat acceptance, or the prohibition on strategy work before all three venue lifecycles are operationally accepted.
- The user permanently authorizes Chief Coordinator to create or replace each lane's sole Architect whenever Chief Coordinator judges it necessary, without separate approval. At most one Architect may be active per lane and at most three venue-lane Architects project-wide. Every future Coordinator/Architect handoff context must repeat this standing authorization and limit. This changes no product or implementation scope.
- Chief Coordinator reuses the current lane Architect across checkpoints and replaces it only when it is genuinely unavailable, persistently non-responsive, out of scope, or otherwise unable to continue safely. Replacement never creates a duplicate lane owner, and the replacement receives the exact accepted base, unresolved evidence, active bounded slice, and next gate.
- Each venue-lane Architect is the autonomous technical owner of that lane's architecture, official-source research, `NEXT_TASK.md`-bounded planning, Builder orchestration, review, acceptance, evidence, and source-of-truth updates on its lane branch.
- At most one Builder may work per venue lane and at most three Builders project-wide. A Builder may start only after that venue's Phase 0 and the central governance gate are accepted. Builder must not spawn agents.
- Only the lane Architect creates, directs, reviews, and accepts its Builder. Chief Coordinator authorization is required before Builder creation and again for implementation acceptance.
- Chief Coordinator may appoint a temporary bounded independent auditor only for a specific high-risk gate. The auditor does not manage an Architect or Builder, does not change code, and ceases to participate after returning that gate's verdict.
- Mandatory Chief Coordinator gates are Phase 0 acceptance, Builder authorization, implementation acceptance, any credential/private/testnet write or order action, exact-flat acceptance, any shared-core edit, and each sequential `main` integration.
- Builder implements only the bounded milestone in `NEXT_TASK.md` and must not begin the next milestone.
- Work only within milestones already authorized by `SYSTEM_SPEC.md` and the active bounded slices in `NEXT_TASK.md`. Corrective branch labels are audit identifiers, not product milestones. A new product behavior, strategy, venue, or milestone requires an explicit user decision.
- The user's ongoing stabilization decision authorizes successive strictly bounded corrective slices needed to make the existing paper system correct, without repeated user selection of technical task numbers. Administrative stabilization labels and branches are audit identifiers, not new product milestones.
- The user's testnet decision authorizes the three venue lifecycle programs only through the exact active boundaries in `NEXT_TASK.md`. Deterministic acceptance never authorizes credential loading, private traffic, signatures, or writes. Each credential/private-read action and each later live-write lifecycle requires its own Chief Coordinator gate. A blocked or ambiguous one-shot is never retried or rearmed unless the accepted contract explicitly proves that no request was dispatched and a new bounded gate is recorded.
- Any new product behavior, economics, strategy, API/private access, live work, or Telegram expansion still requires a separate explicit user decision.
- Infrastructure expansion is frozen to the minimum corrections required to complete the three accepted venue lifecycles safely. Do not add frameworks, generalized execution abstractions, dashboards, new venues, execution features, or shared runtime behavior merely to prepare for future strategy work.
- After RISEx, Nado, and Extended each independently prove the separately gated minimal testnet place/reconcile/cancel/close lifecycle and authoritative zero open orders plus exact flatness, stop lifecycle infrastructure work. The next task is a separately governed strategy-testnet measurement using the already accepted strategy; it must measure opportunity frequency, planned-versus-actual execution, fees, resolved funding, and complete net PnL. Degraded or unresolved observations never count as profitability evidence.

## Parallel venue lanes

- Up to three venue lanes may exist: RISEx, Nado, and Extended. Each lane has at most one Architect and one Builder; the project-wide maxima are three venue-lane Architects and three Builders.
- `NEXT_TASK.md` may contain at most one active bounded slice per venue and at most three active slices total. Each slice retains its venue-local scope, ownership, evidence, and acceptance boundary.
- Every lane uses a separate worktree and branch from its exact authorized base. Branch and task identifiers must carry an unambiguous unique `risex`, `nado`, or `extended` venue/lane prefix. Chief Coordinator keeps each lane's identity and state separate and never authorizes cross-venue edits.
- Read-only research and independent venue implementation may proceed in parallel. Testnet order operations and `main` integration are sequential, one lane at a time.
- Chief Coordinator keeps every unblocked venue lane moving in parallel. Completion or attention from one lane is handled immediately without pausing the other lanes; a lane may be left idle only when it is waiting at an explicit Chief gate, waiting on an already-running bounded action, or has a recorded concrete blocker.
- Venue teams never merge or push `main`. Chief Coordinator owns the sequential central integration gate: before each candidate, verify its exact base against current `main`; after integrating each accepted candidate, run the full suite before considering the next lane candidate.
- Nado or Extended implementation may not edit shared Scanner, runtime, economics, strategy, or Telegram code. If either lane requires a shared-core edit, that lane stops and needs a separate central integration decision before any such change.
- Every lane is testnet-only, with every potential exposure/notional `<= USD 500`; mainnet and real funds are prohibited. Operational success requires authoritative zero open orders and exact flatness. Ambiguous place, cancel, or close is never retried blindly and must reconcile through exact official identity. The sole narrow exception is the user-accepted empirical risk in `TESTNET-002-RISEX-ORDER-LIFECYCLE-001`: after its later separately gated live experiment, bounded recovery may stop as `FAILED_HALTED_MANUAL_RECOVERY` with only the minimum-size RISEx testnet exposure or a known experiment order still unresolved. That outcome is failure, never operational acceptance, exact-flat acceptance, or strategy readiness.
- Accepted venue history and the sole current follow-up for each lane live in `STATUS.md` and `NEXT_TASK.md`, not in this stable process file. No venue Builder may start until that venue's governance candidate is centrally accepted and published to `main` and Chief Coordinator separately authorizes the sole Builder.

## Git workflow

- `main` contains only centrally integrated, Architect-accepted and Chief Coordinator-accepted work.
- Builder starts from the exact centrally authorized `main` and works on a separate `codex/<venue-or-lane-prefix>-<bounded-slice>` branch/worktree.
- Before edits, Builder reports repository root, branch, HEAD, and short status.
- Builder must not work on `main`, merge, rebase, rewrite history, or alter unrelated changes.
- Builder runs focused tests and full `pytest`, reviews the diff, and creates one milestone commit.
- Architect reviews the candidate and either accepts it, requests an in-scope fix, or rejects it. Before rejection, fixes remain on the same branch. After rejection, that branch and its commits remain immutable audit history; a later correction may proceed only under an explicit Chief Coordinator gate on a fresh venue-specific branch/worktree from the exact centrally authorized base, within the same bounded milestone and scope.
- Fix cycles have no fixed numerical limit. Each cycle is permitted only for a newly identified, concretely reproduced acceptance-breaking defect; it must remain within the same bounded milestone and scope and undergo a full repeated Architect and Chief Coordinator review. A fix cycle does not authorize product, live, credential, strategy, shared-core, or cross-venue expansion. Removing the numerical limit does not accept, alter, or reopen rejected branches or commits. If there is no concrete reproducible defect, no bounded progress, or the task genuinely does not converge, stop with `BLOCKED — TASK DID NOT CONVERGE`.
- A venue Architect never integrates or pushes `main`; Chief Coordinator's central integration preserves the accepted Builder commit without rewriting it and follows the sequential lane gate above.

## Scope and safety

- Python 3.11; one async process; `aiohttp`, `sqlite3`, `Decimal`, dataclasses, `pytest`, and `pytest-asyncio`.
- Paper remains the default product. Current testnet steps are only those explicitly bounded in `NEXT_TASK.md`. Public checks precede every secret boundary; credential/private reads and order/cancel/close writes require separate Chief Coordinator operational gates. Mainnet, real funds, and strategy execution remain prohibited until their separately authorized phase.
- Use only official RISEx, Extended, and Nado sources. Unknown semantics or parity blocks operational acceptance; never guess. The bounded RISEx empirical slice may fixture-test only its exact official API/ABI and current official-UI-compatible contract under the explicit user risk decision, and no empirical outcome generalizes undocumented semantics.
- Keep the implementation small. No compatibility layers, frameworks, services, dashboards, or functionality excluded by `SYSTEM_SPEC.md`. TELEGRAM-001 permits only the outbound notifications defined there, not a general alerting platform.
- Each fact, formula, and state has exactly one authoritative owner: adapters own venue normalization and funding semantics; runtime owns public data, cadence, and lifecycle orchestration; Scanner owns route blockers and PnL evaluation; repository owns persistence only; Telegram owns presentation and delivery only. When duplication or accumulated complexity causes a defect, remove or consolidate it before adding another flag, layer, cache, or state machine.
- Product rules in frozen `SYSTEM_SPEC.md` change only for a proven official-API contradiction, implementation impossibility, or explicit user decision.
- Never commit secrets, credentials, local databases, caches, or generated reports.

## Milestone completion

- Preserve exact arithmetic and funding/PnL invariants even when simplifying code or documentation.
- CI tests use fixtures only; live smoke checks are opt-in.
- Builder report is at most 20 lines and states branch, commit, tests, and material limitations.
- Architect updates `STATUS.md` and `NEXT_TASK.md` only after acceptance.
- After an accepted stabilization slice, its venue Architect may authorize only the next strictly corrective slice required by evidence and must record at most one active slice for that venue in `NEXT_TASK.md`, subject to the three-slice project maximum. Do not infer authorization for product expansion; PAPER-007 and TELEGRAM remain within their explicit user-approved boundaries.
