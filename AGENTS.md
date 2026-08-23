# Agent rules

This is a new standalone paper-only research project. Do not inspect, import, or copy other repositories, local projects, old RISEx/Radar code, or old architecture documents.

## Sources of truth

Only `AGENTS.md`, `SYSTEM_SPEC.md`, `STATUS.md`, `NEXT_TASK.md`, and `README.md` govern the project. Git history is the task history. Do not create other governance or task-history documents.

## Roles

- The permanent role chain is user -> one Chief Coordinator -> one Architect per RISEx, Nado, or Extended lane -> at most one Builder per lane. There are no permanent venue Reviewer sessions.
- Chief Coordinator formulates bounded follow-up directly for each venue Architect and independently verifies the required gates. Architects perform ordinary lane work autonomously without routine Coordinator participation. Chief Coordinator may observe lane progress and, between mandatory gates, intervene only to correct scope or safety drift, respond to an Architect-requested escalation, help resolve a genuine blocker, or address a cross-lane/shared-state conflict. Chief Coordinator does not write implementation code and does not direct or manage a Builder; each Architect directs only that lane's Builder.
- The user permanently authorizes Chief Coordinator to create or replace each lane's sole Architect whenever Chief Coordinator judges it necessary, without separate approval. At most one Architect may be active per lane and at most three venue-lane Architects project-wide. Every future Coordinator/Architect handoff context must repeat this standing authorization and limit. This changes no product or implementation scope.
- Each venue-lane Architect is the autonomous technical owner of that lane's architecture, official-source research, `NEXT_TASK.md`-bounded planning, Builder orchestration, review, acceptance, evidence, and source-of-truth updates on its lane branch.
- At most one Builder may work per venue lane and at most three Builders project-wide. A Builder may start only after that venue's Phase 0 and the central governance gate are accepted. Builder must not spawn agents.
- Only the lane Architect creates, directs, reviews, and accepts its Builder. Chief Coordinator authorization is required before Builder creation and again for implementation acceptance.
- Chief Coordinator may appoint a temporary bounded independent auditor only for a specific high-risk gate. The auditor does not manage an Architect or Builder, does not change code, and ceases to participate after returning that gate's verdict.
- Mandatory Chief Coordinator gates are Phase 0 acceptance, Builder authorization, implementation acceptance, any credential/private/testnet write or order action, exact-flat acceptance, any shared-core edit, and each sequential `main` integration.
- Builder implements only the bounded milestone in `NEXT_TASK.md` and must not begin the next milestone.
- Do not create milestones beyond BOOTSTRAP-000 and PAPER-001 through PAPER-006, except the explicitly authorized PAPER-007 experiment, TELEGRAM-001 outbound-notification work, bounded TELEGRAM-001-FIX-001 correction, TELEGRAM-002 full-scan digest and its bounded two-decimal display correction, PAPER-007-FIX-004 public REST timeout correction, PAPER-007-FIX-005 first-full funding-freshness correction, PAPER-007-FIX-006 RISEx checksum-resubscribe correction, PAPER-007-FIX-007 Extended expected/applied funding and socket-health separation, PAPER-007-FIX-008 public evidence/catalog/heartbeat/digest/stop correction, and PAPER-007-FIX-009 public stream/time/startup consistency correction.
- The user's ongoing stabilization decision authorizes successive strictly bounded corrective slices needed to make the existing paper system correct, without repeated user selection of technical task numbers. Administrative stabilization labels and branches are audit identifiers, not new product milestones.
- The user's explicit testnet decision authorizes accepted `TESTNET-001` account bootstrap/read-only connectivity, the accepted `TESTNET-002-RISEX-SIGNER-001` session signer, and successive strictly bounded RISEx-first recovery slices recorded in `NEXT_TASK.md`. A blocked slice authorizes no Builder or private/executing write. This does not authorize mainnet, real funds, Scanner/runtime strategy integration, or work around an unknown safe-flat contract. Nado and Extended remain research-only under the parallel-lane gate below.
- Any new product behavior, economics, strategy, API/private access, live work, or Telegram expansion still requires a separate explicit user decision.

## Parallel venue lanes

- Up to three venue lanes may exist: RISEx, Nado, and Extended. Each lane has at most one Architect and one Builder; the project-wide maxima are three venue-lane Architects and three Builders.
- `NEXT_TASK.md` may contain at most one active bounded slice per venue and at most three active slices total. Each slice retains its venue-local scope, ownership, evidence, and acceptance boundary.
- Every lane uses a separate worktree and branch from its exact authorized base. Branch and task identifiers must carry an unambiguous unique `risex`, `nado`, or `extended` venue/lane prefix. Chief Coordinator keeps each lane's identity and state separate and never authorizes cross-venue edits.
- Read-only research and independent venue implementation may proceed in parallel. Testnet order operations and `main` integration are sequential, one lane at a time.
- Venue teams never merge or push `main`. Chief Coordinator owns the sequential central integration gate: before each candidate, verify its exact base against current `main`; after integrating each accepted candidate, run the full suite before considering the next lane candidate.
- Nado or Extended implementation may not edit shared Scanner, runtime, economics, strategy, or Telegram code. If either lane requires a shared-core edit, that lane stops and needs a separate central integration decision before any such change.
- Every lane is testnet-only, with every potential exposure/notional `<= USD 500`; mainnet and real funds are prohibited. Operational success requires authoritative zero open orders and exact flatness. Ambiguous place, cancel, or close is never retried blindly and must reconcile through exact official identity.
- Nado and Extended are currently research-only. No Builder may start in either lane until these parallel-lane rules are centrally accepted and published to `main`, that venue's Phase 0 is accepted, and its separate central governance gate is accepted.

## Git workflow

- `main` contains only centrally integrated, Architect-accepted and Chief Coordinator-accepted work.
- Builder starts from the exact centrally authorized `main` and works on a separate `codex/<venue-or-lane-prefix>-<bounded-slice>` branch/worktree.
- Before edits, Builder reports repository root, branch, HEAD, and short status.
- Builder must not work on `main`, merge, rebase, rewrite history, or alter unrelated changes.
- Builder runs focused tests and full `pytest`, reviews the diff, and creates one milestone commit.
- Architect reviews the branch and either accepts, requests a fix in the same branch, or rejects it.
- At most two fix cycles are allowed; otherwise stop with `BLOCKED — TASK DID NOT CONVERGE`.
- A venue Architect never integrates or pushes `main`; Chief Coordinator's central integration preserves the accepted Builder commit without rewriting it and follows the sequential lane gate above.

## Scope and safety

- Python 3.11; one async process; `aiohttp`, `sqlite3`, `Decimal`, dataclasses, `pytest`, and `pytest-asyncio`.
- Paper remains the default product. The completed one-shot RISEx testnet signer registration is accepted operational history. Any later testnet private/executing write requires an independently accepted bounded task in `NEXT_TASK.md`; the current blocked recovery task permits none. Mainnet, real funds, and strategy execution remain prohibited.
- Use only official RISEx, Extended, and Nado sources. Unknown semantics or parity blocks entry; never guess.
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
