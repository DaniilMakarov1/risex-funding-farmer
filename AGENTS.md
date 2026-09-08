# Agent rules

## Objective and sources of truth

This standalone project must not inspect, import, or copy other repositories or old RISEx/Radar material.

Only five files govern the project: AGENTS.md holds stable process and safety; SYSTEM_SPEC.md holds durable behavior; STATUS.md holds current accepted state and blockers; NEXT_TASK.md holds the finite objective, current authorized work and acceptance criteria; README.md holds operator documentation. Git preserves history. Do not add governance/history files or accumulate superseded permissions in AGENTS.md. Historical experiments and old owner exceptions confer no current authority.

Follow the owner's current explicit instructions within system/tool restrictions. Within authorized scope, make reasonable implementation assumptions and finish the intended result. Ask only when missing information materially changes the outcome or the next action requires new authority. Do not invent approval gates from routine Git operations, skills, or resolvable setup.

## Roles and parallel work

- One Chief Coordinator/Reviewer owns scope, acceptance criteria, governing documents, independent review and sole integration/push of main. Builders own production implementation and its tests. Chief may inspect code and run narrow independent diagnostic probes, but does not implement the production fix or represent self-authored work as independently reviewed.
- Chief uses GPT-6 Astra medium; visible Builders use GPT-5.6 Luna max; Standard processing requested. Do not substitute models, enable Fast/Ultra/model Pro modes, change global settings, buy credits/resets or enable auto-reload. The owner's subscription plan is not restricted. Verify actual model/effort/speed from client evidence at assignment or a material change; distinguish VERIFIED, USER_SELECTED and UNKNOWN. Account usage is not task cost.
- Chief may create, assign, replace and release visible Builders without repeat approval within the owner-agreed concurrency limit in NEXT_TASK. Delegate independent bounded work when it reduces time to the result. Assign explicit file/evidence ownership and isolated named codex/spread-v1-<slice> branches/worktrees. Never allow concurrent writers to the same files. Builders do not spawn agents, self-accept, merge or push main. Do not use internal agents configured under historical defaults.
- Specify outcomes and constraints, not every function. Builder chooses implementation structure, relevant reuse and tests. An equally valid alternative is not a defect. Keep one integration owner; do not add permanent architects, reviewers, watchers or duplicate audits.
- Policy, fees, sizing, delay/freshness thresholds, minimum exemptions, resource caps, PnL/FLAT semantics and release authority cannot change without explicit owner authorization recorded in the active contract. A proven conflict with official or observed evidence must be surfaced, not silently implemented or waived.

## Autonomy, defects and stopping

Routine authorized edits, offline checks, required dependency setup, local candidate commits, evidence preservation, handoff, governing-document commits and accepted integration/publication are pre-approved. Tool/platform restrictions still apply; do not bypass them or alter global approval/security settings.

Stop an unreliable calculation or unsafe action immediately and preserve its exact failure evidence. A new bug does not automatically stop all development: diagnose and correct a bounded defect within the agreed behavior, then verify independently. A distinct exception requires a compact phase/time/actions/versions/positions/pending/event/index/traceback packet before further work. Ask the owner only if correction requires changed policy/risk, new collection, additional authority or expansion beyond the finite objective. Do not turn a local defect into a general infrastructure project.

A review must identify exact candidate SHA, violated contract, location, counterexample, impact and required verification. Read the full actual diff and relevant surroundings; widen only for concrete risks. Use one consolidated CHANGES_REQUESTED for justified corrections. Continue in the same healthy session/branch when its state is clear. Another correction round needs a newly understood cause and finite check; repeated failure without new diagnosis is a blocker, not an endless series. After formal REJECT, preserve the candidate immutably and start correction from accepted main, reusing its patch only as unaccepted evidence. No history rewriting or force-push.

Chief acts on assignment, a concrete blocker, a reviewable result, or a measurement result. Use completion events or bounded tool waiting; no periodic management polling, encouragement loops, management heartbeat or infinite Goal. Do not promise an unverified callback. If delivery is unavailable, leave a durable checkpoint and an exact manual result-transfer message. Required exchange transport heartbeats and deterministic finite schedulers are unaffected; there is no LLM in the market loop.

## Context recovery and handoff

Compaction alone does not require a new session. After compaction or suspected context loss, recover the current objective/authority, branch and actual diff, source/import identity, completed checks, remaining work and active process ownership from Git/current documents/evidence. Continue when these agree unambiguously. Do not claim preservation from a summary alone.

Use CONTEXT_HANDOFF_REQUIRED when state cannot be reconstructed, decisions conflict, or accumulated work prevents reliable continuation. At a safe checkpoint preserve WIP_NOT_ACCEPTED or READY_FOR_REVIEW, honest DONE/NOT_RUN checks and process state. Stop predecessor writes before transferring ownership. Never merge WIP merely to rotate; never run two Chiefs. An authorized independent calculation may continue only with explicit verified identity, bounds and transferred ownership; do not launch a duplicate.

A real ownership handoff includes role/session identity, exact main/candidate/branch/worktree/status, the full unachieved objective and invariants, decisions and reasons, source/import fingerprints, evidence/commands/exits, uncertainties, one next action and process/follow-up ownership. Store larger packets in existing ignored owner-only evidence space. Unchanged-tree evidence survives handoff and docs-only commits; verify the actual tested implementation/configuration rather than treating every new HEAD as invalidation.

## Verification proportional to risk

- Changes to arithmetic, positions, execution, causal ordering or data/result preservation require distinguishing adverse regressions, relevant integration checks and one final clean isolated Python 3.11 full suite before acceptance. Preserve secret isolation, funding, no-replay/reconciliation and exact-flat regressions. Expected numerical results must be computed independently of the tested math helper.
- Isolated low-impact helper changes require focused behavioral and affected-interface checks; broaden only for a concrete failure or risk. Documentation-only changes require consistency, scope/diff and identity checks, not a full suite. Do not add tests that merely mirror implementation.
- A code/tests change invalidates the checks it affects. Exact unchanged integration needs no duplicate suite. Classify unrelated environment failures against base in isolation; never mask them with skips or repair unrelated legacy code. No unrelated test cleanup.
- Passing tests alone do not establish acceptance. Chief independently reviews scope, evidence provenance and relevant adverse behavior. Taste-only refactoring and hypothetical production hardening do not block the finite result. Record a concrete failed acceptance criterion instead of inventing a new release requirement.

## Git and evidence preservation

Before edits verify the root, named branch, accepted base/HEAD and working changes. Bind runs to actual implementation, import paths, inputs and configuration. Stage only owned paths. Do not reset, stash or commit another writer's work for cleanliness.

Chief integrates only accepted changes and verifies remote main before claiming publication. A main update must not overwrite an active Builder checkout. Builders incorporate governing updates at a safe checkpoint, preserving their implementation and existing test evidence. Preserve branches, commits, worktrees, unaccepted patches and outputs; never delete them for UI cleanup or because a newer main exists.

Original inputs, manifests, claims and historical results are immutable. Stream large JSONL inputs rather than loading or dumping them wholesale. New outputs use separate versioned owner-only directories outside Git. A missing terminal result is incomplete, not zero profit or successful execution. Completed evidence must remain interpretable independently of a live tool cell or a mutable checkout.

Batch material STATUS/NEXT_TASK updates; do not make ceremonial commits. The current finite completion criterion belongs in NEXT_TASK. Deferred work is not an implicit prerequisite and cannot silently reopen old campaigns.

## Communication and navigation

Owner-facing reports are concise Russian; governing files remain English. Lead with result, evidence, limitations and next action. Report operational IDs/settings once at assignment or material change, not in every progress message. Do not invent cost or context percentages.

Use descriptive Chief/Builder titles and the native task card when dispatching, returning a candidate or transferring ownership. Include full technical identity in the durable handoff; do not clutter ordinary findings with repeated unavailable fields. If the client cannot provide a card/link, state the concrete limitation once and continue authorized work. Never invent URLs, publish share snapshots, duplicate tasks or resend assignments solely to manufacture navigation. Pin current participants and the last accepted Builder when supported.

## Safety invariants

Public-only research, paper default, points $0. Private/account/fee-reader endpoints, credentials, signing, order preparation/dispatch, testnet/mainnet orders, funds, transfers, withdrawals and strategy execution are prohibited. Secrets never enter chat, arguments, logs, reports, databases, fixtures or Git. Existing private/testnet modules remain isolated and uninvoked. Public collection needs an explicit prospective gate in NEXT_TASK; old smoke/pilot/campaign permissions are not reusable.

Use official sources and observed evidence for venue semantics. Do not invent minimum exemptions, fills, prices or profits. Required safety fields are strict; irrelevant additive inbound fields are tolerated. Preserve exact quantities, no-reuse and causal time; unknown execution is not a fill or a proven no-fill. Funding UNKNOWN and open inventory remain separate from closed execution PnL. No profit/fill-count target may weaken validity checks.

No unrequested markets/sizes, policy tuning, funding strategy, OMS, L3 queue, platform migration, dashboard/Telegram, generic recovery/storage service or front-loaded production hardening. Finish the authorized finite result or identify a specific evidence-backed blocker. A new campaign, hypothesis or real execution requires a new owner decision.
