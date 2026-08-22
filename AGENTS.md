# Agent rules

This is a new standalone paper-only research project. Do not inspect, import, or copy other repositories, local projects, old RISEx/Radar code, or old architecture documents.

## Sources of truth

Only `AGENTS.md`, `SYSTEM_SPEC.md`, `STATUS.md`, `NEXT_TASK.md`, and `README.md` govern the project. Git history is the task history. Do not create other governance or task-history documents.

## Roles

- The user-appointed Chief Reviewer formulates work only for the Architect and independently reviews the Architect's decisions and results. The Chief Reviewer does not direct the Builder or write implementation code.
- Architect owns architecture, orchestration, review, acceptance, and source-of-truth updates.
- Exactly one Builder may work at a time. Builder must not spawn agents.
- Builder implements only the bounded milestone in `NEXT_TASK.md` and must not begin the next milestone.
- Do not create milestones beyond BOOTSTRAP-000 and PAPER-001 through PAPER-006, except the explicitly authorized PAPER-007 experiment, TELEGRAM-001 outbound-notification work, bounded TELEGRAM-001-FIX-001 correction, TELEGRAM-002 full-scan digest and its bounded two-decimal display correction, PAPER-007-FIX-004 public REST timeout correction, PAPER-007-FIX-005 first-full funding-freshness correction, PAPER-007-FIX-006 RISEx checksum-resubscribe correction, PAPER-007-FIX-007 Extended expected/applied funding and socket-health separation, PAPER-007-FIX-008 public evidence/catalog/heartbeat/digest/stop correction, and PAPER-007-FIX-009 public stream/time/startup consistency correction.
- The user's ongoing stabilization decision authorizes successive strictly bounded corrective slices needed to make the existing paper system correct, without repeated user selection of technical task numbers. Administrative stabilization labels and branches are audit identifiers, not new product milestones.
- Any new product behavior, economics, strategy, API/private access, live work, or Telegram expansion still requires a separate explicit user decision.

## Git workflow

- `main` contains only Architect-accepted work.
- Builder starts from current `main` and works on `codex/<milestone-lowercase>`.
- Before edits, Builder reports repository root, branch, HEAD, and short status.
- Builder must not work on `main`, merge, rebase, rewrite history, or alter unrelated changes.
- Builder runs focused tests and full `pytest`, reviews the diff, and creates one milestone commit.
- Architect reviews the branch and either accepts, requests a fix in the same branch, or rejects it.
- At most two fix cycles are allowed; otherwise stop with `BLOCKED — TASK DID NOT CONVERGE`.
- Architect integrates accepted work to `main` without rewriting the Builder commit.

## Scope and safety

- Python 3.11; one async process; `aiohttp`, `sqlite3`, `Decimal`, dataclasses, `pytest`, and `pytest-asyncio`.
- Paper only: public endpoints and public market data. No API keys, authentication, account/private endpoints, real orders, collateral, positions, or live execution.
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
- After an accepted stabilization slice, the Architect may authorize only the next strictly corrective slice required by evidence and must record exactly one active slice in `NEXT_TASK.md`. Do not infer authorization for product expansion; PAPER-007 and TELEGRAM remain within their explicit user-approved boundaries.
