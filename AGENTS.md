# Agent rules

This is a new standalone paper-only research project. Do not inspect, import, or copy other repositories, local projects, old RISEx/Radar code, or old architecture documents.

## Sources of truth

Only `AGENTS.md`, `SYSTEM_SPEC.md`, `STATUS.md`, `NEXT_TASK.md`, and `README.md` govern the project. Git history is the task history. Do not create other governance or task-history documents.

## Roles

- Architect owns architecture, orchestration, review, acceptance, and source-of-truth updates.
- Exactly one Builder may work at a time. Builder must not spawn agents.
- Builder implements only the bounded milestone in `NEXT_TASK.md` and must not begin the next milestone.
- Do not create milestones beyond BOOTSTRAP-000 and PAPER-001 through PAPER-006, except the explicitly authorized PAPER-007 experiment, TELEGRAM-001 outbound-notification work, bounded TELEGRAM-001-FIX-001 correction, and TELEGRAM-002 full-scan digest.
- Any further paper milestone, Telegram expansion, or live work requires a separate user decision.

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
- Product rules in frozen `SYSTEM_SPEC.md` change only for a proven official-API contradiction, implementation impossibility, or explicit user decision.
- Never commit secrets, credentials, local databases, caches, or generated reports.

## Milestone completion

- Preserve exact arithmetic and funding/PnL invariants even when simplifying code or documentation.
- CI tests use fixtures only; live smoke checks are opt-in.
- Builder report is at most 20 lines and states branch, commit, tests, and material limitations.
- Architect updates `STATUS.md` and `NEXT_TASK.md` only after acceptance.
- After an accepted milestone, do not infer authorization for another milestone. PAPER-007 and TELEGRAM-001 proceed only within their explicit user-approved boundaries.
