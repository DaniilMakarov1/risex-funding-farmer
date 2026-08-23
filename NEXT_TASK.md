# PAPER-007-STABILIZATION-007 — Cadence-safe Extended Watchdog Rotation

Status: `RED ONLY — PRODUCTION IMPLEMENTATION NOT AUTHORIZED`.

This is one fresh corrective slice explicitly authorized by the user after STABILIZATION-005 and STABILIZATION-006 were blocked. It starts strictly from published `main == origin/main == 037c4df35de6cc8dfddce48a50b6c8af488b0908`. Failed 005/006 branches, commits, tests, samples, diffs, and quarantine objects are immutable audit history and must not be inspected, imported, copied, cherry-picked, or reused.

## Narrow contract

- Runtime remains the sole cadence, Extended session, watchdog, and transport-task owner. Fix only the proven defect that `tick()` synchronously waits for an old Extended transport task to retire and therefore delays scan and open-position cadence.
- Fence the old session before successor activation, install exactly one successor, and prevent all old-session publication after fencing. Watchdog stale/restarted evidence remains logical and must not fabricate `PUBLIC_SOCKET_*` lifecycle rows.
- Collect normal delayed cancellation when it eventually completes, removing ownership and consuming expected cancellation without warning or leak. Surface a genuine unexpected retirement exception through existing runtime evidence/error semantics.
- Preserve the accepted shutdown implementation: runtime stop cancels and awaits owned tasks as it already does. Do not add fatal shutdown semantics, session-close budgets, task-cap state machines, config constants, process-kill behavior, or tests for a coroutine that suppresses `CancelledError` forever. Such a coroutine remains an external process-supervision limitation.
- Production scope is only `src/risex_farmer/runtime.py`; tests only `tests/test_runtime.py`. No adapter, storage schema, lifecycle, Scanner, Telegram, economics, heartbeat, timeout, private/auth/live, or broad refactor change.

## Newly authored RED requirements

1. Exercise the real `tick()` -> `_check_extended_health()` -> `_restart_extended_stream()` path with an old task that acknowledges cancellation but remains briefly gated. Exact accepted main must leave the due `PUBLIC_SCAN_DEADLINE` blocked until gate release.
2. Use a real valid `ScanSnapshot`. After release, exact main must complete the rest of `tick()` without fixture exception, pending-task warning, or leak.
3. Specify prompt candidate return, old-session fencing, and exactly one successor before old retirement completes.
4. Specify idempotence under repeated concurrent health checks and prove the fenced old task cannot mutate readiness, book, observation, confirmation, or logical/physical evidence.
5. Specify eventual removal and outcome consumption for normal delayed retirement; a genuine unexpected exception uses existing runtime evidence/error behavior without redesigning shutdown.
6. Preserve true EOF one-disconnect/one-reconnect, heartbeat/PING/PONG, absolute FULL cadence, open-position evaluation, Scanner/`UNKNOWN`, Telegram, economics, and all existing safe-stop tests.

## Gates

- Exactly one Builder may edit only `tests/test_runtime.py` for RED and must not spawn agents. RED must be newly authored from this clean baseline and substantially minimal; do not inspect or reuse failed slice work.
- Reproduce exact old-main failure in a disposable copy with explicit worktree `PYTHONPATH` and import identity. Report exact test names/hunks, timing rationale, failure output, and successful post-gate fixture completion.
- Run all existing runtime tests and focused EOF/heartbeat/cadence/open-position/Scanner/Telegram/safe-stop preservation. Create one RED-only commit and stop for Chief review before GREEN.
- Maximum two later fix cycles. Do not merge, push, soak, restart Stage B/Telegram, or update acceptance governance before Chief decisions.

## Separate future real-money gate

The user's future real-money request is recorded only as requiring a separate Chief-approved design and security review. No credentials, accounts, authentication, private endpoints, live orders, or real-money implementation are in scope here.
