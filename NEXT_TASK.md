# PAPER-007-FIX-004 — Public REST Timeout 30 Seconds

## Objective

Increase the existing shared public `aiohttp` runtime session total timeout from 15 seconds to 30 seconds so a slow but responsive official public response can complete.

## Scope

- Branch: `codex/paper-007-fix-004-public-rest-timeout` from current main.
- Change only the shared public runtime session timeout and focused deterministic coverage/documentation needed to prove the 30-second value.
- Keep one shared session and the existing adapters, endpoints, absolute scheduling, retry state, readiness behavior, and evidence schema.
- The active Stage B process/database remain untouched during implementation and review.

## Acceptance

- A deterministic test proves the runtime public session uses a 30-second total timeout and closes cleanly.
- Existing focused and full tests pass; compileall and `git diff --check` pass.
- No economics, fees, sizing, Scanner, scheduling, lifecycle, Telegram, adapter, endpoint, private/authenticated, real-order, or live-trading changes.

After Architect acceptance, safely restart the flat Stage B on the same database so the new timeout becomes active. Do not force-close a position.
