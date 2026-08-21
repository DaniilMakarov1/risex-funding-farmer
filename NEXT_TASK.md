# TELEGRAM-001 — Outbound Runtime Notifications

## Objective

Add an optional outbound Telegram `sendMessage` delivery sink for notifications produced by authoritative `PublicPaperRuntime` scans and persisted runtime/lifecycle transitions. Telegram never scans, recalculates, reads SQLite, connects to exchanges, accepts commands, or affects decisions and cadence.

## Expected baseline

- Current accepted `main` after this authorization commit.
- PAPER-007 Stage B continues untouched in its existing detached process and database.
- Telegram is disabled by default.

## Allowed implementation

- Add one small notification module with an immutable payload, no-op sink, and bounded asynchronous Telegram delivery worker.
- Wire environment-only configuration into `paper-run`; fixture and ordinary behavior remain unchanged when disabled.
- Add narrow runtime hooks only after authoritative completed scans or persisted transitions.
- Add deterministic fake-session tests and README setup documentation.

## Payload and events

- Payload: stable `event_id`, notification kind, exact UTC event time, rendered text; opportunity payload also carries authoritative ticker, both-venue route, and unchanged `planned_maker_net_pnl_usd`.
- Events: runtime started/ready; new/materially changed/disappeared eligible opportunity; entry activated; position opened; funding received/reconciled; exit started; position closed with final PnL; critical data loss; data recovery; safe stop.
- Opportunity dedupe state: route + target cycle + displayed-cent expected-PnL state. Identical state never re-enqueues; disappearance emits once.
- Lifecycle/runtime events dedupe by stable authoritative event ID and enqueue at most once.

## Delivery and secrets

- Bounded `asyncio.Queue`, `put_nowait`, separate worker, finite timeout and finite retry policy, no burst or infinite retry.
- Ambiguous timeout is not retried because Telegram has no idempotency key. Explicit pre-acceptance/flood-control retries remain bounded.
- Environment variables only: `RISEX_TELEGRAM_ENABLED`, `RISEX_TELEGRAM_BOT_TOKEN`, `RISEX_TELEGRAM_CHAT_ID`.
- Missing configuration fails closed to disabled/error before paper runtime startup; token and chat ID never enter payloads, Git, SQLite, logs, exceptions, or messages.
- The token disclosed before this task is compromised and forbidden; tests use synthetic secrets only.

## Forbidden scope

- No `/scan`, `getUpdates`, inbound commands, Telegram scheduler, web server, dashboard, Scanner copy, formulas, exchange access, SQLite reads, or direct `scan-once` calls.
- No changes to economics, strategy, route selection, funding, fees, sizing, cadence, order/fill/lifecycle semantics, adapters, exchange endpoints, trading persistence, paper/live boundaries, or the running Stage B process/database.

## Required tests

- Opportunity payload copies authoritative scan values and names both venues.
- Telegram module cannot call Scanner or exchange clients and does not read SQLite.
- Unchanged opportunity state and one lifecycle event do not duplicate notifications.
- Queue, timeout, failure, bounded retries, and shutdown cannot delay runtime deadlines or safe stop.
- Disabled configuration preserves existing behavior exactly.
- Synthetic token/chat ID never appear in logs, SQLite/runtime evidence, payload text, or exceptions.
- No inbound Telegram method is called; only official Bot API `sendMessage` is permitted.
- Full `pytest`, compileall, and `git diff --check` pass.

## Acceptance

Architect reviews the production call graph, non-blocking ownership/shutdown, dedupe semantics, secret handling, diff, focused tests, and full suite. No long-running process is restarted or switched to Telegram without a separate user decision.
