# TELEGRAM-001-FIX-001 — Flood-Control and Outage Dedupe

## Objective

Correct two confirmed P1 defects without changing Telegram's outbound-only boundary or any paper strategy behavior.

## Expected baseline

- `main`/`origin/main`: Architect authorization commit based on `4101afecab2c712d591a77f84b5b082d84882104`.
- Accepted TELEGRAM-001 implementation: `cc321883b7699c142d3d275ad1f9931a5a57e869`.
- Builder branch: `codex/telegram-001-fix-001`.
- Existing PAPER-007 Stage B process and database remain untouched through implementation and review.

## Allowed implementation

- In `notifications.py`, parse Telegram HTTP 429 JSON `parameters.retry_after`, accept only finite positive numeric seconds, cap at 30 seconds, and never perform a zero-delay retry. Missing/invalid retry data must stop delivery or use a positive bounded fallback. `ClientConnectorError` uses a positive bounded backoff. Timeout remains non-retriable; attempts remain bounded; response bodies/descriptions and secret-bearing URLs never escape to logs or exceptions.
- In `runtime.py`/notification state only, unify a physical Extended book socket episode and its logical book-resync notification into one semantic outage identity while preserving every existing FIX-003 persisted evidence row, episode ID, cardinality, and order. One physical outage produces one loss/recovery notification pair, clears active outage state, and permits a later independent book gap notification. Combined sockets retain one pair per physical episode.

## Required tests

- 429 JSON retry-after 7 sleeps once for 7 seconds and retries once within `max_attempts`; malformed/missing/zero values cannot burst; oversized values cap at 30 seconds.
- Connector failure uses positive bounded backoff; timeout makes one attempt.
- Synthetic token, chat ID, token-bearing URL, response description/body never enter logs, SQLite, payloads, or exceptions.
- A production-path Extended book EOF persists separate socket/resync rows but emits exactly one loss and one recovery, leaves no active outage identity, and a later independent gap emits again.
- Combined socket notification cardinality remains one pair per episode.
- Focused tests, full `pytest`, compileall, token-pattern scan, `git diff --check`, and final diff review pass.

## Forbidden scope

No Scanner/economics/funding/fees/sizing/threshold/cadence/scheduling/lifecycle/order/fill/adapter/endpoint changes. No inbound Telegram, `/scan`, `getUpdates`, scheduler, SQLite reads in Telegram, services, frameworks, real orders, private endpoints, or live trading. Builder does not run Telegram or `paper-run`.

## Acceptance and restart gate

Architect independently reviews production call graphs and tests. Only after ACCEPT and clean synchronized `main` may the existing Stage B be safely restarted with Telegram, and only if it is still flat, its database is healthy, and both environment secrets are available without disclosure. Missing chat ID or unsafe secret transfer blocks restart without stopping the current process.
