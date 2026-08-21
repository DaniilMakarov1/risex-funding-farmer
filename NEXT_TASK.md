# TELEGRAM-002 — Full Scan Digest

## Objective

Send one concise outbound Telegram digest after every authoritative persisted `FULL` scan, using only existing route rows.

## Expected baseline

- Builder starts from the Architect authorization commit based on `231a6172239de8267a2c985ce4ab55b3434e0ac6`.
- Branch: `codex/telegram-002-full-scan-digest`.
- Existing Stage B PID/session/database remain untouched through implementation and review.

## Required behavior

- Enqueue exactly one `FULL_SCAN_DIGEST` after each completed and persisted scan whose `scan_kind == "FULL"`.
- Never enqueue this digest for `INITIAL`, `FOCUSED`, or `RECOVERY` scans.
- Render up to the same 15 already ordered authoritative route rows; do not invoke Scanner or recalculate economics.
- Each route line has exactly three fields: `Ticker | Route | Expected PnL`.
- Route names RISEx and the hedge venue plus both LONG/SHORT sides.
- Expected PnL copies `planned_maker_net_pnl_usd`; render `UNKNOWN` when absent. Formatting may shorten display precision but must not change the payload value or decision data.
- Include exact scan UTC and `OPPORTUNITY`/`NO TRADE` in a short header; one Telegram message must stay within the Bot API text limit.
- Stable event ID is derived from the persisted full-scan timestamp. Existing bounded queue, timeout/retry, secret handling, and lifecycle/opportunity notifications remain unchanged.

## Tests

- One FULL scan produces exactly one digest with the authoritative timestamp and route order.
- Digest rows use the authoritative ticker, both-venue route, and planned PnL without economics recomputation.
- Blocked/negative routes remain visible; unknown PnL renders `UNKNOWN`.
- INITIAL, FOCUSED, and RECOVERY scans produce no digest.
- Repeated enqueue of the same full-scan event ID deduplicates.
- Digest length is bounded and up to 15 rows are retained for normal route lengths.
- Existing Telegram, runtime, scheduling, fixture, and full deterministic tests pass; compileall, token-pattern scan, and `git diff --check` pass.

## Forbidden scope

No Scanner/economics/funding/fees/sizing/threshold/cadence/scheduling/lifecycle/order/fill/persistence/adapter/endpoint changes. No inbound runtime, Telegram commands, new scheduler, services, frameworks, private endpoints, real orders, or live trading. Builder does not run Telegram or `paper-run`.

## Acceptance

Architect independently reviews the production call graph and tests. Only after ACCEPT and a clean synchronized main may the existing flat Stage B be safely restarted with inherited environment secrets and the same database.
