# PAPER-007-FIX-003 — Physical WebSocket Reconnect Evidence

## Objective

Persist physical WebSocket disconnect/reconnect episodes with socket-level identity, while keeping book sequence gaps and snapshot recovery as separate logical data-recovery evidence.

## Expected baseline

- Branch base: current accepted `main` after the FIX-003 governance commit.
- Accepted implementation: PAPER-007-FIX-002 @ `25b6a0d34c867bbb891677d6dddfe94407849b38`.
- Stage A scheduling validation remains PASS; Stage B is paused.

## Allowed scope

- Update the existing public runtime and deterministic runtime tests only as needed.
- Persist `PUBLIC_SOCKET_DISCONNECTED` and `PUBLIC_SOCKET_RECONNECTED` through the existing `runtime_evidence` table.
- Give each physical socket episode a stable identity and correlate its disconnect/reconnect rows.
- Extended identity: venue + stream kind + market.
- Combined RISEx/Nado identity: venue + combined stream + one ordered market list.
- Treat unexpected EOF and exception disconnect consistently.
- Stop creating ambiguous `PUBLIC_STREAM_RECONNECTED` rows.

## Required behavior

- Initial connection is not a reconnect.
- One unexpected physical disconnect creates one disconnect row.
- Failed reconnect attempts do not duplicate the disconnect row.
- First successful reconnect after session establishment and subscriptions creates one reconnect row.
- Disconnect evidence precedes reconnect evidence and both identify the same episode.
- One combined socket creates one pair of rows regardless of market count.
- Planned shutdown, cancellation, and symbol-registry task replacement create no outage/reconnect evidence.
- Book gaps and snapshot recovery retain their existing dedicated events and create no socket lifecycle rows.

## Required tests

- Extended initial connect → unexpected EOF → exactly one disconnect and one reconnect, ordered, two attempts, no shutdown duplicate.
- Combined socket with at least two markets → one disconnect and one reconnect with a stable ordered market list, not per-market rows.
- Same-socket sequence gap and snapshot recovery → recovery evidence but zero socket disconnect/reconnect rows.
- Multiple failed reconnect attempts within one episode → one disconnect row and one reconnect row only after eventual success.
- Tests read actual `runtime_evidence` rows and verify ordering and episode identity.
- Run focused tests, full `pytest`, `compileall`, and `git diff --check`.

## Forbidden scope

- No changes to economics, funding, PnL, fees, sizing, universe, roles, scheduling, orders, fills, lifecycle, exits, endpoints, strategy parameters, or trading persistence.
- No credentials, private/authenticated endpoints, real orders, live trading, LLM runtime calls, frameworks, generic connection managers, or new product milestones.
- Do not modify or reuse existing experiment databases.

## Acceptance

Architect must verify production socket identity, row cardinality/order, logical recovery separation, clean EOF behavior, shutdown/cancellation behavior, full deterministic tests, and a short read-only public smoke. Stage B resumes only after acceptance and on a fresh ignored database.
