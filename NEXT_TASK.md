# PAPER-007-FIX-006 — RISEx Checksum Resubscribe Recovery

## Objective

Replace the incorrect RISEx checksum-gap REST replay loop with the official public WebSocket `unsubscribe → subscribe → snapshot` recovery lifecycle so healthy books can recover without inventing an ordering relation that the API does not provide.

## Scope

- Branch: `codex/paper-007-fix-006-risex-book-resubscribe` from current main.
- On a verified RISEx full-book checksum mismatch, fail the affected channel closed, invalidate all books served by that combined orderbook subscription, and send one official public unsubscribe/subscribe pair with the same market set.
- Buffer no pre-snapshot updates across the WebSocket snapshot boundary; each new RISEx snapshot is authoritative for its market, after which checksum validation resumes normally.
- Keep socket lifecycle evidence separate: a logical channel resubscribe creates book-resync evidence but no `PUBLIC_SOCKET_DISCONNECTED` or `PUBLIC_SOCKET_RECONNECTED` rows.
- Do not change Extended or Nado recovery, the accepted 30-second HTTP timeout, Telegram calculations, or any strategy/economic rule.
- The active Stage B process and database remain untouched during implementation and review.

## Acceptance

- A deterministic combined RISEx regression proves checksum mismatch sends exactly one unsubscribe/subscribe pair, receives one fresh snapshot per subscribed market, restores readiness, and resumes valid checksum updates.
- The regression proves there are no REST snapshot recovery calls and no physical socket lifecycle rows for the logical resubscribe.
- Repeated updates while snapshots are pending do not trigger a resubscribe burst or leave stale recovery state.
- Existing focused and full tests pass; compileall and `git diff --check` pass.
- No funding-age threshold, Scanner formula, fees, sizing, ranking, lifecycle, Telegram rendering/delivery, private/authenticated, real-order, or live-trading changes.

After Architect acceptance, safely restart the flat Stage B on the same database so both accepted corrections become active. Do not force-close a position.
