# PAPER-007-FIX-007 — Extended Expected Funding and Socket Health Separation

## Baseline and objective

- Start from accepted main `1c51d9ba5bf60bd00d520c158cc39df7d688eb86` on `codex/paper-007-fix-007`.
- Preserve the stopped experiment database. Implement only the independently confirmed Extended expected/applied funding separation and per-stream socket health isolation.
- Official contract: `https://api.docs.extended.exchange/` documents REST `/api/v1/info/markets/{market}/stats` as current `fundingRate`/`markPrice` plus future `nextFundingRate`, and funding WS `data.T/data.f` as a calculated/applied funding record.

## Required implementation

- Keep REST `fetch_funding_quote()` as the sole Extended future Scanner quote. A WS applied record must never replace `MarketObservation.funding`.
- With no open lifecycle, an applied record changes no expected quote, PnL, or settlement cash flow. With an open lifecycle, reconcile only an exact venue/market/settlement key, once. If official applied cash cannot be established from the public record, retain `UNRESOLVED`; never use book midpoint or carry the rate into a future cycle.
- Track Extended `book`, `trade`, and `funding` connection confirmation/readiness independently per market. Use only `connection_book`, `connection_trade`, and `connection_funding`; never create `connection_combined` for Extended.
- A valid message or ping confirms only its own socket. Valid book/trade/funding messages restore only their own data and connection components. Trade restoration occurs even without an active paper order.
- A stale Extended socket invalidates and restarts only that stream. Preserve one ordered physical disconnect/reconnect episode, no duplicate rows, no stale readiness/dedupe state after recovery.
- Keep HTTP total timeout 30 seconds, scan cadence, silence threshold, endpoints, economics, fees, sizing, ranking, thresholds, lifecycle, Telegram presentation, and paper/live boundaries unchanged.

## Required deterministic tests

- A regression that fails on baseline: REST predicted → WS applied → next FULL retains future expected quote, numeric Extended PnL, and no funding-eligibility/elapsed-cycle blockers.
- Applied record without a position is inert; with a position it matches only exact settlement identity, is idempotent, and mismatches are ignored/unresolved without guessed cash.
- Independently stale/recover book, trade, and funding sockets; verify isolation, correct components, no Extended `connection_combined`, and ordered one-to-one disconnect/reconnect evidence.
- Two Extended markets, three socket kinds each, at least two FULL scans with WS funding records between them: numeric PnL remains where otherwise valid and no stale components remain.
- Tests inspect production call paths and persisted runtime evidence. Run focused tests, full pytest, compileall, diff-check, and secret scan.

## Workflow and forbidden scope

Builder returns a DESIGN CHECKPOINT before edits, then one bounded commit. No subagents, no live network, no Stage B/database interaction. Architect independently reviews and may request at most two same-branch fix cycles. No real orders, private/authenticated endpoints, live trading, new framework, or unrelated refactor.
