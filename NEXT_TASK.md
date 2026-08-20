# PAPER-004 — Paper Entry

## Goal

Implement the paper hedge-maker entry order, versioned cancel/replace and trade-through fill model, immediate RISEx taker hedge, atomic in-memory transition to a fully opened two-leg paper position, and immediate first HOLD/EXIT decision. No exit-order execution, later lifecycle, SQLite, or CLI wiring.

## Mandatory Design Checkpoint

Before code changes, report the proposed minimal state/contracts and atomic decision flow for activation, route lock, order/version identity, reprice/reset, event-key dedup, cumulative eligible volume, strict cutoff, immediate exact-q RISEx taker, timestamps/fees, funding recomputation from actual open time, and immediate HOLD-vs-EXIT comparison. Map every cancellation reason and show fixture/test cases. Do not implement until Architect explicitly approves.

## Deliverables

- Create hedge LIMIT POST_ONLY only for the single selected eligible route, with no active order/position and executable fresh RISEx depth; then lock route.
- Cancel only for negative own plan, stale data, invalid route, lost executability/depth, cutoff, or process-restart contract (restart execution remains PAPER-005).
- Reprice every 10 seconds; changed price creates a new version and resets cumulative volume.
- Count only same venue/market/current-version, tick-aligned, unprocessed, `is_orderbook_match=true`, strict pre-cutoff trades with correct aggressor and one-tick trade-through.
- Full maker fill at cumulative eligible quantity >= order quantity, priced at paper limit; no partial-position/queue/hidden liquidity model.
- Immediately fill RISEx taker at fresh exact-q VWAP, persist entry timestamps/prices/fees, set `position_opened_at = risex_taker_fill_at`, recompute funding from actual open, compute exit/hold/unwind values, and choose HOLDING only on strict hold improvement; otherwise EXITING_NORMAL.

## Acceptance tests

- Wrong aggressor, equal touch, one-tick trade-through, insufficient/multiple cumulative trades.
- Duplicate event key ignored; replacement resets volume and old version cannot fill.
- Stale data and lost RISEx exact-q depth cancel with exact reasons.
- Cutoff uses exchange timestamp, not receipt/coroutine order.
- Full maker fill causes immediate RISEx taker with exact VWAP and fees.
- Maker/taker/open timestamps and `position_opened_at` are exact.
- Funding is recomputed from actual open time, not order creation or T−120 quote.
- Immediate post-entry HOLDING vs EXITING_NORMAL decision uses strict comparison.

## Constraints

- Work on `codex/paper-004` from accepted `main`; no subagents or product-rule changes.
- Use deterministic in-memory evidence only; persistence arrives in PAPER-006.
- Do not add partial positions, queue probability, route switching, taker failure, or exit execution.
- Run focused tests and full `pytest`, review the diff, commit, then report in at most 20 lines.
