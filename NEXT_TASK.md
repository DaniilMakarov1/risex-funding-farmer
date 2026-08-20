# PAPER-001 — Core Models and Exact Economics

## Goal

Implement the exact core contracts and deterministic economics required by `SYSTEM_SPEC.md`. Do not implement venue adapters, networking, scanning, lifecycle orchestration, CLI behavior, or SQLite trading storage.

## Deliverables

- Immutable/dataclass domain contracts in `models.py` for canonical markets, books/depth, routes, funding quotes/cycles/settlements, fills/fees, data quality, and the five allowed lifecycle states.
- Fixed paper configuration in `config.py`, represented with `Decimal` where numeric economics are involved.
- Pure exact functions in `economics.py` for tick validation and maker placement, canonical quantity-step LCM and sizing, minimum-order eligibility, exact-quantity VWAP, venue fee calculation, funding cash, planned and actual long/short PnL, settlement authority/replacement, and applied-rate completeness.
- Focused deterministic unit tests. No floats may enter economic calculations.

## Acceptance tests

- Decimal without float inputs.
- Tick alignment and invalid BBO detection.
- Maker placement at 1, 2, and 3+ tick spreads.
- Canonical quantity LCM with multipliers 1, 1000, and fractional.
- Minimum quantity and notional enforcement.
- Exact VWAP and insufficient-depth result.
- Nado minimum taker fee; normal fill-notional fees.
- Correct LONG and SHORT PnL signs.
- Funding cash-per-base multiplication exactly once.
- `ESTIMATED` replaced by `APPLIED_RATE`, not added.
- Applied-rate completeness and deterministic skipped events.
- Actual closed PnL without double-counting fees.

## Constraints

- Work only on `codex/paper-001` from accepted `main`.
- Do not add application/network/storage functionality or new product rules.
- Run focused tests and full `pytest`, review the diff, commit, then report in at most 20 lines.
