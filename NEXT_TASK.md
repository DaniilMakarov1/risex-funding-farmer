# TELEGRAM-002-FIX-001 — Two-Decimal Bot Values

## Objective

Render every monetary PnL/funding value shown in outbound Telegram text with exactly two digits after the decimal point.

## Scope and acceptance

- Branch: `codex/telegram-002-fix-001-two-decimals` from current main.
- Use one presentation-only Decimal formatter for FULL digest Expected PnL, eligible-opportunity Expected PnL, position-closed final PnL, and funding received/reconciled cash.
- Numeric text must always contain exactly two fractional digits, including zero and negative values. UNKNOWN remains UNKNOWN.
- Preserve full-precision values in notification payload fields, Scanner results, SQLite, evidence, and decision/dedupe inputs.
- Add focused tests for rounding, trailing zeroes, negative/sub-cent values, UNKNOWN, and authoritative payload precision; run full pytest, compileall, and diff-check.
- Do not change economics, fees, sizing, ranking, funding rules, cadence, lifecycle, adapters/endpoints, secrets, or paper/live boundaries.
- Do not touch the active Stage B process/database during implementation or review. After acceptance, restart only if flat and safe.
