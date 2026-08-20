# PAPER-002 — Official Market Data

## Goal

Implement public REST/WebSocket adapters for RISEx, Extended, and Nado using only current official documentation and public APIs. Normalize metadata, books, trades, funding cash, settlement timing, and stream health into PAPER-001 contracts. No scanner, broker, lifecycle, CLI, authenticated endpoint, or trading functionality.

## Mandatory Design Checkpoint

Before code changes, inspect official sources and report to Architect:

- exact official documentation/API URLs used for each venue;
- public REST and WebSocket endpoints/messages for metadata, BBO/book, trades, funding, volume, heartbeat, snapshots, sequence/recovery, and applied funding where available;
- unit, multiplier, price, quantity, funding, eligibility, and timestamp semantics with official evidence;
- proposed normalization and explicit UNKNOWN/blocker fields;
- fixture plan and any proven RISEx funding-semantics blocker.

Do not implement until Architect explicitly approves the checkpoint. Do not use aggregators, UI scraping, manually copied live values, or other repositories/projects.

## Deliverables after approval

- Minimal async adapter contract and venue implementations in `exchanges/` plus coordination in `market_data.py`.
- Official metadata normalization and parity eligibility; unknown evidence blocks entry.
- REST snapshots/recovery and available WebSocket book/trade/funding/health processing.
- Raw timestamps, official/synthetic trade keys, aggressor and orderbook-match normalization.
- Per-stream connection/book/sequence health, documented heartbeat, stale/gap detection, and funding freshness.
- Fixture-only deterministic CI tests; live smoke checks are opt-in.

## Acceptance tests

- Valid linear perpetual; spot/non-perpetual, RFQ, and off-hours exclusion.
- Unknown multiplier and unknown funding eligibility.
- Reconnect, sequence gap, heartbeat timeout, and incomplete recovery.
- Official trade ID and deterministic synthetic key.
- Aggressor normalization and `is_orderbook_match` true/false/unknown.
- Extended percentage funding normalized to cash per canonical base.
- Nado per-unit funding normalized without another price multiplication.
- RISEx semantics blocker path when official evidence is insufficient.

## Constraints

- Work on `codex/paper-002` from accepted `main`; no subagents or product-rule changes.
- Keep all access public/unauthenticated; CI never depends on live services.
- Run focused tests and full `pytest`, review the diff, commit, then report in at most 20 lines.
