# RISEx Funding Farmer

A small, standalone paper trader for testing whether a delta-neutral RISEx-points funding strategy can produce non-negative trading PnL after configured fees and the documented paper execution model.

The project is **paper only**. It uses official public RISEx, Extended, and Nado market data. It never uses trading keys, authenticated/private endpoints, real orders, collateral, or live positions. Live trading is not a mode switch and is outside this specification.

## Requirements

- Python 3.11
- `aiohttp`
- `pytest` and `pytest-asyncio` for tests

## Setup

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[test]'
```

## Tests

```bash
pytest
```

## Commands

The commands use a local SQLite paper database. With no fixture, `scan-once`
performs a read-only public REST scan and `paper-run` maintains read-only public
market-data streams until Ctrl+C or SIGTERM:

```bash
risex-farmer --db paper.db scan-once
risex-farmer --db paper.db scan-once --format table
risex-farmer --db paper.db paper-run
risex-farmer --db paper.db report
```

`scan-once` defaults to backward-compatible JSON. Add `--format table` for a
human-readable view of the same ordered routes (up to 15), including funding
countdown, RISEx/hedge/net funding, entry/exit fee and execution components,
expected net PnL, plain-language trade status, and public venue readiness.
`paper-run` continues through `NO_TRADE` and venue outages, reconnecting public
streams and failing affected routes closed. `report` summarizes persisted paper
and runtime evidence. An open position is never force-closed merely because a
run ends.

RISEx contract quantity and forecast funding use visibly reported paper-only
fallback assumptions. They are enabled only for this experiment and fail closed
when public metadata, grids, stable-quote identity, price, rate, or schedule
checks are inconsistent. They are never represented as official applied funding.

Deterministic fixture mode is intended for CI and local paper verification and
never accesses the network:

```bash
risex-farmer --db paper.db scan-once --fixture tests/fixtures/paper_006/no_opportunity.json
risex-farmer --db paper.db paper-run --fixture tests/fixtures/paper_006/positive_closed.json
risex-farmer --db paper.db report
```

See `SYSTEM_SPEC.md` for frozen product behavior, `STATUS.md` for the accepted baseline, and `NEXT_TASK.md` for the current authorization boundary.
