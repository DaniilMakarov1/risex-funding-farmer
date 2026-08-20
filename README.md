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

The commands use a local SQLite paper database. With no fixture, commands fail
closed when public data is unavailable or RISEx semantics remain unknown:

```bash
risex-farmer --db paper.db scan-once
risex-farmer --db paper.db paper-run
risex-farmer --db paper.db report
```

`scan-once` evaluates the current official public-data opportunity set. `paper-run` runs the ordinary single-process simulator without LLM calls. `report` summarizes persisted paper evidence. An open position is never force-closed merely because a run ends.

Deterministic fixture mode is intended for CI and local paper verification and
never accesses the network:

```bash
risex-farmer --db paper.db scan-once --fixture tests/fixtures/paper_006/no_opportunity.json
risex-farmer --db paper.db paper-run --fixture tests/fixtures/paper_006/positive_closed.json
risex-farmer --db paper.db report
```

See `SYSTEM_SPEC.md` for frozen product behavior, `STATUS.md` for the accepted baseline, and `NEXT_TASK.md` for the current authorization boundary.
