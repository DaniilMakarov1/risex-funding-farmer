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

The shared public HTTP runtime session uses a 30-second total request timeout so
large, slow official responses can complete without changing scan cadence or
retry scheduling.

The commands use a local SQLite paper database. With no fixture, `scan-once`
performs a read-only public REST scan and `paper-run` maintains read-only public
market-data streams until Ctrl+C or SIGTERM:

```bash
risex-farmer --db paper.db scan-once
risex-farmer --db paper.db scan-once --format json
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

On a RISEx orderbook checksum mismatch, `paper-run` follows the official public
WebSocket recovery contract: it keeps affected books unusable, unsubscribes and
resubscribes the orderbook channel, and accepts the new stream snapshots before
resuming calculations. It does not combine unordered REST and WebSocket states.

### Optional outbound Telegram notifications

Telegram delivery is disabled by default and does not change scanning or paper
trading. To enable outbound `sendMessage` notifications for `paper-run`, choose
a bot token and destination chat, and set all three
environment variables before starting a new run:

```bash
export RISEX_TELEGRAM_ENABLED=true
export RISEX_TELEGRAM_BOT_TOKEN='newly-rotated-token'
export RISEX_TELEGRAM_CHAT_ID='destination-chat-id'
risex-farmer --db paper.db paper-run
```

Credentials are read only from the environment and must not be committed,
logged, persisted, or placed in CLI arguments. Any explicit risk acceptance for
a disclosed token is recorded without the token value. This integration is outbound-only: the
application does not poll `getUpdates`, accept commands, trigger scans, or place orders.
An Architect may use a separately authorized one-shot `getUpdates` diagnostic
only to discover the configured destination; it is not part of `paper-run`.
Delivery is best effort; a full queue or Telegram outage can drop messages so it
cannot delay market-data processing, strategy deadlines, or safe shutdown.
Every completed authoritative `FULL` scan sends one concise digest containing up
to 15 existing ordered rows in `Ticker | Route | Expected PnL` form. The values
come directly from the runtime's scanner result; Telegram does not recalculate
economics. Monetary values in Telegram text are displayed with exactly two
fractional digits while authoritative Decimal values retain full precision.
INITIAL, FOCUSED, and RECOVERY scans do not send this digest.

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
