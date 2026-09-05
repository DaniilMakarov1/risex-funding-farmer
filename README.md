# RISEx Spread Shadow and legacy Funding Farmer

This repository contains two deliberately separated research contours.

- **RISEx Spread Shadow** is active. It asks whether hypothetical RISEx maker fills retain positive fee-adjusted entry edge against delayed executable exact-quantity Lighter Standard taker hedges. It is public-only shadow research with points valued at `$0`.
- **RISEx Funding Farmer** is a frozen legacy benchmark. Its code and historical evidence remain available, but its funding-boundary profitability path and operational runs are not active.

The public contour and normal startup authorize no private endpoints, credentials,
signing, order preparation, order dispatch, testnet/mainnet writes, real funds,
transfers, withdrawals, or strategy execution. The one opt-in Level-B fee-read
boundary is described below; current acceptance state and the exact active
boundary live in `STATUS.md` and `NEXT_TASK.md`.

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

## Entrypoints

The legacy benchmark retains the `risex-farmer` entrypoint documented below. It is not the active Spread Shadow strategy and must not be resumed as an operational process.

The active contour uses the separate `risex-spread-shadow` entrypoint. It remains
public-only and does not import the opt-in authenticated fee boundary below.

### Opt-in RISEx owner-fee read

After the SS-001K candidate is independently accepted, the bounded Level-B
read may be run explicitly with no arguments:

```bash
risex-spread-shadow-fee-read
# or: python -m risex_spread_shadow.risex_fee_read
```

The runner reads only the existing owner-only RISEx identity and session-signer
files under `~/.config/risex-farmer`, checks the exact mainnet identity and
registered signer through the public readiness endpoint, then asks for the
owner wallet key through hidden local input. The key is used once to sign the
official login message and is never persisted, placed in an environment
variable, passed as an argument, or included in output. The only authenticated
request is the caller-owned `GET /v1/user/fees` read. Output is sanitized fee
tier/rate/provenance evidence or one classified terminal failure. No order,
position, balance, collateral, transfer, withdrawal, deposit, or strategy path
is available from this entrypoint.

## Legacy commands

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
Delivery is best effort; a full queue or Telegram outage can drop messages so it
cannot delay market-data processing, strategy deadlines, or safe shutdown.
Every completed authoritative `FULL` scan sends all 20 existing ordered route
rows in `Ticker | Route | Expected PnL` form. Long messages are split into
bounded numbered parts without splitting or duplicating a route. `UNKNOWN`
includes a short authoritative blocker in the same third field. The values come
directly from the runtime's scanner result; Telegram does not recalculate
economics. Monetary values in Telegram text are displayed with exactly two
fractional digits while authoritative Decimal values retain full precision.
INITIAL, FOCUSED, and RECOVERY scans do not send this digest.

Extended maintains a validated full-universe catalog in a non-blocking
background task and refreshes only the five required official market mappings
on normal public refreshes. Fresh last-good metadata survives transient catalog
timeouts; expired metadata fails closed with catalog/metadata blockers rather
than `BOOK_UNHEALTHY`. Dedicated book, trade, and funding sockets have isolated
10-second heartbeat and readiness state. Physical transport lifecycle,
watchdog restart, and logical book resync evidence remain distinct.

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

See `SYSTEM_SPEC.md` for the active Spread Shadow contract and preserved legacy specification, `STATUS.md` for the accepted baseline, and `NEXT_TASK.md` for the current authorization boundary.

The RISEx, Extended, and Nado lifecycle modules are not CLI modes and must not be imported by normal Farmer startup. Their verification levels and safety gates are defined once in `AGENTS.md`; current venue state and work live only in `STATUS.md` and `NEXT_TASK.md`.

Strategy-driven testnet execution is a later, separate measurement task. It starts only after all three venue lifecycles are independently accepted and records opportunity frequency, planned-versus-actual execution, fees, resolved funding, and complete net PnL; degraded or unresolved trades do not support profitability claims.
