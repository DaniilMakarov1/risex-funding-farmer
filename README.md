# RISEx Funding Farmer

A small, standalone paper trader for testing whether a delta-neutral RISEx-points funding strategy can produce non-negative trading PnL after configured fees and the documented paper execution model.

The default product is **paper only**. It uses official public RISEx, Extended, and Nado market data. Separately governed optional testnet modules cover the accepted RISEx account bootstrap and accepted session-signer prerequisite; the preserved signer is operationally active. The active RISEx, Extended, and Nado slices are fixture-only designs of isolated bounded lifecycle cores and have no credential, private-network, or live-write surface. Mainnet, real funds, operational orders/positions, and strategy-driven execution remain outside the active specification.

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

See `SYSTEM_SPEC.md` for frozen product behavior, `STATUS.md` for the accepted baseline, and `NEXT_TASK.md` for the current authorization boundary.

The optional TESTNET-002 signer prerequisite is not a CLI mode and is not imported by normal Farmer startup. Its accepted credential remains protected outside the repository and must not be regenerated, replaced, exposed, or revoked.

`TESTNET-002-RISEX-ORDER-LIFECYCLE-001` is also not a CLI mode and must not be imported by normal Farmer startup. After central publication and a separate Chief Coordinator Builder gate, it permits only fixture-first implementation of one exact-minimum BTC testnet lifecycle with durable intent identity, no-blind-retry reconciliation, one opening, at most three state-based close intents, and exact-ID cancellation. Fixtures use synthetic values and an uncalled injected signer loader; this milestone authorizes no credential/XLSX access, real signature, private/live network, nonce consumption, or POST. Operational success remains zero open orders plus exact flatness. The narrow user-accepted later live-test risk may end `FAILED_HALTED_MANUAL_RECOVERY`; that failure is never operational acceptance or strategy readiness.

`EXTENDED-TESTNET-001` is likewise not a CLI mode and must not be imported by normal Farmer startup. After a separate Chief Coordinator Builder gate, it is limited to fixture-first tests and an isolated module pinned to official Extended SDK commit `2130cdb1cd6e7b1867db83bd3af036572d258739`, with durable intent identity and fail-closed reconciliation to fresh zero-open-orders/exact-flat evidence. This milestone does not authorize credential access, private preflight, signing, deposits, authenticated traffic, live POSTs, operational orders/cancels/closes, or any shared paper-runtime change.

`NADO-TESTNET-001` is not a CLI mode and must not be imported by normal Farmer startup. After a separate Chief Coordinator Builder gate, it is limited to fixture-first tests and an isolated Python module pinned to official Nado TypeScript SDK `315e4f23dadefeb2f86f713e423241e81467d4c3`, Rust SDK `e54118786b171a4325871d5bd17e5abae0e90c5a`, and contracts `11c27b2851999f1b4f8cb4a7fbfcc9320253f12f`. It uses only synthetic keys and fixture transports to prove durable digest/nonce/payload intent, signed validation vectors, no-blind-retry reconciliation, bounded reduce-only recovery, and the full-catalog zero-orders/exact-flat barrier. It authorizes no credentials, private or live network, real signing, faucet/deposit, live POST, operational order/cancel/close, linked signer/API key, or shared paper-runtime change.
