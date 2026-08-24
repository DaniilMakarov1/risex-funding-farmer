# Active bounded tasks

One slice per venue. Secrets stay outside Git and reports. Private reads and writes run sequentially and never retry an ambiguous operation.

## RISEx — preserve the fixture/live boundary

Status: `BLOCKED — FROZEN FIXTURE MILESTONE HAS NO LIVE OPERATIONAL PATH`.

- The official identity chain and fixture lifecycle are accepted, but `SYSTEM_SPEC.md` section 25 explicitly excludes a production credential invocation, private/live transport, CLI, and live-smoke path from this milestone. Do not create an operational adapter by governance inference.
- A new explicit user decision is required before the minimum sealed Tier C production binding may be specified or implemented. Until then do not load credentials, sign, dispatch, or run live traffic; all accepted invocations remain consumed.

## Nado — resume public preflight diagnosis

Status: `BLOCKED — OFFICIAL PUBLIC GATEWAY RETURNED HTTP 403`.

- After a genuine external-state change or cooldown, perform exactly one credential-free official public catalog query from a fresh Builder session. If successful, reproduce and correct the remaining op003 catalog mismatch with fixtures; if still forbidden, stop without retry.
- Do not read credentials or create op004 until the public contract passes.

## Extended — wait for official stream recovery

Status: `BLOCKED — OFFICIAL TESTNET ACCOUNT STREAM RETURNED HTTP 503`.

- Private-read op004 is consumed after its first stream-open attempt failed before completion. Do not reuse its invocation or store and do not retry it.
- After a demonstrated external-state change, confirm the current official testnet account stream is available without loading credentials. Only then may a fresh Builder bind one new unique invocation/store from the then-current published `main`; the resulting private-read operation requires a new separate Chief gate.

## Completion

- After private readiness, each venue receives one separate minimum-notional Tier C lifecycle (`<= USD 500`). Strategy work begins only after all three finish with authoritative zero orders and exact flatness.
