# Active bounded tasks

One slice per venue. Apply the A/B/C levels and safety core from `AGENTS.md`; operational run IDs are data, not source milestones.

## RISEx — minimal Level C operational binding

Status: `READY FOR USER LEVEL C BINDING AUTHORIZATION`.

- No Builder for the credential/signing/live-transport binding starts until explicit user authorization is recorded; this correction does not grant it. Once authorized, add only the isolated binding needed to run the accepted lifecycle with runtime run identity; do not change lifecycle semantics, shared code, strategy, or paper behavior.
- Candidate acceptance requires official contract evidence, focused Level C regressions, and one final full suite. A later separate Chief operational gate controls the single minimum-size testnet write lifecycle.

## Nado — fresh authenticated private read

Status: `READY FOR LEVEL B OPERATIONAL GATE`.

- Run exactly one fresh sealed Nado authenticated read-only operation with its runtime journal identity. It may load only the fixed provisioned capability and may sign only the accepted `list_trigger_orders` read contract; no account mutation or write is authorized.
- Require complete public/account validation and a durable terminal report. Any semantic, authentication, terminal, or ambiguous outcome stops without rearm or write; a transport-only retry is never implicit and remains subject to the Level B two-attempt ceiling.

## Extended — resume authenticated read-only readiness

Status: `WAITING FOR OFFICIAL STREAM AVAILABILITY`.

- Check the official stream endpoint with one credential-free bounded handshake. After recovery, if the accepted runner is still source-bound, first make one minimal Level B runtime-run-ID decoupling candidate; then perform the fresh authenticated read under a separate operational gate.
- Validate required account/order/position semantics and authoritative zero orders/exact flatness. Add code/tests only for an observed contract defect.

## Completion

- After private readiness, each venue receives one separate minimum-notional Level C lifecycle (`<= USD 500`). Strategy work begins only after all three finish with authoritative zero orders and exact flatness.
