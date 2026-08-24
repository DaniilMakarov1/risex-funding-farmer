# Active bounded tasks

One slice per venue. Apply the A/B/C levels and safety core from `AGENTS.md`; operational run IDs are data, not source milestones.

## RISEx — minimal Level C operational binding

Status: `READY FOR CHIEF START GATE`.

- Add only the isolated credential/signing/official-transport binding needed to run the accepted lifecycle with runtime run identity; do not change lifecycle semantics, shared code, strategy, or paper behavior.
- Candidate acceptance requires official contract evidence, focused Level C regressions, and one final full suite. A later separate Chief operational gate controls the single minimum-size testnet write lifecycle.

## Nado — capture official catalog semantics

Status: `READY FOR BOUNDED PUBLIC OBSERVATION`.

- Perform one fresh credential-free official catalog read with bounded timeout/size and normal HTTP decoding. Capture only sanitized required field/type semantics; a read failure may use the bounded Level B policy, never a write-style replay rule.
- Change code only if observed semantics contradict it. Do not load credentials until the critical catalog/account contract validates.

## Extended — resume authenticated read-only readiness

Status: `WAITING FOR OFFICIAL STREAM AVAILABILITY`.

- Check the official stream endpoint with one credential-free bounded handshake. If service is available, run one fresh bounded authenticated read using runtime journal identity; do not edit source merely to increment an operation number.
- Validate required account/order/position semantics and authoritative zero orders/exact flatness. Add code/tests only for an observed contract defect.

## Completion

- After private readiness, each venue receives one separate minimum-notional Tier C lifecycle (`<= USD 500`). Strategy work begins only after all three finish with authoritative zero orders and exact flatness.
