# Status

- Accepted implementation: TELEGRAM-001-FIX-001 — Flood-Control and Outage Dedupe @ `717b6350485d04567a3e915468c06a5ee6f53104`
- Previous accepted implementation: TELEGRAM-001 — Outbound Runtime Notifications @ `cc321883b7699c142d3d275ad1f9931a5a57e869`
- Active implementation task: none
- PAPER-007 Stage A scheduling validation: PASS
- PAPER-007 Stage B: running in the existing detached process on accepted FIX-003; untouched by TELEGRAM-001
- Preserved Stage B evidence: `paper-007-stage-a-fix002.db`, SHA-256 `6c9bddbf3e10e5690f8e5d5327adf5c35fad4f2044d96fdb9445b3bd567e68ff`
- Product phase: PAPER ONLY
- Live trading: prohibited
- Telegram: FIX-001 accepted; current Stage B remains Telegram-disabled because restart environment lacks destination chat ID and token

FIX-003 is accepted after 133 deterministic tests and a short public-only smoke. Physical sockets now persist one ordered `PUBLIC_SOCKET_DISCONNECTED` / `PUBLIC_SOCKET_RECONNECTED` pair per episode; combined RISEx/Nado sockets use one ordered market-set identity, while book gaps retain only snapshot-recovery evidence. Stage A timing remains accepted and was not repeated.

TELEGRAM-001-FIX-001 is accepted after 162 deterministic tests. Bot API flood-control now uses bounded positive JSON `parameters.retry_after` delays outside the request timeout, connector retry uses positive backoff, and ambiguous timeout is not retried. A physical Extended book outage and its resync evidence share one notification state without changing persisted FIX-003 evidence; recovery clears the state and later independent gaps notify again. The user explicitly accepted reuse of the previously disclosed token for the single post-acceptance Stage B restart; the token value is not recorded or displayed. The existing Stage B was not stopped because the safe restart environment has no destination chat ID or token.
