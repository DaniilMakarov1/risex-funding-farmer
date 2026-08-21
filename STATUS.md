# Status

- Accepted implementation: TELEGRAM-002 — Full Scan Digest @ `4d59156cda36dc4b99e3c53d1805db873213d6f9`
- Previous accepted implementation: TELEGRAM-001-FIX-001 — Flood-Control and Outage Dedupe @ `717b6350485d04567a3e915468c06a5ee6f53104`
- Active implementation task: none
- PAPER-007 Stage A scheduling validation: PASS
- PAPER-007 Stage B: running on accepted main with outbound Telegram enabled after a safe flat restart
- Preserved Stage B evidence: `paper-007-stage-a-fix002.db`, SHA-256 `6c9bddbf3e10e5690f8e5d5327adf5c35fad4f2044d96fdb9445b3bd567e68ff`
- Product phase: PAPER ONLY
- Live trading: prohibited
- Telegram: enabled for authoritative runtime events and every completed `FULL` scan

FIX-003 is accepted after 133 deterministic tests and a short public-only smoke. Physical sockets now persist one ordered `PUBLIC_SOCKET_DISCONNECTED` / `PUBLIC_SOCKET_RECONNECTED` pair per episode; combined RISEx/Nado sockets use one ordered market-set identity, while book gaps retain only snapshot-recovery evidence. Stage A timing remains accepted and was not repeated.

TELEGRAM-001-FIX-001 is accepted after 162 deterministic tests. Bot API flood-control now uses bounded positive JSON `parameters.retry_after` delays outside the request timeout, connector retry uses positive backoff, and ambiguous timeout is not retried. A physical Extended book outage and its resync evidence share one notification state without changing persisted FIX-003 evidence; recovery clears the state and later independent gaps notify again. The user explicitly accepted reuse of the previously disclosed token for the single post-acceptance Stage B restart; the token value is not recorded or displayed.

The Architect used the newly authorized diagnostic `getUpdates` exception to discover the destination without adding inbound runtime behavior. Stage B PID `42853` stopped with `STOPPED_SAFE` and `forced_close=false`; the same database resumed under PID `45479` in detached screen `risex-paper007-stageb-telegram-fullscan`. Initial STARTED/SCAN/READY persisted, the first accepted-code FULL scan completed at `2026-08-21T08:31:34.802108Z`, and orders/fills/open position remained zero.

TELEGRAM-002 is accepted after 167 deterministic tests. Each authoritative `FULL` scan now emits at most one bounded digest with up to 15 existing ordered route rows in `Ticker | Route | Expected PnL` form. INITIAL, FOCUSED, and RECOVERY scans do not emit this digest. Scanner results, scheduling, economics, persistence, adapters, and paper/live boundaries are unchanged.
