# Status

- Accepted implementation: PAPER-007-FIX-005 — First Full-Scan Funding Freshness @ `b5c1db0ec7f226914976f60a732ce9dfd58ff113`
- Previous accepted implementation: PAPER-007-FIX-004 — Public REST Timeout 30 Seconds @ `7476946263cb68812482215282fc7f50fb73a97b`
- Active implementation task: PAPER-007-FIX-006 — RISEx Checksum Resubscribe Recovery
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

PAPER-007-FIX-004 is accepted after 168 deterministic tests. The shared public HTTP runtime total timeout is 30 seconds; endpoints, retry cadence, scheduling, economics, lifecycle, Telegram delivery, and paper/live boundaries are unchanged. PID `45479` stopped with `STOPPED_SAFE` and `forced_close=false`; Stage B resumed on the same database under PID `50755` in detached screen `risex-paper007-stageb-timeout30`. Extended's first accepted-code catalog request completed successfully in `18.022913` seconds with `PUBLIC_REST_READY`.

PAPER-007-FIX-005 is accepted after 170 deterministic tests. Persisted evidence showed the first post-restart `FULL` scan at `2026-08-21T08:56:17.562429Z` had zero known PnLs because initial RISEx funding observations were about 130 seconds old under the unchanged 120-second freshness rule. Runtime now seeds the existing non-blocking single-flight refresh after readiness; a gated refresh cannot delay readiness, scan deadlines, or safe stop.

PAPER-007-FIX-006 is explicitly authorized after live evidence showed repeated RISEx checksum gaps entering a high-frequency REST snapshot/buffer replay loop and leaving every route `BOOK_UNHEALTHY`. Current official RISEx documentation requires unsubscribe/resubscribe and a new WebSocket snapshot after checksum mismatch; the correction must follow that public contract without creating false physical socket lifecycle evidence. The running Stage B remains untouched until implementation acceptance.
