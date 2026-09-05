# SS-001M — Exact Login-Nonce Shape Witness

Status: `ACTIVE / CHIEF-CONTROLLED LEVEL-B DIAGNOSTIC`.

Objective: resolve the SS-001L terminal `SCHEMA/NONCE_INVALID` without guessing or replaying that gate. Make exactly one fresh `GET /v1/auth/nonce` for the exact protected owner wallet and retain only a sanitized structural witness sufficient to correct the parser: top-level key names, container/value types, nesting path, nonce character length, and whether its characters form a bounded `0x` hexadecimal or canonical decimal value. Never retain or print the nonce itself, response body, request ID, account address, headers, or transport-controlled text.

Official evidence: current `AuthService_GetLoginNonce` OpenAPI documents a top-level response object containing string `nonce`. The live mainnet SS-001L response contradicted the accepted `data.nonce` parser but was deliberately not retained. The witness must distinguish top-level `nonce`, `data.nonce`, both/conflicting fields, or another bounded shape without exposing values.

Attempt boundary: one initial request plus one retry only after timeout, premature EOF/partial body, connection reset, or transport failure before a valid observation. Any complete HTTP, schema, auth, identity, or safety failure is terminal. Use exact `https://api.rise.trade`, exact path `/v1/auth/nonce`, and exact protected wallet query only. No domain/status repetition is needed because SS-001L already proved both in the immediately preceding accepted run.

Allowed: a one-shot Chief-local in-memory diagnostic using the accepted TLS/no-proxy/no-redirect/finite-timeout transport properties; sanitized terminal output containing only source SHA, endpoint name, HTTP success class, structural paths/types, nonce length/encoding class, and observation time. No source edit or Builder is authorized until the witness is terminal.

Forbidden: raw payload/output, nonce value or hash, request ID, account/session address, other account, any other endpoint, owner-key access, signing, login, JWT, fees, token refresh/logout, registration, orders or write payloads, dispatch, positions, balances, collateral, deposits, transfers, withdrawals, strategy execution, `CAL-001`, `HOLDOUT-001`, `SS-002`, and `SS-003`.

Terminal action: if the witness proves a bounded live shape consistent with official evidence, open one fresh Builder correction from published `main` that changes only nonce parsing, focused regressions, and minimal documentation if necessary. If the live shape cannot be sanitized and validated safely, stop the fee gate and report the blocker; do not repeat the request.
