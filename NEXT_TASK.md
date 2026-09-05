# SS-001L — RISEx Access Classification and Conditional Fee Read

Status: `ACTIVE / CHIEF-CONTROLLED LEVEL-B OPERATION`.

Objective: resolve the complete pre-acceptance HTTP `403` without blind replay by making exactly one fresh invocation of the independently accepted RISEx fee runner on published `main` `bfb38ac49444ff565607245de572f6866d0c0745`. The invocation first validates the live official mainnet domain and exact registered session-signer readiness. Only if both public gates pass may that same invocation continue through the account-bound nonce, local no-echo owner login, short-lived JWT, and one caller-owned `GET /v1/user/fees` read.

Exact source: published `main` `bfb38ac49444ff565607245de572f6866d0c0745`. Verification level: B. No Builder or code change is authorized. Run only the accepted no-argument `risex-spread-shadow-fee-read` entrypoint with its opt-in signing dependency in an isolated Python 3.11 environment. Preserve only its sanitized terminal JSON and exact source identity.

Attempt boundary: this is one new diagnostic gate, not a retry inside the failed observation. Within the accepted runner, each required endpoint has one initial attempt plus one retry only after timeout, premature EOF/partial body, connection reset, or transport failure before a valid observation. A complete HTTP, schema, auth, identity, domain, or safety failure is terminal; stop the invocation and do not switch clients, alter address casing, modify headers, call a substitute endpoint, or retry the gate. The public sequence is exact live domain, then exact wallet/session-signer status. Owner-key input is forbidden before both pass.

Conditional owner action: if the runner reaches its hidden local prompt, pause and have the owner type the main-wallet key only into the attached local no-echo terminal. The key must never be requested or supplied in task/chat. It may exist only in the accepted in-memory capability, must derive the exact protected wallet, sign only `Login(address account,uint256 nonce,uint32 deadline)`, and be zeroized on exit. No persistent JWT is authorized.

Allowed endpoints are exactly `GET /v1/auth/eip712-domain`, `GET /v1/auth/session-key-status` for the protected exact pair, `GET /v1/auth/nonce` for the protected exact wallet, `POST /v1/auth/login`, and caller-owned `GET /v1/user/fees`. Output only the accepted sanitized fingerprint, terminal classification, endpoint/source provenance, signer status, fee tier/rates and optional schedule/trial/earned fields.

Forbidden: any other account or endpoint, response-body retention, secret disclosure, arguments or environment credentials, registration/revocation, token refresh/logout, orders, signing order/write payloads, dispatch, cancels, positions, balances, collateral, deposits, transfers, withdrawals, strategy execution, code changes, `CAL-001`, `HOLDOUT-001`, `SS-002`, and `SS-003`.

Terminal action: on success, record only sanitized exact fee evidence and freeze the quantitative `CAL-001` continuation/stop thresholds before any new sample. On any failure, record the sanitized class/reason and stop for an owner-visible decision; do not widen or repeat this gate.
