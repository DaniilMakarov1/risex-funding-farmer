# SS-001Q — Corrected-Input Exact RISEx Owner-Fee Read

Status: `ACTIVE / CHIEF-CONTROLLED LEVEL-B OPERATIONAL GATE`.

Objective: obtain the exact currently applicable RISEx owner-account maker/taker fee schedule through one bounded read-only invocation of the independently accepted fee reader in an ordinary visible macOS Terminal window, with the required key format stated before the no-echo prompt, so profitability thresholds can later be frozen from verified costs rather than assumptions.

Exact source: published `main` containing accepted SS-001N at `08620690418552382aeb83bf99e913753e75520d`. Verification level B. Use the accepted opt-in runner and exact protected identity paths only. Do not modify source during the gate.

Attempt boundary: one fresh post-SS-001P invocation because that invocation terminated before derivation or signing on a classified input-format failure. Before launch, tell the owner to enter the exact main-wallet private key as 64 hexadecimal characters, optionally prefixed by lowercase `0x`, without whitespace, quotes, address, mnemonic, or other text. Within SS-001Q, allow one initial transport attempt per required read plus one retry only after timeout, premature EOF/partial body, connection reset, or transport failure before a valid observation. A complete HTTP, schema, auth, identity, or safety failure is terminal. No prior response or nonce is reused.

Allowed sequence: validate the exact live EIP-712 domain; validate the exact protected wallet/session-signer pair is active; obtain its account-bound nonce; pause for the owner to type the main wallet key only into the local no-echo prompt; derive and verify the exact wallet; sign only `Login(address account,uint256 nonce,uint32 deadline)`; obtain a short-lived JWT; perform one `GET /v1/user/fees`; emit only the runner's sanitized fee evidence. The key may exist only inside the protected local capability and must be zeroized/closed as implemented.

Forbidden: key entry in chat, task text, arguments, environment, logs, reports, databases, fixtures, Git, or process titles; raw nonce, JWT, signature, response body, request ID, wallet/session address, or secret retention; source edits; registration; token refresh/logout; any endpoint outside the accepted allow-list; orders, write payloads, dispatch, positions, balances, collateral, deposits, transfers, withdrawals, strategy execution, `CAL-001`, `HOLDOUT-001`, `SS-002`, and `SS-003`.

Terminal action: on sanitized `FEE_READ_COMPLETE`, record only exact fee/tier/schedule facts and current official Lighter Standard inputs, then open a separate prospective calibration-freeze slice. On any terminal failure, record its sanitized class and stop without blind replay. No economic verdict follows from the fee read alone.
