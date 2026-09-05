# SS-001N — Unprefixed Login-Nonce Contract Correction

Status: `ACTIVE / ONE FRESH RISEX BUILDER`.

Objective: correct only the accepted fee runner's login-nonce parser using the combined SS-001M live structural witness and current official `AuthService_GetLoginNonce` semantics. The live exact-account response was `data.nonce`, string, exactly `64` characters, without `0x`; official documentation classifies the nonce as hexadecimal and says it is interpreted base-16 for the EIP-712 `uint256` field.

Exact base: published `main` containing the SS-001M terminal record. Verification level A. Use one fresh visible Builder, branch, and worktree. No live venue call or protected credential read is authorized in this slice.

Allowed change: the narrow nonce parser plus direct focused fixtures/tests and only necessary documentation wording. Accept the exact observed bounded unprefixed hexadecimal string and the documented bounded `0x` hexadecimal string. Parse either form base-16 to the same `uint256` value. Preserve the exact received string as the `nonce` sent in the login request; do not silently convert an unprefixed value to decimal or rewrite its wire representation. Incoming additive envelope fields remain tolerated.

Required adverse coverage: empty string; bare `0x`; more than 64 hex digits; non-hex characters; whitespace; JSON integer/float/bool/null; ambiguous top-level plus nested nonce; nested nonce object/list; and proof that a 64-character digit-only unprefixed value is interpreted as hexadecimal, not decimal. Existing domain/status/identity/allow-list/retry/redaction/signature/fee tests must remain green. No test may contain a real nonce, key, account, token, or response payload.

Forbidden: live access, other parser/schema changes, endpoint order or allow-list changes, authentication flow redesign, dependency changes, protected-path changes, collection/report/economics/strategy changes, `CAL-001`, `HOLDOUT-001`, orders, write payloads, dispatch, positions, balances, collateral, deposits, transfers, withdrawals, `SS-002`, and `SS-003`.

Acceptance: focused nonce and fee-runner tests, one clean isolated Python 3.11 full suite, compile/import/dependency/private-write-surface/diff/Git checks, and exact scope review. After independent acceptance and publication, open a separately frozen single operational fee-read gate; do not reuse or retry SS-001L/SS-001M.
