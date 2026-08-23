# TESTNET-002-RISEX-SIGNER-FIX-001 — Additive Config and Hex Bitmap Parsing

Status: `RED AUTHORIZED — NO LIVE INVOCATION`.

Start from exact published `main == origin/main == 6d8eb17bd44eb17505fd0ca0ccb0b402286c239a` on `codex/testnet-002-risex-signer-fix-001`. The accepted signer implementation remains a deterministic candidate, but operational onboarding is blocked: its one authorized invocation failed before secret load/signing/claim/dispatch with zero POST because current official public response shapes are narrower than its parser assumptions.

## Proven correction

- `/v1/system/config` remains authoritative when `data`, `chain`, and `addresses` are objects containing exact `chain.name == "Rise Testnet"`, string `chain.chain_id == "11155931"`, and normalized `addresses.auth == 0x6da86f486b5e6536358f5b122dbe184522ca0ee3`. Ignore unrelated additive fields in those objects. Missing, wrong, or type-confused required fields fail closed.
- Current official [`AuthService_GetNonceState` OpenAPI](https://developer.rise.trade/reference/authservice_getnoncestate.md) defines `bitmap` as a `0x`-prefixed hexadecimal string representing `uint256` and gives `0x7` as its example; the official testnet response for the approved wallet is `0x0`. Accept only canonical nonempty `0x[0-9a-fA-F]+`, bounded to `0 <= value < 2**256`. Reject whitespace, sign, empty prefix, non-hex, booleans, numbers, objects, overflow, and all other representations.
- Preserve exact decimal-string `nonce_anchor`, integer `current_bitmap_index` bounds `0..208`, anchor overflow rejection, prescribed signed `nonce_anchor + 1`, and signed bitmap index `0`.
- Do not change the exact host/final URL/redirect/TLS/domain/wallet/status/list/expiration/secret/signature/durability/one-POST/reconciliation gates or public API.

## Mandatory RED and acceptance

1. Exact published baseline rejects the exact observed additive config and official `bitmap: "0x0"` before secret load and therefore cannot reach the governed registration path.
2. Exact observed additive config and required identity nested among additional fields pass; missing/wrong/type-confused name, chain id, auth, chain, addresses, or data fail closed.
3. Bitmap `0x0`, representative nonzero values, and the `2**256 - 1` boundary parse exactly. Empty prefix, sign, whitespace, non-hex, non-string, boolean, object, and `2**256` overflow fail closed.
4. Every rejected response proves zero main-secret callback calls, zero signer signing, unchanged local `CREATED` record, and zero POST.
5. Preserve all existing 444 tests, normal Farmer import isolation, exact one POST site, no revoke dispatch, and no order/cancel/position/trading/mainnet surface.
6. Run focused tests with asyncio debug, full Python 3.11 pytest, compileall, diff-check, dependency/import isolation, secret scan, pending-process audit, and both worktree Git consistency.

## Ownership and workflow boundary

Only `src/risex_farmer/testnet_risex_signer.py` owns these response parsers. Expected production change is limited to `_identity` and `_nonce`; focused tests may change only `tests/test_testnet_risex_signer.py`. Any dependency or other production edit requires Architect justification before change. No compatibility layer, generic coercion, new state, retry, endpoint, service, or configuration is permitted.

Exactly one Builder starts from the governance commit on this fresh branch, authors RED first, and must not spawn agents. Architect reviews RED before GREEN. One implementation commit and at most two fix cycles. Stop for Chief candidate review before merge, push, XLSX/credential access, secret load, signing, claim, registration, revoke, or any other live action. Preserve the existing credential and record exactly; do not generate, delete, reset, rename, replace, or expose them.
