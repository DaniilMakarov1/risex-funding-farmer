# TESTNET-002-RISEX-SIGNER-FIX-002 — EIP-55 Domain Verifier Identity

Status: `ACTIVE — DETERMINISTIC CORRECTION ONLY; NO LIVE INVOCATION`.

Start from exact published `main == origin/main == afc76b9fa3b602232ee9147a4a75bbc6975aea56` on fresh branch `codex/testnet-002-risex-signer-fix-002`. The prior replacement operational registration invoked the function exactly once and stopped fail-closed with zero POST before main-secret load, signing, durable claim, or dispatch. Official `/v1/auth/eip712-domain` returned approved contract `0x6DA86F486b5E6536358F5b122dBe184522CA0eE3`, whose normalized 20-byte identity equals lowercase approved `_AUTH`; current whole-object equality rejects its EIP-55 text spelling.

## Bounded correction

- Keep the exact domain key set and exact `name`, `version`, and string `chain_id` checks.
- Validate `verifying_contract` through the existing strict address normalizer and require normalized equality to approved `_AUTH`.
- Do not accept extra or missing domain fields, wrong but well-formed addresses, malformed/type-confused verifiers, or wrong name/version/chain.
- Change no config, nonce, endpoint, TLS, redirect/final-URL, wallet, status/list, expiration, signing, persistence, reconciliation, state, or transport behavior.

## Mandatory RED and acceptance

1. Exact published baseline rejects the exact current official EIP-55 verifier before reaching the synthetic main-secret loader.
2. Candidate accepts that exact checksum spelling and reaches the loader boundary; the synthetic loader stops execution, proving zero signing, durable claim, record mutation, and POST.
3. Wrong valid address, malformed/missing/non-string verifier, wrong name/version/chain, and extra/missing domain fields all fail before loader, signing, claim, record mutation, and POST.
4. Address validation is strict 20-byte hex identity normalization, not arbitrary lowercasing.
5. Preserve the real signer credential and record byte-for-byte: signer `0x6274d6d9f628ba89c36de4b71efa2c602b7f783b`, expiration `2026-09-22T15:46:50Z`, state `CREATED`; deterministic work never reads credential bytes or accesses XLSX/network.
6. Run focused Python 3.11 tests with asyncio debug, full pytest, compileall, exact diff, dependency/import isolation, secret scan, one-POST/no-trading surface, pending-process, and both-worktree Git checks.

## Ownership and workflow

Production scope is only the EIP-712 domain portion of `_identity` in `src/risex_farmer/testnet_risex_signer.py`; tests may change only `tests/test_testnet_risex_signer.py`. Exactly one Builder starts after this governance commit, authors RED against the exact published baseline, then makes the smallest GREEN production edit in one implementation commit. Builder must not spawn agents. Architect independently reviews RED and every hunk. At most two fix cycles.

Stop for Chief candidate review before acceptance governance, merge, push, XLSX/credential access, secret load, signing, claim, registration, revoke, or live action. No order/cancel/position, Farmer/runtime/Scanner/Telegram, Nado/Extended, strategy/economics, mainnet, real funds, or new product behavior.
