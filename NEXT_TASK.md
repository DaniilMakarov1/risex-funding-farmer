# TESTNET-001-RECOVERY-001 — Module-Owned Transport and Verified Account Bootstrap

Status: `ACCEPTED — BOUNDED RISEX OPERATIONAL BOOTSTRAP AUTHORIZED`.

The accepted exact chain is governance `51b65e41930c7558c7cff25ee0c7795c00c3dd55`, implementation `744dffc4534b2eb970bf4c0589f7282088f076df`, fix cycle 1 `a543239b971124ae3d4cc405abda6fb0e2b7867e`, and fix cycle 2 `603c2cf8174fc55a510197b65d52b6e27e35f82e`, based on published `main == origin/main == c28f40d6a1fc74c1795e26b77695ced2b21dc5a4`. Chief independently passed 43 focused and 367 full tests and found no acceptance blocker. The rejected TESTNET-001 implementation and branch remain blocked audit history and must not be inspected, imported, copied, cherry-picked, merged, pushed, or live-run.

## Authorized operational acceptance

- Integrate and publish the accepted chain, prove `main == origin/main`, and rerun deterministic gates before network use.
- Call `check_risex_account` first for the fixed approved public wallet. If ready, record `ALREADY_READY` and perform zero POSTs.
- Otherwise call `bootstrap_risex_account` exactly once with the fixed explicit intent. `READY` requires an authoritative positive balance; `REJECTED` stops immediately. `SUBMITTED_UNVERIFIED` or `UNKNOWN_AMBIGUOUS` permits only at most five read-only checks over at most 60 seconds and never a second POST.
- Report only public wallet, UTC request times/status classes, raw test balance, and readiness. Do not report response bodies. No XLSX, key, secret, credential, order, position, trade, mainnet endpoint, Farmer/paper runtime, or Telegram runtime is permitted.
- Stop after the RISEx result and report to Chief. Nado and Extended remain explicit blockers and must not be worked around. TESTNET-002 must not start.

## Ownership and bounded design

- Add one small optional testnet bootstrap module, isolated from normal Farmer imports and from runtime, Scanner, lifecycle, storage, Telegram, economics, and paper configuration. No framework, service, daemon, database, CLI-wide abstraction, SDK compatibility layer, or optional dependency is authorized.
- RISEx is the only implementable onboarding path in this slice. The module owns exact constants for official testnet host `api.testnet.rise.trade`, chain `11155931`, paths, methods, and documented identity fields. It creates and closes its own `aiohttp` session with system CA verification, `trust_env=False`, finite total timeout, and `allow_redirects=False` on every request; validates response status and actual URL; and rejects every 3xx or destination mismatch.
- The public API exposes no sender, transport, session, request, URL, base-URL override, or destination callback. Tests intercept only below this boundary through private monkeypatching. No custom SSL context, TLS bypass, redirect following, proxy inheritance, or caller-selected method/path is permitted.
- RISEx bootstrap first verifies `GET /v1/system/config` and `GET /v1/auth/eip712-domain`, derives exact USDC from verified config, then reads `GET /v1/account/balance` for the expected wallet. Positive balance returns `ALREADY_READY` without a write. Otherwise an explicit one-venue/one-operation intent permits exactly one unsigned `POST /v1/account/deposit` with the documented wallet and amount; immediately before dispatch identity and wallet are revalidated.
- POST success is not readiness. A bounded authoritative balance reread through the same owned safe transport must show positive balance to return `READY`. Confirmed write without observable readiness is `SUBMITTED_UNVERIFIED`; timeout, EOF, TLS failure, final-URL mismatch, redirect, cancellation-independent transport uncertainty, or malformed response after dispatch is non-ready/ambiguous and is never retried automatically. `CancelledError` propagates.
- Nado signed onboarding remains fail-closed: do not hand-roll signing, downgrade accepted dependencies, or add an isolated subprocess/sidecar. Extended onboarding remains fail-closed until an official working chain-identity path is proved; do not equate REST hostname with chain identity or guess an RPC. Report exact manual/official prerequisites only.
- No real XLSX access, secrets, credentials, private/account live query, registration, API-key creation, faucet/deposit write, test collateral, order/cancel/replace/position/trading method, mainnet endpoint, real funds, or Scanner/runtime integration occurs before Chief candidate acceptance and separate operational authorization.

## RED and acceptance contract

1. Exact published main lacks the sealed bootstrap and fails the newly authored tests; production remains untouched at the RED checkpoint.
2. A response whose actual/final URL is production is rejected and never submitted. Statuses 301, 302, 303, 307, and 308 are rejected without following.
3. Public API signatures cannot inject sender, transport, request, session, URL, base URL, method, path, proxy, or SSL context. Environment proxy/base-URL tricks cannot redirect requests; default CA verification remains enabled.
4. Wrong host, path, method, chain, signing domain, or wallet mapping fails before any permitted secret/signer callback or write dispatch. RISEx's official unsigned faucet path introduces no production secret seam.
5. Successful POST without an authoritative positive balance is never `READY`; already-ready preflight performs zero writes. The expected wallet, config-derived USDC, exact quantity, and postcondition are fixture-proved.
6. Timeout, EOF, TLS failure, final-URL mismatch, malformed response, or cancellation after POST never causes a retry or success. Exactly one write attempt is made; uncertain outcomes are typed ambiguous/non-ready and cancellation propagates normally.
7. Synthetic secret-bearing chained exceptions, SDK-like objects, response bodies, representations, stdout/stderr, and pytest output are redacted. No response body or low-level exception text enters a public result. No real secret fixture is used.
8. No order, cancel, replace, position-creation, trading transaction, mainnet route, or generic endpoint dispatch is present or reachable. Read-only code cannot invoke a write even with malicious fixture data.
9. Optional testnet code is not imported during normal Farmer startup. The existing 324-test paper suite remains unchanged; CI and Builder work are fixture-only and perform no live calls or writes.
10. Run focused tests, full `pytest`, compileall, diff review, secret scan, import-identity check, and pending-task audit. Builder creates one implementation commit and reports in at most 20 lines; at most two Architect-directed fix cycles are allowed.

## Workflow boundary

Exactly one Builder may author fresh RED tests and then, after Architect RED review, the smallest implementation. Expected production scope is one new optional module and tests in one new focused test file; any additional production file requires Architect justification before edit. Stop at the deterministic candidate checkpoint. Do not merge, push, read the real XLSX, create credentials, make private/live queries, perform onboarding/faucet/deposit, fund accounts, or start TESTNET-002 before Chief independent review.

The user's later testnet-only order/cancel/close and manually armed strategy authorization is recorded but deferred. It does not authorize trading code in this slice and never authorizes mainnet or real funds.
