# TESTNET-002-RISEX-SIGNER-001 — Session-Signer Prerequisite

Status: `DETERMINISTIC ACCEPT — ONE OPERATIONAL SIGNER REGISTRATION AUTHORIZED; NO ORDERS`.

Start from exact published `main == origin/main == f59f654a6d24434d351f5f4489b2ed641fb2288c` on `codex/testnet-002-risex-signer-001`. TESTNET-001 is accepted and operationally complete: the fixed public RISEx wallet is authoritatively READY with raw test balance `1000`, one deposit POST, and an immutable READY bootstrap marker. A bounded official read found zero registered session signers, so signer onboarding is a mandatory prerequisite to the separately authorized order lifecycle.

## Bounded product contract

- Build one optional, disarmed RISEx testnet session-signer primitive, isolated from paper/runtime/Scanner/Telegram/economics and from order execution. It may generate one session key after later operational authorization, register it once for the fixed approved wallet, and authoritatively verify it active.
- The fixed destination is `https://api.testnet.rise.trade`, chain `11155931`, current EIP-712 domain `RISEx` version `1`, and Authorization contract `0x6da86f486b5e6536358f5b122dbe184522ca0ee3`. Revalidate exact official system config/domain before any secret load and immediately before dispatch.
- Fetch exact official nonce state for the fixed wallet. Use `nonce_anchor + 1` and signed bitmap index `0`, as prescribed by the official integration contract. The venue's `current_bitmap_index` may be `0..208` inclusive (`208` means the current anchor is full); reject values above `208`, anchor overflow, malformed/change-in-flight nonce, or identity mismatch before signing.
- The approved main wallet signs exact `RegisterSigner(address account,address signer,string message,uint32 expiration,uint48 nonceAnchor,uint8 nonceBitmap)`. The generated session signer signs exact `VerifySigner(address account,uint48 nonceAnchor,uint8 nonceBitmap)`. Both use the same verified domain and nonce pair and are serialized as 65-byte `0x` hex.
- Use fixed message `RISEx session key`, fixed label `RISEx Funding Farmer testnet probe`, and expiration exactly 30 days from the controlled decision time: the shortest practical period established by the official working example. Validate `uint32` range and future bound before secret load.
- Registration is one exact `POST /v1/auth/register-signer`. No automatic retry. Re-entry first queries the exact account/signer: authoritative active state means zero POST. Timeout, EOF, TLS/DNS failure, cancellation, malformed/redirect/final-URL mismatch, or ambiguous response permits only bounded status/list reconciliation.
- Readiness requires exact `GET /v1/auth/session-key-status` status `1` and a consistent `GET /v1/auth/signers` entry for the exact signer with active status and expected expiration. POST success and local state alone are never readiness.
- Understand and fixture-test exact account-signed `RevokeSigner(address account,address signer,uint48 nonceAnchor,uint8 nonceBitmap)` and request fields. Do not expose or dispatch live revocation in this slice; successful onboarding keeps the signer active.

## Secret and recovery ownership

- Use lazy optional `eth-account >=0.13,<0.14` with its vetted `eth-abi`/`eth-utils` dependencies. Normal Farmer startup must not import them. Do not use a third-party RISEx SDK and do not hand-roll Keccak, ABI encoding, EIP-712, signature recovery, or key generation.
- Only the Architect may generate the real signer after candidate acceptance, using the vetted cryptographically secure account generator. Store it at one fixed passwd-home-derived path outside the repository, current-owner regular single-link mode `0600`, never in Git/SQLite/logs/reports/CLI/process title. Builder/tests use synthetic keys and a private disposable-home seam only.
- Persist only the public signer address, fixed operation identity/expiration, and `CREATED`, `SPENT_UNKNOWN`, or `ACTIVE` state in one separate fixed non-secret passwd-home record. It is not a generic credential store/journal/service and exposes no public path/environment/state/reset/delete/rearm/retry override.
- Before registration dispatch, durably advance the non-secret record to `SPENT_UNKNOWN` using owner-only no-follow files, file fsync, atomic same-home replacement, and home-directory fsync. Crash/cancellation/ambiguity after consumption never makes registration reusable. Only authoritative active verification may advance the record to `ACTIVE`; local ACTIVE is never sufficient without fresh venue reads.
- The main key is loaded only by an Architect-supplied local callback after every public gate. Derive its address locally and require exact approved wallet match before any signature. The signer credential is likewise derived and matched to the fixed public address before signing. Sanitize exception chains and library objects.

## Mandatory RED and acceptance

1. Exact accepted main lacks the signer primitive and fails newly authored tests while all 411 accepted tests remain green.
2. Wrong/mainnet host, chain, domain name/version/verifier, wallet, nonce anchor/index, expiration, response URL, redirect, or malformed schema fails before main/signer secret load or signature.
3. Synthetic main-key and signer-key derived addresses must match their fixed roles. Mismatch and chained `eth-account` failures are fully redacted from repr/stdout/stderr/pytest output.
4. Official published `REGISTER_SIGNER_TYPEHASH`, `VERIFY_SIGNER_TYPEHASH`, and `REVOKE_SIGNER_TYPEHASH` values must match exact type strings. Independently build the same synthetic struct hashes/digests with `eth-abi`/`eth-utils` and `eth-account`; recover both expected signers and verify exact 65-byte hex serialization.
5. Explicit generation creates exactly one fixed `0600` signer credential and public `CREATED` record under async/process races. Partial/wrong-owner/type/mode/link/symlink/malformed files fail closed; no secret is copied to the public record.
6. Explicit registration plus the durable claim yields at most one POST under async/process races, restart, crash, cancellation, and ambiguous response. Claim fsync ordering precedes network dispatch; no blind retry exists.
7. Existing authoritative active signer produces zero POST. POST response alone is never active. Ambiguous registration reconciles only through bounded exact status/list reads. Changed signer, expiration, status contradiction, timeout, or unreadable state remains non-ready.
8. Exact revoke typed data/request construction and account-signature recovery are fixture-tested, but public production surface has no revoke dispatch. It also contains no order/place/cancel/position/trading/mainnet/Nado/Extended method.
9. Sealed `aiohttp` transport owns DNS/TLS, uses default CA, `trust_env=False`, no redirects, exact final URL, finite timeout, one POST site, and normal cancellation propagation. Builder/CI makes no live call and never touches the real home or XLSX.
10. Run focused asyncio/subprocess/crash tests, all 411 preservation tests plus new tests, full pytest, compileall, diff, secret, import-isolation, one-register-site, no-trading-surface, and pending-process/task checks.

## Accepted operational boundary

After fast-forward integration, full deterministic gates, push, and proof that `main == origin/main`, the Architect may generate exactly one fixed session signer and invoke registration exactly once for the approved RISEx testnet wallet. The main-wallet secret is loaded only in memory from the protected external XLSX after public identity/status/nonce gates and an exact derived-address match. ACTIVE requires a separate authoritative `check_risex_session_signer`; SPENT_UNKNOWN permits at most five read-only checks over at most 60 seconds and never another POST. The fixed credential and record are never deleted, reset, regenerated, or revoked. Stop after reporting the operational result; no order probe begins in this slice.

## Workflow boundary

Exactly one Builder may author fresh RED and must not spawn agents. Architect reviews RED before production. Expected production scope is one optional signer module plus `pyproject.toml`; tests are one focused file. Any other production file requires Architect justification. One implementation commit and at most two fix cycles. Stop at Chief candidate review before merge/push, real signer generation, XLSX access, secret load, signature, registration/revocation, order, cancel, or position action. After accepted live signer readiness, return to a fresh RISEx ORDER-001 slice; do not implement it here.
