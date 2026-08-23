# TESTNET-001-RECOVERY-005 — Direct Passwd-Home Marker

Status: `ACTIVE — RED FIRST; NO REAL MARKER OR POST`.

This strictly corrective slice starts from published `main == origin/main == c84cc882c027795bbe4ea15d5233c35b187216b9`. RECOVERY-004 is `BLOCKED — TASK DID NOT CONVERGE`; its branch and implementation/tests are failed audit history and must not be inspected, copied, cherry-picked, or reused.

## Exact ownership and design

- The existing optional bootstrap module remains the sole owner of fixed RISEx testnet destination, identity, balance, one deposit dispatch, and one-shot local authorization. Its public API accepts no path, environment, CLI, ledger, transport, reset, delete, rearm, or retry control.
- The sole marker is fixed at `pwd.getpwuid(os.getuid()).pw_dir/.risex-funding-farmer-testnet-first-deposit-v1.json`. No directory hierarchy is created. Production opens the passwd home with `O_DIRECTORY|O_NOFOLLOW`, validates through `fstat` that it is a current-owner directory, and keeps that descriptor through claim and home-directory fsync. Tests may replace only a private passwd-home helper with a disposable existing directory.
- Canonical metadata is exact: schema version `1`, venue `RISEx`, host `api.testnet.rise.trade`, chain `11155931`, approved wallet `0x20f9153e2eeba0ff7880fb5a23e976e8b2af56ee`, operation `FIRST_DEPOSIT`, amount `1000`, and state `SPENT_UNKNOWN` or `READY`.
- Claim uses home-relative `O_CREAT|O_EXCL|O_NOFOLLOW`, exact mode `0600`, a current-owner regular single-link file, complete canonical `SPENT_UNKNOWN`, file fsync, then held home-directory fsync before any POST. Empty, partial, malformed, wrong-owner/type/mode/link, symlink, exclusive-create loss, or durability failure is terminal consumed/fail-closed and never reusable.
- Existing marker always performs zero POST. Existing `SPENT_UNKNOWN` or invalid/uncertain state is non-ready. Existing `READY` performs exact identity and balance reads but returns ready only for a fresh positive authoritative balance; local state alone is never readiness.
- Only an invocation with a freshly completed durable claim may dispatch the one fixed POST after exact identity/wallet revalidation. The exact observed preflight HTTP 500 tuple may reach this claim path but is never labeled `NOT_REGISTERED`; adjacent or generic errors fail before claim/POST.
- READY transition uses one fixed exclusive temp file in the held home directory, canonical READY bytes, temp-file fsync, atomic same-directory rename, and home-directory fsync. Failure is non-ready and cannot authorize another POST. The product exposes no reset/delete/rearm operation.
- Ordinary concurrent tasks/processes, restart, power loss, crash, cancellation, and network ambiguity are in scope. Intentional same-UID deletion/rename is outside the threat model and operationally prohibited.

## RED and acceptance

1. Exact published main lacks the durable direct-home claim and fails newly authored fixture tests while production remains unchanged at RED.
2. Prove exact ordering: exclusive marker create, complete write, marker fsync, home-directory fsync, then and only then POST. A failure/crash after create, write, file fsync, or home fsync leaves terminal consumed state and cannot replay.
3. Under normal filesystem behavior, concurrent async calls and concurrent subprocesses yield exactly one claim and at most one POST. Restart after abrupt process exit cannot claim again.
4. Existing empty, partial, malformed, mismatched, wrong-owner/type/mode/link, symlink, `SPENT_UNKNOWN`, `READY`, or abandoned temp state never permits a second POST. READY requires a fresh positive authoritative balance; unavailable/nonpositive balance is a non-trading blocker.
5. Post-claim cancellation propagates and remains consumed. POST timeout/EOF/TLS/redirect/final-URL/malformed/4xx/5xx, postcondition failure, READY-update failure, or session-close failure is non-ready and never retried.
6. Preserve sealed fixed HTTPS ownership, `trust_env=False`, default CA, no redirect, exact chain/domain/auth/token/wallet/amount, sanitized errors, one POST site, optional-import isolation, and no order/cancel/replace/position/trading/mainnet surface.
7. Public source exposes no marker path or state override and no reset/delete/rearm/retry API. Builder and tests use only disposable private-home fixtures and perform no live network call or real-home filesystem mutation.
8. Run focused asyncio-debug tests, subprocess race/crash cases, the accepted 367-test baseline plus new tests, compileall, diff, secret/import/one-POST/public-surface/pending-process checks. Production must be materially smaller than the rejected hierarchy design.

## Workflow boundary

Exactly one fresh Builder may author fresh RED, stop for Architect review, then implement only after GREEN authorization. Expected scope is `src/risex_farmer/testnet_bootstrap.py` and `tests/test_testnet_bootstrap.py`; governance remains Architect-owned. One implementation commit and at most two fix cycles. Stop at Chief candidate review: no merge, push, real marker, live balance/deposit, XLSX, secret, Nado, Extended, TESTNET-002, Scanner/runtime, paper, Telegram, or economics work.
