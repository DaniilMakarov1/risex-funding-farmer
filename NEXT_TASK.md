# TESTNET-002-RISEX-ORDER-RECOVERY-001 — Execution-Faithful Reduce-Only Semantics Gate

Status: `BLOCKED — NO SAFE EMPIRICAL METHOD; NO BUILDER; NO CREDENTIAL, SIGNATURE, OR EXECUTING WRITE`.

Start from exact published `main == origin/main == 08053f2eaa5aad044eae4575babb9903049bb415` on `codex/testnet-002-risex-order-recovery-001`. Fixed approved wallet `0x20f9153e2eeba0ff7880fb5a23e976e8b2af56ee` has authoritative raw test balance `1000`, and preserved signer `0x6274d6d9f628ba89c36de4b71efa2c602b7f783b` is authoritatively `ACTIVE` from `2026-08-23T16:40:21Z` through `2026-09-22T15:46:50Z`. Older `CREATED` and pending-registration text is superseded. This is the active RISEx lane task.

## Authorized question and invariant

Determine from current official RISEx evidence whether a state-free empirical method can authoritatively resolve all acceptance-blocking close semantics before any order may create exposure:

1. oversize reduce-only clips or rejects;
2. below-minimum residual close exemption;
3. whether every authoritative residual is step-divisible;
4. accepted `reduce_only` combinations across `MARKET`/`LIMIT` and `IOC`/`FOK`;
5. `MARKET` `price_ticks` and protection semantics;
6. partial reduce-only residual behavior.

Only RISEx testnet and official sources are in scope. Any eventual experiment must cap every potential exposure/notional below or equal to USD 500, use exact venue evidence, never retry an ambiguous place/cancel/close blindly, and finish with authoritative zero open orders and exact flatness (`position.size == "0"`). Final operational safety is mandatory, not best effort. No mainnet, real funds, Nado, Extended, strategy, Scanner, Farmer/runtime, paper economics, Telegram, or XLSX/main-wallet access is permitted.

## Independent official evidence

- Official `POST /v1/orders/place` exposes `reduce_only`, `MARKET`/`LIMIT`, `GTC`/`GTT`/`FOK`/`IOC`, packed `uint88`, `RISE_PERPS_PLACE_ORDER_V1`, `VerifyWitness`, bitmap nonce, EIP-2098/base64 signature, and `client_order_id`, but does not define the six semantics above.
- Official `POST /v1/orders/cancel` and the open/by-id/history/trade/position reads establish identity and reconciliation after dispatch; they do not guarantee that an exposed account can always close.
- Official order state says `OPEN` is active and fillable. `post_only` ensures maker-only placement, not zero fill before cancellation.
- The current official documentation index contains one order-shaped simulation: unsigned `POST /v1/portfolio/detail-preview`, documented as applying a hypothetical order without executing it for portfolio risk assessment. It exposes no contract claiming parity with Router validation or matching-engine execution.
- Two bounded unsigned calls to that preview performed no execution. A normal minimum BTC hypothetical order produced preview position size `0.0001`; on the same authoritatively flat account, `reduce_only=true`, `LIMIT`, `FOK`, opposite side produced preview position size `-0.0001`. Thus preview accepts the flags but applies hypothetical signed size without enforcing live reduce-only semantics. It cannot prove any nonzero-position close behavior.
- Immediate authoritative post-preview reads returned zero open BTC orders and zero BTC trades. The contemporaneous position command incorrectly parsed `.data.size` and returned `size:null` / `market_id:null`, so that command did not prove flatness. A later correctly parsed official read proved `.data.position.size == "0"`. During the bounded Chief fix checkpoint, a fresh correct-path read again returned `.data.position.size == "0"` (with the venue's flat placeholder `market_id == "0"`), and the official open-position list for the same public account/requested market was empty. This later evidence, not the misparsed immediate command, proves the exact-flat postcondition after the documented non-executing preview calls.
- The official documentation index and error catalog expose no other execution-faithful simulation, callStatic, dry-run, or order-test endpoint and no normative error contract for the six unknowns.

Official sources: [documentation index](https://developer.rise.trade/llms.txt), [place order](https://developer.rise.trade/reference/orderservice_placeorder), [cancel order](https://developer.rise.trade/reference/orderservice_cancelorder), [portfolio detail preview](https://developer.rise.trade/reference/accountservice_getportfoliodetailswithorder), [orders channel](https://developer.rise.trade/reference/orders-channel), [open orders](https://developer.rise.trade/reference/orderservice_getopenorders), [trade history](https://developer.rise.trade/reference/getaccounttradehistory), [single-position read](https://developer.rise.trade/reference/accountservice_getposition), [open-position list](https://developer.rise.trade/reference/getallpositions), and [position contract](https://developer.rise.trade/reference/positions-channel).

Governance-only audit commit `73068dc202c6c47607dc9c9b349001d4fb0edbf2` and branch `codex/testnet-002-risex-order-001` remain preserved unchanged, unmerged, and unpushed. They were independently reviewed and were not cherry-picked, merged, amended, rebased, or deleted.

## Safety verdict

No currently documented safe empirical method exists. A flat-account reduce-only live probe can establish only behavior at position zero; it cannot distinguish oversize clipping from rejection against a nonzero position or establish residual exemption, grid, or partial-fill behavior. Creating a nonzero position to test those facts is circular because the unproven close contract is the only proposed recovery. The portfolio preview cannot validate the recovery order, and market liquidity, a price buffer, `post_only`, elapsed time, or an assumed `FOK` combination cannot guarantee exact flatness under the current official contract.

Therefore this blocked task has no ownership handoff, durable trading intent, order nonce, or operational dispatch allowance. Maximum executing dispatch counts are place `0`, cancel `0`, and close `0`; maximum credential loads and signatures are `0`. No Builder, RED, fixture implementation, or live experiment is authorized.

## Exact external evidence required to unblock

RISEx must provide at least one of:

1. a documented execution-faithful, state-free test/callStatic/dry-run endpoint that uses the same Router and matching validation as live placement, can evaluate a supplied nonzero hypothetical position, defines non-mutation, and returns authoritative outcomes for all six semantics; or
2. normative official contract documentation for all six semantics plus an exact, guaranteed close-to-flat primitive, including accepted order-type/time-in-force combinations, price protection, all-or-none/partial behavior, below-minimum and step-grid residual handling, and authoritative reconciliation identity.

After that evidence exists, Architect must replace this blocker with one new strictly bounded governance slice defining exact ownership, one-shot durable intent, preconditions, dispatch maxima, no-retry reconciliation, fixture/adversarial/secret-isolation RED gates, USD 500 cap, and mandatory zero-orders/exact-flat live acceptance. Do not infer those semantics or activate implementation from a successful HTTP response.

## Current acceptance and stop gates

- Exact old main independently passes 87 focused and 498 full Python 3.11 asyncio-debug tests, compileall, `pip check`, import-identity, clean-status, and diff checks in an isolated environment.
- The central parallel-lane governance prerequisite is fulfilled at exact published `main == origin/main == de31ed4cbfe850705e59603cbd5346df4cf6d236`. This file contains two active bounded lane slices: the RISEx blocker above and the Extended fixture-only governance slice below. Nado has no active slice and remains research-only. Extended Builder work requires central acceptance/publication of this candidate and a separate Chief Coordinator gate.
- Governance changes only governing sources and records the accepted `ACTIVE` signer fact, this one blocker, and the standing per-lane Architect/Builder authorization and limits.
- Protected credential/record and `/Users/daniilmakarov/.risex-testnet-secrets/testnet-wallets-2026-08-23.xlsx` were not accessed during this recovery/governance slice; the accepted historical registration accessed the XLSX exactly once inside its approved lazy callback. Deterministic or governance work never reads credential bytes or wallet cells.
- Stop for Chief Coordinator governance review before merge, push, Builder creation, credential access, signing, nonce consumption, durable trading claim, order placement/cancel, or any executing/private write.

# EXTENDED-TESTNET-001 — Fail-Closed Bounded Lifecycle Core

Status: `ACTIVE GOVERNANCE SLICE — PHASE 0 ACCEPTED; NO BUILDER UNTIL SEPARATE CHIEF COORDINATOR GATE; FIXTURE-ONLY; NO CREDENTIAL OR LIVE NETWORK ACCESS`.

This governance candidate starts from exact published `main == origin/main == de31ed4cbfe850705e59603cbd5346df4cf6d236` on `codex/extended-testnet-001-governance`. Its sole purpose is to authorize, after separate Chief Coordinator review/publication and Builder gate, fixture-first implementation of an isolated Extended testnet lifecycle core around pinned official SDK commit `2130cdb1cd6e7b1867db83bd3af036572d258739`. It must not modify or be imported by the normal Farmer, Scanner, runtime, repository, economics, strategy, Telegram, RISEx, or Nado paths.

## Builder scope after the separate gate

- Write RED fixtures first for official Sepolia configuration/domain, signing hash and explicit nonce, SDK settlement-hash external order identity, market/account/order/fill/position normalization, cancellation authorization, and account-stream gap/reconnect behavior. CI is fixture-only and must use synthetic keys and responses.
- Persist one canonical lifecycle intent before any hypothetical dispatch with state `PREPARED`, an explicit never-defaulted nonce within `1..2^31-1`, deterministic official-SDK external ID, canonical payload digest, and server-time expiry. A nonce or external ID is never reused and a conflicting digest fails closed.
- Permit only a price-bounded, short-expiry `IOC` lifecycle in the API/product/SDK common safe subset. `GTC`, `GTT`, `FOK`, `TOB`, indefinite expiry, and unbounded market orders are excluded.
- Require fresh official market/account evidence before arming: active non-RFQ perpetual; `minOrderSize == minOrderSizeChange`; exact price and quantity grid; fresh two-sided one-step depth; entry and worst-case close each `<= USD 500`; account identity/domain match; no unresolved stream gap; and agreeing stream/REST evidence of zero open orders and exact flatness. A public quote is never an exit or liquidity guarantee.
- Reconcile place/cancel/mass-cancel/close by the persisted external ID, nonce, digest, expiry, status/open/history/fills, authoritative position, fresh account stream, REST snapshot, and server time. A timeout or ambiguous response never causes a blind place, cancel, mass-cancel, or close retry.
- After any fill, derive close quantity only from the authoritative signed position and use a new identity for the opposite price-bounded `IOC reduce-only` close. Off-grid or below-minimum residual, partial-fill uncertainty, stale/gapped evidence, identity disagreement, rejected close, or inability to prove bounded exposure stops the lifecycle without guessing.
- Declare completion only after server time is beyond every recorded expiry, no intent is pending or ambiguous, account-stream evidence is fresh and gap-free, REST reports zero open orders and zero positions, exact position is zero, and all fills reconcile to the persisted identities.
- Keep production credentials outside repository/config/fixtures/logs and behind an uncalled loader boundary. The milestone may test that boundary only with synthetic values; it must contain no CLI/live-smoke path capable of credential access or network dispatch.

## Deterministic acceptance gates

- RED proves PREPARED-before-dispatch ordering, canonical digest/external-ID stability, nonce bounds/uniqueness, expiry fencing, and rejection of SDK nonce defaults or identity reuse.
- Fixtures prove every preflight rejection, one-step `<= USD 500` sizing, safe IOC encoding, place/cancel/mass-cancel ambiguity reconciliation without duplicate writes, partial-fill accounting, close rejection, stream gaps/reconnect, off-grid and sub-minimum residual stop, and the exact final barrier.
- Adversarial fixtures prove contradictory REST/stream/status/history/fill/position evidence cannot yield `FLAT`, `COMPLETE`, or another write allowance.
- Full Python 3.11 CI remains fixture-only and passes with no secret, private endpoint, live network, POST, shared-core, or cross-venue change.

## Exclusions and stop gate

This slice authorizes no Builder until Chief Coordinator has centrally accepted and published the governance candidate and then separately authorizes the sole Builder. It authorizes no credentials, private preflight, API-key creation, signing, deposit, faucet, authenticated call, live POST, order, cancel, fill, close, position, mainnet, real funds, Scanner/runtime/economics/strategy/Telegram change, or operational readiness claim. Stop after the governance candidate for central review; do not merge or push `main`.
