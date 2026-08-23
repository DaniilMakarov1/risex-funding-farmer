# TESTNET-002-RISEX-ORDER-LIFECYCLE-001 — Fixture-First Bounded Empirical Lifecycle

Status: `ACTIVE GOVERNANCE SLICE — USER-ACCEPTED BOUNDED TESTNET RISK; NO BUILDER UNTIL SEPARATE CHIEF COORDINATOR GATE; FIXTURE-ONLY; NO CREDENTIAL, SIGNATURE, LIVE NETWORK, OR POST`.

Start from exact published `main == origin/main == d5a4b78de599a9e808fd5aba13aa3d60e2925946` on `codex/risex-testnet-002-order-lifecycle-001-governance`. The fixed approved wallet has authoritative raw test balance `1000`, and its preserved session signer is authoritatively `ACTIVE` from `2026-08-23T16:40:21Z` through `2026-09-22T15:46:50Z`. The credential and signer record remain fixed outside Git and are never regenerated, reset, replaced, exposed, deleted, or revoked. This is the sole active RISEx lane slice.

## Authority, ownership, and terminal contract

- The user accepts that one later separately gated testnet experiment cannot guarantee atomic recovery. General operational success remains authoritative zero open orders plus exact flatness (`position.size == "0"`).
- The sole narrow exception is terminal `FAILED_HALTED_MANUAL_RECOVERY`: after all bounded attempts, the later live experiment may leave only its minimum-size RISEx testnet exposure or a known experiment order unresolved. This is failure, never operational acceptance, exact-flat acceptance, lifecycle readiness, or strategy authorization.
- The current milestone is fixture-only. One dedicated RISEx lifecycle controller owns preflight, state-based dispatch eligibility, reconciliation, recovery, and terminal classification. A dedicated local repository owns durable intent rows only. A direct official REST/EIP-712/ABI adapter owns request encoding and authoritative response normalization. The existing session signer is available only through an injected loader that remains uncalled in Builder tests.
- The module is isolated from and is never imported by normal Farmer startup, Scanner, shared runtime, paper repository, economics, strategy, Telegram, Nado, or Extended.

Before every hypothetical signature or dispatch, commit one immutable intent containing:

- unique experiment ID, intent kind and ordinal;
- unique nonzero `uint64 client_order_id` and a never-reused bitmap nonce pair;
- canonical unsigned action/payload digest and exact market, side, type, TIF, flags, size, price bound, source-position identity, BBO evidence, and permit deadline;
- durable `PREPARED` and then `DISPATCHING` state before the one transport call.

A timeout, disconnect, cancellation, crash, or indeterminate response marks that identity `AMBIGUOUS`; the same intent, client ID, nonce, or payload is never replayed. A later intent requires the prior one to be terminal/reconciled or past its permit deadline with authoritative post-expiry state. Exact identities are retained only in the protected local recovery journal for matching; committed fixtures, ordinary logs, reports, exceptions, and review evidence redact wallet, signer, client/order/transaction identities, signatures, and payloads.

## Accepted Phase-0 evidence

- Official `POST /v1/orders/place` exposes `reduce_only`, `MARKET`/`LIMIT`, `GTC`/`GTT`/`FOK`/`IOC`, packed `uint88`, `RISE_PERPS_PLACE_ORDER_V1`, `VerifyWitness`, bitmap nonce, EIP-2098/base64 signature, and `client_order_id`, but does not fully define oversize handling, below-minimum close exemption, residual grid invariants, reduce-only TIF combinations, market protection, or partial-close residual behavior.
- Official `POST /v1/orders/cancel` and the open/by-id/history/trade/position reads establish identity and reconciliation after dispatch; they do not guarantee that an exposed account can always close.
- Official order state says `OPEN` is active and fillable. `post_only` ensures maker-only placement, not zero fill before cancellation.
- The current official documentation index contains one order-shaped simulation: unsigned `POST /v1/portfolio/detail-preview`, documented as applying a hypothetical order without executing it for portfolio risk assessment. It exposes no contract claiming parity with Router validation or matching-engine execution.
- Two bounded unsigned calls to that preview performed no execution. A normal minimum BTC hypothetical order produced preview position size `0.0001`; on the same authoritatively flat account, `reduce_only=true`, `LIMIT`, `FOK`, opposite side produced preview position size `-0.0001`. Thus preview accepts the flags but applies hypothetical signed size without enforcing live reduce-only semantics. It cannot prove any nonzero-position close behavior.
- Immediate authoritative post-preview reads returned zero open BTC orders and zero BTC trades. The contemporaneous position command incorrectly parsed `.data.size` and returned `size:null` / `market_id:null`, so that command did not prove flatness. A later correctly parsed official read proved `.data.position.size == "0"`. During the bounded Chief fix checkpoint, a fresh correct-path read again returned `.data.position.size == "0"` (with the venue's flat placeholder `market_id == "0"`), and the official open-position list for the same public account/requested market was empty. This later evidence, not the misparsed immediate command, proves the exact-flat postcondition after the documented non-executing preview calls.
- The official documentation index and error catalog expose no other execution-faithful simulation, callStatic, dry-run, or order-test endpoint and no normative error contract for the six unknowns.
- Current official API Integration defines market `price_ticks` as the bound the order will not cross and requires `FOK` or `IOC` for market orders. Official order-type material defines `FOK` as all-or-none and `IOC` as immediate fill with remainder cancelled. The current official testnet UI constructs exact-authoritative-size reduce-only close orders, using an empirical `MARKET+FOK` path or a 30 bps price-bounded `LIMIT+IOC` path. These are sufficient primitives for a bounded empirical test under the user's risk decision, but they do not prove undocumented semantics or guarantee fill/flatness.

Official sources: [documentation index](https://developer.rise.trade/llms.txt), [place order](https://developer.rise.trade/reference/orderservice_placeorder), [cancel order](https://developer.rise.trade/reference/orderservice_cancelorder), [portfolio detail preview](https://developer.rise.trade/reference/accountservice_getportfoliodetailswithorder), [orders channel](https://developer.rise.trade/reference/orders-channel), [open orders](https://developer.rise.trade/reference/orderservice_getopenorders), [trade history](https://developer.rise.trade/reference/getaccounttradehistory), [single-position read](https://developer.rise.trade/reference/accountservice_getposition), [open-position list](https://developer.rise.trade/reference/getallpositions), and [position contract](https://developer.rise.trade/reference/positions-channel).

Governance-only audit commit `73068dc202c6c47607dc9c9b349001d4fb0edbf2` and branch `codex/testnet-002-risex-order-001` remain preserved unchanged, unmerged, and unpushed. They were independently reviewed and were not cherry-picked, merged, amended, rebased, or deleted.

## One fixture-first Builder milestone after the separate gate

- Add one isolated Python module and one focused fixture suite. The module may expose injected official query/execute, signer, clock, and durable-store boundaries, but no production credential invocation, live transport, CLI command, URL override, or live-smoke path.
- Preflight fails before signer loading or hypothetical dispatch unless runtime-fetched official testnet host, chain, domain and Router identities match; the preserved signer status is `ACTIVE`; BTC/USDC is active/unlocked; tick, step and minimum are exact; two authoritative reads agree on zero open orders and `position.size == "0"`; no lifecycle intent or unexplained account state exists; and a fresh two-sided BBO has minimum-size depth inside a fixed test-only 30 bps adverse bound.
- Opening size is exactly the current official BTC minimum, never a configurable size up to the cap. Its adverse-bound notional must be `<= USD 500`; any grid, depth, freshness, identity, or cap failure permits zero signatures and zero writes.
- Permit at most one opening intent: fixed BUY, exact minimum size, price-bounded `MARKET+FOK`, `reduce_only=false`, `post_only=false`. `MARKET+FOK` is an empirical official API/current-UI-compatible contract, not guaranteed semantics.
- Reconcile a normal or ambiguous opening solely by the persisted client/order identity, place response, order by ID, open orders, history, trades/fills, authoritative position, permit expiry, and fresh official state. Never replay. Terminal no-fill plus zero orders/exact flat is `COMPLETED_NO_FILL_FLAT`, not close-lifecycle acceptance.
- After a fill, derive each close only from fresh authoritative signed `position.size`: opposite side, exact absolute size, no enlargement or rounding, `reduce_only=true`, and a new durable identity. Close intent 1 is price-bounded `MARKET+FOK`. If terminally rejected/no-fill and the fresh position remains nonzero, close intent 2 may use the official-UI-equivalent 30 bps price-bounded `LIMIT+IOC`. A partial result may permit close intent 3 only from the new exact authoritative residual and fresh BBO. At most three automatic close intents exist.
- A later close requires the prior intent to be terminal/reconciled, a fresh position/open-order snapshot, fresh BBO/depth, and bound notional `<= USD 500`. Do not retry an unchanged deterministic rejection. A positive residual not exactly representable in current steps, unexplained position growth/sign change, stale evidence, or identity disagreement stops writes.
- FOK/IOC are selected to avoid resting orders. Only an exact known order linked to this experiment may be cancelled, at most once per exact order ID. An ambiguous cancel is reconciled and never replayed; unrelated orders are never cancelled and force a halt. Maximum place dispatches are four: one opening plus three closes. Maximum cancels are four: at most one for each possible known experiment order.
- `SUCCESS_CLOSED_FLAT` requires an observed opening fill/positive position, all experiment intents terminal/reconciled, zero open orders, and exact `position.size == "0"` on consistent final reads. Any remaining position/order, exhausted close/cancel budget, non-step residual, lost connectivity, or unresolved identity persists `FAILED_HALTED_MANUAL_RECOVERY`, stops every automated write, and produces redacted evidence plus operator-only official RISEx testnet UI full-close/read-only-verification instructions. The agent never accesses the main-wallet XLSX for manual recovery.

## Exact RED, fixture, and adversarial gates

On exact base `d5a4b78de599a9e808fd5aba13aa3d60e2925946`, `tests/test_testnet_risex_order_lifecycle.py` must first fail because the bounded module/behavior is absent. Its exact required regressions are:

1. `test_preflight_blocks_before_signer_or_post`
2. `test_intent_nonce_and_digest_are_durable_before_dispatch`
3. `test_ambiguous_open_is_never_replayed`
4. `test_open_is_exact_minimum_price_bounded_market_fok`
5. `test_fok_no_fill_finishes_flat_without_close_acceptance`
6. `test_first_close_uses_exact_authoritative_size_market_fok`
7. `test_close_fallbacks_use_fresh_state_limit_ioc_and_stop_at_three`
8. `test_partial_ioc_uses_exact_residual_without_rounding`
9. `test_non_step_residual_halts_without_another_dispatch`
10. `test_permit_expiry_prevents_delayed_ambiguous_replay`
11. `test_known_open_order_is_cancelled_once_by_exact_id`
12. `test_ambiguous_cancel_is_never_replayed`
13. `test_unrelated_order_or_position_drift_halts_without_mutation`
14. `test_disconnect_persists_failed_manual_recovery_and_stops_writes`
15. `test_success_requires_observed_fill_zero_orders_and_exact_flat`
16. `test_minimum_size_and_usd_cap_are_invariants`
17. `test_secrets_signatures_payloads_and_identities_are_redacted`

Fixtures must cover inactive signer, stale/malformed/off-grid market evidence, insufficient depth, cap failure, timeout before response, delayed order appearance, FOK reject/no-fill, IOC partial fill, below-minimum step-divisible residual, non-step-divisible residual, close rejection, unexpected order, cancel ambiguity, permit expiry, and connectivity loss while exposed. Adversarial contradictions across place/order/open/history/trade/position evidence may never grant another write or any success state.

Secret isolation proves the real loader is never called; synthetic fixtures and disposable storage only; no credential, raw signature, payload, public account/signer/order identity, private endpoint, network, or POST appears in test output or Git. The Builder runs focused tests, the full clean Python 3.11 suite, compileall, `pip check`, import identity, diff/secret checks, and creates one implementation commit without governance, shared-core, or cross-venue edits.

## Current acceptance and stop gates

- Exact published base `main == origin/main == d5a4b78de599a9e808fd5aba13aa3d60e2925946` passed the Chief's clean Python 3.11 full suite with 498 tests.
- This candidate records exactly three active bounded lane slices: the RISEx fixture-only lifecycle above and the existing Extended and Nado fixture-only slices below. Each venue's Builder requires its own separate Chief Coordinator gate.
- This governance candidate authorizes no Builder. After central acceptance/publication, Chief Coordinator must separately authorize the sole RISEx Builder. After fixture implementation acceptance, another separate Chief Coordinator gate is mandatory before any credential access, real signer load, signature, private/live request, nonce consumption, or order/cancel/close POST.
- Governance and deterministic work never read credential bytes or wallet cells. The protected signer record and main-wallet XLSX were not accessed during this governance slice.
- Stop for Chief Coordinator governance review before merge, push, Builder creation, credential access, signing, nonce consumption, durable trading claim, order placement/cancel/close, or any executing/private write.

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

# NADO-TESTNET-001 — Fail-Closed Bounded Lifecycle Core

Status: `ACTIVE GOVERNANCE SLICE — PHASE 0 AND MANUAL ELIGIBILITY ACCEPTED; NO BUILDER UNTIL SEPARATE CHIEF COORDINATOR GATE; FIXTURE-ONLY; NO CREDENTIAL OR LIVE NETWORK ACCESS`.

This governance candidate starts from exact published `main == origin/main == 1426cdcf980e8920aba4a3a1f6767412c354f620` on `codex/nado-testnet-001-governance`. Its sole purpose is to authorize, after separate Chief Coordinator review/publication and Builder gate, one fixture-first isolated Nado testnet lifecycle core translated minimally into Python from official Nado-owned TypeScript SDK commit [`315e4f23dadefeb2f86f713e423241e81467d4c3`](https://github.com/nadohq/nado-typescript-sdk/commit/315e4f23dadefeb2f86f713e423241e81467d4c3), Rust SDK commit [`e54118786b171a4325871d5bd17e5abae0e90c5a`](https://github.com/nadohq/nado-rust-sdk/commit/e54118786b171a4325871d5bd17e5abae0e90c5a), and contracts commit [`11c27b2851999f1b4f8cb4a7fbfcc9320253f12f`](https://github.com/nadohq/nado-contracts/commit/11c27b2851999f1b4f8cb4a7fbfcc9320253f12f). It must not add a JavaScript service/framework or modify/import the normal Farmer, Scanner, runtime, repository, economics, strategy, Telegram, RISEx, or Extended paths.

## Fixed official contract

- Use only Ink Sepolia chain `763373`, EIP-712 domain `Nado` version `0.0.1`, current Endpoint `0x698D87105274292B5673367DEC81874Ce3633Ac2`, gateway `https://gateway.test.nado.xyz/v1`, gateway WebSocket `wss://gateway.test.nado.xyz/v1/ws`, archive `https://archive.test.nado.xyz/v1`, and trigger service `https://trigger.test.nado.xyz/v1`. A fixture must reject every mismatched chain, domain, Endpoint, host, subaccount owner, or product verifier identity.
- Use one dedicated owner EOA and one fixed subaccount identity. Linked signers and API keys are excluded. The user has already satisfied the manual territorial/legal/Terms/no-VPN-or-proxy-circumvention gate; do not request identity documents or credentials in this milestone.
- Engine queries are authoritative for current order/account/position safety. The exact EIP-712 order digest is the lifecycle identity and must agree with validate/place/open-order evidence. Archive digest-to-match/submission/transaction evidence is audit-only and cannot independently authorize another write or `COMPLETE`.

## One bounded Builder milestone after the separate gate

- Write RED fixtures first for official environment/domain/product verifying-contract encoding, x18 price/amount/market grids, 44-bit `recv_time` plus 20-bit salt order nonce and cancellation `tx_nonce`, EIP-712 digest/signature vectors, signed `validate_order`, place/open/history/fills/positions, `cancel_product_orders` with an empty product list, active triggers, and reduce-only clamp behavior. Use synthetic keys and fixed fixture responses only.
- Add one isolated Python module with no live transport. It may expose fixture-injected query/execute boundaries but no production HTTP/WS client, URL override, credential loader invocation, CLI command, or live-smoke path. Translate only the pinned source contract required for this lifecycle; do not add a JS runtime, service, framework, generic venue layer, or dependency workaround.
- Durably persist the canonical payload bytes, endpoint/domain/product identity, EIP-712 digest, signed `recv_time`, and unique 44+20 nonce or cancellation `tx_nonce` before any hypothetical dispatch. The persisted digest and payload are immutable; the same digest/nonce is never reused, including after rejection, cancellation, expiry, fill, duplicate-digest error, or ambiguity.
- Preflight from the complete dynamic product catalog and fresh authoritative account state: exact chain/domain/Endpoint; active perpetual; exact x18 tick/step/minimum/notional math; fresh bounded price; no regular order in any product; no active trigger order; every cross-perpetual balance amount exactly zero; no isolated position; and entry plus every worst-price recovery order each `<= USD 500`.
- Permit one minimum-size price-bounded post-only entry only after a matching signed `validate_order` fixture result. Reconcile acceptance, rejection, unmatched/resting, cancellation, IOC outcome, every partial fill, and position change by the exact persisted digest and fresh authoritative engine state. A timeout, disconnect, duplicate-digest rejection, stale indexer, or missing response never causes replay or success.
- On any unresolved regular order, use `cancel_product_orders` with an empty product list and reconcile zero regular orders across the complete catalog. An ambiguous cancel-all is not retried blindly. Active trigger orders are a fail-closed preflight/final-barrier blocker; this milestone does not introduce trigger placement.
- Derive close direction and size only from a fresh authoritative position snapshot. Use a new durable identity for an opposite-side aggressive-limit IOC reduce-only close. For a below-minimum residual, the submitted minimum may rely on the official oversize reduce-only clamp only when its signed worst-price notional remains `<= USD 500`. Permit at most three total state-based close attempts, each only after a fresh snapshot and after the preceding signed request's recorded `recv_time` has elapsed; partial fill, close rejection, off-grid/unsupported residual, stale/contradictory evidence, or attempt exhaustion halts for a manual gate and is never `COMPLETE`.
- Declare `COMPLETE` only when every persisted digest and fill reconciles, no intent is pending/ambiguous/unexpired, the complete product catalog has zero regular orders, the trigger service has zero active orders, every cross-perpetual balance amount is exactly zero, and isolated positions are empty in fresh authoritative state.

## Deterministic acceptance gates

- RED proves durable-intent-before-dispatch ordering, exact official digest/signature/nonce vectors, duplicate/replay rejection, dynamic-catalog preflight, `<= USD 500` bounding, and that no ambiguity or contradiction grants another write, `FLAT`, or `COMPLETE`.
- Fixtures cover accepted-but-unmatched entry, immediate/partial/full fill, post-only rejection, expiry, ambiguous place/cancel-all/close, indexer lag, nonce/digest reuse, below-minimum clamped residual, close rejection, all three bounded close attempts, trigger-order presence, isolated/cross position presence, and the exact final barrier.
- Full Python 3.11 CI remains fixture-only and passes with no secret, private endpoint, live network, real signature, POST, shared-core, or cross-venue change.

## Exclusions and stop gate

This slice authorizes no Builder until Chief Coordinator has centrally accepted and published this governance candidate and then separately authorizes the sole Nado Builder. It authorizes no credentials, wallet/secret read, private preflight, linked signer, API key, real signing, account creation, faucet/deposit, authenticated/private call, live POST, order, cancel, fill, close, position, mainnet, real funds, Scanner/runtime/economics/strategy/Telegram change, or operational-readiness claim. Later operational work would require only the dedicated owner EOA private key/public address and fixed subaccount identity, Ink Sepolia gas, and at least the officially required test USDT0 collateral, all under separate user and Chief Coordinator gates. Stop after this one governance commit for central review; do not merge or push `main`.
