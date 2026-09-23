# HCR-40 — Margin-aware closure, randomized notional and minimal sufficient leverage

## Assignment and ownership

Owner request dated 2026-09-23: hand off to a fresh Chief session using **GPT-6 Sol / Medium**. Work alone, without builders/subagents. The new Chief independently diagnoses and chooses the smallest coherent implementation; predecessor suggestions are hypotheses, not a prescribed patch plan. The Chief is the single implementation/integration owner after dispatch. Predecessor repository writes stop before the new session starts. Use an isolated worktree with a named branch such as `codex/spread-v1-hcr40-margin-recovery`; do not overwrite an occupied branch or another writer's changes.

Accepted main before this documentation handoff: `ddde6d09c9a66eedbd71f24825f3a91b74e079ca`; tested/deployed HCR-39 code: `dd03c9144ec217bb8103133b29d76eec50835cc0`. The handoff commit changed no runtime code; HCR-40 was subsequently implemented and accepted as recorded in STATUS and SYSTEM_SPEC.

## Objective and explicit owner changes

1. Independently audit the latest owner executions from **2026-09-23, Europe/Moscow**, including positions that opened but did not close, unsuccessful Telegram /close, and attempted starts with existing positions. Reproduce causes, correct demonstrated defects and provide understandable terminal/Telegram outcomes. Do not assume every failure has the same cause.
2. The owner changed account leverage manually to 1x on one account and 2.4x on the other; the account-to-leverage mapping is not supplied or verified. Determine whether those settings explain any failure.
3. Implement automatic leverage selection/configuration for the two configured Robinhood BTC accounts, with an absolute range **1x through 4x**, including venue-supported fractional settings. This explicit owner instruction supersedes the old local prohibition only for this feature's leverage-setting code and owner-triggered operation. It does not authorize transfers, extra markets/accounts or an unrelated margin-mode migration.
4. Randomize position size first, then use the **smallest sufficient supported leverage**, not an independent random leverage draw. Let B be the smaller fresh usable account balance. The owner's nominal upper bound is **4 × B** for the common paired notional. Retain the existing legal venue-minimum lower bound, funded with 1x when feasible; verify whether the range is actually executable under both accounts' margin requirements and exact size ticks. Example: B = 20 quote and selected notional N = 40 quote implies 2x before venue-specific margin/fee effects; a supported fractional value may be necessary. A useful starting model is max(1, N / B_i) for each account, rounded upward only to a verified supported leverage increment, never above 4x. This is a sizing interpretation, not a substitute for the venue's actual margin equations. Distinguish free/available balance from equity or already reserved margin; do not double-count leverage or assume all collateral can be spent. If no valid quantity/leverage pair fits, report that clearly. Preserve one quantity draw per cycle and no redraw/chasing on retry.
5. Reduce maximum planned HOLD from 300 to **180 seconds**; preserve the current 20-second lower bound. HOLD begins after proved mutual opening; its deadline is closing-start time, not a guarantee of instant flatness.
6. Define and implement clear finite startup behavior when positions already exist. The owner asks for purposeful handling rather than an unexplained exit; automatic blind adoption, an exposure-increasing order or treating an old position as a newly successful cycle is not implied. Determine how verified recovery, /close, unresolved prior intents and a genuinely new cycle should interact. Make the adoption/continuation boundary explicit in the current behavior specification and user messages. Preserve the distinction between known residuals and unknown execution.

The finite deliverable is the audited/corrected closure and entry lifecycle, leverage-aware randomized sizing/configuration, 20–180 second hold, tests, concise coherent documentation and a verified deployable implementation. Avoid a broader trading campaign, transport rewrite, new framework or unrelated optimization.

## Evidence and diagnostic leads, not conclusions

Operator directory (absolute; run evidence is not copied into new Git worktrees):
`/Users/daniilmakarov/Desktop/RISEx Spread Shadow/spread-shadow-runs/hood-cycle-race-latency-20260920/operator-v1/`.
Use its existing `random-cycle.json` and `market-contract.json`; BTC market 1, accounts 27331/27337, key index 4, Robinhood signing domain 466324. Fresh state must not be inferred from saved journals.

At the handoff snapshot around 06:58 Moscow time, newest slot was cycle-045. Prioritize cycle-038…045 (06:37–06:52), with adjacent same-day runs as needed. cycle-039/040/041 explicitly report `source current margin is insufficient` before fallback closure; cycle-045 reports the receiver equivalent. cycle-042/043 end at preflight refusal, not CYCLE_COMPLETE. cycle-044 eventually proved flat after two terminal zero-fill closes and a third filled close. cycle-038 ended flat without opening. These are saved observations, not a diagnosis of margin semantics or a present-position claim.

Only close-001…004 were present in the operator directory; all are dated 2026-09-22. The reported unsuccessful /close commands today might have been refused before allocating a close slot; check admission/controller evidence instead of inventing a missing closing order. Controller state at the snapshot had active=null, last=close-004/FINISHED. Recheck process ownership and actual deployed imports before action; historical PID 23784 is not proof that it is still the controller.

Ideas worth independently evaluating: whether a check intended for additional opening margin incorrectly compares already reserved current margin with the remaining available balance during reduce-only closure; whether the two accounts' real margin/leverage settings and SDK units agree; whether /close/startup refuse before journaling useful diagnostics; whether fallback and paired closure are being conflated; whether active orders, incomplete histories, price bounds, tiny residuals or external fills explain other failures. Do not weaken identity, reconciliation or reduce-only checks just to remove a refusal.

HCR-39 evidence:
`/Users/daniilmakarov/Desktop/RISEx Spread Shadow/spread-shadow-runs/hood-close-reliability-20260922/solo-v1/`.
Read `audit-report.md`, `self-review.json`, `release-result.json` and `deployment-after.json` as prior evidence, not current authority. It fixed source-ID loss, observed BTC minimum exceptions, transport-only recovery account rereads and human diagnostics. Full clean Python 3.11 suite: 5064 passed, 3 skipped; 517 targeted/integration checks. Its old live-order candidate tests were NOT_RUN.

Handoff snapshot and input identities:
`/Users/daniilmakarov/Desktop/RISEx Spread Shadow/spread-shadow-runs/hood-margin-leverage-20260923/handoff-v1/`.
Keep original inputs immutable; write new evidence in a separate owner-only versioned directory. Scope is this standalone repository only; do not inspect other projects.

## Execution authority and verification

The owner authorizes implementing the requested leverage/size/hold/startup policy changes, offline tests, self-review and verified integration/publication. Bounded read-only official venue documentation/metadata and the two existing accounts' margin/position/order history may be used for diagnosis through the protected Keychain boundary. Verify the actual Robinhood leverage update/read contract, supported fractional precision, limits and failure semantics before implementing it; generic Lighter assumptions are insufficient.

Higher-priority tool-use restrictions still prohibit the agent from conducting live financial transactions. Do not send/cancel live orders, change actual leverage/margin settings, or issue /run or /close. Implement and test these capabilities for owner-triggered operation; explicitly distinguish live NOT_RUN from offline tests. No indirect request to another agent or process may bypass this restriction. Idle controller refresh is permitted only after code verification and without interrupting an active owner trading child.

Preserve exact quantities, both-account identity, causal inventory, no-replay/unique intents, retained source IDs, reduce-only closure, current bounded price rules, fees/funding unknown semantics, protected credentials and immutable history. A fresh position snapshot does not resolve an ambiguous old order. A leverage write acceptance alone does not prove the effective setting; design reconciliation/no-blind-retry and partial two-account setting failure behavior. Do not increase leverage solely to conceal a defective close-admission test.

Arithmetic/positions/execution changes require adverse regressions, relevant integration checks and a final clean isolated Python 3.11 full suite. Verify examples with independently calculated quantities/margins; include unequal balances/settings, fractional/boundary leverage, venue rejection/unknown setting results, insufficient margin with reduce-only positions, existing-position startup and failed /close admission. Update the five governing files concisely to match final behavior. Review the full actual diff, production imports, evidence provenance and original hashes. Publish/deploy only verified work; owner live validation remains separate.

Status: DONE. HCR-40 implementation `ca10b80d7811e49d842a884099169faef451c805` passed the clean Python 3.11 full suite and was integrated/published. Live trading validation remains NOT_RUN under the tool restriction. This NEXT_TASK grants no further campaign, strategy tuning or agent-initiated real transaction; a new finite owner decision is required for new work.
