# HCR-41 — Audited execution corrections; return for independent review

## Final release handoff — owner authorization

On 2026-09-23 the owner explicitly assigned Chief task `01a0cc6e-175d-7582-a7ad-74a9601e2ca1` sole ownership of the finite HCR-41 integration, publication and idle Telegram-controller update. The independent auditor accepted offline candidate `22296ebfeedd6e382321270e1685f6a861b3cc81` in `/Users/daniilmakarov/Desktop/RISEx Spread Shadow/spread-shadow-runs/hood-hcr41-corrections-20260923/auditor-v7/` and stopped implementation writes and polling. This authorization supersedes the older Chief no-main/no-push/no-activation and auditor-only integration instructions below for this release only. Chief works alone; no further delegation. Preserve all predecessor branches/worktrees/evidence and do not rewrite history or overwrite other writers.

Before runtime changes verify actual local and remote main, branch cleanliness and change authorship, active controller/trading-child processes and instance/operator locks. Integrate only the accepted candidate, resolve any new changes with corresponding checks, update governing documents to the actual accepted/published/activated state, push main and verify remote HEAD. Update the Telegram controller only when no owner trading cycle is active and without clearing state/journals or replaying queued commands. Verify the runtime import source, Python and SDK, and idle controller health without any trading command. If a cycle is active, defer activation with a precise record and no monitor. Preserve a separate owner-only release packet with exact local/remote/runtime identities and results; report back to the auditor task.

No real orders, cancellations, leverage-setting writes, `/run` or `/close` are authorized for Chief in this handoff. The owner will run live controls after release. No new reserve, numeric policy, strategy adjustment or research campaign.

## V7 return for correction — D4 preparation cancellation only

The auditor returned v6 commit `edc651f5d0a1698223feeb6153875d94ba7c19d3` for one new, reproduced defect. Preserve that branch, commit, clean-suite output and packet unchanged. Current Chief branch `codex/spread-v1-hcr41-preparation-cancel-v7` in `/Users/daniilmakarov/.codex/worktrees/risex-hcr41-preparation-cancel-v7` starts from assignment seed `e70cff08438b00570ec0f175716cf52b144f4a8f`; port the reviewed v6 implementation explicitly, then correct only D4 and its direct tests/docs. Auditor task `01a0c9cd-c8bc-75b0-92b3-04b75270a65e` remains sole acceptance/integration owner. Chief works alone.

D4: an outer `asyncio.wait_for` can cancel `sdk.update_leverage_fraction` during nonce/signing/lock preparation. `CancelledError` bypasses its `except Exception`, leaving only an unresolved intent although transport was never entered. Auditor's actual-adapter synthetic probe is `hood-hcr41-corrections-20260923/auditor-v6/preparation_timeout_probe_v2.py`; v2 establishes nonce started, zero HTTP calls, UNKNOWN and next-run block. V1's cold import did not establish the same race. Add causal pre-transport cancellation/timeout classification with a durable NOT_SENT event and preserve cancellation semantics. Once transport may have begun, cancellation/timeout remains UNKNOWN and is never replayed. Do not treat every `CancelledError` as safe. Verify outer deadline in nonce/signing/lock, explicit cancellation before/after transport, one send only and next-run admission. Run relevant adverse/integration checks and one final clean isolated Python 3.11 full suite with pinned SDK. Save a separate owner-only chief-v7 packet and return for independent review. No new numeric policies, real financial action, activation, merge/push or subagents.

## Authority and ownership

Owner request on 2026-09-23: give Chief a detailed implementation assignment covering the independent review and require Chief to return the result to the owner auditor for checking. This is a new finite assignment in the existing Chief task, superseding HCR-40's completed ownership arrangement. Preserve the session's model/effort settings. Chief works alone: no builders, subagents or further delegation.

Accepted runtime/main base: `1af259dc766928bcb247c10fdd63487d48c5d6cb`; HCR-40 implementation: `ca10b80d7811e49d842a884099169faef451c805`. Their source/tests trees agree. The assignment seed changes only NEXT_TASK and STATUS. Preserve all predecessor branches, worktrees, journals and audit packets.

Implementation owner: Chief task `01a0cc6e-175d-7582-a7ad-74a9601e2ca1`, host `local`, current worktree and branch named in the v7 section above. Preserve any intervening work instead of overwriting it. Own the current hood_handoff package, directly relevant tests and coherent updates to the five governing files. Do not edit frozen modules or other repositories.

Independent reviewer and sole integration/publication owner: auditor task `01a0c9cd-c8bc-75b0-92b3-04b75270a65e`, host `local`. Auditor implementation writes stop on dispatch. Chief may implement, test, self-review and commit locally, but must not merge/push main, activate the candidate, restart the production controller or replace the running checkout. Return READY_FOR_REVIEW or an honest partial/blocker packet; acceptance belongs to the auditor.

Authorized now: bounded code/documentation corrections, offline reproductions, simulations, relevant integration tests, local commits and official-documentation/installed-SDK research. The requested usable-margin correction is in scope. Arbitrary enabled reserve percentages, changed slippage policy, wider price bounds, altered minimum exemptions, new markets/accounts or altered hold/risk limits are not implicit. If a venue fact or numeric policy remains materially unproved, finish independent work and return the exact unresolved decision with concrete alternatives instead of guessing.

Prospective read-only diagnostic gate: if saved evidence and official documentation cannot resolve an in-scope fact, one finite pass against official Robinhood endpoints may read metadata/book and the two configured accounts' settings/order/trade history through the existing protected credential boundary. Limit this diagnostic pass to five minutes and 60 requests; record purpose, actual endpoints, start/end and input identities in the new evidence packet. These are collection bounds, not changed trading limits. No recurring collector, unsolicited messages or persistent streaming service. Any materially broader collection needs an updated finite gate before it starts.

No real orders, cancellations, leverage-setting writes, `/run` or `/close` are part of this agent assignment. Higher-priority financial-action restrictions continue to apply. Implementing the previously authorized owner-triggered leverage feature is allowed; executing it with real accounts is not authorized here.

## Evidence and initial recovery

Read these immutable packets using absolute paths; ignored evidence is not copied into worktrees:

- `/Users/daniilmakarov/Desktop/RISEx Spread Shadow/spread-shadow-runs/hood-chief-review-20260923/solo-v1/`: `review.md` is the detailed defect report; `review-packet.json` identifies inputs; `review-measurements.json`, `probe-results.json`, `review_probes.py`, `current-reporter-results.json`, `telegram-cycle-050-render.txt` and `focused-tests.log` support it. Verify hashes. Probes demonstrate bad current behavior, not acceptance criteria; rerun adapted probes in a new packet.
- `/Users/daniilmakarov/Desktop/RISEx Spread Shadow/spread-shadow-runs/hood-fill-latency-20260923/solo-v1/`: Chief's latency audit and 13-file manifest.
- `/Users/daniilmakarov/Desktop/RISEx Spread Shadow/spread-shadow-runs/hood-margin-leverage-20260923/solo-v1/`: implementation/test/deployment provenance, including clean Python 3.11 full run, 5074 passed / 3 skipped.
- Operator inputs: `/Users/daniilmakarov/Desktop/RISEx Spread Shadow/spread-shadow-runs/hood-cycle-race-latency-20260920/operator-v1/`. Prioritize cycles046–050 and child journals; 048 is launch-only. Configuration identifies Robinhood BTC market 1, accounts 27331/27337, key index 4 and signing domain 466324. Saved positions are not current observations.

Create a separate owner-only output packet outside Git, for example `/Users/daniilmakarov/Desktop/RISEx Spread Shadow/spread-shadow-runs/hood-hcr41-corrections-20260923/chief-v1/`, without overwriting an existing directory. Stream journals. Retain input hashes, commands/environment/import paths, failures and a finite findings-to-checks table. Never retain credentials or raw signed/authentication payloads.

## A. Reliable and faster residual close, including /close

Confirmed defect: random_cycle fallback obtains book, then account and calls generic SDK submission without carrying `mutation_deadline_monotonic`. SDK preparation starts a new freshness period. The offline probe reached fake transport with book age 0.266 s despite a 0.200 s limit. Relevant locations: `random_cycle.py:3780–3832,3920–3944`; `sdk.py:1257–1262,1416–1423` at the accepted base.

Cycles047/050 required two/three residual-close attempts. Three of five were canceled-too-much-slippage. Book-to-intent was 0.297–0.319 s; book-to-response 0.898–0.944 s. These are local intervals, not matching timestamps, and do not prove historical expiry of the configured 10-second freshness limit.

Deliver a send boundary preserving book/account/metadata/nonce deadlines through preparation and transport. Remove avoidable post-quote waiting using existing mechanisms where applicable, obtain executable prices late and retain causal exact-residual/identity validation. Choose the smallest coherent implementation, not a new framework. Record close quote age, account/nonce/signing/preparation/transport and terminal-observation intervals with explicit meanings. Keep shared recovery behavior for cycle fallback and explicit close.

Acceptance: expired account/book/nonce/signing evidence causes zero sends; normal closure succeeds; partial/terminal-zero-fill attempts reconcile before new residual/ID; ambiguous sends never replay; changed position/identity, account fairness and BTC tiny-residual regressions remain covered. Do not widen prices or multiply retries as a substitute for correcting preparation.

## B. Executable quantity and minimum sufficient leverage

Confirmed gap: floor(10000 * free_balance / notional) consumes almost all modeled free balance above 1x. cycle050 selected 34.65% IMF for balance 22.054166 and notional 63.638626: modeled margin 22.050783909, headroom 0.003382091. Submitted-price headroom was 0.003533858. Receiver was terminal canceled-margin-not-allowed with zero fill; source filled externally. The exact venue admission equation is not proved. Locations: `random_cycle.py:490–504,527–530,2453–2459,2843–2849`.

Determine applicable margin inputs/units from installed SDK/contracts and official Robinhood evidence, distinguishing mark/execution price, fees, reserved margin and free balance. Correct usable capacity for both accounts before exposing the source LIMIT. Retain the owner's absolute 1x–4x supported fractional range, smallest sufficient leverage, single quantity draw and 20–180 s HOLD. The random range must respect executable capacity as well as its absolute 4x cap. Preserve the occupied-margin closing fix. No invented fees, hidden reserve default, blind setting retry, all-4x policy or quantity redraw. Recheck movement between selection, confirmed settings, refreshed quote and send.

Acceptance: independent arithmetic for cycle050, unequal balances, 1x sufficiency, fractional rounding, 4x boundary, nonzero evidenced costs/reserves, adverse price/mark movement and accepted-but-margin-canceled orders. Show budget components and remaining headroom. If required reserve/venue facts remain unknown, return the exact blocker and reviewable choices; neither the old zero-reserve formula nor an arbitrary enabled buffer completes this outcome.

## C. Terminal classification and understandable outcomes

Confirmed defect: contracts.py knows canceled-margin-not-allowed; offline_report.py's smaller terminal set omits it. cycle050 receives false UNRESOLVED_EXECUTION/UNRESOLVED_INTENT, inventory/pairing UNKNOWN and an unproved receiver despite terminal zero fill. It correctly names external source account 23942. Immediate output also omits the cancellation reason. Locations: `offline_report.py:864–866,934–935`; `contracts.py:104–120`; `operator_view.py:224–225`.

Use one explicit supported terminal contract, preserving intentional compatibility and identity/quantity/history checks. Never accept every arbitrary canceled-* string. Surface actual cancellation reasons in Telegram and terminal. Reconstructed cycle050 must distinguish external source fill, receiver margin cancellation/zero fill, failed reciprocal pairing and completed residual cleanup. Missing fees stay unknown. Reconstruct from immutable journals without rewriting original outcomes.

Acceptance: cycle050-shaped saved journal -> report -> Telegram/terminal integration regression; all supported terminal cancellations; unknown/missing-terminal/incomplete-history negative controls. Report per-phase/account own/external/unproved quantities, zero fill versus unsent, residual positions and closure concisely. Preserve gross PnL, fees, net PnL and funding distinctions; fee uncertainty never erases execution proof.

## D. Recoverable ambiguous leverage setting

Confirmed gap: operator_recovery.resolve_prior rejects unresolved leverage history before current reads; only original-cycle CONFIRMED/REJECTED clears it. The counterexample stays blocked after the requested fraction becomes effective and accounts are flat. /close readiness succeeds without resolving the setting. Existing tests cover blocking, not recovery. Locations: `operator_recovery.py:138–219,242–247`; `random_cycle.py:2473–2511`.

Implement bounded read-only reconciliation of the original update, binding transaction/nonce, identity, terminal/effective state and pending-write risk as appropriate. Preserve a separate resolution checkpoint, original history and restart behavior. Current fraction alone must not dismiss a possibly pending write. Persist sufficient sanitized identity at new intents and explain older insufficient records. Keep /close available for proved inventory; no blind resend/state deletion.

Acceptance: delayed provable confirmation restores admission without another setting mutation; conflicting/unchanged/pending/unproved outcomes stay unresolved; restart/checkpoint validity and partial two-account setting failure are covered. User guidance must expose an implemented recovery action, not permanent manual-reconciliation wording with no procedure.

## E. Opening visibility and measurable latency improvements

Distinguish three observed LOST cases, three private-visible/public-absent cases, one pre-receiver external fill and one margin rejection. Absent-level examples: 046/2 BUY own 87210.1/public best bid 87210.0; 049/1 SELL own 87263.2/public best ask 87263.3; 049/3 SELL own 87242.1/public best ask 87242.2. Do not claim better-price competition explains these absences.

Reproduce causal private/public observations; add bounded request start/end and available publication/version/continuity provenance. Reduce avoidable waits only while equivalent exact order/owner/quantity/priority/freshness evidence remains valid. Signing medians are already 1.526/0.703 ms; source ACK 0.303 s, first observation 0.600 s, guard 0.902 s. Investigate state/book reads and refreshes. Do not weaken priority guards or substitute anonymous depth. A stream alternative needs verified Robinhood equivalence; a platform/transport rewrite is outside scope.

Telegram sends notifications outside the same child execution path; existing evidence does not prove that notifications delay the receiver. Keep per-interface measurements comparable and state uncertainty. Cover opening and normal paired closing integration paths. Saved 046–050 contain no successful paired opening/closing: their paired-close performance is NOT_RUN. External-liquidity emergency cleanup is separate from reciprocal strategy success.

Acceptance: public-level delay versus real competition has distinct diagnostics; delayed/conflicting/future/stale observations never admit a receiver; a later valid bounded observation can recover where causal rules permit; timings identify overlapping/sequential work. Quantify offline improvement if achieved; otherwise identify the remaining limit without promising a live gain.

## F. Incomplete launches and concise documentation

cycle048 has only launch.json. A slot is allocated before credential/prior-intent checks, while Telegram discards child stdout/stderr. Its exact historical cause is unknown. Persist a sanitized structured failure for future preparation exits before the cycle journal starts, visible in Telegram and terminal. Preserve UNKNOWN inventory when no sufficient observation exists. Never capture credential prompts/tokens or unsafe raw exceptions. Missing terminal remains incomplete.

Keep the five governing files concise and consistent with final behavior: existing-position startup, eventual setting recovery, paired versus emergency close, planned hold deadline versus actual flatness, execution/fees/PnL and remaining limits. No governance/history files, unrelated cleanup or new frameworks. Multiple MARKET fragments per LIMIT remain deferred.

## Verification and return contract

1. Reproduce confirmed defects on the accepted base and demonstrate correction. Review the full actual diff and callers; call Chief's review self-review. Preserve failures; compute expected arithmetic independently.
2. Run adverse regressions, relevant integration checks and one final clean isolated Python 3.11 full suite. Bind interpreter/import/SDK/configuration to the candidate. No skip masking or unrelated frozen fixes. Subsequent source changes invalidate affected checks; docs-only changes need no duplicate suite.
3. Rebuild reports from unchanged inputs; verify hashes again. Include before/after Telegram and terminal examples, expiry counterexample, margin calculation and delayed-setting reconciliation. Preserve credential, funding, no-replay and exact-flat regressions.
4. Deliver a clean committed READY_FOR_REVIEW candidate, or WIP_NOT_ACCEPTED with remaining work and honest NOT_RUN checks. No activation/publication. Packet must contain base/HEAD/branch/worktree, changed files, A–F disposition, proved causes versus hypotheses, commands/results/evidence paths, unresolved facts/policy choices and live checks NOT_RUN.
5. Send the compact return report to auditor task `01a0c9cd-c8bc-75b0-92b3-04b75270a65e`, host `local`, using the task-message tool. Request independent review; include absolute packet path and candidate commit. Do not create another task or poll the auditor. Also leave a self-contained Chief final result. If delivery fails, preserve the packet and explicitly report delivery failure.

Dispatch status: ASSIGNED; implementation/test acceptance incomplete. No guaranteed counterparty, live fill-rate improvement or confirmed current inventory is claimed.
