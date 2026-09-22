# HCR-36 — Clear contracts, lifecycle messages and execution economics

Owner requests a current, concise review of the five governing files and clearer Telegram/terminal lifecycle reporting with fees and post-trade PnL. Work alone; no Builders or independent-review claim. Accepted base: 699e0a68791aa0cbd409cb5e200c588a9d056a10. Branch: codex/spread-v1-hcr36-operator-clarity.

## Authorized work

- Inspect existing local cycle journals, especially the newest operator cycle; preserve original inputs. Read official venue documentation and the installed pinned SDK for economics fields. No agent-initiated trade, cancellation, new live account/market collection or trading-key access.
- Replace duplicated/superseded governance with one current contract. Keep execution policy and frozen-module boundaries; retain historical specifications in immutable Git history, not new governance files. Default to solo work unless the owner explicitly requests delegation.
- Show confirmed account/side/quantity, hold start/duration and planned closing start in concise Russian terminal/Telegram messages, using Moscow time. Send finite lifecycle notices during an operator-requested cycle; delivery cannot delay, cancel or replay trading. Preserve locks, private-owner command authorization, deduplication and restart barriers.
- Preserve actual per-trade fee evidence from already-required trade reads. Convert only established units; missing/ambiguous data stays UNKNOWN. Calculate closed execution gross PnL from exact fills, and net execution PnL only with complete fees. Include fallback fills and per-account totals. Funding-inclusive total stays separate/UNKNOWN without funding evidence. Economics cannot change execution success, flatness or risk gates.
- Integrate/publish the verified candidate and refresh the controller only when no cycle is active, preserving saved state and sending no run command.

## Acceptance

Document consistency and scope review; adverse receipt/fee/PnL tests with independently computed expected values; partial/unknown/open inventory never shown as closed profit; truthful hold/close times; transient progress without affecting launch/transport safety; terminal and Telegram rendering/integration tests; final clean isolated Python 3.11 full suite for accounting/evidence changes. Verify actual imports, original-input hashes and remote main. Store evidence under spread-shadow-runs/hood-operator-clarity-20260922/chief-v1/.

## Deferred

One LIMIT consumed by multiple MARKET orders is a future owner-requested objective, not part of this implementation. Existing series uses separate paired slices and does not establish that behavior. Minimum executable quantity/notional for each fragment matters; insufficient balance is not yet proven to be the only constraint. No size, hold, route, price, retry, margin, fee schedule, timing threshold, venue, funding strategy or public-book guard changes.

Status: IN_PROGRESS. Latest local cycle-011 proves a full own-account opening of 0.00023 BTC and historical flat inventory after external closing/recovery; complete-cycle strategy result is PARTIAL, not SUCCESS. Historical fees are missing; old journals cannot retroactively acquire new fee fields.
