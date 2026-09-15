# HCR-2 — Robinhood Chain and sequential variable slices (2026-09-15)

## Active owner authorization

Owner explicitly approved sequential liquidity-aware variable slices and requires https://robinhoodchain.lighter.xyz, with an early compatibility check before later owner-operated account testing. This supersedes HCR-1 single-attempt-only scope for a finite series, not its per-attempt invariants. Root remains sole Chief. One Builder may implement in an isolated codex/spread-v1-hood-rh-slices worktree; no second Chief. Builder owns hood_handoff source, its tests and dependency changes only; Chief owns governing documents and operator README and sole integration. Requested Builder Luna/max/Standard; report actual client evidence honestly. No other repository implementation may be copied; official venue SDK/docs are protocol references.

## Early compatibility result — target clarification required

Two public GETs completed and original responses preserved in spread-shadow-runs/hood-rh-20260915/compatibility-v1/. Official elliottech/lighter-agent-kit install documentation maps the owner website to https://api.rh.lighter.xyz. orderBooks and orderBookDetails agree: 57 perpetual and 27 spot markets, no HOOD symbol in either. Do not substitute a ticker. Owner has been asked which instrument on this deployment is intended. No Builder dispatched or production changes made; reserved worktree codex/spread-v1-hood-rh-slices remains at governing-contract commit 7bbbba2. Dependent implementation awaits the target answer; no background process or scheduled callback exists. Mainnet chain/signing identity still needs verification when a target is selected. Remaining public request allowance: 10, not a standing collection permission.

## Finite work and acceptance

1. Establish official website-to-API and signer/network identity for Robinhood deployment; distinguish EVM chain ID from Lighter signing domain. Never silently fall back to ordinary Lighter mainnet. Verify actual HOOD perpetual availability first; absent HOOD or unsupported signing semantics is a concrete blocker, not authority to substitute a market.
2. Add a simple opt-in sequential series over the existing two-account attempt. Operator supplies total quantity, desired slice quantity, allowed price deviation and existing exact prices/time bounds. Size adapts downward to fresh executable depth, rounds down to venue size steps, respects venue minima and handles an untradeable remainder explicitly. No invented production numerical defaults, randomization requirement, gross/fee caps or upward risk expansion. Sizes may naturally differ with depth. Long and short supported.
3. Only one child attempt at a time. Advance only after proven full paired child success and no remaining active orders; partial/unknown/conflicting results stop the series. Exact cumulative quantity never exceeds total. Durable parent/child journal bindings include venue and selection inputs. Crash/restart never automatically submits the next slice or replays mutations; reconcile-only remains safe. Do not add wallet orchestration or unrelated framework work.
4. Validate real public deployment compatibility early, then adverse offline integration (depth, minima, remainder, both sides, changing books, wrong venue, partials, unknown, restart). Required final clean isolated Python 3.11 full suite and independent Chief full-diff/evidence review before acceptance. Preserve artifacts and exact source/import identities in fresh ignored owner-only evidence space. Live account/signing/order execution remains NOT_RUN.

## Prospective public compatibility gate

Owner's requested early target verification authorizes up to 12 bounded public HTTP GET requests, each timeout <=20 seconds, to the official Robinhood Lighter API for deployment metadata, orderBooks/orderBookDetails and HOOD orderBookOrders only; no continuous collection. Save original responses and timestamps under spread-shadow-runs/hood-rh-20260915/. Website and official documentation reads are allowed separately. No credentials, account/private/fee/nonce endpoints, signer invocation, signed payloads or orders. Later owner-operated test awaits actual account/size setup; this development does not execute trades for the owner.

## Prior accepted deliverable (reference)

HCR-1 accepted implementation 74fbbbf9f298648b643ce9a69353e3abcdcf6c27; accepted main before HCR-2 34059cfea0c9fcf5e91707175dae6dbc5d6ac40a. Existing acceptance evidence remains immutable in STATUS and ignored evidence directories. HCR-2 is not accepted yet.
