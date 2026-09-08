# STOP — conditional-episode probe blocked by S1b cancellation accounting

## Current boundary, 2026-09-08

The owner-authorized single fixed offline conditional-episode probe has reached its explicit production-blocker stop. Do not resume the matrix, change production/tests, create Builder/agents, or start another diagnosis/campaign without a new owner decision. No active process or scheduled follow-up remains. No new policy, threshold, fill assumption, public data, account access, signing or trading activity is authorized.

## Unachieved finite objective and preserved result

The full objective was to enumerate the original 764 decision seconds before 765s on the immutable 900s pilot, run each of 317 valid conditional zero-inventory starts through each of four existing S1b alternatives, preserve all outcomes and the 135s tail, and report activation/fill/hedge/exit/defined closed-PnL/overlap. The complete frozen contract is in Git at `bfe616858316d23997df955de0876d1ea0ab4677:NEXT_TASK.md`; it is preserved history, not authority to bypass this blocker.

Only TRADE_THROUGH_ONLY/PRIMARY was attempted: 300 serialized UNRESOLVED episodes from seconds 1..718, then one engine exception at start 719. The 301 attempts include 155 known valid activations,27 known maker-filled episodes,18 full and 9 partial hedges of known entry quantity, no full closes. No closed-PnL distribution is defined. The other three alternatives and 16 remaining admissible PRIMARY starts are NOT_RUN_CORE_BLOCKER. All 3056 grid/alternative entries, including the exact omitted remainder, are preserved in owner-only all-starts.jsonl. This is a partial engineering-blocked prefix, not a full economic result or a zero-fill result for unrun alternatives.

## Exact blocker

Accepted implementation unchanged from the frozen pilot; analysis base `bfe616858316d23997df955de0876d1ea0ab4677`. Original pilot SHA256 ecf9ef41de6891bcc8fc75f03516651c28243f95e4b0692a6e2e9056b82fbc01. At start 719, actual record 41247 / collection+736.623425041s, `_s1b_entry_action_complete` (`s1b.py:1737`) changes COMPLETED entry-cancel:1 requested 0.00126 -> 0.001038 while executed stays 0.00126. Remaining -0.000222 violates CycleAction and aborts result construction. Known maker fill 0.000222 BTC and Lighter hedge 0.00022 BTC are retained separately; no valid terminal result is fabricated. The exact exception and amounts are reproduced through the original uninterned stream. A production correction is outside this diagnostic authorization.

Evidence root: `/Users/daniilmakarov/Desktop/RISEx Spread Shadow/spread-shadow-runs/scanner-v1-20260906/conditional-episodes-20260908/`. Start with report.md, results.json and blocker719.json; provenance, cache equivalence, immutable-prefix/source checks and the single diagnostic script are alongside. All original pilot/A–C reports remain immutable. Full suite NOT_RUN for unchanged code; no production fix accepted.

Sole diagnostic Chief `01a07f8b-131d-7c03-8e97-80dc4b08823a`, `[CHIEF] Conditional episodes — economic probe`; model/effort/speed/cost UNKNOWN. Branch `codex/spread-v1-conditional-episode-probe`, source worktree `/Users/daniilmakarov/.codex/worktrees/a2b0/RISEx Spread Shadow`. No current Builder or unaccepted production candidate. Further work requires an explicit owner decision; STOP.
