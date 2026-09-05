# SCAN-001 — Deadline Capture and Run-Wide Trade Identity

Status: ACTIVE / ONE FRESH VISIBLE SPREAD BUILDER. Verification level A.

Objective: fix only two independently reproduced observer defects before implementing the simple scanner report. Base: the exact published main commit containing this task; the Chief supplies its SHA at Builder creation. Use one fresh codex/spread-scan-001-* branch/worktree from that exact SHA. Before edits report root, branch, HEAD, and clean status.

Required behavior:
- A horizon selects the latest current-session valid Lighter book actually received at or before its absolute detection-plus-horizon deadline, including any such event already waiting in ingress or currently being handled. No post-deadline book or interpolation is admitted. A scheduling yield is not a receipt/processing barrier. Use the smallest existing ingress/observer coordination change; bounded failure stays explicit, and capture tasks must not deadlock consumer/store/stop draining.
- Eligible-trade counting is unique by exact RISEx trade event key over the entire bounded run, including after quote replacement, short-history pruning, and reconnect. Retaining run-wide dedup identity must be bounded and fail closed on capacity. Identical retransmission cannot supply a second fill, counter increment, or volume; changed semantic content for the same identity fails closed. Receipt/session differences alone are not a new trade identity. Existing quote-local cumulative logic remains unchanged.
- Preserve old default economics, fees, grid, quote lifetime, strict/optimistic price rules, horizon set, public protocol acceptance, store representation/caps, and material-stop rules. No CAL or scanner configuration is added in this slice.

Allowed: src/risex_spread_shadow/runner.py, the minimal necessary ingress coordination in feed.py, and direct focused tests in tests/spread_shadow. No unrelated refactor, generic queue/event framework, governance edits, other repositories, legacy implementation, fee reader, network observation, credentials, or writes to main.

Required evidence: regression reproducing the old capture of notional 100 while a received pre-deadline book with notional 95 waits in ingress; late-book exclusion and exact-deadline inclusion; scheduling/slow-store/drain/close failure cases; duplicate key after version replacement/pruning/session changes; conflicting semantic duplicate; bounded dedup failure; focused/adverse tests; one clean isolated Python 3.11 full suite plus dependency, compile/import, surface, scope, diff, and Git checks on final candidate. Avoid repeated full suites for an unchanged tree. Report final SHA, changed evidence, limitations, and next action; never self-accept, merge, push main, or spawn agents.

Acceptance belongs only to Chief. CAL/HOLDOUT and all live/private operations remain closed after this slice.
