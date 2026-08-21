# PAPER-007-FIX-008 — Final corrective cycle

Reproduce the production sequence in which a successful Extended universe and filtered refresh adds five Extended observations after the initial Nado-only scan, followed by a `FULL` scan that raised `AssertionError` at `2026-08-21T13:30:17.221171Z`.

Fix only the invalid runtime/scanner state transition. The next FULL must fail closed per route rather than terminate, persist all 20 authoritative rows when both hedge venues are available, and keep catalog, heartbeat, funding, economics, scheduling, and paper/live semantics unchanged. Add a deterministic production-call-graph regression that fails on candidate `e7d6f26` and passes after the correction. Run focused tests, full pytest, compileall, diff-check, and secret scan. This is the second and final permitted FIX cycle; no Stage B restart before Architect acceptance.
