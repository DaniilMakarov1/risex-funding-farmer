# SS-001J — Effective-Level and Cluster-Aware Calibration Evidence

Status: `ACTIVE R2 AFTER REJECTION / ONE FRESH SPREAD BUILDER`.

Objective: extend only the deterministic Spread offline report and focused tests so calibration evidence is based on distinct actual maker-price levels and official venue taker-order clusters, while preserving every existing canonical field and the frozen DG-006/DG-007 verdicts.

Exact base: published `main` SHA `3acc8df31c15c58760c3630a6f025e791b815425`. Verification level: A. The rejected `codex/spread-effective-level-clusters` worktree and its uncommitted residue are forbidden inputs.

Allowed scope: Spread report code and directly focused tests only. Add the separately labelled effective-level/cluster calibration section specified in System Specification 0.19, including actual-price collisions, signed tick separation, paired cross-arm cluster attribution, repeated quote versions, descriptive rates/concentration, and complete distinct-wider-level horizon curves.

R2 implementation boundary: prefer one narrowly named offline-report helper that accepts already validated record dictionaries plus a minimal call from `report.py`; do not duplicate canonical book reconstruction, gap/episode validity, or verdict logic. Keep new production code at or below `800` nonblank lines and fail closed instead of adding fallback identity/session heuristics. If the complete contract cannot fit without a generic framework or duplicated canonical accounting, stop with the exact conflict rather than growing the candidate.

Forbidden scope: collection, feeds, runtime quote construction, economics, fill definitions, eligibility, online stops, storage format, queues/caps/timeouts, protocol acceptance, markets, configured fees, credentials, authenticated access, private endpoints, signing, orders, positions, execution, `SS-002`, `SS-003`, `CAL-001`, and `HOLDOUT-001`. Do not collect a new sample or alter immutable evidence.

Acceptance: adversarial collision/direction/repetition/dependence/identity/determinism tests; exact deterministic DG-006/DG-007 replay with all prior fields and verdicts preserved; known DG-007 BTC `$100` sell/buy `1/2 bps` effective-level results explained from venue identity rather than time proximity; one fresh isolated Python 3.11 full suite; clean dependency, compile/import, diff, scope, private/write-surface, worktree, and Git checks. Builder must commit one candidate and report exact SHA and evidence; Builder does not self-accept or merge/push `main`.
