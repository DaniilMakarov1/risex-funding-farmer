# SCAN-004 — Chief Handoff and Prospective Readiness Gate

Status: CHECKPOINT / NEW CHIEF READINESS ONLY. No Builder slice or market observation is open.

Venue: central SPREAD (RISEx maker / Lighter Standard exact-q taker hedge).
Objective: take over the accepted simple scanner, independently decide whether its first bounded public calibration can be opened, and avoid further infrastructure expansion.
Exact implementation base: `b8e9415e648543fd3435bc683eb649934c9dd0d5`; the published main handoff commit adds governance/operator documentation only. Use Git to verify the exact published checkpoint before action.
Verification level: A for this readiness task. The implementation passed `3915 passed, 3 skipped` in isolated Python 3.11, with Chief independently passing the 22 new focused tests. Reuse evidence for the unchanged source tree.

Required next action:

1. Read AGENTS.md, SYSTEM_SPEC.md (especially 0.17–0.20), STATUS.md, NEXT_TASK.md and README.md in full. Verify clean published main, accepted source SHA and absence of an active Builder/candidate or operational run. The predecessor has handed over Chief authority; do not create an Architect or reuse old candidates.
2. Independently review only remaining operational readiness: exact clean loaded release, one durable store root shared by both stages, unused CAL/HOLDOUT claims, fixed fees/configuration and first-stop behavior, bounded storage, actual source-to-report path, and feasibility of the frozen evidence floors. No fee-reader or new data is needed for this review. State any concrete blocker; do not infer profitability from fixture passes.
3. Before any public request, publish a separate prospective operational gate naming the exact accepted release, fixed store root, CAL-001 start window, immutable parameters/stops and failure interpretation. The present task supplies no window and authorizes no observation. Use an exact clean checkout of the accepted implementation release for both stages; later governance commits must not silently change their policy fingerprint. The start window governs launch time, not the terminal drain time.
4. If CAL is later opened and passes, prospectively publish HOLDOUT's separate later start window, bind the exact CAL reference and use the same release/root/policy. Only the unchanged two-stage contract may produce a conditional public entry-edge candidate. A failed, missed, capped or unmeasurable interval is not repeated or replaced; no threshold or arm-grid tuning from new observations.

Allowed now: read-only review and Chief-owned updates to the five governing files establishing a concrete readiness decision. If a specific code defect blocks measurement, open one narrow slice with a fresh visible GPT-5.6 Luna `max` Spread Builder and exact accepted main base; Chief never implements. No speculative cleanup or framework work.

Forbidden: public sampling before the separately published gate; private/credential/fee-reader access; signing/orders/trading; paper trader; other repositories; dashboards/services/frameworks; changing thresholds or historical results; retrying stages via another store root or deleting claims.

Acceptance/evidence: concise readiness verdict with exact SHA, named residual limits and the next concrete action. A public-data screen cannot establish real queue position, executable PnL, full-cycle profitability or cross-regime reproducibility. Stop the current configuration honestly on failed economics or insufficient measurability. No positive or negative current market result exists at this checkpoint.
