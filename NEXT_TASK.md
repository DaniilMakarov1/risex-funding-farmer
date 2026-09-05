# SCAN-005 — Prospective CAL-001 public scanner gate

Status: OPEN FOR ONE PROSPECTIVE CAL-001 ATTEMPT ONLY IN THE WINDOW BELOW.
Venue: central SPREAD, public RISEx maker SELL / Lighter Standard exact-q taker BUY.
Objective: obtain the first bounded real-public scanner result; distinguish conditional positive entry edge, measured negative economics, failed thresholds and insufficient evidence. The owner on 2026-09-05 explicitly instructed Chief to continue to a working scanner and an honest assessment. No Builder candidate is open.
Verification: accepted level-A implementation evidence (3915 passed, 3 skipped) plus SCAN-004 independent readiness; this gate adds only public unauthenticated observation.

## Prospective identity and launch

- Exact accepted implementation for CAL and any later HOLDOUT: `b8e9415e648543fd3435bc683eb649934c9dd0d5`.
- Exact clean loaded checkout: `/Users/daniilmakarov/.codex/worktrees/scan-003-release/RISEx Spread Shadow`. Chief independently validated it with isolated Python 3.11.5 and explicit PYTHONPATH to this checkout's src before publishing this gate. Later governance commits do not change this release.
- Stable policy fingerprint: `4bcc87dac8498b5280eb084e0351fd4be5fb45d898be108a1f6c88bee5a85734`.
- Single absolute durable store root for both stages: `/Users/daniilmakarov/Desktop/RISEx Spread Shadow/spread-shadow-runs`. Root and both claims were absent at preflight; no prior stage exists. Runtime evidence remains local and outside Git. Never delete claims or substitute another root.
- CAL-001 UTC launch window: **2026-09-05T11:26:00Z inclusive to 2026-09-05T11:31:00Z exclusive** (14:26–14:31 Europe/Moscow). This is the launch window, not the terminal-drain deadline. Publish this gate on clean main before any market request. If publication or launch misses it, record MISSED and stop; no replacement window.
- At launch recheck exact clean loaded release, unused claims, no concurrent observer, owner-only storage and at least 24 GiB free. Pre-publication free space was approximately 514 GiB. The scanner durably reserves its create-once claim before its first request; a failed or missed attempt consumes the stage. No manual reserve or probe request is needed.

## Immutable collection and evaluation

- BTC only; RISEx SELL / Lighter BUY; target $100; both nominal 1/2 bps arms; deterministic exact common-grid q and actual tick-aligned maker prices. RISEx maker rate 0.0001 (tier 1, recorded SS-001Q); Lighter Standard taker rate 0, published latency 300 ms, recorded OFFICIAL_LIGHTER_ACCOUNT_TYPES_2026-09-05. No fee-reader refresh.
- Horizons 0/300/500/1000 ms; quote lifetime 5 seconds; freshness 25 seconds; received-before-deadline books only. Points, funding and future-exit income are zero/excluded.
- Stop at the first of 250 unique eligible BTC trade keys, 1200 seconds from sample start, 1,000,000 records, 4 GiB, or fatal/integrity/completeness failure. Record/byte caps are failures. No fill-count stop, manual extension, threshold tuning or replay. Drain only already-pending horizons under the unchanged 2.2-second horizon-tail allowance and accepted shutdown behavior.
- Apply every formula, evidence floor, concentration limit, effective-level pairing rule, exact-q completeness requirement, edge/markout threshold, positive-sum condition, selector and failure precedence in SYSTEM_SPEC sections 0.19–0.20, encoded by the fingerprint above, without alteration. In particular: 50 common eligible units; 20 clean filled units and clusters per arm; 15 detection timestamps; 20 paired units; at least 16/80% distinct wider levels; at most 4/20% collisions; no reversed/unresolved units; full hedges at all horizons; one-minute/five-minute concentration at most 25%/50%.
- Retain all raw, contaminated, inactive and unresolved evidence. No exclusion to improve the denominator. An early trade stop may fail concentration; that is an honest insufficient-evidence result, not permission to extend.

## Acceptance, failure and next action

Chief launches the accepted scan CLI once in the window, retains claim and evidence, independently checks exact run/release/policy/stage identities, first stop, physical terminal/index integrity, file permissions/caps, and two byte-identical canonical offline reports. Inspect coverage, both arms, full edge/markout curves, negative tails, sums and every failed gate. Runtime exit code alone is not acceptance.

A valid measured negative result is NEGATIVE; positive scores below frozen thresholds are NOT_CONFIRMED; invalid, capped, missed, contaminated or insufficiently measurable evidence is INSUFFICIENT. Any nonpassing CAL stops this configuration, leaves HOLDOUT closed, and is not repeated or replaced. A concrete measurement defect is reported as such, never as evidence of bad venue economics; any authorized correction requires a fresh visible GPT-5.6 Luna max Builder, with no automatic new economic interval.

Only CAL_PASS_PROVISIONAL permits Chief to commit and publish the CAL evidence identity, report and selected arm, then separately freeze a later untouched HOLDOUT window using the exact same release/root/policy. No HOLDOUT window is supplied here. Only both passing stages selecting the same arm can support the conditional public entry-edge candidate label. This is not real queue position, executable PnL, full-cycle profitability or cross-regime proof.

Forbidden: private/credential/fee-reader access; signing, order preparation, orders or trading; paper trader; other repositories; new venues, strategies, dashboards, services or frameworks. Chief writes no implementation code. Trading bots remain a later separately authorized decision.
