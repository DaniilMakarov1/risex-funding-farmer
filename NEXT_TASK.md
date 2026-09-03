# Active bounded task

## SS-001I — Open-Ended Gap Replay Parity

Status: `AUTHORIZED / BUILDER NOT YET OPENED`.

Objective: correct the proven offline null-ended-gap overlap defect so deterministic replay agrees with the accepted online interval/identity semantics and the immutable DG-007 evidence can receive its frozen verdict.

Exact base: accepted published `main` after this governance record. Create one fresh visible Spread Builder and worktree from that exact base.

Allowed: change only offline report gap-overlap handling and focused tests. A null-ended gap is open from its recorded start and never overlaps evidence completed before that start; matching venue/market/session/recovery and later interval overlap remain mandatory and fail closed where evidence is genuinely malformed or incomplete.

Acceptance: adverse null-ended and finite gap boundaries; exact identity mismatches; mixed same-policy recovery generations; unchanged raw counts/horizons; two byte-identical reports for each immutable DG-006 and DG-007 store; focused tests and one fresh isolated Python 3.11 full suite. Corrected DG-007 stop-policy validity must agree with its online `10`-episode/`10`-timestamp signal or the gate remains insufficient.

Forbidden: collection/runtime changes, economics, fees, quote construction, fill models, eligibility, online stop logic, horizons, queues, storage/caps/timeouts, protocol or venue behavior, any new sample, private/auth/credential/signing/order preparation/dispatch/testnet/mainnet/write activity, strategy, `SS-002`, or `SS-003`.

After candidate delivery, Chief independently reviews and alone accepts/integrates, then applies the already-frozen DG-007 verdict to the unchanged immutable evidence. Do not collect another sample.
