# Current status

## Accepted baseline and ownership

- Current accepted and published implementation `main` is `9ac7b73941b9f0217cfa6a2ef68b21d6040fd015` before the `DG-001` governance checkpoint.
- Transition preflight on `2026-09-02` independently verified local `main`, local `origin/main`, and GitHub `refs/heads/main` at `554b6c9c5e2b60eb13c9f33c6c2184e6932c84f0` (`Refine conservative PAPER economics gate`). That SHA remains the historical transition baseline; later accepted work is recorded below.
- The active project root is `/Users/daniilmakarov/Desktop/RISEx Spread Shadow`, created as a clean checkout of the existing `DaniilMakarov1/risex-funding-farmer` repository. This is not a new GitHub repository.
- The historical checkout `/Users/daniilmakarov/Desktop/risex-funding-farmer` contains a pre-existing modified `README.md`, six untracked operational artifacts, and old dirty worktree residues. They were not changed, accepted, or used as a base. Old unaccepted candidates are abandoned/rejected residue.
- One Chief owns `main`, governance, acceptance, push, and operational decisions. No old Chief or Builder task is active.
- During the `2026-09-03` acceptance preflight, one frozen legacy PAPER process was found under launchd label `com.risex.paper.chief49`, still holding its historical database. The Chief stopped it with `launchctl bootout` and disabled the label. The database and plist were preserved; subsequent process, label, and open-handle checks were clear. No unfinished venue write intent was found and no operational run is authorized.

## Product state

- RISEx Funding Farmer remains in the repository as a frozen legacy benchmark. Its funding-boundary profitability path and all old PAPER/testnet/mainnet operational work are frozen/replaced.
- RISEx Spread Shadow is the active public-only research contour.
- Active product question: does a repeatable positive fee-adjusted execution edge exist when each hypothetical RISEx maker fill is evaluated against delayed executable exact-q Lighter Standard taker hedge books?
- Primary horizons are `0/300/500/1000 ms`; the whole curve is reported. `500 ms` is diagnostic, not presumed actual end-to-end latency.
- Points equal `$0`. Funding is diagnostic and separate. Future maker fills, maker exits, and basis convergence are not recognized as earned entry income.
- Private endpoints, credentials, signing, order preparation, dispatch, testnet/mainnet writes, real funds, transfers, withdrawals, and strategy execution remain prohibited.

## Current implementation state

- SS-001A is independently accepted at `f7251387f0ffe684ed5f3d61f4ea4601b6156183` and is part of `main`. It contains only the pure `risex_spread_shadow` economics/evidence contracts and focused tests.
- Chief acceptance evidence on the exact committed tree: 35 focused tests passed; an isolated Python 3.11 full suite passed `3694` tests with `3` skipped; dependency, compile, import/surface, diff, scope, and Git checks passed. Production size is `2289` lines, within the frozen `2300`-line upper bound.
- SS-001B is independently accepted and published at `9ac7b73941b9f0217cfa6a2ef68b21d6040fd015`. Final evidence includes `54` focused tests, an isolated Python 3.11 full suite of `3713 passed, 3 skipped`, clean dependency/import/diff/Git checks, and `2696` new production lines against the `3200` cap.
- The Chief's exact-SHA public-only BTC smoke used fresh run `5AuNTOVaN842jMZZ0Oss87ov`: `95` RISEx books, `254` Lighter books, `3` RISEx trades, `8352` active quote records, no fatal or real transport/schema gap, deterministic offline report, clean shutdown, and owner-only `0700/0600` storage. No natural strict fill occurred in that 15-second acceptance smoke; deterministic tests prove the event-driven four-horizon path without lookahead.
- External governance review accepted the transition. Before any SS-001A candidate acceptance, target-margin formulas, deterministic quantity sizing, conservative/optimistic fillability interpretation, exact hedge-failure outcomes, and the bounded SS-001A/SS-001B complexity limits were frozen in System Specification 2.1.
- The prospective real-public discovery gate `DG-001` is frozen in System Specification 2.3 before any discovery sample. It fixes the `BTC/ETH/SOL` universe, `25 s` freshness, fee inputs/provenance, numeric fillability and edge thresholds, completeness/fatal rules, a `60 s`/`250,000`-record/first-`50`-strict-evidence bound, and exact seven-verdict precedence.
- The only active task is the single bounded `DG-001` observational discovery run and terminal Entry Viability verdict. No implementation slice is open.
- SS-001A candidate `a3ac545a78a687788595470c1e1e1e91a501ec74` was independently rejected and was not merged. Its green tests did not cover fail-open missing economics, invalid cross-clock exchange-monotonic ordering, missing explicit would-fill detection time, or recovery-generation displacement. The original Builder is released; correction requires a fresh visible Spread Builder from current accepted `main`.
- Correction candidate `7b3e370865de28bc4a4566445c676772dc0727cc` was also independently rejected and not merged. It failed a required repeating-decimal multi-level exact-q case, accepted sizing evidence with the wrong direction/target notional, and lacked venue identity on gap evidence. System Specification 2.2 freezes these adverse contracts before the next fresh candidate.
- The rejected candidates remain historical non-authoritative evidence and were not merged. `SS-002` and `SS-003` remain closed.
- The first SS-001B Builder attempt from `2d67811757994deef8523ae384c5e09cd1b2502f` was formally rejected and released without a commit. Its unaccepted worktree duplicated implementation paths, reached about 4,600 new production lines, and produced no final tests or smoke evidence; it remains non-authoritative history.

## Current Chief mission

- The Entry Viability Stage is not complete when SS-001B and its smoke are complete. Those are intermediate technical milestones.
- The mission ends only after: accepted/integrated minimal SS-001B; successful bounded public-only pipeline smoke; a separately frozen pre-discovery gate covering universe, freshness, public fee inputs, fillability thresholds, completeness, stop rules, and verdict rules; bounded real-public discovery; and one evidence-backed Entry Viability product verdict.
- The terminal verdict is one of `ENTRY_EDGE_CANDIDATE`, `NO_SNAPSHOT_EDGE`, `LATENCY_DESTROYS_EDGE`, `PROFITABLE_QUOTES_UNFILLABLE`, `FILLABILITY_INSUFFICIENT_EVIDENCE`, `LIGHTER_DEPTH_UNSUITABLE`, or `DATA_INSUFFICIENT`.
- `DG-001` discovery is now authorized exactly as frozen. `SS-002` remains closed until the product verdict and may be proposed only after `ENTRY_EDGE_CANDIDATE` satisfies that frozen gate.
