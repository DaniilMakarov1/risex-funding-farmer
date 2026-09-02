# Current status

## Accepted baseline and ownership

- Transition preflight on `2026-09-02` independently verified local `main`, local `origin/main`, and GitHub `refs/heads/main` at `554b6c9c5e2b60eb13c9f33c6c2184e6932c84f0` (`Refine conservative PAPER economics gate`). The published commit containing this status is the current accepted governance tip.
- The active project root is `/Users/daniilmakarov/Desktop/RISEx Spread Shadow`, created as a clean checkout of the existing `DaniilMakarov1/risex-funding-farmer` repository. This is not a new GitHub repository.
- The historical checkout `/Users/daniilmakarov/Desktop/risex-funding-farmer` contains a pre-existing modified `README.md`, six untracked operational artifacts, and old dirty worktree residues. They were not changed, accepted, or used as a base. Old unaccepted candidates are abandoned/rejected residue.
- One Chief owns `main`, governance, acceptance, push, and operational decisions. No old Chief or Builder task is active.
- No active PAPER, testnet, mainnet, RISEx, Lighter, Nado, or Extended project process or launchd label was found. No active database handle or unfinished live write intent was found. No operational run is authorized.

## Product state

- RISEx Funding Farmer remains in the repository as a frozen legacy benchmark. Its funding-boundary profitability path and all old PAPER/testnet/mainnet operational work are frozen/replaced.
- RISEx Spread Shadow is the active public-only research contour.
- Active product question: does a repeatable positive fee-adjusted execution edge exist when each hypothetical RISEx maker fill is evaluated against delayed executable exact-q Lighter Standard taker hedge books?
- Primary horizons are `0/300/500/1000 ms`; the whole curve is reported. `500 ms` is diagnostic, not presumed actual end-to-end latency.
- Points equal `$0`. Funding is diagnostic and separate. Future maker fills, maker exits, and basis convergence are not recognized as earned entry income.
- Private endpoints, credentials, signing, order preparation, dispatch, testnet/mainnet writes, real funds, transfers, withdrawals, and strategy execution remain prohibited.

## Current implementation state

- No `risex_spread_shadow` implementation has been accepted yet.
- External governance review accepted the transition. Before any SS-001A candidate acceptance, target-margin formulas, deterministic quantity sizing, conservative/optimistic fillability interpretation, exact hedge-failure outcomes, and the bounded SS-001A/SS-001B complexity limits were frozen in System Specification 2.1.
- “Approximately zero” and “materially positive” fillability thresholds are deliberately not hidden in SS-001A; SS-001B must freeze numeric thresholds before its first discovery sample.
- The only authorized implementation slice is `SS-001A`, defined in `NEXT_TASK.md`.
- SS-001A candidate `a3ac545a78a687788595470c1e1e1e91a501ec74` was independently rejected and was not merged. Its green tests did not cover fail-open missing economics, invalid cross-clock exchange-monotonic ordering, missing explicit would-fill detection time, or recovery-generation displacement. The original Builder is released; correction requires a fresh visible Spread Builder from current accepted `main`.
- `SS-001B`, discovery runs, `SS-002`, and `SS-003` are closed until their preceding acceptance and explicit authorization gates.
