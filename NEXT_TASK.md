# HCR-20 — reliable runtime selection before cycle-slot claim

## Active owner-authorized objective

On 2026-09-20 the owner reported that the new `./start` claimed `cycle-002` and then stopped with `cycle preflight blocked: sdk_error`. Diagnose and correct this launch defect, preserve all consumed evidence, independently verify the correction, install it in the operator checkout and publish it. This is offline software work; no credential, Keychain, private endpoint, account read, order, cancellation or live validation is authorized.

## Proven cause and immutable evidence

Published `./start` invoked system `/opt/homebrew/bin/python3`, which has no `lighter` module. The prepared project `.venv-hood/bin/python` exists and its installed `lighter-sdk` distribution is exactly 1.1.2. Consumed `operator-v1/cycle-002` contains only `CYCLE_STARTED` followed by `CYCLE_PREFLIGHT_BLOCKED: sdk_error`; it has no first-mutation boundary or dispatch intent. It is immutable and non-reusable. Current account state remains NOT_READ.

## Ownership

Sole Chief task `01a0aafe-23fa-7c90-87a6-e807b3f6450a` owns contract, review, integration and publication. One fresh GPT-5.6 Luna max Builder owns production changes and tests in isolated branch `codex/spread-v1-runtime-launch`. The Builder must not edit governing documents, merge or push main. There is no fixed Builder concurrency limit; every new finite objective still requires a fresh Builder, and parallel writers require non-overlapping ownership.

## Required behavior

- Repository-root `./start` must execute the prepared project `.venv-hood/bin/python` when it is valid, independent of the user's PATH or active shell environment. A missing/non-executable environment must produce a short actionable Russian error and create no cycle slot.
- The simple launcher must validate the required `lighter-sdk==1.1.2` distribution/import locally before allocating a cycle slot. Direct module invocation must receive the same protection. Missing/wrong/broken SDK must report the specific local setup cause rather than generic `sdk_error`, access no Keychain/network/account, and create no slot.
- Validate local configuration and market-evidence readability before slot allocation. Local deterministic setup failures must not consume a cycle number.
- Keep confirmation offline and credential-free. After Enter, successful local runtime validation may proceed to the existing protected credential/client/cycle path. Preserve the exact first-mutation/no-replay boundary and never imply that a claimed slot can be reused.
- Replace the misleading `Новый слот занят` success phrase with clear Russian wording that a new slot was created and reserved. Error output must distinguish local setup failure before slot allocation from an execution failure in a claimed slot.
- Preserve all HCR-19 repricing/retry behavior, existing policies, legacy CLI compatibility and all consumed cycle directories. The next real launch must choose a fresh slot after cycle-002; never repair by deleting or reusing cycle-002.

## Acceptance

Builder tests PATH-independent `.venv-hood` selection, missing/non-executable environment, direct-simple missing/wrong/broken SDK, invalid local evidence/config, no Keychain/client/read/slot on those failures, successful allocation after validation, Russian messages and legacy compatibility. Chief reviews the complete diff, independently replays the exact system-Python failure and verifies `.venv-hood` success, then runs the final clean isolated Python 3.11 full suite. Installation verification runs only help/cancel and synthetic/offline runtime checks without consuming cycle-003. No live validation.
