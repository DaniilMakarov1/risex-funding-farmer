# HCR-32 — Telegram account inspection

Owner requests a new bot command showing account balances and positions. Chief implements and self-reviews alone, without Builders. Base `70703ea3e1064846bcf836fcf02932d8c932161d`.

Add owner-only `/accounts` and read-only navigation, using the existing read-only SDK adapter and protected Keychain credentials. Show available balance, configured-market position and active-order count for both configured accounts, observation timestamps, and explicit unavailable/stale states. No claim of all-market flatness or total equity. Missing credentials never prompt or provision. Preserve launch policy, persistent barriers, configuration binding and credential containment. No bot deployment, real orders or live account queries in this implementation task.

Acceptance: adverse reader/auth/controller/HTML tests, relevant existing read-only adapter regressions, clean isolated Python 3.11 full suite, self-review and documentation, exact tested integration and verified remote main. Preserve historical evidence and private runtime state. Evidence: ignored owner-only `spread-shadow-runs/hood-telegram-accounts-20260922/`.

## Result

Completed in candidate `e580369e0c4b439b5d556efd657521e3279e7754`. 276 affected checks passed. Final isolated Python 3.11.5 suite: 4849 passed, 3 existing optional Extended dependency skips, exit 0, 130.43 seconds. Full diff self-reviewed; exact source imports, 42 historical hashes and unchanged private runtime state verified. Integrate exact tested implementation and verify remote main. No deployment, live reads or execution performed; no continuing campaign assigned.

## Owner-authorized controller startup

The owner explicitly requested a restart after HCR-32 delivery. No existing controller/runner process was found; the controller lock is free and saved active/last are null. Start the accepted updated controller with the existing bound owner and configuration, preserve state and credentials, and verify process/lock/network health. This is controller deployment only: do not send `/run`, synthesize owner commands, query trading accounts or place orders during startup verification. Subsequent operator commands retain existing authorization and safety gates.
