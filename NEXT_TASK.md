# HCR-32 — Telegram account inspection

Owner requests a new bot command showing account balances and positions. Chief implements and self-reviews alone, without Builders. Base `70703ea3e1064846bcf836fcf02932d8c932161d`.

Add owner-only `/accounts` and read-only navigation, using the existing read-only SDK adapter and protected Keychain credentials. Show available balance, configured-market position and active-order count for both configured accounts, observation timestamps, and explicit unavailable/stale states. No claim of all-market flatness or total equity. Missing credentials never prompt or provision. Preserve launch policy, persistent barriers, configuration binding and credential containment. No bot deployment, real orders or live account queries in this implementation task.

Acceptance: adverse reader/auth/controller/HTML tests, relevant existing read-only adapter regressions, clean isolated Python 3.11 full suite, self-review and documentation, exact tested integration and verified remote main. Preserve historical evidence and private runtime state. Evidence: ignored owner-only `spread-shadow-runs/hood-telegram-accounts-20260922/`.
