# HCR-33 — Random first account and order side

Owner explicitly requests randomization of which configured account places the first limit order and its side. Chief implements and self-reviews alone. Apply to the normal `simple` launcher (`./start` and Telegram `/run`): independently uniform choice of the two configured accounts as source and BUY/SELL for its first limit order, once per newly confirmed cycle. Receiver is the other account and its leg is opposite. Preserve size/hold policies, thresholds, fees, margin checks, reconciliation and no-replay behavior. Low-level explicit-plan interfaces retain their specified identities/direction.

Persist selection with the immutable slot reservation before credentials/client construction or venue requests. Bind admission to that exact selection and retain it through retries, paired close and residual reconciliation. Expose actual selected roles/side in operator output and saved Telegram reports; do not confuse configured account labels with selected roles. No selection redraw to obtain a favorable preflight or retry an admitted slot.

Acceptance: deterministic all-four-combinations execution tests, adverse persistence/binding/replay checks, relevant report/controller regressions and final isolated Python 3.11 full suite; preserve original evidence. Record self-review, integrate tested source and verify remote main. Owner has authorized software behavior, not a test trade. No synthetic `/run`, live account query or order during verification. Existing controller remains under operator ownership; refresh it after integration only if no active cycle/child, preserving state. Evidence: ignored owner-only `spread-shadow-runs/hood-random-route-20260922/`.

## Result

Candidate `99b35ab40b300280d4f4fe89d08b8e70e06b4d19` accepted after self-review and 281 affected checks. Final clean isolated Python 3.11.5 full suite: 4873 passed, 3 existing optional Extended dependency skips, exit 0, 141.08 seconds. Four first-account/side combinations tested through full paired execution and closure; persistence and admission failures block access. Original 42 evidence hashes preserved. Integrate exact tested implementation and refresh the idle controller without synthetic trading commands.

Integrated and published; idle controller refreshed to PID 78410. Process, exclusive lock and Telegram HTTPS connection verified, startup log empty. No test trades or synthetic commands. Controller remains under operator command ownership; assignment complete.
