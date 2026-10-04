// Round 3: the two lanes the OOM round lost. Forks, serialized gates, rigid single-item scope.
const CONTRACT = `
CONTRACT (binding):
- Repo /home/asdf/repos/rust/townframe-3. Read AGENTS.md first; follow it. NEVER run git or any jj mutation (read-only jj log/diff/st allowed).
- You are a fork: your inherited context contains a LOT of session state, including OTHER pending fixes. DO NOT orchestrate: do not verify other lanes, do not run cross-lane sweeps, do not act on other pending work you remember seeing. Your task below is your ENTIRE job; anything else you notice gets at most one line in your report ("saw X, left it").
- Do not touch examples/parent_historical_jwk_smoke.rs, docs/, Cargo.toml, or any file not named in your task.
- Working copy holds uncommitted work from parent + operator + earlier lanes. Do not commit or amend. Write only your named files.
- BUILD REGIME: every cargo invocation runs as flock /tmp/townframe-cargo-validation.lock cargo <rest> with CARGO_TARGET_DIR=/run/media/asdf/p3N/tmp/townframe-target exported. Builds serialize machine-wide; queueing is correct. Never run two cargo processes concurrently. No lane-suffixed target dirs.
- On ENOSPC while df <10GiB on /run/media/asdf/p3N: reply to the supervisor; never delete any target dir.
- Gates: flock-locked cargo clippy -p daybook_core --all-targets must end with 0 ERRORS (warnings in YOUR files fixed by you; warnings elsewhere left) + the named narrow nextest filter under the lock. FORBIDDEN: full-suite sweeps, unfiltered nextest, cargo test.
- One command, one check; re-check with a different-shaped single command. No sleep-poll loops; one bounded sleep inside a single bash call is the cap.
- Tests: no skip, no sleeps beyond what the task names, timeout exceptions only as named.
- Report exactly which commands you ran and their results. Ask product questions instead of deciding them.
`;

const faults = await runs.run("faults-scoped-3", { agent: "worker", context: "fork", maxRuntimeMs: 45 * 60 * 1000, task: CONTRACT + `
TASK (lane E3 — the ONLY item: scope the encryption worker's fault flags to the machine). Your only files: src/daybook_core/blobs/encryption_worker.rs and src/daybook_core/blobs/encryption_worker/tests.rs.
CURRENT ON-DISK STATE (read it fresh): the process-global statics faults::FAIL_RECONCILE / ATTEMPTS / FAIL_ROTATE / FAIL_ROTATE_AFTER_INSTALL remain at encryption_worker.rs:92–119 (a prior lane's run of this task never happened). PRESENCE_RESOLVES (:119) may be production-shape already — check whether it is a test fault or an observable; only the four fault flags + ATTEMPTS are in scope for scoping; leave PRESENCE_RESOLVES as it stands unless it is itself one of the cross-test-infecting statics — if it is, include it and say so.
DO (and nothing else):
1. Replace the four global statics with a per-machine faults state (an Arc'd struct built with the machine/Ctx the tests construct — follow exactly how these tests construct Ctx; if the flags are READ in production code paths, stop and report before changing anything).
2. Keep the test-facing semantics identical: flip-before-act, observe attempts — existing fault tests must keep their meaning with no weakening.
3. Update every touch site in worker.rs and its tests.rs; no vestigial statics, no underscore seams, no backcompat.
4. Add ONE narrow test proving two machines/Ctxs no longer interfere (faults armed on one while the other rotates normally).
Gates under the lock: clippy (0 errors) + nextest -E 'test(blobs::encryption_worker)'.
Report exactly which commands ran + results; ask product questions instead of deciding them.
` });

const w = await runs.run("release-order-spawnleak-3", { agent: "worker", context: "fork", maxRuntimeMs: 45 * 60 * 1000, task: CONTRACT + `
TASK (lane W3 — TWO approved fixes, both previously operator-approved; do these and nothing else).
(A) Release-order fix in YOUR file src/daybook_core/blobs/pin_worker.rs (read fresh): locate the inventory-diff flow where the pin removal for the encrypted-representation inventory is written BEFORE release_pairs runs (release_pairs ~pin_worker.rs:505-560; find its caller — the apply/reconcile step that removes inventory pin rows). Today: removal write first, then release_pairs — a failure or crash between them leaves ct:/pt: GC-root pairs with no owner, forever. Reorder: release_pairs FIRST, then the inventory removal write. Adjust doc comments on both sides (state the new order + why: released-pair-on-crash is harmless, orphaned-pair-on-crash is permanent). Do not weaken any test; update tests pinning the old order instead. W1 regression test: with release made to fail (existing fault hooks in pin_worker, or the narrowest injectable point in that path), the inventory removal must NOT be applied and the pair tags stay intact — if there is genuinely no injectable point, report it rather than inventing a new fault channel.
(B) Spawn-leak fix in YOUR file src/daybook_core/blobs/encryption_worker.rs (read fresh — read-only jj diff is fine): the boot spawn chain (spawn_blob_pins_part_worker, spawn_blob_pin_worker, then spawn_blob_encryption_worker; all called from the boot path in rt.rs or repo.rs — grep the caller chain) starts earlier tasks on child cancel tokens; if a LATER spawn returns Err the earlier tasks leak with live channels. The operator approved fixing this with the repo's existing AbortableJoinSet machinery (utils_rs::AbortableJoinSet — see big_repo/test2/edge.rs:1150 usage as the pattern; if the pin/encryption workers are not spawned through a task-set today, follow the nearest established spawn pattern that supports abort-on-failure — reuse, do not invent). Every earlier-started task must be cancelled/aborted on any failure return from the chain. Regression test: force a late failure (fail spawn_blob_encryption_worker via its arg surface — e.g. an invalid arg or a fault hook if one exists) and assert no earlier worker task survived, observable through the same channels/state existing tests use.
Both fixes end with the same gates: clippy (0 errors) + flock-locked nextest -E 'test(blobs::encryption_worker) or test(blobs::pin_worker)'.
NOTE: a previous lane already landed the download.rs read_meta rs validation — it is DONE, it is not yours, do not touch download.rs at all. Also NOTE: another lane (E3) is scoping the fault statics in encryption_worker.rs at the same time as you — wait for it to COMPLETE before touching encryption_worker.rs, and when you do, re-read the file fresh so you edit on top of its landed state; sequence your (A) work on pin_worker.rs first while E3 runs.
Report exactly which commands ran + results; ask product questions instead of deciding them.
` });

return { faults: faults.output, workerFixes: w.output };