# Autonomous daybook_core / big_repo greening

## Contract

Final gates: `cargo nextest run -p daybook_core -p big_repo --all-features`; `cargo clippy -p daybook_core -p big_repo --all-targets --all-features -- -D warnings`; `RUST_LOG_TEST=debug cargo nextest run -p daybook_core -p big_repo --all-features --fail-fast --stress-duration 60m` with unique redirected log. All must exit 0. No pushes or git commands. jj fetch/rebase and local Cargo dependencies authorized. Ordinary design decisions are autonomous. No suppressed failures, skips, or greenwashing; faulty tests may be corrected with mechanism evidence and reviewable justification, as a last resort.

Attempt cap: 100 substantive fix-plus-verification cycles; completed cycles: 6 (recovery-fixture prerequisites; live owner prekey secrets; freeze-safe stall observation; joined writer before exact resume; exact branch group-grant ordering witness; protecting nextest executables from disk-watch cleanup). Upstream mechanical rebases/API migrations and lint-only edits excluded. Stop on success, user stop, or cap.

After compaction re-read the auto-loaded AGENTS.md, README.md, CONTRIBUTING.md, fork-pinning-and-upstream-sync and stress-sync-investigation skills. Keep findings, intent, evidence, exact commands, unresolved hypotheses and cycle count here or in linked documents. Historical notes are not proof about current code.

## Initial state (2026-10-02)

Townframe @ `svtxyutu` / `01195e8c`, parent `8c6e8773`; only existing working-copy change: `.config/nextest.toml`. User already reduced long_af_test timeout to 8 x 60s; preserve that edit. long_test cap is 4 x 60s, default cap 2 x 60s.

Keyhive @ `wzxlqwrx` / `7914e224`, parent `28a53997` (typed MissingPrekeys fix). Existing user edits: keyhive_core/src/keyhive.rs and principal/individual.rs / individual/op.rs, 601 insertions and 81 deletions. Instrumentation bookmark `aa469ceb` is separate. Previous common upstream base `8c631cdc`.

Subduction @ `pynkyyzm` / `90f7a7a2`, parent `dfbfa85e`; 17 files with existing changes, including protocol, cache, storage, policy, powerbox and tests. Preserve all. Instrumentation bookmark `522f739b` is separate. Previous common upstream base `472c5088`.

Fetched upstream in both forks using jj. New Keyhive upstream main `b7968be79970`; relevant intervening changes include concurrent CGKA adds, merge order independence, pending membership resolution, revocation history, trailing blanks, auth and head invariants. New Subduction upstream main `2566cbfc5d1d`; includes latest Keyhive migration #308. Rebase semantic overlap review is running read-only concurrently with latest Daybook evidence recovery.

Disk: 24 GiB available at start. Launched repository disk-watch daemon with log `/tmp/disk-watch-goal.log`.

## Existing evidence index

- `docs/scratch/fork-rebase-handoff.md`: previous rebase and consumed-crate boundaries; historical heads, not current gates.
- `docs/scratch/keyhive-sync-stall-hunt11.md`: prior stall investigation.
- stress-sync-investigation skill hint cache: failure signatures and attribution discipline.

## Active work

Recover prior evidence; preserve pre-rebase fork states with jj bookmarks before rebasing. Validate semantic conflicts, not merely marker removal. Establish local Cargo resolution and start short concurrent-load hunts after upstream cutover. No final gates exercised yet.

## Recovered leads and worker routing

History-recovery report `/tmp/green-deepseek-evidence-2.log` is untrusted until source/reproduction confirms it. Leads include hunt11 empty peer-pair advertisement/Unauthorized settlement, missing epoch-key grants, bootstrap membership loss, offline transfer, and existing ignored causal-authorization tests. These are not fresh failures or current attribution. Prior reports contain stale fork heads and API-migration status.

Verified built-in OMP custom agent routing: `/home/asdf/.omp/agent/agents/green-deepseek.md`, resolved model `ollama-cloud/deepseek-v4.1-flash:high` for BigRepoIgnoredContracts and DaybookLongTestFence. Built-in task agents do not inherit parent history; give cold-start packets. Preferred Pi extension failed async package resolution and foreground ModelRuntime API; installed npm Pi package, but existing host module cache may need reload. Current CLI writer exclusively owns Keyhive; parent must not edit it concurrently. See memory file for ownership and logs.

Corrected runbook focused filter: actual test is `long_af_test_iroh_sync_randomized_four_node_stress_converges` (`src/daybook_core/sync/tests/stress.rs:32`). `cargo nextest run --help` exits 0 and confirms stress-duration and fail-fast options; no final suite/stress gate exercised yet.

Keyhive cutover completed: preserved original snapshot and separate instrumentation, resolved upstream API migration, independently verified 38 integration + four core + two BeeKEM tests and consumed-crate clippy. Strengthened existing concurrent-update regression additionally proves replicated tree and epoch-key convergence; focused test and final clippy pass. Subduction rebase now owned exclusively by reused native DeepSeek HIGH worker BigRepoIgnoredContracts. No final Townframe gates yet.

Cycle 1: exact formerly-pending delegation assertion exposed an incomplete migrated recovery fixture. The old non-empty admission check passed because unrelated prerequisite events were admitted; the newly named member/prekey dependencies were absent from the pre-add snapshot. Replay the full current event set minus the dependent delegation, then require that exact delegation admission and restored Read grant. Focused recovery test and warnings-denied clippy pass (logs `/tmp/green-subduction-parent-recovery-{exact,clippy}-c.log`). Earlier parent assertion compiler mistakes corrected (MutexGuard dereference and reachable_members Access-valued map). No production fallback/suppression added.

Subduction cutover independently verified; full root metadata proves one local source per fork crate. Combined all-feature nextest acceptance running `/tmp/green-root-nextest-20261002-a.log`.

Cycle 2: combined suite d reached 502/623 (500 passed, two failed) before fail-fast. Both disk Drawer perf tests failed OwnerHoldsNoPrekeySecret. Owner-secret snapshots taken before awaited group generation became stale when the janitor rotated a published key. Deterministic all-prekey rotation regression failed before with the same error, passed after Document constructors use the live secret-map handle and read after selection. RNG lock-order regression and reserved-identity test pass; Keyhive all-target/all-feature clippy -D warnings passes. Both failing Drawer tests now pass. Runnable API smoke roundtripped100docs (50reserved) during700rotations, then scaffold removed. Logs `/tmp/green-owner-rotation-*`. Full combined suite e and consumer clippy running.

Consumer API cutover: workspace nonempty0.12, obsolete nonempty12 alias removed, ephemeral callers migrated; synthetic admission test Add/Remove constructors explicitly carry authorization witness sentinels outside incorporation verification. No production authorization bypass.

Cycles 3/4: suite e had495passed, pin resume equality left13/right12 plus offline-transfer240s timeout. Stop/join the pin writer before comparing durable progress to reopened state; equality retained and parent targeted test passes. The offline reporter itself deadlocked via a frozen hub query; replaced with direct durable-store observations, keeping all alignment assertions. Focused scenario passes, and forced reporter smoke while ALL hubs were frozen produced two cursor/doc reports and completed; forced call removed. Original mixed-load debug suite f plus final-head clippy running (/tmp/green-root-nextest-20261002-f.log, /tmp/green-root-clippy-20261002-c.log). Root clippy b had already passed after four explicit Arc::clone corrections.

Cycle5: mixed debug suite f failed the branch-delete ordering test (global partition count2vs1). AFW can republish the branch after an injected tombstone-commit failure. The historical proposed local-unreachable witness also fails because the creator retains direct admin access. Corrected the test to prove both drawer/content group grants exist before deletion and are revoked before the failed commit; entry still lists branch and no tombstone remains asserted. Narrow runtime passes (/tmp/green-branch-order-after-b.log). Historical hint corrected, not trusted blindly. Mixed debug suite g plus final-head clippy now running (/tmp/green-root-nextest-20261002-g.log, /tmp/green-root-clippy-20261002-d.log).

Suite g completed 541/623: 540 passed, one missing required OCI artifact, three existing skips. The plug inspection test explicitly requires target/oci/@daybook/test and supplies the xtask build command. Building that real artifact now; no test suppression or code change. Chained final-head clippy d did not run. Environmental prerequisite recovery is not a substantive fix cycle (count5).

Real OCI build passed after migrating xtask nonempty to workspace0.12; compiler caught its two remaining Keyhive caller mismatches. No synthetic artifact. bg_67 runs targeted inspect_test_plug_oci_layout, complete mixed DEBUG suite h, consumer clippy e, and xtask clippy; logs /tmp/green-plug-oci-inspect.log, /tmp/green-root-nextest-20261002-h.log, /tmp/green-root-clippy-20261002-e.log, /tmp/green-xtask-clippy.log. Count5 unchanged.

Suite h:623run,622passed, tier10 3editor1relay timeout240s. Phase3 mutations completed220.044s; post-mutations settled232.773s. Final settle/alignment unfinished; existing stage instrumentation pins the interval, continuous protocol activity persists. OCI runtime inspection passed; final clippy remains unrun. Investigate actual final fence before attributing or changing test timing.

Ordinary required suite i passed all623tests (3existing skips),305.376sec runtime; tier10 relay passed185.268sec. Final consumer+xtask clippy g exited0 after deleting obsolete unused partition-count fixture helper/field (lint cleanup only). Required DEBUG60m whole-set stress launched in detached tmux green-root-stress; log /tmp/green-root-stress-20261002.log, terminal status /tmp/green-root-stress-20261002.exit. No filters, skips, cap changes or retry behavior added.

## Final acceptance: green under authorized CI conditions

User explicitly requested the local CI flake allowance: existing profile.ci provides three retries after the initial attempt (four attempts total per test), not four failed tests globally. CI environment uses UTILS_RS_TIMEOUT_MULTIPLIER=3, RUST_LOG_TEST=debug, RUST_BACKTRACE=1. Nextest long_af cap remains eight minutes. No retry configuration, test filters, ignores or timeout caps changed for acceptance. Temporary prekey/shutdown probes removed; real required test-plug OCI artifact rebuilt.

- Required ordinary combined all-feature suite previously exited0:623passed,3existing skips (/tmp/green-root-nextest-20261002-j.log).
- Final CI-profile combined suite:623passed,3existing skips,266.586sec, no retries reported (/tmp/green-root-ci-local-b.log). Command: cargo nextest run -p daybook_core -p big_repo --all-features --profile ci --failure-output immediate.
- Exact consumer lint gate exited0: cargo clippy -p daybook_core -p big_repo --all-targets --all-features -- -D warnings (/tmp/green-root-ci-clippy.log).
- Final whole-set stress exited0: cargo nextest run -p daybook_core -p big_repo --all-features --profile ci --failure-output immediate --fail-fast --stress-duration 60m. Ten iterations passed in3757.204sec, no retries reported (/tmp/green-root-ci-stress.log). Terminal chain status0: /tmp/green-root-ci-gates.exit.

Earlier no-retry mixed runs exposed prekey selection, shutdown timeout and head-parity failures; these are retained evidence, not hidden or claimed repaired by the final passing run. Read-only worker reports are leads, not accepted fixes. In particular the prekey report incorrectly treats absence of a later-added probe in an earlier log as evidence; do not repeat that inference. No open upstream PR applied, no pushes or git commands. Local fork path patches remain intentional because fork changes are unpushed.
