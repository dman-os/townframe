# Wflow fix ledger (generation 3)

Status: Open. Defects with a landed fix are marked in the waves table; the
remaining open items are listed below them. Decisions are labelled WFR-; a
recommendation that has not been accepted says so at the site.
Owner: operator + wflow replacement track
Updated: 2026-11-16
Scope: Outstanding defects and fix evidence for the `feat/native-wflow` engine
replacement. Design decisions (WFR-001..013) live in
`wflow-replacement-ledger.md`; this file holds the defect ledger.

Open items from the post-closeout review of `feat/native-wflow`. Complements
`docs/scratch/wflow-replacement-ledger.md`, which holds the design decisions
(WFR-001..013); this file holds the outstanding defects and their fix state.

Base: `cba72f24` (merge of `origin/main`). Working tree clean at open.

## Verification environment

- Builds run through `nix develop` → mbx-managed cargo. `./x/agent-build.ts` no
  longer exists (removed by the `main` merge that brought in mbx).
- Do not set `CARGO_TARGET_DIR`; mbx owns `target`.
- **`TMPDIR` must be short.** mbx cannot bind its session socket under the arena's
  long `TMPDIR` (`path must be shorter than SUN_LEN`), so every nix/cargo
  invocation needs `TMPDIR=/run/user/1000` exported first.
- **`.env` must not set `CARGO_TARGET_DIR`.** The arena image shipped this
  worktree a gitignored `.env` exporting
  `CARGO_TARGET_DIR=/run/media/asdf/p3N/tmp/townframe-target-p3n-5` — outside the
  checkout and shared with every other worktree of this repository, including the
  opposing contestant's. That bypasses mbx's managed target, lets cargo's
  mtime-based freshness check accept artifacts built from a different checkout
  (checkouts carry identical mtimes, so it does), and lets two processes write
  one target directory at once. It produced two bogus test failures during this
  session before it was found. The file is now a comment explaining why. **Every
  build result recorded before this was fixed is suspect and was re-run.**
- Build evidence must be re-established after switching: `cargo clean -p <crate>`
  in the same invocation as the check if a stale artifact is suspected.
- **`.env` must supply the `OLLAMA_*` variables.** `mltools` reads them with
  `env!()` at compile time, so `daybook_core` and `daybook_cli` cannot build at
  all without them. This worktree's `.env` originally held only the
  `CARGO_TARGET_DIR` line, so daybook was unbuildable here before this session
  touched anything. The file now carries the placeholders from `.env.example`.
- Cargo's MSRV resolver rewrites `Cargo.lock` on every invocation in this
  worktree. Revert it; the hygiene work owns the lock.

## Commits on this branch (generation 3)

| Commit | What |
| --- | --- |
| `2e7c11cd` | one durable fault path, free parked re-grants, fingerprinted receipts (Lane A) |
| `5a19b75a` | align the authority model with the durable fault contract (Lane C) |
| `fb331b5e` | model the lease-holder fault path and its fences (Lane C) |
| `bf1baea6` | CI actually checks the model invariant; Apalache 0.62.4 (Lane D) |
| `ff428451` | settle every unrecoverable invocation as a durable fault (Lane B1) |
| `c922ad0a` | bound guest-controlled text in durable retry faults (Lane B1) |
| `8483a3a9` | bound every free-text field of a durable fault at the authority |
| `2eb6581a` | restore the workspace `wash-runtime` pin in five manifests |
| `f8a11188` | CI bound documented; `boundedVerify` instance for manual depth-4 checks (Lane D) |

Process note: `8483a3a9` was committed with `git add -A src/` while another lane
held uncommitted work in `authority.qnt`, so it swept that lane's in-progress
`boundedVerify` module into an unrelated commit. Nothing was lost or corrupted
(the owning lane corrected its comment in `f8a11188`) but the commit boundary is
wrong. Commit by explicit path, never `git add -A`, while lanes are live.

## Audit triage (docs/scratch/wflow-smell-audit.md)

The read-only audit found defects more severe than several of the original
eight. Findings 1, 2, 5, 6, 7 and 10 I re-verified against source myself; the
rest are taken from the audit's file:line evidence.

Severity context: `utils_rs::setup_tracing` installs a panic hook that calls
`std::process::exit(1)`, so every "panics the actor" finding below is a daemon
exit, not a task failure.

| # | Finding | Disposition |
| --- | --- | --- |
| 1 | guest-declared step kind `assert_eq!` exits the daemon, then boot-loops on replay | **B2** |
| 2 | `Complete.expected_steps` is read from the authority, so the replay-length guard is a tautology and a short deploy commits over unexecuted durable steps | **B1** |
| 3 | step rejections other than `StaleLease`/`Nondeterministic` panic the actor instead of faulting the job | **B2** |
| 4 | every Wasm store/link/trap error settles as `Completed(Err(Terminal("wasm error: …")))` | **B2** |
| 5 | any `CommitError` panics; journal `NotCommitted`/`Unknown` reconciliation is unreachable | **B2** |
| 6 | shutdown abandons the mailbox, so queued callers panic at `expect(ERROR_ACTOR)` | **B2** |
| 7 | `await_receipt` conflates "unknown key" with "runtime died" and unwraps storage errors | **B2** |
| 8 | one native `current_thread` `LocalSet` for all workflows; no activation deadline; `LocalExecutor::drop` joins and hangs | open — needs a product decision on deadlines (see below) |
| 9 | the SQLite committer is an unsupervised `tokio::spawn`; its death becomes caller panics | **refuted** — see below |
| 10 | adapter-detected divergence flattened to a `Terminal` string, so the durable failure class depends on a race | **B1** |
| 11 | guest-chosen sleep duration panics on timestamp overflow | open |
| 12 | `waiters` grows unbounded; closed senders never removed | **B2** (cheap) |
| 13-14 | snapshot clones and re-serialises all state on the actor thread; recovery materialises the whole suffix | open — belongs with item 6 |
| 15 | step values and args JSON-encoded twice on every durable write | open |
| 16 | native panic payload only decoded as `&str`, so `panic!("{msg}")` loses its message | **B2** (cheap) |
| 17 | fault prose is guest-controlled and unbounded per record | **B1** (cap it with A's `Panicked` change) |
| 18 | Wasm run tasks never joined; their panics become ordinary activation loss | **B2** |
| 19 | `submit` acks before an `Execute` is durably issued | open — API semantics |
| 20 | `PersistStep.start_at`/`end_at` ferried over the protocol and discarded | open — dead protocol surface |
| 21 | `next_step` "stale completed step" repair is unreachable dead code | **B2** (cheap) |
| 22 | guest-facing config panics in the plugin binder (`unreachable!`, `assert`) | **B2** |
| 23 | recovery asserts instead of returning typed storage errors | **B2** (cheap) |
| 24 | namespace bookkeeping never shrinks | open |
| 25 | result-index reads `expect` on corrupt rows | **B2** (cheap) |
| 26 | the only native/Wasm conformance test cannot find its fixture under this repo's build policy, so parity has no CI evidence | **B2** |
| 27-31 | tests assert failure classes by substring, use wall-clock sleeps, pin commit counts, have no fault tests, and depend on a hard-coded sampling seed | **B2** (the three named tests) |

## Parity now has evidence (audit 26)

The native/Wasm conformance test resolves its fixture from `TEST_WFLOWS_WASM` or
an *external* `CARGO_TARGET_DIR`. Under this repo's build policy
`CARGO_TARGET_DIR` is unset and the guest is its own workspace, so its artifact
lands in `src/wasm_utils_rs/target` and the test could not find it by default:
the parity claim had no evidence anyone could reproduce.

Measured, not assumed: `cargo build --locked --manifest-path
src/wasm_utils_rs/Cargo.toml --release --target wasm32-wasip3 -p test_wflows`
takes about four minutes warm and produces
`src/wasm_utils_rs/target/wasm32-wasip3/release/test_wflows.wasm` (1.9 MB).
With that file present, `cargo nextest run -p wflow` is
`11 tests run: 11 passed`, including
`authority_conformance::authority_native_and_wasm_conformance` (0.16 s).

`wasm_path()` now looks in the guest workspace target before
`CARGO_TARGET_DIR`, so the documented build is enough on its own. That is also
what makes the test pass in CI: the `tests` job already builds `test_wflows` into
`src/wasm_utils_rs/target` and runs the whole workspace under `cargo llvm-cov`.
CI never sets `TEST_WFLOWS_WASM`, and whatever `CARGO_TARGET_DIR` the test sees
there (unset, or `cargo llvm-cov`'s own directory under the root `target`) is
never the guest workspace's target, so the old lookup could not find the
fixture in CI either.

### CI layout (review of `5e9f6d60`)

`5e9f6d60` first added a `wflow-parity` job, beside the earlier
`wflow-authority` job. Both were mostly duplicates and were removed after
checking real timings on main (scheduled run 43848942334):

| Job | Wall | Note |
|---|---|---|
| `tests` | 33.5 min | critical path: 23.6 min nextest, 4.9 min plug OCI, 1.4 min `test_wflows`/`test_nodule_guest` builds |
| Nix CLI package | 26.5 min | |
| rust-target-matrix native | 21.6 min | 17.7 min check-matrix |
| E2E (CLI) | 12.9 min | |
| Lint (pre-commit + nix checks) | 10.5 min | 8.9 min hooks |

Per-job setup (checkout, Nix installer, FlakeHub cache, rust-cache) is ~1-1.5
min before the first `nix develop`, and the first `nix develop` plus WIT fetch
is another 1.3-3.5 min.

- `wflow-parity` rebuilt `test_wflows` and ran `cargo nextest -p wflow`; the
  `tests` job already does both (its `builds` step and its `--workspace` run).
  Removed outright.
- `wflow-authority` ran `cargo nextest -p wflow_core`, also in `tests`. Its only
  unique work is quint: typecheck 2 s, `quint run` 2 s, `quint verify
  --max-steps=3` 448 s locally (391 s on an earlier run). Those three steps moved
  into the Lint job under `if: !cancelled()`: Lint has ~23 min of slack under
  `tests`, already realizes `ci-desktop` (which carries `formalTools`), and the
  model is a source-only check like the hooks. A job of its own would add a
  runner's setup for under 8 minutes of work and no wall-clock gain.

## Adapter fixes (unblocked by the manifest revert)

`wash_plugin_wflow` did not compile at HEAD, so three of its defects could be
neither reproduced nor fixed until `2eb6581a` restored the `wash-runtime` pin.
Then, in the plugin and the native adapter only:

- **Wasm trap laundering (audit 4).** Every store/run error became
  `SessionYield::Complete { result: Err(Terminal("wasm error: …")) }`, so a
  component that panicked was durably recorded as *completed*. A Rust panic in a
  component surfaces as a trap, so it is now `JobTrap::Panicked` →
  `SessionYield::Panicked` → `Faulted(Panicked)`, matching native. A detected
  divergence still outranks the trap.
- **Formatted panic messages (audit 16).** The native adapter only downcast the
  payload to `&str`, so `panic!("boom {n}")` recorded "unknown panic payload".
  `String` is tried first now, and a non-string `panic_any` payload is named as
  such rather than silently dropped.
- **Guest-facing binder panics (audit 22).** `on_workload_bind` did
  `unreachable!()` when a component declared no wflow interface, and `spawn`
  asserted `"fishy"` on a duplicate activation context. Both are configuration
  states a workload can reach, and both exited the plugin host; they are typed
  errors now.

## Lane B split

Findings 9 was **refuted on inspection**: `src/wflow_core/kvstore/atomic.rs` is
184 lines and contains no spawned committer at all (`atomic.rs:112-135,399-411`
do not exist); `journal.rs` has no `tokio::spawn` either. There is nothing to
supervise, so the finding is dropped rather than "fixed". This is why every audit
row is re-read against the source before it is acted on.

`authority_runtime.rs` is one file, so B1 and B2 are sequential, not parallel.

- **B1 — contract adoption.** Take A's shapes; make every unrecoverable outcome
  a typed `Faulted`; give the session protocol its own executed-step count so
  the completion guard stops being a tautology (finding 2); carry the divergence
  error class from the WDK boundary through both adapters so the durable class
  stops depending on a race (finding 10); cap guest-controlled fault prose
  (finding 17).
- **B2 — robustness.** C1 and H3/H4/H5/H6/H7/H9-adjacent crash and lifecycle
  paths, plus the cheap ones (12, 16, 21, 22, 23, 25) and the conformance
  fixture (26). Every one of these replaces a process abort with a fenced,
  typed outcome for one job.

## Decision needed (finding 8)

A wedged native workflow stalls every other native workflow because all
activations share one `current_thread` `LocalSet`, and no activation deadline
exists. The audit is right that this is an availability outage, and the earlier
review was wrong that it was a design flaw in the cooperative model — the model
is fine; the missing piece is a yield/interruption deadline and a way to reclaim
a session that stops yielding. This needs a product call on what the engine
guarantees — and the question is genuinely about the promise, not the code.

Recommendation, not yet implemented and not yet accepted:

1. **Bounded teardown first.** `LocalExecutor::drop` does `thread.join()`, so a
   guest blocked in synchronous code hangs host shutdown forever. Cancel, wait a
   short bounded interval for the run future to finish, then detach the thread
   and record that the abandoned activation's thread is unrecoverable. That is
   honest: we cannot preempt arbitrary blocking code on the same OS thread, and
   pretending otherwise with an async deadline is a lie.
2. **Then per-activation isolation, if the availability promise matters.** One
   `current_thread` runtime + `LocalSet` per activation instead of per driver.
   Non-`Send` guest futures stay legal because each activation owns its thread.
   Cost is one thread per in-flight activation (tens, for a personal Daybook),
   and cancellation becomes "drop the runtime", which is a real reclaim rather
   than a request.
3. **An activation deadline is not a substitute** for either: a wall-clock
   deadline cannot interrupt synchronous code, and a cooperative yield deadline
   only catches guests that yield.

Recommended: (1) now because it is a shutdown hang with no user-visible benefit,
and (2) as a separate change once the availability promise is stated. (3) only
for guests we do not trust.


Three waves, because the authority is the shared contract and the runtime,
adapters, model and WIT all have to move with it. One writer per file set at a
time; builds serialised (memory is the constraint, not CPU).

## Waves

| Wave | Lane | Scope | Depends on | State |
| --- | --- | --- | --- | --- |
| 1 | A-authority | `src/wflow_core/partition/**` | — | **done** `2e7c11cd`, verified: clippy clean, 36/37 (conformance awaits C) |
| 1 | R-restate | `docs/scratch/restate-retention-research.md` | — | **done** |
| 1 | S-smells | `docs/scratch/wflow-smell-audit.md` | — | **done** |
| 2 | C-model | `src/wflow_core/partition/authority.qnt` | A | **done** `5a19b75a` + `fb331b5e`, verified 37/37 |
| 2 | D-ci | `.github/workflows/checks.yml` | A, C | **done** `bf1baea6`; depth-4 follow-up dispatched |
| 2 | B1-contract | `src/wflow_tokio/**`, `src/wflow_wdk/**`, adapters | A | **done** `ff428451` + `c922ad0a`, verified 39/39 + 37/37 |
| 3 | B2-robust | `src/wflow_tokio/**`, adapters | B1 | **done** `e8879cd1` + `45b0c3c7`, verified 36/36 + 37/37 |
| 3 | E-retention | WFR-014 (core + runtime + model) | B2 | **done** `af8649d1`, verified 42/42 + 37/37 |
| 4 | F-hygiene | item 3, item 8, audit 11/21 | B2 | **done** `4659843d`, verified 42/42 + 39/39 + 10/10 |
| 5 | G-guardrails | module split, doc metadata, allow attribute | — | **done** `6ac671fd`, lint 0 diagnostics |
| — | I-parity | audit 26: conformance fixture | — | **done** fixture resolves from the guest workspace; runs in the `tests` job |
| — | adapter | audit 4/16/22 (Wasm trap class, panic text, binder panics) | — | **done** `6268314f` |

B owns the adapters too. Splitting "runtime" from "adapters" is what produced
the two laundering defects in the first place: the error class has to be
translated consistently from the guest boundary to the durable settlement and
that crosses all four crates.

## The cancellation error class (item 7, folded into B)

`wflow_wdk::BackendError` distinguishes `NondeterministicReplay` from
`Protocol`, but `impl From<BackendError> for JobErrorX` collapses both into
`JobErrorX::Terminal`, so an adapter-detected divergence is reported to the
caller as workflow-level failure while an authority-detected one faults the job.
That is item 2 seen from the guest side: the two paths must agree. B introduces
the missing error class at the WDK boundary and carries it through both
adapters, so `ReplayDiverged` and cancellation are engine-level outcomes on
every path, and `Protocol` stays a genuine user error.

## Open items

### 1. Restart budget charges jobs that are parked on a durable wait

`Job::restarts_after` (`authority.rs:238`) charges a restart whenever the
re-grant's `Progress` equals the recorded one. A job parked on a durable sleep
or message wait records `progress` at grant time (`waiting: None`), so the first
daemon restart after it parks is free — but from the second restart on the
recorded progress equals the current one and each boot charges one restart. A
long sleep therefore dies of `RestartsExhausted` after four daemon restarts
without ever failing on its own.

Shape of the fix: a re-grant that re-attaches to durable state (a held wait) is
not a crashed run and must not charge the restart budget. Compare with
`retry.is_some()`, which already keeps the count without charging.

### 2. Two incompatible durable failure shapes

`Cancelled` and `RestartsExhausted` settle as `Outcome::Faulted(JobFailure)`,
but `Panicked`, `ReplayDiverged` and `RetriesExhausted` go through
`failure_envelope` (`authority_runtime.rs:1475`) into
`Outcome::Completed(Terminal { value: "Err(Terminal(<json>))" })` — a
`JobFailure` JSON-encoded into a string inside a *Completed* receipt.
`validate_fault` (`authority.rs:286`) only admits two variants, so the typed
path is half-used and callers cannot pattern match a failure kind.

Shape of the fix: one durable representation. Every unrecoverable job outcome is
`Outcome::Faulted`, `validate_fault` admits all `JobFailure` variants that can be
recomputed from log-derived state, and the runtime stops double-encoding.

### 3. Host spawn failure is discarded and retried without backoff

`execute` maps `host.spawn` errors to `SpawnOutcome::Failed` and drops the error
(`authority_runtime.rs:987`); `recover_activation_loss` then re-grants
immediately (`:1032`, `:1261`). A deterministic instantiation failure therefore
hot-loops through the restart budget and settles as `RestartsExhausted` with no
diagnosis, violating WFR-011's "no arbitrary debug prose" in the wrong
direction: it persists prose instead of the actual cause, and reports the wrong
failure class.

Shape of the fix: classify spawn failure. A returned host error is recoverable
activation loss but must be *reported* on the fault path (typed, bounded
detail); the re-grant must not be an unbounded immediate loop.

**Resolved `4659843d`.** `Host::spawn` returns
`Result<Box<dyn Session>, SpawnError>` with `Rejected` (settles as
`JobFailure::ActivationFailed { message }` under the lease the grant issued) and
`Unavailable` (durable revoke, then a re-grant armed on the deadline wheel behind
100 ms doubling to 30 s). Residual, deliberately not changed: `handle_spawned`
still asserts `host.contains_wflow` *before* matching the outcome, so a workflow
key removed from a live host between admission and grant panics rather than
faulting. `submit` refuses an unregistered key and recovery refuses to start if a
durable job needs one, so no supported path removes a key mid-flight; the assert
is the invariant that makes the `Rejected` doc example ("an unknown workflow
key") reachable only through an unsupported mutation.

### 4. CI simulation checks no invariants; the verify step cannot pass — **resolved `bf1baea6`**

Root cause was not the model. `.github/workflows/checks.yml:71` ran `quint run` with
no `--invariant`, so it explored traces and asserted nothing. `quint verify` never
reached the model at all: quint sends the flattened spec to the Apalache server as
one string inside the JSON-RPC config, and Apalache 0.62.1 parses that config with
Jackson's 20 MB default string cap while this model flattens to ~20.05 MB. The cap
cannot be raised from outside the JVM in the Jackson version 0.62.1 bundles, so
`JVM_ARGS` has no effect (the property does not exist there); the fix is Apalache
0.62.4, which raises the JSON content limits to 1 GB (apalache-mc/apalache#3494).
`src/nix/formal-tools.nix` ships 0.62.4 and aliases the dist dir so a bare
`quint verify` does not try to download into the read-only store.

Both steps now have negative controls: a false invariant conjunct makes
`quint run` report `[violation]` and makes `quint verify` produce a
counterexample, so the checks are demonstrably evaluating the property rather
than passing vacuously.

Two residual facts worth keeping:
- `quint verify` reuses **any** Apalache server already on `localhost:8822`, so a
  stale server silently serves a different version. In CI the runner is fresh.
- **The exhaustive bound is three transitions, and that is a real limitation.**
  `submit → grant → commitStep → complete` is a four-transition trace, so it is
  covered only by the sampled simulation, not by the proof. Shrinking the
  instance does not help: the cost is the ~25 enabled transitions enumerated at
  each step times the nine invariants, measured at ~100 s per depth-4 state
  group, so depth 4 needs 50+ minutes at any constant set. Splitting the
  conjunction across four Apalache servers was tried and rejected (4 × `-Xmx4096m`
  on a 4-core/16 GB runner; one sub-run died silently, and the arithmetic still
  leaves an ~11 minute floor). An inductive invariant would recover it, but
  quint 0.33 rejects `--inductive-invariant` on this model with *"state is used
  before it is assigned"*. The unproven trace is therefore a known, measured gap
  rather than an oversight, and the CI step is named for the bound it proves.
- `module boundedVerify` (1 key, 1 worker, 1 message id) exists so a deeper check
  can be run by hand — `quint verify --main=boundedVerify --max-steps=4` — in
  ~50 minutes instead of hours on the two-key instance. CI does not use it.

### 5. `wash-runtime` default features re-enabled — **root cause of an unbuildable crate**

Five manifests were changed from the workspace pin to re-enable default features,
and `src/wflow` silently *lost* its `wasi-config` feature in the process:

| Crate | `origin/main` | branch |
| --- | --- | --- |
| `wash_plugin_sqlite` | `wash-runtime.workspace = true` | `default-features = true` |
| `wash_plugin_wflow` | `wash-runtime.workspace = true` | `default-features = true` |
| `wflow` | `{ workspace = true, features = ["wasi-config"] }` | `default-features = true` (**feature lost**) |
| `daybook_cli` | `{ workspace = true, features = ["wasi-logging"] }` | `+ default-features = true` |
| `daybook_core` | `wash-runtime.workspace = true` | `default-features = true` |

This was not only bloat. `wash-runtime`'s default features pull the `wasmtime-wasi`
preview1/wiggle surface, which disagrees with the workspace's `p3`-only
`wasmtime-wasi` pin, so `wash_plugin_wflow` **does not compile at HEAD**:
`cargo clippy -p wash_plugin_wflow --lib` fails with ~180 errors inside the
generated p1 glue (`expected an FnOnce(wasmtime::Error) closure, found
fn(wiggle::error::Error) -> p1::types::Error`, `proc_exit has an incompatible
type for trait`). The "native and Wasm parity" claim therefore rested on a crate
that could not be built. Reverted to `main`'s exact form in all five; see the
commit for the verification.

### 6. Receipt retention is unbounded (policy decision — see §Restate)

`Receipt` keeps the full `args` string, the policy, the budget and the outcome
for every job that ever settled, and `maintain()` re-serialises the whole
authority (receipts included) into one snapshot blob.

Needed (from WFR-006, deferred): separate the idempotency-key lifetime from the
result-payload lifetime, and stop retaining submission payloads at all.

### 7. Wasm cancellation is delivered as a catchable protocol error — **verified benign; residuals fixed**

Re-checked against the code. `wash_plugin_wflow`'s run task wraps the guest in
`cancel_token.run_until_cancelled(...)`, and the `Store` is owned by that future,
so cancellation **drops the instance** rather than leaving the guest running.
The run task then ends without sending a trap, `next_yield` returns `None`, and
`recover_activation_loss` sees a settled job: `job_for_lease` answers
`StaleLease` for the same incarnation, which the recovery path already returns
quietly. So a cancelled Wasm job settles exactly once, as `Faulted(Cancelled)`,
and never re-executes. The guest-observable `Protocol("session cancelled")` is
only reachable in the window before that drop.

Two residuals were real and are fixed in the adapters:

- **A Wasm trap was laundered into a completed receipt.** Every store/run error
  became `SessionYield::Complete { result: Err(JobError::Terminal("…")) }`, so a
  workflow that panicked inside the component was recorded as having *completed*
  with a terminal error. A Rust panic in a component surfaces as a trap, so
  `JobTrap::Panicked` now carries it to `SessionYield::Panicked` and settles as
  `Faulted(Panicked)`, matching the native adapter. A detected divergence still
  outranks the trap, so the durable class cannot depend on which came first.
- **A formatted native panic lost its message.** The adapter only downcast the
  payload to `&str`, so `panic!("boom {n}")` — the common form — recorded
  "unknown panic payload". It now tries `String` first, then `&str`, and names a
  non-string `panic_any` payload as such.

### 7a. (superseded text, kept for context) Wasm cancellation is delivered as a catchable protocol error

`src/wash_plugin_wflow/lib.rs:126,135,181` return
`HostError::Protocol("session cancelled")` into the guest. Native path drops the
session before the guest can observe it
(`wflow_wdk_native/lib.rs:111,121` is checked before the guest resumes).

### 8. Dead code and leftover schema — **resolved `4659843d`**

- `Step::attempt`, `Command::CommitStep::attempt` and `Rejection::InvalidAttempt`
  were removed: the runtime always stamped 0 and the authority rejected anything
  else, so the field recorded nothing.
- `Command::Retry::jitter_ms`, `retry_jitter_ms` and `RETRY_JITTER_WINDOW_MS`
  were removed: the window was 0, so the field was always 0.
- `Job::receipt` and `Job::settle` were collapsed into one constructor in
  `2e7c11cd`.
- `SNAPSHOT_FORMAT` 2 -> 3 and journal `FORMAT_VERSION` 3 -> 4 follow from the
  step/retry shape change; a snapshot or journal written before it is rejected at
  recovery rather than misread.

### 9. A parked job keeps its lease, so `RetriesExhausted` can settle it

Found while aligning the model (`fb331b5e`). `Transition::WaitStarted` sets
`job.wait` but does not clear `job.lease`, so a job parked on a sleep or a
message wait is simultaneously leased and waiting — that is deliberate, since
the parked session must still receive the delivered message. The consequence is
that `validate_fault`'s `RetriesExhausted` arm, which only requires the reporting
lease to be current, admits a fault reported by a *parked* session. A session
cannot produce a transient failure while parked, so this is not reachable from
the in-tree adapters, and the counter and budget are still recomputed — it is
harmless but loose. Recorded, not fixed: tightening it means adding a
`wait.is_none()` conjunct, which would also have to be mirrored in the model.

## Restate retention model (research for item 6)

Pinned research: `docs/scratch/restate-retention-research.md` (source:
`restatedev/restate@12adfb14`). Restate separates three independent horizons,
each a policy knob with a server-wide default, a per-service/handler override,
and an optional max clamp, all counting down **from completion**:

| Knob | Meaning | Default |
| --- | --- | --- |
| idempotency/result retention | how long a completed invocation's result is kept so a retry with the same idempotency key returns it instead of re-executing | 24h |
| workflow completion retention | how long a completed workflow's K/V state, promises and result stay reachable | 24h |
| journal retention | how long the execution *journal* is kept; `0` = retain nothing | 24h (server default `0` in production) |

Journal retention is capped by result retention. Deadlines are resolved at
submission and stored on the invocation, so a later config edit cannot move an
existing deadline. Pruning is an ordinary replicated command
(`PurgeInvocation`/`PurgeJournal`) proposed by a per-partition cleaner on a
periodic tick, at most `cleanup-max-in-flight-purges` (default 32) outstanding,
applied idempotently (`Ok`/`NotFound`/`NotCompleted`). Expiry is never influenced
by readers — there is no equivalent of "an active reader keeps the receipt
alive".

The decisive detail: **Restate keeps no tombstone.** The invocation id is a hash
of (scope, service, key, handler, idempotency key), so the completed row *is* the
dedup record; purging deletes it and a resubmission after expiry re-executes
under the same id with an empty journal. State is bounded by completion rate ×
retention, not by history. Restate also does not fingerprint the payload.

### WFR-014 — receipt retention (supersedes WFR-005's "permanent keys")

Operator decision, this session:

- **Job keys are reusable after cleanup.** A receipt is deleted once its
  retention window from settlement has elapsed; a later submission of the same
  key is admitted as a new incarnation and re-executes from an empty journal.
  This replaces WFR-005's permanent-identity rule, which was stronger than the
  evidence supported.
- **The conflict check stays.** While a receipt exists, a resubmission of the
  same key with different `(args, policy, budget)` is still
  `Rejection::PayloadConflict`, which is stricter than Restate. The digest that
  makes this possible survives payload pruning and dies with the receipt.
- **One horizon for now.** `retention` is resolved per submission (default from
  the runtime config, with a max clamp) and counts from settlement. A separate
  per-job *journal* horizon buys nothing here: `settle` already drops the replay
  log, waits, inbox and retry state, and the journal entries themselves are
  reclaimed by the existing snapshot-watermark paging. That mapping is written
  at the mechanism site so the divergence from Restate is explicit.
- **Pruning is a durable transition**, proposed by the actor from the deadline
  wheel (earliest `retained_until_ms`), never a poll and never a reader-driven
  decision. Replay re-validates that each pruned key was genuinely expired.
- **Waiters are woken on prune** so a caller awaiting a result observes "gone"
  rather than hanging.

Nothing about this touches the `PayloadConflict` strength or the fault taxonomy.


