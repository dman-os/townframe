# wflow production-readiness smell audit (generation 3)

Status: Findings only, one pass. Several rows were refuted on contact with the
source; the disposition of every row is in `wflow-fix-ledger.md`, which is the
authority on what was real.
Owner: operator + wflow replacement track
Updated: 2026-11-16
Scope: Read-only audit of the wflow runtime and adapters at `cba72f24`, plus the
eight items already known at the time, which are deliberately excluded.

Read-only review of `src/wflow_core`, `src/wflow_tokio`, `src/wflow_wdk`,
`src/wflow_wdk_native`, `src/wflow_wdk_wasm`, `src/wash_plugin_wflow`, `src/wflow`,
`src/test_wflows` at `cba72f24` (tree clean apart from this ledger pair).
Deliberately excludes the eight items in `docs/scratch/wflow-fix-ledger.md`.
Process context that raises several severities: `utils_rs::setup_tracing` installs a
panic hook that calls `std::process::exit(1)` (`src/utils_rs/lib.rs:206-213`), so any
panic listed below terminates the daemon, not just the task.

## Critical

### 1. A guest-declared step kind panics the single-writer actor (process exit)

`src/wash_plugin_wflow/lib.rs:227-231` stores the *guest-supplied* `host::StepMeta` in
`ActiveStepCtx`; `authority_session.rs:45-60` lifts it into `SessionYield::WaitStep`; then
`src/wflow_tokio/authority_runtime.rs:1174-1180` does `assert_eq!(kind.step_kind(), meta.kind)`.
The WIT `host.next-step(job-id, meta)` is a public guest entry point, so any component that
declares `kind: effect` and then calls `sleep`/`recv-message` (a stale build, a hand-written
component, a WDK bug) aborts the actor task and, through the process-wide panic hook, the daemon.
On reboot the same durable job replays to the same boundary, so the daemon boot-loops.
Severity: critical. Bug. Proof: a WIT component that calls `next_step(effect)` then `sleep`;
or a unit test driving `handle_yield` with `WaitStep{deets: Timer, meta.kind: Effect}`.

### 2. `Complete.expected_steps` is read from the authority, so the replay-length guard is vacuous

`AuthorityRuntime::complete` (`authority_runtime.rs:1372-1378`) fills `expected_steps` with
`self.authority.jobs()[&key].steps.len()`, and `prepare_complete` (`authority.rs:1000`) then
asserts exactly that equality — it can never fail. The session's own cursor is never consulted.
So if deployed workflow code now performs two of five durable boundaries and returns, the job
commits `Completed` over three unexecuted steps (`Receipt.steps` still says five). Kind/name
checks only fire when a boundary is *reached*, so nothing else notices.
Severity: critical. Bug (silently wrong results after a bad deploy).

## High

### 3. Every step rejection that is not `StaleLease`/`Nondeterministic` panics the actor

`handle_yield` (`authority_runtime.rs:1126-1153`) treats `PayloadConflict`, `OutOfOrder`,
`InvalidAttempt`, `UnknownJob` and `IncarnationConflict` as `panic!("step … rejected")`, and a
`Duplicate` ack (empty intents) hits `panic!("… not granted continuation")`. The authority
itself models duplicate step commits as legitimate and idempotent (`authority.rs:913-933`
returns `Duplicate(Park)`; the Quint model has a `duplicate_step` action), so authority and
runtime disagree about the same protocol event. In-tree adapters cannot emit it today, but any
other `Session` implementation, a host retry of a delivery, or a future adapter turns one job's
odd result into a whole-process abort instead of a fenced `ReplayDiverged` fault.
Severity: high. Bug (contract mismatch; failure blast radius is the process).

### 4. Any WASM/instantiation error settles the job permanently as a terminal error

`wash_plugin_wflow/lib.rs:521-529` wraps *every* failure of `run_concurrent` (store creation,
link error, trap, host-call trap) into `types::JobError::Terminal(format!("wasm error: {err:?}"))`.
The authority therefore commits a `Completed` receipt for what is a host/infrastructure failure:
no transient retry, no restart, no `JobFailure` class, and operator-unstable debug prose becomes
the durable result (contra WFR-011). A module that fails to link after a deploy therefore looks
exactly like the workflow returning an error.
Severity: high. Bug.

### 5. A single storage error aborts the process; the journal's own reconciliation is unreachable

`commit` (`authority_runtime.rs:1429-1433`) is `panic!` on any `CommitError`. The journal
carefully distinguishes `Conflict`, definite `NotCommitted` and `Unknown` (`journal.rs:262-281`,
`interrupted`), and `Journal` explicitly supports retrying a definite rollback — but nothing
above it can: `Writer::commit`'s `NotCommitted` (SQLITE_BUSY at `BEGIN IMMEDIATE` is mapped
here, `wflow/kvstore/atomic.rs:473-478,568-579`) kills the daemon. A transient busy/IO error
becomes a full-stack restart, and the reconciliation path exists only in unit tests.
Severity: high. Design smell.

### 6. Shutdown abandons queued commands, so concurrent callers panic

The actor breaks out of the loop on `Command::Shutdown` (`authority_runtime.rs:629-639`) without
draining the mailbox. Any `Submit`/`SendMessage`/`Cancel` already queued has its `oneshot` reply
dropped, and callers observe it through `.await.expect(ERROR_ACTOR)` (`:427`, `:465`, `:483`,
`:510`). `stop()` can therefore panic an unrelated business task instead of returning a
"runtime stopped" error, and `send()` on a stopped runtime panics for the same reason.
Severity: high. Bug (lifecycle/API contract).

### 7. `await_receipt` cannot distinguish "unknown key" from "runtime died", and panics on read errors

`authority_runtime.rs:219-241`: `self.commands.upgrade().expect(ERROR_ACTOR)` panics if the
runtime was dropped (a `JobHandle` holds only a `WeakSender` and is meant to outlive it);
`wakeup.await.ok()?` maps a dropped waiter to `None`, which the API documents as "key was never
submitted"; and all three `self.receipt().await.unwrap()` calls panic on a storage error in a
function that returns `Option`. A caller waiting for a result across a restart cannot tell
"no such job" from "the engine went away".
Severity: high. Bug.

### 8. Cancellation and shutdown cannot stop a non-yielding native workflow, and it blocks every other native job

`NativeHostDriver` runs *all* native activations on one `current_thread` runtime + `LocalSet`
(`wflow_wdk_native/lib.rs:285-314`), each a bare `spawn_local` whose failure is never joined. A
workflow that blocks in synchronous user code (`loop {}`, a blocking IO call) stalls every other
native workflow on that thread; `NativeSession::drop` only fires a token the guest must reach an
`await` to observe, so `cancel` leaves it running; and `LocalExecutor::drop` does `thread.join()`
(`:347-355`), so host teardown hangs forever. The actor has no activation deadline either.
Severity: high. Design smell + bug (one wedged job is an availability outage).

### 9. The SQLite committer is an unsupervised task whose death converts into caller panics

`SqliteFactory::boot` (`wflow/kvstore/atomic.rs:112-135`) `tokio::spawn`s `Committer` and keeps
no handle. If it exits (panic in `decode_entries`, any eyre path, runtime shutdown), in-flight
`oneshot` replies are dropped and every later `Writer::commit` panics at `.expect(ERROR_ACTOR)`
(`:399-411`). The storage layer's only health signal is a process abort inside the workflow actor.
Severity: high. Design smell.

### 10. Adapter-detected replay divergence is flattened into a `Terminal` string

`wflow_wdk/lib.rs:170-174` converts every `BackendError` (including the richly typed
`NondeterministicReplay{step_id,expected,found}` that the WIT protocol carries) into
`JobErrorX::Terminal(eyre)`; `job_error_from_x` (`:353-365`) then persists `format!("{err:?}")` as
the result. The same divergence therefore settles as `JobFailure::ReplayDiverged` when the
authority notices it (`authority_runtime.rs:1129-1147`) and as a generic terminal string when the
adapter notices it first (`wflow_wdk_native/lib.rs:75-102`). Which durable outcome a job gets
depends on a race, and the typed fields are discarded.
Severity: high. Bug (failure taxonomy is nondeterministic across adapters).

## Medium

### 11. A guest-chosen sleep duration panics in the host, with adapter-dependent fallout

`wflow_wdk_native/lib.rs:212-215` and `wash_plugin_wflow/lib.rs:308-310` both do
`start_at.checked_add(Duration::from_millis(duration_ms)).expect("ts overflow")`. Native catches
the panic in `catch_unwind` and reports `JobFailure::Panicked("ts overflow")`; the Wasm path
panics inside the spawned run task (never joined), which presents as activation loss and is
retried until the restart budget is exhausted. A guest argument merely too large for a timer is
treated as an internal invariant break in both cases.
Severity: medium. Bug.

### 12. `waiters` grows without bound for long-lived jobs

`authority_runtime.rs:602,724-726` pushes a `oneshot::Sender` per `WatchResult`, and only
`settled()` (`:1054-1058`) removes them. A caller that awaits a result with a timeout, or a UI
that polls `await_result` per frame, leaves a dead sender per attempt for the whole lifetime of a
sleeping/receiving job. There is no removal of closed senders and no cap.
Severity: medium. Design smell (unbounded collection on a hot path).

### 13. Every snapshot deep-clones and re-serialises the whole authority on the actor thread

`Authority::snapshot` (`authority.rs:666-674`) clones all jobs (step logs, inboxes, message
maps), and `maintain`/`final_snapshot` (`authority_runtime.rs:669-717`) serialize that clone to
JSON inline. At the 100-transition/60-second cadence this is O(all live state) — including every
retained `args` and every step value — on the single writer that also serves cancellation,
messages and steps. Cost is unbounded in job history, so snapshot latency grows with the oldest
job.
Severity: medium. Design smell (bounded pages, unbounded snapshot).

### 14. Recovery materialises the entire uncompacted suffix in memory

`read_transitions` (`authority_runtime.rs:514-534`) collects every entry from the snapshot cut to
the frontier into one `Vec<Transition>` (each carrying full step/args payloads) before
`Authority::recover` applies them, and `recover` clones each transition again
(`authority.rs:657-659`). Startup memory and latency are proportional to journal size; the
512-entry paging buys nothing for peak memory.
Severity: medium. Design smell.

### 15. Step values and submission args are JSON-encoded twice on every durable write

`CommitStep.value` is already a JSON document (`SessionYield::value_json.to_string()`,
`authority_runtime.rs:1122`), stored as a `String` field (`authority.rs:33`), then the whole
transition is serialized again (`:1407`), so all quotes/escapes are escaped a second time. The
same double encoding applies to `Invocation.args_json` and is repeated in every snapshot.
Recovery parses twice (journal entry, then the guest's `serde_json::from_str`). Pure overhead on
the per-step path, up to ~2x bytes for string-heavy payloads.
Severity: medium. Design smell (hot-path cost).

### 16. Native panic payloads are only decoded as `&str`

`wflow_wdk_native/lib.rs:332-341` does `payload.downcast_ref::<&str>()` and otherwise records
"unknown panic payload". `panic!("{msg}")` (and any `expect`/`unwrap` with formatting) produces a
`String` payload, so the most common panic shape in real workflow code loses its message and the
durable receipt says only "unknown panic payload".
Severity: medium. Bug (diagnostics).

### 17. Failure prose stored durably is guest-controlled and unbounded

`JobFailure::Panicked(String)`, `ReplayDiverged{detail}`, `RetriesExhausted{last_error}`
(`wflow_core/partition.rs:24-42`) are written into the receipt, the result index, and every
subsequent snapshot. `last_error` is the guest's `error_json`; a workflow can inflate every
snapshot of the partition for the rest of the deployment. (This is not the retention policy in
fix-ledger item 6 — it is the size of a single fault record.)
Severity: medium. Design smell.

### 18. Wasm activation tasks are never joined; their panics are silently reinterpreted

`start_session` (`wash_plugin_wflow/lib.rs:508-537`) `tokio::spawn`s the run task and stores only
a `JoinHandle` that `SessionHandle::drop` aborts (`:160-166`); nothing ever awaits it. A panic
inside the Wasm store/engine therefore surfaces to the actor as a closed channel
(`next_yield` -> `None`) and is treated as ordinary activation loss: revoke, re-grant, replay,
repeat — with no log line, no fault class, and no way for an operator to see the real cause.
`WflowPlugin::stop` (`:656-658`) is empty, so plugin shutdown does not cancel live sessions.
Severity: medium. Design smell.

### 19. `submit` acknowledges before an `Execute` is durably issued or a spawn has happened

`submit` (`authority_runtime.rs:894-931`) sends `Ok(ack)` and only then calls `grant`, which can
itself settle the job as `RestartsExhausted` (budget already spent) or fail to spawn. A caller
that treats the returned ack as "the workflow is running" is wrong, and there is no API to
observe "admitted but never executed". Related: the duplicate-submission compare includes
`policy`/`budget` (`authority.rs:780`), so a resubmission after an operational policy change is
reported as `PayloadConflict` rather than returning the original admission.
Severity: medium. Design smell (API semantics).

### 20. `PersistStep.start_at`/`end_at` are carried over the protocol and thrown away

`wflow_wdk/session.rs:8-16` (and both adapters) compute and ferry per-step timestamps, but `Step`
(`authority.rs:22-36`) has no timing fields and `handle_yield` drops them. Either the data is
meant to be durable (it is not) or the protocol and both adapters pay for two `Timestamp::now()`
calls per step for nothing. Dead protocol surface, distinct from the `attempt`/jitter dead fields
already listed.
Severity: low-medium. Design smell.

### 21. `next_step`'s "stale completed step" repair is unreachable

`wash_plugin_wflow/lib.rs:184-206` handles an `active_step` whose replay slot already has a
value. That state cannot occur: `active_step` is only set after the `value_json.is_some()` early
return (`:213-224`), and `job.replay` is never mutated after `start_session` constructs it
(`:496`). The branch, its `compare_exchange(...).expect("impossible")` and the nested lock dance
are dead code pretending to be a recovery path.
Severity: low. Design smell.

### 22. Guest-facing config panics in the plugin binder

`on_workload_bind` (`wash_plugin_wflow/lib.rs:587-592`) does `unreachable!()` when the workload
does not declare `townframe:wflow`, and `start_session` asserts `_old.is_none()` on a duplicate
`ctx_id` (`:501-506`). Both are reachable from host configuration/plugin wiring, and with the
panic hook both exit the daemon.
Severity: low. Bug.

### 23. Recovery panics where it should return an error, and every reopen writes

`read_transitions` asserts `!page.entries.is_empty()` ("journal shorter than its frontier",
`authority_runtime.rs:524`) instead of returning a typed storage error, and `Journal::open`
unconditionally commits a bumped `authority_epoch` (`journal.rs:95-98`) even for an unchanged
reopen, so every start is a write that can fail and panic at the call site.
Severity: low. Design smell.

### 24. Namespace bookkeeping never shrinks

`SqliteFactory.namespaces` (`wflow/kvstore/atomic.rs:83,186-203`) keeps one `Arc<Namespace>`
(with a watch channel and semaphore) per namespace name forever, and `wflow::Ctx` derives the
name from the partition id. Any deployment that creates short-lived partitions grows the map and
the `wflow_atomic_namespaces` table without bound.
Severity: low. Design smell.

### 25. Result-index reads panic on corrupt/foreign data

`read_receipt`/`decode_terminal` (`authority_runtime.rs:536-541,1461-1470`) `expect(ERROR_JSON)`
on the index row. One corrupt row, or an index written by a different format generation, panics
whichever process calls `get_result`, even though the journal and snapshot are intact and could
rebuild the receipt.
Severity: low. Bug.

## Test gaps

### 26. The only real cross-adapter conformance test cannot locate its fixture under this repo's build policy

`wasm_path()` (`src/wflow/authority_conformance.rs:130-149`) resolves `TEST_WFLOWS_WASM`, else
`$CARGO_TARGET_DIR/wasm32-wasip3/release/test_wflows.wasm`, else returns an error. AGENTS.md
requires `CARGO_TARGET_DIR` to stay unset, and CI builds `test_wflows` into the workspace
`target/` while exporting neither variable (`.github/workflows/checks.yml:227-230,258-261`). The
one test that proves native/Wasm parity therefore either fails red or, if the lane is tolerated,
covers nothing. `setup()` also leaks `WashHost` when an assertion fails before `stop()` (`:510`).
Severity: high (test gap). Bug in test infrastructure.

### 27. Failure kinds are asserted by substring, never by type

`tests.rs:718` (`contains("nondeterministic")`), `:763-764` (`contains("ReplayDiverged")`,
`contains("durably Recv")`), `:406,415` (`contains("nope")`, `contains("kaboom")`) and the native
test's `contains("bad input")` are the only assertions on failure classes, because `get_result`
yields `Terminal(String)` with no typed accessor. A change that reports the wrong class, or swaps
`Panicked` for `Cancelled`, still passes.
Severity: medium (test gap).

### 28. Wall-clock sleeps and elapsed-time assertions where a paused clock exists

`tests.rs:453-477` (600 ms policy + `started.elapsed() >= 600ms`), `:496-507` (50 ms),
`:511-538` (400 ms plus `tokio::time::sleep(150ms)`), and `:533`'s real sleep to let a deadline
pass. `snapshot_follows_the_dirtying_timer_without_sleeping` already shows the `start_paused`
pattern, so these are avoidable flake and suite-latency sources.
Severity: medium (test gap).

### 29. Tests pin exact commit counts, not durable state

`committed(&mut commits, 4|5|7|9)` and `journal_frontier(...) == 6/7/8` (`tests.rs:321,395,504,
618`; `lifecycle.rs:425,457,527`) encode the current number of durability writes per lifecycle.
The ledger already records that an earlier maintenance change broke five tests this way; the
pattern is unchanged, so any further durability write breaks unrelated behaviour tests.
Severity: medium (test gap).

### 30. No fault tests for the paths this audit found

Nothing covers a duplicate/late `PersistStep` (finding 3), a guest `step-meta` mismatch (finding
1), a `Writer` returning `NotCommitted`/`Unknown` *during* runtime operation (finding 5;
`journal_tests.rs` covers the journal only), a vanished `oneshot` reply for `await_receipt`
(finding 7), or shutdown with a non-empty mailbox (finding 6). The journal's `FaultWriter` is a
good pattern that stops at the runtime boundary.
Severity: medium (test gap).

### 31. Conformance coverage depends on a hard-coded seed, sample count and a relative file path

`authority_conformance` (`src/wflow_core/partition/authority/conformance.rs:518-555`) requires
the fixed `seed=424242`, 200 samples x 32 steps to see every action, and `model_actions()` reads
the `.qnt` file with `std::fs::read_to_string(SPEC)` at test time. A model edit that changes
sampling — or a runner with a different CWD — fails the test for reasons unrelated to Rust
behaviour, and a model action that quietly stops being sampled is only caught by luck.
Severity: low (test gap).

## Most valuable next test

1. **`workflow_completing_before_its_durable_steps_faults`** — commit three named effect steps,
   stop, re-register the same key with a workflow that performs one effect and returns, restart.
   Assert the receipt is `Outcome::Faulted(ReplayDiverged{step: 1, ..})`, not `Completed`. This is
   the only test that would catch finding 2, and the cheapest guard against a bad deploy silently
   reusing a job's durable history.
2. **`guest_declared_meta_mismatch_faults_one_job_and_keeps_the_actor_alive`** — drive a scripted
   `Host`/`Session` (and the Wasm host's `next_step` + `sleep` pair) that yields
   `WaitStep{Timer, meta.kind: Effect}`, plus a duplicate `PersistStep` for an already committed
   step. Assert a terminal fault for that key, another key still completing, and no actor panic.
   Covers findings 1 and 3 and pins the intended blast radius in an executable contract.
3. **`storage_failure_mid_lifecycle_is_reported_and_recovery_matches_the_committed_prefix`** — a
   `Writer` wrapper returning `NotCommitted` once and `Unknown` once at chosen commits (the
   `FaultWriter` pattern from `journal_tests.rs`, lifted to `AuthorityRuntime`). Assert no process
   panic, a typed shutdown/command failure, and that reopening the same store replays exactly the
   committed prefix with no duplicated step. Covers findings 5, 8 and 9 and turns the existing
   journal-only crash injection into runtime evidence.
