# Wflow replacement design ledger

Status: Active design audit; implementation is frozen except for repairs needed to inspect or verify the branch
Owner: operator + wflow replacement track
Updated: 2026-11-04
Scope: Full replacement of the main-branch wflow engine. The current `daybook_core` consumer and the old partition reducer/effect-worker path are legacy and may be broken or deleted rather than preserved through compatibility shims.

## 0. Purpose

This is the review ledger and task list for the wflow replacement PR. It records accepted decisions, unresolved design questions, audit findings, implementation work, and verification evidence. A checked implementation item is not evidence that its underlying design was approved.

The trusted baseline is `origin/main`. The replacement branch is unreviewed work: every semantic or architectural departure from main must be retained deliberately, revised, or removed.

## 1. Governing decisions

- [x] **Full replacement rights.** This PR may break internal and public wflow interfaces. It does not need to keep the legacy `daybook_core` integration working while the replacement is being constructed.
- [x] **No permanent dual engine.** The target is one coherent runtime. The old reducer, effects map, worker queues, persistence path, and compatibility entry points should be deleted once their useful invariants have been identified and carried forward.
- [x] **Main is the design baseline.** Existing behavior on main is presumed intentional until the audit establishes otherwise. Branch behavior and the prior conversation digest are claims, not approved requirements.
- [x] **Design before closeout.** Phase-5-style benchmarks and broad closeout work remain paused until the replacement architecture and semantics are reviewed.
- [x] **No agent-lifetime workflow.** Wflow is durable computation for bounded tasks and activations. Agent conversations, memory, inboxes, and long-lived identity belong to a separate subsystem that may use wflow underneath it.

## 2. Review method

For each surface:

1. Record the main-branch mechanism and the invariant it protects.
2. Record what the replacement branch changed.
3. Classify the change as **retain**, **revise**, **remove**, or **operator decision required**.
4. Add the smallest implementation tasks needed by the accepted design.
5. Pin each correctness claim with a focused test, model property, fault test, or benchmark as appropriate.

Passing tests or a matching Quint model prove consistency with the encoded behavior; they do not approve the behavior itself.

## 3. Architecture audit

### 3.1 Surfaces to compare with main

- [ ] Workflow programming model: effects, replay, waits, messages, defers, and error classification.
- [ ] Commit and publication ordering: durable transition, indexes, authority publication, and execution intents.
- [ ] Persistence contract: log frontier, snapshots, compaction, range reads, and recovery failures.
- [ ] Execution lifecycle: warm sessions, cold replay, worker loss, lease fencing, and host failures.
- [ ] Cancellation: cooperative cleanup, in-flight effects, hard termination, and saturation behavior.
- [ ] Retry and restart policy: defaults, progress definition, deadlines, jitter, and exhaustion.
- [ ] Terminal state: receipts, idempotency, result lookup, retention, expiry, and key reuse.
- [ ] Native/Wasm parity and the common host/session abstraction.
- [ ] Formal model boundary: properties worth modeling versus accidental implementation policy.
- [ ] Production integration and deletion of the legacy engine.

### 3.2 Main-branch baseline and branch disposition

#### Main-branch mechanism and intent

- **Single ordered event history.** Job init/cancel/message/timer/run-result entries are reduced serially. The reducer is the state owner; workers only execute effects and append result events.
- **Effects are derived durable work.** Reducer output is appended as `JobPartitionEffects` with its source entry identity before workers execute it. Recovery indexes existing effect-source entries so replay does not emit a second batch, then reschedules durable outstanding run/timer effects.
- **Warm execution is an optimization.** Effect workers cache native/Wasm sessions and route the next run to the preferred worker, while a cold worker can reconstruct execution from durable positional steps.
- **Workflow history is positional.** `next_step` either returns the completed value at the current cursor or permits the closure to run and persist its result. Sleep and receive use the same step cursor.
- **Failures are workflow-level terminal/transient or worker failures.** Immediate retry is the only policy. Terminal, worker, success, and aborted outcomes archive the job.
- **Cancellation is an input event.** A running job emits an `AbortRun` effect; a waiting job cancels its timer and archives immediately. Cancellation has no durable cleanup protocol and can be delayed behind occupied worker queues.
- **Snapshots are derived reducer state.** They include active jobs, archived jobs, full run history, steps, inboxes, waits, and the outstanding effects map.
- **Known persistence weakness.** `KvStoreLog` reserves the frontier separately from writing the entry and explicitly admits multi-writer crash holes.

These mechanisms reveal the baseline invariants worth carrying forward: one serial state owner, durable work before execution, publish-after-commit, replayable positional steps, warm/cold equivalence, stale-work fencing, and explicit cancellation input. The concrete archive/effects-map/log implementation is not itself protected.

#### Replacement classification

**Retain**

- [x] Pure authority decisions followed by atomic journal append and only then publication/intents.
- [x] One single-writer authority owner with disposable native/Wasm sessions.
- [x] Lease and job-incarnation fencing, replacing implicit `runs.len()` ordering.
- [x] Atomic storage contract replacing frontier reservation plus later entry writes.
- [x] Durable timer/message waits and recovery from their recorded state.
- [x] Positional replay augmented with durable step kind and optional boundary name.
- [x] Common WDK semantics with separate native and Wasm adapters.
- [x] Compact terminal state in place of main's unbounded archive and run history.

**Revise before adoption**

- [x] Remove automatic key reuse; permanent idempotency identity eliminates the old-handle/new-incarnation ABA path (WFR-006).
- [ ] Turn unexpected session loss into fenced revoke/recovery or a classified durable failure instead of panicking the authority actor.
- [ ] Move potentially expensive host/Wasm spawn work outside the single-writer command loop while preserving lease fencing.
- [x] Remove durable compensation/`defer` and its cleanup-failure semantics from this PR (WFR-004).
- [x] Preserve main's abort-oriented cancellation as immediate durable fencing and terminal cancellation; no compensation or second cooperative mode in this PR (WFR-004/WFR-005).
- [ ] Replace unbounded immediate retry/restart defaults with deliberate engine defaults and an explicit unbounded opt-in.
- [x] Keep idempotency identity permanent in this PR; remove automatic expiry/reuse and leave result retention plus administrative GC as separate future policy (WFR-006).
- [ ] Bound snapshot/compaction work and specify stale-reader behavior.

**Remove before closeout**

- [ ] Main's reducer/effects engine, worker queues, effects map, archived full job histories, old `KvStoreLog`, and compatibility entry points after the accepted replacement reaches production integration.
- [ ] Branch-only legacy accommodations, including non-durable cleanup behavior in the old worker path.

### 3.3 Execution, recovery, retry, and crate-boundary audit

#### Execution and failure boundaries

Main distinguishes a workflow result from a worker result: a job-specific worker failure is appended as a `JobRunEvent` and archives that job, while reducer/effect-worker invariant failures crash their owning task. Recovery reschedules durable outstanding work. The replacement should preserve that boundary rather than panic the whole authority actor when one session ends or one host cannot instantiate a job.

- [x] **Retain:** recovery clears process-local leases and re-grants active jobs; this is the authority equivalent of main rescheduling outstanding `RunJob` effects.
- [x] **Retain:** stale lease results receive no continuation and cannot mutate the current job.
- [ ] **Revise:** replace `next_yield().expect(...)` with explicit session-loss handling. Current-lease loss becomes a classified worker/host outcome or a fenced re-grant according to the failure class; stale-lease loss is discarded.
- [ ] **Revise:** host spawn/instantiation failure must settle or retry the affected job, not panic the authority actor. Missing pre-registered workflow remains an admission/configuration invariant.
- [ ] **Revise:** do host/Wasm instantiation outside the single-writer actor. Its completion returns with the lease identity and is fenced like every other asynchronous result, so slow linking cannot block cancellation, messages, or deadlines.
- [ ] **Revise:** make explicit revoke an internal recovery/administrative primitive unless a real external caller needs it; public API should describe product operations rather than expose lease machinery.

#### Retry policy

Main's `RetryPolicy` has only `Immediate`, but making it an enum and permitting per-job/per-error override establishes an intended policy extension point. The replacement's fixed and capped-exponential schedules fit that intent and prevent every transient failure from becoming a hot loop. Retry budgets are also a legitimate liveness bound, but unlimited defaults merely preserve main's accidental behavior.

- [x] **Retain:** durable submission retry policy and one-failure override.
- [x] **Retain:** fixed and capped-exponential delay calculations as pure policy; the runtime supplies clock/jitter inputs.
- [ ] **Revise:** define finite engine defaults rather than `Immediate` plus unlimited retries/restarts. Keep unlimited behavior as an explicit opt-in.
- [ ] **Revise:** simplify restart accounting after worker-loss semantics are settled; do not preserve counters or model actions that existed only to support removed cleanup phases.
- [ ] **Verify:** retries never consume the single-writer loop, due deadlines survive restart, stale deadlines are harmless, and a retry budget is charged exactly once per failed activation.

#### Crate boundary

The guest-facing WDK currently depends on the entire `wflow_core` crate to share a handful of protocol types. That reverses the natural dependency boundary: workflow code should not compile the authority, journal traits, reducer, Quint harness hooks, and tracing surface merely to name a step or retry policy.

- [ ] **Revise:** restore a leaf protocol/interface boundary. Put guest-visible step metadata, retry declarations, job context/results, and error envelopes in the WDK interface layer (or a tightly scoped existing interface module); let the authority translate or depend inward. Do not add another crate unless the cycle cannot be eliminated cleanly.
- [ ] **Verify:** the Wasm guest closure contains no authority, journal, native runtime, or formal-model dependencies.

### 3.4 Persistence audit

The replacement atomic namespace contract is a sound correction to main's split frontier reservation and entry write. Memory and SQLite provide the same revision-checked batch mutation and coherent multi-key/range read abstraction. SQLite pins reads to one WAL snapshot, publishes revision hints only after commit, distinguishes definite rollback from unknown commit outcome, and excludes concurrent namespace writers across factories/processes.

**Retain**

- [x] One namespace revision and compare-and-swap batch commit.
- [x] Journal metadata, transition entry, and secondary-index mutations in one atomic commit.
- [x] Unknown commit outcome poisons the open journal writer until reopen/reconciliation; it is never blindly retried.
- [x] Snapshot blob and published snapshot cut committed atomically before covered entries are reclaimed.
- [x] Recovery rejects missing published snapshots, non-contiguous suffixes, incompatible formats, and partition-incarnation mismatch.
- [x] Bounded ordered scans use the same byte ordering in memory and SQLite.

**Revise or specify**

- [ ] Chunk compaction: current `Journal::compact` materializes one delete mutation per covered entry and submits all deletions in one storage commit. Define a bounded reclamation cursor while keeping the published snapshot as the recovery boundary.
- [ ] Automatic policy: define when the runtime snapshots and requests reclamation; a public manual `compact()` call is a mechanism, not an operating policy.
- [ ] Cursor contract: specify what a reader starting at or behind `compacted_through` does. Recovery can use the snapshot; an observational tail needs an explicit reset/error contract.
- [ ] Notification scope: SQLite revision watches are process-local. Keep that limitation explicit while wflow is a single-process local runtime; do not imply cross-process tail wakeups.
- [ ] Durability contract: SQLite uses WAL `synchronous=NORMAL`, which protects process-crash atomicity but may lose recently acknowledged commits on power loss. State this as the selected durability level or make it configurable at the database edge.

### 3.5 Workflow API and formal-model audit

#### Programming model

Main's useful programming abstraction is small: ordered `effect`, `sleep`, and `recv` boundaries over a positional replay cursor, with workflow-level terminal/transient errors. The replacement should keep that shape rather than expose authority concepts to workflow authors.

- [x] **Retain:** `effect`, `sleep`, and `recv` as the complete initial boundary set.
- [x] **Retain:** closures run only for an active effect; completed effects deserialize their durable value without rerunning the closure.
- [x] **Retain:** nested boundary entry is rejected, preventing two operations from claiming one positional cursor.
- [x] **Retain:** durable step kind checks. Main could replay a sleep/receive/effect value through the wrong operation at the same position; rejecting that is a correctness improvement.
- [x] **Retain:** optional stable boundary names and replay name checks. They detect reorder/rename divergence without forcing names into the simple API. Document that unnamed adjacent boundaries of the same kind retain main's positional limitations.
- [ ] **Remove:** `Defer`, cleanup causes, interruption-for-cleanup, `begin-cleanup`, and all associated WIT variants (WFR-004).
- [ ] **Remove or redesign:** step `attempt_id`/`failed_attempts` in the replacement path. The authority never commits failed step attempts and asserts every attempt is zero; carrying main's per-step-attempt representation now creates false complexity. If failure diagnostics are needed, model activation failures directly.
- [ ] **Simplify:** after removing cleanup, `BackendError` should represent replay divergence and protocol/transport failure only. Cancellation is host/session control, not an error delivered into workflow code.
- [ ] **Split WIT:** remove legacy ingress, partition-host, metastore, and cleanup declarations from the replacement guest contract. The WDK world should import only the types and host calls workflow code uses.

#### Error classes

- [x] Keep workflow `Terminal` versus `Transient`; this is main's intentional retry boundary.
- [x] Keep replay divergence as a distinct classified job failure rather than pretending it is transient.
- [ ] Define a compact worker/host failure envelope for session loss and instantiation failure; do not serialize arbitrary internal debug prose as the stable API.
- [ ] Keep invariant violations as crashes. User workflow failure, host availability, cancellation, and storage/network errors at their boundaries are not invariants.

#### Formal model boundary

The pure authority plus a conformance driver is valuable. The model should cover durable semantics, not every runtime data structure or speculative feature.

- [x] **Model:** admission/idempotency, lease fencing, ordered steps, kind/name divergence, waits and message/timer races, cancellation, retry scheduling/exhaustion, terminal receipts, and recovery from snapshot plus suffix.
- [ ] **Remove from model:** cleanup/defer and automatic receipt expiry/key reuse.
- [ ] **Avoid modeling:** host task scheduling, Wasm instantiation internals, queue implementation, snapshot cadence, and compaction batch size. Pin those with Rust fault tests and benchmarks.
- [ ] **Report honestly:** Quint simulation and depth-3 Apalache are bounded evidence, not proof of liveness or approval of the encoded policy. Record versions, bounds, invariants, and uncovered actions with every verification run.
- [ ] **Keep model and Rust vocabulary aligned:** every accepted command/transition/rejection should have one semantic counterpart; remove compatibility-only variants rather than teaching the model both engines.

### 3.6 Production cutover and deletion audit

The replacement is not integrated merely by changing `daybook_core`: the facade and Wasm/native hosts still expose both execution protocols.

- `src/daybook_core/rt.rs` imports legacy job events, partition log/state, and `TokioPartitionWorkerHandle`, then calls `wflow::start_partition_worker`.
- `src/wflow/lib.rs` constructs legacy metastore/log/snapshot stores and exposes `start_partition_worker[_with_native_host]`; it does not expose a production authority constructor.
- `src/wflow_tokio/lib.rs` publicly exports both `authority_runtime` and `partition`.
- `src/wash_plugin_wflow/lib.rs` is a dual host: the new `Host::spawn` adapter shares the component/session machinery with legacy partition service, metastore, ingress bindings, `run_job`, and journal-shaped replay.
- `src/wflow_wdk_native` is also dual-purpose: its replacement session implementation still imports legacy `effects`, `job_events`, and `state` to service the old worker.
- The broad legacy integration suite under `src/wflow/test/` primarily asserts legacy log/effect shapes. Keep only behavioral cases rewritten against the replacement; do not mechanically port journal-shape assertions.

Deletion endpoint after migration:

- `src/wflow_core/partition/{reduce,effects,job_events,log,state}.rs`
- `src/wflow_core/kvstore/log.rs` and legacy log/snapshot/metastore traits/implementations not used by another subsystem
- `src/wflow_tokio/partition.rs` and `src/wflow_tokio/partition/`
- `src/wflow/ingress.rs` and legacy worker constructors/context fields
- legacy partition-host/metastore WIT worlds and linker implementations
- legacy `run_job`/journal adapter paths in native and Wasm hosts
- legacy log-shape tests, replacing only distinct behavior and fault cases

Cutover must therefore be one coherent dependency migration. Temporarily compiling both paths during the edit is acceptable; shipping both or adding adapters between their journals is not.

### 3.7 Validation outcome

All initial audit leads have a disposition:

- [x] Key/incarnation ambiguity is eliminated by permanent idempotency identity; no automatic expiry/reuse.
- [x] Unexpected worker/session loss is a recoverable activation failure, while explicit revoke remains internal.
- [x] Host creation moves outside the authority actor and returns under lease fencing.
- [x] Cleanup/defer machinery is removed rather than repaired.
- [x] Cancellation is durable abort-oriented fencing without a second cooperative-cleanup mode.
- [x] Retry/restart defaults become finite; unlimited behavior is explicit.
- [x] Result/idempotency identity is separate from future administrative garbage collection and journal compaction.
- [x] Snapshot and compaction work is bounded and automatic.
- [x] The production migration/deletion endpoint is enumerated in §3.6.

## 4. Target replacement architecture

### 4.1 Crate and dependency shape

```text
wflow_wdk                 portable workflow API + protocol values
    ↑              ↑
wflow_core        native/wasm adapters
    ↑                   ↑
wflow_tokio ───── host/session traits ───── wash_plugin_wflow
    ↑
  wflow facade / production construction
```

- `wflow_wdk`: leaf portable interface. Owns step metadata, workflow context/results, retry declaration, replay/session protocol values, and the `effect`/`sleep`/`recv` programming API. No dependency on authority, journal, Tokio, Wasmtime, or storage.
- `wflow_core`: pure authority state machine plus atomic store/journal contracts. Depends on WDK protocol values where durable state uses them. No runtime tasks or host implementation.
- `wflow_wdk_native` and `wflow_wdk_wasm`: execute the same WDK protocol in their respective environments; no policy decisions.
- `wflow_tokio`: single-writer authority actor, deadline wheel, asynchronous activation lifecycle, storage orchestration, and native host adapter.
- `wash_plugin_wflow`: Wasm component host and session adapter. It reports activation events; it does not own durable job policy.
- `wflow`: production construction/facade only. It must start the authority runtime, not the legacy partition worker.

No additional crate is planned. If moving protocol values into the WDK creates a real dependency cycle, stop and revisit rather than adding features or duplicate mirrored Rust enums.

### 4.2 Durable state machine

Accepted commands after simplification:

- `Submit`: durably admits a permanent job key and invocation/retry configuration.
- `Grant`: assigns a fenced activation lease.
- `CommitStep`: records one effect result with its positional kind/name identity.
- `BeginWait`, `FireTimer`, `SendMessage`: own durable suspension and resolution.
- `Retry`: releases a lease and records the next activation deadline.
- `Revoke`: internal worker-loss/administrative fencing primitive.
- `Complete`: settles success or workflow/host failure into a compact permanent receipt.
- `Cancel`: atomically fences active work and settles a compact cancelled receipt.

Remove `Fail`, cleanup transitions/intents, defer steps, receipt `Expire`, retention deadlines, and automatic key reincarnation.

### 4.3 Runtime lifecycle

1. A caller submits a key and receives a handle bound to that permanent identity.
2. The actor commits admission and grant before execution starts.
3. Host/session creation runs asynchronously outside the actor and returns tagged with its lease.
4. Each yielded boundary becomes an authority command. Only committed intents resume or deliver to the session.
5. Unexpected current-lease session loss is durably revoked and retried within the restart bound; stale loss is discarded.
6. Cancellation commits terminal cancellation first, fences the lease, drops the session, clears waits/deadlines, indexes the receipt, and wakes result waiters.
7. Recovery loads snapshot plus suffix, clears process-local leases, reconstructs deadlines, and re-grants active jobs.
8. Snapshotting and bounded reclamation are automatic runtime maintenance; compact receipts preserve submission idempotency.

### 4.4 Delivery order

1. Remove unapproved semantics: compensation/cleanup and receipt expiry/key reuse.
2. Restore the WDK leaf boundary and simplify attempts/errors/WIT.
3. Simplify the authority and Quint model to the accepted command set.
4. Fix asynchronous spawn, session-loss recovery, cancellation, and finite retry defaults.
5. Finish bounded automatic snapshots/compaction.
6. Switch the production facade to `AuthorityRuntime` and update the new-system caller surface.
7. Delete the legacy engine and compatibility-only tests/types.
8. Run focused verification, fault matrix, then benchmarks and final guardrails.

## 5. Decision log

Add entries here only after discussion or when a correctness requirement leaves no legitimate product choice.

| ID | Decision | Status | Consequence |
|---|---|---|---|
| WFR-001 | Replace the legacy engine rather than maintain two implementations. | Accepted | Compatibility shims and downstream migration work are deferred; obsolete paths are deleted before PR closeout. |
| WFR-002 | Treat main as trusted and the branch as unreviewed. | Accepted | Existing branch checklists and model behavior must be re-audited. |
| WFR-003 | Keep agent identity/history outside the workflow execution log. | Accepted | The replacement is evaluated as a durable task runtime, not as the complete agent architecture. |
| WFR-004 | Durable compensation/`defer` is out of scope for this PR. | Accepted | Remove the cleanup phase and its WDK/WIT/native/Wasm/model surface. Keep cancellation durable and leave an explicit extension point for a future compensation design driven by a real consumer. |
| WFR-005 | Preserve main's cancellation character: cancellation is a durable input that stops further workflow progress; without compensation, the replacement fences the lease, drops the session, ignores late results, and settles cancelled without claiming an in-flight external side effect was undone. | Accepted baseline | This avoids waiting indefinitely for a cooperative boundary while preserving main's abort-oriented behavior. |
| WFR-006 | Preserve main's job-key identity semantics: a submitted key remains an idempotency identity and is not automatically reused. | Accepted baseline | Remove automatic receipt expiry/key reincarnation from the core state machine. Result retention and explicit administrative garbage collection are separate future policy. |
| WFR-007 | A second cancel after any terminal receipt returns `AlreadyTerminal`; cancellation reason is immutable receipt data, not a second idempotent command payload. | Accepted technical policy | Authority has one simple terminal boundary and never re-enters cancellation logic for receipts. |
| WFR-008 | Keep canonical WIT files in `wash_plugin_wflow` for this replacement; make the Rust WDK a leaf without relocating every routine package. | Accepted technical scope | Split replacement and legacy declarations in place; F deletes legacy worlds. |
| WFR-009 | `RetryPolicy` is portable workflow protocol; `RetryBudget` is engine/submission policy owned by core. | Accepted boundary | Avoid exposing host restart policy to guest workflow code. |
| WFR-010 | Defaults are capped exponential retry (100 ms base, 30 s cap), 8 transient retries, and 3 host restarts; unlimited behavior requires explicit configuration. | Accepted initial operational policy | Prevent immediate infinite loops; benchmarks may tune finite numbers without changing semantics. |
| WFR-011 | Repeated host/session loss settles as `RestartsExhausted` without persisting arbitrary debug prose. Missing workflows at recovery fail runtime startup; disappearance after valid admission is an invariant violation. | Accepted failure boundary | Stable receipts remain compact; configuration failure is handled at the runtime edge. |
| WFR-012 | Spawn futures finish off-actor and are fenced on completion rather than relying on cancellation safety. | Accepted lifecycle policy | Cancellation stays responsive and late sessions are immediately dropped; actor never awaits instantiation. |
| WFR-013 | Automatic maintenance inherits main's 100-transition/60-second delayed cadence and publishes a final snapshot on graceful shutdown; reclamation is paged with configurable default 512 and need not drain at shutdown. | Accepted baseline | Preserve main's cadence while bounding each reclamation transaction. SQLite `synchronous=NORMAL` remains the documented selected level for this PR. |

## 6. Implementation ledger

No implementation item is approved merely because code already exists on the branch.

### 6.1 Ordered implementation work

The audit and target architecture are complete enough to resume implementation. No open item currently requires an operator/product decision.

#### Phase A — remove rejected policy

- [ ] Remove cleanup/defer types, transitions, intents, session paths, tests, WIT declarations, and Quint actions.
- [ ] Replace cancellation-with-cleanup with atomic terminal cancellation and lease fencing.
- [ ] Remove receipt expiration, retention deadlines, reincarnation-by-key, and key-reuse tests/model actions.
- [ ] Keep compact permanent receipts and duplicate-submission comparison.

#### Phase B — establish the interface boundary

- [ ] Move portable step/retry/result/replay/session protocol values out of `wflow_core` into leaf `wflow_wdk` modules.
- [ ] Remove `wflow_wdk -> wflow_core`; make the pure authority consume the WDK protocol.
- [ ] Remove zero-only step attempt fields throughout the replacement path and replace any desired diagnostics with explicit activation counters/events.
- [ ] Reduce the replacement WIT world to workflow types and host calls; remove legacy ingress/partition/metastore declarations from it.
- [ ] Keep native and Wasm adapters behaviorally identical over that protocol.

#### Phase C — simplify and prove the authority

- [ ] Reduce command/transition/intent enums to §4.2 and update snapshot validation.
- [ ] Preserve permanent key identity, unique job incarnations, fenced leases, wait identity, compact terminal receipts, and strict replay kind/name checks.
- [ ] Simplify Quint to the accepted semantics and update generated conformance traces/invariants.
- [ ] Add focused race tests for cancel versus step commit, wait resolution, retry deadline, session loss, and late stale results.

#### Phase D — correct runtime lifecycle

- [ ] Move host/session creation out of the authority actor; route completion back with lease identity.
- [ ] Convert spawn and unexpected session loss into durable revoke/restart behavior rather than actor panic.
- [ ] Select finite default workflow retry and host-restart budgets; require explicit opt-in for unlimited policy.
- [ ] Verify one charge per failed activation, durable retry deadlines, stale deadline suppression, and nonblocking unrelated jobs.
- [ ] Make result waiters depend on permanent receipt publication, not expiry guards.

#### Phase E — bounded persistence maintenance

- [ ] Add automatic snapshot cadence and bounded compaction pages.
- [ ] Publish a complete snapshot before advancing reclamation; never expose a cut whose recovery suffix has a hole.
- [ ] Specify readers behind `compacted_through` as stale and require reload from snapshot.
- [ ] Test crash/failure injection at journal commit, snapshot publish, and every compaction page boundary.
- [ ] Document process-local watch scope and SQLite `synchronous=NORMAL` power-loss semantics.

#### Phase F — production cutover and deletion

- [ ] Change the `wflow` production facade and `src/daybook_core/rt.rs` to construct/use `AuthorityRuntime` through the new API.
- [ ] Migrate all active workflow callers and tests; do not retain compatibility entry points.
- [ ] Delete legacy reducer/effects/effect-worker/persistence modules and old partition startup.
- [ ] Remove obsolete feature flags, generated bindings, dependencies, and API snapshots.
- [ ] Confirm one production workflow engine remains by dependency and symbol search.

### 6.2 Verification and closeout

- [ ] Focused unit and state-machine tests for accepted authority transitions.
- [ ] Recovery and crash-boundary tests against both memory and SQLite stores.
- [ ] Native/Wasm behavioral conformance for the retained programming model.
- [ ] Formal properties and bounded verification with tool versions and bounds recorded.
- [ ] Benchmarks only after semantics and architecture settle.
- [ ] Delete legacy engine and compatibility entry points.
- [ ] Update public API snapshots and downstream callers at the end of the PR.

## 7. Fleet delivery ledger

This is the manager-owned source of truth for delegated delivery. Children may report evidence, but only the manager changes lane state or accepts a handoff.

Implementation topology: **multi-seam**. Authority policy, portable WDK/WIT, runtime lifecycle, persistence maintenance, formal model, and production cutover are independently testable contracts. Each mutation lane receives an isolated worktree and exclusive files; integration is a separate lane after component handoffs.

Verification policy for the implementation waves: workers run only narrow compile/tests needed to avoid handing off obviously broken code. Broad clippy, conformance, fault matrices, formal checks, and benchmarks are deliberately deferred to the integration/verification waves.

| Lane | Exact decision/deliverable | Claimed contract/files | Isolation | State | Next gate / handoff |
|---|---|---|---|---|---|
| A-policy | Remove cleanup/defer and receipt expiry/key reuse from durable authority semantics | `wflow_core/partition/authority*`, conformance Rust projection, one journal fixture | managed worktree | Writer queued | focused authority tests; committed handoff |
| B-sdk | Remove compensation from programming API; define leaf Rust protocol and minimal replacement WIT | `wflow_wdk*`, `wflow_wdk_{native,wasm}*`, WIT split/protocol adaptation | managed worktree | Writer queued | focused WDK tests/build; committed handoff |
| C-runtime | Async activation/session-loss/cancel lifecycle and finite defaults | `wflow_tokio/authority_runtime*`, host/session traits | managed worktree after A/B contracts | Planned | focused runtime tests; committed handoff |
| D-storage | Implement bounded journal snapshot discovery, typed stale readers, and paged compaction | `wflow_core/kvstore/journal*`, focused memory tests only in first slice | managed worktree | Writer queued | focused journal tests; committed handoff |
| E-formal | Remove rejected actions and align model vocabulary with accepted authority | Quint/formal/conformance model files | managed worktree after A contract | Pending | bounded model checks later; committed handoff |
| F-cutover | Integrate component handoffs, switch production facade/caller, delete legacy engine | `wflow`, `daybook_core`, legacy modules/hosts/tests | integration worktree only | Pending | targeted build, then review wave |
| V-review | Fresh-context review of integrated diff | read-only whole diff | no mutation | Pending | blockers disposition |
| V-verify | Clippy, focused tests, fault/recovery matrix, native/Wasm conformance, formal checks, guardrails | integrated tree | one verification owner | Pending | evidence ledger |
| V-bench | Phase-5 benchmark and closeout | integrated tree | verification owner | Frozen | starts only after architecture/semantics integrated |

### Fleet events

- 2026-11-04: Audit accepted; baseline commit `967e24bd`. First scouting wave commissioned on the exact source seams above.
- 2026-11-04: Scout workflow `e1a21bee-7195-488f-8c9f-96afb41e6dc1` launched with four fresh-context `openai-codex/gpt-5.6-sol:high` lanes. Revisit trigger: native workflow completion notification.
- 2026-11-04: Four scout handoffs accepted. Manager resolved their boundary/default questions as WFR-007..013. Writer wave A/B/D partitioned into isolated, non-overlapping worktrees; runtime/formal lanes remain dependency-blocked.
