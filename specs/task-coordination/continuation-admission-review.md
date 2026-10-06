# Continuation admission before RunJob — current-source counterexample

Independent read: wflow_core/partition/reduce.rs; wflow_tokio/partition.rs ExecutionGate; partition/effect_worker.rs admission, deferred effects and timers; rt.rs startup/start/expiry and managed publication; rt/task_adapter.rs gate hold/releases. No Rust/Cargo tests run.

## Actual path ledger

| Path | Reducer / driver boundary | Implemented authorization |
|---|---|---|
| New job | reducer emits RunJob | Tokio active/held-job gate, no deadline |
| Timer sleep/wake | WaitTimer registered; fire_due_timers logs JobTimerFired; reducer completes matching wait and emits RunJob | wait identity fence; no immutable deadline check |
| Message wait | exact message/wait consumed; queued message or later receipt emits RunJob | received-message dedup; no deadline check |
| Transient retry | Immediate retry policy emits RunJob after error | cancellation reducer branch, not deadline |
| Successful step effect | completion emits next RunJob | step/attempt identity; no deadline |
| Snapshot/log restart | outstanding effects restored and queued; partition initially paused | rt startup holds mapped PreparedAttempt jobs, skips expired workload loading; inherited-only jobs not held through task_attempt mapping |
| Deferred effect release | run_deferred_effects rechecks ExecutionGate::permits, then handle_partition_effects | active + RunJob job not held only |
| Independent expiry | host discovers due root/descendants and requests cancellation | request and reducer incorporation lag; not pre-action admission |
| Managed effect/final success | retained deadline at merge/start/SQLite complete | actual finite fences; cannot undo prior execution or external effects |

Effect-worker admission check precedes service/context awaits, so an eventual deadline-aware decision must also specify its authorization linearization immediately before action, not only early queue sampling. Deadline propagation API, unknown authorization behavior and persistent job binding are parent design decisions. No engine policy selected here.

Concrete counterexample: released active authorized job waits until deadline20; wall time21; host requests cancellation but reducer has not incorporated it; timer/message/retry schedules RunJob; ExecutionGate active=true and held=false permits service execution at21. Startup descendant case: retained inherited child job restores while gate paused; child lacks PreparedAttempt lookup, so startup holds no job ID through that mapping; activation permits outstanding RunJob even if rt skipped expired workload boot. Actual availability of service determines observable execution, not engine authorization. Missing gate exists even where bundle unavailable accidentally prevents execution. Reported to parent, not a native reproduction claim.

## Executable artifact and evidence

continuation_admission.qnt contains two profiles over identical actions: initActual models deadline-free gate; init models **required-contract diagnostic**, adding pre-action conjunction/min(parentDeadline20, direct taskDeadline30). The latter is not implemented-source proof. Wake tags1..5 (timer/message/retry/step-success/restore), exact pending kind, cancellation request vs incorporation, gate hold/release, activation, one crash and times10/21. Ghost executions independently check action time<=20. Existing model domains/properties untouched.

`node runner.mjs continuations`: exit0 `/tmp/quint-continuations-accepted.log`; three witnesses demonstrate actual-source bad schedules. Required-contract safety passes161451 generated /7716 distinct states. Current-source preActionAuthorization fails TLC; ignoring inherited deadline also fails. Both failures have actual violation evidence, not parser errors. Initial `/tmp/quint-continuations-final.log` parser failure used reserved word run as action name; renamed execute, preserved failed log and reran.

No conditional progress claim. Cancellation-resistant running routines, managed host syscall admission, long awaits after gate checks, reducer stream ordering, durable job->authorization binding, arbitrary retries, sleep/restart clock rollback/skew and historical child migration remain unresolved. This is meaningful pre-action counterexample evidence, not eventual-terminal safety substituted for continuation authorization.

## Source changes reported by parent

execution_deadline now takes min(original task deadline, inherited witness), superseding earlier precedence concern. This model exercises direct30/inherited20 conjunction; descendant_authorization's inherited-only profile remains accurate but does not itself explore both witnesses. Parent pending delivery tests pass independent draining and readiness; eventual actual materialization still not demonstrated here. Parent Router safety/guards and typed Rust companion tests are reported evidence only. No whole-system proof.

Recommended fixes for parent evaluation: retain exact job->dispatch/deadline authorization across restore; bind immutable effective deadline into RunJob admission; check at actual service execution boundary including deferred/cache resume; keep AbortRun/CancelWait available to drain expired jobs. These are source-grounded required seams, not an approved new generic engine interface.
