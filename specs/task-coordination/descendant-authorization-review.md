# Descendant finite authorization / host expiry correspondence

Independent source read: rt/dispatch.rs add_with_task_attempt, execution_deadline, expired_descendants, next_task_expiry, complete_owned, cancellation/finalization CAS; rt.rs startup exclusion, start_active_dispatch, host expiry loop and managed publication path. No Rust/Cargo/native execution by worker.

## Source-grounded boundaries

- Repository-owned Add queue survives submitting caller cancellation. Precommit requests are volatile across process crash. Child dispatch and exact parent/deadline witness commit in one BEGIN IMMEDIATE transaction; SQLite unixepoch predicate refuses expired admission and rolls back dispatch insertion.
- CommandInvokeReply resolves current active parent by workflow job. For authorized parent, terminal status rejects child admission; retained inherited deadline propagates to grandchild unchanged. Parent identity retained in witness; parent finishing does not remove child's authorization.
- execution_deadline resolves original PreparedAttempt deadline first, otherwise retained descendant witness independent of pending flag. Completion clears pending, not immutable authorization. No deadline renewal on reopen.
- expired_descendants uses strict deadline<now, pending1/cancellation_requested0, ordered deadline/id and bounded LIMIT. next_task_expiry independently considers root and descendant queues, returns their minimum. Model expiryMarked represents cancellation_requested, separate from actual cancellation winner; publication CAS may refuse cancellation even though watcher marks enumeration requested.
- Startup skips loading expired workloads. start_active_dispatch refuses expired start and completes Cancelled. Managed merge receives retained deadline; publication claim does not seal expiry. SQLite complete_owned independently chooses Cancelled for requested success after deadline, removes expiry ownership/clears descendant pending and retains witness, commits outbox with actual winner.
- Ordinary cancellation requested after publication claim returns false without waiting; expiry still fences publication and success. Cancellation prior to claim is durable ownership. Model has distinct request/cancellation and publishing classes; publicationCancellationTooLateTest exercises the refused request.

## Model / evidence

`descendant_authorization.qnt`: parent1/child2/grandchild3, injective IDs/no reuse, explicit parents/deadlines, caller state and volatile queue, admitted durable set, pending index, enumeration mark, actual cancellation, volatile publishing/running, terminal status and independent start/success/effect histories. Times10 (authorized) and21 (expired), immutable deadline20. These are finite classes, not a default lifetime or clock-skew contract. Single crash/restart; publication-failed recovery and full workflow state are not modeled.

Safety independently checks lineage/deadline20, start/success/effect time<=20, retained witness and pending matching nonterminal durable status. Five witnesses: caller cancellation before SQL admission; nested admission then parent finishing; retained expired child restart and requested late SQL success=>Cancelled; publication claim across expiry; cancellation request during publication too late. `complete` is the requested-success SQL seam, not a guarantee that runtime execution succeeded. Managed publish separate; terminal success does not retroactively undo committed external effects.

`node runner.mjs descendants`: exit0 `/tmp/quint-descendants-final.log`;5 tests;1,573,924 generated /147,654 distinct safety states/depth24; four actual TLC violations for deadline extension, missing SQL expiry success fence, missing managed-publication expiry fence, missing start expiry guard. Prior initial gate `/tmp/quint-descendants-gate.log` preserved; refined expiry enumeration mark and explicit cancellation request in final model. No temporal/liveness claim: cancellation-resistant running work, actor wakes, watcher notifications, eventual clock progress and fair settlement remain external obligations.

`node --check specs/task-coordination/runner.mjs`: passed. Source dispatch.rs SHA256 `4314da772a57961d2dac5859a90f53d9b19819d662b781c7368eec9b80484949`; model SHA256 `0f643a1e57194504c456d25b10f6dc379013ecb7d52fea73f1adcc05b4a6474e`.

## Unclosed source contract reported to parent

execution_deadline prefers task_attempt_for_dispatch over descendant witness. If a dispatch can have both a PreparedAttempt and CommandInvokeReply inherited authorization, a longer direct task deadline could supersede shorter inherited deadline. Existing command construction may exclude this shape; not concluded a runtime bug. Model covers inherited-only descendants, not silently assumes min(deadlines). Need explicit ingress exclusivity proof or source decision before claiming all authorized dispatch shapes covered. Parent was notified.

## Missing continuation/migration/whole-system obligations

Historical children admitted before descendant witness table exists have no automatic backfill in the inspected schema creation; recovering parent identity from old hook/job data is not modeled. No migration policy invented. Workflow retry/wake/sleep and internal continuation authorization inheritance, arbitrary child graphs, multiple parent hooks, replaced workflow identities, stale exact-attempt completion CAS, queue admission after parent replacement and future clock rollback/skew remain incomplete. LIMIT1 model checks finite independent enumeration semantics, not throughput or absence of head-of-line blocking in full runtime.

Parent tests/native gates are reported evidence only (`/tmp/parent-descendant-reopen-fence-v2.log`, `/tmp/parent-descendant-native-gate.log`). Parent says captured-domain Pending delivery now retains outbox with delivered=false, adapter outcome None, independent bounded pagination and subscribed wakes. That supersedes prior generic-error implementation finding but targeted runtime validation remains parent-owned; model per-effect fairness/materialization is not runtime liveness proof. Lifecycle/triage/router artifacts preserved. Independent review required.
