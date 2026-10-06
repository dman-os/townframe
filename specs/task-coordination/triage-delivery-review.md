# Triage source ordering and pending captured-domain delivery

This bounded item leaves router models untouched. Source seams independently read: triage.rs refresh_processors/install_activation_snapshot/query_activation/DocProcessor source_boundary path; triage/domain.rs ProcessorDomainLoad; rt.rs completion watcher/callback; triage.rs processor_slot_store. Existing triage_activation actions/property preserved, query-status witness added. New triage_delivery imports existing activation and processor_slots reducers.

## Branch correspondence

| Source branch | Model / coverage | Remaining obligation |
|---|---|---|
| Same activation target, unavailable manifest, not rejected | install retains ack/boundary; enabledManifestPendingRetainsActorTest retains actor | async snapshot/config changes not all modeled |
| Changed available target | install samples current source boundary, same enabled ref retains processor generation | existing changedTargetSamplesBoundaryAndDisableFencesTest |
| Rejected/disabled desired target | refresh withdraws actor; query returns Rejected/Disabled before stale ack | rejectionAndDisableWithdrawTest/queryCurrentTargetStatusTest |
| Attached shared domain Pending or referenced manifest missing | pending_shared retains actor, pending_activation inhibits evaluation | refresh pending guard; readiness is explicit |
| Shared domain Unauthorized | refresh skips definition, withdrawal permitted | modeled rejected class; other metadata errors crash, not recovery |
| Enabled plug manifest absent | pending_plugs prevents empty inventory withdrawing attached actors | dedicated witness and withdrawal mutant |
| DocProcessor pending_activation | source processing returns None retaining work | evaluate disabled while pending |
| revision <= source_boundary | no historical evaluation | activation eligibility/query witness; source1->2 composition |
| Domain Ready/config materialized | source evaluate allowed when target/actor enabled and strict boundary | evaluate/materialize transitions |
| Durable terminal commit | outbox retained independent of actor/current producer observation | complete/domainPending/crash witness |
| ProcessorSettlement callback | exact captured slot/capture, selected logical owner; no current desired capture substitution | settle imports processor_slots, settles capture1 only |
| Terminal receipt | only after original capture settlement/outbox drain | safety and early-delivery mutant |

Model profile: one actor/target, two source revision classes, original capture1, desired slot initialized capture1, one crash, booleans for manifest/domain materialization and durable terminal/outbox/settlement. Disable/reject and repeated Pending/materialization remain enabled. Terminal status, producer observation, settlement, delivery and receipt are separate states. No arbitrary per-document source interleaving, generation changes, native authority proofs, or full DocProcessor predicate/plan coverage claimed.

Conditional progress: eventual stable online repository **and captured-domain materialization**, plus separate weak fairness of settle, delivered, producerObserve and receipt. It establishes terminal->eventual receipt, not eventual task creation/execution. Source evaluation requires manifest/domain readiness and activation boundary; fairness does not supply either. Disabling an actor does not erase a retained completion obligation.

## Concrete runtime gap reported to parent

`processor_slot_store` returns Ok(None) for domain Pending(authority_pending=false), and CoordinationError::Pending for authority_pending=true (including selected owner). ProcessorSettlement callback converts None into generic `captured processor settlement domain is not materialized` error. `watch_dispatch_completions` propagates delivery error with `?`. Thus model settle-until-materialized is an intended retained retry obligation, NOT established runtime callback liveness. Outbox retention prevents receipt fabrication but does not itself make watcher recovery correct. Parent received both concrete source locations; no Rust change or policy chosen here.

## Executed evidence

`node runner.mjs triage-delivery`: exit0 `/tmp/quint-triage-delivery-final.log`; three activation and three delivery witnesses; activation safety25 distinct states, composed safety1232 states and conditional temporal progress1232 states. Two controls: enabled actor withdrawn during manifest Pending -> QNT508; delivered outbox before original capture settlement -> TLC safety violation. Source classes/errors outside profile remain explicit.

Initial `/tmp/quint-triage-delivery-gate.log` exposed Quint self-import alias flattening failure when composition shared file with its imported activation module. Kept source-owned activation module unchanged and composition in triage_delivery.qnt; v2/final passed. No discarded actions/properties to bypass compiler.

## Parent-requested durable admission correction

Parent review found lifecycle queued state incorrectly surviving process crash before SQL preparation. Historical model and original queued->progress property retained in lifecycle_composition_historical_queue.qnt, marked historical incorrect abstraction. Current crash drops queued-but-unprepared requests; prepared acknowledged SQL admission survives. Corrected progress obligation is prepared->release/failure-delivery under original fairness/environment, not invented durable mpsc queue. New crashDropsUncommittedAdmissionTest and crashRetainsAcknowledgedAdmissionTest plus invent-durable-admission-queue mutant detect error. Caller-cancellation ownership evidence remains separately in host_admission_ownership.qnt; repository worker survives caller cancellation, not process crash. No claim that unacknowledged submission is guaranteed without client replay.

`node runner.mjs lifecycle`: final exit0 `/tmp/quint-lifecycle-durable-admission-final.log`;25 named/imported tests, corrected bounded safety171987 distinct states (depth13), replay temporal46824 states each (depth21), six expected violations. Prior `/tmp/quint-lifecycle-durable-admission.log` stopped at ambiguous old mutation site after queue edit; mutation anchor corrected and full gate rerun. No property silently removed: historical incorrect contract remains inspectable.

## Whole-system scope

Still missing: actual Pending delivery recovery/wake ownership, all triage predicate/config/source interleavings, arbitrary partitioned slot generations, exact native effect bridge, durable successor domain decisions/notification collection, descendant/continuation complete enforcement, clock/rollback/GC contracts. Parent reports descendant SQL reopen fence passed, but this item does not verify that active seam or claim whole-system proof. Independent review/rerun required.
