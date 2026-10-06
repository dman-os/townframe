# Townframe model architecture and coverage proposal

Status: ARCHITECTURE APPROVED by parent after research review. The first bounded encrypted-register implementation is under `specs/task-coordination/`; its exact coverage/results live beside the model. All remaining scoped modules stay explicitly pending subsequent assignments. Rust source is concurrently changing; this inventory describes inspected source and ADR010/011/012, not a frozen implementation theorem. Parent owns architectural decisions and Rust fixes.

## Scope and organization

Approved location: `specs/task-coordination/`, with ownership-aligned modules for the rows below and eventual `contracts.qnt`, `profiles.qnt`, and `system.qnt` only as needed. Keep a human-readable coverage ledger beside models; source links identify full paths and symbols, not fragile line numbers. The register item uses a self-contained Quint0.33.0 package/lock/runner, with no root dependency/CI changes, Quint Connect dependency or generic generator.

| Proposed module | Rust owner / surface | Mandatory coverage |
|---|---|---|
| `register.qnt` | `src/big_sync_core/encrypted_register.rs` | Pure merge, restore, projection, original/wrapper identity, poison and replay fences. |
| `task_ticket.qnt` | `src/daybook_core/tasks/model.rs`, `store.rs` | Immutable declaration, publisher evidence, terminal joins, raw-register projection and collision rejection. |
| `router.qnt` | `src/daybook_core/tasks/router.rs` | RouterSlot join and every RouterMachine event/action/guard. |
| `worker.qnt` | `src/daybook_core/tasks/worker.rs` | Every PoolWorkerMachine event/action; classify, admission, retained attempts, sessions and finishing. |
| `processor_slots.qnt` | `src/daybook_core/rt/triage/slots.rs` | ProcessorCell evaluate/settle, concurrent observation, desired generation and execution baseline. |
| `triage.qnt` | `src/daybook_core/rt/triage.rs` | Activation, planning, exact capture, deterministic publication, deferred revision and source settlement. |
| `execution.qnt` | `src/daybook_core/rt/task_adapter.rs` and actual dispatch journal contracts | CapturedRoutineAdapter admission, exact retained input, preparation/start/recovery/outcome/receipt incorporation. |
| `driver.qnt` | `src/daybook_core/tasks/driver.rs` | Native actor effect ordering, completion fencing, reader lifetime, checkpoints, shutdown and selection rebind. |
| `transport.qnt` | `src/daybook_core/tasks/storage.rs`, `transport.rs`, `control.rs`, pool/processor control wrappers | Selected register lifecycle, key/source certificates, publication, carry-forward, consumer transactions and permission roles. |
| `system.qnt` | Composition of the above | Small cross-component executions and liveness under explicit dependency contracts. |

Pure machines not called `Machine` are included. BigSync scheduling, delta walkers, bucket/cursor/watermark machines are enumerated dependencies, not implicitly verified standalone components. No new retries, fairness mechanism, telemetry, purge policy or product behavior is introduced by modeling.

## Detailed coverage ledger seed

Each entry below must expand into rows with: source operation/event + guard case; named model action; independently computed property; finite profile; negative control; source/trace correspondence; result/status. All rows presently have status proposed/unverified. Merely naming an enum is not full transition coverage.

### Encrypted register — first standalone model

State: logical `RegisterKey(scope, slot)` binding, per-writer lane sequence, semantic originals, canonical outer representations, at most two equivocation witnesses, immutable exact signed-original identity and frontier. Representation incarnation/key/encoding metadata are opaque authenticated wrapper fields; physical selected part/incarnation admission belongs to the host transport model, not the pure reducer. Ghost history records admitted original statements independently of chosen representation. Key availability is separate environment state.

Cover: empty restore; duplicate writer decode; cross-binding/schema/size/noncanonical input rejection without partial mutation; all-signatures-before-merge batch atomicity; lower sequence ignored; higher sequence replacement including poison recovery; equal sequence/same semantic canonical wrapper; equal sequence/different semantic bounded poison; canonical witnesses after merges of poisoned lanes; omissions preserve lanes; arrival permutations/idempotence/commutativity/associativity; same-record causal head projection; cross-record frontier does not dominate; hidden replay lanes retained; exact-original preservation across historical rewrap; local allocated sequence cannot be stale or equivocate (same-sequence same-semantic representation replay is allowed); reserved sequence gaps after failures belong to the host storage model.

Properties: join algebra on valid snapshots; heads derived from semantic causality independently of join implementation; equivocation never appears as a readable live head; witness count bounded while evidence transferable; no accepted malformed batch partially mutates; encrypted/body/key-reference changes cannot alter logical merge rank or signed original; omitted/hidden lanes prevent replay resurrection. Negative controls remove replay lanes, rank by decryptability, accept partial batches, or replace original with rewrapper identity.

### TaskTicket and RouterSlot

TaskTicket: validate full remote identity/schema/declaration and equal-sequence conflicts before mutation; declaration collision immutable; publisher witnesses accumulate canonically rather than overwrite one publisher; per-writer terminal fact success dominates cancellation even at lower sequence; same-outcome sequence precedence; canonical winning success by `(attempt,result)` and cancellation by writer; version digest changes fence async work. Model raw register lane projection separately: register sequence dominance does not automatically implement terminal semantic dominance; TaskStore's retained projection matters.

RouterSlot: stale sequence no-op, increasing sequence generation regression rejected, equal-sequence unequal claim rejected, omission retains. Do not substitute register equivocation poisoning for this distinct join.

Properties: ticket convergence for valid join inputs, immutable capture/declaration, terminal success cannot be downgraded, distinct publishers cannot invalidate authenticated declaration, conflicts fail atomically. Negative controls use last-arrival terminal winner or silently accept declaration collision.

### RouterMachine

Events: AuthorityChanged, ObsolescenceChecked, SlotMerged, HeartbeatObserved, HeartbeatExpired, ConsiderTakeover, ClaimPersisted, ClaimPersistenceFailed, HeartbeatDue, RegisterExecutor, SessionClosed, TaskObserved, TaskRemoved, OfferAccepted, OfferDeclined, AttemptChanged, ReadinessWakeup, RoutingTick.

Actions: CheckObsolescence, PersistClaim, PublishHeartbeat, RejectSession, SendOffer, StartAttempt, CancelAttempt, RejectClaim.

Guard families: eligible current Edit vs Pending/denied; own proposed vs durably persisted claim; max generation, live candidates, protocol then rank winner, no-live fallback; takeover despite stale lanes but not a live winner; claim failure; exact claim acknowledgement; injected unique ID and durable writer-sequence correction; protocol/capability/place eligibility; task nonterminal freshness proof vs terminal bypass; stale obsolescence token/version; offer exact declaration/session/allocation/phase; capacity includes offered and accepted attempts without double count; cancel request retains allocation until actual resolution; decline is local not global cancellation; blocked readiness/capacity/reconnect; session loss frees only owned allocations. Partitioned routers may temporarily duplicate work: do not assert global exactly-once execution.

Properties: leadership only for eligible persisted winner; no start from stale offer/proof; no capacity overcommit within one router's allocation accounting; no globally terminal fact from a local decline. Conditional liveness: stabilized elected router eventually offers an eligible pending task with available executor capacity, subject to fair per-task scheduling and no endless higher-priority arrivals.

### PoolWorkerMachine

Events: ParticipationChanged, Classified, DispatchStarted, RouteDiscovered, RouterLost, OfferReceived, StartReceived, CancelReceived, AttemptPersisted, AttemptPersistFailed, AttemptFinished, AttemptFailed, TerminalFactAccepted, TicketObserved, ReadinessWakeup, OriginAttemptStart.

Actions: ClassifyTask, RegisterSession, AcceptOffer, DeclineOffer, PersistAttempt, DomainTerminalFact, StartDispatch, CancelAttempt, ReportAttemptChanged, WatchReadiness, ReportReadinessWakeup, UnsupportedRouterProtocol.

Guard families: session/protocol/capacity/place; classification request/task/digest/ticket version/session/allocation identity; passive unchanged classification cannot supersede an allocation-owned one; invalidated/superseded classification cannot admit; captured invocation immutable; persist-emitted vs persisted vs start-requested vs start-emitted; cancel before persistence ack; prepared/running restore without duplicated admission; retire old allocations but retain admitted running/origin attempts; terminal invalidates only appropriate work; finishing retains capacity until actual domain incorporation/publication acknowledgement; readiness is a hint not authorization; origin execution uses the same persistence/capacity gates; failure local, success/actual cancellation global as permitted.

Properties: no start before exact persistence ack; at most one emitted start per admitted attempt; stale session completion cannot own new session; no invented cancellation winner; finishing capacity not prematurely released; retained running attempt survives mere reconnect. Conditional liveness: admitted eligible work eventually starts, actual outcome eventually incorporates, and capacity eventually releases under fair effects and terminating adapter outcomes. Best-effort cancellation alone is not a termination assumption.

### ProcessorSlot evaluate/settle

State: per-writer evaluation clock and observed-other evaluations, desired captures, latest settlement and bounded desired settlements. Capture includes processor/config semantic generations, sorted unique target heads, and optional baseline (`None` is distinct from empty heads).

Cover evaluate no-op vs new capture, evaluation-only increment, observations suppressing dominated desires, concurrent desires, exact settlement recognition, settle without invented observations or evaluation advance, old settlement retained when still desired, bounded desired/latest settlement retention, baseline union from successful settlements across writers. Distributed backend uses all register lanes and rejects poisoned originals; semantic observations live inside cells (empty register frontier deliberately). Local publication and membership are separate crash boundaries.

Properties: no desired capture suppressed without observing its evaluation; settlement cannot invent causality; an unrelated terminal ticket cannot manufacture exact slot settlement/baseline; successful baseline retains all acknowledged writer contributions; duplicate exact evaluations suppress duplicate logical tasks. Do not assert one global execution while concurrent desires exist.

### Triage activation, planning and source settlement

Activation: desired target vs installed acknowledgement, manifest/config heads and enabled reference, same target temporarily unavailable retains boundary, disable/rejection distinct from unavailable, changed target gets boundary sampled from source revision, `FutureObservedDeltasOnly` excludes R <= B, pending activation defers settlement. Model installer awaits and config change notifications; selected/deferred source cannot bypass the finite prefix of already admitted config notifications. Restart reconstruction and effective acknowledgements must be mapped before claims.

Planning: logical-main physical branch only; full captured Read set vs changed-key delta; snapshot target vs exact successful delta baseline; predicates and missing materialization; exact current installed manifest after awaited capture; processor/config semantic generation; authenticated source origin vs preference label; deletion invalidates desires independently of filters without invoking routines; distributed managed owner explicit; slot evaluation durable before enqueue; deterministic task H(slot,generation); immutable publisher/source/config witnesses; no invented processor deadline.

Settlement: one deferred input retains the whole source revision; planning can already have published slot changes before a later document defers, so do not model planning as one atomic transaction; enqueue all planned tasks before source settlement; crash after enqueue before checkpoint repeats deterministic identity; derived sparse state and source cursor committed together. Properties: no checkpoint past unmaterialized/unpublished work; captured source/manifests/config identity never replaced with latest data; no retrospective processing across activation boundary; repeat/crash cannot change the logical task. Intended unwitnessed-candidate/reconciliation behavior described in ADRs must be reconciled with actual source, not invented as implemented.

### CapturedRoutineAdapter admission and recovery

State: durable mapping `(task,attempt,digest,dispatch,job,staging)`, exact encoded captured invocation, Prepared/Running/Finishing records, job journal, execution gate and actual outcome/receipt. Admission locks, native rights resampling and awaited materialization are separate transitions.

Cover persist/classify before durable mapping; captured invocation equivalence; fresh pool and native rights at actual admission; subscription before querying retained job; exact prefix/job identity, append or reuse once; deadline/authority check after awaits; exact blobs/manifests/config/source, no current fallback; execution gate held until durable job admission and final checks; retained boot admission; prepared/running/finishing recovery; local failure vs actual success/cancel; receipt exact key/digest/result; local own success requires actual native outcome; remote processor success waits for exact locally observed slot settlement rather than inventing baseline. Model actual recovery implementation before fixing restore classifications in the ledger.

Properties: executable prefix never authorized by stale pre-await check; admitted job and receipt keep original immutable capture across rotation; local classification/failure cannot forge global terminal cancellation; terminal metadata alone cannot create local processor settlement. Liveness requires exact inputs eventually materialize and native execution actually terminates, or explicitly excludes that attempt from the progress claim.

### Native driver effects and checkpoints

Commands Submit/Rpc; external router/worker events; completions Cancelled, Classified, Router, Obsolescence, OfferChecked, Prepared, Outcome, Started, StartCancelled, ClaimStored, SessionLost, SessionOpened, SessionMessage, Ready. Cover all completion token checks and all action-to-effect-to-completion edges.

State: retained reader future, pending revision entries, reader_next, suspended/replay-required flags, action queues, independently spawned jobs, prepared/start-cancel/finishing maps, session and readiness generations, selected transport snapshot. Cover reader cancellation versus retention across select branches; independent effect jobs completing while actor waits on a SQL writer; consume entry only after incorporation succeeds; actions admitted before projection checkpoint, but spawned effects need not all finish before checkpoint (do not strengthen actual contract silently); replay rebuilds volatile state from current part plus durable attempt records; action requeue after publication/receipt errors; shutdown abort/join releases transactional writers.

Rotation seam: selected SQL binding commits before strong in-memory owner replacement and selection notification. Old actor may observe `Unauthorized` due to stale binding while native Edit remains. Distinguish pause/rebind from actual native revocation and cancellation. Preserve unread revisions, admitted attempts, actual outcomes and receipts; checkpoints are per part, new reader starts/replays selected part. Adapter cancellation is a request: actual subscription outcome decides winner. Parent's reported failing rotation scenario is an unresolved regression target, not declared fixed here.

### Storage, transport, certificates and key provenance

Cover first attach, repeated same owner, losing generation/tie candidates, cross-scope/incarnation/group rejection, one strong selected owner; selected binding/certificate commit before open/owner/watch publication; current + predecessor hints resolve within one logical owner, cross-owner ambiguity rejects; boot audience clearing, fresh native all-part refresh; Edit publication versus Read consumption versus Relay ciphertext transfer.

Publication: reserve scope-wide writer sequence transaction before signing (failed publication leaves gap), fresh authority samples, exact pinned publication key reference, exact historical source key provenance, merge snapshot and BigSync membership one transaction. Receive: structural/crypto binding validation, missing key material staged/fetched/imported before committed receive, completion only after actual receive commit. Consumer transaction: selected binding check, Read, cursor <= durable revision and nonregression, derived state + checkpoint one transaction with rollback on drop.

Carry-forward: per-source/per-destination progress, saved predecessor snapshots, exact existing Representation/original/ciphertext copied into selected membership (not rewrapped or resealed), publish batch before checkpoint; crash repeats idempotently; unavailable source cannot fabricate completion. Separately supported representation canonicalization/rewrapping must not be confused with this migration. Source control certificates authenticate key provenance only: never adopt old parts, grant Read/Edit, select authority, enable processor, or silently substitute latest JWK. Check independent requester/author/key-source Relay/Edit and sampled native generation around awaits. A retained source certificate is not retirement permission.

Properties: selected-owner monotonic preference under valid candidates; stale binding cannot publish; key lineage cannot rewrite semantic original; cert import has no authority/part side effects; no early receive acknowledgement; carry checkpoint never outruns destination durability; derived consumer checkpoint atomic. Physical purge/garbage-trigger retirement is unimplemented: model retention safety boundaries and explicit external purge assumptions, not a fictional purge algorithm.

## Composition and dependency contracts

Composition uses asynchronous queues with reorder/duplication/delay, explicit drop only where actual transport permits, and crash/restart at each durable boundary. Select two cross-cutting scenarios first without treating them as total coverage: historical register rewrap + selection + consumer checkpoint; and captured processor task + persist/start + rotation + exact receipt/settlement. Then extend to every ledger edge and error path.

Dependencies that must be enumerated as assumptions:

- BigSync revisioned/keyed-frontier store: examine and model the actual per-reader revision ordering, checkpoint and page boundaries before composition; do not assume every global revision appears in each part or a gap-free contiguous delivery stream. Same-transaction snapshot/membership publication and eventual notification/replay exposure require explicit source correspondence, not an unexamined stronger prefix axiom.
- Watermark/JobBoard: each owed lane and associated job settles exactly once; shared revision slot advances only after all required jobs/lanes; stream supersession/drop does not erase still-owed membership; empty revisions finish correctly.
- Cursor/bucket: scanned, in-flight and acknowledged object positions are distinct; replacement/abandonment cannot acknowledge uncompleted synchronization; remove-part explicitly retires obligations; object transfer ack follows real effect completion.
- Serial/concurrent delta walkers: revision batches/checkpoints respect owed work; cancellation, replay, concurrent completions and supersession preserve at-least-once committed entries and prefix ordering.
- Scheduler/keyed scheduler/outbox: actual keyed replacement/cancel/requeue/retry semantics and exact front completion token; eligible queued effects eventually run under stated fair scheduling, not invented extra retries.
- Native authority/key/document materialization: role-specific decisions, generation freshness and exact content-addressed provenance are trustworthy; Pending can resolve only under the explicit environment assumption.
- Dispatch/job store and SQLite: transactions atomic/durable; journal maps exact inputs/jobs/outcomes; execution gate prevents effect before admitted durable job; subscribe/query closes outcome races.

An incorrect dependency contract invalidates composed claims; it is not discharged by a passing system model. Future standalone dependency models map directly to `lib.rs` BigSyncMachine, cursor + watermark/JobBoard, bucket, scheduler + keyed scheduler, serial/concurrent delta walkers, outbox, replay-page and peer-selection reducers. Maintain this extension map without claiming them covered now.

## Finite profiles and liveness assumptions

Initial profiles: 2 writers (3 for canonical poison witness replacement), 2 tasks, 3 nodes including Relay-only, 2 sessions with retained old completions, 2 part generations plus predecessor, 2 keys, 2 captures/baselines, capacities 1 and 2, sequence/evaluation levels sufficient for stale/equal/new/poison-recovery. Identity domains must cover simultaneous retained and stale tokens; no reuse while outstanding. Separate crash cuts and authority Pending/Edit/Read/Relay/denied outcomes. Make bounds explicit in every result; finite population does not prove arbitrary-population Rust correctness. TLC explores unbounded execution length of its finite state graph; Apalache max-steps explores only its stated bounded depth unless an actual inductive proof is established.

Stabilized progress profile disables endless fresh arrivals/rotations/claims after a finite disturbance phase, not legitimate completion interleavings. Assumptions: eventually stable permitted authority/selection; exact docs/keys/captured blobs eventually available; durable writes succeed eventually; relevant connected messages eventually deliver; per-outstanding-identity effect and completion processing fair; clocks progress and authorized leader heartbeats eventually arrive with adequate gaps; a required execution terminates with an actual outcome; finite competing workload or justified per-task fairness. Intermittent enablement may need explicit strong fairness rather than weak fairness. No unconditional progress under permanent denial, perpetual Pending, partitions, infinite adversarial writes, nonterminating routines or best-effort cancellation.

Candidate temporal obligations: admitted selected publication eventually durably visible; unblocked consumer eventually incorporates and checkpoints; stable eligible router eventually has persisted winning claim and offers; persisted eligible worker attempt eventually starts; actual finished attempt eventually incorporates original receipt and releases capacity; exact settled processor capture eventually contributes baseline; source revision with resolved dependencies eventually enqueues then checkpoints; stable rotation eventually rebinds/carries evidence without cancelling otherwise authorized running jobs. Every antecedent must have a reachable witness and every obligation an unfair/mutated negative control.

## Approval questions and explicit limits

Approved: module location/ownership, complete component scope, finite profiles with disclosed bounds, TLC finite-state liveness and optional Apalache bounded safety. Finite fairness/materialization/termination profiles are experiments, not accepted product guarantees. No Quint Connect dependency or generic generator now. The register item stops for parent review before the next component.

Still explicit product/composition questions: environmental service guarantees; intended ADR-only reconciliation/retirement versus implemented behavior; physical purge budget/trigger (unimplemented); exact BigSync dependency semantics. At research time the Relay timeout and rotation regression were unproved; parent subsequently reported targeted native rotation passes and corrected key-Pending/mixed-part binding/SQL-versus-native cancellation behavior. This worker has not verified that Rust or composed model; fresh bounded reads are required when composition begins. The full remaining standalone/system scope is pending, not silently narrowed to register.
