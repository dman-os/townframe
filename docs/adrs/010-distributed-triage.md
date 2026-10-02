# ADR 010: Distributed Triage

**Status:** Proposed

**Depends on:** [ADR 011: Local-First Task Pools over BigSync](./011-local-first-task-coordination.md)

## Context

Daybook processors react to document changes by evaluating processor manifests and dispatching matching routines. The current `daybook_core::rt::triage` implementation combines facet-delta consumption, processor refresh, predicate evaluation, local-origin detection, materialization, done-token suppression, dispatch creation, and whole-stream cursor settlement.

Some processors produce node-local indexes or caches and must run on every participating node. Others produce synchronized state or expensive external computation and should ordinarily execute once on a suitable Daybook node. Distributed processors require two different kinds of coordination:

```text
Distributed triage
    understands documents, processors, source generations, predicates,
    persistent settlement, supersession, and missing-coordination recovery

Task pools (ADR 011)
    retain current runnable tickets, elect routers, discover executors,
    allocate attempts, and carry short-lived terminal facts
```

The previous design made the generic task object a reusable `(document, branch, processor)` slot with mutable desired revisions. That generalized triage's data model rather than generic execution allocation. ADR 011 now models one task ticket as one execution obligation. Triage retains its own persistent bounded **processor slot** and derives a deterministic task ticket for each currently unsettled processor work generation.

The central pipeline is:

```text
source delta
    ▼
TriageRepo evaluates processor
    ▼
ProcessorSlot records desired generation
    ▼
TaskTicket(generation) enters processor pool
    ▼
PoolRouter allocates to an executor node
    ▼
DispatchRepo executes locally
    ▼
ProcessorSlot records settlement
    ▼
matching task is cancelled and pruned
```

Source, processor-slot, and task-ticket data synchronize independently. Their ordering is never assumed to be atomic. Triage therefore retains explicit unwitnessed-candidate and reconciliation semantics.

## Terminology

**Processor slot:** Persistent distributed triage coordination for one `(document, branch, stable processor identity)`. It records current desired work and accepted settlement compactly after task tickets are pruned.

**Processor work generation:** Digest of the exact source version, processor generation, and effective configuration generation to process.

**Processor task:** One ADR 011 task ticket derived from one processor slot and one work generation.

**Processor pool:** ADR 011 task pool associated with a stable processor identity. It is a scheduling and interest class, not the permanent processor result store.

**Evaluation receipt:** Local durable evidence that a particular processor/configuration evaluated a particular source version and matched or missed.

**Unwitnessed candidate:** A locally observed matching source generation for which neither corresponding processor-slot coordination nor a task ticket has yet been observed.

**Origin:** The Daybook node identity that authored the triggering relevant source change, not necessarily the document's first creator.

## Scenarios

### 1. Per-node processor on a new node

For `ProcessorCoordination::PerNode`:

1. node B replays existing document routes from its local cursor;
2. matching processors dispatch locally;
3. B records local evaluation and dispatch settlement; and
4. no distributed processor slot or task ticket is required for execution allocation.

Initial replay is real work because B needs its own local derived state.

### 2. Locally authored change executes immediately

Processor P is distributed, prefers its origin, and can execute on authoring node A.

1. A observes source version H1 and evaluates P.
2. A writes desired processor work generation G1 into stable processor slot S.
3. A derives `T1 = H(S, G1)` and inserts T1 into P's active task-pool part.
4. A validates the exact source, processor, configuration, and capacity locally.
5. A starts DispatchRepo immediately without waiting for router settlement.
6. If the pool router is reachable, A registers the active attempt over RPC.
7. On success A commits processor effects, writes settlement G1 to S, and writes T1's successful terminal fact.
8. Settlement G1 cancels any local duplicate and removes T1 from A's active task part.

The fast path skips a router round trip, not local durability.

### 3. Origin is ineligible and goes offline

A phone authors H1, but P requires a GPU.

1. A writes desired G1 and task T1 but does not execute it.
2. A sponsored relay retains the encrypted processor slot, router slot, and task ticket.
3. A disconnects.
4. GPU-capable B later obtains the pool descriptor and T1 from the relay.
5. B registers with the active router and accepts T1 only after materializing H1 and matching the exact processor/configuration generation.

!> yeah so this is a bit painful when it comes to "accept/retry" I bet
!> maybe we can do hints in the worker node heartbeats so that they specify
!> if they're settled for incoming data
!> i.e. if their triage is settled (done processing incoming stuff)
!> this can be a hint to the router

Task declaration and executor readiness remain separate.

### 4. Source supersedes running work

A is processing T1 for G1 when source H2 produces G2.

1. Triage records G2 as desired in S.
2. Triage derives new task `T2 = H(S, G2)`.
3. G1 is now obsolete even if T1 remains present or running.
4. The local adapter requests best-effort cancellation of T1 and removes it from the active task part.
5. A late G1 result remains attributable to G1 but cannot settle G2.
6. T2 is scheduled normally.

The generic task layer does not model mutable task revisions. Replacement is an old obligation becoming obsolete and a new obligation appearing.

!> btw, how is cancellation working? every node do have a direct line to the router, it could be an RPC call

### 5. Processor no longer matches

H2 no longer satisfies P's predicate.

1. Triage records a non-matching/current generation in S while causally observing the previous desired state.
2. Any prior task becomes obsolete.
3. Triage requests cancellation and prunes the old active ticket.
4. No replacement task is created.

A disappeared task alone never proves that the processor stopped matching; the processor slot carries that durable meaning.

!> very interesting scenario that i haven't thought aobut before, do succesive processor mismatches deschedule work from previous matches? I agree that's a yay

### 6. Task arrives before source materialization

B receives T2 before the document changes required to materialize H2.

1. B retains T2.
2. Its triage adapter classifies T2 as `NotReady` or `CoordinationIncomplete`.
3. It cannot accept an offer merely because it has the ticket.
4. Source synchronization later makes H2 materializable.
5. Triage evaluates the exact generation and records a receipt.
6. If S still desires G2 and does not settle it, B may become runnable and wake router consideration.

The triage cursor, task cursor, and source version are different domains and are never compared numerically.

!> yeah so this is the messy bit
!> btw, in our system, we do have a consistent authority
!> the router. we could have multiple routers for the same task but that's all eventually consistent
!> what i'm getting at is that we could make a dag from tasks.
!> i.e. some kind of seen set for tasks. T2 can say seen: [T1]
!> the point is, the worker nodes can tell the router their seen tasks frontier
!> but wait a minute, is this even necessary
!> because i got confused and assumed worker nodes looking at the incoming tasks
!> implied they'd have to receive it through big_sync for them to be elgible to accept it
!> not really, their elgibility ddoens't depend on their task frontier but the data frontier which is equivalent to the processor frontier
!> the question is then, do we need to classify tasks as we see them or do we just mantain a desired set from the processor evaluations that have run and correlate tasks with those?
!> i suppose it's equivalent to the routine described here, sorry for yapping

### 7. Source arrives before task coordination

An established node B receives replicated H2 before S or T2.

1. Per-node processors run normally.
2. Distributed processor P matches H2.
3. B does not infer immediately that distributed execution is missing.
4. B stores a durable unwitnessed candidate for G2.
5. Arrival of matching slot settlement or task state resolves it.
6. Otherwise adoption policy may eventually recreate deterministic T2.

Unwitnessed candidates are not limited to new-node bootstrap.

!> this one is tricky because it's one of the places where the triage and
!> the router closely work
!> unwitnessed candidates reclaimings should only be done by routers maybe?
!> i.e. instead of multiple nodes nominating unwitnessed candidates tehy have
!> the current router just considers it's local ones?
!> this implies the triage knows if it's a router node or not but it does remove
!> coordination around unwitnessed candidate claimings if I understand correcltty

### 8. Source peer disappears before coordination arrives

B receives H1 from A, but A disappears before B receives processor-slot or task state.

1. B records an unwitnessed candidate.
2. It cannot distinguish “A never projected work” from “coordination is delayed or exists in another partition.”
3. The candidate remains durable and observable.
4. Relevant replay boundaries and reconnection may resolve it.
5. Processor adoption policy may eventually promote it to desired G1 and ensure T1.
6. Otherwise an operator may recover or dismiss it explicitly.

This ambiguity is information-theoretic without atomic source/task replication.

### 9. Several nodes adopt the same candidate

B, C, and D independently adopt matching H1.

1. All derive the same processor slot S, generation G1, and task ID T1.
2. Their slot and task writes merge by identity.
3. Connected nodes ordinarily converge before more than one attempt begins.
4. Separate routers or origin fast paths in partitions may still execute duplicates.
5. Any valid settlement G1 makes every observed T1 obsolete.

Triage decides when uncertain source-derived work should exist; ADR 011 routes the resulting obligation.

!> this is another question that we have. while any node submitting tasks is important for the generalized task pool, in the triage case, we can make it so that the router node's triage only submits
!> that does defeat the scenario 2 case tho
!> and this hurts latency
!> the clear answer that falls out is that the router node's triage doesn't attenuate weather or not a task gets scheduled (to improvel latency)
!> the router is just helping us with execute once guarantees on a single partition

### 10. New-node historical replay

B joins with source and triage cursors at zero.

1. B subscribes to processor-slot and task-pool parts for locally active processors.
2. It incrementally replays documents, slots, and retained active tasks.
3. Per-node processors execute for matching documents.
4. Distributed observations resolve against slot settlement/task state or become unwitnessed candidates.
5. B never treats temporary task absence as proof that every historical document needs execution.

No global atomic “task mirroring complete” fence exists. Domain-specific replay boundaries inform candidate adoption and `CoordinationIncomplete` classification.

### 11. Concurrent source changes during partition

A and B independently produce H1 and H2.

1. Their processor-slot lanes may retain concurrent desired generations G1 and G2.
2. Each partition may derive and execute its corresponding deterministic task.
3. The document later converges to H3.
4. Triage evaluates H3 and writes desired G3 while causally observing G1 and G2.
5. T1 and T2 become obsolete and are cancelled/pruned where observed.
6. Neither G1 nor G2 settlement satisfies G3.

Source causality remains a triage concern. TaskRepo never orders opaque document heads.

### 12. Stale task crosses from another partition

A has already settled current G3. Delayed pending T2 arrives.

1. The triage adapter compares T2's coordination reference with local S.
2. It classifies T2 as `Obsolete`.
3. The router does not offer it; an executor must decline it if offered from a stale router.
4. Triage removes T2 locally.
5. Settlement G3 eventually propagates and causes other replicas to do the same.

A permanent task tombstone is unnecessary while S retains the compact authoritative settlement.

!> this is another blurry thing. here, it implies that the router's traige is attenuating scheduling
!> a valid case I guess, if the router's triage knows that this is obsolete, better nip it in the buds
!> but the previous case avoided this attenutation
!> this also interplays with the accept/reject semantis
!> presumabily, the offered node will also see that it's obsolete and reject it
!> but i wonder if we can solve this without having traige attenuated scheduling

### 13. Settlement bridges into a stale-task partition

Partition A completes T1, records settlement G1, and prunes T1. Partition B retains pending T1 but has no GPU. Node N syncs with A after pruning, then connects to B.

1. N carries S with settlement G1, but no historical T1 or T1 tombstone is required.
2. N receives stale T1 from B.
3. N refuses execution, requests cancellation of any local duplicate, and removes T1 locally.
4. N propagates S into B.
5. B's nodes independently prune T1 after observing settlement.

If another GPU executor sees T1 before it sees settlement, it may duplicate execution. Avoiding every such cross-partition race would require a designated online authority and violate local-first availability.

!> this is a good case falling out of rejection due to obsolete or not carrying data

### 14. Two bridges and newly gained capability

N1 carries stale T1 while N2 carries settlement G1. A node in the destination partition gains a GPU between their arrivals.

- If the executor has observed G1 settlement, final readiness classification declines T1.
- If it has not observed settlement, it may execute T1 before N2's state arrives.
- `CoordinationIncomplete` and domain replay boundaries narrow ordinary ordering races but do not create unavailable knowledge.

Triage processors must therefore be duplicate-safe unless their placement/effect policy deliberately sacrifices partition progress.

!> or they should use policies that is safer like origin only

### 15. Processor generation changes

P changes from processor generation P1 to P2 while old work runs.

1. The stable processor slot and processor pool identity do not change.
2. A future matching source delta produces a work generation containing P2.
3. Its new deterministic task ID differs from the P1 task.
4. Nodes possessing only P1 classify P2 work as `NotReady`.
5. P1 completion cannot settle a P2 generation.
6. Installing P2 does not implicitly rescan every historical document.

### 16. Configuration changes without a source delta

Every effective configuration change advances configuration generation C. It does not itself schedule every existing document. Future matching deltas use the new C.

A plug requiring historical processing exposes an explicit command that creates local dispatches or generic one-shot tasks. It does not synthesize triage deltas.

### 17. Mobile participation

A mobile node may retain processor descriptors, slots, and encrypted tasks while configured not to route or execute general work.

It still:

- advances triage observations and candidates;
- runs required per-node processors;
- may use the immediate origin path for explicitly allowed local work; and
- propagates settlements that make stale tasks obsolete.

Retention, execution, and router candidacy are independent.

### 18. Settlement observed while a duplicate is active

A node receives S settling G while its local DispatchRepo runs T(G).

1. Triage atomically marks the local scheduling projection obsolete with respect to S.
2. It requests best-effort DispatchRepo cancellation.
3. It removes T from its active task part.
4. A late local completion cannot replace or regress S.

Settlement-driven cancellation and pruning are required on every participating triage replica.

!> hmm, so is the triage actively pruning dispatches?
!> or do we have the task settlement arriving to the node cancel the dispatch?
!> and anyways, the router knows who is running what yes? it can do the cancellations
!> we do have to lock in 011 how takeover router works with knowing who's currently
!> allocated which task tho

!> if we do router based cancellations, we'd have to have a decision where a node currently executing a task goes offline. presumably, it becomes a router for it's own network of one
!> but in this case, we probably expect the main network to re-allocate the task to another node
!> we gotta talk and design for this scenario even tho it's an 011 concern

## Decision

### 1. TriageRepo

The current driver becomes a repository-shaped subsystem following the worker structure used by Daybook indexes:

```text
TriageRepo
TriageWorker
TriageHandle
RepoStopToken
machine_loop
ConcurrentDeltaWalker keyed by (BigRepo document ID, branch path)
```

The concurrent walker permits unrelated documents to progress while one waits for materialization or coordination.

TriageRepo's local durable observable state includes:

```text
global inspected facet-delta cursor
active processor identities and generations
processor activation cursor
per-(document, branch) inspected cursor
evaluation receipts
unwitnessed candidates
deferred materialization/domain coordination
!> what is deferred mat?
processor-slot projection cursor
task-pool projection cursor
```

### 2. Processor classes

```text
enum ProcessorCoordination {
    PerNode,
    Distributed(DistributedProcessorPolicy),
}
```

Per-node processors bypass ADR 011. Distributed processors own persistent processor slots and derive ADR 011 task tickets.

Processor effects are declared:

```text
LocalState
SyncedDocumentWrites
ExternalIdempotent
ExternalNonIdempotent
```

Validation rejects or requires explicit acknowledgement for dangerous combinations. In particular, distributed execution does not make arbitrary external effects exactly-once.

### 3. Branch selection

```text
MainOnly
DurablePaths(patterns)
```

Most processors observe only `main`. Temporary branches are excluded unless explicitly selected.

### 4. Stable processor-slot identity

```text
ProcessorSlotKey {
    document_id: BigRepoDocumentId
    branch_path: Utf8Path
    processor_full_id: String
}

ProcessorSlotId = BLAKE3(
    "daybook/processor-slot/v1\0"
    || canonical(document_id)
    || canonical_length_prefixed(branch_path)
    || canonical_length_prefixed(processor_full_id)
)
```

The slot is stable across source, processor-artifact, and configuration changes.

It belongs to:

- the existing Keyhive `group_part_id`; and
- a stable processor coordination part derived from `processor_full_id`.

Relays and passive nodes may retain encrypted slots without executing processors.

### 5. Processor work generation and task identity

```text
ProcessorWorkGenerationInput {
    source_version: OpaqueBytes
    processor_generation: Digest32
    configuration_generation: Digest32
}

ProcessorWorkGeneration = BLAKE3(canonical(ProcessorWorkGenerationInput))

TaskId = BLAKE3(
    "daybook/processor-task/v1\0"
    || ProcessorSlotId
    || ProcessorWorkGeneration
)
```

One generation is one task obligation. The TaskId does not identify the reusable slot itself.

Processor generation covers the canonical complete plug manifest and packaged artifact digest. Configuration generation covers canonical complete effective plug configuration. Broad generation bumps are preferable to silently missing indirect dependencies.

### 6. Persistent processor-slot payload

Processor slots use a compact custom encrypted BigSync payload rather than an Automerge task history:

```text
ProcessorSlotPayloadV1 {
    protocol: "daybook/processor-slot/v1"
    slot_id: ProcessorSlotId
    lanes: Map<NodePubkey, SignedProcessorCell>
}

ProcessorCellBodyV1 {
    desired: Optional<DesiredProcessorState>
    settlements: BoundedSet<ProcessorSettlement>
}

DesiredProcessorState {
    generation: ProcessorWorkGeneration
    matches: bool
    source_version: OpaqueBytes
    processor_generation: Digest32
    configuration_generation: Digest32
}

ProcessorSettlement {
    generation: ProcessorWorkGeneration
    result_ref: Optional<OpaqueResultRef>
    attempt_id: AttemptId
}
```

Signed per-writer lanes retain causal observations. Concurrent desired siblings survive until a later source evaluation writes a desired state observing them. A settlement satisfies only an equal work generation. The bounded settlement projection must retain enough current/concurrent evidence to reject stale tasks without preserving every historical attempt.

### 7. Processor pool descriptor

Each stable distributed processor has an ADR 011 pool descriptor. It supplies:

- active task part;
- router slot and heartbeat topic;
- authority group mapping;
- handler family and routing defaults;
- retention/admission policy; and
- supported live allocation transports.

The descriptor may expose opaque identifiers to relays while processor semantics remain encrypted. Processor pool identity does not change on plug upgrades.

### 8. Task declaration

For matching desired generation G, TriageRepo ensures:

```text
TaskDeclarationV1 {
    task_id: H(processor_slot_id, G)
    pool_id: processor_pool_id
    domain: "daybook/distributed-triage/v1"
    coordination_ref: processor_slot_id
    handler: processor routine and artifact generation
    encrypted_input: document, branch, source, processor and configuration data
    placement: processor placement policy
    preference: PreferNode(trigger_origin) | None
    effect_policy: processor effect policy
    result_retention: ExternalSettlement(processor_slot_id)
}
```

Equivalent independent declarations must canonicalize to equal bytes. A collision with unequal declaration data is invalid.

### 9. Domain classification and settlement reaction

The triage task-domain adapter implements ADR 011 classification:

```text
Runnable
    slot desires task generation
    generation is not settled
    exact source/processor/configuration is materialized and matched

NotReady
    task remains desired but local inputs/capability are absent

CoordinationIncomplete
    required source or processor-slot replay boundary is unresolved

Obsolete
    generation is settled, superseded, non-matching, deleted, or disabled

Invalid
    identity, authority, generation, or encrypted declaration is malformed
```

On `Obsolete`, TriageRepo must:

1. stop local routing consideration;
2. request best-effort cancellation of matching DispatchRepo attempts;
3. remove the task from its local active task part; and
4. propagate the processor-slot state that proves obsolescence.

Multiple replicas may race to prune the same task.

A successful executor commits processor effects and processor settlement before relying on the task terminal fact. If it crashes after settlement but before terminal publication, settlement still makes the pending task obsolete.

### 10. Source adapter and evaluation receipts

The source adapter provides conceptually:

```text
current_version(document, branch) -> SourceVersion
materialize(document, branch, source_version) -> Ready | Pending
is_current(document, branch, source_version) -> bool
change_provenance(delta) -> LocalAuthor(pubkey) | Replicated | Unknown
```

A runnable local task requires an evaluation receipt binding:

```text
processor_slot_id
task_id
work_generation
source_version
processor_generation
configuration_generation
triage_cursor
outcome: Matched
```

Receiving a task ticket is never itself evidence of readiness.

### 11. Unwitnessed candidates

```text
UnwitnessedCandidate {
    processor_slot_id: ProcessorSlotId
    task_id: TaskId
    generation: ProcessorWorkGeneration
    document_id: BigRepoDocumentId
    branch_path: Utf8Path
    processor_full_id: String
    source_version: OpaqueBytes
    processor_generation: Digest32
    configuration_generation: Digest32
    first_seen_local: Timestamp
    source_peer: Optional<PeerId>
    task_sync_baseline: Optional<Cursor>
    slot_sync_baseline: Optional<Cursor>
}
```

Candidates arise during ordinary replication and cursor-zero replay. They resolve when:

- matching desired task or settlement arrives;
- newer desired source makes them obsolete;
- reevaluation no longer matches;
- the processor is disabled or changes generation;
- policy adopts them and ensures the deterministic task; or
- an operator dismisses or recovers them.

A completed replay boundary from the source peer is useful evidence but not global proof of absence. Adoption policy chooses the liveness-versus-duplicate trade-off.

### 12. Pool joining and historical filtering

TriageRepo derives subscriptions from locally active processor manifests:

1. load the processor pool descriptor;
2. subscribe to the processor coordination and active task parts;
3. combine them with visible Keyhive group-part policy;
4. resume retained cursors or replay from zero;
5. reconcile slots, tickets, receipts, and candidates incrementally; and
6. register with the pool router only when local participation policy permits execution.

Disabling a processor removes execution interest but may retain coordination state needed for authority, relay sponsorship, or later reconciliation.

Until BigSync supports `Part(group) AND Part(processor)`, authorized nodes may receive unwanted processor objects. Composite parts are not introduced solely as a workaround.

### 13. Historical processing

Processor enablement, upgrade, and configuration changes do not automatically process existing documents.

A plug requiring historical work exposes an explicit command. The command may dispatch locally or create generic one-shot ADR 011 tasks. It does not mutate triage cursors, create fake source deltas, or pretend that every historical command task belongs to the persistent processor slot.

## Guarantees

Distributed triage provides:

- persistent bounded settlement per `(document, branch, processor)` after task pruning;
- one deterministic task obligation per unsettled work generation;
- latest-state source reconciliation outside the generic task protocol;
- origin-preferred immediate execution when locally ready;
- readiness only after exact materialization and matching evaluation;
- explicit source/slot/task ordering uncertainty;
- unwitnessed candidates on both established and new nodes;
- settlement-driven cancellation and pruning;
- stale-task rejection after settlement crosses partitions;
- source-driven reconciliation of concurrent desired generations; and
- independently observable durable progress and test fences.

Together with ADR 011 it gains router election, capability-aware allocation, encrypted relay retention, RPC supervision, and router takeover.

It does not provide:

- execution of every intermediate source revision;
- implicit historical processing on processor activation or upgrade;
- prevention of every duplicate across partitions or cross-stream ordering races;
- exactly-once external effects; or
- generic source-version ordering inside TaskRepo.

## Rejected alternatives

### Make the processor slot the generic task

Rejected because latest-state replacement and persistent source settlement are triage semantics. Generic task pools also serve independent batch items, scheduler occurrences, and one-shot agent commands.

### Remove persistent triage settlement after task completion

Rejected because a later node or stale partition would have no compact evidence that an old task is obsolete. Task tickets and tombstones are not retained forever.

### Retain every historical task completion

Rejected because the processor slot compactly records current/concurrent settlement required to reject stale work. Historical attempt logs remain in DispatchRepo or explicit diagnostic storage.

### Let the router interpret document and processor state

Rejected because it couples routing to TriageRepo and makes other task domains second-class. Generic routers consume domain classification and placement hints.

### Wait for atomic source, slot, and task synchronization

Rejected because independent BigSync paths and disappearing peers cannot provide such a global fence. Replay boundaries reduce uncertainty; unwitnessed adoption handles the remainder.

### Infer missing work from task absence

Rejected because a task may be delayed, pruned after settlement, retained in another partition, or never projected. Source observations without coordination become candidates.

### Put source heads or processor generation only in stable identity

Rejected because stable processor slots must survive source and plug changes, while task IDs must distinguish individual obligations. Both identities are required.

### Automatically rescan documents on processor enablement

Rejected because a new plug could trigger unbounded unexpected compute. Historical processing is explicit command behavior.

### Use a serial whole-stream walker

Rejected because one unavailable document would block unrelated progress.

## Consequences

### Positive

- The generic task protocol is no longer distorted by triage's latest-state semantics.
- Processor settlement survives aggressive task-pool pruning.
- A settlement crossing a partition automatically suppresses stale task execution.
- Triage preserves immediate local execution and data-local readiness.
- Relays and passive nodes can carry encrypted coordination without participating.
- Task/source races are explicit and testable rather than hidden behind LWW state.

### Costs and risks

- Triage owns both local evaluation state and distributed processor-slot projection.
- Every distributed processor needs a pool descriptor and persistent slots.
- Source, slot, and task streams remain eventually consistent and may cause duplicate attempts.
- Concurrent desired and settlement state needs a bounded, carefully tested merge.
- BigSync intersection filtering remains desirable for efficient group-plus-processor subscriptions.

## Open questions

1. What bounded settlement representation preserves concurrent source siblings without retaining unbounded processor history?
2. What default adoption timing should unwitnessed candidates use under intermittent cellular connectivity?
3. What exact replay boundary is sufficient to change `CoordinationIncomplete` into `Runnable`?
4. Which canonical plug-manifest and effective-configuration serialization defines generation hashes?
5. How does source deletion update the processor slot and remove every active generation task?
6. Which explicit repair command may reassert a current processor task without becoming historical bulk processing?
7. What diagnostic attempt history, if any, is retained after compact processor settlement?
8. Should a disabled processor retain its slot subscriptions solely to propagate stale-task suppression?
