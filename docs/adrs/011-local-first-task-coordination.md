# ADR 011: Local-First Task Pools over BigSync

**Status:** Proposed

## Context

Daybook needs to allocate work across intermittently connected nodes without requiring an always-available server or consensus system. Distributed triage is the first consumer, but commands, recurring automation, collaborative batches, GPU work, and personal agents need the same networking and scheduling machinery.

[Patchwork's task framework](https://www.inkandswitch.com/patchwork/notebook/tasks-02/) separates tasks, task queues, workers, routers, and run history. Its active router learns worker availability from periodic ephemeral messages and assigns pending tasks. The queue document durably records pending work and the active router. Partitions may temporarily produce multiple routers and duplicate execution.

Daybook has different storage and runtime primitives:

- a **Daybook node** is already a public-key-identified runtime; its internal threads, Wasm workers, and DispatchRepo workers are not network actors;
- BigSync parts provide incremental, independently replicated sets without retaining an Automerge change history for every task mutation;
- BigEphemeral provides authenticated, lossy, non-persisted topic fan-out through live peers and relays;
- iroh RPC and iroh HTTP can provide reliable live point-to-point communication after reachability discovery;
- Keyhive provides authority, encryption keys, and group-derived BigSync filtering;
- DispatchRepo and wflow provide local attempt durability; and
- relays and non-executing nodes must be able to retain encrypted task state without becoming router candidates.

Task coordination is not the permanent source of every work obligation. A task domain normally owns durable state from which current work can be derived and into which successful results are incorporated. The task system carries the current distributed scheduling working set. A domain without another durable settlement record may instead make the task ticket itself authoritative for its result.

The system does not provide distributed exactly-once execution. During a partition, or while source, settlement, and task data cross independently, more than one attempt may run. The target is store-and-forward durability, intelligent ordinary-case allocation, bounded replicated scheduling state, deterministic router convergence after connectivity returns, and explicit domain-defined duplicate safety.

## Terminology

**Task domain:** The subsystem that knows why work exists, how to validate readiness and results, and when a task is obsolete. Triage, an agent-run repository, and a command scheduler are task domains.

**Task pool:** A named scheduling and retention class. A pool has a low-churn descriptor, one active-task BigSync part, one router-election slot, and one BigEphemeral router-heartbeat topic. “Pool” does not imply FIFO ordering.

**Pool descriptor:** A Keyhive-authorized Automerge document containing stable pool metadata and protocol rendezvous identifiers. It does not contain task arrays or live allocation state.

**Task ticket:** One independently replicated BigSync object asserting that a particular execution obligation is available for scheduling.

**Router:** A Daybook node currently selected to tail a pool's active task part and allocate its runnable tickets. One router instance may lead several pools.

**Executor node:** A Daybook node that registers with a router and may accept tasks. Internal DispatchRepo workers are not network-visible executors.

**Router slot:** A compact BigSync object used only to arbitrate router takeover. It never stores allocations or executor inventories.

**Allocation:** Live agreement between a router and one executor incarnation to attempt one task. Allocations are not durable distributed state.

**Domain settlement:** Durable domain-owned evidence that an obligation has been satisfied. Triage settlement is one example.

**Terminal task fact:** A durable task-ticket fact such as successful completion or cancellation. It stops ordinary routing but is not necessarily the domain's permanent settlement record.

## Grounding scenarios

### 1. Triage task executes immediately at its origin

A local document change produces processor generation G. Its policy prefers the authoring node A, and A is ready.

1. Triage durably records G as desired and derives deterministic task ticket T from `(triage slot, G)`.
2. A inserts T into the processor pool's active BigSync part.
3. A validates authorization, source materialization, processor generation, configuration, and capacity locally.
4. A starts a DispatchRepo attempt without waiting for a router round trip.
5. If a router is reachable, A registers the active attempt over the pool's RPC transport. Otherwise it continues under the origin fast path.
6. The router does not allocate T while it observes A's active attempt.
7. A commits processor effects, publishes triage settlement, and publishes T's successful terminal fact.
8. Settlement makes T obsolete. Triage requests cancellation of any duplicate local attempt and removes T from its local active task part.

The origin fast path preserves interactive latency. It may duplicate work during a partition.

### 2. Origin cannot execute and disappears

A phone produces GPU task T but has no GPU.

1. It publishes encrypted T to BigSync.
2. A sponsored relay and passive mobile nodes retain T without decrypting or scheduling it.
3. The phone disconnects.
4. A GPU node appears later, discovers the pool descriptor and active router, syncs T, and registers with the router.
5. The router offers T over RPC.
6. The GPU node accepts only after its task-domain adapter verifies the particular input is materializable.

Neither the producer nor the relay must be online when execution begins.

### 3. Task arrives before its input

B receives T but not its referenced document or blob.

1. B retains T and may register coarse capability with the router.
2. An offer to B is declined as `NotReady`.
3. The domain adapter records what local input change can make T ready.
4. When that input materializes, the adapter emits a readiness wakeup and the router reconsiders T.

Routers use capability and locality hints to avoid poor offers, but the selected executor makes the authoritative readiness decision.

### 4. Router takeover while an executor remains alive

Router R1 disappears while executor A runs T.

1. R1's BigEphemeral heartbeat expires locally at candidates.
2. Candidates publish next-generation claims in the durable router slot.
3. After a settlement delay, deterministic projection chooses R2 in the connected component.
4. R2 advertises its reachability in a new heartbeat.
5. A connects and registers with R2 over RPC, including its bounded current attempt set.
6. R2 reconstructs live allocation state from registrations and continues routing remaining pending tasks.

There is no snapshot request over BigEphemeral and no durable router allocation log. A currently live executor introduces its own current state to the new router.

### 5. Router and executor disappear

R1 and A both disappear while T is pending in BigSync and running locally on A.

1. Their live allocation state disappears.
2. T remains pending in the active BigSync part.
3. A later router receives no executor registration for T.
4. After takeover stabilization and task policy permit it, the router reallocates T.
5. A may later restart with a local DispatchRepo checkpoint; it must re-read durable task/domain state and register a new incarnation before resuming.

Duplicate execution is possible if A was only partitioned rather than dead.

### 6. Two partitioned routers

A network partition gives each component a router and enough data to schedule T.

1. Both routers may allocate T.
2. Both attempts may finish.
3. Authenticated terminal facts merge in T.
4. The task domain decides whether either or both results satisfy its obligation.
5. Router slots converge when BigSync connectivity returns; the deterministic maximal-generation winner remains active.

The router reduces duplicates; it is not a consensus lease.

### 7. Triage settlement crosses into a stale-task partition

Partition A completes processor task T(G), records durable triage settlement G, and prunes T locally. Partition B retains pending T but has no eligible executor. Node N first syncs with A and later bridges to B.

1. N carries settlement G but need not carry T or a permanent T tombstone.
2. B sends stale pending T to N.
3. N's triage adapter classifies T as `Obsolete`, refuses execution, requests cancellation of any local duplicate, and removes T locally.
4. N propagates settlement G into B through the triage coordination path.
5. Nodes in B independently classify and prune T after observing settlement.

The persistent compact triage settlement—not historical task completion—is the anti-replay fact.

### 8. Two bridges and a capability race

Partitions are bridged by N1 carrying stale T and N2 carrying settlement G. Meanwhile a previously ineligible node gains a GPU.

Delivery order may allow a router to offer T before settlement arrives. An executor that already has G declines T. An executor that has not observed any surviving settlement may run T again. Preventing that execution would require sacrificing partition availability or consulting a designated authority.

Ordinary synchronization narrows this race by classifying a newly received task as `CoordinationIncomplete` until its declared domain coordination input reaches a local replay boundary. It cannot eliminate duplicates across genuinely disconnected knowledge.

### 9. Source arrives with no task or settlement

This applies to established nodes as well as new ones.

1. A node receives source generation G from a peer that disappears.
2. It has neither T(G) nor settlement G.
3. The domain records an unwitnessed candidate rather than immediately creating work.
4. Relevant task and domain synchronization may resolve the candidate.
5. Domain policy may eventually adopt it and recreate deterministic T(G).

The generic task system does not infer missing obligations from domain source state.

### 10. Settlement cancels and prunes active work

Whenever a task domain observes that an obligation is settled or no longer desired:

1. its local adapter marks matching tickets obsolete;
2. the router stops offering them;
3. executors request best-effort cancellation of matching attempts;
4. authorized domain replicas remove those tickets from their local active parts; and
5. repeated concurrent removals are harmless.

Settlement-driven cancellation and pruning are required behavior, not optional cleanup.

### 11. Background agent command with no separate settlement repository

A phone submits “research train routes and send me a report.” It creates random task T in an agent-background pool. No AgentRun or other durable domain object is created.

1. T's declaration, retry/effect policy, deadline, and encrypted input are authoritative in the task ticket.
2. A relay retains T after the phone disconnects.
3. A router assigns T to a reachable executor with the required model and network tools.
4. The executor durably publishes `Succeeded(result_ref)` in T.
5. T immediately leaves the active scheduling part but remains in an authority/archive part because it is the only durable result record.
6. The phone later reads the report from T.
7. T may be deleted only by explicit user/domain deletion or after its declared retention horizon and execution deadline make resurrection permanently inert.

A task with no external settlement owner cannot discard its terminal record merely because a router saw it.

### 12. Recurring scheduler

A durable schedule says “produce a morning digest.” The schedule is domain state; each occurrence is a distinct task.

1. The scheduler derives occurrence ID O from the schedule and time window.
2. It creates deterministic T(O), including a `not_after` deadline.
3. Completion is incorporated into a durable occurrence/run record when that facility exists.
4. Once incorporated, T(O) can be pruned like triage work.
5. Without an occurrence record, T(O) remains the authoritative terminal record until retention expiry.
6. After `not_after`, a stale pending T(O) is never executed even if an old replica reintroduces it.

A stable schedule must not be modeled as a mutable latest-wins task: distinct occurrences are separate obligations.

### 13. Non-idempotent external effect

A task that sends email cannot be made exactly-once by router election.

It must use at least one of:

- an external idempotency key derived from TaskId;
- placement on one explicitly authoritative node;
- a human confirmation boundary;
- a domain-specific transactional outbox; or
- explicit acceptance of duplicates.

Task declarations must state their duplicate/effect policy. Unsafe combinations are rejected at the producer boundary where possible.

### 14. Relay withholding, reordering, and eviction

A relay can delay liveness by withholding state but cannot forge signed cells or decrypt task bodies. Relay-local storage eviction is not a task cancellation and must not author an authoritative BigSync removal.

Sponsored retention must either admit a task/pool within a declared budget or report that durability was not achieved. Opportunistic retention may evict, in which case a domain with external durable state can recreate its task later.

## Decision

### 1. Component boundaries

```text
TaskDomain
    durable obligation, readiness, result acceptance, obsolescence
         │ derives and reconciles
         ▼
TaskRepo
    pool descriptors, router slots, active task tickets
         │ observed by
         ▼
PoolRouter ───── reliable live RPC ───── ExecutorNode
                                             │
                                             ▼
                                        DispatchRepo
```

TaskRepo and PoolRouter may initially share a Rust process, but their responsibilities remain separate. A router is generic scheduling logic; domain adapters supply task classification and execution arguments.

### 2. Pool descriptor and chosen names

The design uses **task pool**, **task ticket**, **pool router**, **router slot**, **executor node**, and **task domain**. “Queue” is rejected because no FIFO order is promised; “worker” is reserved for internal execution machinery rather than a Daybook network identity.

Each task pool has one low-churn Automerge descriptor:

```text
TaskPoolDescriptorV1 {
    protocol: "daybook/task-pool/v1"
    pool_id: TaskPoolId
    authority_group: KeyhiveGroupId
    active_task_part: PartId
    archive_part: Optional<PartId>
    router_slot: ObjId
    router_heartbeat_topic: BigEphemeralTopic
    allowed_rpc_transports: Set<RpcTransport>
    retention_class: RetentionClass
    routing_defaults: RoutingDefaults
}
```

The descriptor is a configuration and authority touchstone. It contains no task array, executor list, heartbeat, or allocation history. One router process may lead multiple pools.

### 3. One task-coordination BigSync backend

ADR 011 introduces one logical BigSync backend, `daybook/task-coordination/v1`, with two object schemas:

```text
RouterSlotPayloadV1
TaskTicketPayloadV1
```

Separate parts allow separate incremental consumers, but they are not separate task backends. Pool descriptor documents continue to use the existing Automerge/BigRepo backend.

Every router slot and task ticket carries:

- its pool-specific part; and
- the same Keyhive `group_part_id` used by its pool descriptor.

Existing group-part policy therefore filters task coordination objects by authority. Pool-specific parts express interest and replay. Future BigSync intersection filters can request `group AND pool` without composite parts.

### 4. Authorization and encryption

Possession of a part ID is not authority. The pool's Keyhive group controls:

- who may read task plaintext;
- who may publish task terminal facts or removals;
- who may claim router candidacy;
- who may subscribe or publish to pool BigEphemeral topics; and
- which relays may retain encrypted coordination objects.

BigEphemeral currently uses an open policy in BigRepo; implementation of a Keyhive-backed ephemeral policy is required. It maps pool topics to the descriptor's authority group.

Task bodies and router semantics are encrypted under domain-separated keys derived from the applicable Keyhive/Beekem epoch. Relay-visible authenticated envelopes expose only identifiers, part membership, framing, ciphertext size, and data required for admission/routing policy.

### 5. Router election over BigSync

The router slot contains one latest authenticated claim lane per candidate:

```text
RouterSlotPayloadV1 {
    protocol: "daybook/router-slot/v1"
    pool_id: TaskPoolId
    lanes: Map<NodePubkey, SignedRouterClaim>
}

RouterClaimV1 {
    pool_id: TaskPoolId
    candidate: NodePubkey
    incarnation: NodeIncarnationId
    writer_seq: u64
    generation: u64
    claim_id: Random128
    observed: Map<NodePubkey, u64>
}
```

Per-writer lanes merge by greatest `writer_seq`; equal-sequence unequal bytes are equivocation. Omitting a known lane never deletes it.

The projected winner is the lowest deterministic rank among valid claims at the greatest generation:

```text
BLAKE3(pool_id || generation || candidate || claim_id)
```

A candidate attempts takeover only after the currently projected claim's BigEphemeral heartbeat has remained absent for `router_takeover_after` on its local monotonic clock:

1. read maximal observed generation;
2. publish a claim at `generation + 1`;
3. wait `router_settle` for BigSync exchange;
4. re-read the slot;
5. begin routing only if still the projected winner; and
6. stop promptly if a later merge defeats the claim.

Each partition may elect a router. Claims converge after healing. No router claim grants exclusive execution across partitions.

Non-candidates and encrypted relays retain the router slot but never produce eligible claims.

### 6. Router discovery and live RPC

The active router periodically publishes through the pool's BigEphemeral topic:

```text
RouterHeartbeatV1 {
    pool_id: TaskPoolId
    router_claim: (generation, claim_id)
    router_node: NodePubkey
    router_incarnation: NodeIncarnationId
    heartbeat_seq: u64
    reachability: Vec<ReachabilityEndpoint>
}
```

A public key identifies a Daybook node but does not address it. Reachability may advertise iroh iRPC, iroh HTTP, or a future relay-oriented transport. A separately keyed endpoint must include a signature binding it to the Daybook node identity.

BigEphemeral is appropriate for repeated heartbeats because messages are useful only while fresh and loss is tolerated. It is not treated as reliable RPC and offers no replay, response, or delivery acknowledgement.

After discovery, executor nodes establish a live routing session using an advertised transport. A pool may advertise a dedicated BigEphemeral request/response topic as a lossy fallback, but reliable iRPC/HTTP is preferred and targeted traffic must not be broadcast to every pool subscriber.

### 7. Executor registration and allocation

On every router change or executor restart, an interested executor registers:

```text
RegisterExecutorV1 {
    node: NodePubkey
    incarnation: NodeIncarnationId
    capabilities: CapabilitySummary
    capacity: Capacity
    active_attempts: Vec<ActiveAttemptSummary>
}
```

The bounded active attempt set is normal session registration, not a network snapshot protocol. The session then carries executor liveness, capacity changes, offers, accept/decline, cancellation, and completion hints.

```text
Router -> Offer(task_id, allocation_id)
Executor -> Accept(allocation_id) | Decline(reason)
Router -> Start(allocation_id)
Executor -> AttemptChanged(...)
```

The exact start handshake may be collapsed after implementation testing. The executor must never begin solely because it received an unauthenticated or stale offer.

Allocations live only in router/executor memory and local DispatchRepo state. If all live witnesses disappear, the pending BigSync ticket becomes allocatable again. Router heartbeat and RPC session liveness do not prove useful task progress; DispatchRepo/wflow must report or fail unexpected hung attempts according to local policy.

### 8. Task ticket payload

```text
TaskTicketPayloadV1 {
    protocol: "daybook/task-ticket/v1"
    task_id: TaskId
    declaration: SignedEncryptedTaskDeclaration
    terminal_lanes: Map<NodePubkey, SignedTerminalCell>
}

TaskDeclarationV1 {
    task_id: TaskId
    pool_id: TaskPoolId
    domain: TaskDomainId
    producer: NodePubkey
    handler: HandlerRef
    encrypted_input: Vec<u8>
    coordination_ref: Optional<DomainCoordinationRef>
    placement: Placement
    preference: Preference
    effect_policy: EffectPolicy
    not_before: Optional<Timestamp>
    not_after: Optional<Timestamp>
    result_retention: ResultRetention
}

enum TerminalFactV1 {
    Succeeded {
        attempt_id: AttemptId
        result_ref: Optional<OpaqueResultRef>
    }
    Cancelled {
        reason: OpaqueReason
    }
}
```

A task declaration is immutable for one TaskId. Concurrent unequal declarations for the same ID are an invalid collision/equivocation, not “desired revision siblings.” Replacement creates a different TaskId and retires the old ticket.

Terminal lanes use latest signed per-writer sequence and preserve concurrent authenticated facts. Any valid success or cancellation stops ordinary scheduling. Detailed logs, retries, and intermediate failures remain in DispatchRepo or domain state; a failed attempt leaves the task pending unless policy makes it terminal.

### 9. Task identity and replacement

One task is one execution obligation.

- Random IDs represent independently submitted one-shot work.
- Deterministic IDs deduplicate independently derived obligations.
- Recurring schedule occurrences use distinct deterministic IDs.
- Triage uses `H(triage slot, work generation)`.

The generic layer has no reusable mutable task revision. A producer replaces work by creating the new task and cancelling/removing the old task. Late completion remains attributable to the old ID.

### 10. Domain classification

Before offering, and again before accepting, the registered task domain classifies a ticket:

```text
enum TaskClassification {
    Runnable(DispatchArgs)
    NotReady(ReadinessWatch)
    Obsolete
    CoordinationIncomplete
    Invalid
}
```

The router may cache classifications for scheduling efficiency. The executor performs the final classification because materialization, authorization, and capacity can change after an offer.

`CoordinationIncomplete` allows a domain to wait for a relevant replay boundary before acting on newly arrived task state. It reduces source/task/settlement ordering duplicates but cannot manufacture knowledge across a partition.

When classification changes to `Obsolete`, the domain must stop routing, request best-effort cancellation, and prune the local active ticket.

### 11. Active and archival retention

A successful or cancelled ticket leaves the active task part immediately after local validation. Its durable terminal facts then follow one of two policies:

```text
enum ResultRetention {
    ExternalSettlement(DomainCoordinationRef)
    TaskTicketAuthoritative {
        retain_until: Optional<Timestamp>
    }
}
```

For `ExternalSettlement`, task data may be fully removed after the local domain state proves the obligation settled or obsolete. Multiple authorized domain replicas may race to remove it; removal is idempotent. The router is not the semantic pruning authority.

For `TaskTicketAuthoritative`, the ticket leaves the active part but remains in the authority/archive part. With no retention horizon it is retained until explicit deletion. With a horizon, `not_after` must ensure stale pending copies are permanently non-runnable before terminal evidence can be discarded.

BigSync removal/tombstone mechanics prevent ordinary local resurrection, but no permanent task tombstone is required for domains whose durable compact settlement rejects stale tasks. An old replica may temporarily reintroduce a ticket; domain classification makes it inert and removes it again.

Relay-local eviction is never published as cancellation or protocol removal.

### 12. Producer and router cursors

Task domains incrementally project obligations into active task parts and consume terminal facts through durable local BigSync cursors. Pool routers independently tail active task parts through their own durable cursors.

A router restart therefore reconstructs pending tickets from BigSync rather than from its predecessor. A producer restart reconstructs what tickets should exist from domain state and repairs missing or stale projection entries.

Cursor completion is evidence that one particular stream was consumed through a boundary. It is not proof that every peer observed a task or completion.

### 13. Relay and passive-node operation

A relay or passive node may retain:

- the pool descriptor;
- the router slot; and
- encrypted task tickets selected by sponsored group and pool parts.

Retention does not imply router candidacy or executor participation. Local configuration distinguishes:

```text
RetainOnly
Execute
RouterCandidate
ExecuteAndRoute
```

A relay enforces sponsorship, object-size, count, byte, and metadata-growth budgets. Guaranteed retention requires explicit admission; opportunistic retention may discard local replicas but must expose that it did not provide durable store-and-forward.

### 14. BigSync merge and network convergence

Router slots and task tickets use compact custom merge payloads rather than Automerge histories. Their joins must be commutative, associative, and idempotent for valid writers.

When incorporating a remote payload changes local state, BigSync advertises the result back to the sender. Reconciliation may therefore be chatty during concurrent writes, but converges once no participant derives new state.

Payload framing, writer lanes, causal observations, part counts, ciphertext sizes, and terminal-fact counts are bounded. Authorized malicious writers remain outside the liveness guarantee.

## Guarantees

The task-pool system provides:

- encrypted store-and-forward of current work through BigSync and sponsored relays;
- independently replicated task objects rather than one growing queue document;
- deterministic router convergence in connected components;
- live reachability discovery without treating node public keys as addresses;
- point-to-point intelligent allocation after discovery;
- executor-authoritative readiness;
- router takeover without a durable allocation log;
- domain-driven cancellation and pruning;
- support for both externally settled and task-authoritative results; and
- bounded active scheduling sets when domains settle or tasks expire.

It does not provide:

- exactly-once execution;
- progress against every partition while also preventing all duplicates;
- durable distributed allocation ownership;
- automatic safety for non-idempotent external effects;
- reliable RPC through BigEphemeral itself; or
- safe deletion of a task-authoritative result without an explicit retention rule.

## Rejected alternatives

### Store task arrays and router traffic in the pool Automerge document

Rejected because high task churn retains an expensive shared change history and turns unrelated task updates into one synchronization hotspot. Automerge remains appropriate for the low-churn pool descriptor.

### Encode triage latest-state reconciliation as generic mutable task revisions

Rejected because batches, scheduler occurrences, and agent commands are distinct obligations. Triage keeps a persistent domain slot and derives one deterministic task per unsettled generation.

### Persist every allocation and heartbeat in BigSync

Rejected because write and relay traffic scale with live execution. Allocations are reconstructed from current RPC registrations; loss permits at-least-once reallocation.

### Use BigEphemeral as reliable RPC

Rejected because BigEphemeral is fire-and-forget, non-replayed, and may silently drop messages. It discovers a router and may provide an explicitly lossy fallback transport; reliable live allocation prefers iRPC or iroh HTTP.

### Broadcast every offer on the pool topic

Rejected because targeted allocation traffic would fan out to every subscriber. The router uses a point-to-point endpoint advertised in its heartbeat.

### Use snapshot requests during router takeover

Rejected because BigEphemeral provides no delivery guarantee and exceptional snapshots duplicate normal registration state. Executors register their current bounded attempt set whenever they connect to a router.

### Let the router decide permanent result pruning

Rejected because observing completion does not prove a domain incorporated it. Stable routers would also accumulate data if pruning required a later router generation. Domain settlement or explicit task-authoritative retention controls deletion.

### Retain every task tombstone forever

Rejected for domains with compact durable settlement. Settlement makes resurrected old tasks inert. Domains without settlement retain the task terminal record or use an execution deadline before expiry.

### Treat a BigSync part ID as authorization

Rejected because parts express routing interest. Keyhive policy determines read, write, ephemeral-topic, and relay-retention authority.

### Require relays

Rejected. Relays improve asynchronous retention but direct peer-to-peer operation remains complete.

## Consequences

### Positive

- Triage remains sharply domain-specific while sharing routing and execution allocation.
- Router traffic scales with active executors and attempts rather than every node contending on every task.
- Only router heartbeats require pool-wide ephemeral fan-out; detailed traffic becomes point-to-point.
- Relays and mobile nodes can carry encrypted state without performing work.
- A pool with no external settlement design still has an honest result-retention model.
- Task pruning no longer depends on router generations or global acknowledgement.

### Costs and risks

- The system introduces pool descriptors, router election, RPC routing sessions, and a task-domain interface.
- BigRepo needs a Keyhive-backed BigEphemeral policy.
- BigSync group-plus-pool intersection filtering remains desirable to avoid authorized overfetch.
- Domain and task streams are not atomically ordered, so ordinary synchronization races can still duplicate work.
- Task-authoritative pools require archival storage, deadlines, or explicit deletion.
- Router election timing must tolerate cellular latency without producing constant generation churn.

## Open questions

1. What exact `router_settle`, heartbeat, takeover, and registration-grace defaults survive intermittent cellular networks?
2. Should all pools initially use one task-coordination backend instance, or should authority scopes receive separate physical stores while preserving one protocol?
3. What BigSync removal watermark is sufficient before relays physically delete externally settled tickets?
4. Which pool metadata must remain relay-visible, and which can remain encrypted?
5. What strict bounds apply to router lanes, terminal lanes, part counts, and active-attempt registration?
6. Which reliable RPC transport is implemented first: existing iRPC, iroh HTTP, or both?
7. How does the router persist local scheduling priorities and decline backoff without making them protocol state?
8. What generic interface expresses a domain replay boundary for `CoordinationIncomplete`?
9. Should task-authoritative terminal records use a dedicated archive part or remain addressable only by object ID after leaving the active part?
10. What user-facing signal proves a sponsored relay admitted the pool and synchronized its active task cursor?
