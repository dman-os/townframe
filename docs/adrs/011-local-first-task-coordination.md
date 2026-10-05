# ADR 011: Local-First Task Pools over BigSync

**Status:** Proposed

**Implementation transport:** use standard iroh/IRPC following the existing BigRepo RPC design. XRPC and service-interface migration are deferred to a separate PR; ADR 016 is an unaccepted draft, not a dependency.

The current `tasks::store::TaskStore` adapter projects versioned immutable declarations and writer-owned terminal facts from authenticated encrypted register lanes. It owns its register host, serializes local semantic publication, rejects declaration collisions and unknown payload schemas, and preserves local success against later cancellation. `RegisterBinding::part` is selected explicitly by the domain descriptor; register scope and cryptographic incarnation do not implicitly select network subscription identity. `TaskSyncBackend` retains one owner per explicitly attached pool in the dedicated `daybook-tasks` scope, sharing its SQLite store and revision notifications with native BigSync worker/RPC. Checked register admission commits before transport completion. Native document parts and local derived/run-log state are not served through this scope. Fresh document/group Relay audiences replace persisted permissions before serving; unresolved native authority remains fail-closed and retains its refresh obligation. Physical membership removal is not authenticated retirement and is rejected. The native two-node scenario exercises opaque Relay, late Read, durable consumer checkpoint/effect, reverse publication, revocation, reopen and stale/incarnation rejection; this is transport proof, not distributed allocation/execution.

The live wire contract uses `townframe/task-coordination/2` and advertises scheduling protocol 2. Optional source origins change the positional postcard declaration encoding, so version 1 is not wire-compatible. Its native ingress resolves the application peer from the existing authenticated BigRepo endpoint mapping on every request, rejects missing mappings and mismatched session identities, and carries the authenticated identity separately into the request queue. This identity boundary does not grant pool authority: the pool driver must check current document/group access and exact session ownership before applying scheduling events. Native ingress and connection lifecycle errors remain explicit rather than silently manufacturing an identity.

The native pool actor owns independently scheduled Tokio jobs for publication,
classification and execution observation. A handler may await the same SQLite
writer as a publication job, so jobs cannot depend on that actor polling their
futures. Shutdown aborts and joins all owned jobs before releasing actor resources;
dropping detached handles is not a shutdown barrier.

Classification and offer freshness fences use `TaskTicket::version_digest`,
separate from the immutable declaration digest. It includes publisher evidence
and ordered terminal lanes, so incorporation invalidates stale asynchronous
decisions without JSON-serializing non-string public-key map keys. The digest
streams framed fields directly into BLAKE3 without constructing encoded ticket bytes.

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

The implemented metadata API uses a caller-provisioned, existing common Automerge rendezvous document. Its `task_pool_directory` map contains additive pool/group/document references: lookup hints, never authority grants. A stable processor pool ID hashes the stable plug ID and processor name, excluding plug upgrades, drawer placement and physical task parts. That hash does not create a common descriptor document identity, and discovery never implicitly creates a document on a local miss.

For triage, the named adapter is the existing `RepoCtx::doc_config` document; management explicitly supplies the descriptor document and an existing authority group for each managed distributed processor. `PoolRepo::lookup_binding(pool_id)` examines all additive references for that requested stable pool across groups: zero returns `Absent`, one returns a non-authoritative `Hint`, and multiple distinct document/group bindings return `Conflict`. An unavailable rendezvous is `Pending`, and malformed lookup metadata is rejected. A recovered hint must separately pass `load_descriptor`'s current document/group Read admission. Neither lookup nor document/facet arrival creates a pool or starts a role; triage owns that lifecycle explicitly. The directory is a lookup adapter, not a required generic registry or duplicate typed pool facet.

The actual referenced document is the CGKA authority locus. Its `task_pool_descriptors` map stores each complete versioned descriptor as one atomic JSON value; multiple pools and unrelated metadata may share that document. Readers enumerate conflicting map objects and scalar values rather than accepting an arbitrary Automerge winner. Independently registered descriptor documents for one pool/group, or unequal complete descriptor alternatives, produce `Conflict`. Known references whose bytes or keys are unavailable produce `Pending`; `Absent` means only that the locally readable directory has no reference.

`Ready` is a descriptor snapshot, not an ongoing authority grant. Loading captures private descriptor bytes and heads before final checked admission under current document **and named-group Read**. Registration prepares the exact descriptor and reference before final checked document/group Edit admission, which linearizes the accepted operation; subsequent group revocation does not undo that admitted mutation. Ordinary document commit still enforces document Edit and supplies durability. Descriptor publication and non-authoritative directory publication are two independent commits, with no cross-document atomicity: a failure between them may leave an unadvertised descriptor, and repeating registration is idempotent.

An owned, taskless watch subscribes to descriptor/directory changes, materialization and authority notifications before its initial query. Notifications trigger fresh durable queries and authority admission, not cached permission. Group membership events name only their direct target, so every group membership change conservatively wakes discovery to cover transitive nested-group changes even when independent direct document Read survives. Dropping the watch unregisters its listeners before repository shutdown. This metadata API does not implement the generic task backend, physical-part migration or an authenticated retirement/deletion fence; additive directory hints do not make those retention claims.

### 3. One task-coordination BigSync backend

ADR 011 introduces one logical BigSync backend, `daybook/task-coordination/v1`, with two object schemas:

```text
RouterSlotPayloadV1
TaskTicketPayloadV2
```

Separate parts allow separate incremental consumers, but they are not separate task backends. Pool descriptor documents continue to use the existing Automerge/BigRepo backend.

Every router slot and task ticket carries:

- its pool-specific part; and
- the same Keyhive `group_part_id` used by its pool descriptor.

Existing group-part policy therefore filters task coordination objects by authority. Pool-specific parts express interest and replay. Future BigSync intersection filters can request `group AND pool` without composite parts.

Task/router payloads use the reusable `big_sync_core::encrypted_register` CRDT building block described in ADR 012, rather than a task-owned coordination primitive. The encrypted register owns authenticated causal versions and encrypted representations; task interpretation, current authority, JWK resolution, SQL durability, and task-specific retention remain backend/domain adapters. A distributed triage processor has one explicitly managed pool with one active application JWK in its pool document; historical JWK versions remain resolvable. Different processors may have different sharing audiences.

### 4. Authorization and encryption

Possession of a part ID is not authority. The pool's Keyhive group controls:

- who may read task plaintext;
- who may publish task terminal facts or removals;
- who may claim router candidacy;
- who may subscribe or publish to pool BigEphemeral topics; and
- which relays may retain encrypted coordination objects.

BigEphemeral currently uses an open policy in BigRepo; implementation of a Keyhive-backed ephemeral policy is required. It maps pool topics to the descriptor's authority group.

Task bodies and router semantics use application keys stored as generic JWK facets in the Keyhive-protected pool document, following ADR 003 §5–6. Keyhive/Beekem CGKA and the document causal-encryption mechanism protect and recover the key-bearing document; custom task payloads do not invoke CGKA directly or maintain a second causal-encryption/checkpoint DAG. A representation binds its JWK facet reference and the exact key-document heads used to resolve it, following ADR 003 `keyRef`/`keyRefHeads`; resolution must never silently substitute the latest key. The key write is durable before dependent ciphertext is published. Relay-visible authenticated envelopes expose only identifiers, part membership, framing, ciphertext size, key-reference metadata, and data required for admission/routing policy, never the JWK secret. Cipher suite and reusable key-resolution integration require source review before implementation.

Replacing a JWK does not erase historical key material. Existing ciphertext retains its pinned key reference; current authorized readers obtain that key through the document historical-recovery contract. Former readers may retain keys already learned. A new access domain may require a new key-bearing document and migration rather than overwriting a facet, as described in ADR 003 §16. Application-key rotation is distinct from both CGKA membership rotation and transport-part rotation.

Removing a reader first removes permission to deliver new protected bytes. Rotate the application JWK on audience reduction or suspected compromise as additional protection against future ciphertext disclosure; neither operation retracts previously learned keys or plaintext. Adding a reader does not require application-key rotation. Part rotation and garbage collection do not rotate JWKs, and there is no default calendar-driven application-key churn. Offline writers cannot promise immediate global exclusion before learning new authority/key state.

Registers may contain valid representations using different pinned JWK versions concurrently. There is no store-wide current-key or CGKA-epoch eligibility gate, and missing local key material is not obsolescence or a reason to mutate convergent state. Future-publication key selection is separate from historical decryption. Re-encrypting an unchanged original statement preserves its original signature, writer sequence, and causal observations; only the representation changes. Concurrent key rotations must leave both published key references resolvable and use an explicit domain-agreed rule for future publication/wrapper preference, never node-local decryptability as a merge tie-break.

The generic register does not own membership, depend on Keyhive, or impose concurrent-revocation exclusion. It joins correctly bound signed evidence deterministically. Partition policies govern network admission and may be Keyhive-backed or use another authority system; domain consumers separately decide which router claims, tasks, or results are actionable. Original author identity and delivering-peer identity are distinct. Node-local delivery origin, arrival time, current membership, and decryptability do not determine register merge or causal dominance. Cryptographic validity is not permission to execute an effect.

Backdating resistance remains open ([issue 53](https://github.com/dman-os/townframe/issues/53)). Signatures, author timestamps, writer sequences, and blocking a revoked transport peer do not alone distinguish fresh old-context records relayed by another peer from legitimate delayed work. A domain may define deterministic concurrent-removal exclusion or signed historical finality, but neither is a mandatory generic register feature. A router checkpoint would require explicit authority for the affected records, an authenticated accepted set/frontier, and competing-checkpoint rules; no such protocol is established here. No centralized timestamp authority or relay consensus is introduced. Temporary acceptance before authority converges, eventual domain eligibility, and irreversible effects are distinct risks; historical key disclosure cannot be undone.

BigRepo's coordination bridge produces opaque `CoordinationAuthority` views
from the actual descriptor document and its transitive binding to the existing
pool group. Effective writers require Edit on **both** that group and document;
local register reads and `admit_coordination_read` require Read on both. A direct document
grant outside the pool group does not grant pool eligibility, and Relay never
permits plaintext. Missing authority nodes are Pending; a known wrong group
binding is Unauthorized. Authority admission does not require document CGKA keys.

`with_coordination_signer` supplies a borrowed opaque private signer only inside
a synchronous callback under the document lock. It implements the existing
`Signer<Signature>` and `Verifiable` traits, so original statement/header and
Representation signatures cover the domain's exact transcripts rather than a
different serialized `Signed<T>` wrapper. No raw key or owned signer escapes.
Payload encryption is application-owned: coordination registers use the shared
RFC 8188 codec and domain-provisioned document JWK facets. The former direct
CGKA `CoordinationCiphertext`, sealing/opening APIs, and their rewrap-specific
tests are removed. Native CGKA continues to secure document/key distribution.
The authority adapter checks native group/document membership independently of
application payload keys, so ciphertext-only Relay does not need a JWK or CGKA key.

Shared Keyhive generation brackets membership traversal and invalidates stale
nested-group views even when document heads do not change. The final generation
check holds the authority document lock; generation is freshness evidence, not
an epoch clock or a register merge rank.
Checked admission linearizes at its final successful
check, not a later SQL/document durability commit. Revocation observed before
that check rejects the operation; later revocation cannot undo an already
accepted write. Returned views and their local-access diagnostics are not
long-lived cached permission grants.

Historical keys are recovered from the exact JWK facet at signed `keyRefHeads`,
using native document history. No latest-key fallback, create-on-miss key,
repo-wide triage key, or blob-worker-only ownership is introduced. Generic
register joins do not require plaintext or current writer membership.
Checked local Read/Edit admission remains a concrete host policy, not a
universal historical authorization rule.

#### Latest coordination lanes

`big_sync_core::encrypted_register` implements the policy-neutral signed latest-state reducer. The task-owned reducer and its epoch/membership-dependent admission rules are removed from main. The core has no Keyhive, document, SQL, or task interpretation. Stable binary `RegisterKey` framing independently length-prefixes opaque scope and slot bytes; writer sequences and work generations do not create new physical keys.

Writers sign exact original statements and public headers containing their record identity, sequence, compact frontier, and commitment to the complete signed plaintext. Frontiers authenticate writer assertions, not Byzantine causal completeness or historical permission. Domain consumers resolve dependencies and interpret actionable state; the core does not reconstruct membership or a historical DAG.

Each writer retains only its greatest sequence. Concurrent writers remain distinct. Equal-sequence unequal semantic identities retain exactly two canonical signed witnesses and exclude that lane from head projection; a higher sequence advances it. Same-record observations affect projection without deleting writer replay fences. Missing remote lanes never delete retained lanes. Historical-writer retirement and total writer-count bounds require a separate protocol.

Representations preserve the signed original and bind a publisher, opaque incarnation, key reference, exact sorted unique key heads, encoding token, encoding parameters, and ciphertext. All binding fields participate in signatures and canonical wrapper ordering. Mixed historical key versions join independently of current membership, local key availability, delivery origin, or arrival order. Canonical selection does not implement a preferred fresh-container migration policy.

`RegisterSnapshot` binds schema and record identity to writer/value pairs, rejects duplicate writer entries during decoding, and serializes borrowed state without copying ciphertext. Structural key/body/frontier/metadata budgets precede cryptographic verification; all remote lanes are verified before mutation. `restore` uses the same verification as remote merge and makes no historical-authorization claim. Hosts must separately bound transport decoding and durable resource growth.

Nine focused core regressions cover component framing, concurrent projection/replay fences, mixed-version wrapper convergence and restore, transferable equivocation, atomic invalid-map rejection, original/outer binding, cross-record frontier semantics, duplicate-writer decoding, and structural limits. A standalone core executable completed 2,000 signed turns across two serialized replicas with 20 snapshot restores, mixed pinned-key metadata, stale replay rejection, and exactly two retained writer lanes (4,225 final wire bytes). Core and daybook_core all-targets/all-features clippy passed. This proves signed opaque-register behavior, not encryption, disk restart, host JWK resolution, durable publication, partition-policy integration, or physical reclamation.

`daybook_core::tasks::storage::RegisterStore` binds an opaque domain scope,
native document/group authority, configured allowed JWK facet references, a
publication key with exact heads, and a transport incarnation. Opening admits
Relay without resolving payload keys; publication resolves the already-provisioned
key without minting one. Plaintext recovery requires Read. Writer sequences
are durably reserved before signing; failures leave gaps rather than reuse.
Admitted current snapshots and their identical BigSync payload publish in one
SQLite transaction. Consumer checkpoints and derived SQL settle together.

A standalone disk-backed host executable exercised ordinary owner JWK writes,
K1/H1 publication, K2/H2 rotation, store reopen, exact H1 recovery and unchanged
signed-original verification, sequence advancement, and stale replay remaining
unchanged. A wrong current K2 could not decrypt the old ciphertext. This proves
the concrete pinned-key publication path, not process restart, late-reader
Drawer integration, task-driver composition, preferred-container rewrap, or
authenticated physical reclamation.

A disk-backed executable completed 2,000 encrypted durable publications after
the initial K1/K2 scenario. Scoped object, part, membership, pending membership,
peer-cursor, current-register and sequence-row counts stayed exactly
`(1, 1, 2, 0, 0, 1, 1)`; the revision advanced by 2,000 and the final writer
sequence was 2,002. The final JSON payload was 236,071 bytes, including the
shared codec's 64-KiB padded ciphertext represented as JSON bytes. This is a
single-writer fixed-slot growth proof, not retired-writer/slot reclamation or
nonzero peer-cursor accounting. A separate ten-turn executable fully stopped
and reopened the disk-backed native repository, blob store, Plugs and Drawer
in one process (the test keyring is process-local). It recovered the identical
current payload, decrypted through exact pinned JWK heads, replayed one current
slot rather than a turn journal, durably advanced the consumer checkpoint, and
published the next writer sequence 13. The replay boundary follows the shared
revision clock, including native repository bootstrap writes.

A two-peer native regression bootstraps shared Drawer metadata before private
key publication. A Relay-only peer retains and republishes identical signed
ciphertext without JWK access; plaintext recovery and local publication reject
that permission. After owner K1/H1 to K2/H2 rotation, a later Read grant and
native history synchronization recover the exact H1 statement without changing
the original signature or current register. Loading already-populated foreign
Drawer metadata still encounters its separate legacy authority migration; this
scenario does not claim to fix that bootstrap path.
All-targets/all-features clippy passed for `big_sync_core`, `big_repo`,
`daybook_types` and `daybook_core` after smoke scaffolding was removed. Native
FFI generation and the Compose application check passed; the Gradle check
required IPv4 JVM networking after dependency downloads failed over the default
network path. These checks do not establish authenticated retirement or task
backend/rotation integration.


#### SQLite publication boundary

BigSync's `SqlitePartStore::begin_obj_write` binds a SQLite write transaction to
one object. `SqliteObjWrite::payload` reads its current content in that same
transaction; `context_mut` permits domain-owned SQL, not direct mutation of
BigSync tables or raw transaction commit/rollback. A caller can atomically persist an admitted domain
snapshot or counter with `publish` and `commit`, without exposing separate
payload, membership and frontier writes.

One publication replaces the object's payload and touches the union of existing
live parts, its pending parts and caller-admitted parts. Duplicate targets are
joined once. Bucket summaries, pending promotion, memberships, part cursors and
current frontier entries share the transaction and revision. Unrelated pending
memberships remain untouched. Dropping the transaction rolls everything back;
live readers are notified only after commit. A failed or cancelled publication
cannot be committed, and a transaction permits at most one publication.

The generic boundary has been exercised against real disk SQLite: dropped writes
roll back domain state, payload and pending promotion; committed deduplicated
targets and domain state survive pool close/reopen while unrelated pending
membership survives. Failure after bucket changes leaves the durable cursor,
original payload and original targets unchanged. Live readers see nothing before
commit, then receive one coalesced object event with the committed target union.
It is not a task repository, an authorization check, an epoch fence, a remote
merge backend or a retirement implementation. Those callers must supply their
own admission and durable counter rules before using this publication boundary.


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

#### RPC compatibility and rolling upgrades

The initial cross-node transport follows the existing BigRepo iroh/IRPC pattern: an explicitly versioned ALPN identifies a supported wire protocol. Incompatible peers need not allocate work together; unsupported ALPN fails connection establishment rather than translating task messages. No cross-version shim is required. ALPN selection is an application-defined compatibility gate, not automatic schema negotiation by IRPC.

IRPC uses postcard. Derived structs are positional, not named-field maps: Serde JSON-style unknown-field tolerance does not establish wire compatibility. Adding, removing, reordering, or changing fields, or changing enum variant indices, can fail decoding or silently change meaning. Keep an ALPN only when old/new byte fixtures demonstrate compatibility in both directions and operation semantics remain compatible; otherwise bump it. Optional fields and Serde defaults alone do not establish compatibility. Any skippable extension mechanism must be explicitly framed and distinguish optional hints from required execution, authority, or placement semantics.

Rolling upgrades retain one shared router election per pool, not independent version cohorts. Authenticated claims advertise a scheduling protocol version and supported live RPC versions, independently of pool-descriptor and ticket-payload schema versions. Prefer the newer live scheduling version among otherwise eligible candidates; workers incompatible with the selected router do not accept offers and do not form a second version-specific election. The shared election envelope/projection must remain understandable to supported older participants so they can stand down. A dead newer-version historical claim must not block compatible takeover: version preference applies to live candidacy, not permanent retention of an old claim. Heartbeats/session discovery expose compatibility and unavailability. Changing these election semantics requires an explicit versioned contract, not merely bumping an RPC ALPN.

Live RPC versions are distinct from persisted task-ticket, processor-slot, and router-slot payload versions. An ALPN bump does not migrate retained data. Unknown persisted versions are unsupported data, not proof of cancellation or permission to prune; schema upgrades require their own compatibility or migration decision.

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

For each accepted or origin attempt, the executor captures the domain's `ResolvedInvocation { args: Vec<u8> }` and the canonical declaration digest. `PersistAttempt` writes both before its exact attempt acknowledgement permits `StartDispatch`. Dispatch consumes that captured invocation, even if classification changes while persistence is pending or the executor reconnects; a fresh attempt resolves anew. Argument encoding belongs to the domain/handler, not the generic task protocol.

The local dispatch boundary persists concrete source/configuration document heads and versioned `CapturedWflowExecution` before enqueueing. The capture identifies exact main-manifest heads, the original workflow handler and version-specific workload, ordered immutable component blob IDs, and bundle handler keys. File components are copied into owned blob storage at admission; execution verifies captured bytes against their digests. Immediate, delayed and boot workload loading consume this retained capture, not current enablement. Boot waits for the workflow journal replay boundary before reconciling dispatch state, including archived outcomes and prepared admissions with or without durable JobInit. Native disk-reopen scenarios exercise waiting/queued WASM, both prepared admission crash cuts, archived settlement and missing/corrupt captures. These local runtime recovery checks do not yet provide remote artifact materialization or production task-driver recovery.

Pool descriptor protocol v2 provisions the register scope, representation
incarnation, allowed canonical native-document JWK facet references, and exact
publication-key heads. The descriptor snapshot builds the register binding under
fresh document/group Read admission; opening opaque Relay storage does not need
payload-key materialization. JWK facets remain in the pool authority document:
Drawer registration would add broader admin grants. Plaintext release rechecks
current Read after asynchronous exact-head key resolution. This is provisioning
and storage admission, not an integrated scheduler or historical writer proof.

`PoolWorkerMachine::restore_running_attempt` accepts an existing authorized
workflow attempt before routing registration. It preserves the attempt ID and
capacity reservation, discards the old connection allocation, and emits no new
dispatch or input classification. Prepared offers are not running jobs and must
not use this entry point. The driver still owns durable DispatchRepo/wflow
reconciliation, domain settlement checks, and registration of the new executor
incarnation; the machine entry point alone is not production restart recovery.

Allocations live only in router/executor memory and local DispatchRepo state. If all live witnesses disappear, the pending BigSync ticket becomes allocatable again. Router heartbeat and RPC session liveness do not prove useful task progress; DispatchRepo/wflow must report or fail unexpected hung attempts according to local policy.

### 8. Task ticket payload

```text
TaskTicketPayloadV2 {
    schema: 2
    task_id: TaskId
    declaration: SignedEncryptedTaskDeclaration
    terminal_lanes: Map<NodePubkey, SignedTerminalCell>
}

TaskDeclarationV2 {
    task_id: TaskId
    pool_id: TaskPoolId
    domain: TaskDomainId
    producer: Optional<NodePubkey> // source origin, not publishing authority
    handler: HandlerRef
    input: Vec<u8>
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

Canonical equality and the declaration digest include the domain-owned input bytes, not encryption envelopes or publisher evidence. Different input under one TaskId is rejected atomically as a collision. Local invalid declarations are programming-invariant failures; malformed remote declarations are rejected before publisher, terminal, readiness, or admission state changes. `AuthoritativePlacement` requires `Preference::Only`; ordinary preference is not an execution-authority constraint.

Publisher evidence carries an authenticated domain-owned `input_witness` outside canonical declaration meaning. The native writer lane signs both declaration and witness; projection obtains publisher identity and witness from the verified original, never from an RPC caller's claimed publisher. Equivalent publishers may retain different exact-history witnesses under one task identity. The domain validates a selected witness and its actual publisher's current rights; source-origin attribution is only scheduling input. Task payload schema 2 rejects older retained schemas as unsupported data, not as cancellation or permission to prune. Router-slot payload schema and its shared election remain unchanged.

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

For `TaskTicketAuthoritative`, the ticket leaves the active part but remains in the authority/archive part. With no retention horizon it is retained until explicit deletion and no execution deadline is required. With a finite horizon, an immutable `not_after` is required and must be less than or equal to `retain_until`; equality is valid. Stale pending copies must be permanently non-runnable when terminal evidence can be discarded.

BigSync removal/tombstone mechanics prevent ordinary local resurrection, but no permanent task tombstone is required for domains whose durable compact settlement rejects stale tasks. An old replica may temporarily reintroduce a ticket; domain classification makes it inert and removes it again.

This semantic task-retention rule does not imply physical BigSync tombstone collection. ADR 012 decision 9 retains dead membership rows until an authority/reconstruction rule makes their removal safe. Dropping a ticket payload leaves such metadata behind; a drained active pool can therefore still accumulate historical dead rows and dead fingerprints. Request-cursor filtering avoids sending removals to readers that never saw the add, but does not bound local storage. The task/domain backend needs an explicit membership-authority and stale-reintroduction rule, including what a ciphertext-only relay can verify, before claiming bounded total pool storage. Arbitrary tombstone TTL or cursor rotation is not a substitute.
Relay-local eviction is never published as cancellation or protocol removal.

#### Task-part rotation

Rotation uses a pool-local unsigned 64-bit generation, starting at zero. This is transport retention identity, not the router election generation, a writer sequence, a task identity, or a CGKA epoch. Advancing from an observed generation uses checked addition by one; exhaustion is an invariant failure, not wraparound.

Part keys are arbitrary bytes. The canonical key is the concatenation of `daybook/task-part/v1\0`, the byte length of the UTF-8 pool ID as unsigned 32-bit big-endian, those pool-ID bytes, the 32-byte authority-group ID, the rotation generation as unsigned 64-bit big-endian, and one role byte (`0` active, `1` archive). Other roles require an explicit protocol extension. An optional archive is derived only when configured. Keys depend on neither the proposing node nor its clock, signature, CGKA epoch, or router claim. Knowing a key does not grant authority to declare a rotation or access the part.

Every generation has a predetermined successor. Nodes partitioned for several rotations derive the same chain, even when they reach different depths. The greatest valid authenticated rotation generation is selected on reconnection; equal generations name the same parts, so they require no proposer tie-break. A node never regresses its accepted generation. Authentication and fresh-node bootstrap of this current control state remain implementation obligations; an unauthenticated large integer or router endpoint is not evidence of advancement.

Automatic rotation is garbage-triggered, not calendar-triggered. Pool management configures positive absolute budgets for dead membership rows and retained garbage bytes; reaching either requests advancement to the next generation. These are local observations, not a global agreement on counters. Counters are attributed to the current generation: predecessor garbage awaiting cleanup must not repeatedly rotate an otherwise clean successor. A ratio alone is not a trigger, because a tiny idle pool can have a large dead-to-live ratio. Numerical defaults require measured storage-growth acceptance evidence.

A router considers tasks only in its accepted current active part. Router contact and synchronization advertise current rotation control so an old producer can learn the successor. On learning a valid newer generation, each node locally migrates still-required state directly into the selected generation; it need not publish through every intervening part. This does not wait for acknowledgement from all writers. An offline producer retains its pending state until successor publication is durable. Local adoption fences new old-generation publication and reconciles writes admitted before that fence, so a concurrent write cannot fall between migration and switching the selector.

Rotation preserves stable logical task/slot identity, exact original signed statements, and the existing ciphertext with its pinned JWK reference. Transport generation is authenticated separately; moving between parts alone does not require decrypting or resealing. Fresh domain admission rejects already-settled or obsolete obligations instead of blindly resurrecting them. Learning an old task never authorizes recreating its retired transport part or its memberships. Relay-held required state must also have a carry-forward path before its only retained copy is deleted.

This is a local carry-forward protocol, not a certificate that one node observed every offline task. Physical purge still requires authenticated old-generation fencing at every allocation surface, including object payloads, live/dead/pending memberships, part metadata, buckets, peer cursors, and obsolete consumer state. Consumer selector changes require an explicit successor checkpoint, not cursor reset to zero. The bounded control-state/bootstrap rule, relay carry mechanism, and all-target purge integration remain unimplemented acceptance requirements; deterministic names alone do not prove bounded storage.

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
