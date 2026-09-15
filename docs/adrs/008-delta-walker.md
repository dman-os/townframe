# ADR 008: Revisioned Stores, Delta Walkers, and Durable Projections

**Status:** Proposed.

## Context

The current repository update path grew from `triage.rs` into a synchronous
multiplexer serving several unrelated repositories and indexes. It has four
structural problems:

1. One source cursor advances only after every registered sink finishes
   processing an event. One slow or failed consumer therefore stalls all other
   consumers.
2. All sinks share one replay position. A sink added later cannot independently
   reconstruct its state without bespoke replay code.
3. Initialization and live processing use separate paths (`events_for_init()`
   and listeners), creating handoff races and duplicated diff logic.
4. Drawer membership, document content, branch lookup, and derived index events
   are conflated. The multiplexer can consequently fabricate drawer events for
   changes that never changed a drawer.

This is the wrong boundary for a durable log playback system. Per-event fan-out
is a premature optimization, while per-consumer progress, replay correctness,
and independently recoverable derived state are fundamental requirements.

At the same time, current store notification loops can apply a remote Automerge
change through the same `mutate_sync` path used for local edits. That path
reconciles and flushes the projection back to the source, so observing a remote
write can cause another write. The redesign must make the direction of data flow
explicit.

ADR 005 introduced the materialization barrier and source-versus-projection
distinction. This ADR retains that distinction while superseding ADR 002's
section describing `SwitchWorker` as a pure multiplexer.

## Terminology

- **Keyed frontier:** low-level collapsed storage and notification machinery
  keyed by the object whose latest state matters.
- **Source revision:** one atomic revision from a durable source. A revision may
  affect several keys.
- **Revisioned store:** an inert, cursor-addressed source of atomic revisions. A
  revisioned store may project, filter, or enrich another revisioned store; it
  does not own an event loop or advance consumer progress by itself.
- **Delta walker:** the operational composition of a revisioned store, consumer
  state, and a serial or concurrent walking policy. The embedder drives its
  event loop and performs the I/O requested by the walker.
- **Projection:** derived state built from accepted source state.
- **Materialization watermark:** the latest source revision incorporated into a
  repository's passive in-memory or local-database view.
- **Route:** a reason an object belongs to a revisioned store's current scope,
  such as exact-object interest or membership in a followed group.
- **Watch:** low-latency, lossy observation intended for UI refresh.
- **History:** optional backend history, distinct from the durable work feed.

## Decision

### 1. Remove the switch and sink abstractions

The replacement architecture has no central switch, sink registry, or shared
consumer cursor. Each durable consumer owns a stable identity and independent
progress. Adding, removing, restarting, or slowing one consumer does not affect
any other consumer.

The old names may remain temporarily in migration code, but they are not part of
the target model. New APIs and documentation use revisioned stores, walkers,
projections, and watches.

No per-consumer outbox is introduced as an intermediate design. Outboxes would
decouple execution, but would preserve centralized routing and still require a
separate solution for late consumers. Independent replayable revisioned stores
and per-consumer walkers solve both problems directly.

### 2. KeyedFrontier is machinery; RevisionedStore is the inert boundary

`KeyedFrontier` owns difficult low-level semantics shared by part storage and
future log-shaped indexes:

- latest-value collapse per key rather than unbounded append-only storage;
- atomic multi-key revisions;
- cursor-based reads;
- replay boundary capture;
- notification without lost-wakeup races;
- transactions supplied by the storage implementation.

`KeyedFrontier` is one implementation substrate for a `RevisionedStore`; it is
not the stable repository API. `RevisionedStore` is the reusable inert boundary.
It can be implemented locally or across WIT/wRPC and may itself be a projection
over another revisioned store.

Its fundamental operation is equivalent to:

```text
read_after(arbitrary_cursor, limit) -> RevisionPage
changed_after(arbitrary_cursor) -> Wakeup
```

The cursor is always supplied by the caller. Reading never acknowledges work,
and callers may fetch again after cursor `C` without advancing durable progress
beyond `C`.

Conceptually, a revisioned store yields opaque atomic revisions:

```text
RevisionPage<Entry> {
  revisions: [RevisionBatch {
    entries: [Entry],
    through: opaque Cursor,
  }],
  replay: Replaying | ReplayComplete,
}
```

The exact Rust representation may use associated types or resources. The
following semantics are mandatory:

- One atomic source revision is never split across batches.
- A projected revision may contain no typed entries and still cover a later
  source cursor.
- A read session captures one replay boundary when opened and reports exactly
  one transition to `ReplayComplete` after processing through that boundary.
- Revisions after the captured boundary use the same read and mapping path;
  replay and live processing are not separate algorithms.
- Reading a batch has no acknowledgment side effect.
- Re-reading from the same cursor returns equivalent source work, subject to the
  source's declared retention contract.

Reaching the captured cursor defines replay completion. It does not depend on an
internal subscription handoff, because the frontier read/changed loop makes that
handoff an implementation detail.

### 3. DeltaWalker composes a revisioned store with consumer state

The `DeltaWalker` is the convenient operational face. It composes three pieces:

```text
RevisionedStore<Entry>   inert source or source projection
ConsumerDeltaState       durable progress and specialized projection state
WalkPolicy               serial or concurrent scheduling and settlement
```

A walker does not hide a task or take a closure callback. Like
`runtime2::driver`, it exposes a machine which the embedder drives: the embedder
supplies source batches and task completions, drains serial commands and
scheduled work, and decides how those effects execute in its process. The walker
owns transition rules, not the surrounding Tokio, wRPC, or cloud event loop.

A serial walker reads after its durable cursor, exposes one ordered unit of
work, accepts its completion, emits the applicable state write, and only then
advances the cursor. The existing notification loop is one embedding of this
policy; replay sequencing belongs in the reusable walker machine rather than
being rewritten by every store or RPC provider.

A concurrent walker additionally owns:

- a transient fetch cursor, which may move ahead without changing durable
  progress;
- `KeyedScheduler`, which prevents overlapping work for the same item key while
  allowing independent keys to run concurrently;
- `WatermarkBook`, which records completed source revisions and drains only the
  largest contiguous prefix into the durable cursor.

An atomic source revision is settled only after all jobs derived from it have
completed. A gap therefore prevents durable advancement but does not prevent
later independent jobs from running. On restart, anything beyond the durable
contiguous cursor is replayed.

The walker emits state-write commands but does not perform their I/O. A proposed
specialized state mutation becomes durable only after the embedder reports the
consumer effect successful. When possible, the effect, specialized state, and
newly drained durable cursor commit in one transaction.

### 4. Delivery is at least once unless state shares a transaction

A durable consumer generally performs:

```text
read batch
  -> update projection or side effect
  -> settle item in consumer state
  -> persist the newly contiguous cursor
```

If the projection update and consumer state commit in the same storage
transaction, the consumer can provide exactly-once projection effects. If they
cannot share a transaction, delivery is at least once and the consumer must make
its effects idempotent.

The walker does not claim exactly-once external effects. A network request,
message publication, or write to another database requires its own idempotency
key or transactional mechanism.

The transaction token is storage-specific and may be erased across generic APIs.
For SQLite, a caller already holding a transaction can construct the
keyed-frontier transaction adapter, pass it through the generic layer, then
recover and commit the owning transaction at the boundary. The frontier must not
invent nested SQLite transactions or make all repository event emission
transaction-aware.

### 5. Revisioned stores form explicit semantic layers

The system will provide progressively more semantic revisioned stores rather
than one universal event enum. Because every layer remains inert, its output can
be consumed by either a serial or concurrent delta walker.

A derived revisioned store owns its projection boundary completely:

```text
upstream RevisionedStore
  -> projection cursor and projection state
  -> projected RevisionedStore
  -> any number of downstream DeltaWalkers
```

Polling the derived store may cause it to read and atomically incorporate more
upstream revisions. That is store maintenance, not downstream acknowledgment.
The derived store persists enough output and cursor state for downstream walkers
to replay independently; it must not consume an upstream revision into private
memory and thereby make that delta visible to only the caller which happened to
poll it. Filtering is therefore just another revisioned-store projection,
including when an upstream revision produces no output entry.

#### Physical object revisioned store

An internal revisioned store exposes the keyed frontier produced by
`AutomergeFrontierWorker`. As described by ADR 002, that worker consumes both
Keyhive admission events and watched source-part subscriptions, waits for the
Keyhive and Sedimentree materialization barriers, and only then publishes
materialized Automerge heads. The new keyed frontier replaces its output part
log; it does not bypass those input streams or barriers.

The physical store reports branch document ID, source revision, current heads,
source-group membership changes, and removals. This layer knows storage IDs but
does not claim Daybook logical semantics.

#### Document delta revisioned store

`DocDeltaRevisionStore` validates `daybook.branch` and maps accepted physical
changes to logical `DocumentId` and `BranchId` values defined by ADR 007. It is
an inert filtered store over the physical store. Its own sparse, revisioned
state records the accepted per-branch version needed to calculate
`previous -> current`. Advancing that projection is part of polling the store;
it does not acknowledge downstream consumption.

That sparse state is keyed by physical `BranchId`, not logical `DocumentId`, and
contains at least:

```text
DocDeltaItemState {
    last_accepted_version: optional DocumentVersion,
    active_routes: Set<RouteId>,
}
```

It exists only for branch documents admitted by that store's filters. Separate
filtered stores may therefore have different sparse state. Downstream walkers
retain independent consumer progress and may consume the same store serially or
concurrently.

Source routes can target exact branch IDs or Keyhive groups. Keyhive groups are
valid tag-like physical filters: they may be private/local or shared and a
document may appear in many. A drawer may therefore maintain one group for all
of its shared branches and another for main branches. Attaching a document to a
shared group still deliberately changes its authority/synchronization graph;
local/private routing groups do not expose that membership to other peers.

Filtering to main branches may use such a main-branch group. Where one is not
maintained, a thin revisioned-store projection over `DocDeltaRevisionStore`
accepts only branch identities for which `BranchId == DocumentId` and can
materialize a local keyed route for later consumers.

#### Facet delta revisioned store

`FacetDeltaRevisionStore` is an inert projection configured with an exact branch
and `FacetKey`. It composes `DocDeltaRevisionStore`, enumerates accepted facet
write points, and includes the authored identity/provenance required by the
dmeta model. Downstream consumer progress remains owned by whichever serial or
concurrent delta walker consumes it.

#### Repository delta revisioned store

A repository configures inert `RepoDeltaRevisionStore<Event>` with a source
store and a pure mapper from explicit previous and current versions to
repository-specific events. It adds no downstream cursor or acknowledgment
behavior.

Initialization is simply:

```text
None -> current version
```

Normal advancement is:

```text
previous accepted version -> current accepted version
```

One source revision may map to zero, one, or several typed events. The enclosing
`RevisionPage` retains the source revision boundary even when there are no
events, so the walker can settle progress without inventing a semantic event. If
several events must succeed as one repository transition, the mapper returns
them in one indivisible projected work item.

This replaces the combination of `events_for_init()`, live listener callbacks,
and ad hoc replay loops. The mapper must not inspect a mutable store snapshot as
a substitute for either explicit version.

### 6. Scope changes are durable input

A physical revisioned store can follow exact branch document IDs and Keyhive
group parts published by the Automerge frontier. Higher semantic stores inherit
or narrow that physical scope; they do not query raw Sedimentree partitions
directly.

Route membership is persisted as part of consumer projection state; processing
an object event must not query `get_obj_parts()` to rediscover all memberships
in the hot path.

Dynamic scope follows these rules:

- Adding a target starts that target at cursor zero and bootstraps every
  accepted object currently in its scope.
- An object present through several routes appears once in the union projection.
- Removing one of several routes does not emit removal while another route still
  includes the object.
- Removing the final route emits one logical removal.
- Sparse per-object diff state is deleted only when the projecting store settles
  that removal in consumer state.
- Re-adding a target after final removal bootstraps it again.

Group membership and object heads must be published as part of the same logical
frontier revision where the source permits it. The Automerge frontier producer
therefore mirrors group memberships into its local scope and cannot discard the
document ID from `PartRemoved` events.

### 7. Generic progress and document projection state are separate

Every durable consumer has simple source progress:

```text
ConsumerProgress {
    durable_contiguous_cursor,
    per_target_source_cursors,
}
```

The concurrent walker wraps this state with an in-memory `WatermarkBook` for
currently fetched and completed revisions. The book is reconstructed empty on
restart; only the drained contiguous cursor and target cursors are durable.

Sparse per-object versions are not part of the generic `DeltaWalker` state. They
belong to `DocDeltaRevisionStore`, whose projection transaction advances its
upstream cursor, updates `DocDeltaState`, and publishes the corresponding output
revision atomically. Keeping these two state types separate prevents an ordinary
downstream walker from acquiring unnecessary per-object rows.

Both are opened by stable consumer identity where required, conceptually:

```text
DeltaWalkerStateRepo::get_progress(stable_consumer_id)
DocDeltaRevisionStateRepo::get_doc_state(stable_store_id)
```

Implementations include:

- in-memory state for non-durable consumers and tests;
- central SQLite state for durable at-least-once consumers;
- consumer-local SQLite state when projection rows and progress must commit
  atomically.

Only a `DocDeltaRevisionStore` with actual interest in a branch retains sparse
branch state. A single-facet store therefore owns one row; a corpus-wide store
owns rows only for branches admitted by its filter. Its published output remains
a revisioned store, so multiple downstream walkers may replay it independently
and may choose serial or concurrent walking without changing document diffing.

### 8. Repository stores are passive materialized views

A store handle does not own a hidden subscription task. It exposes a typed
materialized view and mutation operations. One privileged repository delta
walker composes `RepoDeltaRevisionStore`, repository consumer state, and the
chosen serial or concurrent walking policy to maintain that view.

The materialized view may be ephemeral or durable:

- An ephemeral store keeps its accepted value and materialization watermark in
  memory and rebuilds them by replay after restart.
- A durable store commits projection rows, inherited `DocDeltaState`, and
  consumer progress in its database transaction. An in-memory cache, if any, is
  published only after that transaction commits.

A durable index is the general form of the second case. Repository-maintained
durable caches and search/application indexes differ in domain and query API,
not in replay semantics.

Local mutation follows this order under the repository's serialization gate:

1. Load the accepted current heads.
2. Build and validate the candidate logical value.
3. Write the CRDT change against those heads.
4. Wait until the source accepts the resulting revision.
5. Publish the candidate value and new heads to the passive store.

The in-memory value must not lead durable acceptance.

Remote or source-driven materialization follows a different path under the same
gate:

1. Observe a source revision.
2. Hydrate the latest accepted materialized heads, not necessarily the exact
   older heads carried by a delayed notification.
3. Replace the passive projection without running Autosurgeon `Reconcile` and
   without flushing a write back to the source.
4. Run repository invariants and derive repository events.
5. Advance the repository materialization watermark.

Hydrating latest accepted heads prevents a delayed remote notification from
clobbering a local write already accepted after it. Keeping remote application
out of the mutation path prevents read-triggered write loops.

### 9. Materialization precedes public repository events

One privileged internal repository delta walker owns:

- passive store and cache updates;
- repository invariant checks;
- low-latency ephemeral listener publication;
- the materialization watermark.

Public repository revisioned stores may independently read the source and
version history, but before yielding a typed event for revision R they await:

```text
repository materialization watermark >= R
```

They never derive historical events from whatever value happens to be in the
current mutable projection. This preserves ADR 005's materialization barrier
while removing synchronous fan-out.

The internal walker's task state is observable as `Running`, `Failed`, or
`Stopped`. Liveness is not persisted as durable repository truth. Unexpected
task failure is an invariant violation and must surface through normal
process-failure policy rather than being logged and ignored.

### 10. Facet stores are sibling projections

A `FacetStore` for one document/facet is built directly on
`DocDeltaRevisionStore` or `FacetDeltaRevisionStore`. It is not downstream of a
public repository revisioned store and does not require the repository to
fabricate a broad event merely to wake it.

The store defines an explicit policy for the followed document disappearing from
scope, such as invalidate, close, or reset. Missing-object handling is not
hidden as a generic fallback.

### 11. Watches, work feeds, and history remain separate

The stable service distinguishes:

- `Watch`: low-latency and possibly lossy; a lagged client resnapshots.
- `DeltaWalker` plus consumer state: durable at-least-once work or dirty feed.
- `History`: optional logical or backend version history, with no promise that
  every durable source revision is a pristine user-level operation.

UI code normally uses a watch. Indexers, triage projections, and durable
processors use walkers. Audit and version-browsing features use history.
Ephemeral transport messages never stand in for durable consumer progress.

## Consequences

### Positive

- Consumers replay, fail, and advance independently.
- Late-added consumers can build complete state without resetting existing
  consumers.
- Initialization and live following share one algorithm and one replay boundary.
- Repository events are derived from explicit versions after materialization.
- Exact-object consumers avoid corpus-wide cursor and state rows.
- KeyedFrontier can support memory, SQLite, and later JetStream-backed machinery
  without exposing those storage details through the logical API.
- WIT/wRPC clients can use opaque batches and cursors without receiving Rust
  locks or mutable Automerge handles.

### Costs and trade-offs

- Every durable consumer owns progress state.
- Wide document-delta revisioned stores may own sparse per-object version rows
  across a large corpus.
- Typed deltas may require loading explicit previous and current document
  versions.
- Exactly-once projection effects require a shared storage transaction; other
  consumers remain responsible for idempotency.
- Dynamic source membership becomes durable state rather than a transient
  subscription option.

## Migration

1. Complete the KeyedFrontier contract and memory/SQLite implementations,
   including collapsed writes, atomic multi-key revisions, replay boundaries,
   and lost-wakeup tests.
2. Move the Automerge source feed and group-membership projection onto
   KeyedFrontier.
3. Add the physical, document, facet, and repository revisioned-store layers
   with a shared contract suite.
4. Convert one repository to a passive store plus internal materializer and
   verify local/remote serialization and watermark ordering.
5. Move broad facet-filtered consumers behind the routing frontier defined by
   ADR 009.
6. Add serial and concurrent delta-walker machines, give every durable consumer
   its own stable progress state, and remove synchronous fan-out.
7. Delete the old switch, sink registry, shared cursor tables, synthetic drawer
   content events, and duplicated initialization paths.

## Deferred decisions

- The final WIT/wRPC resource shape for revisioned-store reads and for driving
  delta-walker commands, completions, progress, and scope changes.
- Retention and compaction policy for source revisions needed by lagging
  consumers.
- JetStream implementation details and whether its acknowledgement model can
  implement the full collapsed keyed-frontier contract.
- The complete logical history API.
- Cross-database or external-effect idempotency conventions.
