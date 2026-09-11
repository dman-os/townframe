# ADR 010: Willow-Native Group Collection Synchronization

**Status:** Proposed.

## Context

BigRepo has two distinct synchronization problems:

1. **Collection synchronization:** discover which objects belong to a followed
   collection and which of their advertised versions differ.
2. **Object synchronization:** synchronize one already-known object through
   Keyhive and Subduction.

Willow addresses the first problem. It does not replace or carry the second.
After Willow discovers a missing or divergent object, BigRepo invokes its
existing object-sync API. BigRepo may also invoke object sync directly without
consulting Willow when it already knows the object identifier.

`big_sync` currently combines both concerns. Its parts provide collection
selection, BigRepo maps Keyhive groups to those parts, object payloads advertise
Automerge heads, and exact-object subscriptions also pass through the same
protocol. This made parts appear more fundamental than they are. BigRepo's
actual collection boundary is a Keyhive group.

The current cursor representation is a collapsed frontier rather than an
append-only event log. For each object/part key, only the latest `Added`,
`Changed`, or `Removed` state and its node-local cursor remain. Cursor replay is
an efficient catch-up strategy, while bucket reconciliation repairs peers which
cannot use that cursor.

Willow already defines a native current-state model:

- an Entry belongs to a namespace and has a subspace, path, timestamp, payload
  length, and payload digest;
- Areas select entries by subspace, path prefix, and time range within a
  namespace;
- newer entries prefix-prune older entries;
- Areas naturally describe collections;
- Meadowcap demonstrates Area-scoped read authorization;
- RBSR and Confidential Sync define efficient state reconciliation over Areas.

The available `willow25` Rust crate implements entries, paths, Areas,
Meadowcap, Willow storage, payload-prefix storage, encodings, and the drop
format. It does not currently implement RBSR or Confidential Sync. BigRepo can
adopt Willow state and Area semantics without implementing those protocols
immediately.

Keyhive remains BigRepo's authority system. Keyhive pull access to a group
implies pull access to every object governed by that group. Its granular direct
object grants do not form a geometrical collection, but they do not need to:
Keyhive reveals the object identifier and BigRepo can use exact object sync.
BigRepo does not require a collection operation meaning "everything this
principal may access."

An earlier attempt at Willow integration (`src/daybook_willow`, removed in the
`wip: willow init` line of work) wrapped the upstream `MemoryStore` and
`PersistentStore` behind an enum, precisely because the upstream `Store` trait is
not object safe. That approach inherited upstream's single-threaded internals,
exposed no paging, and never progressed beyond the store wrapper. It is recorded
here as prior art rather than as a base to build on.

## Terminology

- **Group collection:** the objects governed by one Keyhive group.
- **Collection advertisement:** Willow state containing enough object identity
  and version information to decide whether non-Willow object sync should run.
- **Source namespace:** a Willow namespace authored by one advertising node.
- **Group subspace:** the node-derived subspace that makes one Keyhive group a
  geometric Willow coordinate within that node's namespace.
- **Storage scope:** the BigRepo instance partition in a shared database. It is not
  a Willow namespace, subspace, path, or capability.
- **Cursor replay:** an optional optimization which asks for collection
  advertisement state changed after a source-local position.
- **State reconciliation:** comparison of current Willow state without assuming
  that either peer has a usable cursor.
- **Live notification:** a hint that collection state may have changed. It is
  not durable work and need not itself be a Willow protocol.

## Decision

### 1. Willow is the collection-discovery and collection-diff layer

The Willow-facing BigRepo target is a Keyhive group:

```text
CollectionTarget = Group(KeyhiveGroupId)
```

For that target, Willow represents:

- current group membership;
- removals from the group;
- the latest advertised object version known by the source node.

A received advertisement is only evidence that BigRepo may need to synchronize
an object. It never directly creates or mutates the corresponding BigRepo
object.

The resulting boundary is:

```text
Willow group comparison
    -> missing or divergent DocumentId
    -> existing BigRepo object-sync request
    -> Keyhive admission and Subduction synchronization
    -> accepted local object state
    -> local Willow collection advertisement update
```

Willow does not transport Automerge changes, authorize an exact object-sync
request, or replace Keyhive/Subduction object synchronization.

### 2. Collection synchronization is group-scoped

BigRepo supports full collection reconciliation only for Keyhive groups. The
server verifies that the authenticated peer has Keyhive pull access to the
requested group. Because that access applies uniformly to the group's governed
objects, the group maps to one reusable Area per advertising node without any
per-object policy check.

Directly granted and otherwise known objects remain outside Willow collection
sync. Keyhive state, application logic, or an explicit request supplies their
identifiers, and BigRepo synchronizes them using the existing exact-object path.

BigRepo does not expose Willow reconciliation over the arbitrary union of every
object a principal can access. This is an intentional semantic narrowing which
matches BigRepo's group-oriented collection needs and avoids constructing a
principal-specific RBSR view.

The existing global part does not silently become a universal Willow Area. Any
remaining global collection use must be represented as an explicit system,
bootstrap, or administrative collection with separately documented authority.

### 3. Each advertising node owns a Willow namespace

A node publishes only collection state it has accepted locally. Remote Willow
state is input to local decisions; it is not copied into the local source
namespace merely because it was learned.

Conceptually, for one node:

```text
Namespace(node)
    Area(node x group G)
        object advertisements for G
    Area(node x group H)
        object advertisements for H
```

The collection schema is:

```text
namespace   node            owned; derived from the node identity seed
subspace    (node, group)   derived per group from the node identity seed
path        /<document-id>
timestamp   per-publication monotonic stamp
scope       storage partition, not part of the Willow data model
```

**Namespace = node.** Each node owns one Willow namespace, derived from its
identity seed, and authors every advertisement in it.

This is required for cursor replay, not merely conventional. Replay selects entries
by `timestamp > C`, which is a sound predicate only if every entry in the namespace
was authored locally and therefore carries a locally monotonic timestamp. A
namespace shared by several writers would mix unrelated wall clocks and make `C`
meaningless without a non-Willow receipt revision.

It also preserves the option of Willow-native read authorization. A communal
namespace has no secret, so a communal read capability cannot be a security
boundary; only an owned namespace can later carry Meadowcap read capabilities.

**Subspace = (node, group).** The group is a geometric coordinate, not a path
convention. A node derives a distinct subspace keypair per group from its own seed
and the group identifier, and mints its own write capability from its own namespace
key. No coordination with other members and no group key material is required.

A Keyhive group identifier is not usable as the subspace. A Keyhive `Group` carries
an identifier and membership state but no signing key available to members, and a
shared subspace secret would be wrong regardless: entries from different members
would prefix-prune each other under unrelated timestamps, and authorship would
become unattributable.

A `(node, group)` pair therefore maps to exactly one Area:

```text
Area { subspace: Some(node_group_subspace), path: full, times: full }
```

The schema must preserve these properties:

- group membership can be represented before an object version is available;
- an object version can be known locally before any group membership is known;
- learning either later can complete the group advertisement without losing the
  earlier state;
- removal remains visible to a lagging peer as Willow state rather than local
  `forget_entry`;
- entries for one group cannot accidentally prefix-prune another group;
- a node's namespace records only that node's accepted view.

Membership and version may be separate entries or separate fields of one application
encoding. This ADR requires their independent production semantics, not a particular
layout.

**Scope is not a Willow concept.** Scope is the storage partition isolating BigRepo
instances that share a database, exactly as `scope_id` does today. It is not part of
a namespace, subspace, path, or capability. Making it a namespace would require a
distinct namespace secret per scope and would turn the same Keyhive group into two
different collections, which is the opposite of sharing a group across scopes.

### 4. Multi-group membership may duplicate advertisements

An object governed by several groups participates in several independently
selectable Areas. Willow may consequently contain and transmit one small
collection advertisement per group.

This M×N write and metadata-wire amplification is accepted. It expresses real,
independent collection membership and is already inherent in the current
part-based model. Advertisement payloads must remain small.

BigRepo deduplicates the expensive consequence: equal advertisements for one
object version discovered through several groups produce one non-Willow object
synchronization job.

### 5. Keyhive remains authoritative for read access

The first implementation does not maintain a second persistent Meadowcap graph
which mirrors Keyhive.

Before disclosing a group Area, the server verifies Keyhive pull access to the
group. A peer lacking that access receives no object identifiers, paths,
versions, digests, tombstones, or fingerprints for the group.

Meadowcap remains compatible with the model: a Keyhive group grant could later
be projected into a Willow read capability for the corresponding Area. That is
an optional transport integration, not a second source of authority and not a
prerequisite for the first implementation.

Revocation remains driven by Keyhive. Loss of one peer's access is not a global
Willow tombstone because other peers may remain authorized. A collection
tombstone means that an object left a group in the source node's accepted view;
it is distinct from one recipient losing access to the group.

### 6. Willow storage is Willow-native

The authoritative collection representation is a Willow store implementing
Willow's specified entry, authorization, and prefix-pruning semantics. It is
not a `KeyedFrontier` database with Willow terminology layered over it.

`big_willow` owns its store implementation rather than wrapping the upstream
`willow25` stores. Upstream `MemoryStore` and `PersistentStore` are single-threaded
by construction: they hold `Rc` over a lock built from `Cell` and `UnsafeCell`. The
upstream `Store` trait is also not object safe, because its methods are `async fn`.
Neither is usable from BigRepo's multi-threaded actors.

`big_willow` therefore defines its own store trait which is `Send + Sync`, object
safe, and exposes the paging and ordered range reads that the upstream `Store` trait
lacks. It additionally provides an adapter implementing `willow25::Store`, so the
implementation can be validated against Willow's specified semantics and used as a
general Willow store rather than only as a BigRepo component.

The store contract and the data model it describes stay generic. `big_willow` must not
model Keyhive groups, BigRepo documents, or any other embedder concept: those are
embedder layers that use Willow, not parts of Willow.

A concrete backend may nevertheless carry storage-level concerns the contract does not
name. The SQLite backend partitions rows by a caller-supplied scope so several
embedder instances can share one database, and it installs its migrations under its
own migration table so its schema can coexist with other modules' tables. Foreign keys
between the Willow schema and an embedder's schema are anticipated. None of this is
visible in `WillowStore`: scope is fixed when a backend is constructed and is never a
parameter of an entry or an `Area`.

Object identifiers are byte strings, not fixed-width values. A `BigRepo` document
identifier happens to remain a 32-byte Keyhive-derived value, but `big_willow` must
not encode that assumption.

The store key encoding must therefore preserve Willow's ordering exactly. `Path`
orders component-wise lexicographically, so neither raw concatenation nor a
fixed-width length prefix is a valid encoding of the component sequence. The
implementation uses an order-preserving component encoding and asserts it against
Willow's own ordering.

Willow paths are not `camino` paths. Components are arbitrary byte strings, so a UTF-8
path type cannot represent them, and filesystem semantics such as separators, `..` and
absoluteness do not describe component-prefix comparison. The `camino` usage elsewhere
in this workspace is for user-facing document paths that cross an FFI boundary as
strings, which is a different problem.

SQL lives in `sql/migrations` and `sql/queries` as `.sql` files. Queries are loaded
with `include_str!`. No SQL is written inline in Rust and no `sqlx` query macros are
used.

The first persistent implementation will be SQLite-backed so it can integrate
with BigRepo. It may maintain Willow-specific indexes and sidecar state needed
for efficient queries, transactions, cursors, notifications, and future range
fingerprints. Those structures accelerate or observe the Willow store; they do
not redefine its merge semantics.

When BigRepo and Willow state share a SQLite database, APIs should permit a
caller-owned transaction so accepted BigRepo state and corresponding Willow
advertisements can commit atomically. The exact transaction hook is an
implementation detail, but the implementation must not create a window where a
committed advertisement names application state which was rolled back.

The initial implementation must satisfy reusable Willow store contract tests.
It must not claim general Willow compatibility while implementing only exact-key
replacement and omitting prefix pruning or authorized-entry validation.

Prefix pruning follows the reference implementation, which differs from a strict
reading of the specification in both directions. Where two entries in one
subspace tie exactly on recency and one path is a prefix of the other, the
specification's `prunes` relation does not relate them, so a strict reading keeps
both. `willow25`'s in-memory store prunes the tie instead, and the published
`store_pruning` vectors were generated by running that store, so the vectors
require the tie to prune. `big_willow` prunes it too: the vectors are the only
executable definition of store semantics available, the invariant a store must
maintain ("no two retained entries prefix-prune each other") holds either way,
and the reference tried strict pruning once (`3eda33b`) and reverted it the same
day (`435a56b`, "Oops"), which reads as intent rather than accident.

A tie at the new entry's own path is the one exception. That row is replaced in
place rather than pruned, which is what keeps a retained payload attached to an
entry that did not change.

The second half of the reference behaviour is that re-inserting the entry that is
already stored is a no-op, and the no-op is observable: it prunes nothing, not
even the entries the insert would otherwise remove. One `store_pruning` vector
depends on this — it stores `/`, then a descendant that ties it, then `/` again,
and requires the descendant to survive.

`big_willow` therefore registers `store_pruning` as a passing suite rather than
carrying an unexplained exclusion. `willow-ts` documents that it does not pass
this suite, which is consistent: it implements strict pruning. There is no
cross-implementation agreement on this point, and the reference implementation
is the tiebreaker.

Those contract tests are the crate's own, so they cannot establish conformance
by themselves: they encode the same reading of the specification as the
implementation does. `big_willow` therefore also checks itself against the
protocol's published test vectors. The vectors are fetched rather than
vendored (`./x/willow-vectors.ts`, pinned to an exact revision) and are read
only behind a non-default `conformance` feature, which fails loudly when the
corpus is absent so that a missing corpus cannot be mistaken for a pass.

The absolute `EncodeMeadowcapAuthorisedEntry` encoding is checked first,
because the other suites are expressed through it and the store persists
entries with exactly those bytes. Decoding must consume its input exactly: a
decoder that accepted a valid prefix would accept a truncated and padded blob.

The specification's prefix relation is checked against `prefix_range`, because that
is the property the storage layout rests on: a byte-keyed store cannot call
`Path::is_prefix_of`, so every prune and every area read depends on the byte range
admitting exactly the prefixed paths. The range's upper bound is additionally
checked against the specification's successor operation, which is defined
independently of any encoding.

The entry relations the store's own logic is written against are checked
directly: recency, prefix pruning, and pruning's reciprocal. A divergence in
any of them would silently change which publication wins, and recency is
otherwise only exercised through the store's own behaviour.

### 7. First-contact diff converges on full Willow RBSR

The target first-contact protocol is Willow RBSR over the current authorized
group Area. BigRepo will adopt an upstream compatible implementation when one is
available or implement the required integration when justified. RBSR, rather than
the current BigSync bucket protocol, is the intended long-term source of
cursor-independent collection correctness.

Until RBSR or Confidential Sync is available, the minimal correct implementation
may page through all current advertisement metadata in the Area. It then invokes
object sync only for missing or divergent objects. This is a migration-stage
fallback, not the target reconciliation design.

The Willow SQLite store must support efficient range queries and must leave room
for persistent, composable range fingerprints. Such summaries are Willow storage
acceleration state, not a replacement authoritative frontier. Their exact schema
is deferred until the RBSR integration establishes its required interface.

### 8. Cursor replay is an optimization over Willow state

For repeated synchronization with the same source namespace, BigRepo may expose
a cursor replay API over the Willow data:

```text
read group Area states changed after source cursor C
    -> finite pages of current Willow entries
    -> completion through source cursor T
```

The cursor API does not define a second authoritative frontier and does not
replace Willow reconciliation. It is a source-local acceleration index or
change observation over mutations of the canonical Willow store. If its cursor
is absent, invalid, too old, or inconsistent, the receiver falls back to Willow
state reconciliation.

Replay uses the Willow timestamp as the cursor coordinate. Section 3 is what makes
this sound: every entry in a source namespace is authored by that node, so its
timestamps are locally monotonic. Within one publication, all entries share a single
freshly allocated timestamp that is strictly greater than every timestamp the node
has previously published, so `timestamp > C` never skips a publication and never
re-delivers one.

Whether an atomic local publication additionally needs a sidecar receipt revision
remains open. The intent is that it does not.

Cursor reads must be finite and paged. A request captures or reports a fixed
upper boundary, and one atomic local publication must not be acknowledged only
partially. Reading has no acknowledgment side effect; the receiver advances its
durable source cursor only after every discovered object-sync consequence has
settled locally.

Existing `RevisionedStore` abstractions may inform or implement this sidecar
cursor reader, but their current API and replay/live semantics are not part of
the Willow storage model and are not mandated by this ADR.

### 9. Live notification is orthogonal to Willow reconciliation

Willow owns durable collection state. Low-latency notification that this state
changed may initially be implemented independently. If Willow later provides a
live-following arm which fits these requirements, BigRepo should use it rather
than preserving a parallel protocol for its own sake.

A notification need only identify the source and indicate that a group may have
newer state. It may be a coalescing watch value, bounded stream, long poll, or
another transport primitive. It may be duplicated or lost. On notification or
reconnection, the receiver performs a finite cursor read when possible and Willow
RBSR when cursor-independent reconciliation is necessary.

Source mutation must not block on a subscriber consuming an event channel, and
an infinite pushed stream must not become a second durable work queue. The
receiver controls backpressure by deciding when and how much state to request.
Durable Willow state and the receiver's stored cursor—not notification delivery—
determine correctness.

`LiveRevisionWatch`, `RevisionedStore`, and the current notification machinery
are possible interim implementation tools. This ADR intentionally does not
require their semantics to shape Willow or preclude adopting a suitable future
Willow live protocol.

### 10. Object-sync settlement remains a BigRepo concern

After collection comparison identifies an object, BigRepo schedules its
existing object synchronization operation. BigRepo owns:

- Keyhive admission;
- Subduction transfer;
- materialization barriers;
- retries and per-object work deduplication;
- deciding whether accepted local state changed;
- publishing the resulting local collection advertisement;
- deciding when a remote cursor can safely advance.

Willow storage does not need to know how the object is fetched or materialized.
Conversely, direct object sync does not need to create a Willow query. It updates
Willow only when the accepted result changes collection advertisement state.

The current concurrent walker, scheduler, watermark, and settlement machinery
may be reused behind this BigRepo boundary. Their reuse is an implementation
choice rather than part of Willow's data or synchronization model.

### 11. Willow drop transport is deferred

The Willow drop format can stream selected authorised entries and payload
slices, but it does not define cursor boundaries, live notification,
authorization policy, or settlement. This ADR does not select it as the cursor
or reconciliation wire format.

The Area and storage boundaries must leave room to use drops, RBSR, or
Confidential Sync later without changing BigRepo's separation between
collection discovery and object synchronization.

## Consequences

### Positive

- Willow is used for the problem it directly models: group collection state and
  collection comparison.
- Object synchronization remains on the established Keyhive/Subduction path.
- BigRepo can request synchronization of a known object without Willow.
- The authoritative representation follows Willow's specified storage and merge
  semantics rather than a BigSync-specific frontier abstraction.
- Keyhive groups map to reusable geometric Areas with one authorization check
  per collection.
- Direct granular grants do not require principal-specific reconciliation sets.
- First contact has a simple interim implementation and a committed path to full
  Willow RBSR.
- Cursor replay and interim live notification can improve repeat-sync latency
  without becoming correctness foundations.
- A suitable future Willow live-following protocol can replace the interim
  notification mechanism.
- Notification loss and slow consumers do not lose durable collection state.
- Multi-group metadata duplication does not duplicate object transfer.

### Costs and trade-offs

- An object in several groups requires advertisements in several Areas.
- Full first-contact reconciliation may initially transmit every current
  advertisement in a group.
- BigRepo must join independently produced membership and object-version state.
- A SQLite Willow store requires correct prefix pruning, authorization, payload,
  range-query, and transaction behavior.
- Cursor acceleration requires sidecar indexing or disciplined timestamp use.
- Keyhive synchronization remains necessary for authority, group discovery, and
  direct document grants.
- Some current `big_sync_core` machinery may not fit the Willow-native design and
  may be retired rather than preserved.

## Performance expectations

The common repeated-sync path should inspect current group entries whose latest
publication is newer than the receiver cursor, not historical mutations and not
the arbitrary union of every object accessible to the principal.

First-contact enumeration is paged and transfers advertisements rather than
object content. Object synchronization is proportional to distinct divergent
objects after multi-group deduplication.

Live notification is O(1) coalescing state per observed source or group and must
not queue every mutation for every subscriber. Receiver memory and outstanding
object work remain explicitly bounded.

Implementations must not rebuild a full in-memory collection per connection,
maintain per-peer RBSR trees for arbitrary ACL unions, retain an unbounded event
log, or treat a notification queue as durable state.

## Migration

1. Add the `big_willow` crate with its own `Send + Sync`, object-safe store trait,
   an order-preserving path key encoding, and a `BTreeMap` reference store. Run one
   contract suite against the reference store, including an adapter implementing
   `willow25::Store`.
2. Implement the same trait over SQLite in `big_willow`, sharing the contract suite,
   with `sql/migrations` and `sql/queries` files and caller-owned transaction
   integration where required.
3. Define the advertisement schema for group membership, object-version
   advertisements, and collection tombstones, together with the node namespace,
   per-group subspace derivation, and group Area mapping.
4. Mirror locally accepted Keyhive group membership and object-version changes
   into node-authored Willow namespaces while retaining existing BigSync.
5. Implement Keyhive-authorized, paged group Area enumeration as the first-contact
   reconciliation path.
6. Feed divergent discoveries into the existing BigRepo object-sync API and
   verify admission, materialization, and multi-group deduplication.
7. Add an optional finite cursor API over canonical Willow mutations and a lossy
   coalescing live-notification mechanism.
8. Compare collection convergence, revocation, restart, and stress behavior with
   the existing BigSync implementation.
9. Move BigRepo group collection synchronization to Willow and remove obsolete
   part-shaped APIs and storage.
10. Implement or adopt full Willow RBSR for first-contact group reconciliation;
    retain full Area enumeration only as an interim compatibility path.
11. Replace the interim live mechanism if Willow gains a better fitting
    live-following protocol.

## Deferred decisions

- Exact path layout and advertisement payload encoding.
- Whether membership and version are separate entries or one combined entry.
- The exact derivation of the node namespace secret and per-group subspace keys.
- Encoding of independently available membership and object-version state.
- Whether Willow timestamps are sufficient as source cursors without any sidecar
  receipt revision, and what the continuation token for a paged cursor read is.
- Sidecar cursor schema and atomic publication boundary.
- Cursor page and continuation-token wire format.
- Live-notification transport and granularity.
- Tombstone retention under Willow pruning semantics.
- SQLite range indexes and future RBSR summaries.
- Meadowcap projection from Keyhive group authority.
- Willow drop use.
- The disposition of the existing global part.
- Which `big_sync_core` abstractions remain reusable after the Willow-native
  storage and protocol APIs are defined.
