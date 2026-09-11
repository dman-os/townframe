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

### 3. Each group is one communal Willow namespace

A node publishes only collection state it has accepted locally. Entries authored by
other members are held as a replica of the group's collection, verified against their
signatures and capabilities, and never treated as this node's own authorship.

Conceptually, for one group:

```text
Namespace(group)
    Area(node A x group G)
        advertisements authored by A for the group
    Area(node B x group G)
        advertisements authored by B for the group
```

The collection schema is:

```text
namespace   group           shared collection identity
subspace    (node, group)   the node's per-group public key, derived from its seed
path        /<document-id>
timestamp   per-author monotonic publication stamp
scope       storage partition, not part of the Willow data model
```

**Namespace = communal, subspace = author.** The namespace is the group's collection,
not a node's private stream. Every member authors entries in its own subspace,
`(node, group)`, inside that one namespace, and every member may hold a replica of
the whole collection.

This is what makes the collection reconcilable, and it is the load-bearing reason
the namespace is not per node. Reconciliation compares two peers' entries, so it
is meaningful only when both peers hold the *same* entries. An entry is identified
by namespace, subspace, path, timestamp, capability, and signature, so two nodes
re-authoring the same advertisement under separate namespaces never produce
matching entries and every range mismatches. Under one shared namespace an
advertisement is authored once by the member that observed the change, and its
peers replicate that entry verbatim.

Nothing in Willow's semantics needs clocks correlated across nodes. Prefix pruning
requires equal namespace, equal subspace, a prefix path, and greater recency, so
recency is only ever compared within one subspace. A subspace has exactly one
author, so its timestamps are locally monotonic, and a per-subspace cursor is sound
without any cross-node reference.

The namespace is communal rather than owned. A communal namespace has no secret:
its namespace identifier is not a key and confers no ability to write or to grant
access. Ownership of a subspace is cryptographic — the subspace identifier is the
author's public key, and holding the corresponding secret is the entire authority to
write in it — so a member needs no capability, no delegation, and no coordination
with anyone to author its own advertisements, and cannot author anyone else's.
Read capabilities remain meaningful and are area-restricted, so a subspace owner can
mint access for its own area and delegate onward. Whether group-wide read access is
expressed as delegations from every subspace owner or stays with Keyhive at serve
time is deferred; §5's first implementation uses Keyhive.

**Derived identifiers.** The namespace identifier is derived deterministically from the
Keyhive group identifier, so every member computes it without exchanging key
material. A subspace identifier is the member's per-group public key, and its
derivation from that member's identity key should be verifiable by third parties — an
additive derivation over the identity public key, for instance — so a replica can
decide which subspaces it is willing to accept without a certificate per member per
group. Whether to accept only such subspaces, or to gate participation purely on
Keyhive at serve time, is deferred.

**Every member advertises its own view, and every member can serve.** A member writes
entries only in its own subspace: for each document it holds in the group, its local
version commitment, and a tombstone when the document leaves. A reader therefore sees
one advertisement per (member, document), which is not redundancy — it is how the
reader learns which peers hold which version. Because every member holds a replica of
the whole collection, any member can serve a synchronization request for the group.

**Alternative under consideration: one entry per fact.** A shared subspace holding one
entry per (group, document) — subspace = the group, path = the document identifier,
timestamp = a monotone function of the version such as its operation count, payload
digest and length = the version commitment — written under a single delegated write
capability shared by the members. Every member that observes the same version then
constructs the *same entry bytes*, since the signature is deterministic. Writes become
idempotent, exactly one entry exists per fact rather than one per member, and a peer
can derive its own side of a reconciliation from its own object state without keeping
a replica of anyone else's entries. The costs are that the entry is attributed to the
group's capability rather than to a member, so write authorization and revocation
become policy checked at serve time instead of cryptographic, and that the namespace
becomes owned rather than communal, which raises the question of who holds the
namespace key. Choosing between the two mappings is open.

**Subspace = (node, group).** The group is a geometric coordinate, not a path
convention. A node derives a distinct subspace keypair per group from its own seed
and the group identifier. Only that node can sign entries in that subspace, so
authorship is attributable without coordination; holding that subspace's secret is
the whole of the authority to write it, and nothing is granted at admission.

A Keyhive group identifier is not usable as the subspace. A Keyhive `Group` carries
an identifier and membership state but no signing key available to members, and a
shared subspace secret would be wrong regardless: entries from different members
would prefix-prune each other under unrelated timestamps, since pruning requires
equal namespace *and* equal subspace, and authorship would become unattributable.

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
- a replica holds only entries whose signature and capability verify, while a node's
  own subspace holds only what that node accepted locally.

**The advertisement is one entry per group Area.** For one node, one group, and
one document, the canonical advertisement is a single entry:

```text
subspace        (node, group)
path            /<document-id>
timestamp       per-publication monotonic stamp
payload_digest  commitment to the object version, or the empty payload
payload_length  length of that commitment, zero for a tombstone
```

Membership and version are fields of one application encoding rather than
separate entries, so that the Area of a group is *complete*: a paged read, a
range fingerprint, and a future read capability over `Area(node x group)` all
cover both, and no part of the collection is reachable only through a
non-Willow join.

The version enters the entry as its payload *commitment*, not as a retained
payload. Every Willow entry already carries `payload_digest` and
`payload_length`, both are covered by the entry's signature, and a range
fingerprint covers all aspects of the entry (§7). A peer can therefore compare
object versions without any payload byte being stored or transferred. The Willow
layer never retains a payload: entries are written metadata-only, and version
content stays object-layer state (§10), fetched only for objects the diff marks
as divergent.

Version commitments must be canonical and non-empty. The digest is computed over
a canonical encoding of the object version, so that equal versions yield equal
digests on every node, and `payload_length` is that encoding's length. A
tombstone — the object left the group — is the empty payload: `payload_length`
zero with the digest of the empty string. That is an ordinary valid Willow entry
rather than a reserved convention, and its zero length distinguishes it from any
live advertisement, whose encoding is never empty.

This layout is forced by Willow's containment rule, not chosen for convenience.
An Area contains a coordinate exactly when the time range contains the
timestamp, the subspace matches (or the Area's subspace is `None`), and the
coordinate's path is prefixed by the Area's path:

```text
includes(coord) = times.contains(coord.timestamp)
               && subspace.map(|s| s == coord.subspace).unwrap_or(true)
               && coord.path.is_prefixed_by(self.path)
```

So the Areas containing one entry are the prefixes along that entry's single
path, intersected with its subspace: a chain, not an arbitrary set. Two Areas
with different `Some(subspace)` values do not intersect at all, and
`Area::intersection` returns `EmptyGrouping` for them. Since a group Area is
exactly a box over `(node, group)`, one entry lies in exactly one group Area,
and an object governed by M groups needs M advertisement entries. A version
change rewrites all M.

No layout avoids this:

- making the document the subspace and the group a path component only moves the
  duplication from the subspace dimension to the path dimension;
- placing the version at a path outside the group's prefix removes it from the
  group Area, and an Area cannot reference an entry it does not contain;
- Willow has no pattern matching to bridge the two. Path selection is
  prefix-and-range only, and the sole wildcard in the model is
  `subspace: None`.

The properties listed above are therefore properties of the source's accepted
state, not of separate storage locations: an advertisement written before the
version was known is rewritten in place at the same path with a later timestamp,
and prefix pruning replaces the earlier advertisement rather than retaining both.

**Scope is not a Willow concept.** Scope is the storage partition isolating BigRepo
instances that share a database, exactly as `scope_id` does today. It is not part of
a namespace, subspace, path, or capability. Making it a namespace would require a
distinct namespace secret per scope and would turn the same Keyhive group into two
different collections, which is the opposite of sharing a group across scopes.

### 4. Multi-group membership duplicates advertisements by construction

An object governed by several groups participates in several independently
selectable Areas: §3 shows that one advertisement entry per group is forced by
Willow's containment rule.

Given that, a version change of an object in M groups can be served two ways:

- **write time**: rewrite all M advertisements at the new version, keeping each
  group Area current and self-contained;
- **read time**: keep only membership in the Area and join object-version state
  into the read, so that a version change writes once.

Read time is rejected. A join answers a point query but cannot be reconciled:
range reconciliation, range fingerprints, page boundaries, and read capabilities
all operate on the materialized entries of an Area, and a predicate over state
outside that Area is not an Area. Under read time, discovering which objects
diverge during first contact or range reconciliation would require materializing
the join in memory across the whole collection before anything could be
summarized — exactly the work that write-time materialization performs once and
then indexes.

The cost is also not symmetric, and read time is not a cheaper form of the same
thing:

- write time pays storage proportional to membership, M entries per object, and
  on the wire the per-subspace signatures that make each advertisement an
  independently verifiable entry;
- read time avoids that storage and those signatures, and pays instead in read
  CPU: a join per request, on the serving node's hot path, repeated once per
  receiving peer for the same collection state, and it cannot be summarized or
  verified.

Write time is paid once per publication and amortized across every peer and every
later read; read time is paid on every request forever. The remaining wire
difference is small: coalesced write-time advertisements carry the same path,
timestamp, payload digest, and payload length once, so the delta over a synthesized
roughly one signature per subspace.

The duplication is therefore accepted as the price of each group Area being a
complete, independently reconciliable, and capability-attenuable collection. The
No payload is duplicated: the version enters an entry as its payload commitment
(§3) and the Willow layer retains no payload, so the per-entry cost is the entry
header — coordinate, digest, length, and signature — and nothing proportional to
object size.
the same class of cost the current part-based model already pays, and that model
pays more of it for less: one object version change rewrites a membership row per
part and performs a per-part bucket transition, and none of that state is a
collection a capability or a range reconciliation can address.

The M entries of one publication are written by one multi-row statement inside that
publication's transaction, not by a loop of statements.

BigRepo deduplicates the expensive consequence: equal advertisements for one
object version discovered through several groups produce one non-Willow object
synchronization job.

### 5. Keyhive remains authoritative for read access

The first implementation does not maintain a second persistent Meadowcap graph
which mirrors Keyhive.

Before disclosing a group Area, the server verifies Keyhive pull access to the
group. A peer lacking that access receives no object identifiers, paths,
versions, digests, tombstones, or fingerprints for the group.

Meadowcap remains compatible in both directions. Write authority is not delegated at
all: a subspace is identified by its author's public key, so an entry is authorized
by its subspace signature and is attributable to exactly one member. Read authority
is delegable and area-restricted, so a subspace owner can mint access to its own
area. Keyhive remains the authority for whole-group access at serve time, because a
group-wide read capability would require a delegation from every subspace owner
(§3). This is an optional transport integration, not a second source of authority
and not a prerequisite for the first implementation.

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

The target first-contact algorithm is Willow RBSR over the current authorized
group Area. Because the Area is complete (§3, §4), one range reconciliation over
it resolves membership and object versions together, with no separate
version-summary protocol.
(§11) rather than depending on an external transfer arm; RBSR, rather than the
current BigSync bucket protocol, is the intended long-term source of
cursor-independent collection correctness.

Until RBSR is implemented, the minimal correct implementation may page through all
current advertisement metadata in the Area, and invoke object sync only for missing
or divergent objects. This is a migration-stage fallback, not the target
reconciliation design.

RBSR's wire cost is set by its range structure, not by entry size. Peers exchange
one fixed-width fingerprint per probed `3dRange`, 32 bytes in `willow25`, and send
entry sets only for ranges that remain small and mismatched, so regions the peers
already agree on cost one fingerprint each. A fingerprint covers all aspects of an
entry: namespace, subspace, path, timestamp, `payload_digest`, `payload_length`,
and the number of available payload bytes. The version commitment is
`payload_digest` and `payload_length` (§3), so reconciling a group Area resolves
object versions and membership together at no wire cost above membership alone.
The levers are the number of sub-ranges split per round and the cutoff at which a
peer switches from fingerprints to entry sets.

A fingerprint's coverage of available payload bytes also constrains storage: if one
peer retained payloads and another did not, reconciliation would chase differences
that are not version differences. This is a second reason the Willow layer never
retains payloads (§3). It is also why a per-entry fingerprint shorter than the
payload digest would be pure loss: it would weaken the local version comparison
without reducing reconciliation traffic, since no per-entry marker is ever sent for
an agreeing range.

Reconciliation is cheap in proportion to what two peers differ on, so the replica is
what keeps repeat synchronization cheap: a peer holding almost the same entries
transfers only the difference. A cursor does not replace that replica; it is a
low-latency shortcut over one author's stream, sound because a subspace has one
author, and it does not expire, because the delta is derived from the source's
current state rather than from a retained log. A source whose publication boundary
moves backwards, as after a restore, is caught by comparing the boundary a response
reports against the stored cursor.

RBSR is cheap relative to enumeration but never cheap relative to a working cursor.
Its cost is the fingerprints spent descending to the differences, plus the differing
entries themselves: with an adaptive split, on the order of `d log(N/d)` fingerprints
of 32 bytes for `d` differences among `N` entries, a logarithmic number of
communication rounds, and one entry header per differing key. A cursor read of the
same delta costs one round trip, the same entries, and no fingerprints at all. That
is why a cursor remains worth having as a low-latency shortcut, while reconciliation is
what makes collections converge.
The split policy is the tuning knob; `willow25` implements no RBSR, so these are
complexity expectations rather than measurements.

Range reconciliation is meaningful here because §3 makes the collection a replicated
set. Both peers hold the same entries — the author's entry for an object version,
replicated verbatim with its subspace, timestamp, capability, and signature — so
equal state yields equal fingerprints and the differences are exactly the entries one
side lacks. That is what makes two mostly identical peers meeting for the first time
cheap: they already hold almost the same entries, almost every range matches, and only
the difference is transferred. A peer meeting a collection from nothing receives all
of it, which is proportional to what it must fetch anyway.

The compared values are the authored entries themselves; nothing needs correlated
clocks. Recency and pruning are per subspace (§3), and a fingerprint hashes a
timestamp rather than interpreting it.

Reconciliation is bidirectional, so a plain deletion would be undone by whichever peer
still held the entry. Removal is therefore a tombstone that prunes the advertisement
it replaces within the same subspace (§3), which converges because the tombstone is
newer under the only comparison Willow offers.

Fingerprints over a node's own subspaces are peer-independent, so one computation is
served to every peer and no per-peer fingerprint tree is needed. They can also be
maintained across mutations rather than rebuilt: an additive fingerprint over fixed
sub-ranges accepts removing one entry and adding another, which is the persistent
range-fingerprint acceleration state this ADR leaves room for. The caveat is that
reconciliation picks its sub-ranges adaptively, so a cached sub-range only helps when
a split coincides with it.

The Willow SQLite store must support efficient range queries and must leave room
for persistent, composable range fingerprints. Such summaries are Willow storage
acceleration state, not a replacement authoritative frontier. Their exact schema
is deferred until the RBSR integration establishes its required interface.

### 8. Cursor replay is an optimization over Willow state

For repeated synchronization with the same source subspace, BigRepo exposes one
cursor replay API over the Willow data:

```text
read subscription set S of group Areas
    after:   Timestamp          exclusive lower bound on entry timestamp
    through: Timestamp          inclusive snapshot boundary chosen by the source
    resume:  Option<EntryKey>   intra-pass continuation, exclusive
    limit:   usize              soft page bound
    wait:    bool               bounded long poll
    -> advertisement runs, each carrying the subspaces it appears in
    -> next: Option<EntryKey>
    -> through: Timestamp
```

The cursor API does not define a second authoritative frontier and does not
replace Willow reconciliation. It is a source-local acceleration index or
change observation over mutations of the canonical Willow store. If its cursor
is absent, invalid, too old, or inconsistent, the receiver falls back to Willow
state reconciliation.

Replay uses the Willow timestamp as the cursor coordinate. Section 3 is what makes
this sound: every entry in a source subspace is authored by that node, so its
timestamps are locally monotonic, and a subspace is what a cursor is scoped to.
freshly allocated timestamp that is strictly greater than every timestamp the node
has previously published, so `timestamp > C` never skips a publication and never
re-delivers one.

Publication allocation and commit are one transaction, so a publication becomes
visible only when it is complete. Within a single-writer store that is what makes
`timestamp > C` sound: no reader can observe half of a publication and no
publication can become visible out of order. A store with several concurrent
writers or out-of-order commits would need an additional read watermark or receipt
revision; this ADR does not assume one.

The durable cursor for one source and one Area is the timestamp alone, while
`resume` is intra-pass paging state, because the time range is a read filter while
page order is `EntryKey` order. A page boundary is snapped to an advertisement
run, so that the advertisements of one publication are not split across pages,
and `through` is always the source's publication boundary.

`wait` is a bounded long poll on the source's publication boundary for the
namespace, so the source keeps no per-subscriber state. A wait that observes no
new publication returns an empty page rather than an error, which doubles as a
heartbeat and forces the receiver to re-resolve its subscription set.

The M advertisements of one object version are equal in path, timestamp, payload
digest, and payload length and differ only in namespace and subspace, so one read
may return them as one run carrying the namespaces and subspaces it appears in and
one signature per subspace. Coalescing is
a wire encoding over canonical entries, not a store or specification mechanism: a
source must remain correct when it does not coalesce, and a receiver must remain
correct when it does.

Reading has no acknowledgment side effect: a cursor read neither consumes entries
nor notifies the source. The receiver advances its durable source cursor only
after every discovered object-sync consequence has settled locally.

Existing `RevisionedStore` abstractions may inform or implement this cursor
reader, but their current API and replay/live semantics are not part of
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

### 11. BigRepo owns the collection protocol

No WTP, Confidential Sync, or Willow drop arm is a target of this design. The
collection protocol is BigRepo's own: the bundled, paged, group-Area cursor read
with a bounded long poll described in §8, together with the existing BigRepo
object-sync protocol for payload transfer (§10).

This is a decided boundary, not a fallback. Membership-attenuated collection state
requires the object version to be materialized inside each group Area (§3, §4), so
a transfer protocol built around a Willow-native attenuation arm would have to be
re-derived around that choice in any case, and no such arm exists today. Willow's
contribution here is its data model and semantics — entries, Areas, prefix
pruning, timestamps, and authorization — not a transfer format.

The Area and storage boundaries must still leave room to replace the transfer
algorithm with drops, RBSR, or Confidential Sync later without changing BigRepo's
separation between collection discovery and object synchronization. RBSR is
anticipated as an algorithm (§7) that BigRepo implements over its own protocol
rather than as an external dependency.

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
- One paged read or one range reconciliation over a group Area covers membership
  and object version together, so authorization, cursor replay, and first contact
  all act on the same collection.
- The collection is one replicated set, so equal state compares equal and repeat
  synchronization transfers only the difference.
- First contact has a simple interim implementation and a committed path to full
  Willow RBSR.
- Cursor replay and interim live notification can improve repeat-sync latency
  without becoming correctness foundations.
- A suitable future Willow live-following protocol can replace the interim
  notification mechanism.
- Notification loss and slow consumers do not lose durable collection state.
- Multi-group metadata duplication does not duplicate object transfer.

### Costs and trade-offs

- Every member stores the group's advertisement metadata as a replica, so collection
  storage grows with the number of distinct authors and objects, not only with a
  node's own advertisements.
- An object in several groups requires advertisements in several Areas.
- Full first-contact reconciliation may initially transmit every current
  advertisement in a group.
- An object version change rewrites one advertisement per group the object belongs
  to, and each group Area retains a tombstone path after the object leaves.
- Read-time joins over membership and version state are rejected for
  reconciliation, so server read CPU does not grow with the number of receivers.
- A SQLite Willow store requires correct prefix pruning, authorization, payload,
  range-query, and transaction behavior.
- Cursor acceleration relies on disciplined timestamp use: one timestamp per
  publication, allocated inside that publication's transaction. A sidecar receipt
  revision is deferred to a multi-writer store.
- Keyhive synchronization remains necessary for authority, group discovery, and
  direct document grants.
- A communal namespace makes read capabilities non-exclusive, so read authorization
  stays with Keyhive at serve time (§3).
- Some current `big_sync_core` machinery may not fit the Willow-native design and
  may be retired rather than preserved.

## Performance expectations

The common repeated-sync path should inspect current group entries whose latest
publication is newer than the receiver cursor, not historical mutations and not
the arbitrary union of every object accessible to the principal.

First-contact enumeration is paged and transfers advertisements rather than
object content. Object synchronization is proportional to distinct divergent
objects after multi-group deduplication.

Mirroring an accepted object version rewrites one advertisement per group the
object belongs to, written by one statement inside the publication's transaction.
Because each group Area is self-contained, neither replay nor first contact joins
membership against object-version state. The per-object metadata cost on the wire
is one run per distinct object version when the source coalesces and one entry per
group when it does not.

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
3. Define the advertisement schema — one entry per `(node, group)` Area at
   `/<document-id>`, whose presence is membership and whose payload commitment is
   the object version — together with the node namespace, per-group subspace
   derivation, and group Area mapping.
4. Mirror locally accepted Keyhive group membership and object-version changes into
   the group's namespace, under the node's own subspace, while retaining existing
   BigSync.
5. Implement Keyhive-authorized, paged group Area enumeration as the first-contact
   reconciliation path.
6. Feed divergent discoveries into the existing BigRepo object-sync API and
   verify admission, materialization, and multi-group deduplication.
7. Add the bundled group-Area cursor API of §8 — per-Area durable timestamp cursors,
   page boundaries snapped to advertisement runs, optional run coalescing — and a
   bounded long poll on the publication boundary as the lossy live notification.
8. Compare collection convergence, revocation, restart, and stress behavior with
   the existing BigSync implementation.
9. Move BigRepo group collection synchronization to Willow and remove obsolete
   part-shaped APIs and storage.
10. Implement or adopt full Willow RBSR for first-contact group reconciliation;
    retain full Area enumeration only as an interim compatibility path.
11. Replace the interim live mechanism if Willow gains a better fitting
    live-following protocol.

## Deferred decisions

- The canonical object-version encoding whose digest becomes an advertisement's
  payload commitment.
- How an advertisement whose object version is not yet known is encoded, given that
  the same entry is rewritten in place when the version arrives.
- The exact derivation of the node namespace secret and per-group subspace keys.
- Whether a multi-writer store or an out-of-order commit needs a read watermark or
  a receipt revision. Within a single-writer store the continuation token for a
  paged cursor read is an `EntryKey` plus the boundary timestamp (§8).
- Sidecar cursor schema, if a multi-writer store ever requires one.
- Whether an auxiliary read-time join over membership and object-version state is
  worth maintaining for point queries; it can never participate in reconciliation.
- Cursor page and continuation-token wire format, including the encoding of a
  coalesced advertisement run.
- Live-notification transport and granularity.
- Tombstone retention under Willow pruning semantics.
- SQLite range indexes and future RBSR summaries.
- How much of each replicated collection a node retains, and tombstone retention
  within a replica.
- Which subspaces a replica accepts: only those derivably bound to a current member,
  or any subspace whose entry verifies, gated by Keyhive at serve time (§3).
- Whether whole-group read access is expressed as Meadowcap delegations from every
  subspace owner or stays with Keyhive serve-time checks.
- Whether a group's collection uses per-author claims (communal namespace) or one
  entry per document (owned namespace, one shared subspace, content-derived
  timestamps) (§3).
- Meadowcap projection from Keyhive group authority.
- Willow drop, WTP, and Confidential Sync use, which §11 removes as targets.
- The disposition of the existing global part.
- Which `big_sync_core` abstractions remain reusable after the Willow-native
  storage and protocol APIs are defined.
