# ADR 017: Clay collections — a chained replica/ledger/view substrate

**Status:** Accepted. Implemented in `clay_collection`, consumed by `clay_host` (contribution
collections), `clay_docs` (document facets), and `clay_mdk::view::use_collection`.
**Amends:** ADR 015 §4 (optimistic-write visibility, contribution collections) and §6
(contribution collections are built on this substrate rather than a host-private store).
**Context documents:** the session design log in `docs/scratch/dayframe-micros.md` (Turn 31,
slices 1–5, and the guided review tour) and the original plugin-theory transcript cited by
ADR 015.

## Context

Collections are how micros talk to each other. A provider declares one, others contribute or
read, and the provider renders from its own schema; DOM structure is never an interface
(ADR 015 settled rules). Until this change Clay had three unrelated shapes for that:

- `HostedCollection` delivered a whole `BTreeMap<u64, T>` snapshot per change through a
  `watch` channel — the performance and security footgun the plugin-theory discussion named.
- `clay_data` carried a private optimistic map whose acceptance simply promoted the predicted
  value into the base. That only works when accepting a write installs exactly that value.
- Document data, contribution registries, and guest-provided collections each needed the same
  reactive read path, but nothing let a guest consume another guest's collection.

The data plane must serve node → host → guest, guest → host → guest, and later node → collection
(TanStack-style query sources), with documents and facets as only one bespoke layer on top.
TanStack DB (synced base plus segregated optimistic transactions, live queries over the visible
set) and Zero (authoritative base, replayable mutators, processed-mutation progress riding with
row patches) were studied. We take TanStack's segregation and visible-set queries, and Zero's
lesson that reconciliation evidence must ride in the same message as the rows; we do not take
replayable mutators.

## Decision

### 1. One hop, chained

Every participant runs the same component, a **hop**:

```text
             ┌──────────────────── hop ────────────────────┐
query ──────►│ local query over Visible = Ledger ⊕ Replica │──► rows (immediately)
             │   Replica: key → (Version, row), tombstones │
             │   Ledger:  OperationId → whole-row overlay  │
demand ─────►│──────────────► Upstream ───────────────────►│──► next hop
batches ◄────│◄────────────── Downstream ◄─────────────────│◄── next hop
writes ─────►│──────────────► Upstream ───────────────────►│──► next hop
             └─────────────────────────────────────────────┘
```

- **Shared:** `Replica` (latest row per key, version-guarded, deletion tombstones), `Ledger`
  (segregated overlays), `View` (both, plus local query, demand bookkeeping, notification).
- **Bespoke per source:** the `Upstream` implementation (what feeds a hop and how it narrows
  demand) and the row codec. The facet frontier, a host-held store, a computed collection, and a
  future `queryFn` source are all `Upstream` implementations.
- A writable `View` implements `Upstream`, so **a view is a legal source**. host → guest and
  guest → guest are the same relay; no second mechanism exists for guest-provided collections.
- Mounting a query answers from the local visible set first and only then mounts its demand
  upstream; each hop answers from its own replica and forwards what it cannot cover. This gives
  "preload, then filter locally" reads with no loading flash when data is already near.

### 2. The waist

The protocol between hops never names "collection"; the near end cannot tell whether the far end
is another hop, the facet frontier, or a query source.

```rust
// consumer → upstream (clay_collection::Upstream)
fn attach(&self, downstream: &Downstream<K, V>); // before any demand: no read-then-subscribe gap
fn demand(&self, token: DemandToken, id: DemandId);
fn release(&self, id: DemandId);
fn write(&self, id: OperationId, key: K, value: Option<V>, writer: ScopeId);

// upstream → consumer: one Batch is one visible transition
struct Batch<K, V> {
    incarnation: u64,
    base: Vec<BaseDelta<K, V>>,               // Upsert { key, version, value } | Remove { key, version }
    overlays: Vec<OverlayDelta<K, V>>,        // Put { id, key, value: Option<V>, writer } | Clear { id }
    settled: Vec<(OperationId, WriteOutcome)>,// Durable { version } | Rejected | Unknown
    covered: Vec<DemandId>,
}
```

- **One batch is one visible transition.** Base rows, overlays, settlements, and coverage land
  in one `apply`; forwarding them separately would render a frame where a pending row reverts
  to base and re-appears, or loses its settlement evidence.
- **Base and overlays never merge on the wire.** A consumer can always tell an accepted value
  from someone's pending intent.
- Payloads crossing an address space are self-describing data (JSON today, CBOR acceptable).
  A host change must not force every micro to rebuild (ADR 015 R9). `DemandToken` is
  `serde_json::Value` for this reason.

### 3. Versions and epochs

- `Version { incarnation, seq }`. `seq` is chosen by whoever is authoritative for the row;
  `incarnation` is that authority's epoch. Versions compare only within one incarnation of one
  authority. A batch from a higher incarnation resets base, overlays, and coverage in a single
  event; a lower one is ignored.
- Causal positions (Automerge heads) are never orderings and never enter `Version`.
- A rebuilt node frontier or a restarted provider is an epoch bump that invalidates the whole
  collection. We do not try to keep or reconcile the old collection.
- Node-backed sources stamp `seq` from the per-node, per-boot frontier revision
  (`FrontierRevision`). How the drawer's write path and the facet-set index agree on an order
  for an accepted facet write is a node-side problem, owned outside the UI; this ADR assumes the
  write path will eventually return a version comparable to that frontier.

### 4. Optimistic writes are segregated whole rows

- **There is no predictor.** A mutation produces the whole row it wants visible (`Some(row)`,
  or `None` for an optimistic delete). It is held in the `Ledger`, never written into the base.
- `OperationId` is minted by the view the write originated in and carried unchanged to every
  hop, so all hops retire the same overlay together and an echo adopts the existing overlay
  instead of applying it twice.
- An overlay is removed when:
  - the write is `Rejected`;
  - the write is `Unknown` (transport failure): treated as a rejection for visibility, because
    replication restores the value if the write landed. Transport uncertainty is never reported
    as rejection to the caller (`clay_docs` keeps `RolledBack(Transport { outcome_unknown })`);
  - the write is `Durable { version }` **and** the base reaches that version
    (`OverlayWait::Base`). The overlay is parked, not dropped, on acceptance so the committed row
    arrives by replication instead of flashing back to its pre-write value. This works in every
    arrival order (receipt first, replication first, or both in one batch);
  - the writer's scope disposes (`View::dispose_scope`).
- Closing a view, a component, or a micro never cancels an already submitted write.
- Overlay visibility is a property of the collection, `OverlayPolicy`:
  - `Local` keeps the prediction in the originating view only; siblings see the value when the
    authority echoes it.
  - `Replicated` records and re-emits the overlay at every hop, so two views of one row on one
    screen cannot disagree. `clay_docs` uses `Replicated`.
  Replicated overlays obey the same grants as base rows: a context that may not read a row never
  receives a pending write to it.
- `clay_docs` keeps its caller-facing receipts (`LocallyApplied`, `Accepted`, `RolledBack`). This
  amends ADR 015 §4: acceptance resolves the receipt immediately, but the overlay stays visible
  until base reaches the accepted version. It does not "commit the intent locally".

### 5. Demand and coverage

- **Demand is a narrowing token, opaque to the substrate.** `DemandToken` is transported, never
  interpreted, by the shared hop; the narrow end of the chain (a facet selector builder, a host
  store, a computed source) parses it. A guest can therefore only ask for demands its provider's
  contract defines.
- **Demand is caller-authored.** A `Query`'s filter and order are closures, so no token can be
  derived from one. `Query::demand(token)` attaches the caller's token; `View::query` evaluates
  locally first, then mounts one lease per distinct token, view, and authority incarnation. A new
  incarnation invalidates the old evidence and the next read renews the lease.
- **Demand is coarse; queries are exact.** The common shape is "preload N things, filter
  locally": a document editor demands every facet of one document, a list demands some facets of
  N documents. Overfetching is acceptable.
- **Releasing demand never evicts rows.** Demand controls the upstream subscription; retention is
  the replica's own policy. Otherwise an unmount yanks data another view is about to read.
- **Coverage is part of the waist.** `covered` resolves a demand at the hop that issued it, and
  `View::query_coverage` reports `Unknown` (not mounted), `Partial` (awaiting evidence), or
  `Complete`. "No row" is distinguishable from "not loaded", so a consumer never flashes a 404.
  Coverage is routed per subscription, including two subscriptions of one cloned reader;
  sibling, unsolicited, and released coverage is ignored. Every bespoke source must define what
  "complete" means for its demands: the keyed frontier's `ReplayComplete` is **not** coverage,
  because overwritten rows can move into its live phase.

### 6. Roles and capabilities

- **Authority is an explicit role, never inferred from a missing upstream.** `View::authority`,
  `View::consumer`, and `View::reader` construct the three roles. A consumer that was never wired
  must not answer its own demand with nothing or treat its own writes as terminal.
- **Write capability is type-state in Rust.** `View<K, V, W = Writable>` with `Writable` and
  `ReadOnly` markers; `write`, `apply`, and `dispose_scope` exist only on writable views. A
  host-collection reader cannot fabricate a row because the method does not exist.
- **At the WIT boundary the same fact is a capability.** WIT cannot express type-state, so write
  access is a resource handle a guest either holds or does not; WIT resources also give RAII for
  demand leases. Two shapes of one fact at two boundaries is deliberate, not a shim.
- **Reading and ingestion are separate capabilities.** `View::reader` returns the read-only view
  and a `Downstream` ingestion handle; only the adapter keeps the latter. `clay_host`'s
  `CollectionView` owns the view, its ingestion handle, and its subscription as one value: it is
  the thread boundary (the host publishes under its mutex, a view is single-threaded) and a view
  held without its pump would silently stop advancing.

### 7. Lifecycle and ordering

- `close` is terminal: it clears rows, overlays, coverage, and query leases, releases upstream
  demand, closes relays, notifies listeners, and later batches are ignored. Dropping the last
  binding closes retained view clones rather than leaving a frozen cache that looks live.
- Only the authority bumps an epoch. A consumer's close removes its own streams; provider
  cleanup resets the collection for everyone.
- Owner cleanup is an authority-side batch of removes (`Replica::plan_remove_owner`), not an
  overlay. Consumers receive ordinary removes and need no cleanup logic of their own.
- Notifications are FIFO per hop. Applying a batch is synchronous, but delivering a reentrant
  batch's notification waits until the outer one has reached its captured listeners and relays,
  so a rejection issued from a listener cannot overtake the overlay it rejects.
- Tombstones persist until a newer row supersedes them or the epoch resets, so a stale upsert
  cannot resurrect a deleted row and a settle-only batch can still retire an overlay after a
  newer removal.

### 8. Contribution collections and overrides

- Contribution collections (Settings sections, toolbar entries, root mounts) and data
  collections share this substrate and its waist. They do **not** share ownership or persistence
  semantics (ADR 015 §4).
- **Contribution collections are host-held.** The provider declares the collection; the host
  stores it (`HostedCollection` over `Replica<u64, T>`), enforces owner disposal and ordering,
  and fans out to consumers without waking the provider. A provider's write policy is a
  validation path into the provider (today `Contributable::contribute`, which rejects malformed
  payloads with `AccessError::Denied`).
- Computed guest-provided collections (a search micro's results) may implement `Upstream`
  themselves. Consumers cannot tell the difference.
- **Ownership is identity.** Every contributed row carries its owner scope, and disposing the
  owner removes its rows in one batch. The decided key shape is owner-scoped,
  `(owner, local_key)`, with overrides keyed `(overrider, target_key)`. Today's host mints a
  `u64` entry id and keeps the owner as a replica tag; the owner-scoped form lands with the
  collection WIT transport, where cross-guest id namespacing is required anyway.
- **Overrides are not optimistic writes.** They are the seam that keeps micro load and unload
  spatiotemporally clean: an overriding micro owns its override rows, disabling it reveals the
  current base entry, and nothing needs a reload. Overrides live in a sibling collection the
  provider reads and merges in its own code; the substrate has no third "override" layer.
  Ordinary node-backed collections pay nothing for a plugin-only concept.

### 9. Documents are one bespoke layering

- **A facet is a row**, keyed `FacetRoute { document, branch, tag, key_id }`. A document is a
  query (group by document), not a storage unit. This matches the node's keyed frontier, keeps
  per-facet retention and selector pushdown, and sidesteps whole-document rewrites; typed MDK
  accessors provide document-shaped ergonomics on top.
- `clay_docs` backs `DocumentService` with `Replica<FacetRoute, Value>` and a `Replicated`
  `Ledger`. Facet states are `FacetValue::{Loading, Absent, Present}` with separate `Freshness`;
  a refresh never hides an existing value.
- An Automerge-native node API (row bytes plus change streams) would be another `Upstream`; it
  costs the waist nothing. Deferred: row-per-facet already covers concurrent edits to different
  facets, and it would put Automerge in every guest that touches documents.
- "Guests learn ids/keys and read values synchronously from the host" is a deferred memory
  optimization for documents, not the initial contract.

## Consequences

- One mechanism serves host, guest, and node-backed collections. A new source is an `Upstream`
  implementation plus a codec; the shared hop never changes for it.
- Rejection, concurrent remote edits, and receipt/replication reordering cannot restore a stale
  snapshot over a newer value, because the visible row is always derived from the current base
  plus the remaining overlays.
- Read-only access is unrepresentable as a write in Rust and absent as a handle in WIT.
- Every replica keeps its own copy of rows; deduplicating copies across guests is a later
  host-side optimization.
- Each bespoke source owes a definition of "complete" coverage; a source that cannot give one
  can only report `Partial`.

## Not yet implemented

These are decided boundaries without code. Nothing above claims them.

- **Collection WIT transport.** Only in-process micros reach collections today; Wasm guests see
  documents and their own surfaces, not collections.
  `OperationId` and `DemandId` are process-wide counters; across address spaces the host must
  namespace them per guest, together with owner-scoped contribution keys (§8).
- **Production node adapter.** A narrower from facet demand to `FacetSetRevisionStore` /
  `DocDeltaRevisionStore` selectors, its coverage definition, and versions for direct
  doc-service reads taken from the frontier row so a value never carries a newer label than its
  data.
- **Write-path versions.** The drawer write path returning the frontier version it lands at
  (§3), required for exact `Durable` retirement of node-backed writes.
- **Host-side query evaluation for Wasm guests.** Guests query their own partial replica. A host
  that evaluates queries and relays already-filtered rows is a later performance specialization
  of the same traits, not a second API.
- **Query planning.** Queries scan the visible set; there is no index or serialized query IR.
- **Tombstone compaction** needs a safe watermark; tombstones are kept until superseded or reset.
- **Backpressure.** Host delivery uses unbounded channels. A lag/reset policy must be designed
  rather than silently dropping batches.
- **Automatic demand in `use_collection`.** The hook observes coverage but does not lease demand
  on mount; that arrives with the first UI that renders a pending state.
- **Derived collections as first-class owned values.** Every guest collection is already derived
  from its upstream; naming and lifetime-owning a derived query result (owned by the declaring
  micro's activation) is agreed in direction, not built.
