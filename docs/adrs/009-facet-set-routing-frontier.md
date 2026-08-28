# ADR 009: Facet-Set Routing Frontier

**Status:** Proposed.

## Context

Many durable consumers are not interested in every document. Triage, indexes,
plug discovery, and application repositories usually care about documents
containing one or more facet tags. Feeding all accepted document changes to each
consumer and asking it to filter after loading the document repeats expensive
work and creates unnecessary cursor and per-object state.

The existing facet-set index can answer current membership queries, but its
listener notifications are ephemeral. It cannot independently replay a tag's
changes, reconstruct a late-added consumer, or atomically describe a document
leaving a tag. It is therefore useful as a lookup table but not as the durable
routing layer required by ADR 008.

The new layer must preserve two properties that can be easy to lose in a
conventional append-only event log:

- consumers need the latest routed state for each
  `(facet tag, logical document)` key, not every obsolete intermediate payload
  forever;
- removal must remain observable long enough for every interested consumer to
  remove its derived state.

KeyedFrontier already provides collapsed per-key state, atomic multi-key
revisions, replay boundaries, and notification. The facet-set index will use
those primitives rather than inventing another replay and handoff
implementation.

## Terminology

- **Facet tag:** the schema/type tag shared by one or more facet instances,
  distinct from a full `FacetKey`.
- **Route key:** `(FacetTag, DocumentId)`.
- **Routing value:** the latest accepted default-branch version and matching
  facet keys for one route key.
- **Routing tombstone:** a durable frontier value saying that the document no
  longer matches the tag.
- **Source cursor:** the durable contiguous `DocDeltaRevisionStore` cursor
  through which the routing projection has been updated.
- **Default branch:** the logical document's main branch, whose `BranchId`
  equals `DocumentId` under ADR 007.

## Decision

### 1. The facet-set index becomes a routing projection

The facet-set subsystem retains its efficient current-state membership queries
and adds a KeyedFrontier-backed collapsed routing projection. These are two
views of the same accepted index state and must advance atomically.

The facet-set builder is a specialized durable projector, not a new walker state
model. It consumes a main-branch-filtered `DocDeltaRevisionStore` through an
active `DeltaWalker`, and publishes a `RepoDeltaRevisionStore<FacetSetEvent>`.
Its projection state records the accepted source cursor and sparse document
state; downstream consumers own their own `DeltaWalker` state and progress. The
embedder drives each walker's serial or concurrent state machine and commits
facet-set rows, proposed document-state changes, and contiguous progress
together.

The output routing frontier is exposed through the inert typed
`RepoDeltaRevisionStore<FacetSetEvent>`. `DeltaWalker` is reserved for the
active store-plus-consumer-state machine driven by an embedder; no separate
facet-set walker or acknowledgment model is introduced.

Branch-specific consumers use `DocDeltaRevisionStore` or
`FacetDeltaRevisionStore` directly; routing every branch through the default
facet-set index would blur logical document membership and multiply common index
work.

This default is especially useful because broad consumers ordinarily model the
logical document's published/current content, while editors and branch review
tools already know the exact physical `BranchId` they want.

### 2. Route keys are tag and logical document

The frontier key is:

```text
(FacetTag, DocumentId)
```

The live value contains at least:

```text
FacetRoute {
    document_id: DocumentId,
    branch_id: BranchId,           // the default branch
    document_version: Version,
    matching_keys: Set<FacetKey>,
}
```

Including `matching_keys` matters because one document can contain several facet
instances with the same tag. A route answers both “this document currently
matches” and “which concrete facets caused the match.” The version is the
explicit accepted version a downstream walker must read; it must not infer
content from a mutable current store.

`BranchId` is retained even though it equals `DocumentId` for the default
branch. This makes the read target explicit and prevents callers from depending
on the equality as an untyped storage shortcut.

### 3. Every source revision updates all current tags

For each accepted default-branch document revision, the projection performs one
transaction:

1. Load the distinct facet tags and matching keys at the explicit new document
   version.
2. Load the previously indexed tag set for the document.
3. Write a live routing value for every currently present tag.
4. Write a tombstone for every previously present tag that is now absent.
5. Update the current membership/query rows.
6. Persist the document's sparse indexed version and the source cursor.
7. Commit the entire multi-key frontier revision.

A live value is written for every currently present tag even when membership
itself did not change. The document's content or matching facet values may have
changed, and downstream consumers must be told to recompute from the new
version.

One source document revision is one atomic routing revision. Consumers must
never observe half of a tag transition, such as the new tag without the old
tag's tombstone.

### 4. Deletion and removal use tombstones

When a document is deleted or leaves the accepted default-branch scope, the
projection writes a tombstone for every tag previously present on that document
in the same revision.

A tombstone contains enough information to route removal without reading a
document that may no longer be available:

```text
FacetRouteRemoved {
    document_id: DocumentId,
    prior_branch_id: BranchId,
    prior_document_version: Version,
}
```

The keyed frontier may later prune superseded physical rows according to its
consumer and retention contract, but the logical design is not an unbounded
append-only log. There is one latest collapsed state per route key. A removed
route remains a tombstone until it is safe for the frontier implementation to
discard it.

### 5. Queries and walkers read the same committed projection

The subsystem exposes:

- current membership queries by tag and document;
- current matching `FacetKey` values;
- durable tag-filtered delta walkers;
- low-latency lossy watches for UI refresh where useful.

The current query rows and frontier revision become visible together. A client
must not query membership at revision R while the durable walker still reports
the projection at R-1, or receive a route event before the membership row is
readable.

After the transaction commits, the subsystem advances a materialization
watermark and wakes walkers/watches. As in ADR 008, notification is a wakeup;
durable state and cursors determine what must be read.

### 6. Tag filters are independent consumer scopes

A durable consumer opens its own walker with a stable ID and an explicit set of
facet tags. Scope changes follow ADR 008's dynamic route rules:

- adding a tag starts that route source from cursor zero and replays its current
  matching set;
- removing a tag emits logical removals for documents whose final route to that
  consumer disappears;
- overlapping tags form a union and do not duplicate a document solely because
  it matches several requested tags;
- re-adding a tag after removal bootstraps it again.

The facet-set projection stores source routes; the consumer's walker stores its
own filter scope and progress. The projection does not maintain one outbox table
per consumer.

Consumers that require a separate event for each matching tag can request
tag-keyed events. A logical-document consumer can request the union view. The
underlying frontier retains enough route identity to implement either mapping
without reopening irrelevant documents.

### 7. Bootstrap and crash recovery use the same walker path

An empty or rebuilt facet-set projection opens `DocDeltaRevisionStore` from
empty state. For each current document, the ordinary `None -> current` mapping
produces its initial tag routes. There is no separate full-scan initialization
algorithm.

The source cursor advances only in the same transaction that commits membership
rows, route values/tombstones, and sparse per-document version state. A crash
before commit causes the document revision to be replayed. Reapplying it is
idempotent because route values are keyed and carry the explicit source version.

The projection reports replay completion only after processing through the
boundary captured when the walker opened. New source changes can continue
through the identical processing loop.

### 8. The projection is local and authority-filtered

Facet-set membership describes the documents and versions accepted by this
checkout. It is not a global statement that a document contains a facet or that
every peer can access it.

If authority changes remove a document from the accepted source scope, the
facet-set projection emits tombstones exactly as it does for deletion. If access
returns later, normal bootstrap through the source route recreates the live
entries.

The projection must not leak private facet tags or document existence into
consumers lacking the corresponding source authority. Authorization is applied
before a document delta enters the routing projection exposed to that consumer
boundary.

### 9. Broad consumers run downstream of the routing frontier

Triage, search indexes, plug repositories, dispatch indexes, and similar broad
consumers subscribe to relevant facet tags instead of every repository document.
On a routed value they load the specified explicit document version, compute
their derived state, and commit their projection plus their own walker consumer
progress where possible.

This does not make the facet-set projection a semantic event bus. It supplies
durable dirty routes. Each downstream consumer owns the mapping from explicit
document versions to its typed domain events or index rows.

Single-document `FacetStore` instances remain direct consumers of
`FacetDeltaRevisionStore` through a `DeltaWalker`; routing them through a
corpus-wide tag index would add avoidable latency and state.

## Storage shape

The first SQLite implementation may combine the current membership table and
frontier state or keep them in closely related tables. The schema must support:

- lookup of current tags and matching facet keys by document;
- lookup of current documents by tag;
- a monotonically ordered atomic frontier revision;
- collapsed live/tombstone values by `(FacetTag, DocumentId)`;
- the source cursor and sparse prior version used for idempotent projection;
- transactionally consistent queries and walker reads.

Multi-line SQL follows the repository's commas-first style. Schema shape is
otherwise an implementation detail and may evolve without changing the logical
contract.

## Consequences

### Positive

- Broad consumers avoid loading documents irrelevant to their facet filters.
- Late-added consumers receive correct replay from independent progress.
- Content changes route even when tag membership itself remains unchanged.
- Removing a facet, deleting a document, and losing authority all produce
  durable removals.
- Current queries and durable changes cannot disagree across a committed
  revision.
- Replay, handoff, collapse, and notification reuse KeyedFrontier contract
  tests.

### Costs and trade-offs

- A changed document writes one routing row for every distinct current tag plus
  every removed tag.
- Documents with unusually many tags cause proportional write amplification.
- Tombstones require a safe retention/pruning policy in the frontier
  implementation.
- The default-branch focus means branch-aware consumers use a different, more
  exact walker.
- Authority-filtered projections may differ between peers, as intended by the
  idiocentric model.

## Performance expectations

The expected common-case work for one changed document is proportional to the
number of distinct facet tags on that document, not the number of registered
consumers or the size of the repository. Downstream consumers load only
documents admitted by their filters.

Implementations must not recover correctness by scanning the entire membership
table into a memory set for each revision, querying all object-to-part
memberships for every event, or retaining an unbounded append history after all
relevant cursors have advanced.

## Migration

1. Add KeyedFrontier-backed route state alongside the existing facet-set query
   tables.
2. Feed it from `DocDeltaRevisionStore` through a `DeltaWalker` and establish
   atomic projection/progress commits.
3. Add contract tests for unchanged membership with changed content, multi-tag
   atomic revisions, tag removal, document deletion, authority removal, replay
   boundaries, and crash redelivery.
4. Move one broad consumer to a tag-filtered walker and compare its projection
   with the existing path.
5. Move the remaining facet-filtered consumers and delete their broad document
   subscriptions.
6. Remove the old ephemeral facet-set listener as a durability mechanism; retain
   a lossy watch only where UI latency benefits from it.

## Deferred decisions

- The precise tombstone pruning condition and retention window.
- Whether route values store full head sets, a logical version identifier, or
  both.
- The exact API for tag-union versus per-tag event mapping.
- Remote/cloud query delegation for corpora too large to index in one checkout.
- Schema migration details for the existing facet-set SQLite tables.
