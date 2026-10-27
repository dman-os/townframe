# ADR 009: Datum-Set Routing Frontier

**Status:** Implemented — `DatumSetStore` (`src/big_cabinet/index/datum_set.rs`)
is the AFW-fed projection described here, and doc-scoped observation
(`ObserveDatums`, §6) is implemented in DocsRepo.

## Context

Many durable consumers are not interested in every document. Triage, indexes,
plug discovery, and application repositories usually care about documents
containing one or more datum tags. Feeding all document changes to each
consumer and asking it to filter after loading the document repeats expensive
work and creates unnecessary cursor and per-object state.

An index that only answers current membership queries, with ephemeral listener
notifications on the side, is a lookup table and not a durable routing layer:
it cannot replay a tag's changes, reconstruct a late-added consumer, or
atomically describe a document leaving a tag, all of which ADR 008 requires.

Consumers need the latest routed state for each `(doc, datum)`, not every
obsolete intermediate payload forever, and removal must remain observable long
enough for every interested consumer to remove its derived state. KeyedFrontier
already provides collapsed per-key state, atomic multi-key revisions, replay
boundaries, and notification, so the datum-set index uses those primitives
rather than inventing another replay and handoff implementation.

Most document writes are remote: they arrive through BigRepo sync, not through
DocsRepo. Any projection fed from DocsRepo writes would miss them. The
projection is therefore fed from the Automerge frontier walker (AFW), which
observes every locally applied change regardless of origin.

## Terminology

- **Datum tag:** the schema/type tag shared by one or more datum instances,
  distinct from a full `DatumKey`.
- **Route:** `(DocId, DatumKey)` — one datum slot in one document.
- **Membership:** `Present { doc_heads, datum_heads, clock }` or
  `Removed { doc_heads, datum_heads, clock }` (a tombstone retaining the last
  signature-introduction heads).
- **Clock:** the Lamport clock of `datum_heads`: the greatest Automerge
  `max_op` among the introducing changes (see ADR 020 §2 `DatumVersion`).
  A version written on top of another has a strictly greater clock; concurrent
  versions are incomparable and equal clock with different heads means the
  versions merged or diverged.

## Decision

### 1. The datum-set index is an ungated routing projection

`DatumSetStore` projects raw document state into current datum membership. It
performs no Gate verification, loads no typed dmeta, and hydrates no datum
values: live rows carry document heads and *unverified*
signature-introduction heads. Consumers that read values do so through the
DocsRepo gate at the reported document heads; consumers that act without
reading values must provide their own gating.

The projection is deliberately **not authority-filtered**. It is a
trusted-subsystem index: it describes every document present in this checkout,
including documents a given caller may not read. Authorization is applied at
the observation layer (§6), never by withholding rows from the index, because
the index's consumers (sync, triage, derived indexes) each apply their own
authority model downstream.

The first derived index on this projection is implemented: `datum_ref`
subscribes to the tags whose schema declares at least one `reference` storage
position (ADR 021), gate-verifies each live route's value at the reported
document heads, and records one edge per declared reference — origin route and
JSON pointer, target route with `self` resolved against the origin document,
and the sibling `refHeads` pin. Edges are replaced per route in the same
transaction that advances the per-tag cursor, so replay is idempotent; only a
gate-accepted value publishes edges, so nothing a forged or pending datum
states is indexed.

There is no branch dimension: every branch is an ordinary doc with its own
`DocId`, and no datum (including blobs) is special-cased.

### 2. Routes are keyed by doc and datum; the frontier is namespaced by tag

Current state lives in `big_cabinet_datum_set_entries`, one row per
`(doc_id, datum_key)` with `doc_heads`, `datum_heads`, and `clock`, plus
indexes by `(datum_tag, doc_id)` and `(datum_key, doc_id)`. The keyed frontier
stream is namespaced by datum tag, so a consumer subscribes to the tags it
cares about and receives collapsed latest-state entries per route.

Queries (`DatumSetQuery`) are:

- `Doc(DocId)` — every route in one document;
- `Tag(DatumTag)` — every route carrying a tag;
- `Route(DatumRoute)` — one exact route;
- `Key(DatumKey)` — one datum key across all documents.

### 3. The projection is fed by the Automerge frontier walker

`DatumSetStore::run` drives a `ConcurrentDeltaWalker` over
`AutomergeFrontierRevisionStore`, keyed by `DocId`: a newer transition
supersedes work for the same document while independent documents proceed
concurrently. Unavailable documents park individually; local lifecycle
notifications serve only as wakeups, never as projection input.

Route effects and the sparse per-document heads/epoch/cursor commit in one
SQLite transaction before the walker acknowledges the cursor. A crash before
acknowledgement replays idempotently. The embedder owns the run future and its
cancellation and must stop it before BigRepo.

Initial snapshots, envelope replacements, and materialization-epoch changes
scan the raw datum and dmeta maps. Incremental projection inspects only the
changes added between the settled and requested heads and resolves their
object-parent paths: visible `diff_obj` patches alone cannot identify hidden
conflict operations, so the raw operation footprint is necessary to retain
unsigned and hidden content touches even when presence and signature heads are
unchanged. Only the dmeta `datums` subtree affects membership or signature
heads; other dmeta fields (aliases, actors, timestamps) touch no slot.
Shared provenance changes conservatively invalidate affected routing state.

### 4. Removal uses tombstones

When a datum is removed, the row becomes a `Removed` membership retaining the
document heads and the last signature-introduction heads, so consumers can
remove derived state and a later resurrection is distinguishable from a first
write. The keyed frontier may prune superseded physical rows according to its
retention contract; logically there is one latest collapsed state per route.

### 5. Queries and the frontier read the same committed projection

Current-state queries and frontier entries become visible in the same commit.
A reader must never query membership at revision R while the frontier still
reports the projection at R-1, nor receive a route entry before its membership
row is readable. Keyed-frontier replay is latest-state catch-up, not an
append-only audit log: a late-added consumer opens at revision zero for its
tags and receives the current matching set, then follows new revisions.

As in ADR 008, a commit notification is only a wakeup; durable frontier state
and the reader's cursor decide what is read. There is no separate lossy watch:
a UI that wants refresh latency observes through `ObserveDatums` (§6) like any
other caller. **Deferred:** a lossy watch for UI refresh, added only where its
latency benefit over the durable stream is shown.

### 6. Observation and queries are DocsRepo operations

`DatumSetStore` is trusted-subsystem-only and is never exposed to callers
directly. Callers reach it through DocsRepo operations carrying the same
authority model as doc reads:

- `QueryDatums` and `QueryDatumRefs` answer current-state queries over the
  datum-set and `datum_ref` projections. With a `drawer_id`, membership comes
  from the drawer's verified roster (`readable_in_drawer`), never from
  unverified datum-set tag presence; without one, each candidate document is
  checked for read access.
- `ObserveDatums` subscribes to the frontier, resuming after a frontier
  revision, with one of three scopes:
  - `Doc { doc_id }` is authorized before the stream opens and holds a sync
    interest in the document while the caller can read it;
  - `Tag { tag }` opens only its tag's frontier namespace and checks each batch
    of entries as it arrives;
  - `Drawer { drawer_id, tag }` resolves the drawer's readable documents when
    the stream opens and keeps that set current across access changes; the
    optional tag narrows it to one namespace.

  The stream marks the end of its opening replay with `ReplayComplete`.

Entries for documents the observer has never been delivered are silently
omitted — naming one would disclose its existence. Losing access to a document
the stream has delivered (a `Doc` scope names its own document) emits one
`AccessLost` per loss, either when a capability or access-policy change is
re-checked against the delivered documents (policy changes are re-checked once
the opening replay completes) or on the next entry for that document, whichever
comes first. If access returns, the stream delivers the document's current
datums and observation resumes. This keeps the index ungated while making every
caller-visible read and stream authority-filtered.

### 7. Broad consumers run downstream of the routing frontier

Triage, search indexes, plug repositories, dispatch indexes, and similar broad
consumers subscribe to relevant datum tags instead of every repository
document. On a routed entry they load the document at the reported heads
through the DocsRepo gate, compute their derived state, and commit it with
their own consumer progress where possible. The datum-set projection is not a
semantic event bus; it supplies durable dirty routes, and each downstream
consumer owns the mapping from explicit document versions to its typed domain
events or index rows.

A consumer interested in one document does not need a tag filter, but it still
reads the corpus-wide frontier: a `Doc` scope (and a `Drawer` scope without a
tag) opens every tag namespace and filters entries by document, so its opening
replay reads every route committed after its resume revision. **Deferred:** a
per-document frontier selector, so a single-document observer's cost tracks its
document rather than the corpus.

### 8. Tag filters are independent consumer scopes

A durable consumer owns its own scope and progress: the set of tags it follows
and one cursor per tag. It opens the frontier with `open_tags`, one resume
revision per tag namespace, and commits its cursors with its derived state;
`datum_ref` keeps one cursor per reference-declaring tag and advances it in the
transaction that replaces the route's edges. The projection stores routes only
and keeps no outbox or acknowledgement state per consumer.

- Adding a tag to a consumer's scope starts that tag at cursor zero and replays
  its current routes; `datum_ref` does this on its next run after a schema
  newly declares a reference position.
- A route's key carries its tag, so a route lives in exactly one namespace and
  overlapping tag filters never deliver one route twice. A consumer that works
  per logical document collapses routes by `DocId` itself.
- **Deferred:** removing a tag from a consumer's scope has no projection-side
  support. The consumer must remove the derived state it built from that tag;
  the intended design emits logical removals for documents whose last route to
  the consumer disappears, and re-adding the tag bootstraps it again from
  cursor zero.

## Consequences

### Positive

- Broad consumers avoid loading documents irrelevant to their datum filters.
- Late-added consumers receive correct replay from independent progress.
- Content and signature changes route even when tag membership is unchanged.
- Removing a datum produces a durable tombstone with its prior version.
- Remote (synced) writes are projected exactly like local ones.
- Replay, handoff, collapse, and notification reuse KeyedFrontier.

### Costs and trade-offs

- A changed document writes one routing row per touched datum.
- Tombstones require a safe retention/pruning policy in the frontier.
- The ungated index must only be reachable from trusted subsystems; the
  authority boundary is the observation layer (§6).
- Single-document and untagged drawer observation filter the corpus-wide
  frontier by document (§7) until a per-document selector exists.

## Performance expectations

The expected common-case work for one changed document is proportional to the
number of datums it touches, not the number of registered consumers or the
size of the repository. Implementations must not recover correctness by
scanning the entire membership table into a memory set for each revision or
retaining an unbounded append history after all relevant cursors have
advanced.

## Deferred decisions

- The precise tombstone pruning condition and retention window.
- Remote/cloud query delegation for corpora too large to index in one checkout.
