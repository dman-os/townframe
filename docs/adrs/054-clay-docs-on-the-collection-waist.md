# ADR 054: Clay docs on the collection waist

**Status:** Accepted (2026-11-24). Proposed by the claytxt lane, reviewed and accepted by the operator the same day.
Nothing here is built yet. Byte budget numbers (§4) are to be measured.

**Amends:** ADR 015 §4 (`clay_docs` stops owning a document cache and transactions and becomes a view over the
collections below).

**Needs from the node:** one small change in `big_node` (§5, meta groups of known drawers). Everything else uses
existing node methods.

Related: [ADR 017](017-clay-collection-substrate.md) (the collection waist),
[ADR 019](019-cabinet-drawers-and-selective-mirroring.md) (drawers, known drawers, mirroring),
[ADR 023](023-cursor-consumption-is-long-poll.md) and [ADR 025](025-cabinet-datum-predicates-and-polling.md)
(`pollDatums`), [ADR 031](031-manifest-declared-collections-and-lazy-capabilities.md) (host-provided collections),
[ADR 049](049-one-permission-model-for-nodules-and-micros.md) (one permission model), ADR 052 (micro handles), the
[drawer wiki](../wiki/drawer.md) and [FDR 001](../fdrs/001-drawers-as-document-collections.md) (use cases).

## 1. Context

Micros have no document API. The review micro calls node methods through `Context::call` with an interim `docCap`
minted at install (`daytxt_native/interim.rs`). `clay_docs` (ADR 015 §4) has typed facets, a shared cache and
optimistic transactions, but only over an in-memory `MockDocuments`.

What the node offers today, checked against the code on 2026-11-24:

- **Polls read the index, not documents.** `pollDatums` answers from `big_cabinet_datum_set_entries`, which holds
  `(doc_id, datum_key, tag, doc_heads, datum_heads, clock)` and no values. A `datum` event carries a version
  (`ObservedDatum::Present { doc_heads, version }`), not the datum, so a poll never hydrates a document.
- **Values cost a hydration.** `readDatums { docId, at, datums? }` and `listDatums` load the document.
- **Only local documents are in the index.** A document gets rows when it materializes on this node. A roster entry
  for an absent document matches `InDrawer` but has no rows to deliver.
- **Membership is readable without the members.** A roster is `drawer.docEntry/<docId>` datums on the drawer's
  metadata document. `drawers.listDocs` reads it, absent members included.
- **Interest is per document.** `big_repo::DocInterest` leases one document into `DocSyncDemand`, which becomes a
  big_sync object subscription on every connected peer. An `IsDoc` poll holds one per poll, with a 30 s grace.
- **Mirrored drawers already stream.** `Drawers::mirrored` (`big_cabinet/drawer/mirror.rs`) turns each known drawer
  with `mirror: true` into its meta, docs and sensitive groups. On every connection and access delta, `big_node`'s
  peer loop (`peers.rs`, `apply`) subscribes the groups the peer is a member of (`groups_with_member`). A group's part
  carries every member document's content changes, not only membership, and big_sync keeps one subscription per part
  however many documents it holds. The cost is paid once, when a part is first subscribed and its ids are exchanged.
- **A known but unmirrored drawer streams nothing.** No part is subscribed, so its roster only changes when
  something else pulls its metadata document.

## 2. Four facts about a document

For a given caller and document, four facts are independent:

| Fact       | Source                                             | What it lets a micro do                     |
|------------|----------------------------------------------------|---------------------------------------------|
| listed     | a roster it can read (`listDocs`, `docEntry` rows) | know the document exists, and its id        |
| authorized | its cartridge's grants (ADR 049)                   | ask anything about it                       |
| local      | the datum-set index                                | query its datums and versions; read values  |
| live       | someone holds interest, or its drawer is mirrored  | expect peers' changes to arrive soon        |

A list view needs listed and local. A detail view needs local and live. The API never lets one fact silently cause
another:
- a query never fetches anything;
- reading a listed but absent document is an explicit open, which takes interest;
- mirroring a drawer is an explicit, authorized policy change, never a side effect of a query or an open.

## 3. Decision: four host-provided collections

All four are collections on the ADR 017 waist, with its leases, pages, cursors, overlays and resets. The collection
a micro sees is defined by the query it mounts, which may cross drawers. The drawer is the primitive a query is
anchored on, not the unit a micro subscribes to.

1. **`docs.datums`: a datum query.** What a list view mounts.
   - Selector: `{ predicate, datums }`, `pollDatums`' language unchanged (ADR 025).
   - Row key: `(docId, datumKey)`. Row value: the version, plus the value when the host holds it (§4).
   - Membership comes from the node's `Datum` and `Leave` events, so it only covers local documents.
2. **`docs.open`: one document, live.** What a detail view mounts; the `use_doc` and `use_datum` hooks sit on it.
   - Selector: `{ docId, datums? }`. Rows are the selected datums with values, and the document's availability is a
     field on the page.
   - The lease is the interest. Acquiring it takes a `DocInterest` in the host, and dropping it releases it after the
     node's grace.
   - It also opens a document that isn't local: the interest subscribes it on every connected peer, and whichever
     peer has it serves it. Before that, the host checks that the micro's grants reach the document through a drawer
     whose roster lists it.
3. **`docs.roster`: drawer membership.**
   - Selector: `{ drawerId, transitive }`. Row key: `docId`. Row value: the entry's access level and whether the
     member is local.
   - Backed by the drawer's `docEntry` rows, which are local whenever the drawer is known, so it lists members that
     are not here. A micro that wants an unmirrored drawer's members mounts this, then opens the ones it wants.
   - Whether a member is local is answered from the datum-set index (a member with rows is local), for the whole
     roster in one query. The per-document availability methods are not used for this.
4. **`docs.drawers`: drawer policy.**
   - Selector: every drawer the micro's grants reach, or one drawer id. Row value: the node's policy for it.
   - Today's policy states, from the known-drawers list: **unknown** (not listed), **known** (`knownEntry` without
     `mirror`) and **mirrored** (`mirror: true`). ADR 019's deferred per-node policy adds **selected** (a subset).
   - A micro reads it to know whether a drawer is mirrored before it offers the user something that depends on it.
     The state is also a field on `docs.roster`'s page.

**Writes.** A write is a domain command on the collection, which the host lowers to `writeDatums`. The `command` id
is the collection's `OperationId`. The pending row shows at once through `clay_collection`'s overlay, the
authoritative row retires it, and a rejection removes it and fails the command. Writing to a document that isn't
local fails; it never fetches.

**Mirroring.** A micro or nodule that needs a whole drawer present asks with `setDrawerPolicy { mirror: true }`,
gated like any drawer write by its cartridge's grants. Querying, listing and opening never change policy.

**`clay_docs`.** The typed `Facet<T>` stays as the authoring surface, as a view over `docs.open` and `docs.datums`.
`DocumentService`'s own cache, refresh and transaction machinery goes away.

**The source selects remotely.** `clay_collection::Projection` assumes its source publishes everything into the
host and filters each lease locally with `Source::matches`. For node documents the node evaluates the predicate. So
the docs sources select remotely: acquiring a selector starts its upstream poll, membership comes from `Datum` and
`Leave` events instead of `matches`, and releasing it stops the poll. Leases, pages, cursors, overlays and resets are
unchanged.

## 4. The host owns one cache; hydration is the budget

**One cache.** The Clay host holds one projection per distinct selector, and every lease reads from it, so two
micros mounting the same query share one upstream poll. Authority stays per lease:
- each lease is checked against its micro's handle on acquire and on every page, as `pollDatums` re-authorizes
  every poll;
- a lease only sees rows its handle reaches. Authorization never overfetches (ADR 017 §2).

Values are cached by version, `(docId, datumKey, datum_heads)`. A version names immutable content, so one cached value
serves every lease authorized to see that version, and a document is hydrated once however many micros show it.

**Hydration is the budget.** Reading values hydrates documents and is the expensive step; polls and rosters are index
reads.
- A `docs.datums` page carries versions. The host fetches values only for rows a lease asks to see, up to a byte
  budget per page. Rows past the budget arrive as versions and fill in on later pages.
- `docs.open` hydrates its document once in the host, however many leases hold it.
- The host bounds concurrent hydrations per micro and in total, so a list of ten thousand rows can't hydrate ten
  thousand documents at once.
- The numbers are measured against the fake node (§7) before they are fixed.

**Nodules.** Nodules call the node directly and get no host cache. The selectors, rows and commands are the same, so
a nodule's poll and a micro's lease describe the same rows, with or without the Clay host in between.

**Authority.** The host fetches each micro's handles as the operator (`micros.handles`, ADR 052 §3). Every node
call it makes with those handles is signed as the micro's own actor (`micros/<cartridge>/<micro>`), because the node
honours a body capability only under the actor it is bound to.

## 5. Interest is per document; known drawers stream their meta group

There is no drawer-level interest. A mirrored drawer already streams from every peer that holds it.
- `docs.open` holds document interest for the lease's life, as above.
- `docs.datums`, `docs.roster` and `docs.drawers` hold no interest. A list view stays current with whatever the
  node already syncs.
- **Node change:** `Drawers::mirrored`'s answer also carries the **meta group** of every *known* drawer, not only
  the mirrored ones. The meta group carries the descriptor and roster, never member content, so a known drawer's
  roster stays live and `docs.roster` stays current. Mirroring still adds the docs and sensitive groups, and with them
  member content. This is assumed cheap enough; measure it on a node that knows many drawers.
- A drawer known only by its id learns its meta group from its descriptor-bearing doc, and today's roster is inline
  on that doc. So a known drawer's descriptor doc is pulled too, as a mirrored drawer's already is. For a self-doc
  drawer that doc is its whole content. That is acceptable: it is one doc the node was granted, and remembering a
  drawer is an explicit act (`rememberDrawer`), not a side effect of a query.

## 6. Use cases

| Use case (FDR 001, drawer wiki)                 | What the micro mounts                                             |
|-------------------------------------------------|-------------------------------------------------------------------|
| Personal multi-device cabinet (mirrored)        | `docs.datums` with `InDrawer` and tags; `docs.open` on select     |
| One shared document                             | `docs.open` on the document                                       |
| Family photo album                              | `docs.datums` for the list; blobs through `blobs` (ADR 026)       |
| Workplace project, child drawers                | `docs.datums` with `InDrawer { transitive: true }`                |
| Chat                                            | `docs.open` on the thread document; interest gives liveness       |
| Large public wiki, plug registry (not mirrored) | `docs.roster` (live through the meta group); `docs.open` per read |
| Micro with its own remote index                 | its own call for ids; `docs.open` per id, fetched from any peer   |
| intxt review micro                              | `docs.datums` for its review queue; `docs.open` for one review    |

## 7. Order of work

1. A fake node in tests: datum index, roster, values, known drawers and availability over an in-memory store, behind
   the seam the real node uses. It counts hydrations, so the budgets are measurable.
2. `docs.datums` with the remote-selecting source and the shared host projection, leases end to end in
   `clay_collection` tests.
3. `docs.open`, with interest held by the lease and the value cache by version.
4. Writes through the overlay, including rejection.
5. `docs.roster` and `docs.drawers`.
6. `use_doc` and `use_datums` in the MDK; `clay_docs` facets as a view.
7. The real node: handles from `micros.handles`, calls signed as the micro's actor. The review micro drops its
   interim `docCap` reads.
8. The nodules lane: meta groups of known drawers (§5).

## Consequences

- Positive: micros get documents through the same waist as every other collection, with one cache and one
  authorization story, and nodules see the same rows.
- Positive: list views cost index reads; hydration happens only for rows on screen and documents opened.
- Positive: no query, list or open can widen what the node mirrors.
- Cost: the docs sources select remotely, a second mode of `Source` beside local `matches`.
- Cost: values arrive after versions on large pages, so views must render a row before its value.

## Deferred

- **Opening many documents at once.** `docs.open` fetches one absent document. Resolving authority for, and fetching,
  n of them in bulk is a later optimization.
- **Remote rosters and remote queries.** Querying a drawer whose metadata isn't local, through another node's
  cabinet, waits for requirements: this is a peer-to-peer system first. One later shape is a proxied drawer, where
  the local cabinet answers for a drawer it doesn't mirror by forwarding to a node that does.
- History reads (`at`), branches and merges, and `queryDatumRefs`.
