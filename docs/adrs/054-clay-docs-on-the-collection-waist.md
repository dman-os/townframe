# ADR 054: Clay docs on the collection waist

**Status:** Accepted (2026-11-24). Proposed by the claytxt lane, reviewed and accepted by the operator the same day.
Built so far: the fake cabinet and `docs.datums` feeds (`clay_cabinet`). Byte budget numbers (§5) are to be
measured.

**Amends:** ADR 015 §4 (`clay_docs` stops owning a document cache and transactions and becomes a view over the
collections below).

**Needs from the node:** one small change in `big_node` (§8, meta groups of known drawers). Everything else uses
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
   - Row key: `(docId, datumKey)`. Row value: the version, plus the value when the host holds it (§5).
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

**Each query is its own feed.** `clay_collection::Projection` filters each lease locally with `Source::matches`,
but for node documents the node evaluates the predicate. So each feed gets its own projection, with the waist's plain
source that matches everything, fed by its own upstream poll (`clay_cabinet::DatumFeeds`). The poll pages from the
start until the node reports it caught up, publishes that as one bootstrap transaction, then holds for changes;
`Leave` events remove every row of the document. The first lease starts the feed and the last lease dropping it stops
the poll. `Source`, leases, pages, cursors, overlays and resets are unchanged.

## 4. Prior art: Zero

Zero (Rocicorp) solves the same problem between its clients and `zero-cache`, as of 2026-11:
- **The client keeps one normalized store and evaluates every query over it.** A new query first runs against the
  local store and returns what it has at once, as result type `unknown`. It becomes `complete` once the server has
  run it. The server never tags rows with the queries they belong to; the client computes membership itself.
- **The server sends each row once.** `zero-cache` keeps a client view record (CVR) per client: one record per row,
  with `refCounts` keyed by query hash. Rows two queries share are one record. Removing a query decrements its
  counts, and a row left with no references is deleted from the client, after the query's TTL.
- **Completeness is its own patch.** Besides row puts and deletes, a poke carries query patches: "desired" (the
  client asked) and "got" (the server ran it).
- **Sharing stops at one authority.** The CVR belongs to one client, and the server applies that client's
  permissions when it resolves each query. Rows are shared between queries of the same authority, never across.

Sources: [Zero docs](https://zero.rocicorp.dev/llms-full.txt); `rocicorp/mono`,
`packages/zero-cache/src/services/view-syncer/cvr.ts` and `schema/types.ts`.

What we take from it, per link:

| Link          | Zero's piece                  | Ours                                                                       |
|---------------|-------------------------------|----------------------------------------------------------------------------|
| node → host   | server runs the query         | the node runs the predicate (one SQL statement); polls stay stateless (ADR 025 §6), no node-side CVR. Overlap costs ids and versions, never values |
| host → micro  | CVR: rows once, refcounted    | §6: membership pages plus a value stream, each value sent once per micro   |
| local answers | client query evaluation       | §7: the host and the guest can evaluate a predicate over what they hold; instant answers come later |
| authority     | one client's permissions      | §5: one micro's capability, its actor's grants                            |

## 5. Feeds are per authority; the node filters

A feed is keyed by `(authority, selector)` and polls with that authority's own capability, signed as its actor. The
node binds each capability to one actor, and a micro's handles are bound to its own actor (`micros/<cartridge>/<micro>`,
ADR 052). So today the authority is one micro, as one client is in Zero: queries of one micro share a feed. A
cartridge's micros hold the same grants (ADR 049), so a cartridge-wide actor would widen sharing to the cartridge
without changing this design.
- The node authorizes every poll and filters by the caller's reach (ADR 025 §2, §6). Whatever a feed delivers, its
  cartridge may see; there is no admission check of its own and no second copy of the grants.
- A withdrawn grant takes effect at the next poll, and the node wakes held polls on capability changes, so a held
  feed doesn't wait out its hold.
- Two micros mounting the identical query poll twice. Polls are index reads, so that is cheap, and the values they
  name are shared (below).

**The host mirrors no grants.** Filtering on the host would need the node's authority algebra: drawer levels, the
weakest link along a child path, the strongest path winning (ADR 019). A second copy of it would be a second
authoritative access graph, which ADR 019 rules out for Cabinet itself. The node stays the only place that answers
"may this authority see this row".

**What crosses authorities is values, by version.** Values are cached by `(docId, datumKey, datum_heads)`. A version
names immutable content, so one cached value serves every micro whose own poll delivered that version, and a
document is hydrated once however many micros show it. A micro can only ask for a value whose version its own feed
delivered, so the cache never answers past its reach.

**Hydration is the budget.** Reading values hydrates documents and is the expensive step; polls and rosters are index
reads.
- A `docs.datums` page carries versions. The host fetches values only for rows a lease asks to see, up to a byte
  budget per page. Rows past the budget arrive as versions and fill in on later pages.
- `docs.open` hydrates its document once in the host, however many leases hold it.
- The host bounds concurrent hydrations per micro and in total, so a list of ten thousand rows can't hydrate ten
  thousand documents at once.
- The numbers are measured against the fake node (§10) before they are fixed.

**Nodules.** Nodules call the node directly and get no host cache. The selectors, rows and commands are the same, so
a nodule's poll and a micro's lease describe the same rows, with or without the Clay host in between.

**Actors.** The host fetches each micro's handles as the operator (`micros.handles`, ADR 052 §3). Every node call
made with those handles is signed as the micro's own actor (`micros/<cartridge>/<micro>`), because the node honours a
body capability only under the actor it is bound to.

## 6. Rows reach a micro once: membership pages and a value stream

Between the host and one micro, the Zero pattern applies directly.
- **Membership.** A lease's pages carry membership only: a row's key and version, entering or leaving that lease's
  result.
- **Values.** One value stream per micro carries each `(key, version)`'s value once. The host keeps, per micro, the
  versions the micro holds and a reference count of the leases whose membership includes each. A value is sent the
  first time any lease needs it, and released when the last lease referencing it drops it.
- The guest keeps one value store and joins it with each lease's membership. Two overlapping queries in one micro
  cost one copy of each shared value, on the wire and in guest memory.

The guest evaluates no predicate in this design: membership comes from the host. That keeps the guest small and keeps
one evaluator authoritative.

## 7. One query evaluator, three places

Every clause is answerable from index facts (ADR 025 §1): which datum keys a document carries, which drawers list it.
So the predicate has one meaning over three stores:
- the node, compiled to one SQL statement over `datum_set_entries` and the drawer projection (today);
- the host, over the rows and rosters its feeds hold;
- a guest, over the rows its leases hold.

**Code structure.** The predicate types already live in `big_cabinet_core::query`. The in-memory evaluator goes beside
them, as `Predicate::matches(&self, facts: &impl IndexFacts, doc_id)` over a small trait (`has_key`, `has_tag`,
`listed_in`). The fake cabinet uses it, and a conformance table of `(facts, predicate, expected docs)` runs against both
the evaluator and the node's SQL, so they cannot drift. This is ADR 017 §2's "one logical contract, shared conformance
fixtures", with the node's SQL as the second interpretation.

**What the host can evaluate.** Has-tag, has-key, is-document and drawer membership (roster entries are datums on the
drawer doc, so the host has them whenever it holds that roster), and `And`/`Or`/`Not`. It cannot evaluate
`HasReferenceTo` (no edge table) or `InDrawer`'s `atLeast` attenuation (it holds no grants, by §5).

**Instant answers come later.** With the evaluator, the host can answer a new query at once from what that micro's
feeds already hold, marked incomplete (`bootstrap_complete: false`), with the node's first caught-up page completing
it: Zero's `unknown` then `complete`. Only that micro's own rows are eligible, so permissions need nothing more.
This waits until values and `docs.open` exist.

## 8. Interest is per document; known drawers stream their meta group

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

## 9. Use cases

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

## 10. Order of work

1. A fake node in tests: datum index, roster, values, known drawers and availability over an in-memory store, behind
   the seam the real node uses. It counts hydrations, so the budgets are measurable.
2. `docs.datums`: feeds per authority, one projection each, leases end to end.
3. The in-memory evaluator in `big_cabinet_core::query` and its conformance table against the node.
4. `docs.open`, with interest held by the lease and the value cache by version.
5. Writes through the overlay, including rejection.
6. `docs.roster` and `docs.drawers`.
7. Membership pages and the per-micro value stream (§6); `use_doc` and `use_datums` in the MDK; `clay_docs`
   facets as a view.
8. The real node: handles from `micros.handles`, calls signed as the micro's actor. The review micro drops its
   interim `docCap` reads.
9. The nodules lane: meta groups of known drawers (§8).
10. Instant answers from the host cache (§7).

## Consequences

- Positive: micros get documents through the same waist as every other collection, with one cache, and the node as
  the only authorization, and nodules see the same rows.
- Positive: list views cost index reads; hydration happens only for rows on screen and documents opened.
- Positive: no query, list or open can widen what the node mirrors.
- Cost: one projection per (authority, query), so overlapping queries keep overlapping keys and versions twice in
  the host. Values are shared by version.
- Cost: one more evaluator of the predicate language, kept honest by a conformance table against the node.
- Cost: values arrive after versions on large pages, so views must render a row before its value.

## Deferred

- **Opening many documents at once.** `docs.open` fetches one absent document. Resolving authority for, and fetching,
  n of them in bulk is a later optimization.
- **Remote rosters and remote queries.** Querying a drawer whose metadata isn't local, through another node's
  cabinet, waits for requirements: this is a peer-to-peer system first. One later shape is a proxied drawer, where
  the local cabinet answers for a drawer it doesn't mirror by forwarding to a node that does.
- History reads (`at`), branches and merges, and `queryDatumRefs`.
