# Clay docs on the collection waist

Status: design note for the operator, second draft. Nothing here is built.
Owner: claytxt lane.
Updated: 2026-11-24
Scope: how micros (and, through the same contract, nodules) find, watch, read and write node documents.
Clay docs becomes collections that the host provides over the node's document and drawer methods, instead of a
separate document mechanism.
Context: the operator's review of the first draft, 2026-11-24:
- build on the collection waist;
- a micro mounts queries and never streams its whole reach;
- the Clay host owns one intermediate cache that every micro goes through;
- a query must never make a drawer eligible for mirroring;
- the API has to work with sync interest;
- hydration is the cost to bound;
- it covers both a live document or datum and the sync of a set ("what is a collection?").

## What exists today

Checked against the code, 2026-11-24.

- **Polls see the index, not documents.** `pollDatums` answers from `big_cabinet_datum_set_entries`, which holds
  `(doc_id, datum_key, tag, doc_heads, datum_heads, clock)` and no values. A `datum` event carries a version
  (`ObservedDatum::Present { doc_heads, version }`), not the datum. So a poll never hydrates a document.
- **Values cost a hydration.** `readDatums { docId, at, datums? }` and `listDatums` load the document.
- **Only local documents are in the index.** A document gets index rows when it materializes on this node. A
  roster entry for a document that is not here moves nothing in a poll: the document matches `InDrawer`, but has no
  rows to deliver.
- **Membership is readable without the members.** A roster lives as `drawer.docEntry/<docId>` datums on the
  drawer's metadata document, and `drawers.listDocs` reads the scoped roster, including members not present locally.
- **Interest is per document.** `big_repo::DocInterest` leases one document into `DocSyncDemand`, which becomes
  big_sync object subscriptions to the connected peers. A poll with an `IsDoc` anchor holds one for the poll's
  duration, with a 30 s grace after it drops. An `InDrawer` poll takes no interest.
- **Mirroring is a durable node decision.** The known-drawers list (`knownEntry/<drawerId> { mirror?: true }`,
  written only by `rememberDrawer`/`forgetDrawer`/`setDrawerPolicy`) is what `drawer/mirror.rs` turns into group-part
  subscriptions. Nothing else subscribes a drawer's parts.
- **Availability is per document.** `getDocAvailability`/`observeDocAvailability` return
  `{ localUsability: NotObserved | NoLocalContent | PendingMaterialization | Usable, syncDemand }`.
- **Micros have no document API.** The review micro calls node methods through `Context::call` with an interim
  `docCap` (`daytxt_native/interim.rs`). `clay_docs` (ADR 015 §4) runs over `MockDocuments` only.

## A document is in one of four states

For a given caller and document, these four facts are independent:

| Fact        | Source                                            | What it lets a micro do                          |
|-------------|---------------------------------------------------|--------------------------------------------------|
| listed      | a roster it can read (`listDocs`, `docEntry` rows) | know the document exists and its id              |
| authorized  | its cartridge's grants (ADR 049)                  | ask for anything about it                        |
| local       | the datum-set index, availability                 | query its datums and versions; read values       |
| live        | someone holds interest                            | expect peers' changes to arrive soon             |

A list view needs listed + local. A detail view needs local + live. The API keeps the four facts apart, and never
lets one silently cause another:
- a query never fetches anything;
- reading a listed but absent document is an explicit open, which takes interest;
- mirroring a whole drawer is an explicit, authorized policy change, never a side effect of a query or an open.

## Proposal

### 1. Three contracts, one waist

All three are collections on the ADR 017 waist: leases, pages, cursors, overlays, resets.

1. **`docs.datums`: a datum query.**
   - Selector: `{ predicate, datums }`, `pollDatums`' language unchanged.
   - Row key: `(docId, datumKey)`. Row value: the version, plus the value when the host already holds it.
   - Membership comes from the node (`Datum`/`Leave`), so it only ever covers local documents.
   - This is what a list view mounts.
2. **`docs.roster`: drawer membership.**
   - Selector: `{ drawerId, transitive }`. Row key: `docId`. Row value: the entry (access level), plus the
     document's availability as a field.
   - Backed by the drawer's own `docEntry` rows, which are local whenever the drawer is known, so it lists members
     that are not here.
   - A micro that wants a non-mirrored drawer's members uses this, then picks documents to open.
3. **`docs.open`: one document, live.**
   - Selector: `{ docId, datums? }`. The lease **is** the interest: acquiring it takes a `DocInterest` in the host,
     and dropping it releases it, after the node's grace.
   - Rows are the selected datums with values; the document's availability is a field on the lease's page.
   - This is what a detail view mounts, and the `use_doc`/`use_datum` hook sits on it.

Writes go through all three the same way: a write is a domain command on the collection, which the host lowers to
`writeDatums`. The `command` id is the collection's `OperationId`, the pending row shows at once through
`clay_collection`'s overlay, the authoritative row retires it, and a rejection removes it and fails the command.
A write to a document that is not local fails; it never fetches.

`clay_docs`' typed `Facet<T>` stays as the authoring surface, as a view over `docs.open` and `docs.datums`. Its own
cache, refresh and transaction machinery goes away.

### 2. The host owns one cache

Like other collections, the Clay host holds one projection per distinct selector, and every lease on it reads from
that projection. Two micros mounting the same query share one upstream poll.

Authority stays per lease:
- the projection is fed with the host's own reach;
- each lease is checked against its micro's handle on acquire and on every page, as `pollDatums` re-authorizes
  every poll;
- a lease only ever sees rows its handle reaches. ADR 017 §2: "authorization never overfetches".

Values are cached by version, `(docId, datumKey, datum_heads)`. A version names immutable content, so one cached
value serves every lease that was authorized to see that version, and a value is read (and its document hydrated)
once, however many micros show it.

Nodules use the node directly and get no host cache. The contracts are the same, so a nodule's poll and a micro's
lease describe the same rows. That is the isomorphism: the same selectors, rows and commands, with or without the
Clay host in between.

### 3. Hydration is the budget

The expensive step is reading values, because that hydrates documents. Polls and rosters are index reads.
- A `docs.datums` page carries versions. The host fetches values only for rows some lease asks to see, and only up
  to a byte budget per page. Rows past the budget arrive as versions and fill in on later pages.
- `docs.open` hydrates its document, once in the host, however many leases hold it.
- The host bounds concurrent hydrations per micro, and in total, so a list of ten thousand rows cannot hydrate ten
  thousand documents at once.

### 4. Interest follows leases

- `docs.open` holds document interest for the lease's life. This is today's `DocInterest`, unchanged.
- `docs.datums` and `docs.roster` hold no interest. A list view stays current with whatever the node already syncs.
- **Proposed node addition: drawer interest.** A lease-scoped subscription to a drawer's group parts, so a micro
  that has a drawer open sees its roster and metadata changes live, without the node adopting the drawer as a
  mirror. It is the group-part analogue of `DocInterest`: held, then released after a grace, and never written to
  the known-drawers list. Without it, a non-mirrored drawer's roster only changes when something else syncs its
  metadata document.

### 5. Mirroring stays explicit

A micro or nodule that needs a whole drawer present asks for it with `setDrawerPolicy { mirror: true }`, gated like
any drawer write by its cartridge's grants. Querying, listing and opening never change policy. A drawer the node
knows about but doesn't mirror is still fully usable through `docs.roster` plus `docs.open`.

## Use cases

| Use case (FDR 001, drawer wiki)              | What the micro mounts                                         |
|----------------------------------------------|---------------------------------------------------------------|
| Personal multi-device cabinet (mirrored)     | `docs.datums` with `InDrawer` + tags; `docs.open` on select     |
| One shared document                          | `docs.open` on the document                                    |
| Family photo album                           | `docs.datums` for the list; blobs through `blobs` (ADR 026)     |
| Workplace project, child drawers             | `docs.datums` with `InDrawer { transitive: true }`              |
| Chat                                         | `docs.open` on the thread document: interest gives liveness    |
| Large public wiki, plug registry (not mirrored) | `docs.roster` plus drawer interest; `docs.open` per page read |
| XRPC-only client, third-party index          | out of scope: a remote index is a different source             |
| intxt review micro                           | `docs.datums` for its review queue; `docs.open` for one review |

## Open questions

1. **Drawer interest** (§4): a node change, so it needs the nodules lane. Is it a `big_repo` group-part lease next
   to `DocInterest`, and does a poll with an `InDrawer` anchor take one, as an `IsDoc` poll takes a `DocInterest`?
2. **Roster availability.** `docs.roster` reports each member's availability. Fed by `observeDocAvailability` per
   member, that is one stream per member. Does the node need a drawer-scoped availability poll?
3. **What is a collection to the user?** A drawer, a saved query, or both? This note treats a drawer as a scope
   and a query as the thing mounted. Saved queries as documents are left for later.
4. **Byte budgets.** The numbers for the per-page value budget and the hydration bounds. Measure with the fake
   node first.
5. **Remote rosters.** A huge public drawer whose metadata isn't local at all needs a remote listing (the drawer
   wiki's XRPC). Out of the first cut, along with history reads (`at`), branches and merges, and
   `queryDatumRefs`.

## Order of work

1. A fake node in tests: datum index, roster, values and availability over an in-memory store, behind the same seam
   the real node uses. It counts hydrations, so the budgets are measurable.
2. `docs.datums` with the remote-selecting source, and the shared host projection, with leases end to end in
   `clay_collection` tests.
3. `docs.open` with interest held by the lease, and the value cache by version.
4. Writes through the overlay, including rejection.
5. `docs.roster`.
6. `use_doc`/`use_datums` in the MDK, and `clay_docs` facets as a view.
7. The real node: handles from `micros.handles`, then the review micro drops its interim `docCap` reads.

The remote-selecting `Source` from the first draft stands: `Projection` filters locally with `Source::matches`, but
for node documents the node evaluates the predicate, so the source selects remotely. Membership comes from
`Datum`/`Leave` events, and acquiring or releasing a selector starts or stops its upstream poll.
