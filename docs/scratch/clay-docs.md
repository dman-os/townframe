# Clay docs on the collection waist

Status: design note for the operator, 2026-11-24. Nothing here is built.
Owner: claytxt lane.
Updated: 2026-11-24
Scope: how micros read and write node documents. Clay docs becomes a collection that the host provides, over the
node's `pollDatums`/`writeDatums`, instead of a separate document mechanism.
Context: the operator, 2026-11-24: "build on top of the clay collections waist if possible"; a micro mounts a query
over what its grants reach and never streams every document it can access (TanStack DB style live queries);
intxt is waiting on this to drop its interim `docCap` reads.

## What exists today

- **The node.** Every document method takes a capability.
  - Documents: `docs.createDoc`, `readDatums`, `listDatums`, `writeDatums` (with an idempotent `command` id),
    `pollDatums`, `branchDoc`, `mergeDoc`, `getDocAvailability`, `observeDocAvailability`, `queryDatumRefs`.
  - Drawers: `drawers.*` (`listDocs`, `addDoc`, `getDrawer`, ...).
  - Source: `big_cabinet_core/doc.rs`.
- **Nodules** call these with the handles the nodules service mints for their actor.
- **Micros** have no document API of their own. The review micro calls the same methods through `Context::call`,
  holding a `docCap` minted for it at install (interim, `daytxt_native/interim.rs`).
- **`clay_docs`** (ADR 015 §4) has typed facets, a shared cache and optimistic transactions, over an in-memory
  `MockDocuments` only. Nothing connects it to the node.

## Why the waist fits

`pollDatums` is already a projection lease in ADR 017's terms:

| ADR 017 waist (`clay_collection`)            | `pollDatums` (ADR 023, ADR 025)                         |
|----------------------------------------------|---------------------------------------------------------|
| `acquire(selector)`                          | `predicate` + `datums` selection                        |
| `read(committed, limits, poll-timeout)`      | `after`, `limit`, `hold_ms`                             |
| `Page { transactions, through, bootstrap_complete }` | `DatumPage { events, through, caught_up }`      |
| `Change::Upsert` / `Change::Remove`          | `DatumEvent::Datum` / `DatumEvent::Leave` (move-out)    |
| `RowEvidence::Pending` + `Ledger` overlays    | `writeDatums` with a `command` id; settles or rejects   |

## Proposal

1. **A host-provided collection contract `docs`.**
   - Selector: `{ predicate, datums }`, the `pollDatums` query language.
   - Key: `(doc_id, datum_key)`. Value: the observed datum.
   - A micro requests it like any collection (`collection_requests`, ADR 028 §4). Its scope is its cartridge's
     grants, the same for its nodules and micros (operator, 2026-11-24).
2. **A micro mounts queries, never its whole reach.** Each mounted query is one projection lease. The MDK gives it
   a live-query hook, `use_docs(selector)`, which returns the narrow list and updates incrementally.
3. **The host polls with the micro's own authority.** The docs handle comes from `micros.handles` (ADR 052). The
   host runs one `pollDatums` loop per distinct `(handle, selector)` and feeds it into the projection.
4. **Writes are collection commands.** A write is a domain command on the collection, which the host lowers to
   `writeDatums`.
   - The `command` id is the collection's `OperationId`.
   - The pending row shows at once through `clay_collection`'s overlay, and the authoritative row retires it.
   - A rejection removes the overlay and reaches the micro as the command's failure.
5. **`clay_docs` becomes a view over that collection.** The typed `Facet<T>` stays as the authoring surface.
   `DocumentService`'s own cache, refresh and transaction machinery goes away.

## The one structural special case

`clay_collection::Projection` assumes its authority publishes the whole collection into the host, and each lease
filters locally (`Source::matches(selector, key, value)`). Node documents cannot work like that: the host does not
hold every document, and the node evaluates the predicate.

So the docs source **selects remotely**:
- `Source::acquire` starts the upstream poll for that selector.
- Membership comes from the node's `Datum`/`Leave` events instead of `matches`.
- Release stops the poll.

This is a variant of `Source`, not a fork of the waist: leases, pages, cursors, overlays and resets stay as they
are. Does `Source` gain a "remote selection" mode, or does each selector become its own projection? Measure both
once there is a fake node.

## Open questions

1. **Sharing.** May two micros with the same grants share one upstream poll and cache? Or is each projection keyed
   by the handle that made it, so the cache never crosses authority? Proposed: key by handle, and revisit only if
   it costs too much.
2. **Availability.** With partial replication a matching document may not be local yet. Does a row carry a
   `pending` availability state (from `observeDocAvailability`), or does it only appear once present?
3. **Latency and fan-out.**
   - Long-poll holds are enough to start. When is a push stream worth it?
   - What bounds one query: the most documents in a result, or the most concurrent leases per micro?
4. **Large values.** Datums can be large. Page limits bound transactions, not bytes per row. Do we need field
   selection inside a datum, or a byte bound on a page?
5. **Out of the first cut:** history reads (`at`), branches and merges, and `queryDatumRefs` (references across
   documents).

## Order of work

1. A fake node source in tests: `pollDatums`/`writeDatums` over an in-memory store, behind the same seam the real
   node uses.
2. The remote-selecting `Source` plus the `docs` contract, with leases end to end in `clay_collection` tests.
3. Writes through the overlay, including rejection.
4. `use_docs` in the MDK; `clay_docs` facets as a view over it.
5. The real node: handles from `micros.handles`, polls through dispatch. The review micro drops its interim
   `docCap` reads.
