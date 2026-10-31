# ADR 025: Cabinet datum predicates and `PollDatums`

**Status:** Accepted. Not yet implemented. It replaces `QueryDatums`, `ObserveDatums`,
`DatumSelector` and `ObserveScope` in `big_cabinet_core::doc`, and amends
[ADR 009](009-datum-set-routing-frontier.md) §6. It depends on
[ADR 023](023-cursor-consumption-is-long-poll.md) (long polling) and
[ADR 024](024-indexes-are-fed-by-the-change-stream.md) (frontier-fed drawer projection
and folded reference edges).

## Context

Cabinet exposes two ways to read datums across docs:

- `QueryDatums { capability, drawer_id?, selector: DatumSelector }` for current state,
  where the selector is one of `All`, `Doc`, `Tag`, `Route` or `Key`;
- `ObserveDatums { capability, scope: ObserveScope, after }`, a `stream` operation
  whose scope is one of `Doc`, `Tag`, or `Drawer { drawer_id, tag? }`.

Both take a single selector, although the store supports more: `DatumSetStore::open_tags`
takes many `(tag, after)` pairs. A blob worker needs Blob, cipherBlob and JWK datums of
one drawer's docs, which today means several streams or an unfiltered one filtered
client-side.

The `Drawer` observe scope also has three defects:

- **Full recompute on every tick.** Every tick of the global drawer-change counter, and
  every capability `Live`, makes every Drawer-scoped observer recompute its whole
  transitive readable set (`recheck_drawer` → `readable_in_drawer`) and diff it. The
  cost is O(observers × closure) per unrelated drawer edit anywhere on the node.
- **Stamps ahead of the reader.** `recheck_delivered` and `recheck_drawer` stamp
  access- and membership-triggered events with `store.latest_revision()`
  (`doc/observe.rs` ~325, ~389), which can be ahead of the page the reader consumed. A
  consumer that resumes from that stamp skips the entries in between.
- **Membership is not in the stream.** The readable set is computed from current state
  when the stream opens. A resume with `after > 0` misses docs that joined while the
  consumer was away (their datums are older than `after`), and sends no leave for docs
  that left.

A richer predicate language exists in `daybook_types::manifest` (`DocPredicateClause`).
It is reference material only. `daybook_types` and `daybook_core` are not touched, and
this ADR does not port it.

## Decision

### 1. A predicate language in `big_cabinet_core::query`

A new `query` module in `big_cabinet_core` holds the predicate, the datum selection,
and their wire spellings. **Every clause must be answerable from indexes.** No clause
may need a doc hydrated or a datum value read. That rules out value matches
(`FieldMatch`). A consumer that needs a value filter applies it to the datums it is
delivered.

Clauses:

- `HasTag { tag }` and `HasKey { key }`: rows of `big_cabinet_datum_set_entries`, which
  is indexed by tag and by key.
- `IsDoc { docId }`: one named doc. It replaces today's `Doc` scope and `Doc` selector.
- `InDrawer { drawerId, transitive, atLeast }`: the docs a drawer reaches, from the
  drawer projection's recursive CTEs (`scoped_docs.sql`).
  - `transitive: false` means the drawer's own `docEntry`s plus its own self-doc
    children, which is what a per-drawer worker needs.
  - `atLeast` is a Keyhive level (`Relay`, `Read`, `Write`). The level a doc has is
    `min(the caller's grant on the drawer, the CTE level along the best route)`.
- `HasReferenceTo { targetTag, targetDoc? }`: docs with a reference edge to a datum of
  `targetTag`, optionally in one target doc. It joins `big_cabinet_datum_ref_edges`
  with the target's `datum_set_entries` row, which supplies the target tag. The edge
  table has no tag column. Both tables share one SQLite database, and with
  ADR 024 they also share one revision.
- `And`, `Or`, `Not`.

The access level is defined only relative to a drawer, which is how it stays answerable
from indexes. A drawer-free "docs I can write anywhere" clause would need capability
grants in SQL, and they are not there; it is left out on purpose.

The predicate is a request input, so its union is closed. A server that does not know a
clause refuses the request; it does not ignore the clause, because ignoring a clause
silently widens or narrows the answer. Wire spellings follow
[ADR 021](021-datum-schema-language-and-automerge-codecs.md) §5: internally tagged
unions and camelCase fields.

### 2. Anchors: the predicate carries its own authorization

Drawer membership is a clause, not a scope. A caller below node-wide authority must
make the predicate a top-level `And` containing an **anchor**, and the anchor is also
the authorization check:

- `InDrawer` with `atLeast` of at least `Read`: one capability check on the drawer,
  then the reach CTE;
- `IsDoc`: one `Read` check on that doc, the check today's `Doc` scope makes before it
  opens.

`Not` needs a positive sibling in its `And`, so no request enumerates "every doc
without X". A capability with node-wide Read may send an unanchored predicate; for it,
the per-doc check is vacuous. That replaces the per-candidate `authorize` loop in
`doc/query.rs` and the per-batch check of the `Tag` scope in `doc/observe.rs`.

### 3. A request has two halves

- The **predicate** selects docs.
- The **datum selection** (`datums`: a set of tags and keys) selects which datums of the
  matching docs are delivered.

The per-drawer blob worker, for example, sends the predicate
`And[InDrawer { X, transitive: false, atLeast: Read }, HasTag(blob)]` with the selection
`{blob, cipherBlob, jwk}` ([ADR 026](026-drawer-native-blobs.md)).

### 4. One long-poll operation: `PollDatums`

```text
PollDatums { capability, predicate, datums, after, limit, holdMs }
  -> DatumPage { events, through, caughtUp }
```

- Each event carries its revision and is one of:
  - `enter { docId, datums }`: the doc now matches; `datums` are its current selected
    datums;
  - `datum { docId, datumKey, datum }`: a selected datum changed on a matching doc
    (`ObservedDatum::Present` or `Removed`, as today);
  - `leave { docId }`: the doc no longer matches.
- `through` is the cursor to send next. `caughtUp` replaces `ReplayComplete`.
- **A current-state query is a poll from `after = 0` with `holdMs = 0`, paged until
  `caughtUp`.** A frontier replay from zero yields the latest entry per key, which is
  the current state. One operation covers paging and waiting. A multi-page query is
  not a point-in-time snapshot. Later pages may carry newer revisions, and the consumer
  converges by continuing from `through`.
- In-process consumers, such as the big_blobs pin worker, use the same operation
  through the Direct client.

`QueryDatums`, `ObserveDatums`, `DatumSelector`, `ObserveScope`, `DatumObserved` and
`ObservedDatum::AccessLost` are removed, which is an interface break. Their callers are
Cabinet-internal: `doc/query.rs`, `doc/observe.rs`, `protocol_wire.rs`, the doc and
drawer e2e tests, and the wire snapshot. `DatumSetStore::query(DatumSetQuery)` stays as
the store's internal point lookup (used by `drawer.rs` and the `big_node` e2e); it is
not a protocol surface. `QueryDatumRefs` stays as the edge point-query surface.

### 5. One cursor over datums and membership

The cursor is the datum-set frontier revision, which is itself driven by the AFW
cursor. Roster facts are datums on metadata docs, so ADR 024's frontier-fed drawer
projection puts membership changes on the same cursor as the datums. The promise to a
consumer is ADR 023 rule 2: **an event stamped r says everything at revisions ≤ r has
been delivered.**

A poll opens one frontier reader at `after`, on the union of the selected tags, the
tags the predicate names, and the roster tags. For each page at revision r:

1. Wait until the drawer projection's progress is at least r.
2. For datum entries, re-evaluate the predicate for the page's docs in one SQL
   statement (`doc_id IN page`).
3. For roster entries, first check that the changed drawer is the anchor drawer or a
   descendant of it (one `ancestors` CTE). The affected docs are the one doc of a
   `docEntry` change, or `scoped_docs(child)` for a `childEntry` change. Re-evaluate
   those docs in one statement, so a doc reachable through two children survives
   losing one path.
4. Stamp every resulting `enter`, `datum` or `leave` with **r**, never with the store's
   latest revision.

Why resume is now correct:

- a join after `after` is a roster entry with a revision after `after`, so it is
  replayed, and the doc's datums are delivered then, even though they are older;
- a leave after `after` is replayed as a leave, which is idempotent for a consumer that
  never had the doc;
- delivery is at least once and converges. The projection may be ahead of r when a
  page is evaluated. That can only bring a later fact forward, and the later page
  re-evaluates it.

The predicate compiles to **one** SQL statement: a reach CTE per `InDrawer` (without
recursion when `transitive: false`), `EXISTS` subqueries on `datum_set_entries` for tags
and keys, the edge join for `HasReferenceTo`, and the datum selection as the outer
projection. No doc set is materialized in Rust. There is no closure table. If CTE cost
on deep drawer graphs ever dominates, a materialized closure table is the escalation.

### 6. Grants and capabilities stay outside the cursor

Grant and capability changes are not datum facts. **Every poll re-authorizes** and
recomputes `atLeast` on its anchor:

- below the anchor's required level the poll answers `Denied` and the cursor does not
  move;
- a level change applies from the next page on;
- a held poll also wakes on a capability change on the node, so a revocation does not
  wait out a hold.

There is no `AccessLost` push, no in-pump `ObserveCapability` subscription, and no
per-consumer delivered set. The server keeps no state between polls; §5 makes every
event a function of the page it came from.

An `IsDoc` anchor takes a BigRepo `DocInterest` for the duration of each poll, where
today's `Doc` scope holds one for the stream's life. BigRepo keeps an interest alive for
`SYNC_INTEREST_GRACE` (30 s, `src/big_repo/interest.rs:12`) after its last holder drops
it. That is longer than the 15 s hold cap, so a consumer that keeps polling keeps the
doc in sync without a server-side session.

## Consequences

- Positive: one operation for paging and watching, with exact resume across joins and
  leaves. Membership changes cost work proportional to the changed entry, not to every
  observer's closure.
- Positive: a consumer names exactly the datums it needs from exactly the docs it
  needs, in one request.
- Cost: an interface break across Cabinet's doc protocol, its e2e tests and its wire
  snapshot.
- Cost: held polls need the XRPC admission class of ADR 023 rule 5.
- Cost: an entry admitted late by a Keyhive change reaches observers at their next poll
  rather than at a replayable revision (ADR 024, drawer projection admission).
