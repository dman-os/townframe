# ADR 024: Indexes are fed only by the change stream

**Status:** Accepted. Not yet implemented. Today `drawer::State` is written directly by
DrawerService ops and rebuilt by on-demand rescans, and `datum_ref` is a separate
frontier consumer with its own cursors. Both are converted as described below.

## Decision

**Local writes take the remote path.** Every index and projection in the node is a
function of the change stream and nothing else. That covers the datum-set index, the
reference edges, the drawer projection, and every big_blobs index. An operation writes
documents and returns a receipt. It never writes an index row, and it never waits for
an index to catch up.

The change stream is the Automerge frontier walker (AFW, ADR 008), and the datum-set
frontier is layered over it (ADR 009). A remote change can reach an index only
asynchronously, through that stream. A local change reaches it the same way: it is
written to the doc, and the same watcher picks it up. There are not two code paths
maintaining one incremental structure, an op-side direct write and a stream-side
rebuild. An index lives behind the stream, in its own "address space", not inside the
writer's transaction.

The reasons:

- **Most writes are remote.** Roster entries, datums and grants arrive from peers far
  more often than from local ops. A local write path that updates an index directly
  never gave a consistent view anyway. It only made local writes look synchronous
  while remote ones were not.
- **Two writers of one structure diverge.** An op-side write and a stream-side
  rebuild must agree row for row, forever. Any difference is a bug that only shows on
  one path. With one writer, the index's state is reproducible from the stream.
- **Recovery is replay.** An index that is a function of the stream rebuilds from
  cursor zero and needs no repair path.

## Reading your own write

A caller that must observe its own write waits on the index's cursor. It long-polls
([ADR 023](023-cursor-consumption-is-long-poll.md)) until the index covers the receipt.
The op does not do this; the caller chooses to.

A receipt names the doc and the heads the op committed. An index covers a receipt once
it has settled that doc at heads that include the receipt's heads. The datum-set
index already records `doc_heads` for every route it settles, and indexes downstream of
it inherit them. **Open for implementation:** the exact wait surface. The candidates are
a per-doc "settled heads" query on the datum-set index, or a cursor that the receipt
carries once the frontier assigns it a revision. The choice must not add an op-side
write.

## The drawer projection becomes a frontier-fed index

Roster facts are already datums on metadata docs: the descriptor
(`org.example.daybook.drawer`), `drawer.docEntry/*`, `drawer.childEntry/*`, and
`drawer.metaDoc/*` ([ADR 026](026-drawer-native-blobs.md)). They are already in the
datum-set frontier at their own revisions.

- `drawer::State` consumes the datum-set frontier on those tags. It stores a progress
  revision in the same transaction as its rows, and exposes it as `progress:
  watch<u64>`, replacing today's bare change counter (`state.rs:28`).
- That replaces the roster rescan (`drawer.rs` ~740–790, `replace_roster`), and the
  direct `put_doc`/`put_child` writes from `AddDoc` and `AddChild`
  (`add_doc.rs:108`, `add_child.rs:70`).
- DrawerService ops write the metadata doc and return its receipt. Their idempotency
  checks ("already listed at this level?" in `publish_entry` / `publish_child`) read
  the metadata doc they are about to write, not the projection.
- **Authorization answers from the projection only.** `Core::effective`
  (`drawer.rs:489`) and `Core::reach` (`drawer.rs:512`) no longer fall back to
  `sync_discovered` → `ensure_drawer` → `discover_drawer` on a miss, and
  `PolicyOperation::Refresh` (`access.rs` ~186–238) goes away. Those fallbacks reload
  every announced descriptor and rescan rosters breadth-first on the hot read path.
  After this change a miss means "not reachable as of the projection's cursor", and the
  answer is `Denied`. A caller that just wrote the roster waits as described above.
- The projection loads values from the doc, as `datum_ref` does today, and runs the
  admission checks ADR 019 and ADR 026 require: the `metaDoc` delegation check and the
  fixed-slot value. `childEntry` admission checks the child's Keyhive delegations
  against the child's own descriptor layout; it derives no shape column of its own
  (ADR 019, "Self-doc children").

Admission reads Keyhive as well as the roster datum, and Keyhive state can arrive after
the datum (the big_sync ↔ keyhive race in `AGENTS.md`). The existing code already
answers how that converges, and this ADR keeps it: `doc/observe.rs` merges the datum
frontier and the authority stream in one select loop — a capability change on the
observer's drawer raises `Wake::MembershipChanged` and re-runs the drawer recheck, and a
retracted capability ends the stream with `Denied`. This ADR removes the pump but keeps
the discipline: the projection admits an entry whose backing Keyhive state has not
arrived as listed-but-pending (ADR 019), authority changes recheck the observer's
drawer once per poll, and the entry flips to admitted on the next recheck after the
late Keyhive state lands. No second observer cursor, no projection-owned revision, and
nothing blocks on Keyhive.

## Reference edges are extracted by the datum-set worker

`datum_ref` today is a separate consumer of the datum-set frontier. It has per-tag
cursors (`big_cabinet_datum_ref_progress`), durable parked routes, a second doc load
per route, and its own `settled` watch. Its edges can lag the datum-set revision, so no
reader can ask for "edges as of revision r".

- `DatumSetStore::prepare` already loads the doc at the delta's heads (`DocView`) and
  defers a doc whose heads are missing by returning `None`. For changed slots whose tag
  declares references, the same `DocView` runs the gate verify, `hydrate_value` and
  `declared_references`, which is the code `datum_ref::edges_from` holds today.
- `settle` replaces the route's edges in the same write transaction that updates
  `datum_set_entries`, so edges and entries share one revision.
- Removed: datum_ref's cursors, parking, second load and `settled` watch.
  `DatumRefStore` shrinks to the edge-query surface. Both stores already share one
  SQLite database (`Indexes::boot` hands the same `sql` to both).
- **Backfill.** A build that newly declares references on a tag needs those tags
  extracted once. A `big_cabinet_datum_set_ref_tags` table records which tags have been
  extracted. At boot, before the worker runs, an unseen reference tag re-extracts from
  its `datum_set_entries` rows, using their stored `doc_heads`. This replaces what the
  per-tag cursors do today.
- The datum-set worker takes `Arc<SchemaRepo>`. The gate and hydration run in its hot
  path, but only for reference-declaring tags.

The rejected alternative keeps `datum_ref` separate and makes readers wait for its
progress to reach r. Parked routes break that wait, because a tag's cursor moves past
a parked route.

## Consequences

- Positive: one writer per index, and resume and rebuild become the same operation.
  Authorization no longer does reload-everything work on a miss.
- Positive: a datum, its reference edges and the roster facts that admit its doc are
  ordered by one cursor ([ADR 025](025-cabinet-datum-predicates-and-polling.md) builds
  observation on that).
- Cost: an op's effect is not visible to authorization until the projection settles
  it. Flows that write and then immediately use the result, such as creating a drawer
  and then exchanging an identity proof for a cap on it, must wait on the projection.
  Tests that assert projection state straight after an op change the same way.
- Cost: the datum-set worker does reference extraction work it did not do before.
