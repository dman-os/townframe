# ADR 013: A Keyhive Access Delta Stream for Derived Part Access

**Status:** Proposed

## Context

Blob data is stored in a deliberately policy-free part store in its own scope
(`repo.rs:161-168`): possession of the hash is the only authorization the store
itself knows. Remote fetchability of a part is decided by that store's
`big_sync_syncable` rows, written at `big_repo/store/sqlite/parts_cursors.rs:866-889`
and read as the fetch predicate at `big_repo/store/sqlite.rs:836-841`; today nothing writes rows for the
derived blob-inventory partition parts
(`blob_inventory_part_id_from_doc_id`, `blobs.rs:17-31`). Every peer is therefore
denied, and no keyhive-derived access reaches the blob store at all.

The one keyhive→access-row translator in the tree is the group-part worker
(`group_part_worker.rs`). It derives, per document, the agent sets of the parts
the document resides in and writes them as raw rows (`sedimentree.rs:617-670`).
It is doc-and-group scoped, it scans (its affected-set expansion walks every group
or every doc: `group_part_worker.rs:773,792,808`; `keyhive.rs:521-545`), and its
output is written into big_repo's own part store, not into another crate's.

The raw source of truth is the durable *incorporation* log. An admission row means
the event's effects are already applied to the local keyhive projection
(`store/sqlite.rs:311-327`), and its `seq` is incorporation order
(`events.rs:130-233`). The whole log is `pub(crate)` inside big_repo
(`runtime2/mod.rs:8`, `keyhive.rs:504,521`) and unreachable from daybook; the
existing revisioned-store adapter over it (`keyhive_admission.rs:30-56`) is likewise
crate-private. Retention of the raw bytes is pinned by the minimum over registered
readers (`events.rs:350-382`), of which there are four today (`events.rs:4-7`).

The consumer is the blob-pins-part machine (`pins_part_worker.rs`). It already
derives blob-inventory membership from the `BlobPin` facet delta stream, keys work
by physical branch, and writes the part store (`pins_part_worker.rs:381-440,537-660`).
It has no keyhive access today, and the worker is spawned with no knowledge of the
inventory documents (`rt.rs:297-303`).

## Terminology

- **Access subject:** a keyhive *membered* identity that carries a membership
  graph — a document or a group — addressed by its 32-byte keyhive identifier.
  An individual holds capabilities but is not itself a subject; delegation is not a
  subject either, it is an *event* whose subject is the graph it modifies.
- **Access closure:** the transitive member set of a subject with effective access
  levels, i.e. what `BigKeyhiveHandle::agents_for_membered` returns
  (`keyhive.rs:390-411`).
- **Watched subject set:** the subjects whose closures a consuming site mirrors.
- **Access delta:** one collapsed current-state transition for one watched subject,
  in the shape of `DocDelta` (`doc_delta_store.rs:53-64`) and `FacetDelta`
  (`facet_delta.rs:54-67`).
- **Retention cursor:** this consumer's row in
  `big_repo_keyhive_admission_readers`, which bounds log pruning
  (`events.rs:269-283,350-382`).

## Decision

### 1. The stream is an inert mapping over the admission source, not a new durable projection

`KeyhiveAccessRevisionStore` is `RevisionedStore<Revision = u64, Entry =
KeyhiveAccessDelta, Selector = KeyhiveAccessSelector<M>>` over
`keyhive_admission::Store` (`keyhive_admission.rs:30-56`). It owns no cursor, no
event loop, no output storage, and no revision assignment of its own: exactly the
shape of `DocDeltaRevisionStore` as built (`doc_delta_store.rs:123-135`), not the
shape ADR 008 §5 describes (see "Drift" below). It performs no content I/O; it does
read keyhive state, which the frontier-derived precedent does not.

It lives in big_repo and is exported, for one reason: it needs the crate-private
admission store and the crate-private keyhive helpers, and an exported mapping
avoids widening either. `AutomergeFrontierRevisionStore` (`lib.rs:33`) is the
precedent for exporting a revisioned store from big_repo; the difference — that
this store, unlike that adapter, must reach into the keyhive handle — is stated
rather than hidden. The mapping is exported from `big_repo` and nothing else is:
exposing the raw admission log as a public durable stream — the shape
`AutomergeFrontierRevisionStore` is intended for — is a separate future artefact
and does not change this decision.

!> this living in big_repo is acceptible. i'd wanted to provide a durable stream for keyhive changes and maybye we'd want to expose the admission log in the future the same way the afw is intended for external consumers but yeah

### 2. The selector names subjects, not documents

```text
KeyhiveAccessSelector<M> {
    watch: AccessSubjectSet,
    memory: M,          // DeltaWalkerStateRepo + DeltaWalkerSparseStateRepo
}

AccessSubjectSet {
    Watched(BTreeSet<AccessSubject>),   // this ADR's path
    All,                                // baseline expansion path, for a converting consumer
}

AccessSubject { Document(Identifier), Group(Identifier) }
```

The subject is a keyhive identifier with a kind tag, never a part id and never a
logical document id: the same mechanism already derives a part for a document, a
group, and an individual issuer alike (`group_part_worker.rs:759,786`; that call site uses the
immediate signer, which is the bug B19 records — §6 derives the subject instead), and the part-store sink for a subject is the
consumer's business, not the store's. `All` is kept because a converting consumer
(the group-part worker) mirrors a hive rather than a set. Its semantics are exact:
with `All` there is no affected test, so every admitted event's subject produces an
entry, and no closure cache is consulted. Expanding a subject into the consumer's
own affected set stays consumer-side (`group_part_worker.rs:747-830`); the store
only names subjects. `All` is expensive (a closure per event), and it is what the first
consumer — the blob-inventory permission writer — ships on; `Watched` is the optimisation
for a consumer that watches a small subject set, and it is the one that pays for the
affected test.

Selection over the raw log is necessarily a post-decode predicate: the log has no
per-subject key space to select on, unlike the frontier store where a target
becomes a part/object subscription (`doc_revision_store.rs:47-61`).

### 3. A watcher keeps only the cached closures it needs

```text
KeyhiveAccessMemory {
    agents: BTreeMap<Identifier, Access>,   // the whole recomputed closure, id -> level
}
```

That type is a **row value**, not a repository: it is what the consumer writes through the
walker-state repo named below. There is no `KeyhiveAccessMemoryRepo`, and no method anywhere
returns it — a consumer can only count with it (`member_count`), never read the closure back.

One row per watched subject, in the consumer's own walker-state namespace, through
the existing `SqliteDeltaWalkerStateRepo` (`big_sync/delta_walker_state.rs:38-48`,
the `delta_walker_key_state` sparse table; reads at `:139-192`). That is the same
repo instance that owns the walker
cursor, so memory and cursor commit in one transaction
(`delta_walker_sparse_state.rs:1-16`); `FacetSetRevisionStore`'s input state is the
precedent (`facet_set.rs:212-222,690-716`), and no new durable store is required.
A subject with no row is a subject whose closure is unknown — never a subject with
no access. Two watched subjects cost two rows; nothing else about the hive is
stored, and in particular there is no per-document or per-group mirror of "who can
access what", which is what `membered_for_agent` would require and must not be built
(`keyhive.rs:442-460`).

This row is **not** a payload cache: it is the index §6 tests against, and it cannot
be reconstructed from the sink. The part store exposes no member-read API
(`big_sync_core/part_store.rs:39-71` has `member_count` and `part_dirty_count` only),
and no sink can answer "which groups are reachable from this subject".

The row's key is the `(id -> Access)` map, not the id set: `Group::get_capability`
keeps the highest-`can` delegation per member
(`../keyhive/keyhive_core/src/principal/group.rs:337-346`) and the walk takes the max
access per id, so a re-delegation can change a member's level without changing the id
set. Comparing id sets would silently keep a stale level.

!> you know, did we evenr implement an sqlite based doc_delta store?? I imagine we'd need that here to make it crash safe? RIGHT, the spare state repo. can we use that here??

### 4. The delta entry is the resulting access state

```text
KeyhiveAccessDelta {
    subject: AccessSubject,
    agents: BTreeMap<Identifier, Access>,   // the resulting closure, verbatim
    computed_at_seq: u64,                   // admission head observed when computed
}
```

`agents` is the full resulting set, not a diff, and access levels are copied
verbatim including `Relay`: relays are *supposed* to retain blob partitions
without reading private documents (ADR 001 §4), and fetchability is the part
store's own predicate (`keyhive_core::access::Access::is_fetcher` at
`../keyhive/keyhive_core/src/access.rs:42-45`, i.e. `>= Access::Relay`, wrapped by `is_fetch_access`
at `store/sqlite.rs:836-841`). Filtering
the set to readers here would silently withhold exactly the entries that ADR 001
intends.

`computed_at_seq` is not needed for correctness (per-key ordering and supersede
already cover it) but makes the freshness rule of §7 assertable instead of
implicit; a consumer may drop an entry it is already holding for that subject once
a newer one arrives, but it must not use the field to decide that a recompute is
unnecessary. It is diagnostic, not an ordering mechanism: with one in-flight task
per subject (`tokio_keyed_scheduler.rs:89-106`) and merge-by-cursor
(`pins_part_worker.rs:658-695`), supersede already excludes stale application.
Because a closure read may lead, `computed_at_seq` does not define a total order over
snapshots and must never be used to skip a recompute of a subject — the memory row is
what decides that.

An unchanged `agents` map is not an entry. `DocDelta` already models this
(`frontier_delta` returns `None` when heads and epoch are unchanged,
`doc_delta_store.rs:322-355`) and it is what keeps this stream free of the
`set_part_members` no-op-write wart recorded in ADR 012 decision 5.

### 5. The consumer's memory is the only sparse state, and it is what makes replay a no-op

The consumer advances the `agents` row in the same transaction as the walker
progress, exactly as `DocDeltaSettlement::settle` does
(`doc_delta_store.rs:299-312`). Memory therefore leads the durable cursor and never
lags the durable effect, so a crash between the effect and the settlement replays
the entry, the reader recomputes against the memory that leads, gets an unchanged
`agents` map, and drops it — the walker settles the entry-less revision with no task
(`concurrent_delta_walker.rs:155-158`). `set_part_members` is a full replacement
(`big_sync/part_store.rs:519-524`; sqlite impl `big_sync/part_store/sqlite.rs:1476`; there are two
files named `part_store.rs`, so a bare reference here is the `big_sync` one) and is idempotent for
that reason. Note the type hop the consumer owns: the closure is keyed by keyhive `Identifier`,
while the sink takes `HashMap<PeerKey, Access>`, so the conversion (`Identifier::to_bytes()` ->
`PeerKey`) happens at write time and never inside the stream.

**Memory-less consumers.** With `AccessSubjectSet::All` the affected test is
vacuous, and a consumer whose sink already holds the last snapshot may run with no
memory row: the reader emits an entry per event subject and the settlement
transaction writes only the cursor. The `ack` path is unchanged —
`ConcurrentDeltaWalker` requires only `DeltaWalkerStateRepo`
(`concurrent_delta_walker.rs:60-78`) — and `delta_walker_progress` still bounds the
source. Two things are lost and must be stated where a consumer opts in: replay is no
longer entry-less (the replayed entry re-applies the same full replacement, which is
idempotent), and an unchanged set is re-written, which re-triggers the ADR 012
decision 5 no-op wart (`set_part_members` restamps `big_sync_syncable.changed_at`,
`big_sync/part_store/sqlite.rs:1456-1486`, visible to peers as `access_changes`).
Under a `Watched` set this shape is **not** equivalent: §6's test needs the cached
closures, so a memory-less `Watched` consumer would have to admit every event instead
of O(k) lookups.

### 6. Watched subjects must be closed over reachability

An event's subject is derived from the signed payload: a `CgkaOperation` names its
document, and a `Delegated`/`Revoked` names the graph it modifies, which is what
`Delegation::subject_id` is for. The group-part worker itself uses `delegation.issuer`
as a proxy instead; see below for why that is not safe to copy. Consequently an
event on group `G` changes the closure of every doc that
has `G` in its closure.

The affected test is therefore:

```text
subject(event) ∈ watch  ∪   ⋃ closure(subject) for subject in watch
```

tested against the ids in the cached `agents` maps of §3, so it is O(k) hash lookups
per event and needs neither `document_ids_containing_group` nor
`group_ids_containing_document`. It is exact, not an over-approximation: if `G` is not
a key of the cached `agents` map of a watched subject, no event whose subject is `G`
can change that map, and the event that *does* add `G` has the watched subject as its
own subject. Boot (§8) computes the closures first, so the invariant is established
before the first live event, and the daybook wiring adds the group to the doc
(`repo.rs:685-695`), so the group is in the closure by construction.

The event subject must be the graph keyhive dispatched the operation to —
`Delegation::subject_id()`, which walks the proof chain to its root issuer
(`../keyhive/keyhive_core/src/crypto/signed_ext.rs:28-46`; consumed at
`../keyhive/keyhive_core/src/keyhive.rs:1980` for delegations and `:2066` for
revocations, including the individual→group promotion at `:2000-2004` and the
member-map insert at `../keyhive/keyhive_core/src/principal/group.rs:734-737`). The
*immediate signer* is a different id whenever a non-root member re-delegates
(`compute_add_proof`, `group.rs:2154-2183`), which is what
`group_part_worker.rs:773,792,808` uses (`delegation.issuer`). A stream that copied
that proxy would emit entries for the wrong subject id — an id that is not a membered
graph at all.


Not every admitted event has a subject, and the two subject-less cases are classification
decisions rather than silent drops, each with a test that asserts the no-entry answer:
`StaticEvent::{PrekeysExpanded, PrekeyRotated}` change which peers can be *reached*, not which
agents are members of anything, so they cannot change a closure — a rotated principal
therefore waits for the next membership event, which is correct because the closure it needs
is unchanged by the rotation; and a `CgkaOperation` whose document id is not a keyhive
document has no closure to read at all (the hive already returns empty for such ids,
`big_repo/keyhive.rs:521-530`), so it produces no entry either.
### 7. The closure is read when the revision is read

The reader computes `agents_for_membered(subject)` at read time. Because an
admission row exists only after its effects are applied (`store/sqlite.rs:311-327`)
and admission `seq` is incorporation order (`events.rs:130-233`), the closure read
is always greater than or equal to the state implied by the revision being
reported. A closure may therefore *lead* its revision, never lag it; the collapse
in §4 makes leading harmless, since the later revisions that would publish the same
state produce no entry. This is what makes the design replay-safe without a durable
projection, and it is also why the log needs no complete replay: the log carries
*when* to look, keyhive carries *what is true*.

### 8. Boot seeds unconditionally, then registers, then opens

```text
1. h0 = admission head                                   (events.rs:234)
2. for each watched subject: closure = agents_for_membered(subject)   // state >= h0
3. write every part's members (full replacement) and one memory row per subject
4. commit the consumer's memory + walker progress at h0   // advance_from, as
                                                          // group_part_worker.rs:115-121
5. register the retention cursor at h0                    (events.rs:269-283)
6. open the reader after h0
```

The order of 1–3 is the load-bearing part: the head is read *before* the closures,
so an event admitted in the middle cannot be missed by an already-recorded closure.
Seeding is unconditional, not conditional on a durable cursor, for the same reason
the group-part worker's initial build is unconditional when its cursor is zero
(`group_part_worker.rs:62-104`), plus one more: the raw log may already be pruned
past this consumer, and a reader that starts at zero would silently skip the pruned
rows (the reader joins admissions to the byte log, `events.rs:400-427`, and pruned
rows simply do not appear). Boot never depends on log completeness.

### 9. Retention is bounded by this consumer's lag, and it is load-bearing

The pruner's watermark is `min(archive_floor, min over registered readers)`
(`events.rs:350-382`), so for every registered reader no byte row with
`seq > cursor(reader)` is deleted. This consumer *must* register: pruning under an
unregistered reader loses wake-ups, and since the payload is read-time state a lost
wake-up is not self-healing — the subject would stay stale until some later event
touched it again. Registration is not an optimisation here, it is the correctness
mechanism that keeps the wake-up signal from being dropped.

Registration is monotone (`MAX(seq, excluded.seq)`, `events.rs:269-283`), so the
retention cursor may lag the walker state and must never lead it. A crash between
the settlement commit and the retention write leaves the reader behind, which only
retains more log. Recovery, if wanted later: a gap in a page (`first seq > cursor + 1`)
or a cursor below `archived_through` is a sound "may have lost wake-ups" signal
(`events.rs:244-249,350-382`; note `archived_through` is currently `#[cfg(test)]`-gated
and must be un-gated for that use), and the fix is to re-run the §8 seed mid-run at a
fresh head rather than to replay the log. That makes this consumer a "non-normal"
reader: its payload is read-time state, so a pruned log is recoverable, unlike a
consumer that rebuilds state from the log. Not built here.

!> this is a good point but you know what, we could consider these non normal admission log readers? i.e. if they detect the admissionn log has been pruned, it should techincally be possible to catch up to it by doing full keyhive reads
!> we shall tackle it in the future I guess.

## Cost

### Stream cost for a two-subject watch

An admission page is bounded by the walker's available task budget
(`group_part_worker.rs:245-246`; daybook uses 64, `pins_part_worker.rs:454`) and the
reader stamps the page's *maximum* `seq` as its revision
(`keyhive_admission.rs:119-121`). So a page of unrelated rows costs one closure
comparison per row (one `get_many` over k keys per page, `doc_delta_store.rs:213-236`),
no tasks, and a single progress-row write per drained page
(`concurrent_delta_walker.rs:193-221`).

Entry-producing fraction (`Watched` only). Let `M` be the subject population that
admitted events name and let a watch of `k = 2` inventory documents have closures
covering `S` subjects. Under a uniform-subject model the entry-producing fraction is
at most `S/M ≈ k/N` for a hive of `N` content documents: at `N = 1000`, ~0.2%, so
~99.8% of admitted rows are acked past without an entry. The real fraction is smaller,
for two reasons that hold without any modelling assumption: prekey churn
(`PrekeysExpanded`, `PrekeyRotated`) belongs to no subject at all and is acked
immediately today (`group_part_worker.rs:800`, `385-386`), and even a row whose
subject *is* watched produces an entry only when the closure changed — a Cgka key
rotation does not. These are model bounds, not measurements; the implementation
adds a counter for rows-read / rows-touching-watch / entries-emitted and this ADR
should be updated with real ratios.

Retention floor, steady state: `min` over readers, so the added floor is this
consumer's lag in `seq`s ≈ arrival rate × (idle poll + settle), where the idle poll
is 250 ms (`keyhive_admission.rs:18`). At 100 events/s that is roughly 25–100 extra
retained rows above the status quo, because entry-less pages settle immediately and
contribute lag, not backlog.

Retention floor, stalled: the minimum is taken over readers, so a stalled consumer
pins the entire log from its cursor — pruning stops completely, for every reader,
and the retained set grows at the arrival rate for the whole outage (at 100 events/s,
~360k rows/hour). This is not a new class of failure (the four existing readers have
it, `events.rs:4-7`), and it is bounded by the *downtime* rather than by the cursor's
age: boot re-seeds from keyhive and re-registers at the head (§8), so the pin ends
at the next start. The recovery lever is a direct consequence of read-time closures:
because the log is a wake-up signal and not the payload, an operator can drop this
reader's row and let the next boot re-seed, which is not true of any consumer that
replays the log to rebuild state.

### Transitive versus direct edges

Transitive (chosen): one recompute is `agents_for_membered`
→ `transitive_members_walk`, i.e. O(V + E) over the reachable subgraph, with at most
one lock held at a time and one `HashMap` of V entries allocated
(`keyhive.rs:1076-1100`; `../keyhive/keyhive_core/src/principal/group.rs:2068-2130`).
Storage per watched subject is V entries of (32-byte id + access), ~45–70 bytes each
serialized: V = 250 agents is ~15 KB per subject, 2 subjects ~30 KB, plus the same
order per entry while it is in flight. Computation is bounded by *watch activity*,
not log volume, because §6 recomputes only for events whose subject is in a cached
closure.

Direct edges: O(out-degree) payload per event, but every consumer that needs to
answer "may this peer fetch this part" needs the closure, so the reachable subgraph
must be retained anyway — O(V + E) of state, no better than transitive — and the
closure must then be recomputed by the consumer, including `access.min(can)`
minimisation along paths (`group.rs:2100-2125`). That is the wrong thing to
re-derive: an error in the minimisation is an over-permission, and the daybook sink
is a flat principal→access table (`big_sync/part_store.rs:519-524`, backed by the
`big_sync_syncable` / part-member rows), so a graph must be
folded into a table before it can be written at all.

Freshness: transitive gets a snapshot per recompute, always ≥ its revision (§7),
self-healing on the next event that touches the subject. Direct gets a stream whose
completeness is its correctness: a missed or filtered event leaves a stale edge with
no re-query to correct it, and the failure is silent under- or over-permission.
That asymmetry, not the byte count, is the reason to prefer transitive even though
its per-event payload is larger.

What each buys a consumer: transitive buys the exact sink format, an O(k) exact
affected test, and self-healing. Direct buys a smaller entry and finer change
attribution at the cost of reimplementing a keyhive algorithm.

## What the baseline computes today

`reconcile_doc(doc, affected_group_parts, scope, local_principal, group_agents)`
(`group_part_worker.rs:621-687`):

1. It refuses to proceed unless the doc id is a valid Ed25519 point
   (`:622-624`), then asks `document_has_content` (`:626-627`; `keyhive.rs:504-507`).
   With no content there is no keyhive document node, and it returns three empty
   maps (`:643-645`) — an empty access state, not an error.
2. `agents = agents_for_membered(Identifier::from(doc_verifying_key))` (`:632-640`):
   the *transitive* member set of the document, mapped to `(PeerKey, Access)`.
   This is the doc-level union and is never written to a part row
   (`store/sqlite.rs:299-301`).
3. `part_agents`: for every group id returned by `group_ids_containing_document(doc)`
   (`:648-660`; `keyhive.rs:521-545`, which enumerates *all* keyhive groups and tests
   membership in the doc's closure), compute `part = group_part_id(group_id)`
   (`:759,786`), skip it when the worker scope excludes it (`:654-658`), record it as
   a candidate part, and set `part_agents[part] = agents_for(group_id)`, the *group's*
   closure, from the shared memo (`:696-731`). The comment at `:641-647` records why:
   a part is a one-way digest of a group id, so writing the doc-level union onto every
   containing part would hand one group's principal pull access to every other group
   holding the doc.
4. `/seds`: if the local principal's access in `agents` satisfies `is_reader()`
   (`:671-675`; `access.rs:49-51`), the global part is desired. Membership is ordinary
   membership — no flag, no special branch (ADR 012 decision 9).
5. `managed_group_parts` = the parts named by the triggering event, plus `/seds`, plus
   the desired set (`:678-681`).

The store then, per doc: computes `stale = (current ∩ managed) \ desired` and
`additions = desired \ current` (`sedimentree.rs:546-552`); for each affected part
replaces that part's rows from `part_agents` only, never from the doc union
(`:617-640`), deleting rows for a part the doc is leaving even when no agent set was
derived (`:630-636`), and leaving rows alone for a part the doc is staying in when no
agent set was derived, because those rows are not keyhive-derived (`:628-637`). A
principal that is newly present triggers a re-emit of a live transition for the parts
the doc is already live in, because a delivery-time policy filter would have denied
the earlier membership touch (`:660-690`).

What this means for the blob parts. The requirement "if you can read the inventory
doc you can read its part" is the **doc-level** closure, which the baseline computes
and discards. `part_agents` is the wrong source for it: it is per containing group,
so it is a subset of the doc closure and omits direct doc-level grants. The mirror is
therefore `agents_for_membered(inventory_doc)`, written with a full-replacement
`set_part_members` on the blob part store. This is a deliberate divergence from the
baseline's row provenance, stated here so the next reader does not "fix" it back.

`access.is_reader()` is the only access predicate the baseline applies, and it is
applied to *one* question — whether the local node should want the doc bytes in
`/seds` — not to the rows it writes. Its rows carry every level, and fetchability is
enforced later by the part store (`is_fetch_access`, `store/sqlite.rs:836-841`).

## Consumer shape

The permission stream is a **second machine in the same worker task**, driven by its
own walker and its own keyed scheduler, joined with the existing facet machine, which
is the pattern already used for the blob-pin worker's two inputs
(`pin_worker.rs:67-78`). Keys are watched subjects, never admission `seq`: keying by
`seq` (as the group-part worker does, `group_part_worker.rs:281`) would let two tasks
write the same part concurrently, and the pin worker's own merge logic already assumes
one in-flight task per entity (`pins_part_worker.rs:658-695`).

```text
select! { cancel, task completion, walker.next(budget), next deadline }
  - delta:  merge newest per subject into pending; start task
  - task:   set_part_members(part(subject), delta.agents)     // blob part store
            then one transaction: agents memory row + walker progress
            then ack(subject, cursor)                          // pins_part_worker.rs:618-645
            then note_retention(cursor)                        // may lag, MAX semantics
```

The effect and the memory live in different databases (blob store vs daybook
local-state), so delivery is at least once and the effect is idempotent by full
replacement. The retention cursor is advanced last and may lag, per §9.

Two things the wiring must add and does not have today: the watched subject set
(`core_inventory_doc_id`/`docs_inventory_doc_id`, `repo.rs:116-117,685-695`) passed at
spawn (`rt.rs:297-303`), and a reachable keyhive handle (`SharedBigRepo::keyhive`,
`lib.rs:470`; daybook already holds `big_repo`, `repo.rs:109`). The exported store
must expose exactly one new retention operation
(`note_retention(consumer_id, cursor)`), monotone, wrapping
`register_keyhive_admission_reader` (`events.rs:269-283`).

## Alternatives considered

### No store: the consumer tails the raw admission stream itself

Rejected as the *shape*, not as a possibility. It is what the group-part worker does
(`group_part_worker.rs:129-135, 747-800`), and it would work: durable resume is the
consumer's `delta_walker_progress` row, filtering is the same post-decode subject
test, and replay safety is §7 unchanged. What it costs is that the keyhive event
vocabulary, the subject derivation, the scope expansion, and the closure semantics
become consumer code in daybook, in a crate that cannot see the admission store at
all without widening `pub(crate)` items (`runtime2/mod.rs:8`). The mapping is the
part that must not be reimplemented per consumer; it is the same argument that puts
`DocDelta` in a store rather than in each facet consumer.

### A durable projection store with its own cursor and output

Rejected for the first consumer, and it is the shape ADR 008 §5 describes rather than
the shape the code implements. A store-owned projection means: its own sparse memory,
its own poll driver (per ADR 008 §5, polling is what advances it — and a projection
that only advances when a consumer polls pins retention exactly when no consumer is
running), durable output storage with its own revision assignment (the
`FacetSetRevisionStore` shape: a `SqliteKeyedFrontier` plus a producer machine,
`facet_set.rs:190-222,393-458`), and one more cursor that must be settled
entry-lessly. It buys decode-once for *N* consuming sites and independent replay of
its output. With one consumer, that is a driver, an output log, and a second retention
pin for no reader. Revisit when a second consumer needs the same subjects: at that
point the decision is between this store and letting each site pay its own decode,
and the decode is per-page, not per-entry, so the bar is genuinely a second consumer.

### Watch the containing group instead of the document

Rejected as the default. The `blob_inventories` group is already the doc's authority
(`authority.rs:106-118,143-148`, `repo.rs:685-695`) and the group part's rows are
already derived by the group-part worker, so watching the group would be cheaper and
would reuse an existing derivation. It is also wrong for the stated rule: the doc
closure is a superset of the group closure, so a direct grant on the inventory doc
would be dropped, and "if you can read the inventory doc you can read its part" would
be false in exactly the case an operator is most likely to reach for, and there is no
existing way to observe it: part access/membership is pull-only
(`big_sync_core/part_store.rs:39-71`), and the local revision stream carries object
events only (`sqlite_frontier.rs:24-25`, `rpc.rs:386-388`). A group-part watcher would
need new notification machinery to observe the very rows this worker already derives.

!> do we even have mecahisnms for watching auth events on big_sync parts?

### A full keyhive mirror of who can access what

Rejected. It is what `membered_for_agent` would build — every doc, every group,
O(all subjects × transitive closure) per pass (`keyhive.rs:442-460`) — and it is
unbounded state with no consumer that wants it. Interest-scoped sparse state only.

### Direct edges instead of transitive closures

Rejected; see Cost. Smaller entries, but the reachable subgraph is retained anyway,
the sink needs the folded table, the closure minimisation must be re-derived, and a
missed event is silently permanent.

### Periodic resampling instead of a retention reader

Rejected. Polling every watched subject's closure on a timer is correct (the payload
is read-time state) but pays O(V) per subject per tick forever, to bound a lag that a
registered cursor bounds exactly. Worth keeping as the fallback if the retention
cursor ever becomes the constraint.

### Keyhive `MembershipListener` / ephemeral notifications as the input

Rejected. ADR 008 §11 and ADR 012 decision 9: a lossy notification is not durable
consumer progress, and there is no resume position behind it.

## Consequences

### Positive

- The blob store stays policy-free and keyhive-unaware; the mirror is the only
  keyhive-derived state it carries.
- Two watched subjects cost two memory rows and two part writes; nothing about the
  rest of the hive is stored.
- An unchanged `agents` map produces no entry (`Watched` only), so the ADR 012
  decision 5 no-op-write wart is not triggered by this path.
- The affected test is O(k) per event instead of an expansion over every group or
  every document.
- Replay and crash recovery are no-ops, because the memory leads the cursor and the
  payload is read-time state.
- Boot does not depend on the log being complete, so retention pressure has a
  recovery lever without a replay.

### Costs and trade-offs

- One more admission reader pins retention; a stalled consumer stops pruning for
  every reader (steady-state cost is small, stalled cost is unbounded).
- The reader performs keyhive I/O, which the frontier-derived precedent deliberately
  does not; a large closure is paid per recompute of that subject.
- Three databases are involved, so effect + memory + cursor + retention cannot be one
  transaction and delivery is at least once.
- The mapping lives in big_repo and is generic over a daybook-owned state repo, so
  big_repo gains an exported type whose type parameter is implemented downstream.
- Watched subjects must be closed over reachability (§6); a consumer that names a
  subject but not the groups in its closure will miss group-driven changes.

## Migration

1. Compile the selector/entry/memory types and the subject derivation against the
   existing `keyhive_admission::Store`, with contract tests alongside the admission
   store's own harness (`keyhive_admission.rs:131-227`).
2. Export the store and the single `note_retention` operation from big_repo; assert
   the retention ordering rule (effect → memory+cursor → retention) with a fault
   injected between the steps.
3. Wire the watched subject set into `spawn_blob_pins_part_worker`
   (`rt.rs:297-303`) and spawn the permission machine beside the facet machine.
4. Add the counters from Cost and record the real entry-producing fraction and
   steady-state retention delta in this ADR.
5. Only then consider converting the group-part worker's row writing to the same
   derivation, which is a separate ADR: its rows are per containing group, and that
   difference is load-bearing, not incidental.

## Open questions

1. **Decided:** the watch set stays `core_inventory_doc_id`/`docs_inventory_doc_id`
   (`repo.rs:116-117,685-695`). More inventories (ADR 001 §3 sharding) will make the
   watch set configurable per partition; the selector already takes a set, so that is
   wiring, not design.
!> yeah for now. in the future, we'll have more inventories but for now, these are the two
2. **Decided:** `note_retention` takes no raw reader string. The exported store
   derives the retention reader id from the same `(namespace, consumer_id)` that owns
   the walker cursor (`big_sync/delta_walker_state.rs:65-71`), so one consumer cannot
   acquire two retention rows and two consumers cannot share one.
!> hmm, i imagine this is a spare delta walker store concern?
3. Is `computed_at_seq` worth carrying? Kept as a diagnostic per §4: it is not an
   ordering mechanism, and the `agents` map is what decides a recompute.
!> your dugment
4. Revisit only if a consumer's watch set becomes the hive itself (`All`), where
   recompute cost is O(events × (V+E)) rather than O(touching events). A
   cached-closure + incremental-edge-delta design is **not** reachable from today's
   keyhive API — the only traversal surface is the full walk
   (`../keyhive/keyhive_core/src/principal/group.rs:277`, `document.rs:133`), and
   big_repo's short-locked copy (`keyhive.rs:1069-1130`) would have to be extended in
   lockstep. Closure size alone is not a trigger; watch activity is.
!> per-subject? do you mean getting the full transitive walk? doing incremental walks would require modifying keyihve graph code no?
5. **Decided:** two machines, one per source, as `pin_worker.rs:67-78` (facet machine
   budget 64 at `:615,756`; plug-events machine budget 1 at `:1059`) — note that
   precedent does not split one budget. The permission machine does not take the pin
   worker's reconcile lock: it writes access rows (`big_sync_syncable`), the facet
   machine writes object/member rows, and no transaction spans both. Give the
   permission machine its own (smaller) budget.
!> you mean in the pins_part_worker? It should be semantically identical yeah? I'm guessing the other one is splitting it's budget between the two. your judgment
6. **Decided:** a consumer may ack without a sparse memory row ONLY on the `All`
   path, where the affected test is vacuous. On the `Watched` path the memory row is
   required — it is §6's index, not a payload cache, and no sink can answer the
   reachability question (`big_sync_core/part_store.rs:39-71`). The group-part
   worker's present behaviour is exactly the `All` + memory-less shape (no sparse row,
   `group_part_worker.rs:108-121`; unconditional row replacement,
   `sedimentree.rs:617-640`); converting that worker is deliberately not part of this
   ADR.
!> this is a good question. one thing to note is that the group part worker need not mantain a separate diff because it's parts already carries the last snapshot so that sparse state is redeundant. this might also be the case for some of our consumer and even pin_part_worker. i.e. it might be preferrable to enable the possiblity to ack a delta without creating a sparse entry to track last state. if we do that, we could have the group part worker also take this on but let's not make that change yet.
7. **Decided:** `All` is built now. Semantics: no affected test — every admitted
   event's subject produces an entry — with no closure cache, because there is nothing
   to test membership against. Turning a subject event into the affected document/part
   set stays consumer-side (`group_part_worker.rs:747-830`). This is the only shape in
   which memory-optional acking is sound (see §4/§5).
!> sure, build it now, it's really a single if branch somewher I bet

8. **Decided (reading of an existing contract):** the read limits bound *source entries
   read*, not entries emitted. `RevisionReadLimits` is documented as "at most `limit`
   source entries" (`concurrent_delta_walker.rs:120-124`), which is unambiguous on `All`
   (one subject per entry) but not on `Watched`, where one event can change the closure of
   several watched subjects and therefore emits several entries from one source entry. The
   `Watched` lane codes against the documented reading; it does not get to redefine the limit
   as an output bound.
## Drift (recorded here, not amended elsewhere)

ADR 008 §5 describes a derived revisioned store as owning sparse projection state,
durable output, and cursor advancement on poll. That is what `FacetSetRevisionStore`
does (`facet_set.rs:190-222,393-458`); it is not what `DocDeltaRevisionStore` does,
which owns no state and takes the consuming site's memory through its selector
(`doc_delta_store.rs:109-135`). ADR 008 §5 also lists source-group membership changes
as part of the physical store's reports, which `AutomergeFrontierEvent` does not carry
(`doc_revision_store.rs:23-40`). This ADR follows the code.
