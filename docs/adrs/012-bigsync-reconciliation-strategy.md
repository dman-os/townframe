# ADR 012: BigSync Reconciliation, Keys, Authorization, and Delivery

**Status:** Proposed.

**Supersedes (by topic):** the abandoned Willow collection-sync exploration
(`docs/adrs/010-willow-collection-sync.md`, orphaned on the side branch at commits
`skuzkrotxkux` / `vnlrzzmswlsq`, not reachable from the current working copy). The
Willow work is not carried forward as a data model, but several of its results are
absorbed here: keys are byte strings with the ordering as a property of the key,
authorization must narrow reconciliation to a set both sides agree on,
reconciliation summaries must be maintained per *agreed set* rather than per peer,
and an event stream should be pulled in bounded pages rather than pushed without a
window.

## Context

BigSync synchronizes *parts*: a part is a set of members, and an object records
which parts it resides in.

- `big_sync_meta` holds a single `global_cursor` per scope; every membership
  transition takes the next value and stamps `big_sync_members.txid` (indexed by
  `(part_ref, txid)` and `(obj_ref, txid)`).
- `big_sync_parts` holds a **per-part `latest_cursor`**, and `big_sync_peer_cursors`
  holds a cursor per `(peer, part)`.
- Two strategies exist, selected by `DecidePeerStrategyTask`:
  - **cursor replay**: read member rows with `txid > C` for a part and stream the
    events; and
  - **bucket tree**: materialized per-range fingerprints over a fixed arity-16,
    4-level trie indexed by `obj_id[0..2]`, with `changed_at` per bucket,
    `live_fp`/`dead_fp` modular sums, `live_count`, and leaf pages carrying a
    per-object fingerprint. `big_sync_buckets` is already keyed by `part_ref`.
- Authorization is materialized per principal *and per object*:
  `big_sync_syncable(scope_id, obj_ref, principal_id, access_level)`, consulted by
  `event_permitted` / `is_event_permitted` for delivery.

### BigSync is a layer over a KV store for parts and permissions

Object-to-part assignment and object payload storage are **causally unrelated**.
`runtime2/group_part_worker.rs` derives part membership from Keyhive group
admissions through its own reader cursor
(`KEYHIVE_ADMISSION_READER_GROUP_PART`) and calls `ensure_part`, while the payload
for the same object is stored by the storage path. Neither orders against the
other; they race, and either may land first.

The two are deliberately kept in one event stream anyway, so that events arrive
with their payloads in a single pass. Divorcing payload membership from part
membership into separate structures would undo that, and is not planned. The
consequence to keep in mind throughout this document: **a member row's existence
and an object's payload availability are independent facts**, and code in the
synchronization path may observe one without the other.

BigSync itself is also **independent of Keyhive**. It defines the *shape* of
authorization data and the mechanics of reconciliation; it never decides who a
principal is. Embedders materialize that data. In big_repo the source of truth is
Keyhive group membership.

### Six structural problems

**1. The reconciled set is per peer, so the tree cannot prune.** Peer `p`'s
visible set is an arbitrary predicate `{obj : syncable(obj, p)}`, while the tree's
fingerprints cover a part's whole membership. Every bucket containing at least one
invisible member mismatches, every ancestor of such a bucket mismatches, and
nothing prunes: the walk degenerates to enumerating the entire part and then
rejecting per object, with `obj_payload()` plus `obj_parts()` lookups per entry.
A reconciliation summary is only meaningful against a set *both sides agree on* —
this is the property the tree is built on, and the code says as much: the bucket
strategy is gated to opt-in embedders (`SyncMode::CursorOnly` by default), and the
gate's comment attributes the four-node stress hang to the bucket path, while the
FIXME beneath it states the real cause — *"the real issue with bucket is not a
deadlock, it just doesn't deal with filtered sets well enough"*.

**2. Dirt is measured as global-time distance, not as relevant change count.**
`decide_peer_strat.rs` compares a part's `latest_cursor` against the peer's stored
cursor for that part. Both are per-part *watermarks expressed on the global
counter's scale*, so `abs_diff` answers "how far apart in global time are these
two marks", not "how many changes are relevant to this peer". A part that sits
idle from global tick 100 to tick 9000 and then takes a single edit yields a diff
of 8900 from one relevant change, and a relay whose scope is busy forces peers
into the expensive path. The same blind spot prevents per-part delta reads:
`big_sync_syncable` has no cursor column, so a grant added later makes an old
object newly relevant with no way to express "relevant since C".

**3. The machinery is asymmetric.** `set_peer` registers peer and parts,
`DecidePeerStrategyTask` requests the peer's summary and picks the strategy,
`BucketMachine` drives the level walk and the leaf pages, and `sub_parts` carries
a single `lower_bound` shared by all targets. One side is the driver, the other is
a pure responder; the driver chooses the level and the bounds. Set reconciliation
algorithms are *symmetric* — either side may encode, either side may terminate,
and completion depends on both sides' state (RIBLT in particular completes when
`a₀ ⊕ b₀` decodes, regardless of which side sent symbols). Nothing about that fits
a driver/responder split, and neither will the next algorithm we add.

**4. Event delivery is an unbounded push stream.** `PeerReplayTask` calls
`sub_parts` and then forwards every event into the machine's ingress channel for
as long as the stream lives. There is no paging and no protocol-level window: the
response length is the peer's history, the task's lifetime is unbounded, a
reconfiguration replaces the whole target set with a new barrier, and flow control
is a shared fixed-size local channel rather than the subscriber asking for less. A
slow machine stalls the whole replay task. Contrast `LeafBucketsTask`, which is a
discrete request/response task that completes and is re-driven by the machine.

**5. Identity is coupled to distribution.** `ObjId` is the document id as 32
bytes, and the trie index is literally the first two bytes of it. Key layout
therefore depends on ids being opaque and uniformly random; ids carry an
artificial 32-byte limit; SQLite keys are fixed-width blobs; and none of the key
space carries meaning that a range query or a range reconciliation could exploit.
Note that the ids are *meant* to be bytes already — `runtime2/group_part_worker.rs`
constructs `DocumentId::new(*operation.payload().doc_id().as_bytes())` from bytes.

**6. Delivered events disclose part keys.** `ObjChanged` carries
`part_ids: Vec<PartId>` and the old `Added`/`Removed` named a part outright, so a peer
learns the membership of parts it has no access to. There is no denial here today
because there is no part-scoped authorization to be denied: the per-object row
authorizes an event that names every part the object belongs to. Once
authorization is part-scoped, this becomes a disclosure rather than an oversight.

**Cost ordering.** Our primary targets are mobile devices: network dominates,
then memory, then CPU. Relays and servers invert this (CPU, memory, network). The
design therefore spends CPU — and then memory — to save bytes and round trips. A
round trip is a network cost (latency, radio wake-up), not a free control message.

## Terminology

- **Key** — the byte string that identifies something. We say *key* rather than
  *id*: `ObjKey`, `PartKey`, `PeerKey`. A key is unique by construction; any
  distribution requirement over it is served by a separate, chosen hash.
- **Part** — the unit of membership and of authorization. A part holds members; an
  object records the parts it resides in.
- **Object lane / object part key** (`o:{object_key}`) — the reserved key *name* an object
  subscription is expressed under, derived from the object's key. A name, not a storage
  class: nothing stores, enumerates or interprets it (decision 3). Formerly called a
  "virtual part".
- **`/seds`** — the reserved part key for the locally saved sedimentrees: the
  owner's own list. Not a special case in any mechanism, only in its meaning.
- **View** — the set a reconciliation runs over: a part. *Not* a peer.
- **Part view** — a view whose membership predicate is "members of part P", so
  every authorized member of P reconciles the same set.
- **Cohort** — the distinct part set a peer holds. Peers whose accessible sets
  coincide can share a materialized difference structure; this is a *cache*, not a
  universal property.
- **Recipient filtering** — computing the parts an event may name for its
  recipient, and omitting the event if none remain.
- **Dirty count, `δ_part`** — the number of part-relevant changes since a peer's
  cursor for that part (member changes in the part, plus access changes).
- **View digest** — a compact function of a view's summaries, exchanged so a pair
  that has never synced can tell "identical", "close", or "far apart" without a
  walk.
- **Enumeration** — stream the view's metadata and let the receiver diff locally.
- **Cursor replay** — read the change log above a cursor.
- **Range fingerprints / bucket tree** — a maintained aggregate per range, used to
  skip ranges both sides already agree on.
- **RIBLT** — rateless IBLT set reconciliation: an unbounded stream of coded
  symbols over the difference, peeled by the receiver. No parameters, no
  difference-size estimate, receiver-decided completion.
- **RBSR** — range-based set reconciliation: recursive per-range fingerprint
  exchange over a chosen total order. Not hash bucketing.
- **Long-poll page** — a bounded request for the next events of a part from a
  cursor; the server holds the request while there is nothing to send, and the
  client re-issues. Paging is the flow control.
- **Band** — which of the mechanisms above a given view runs in.

## Decisions

### 1. Keys are byte strings; distribution is a derived hash

`ObjKey`, `PartKey` and `PeerKey` become variable-length byte strings rather than
`[u8; 32]`. Identity is then unique by construction (document key, part key,
principal key) instead of probabilistically unique, and the artificial 32-byte
limit disappears along with its collision surface. The type rename rides along
with this change so the names are touched once.

Any *distribution* requirement is served by an explicitly chosen hash of the key,
at the point where distribution is needed. A range index or a bucket index is no
longer inherited from the first bytes of a random key; the hash width becomes a
balance/precision knob, and a hash collision degrades balance or precision rather
than identity.

The ordering of the key space is a property of the key, not of a hash:
lexicographic order on byte strings is available for prefix queries and for range
reconciliation, while a uniform layout is obtained by hashing where a uniform
layout is wanted. Both are allowed; neither is implied by the other.

Key structure also becomes usable, and we use it deliberately: reserved key spaces
(`o:…`, `/seds`) make derivable and reserved parts expressible without a `kind`
column or a lookup.

Keys are path-shaped, and a key's value may itself be a path. Object keys carry the
`o:` scheme so that the boundary between scheme and value is unambiguous: for a value
like `/object/path`, a naive `/o/` prefix produces `/o//object/path` (a doubled slash
because the value already begins with `/`), and a bare `/o` prefix leaves the boundary
unreadable. Reserved part keys are themselves plain paths, so the part key's *bytes* are
`/seds`. That is a statement about the bytes and the bytes only: the key's text form is
multibase base58btc for every key (`z` + the bytes), with no specialization by key shape,
so nothing can be mistaken for raw text and `Display`/`FromStr` are exact inverses.
`/seds` remains a *label* — it is how the docs, the migration comments and this ADR name
the key — not its text form.

**As built.** The renames have landed — `ObjKey` / `PartKey` / `PeerKey` / `ByteKey`
across 89 files — together with `GLOBAL_PART_ID` becoming `seds_part_id()` (it names the
`const` cannot hold a key once keys stop being const-constructible. The representation is
`Arc<[u8]>`, and it was chosen on measurement rather than taste: 16 bytes per key with an
O(1) clone that is an atomic increment, against 24 for `Vec<u8>`, 16 for `Box<[u8]>`, 32
for `bytes::Bytes` (4 words, always allocated), 32 for the `[u8; 32]` this replaces, and 48
for `SmallVec<[u8; 32]>` — 32 bytes inline *plus* a length and a discriminant, i.e. larger
than the fixed-width key it was meant to un-bloat. The one shape that beats both is a
hand-rolled `Inline { len: u8, buf: [u8; 32] } | Heap(Arc<[u8]>)` at ~40 bytes, which keeps
the common case inline and allocation-free; it is recorded here as the option to reach for
if a measured need appears, not built now. `Copy` was never on the table once keys gained
storage, so `SubscriptionTarget` and `ReplayRoute` lost it. Nothing at the persistence or
wire layer blocked the change: key columns were already unconstrained `BLOB`s, binary serde
already used `serialize_bytes`, and `sqlx prepare --check` confirmed zero metadata changes.
The pass itself was the sized ~800 move-and-reuse sites workspace-wide, dominated by `E0382`
value reuse over the hottest names (`part_id`, `peer_id`, `peer`, `obj_id`, `obj`), and it
had to carry two things: a global rename excludes same-named foreign types
(`automerge::ObjId`, `subduction_core`'s `PeerId`, and `daybook_core::sync::PeerId`, an
`Arc<str>` alias whose fields change type if renamed), and the text encoding is display text
when the bytes are printable UTF-8, else multibase base58, with human-readable serde staying
multibase so no persisted format moves.

**The concrete site this lands on, and one consequence to face.** `BuckId::from_obj_key` derives its
index from the key's first two bytes — `u16::from_be_bytes([obj_key[0], obj_key[1]])` — at arity 16
with four levels, so a level-4 bucket *is* the key's first four hex digits and the tree is a prefix
trie over key space. That only works because today's keys are random digests, where the key's own
bytes happen to supply a uniform layout. Textual keys break it immediately and not subtly: `o:` is
exactly two bytes, so every object key would land in the single level-4 bucket `0x6f3a` and the tree
would degenerate at its top level. The index therefore has to come from a hash of the key. The
level/truncation hierarchy survives that, because truncation then takes the top bits of the hash and a
parent still contains its children. What does not survive is the *correspondence* between bucket order
and key order: buckets still form a complete partition, but no longer in key order, so anything that
relies on the two agreeing changes meaning. `BuckId::increment`, sibling iteration,
`next_page_offset`, and the starting-`working_level` choice all need auditing as part of this pass, and
they are adjacent to the deferred question of whether the range structure keeps its fixed arity and

**What the audit found was actually load-bearing, and what had to change.** Of the four
sites named above, `increment` needed no change: it counts pages *at one level*, requests
are level-scoped (`get_changed_buckets` matches on `offset.level()`), and an increment past
a level's last index is only ever an exclusive lower bound in bucket order — the store
answers an empty page and `filter_buckets` reads that as `Done`, which *is* what "nothing
more at this level" looks like. The offsets that do change level come from a dive
(`Relist(dirty.to_level(level + 1))`), which the machine's
`next_page_offset.level() > working_level` check catches. Sibling iteration and the starting
`working_level` are unaffected: `working_level` stays size-derived, and a bucket is a
complete partition of the key space under either index.

The dependency is *leaf enumeration*. `obj_id_bounds_for_bucket` turns a bucket id into a
range of object keys, and that key range is how all three stores find a bucket's members —
the memory store's `bucket_items_for_path`, `big_sync`'s sqlite store, and the host store's
sqlite implementation (which carries a duplicate of the helper). That range exists only
while the bucket index *is* the key's own leading bytes. Under a hash index nothing may
derive a bucket's members from key order, so membership has to be stored: the object's
deepest bucket index becomes a column on `big_sync_objs`, one per object because the index
is a pure function of the object key and therefore needs no per-membership fan-out; the leaf
predicate becomes a range over that index while the page cursor stays an object key ordered
by `obj_id`, so no wire format moves; and the duplicate helper disappears. The
`big_sync_buckets` aggregate rows are keyed by the same index and are derived state, so any
store that predates this change holds aggregates under bucket ids no new request will name —
a rebuild rather than a compatibility path, and only observable once a part uses the bucket
band at all.
level.

Content addressing stays separate: payload digests remain real hashes and are
never used as identity.

### 2. The authorization boundary is the part; delivery is recipient-filtered

Access is granted at part granularity. The stored rows are therefore per
`(scope, part, principal)` with an access level and a change stamp, not per
`(scope, object, principal)`:

```sql
CREATE TABLE big_sync_syncable (
      scope_id INTEGER NOT NULL REFERENCES big_sync_scopes(scope_id)
    , part_ref INTEGER NOT NULL REFERENCES big_sync_parts(part_ref)
    , principal_id BLOB NOT NULL
    , access_level INTEGER NOT NULL
    , changed_at INTEGER NOT NULL
    , PRIMARY KEY(scope_id, part_ref, principal_id)
) STRICT;
```

An object is syncable to a peer **iff at least one part it resides in is
accessible to that peer**. Syncability is derived, never stored: the existential
over containing parts is a join, and the set both sides reconcile over is "the
members of a part", which both sides already agree on because the part's
membership *is* the reconciled object.

Nothing in big_sync decides who the principals are. big_sync is independent of
Keyhive: the embedder computes and materializes access rows, and big_sync defines
only their shape. In big_repo the source of truth is Keyhive group membership,
queried per group, which requires the group key to be retained at derivation time —
a part's key is a one-way digest of its group key
(`digest(b"townframe/big-repo/group-part/sedimentree/v1" ++ group_key)`), so that
handle cannot be recovered afterwards. Parts with no group provenance therefore
have no derivable principal set; they are not authorization units.

**Delivery filters part keys rather than testing a predicate.** For each outbound
event, compute its containing-part set — from the event's own part keys when that
list is non-empty, otherwise from the object's membership — filter that set to the
parts the recipient may read, and omit the event entirely if nothing remains.
Deliver with the filtered set. Authorization and non-disclosure are therefore the
same operation: permitted iff the filtered set is non-empty.

This is what closes problem 6. A `Changed` event for an object in parts `P` and `Q`
delivered to a peer that may read only `Q` names `Q` alone, never `P`; an
an `Added`/`Removed` naming a part the recipient cannot read was not delivered at all.
A peer's own requests — the part keys it names in a subscription, the keys echoed
back in `ListPartsError::UnkownParts` — are its own and are unaffected.

An empty part list on an event is treated as *unreliable* rather than as "no
parts": the in-memory store can emit `Changed` with an empty list, so the
membership is resolved and then filtered.

Local subscriptions remain unfiltered (`principal == None` permits), because local
reads cross no trust boundary.

An object that resides in no part has an empty containing set, so its events filter to
empty and are **not remotely deliverable**. This is fail-closed and deliberate: the
alternatives were an unauthenticated lane (any peer knowing an object key could pull it) or
per-object grant rows, and decision 3's revision declined both. Local subscriptions are
unaffected, and remote single-object delivery is the containing-part existential above —
not a per-object grant.

A directly granted object outside any part is not served: there is no per-object grant row, so it
stays fail-closed (decision 3's revision).

**Revocation is not an event.** There are no authorization events, so there are no
revocation events either. Because delivery is paged (decision 9), a page request
for a part the requester may not read answers *unknown part* or *unauthorized
part* and the page is empty. A revocation therefore needs no new message type: the
next page request discloses it. Nothing needs to distinguish "revoked" from "never
had it".

**As built.** The object-granular notice channel this replaces is gone: the subscription bus
holds no `revoked_fetch` set, `arm_revocation_notices` and `take_revocation_notice` are
deleted, and a filtered event is dropped whether or not the recipient just lost access. The
removal filter's exemption went with it: an event whose parts all fail is omitted even when it
is a `Removed`, because a membership removal is expressed as membership, not as a notice. The
tests pin the whole rule from both sides: a revoked peer and a never-authorized peer receive
neither the advance nor a payload-free hint nor a replayed removal, while a peer that keeps
access still receives the removal.


**Access is a property of the part, not of an interval.** A page is a list operation over one
part, and the only authorization question it asks is whether the caller may read that part *now*:
no event carries an access epoch, and nothing is protected according to when a grant was made. A
reader that keeps access can therefore read the whole retained history of a part back to its
cursor, and a reader that regains access reads exactly what its cursor still has behind it — the
bound is retention, not authorization. One consequence is worth stating because it is easy to get
backwards: the filter is evaluated at page time against current access, so no ordering between an
access change and a membership change is part of the contract. A reader either sees the membership
removal or is refused the part it names, and both are correct.
### 3. Object subscriptions are object parts under a reserved `key` space

Object subscriptions exist today as a separate lane. The reason they were separate
is mechanical, and now removable:

- `SubscriptionTarget::Part { part_id, cursor }` carries a cursor while
  `SubscriptionTarget::Object { obj_id }` carries none, because cursors are
  part-keyed (`big_sync_peer_cursors.part_ref REFERENCES big_sync_parts`);
- `PeerReplayTask` computes its one `lower_bound` by *discarding* object targets;
- object-subscription membership lives in the partless lane (`part_ref = 0`,
  `big_sync_members.maybe_part_ref`), which the store's own tests call
  "zero-real-part object replay";
- part subscriptions are authorization-filtered and object subscriptions are not.

**Key space.** Object parts live under a reserved prefix, `o:{object_key}`. The
key is therefore *derived from the object key*: anyone holding the object key can
compute it, so it discloses nothing beyond the object key itself, and it cannot
collide with an unrelated part named with arbitrary bytes. Nothing branches on the
prefix: it is a reserved *name*, not a storage class, so a part whose arbitrary key
happens to begin `o:` is an ordinary part like any other — the reserved scheme is
collision-free in behaviour, not merely in intent.

**Membership is the object's own rows in real parts.** There is no part whose single
member is "the object": an object lane reads the object's rows in the parts that
actually contain it. Nothing is materialized for an object subscription and nothing is
enumerated for it — the reserved key is a *name*, not a storage class.

**Authorization is the containing-part existential.** An object subscription is
authorized exactly as decision 2 authorizes an object event: the object is syncable to
a peer iff at least one part it resides in is accessible to that peer, resolved from
membership. No derived part takes part in that decision, so there is no object-granular
row to materialize, inherit, collect or revoke — and **access stays purely additive:
there are no deny rows.** Revocation is expressed by membership (remove the object from
parts) or by part access, never by a negative row; a per-object deny remains a new
decision, not an inference.

**An object in no part is not remotely deliverable.** Its containing set is empty, so its
events filter to empty (decision 2) and an object page is refused rather than answered
empty. Fail-closed and deliberate: the alternatives were an unauthenticated lane (any
peer holding the key could pull it) or a per-object grant mechanism. Local subscriptions
are unaffected.

**The object lane carries content, not membership.** A membership transition — removal
included — is a part-lane fact: delivered to peers that may read that part, never
projected onto the object lane. In particular the object lane must not synthesize a
payload-less `Changed` for a deletion (decision 9): a `Changed` with no part ids means
*resolve the membership*, not "the object left a part", and conflating the two spends the
object lane's own cursor bookkeeping on a fact it cannot name.

**Machinery must not know the distinction, and after this revision it cannot**: an object
subscription reaches the same store surfaces as a part subscription (`subscribe` on
`SubscriptionTarget::Object`, `replay_page` on `ReadTarget::Object`) and resolves content
through the same frontier rows, with no part to name. The difference inventory is
therefore answered rather than deferred, and the *only* asymmetry left is that an object
route carries its cursor in the target rather than in `big_sync_peer_cursors` — the
part-keyed cursor table (decision 1) cannot key it.

**As built, and the revision.** The key is the literal reserved-prefix key of decision 1:
`o:` followed by the object key's own bytes, so `o:/object/path` and `o:z…` each read as
what they are (step 7 replaced the earlier domain-separated digest). What first shipped
went past the key: a remote object subscription *materialized* a part row plus the
object's single live membership row, and an event naming `o:{O}` resolved through
candidate inheritance because a derived part holds no access row and would otherwise
filter to empty. Both are removed. The materialization wrote state on a read — in sqlite
the membership row *is* the keyed frontier, so the write was observable to other
subscribers as a touch at the current revision — it put a derived part into `obj_parts()`
where the frontier reconciler then removed it as "not desired", producing a removal for a
part nobody had asked about, and it left the object lane synthesizing a payload-less
`Changed` for that deletion. **Case 10 below is dropped with it**: a direct single-object
share to a principal with no containing part had no production writer, and every
mechanism above existed to serve it, so the mechanism is deleted and a future per-object
grant is a new decision. `ObjKey::object_part_key` and `PartKey::object_key` go with it:
after the revision nothing derives or interprets the reserved key, and a part whose
arbitrary key happens to begin `o:` is simply an ordinary part.

### 4. Keep the bucket tree; the fix is the boundary, not the tree

The tree is the only structure here that maintains an *aggregate* incrementally:
`O(1)` per transition per range, memory sublinear in the part, and a `changed_at`
filter that can skip ranges untouched since a cursor. Under a mobile-first
ordering that is cheap CPU and cheap RAM, and it is the cheapest way to serve many
peers from one maintained structure.

Critically, `big_sync_buckets` is **already keyed by `part_ref`**, so its scope
already matches the authorization boundary of decision 2. There is no re-scoping
work: once the per-object filter is gone, the tree's ranges describe sets both
sides agree on and pruning works. Three corrections remain:

1. **Compare counts with fingerprints.** Pruning tests
   `local_summary.fp == buck.fp`, where `fp` is already a pair
   `(live_fp, dead_fp)`; `len` and `live_count` are materialized and transmitted
   (`dead_count` is `len - live_count`). Bucket fingerprints are modular sums
   under constant seeds — they must be stable because they are materialized — so a
   sum alone is cancellable by a crafted pair of objects. Comparing counts makes
   cancellation require count-preserving collisions and also catches
   summary-maintenance defects. This needs no protocol change: every field is
   already on the wire.
2. **The level estimate stays size-derived; the dirty count chooses only the band.**
   `calc_working_level` bounds what a leaf page can return — objects per bucket at
   `member_count / ARITY^level <= ACTIVE_SYNC_JOB_TARGET` — which is not the quantity the dirty
   count measures. The dirty count totals *relevant changes* (decision 5) and is the band
   input: bucket-versus-cursor is decided first, and a part that stays on the cursor band
   never computes a level at all. Substituting one for the other would let a large part
   with small dirt start *shallower* than its width bound and hand a leaf page many
   unchanged objects, so the size-derived level stands as the width bound and the dirty
   count does not move it. An earlier draft of this decision said the count "should choose
   both the band and the starting level"; the code disproved it (`decide_peer_strat.rs`
   decides the band, then calls `calc_working_level(member_count, deepest_bucket_level)`).
3. **Collapse the round-trip chain.** The walk was requested level by level, so a
   depth-4 dive cost a chain of round trips. A request carries a range and returns the
   summaries beneath it, reducing the chain to a small constant number of exchanges.

   **As built.** The request carries `to_level`, and a page contains the changed buckets at
   every level from the cursor through it, in bucket order. A level-*N* walk is one scan,
   and the client's per-level dive is gone: a dirty bucket *above* the working level is
   answered by advancing the cursor, because its descendants come later in the same scan.
   The narrowing therefore comes from `since` rather than from the dive — a change stamps
   every ancestor's `changed_at`, so the peer's cursor already restricts a page to the
   ranges that actually differ, and the common mostly-caught-up case is a single exchange.

The tree is, properly described, RBSR with materialized per-range fingerprints, a
fixed arity, and a since-filter. The arity and the level are implementation
details of the range structure, not protocol. The `bucket.rs` FIXME saying the
machine "should be per peer like `CursorSyncMachine`" is right that the current
per-part framing is wrong and wrong about the target: the unit is the part or
view, and per-peer structure is what we are removing.

**Deferred, and the largest remaining cost: leafing.** `filter_objects` is where the
sync path spends real CPU, and its cost is per object, not per bucket. Per remote entry
it reads and parses the local payload (`big_sync_objs` keeps `payload_json` and has no
digest column), fingerprints the *parsed* value — deliberately, because `Value` hashing
is insensitive to key order and formatting, which is what lets two peers with different
serializers agree — and in the matching case issues a second query for membership, since
the fingerprint covers `(tag, obj_id, payload)` but not which parts the object is in. A
leaf request admits up to `LEAF_BUCKET_LIMIT_HINT` objects, so one call can cost on the
order of a thousand of those, and the only fast path is an empty local bucket.

Three routes, none taken yet:

- **Batch the page.** One IN-list query for payloads and one for membership, instead of
  two awaits per object. Local, no protocol change; it removes the query storm but still
  parses and hashes.
- **Compare digests rather than payloads.** Maintain a canonical payload digest at write
  time, carry it per entry, and resolve a page with one indexed query: no payload read,
  no parse, no hash on the sync path. This is the only route that helps CPU, memory and
  network together, and it costs a schema column plus a wire field. It also forces a
  decision about the leaf path's per-request random seed, whose anti-precomputation
  property the bucket path has already traded away for stability plus counts. Payload
  digests remain real hashes and never identity, so decision 1 is undisturbed.
- **Dive deeper rather than leaf wider.** The receiver pays for whole pages however
  little changed; a deeper working level shrinks the page toward the actual difference.
  It trades list round trips for per-object work, so it sits behind the same
  instrumentation as correction 2 — which already reaches it, since the working level
  fixes `bucket_width` and therefore the terminal page size.

Batching the *local* summary reads in `filter_buckets` is rejected as separate work: that
loop reads the local store, one indexed select per bucket with no round trip, over a
request already bounded by the listing limit hint. There is no N+1 to amortize there, so
the batching instinct belongs to correction 3 and to the leaf page above.

**Measured: where the tree pays.** Work, not time, because the same 100k case measured 72s
under concurrent load and 25s alone. A cold seed moves comparable work either way (1000
against 1000 object syncs for 1000 objects; 558ms against 653ms), which is decision 7's
point that cursor replay is enumeration for a cold peer. Sparse dirt is where the tree earns
its place: a 20000-object part with 3 changed moved 3 object syncs against 20000, and 711ms
against 3440ms. Note the granularity floor: below `ACTIVE_SYNC_JOB_TARGET * ARITY` (16384)
objects `calc_working_level` returns 0 and the part is a single root bucket, so there are no
ranges to prune and the gain comes only from per-object fingerprint comparison in
`filter_objects`. At 20000 objects the level is 1 and 276 summaries reduce to two leaf
requests covering 230 entries.

### 5. Dirt is counted per part, and access changes are stamped

A peer's distance from a part is a *count of relevant changes*, not a difference
of global counter values. Two sources constitute relevance:

- member changes inside the part (`members.txid > C` for that part), and
- access changes — grants and revocations — which today are invisible to any delta
  because `big_sync_syncable` has no cursor column.

Both are count queries over indexed columns:

```sql
SELECT COUNT(*)
  FROM big_sync_members
 WHERE maybe_part_ref = ?
   AND txid > ?
```

```sql
SELECT COUNT(*)
  FROM big_sync_syncable
 WHERE part_ref = ?
   AND changed_at > ?
```

plus the principal in question, which decision 2's `changed_at` column makes
expressible. A peer that has caught up advances past irrelevant churn without
re-reading it, which is what makes the cursor fast path reachable for a peer that
syncs rarely and for a relay whose scope is busy.

Note explicitly: `big_sync_parts.latest_cursor` must **not** be used as this
count. It is a watermark on the global scale, which is exactly the conflation
described in problem 2. It remains useful for exactly what it is — a watermark for
since-filtering.

The count is also only coherent on the side that owns the rows being counted. Member
`txid`s and access `changed_at` stamps are allocated from the *local* cursor, while the
cursor a peer stores for us is a position in *our* stream. A puller that counted its own
`big_sync_members` rows against the cursor it holds for the peer would be comparing two
independent scales and would read approximately zero — quietly routing a large gap down
the Cursor band and making the bucket path unreachable exactly when it is wanted. The
count is therefore computed by the announcing side, against the cursor the asker
advertises, and published in the descriptor of decision 6; it is not a local read. The
primitive that performs the count exists, but decision 6's exchange does not yet, so no
call site is wired to it.

Because member revisions and access revisions share one counter, every stamped write —
including a grant — consumes a value from it. Anything that asserts an absolute cursor
position after an access write is therefore asserting how many writes happened, not a
property; that mistake has now been made twice, once when the stamp index landed and once
when the memory store was brought to parity with sqlite. One consequence is recorded as a
follow-up rather than fixed here: `set_part_members` stamps and rewrites its rows even
when the requested agent set is identical to the stored one, so a redundant write makes
the part read as access-dirty to every peer. Relevance argues that such a write should be
a no-op consuming nothing; that is not implemented yet.

### 6. Strategy selection is symmetric, deterministic, and computed from exchanged descriptors

The choice of band and of who drives is a pure function of exchanged state, not a
privilege of whichever side happened to call `set_peer`. Both peers publish a
descriptor — part key, dirty count, view digest, summary roots — and both compute
the same function over the same inputs, so choosing a band costs no negotiation
round trip. Roles (who encodes, who serves a page, who terminates) are per
direction and per strategy, not per connection, and the transports must be able to
express:

- paginated request/response over summaries (range fingerprints),
- a page the client can stop asking for (cursor replay, and RIBLT symbol streams),
  and
- a difference reply (the items the responder holds that the initiator does not).

`SyncMode` as an embedder-side per-part hint does not survive this; it becomes one
input to the decision function rather than the decision. The opt-in-only gate in
`decide_peer_strat.rs` exists because the bucket path is currently unsafe for
filtered sets; once decision 2 lands, that gate is a measurement question, not a
correctness one.

Two consequences, found while wiring decision 2. `sync_modes` is `default()` — empty — at
every production construction site, and the only place that ever sets `SyncMode::Bucket`
is a unit test, so with the gate closed the bucket strategy does not run at all; the
decision line that reports the dirty count sits *after* the `CursorOnly` early return,
where it is unreachable. The instrumentation that decision 2 depends on therefore cannot
produce data until it moves above that return. The four long tests named for bucket
catchup inherit the same problem: they converge and assert the fully-synced stats but
never enter the bucket machine, so their name is a claim they do not check.

**The default inverts.** The gate existed because bucket pruning was unsound while
visibility was per object. With authorization per part, an authorized peer sees a part's
whole membership, both sides describe the same set, and the tree's ranges are comparable —
so `Bucket` becomes the default and the disabling moves to the embedder that actually
wanted it, recorded at those call sites rather than baked into a global default that
silently disables the strategy for everyone. Two preconditions were named for the
production default: one bucket-shaped test completing end to end, and the cutoff coming
from instrumentation rather than from the placeholder. The opt-out itself is gone: the
reason those sites recorded was the band stalling in the offline-reopen path with no test
covering it, and that path now has one
(`bucket_band_reconciles_after_offline_reopen` in big_repo). The cutoff remains the
placeholder, so it stays deferred rather than guessed.

Stage 1 confirmed the precondition and also showed the four bucket-shaped tests were in the
wrong regime: they are cold seeds, where everything is dirty and pruning has nothing to
prune — the case decision 7 already describes as enumeration. Bucket earns its place on a
large part where a peer is mostly caught up and little has changed, so that is the shape
the tests must create. Comparing bands by wall-clock is not viable: the same 100k case
measured 72s under concurrent load and 25s solo. The comparison asserts work — payloads or
messages moved — and logs durations beside it.

**As built.** `SyncMode` defaults to `Bucket`, and the opt-out is gone: the reason those
sites recorded was the band stalling in the offline-reopen path with no test covering it, so
the coverage was written (`bucket_band_reconciles_after_offline_reopen`) and daybook's four
production spawns and big_repo's harnesses now run the band. big_sync's own suite — about
sixty tests, including the three randomized four-node stress runs this gate was protecting —
already exercised it and passes. `restart_node` inherits the mode it booted with, so a
restart cannot silently change band mid-scenario. Band observability went a different way
than the obvious one: a stats variant is emitted at decision time into a 1024-capacity
broadcast drained only after the scenario, so it is dropped as `Lagged` on exactly the large
cases it would be meant to cover, and daybook matches the event enum exhaustively. The tests
instead ask the transport counters whether the walk happened — which is what closes the
naming gap, and what would have caught the decision-2 category error. The 256 cutoff stays a
placeholder: both measured regimes sit above it and favour the walk, but the crossing point
below it is unmeasured, and forcing the walk below the cutoff needs a seam the per-part hint
cannot provide, because the threshold check overrides it.

### 7. Enumeration stays a first-class band, and is not the answer for peers that already hold data

Enumeration is first-class because it is the cheapest thing that can be done —
stream the view's metadata, let the receiver diff locally, spend no coding at all.
Cursor replay is exactly enumeration for a peer with no prior state, which is
correct and should not be dressed up as strategy.

But a new *pair* is not a new *peer*: a restored device, a second relay, or a
phone that has been syncing through a relay already holds most of a view. Sending
the whole view to such a peer is a fixed `~1.0 · |view| · ℓ` cost where the actual
difference may be near zero. That case is indistinguishable from a cold start
without an extra input, which is what the view digest provides, and the algorithm
for it is a difference-proportional one.

### 8. Overlap between parts is handled per view; items never carry part sets

Object-to-part membership is many-to-many, and — per the causality point above —
it is unrelated to payload storage. An object in five parts must not cost five
copies of its payload on the wire, and the wire format already anticipates the
cheap case: `ObjChanged` carries the object's part keys alongside one payload.

But the reconciliation *item* must not carry a part set. An item is an
(object identity, version commitment) pair. Fusing part keys into an item would
(a) disclose the object's other parts to every recipient of that item, which is
the same leak as problem 6 in a less obvious place, and (b) make the item's
content depend on who is looking, so nothing could be shared. Sharing is instead
per *view*:

1. **Per part.** The reconciled set is that part's membership, which every
   authorized peer agrees on. One structure and one symbol stream serve all of
   them. This is the normal case.
2. **Per cohort.** When two peers' accessible part sets coincide exactly, their
   union is also an agreed set and its encoding can be cached and reused. This is
   a cache, not a universal property, and it is an optimization on top of (1).
3. **Split membership from version** if membership churn ever dominates payload
   churn: membership entries are sparse and tiny, version entries are deduplicated
   per object. This costs a second phase, which network-first ordering penalizes.

A note on RIBLT's "universal sequence": the symbol stream is a function of the
*encoder's* set, not the receiver's — the receiver's set is allowed to differ,
that is what peeling recovers. Universality therefore survives receivers with
different access, and is lost only when the *view itself* is per-peer, which is
the case decision 2 removes. Agreed views are what make cheap sharing possible;
they are not merely a constraint on it.

### 9. Event delivery is a client-driven long-poll page, not a push stream

`PeerReplayTask` is replaced. Instead of one long-lived task per peer/connection
forwarding every event of a whole target set, delivery becomes discrete pages:

- a request names a part and a cursor, and asks for a bounded number of events;
- the server holds the request while there is nothing to send, then answers with
  the page and the next cursor;
- the client re-issues. Paging *is* the flow control, and the client's own
  processing rate decides how fast events arrive;
- page outcomes can be empty, or can report an unknown or unauthorized part.

The shape to copy is `LeafBucketsTask`: a discrete task that completes and is
re-driven, rather than a task whose lifetime is the connection's. Which side drives
follows decision 6, not the connection.

What this fixes: no unbounded response, no shared-buffer-as-backpressure, no
per-peer replay task that cannot resume, resumption by cursor after any
interruption, and revocation for free (decision 2).

**As built.** `ReplayPageRequest { session_id, request_id, supersede, targets, limit, hold_ms }` answers with a
`ReplayPage { events, targets }` over a single oneshot — no stream and no connection-lifetime
replay task. A request batches a set of part and object targets, and each target carries its own
resume cursor. The responder applies authorization and event filtering, merges overlapping target
hits into one page, bounds the page by event count and encoded bytes, and holds the request while
all targets have no event until the hold expires. A hold expiry is a normal page answer, not an
error. Each target receives an explicit resume/drained verdict, so an empty page is not inferred
to be complete. The client supplies one random session ID for each machine lifetime; the session
namespaces its subscription and request IDs. The page request ID and optional supersede ID are
server-side lifetime metadata: a supersede cancels only a request still waiting, while a page whose
read has produced rows is allowed to finish. The page remains pull-driven and re-issued by the
client; the server does not retain replay rows or cursors between requests.

**Decision refinement: persist target metadata, not delivery state.** The dominant cost in a stable live
subscription is not target churn or the choice of shards: every empty 15-second long-poll page currently
repeats the complete target set. A peer with a thousand stable object targets pays those keys again
on every hold expiry even when no payload is delivered. Sharding is therefore an optional optimization
and a trade-off, not the primary answer. Splitting overlapping part and object targets loses the
page-level merge that turns one shared object event into one wire event; any sharding must keep
overlapping targets together or accept at-least-once duplicates as its explicit cost.

The primary protocol should keep the replay task discrete and pull-based while making the target
set stateful. This is stateful control-plane metadata, not a push stream, server-owned cursor, or
retained event queue:

1. The first `Update` for an unknown subscription ID opens the subscription: it carries the client session
   ID and its additions and returns the generation. There is no separate open request — open's full target
   list is just every target as an addition, and it would need the same per-entry outcome handling.
2. `Update` sends coalesced additions and removals. The client debounces rapid target changes, and the
   server applies only the newest generation. Removals are applied before additions, so one request may
   remove a target and re-add the same part under a fresh target ID; the fresh ID keeps old and new from
   colliding even if the server still holds stale entries. The answer reports **partial success**: entries
   the server could not accept come back as `UnknownPart` or `Unauthorized`, and the client retries exactly
   those entries, with their original IDs so a retry is idempotent, on the task's own backoff. New
   additions join the pending batch and reset that backoff.
3. `Next` names the session and subscription and asks for one bounded page. It retains the existing
   `limit`, `hold_ms`, page `request_id`, supersede behavior, and per-target resume verdicts. A page
   verdict of `UnknownPart` or `Unauthorized` is a **successful** `Next`: it means a target the subscription
   still lists cannot be served, and the client answers it with an `Update` that removes that target and
   re-adds the part under a fresh ID. `Next` is never paced per target — it holds on the server and
   re-issues — and only a whole-request failure (transport, a peer that went away) takes the task backoff.
4. `Close` releases the subscription. Subscriptions are scoped to the authenticated peer, storage scope,
   and client session; they expire when abandoned, and reconnect recreates one by opening a fresh
   subscription ID with an `Update`.


A target that comes back unknown or unauthorized is **blocked**: it stays in the subscription, it keeps
blocking full sync, and the server never removes it. Both rejection kinds are one machine outcome, because
absent access rows cannot distinguish a revocation from a grant that has not landed yet. The block clears
when an update for that entry succeeds or the embedder drops the target; only the client ever changes the
set, so a page that reports a bad target can never desync the session.
The existing full-target page request is the migration implementation and must be improved while this
lands, but it is not a second long-term protocol surface.
`Update` and `Next` are each discrete calls. An `Update` performs one reconfiguration and reports per-entry
outcomes; a `Next` fetches one bounded page. Neither carries a target snapshot to re-derive decisions
from: the machine owns the subscription state and hands the task the parameters for the one call it is
making.

This does not require a stateful transport. HTTP remains ordinary request/response transport: the
subscription handle is carried in each `Update` and `Next` request, while `Next` is the one long-poll
request. An update may arrive on another HTTP request and wake a waiting `Next`; the registry is
scoped to the authenticated peer, storage scope, and client session, and deployments without process
affinity must place it in shared state or route the session consistently. A lost or expired handle is
recovered by reopening with the full target set. WebSockets are therefore an optimization for transport
latency, not a protocol requirement.

The target set is updated between pages. If an update arrives while a page is waiting, it may wake
that wait and re-evaluate the latest set. If a read has already produced rows, that page finishes;
the update affects the next page. A server must not discard a row-bearing page merely because its
target set changed. Before serialization, however, events that are no longer covered by any active
target must be filtered. An event still covered by another target is retained once. This requires
the page builder to retain target provenance, or to re-evaluate coverage from the event and current
target set; target verdicts alone are not enough after the page has merged overlapping hits.

The handle maintains a compact target dictionary. Targets receive bounded integer IDs, so page
verdicts and target references do not repeat variable-length `PartKey` and `ObjKey` values. The
current page-level event dictionaries already establish this encoding direction; the subscription
dictionary extends it across pages. An object-target response arm may omit the object key when the
target ID uniquely identifies that object. A merged event whose source is a part target, or whose
object is covered by several target kinds, must retain an object reference or explicit target
provenance; the optimization must not make the generic event shape ambiguous.

All peer-controlled state is bounded by bytes rather than target count alone: maximum active
subscription sets per peer, maximum target count, maximum key length, and maximum encoded target-set
and update sizes. Count limits remain useful for work admission, but byte limits are the actual wire
and memory guard. The live set should normally remain one overlap-preserving subscription; bulk replay
may continue to use separate page scheduling and advisory lane scheduling.

This refinement preserves the reason for decision 9 — bounded, client-driven pages — while removing
the repeated metadata cost. It does not turn replay into a connection-lifetime task and does not
make the server authoritative for cursor progress.

The event vocabulary is two kinds, not three, and that was settled here rather than
inherited: a membership write is a *touch* (`Changed`, carrying the parts it names) and a
deletion is `Removed`. There is no `Added`. A keyed frontier stores the latest transition
per key rather than a history, so it cannot honour one: an object added at one revision and
changed at a later one is simply `Changed` to a subscriber behind both, and two subscribers
at the same cursor can be shown different kinds for the same object — an untrustworthy
label, not merely a redundant one. Whether something is new is also a fact only the reader's
own replica can answer exactly, and it answers it correctly under lag. Nothing branched on the
distinction: `Added` and `Changed` projected to the same object sync. `Removed` stays
because silence is ambiguous between "nothing happened" and "it is gone", and it keeps its
tombstone. A backend wanting richer kinds carries them in the frontier's generic value,
which widens what an embedder may assert rather than narrowing it.

**The add stamp returns, as a predicate rather than a kind.** Dropping `Added` also dropped the
stored `added_at`/`changed_at` stamps from the sqlite membership row (the memory store kept them in
its member state all along, which is where the divergence started), and a page that cannot tell
when an object became a member must hand every retained `Removed` to every reader. For a
long-lived or intermittently connected
subscriber on an active part — the triage case of ADR 010 above all — that is a wire cost paid
for dead slots nobody asked for. The fix is not the `Added` kind: it is one stamp, `added_at` per
`(object, part)` membership row, holding the cursor at which that row most recently became
*present* — set on the absent-to-present transition, preserved by present-to-present touches. A
deleted row at cursor `T` is then delivered to a reader at `c` exactly when `added_at <= c < T`:
if the row became present after the reader's cursor, the reader cannot have seen the object in
that part, so the removal carries no information for it. `cursor = 0` falls out of the same
predicate, since no stamp is zero. The stamp is invisible on the wire, so the "two subscribers
can be shown different kinds" objection above does not return with it: the kinds stay two, and
only the set of rows a page is drawn from depends on the reader's cursor. The stamp is per-key
metadata on the frontier's own row — a column beside `event_type` and `txid`, not a field of
`PartEvent`, because `PartPage.events: Vec<PartEvent>` is the postcard wire type — and the page
query uses it as a predicate, never copying it into a delivered event.

**On the object lane the vocabulary is content only.** A membership transition, removal
included, is a part-lane fact: an object page reports content changes, and a peer that may
no longer read any part containing the object is refused rather than handed a membership
event (decision 2) — which is why a deletion has no object-lane shape to invent. The
projection must therefore not turn a part-level deletion into a payload-less `Changed`:
that shape means *resolve the membership*, and the machine books it as content.


**As built, the page draws its rows with the stamp as a predicate.** The predicate above is
applied where the page's rows are selected: `push_selector_predicate` adds
`m.event_type != 2 OR m.added_at <= <that key's requested cursor>` to its per-key branches, so a
page never fetches a tombstone it cannot use, and the reads a page already makes — the
byte-budget probe and the value pass — are the only ones it makes. The request-scoped nature is
load-bearing and is now explicit in the interface: a *page* read
(`HostPartStore::open_page_reader`) applies the predicate, because a page's cursor is a claim
about what the requester already has; a *pull* read (`HostPartStore::open_revision_reader`) is
handed the log whole, tombstones included, because its bound is only where it starts streaming
and it then advances; and an `All` read carries no per-key cursor at all, so it also keeps the
log whole. The old per-event lookup (`obj_part_added_at`, called once per `Removed` in the page
builder) is gone: the stamp never crosses the store boundary.

**Membership removal is not payload removal.** An object removed from every part does **not** lose
its payload: `remove_obj_from_part` nulls the payload when the live count reaches zero, and that is
wrong — a big_repo object's payload outlives a part letting go of it, and an object is not new
merely because every part released it. The invariant is instead **live membership implies a
payload, and a payload is dropped only explicitly**: an object with no payload cannot be in a part,
`add_obj_to_parts` on such an object records pending membership and emits nothing (so re-adding
before the payload arrives is silence, exactly as the first add is), and payload removal is one
transaction that removes the object from every part it is in and then drops the payload. It needs
no new event kind: every reader that could hold the content acquired it through a part it may
still read, so it receives the `Removed` that empties its containing set and its own replica drops
the content — an inference, not a notice. Which payload-less objects are collected, and when, is
the embedder's policy on top of this: the sync backend owns object GC rather than a membership
removal doing it as a side effect.

**GC, and why tombstone vacuuming is blocked.** Two things can be collected, with different
prerequisites. Payloads are collectable now: the invariant above means an object keeps its payload
after every part releases it, so collection is explicit and belongs to the embedder — the store
offers `remove_obj_payload` (one transaction: remove from every containing part, emit those
`Removed`s, drop the payload), a listing of part-less objects, and counters (live versus dead rows,
payload bytes, tombstone bytes, dead-to-live ratio per part) for a janitorial loop to read.
Dead membership rows are the other thing, and under a hard partition rule they cannot be pruned at
all. Pruning is safe only if the removal is either delivered to every observer or reconstructible
afterwards, and neither holds: a removal is not addressed to anyone — a tombstone records the
object and the part, not which peer holds it — and in a symmetric difference between two peers a
pruned absence is indistinguishable from a stale presence, because "A has no row, B says present"
has no rule that makes A's absence win.

A cursor epoch does not repair that. It was my earlier sketch: a per-part epoch, carried as part of
the cursor's identity, so a rotation invalidates old cursors and a stale reader re-bases from the
full view, where absence is the signal. It fails on two counts. It makes the bucket strategy's
meaning depend on a per-part flag that two partitioned peers cannot agree on — specialization in
the wrong place — and it still has no answer for A syncing from B, where B's stale presence simply
resurrects what A pruned. What pruning actually requires is an **authority for the part**: a rule
saying whose membership set wins when two sides disagree. big_repo can offer that (peers compare
authority for a partition); keyhive cannot, because its authority is over which objects exist, not
over part membership. Detection is already there — bucket state separates `live_count`/`dead_count`
and `live_fp`/`dead_fp`, so "one side says dead, the other says live" is visible — so the missing
piece is only the direction. Until such a rule exists, dead rows are kept and paid for locally, in
storage and in the dead fingerprint; `added_at` removes the *wire* cost, which is what it was for.
Vacuuming is deferred behind the authority model rather than behind an epoch.

**The authority it needs already exists in both canonical deployments.** The sync backend is not
the authority on what may be removed; it is told. In big_repo, partitions are derived from keyhive
groups, keyhive tracks the causal relation for object removal with permanent revocations, and a
removal is therefore derivable from keyhive state rather than from a peer's replay event — which
is the honest reason removals were modelled this loosely here: the primary consumer of the replay
stream does not use remote removal events to change content. In triage (ADR 010) the authority is
the router: a removal traced to a router is respected, and a healed partition re-derives from the
historical routers, so a removal's validity is a function of which router it came from rather than
of who still happens to hold the object. That is the "whose membership set wins" rule vacuuming
was missing; only the plumbing into a part store's pruning decision is, which is why the mechanism
stays deferred although the authority does not. It also bounds the wire cost honestly: on a
two-device deployment that syncs everything, per-object authorization is not the question at all,
and on a relay the ratio counters are what say when the local cost is worth acting on.
Two costs and one divergence, all open. Each page is drawn from a fresh subscription, so a
deep backlog pays setup per page; the page bound was set to 1024 events because 256 made the
100k catchup case take 64s against 31s, and the real fix is a bounded query with the
recipient filter pushed into it rather than a re-subscription per page. Denial backs off
rather than dropping the route, because absent access rows cannot distinguish a revocation
from a grant that has not landed yet. And `Unauthorized` is only producible by a store that
knows access at the target level: the in-memory store filters events rather than targets, so
it answers empty where sqlite or big_repo answer denied. Nothing leaks through that — the
event filter still drops unreadable parts — but the denial path is exercised only on the
stores that filter.

### 10. Machines are layered on the low-level primitives

`watermark.rs` already provides the primitives this needs: `WatermarkBook` /
`WatermarkMachine` (admit, finish, track, settle, supersede, retire stream, and
the settled watermark) and `JobBoard` (per-job tracking, settling, superseding,
stream retirement). `tasks.rs` already provides the task frame (`Tasks`,
`spawn_task`, `spawn_delayed_task`, `enqueue_due_tasks`, `Retry`, `TaskCounts`).

The cursor machine and the bucket machine should express their stream and job
bookkeeping *through* those primitives rather than through their own overlapping
state, so that "what is outstanding, what has settled, when may we advance a
cursor" has one implementation. This is a refactor of the machines' internals, not
a protocol change, and it is best done alongside the long-poll conversion, since
paging removes the long-lived stream that the current bookkeeping exists to
manage.

**As built, and where it stops.** The cursor machine's per-part stream book *is* now
`WatermarkMachine`: `cursor_state` with its `CursorStreamState`/`CursorSlotState` is gone, and the
admission guard, the contiguous-prefix drain, `PartIdle` and the cursor advance all come from the
primitive. It drives the **tracked** half — `admit` / `track` / `settle` / `drop_stream` /
`retire_stream`, with `watermark` / `is_settled` as the predicates — because an object's events
are admitted per job (`track_obj_job`), settled one lane at a time (`Membership`, then `Sync`),
and a single stream is shed while its waiter survives on the object's other parts.
`tasks.rs` already owned the task frame and was not touched. The `WatermarkBook` below the
machine (`begin` / `force_finish` / `track_ref` / `release` / `drain`) has no caller outside
`watermark.rs` other than the slot-level `WatermarkMachine::finish`, which is for empty source
revisions; an earlier draft of this paragraph described the machine as using that untracked layer,
which the code never did. No test changed and none was added — the semantics newly relied on are
covered by `watermark.rs`'s own tests.

Two boundaries, established rather than assumed. The **job-lane half cannot be layered
without changing behaviour.** The machine removes a single stream from a surviving waiter and
frees that stream's cursor, keeping the waiter alive on its other parts; `JobBoard::supersede`
retains lanes and never removes a stream from a surviving waiter — it frees a cursor only when
a waiter's lanes are all dropped, at which point the waiter leaves every stream — and its
`keep` predicate sees a lane rather than the waiter, so the pending-membership case
(`pending_membership && parts.len() == 1`) is not expressible. This is the normal shape, not a
corner: one waiter gates every part an object changed in. The primitive needs more than a wider
predicate view; it needs a new **operation**. `keep` filters lanes, and no shape of it — lane-level
or waiter-level — removes a stream from a waiter that survives on its other streams, which is the
normal disposition here, so the case that motivates the change keeps no code path. A waiter-level
predicate is also needed, because the branch is selected by stream *count* rather than lane
identity, but it is the smaller half. The shape arrived at is
`StreamDrop<Lane> { Retain { keep: Vec<Lane> }, Release }` with
`drop_stream(job, stream, bound, decide: impl Fn(&Waiter<..>) -> StreamDrop<Lane>) -> Vec<Cursor>`:
`Release` removes the stream and frees its cursor, and `Retain` keeps gating the stream with only
the lanes the caller named in `keep`. It is an API decision on a primitive shared with `concurrent_delta_walker`
and belongs to this step's writer rather than being invented in the ADR. Three constraints belong to
the signature rather than to a call site. `supersede`'s emptied-waiter path removes a waiter from
**every** stream but reports a freed cursor only for the superseding stream, leaving a `Pending`
slot no waiter can finish, so `is_settled` stays false and `PartIdle` is never emitted for the
siblings; neither current consumer is exposed, but the `PeerState` retarget makes multi-stream
waiters the norm, so it is fixed with or before that. The board's `Copy` bounds (`JobKey: Ord +
Copy`, streams taken by value) are invalidated by step 7, so the new signature must anticipate
`Clone + Ord` and by-reference stream parameters or step 7 re-opens this API. Last, whether one slot can
carry two objects was settled rather than assumed, and it cannot: **two objects cannot share one
part cursor.** Every membership-mutating path allocates a cursor per `(object, part)` mutation, and
the one path that once shared a single cursor across a batch — the deleted object-part
materialization — gave each object its own derived part, so a share never collided there either:
the slot key is `(part, cursor)`, and a shared cursor in *different* parts is harmless. With the
tracked half this is no longer load-bearing — a waiter is addressed by `(job, cursor, lane)`, so
two objects on one slot would settle independently — but it is what keeps the slot-level
`finish` sound for anyone who reaches for it, and it is enforced by construction rather than by
schema or type — nothing prevents a future writer from putting two objects' events in one part at
one cursor — which is why the reason belongs written down here rather than left for a reader to
guess. One caution that no longer applies: the materialization site this note was written about
has been **deleted** (decision 3's revision — no derived part is stored), so the memory-versus-sqlite
divergence it worried about is closed by construction rather than settled by a coin flip, and "a
subscribe writes nothing and emits nothing" is now a shared assertion instead of a choice. The **bucket machine
has nothing to hand over yet**: one
part per machine, a scalar since-bound rather than a slot book, a job map that is an
object-to-bucket location with no cursor and a single completion event, and no durable cursor
to advance — it hands off via `UpgradeToCursor`. Decision 10's bucket half is therefore
conditional on its object jobs carrying a cursor.

**Retargeting the second half.** The bucket machine was the wrong second target: it has no job
or stream book to hand over. The big machine does, in two places. `PeerState` keeps
`sync_workers`, `remove_workers` and `pending_removals` — three hand-rolled copies of
object-keyed work carrying a set of part hints and a set of cursors, with coalescing and a
cancellation-deferred follow-up; that is one structure written out three times, and the
`pending_removals` comment describes exactly the supersede/retire problem. `SyncStatMachine`
keeps the per-(peer, part) gates — `cursor_active`, `multi_strat`, `pending` — that decide when
a part may be called synced, which is the settled predicate, and its
`FullSyncWaiterState { done_set, need_set }` is a barrier with an "only when every waiter
settles" rule that looks like the tracked, shared-slot half of `WatermarkBook` — the half the
cursor machine had no use for. That second site is where the four-node stress hang lived
(`multi_strat` never cleared), so the stress suite is the acceptance test: gates may become
lanes, but nothing may change when a part is declared synced. `emitted_full_synced` is an
emission flag rather than a gate and should not be forced into a lane.

**The task machinery also splits.** `scheduler.rs` is a second primitive already extracted
from `tasks.rs` with the same lifecycle semantics and two documented deviations: one
user-supplied `Seed` instead of the concrete `TaskSeed::{Sync, Machine}` split, and an explicit
`now` rather than a call to `Instant::now()` inside, which keeps it sans-io and deterministic
under test. `KeyedScheduler` is one task per key with coalescing on replace — `replace_with`
merges an unfinished seed into its replacement — which is precisely `PeerState`'s
`sync_workers` coalescing and `pending_removals`' merge of remaining hints and re-added parts.
This is not speculative: `TokioKeyedScheduler` already drives three runtime2 workers in
big_repo (automerge frontier, causal checkpoint, group part), so the sync machine's `Tasks`
plus its `task_id`-carrying maps are the remaining hand-rolled instance of a pattern the tree
uses elsewhere. The split to aim for: the scheduler owns task lifecycle by key — which task is
in flight, coalescing, cancellation, backoff, due ticks — the job board owns settlement
(streams, cursors, lanes, settled), and the machine keeps only its domain payload. Two
caveats: `Tasks` and `Scheduler` duplicate the same semantics and each exposes counts
(`TaskCounts` versus `SchedulerCounts`) that tests and stats read, so replacing one has to
keep those numbers honest or the swap is observable; and `KeyedScheduler` needs `K: Copy + Eq
+ Hash` with `Seed: Clone`. This work lands after step 7, which renames these types.

Before either one replaces the other, the two implementations must be **diffed rather than
assumed equivalent**. `scheduler.rs` claims the same lifecycle as `Tasks`, but a second copy of
a lifecycle is where semantic drift hides. A difference matters if it changes which task is
live for a key, when a retry is queued or how its backoff grows, whether a stopped task's
running future is actually cancelled, or when a due task becomes runnable. Each difference
found must be classified as a bug or as taste, and the surviving implementation fixed
accordingly: the extraction only replaces `Tasks` once its differences are accounted for
rather than papered over.

**The `tasks.rs` half of step 6 has no target in either machine.** The cursor machine never spawns,
tracks, cancels or counts a task, and holds no `TaskId`: its interface to the frame is by domain
identity — `CursorMachineCommand` out, and `on_obj_sync_job_evt(obj_id, cursor,
CursorJobCompletionKind::{Membership, Sync})` in — with the outer machine mapping between them.
`BucketMachine` contains no task state at all. The one place task bookkeeping does duplicate
`Tasks` is `BucketState.active_list_tasks` / `active_leaf_tasks` in the outer machine's
bucket-strategy wrapper (`lib.rs:299-304`), which mirrors `Tasks::pending`. So step 6's tasks clause
should be read as pointing there rather than at the two machines: they already sit on the far side
of a single frame (`tasks: Tasks`, `lib.rs:684`), and "nothing to hand over" is the honest answer
for them, exactly as it is for the bucket machine's watermark half.

**A precondition the layering depends on: the part stores do not implement one contract.** Three
divergences were found between the memory and sqlite stores, and one of them is still open. The
first two are closed in the stores *and* pinned by the shared harness, which was the structural
half: memory now answers `ReplayPageOutcome::Unauthorized` where it previously filtered per
recipient but reported the denial as an empty page (`assert_page_outcome_contract`), and
`latest_revision` is a read on both stores rather than an allocating one on memory
(`assert_latest_revision_is_a_read_contract`). The trait default's stated justification for never
answering `Unauthorized` — that the in-memory store hands every subscriber the same stream — was
falsified by that store's own `select_memory_event`, which is why the behaviour had to change
rather than the comment. The third divergence this paragraph used to leave open — object-part
materialization, where sqlite recorded one keyed-frontier `Changed` at the current revision and memory
recorded nothing — is **closed** by decision 3's revision: nothing materializes a derived part, so there
is no derived state for a store to publish or withhold, and "a subscribe writes nothing and emits
nothing" is asserted by the shared contract instead of left to a store. A primitive trusted with
settling cannot have two stores emitting different events, and after the revision it cannot.

**The remaining conversions, read and declined.** With the code in hand, none of the four
retargeted sites is wiring. The machine's frame is a single `Scheduler<TaskSeed>` with one
`TaskId` space and one spawn/requeue queue shared with the worker's admission policy, so a
keyed scheduler *per domain* is not a local change: each `Scheduler` allocates its own ids
(`next_id` is per-instance) while `TaskId` is the worker's task handle, and the drain that
preserves the uncapped-machine / capped-sync split and the surplus-sync LIFO order would have
to fan out and re-collect per scheduler. Keying the single frame instead needs a key enum with
an unkeyed variant, because two of the machine's four task kinds are not one per key:
`LeafBuckets` is per working bucket, so one part legitimately has N concurrent leaf requests
and `LeafBucketsResult` carries no key at all, and `DecidePeerStrategy` carries none either.
`KeyedScheduler` also hides its seeds, while the sync, removal and replay handlers all read
their payload (`cursors`, `part_hints`, `caught_up`) — so that shape needs a `seed(&K)`
accessor on the primitive as well. The barrier fails on the other side of the same test:
`JobBoard`'s unit is `(job, cursor, lane)` with one waiter per cursor, so "N needed pairs,
satisfied when all are" needs the pair itself as the cursor (relaxing `JobBoard`'s
`Cursor: Ord + Copy`) and an `is_pending` query, because `settle` panics on a lane that was
never pending, while the sync check fires for every pair that syncs, needed or not. Its
`retire_stream` is the one clean match, and it is what the `remove_peer` cleanup would use if
this is ever revisited. What the six maps add up to is coalescing on references across 121
lines with 28 spawn/cancel sites, and eight characterization tests already pin it. So they
stay, and the honest summary is that the machine's task lifecycle is the frame — `Scheduler` —
not the maps built on top of it.

**Scope discipline: reasonable, not uniform.** The goal is one implementation of what is genuinely
the same question, not DRY for its own sake. Convert where the primitive's semantics actually match;
where they do not, leave the code alone and record why — the job-lane half that needs an operation the
primitive lacks, the bucket machine that has nothing to hand over, and the domain command queue that
only looks like a task queue. A forced conversion buys uniformity and pays for it in indirection, which
is the failure mode this decision exists to avoid.

**As built: the frame swap landed; the wake has not.** `BigSyncMachine` holds `Scheduler<TaskSeed>` and
the `Tasks` frame is gone, with `tasks.rs` left as the task-domain types module. Count unchanged at
144/144 over 6 runs. Four things the swap actually cost, each worth knowing before the next consumer
moves onto these primitives:

- **`Retry` existed twice.** `tasks::Retry` and `scheduler::Retry` were structurally identical but
  distinct types, so every cancel handler failed to typecheck. They were unified — `tasks.rs` now
  re-exports `scheduler::Retry` and keeps `fresh` — because a `From` between two identical types is
  precisely the shim this repo forbids. Nothing outside `big_sync_core` consumed either one except
  `tokio_keyed_scheduler`, which already used the scheduler's.
- **`Scheduler` carries a `Seed: Clone` bound that nothing in it exercises.** Every path in that impl
  moves the seed; only `KeyedScheduler` clones. So the swap forced `Clone` onto five seed types for no
  runtime reason. Dropping the bound is a pure widening, and it is left as a follow-up because the type
  is shared with three runtime2 workers.
- **Threading `now` relocated a clock read rather than removing one.** Spawning always read the wall
  clock; it now reads it at 20 call sites. Genuine sans-io spawning would put `now` on `handle_evt` and
  `handle_task_msg` and thus on every caller — a public API change, and the honest next increment.
- **`TaskCounts` now mirrors `SchedulerCounts`** (`live`, `delayed`, `spawn_queue`, `stop_queue`) with
  `WorkerSnapshot::is_idle` semantics preserved exactly: `delayed` is `pending`, and the single spawn
  queue is the sum the predicate only ever compared to zero. `live == 0` was deliberately not added to
  it, since that would change when a worker counts as idle.

The driver's admission policy survived by partitioning at the drain: one seed kind is taken off the
single queue and the other is requeued untouched and in order, preserving both the
uncapped-machine / capped-sync split and the surplus-sync LIFO order. The `next_due`-driven wake is
still outstanding. And the blast-radius counts in the earlier note were low: `stop_task` has 24 call
sites, not 14, because ten were multi-line chains the search pattern missed.

**Stage 2 landed: the poll is gone, with one piece of scaffolding.** The worker now sleeps until the
machine's next deadline (`BigSyncMachine::next_due`, delegating to `Scheduler::next_due`) instead of
waking on a 500ms interval, so a paced retry fires when it is due rather than up to half a second late.
No-spin is a property rather than an assumption: `tick` removes the entries with `due_at <= now`, so a
deadline that survives a tick is strictly later than the `now` given to it. Two caveats are recorded
rather than hidden. First, the zombie sweep in the loop tail needs a *wake*, not merely a call site — a
zombie's completion is not a wake source — so the sleep stays bounded at the old interval while zombies
are outstanding. That bound is scaffolding, and the `TokioKeyedScheduler` conversion removes it along
with the map itself, since that type owns the handles and surfaces completions as a wake. Second, and
more importantly, the test suite cannot see this hazard at all: `snapshot()` is served through
`host_rx` and the idle wait polls it every 50ms, so the poll is itself a wake source and would keep six
green runs green even with no bound whatsoever. Pinning it needs a non-waking observation path, which
is a decision rather than a test tweak. A 60s idle ceiling was also added as a safety net against an
unknown periodic dependency; it is an invented constant, and the honest version would log when it fires
with nothing due, so that such a dependency surfaces as an observation instead of as silence.

**Candidate rejected after reading: the worker's task maps on `TokioKeyedScheduler`.** Its headline
appeal was simply wrong. The primitive does not model the worker's zombie state; it hard-aborts.
`abort_stopped` removes the handle from `handles` and *then* aborts it, the spawned future is dropped at
its next yield so its completion send is never reached, and a completion that races through is recognised
and discarded — the comment says "A cancelled/replaced task raced with the completion channel". Cancelled
work therefore becomes invisible to the driver, where the worker deliberately keeps it visible:
cooperative cancellation, a `zombie_tasks` map, an `is_finished` sweep, and `is_idle` requiring
`zombies == 0`. Adopting that cancellation means either losing the readiness predicate — the one that made
the idle flake diagnosable at all — or extending a shared primitive with introspection it lacks, for three
runtime2 consumers that do not want it.

Three further costs, each independent of that one. **The keys do not exist**: `MachineTask` carries no key
field and its deets are a four-variant enum, so no single `K` keys them all without an enum over kind and
inner key. **The coalescing is not the worker's**: `replace`/`replace_with` could express one task per
(peer, object), but the worker's maps are keyed by `TaskId` and the coalescing plus the merge — cursor
set, `part_hints`, `remote_payload` — live in the machine, so this is job-versus-task work, not a
scheduler conversion. **The decision split is the point**: in all three runtime2 consumers the driver
picks the key and the scheduler is a passive executor, whereas the machine here deliberately decides and
the worker executes, so hosting those decisions would either give the machine a key type or reintroduce
the mapping the split exists to keep out of the driver. And step 7 pushes against it too, since `K: Copy`
is required by every keyed operation and variable-length keys will not be `Copy`.

Two narrower observations are worth keeping regardless: the runtime2 pattern for budget pressure is that
the driver declines to call `replace` rather than letting the scheduler queue, and `wake` is called by no
consumer at all — only by its own unit test.

**The job half's extraction lost a branch, and `supersede` paid for it.** The original
`supersede_obj_part` had two branches: shed the stream from the waiter and free that stream's cursor (the
general case), and cancel only the sync lane while a queued membership still gates (the special case). The
extraction kept the second as a `keep: Fn(Lane) -> bool` predicate and replaced stream-shedding with
lane-filtering — which makes `lanes.is_empty()` reachable on a waiter that gated *several* streams, a state
the original's invariant forbade (there, a waiter lost all its streams only with its last one, so freeing
that one covered everything). The doc comment kept promising "free the cursor on every surviving stream"
while the return type `Vec<Cursor>` could not name them, and the wrapper released the superseding stream
alone: every other gated stream kept a `Pending` slot with a tracked waiter no one could settle, stalling
its watermark for the life of the machine. Nothing caught it because the one multi-stream test only called
`settle`, and the test that did exercise the empty case asserted `is_settled` on the superseding stream
alone — its comment described the correct behaviour and its body stopped one assertion short of detecting
the bug. `supersede` now reports `(cursor, streams)` exactly as its siblings `settle`/`settle_job` do, and
the machine releases each freed cursor on every stream the waiter gated. The stream-shedding branch is
still missing, and restoring it is what `drop_stream` is for.

### 11. Additional reconciliation algorithms arrive as plug-ins behind one view/summary surface

Near-term work is the part boundary plus per-part dirt plus the bucket fixes plus
delivery. Additional state reconciliation algorithms will be added as we learn
about them, behind a single view/summary surface, so that the *policy* is shared
and only the summary type and the exchange differ. RIBLT is the first candidate,
and it fits the cases the maintained-aggregate structure cannot serve: a large
difference, an unknown difference, an expensive link, or heavy overlap between
parts.

**RIBLT** (Yang, Gilad, Alizadeh, *Practical Rateless Set Reconciliation*,
SIGCOMM '24) reconciles sets of fixed-length items without parameters: the encoder
emits an unbounded stream of coded symbols `(sum, checksum, count)`, subtraction is
field-wise XOR, and the receiver peels until the difference is recovered. Its
mapping uses the degree distribution `ρ(i) = 1/(1 + αi)` with `ρ(0) = 1`, so the
first symbol decodes last and `a₀ ⊕ b₀` is a completion signal; the mapping itself
is realized by direct skip-sampling, so a source symbol touches `O(log m)` coded
symbols in `O(log m)` time. Reported overhead is `1.35 |S|` to `1.72 |S|` coded
symbols, with no estimate of `d` anywhere in the protocol. The paper contrasts this
with Merkle-trie exchange, whose cost depends on `|A|` and `|B|` and needs
`O(log |A| + log |B|)` round trips.

Two properties matter more than the byte counts for our targets:

- **Position-free resumption.** The only resumption point of a cursor replay is
  `(C, T]`, so an interrupted pass re-transfers metadata for everything already
  covered. A symbol stream can resume at any index, which on a flaky mobile link
  is worth more than a fifth of the bytes.
- **One burst, receiver-decided stop.** No dive chain and no idle radio wake-ups;
  the receiver stops when the first symbol decodes.

Per-view structure costs differ in kind, and the difference decides the bands:

| structure | maintain per mutation | memory | bytes per sync | rounds | parameters |
|---|---|---|---|---|---|
| range fingerprints (bucket) | `O(1)` per range | `#ranges × ~48B`, sublinear in the part | dirty ranges × summary | few (batched) | level/start estimate |
| RIBLT, materialized per view | `O(log m)` per item | `~1.35 · |view| · ℓ`, linear | `1.35 · d · ℓ` | one burst | none |
| RIBLT, encoded on demand | `O(|view|)` per sync | `O(1)`+ | `1.35 · d · ℓ` | one burst | none |
| enumeration | none | none | `~1.0 · |view| · ℓ` | one | none |

Under network > memory > CPU, encoding on demand beats materializing unless a
cohort has enough peers to amortize the maintained stream; fingerprints beat both
on memory because they aggregate, which is what makes them viable for a relay
holding many parts.

Crossover: RIBLT is cheaper than enumeration once `1.35 · d < |view|`, that is
`d ≲ 0.74 · |view|`. A peer with nothing has `d = |view|` and should be sent an
enumeration; a peer with 10% drift costs RIBLT a tenth of the bytes. RIBLT is a
middle-band algorithm, and identifying the middle band requires the part-relative
dirty count.

### 12. The local sedimentree-list part is a real part, not a special case

The existing `GLOBAL_PART_ID` ("the global partition: every doc we can read appears
here as a marker") is retained as a real part under the reserved key `/seds` — the
locally saved sedimentrees. No node key is included: part keys are scope-local, so
per-node disambiguation is unnecessary.

Its meaning is what makes it ordinary: it is *the owner's* list, identical for
every requester, which is exactly the "agreed set" property reconciliation needs.
The old name and derivation made it look per-peer ("every doc *we* can read"),
which is why it could never be range-summarized.

Granting `/seds` means letting someone mirror your full sedimentree list. It stays
useful internally — automerge-frontier mirroring uses it — but nothing may rely on
it being special: under decision 2 it is an ordinary part, and any part-scope
resolution that lands on it behaves like any other.

As built, that holds for the write path too: the batch reconciliation carries plain
membership rather than a `desired_global` flag, `add_obj_to_parts` no longer filters `/seds`
out of its inputs, and `scope_includes_part` resolves it generically. A local principal
records its `/seds` membership when it can read the part, by the same rule as any other
part, and the gossip path records `/seds` memberships from remote events like any other
part as well — which is exactly what the earlier special case was hiding.

Two tiers are worth distinguishing, and one is deferred: a grant on `/seds`
permits *enumeration* of the list, while *pulling* a given document is still
governed by the embedder's policy for that document. A future `sync_unknown` call
would let a peer ask for what it does not yet know about without going through the
enumeration path at all. That is a distinct design and is not settled here.

## Scenario rationale

The strategies exist to serve these cases. Percentages are guesses about
frequency, not measurements; the ordering is the point.

| # | scenario | pair state | relevant cost | band |
|---|---|---|---|---|
| 1 | urban phone, hours-to-days offline, relay as primary peer, low activity in its parts | known peer, huge global watermark gap, tiny `δ_part` | wire, then round trips | dirty count → **cursor replay**, paged |
| 2 | relay ↔ relay replicas, both always on, large overlapping state, stale or absent pair cursor | known/stale, small `d` | server CPU and RAM across many peers | view digest → fingerprints, or RIBLT |
| 3 | second device or restore of the same identity | stranger pair, small `d`, huge `|view|` | wire | view digest → **RIBLT** (enumeration would send the whole part) |
| 4 | first contact where both sides already hold part data from other peers | stranger pair, unknown `d` | wire | **RIBLT** — bounded by `1.35 · d` against enumeration's fixed `|view|` |
| 5 | brand-new empty peer | stranger, `d = |view|` | wire | **enumeration** (`1.0 · |view|` beats `1.35 · |view|`; cursor replay from zero is the same thing) |
| 6 | publisher to many followers, one-way, heterogeneous positions | per follower | server CPU, wire | one structure per part, cached per cohort; paged stream per follower |
| 7 | frequent small edits inside a live session | known, tiny `δ_part` | wire | **cursor replay**, with per-part cursors so it stays reachable |
| 8 | many parts sharing objects, expensive link | known/stale, high overlap | wire | cohort-cached encoding; items stay (object, version) |
| 9 | single-object subscription | object lane over the object's containing parts | wire | content events only; authorization is the containing-part existential; membership transitions stay on the part lanes |
| 10 | direct single-object share to a principal with no containing part | — | — | **dropped** with the derived part: it had no production writer, and a per-object grant is a new decision (decision 3) |
| 11 | object in no part at all | — | — | not remotely deliverable (fail-closed); local subscriptions unaffected |
| 12 | adversarial author, or collision grinding against summaries | any | — | count-compared summaries; per-request seeds wherever the summary is not materialized |
| 13 | broad public part, mostly read | known-ish | server RAM | enumeration or paging for cold, fingerprints for steady state |
| 14 | LAN or desktop-to-desktop, bandwidth cheap | any | CPU | enumeration; spend CPU rather than cleverness |
| 15 | revocation mid-session | any | — | not an event: the next page answers "unauthorized part" (decision 2) |
| 16 | relay replica mirroring an owner's whole store | `/seds` granted | wire, server RAM | per-part bands over the parts the grant exposes; enumeration permitted, per-document pull still policy-gated |

Rationale for the ordering: (1) and (7) are the same case at different time scales
and are the bulk of real usage; (2) and (6) are the cost centres that must not
degrade as peers accumulate; (3) and (4) are rare but expensive to get wrong, and
they are the reason enumeration cannot be the only fallback; (8) is the case where
per-part work multiplies without a shared view; (5), (9) and (14) are cheap and
should stay simple; (10) is dropped with the derived object part (decision 3) and (11) is
fail-closed by the same revision; (15) shows why paging makes the authorization model
simpler rather than more complex; (12) is a constraint on every summary we keep;
(13) is the shape that motivates keeping an aggregate structure at all; (16) is why
`/seds` is retained.

## Interface shape

The store and the protocol need to expose, for a view:

1. **A part-relative dirty count.** Requires the access stamp on part access rows
   plus an index that makes the count bounded, and requires that
   `parts.latest_cursor` is never mistaken for it.
2. **Per-range summaries with a since-filter**, scoped to a part, retrievable for
   a range in one page rather than one level at a time.
3. **Ordered item enumeration for a view**, ordered by the view's key order so
   ranges and enumeration agree.
4. **A view descriptor and digest**, exchangeable before choosing a band, so a pair
   with no prior cursor can be classified rather than inferred.
5. **A symmetric decision function** over exchanged descriptors, computed
   identically on both sides, with a deterministic tie-break when the two sides'
   descriptors disagree.
6. **A paged event read** over a bounded target set: `(subscription, limit, hold_ms) →
   (merged filtered events, per-target resume/drained verdicts)`, with empty pages, long-poll holds,
   explicit unknown/unauthorized outcomes, and a bounded target-set handle so stable target keys are
   not repeated on every page.
7. **Derived part key names** for object parts, `o:{object_key}`, computable by any
   holder of the object key without a lookup. A name only: after decision 3's revision nothing
   stores, derives or interprets it.
8. **One reconciliation surface for parts and object parts alike**, with any API
   separation justified by the difference inventory (answered by that revision) rather than assumed.

## Consequences

### Positive

- The common case (phone, hours-to-days offline, relay as primary) takes the
  cursor fast path again, because the band is chosen from a measured change count
  rather than from a global-watermark distance.
- The bucket tree becomes usable rather than opt-in: its scope already matches the
  authorization boundary, so its fingerprints prune once the per-object filter is
  gone. The FIXME naming filtered sets as the real cause is resolved by the
  boundary change, not by touching the tree's shape.
- Per-range summaries are validated against counts as well as fingerprints, so a
  cancellable sum cannot silently hide a divergence.
- Part keys stop being disclosed: delivery names only parts the recipient may
  read, and the reconciliation items never carry membership at all. The
  membership-metadata leak is closed rather than accepted.
- Authorization becomes cheaper and smaller: one row per `(part, principal)`
  replaces a materialized expansion rewritten per object, and revocation needs no
  message type.
- Agreed views make cheap sharing possible: one encoding per part serves every
  authorized peer, and cohorts cache the shared case.
- Object subscriptions stop being a second lane: they gain a cursor, resumption, a
  place in the access model, and a key that is derived rather than allocated.
- Delivery gains paging: bounded responses, subscriber-driven pacing, cursor
  resumption, and no long-lived per-peer replay task.
- Adding a reconciliation algorithm becomes a plug-in behind one surface instead
  of a redesign of the machine.
- Byte-string keys remove the artificial 32-byte limit, make key order meaningful,
  and make reserved and derived key spaces expressible.

### Costs and trade-offs

- Part-granularity access gives up object-granularity *denial*: access is additive
  and there are no deny rows, so revoking a single object is done through
  membership or part access.
- Object parts are only derivable from an object key. A peer that has never been
  told about an object cannot compute its part, so discovery still requires the
  object key to arrive by some other route.
- The causal independence of part assignment and payload storage does not go away.
  Ranged reconciliation must tolerate a member row whose payload is not yet stored,
  and the count-based comparisons must not treat that as a divergence.
- Filtering costs a per-event check of one to three part keys, and an event whose
  parts all fail must be dropped rather than forwarded.
- Relevance stamping adds a column and an index, and every grant path must write a
  stamp.
- Symmetric exchange means both sides carry descriptor computation and summary
  state, not just the initiator.
- Long-poll paging trades a persistent stream for repeated round trips; the hold
  duration and page size are latency/throughput trade-offs to be measured, and a
  long enough hold costs a blocked request per idle peer.
- Consolidating the machines touches the core lifecycle and must be done without
  regressing the ordering guarantees that make cursor advancement safe.
- Byte-string keys touch persisted keys, RPC payloads, and every blob-key helper;
  it is a wire and storage migration, and the `Key` type rename rides with it.
- Maintaining several bands is a real maintenance tax, which is why they share one
  policy surface and one decision function.
- Thresholds (dirty-count cutoffs, overlap cutoff, view-size cutoff for
  enumeration, page size, hold duration) must be measured. Inventing them would
  reproduce the `BUCKET_DIFF_THRESHOLD` mistake at a larger scale.

## Migration

1. **Part-scoped access and filtered delivery.** Landed: access rows are
   `(scope, part, principal)` with a change stamp, delivery computes the
   recipient-filtered part set and drops events that filter to empty, and
   `event_permitted` / `is_event_permitted` and the write paths that used to delete
   all rows for an object and re-insert one row per principal were rewritten. This is the change
   that makes the tree prune and stops the disclosure.
   - The zero-part contract test changed meaning, deliberately: an object in
     no part is not remotely deliverable. Its replay-versus-live convergence
     assertion is kept on the local, unfiltered lane, and a remote assertion was added
     for the fail-closed behavior. This is a semantic change, not a weakened test.
2. **Per-part dirt.** Landed: the access-change stamp index, the counting primitive, the
   descriptor exchange, and the wiring. The asker advertises its per-part cursors in
   `PeerSummaryRequest.asker_part_cursors`, the responder counts its own rows against
   them for the authenticated asker — the count has to be computed on the announcing
   side because a local read compares two independent scales (decision 5) — and
   `decide_peer_strat` consumes `CursorPartSummary.dirty_count` as the band input.
   Both numbers are instrumented for every band, `CursorOnly` included, so the cutoff
   can be measured rather than guessed. The cutoff itself is still the unmeasured
   placeholder, which is why it stays deferred.
3. **Bucket fixes.** Compare counts alongside fingerprints (landed); a range request
   returns its summaries in one page (landed — see decision 4's `to_level`). The starting level stays the
   size-derived `calc_working_level` choice: the dirty count selects the band and nothing
   else, so there is no second level input to plumb (decision 4). The opt-in-only gate is
   inverted (landed): bucket is the default and the embedders that had opted out run it
   too, with the offline-reopen coverage their recorded reason asked for (decision 6).
4. **Object parts.** Landed, then revised. Object subscriptions first materialized a derived
   `o:{object_key}` part with one membership row, lazy lifecycle, inherited access and optional
   additive rows. That materialization wrote state on a read (observable in sqlite, whose membership
   row is its keyed frontier), put a derived part in front of the frontier reconciler, and existed to
   serve a direct single-object share that had no production writer. Decision 3's revision deletes it:
   the reserved key is a name, nothing derives or interprets it, and a per-object grant is a new
   decision rather than a carried-over mechanism.
5. **Long-poll delivery.** Landed: `PeerReplayTask` is gone, replaced by paged reads
   driven by the cursor machine, with explicit unknown/unauthorized page outcomes and
   filtered events, and the push-stream path is deleted. The event vocabulary is two
   kinds rather than three (decision 9).
6. **Machine layering.** Re-express the cursor and bucket machines' bookkeeping in
   terms of `watermark.rs` and `tasks.rs` primitives. Landed where the primitives'
   semantics match: the cursor machine's stream book is `WatermarkMachine`; the machine's
   task frame is `Scheduler<TaskSeed>`, with the `Tasks` frame gone and the `next_due` wake
   in its place; and the job-lane half got the `drop_stream` / `StreamDrop` operation it was
   missing. Read and declined, with the reasons in decision 10: the bucket machine, the
   `PeerState` object-work maps, the bucket-strategy task maps, the full-sync waiter
   barrier, the domain command queue, and the worker's task maps. The thin machine layer
   that remains — which commands a subscription event emits, in what order, and the
   object-cursor dedup — is pinned by unit tests in `cursor.rs`, because `watermark.rs`
   covers the primitives underneath it rather than the translation onto them.
7. **Byte-string keys.** Landed: keys are `Arc<[u8]>` byte strings, the persisted
   columns and wire formats carry them (key columns were already unconstrained `BLOB`s),
   the `Key` types are renamed, the reserved key spaces are literal, and the distribution
   hash is derived at the point of use — as a stored `buck_index` on the object row, since
   the stores range-scan a bucket's members and SQL cannot hash (decision 1).
8. **RIBLT — deferred, not in this PR.** Adding it as the first difference-proportional
   algorithm behind the same surface, then the next one when a scenario demands it. This is out
   of scope for the current change by decision rather than by omission: it is an addition, not a
   fix, it depends on the unmeasured item-width `ℓ` (decision 5, and the deferred list below),
   and nothing already built needs it to work.
9. **`/seds`.** Landed. It is named for what it is — `seds_part_id()` returns the reserved
   key `/seds`, this node's local index of the sedimentrees it has saved — and it stays a real
   part: the three remaining special cases are gone. `add_obj_to_parts` no longer filters
   `/seds` out of its inputs, the batch write path carries plain membership instead of a
   `desired_global` flag, and `scope_includes_part` resolves it generically. The store owns it,
   because the store is the side that knows a tree exists: every content mutation runs through
   `mutate_tree_in_tx`, which writes the membership for the tree-derived object alongside the
   payload, and `delete_sedimentree_id` removes it again. The group-part worker's subject is
   keyhive-derived group membership, so it no longer writes `/seds` at all. The gossip path
   records `/seds` memberships from remote events like any other part, which decision 12 now
   states. Being local bookkeeping, it carries no derived access rows: access rows are written
   only for parts with a derived agent set, which `/seds` never has. What a peer may see through
   it is decided per event by the readability filter, and a peer meant to pull the partition
   itself is granted `/seds` explicitly.

Steps 1–3 are the near-term focus: a bucket that works for the authorized case,
with a measured dirty count replacing the global-watermark heuristic and no part
keys leaking to unauthorized peers.

## Deferred decisions

- ~~The object-versus-part **difference inventory**~~ — **closed** by decision 3's revision: the
  only remaining asymmetry is where an object route keeps its cursor, and the two store
  surfaces are unified on the parts that matter (subscription, page, authorization).
- Whether a per-object **deny** is ever needed. Today access is purely additive;
  a deny would be a new mechanism, not an inference.
- ~~The **lifecycle and GC policy** for object parts~~ — **closed** by decision 3's revision: no
  derived part is stored, so there is no row to collect or keep as a cache.
- ~~Whether **derived access** for object parts is ever materialized~~ — **closed**, and answered
  no: not on subscription, not on access, and nothing derives or interprets the reserved key.
- The **enumeration-versus-pull** split for `/seds`, and the shape of a future
  `sync_unknown` call.
- The `/seds` **migration**: how to re-derive keys and whether the old marker key
  is retained as an alias for one release.
- The cardinality of relevance stamps, and whether the per-part principal set is
  materialized or derived at read time.
- Whether `set_part_members` and its siblings become **no-ops when the access set is
  unchanged**, and whether a redundant access write consumes a cursor at all (relevance
  argues it should not).
- Whether the range structure keeps its fixed arity and level or becomes a
  key-derived lexicographic split.
- The item encoding width `ℓ` for a difference-proportional algorithm, and whether
  membership and version travel as one fused item or as two phases.
- Adding **RIBLT** at all. The plug-in surface is what the current change owes; a second
  algorithm in tree is not required by it, so the first difference-proportional algorithm stays
  deferred together with the item-width and fused-item questions directly above.
- The view digest's shape and whether it is exchanged before the first summary
  exchange or piggybacked on it.
- Cutoffs for every band, to be measured rather than chosen. The bucket dirty-count cutoff
  is the clear case: it is an optimization knob, not a blocker. Both measured regimes sit
  above it and favour the walk, and measuring the crossing point below it needs a seam that
  can force the band below the cutoff, since the per-part hint cannot.
- The **leafing optimizations** in decision 4 — batch the page, compare digests, or dive
  deeper — and with them the fate of the leaf path's per-request seed.
- Whether a receiver ever keeps a durable mirror of a view, or always re-derives
- Whether the **first page request for a part racing the grant that authorizes it** is worth
  avoiding. A fresh route is denied when the grant has not yet reached the peer's store, and the
  denial is paced by `UNAUTHORIZED_BACKOFF` (30s). The pacing is deliberate and reversible, but it
  means a first sync can sit idle for 30s on a part, and the constant is a pacing knob rather than a
  measurement. Whether the authorizing side can apply its own grant before asking, or whether only
  the first retry needs that long, is open.
  The same asynchrony has a second face worth naming, because it has now been mistaken for a store
  bug once: a member granted *after* an event was written must not see that event, so an assertion
  that "a granted member sees the part's events" is only true for one of the two orderings. A store
  contract test was observed failing intermittently on exactly that, with the page correctly
  un-denied but the event correctly filtered out. Such a case has to be settled by constructing both
  orderings deliberately, not by sampling runs until one of them stops appearing.

  A third face, verified in the page builder itself: `drained` is set only when `ReplayComplete`
  arrives *and* the page already holds events, and the result is `if drained { None } else { resume }`.
  On an empty page that breaks on its hold, `resume` was never assigned, so the caller is handed
  `None` — documented as "the log is caught up as of the last event" — even though the replay half
  never completed and nothing was read. The undrained case is indistinguishable from the exhausted
  one because the not-drained branch reuses a cursor that is `None` for an unrelated reason. A zero
  hold reaches it trivially; production holds of 50/250ms narrow the window without closing it. The
  builder already keeps holding when a *drained* replay is empty, so the distinction is understood — it
  simply is not expressed in the result. Reporting a resume cursor whenever the replay half did not
  complete would reserve "caught up" for when it is true.
  its side from local state.
- Retention and pruning rules for range summaries and for stale peer cursors.
- ~~**Cursor epochs** as the way to prune tombstones~~ — **withdrawn** (decision 9): it makes the
  bucket strategy's meaning depend on a per-part flag two partitioned peers cannot agree on, and it
  does not stop a stale peer's presence from resurrecting a pruned removal.
- **Tombstone vacuuming, blocked on an authority rule for a part.** Pruning needs "whose membership
  set wins on disagreement"; big_repo's per-part authority comparison is the candidate and
  keyhive's object-existence authority is not. Until then dead rows are kept, and the counters
  that would have triggered a rotation only measure the local cost.
- The **janitorial loop's inputs**: the part-less object listing and the counters (live versus dead
- The **leaf page's byte budget**: `LeafBucketsRequest::limit_hint` bounds *entries*, so a page of
  32-byte keys at the cap is ~42 KB and a longer key is worse. A byte bound beside the entry cap is
  the direct fix, and it is worth doing before the leaf page grows a payload-carrying variant.
  rows, payload bytes, tombstone bytes, dead-to-live ratio) — what a backend's collection policy
  reads.
- The `unscoped`/all mode question: what "mirror everything" means once `/seds` is
  an ordinary part, and whether that mode becomes "all parts" or a real group.
