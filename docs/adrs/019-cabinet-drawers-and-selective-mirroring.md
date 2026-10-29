# ADR 019: Cabinet Drawers

**Status:** Accepted — implemented by `DrawerService` (`src/big_cabinet/drawer.rs`)
for the self-doc and single-group layouts; the deferred list at the end is unchanged.

## Scope

[The drawer FDR](../fdrs/001-drawers-as-document-collections.md) defines the
user-visible collection model. This ADR is the contract for the drawer model
Cabinet implements: the drawer datums, roster, child entries, and required
local drawers below are `DrawerService`'s contract. Namespace capabilities, actor identities,
domain-owned grants, signing, and their implementation migration are specified in
[ADR 022](022-authority-actors-capabilities-and-grants.md). Subset drawers, managed-participant
layouts, roster sharding, provider/home datums, and Panproto integration are
later work.

A doc can be entered in several drawers. Official inclusion, Keyhive access, and local
byte availability are separate facts.

## Identity and layouts

A `DrawerId` is the `DocumentId` of the doc carrying `org.example.daybook.drawer/main`.
Neither a Keyhive group nor a hosting node identifies the drawer.

This PR supports two layouts:

1. **Single-doc drawer.** The descriptor doc is the only doc in the drawer. It needs no
   collection groups or roster entries.
2. **Single-group drawer.** A separate drawer metadata doc names a meta group and docs
   group and carries the roster directly.

The self-group layout and managed-participant layout are not implemented.

A self-doc drawer cannot grow in place. To create a collection around it, Cabinet
creates a new single-group drawer with a new `DrawerId` and adds the original doc as a
`docEntry`. The original doc keeps its ID and may remain a self-doc drawer
independently. This avoids changing the meaning and disclosure boundary of an existing
identity.

A self-doc drawer's descriptor and content share one doc, so Read of the descriptor is
Read of the content; use a single-group drawer when those audiences must differ. Sharing
a self-doc drawer discloses the doc's retained history, and later moving content out or
separating the descriptor does not retract that disclosure: a stable ID is not a promise
that an audience change can erase earlier content.

Every branch is an ordinary doc with its own `DocumentId`. Branch creation does not add
the new ID to a drawer containing its origin; admission is explicit.

## Groups and entries

A single-group descriptor names:

- `metaGroup`: authority over the descriptor and its canonical roster;
- `docsGroup`: the collection group to which included docs delegate access.

Use **entry** for objects listed in a roster. Reserve **member** for actors/principals and
Keyhive group membership. Design and field names prefer **doc** over “document.”

The roster is authoritative for official inclusion. A Keyhive grant without a roster
entry is an accessible candidate, not an official entry. A roster entry whose requested
grant is absent or narrower remains officially listed but is pending, unavailable, or
narrower than intended. Local byte presence is independent of both.

### Why a grant is not an entry

Keyhive answers *who may access D*, not *whether a drawer writer approved listing D*. In
Keyhive's authority direction, `D → docsGroup` is a grant over D to the group. Anyone with
authority over D can issue it without being able to write the drawer, so it makes D
discoverable to the group's holders as a candidate but cannot be an official addition.
The reverse, `docsGroup → D`, would give D and its downstream principals authority over
the group and, even at Relay, expose the group's Relay-visible membership metadata; it is
never created just to list a doc.

A holder of `docsGroup` can discover docs reachable through such grants without loading
the roster. That is a candidate set, not the drawer's approved roster: the holder reaches
docs through other paths too, and a doc owner can grant to the group without the drawer
writers' consent. Official enumeration reads the verified roster and intersects it with
observed access where an accessible listing is asked for; roster-only entries stay
visible as pending or unavailable to a reader allowed to see the roster. A reader of D
may see the recipient group's ID in D's authority metadata, but that grants neither the
group's roster nor its purpose; the group ID is not promised to be secret.

Where a doc is already shared with the drawer's audience, an entry need not mint a
redundant grant just to record admission. Revoking access and removing an entry remain
different operations.

Public docs (**Deferred** with public indices) take per-doc `D → Public` Read grants
rather than making a drawer group publicly readable, because Keyhive Read holders can
subdelegate. The roster stays writer-controlled; a public index may project it for
scalable enumeration but is not its source of truth. A stranger's public grant neither
edits the roster nor makes their doc an official entry. A drawer that wants open
submission needs an explicit admission policy rather than getting one by equating Keyhive
reachability with curator approval.

A doc entry is keyed by the doc ID and records the requested Keyhive access:

```jsonc
{
  "org.example.daybook.drawer.docEntry/zDocA": {
    "access": "Write"
  }
}
```

The key already states the doc ID; the value does not repeat it. `access` uses Keyhive's
`Relay`, `Read`, `Write`, or `Admin` vocabulary. It records desired drawer state while
Keyhive remains authoritative for effective access.

Adding a doc establishes the requested grant to `docsGroup` before publishing the roster
entry. Removal deletes the roster entry before revoking the corresponding grant where
authorized. Widening establishes stronger authority before publishing the stronger
entry; narrowing publishes the narrower entry before reducing authority. The two stores
are not one distributed transaction, so interrupted operations remain visible and
retryable rather than falsely complete.

## Child entries

A child drawer is a typed roster entry, not a separate `DrawerRelation` datum:

```jsonc
{
  "org.example.daybook.drawer.childEntry/zDrawerEngineering": {
    "access": "Read"
  }
}
```

The key identifies the child. The child need not list its parents. Adding the entry also
arranges the explicit Keyhive group delegations needed to propagate access from the
parent through the child's groups, bounded by the granter's own authority. The entry is
published after those delegations are established; removal reverses that order.

One access setting describes the meaningful child relationship:

- `Read`: readable set union; grant Read over the child meta and docs groups.
- `Write`: writable union; propagate Write over both groups, while actors remain bounded
  by their own access through the parent.
- `Admin`: administratively managed union; propagate Admin over both groups.
- `Relay`: enumerable retention; grant Read over child metadata so the canonical roster
  can be enumerated and Relay over child docs so ciphertext can be retained. An actor
  whose own path is only Relay does not thereby gain Read.

There is no docs-only child relationship: without Read of the canonical child roster,
the parent cannot know which reachable docs are official entries. The ordinary “add
child” default is `Read`. User interfaces may label the four choices Retain, View,
Contribute, and Manage while storing the Keyhive access vocabulary.

Child traversal computes recursive set union, terminates on already visited drawer IDs,
and uses the strongest effective access when a doc is reachable by several independent
paths. Keyhive reachability alone never creates an official entry.

## Datums

The first-version vocabulary is:

```text
org.example.daybook.drawer
org.example.daybook.drawer.roster
org.example.daybook.drawer.docEntry
org.example.daybook.drawer.childEntry
```

Datum tags are grouped by dotted names. Drawer values carry no ad-hoc version integer;
compatible evolution occurs under the datum identity, while incompatible meanings need a
new identity.

The layout is an open union, internally tagged as every published union is (ADR 021 §5):
the member object carries `$type` beside its own fields. Carrying is not operating — a
node that does not know a layout still reads, stores and lists the drawer untouched; only
an operation that must interpret the layout refuses. A Lexicon object states at least one
property, so a member with nothing to say carries `empty: true` — the convention for an
empty member, excluded from the wire by `skip_serializing_if` until a later field lands
beside it. Adding a layout is a new member under the same datum id, not a new datum tag.

A self-doc drawer is:

```jsonc
// The hosting doc ID is the DrawerId.
{
  "org.example.daybook.drawer/main": {
    "layout": { "$type": "org.example.daybook.drawer.selfDoc" }
  },

  "org.example.daybook.note/main": {
    "text": "This doc can be shared as a drawer."
  }
}
```

A single-group drawer is:

```jsonc
// Doc ID: zDrawerWork
{
  "org.example.daybook.drawer/main": {
    "layout": {
      "$type": "org.example.daybook.drawer.singleGroup",
      "metaGroup": "zGroupWorkMeta",
      "docsGroup": "zGroupWorkDocs"
    }
  },

  "org.example.daybook.drawer.roster/main": {
    "layout": { "$type": "org.example.daybook.drawer.roster.inline" }
  },

  "org.example.daybook.drawer.docEntry/zDocA": {
    "access": "Write"
  },

  "org.example.daybook.drawer.docEntry/zDocB": {
    "access": "Read"
  },

  "org.example.daybook.drawer.childEntry/zDrawerArchive": {
    "access": "Relay"
  }
}
```

The `drawer.roster/main` datum marks the canonical roster and states where its entries
live. A Lexicon object states at least one property, so the marker is not `{}`: it
carries `empty: true`, the empty-member convention. Today every entry is a datum on the
drawer doc itself, so the marker has nothing further to say; the fields an external
roster layout needs (roster docs, ranges, shard routing) land beside `empty` as optional
fields when the sharding design below is implemented.

Group ids in the descriptor are the 32 identifier bytes as multibase base58btc, the text
form every other 32-byte key in this tree has.

## Actors and node authority

ADR 022 separates DrawerService's namespace minting authority from its actor proof.
The embedder provisions both. DrawerService owns durable standing grants, evaluated
when a caller exchanges an identity proof for a scoped drawer cap; resource operations
then accept that cap, with no identity fallback. Grant gates tie derived caps to
their support, replacing central grant RPCs and two-slot resource requests.

Standing grants cover drawer levels and collection creation. A level covers the ones
below it, and a grant on a parent drawer reaches a child at the weaker of the grant and
the child entry, strongest path winning. Not every actor needs grant rows: a dispatch
can receive a scoped cap narrowed from another principal's authority. Withdrawing a
grant must retract the authority derived from it, including cross-domain derivations.
Creating a drawer of either layout grants the originating actor Admin on it; a self-doc
drawer needs Write on the doc instead of collection creation authority. Creation records the originating actor independently from the bound signer, and
recovery preserves that provenance when applying creation grants. Actor signing keys
remain sealed under a `secrets_rs` keyring key.

Domain capabilities authorize Cabinet operations. BigRepo/Keyhive separately gate what
the node can do. If the node lacks authority, no local capability bypasses it. Recovery
principals are node-level work deferred to another PR.

Drawer access uses Keyhive's `Relay`, `Read`, `Write`, and `Admin` vocabulary in
roster entries. Standing grant objects name levels (`write drawer D`), while scoped
capabilities encode levels, signer bindings, and originating-actor provenance under
`DrawerService/scope/v2/`. Rights use ADR 022's generic vocabulary; the meaning of
these objects belongs to DrawerService, not to the capability table. Removing a doc
entry never means global doc deletion.

## Listing a drawer's docs

`listDocs` answers what a drawer holds as the caller may see it: one effective-access check
on the drawer (Relay suffices, as for `getDrawer` — Relay exists to enumerate), then the
scoped roster, not a per-doc check. The drawer's own entries come first (an entry caps what
the drawer confers), then every child drawer reachable by links the level reaches, with the
child's docs attenuated by the weakest link along the path and the strongest path winning —
the same algebra child traversal applies, walked downward. `listDocs` reports what the
*drawer* confers; a doc the caller can reach through another drawer or a Keyhive grant is
that path's business, and DocsRepo checks doc operations against every path as before.

## Required local drawers

Every node has two ordinary single-group drawers that DrawerService creates on its first
boot and records locally:

- docs physically present on this node;
- docs originated by this node.

Plus one registry drawer: the known-drawers list, a self-doc drawer holding
`org.example.daybook.drawer.knownEntry/<drawerId>` datums (`{ mirror?: true }`). It is an
explicitly managed list, not a collection and not a tag scan: remembering a drawer grants
no access to it, mirroring intent certifies no fetched or retained bytes, forgetting edits
nothing but the local registry, and child relationships or observed datums never write it.
DrawerService's `rememberDrawer` / `forgetDrawer` / `listKnownDrawers` / `setDrawerPolicy`
are the only writers. The registry doc is the durable home — the list survives a restart
by being read back from the doc — and its id anchors in the same local record as the other
two, because it cannot name itself inside itself.

They use the same drawer model and remain local because nobody grants them to other
nodes; the service itself administers them and the embedder reads their ids from the
booted service. The authority service does not create them. Doc creation always names a
target drawer (`placement: "drawer <id>"`, needing Write on it) and the placement also
enters the new doc in both local drawers, at Admin in all three, so a doc is never created
outside every drawer. A drawer's own metadata doc enters only the local drawers.

The doc-access policy DocsRepo is booted with (`DrawerAccess`) answers from the same
projection: an actor reaches a doc through any drawer listing it — the doc being a drawer
itself, or a roster entry capping what the drawer confers — and through parent drawers as
above.

## Boundaries

Cabinet owns drawer semantics, roster writes, actor checks, and the projections needed to
query them. BigRepo owns node-level Keyhive enforcement and physical content. An embedder
chooses peers, transport, sync, byte retention, and public/provider APIs.

`datum_set` and `datum_ref` move from daybook_core into Cabinet as generic projections;
blob-specific behavior is removed. SQLite tables and transitive-index strategies are
implementation details, not authoritative drawer state.

Cabinet reads descriptors, rosters, and the known-drawers list through DocsRepo and uses
Keyhive for current access and candidate discovery. The `datum_set` index can wake it when
drawer datums materialize, but it cannot remember an ID whose descriptor has not arrived.
`KeyhiveAccessRevisionStore` wakes it for access changes; Cabinet keeps no second
authoritative access graph. BigRepo's group-part worker projects authority changes into
big_sync parts without proving that bytes exist there.

For a peer, Cabinet answers which drawer parts are worth considering given this node's own
policy and present authority. The embedder (`BigNode` today) picks peers, initiates
big_sync subscriptions, handles retries and backoff, and reports availability back to
Cabinet's catch-up work; a relay embedder additionally handles customer requests, quotas,
and acceptance. XRPC indices sit above Cabinet. Neither a home hint nor another node's
mirror settings let the embedder assume a peer stores all of a drawer's contents.

## Deferred

- subset drawers and subset-intersection sync;
- managed-participant and self-group layouts;
- external roster docs and sharding (design below — deferred means unimplemented, not
  undesigned);
- provider/home datums and node URL design;
- public indices and XRPC;
- sensitivity labels;
- recovery principals;
- Panproto integration.

A mirror is another node's copy of the same drawer, not another drawer identity. None of
the deferred discovery or retention mechanisms changes roster authority.

### Roster sharding (deferred design, restored)

This design predates the authority redesign and was dropped from this ADR when sharding
was deferred to a later PR. Deferring the implementation is not a reason to lose the
design; it is recorded here again so the later PR starts from it, not from scratch.

Cabinet uses a drawer-writer-controlled Automerge roster for official membership. A
drawer has a roster registry document; it identifies the active roster-shard documents.
Each shard holds one membership datum per admitted logical document ID (including an
explicitly admitted branch ID), not one datum containing a giant set. The registry has
one registration datum per shard. Only authorized edits to registered roster shards count
as official entries; a Keyhive grant to the metadata group cannot register an arbitrary
document as a shard. DocumentsRepo defines and enforces the write gate, including how an
Automerge change's actor maps to an authorized writer. A group grant or unverified datum
author is not curator approval.

Every descriptor, roster registry, and registered roster shard grants Read to the
drawer's metadata group M. M's group part is the narrow ongoing subscription through
which readers discover metadata and roster changes, including new shards; it need not
contain or download member-document bytes. Metadata owners may Edit the descriptor;
roster writers may Edit the registry and shards without Edit on the descriptor. An
optional roster-writer group R can carry those Edit grants across all roster documents;
no separate R is needed where the same people edit everything. Simply granting an
arbitrary document Read to M does not authenticate it as a roster shard: it must also
have a valid registry entry. A registered shard may be visible through M before its
registry entry or vice versa; neither order authorizes an unregistered shard, and Cabinet
retries discovery after the missing half arrives.

A self-drawer can keep its initial registry and member datum on the descriptor itself;
there is no requirement to allocate multiple documents just to share one document.

*Reconciliation with the settled self-doc rule:* the paragraph above is the pre-redesign
text and it contradicts "a self-doc drawer cannot grow in place" (Identity and layouts,
above), which is the later, settled position. The settled reading is: growth means a new
single-group drawer with a new `DrawerId`, and the original doc is added as a `docEntry`;
the registry/shard machinery here belongs to single-group drawers whose roster outgrows
one doc, not to self-doc drawers changing layout in place. Further registered
shards bound the size of any one roster document we must hydrate, not the total drawer
history. When a shard's encoded Automerge document size approaches a measured hydration
or sync-cost threshold, roster writers may create another shard; do not use current
visible member count as the limit. Automerge retains historical changes, so ten thousand
live entries can be much smaller or larger than a shard with a long add/remove history. A
provisional 10,000-entry figure is a benchmark workload, **not** an enforced entry cap or
final sharding threshold. Instrument serialized size, load time, and update cost before
choosing a byte threshold; the measurement API may need to be added.

Routing must be deterministic for a given layout: a member DocumentId has one logical
shard, rather than writers arbitrarily choosing any shard with capacity. Splitting a
shard records the predecessor's exact heads and successor ranges/IDs. A returning
old-generation writer reconciles changes since that split frontier into the successors;
chained splits must remain discoverable. Nodes may discard predecessor bytes after
migration; universal indefinite retention is not required. Retain enough
routing/checkpoint information to process late writers that still carry old history.

A plain two-snapshot JSON diff is insufficient where an inherited admission and a
concurrent remove/re-add have the same final value. Migration must preserve the relevant
causal distinction, not turn a concurrent removal into a later sequential delete. The
target is Automerge-style conflict semantics, but the cross-shard algorithm,
claim-lineage transfer, and duplicate physical shard creation rule remain open.
Deterministic routing alone does not solve these. One membership datum per document also
provides authenticated who-added-what provenance; it is not merely a workaround for
rewriting a large map.

In today's datums this design is what `drawer.roster/main`'s `layout` vocabulary is for:
`inline` is the one spelling implemented (the registry and the shard are the drawer doc
itself); external registry/shard spellings arrive with this design. The vocabulary stays
closed — a node that cannot read a drawer's roster layout cannot enumerate the drawer, so
it refuses rather than guessing where entries live.

### Home and provider hints (deferred design)

Drawer writers may publish home-node datums on the descriptor, one per home node
(`DrawerHome { nodeId, addressHints[] }`), for finding the descriptor and its content. A
home is a likely contact point: not an exclusive owner, not a mandatory mirror, and not
proof that every entry's bytes are present. Another node's self-advertisement as a mirror
is a separate, untrusted claim, never a canonical home. Cabinet returns candidate
providers; the embedder attempts connections and byte sync. Node addressing and XRPC
discovery are specified elsewhere.

### Per-node drawer policy (deferred design)

The known-drawers registry above records this node's remembered drawers and its mirror
intent. The fuller design makes each node's policy an ordinary syncable doc
(`NodeDrawerPolicy { nodeId, drawers: map<DrawerId, Know | Mirror | Selected { subset }>,
retentionPreferences? }`) rather than a SQLite-only list or a Keyhive group used as a
setting, so the laptop can mirror more than the phone and the choice survives the node.
A newly authorized node may read another node's policy as advice but never executes that
node's choices as its own; copying policy is an explicit action. A node policy is not an
access grant, not a byte inventory, and not a relay's decision to accept a sponsorship
request: the relay controls its own accepted-retention policy. SQLite holds only derived
indices, durable worker cursors, and pending work.

### Subset drawers and catch-up (deferred design)

Suppose an origin drawer O holds many docs and this node wants only some. It creates a
subset drawer S: an ordinary drawer with its own descriptor, roster, and groups, plus a
`SubsetOf(O)` claim by S's writer. S's selection is S's roster, not every Keyhive edge a
source happens to hold, and S's groups need not mirror the actor grants of O.

Subset authority has three intended policies: origin-private (the default, no published
selection optimization), origin-visible (delegate Relay visibility to O's audience, not
edit authority over S), and origin-derived (delegate the intended authority through O's
group). Delegation follows group membership; it is not a copied roster of principals.
Keyhive metadata reaches only eligible observers, but visibility still leaks the
selection to them, so it is a deliberate choice.

In BigRepo every Keyhive group is mirrored into a big_sync part. To avoid subscribing to
all of O's part and paying its wire cost, S's owner shares a source-visible optimization
group, given visibility to O's audience, that advertises the selected doc IDs as Keyhive
metadata. Any node carrying O can then project a narrow part from docs it already holds,
without downloading S's docs. Visibility into that group gives no reader or writer right
over S. A newly projected old doc must produce a part membership change even though the
doc was not edited. The embedder subscribes to the narrow part, not to every selected
object forever. Small private selections can use bounded object subscriptions and larger
ones can replay O's part; replay exchanges heads, not every entry's bytes. Thresholds and
any large-drawer guard need measurement, and the optimization group is never a
prerequisite for correctness.

Catch-up work exists because a node may put D into its optimization group before S's
owner enters D in S's roster: the part event arrives and its cursor advances while D is
unselected, and the later roster edit produces no new part event. Each newly admitted S
entry therefore schedules durable catch-up for that D. The work does not track origin or
"source" nodes: any node carrying O's bytes can satisfy it, and big_sync subscriptions
are already downstream of which drawers a connected node is authorized for. So when a
node connects, the worker looks at its pending catch-up docs, the subset each belongs to,
and that subset's origin, checks whether the connected node is eligible for the origin,
and if so compares required heads, fetches missing sedimentree content and requested
blobs, and keeps the entry pending until it is actually available. After seeding, the
narrow part carries later changes. The worker never fetches unselected origin docs or
holds thousands of permanent object subscriptions, and an unsolicited edge in the
optimization group cannot enter D in S's roster or cause unbounded fetch work.

Relay sponsorship composes the same design: the customer's selection is a proposal, and
the relay admits only what fits its quota into its own accepted subset and exposes an
optimization group to carriers of the origin. A customer's grants to that group do not
expand the relay's accepted roster. Relay authority permits ciphertext retention and the
relevant Keyhive metadata sync, not decryption; a sponsor needs grants on the accepted
subset's own descriptor and roster to read the accepted set.

An entry can be listed, authorized, projected by a carrier, locally present in `/seds`,
current to the required heads, and retained under policy at different times. A part
cursor acknowledges observed events; it certifies neither adoption nor available bytes.
Removing one subset or sponsorship reason cannot evict bytes another mirror,
sponsorship, or pin still needs. Explicit local eviction edits no roster, though an
active mirror policy will fetch the content again, and revocation may stop future sync
without erasing a local copy: byte lifecycle is a separate decision.

## Verification and follow-up

Before an implementation is considered complete, test (1) grants to `docsGroup` without
roster entries and entries before grants, including revocation; (2) a doc reader who sees
`D → docsGroup` but cannot read the roster; (3) a branch entered explicitly by an
authorized drawer writer; and, with the deferred designs, (4) an old doc entered in a
remotely projected subset, including offline selection and carrier pre-addition; (5)
unsolicited optimization-group edges under a bounded relay quota; and (6) overlapping
local retention reasons and missing bytes despite a replayed cursor.

[ADR 005](005-relay-sponsorship-retention-admission.md) must keep relay request,
acceptance, and retained bytes distinct. [ADR 012](012-bigsync-reconciliation-strategy.md)
must not claim that full-origin replay or an acknowledged part cursor suffices for
attenuated selection catch-up.
