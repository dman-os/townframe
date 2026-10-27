# ADR 019: Cabinet Drawers

**Status:** Accepted — implemented by `DrawerService` (`src/big_cabinet/drawer.rs`)
for the self-doc and single-group layouts; the deferred list at the end is unchanged.

## Scope

[The drawer FDR](../fdrs/001-drawers-as-document-collections.md) defines the
user-visible collection model. This ADR is the contract for the drawer model
Cabinet implements: the drawer facets, roster, child entries, and required
local drawers below are `DrawerService`'s contract. Namespace capabilities, actor identities,
domain-owned grants, signing, and their implementation migration are specified in
[ADR 022](022-authority-actors-capabilities-and-grants.md) and the
[authority guide](../big-cabinet-authority.md). Subset drawers, managed-participant
layouts, roster sharding, provider/home facets, and Panproto integration are
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

A child drawer is a typed roster entry, not a separate `DrawerRelation` facet:

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

## Facets

The first-version vocabulary is:

```text
org.example.daybook.drawer
org.example.daybook.drawer.roster
org.example.daybook.drawer.docEntry
org.example.daybook.drawer.childEntry
```

Facet tags are grouped by dotted names. Drawer values carry no ad-hoc version integer;
compatible evolution occurs under the facet identity, while incompatible meanings need a
new identity.

The layout is an open union, internally tagged as every published union is (ADR 021 §5):
the member object carries `$type` beside its own fields. Carrying is not operating — a
node that does not know a layout still reads, stores and lists the drawer untouched; only
an operation that must interpret the layout refuses. A Lexicon object states at least one
property, so a member with nothing to say carries `empty: true` — the convention for an
empty member, excluded from the wire by `skip_serializing_if` until a later field lands
beside it. Adding a layout is a new member under the same facet id, not a new facet tag.

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

The `drawer.roster/main` facet marks the canonical roster and states where its entries
live. A Lexicon object states at least one property, so the marker is not `{}`: it
carries `empty: true`, the empty-member convention. Today every entry is a facet on the
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
A self-doc drawer needs Write on the doc instead of collection creation authority.
Creation records the originating actor independently from the bound signer, and
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
`org.example.daybook.drawer.knownEntry/<drawerId>` facets (`{ mirror?: true }`). It is an
explicitly managed list, not a collection and not a tag scan: remembering a drawer grants
no access to it, mirroring intent certifies no fetched or retained bytes, forgetting edits
nothing but the local registry, and child relationships or observed facets never write it.
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

`facet_set` and `facet_ref` move from daybook_core into Cabinet as generic projections;
blob-specific behavior is removed. SQLite tables and transitive-index strategies are
implementation details, not authoritative drawer state.

## Deferred

- subset drawers and subset-intersection sync;
- managed-participant and self-group layouts;
- external roster docs and sharding (design below — deferred means unimplemented, not
  undesigned);
- provider/home facets and node URL design;
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
Each shard holds one membership facet per admitted logical document ID (including an
explicitly admitted branch ID), not one facet containing a giant set. The registry has
one registration facet per shard. Only authorized edits to registered roster shards count
as official entries; a Keyhive grant to the metadata group cannot register an arbitrary
document as a shard. DocumentsRepo defines and enforces the write gate, including how an
Automerge change's actor maps to an authorized writer. A group grant or unverified facet
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

A self-drawer can keep its initial registry and member facet on the descriptor itself;
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
Deterministic routing alone does not solve these. One membership facet per document also
provides authenticated who-added-what provenance; it is not merely a workaround for
rewriting a large map.

In today's facets this design is what `drawer.roster/main`'s `layout` vocabulary is for:
`inline` is the one spelling implemented (the registry and the shard are the drawer doc
itself); external registry/shard spellings arrive with this design. The vocabulary stays
closed — a node that cannot read a drawer's roster layout cannot enumerate the drawer, so
it refuses rather than guessing where entries live.
