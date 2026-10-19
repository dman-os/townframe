# ADR 014: Cabinet Drawers

**Status:** Proposed

## Scope

[The drawer FDR](../fdrs/001-drawers-as-document-collections.md) defines the
user-visible collection model. This ADR is the **target contract** for the
drawer model Cabinet will implement: the drawer facets, roster, child entries,
and required local drawers below are `DrawerRepo`'s contract, not delivered by
any single PR. The authority slice that has landed — actor identities,
capabilities, the grant ledger, and secret-backed signing — is specified in
[ADR 017](017-authority-actors-capabilities-and-grants.md) and the
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

A single-doc drawer cannot grow in place. To create a collection around it, Cabinet
creates a new single-group drawer with a new `DrawerId` and adds the original doc as a
`docEntry`. The original doc keeps its ID and may remain a single-doc drawer
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

A single-doc drawer is:

```jsonc
// The hosting doc ID is the DrawerId.
{
  "org.example.daybook.drawer/main": {
    "layout": {
      "$type": "org.example.daybook.drawer.singleDoc",
      "value": {}
    }
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
      "value": {
        "metaGroup": "zGroupWorkMeta",
        "docsGroup": "zGroupWorkDocs"
      }
    }
  },

  "org.example.daybook.drawer.roster/main": {},

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

The conventional `drawer.roster/main` facet marks the inline canonical roster. In this PR
all entries live in the drawer metadata doc. External roster docs, ranges, shard routing,
and split records are deferred together.

## Actors and node authority

AuthorityRepo manages actor identities, capabilities, and a generic grant
ledger. Boot returns the operator root capability; a subsystem presents its
"is subsystem" capability here, and DrawerRepo checks the ledger for that actor
before minting a drawer capability of its own (ADR 017). Private signing keys
remain behind `secrets_rs`, and the service returns signatures rather than keys.

Actor capabilities authorize Cabinet operations. BigRepo/Keyhive separately gate what
the node can do. If the node lacks authority, no actor capability bypasses it. Recovery
principals are node-level work deferred to another PR.

The drawer capability vocabulary covers Read, Write, Manage, doc creation,
removal-from-drawer, child addition, and delegation. Removing a doc entry never means
global doc deletion.

## Required local drawers

DocumentsRepo is constructed with two ordinary drawers supplied and managed by the
embedder:

- docs physically present on this node;
- docs originated by this node.

They use the same drawer model and remain local because the embedder does not grant them
to other nodes. AuthorityRepo does not create them. Doc creation always specifies a
target drawer and also enters the new doc in both applicable local drawers, so a doc is
never created outside every drawer.

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
- external roster docs and sharding;
- provider/home facets and node URL design;
- public indices and XRPC;
- sensitivity labels;
- recovery principals;
- Panproto integration.

A mirror is another node's copy of the same drawer, not another drawer identity. None of
the deferred discovery or retention mechanisms changes roster authority.
