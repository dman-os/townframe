// FIXME: this dropped earlier design!

# FDR 001: Drawers as Doc Collections

**Status:** Accepted — functional design; self-doc and single-group drawers, child
entries, actor grants and local drawers are implemented by `DrawerService` (ADR 019)

## Purpose

Drawers are Daybook's primary user-facing collection, access, and sync context. They are
built over docs, Keyhive groups, and BigRepo, but users should not need to manipulate
those primitives directly.

A drawer is not a node, relay account, Keyhive group, complete local copy, or search
index. `DrawerId` is the `DocumentId` of the doc carrying the drawer descriptor.

## Vocabulary

- **Doc:** a Daybook doc with a stable ID, datums, and history. A branch is another doc
  with its own ID and CRDT history.
- **Drawer:** a doc carrying drawer metadata. Its doc ID is its drawer ID.
- **Roster:** the authoritative set of docs and child drawers officially included by a
  drawer writer.
- **Entry:** a doc or child drawer listed in the roster. Reserve *member* for actors,
  principals, and Keyhive group membership.
- **Provider:** a node that may offer descriptor resolution, bytes, sync, indexing,
  search, or retention. Providing one service implies none of the others.
- **Mirror:** another node's retained copy of the same drawer, not another drawer ID.

Official roster inclusion, effective Keyhive access, and byte availability are separate
facts. A reachable doc is not automatically an official entry; an official entry can be
unavailable or narrower than intended; an authorized node may not yet hold its bytes.

## Independence rules

1. A doc can be entered in several drawers without being cloned or changing identity.
   Removing it from one drawer does not remove it from another or delete it.
2. A branch is an ordinary doc. Creating it does not enter it in its origin's drawers.
3. A drawer writer does not thereby own every entered doc. The writer can arrange only
   authority it is permitted to delegate.
4. Access through one drawer does not cap a stronger independent path through another.
5. Knowing an ID, reading metadata, enumerating entries, fetching bytes, decrypting,
   writing, and retaining ciphertext are distinct abilities.
6. Drawer and branch references are weak: their targets can be missing, inaccessible, or
   available only through another provider.
7. Removing an entry is not global doc deletion.

## Supported drawer layouts

### Single-doc drawer

A doc may also be a drawer whose only entry is itself. It carries drawer metadata beside
its ordinary content and requires no separate collection groups or roster entries. This
is the inexpensive one-doc sharing case.

A self-doc drawer cannot grow in place. If the user wants a collection around it,
Daybook creates a new single-group drawer with a new drawer ID and enters the original
doc. The original retains its doc ID and may remain independently shareable.

### Single-group drawer

A collection drawer is a separate metadata doc with:

- a meta group governing the descriptor and canonical roster;
- a docs group receiving delegated access over entered docs;
- an inline roster of doc and child entries.

Self-group drawers, managed-participant layouts, and external roster sharding are not in
this PR.

## Doc entries

Adding a doc records both desired inclusion and desired access:

```jsonc
{
  "org.example.daybook.drawer.docEntry/zDocA": {
    "access": "Write"
  }
}
```

The key identifies the doc. `access` is `Relay`, `Read`, `Write`, or `Admin` using
Keyhive's vocabulary. The roster states what the drawer intends; Keyhive states what is
effective.

The normal add establishes the requested docs-group grant and then publishes the roster
entry. Removal first removes the entry and then revokes the grant where authorized. A
partial operation remains visible and retryable. An unlisted grant is only an accessible
candidate; a listed entry with no effective grant remains official but unavailable.

## Child drawers

A child drawer is a typed roster entry, not a separate relationship object:

```jsonc
{
  "org.example.daybook.drawer.childEntry/zDrawerEngineering": {
    "access": "Read"
  }
}
```

The child does not need to know every parent that lists it. Adding the entry arranges the
necessary group delegations before publishing the entry. The single access choice has
these user-visible meanings:

| UI intent | Stored access | Result |
| --- | --- | --- |
| Retain | `Relay` | Enumerate the canonical child roster where readable and retain child doc ciphertext. |
| View | `Read` | Readable set union of the parent and child docs. This is the default. |
| Contribute | `Write` | Set union in which parent writers can also modify the child where authority permits. |
| Manage | `Admin` | Extend administrative authority over the child meta and docs groups. |

`Read`, `Write`, and `Admin` apply to the child meta and docs groups. `Relay` uses Read for
child metadata, because canonical enumeration requires it, and Relay for child docs.
Every actor remains bounded by its own path through the parent.

There is no docs-only child mode: without the canonical roster, reachable Keyhive objects
cannot be identified as official child entries. Recursive traversal terminates at already
visited drawer IDs and uses the strongest effective access when a doc is reachable by
several paths.

## Local system drawers

Every node has two ordinary drawers, created by the drawer service on its first boot:

- docs physically present on this node;
- docs originated by this node.

They use the same model and remain local because they are not granted to other nodes.
Creating a doc always names a target drawer and also enters it into both local drawers.
The authority service does not create or own these drawers.

## Common operations

| Intent | User-visible effect |
| --- | --- |
| Create self-doc drawer | Make one existing doc independently shareable under its own ID. |
| Create collection | Create a new drawer metadata doc and stable drawer ID. |
| Add or remove doc | Change this roster only; preserve doc identity and other drawer entries. |
| Add child | Union the child into the collection at Relay, Read, Write, or Admin. |
| Invite or revoke actor | Change actor capability and effective drawer authority without rewriting every doc. |
| Open doc | Report exact accepted content or distinguish absent, unauthorized, unmaterialized, and unreachable states. |
| Retain locally | Fetch and retain selected bytes without changing official inclusion. |

Operations that touch Automerge roster state and Keyhive authority are not globally
atomic. Success must not be reported until the requested effects are established;
interrupted work remains inspectable and retryable.

## Actors and capabilities

Actors are persistent subsystem identities authorized through capabilities
(ADR 022). The operator holds every right and opens the other actors. Delegation is
not implicit in actor names or paths: an object capability is named by DrawerService from
its own actor capability, a holder can only drop rights from it, and a child's rights
can only shrink. Durable drawer grants are rows in DrawerService's ledger namespace keyed
by subject actor and object (`write drawer D`), so revoking one never rewrites a
document.

Private actor and node keys remain behind the authority service, sealed under a
`secrets_rs` keyring key. Callers request a signature rather than receiving a private
key. Actor capability answers whether Cabinet
may perform an operation for that actor; BigRepo/Keyhive independently enforces whether
the node can perform it.

The ordinary product vocabulary should say view, contribute, manage, create doc, remove
from drawer, add child, and delegate rather than expose arbitrary Keyhive graph editing.

## Branches

A branch is a new doc created from another doc at selected heads. Only the origin and new
branch record that weak relationship; it does not propagate through all related docs.
Merging changes back does not delete the branch or its link.

Branch creation does not imply drawer admission. A user can explicitly add the new branch
to any drawer they are authorized to edit.

## Discovery, public access, and bytes

A drawer ID does not identify a node. Provider and home-node discovery, XRPC, public
indices, and provider URL syntax are separate designs. A provider hint would not prove
authority, completeness, freshness, or availability.

A node can know drawer metadata without storing all entered docs or blobs. Relay access
permits ciphertext retention without plaintext access. Local byte retention and eviction
are node policy and do not edit the roster.

## Deferred

The following are future PRs rather than alternative meanings of this model:

- subset drawers and selective intersection sync;
- managed-participant and self-group layouts;
- external roster docs, sharding, ranges, and split migration;
- provider/home datums and public/provider indices;
- XRPC and node/provider URL design;
- sensitivity labels and information-flow policy;
- recovery principals;
- Panproto integration.

These features must compose with the roster/access/availability distinction rather than
replace it.
