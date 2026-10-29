# FDR 001: Drawers as Doc Collections

**Status:** Accepted — functional design; self-doc and single-group drawers, child
entries, actor grants and local drawers are implemented by `DrawerService` (ADR 019)

## Purpose

Drawers are Daybook's primary user-facing collection, access, and sync context. They are
built over docs, Keyhive groups, and BigRepo, but users should not need to manipulate
those primitives directly.

A drawer is not a node, relay account, Keyhive group, complete local copy, or search
index. `DrawerId` is the `DocumentId` of the doc carrying the drawer descriptor. A drawer
can be useful at the scale of one doc, a family library, a plug registry, or a large
public corpus.

This FDR is downstream of [the drawer wiki](../wiki/drawer.md), which describes the
drawer concept and explores how it lowers onto Keyhive and BigRepo. This document
specifies what people and client applications need to observe; if a functional decision
here changes the drawer model, update the wiki first. The technical ADRs (019, 020, 022)
choose authority layouts, the durable roster representation, sync and indexing
machinery, crate boundaries, and transports, and are evaluated against the use cases
below rather than redefining a drawer as one Keyhive group layout or one node's index.

## Vocabulary

- **Doc:** a Daybook doc with a stable ID, datums, and history. A branch is another doc
  with its own ID and CRDT history.
- **Drawer:** a doc carrying drawer metadata. Its doc ID is its drawer ID.
- **Roster:** the authoritative set of docs and child drawers officially included by a
  drawer writer.
- **Entry:** a doc or child drawer listed in the roster. Reserve *member* for actors,
  principals, and Keyhive group membership.
- **Principal:** an identity or authenticated actor Cabinet can recognize, such as a
  person, device, node, relay, or plug execution. Cabinet-facing identity is not defined
  as a Keyhive ID (ADR 022).
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
8. A drawer may be listed by several parents and may participate in cycles. Navigating
   a child entry implies access to neither endpoint nor to all of its contents, and the
   child need not know every parent that lists it.
9. Metadata and authority can arrive without CRDT or blob bytes. An accessible doc can
   still be pending, unavailable, or only partially retained.
10. Drawers use Keyhive for the primary authority and collection path, but users and
    XRPC-only clients work in terms of Cabinet docs, drawers, principals, and versions.
    Cabinet must not promise that a drawer, a label, or a plug sandbox revokes access a
    principal already holds through another path.

## Use cases

These cases fix the product behavior. Each names the facts a user must be able to tell
apart; where a case depends on unbuilt machinery it is marked **Deferred**, but the
behavior it states still constrains the design.

### 1. Personal multi-device cabinet

A user keeps private notes, records, and media across a phone, laptop, and home server.
They create several drawers, work offline, add a device, and edit docs without changing
drawer or doc identity. The same doc can be entered in several drawers. A device may
know about a drawer but retain only selected docs and blobs: syncing drawer metadata
must not download a whole media library. A relay can be used for availability without
receiving plaintext access.

### 2. One-doc share that grows

A user shares one doc without exposing the rest of their cabinet. The doc carries drawer
metadata itself, so the share is an ordinary self-doc drawer addressed by the doc's ID.
If the user later wants a collection around it, Daybook creates a single-group drawer
with a new drawer ID and enters the original doc; the original keeps its ID, its
recipients' references stay valid, and it remains independently shareable. Growth must
not silently widen any recipient's rights: Cabinet must state the new collection's
effective scope before anyone is granted it.

### 3. Family media library

Several people share photos and videos. They invite and remove people, add media, browse
together, organize albums as child drawers, and retain different amounts on different
devices. Blobs are reachable through their docs' references and follow the relevant
access and local-retention decisions. A family relay retains ciphertext but cannot
decrypt it. One photo can also be entered in a separate public drawer without
duplicating its logical identity.

The default experience describes familiar actions (view, contribute, manage access)
rather than exposing every Keyhive delegation or asking the family to choose an advanced
authority layout.

### 4. Shared workplace project and child drawers

A project has a common drawer and independently governed team drawers, and a doc can be
entered in both. A child entry gives parent readers a navigation path and, at the access
the entry states, a delegation over the child's groups; the delegation is arranged by
someone authorized to make it, never implied by the entry alone. A private team drawer
can be invisible or inaccessible to some parent members, and a child need not know every
parent that lists it.

Small projects may let contributors organize and add docs. Large projects may separate
contribution, roster curation, and access management. These are meaningful product
actions, not a promise that each maps to a distinct Keyhive access level.

### 5. Entering someone else's doc

Alice finds a doc controlled by Bob and enters it in a drawer she writes. She does not
need Admin over Bob's doc. With Read she can make it readable to the drawer's audience
only to the extent her authority allows; a retention-oriented drawer might get only
Relay. Entering it implies neither that Alice can change its content or control its
members nor that its bytes will always be served.

Cabinet distinguishes the intention to include a doc (the roster entry) from effective
access and availability. If Bob revokes an authority path or no provider retains the
bytes, readers see an intelligible unavailable or unauthorized state rather than a
silently missing entry. Entering a public doc as a reference should not create an
independent, durable authority path if none is needed.

### 6. Relay sponsorship

**Deferred.** A user requests retention for docs from one or several drawers, possibly
including a third-party doc for which they hold only Relay. The sponsorship collection is
not the collaboration drawer, and a relay gains no Read, Write, or Admin by sponsoring
content. The request, effective Relay authority, actual byte delivery, and the relay's
adoption under quota are separate statuses. Sponsorship may cover child drawers where a
child entry's delegation permits it; listing a child alone promises no coverage.

The initiating node must arrange for the relay to obtain bytes from an authorized
provider; learning a doc ID or obtaining a grant does not transfer bytes. A failed or
limited adoption is visible, and removing sponsorship neither deletes the doc nor removes
it from any drawer.

### 7. Chat and other low-latency collections

A conversation can use a drawer to scope docs and participants. Creating a message or
editing its doc must not wait on drawer-wide enumeration, full mirroring, or index
rebuilds; the ordinary small-drawer path must stay practical for frequent small
operations. Performance budgets and storage layout belong to the ADRs.

### 8. Public plug registry

An official registry is a public drawer of docs carrying plug-manifest datums (ADR 007).
Anyone can browse and search through a provider's index, inspect a manifest at an
identified version, and fetch or install selected content without mirroring every
manifest, blob, or authority event. The operator can advertise likely providers;
independent mirrors can serve the same public material without editing the official
descriptor.

Official admission is distinct from offering a doc for inclusion. Contributors may
publish a submission in their own or a staging drawer, while registry writers decide
what is entered in the official roster. A catalog cannot treat arbitrary third-party
grants as curator-approved entries, and users must be able to tell official listings,
submissions, and third-party mirrors apart. A node providing public listing or search is
a service provider, not necessarily an ATProto PDS. Provider indices are **Deferred**.

### 9. Large public corpus

**Deferred.** A reader discovers a Wikipedia-scale drawer, searches and pages through a
provider's index, opens individual docs, and pins only a useful subset, without ingesting
the full Keyhive graph or mirroring every doc. Different providers may hold different
subsets or indexed versions, so a service must identify which doc version it returned;
"latest" means the latest accepted version known to that provider, not a global total
order.

Public Read is granted on published docs, not on the drawer's groups: Keyhive's publicly
known signing principal can subdelegate at its held level, so a publicly readable group
is unsuitable as an anti-spam roster. Public doc grants alone do not enumerate a corpus,
so a public provider must offer a listing service for discovery. Whether that listing is
independently verifiable, replicated for offline use, or backed by a curator-controlled
catalog is for the ADRs. XRPC suits large public indices; it is not the primary path for
doc edits and sync. A node that cannot enumerate a public drawer must not claim that
capability merely because it can fetch a doc by ID.

### 10. Proposal and pull-request workflow

A public corpus grants the public Read but not Write on its accepted content. A
contributor creates a proposal doc, a branch doc carrying proposed changes, or both, in a
separately governed staging drawer. Each branch has its own ID and is not entered in its
origin's drawers automatically. A curator reviews the proposal, merges accepted changes,
or explicitly enters a new doc or branch in the authoritative drawer. Rejecting a
proposal does not modify the corpus, and the proposal's retention is separate from the
target doc's lifecycle.

### 11. Private selective mirror and published subset

**Deferred** (subset drawers). A node can retain a private selection from a large drawer
without telling the origin or gaining authority to edit it. If the selection itself
should be named, shared, indexed, sponsored, or synced with other nodes, its keeper
creates an ordinary drawer whose roster enters those docs; the source need not know or
host it. A subset claim is visible where it is published and alters neither the source
drawer's identity nor doc ownership. Maintaining the subset should select docs from the
source without requiring a full source mirror; the intersection-sync mechanism belongs in
an ADR.

### 12. Plug sandbox and information flow

Installing a plug must not give it the whole cabinet. A user grants it a selected drawer,
or creates a narrower plug drawer and enters the desired docs there. Within one drawer a
grant has uniform, understandable semantics; separate drawers express different scopes.
Granting a plug access must show what it may read, write, and send outside the node, and
whether an independent path already gives it more.

The system should be able to flag possible flows such as sensitive drawer → plug A →
shared output drawer → plug B → network egress (**Deferred**). Labels and warnings cannot
stop a plug from copying data it has already read; egress confinement and
declassification need a separate security design. A local plug need not be a globally
replicated Keyhive principal to use Cabinet: the actor that signs produced datums, the
executing plug, and the node authorized to sync them need not share one identity
(ADR 022).

### 13. Distributed processor

A processor may observe an authorized doc and execute on another capable node while the
origin is offline. Offering a task does not grant access to its input drawer. Cabinet
accounts separately for input access, the executable plug and version, executor
authorization, signed output provenance, and authority to write the destination drawer.
Users see when work is waiting on data, authority, or a provider, rather than reading a
task's existence as completed processing.

### 14. Several providers and unannounced mirrors

A drawer may be known from a personal node, a community mirror, and several relays, each
with different reachability and services. Drawer metadata can point to likely or endorsed
providers but is not an exhaustive directory (**Deferred**: provider and home datums). An
unannounced mirror of public material needs neither write access to the descriptor nor
the origin's approval. A provider announcement proves neither authorization, byte
completeness, freshness, nor online status, and nodes may change transports and
addresses without changing the drawer ID.

A node's advertised and observed capabilities (descriptor resolution, catalog queries,
search, direct sync, doc retrieval, relay retention) are distinguishable. Access is
checked per operation; reading a public listing does not imply authority to edit its
docs.

### 15. XRPC-only client and third-party index

**Deferred.** A thin client can discover a drawer, query a node's public or authenticated
indices, and request a JSON projection of a specific doc version without running Keyhive,
BigRepo, or Automerge. A node may expose drawer-related XRPC methods, unrelated ones,
both, or neither, and a public index may be operated independently of the origin. A
response carries its provider and version context so a client can tell a projection from
authenticated sync or a globally authoritative answer.

Lexicons describe Daybook data and RPC shapes (ADR 021); XRPC is a node API convention.
Hosting an ATProto PDS on nodes or relays is a separate feature, neither a prerequisite
nor a consequence of using Lexicons or XRPC.

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
| Add or remove doc | Change this roster only; preserve doc identity and other drawer entries; report insufficient authority or pending bytes explicitly. |
| Add child | Union the child into the collection at Relay, Read, Write, or Admin. |
| Invite or revoke actor | Change actor capability and effective drawer authority without rewriting every doc. Show the effective scope and any independent paths: revoking one drawer cannot promise to remove access held elsewhere or erase data already downloaded. |
| Know a drawer | Keep its identity and available metadata without promising content retention. |
| Open doc | Report exact accepted content or distinguish absent, unauthorized, unmaterialized, and unreachable states. |
| Retain locally | Fetch and retain selected bytes as they arrive, showing progress, failures, and storage cost, without changing official inclusion. |
| Publish subset (**Deferred**) | Share a new ordinary drawer entering selected doc IDs, leaving the source unchanged. |
| Sponsor on relay (**Deferred**) | Request retention; show accepted versus pending or rejected docs rather than reporting the request as a completed backup. |

These are intents, not single atomic Keyhive or storage operations. Operations that touch
Automerge roster state and Keyhive authority are not globally atomic. Success must not be
reported until the requested effects are established; interrupted work remains
inspectable and retryable. Both clients make destructive or authority-widening
consequences explicit rather than hiding them behind a generic "share" control.

### CLI and graphical flows

The GUI and CLI offer the same meaningful operations, even where their interaction
differs. The CLI makes the target and consequence of each action visible before changing
authority or retention: create a drawer; enter an existing doc or a branch by its own
ID; add a child at a stated access; inspect who has access and through which drawer;
request local retention; then inspect pending, accepted, and unavailable entries. Command
spellings are not fixed here. Scripts need stable IDs, structured status, and
distinguishable failure outcomes rather than parsing UI prose.

The GUI offers the same actions through a drawer's entry list, child navigation, an
access panel, and per-node availability controls. A share dialog previews whether it
grants doc access, adds a child entry, or asks a relay to retain ciphertext. A drawer
known only by its descriptor must not appear as if its entries were retained, and a
public drawer shows its provider and the version of each fetched doc. Neither client
labels a relay request "backed up" before adoption and bytes are confirmed.

## User-facing access vocabulary

The simple experience describes **view**, **contribute**, and **manage** as actions, not
raw Keyhive levels as product roles. An advanced drawer may separate curating the
official roster from contributing submissions, managing people's access, and recovery
ownership. Relay, Read, Write, and Admin remain the effective-authority and diagnostic
terms, but a drawer role does not guarantee identical authority on every entered
third-party doc.

A person invited to a drawer is told what they can do with its descriptor, entered docs,
and child entries, and what they **cannot** do where an entered doc carries narrower
authority. A curator's right to organize the roster is not ownership of its docs, and a
reader with an independent stronger path elsewhere cannot truthfully be described as
globally read-only.

Keyhive Read holders can subdelegate Read, so a curator-only roster is never presented as
enforced merely because the public was given Read on the same group. Which actions are
cryptographically enforced rather than mediated by Cabinet is decided per action in
ADR 019 and ADR 022.

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
