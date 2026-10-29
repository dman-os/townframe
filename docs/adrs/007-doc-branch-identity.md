# ADR 007: Doc and Branch Identity

**Status:** Partially implemented — `DocumentId` unification, `db+datum` references, `BranchDoc`/`MergeDoc`, the `org.example.daybook.branch` core datum with concrete parent references and pinned fork heads, and `dmeta.docUuids` provenance across branches and merges are implemented in `big_cabinet_core` and `big_cabinet`. Explicit branch-link removal and physical document deletion remain deferred.

## Context

A branch is implemented as another BigRepo/Automerge doc. The older design added a
second `BranchId` identity, a logical-document layer above the physical docs, branch
publication state, and branch-local authority scope. That duplicated the identity the
hosting doc already has and coupled branching to drawers and authority.

Branches, drawer entries, and Keyhive authority are independent facts. A branch may be
available without its origin, may be admitted to a different drawer, or may remain local.
The origin and branch may also be received independently.

There is also a creation-time cycle. A doc's datums should name the doc's own ID, but
BigRepo used to learn that ID only after creating initial Automerge content. Writing it
afterward needs a second Automerge change and so a second Sedimentree commit. Small docs,
drawer descriptors especially, may otherwise stay at one commit for their whole lifetime,
so avoiding a mandatory second commit is a real storage and sync saving, not cosmetic.

## Decision

### Every branch is a doc

Every ordinary doc and every branch is identified by `DocumentId`. There is no separate
`BranchId` domain type:

```text
ordinary doc       = DocumentId
branch doc         = DocumentId
physical BigRepo ID = DocumentId
```

A branch name or path is a label in branch metadata, not identity. Renaming it does not
change the branch doc ID.

Public APIs address the branch doc directly by `DocumentId`. They do not expose a
logical-document wrapper whose identity differs from the physical doc.

The equality of doc ID and physical BigRepo ID is an implementation invariant, not
permission for Daybook APIs to hand out raw BigRepo, Automerge, Keyhive, or Sedimentree
handles. Public APIs expose Daybook IDs and operations, so the identifiers can stop being
byte-identical under a future backend without application callers having to understand
that migration.

### Branch links are weak, local relationships

A normal branch is a new `DocumentId` whose initial Automerge state is forked (`fork_at`)
from the origin document at the requested heads, and a merge (`MergeDoc`) merges one
history-sharing document's Automerge changes into another (`doc.merge`). Because the
Automerge changes themselves travel between origin and branch, datums are not copied by
value and signatures authored on either document reconstruct at their introducing changes
wherever those changes land (ADR 020 §5). History-truncating compaction is not a branch:
documents that do not share genesis changes cannot be merged (`UnrelatedHistory`).

Creating branch B from doc A at path `/draft` writes B's identity datum on B and, unless
the branch is stealth, announces B on A:

On B, and on A when non-stealth, `org.example.daybook.branch/<B>` holds:

```json
{
  "path": "/draft",
  "from": {
    "datumRef": "db+datum:/<A>/org.example.daybook.branch/<A>",
    "heads": ["<fork-head>"]
  }
}
```

The reference names the concrete parent document, not `self`: the target must remain A
when this datum travels between documents. The pinned heads are the selected fork frontier,
not A's current frontier when the announcement is written. The initial branch change is the
durable source of this value during crash recovery. `from` is optional for root metadata;
like every datum reference it is a weak address and does not assert the parent datum exists.

Keying B's datum by `<B>` (`org.example.daybook.branch/<B>`) rather than `/main` keeps the
datum merge-safe: when B is merged back into A, or a nested branch C is merged into B, the
target acquires the source's `branch/<source-id>` datum without overwriting its own
`branch/<self-id>`. Any document resolves its own branch path by reading
`org.example.daybook.branch/<self-id>`. There is no `branches/main`, `byId`, or `byPath`
map. Per-branch keys preserve concurrent branches with the same label; generic datum-reference
backlinks can discover children through their `from.datumRef`.

Announcing a branch on A (`stealth: false`) mutates A and therefore requires `Write`
access on A (in addition to `Read` on A and `Create` for B's placement); a caller with
only `Read` access on A is refused with `Denied`. Setting `stealth: true` skips the
`org.example.daybook.branch/<B>` write on A and requires only `Read` on A plus
`Create` for B's placement.

The links are weak. Reading either side does not require the other doc to be locally
present, readable, or still available. A branch created from B records the new
relationship in B and the new branch; branch information is not propagated through every
ancestor or sibling.

`BranchDoc` owns `org.example.daybook.branch/*` and signs these datums with `DocsRepo`'s
system actor after
checking the caller's access. Ordinary `CreateDoc` and `WriteDatums` calls cannot
set or remove them (`SystemDatum`), while `ReadDatums` and `ListDatums` read them like any
other datum.

Merging B's Automerge changes into A does not delete B or remove either datum. A merge
is not branch destruction: B remains an ordinary doc with its own ID and history. Any
later lifecycle operation that removes a branch link is explicit and is not implied by a
merge.

### Branches and drawers are orthogonal

Creating a branch does not admit it to any drawer containing its origin. The caller may
explicitly add the new branch doc to a drawer as a separate operation. The origin is not
an authority parent of its branches, and direct grants over the origin do not propagate
through branch metadata.

Consequently branch datums contain neither `BranchPublication` nor `AuthorityScope`.
Publication, retention, and access are drawer and Keyhive concerns.

### Drawers are idiocentric contexts

A `DrawerId` is the `DocumentId` of a doc carrying the drawer descriptor datum (ADR 019),
not a Keyhive group ID. The descriptor names the groups and whatever provider hints its
author knows, but there is no global `DrawerId → provider` or `DrawerId → authority
state` mapping. A peer learns about a drawer through a grant, local configuration, sync,
or an explicit share, and two peers can legitimately know different authority history and
provider sets for the same drawer. Provider identity, current reachability, and
authorization are separate facts: knowing that node N may serve a drawer neither proves
that N is online nor grants the requester access.

One doc may be entered in several drawers. An entry makes the drawer a context through
which the doc is synced, retained, organized, authorized, or resolved; it does not clone
the doc or change its identity.

### Initial identity precedes publication

BigRepo allocates the final doc identity before initial content. DocsRepo can then
write final IDs, initial content, dmeta, and branch metadata in the first Automerge
change, persist its Sedimentree, initialize Keyhive state, and only then expose it through
its requested drawer.

Creation is recoverable and idempotent:

```text
IdentityAllocatedLocally
  -> InitialContentBuilt
  -> SedimentreePersistedLocally
  -> KeyhiveDocInitialized
  -> DrawerAccessEstablished
  -> DrawerEntryPublished
  -> Complete
```

A successful creation never advertises a doc whose initial bytes were not persisted.
The earlier pipeline built the initial heads, then called Keyhive `generate_doc` and
persisted its CGKA and delegation events, and only then persisted the doc: besides
learning the ID too late for the first change, it could publish the doc to its groups
before any peer could retrieve its Sedimentree. Allocation is therefore separate from
publication. `IdentityAllocatedLocally` reserves the final ID and doc keys without
attaching the doc to any intended external group; BigRepo's doc reservation
(`allocate_id` / `abandon_allocation`) and DocsRepo's command ledger are the durable
records recovery reads. A checkout-local Keyhive group may additionally make pending docs
enumerable but must never be the only record. Because the initial Sedimentree is persisted
before Keyhive state exists, learning the doc's authority from Keyhive implies the
creating checkout already stored its first commit. Retrying any completed transition is
safe.

The rules are:

- IDs are globally unique and never reused.
- A pending allocation may be reclaimed only while it has neither local Sedimentree
  content nor replicated Keyhive doc state.
- Once either exists, recovery completes publication instead of reclaiming the ID or
  keys.
- Any Sedimentree content, authority reference, drawer entry, or other durable reference
  prevents reclamation.
- Drawer access and the drawer entry are always the last steps. The initial Keyhive
  creation path never receives drawer groups merely because the create request named a
  drawer.

Allocation, content, and publication are not one distributed transaction. The
requirement is explicit, recoverable intermediate states, so a crash never leaves an
ambiguous object.

### Discovery is contextual

There is no global branch directory. A doc can list branches it created, while local
indexes can project branch links visible to the current node. Receiving a branch directly
does not require first loading an origin-wide directory.

A branch link grants no access, retention, publication, or provider discovery. Missing or
unauthorized linked docs remain unresolved weak references.

### Datum addresses have no URI authority

A datum address uses an absolute path under the `db+datum` scheme:

```text
db+datum:/<doc-id>/<datum-tag>/<datum-key-slot...>
```

There is no URI authority component. Drawers are access/collection contexts and nodes are
providers; neither identifies the datum. The doc ID is the first path component.

`self` is reserved in the doc position for a reference relative to the doc containing the
datum:

```text
db+datum:/self/org.example.daybook.blob/main
```

This lets a datum copied into a branch retain internal references unchanged. `self` is
not a concrete `DocumentId`.

The datum key slot is the entire path remainder after the datum tag, including `/`. For
example, a dpath key slot beginning with `/photos` intentionally produces:

```text
db+datum:/zDoc/org.example.daybook.dpath//photos/beach.jpg
```

A datum address is only an address. It grants no access, implies no retention, and does
not establish that the target exists or is reachable. Node/provider locators are a later,
separate URL design.

Whatever that design adds, each locator form selects one strict resolution strategy (local
knowledge, one drawer context, or bootstrap from one named node) and resolution never
falls through from one strategy to another. A failed drawer-context resolution does not
trigger an implicit global search or reinterpret the drawer as a node; any global naming
system uses its own explicit locator form.

## Consequences

- A new doc or branch is self-identifying in its first Automerge commit, so tiny docs
  avoid an otherwise mandatory second Sedimentree commit.
- BigRepo and DocsRepo carry an explicit allocation, recovery, and reclamation lifecycle.
- Branches use the same doc APIs, authority substrate, and datum machinery as other docs.
- Branch creation and drawer admission remain separately observable operations.
- Branch links tolerate partial replication and asymmetric access.
- Merge does not silently destroy branch identity or metadata.
- Indexes key physical and branch-specific work by `DocumentId`; no compatibility
  `BranchId` wrapper remains.
- Existing branch schemas, ADRs, and indexes must remove logical-document identity,
  publication, and authority-scope fields.

## Deferred

- Explicit branch-link removal or branch-doc deletion policy.
- Node/provider URL syntax and public discovery.
- Future non-Automerge doc representations.
- Retention duration and the reclamation sweep for allocations abandoned before any
  content or Keyhive state existed.
- Any optional global identity or naming overlay.
