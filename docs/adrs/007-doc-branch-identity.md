# ADR 007: Doc and Branch Identity

**Status:** Partially implemented — `DocumentId` unification, `db+facet` references, `BranchDoc`/`MergeDoc`, the `org.example.daybook.branch` and `org.example.daybook.branches` core facets, and `dmeta.branchUuids` provenance across branches and merges are implemented in `big_cabinet_core` and `big_cabinet`. Explicit branch-link removal and physical document deletion remain deferred.

## Context

A branch is implemented as another BigRepo/Automerge doc. The older design added a
second `BranchId` identity, a logical-document layer above the physical docs, branch
publication state, and branch-local authority scope. That duplicated the identity the
hosting doc already has and coupled branching to drawers and authority.

Branches, drawer entries, and Keyhive authority are independent facts. A branch may be
available without its origin, may be admitted to a different drawer, or may remain local.
The origin and branch may also be received independently.

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

### Branch links are weak, local relationships

A normal branch is a new `DocumentId` whose initial Automerge state is forked (`fork_at`)
from the origin document at the requested heads, and a merge (`MergeDoc`) merges one
history-sharing document's Automerge changes into another (`doc.merge`). Because the
Automerge changes themselves travel between origin and branch, facets are not copied by
value and signatures authored on either document reconstruct at their introducing changes
wherever those changes land (ADR 015 §5). History-truncating compaction is not a branch:
documents that do not share genesis changes cannot be merged (`UnrelatedHistory`).

Creating branch B from doc A at path `/draft` writes B's identity facet on B and, unless
the branch is stealth, announces B on A:

```json
// On B: org.example.daybook.branch/<B>
{ "path": "/draft" }

// On A (when stealth = false): org.example.daybook.branches/main
{
  "byId": {
    "<B>": { "path": "/draft" }
  },
  "byPath": {
    "/draft": ["<B>"]
  }
}
```

Keying B's facet by `<B>` (`org.example.daybook.branch/<B>`) rather than `/main` keeps the
facet merge-safe: when B is merged back into A, or a nested branch C is merged into B, the
target acquires the source's `branch/<source-id>` facet without overwriting its own
`branch/<self-id>`. Any document resolves its own branch path by reading
`org.example.daybook.branch/<self-id>`. `byPath` maps a path to a sorted list of branch
`DocumentId`s so concurrent branches taken under the same path converge without losing
either link.

Announcing a branch on A (`stealth: false`) mutates A and therefore requires `Write`
access on A (in addition to `Read` on A and `Create` for B's placement); a caller with
only `Read` access on A is refused with `Denied`. Setting `stealth: true` skips the
`org.example.daybook.branches/main` write on A and requires only `Read` on A plus
`Create` for B's placement.

The links are weak. Reading either side does not require the other doc to be locally
present, readable, or still available. A branch created from B records the new
relationship in B and the new branch; branch information is not propagated through every
ancestor or sibling.

`BranchDoc` owns both system facets (`org.example.daybook.branch/*` and
`org.example.daybook.branches/*`) and signs them with `DocsRepo`'s system actor after
checking the caller's access. Ordinary `CreateDoc` and `WriteFacets` calls cannot
set or remove them (`SystemFacet`), while `ReadFacets` and `ListFacets` read them like any
other facet.

Merging B's Automerge changes into A does not delete B or remove either facet. A merge
is not branch destruction: B remains an ordinary doc with its own ID and history. Any
later lifecycle operation that removes a branch link is explicit and is not implied by a
merge.

### Branches and drawers are orthogonal

Creating a branch does not admit it to any drawer containing its origin. The caller may
explicitly add the new branch doc to a drawer as a separate operation. The origin is not
an authority parent of its branches, and direct grants over the origin do not propagate
through branch metadata.

Consequently branch facets contain neither `BranchPublication` nor `AuthorityScope`.
Publication, retention, and access are drawer and Keyhive concerns.

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
IDs are globally unique and never reused. A pending allocation with durable content or
Keyhive state is completed rather than reclaimed.

### Discovery is contextual

There is no global branch directory. A doc can list branches it created, while local
indexes can project branch links visible to the current node. Receiving a branch directly
does not require first loading an origin-wide directory.

A branch link grants no access, retention, publication, or provider discovery. Missing or
unauthorized linked docs remain unresolved weak references.

### Facet addresses have no URI authority

A facet address uses an absolute path under the `db+facet` scheme:

```text
db+facet:/<doc-id>/<facet-tag>/<facet-key-id...>
```

There is no URI authority component. Drawers are access/collection contexts and nodes are
providers; neither identifies the facet. The doc ID is the first path component.

`self` is reserved in the doc position for a reference relative to the doc containing the
facet:

```text
db+facet:/self/org.example.daybook.blob/main
```

This lets a facet copied into a branch retain internal references unchanged. `self` is
not a concrete `DocumentId`.

The facet key ID is the entire path remainder after the facet tag, including `/`. For
example, a dpath key ID beginning with `/photos` intentionally produces:

```text
db+facet:/zDoc/org.example.daybook.dpath//photos/beach.jpg
```

A facet address is only an address. It grants no access, implies no retention, and does
not establish that the target exists or is reachable. Node/provider locators are a later,
separate URL design.

## Consequences

- Branches use the same doc APIs, authority substrate, and facet machinery as other docs.
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
