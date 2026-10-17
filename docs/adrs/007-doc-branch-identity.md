# ADR 007: Doc and Branch Identity

**Status:** Proposed.

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

Creating branch B from doc A records the relationship in A and B:

```text
A --created branch--> B
B --created from A at heads H--> A
```

The links are weak. Reading either side does not require the other doc to be locally
present, readable, or still available. A branch created from B records the new
relationship in B and the new branch; branch information is not propagated through every
ancestor or sibling.

Branch-management operations own these system facets. Ordinary facet writes cannot
mutate them. Dmeta records their provenance like other privileged facet claims.

Merging B's Automerge changes into A does not delete B or remove the relationship. A merge
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

BigRepo allocates the final doc identity before initial content. DocumentsRepo can then
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
