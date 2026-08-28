# ADR 007: Logical Document, Branch, and Drawer Identity

**Status:** Proposed.

## Context

Daybook presents logical documents, branches, and drawers. The current storage
stack presents Automerge documents, BigRepo documents, Keyhive authority
documents and groups, and Sedimentree history. Those physical concepts are
useful implementation machinery, but they must not define the stable application
API: Daybook may eventually support non-Automerge documents or a different
authority system.

The existing drawer-wide document map also combines several independent
concerns:

- the identity of a logical document;
- the physical object containing each branch;
- mutable branch names and discovery;
- drawer membership and authority;
- lookup state used by repository projections.

That coupling becomes incorrect once one document belongs to multiple drawers, a
branch has a narrower authority scope than its main branch, or a client receives
a branch directly without first loading a drawer-wide index.

There is also a creation-time cycle. A branch should identify the physical
BigRepo document that contains it, but BigRepo currently learns that identifier
only after creating initial Automerge content. Writing the identifier afterward
requires a second Automerge change and therefore a second Sedimentree commit.
Small documents, especially drawer descriptors, may otherwise remain at one
commit for their entire lifetime. Avoiding a mandatory second commit is
therefore a meaningful storage and synchronization optimization, not merely
cosmetic.

## Terminology

- **Physical document:** the BigRepo object that stores one branch's content and
  history.
- **Branch ID:** the physical document ID of a branch.
- **Document ID:** the Branch ID of the logical document's main branch.
- **Branch directory:** system metadata on the main branch that declares
  discoverable shared branches.
- **Branch identity:** system metadata on every branch that identifies the
  branch and its logical document.
- **Drawer:** an idiocentric authority, synchronization, retention,
  organization, and resolution context described by a logical document.
- **Accepted logical view:** the branch and drawer relationships that pass
  structural, provenance, and authority validation. Replicated bytes are not
  necessarily accepted claims.

## Decision

### 1. Logical and physical branch identity coincide

Every `BranchId` is the physical BigRepo document ID containing that branch. The
main branch's `BranchId` is also its logical `DocumentId`:

```text
DocumentId = main BranchId = main physical document ID
BranchId   = branch physical document ID
```

This equality is an implementation invariant, not permission for stable Daybook
APIs to expose raw BigRepo, Automerge, Keyhive, or Sedimentree handles. Public
APIs expose logical Daybook IDs and operations. The identifiers may cease to be
byte-identical under a future backend without forcing application callers to
understand that migration.

A branch can be addressed directly by its `BranchId`. Resolving the hierarchy
“branch B belongs to document D” is only necessary for operations that need
logical-document context, such as listing siblings, applying inherited
authority, or merging into the main branch.

Branch names are optional and mutable. They are relationships in a directory,
not identifiers. Renaming a branch never changes its `BranchId`.

### 2. BigRepo allocates IDs before initial content

BigRepo will support allocation of a physical document ID before the first
Automerge change. The creator can therefore place the final `DocumentId` and
`BranchId` in system metadata in the same change that creates the user content.

The current pipeline does not provide this guarantee. It first constructs the
initial Automerge heads, then calls Keyhive `generate_doc(parents, heads)` and
persists the resulting CGKA and delegation events, and only afterward sends
`PutDoc` to persist and materialize the physical document. Besides learning the
ID too late to include it in the first change, this can publish the document to
its intended drawer groups before any peer can retrieve its Sedimentree.

Creation will instead separate local identity allocation from authority
publication and use an idempotent state machine:

```text
IdentityAllocatedLocally
  -> InitialContentBuilt
  -> SedimentreePersistedLocally
  -> KeyhiveDocumentInitialized
  -> IntendedGroupsAttached
  -> Complete
```

`IdentityAllocatedLocally` allocates the final document ID and document keys
without publishing the document into any intended external group. A local SQLite
pending-allocation table is the durable source of truth for recovery and garbage
collection. A checkout-local Keyhive group may additionally make pending
documents enumerable, but must not be the sole record.

After allocation, the creator builds one initial Automerge change containing the
final IDs and persists its Sedimentree locally. Only then may BigRepo initialize
the replicated Keyhive document state and attach it to the drawer or other
intended parent groups. Consequently, learning the authority relationship from
Keyhive implies that the creating checkout had already stored the initial
Sedimentree. Retrying any completed transition must be safe.

The following rules apply:

- IDs are globally unique and are never reused.
- Garbage collection may remove an expired pending allocation only when it has
  neither local Sedimentree content nor replicated Keyhive document state.
- Once local Sedimentree content or Keyhive document state exists, recovery
  completes publication instead of reclaiming the ID or keys.
- Any Sedimentree content, authority reference, branch-directory reference, or
  other durable reference prevents collection.
- Intended group attachment is always the final publication step. The initial
  Keyhive creation path must not receive drawer groups merely because the
  higher-level create operation requested them.

This ADR does not require the allocation and content stages to be one
distributed transaction. It requires explicit, recoverable intermediate states
so crashes do not produce ambiguous objects.

### 3. Branch topology is stored in system-managed facets

Branch topology is durable document metadata and must synchronize across
checkouts. It will use the normal facet representation and read APIs, while
writes are reserved to branch-management operations.

Every branch contains a `daybook.branch` facet with at least:

```text
BranchIdentity {
    document_id: DocumentId,
    branch_id: BranchId,
    created_from: optional BranchVersion,
}
```

The main branch contains a `daybook.branches` directory keyed by stable
`BranchId` values. A declaration may contain:

```text
BranchDeclaration {
    name: optional String,
    publication: Shared | Archived,
    scope: AuthorityScope,
    created_from: optional BranchVersion,
}
```

The exact schema belongs to the later facet-schema and dmeta work. The durable
invariants are:

- map keys are `BranchId`, never mutable names;
- every branch identifies itself and its logical main document;
- a main-directory entry points to the same logical document claimed by the
  branch;
- dmeta provenance records who introduced or changed each declaration;
- ordinary facet-write capabilities cannot mutate branch system facets;
- branch-service operations validate authority before authoring those facets.

A separate metadata-only physical document per logical document is rejected. It
would double the minimum durable object count and add another object that every
reader must locate and synchronize.

### 4. Replication and acceptance are distinct

Peers may receive malformed, conflicting, or deliberately forged Automerge
changes. Such changes still replicate as physical history. They do not
automatically enter Daybook's accepted logical view.

When both directions are available, the logical service validates:

```text
main directory: D declares branch B
branch identity: B declares document D and branch B
```

It also validates the applicable system-facet schema, signed provenance, and
historical authority rules defined by the dmeta ADR. Missing or invalid evidence
classifies a declaration as malformed, unauthorized, or unverifiable. Callers
must not receive it as an ordinary accepted branch.

This is not local anti-forgery of the underlying CRDT. It is validation of
claims at the logical service boundary.

### 5. Branch discovery is authority-scoped

There is no single globally visible branch directory:

- Shared branches are discovered through `daybook.branches` on the main branch.
- Group-private branches are discovered through authorized group feeds and their
  `daybook.branch` facets. They are not listed publicly merely to improve
  discovery.
- Local branches are discovered through checkout-local state until explicitly
  published.

The logical branch-list operation merges only the sources visible to its caller.
This preserves targeted discovery for public corpora without leaking the
existence of private branches.

Publishing a local branch writes its branch identity, commits the appropriate
authority and storage relationships, and then adds it to the applicable shared
directory. Recovery must tolerate partial progress using the same explicit
lifecycle discipline as physical creation.

### 6. Drawers are documents and idiocentric contexts

A `DrawerId` is the `DocumentId` of a logical document containing a
system-managed `daybook.drawer` descriptor facet. It is not a Keyhive group ID.

The descriptor can identify the authority groups and useful provider hints known
to its author, but no global `DrawerId -> provider` or
`DrawerId -> authority state` mapping exists. A peer learns about a drawer
through a grant, local configuration, synchronization, or an explicit share. Two
peers can legitimately know different authority history and provider sets for
the same drawer.

Provider identity, current reachability, and authorization are separate facts.
Knowing that node N may serve a drawer neither proves that N is online nor
grants the requester access.

One logical document may belong to several drawers. Membership means that the
drawer is a context through which the document is synchronized, retained,
organized, authorized, or resolved; it does not clone or change the document's
identity.

Adding a logical document to a drawer includes:

- its main branch; and
- every shared branch whose authority scope is applicable to that drawer.

It does not silently publish or widen a local branch or a branch with narrower
private authority. Those require an explicit publication or authority operation.

### 7. Addresses carry optional resolution context

The canonical identity remains the ID. Address syntax selects a strict
resolution strategy:

```text
db:<id>                    local knowledge only
db://<drawer-id>/<id>      resolve through one drawer context
db+iroh://<node-id>/<id>   bootstrap directly from one Iroh node
```

`<id>` may be a logical `DocumentId` or a directly addressed `BranchId`. The
parser and typed API must preserve which form the caller supplied where
branch-specific behavior matters.

Resolution does not fall through from one strategy to another. In particular,
failure to resolve a drawer-context address does not trigger an implicit global
search or reinterpret the drawer as a node. Additional global naming systems, if
introduced, use explicit locator forms.

### 8. Document and drawer events are separate

Content and branch lifecycle events belong to a document event model. Drawer
membership and descriptor changes belong to a drawer event model. A content
update does not fabricate a drawer update, and removal from one drawer is not
deletion of the logical document.

The durable processing model for these changes is defined by ADR 008. Existing
drawer-wide branch lookup state and synthetic `DocUpdated` events are migration
inputs, not the new source of truth.

## Consequences

### Positive

- A new logical document or branch can be fully self-identifying in its first
  Automerge commit.
- Most tiny documents can avoid an otherwise mandatory second Sedimentree
  commit.
- Direct branch addressing and targeted public branch discovery are both
  efficient.
- Branch names can change without identity migration.
- Private branch existence need not leak through the public main branch.
- Multiple-drawer membership no longer confuses document identity or deletion
  semantics.
- Logical APIs remain portable to non-Automerge and non-Keyhive backends.

### Costs and trade-offs

- BigRepo gains an explicit allocation/recovery/garbage-collection lifecycle.
- Branch relationships are deliberately redundant in both directions and require
  validation.
- Private and local branch discovery still needs scoped indexes or feeds.
- Replicated physical state can contain claims excluded from the accepted
  logical view.
- Adding a document to a drawer may require applying membership to several
  shared physical branch documents.

## Migration

1. Add pre-content physical ID allocation and durable creation recovery to
   BigRepo.
2. Define schemas and privileged mutation operations for `daybook.branch`,
   `daybook.branches`, and `daybook.drawer`.
3. Write branch identity in the initial commit of newly created documents and
   branches.
4. Build accepted branch discovery from system facets and authority-scoped
   feeds.
5. Move callers from drawer-wide branch maps to logical document and branch
   APIs.
6. Remove synthetic drawer content-update events after ADR 008's durable walkers
   replace their consumers.

## Deferred decisions

- The exact dmeta v2 encoding, signed revision format, and historical authority
  proof.
- The final branch publication state and authority-scope schema.
- Retention duration and implementation details for abandoned pre-authority
  allocations.
- Representation and creation rules for future non-Automerge documents.
- Any optional global identity or naming overlay.
