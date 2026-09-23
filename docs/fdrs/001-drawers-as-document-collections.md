# FDR 001: Drawers as Document Collections

**Status:** Draft — functional design

## Source and scope

This FDR is downstream of [the drawer wiki](../wiki/drawer.md). The wiki describes the drawer concept and explores how it can be lowered onto Keyhive/Keyline and big_repo; this document specifies what people and client applications need to observe. If a functional decision here changes the drawer model, update the wiki first. The technical ADRs will choose authority layouts, durable collection representation, sync and indexing machinery, crate boundaries, and RPC transports.

Drawers are Daybook's primary document-collection, sharing, and sync context. A drawer can be useful at the scale of one document, a family library, a plug registry, or a large public corpus. It is not synonymous with a node, a relay account, a Keyhive group, a complete local copy, or a search index.

## Vocabulary and established behavior

- **Document:** A Daybook document with a stable ID, content, and facets. A branch is another document with its own ID and CRDT history, related to its origin document. Including the origin document in a drawer does **not** automatically include its branches.
- **Drawer:** A document carrying drawer metadata facets. `DrawerId` is exactly that document's logical `DocumentId`. A single-document drawer may carry content and descriptor facets in the same document. It can grow into a collection without changing its ID.
- **Collection:** The documents included through a drawer context. One document may be present in multiple drawers, at different effective access levels. Drawer membership alone does not establish who owns or authored a document.
- **Drawer relationship:** A claim connecting ordinary drawers, such as child, subset, mirror, or related. These are graph relationships, not a second kind of drawer. A claim can be visible to one participant without being known by the other drawer or every node.
- **Principal:** An identity or authenticated actor that Cabinet can recognize, such as a person, device, node, relay, or plug execution. Cabinet-facing identity is not defined as a Keyhive ID; the identity and authority design comes later.
- **Provider:** A node that offers some service for a drawer. Hosting a descriptor, serving bytes, syncing, indexing, searching, and sponsoring retention are different services.
- **Mirror:** A node retaining content from a drawer, or another ordinary drawer publishing a selected subset. Private local retention need not be advertised or accepted by the source.

!> one thing, just because a document is in a drawer, doesn't mean it's branches are. Each branch has it's own id and is a standalone document and also a CRDT. 
!> I.e. branching is not a logical vs non-logical, it's a relationship between documents

### Independence rules

1. One document can appear in several drawers without being cloned or receiving a new ID. Removing it from one drawer does not delete the document or remove it from another.
2. A drawer may have several parents and may participate in cycles. Navigating a relationship must not imply access to either endpoint or all of its contents.
3. A drawer manager does not thereby own every document it includes. A document may be available through that drawer at Read or only Relay access; it may also be unavailable if its authority or provider has changed.
4. Access granted through one drawer does not limit a stronger independent path through another drawer. Cabinet must not promise that a drawer label or plug sandbox can revoke access a principal already has elsewhere.
5. Knowing a drawer ID, reading its descriptor, discovering document IDs, fetching content, decrypting it, editing it, mirroring bytes, and sponsoring relay retention are distinct abilities or states.
6. Metadata and authority can arrive without CRDT or blob bytes. An accessible document can still be pending, unavailable, or only partially mirrored.
7. Drawers use Keyhive/Keyline for the primary authority and collection path, but users and XRPC-only clients work in terms of Cabinet documents, drawers, principals, and versions.

!> the parent or the child might not know about this relationship.
!> in a way, these relationships are view dependent. If I know of a mirror or a subset, doesn't mean everyone access to the drawer knows about it

## Use cases

### 1. Personal multi-device cabinet

A user keeps private notes, records, and media across a phone, laptop, and home server. They create multiple drawers, work offline, add a device, and edit documents without changing drawer or document identity. The same document can appear in several drawers. A device may know about a drawer but retain only selected documents and blobs. Syncing drawer metadata must not automatically download a whole media library. A relay can be used for availability without giving it plaintext access.

### 2. One-document share that grows

A user shares one document without exposing the rest of their cabinet. The document can itself carry drawer metadata; the share is still a normal drawer addressed by its document ID. Later the user can add more documents or adopt a different supported authority layout while preserving the drawer's identity and the recipients' references. Growth must not silently widen the recipients' rights beyond what the user intended; Cabinet must explain any change to the share's effective scope.

### 3. Family media library

Several people share photos and videos. They need to invite and remove people, add media, browse together, organize albums as child or related drawers, and retain different amounts on different devices. Blobs are reachable through their document references and should follow the relevant access and local-retention decisions. A family relay retains ciphertext but cannot decrypt it. One photo can also belong to a separate public drawer without duplicating its logical identity.

The default experience should describe familiar actions—view, contribute, manage access—rather than expose every Keyhive delegation or require the family to select an advanced authority layout.

### 4. Shared workplace project and subdrawers

A project has a common drawer and independently governed team drawers. A document can appear in both. A parent-child link gives people a navigation path; when the people authorized to do so also arrange access propagation, parent participants can reach a child at the resulting limited level. A mere link must not do this on its own. A private team drawer can be invisible or inaccessible to some members of its parent; a child need not know every parent in which it is mentioned.

Small projects may allow contributors to organize and add documents. Large projects may separate contribution, collection curation, and access management. These are meaningful product actions, not a promise that each maps to a distinct Keyhive access level in the first implementation.

### 5. Adding someone else's document

Alice finds a document controlled by Bob and puts it in a drawer she manages. She need not have Admin over Bob's document. If she has Read, the drawer may make it readable to its audience only to the extent her authority allows; a relay-oriented drawer might get only Relay access. Adding it must not imply that Alice can change its content, control its members, or guarantee that its bytes will always be served.

Cabinet distinguishes the intention to include a document from effective access and availability. If Bob revokes an authority path or no provider retains the bytes, readers must see an intelligible unavailable/unauthorized state rather than a silently missing entry. Adding a public document as a reference should not silently create an independent, durable authority path if none is needed.

### 6. Relay sponsorship drawer

A user requests retention for documents from one or several drawers, potentially including a third-party document for which they have only Relay access. The sponsorship collection is not the collaboration drawer, and a relay does not gain Read, Edit, or Admin simply because it sponsors content. A request, effective Relay authority, actual byte delivery, and relay adoption under quota are separate statuses. Sponsorship may cover child drawers where an explicit authority relationship permits it; a child link alone cannot promise coverage.

The initiating node must be able to arrange for the relay to obtain bytes from an authorized provider; learning a document ID or obtaining a grant does not transfer the bytes. A failed or limited adoption is visible. Removing sponsorship does not delete the canonical document or remove it from ordinary drawers.

### 7. Chat and other low-latency collections

A conversation can use a drawer to scope documents and participants. Creating a message or editing its document must not wait for unrelated drawer-wide enumeration, full mirroring, or index rebuilding. The ordinary small-drawer path must remain practical for frequent, small operations. Precise performance budgets and the storage layout belong in the ADR.

### 8. Public plug registry

An official registry is a public drawer of documents carrying plug-manifest facets. Anyone can browse and search through a provider's public index, inspect a manifest at an identified version, and fetch/install selected content without mirroring every manifest, blob, or authority event. The operator can advertise likely providers; independent mirrors can serve the same public material without editing the official descriptor.

Official admission is distinct from offering a document for inclusion. Contributors may publish a submission in their own or a staging drawer, while registry managers decide what appears in the official registry. A catalog cannot treat arbitrary third-party authority grants as official curator-approved entries. Users must be able to tell official listings, submissions, and third-party mirrors apart. A node providing public listing/search is a service provider, not necessarily an ATProto PDS.

### 9. Large public corpus

A reader discovers a Wikipedia-scale drawer, searches and pages through a provider's index, opens individual documents, and pins only a useful subset. They are not required to ingest the full Keyhive graph or mirror every document to use the corpus. Different providers may hold different subsets or indexed versions. The service must identify which document version it returned; “latest” means the latest accepted version known to that provider, not a global total order.

For the public scheme described in the wiki, Public Read is granted on published **documents**, not on the collection group: Keyhive's publicly known signing principal can subdelegate at its held level, making a publicly Read-granted group unsuitable as an anti-spam collection roster. Public document grants alone do not enumerate a corpus, so a public provider must offer an index/listing service for discovery. Whether and how that listing is independently verifiable, replicated for offline use, or backed by a curator-controlled catalog is for the ADRs. XRPC is useful for large public indices; it is not the primary path for all document edits and sync.

Advanced deployments may expose more roles and services, but the ordinary drawer UI should remain small. A node that cannot enumerate a public drawer must not claim to offer that capability merely because it can fetch a document by ID.

### 10. Proposal and pull-request workflow

A public corpus grants the public Read but not direct Edit on its accepted content. A contributor creates a proposal document, a branch document containing proposed changes, or both, in a separately governed staging drawer. Each branch has its own ID and does not become a member of the source document's drawer automatically. A curator reviews the proposal, discusses changes, merges accepted content, or explicitly adds a new document or branch to the authoritative drawer. Rejecting a proposal does not modify the corpus; proposal retention is separate from the target document's lifecycle.

### 11. Private selective mirror and published subset

A node can retain a private selection from a large drawer without telling the origin or gaining authority to edit it. If the selection itself should be named, shared, indexed, sponsored, or synchronized with other nodes, its keeper can create an ordinary subset drawer containing those documents. The source need not know or host this drawer. `SubsetOf`/`Mirrors` is a claim visible where it is published, not an alteration to the source drawer's identity or document ownership. Maintaining the subset as a drawer should permit selecting documents from the source rather than requiring a full source mirror. The exact intersection-sync mechanism belongs in an ADR.

### 12. Plug sandbox and information flow

Installing a plug must not give it the entire cabinet. A user can grant access to a selected drawer or create a narrower plug drawer and put the desired documents there. Within a given drawer, its grant should have understandable uniform semantics; separate drawers express different scopes. Granting access to a plug must show what it may read, write, and send outside the node, and whether an independent path already gives it more access.

The system should be able to flag possible flows such as sensitive drawer → plug A → shared output drawer → plug B → network egress. Labels and such warnings cannot by themselves prevent a plug from copying already-read data; actual egress confinement and declassification controls require a separate security design. A local plug need not be a globally replicated Keyhive principal merely to use Cabinet. The actor that signs produced facets, the executing plug, and the node authorized to sync them need not be the same identity.

### 13. Distributed processor

A processor may observe an authorized document and execute on another capable node while the origin is offline. A task being offered does not itself grant access to the input drawer. Cabinet must account separately for input access, executable plug/version, executor authorization, signed output provenance, and authority to write to the destination drawer. Users should see when work is waiting on data, authority, or a provider, rather than interpreting a task's existence as completed processing.

### 14. Several providers and unannounced mirrors

A drawer may be known from a personal node, a community mirror, and multiple relays, each with different reachability and services. Drawer metadata can point to likely or endorsed providers but is not an exhaustive directory. An unannounced mirror of public material does not need write access to the descriptor or approval by an origin. A provider announcement is not proof of authorization, byte completeness, freshness, or online status. Nodes may use different transports and change their addresses without changing the drawer ID.

If a node offers descriptor resolution, catalog queries, search, direct sync, document retrieval, or relay retention, its advertised and observed capabilities should be distinguishable. Access checks occur for each operation; reading a public listing does not imply authority to edit its documents.

### 15. XRPC-only client and third-party index

A thin client can discover a drawer, query a node's available public or authenticated indices, and request a JSON projection for a specific document/version without running Keyhive, BigRepo, or Automerge. A node may expose drawer-related XRPC methods, unrelated XRPC methods, both, or neither. A public index may be operated independently of the origin. A response carries its provider and version/provenance context so a client can distinguish a projection from authenticated sync or a globally authoritative answer.

Lexicons describe Daybook data and RPC shapes; XRPC is a useful node API convention. Future ATProto PDS hosting on nodes or relays is a separate feature, not a prerequisite or consequence of using lexicons or XRPC.

## Common user operations and observable outcomes

The normal GUI and CLI should offer the same meaningful operations, even if their interaction styles differ:

| Intent | Expected user-visible effect |
|---|---|
| Create drawer | Receive a stable drawer ID; choose a simple sharing/retention starting point. |
| Add or remove document | Change this drawer's collection, not the document's identity or other memberships; report insufficient authority or pending bytes explicitly. |
| Create or link child drawer | Create an ordinary drawer or record a relationship; disclose separately whether access will propagate. |
| Invite, change access, revoke | Show the effective scope and possible independent access paths; removing someone from one drawer cannot promise to remove their access from every other path or erase previously downloaded data. |
| Follow/know drawer | Keep its identity and available metadata without promising full content retention. |
| Mirror or pin locally | Make a chosen scope available offline as bytes arrive; show progress, failures, and storage cost. |
| Publish subset drawer | Share a newly identified ordinary drawer while preserving selected document IDs and leaving the source unchanged. |
| Sponsor on relay | Request retention; show accepted versus pending/rejected documents rather than reporting a request as completed backup. |
| Open/query document | Show an exact version when available, and distinguish absent, unauthorized, unmaterialized, and provider-unreachable states. |

These are *intents*, not assumed single atomic Keyhive or storage operations. If an operation touches several independently synchronized facts, a pending or partially completed state must be visible and recoverable; a success receipt must not claim effects that have not happened. CLI commands should make destructive or authority-widening consequences explicit; the GUI should give equivalent confirmation and status rather than hiding them in a generic “share” control.

### CLI and graphical flows

The CLI should make the target and consequence of each action visible before changing authority or retention: create a drawer; add an existing document or a branch by its own ID; link a child without implicitly sharing it; inspect who has access and through which drawer; request local mirroring or relay sponsorship; then inspect pending, accepted, and unavailable items. Command spellings are deliberately not fixed here. Scripts need stable IDs, structured status, and distinguishable failure outcomes rather than parsing UI prose.

The GUI should offer the same actions through a drawer's document list, relationship navigation, access panel, and per-node availability controls. A share dialog should preview whether it grants document access, merely adds a relationship, or asks a relay to retain ciphertext. A public drawer should show its provider's catalog/search capability and identify the provider/version for fetched documents; a drawer known only by descriptor should not appear as if its contents were mirrored. Neither client should label a relay request “backed up” before adoption and bytes are confirmed.

!> add/remove document/child drawer, mirror drawer on local node

## User-facing access vocabulary

The simple experience should describe **view**, **contribute**, and **manage** in terms of actions, not expose raw Keyhive levels as product roles. An advanced drawer may distinguish curating the official document set from contributing submissions, managing people's access, and recovery ownership. Read, Edit, Admin, and Relay remain relevant *effective authority* and diagnostic terms, but a drawer role does not guarantee identical authority on every included third-party document.

A person invited to a drawer should be told what they can do with its descriptor, included documents, and child links, as well as what they **cannot** do when an included document has narrower authority. A curator's right to organize the drawer is not automatic ownership of a document. Likewise, a reader with an independent stronger path elsewhere cannot be truthfully described as globally read-only.

The precise role set, granter/revoker rules, and which actions are cryptographically enforceable rather than mediated by Cabinet remain ADR questions. In particular, current Keyhive Read holders may subdelegate Read; we must not present a curator-only group roster as enforced merely by giving the public Read on that same group.

## Boundaries for the technical ADRs

The use cases establish the product behavior above. They do **not** settle:

- how a drawer's collection is represented and curated, including the difference between a document offering authority to a drawer and admission into an official public listing;
- which simple and advanced authority layouts can realize the user operations, and how a single-document drawer grows without changing identity;
- how Keyhive/Keyline certificates, group parts, document grants, public per-document grants, child authority propagation, and graph-scope costs work in detail;
- how subset intersection sync, blob availability, metadata-only discovery, and recovery from partial operations are implemented;
- how public/provider indices are authenticated, sharded, synchronized, and protected from spam, or which providers offer XRPC;
- exact Cabinet identity/authentication, plug execution and provenance keys, and the security model for cross-plug information flow;
- the daemon/in-process interface and whether IRPC and XRPC share generated wire types or merely share Cabinet semantics.

Those choices must be evaluated against these use cases, rather than silently turning one possible Keyhive group layout or one node's index into the definition of a drawer.
