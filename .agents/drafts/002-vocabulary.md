# FDR 002: Nodes, drawers, and checkouts

**Status:** Draft for review. This document names Daybook's principal user-visible scopes and their relationships.

## Why these names matter

A Daybook node can hold many documents without presenting them as one repository to copy. Documents have their own identities and histories. Drawers gather documents for sharing and discovery. Nodes connect to other nodes and decide what to keep locally. Checkouts expose selected content to ordinary filesystem tools. None of these is another name for the whole collection of a person's data.

The same document can appear in several drawers; a drawer can be known on several nodes without every node keeping all of its bytes. One machine may host several nodes. The user interface must show what is known, accessible, and locally available without implying that all three are equivalent.

## The vocabulary

**Document.** An identified unit of content and history that can contain facets rather than correspond to one filesystem file. People normally encounter a **logical document** identified by its main document ID, with related branches presented within it. Each branch has its own underlying document ID and CRDT history; this does not make it an unrelated top-level item in the GUI or CLI. A branch is not automatically listed in every drawer containing the main document, and removing a listing does not erase the document or its other memberships.

**Drawer.** An identified collection and a context for sharing. A drawer has the ID of its descriptor document; a single-document drawer may hold its own descriptor and content and later grow into a larger collection without changing identity. A document can belong to more than one drawer. The drawer's official listing, effective access to a listed document, local retention, and actual byte availability are different facts. Adding a document to a drawer neither proves that its bytes are present nor gives the drawer manager authority over that document. A child or subset is another drawer with an explicit relationship; a link alone does not grant access or require every node to know about both ends. The drawer FDR owns membership, sharing, and collection behavior.

**Node.** A locally operated Daybook participant with its own public-key identity, sync and storage context, and choices about which drawers it knows or mirrors. One machine may host multiple nodes.A node is not a drawer, an account, or a complete replica of another node's data. It can know a drawer without mirroring its documents, and it can be granted access before any document or blob bytes arrive. A node's display name can be recorded in an ordinary metadata document and shown on other nodes. Names are not unique or authoritative: the node's public-key identity distinguishes it. The precise metadata facet and proof rules belong in an ADR.

**Mirror (a drawer).** A node deliberately retains some or all of the data it is entitled to retain from a drawer. The drawer ID does not change. Mirroring is not a pristine copy and does not promise that every listed document or blob is present, current, or readable. A node's request to mirror, its authority, and fulfilled local retention must be shown separately. A second drawer publishing a selected subset has its own ID and is not merely another mirror of the same drawer. Mirroring a *node* is not the name for setting up another node.

**Home drawer.** The initial drawer created with a new node, used as the ordinary destination for content when the user has not selected another drawer. It is a normal drawer, not an authority root for every drawer that node later discovers. Adding other drawers does not add them to the home drawer.

**Checkout.** A filesystem working surface selecting Daybook content and reconciling edits. It records bindings between projected outputs and actual filesystem paths. Dpaths are one source of addresses, but checkouts may present other path sources, including `/by-id`; a checkout is not defined as the entire dpath namespace. One document may produce several files; one file is not necessarily a document. A checkout can be durable or temporary; a temporary checkout still needs state while it exists. A checkout does not imply that the node mirrors every drawer that might appear in a query. FDR 001 owns the path and file-operation expectations.

**Contact.** A user-facing representation of another person or identity for communication and sharing. A contact is not assumed to be one node, one device, one Keyhive key, or one drawer. How people and keys are linked is separate identity work.

**Relay account.** A business or sponsorship relationship with a relay service, distinct from a node identity or a drawer. A relay request, granted retention authority, relay acceptance under its policy, delivery of bytes, and durable retention are separate states. A relay may offer services to several nodes; it does not become the owner of their drawers by doing so.

**Sibling nodes (name only).** Nodes intentionally associated through a product-level setup flow, each retaining its own public-key identity. They may share selected configuration documents or drawer access. The relationship, authorization sequence, sharing defaults, and user flow require a separate FDR and ADR; this vocabulary does not infer siblinghood merely from access to the same document and does not call one node the other's clone.

## Creating and finding a node

`db init` creates a node, its home drawer, and a root checkout of **that home drawer only**. It does not make a checkout of all content the node might later know. The root checkout offers a useful place to create and import files without silently expanding when the node joins or mirrors another drawer. A GUI may offer the same starting experience without using CLI commands.

A node's local state is associated with `.dnode`; a checkout's local bindings and transactional state live with `.dtree`. A checkout identifies its node. These names are local layout/context markers, not Daybook data types or dpath namespaces. A node can be placed wherever its operator chooses; a local application may also maintain a list of nodes to help navigate them. The exact store format, secrets, portability, and recovery rules belong in technical documents. Copying a directory is not presented as the ordinary way to authorize another node: a new node needs its own identity and access arrangements. Nested `.dnode` and `.dtree` surfaces must not be mistaken for user files during import.

The CLI is contextual rather than requiring a general login command. When run in a checkout, it finds the nearest `.dtree` and its node; otherwise it looks for a nearby `.dnode`, then an explicitly configured node or a configured default, and otherwise reports how to choose a context. A status or diagnostic command should state which node and checkout it is examining. Local context selection must not grant drawer access on its own. Relay-account authentication for a relay service is separate from choosing a local node. The exact configuration variable and precedence can be specified with the CLI FDR; changing context must be visible rather than silently choosing the wrong node.

The CLI need not contact peers merely to perform local edits; another process or an explicitly started sync service handles network communication. Running that service does not make every known drawer fully mirrored. Read-only CLI commands must not publish edits merely because they inspect a checkout.

## Setting up other nodes

A new node has its own identity and can be granted access to drawers through the same authority machinery used for collaboration. Nodes under the same person's control can participate as siblings; this is not the same operation as mirroring one drawer and does not mean they share one identity, all local policies, or every checkout. The sibling-node FDR will define the setup flow and what is shared. This FDR neither assigns a CLI command to that flow nor prescribes a special self-sync channel.

A newly authorized node may learn a drawer ID and metadata before it receives its roster, documents, or blobs. It can choose to keep only some content. A provider or relay hint tells it where to ask, not that the provider has the latest version or retained bytes. A node's configuration can itself be stored in ordinary documents and shared deliberately; there is no required, universally mirrored special node-metadata substrate. A shared list of known drawers, one node's retention settings, and another node's chosen settings are different information and must not be treated as one policy.

## Technical terms at the boundary

Keyhive provides authority over documents and groups. Its agents, keys, graph edges, and access levels are technical primitives, not synonyms for node, person, drawer, or contact. Cabinet presents drawer and document operations while evaluating authority and local policy. Effective access to one document can come through more than one path and can differ from the access suggested by a drawer label. The authority ADRs define the underlying Keyhive model; this FDR does not require a user to understand its agent type hierarchy to choose a drawer or checkout.

The codebase may continue to contain internal crates or interfaces named `big_repo` or `Repo`; those identifiers do not redefine the user-facing vocabulary. Renaming implementation interfaces is separate migration work.

## Questions for later designs

- The sibling-node FDR/ADR must specify relationship evidence, authorization, configuration sharing, and recovery without collapsing node identities. CLI spelling remains open.
- The CLI FDR must settle the exact context override, node-directory creation and display-name prompts, and whether different checkout specs select one drawer, several drawers, or other path sources.
- An ADR must specify the node metadata document, update authority, and which information is shareable; another must pin the portability and recovery guarantees of `.dnode` and `.dtree`.
- Drawer/provider discovery, shared known lists, per-node mirror policy, and effective retention follow the drawer FDR/ADR; none makes every accessible byte automatically available.
