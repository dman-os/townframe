# drawer

Status: human written; up to date.

Drawers are the primary collection primitive for [documents](./docs.md).

## Why

Keyhive and [big_repo](./big_repo.md) support group-based and document-granular access control.
Drawers are the user-facing primitives built on top of them.
Drawers will be the unit for access control and syncing.

Our primary use cases for drawers include:
- Sharing a single document with someone
  - Drawers should be efficient enough for small granularities
- Sharing and collaborating on a set of documents at work
  - Drawers will live on separate nodes and be synced across nodes
  - They'll need to be multi-writer while keeping the same identity
  - Invites, revocations, the whole collaboration grammar
- A vector for private media library sharing
  - Think family photo albums
  - Drawers will need to contain blobs as well
- A registry for plugs
  - In this case, the drawer is public
  - Being able to specify canonical nodes where drawers can be found is key
  - But also support indicating nodes that mirror the drawer for resilience
  - Nodes need not sync the full drawer to access resources in drawers
- Relay sponsorship
  - Relays can hold documents but not decrypt them
  - Having a drawer doesn't mean being able to read its documents/resources
  - It should be possible for docs to work in multiple drawers
    - I could maybe add a document I didn't create to my sponsored list to ensure that the relay will hold on to it
- Chat
  - Drawers shouldn't introduce overhead that will affect latency
- Large public wikis
  - Drawers should support large-scale collaboration
  - This means having multiple users manage other users and document addition/removal, and so forth
  - Should be able to access documents without necessarily mirroring the full document set or metadata graph
- Sandboxing plugs
  - Drawers can serve as access control primitives for plugs
  - Maybe the plug works in its own drawer
  - Maybe it's granted access to an existing drawer
  - I should be able to look at what a plug routine reads/writes to propagate sensitivity labels (the commonfabric.com idea)
    - This can help prevent routines with egress from accessing sensitive data, blocking exfiltration

## How

We use two kinds of data structures to store a drawer's data in the following general shape:
- Documents
  - A roster document lists the member documents and child drawers.
- Keyline/Kehive
  - Delgations store and enable the transitive access to the member documents for participants
  - Delegations to the drawer store participants: which agents may access/modify the drawer
  - Delegations on the roster document define which participants can add or remove a document from a drawer

Keyline is a graph made up of principal nodes and certificate edges.
A principal can be a document, an agent, or a group, and is identified by a public key.
The certificates delegate access from one principal to another and are signed by the principals authoring the delegation.

In keyline, a membership set can be represented by all delegations from/to the principal, so while there are dedicated group nodes, any node can conceptually be a group.
A principal may only sign delegations over another principal at an access level it has been granted itself.

Four levels of access exist in keyline:
- Relay: hold document ciphertext
- Read: decrypt and read documents
- Write: write to documents
- Admin: revoke other delegations

For drawers, we first set up a collection group, which must be given delegations over documents in the drawer.
Only someone who has access to the document can sign this delegation, and this delegation defines what kind of access the drawer has over the document.
Participants being given permissions to the drawer are then given delegations over this collection group, giving them transitive access to docs that have been added to the collection group.

Note that this allows anyone with access to a document to view what groups have mentioned it.
This is a necessary exposure, because it ensures that drawer participants can present their standing with the collection group to request access to the document at any other node.

On the otherhand, this doesn't allow document holders to enumerate other documents in the collection group that they don't have pre-existing grants for.
Unfortunately, though, it allows these non-participant nodes to write delegations, to a document that they control, to the collection group.
That's why we use the separate Automerge roster as the only authority over what document is included in a drawer.
The delegation set to the collection group is not authoritative.

Additionally, drawers will contain other resources like blobs and other drawers, but these resources must be represented by documents first, making it composable and abstracted.
Note that documents can exist in multiple drawers at once.

A drawer is represented first as a metadata bundle that exists in a document.
The drawer itself will be identified by the id of this metadata document.
Metadata is represented using a number of datums that contain information such as:
- The keyhive groups used to list the documents of the drawer
- The roster that lists all the member docs of a drawer
  - Today this is contained inline in the same document; a separate roster document is deferred
- Nodes upon which a drawer might be found
- Relationships with other drawers
  - A drawer might contain other drawers

In most cases, one finds out about drawers when they're given access.
For public drawers hosted on relays, the drawer metadata may be transferred through other means.
A node will then maintain a list of drawers it knows about and a list of drawers it's actively mirroring.
While keyhive sync is the primary means of syncing drawer metadata, supplemental XRPC services can be used to allow querying drawer information on a node without requiring mirroring the graph.

### Arch

While the Automerge roster datums are used to manage document membership in a drawer, keyhive groups are used as the primitive to enable the mass authority physics.
Keyhive design allows us to be a bit more flexible and offer multiple types of drawer architectures, but it still all depends on document-to-drawer grants in the hive.

- **Single-document drawer**: a normal document that's also a drawer.
   - It's just the drawer metadata datums in the document.
   - This solves the single-document share case.
- **Self-group drawer**: all the documents in the drawer are stored in a single group represented by the drawer document itself.
   - This means that access given to the document is access given to the drawer.
   - The drawer can contain multiple documents with different delegation kinds for each one.
   - So even if an agent has admin access on the drawer, if the drawer itself has read access, the agent only gets read.
- **Single-group drawer**: a drawer specifies a separate group for the collection grants.
   - This allows one to separate access to the drawer metadata document from access to the drawer.
- **Agent-groups drawer**: use separate groups to implement better user management and RBAC for large-scale collaboration.
   - We first have the collection group that has authority over the documents.
   - Then we'll use role groups per access level. A reader's group will only have transitive read access to the main collection group, for example.
   - The same goes for writers.
   - Additionally, we can have an admin group that has admin access on the role groups themselves to manage users. The admin group need not have admin access to the admin group itself.
   - This leaves a higher superadmin/owner class that has admin access on all groups and thus the entire drawer.

We probably don't need to implement the self-group drawer, but the flexibility here allows us to add more architectures in the future when needed.
Additionally, we only use a single group for the document collections in all cases.
While some use cases might require drawers with different partitions that have different access control, this use case is satisfied by the Child multi-drawer relationships and not as a drawer arch.
We want to stick to a simple and small set of well-known drawer architectures.

Note that a user adding a document to a drawer will only be able to provide grants equal to or less than what it has access to.
In practice, this means the greatest access it's allowed to a document through other drawers.
This means that if it loses access to a drawer that it first derived the document authority from, the delegations it signed adding the document to another drawer are lost with it.

### Location

Drawers, like those that are public, might indicate in their metadata nodes where they're able to be found.
This doesn't mean that that's the one and only node, just a likely node to be found at.
Other nodes can sync and mirror the drawer if they have access, and in most collaboration use cases, any node that wants to participate in a drawer will very likely sync/mirror it.
This is our primary multi-writer story seam.

Nodes can be reachable across different protocols, so node addressing is not hard-specified by the drawers.
For the primary implementation, nodes will be mainly reachable through iroh, so iroh pubkeys will be the node addresses.
But maybe some nodes have rotating iroh addresses?
They could use DNS or other mechanisms to indicate the current address.
Anyway, node addressing is out of scope for this document.

A drawer could specify a publicly writable document or group to allow mirrors to register their locations without necessarily having write access to the drawer metadata.

We can maybe have a public group that anyone can add to, where we'd have documents advertising mirrors.
This allows mirrors to self-register?
We can also have drawers contain other drawers completely by using keyhive transitive access.
And depending on the delegation, access to a parent drawer can mean access to a child drawer, but not if the child delegation has narrowed it.

<!--
FIXME: This doesn't hold up. We can technically create new collection groups and redo delegations over the original members using the old delegation we had.

Additionally, a single-doc drawer cannot grow in place into the more complex architectures as it stands today. Forming a collection creates a new drawer metadata doc with a new DrawerId, and the original doc stays a single-doc drawer independently.
-->

### Drawer relationships

Drawers will have different relationships with other drawers.
These must not necessarily be expressed using keyhive delegations.

#### Child

A drawer can contain another drawer.
Use cases for this include:
- Allow participants' access given on parent drawers to transitively apply to all child drawers
- Relay sponsorship drawers can use child relationships to indicate broad sponsorship of documents in other drawers without bothering to add every child drawer document
- Use child drawers to segregate documents at different access levels for agents

Now, the roster will specify the different child drawers and in what means they're related.
Since we have different drawer archs, child relationships will take many different forms in the graph according to the architectures.
For example, a single-group drawer that names an agent-groups drawer as a child must do delegation to each of the child's groups from itself.

The group-to-group delegation level itself determines how access propagates across parents to children.
For example, I could add a drawer I only have read access to into another drawer, but since I only have read access, I can only grant read access from child to parent.

<!--
FIXME: this sort of contradicts what was stated above around document holders needing to know the collection group id of a drawer at the least.

Note that children don't need to know all the parent drawers they exist in.
This can be the case if a child drawer is public.
In this case, agents could have access to a drawer without having any visibility to a parent drawer containing it.
-->

### Subset

To enable local mirroring of only a subset of a large drawer, one can create a new drawer that contains only a subset of documents from the original.
The origin node of the large drawer might not carry this subset drawer or the keyhive group for it.
The subset relationship isn't a keyhive-level architecture but is manually maintained by the owner of the new subset drawer.
This relationship is still important as it defines a new big_repo sync story: syncing documents in a keyhive group only if they're in another keyhive group.

### Keyhive actions

Agents can thus do the following operations on drawers by modifying the keyline delegation graph:
- Add/remove document
  - Documents are added to the roster
  - The collection group is given/revoked grants over the document
- Give access to another agent
  - Agent is granted a delegation to the drawer keyline principals according to the drawer arch
  - Agents can only grant delegations equal to or below the delegation level they were granted
- Add/remove a child drawer

### Relays

Relays are the highly available nodes that other nodes use as an intermediary.
In keyline, getting relay access to a group provides visibility to delegations outgoing from the group.
This allows relays to hold drawers without needing any special grants.

### Public agent

Keyhive supports public items by using a special public principal whose secret/signing key is publicly known.
This principal can be granted different delegations, which then allows anyone to sign delegations as the principal.
Additionally, the sync algorithm doesn't hide metadata and bytes that public principals have access to from anyone.
This allows use cases like public wikis that will grant the public read access to their roster and their collection group but not necessarily write access.

### XRPC

One need we have from large and public drawers is to provide efficient access to contents without requiring every accessing node to mirror the full keyline graph.
To enable this, XRPC endpoints will be offered over iroh or other transports, allowing actions like:
- Enumerating document memberships
- Counting number of documents
- Access to custom indices for search and filtering

### Bytes

Note that one might have the keyhive principals and the keyline metadata without necessarily having the document CRDT bytes.
This context is important for understanding the rest of the document: the drawer layer is all about membership and access at the keyhive layer.
A node having the bytes is a node decision, and [big_repo](./big_repo.md) has its own mechanisms for ensuring that a keyhive group is properly synced from other nodes.
I.e. nodes will decide to engage in full drawer-including-CRDT-and-blobs sync according to their own logic and situation.

For example, a node adding a third-party document into its sponsorship set must ensure that the relay receives those bytes.
If the relay already has the bytes from someone else, all well and good, but it's not expected to automatically fetch the bytes.

### Datums

```js
// Doc ID: zDrawerWork 
{
  // drawer metadata datum
  "org.example.daybook.drawer/main": {
    "layout": {
      "$type": "org.example.daybook.drawer.singleGroup",
      "metaGroup": "zGroupWorkMeta",
      "docsGroup": "zGroupWorkDocs"
    }
  },

  // roster metdata datum, can be colocated with drawer metdata
  // datum. A single-document drawer doesn't need this
  "org.example.daybook.drawer.roster/main": {},

  // Member documents datum
  "org.example.daybook.drawer.docEntry/zDocA": {
    "access": "Write"
  },
  "org.example.daybook.drawer.docEntry/zDocB": {
    "access": "Read"
  },

  // Child drawer datum
  "org.example.daybook.drawer.childEntry/zDrawerArchive": {
    "access": "Relay"
  }
}
```

## Fact list

- One possible solution for managing incoming documents/edits on large wikis
  - The submitting principal creates a new drawer where the documents they intend to edit/submit are staged
  - A staging drawer where the public is given read access
  - We only grant read access to the public on the content, so edits/submissions should be branched documents of the original
  - Admin/owning nodes can then use a separate process to manage these staging drawers and, when satisfied, merge the branch doc into the main doc
- Document branches are modeled as other documents today
  - This means that a branch must be explicitly added to the drawers of its origin document to be carried along

## Questions

- If we give the public read access to the drawer, how do we prevent the public from adding new documents into a drawer at the read access permission?
  - Does keyhive's public principal mean that other agents can also write? I mean, I understand that public means that you're allowed visibility onto the metadata graph that the public has read access to, but does this also allow you to add new delegations using public authority?
  - That doesn't seem possible the way I'm thinking about the graph, but this blocks certain use cases
- If I'm given write access to a document by being given write access to a drawer that contains the document
  - How can my transitive access be used to add the document to another drawer? How does that work on the graph? Keyhive does support adding new delegation edges that don't mention the old vertices that enabled minting such a delegation in the first place?
