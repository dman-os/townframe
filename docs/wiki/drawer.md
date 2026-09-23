# drawer

Drawers are the primary collection primitive for documents.

## Why

Keyhive and big_repo support group based and doc granular access control, drawers are the user facing primitives built on top of them.
Drawers will be the unit for access control and syncing.

Our primary usecases for drawers include:
- Sharing a single document with someone
  - Drawers should be efficient enough for small granularities
- Sharing and collaborating on a set of documents at work
  - Drawers will live on separate nodes and be synced across nodes
  - They'll need to be multi-writer while keeping the same identity
  - Invites, revocations, the whole collaboration grammar
- A vector for private, media library sharing
  - Think family photo albums
  - Drawers will need to thus contain blobs
- A registry for plugs
  - In this case, the drawer is public
  - Being able specify canonical nodes drawers can be found at is key
  - But also support indicating nodes that mirror the drawer for resillience
  - Nodes need not sync the full drawer to access resources in drawers
- Relay sponsorship
  - Relays can hold documents but not decrypt them
  - Having a drawer doesn't mean being able to read it's documents/resources
  - It should be possible for docs to work in multiple documents
    - I could maybe add a document I didn't create into my sponsored list to ensure that the relay will hold on to it
- Chat
  - Drawers shouldn't introduce overhead that will affect latency
- Large public wikis
  - Drawers should support large scale collaboration
  - This means having multipe users manage other users and document addition/removal and so forth
  - Should be able to access documents without necessarily mirroring the full document set or metadata graph.
- Sandboxing plugs
  - Drawers can serve as access control primitives for plugs
  - Maybe the plug works in it's own drawer
  - Maybe it's granted access to an existing drawer
  - I should be able to look at what a plug routine reads/writes to propagate sensitivity labels (the commonfabric.com idea)
    - This can help prevent routines with egress from accessing sensitive data blocking exfil

## How

We use two primitves to store a drawer's data.
First of all, we a roster document to list the member documents of a drawer.
Access permissions on this document define who can add/remove a document to a drawer.
We then use keyhive/keyline primitives to to enable the transitive access properties desired.

Keyline is a graph made up of certificates deligating access from one prinicpal to another.
A principal can be either a document, agent or group.
Group membership set is represented by all the outgoing delegations from the principal and it's also allowed to make documents into groups by looking at it's outgoing delegations.
A principal may only delegate to another principal an access level it has been granted itself.
Four levels of access exist in keyline:
- Relay: hold document cyphertext
- Read: decrypt and read documents
- Write: write to documents
- Admin: revoke other delegations

The way the keyhive graph works, we first need a collection group which must be given delegations over documents in the drawer.
Only someone who has access to the can sign this delegation and this delegation defines what kind of access the drawer has over the document.
Agents being added to the drawer are then given delegations over this collection group giving them transtiive access to docs that have granted access to the collection group.

Note that this allows anyone with access to the document to view what groups have mentioned it but it doesn't allow them to enumerate what other documents have granted access to the group.
It also allows anyone to write delegations to a document that they control to any group.
That's why we use the separate automerge as the only authority over what document is included in a drawer.
The delegation set to the collection group is not authoritative.

Additionally, we'll also have drawers contain other resources but like blobs and other drawers but these resources must be represented by documents first making it composable and abstracted.
Note that documents can exist in multiple drawers at once.

A drawer is represented first as a metadata bundle that exists in a document. 
It'll be identified by the id of this metadata document.
Metadata is represented using a number of facets that contains information such as:
- The keyhive groups used to list the documents of the drawer
- The roster document that lists all the member docs of a drawer
  - This can be contained inline in the same document if desired
- Nodes upon which a drawer might be found
- Relationships with other drawers
  - A drawer might contain other drawers

In most cases, one finds out about drawers when they're given access.
For public drawers hosted on relays, the drawer metadata may be transferred through other means.
A node will then mantain a list of drawers it knows about & a list of drawers it's actively mirroring.
While keyhive sync is the primary means of drawer metadata sync, XRPC is used to allow querying drawer information on a node without requirng mirroring the graph.

### Arch

While the automerge roster documents are used to manage document membership in a drawer, keyhive groups are use as the primitive to enable the drawer to document mass authority transfer.
Keyhive design allows us to be a bit more flexible and offer multiple types of drawers but it all depends on drawer to document grants in the hive.
A user adding a document to a drawer will only be able to provide grants equal or less than what it has access to.
Usually, this means the greatest access it's allowed to a document through other drawers.

1. Single document drawer: a normal document that's also a drawer.
  - It's just the drawer metadata facets in the document.
  - This solves the single document share case.
2. Self-group drawer: all the documents in the drawer are stored in a single group represented byt the drawer document itself. 
  - This means that acess given to the document is acces given to the drawer.
  - The drawer can contain multiple documents with different delegation kinds for each one. 
  - So even if an agent has admin access on the drawer, if the drawer itself has read access, the agent only gets read.
3. Single group drawer: a drawer specifies a separate group to contain all resource.
  - This allows one to separate access to the drawer document from access to the drawer.
4. Agent groups drawer: use separate groups to implement better user mgmt for large scale collab.
  - We first have a group that contains all documents.
  - Then we'll use agent groups per access level so a reader's group will have transitive read access to the main collection group
  - Same goes for writers.
  - Additionally, we can have an admin group that has admin access on the agent groups themeselves to manage users. The admin group need not have admin access to the admin group iteslf.
  - This leaves a higher superadmin/owner class that has admin acess on all groups and thus the entire drawer.

We probably don't need to implement the self-group drawer but the flexiblity here allows us to add more architectures in the future when needed.
Additionally, we only use a single group for the document collections in all cases.
While some usecases might require drawers with different partitions that have different access control, this usecase is satisfied by the Child multi-drawer relationships and not as a drawer arch.
We want to stick a simple and small set of well known drawer architectures.

It should also be possible to have a single document drawer or a single group drawer to transform/grow into the more complex architectures.

### Location

Drawers like public ones might indicate in their metadata locations where they're able to be found.
This doesn't mean that that's the one and only location, just a likely location to be found at.
Other nodes can sync and mirror the drawer if they have access and in most collaboration usecases, any node that wants to participate a drawer will very likely sync/mirror it.
This is our primary multi-writer story seam.

Nodes can be reachable across different protocols so node addressing is not hard specified by the drawers.
For the primary implementation, ndoes will be mainly reachable through iroh so iroh pubkeys will be the node addresses.
But maybe some nodes have rotating iroh addresses?
They could use DNS or other mechanisms to indicate current address.
Anyways, node addressing is out of scope for this document.

A drawer could specify a publicly writable document or group to allow mirrors to register their locations without necessarily having write access to the drawer metadata.

We can have maybe public group that anyone can add to where we'd have documents advertising mirrors.
This allows mirrors to self-register?
We can also have drawers contain other drawers completely by using keyhive trasnsitive access
And depending on the delegation, access to parent drawer can mean access to child drawer but not if the child delegation has narrowed

### Drawer relationships

Drawers will have different relationships with other drawers.
These must not necessarily be expressed using keyhive delegations.

#### Child

A drawer can contain another drawer
Usecases for this include:
  - Allow access given on parent drawers to transitively apply to all child draweres
  - Relay sponorship drawers can use children relationships to indicate broad sponsorship of documents in other drawers without bothering to add every child drawer document
  - Use child drawers to segregate documents on different access levers for agents

Now, the drawer metadata doc will specify the different children drawers and in what means they're related.
Since we have different drawer archs, child relationships can take many different forms in the graph according to the architectures.
For example, a single group drawer that names an agent groups drawer as a child must do delegation to each of the child's groups from itself.

Group to group delegation level itself determines how access propagates across parents to children.
For example. I could add a drawer I only have read access to into another drawer but since I only have read access, I can only grant read access from parent to child.

Note that children don't need to know all the parent drawers they exist in.
Agents could have access to a drawer without having any visibility to a parent drawer containing it.

### Subset

To enable local mirroring of only a subset of a large drawer, one can create a new drawer and the interst docs into it.
The origin node of the large drawer might not carry this subset drawer or the keyhive group for it.
The subset relationship isn't a keyhive level architecture but manually mantained by the node.
This relationship is still important as it defines a new big_repo sync story: syncing documents in a keyhive group only if they're in another keyhive group.

### Keyhive actions

Agents can thus do the following operations on drawers by modifying the keyline delegation graph:
- Add/remove document
  - Documents are added to the roster document
  - The collection group is given grants over the document
- Give access to another agent
  - Agent is granted a delegation to the drawer keyline principals according to the drawer arch
  - Agents can only grant delegations equal or below the delegation level they were granted
- Add/remove a child drawer 

### Public agent

Keyhive supports public items by using a special public principals who'se secret/signing key is known.
This principal can be granted different delegations which will then be avail to any other agent. 
For example, public wikis will grant the public read access to their roster and their collection group but not necessarily write access.

### XRPC

One need we have from large and public drawers is to provide efficent access to contents without requiring every accessing node mirror the full keyline graph. 
To enable this, XRPC endpoints will be offered over iroh or other transports allowingn actions like:
- Enumerating document memberships
- Counting number of documents
- Access to custom indices for search and filtering

### Bytes

Note that one might have the keyhive principals and the keyline metadata without necessarily having the document CRDT bytes.
This context is important for understanding the rest of the document: the drawer layer is all about membership and access at the keyhive layer.
A node having the bytes is a node decision and the [big_repo](./big_repo.md) has it's own mechanisms for ensuring that a keyhive group is properly synced from other nodes.
I.e. nodes will decide to engage in full drawer-including-blobs-and-crdt sync according to their own logic and situation.

For example, a node adding a third party document into it's sponsorship set must ensure that the relay recieves those bytes.
If the relay already has the bytes from someone else, all well and good but it's not expected to automatically fetch the bytes.

### Relays

Relays are the higly avail nodes that other nodes use as a intermediary.
In keyline, getting relay access to a group provides visiblity to delgations outgoing from the group.
This allows relays to hold drawers without needing any special grants.

## factlist

- One possible solution for managing incoming documents/edits on large wikis
  - The submitting principal creates a new drawer where the documents they intend to edit/submit are staged
  - A staging drawer where the public is given read access 
  - We only grant read access to the public on the content so edits/submissions should be branched documents of the original 
  - Admin/owning nodes can then use a separate process to manage these staging drawers and when satisfied, merge the branch doc into the main doc
- Document branches are modeled as other documents today
  - This means that a branch must be explicitly added to the drawers of it's origin document to be carried along

## questions

- If we give the public read access to the drawer, how do we prevent the public from adding new documents into a drawer at the read access permission?
  - Does keyhive public principal mean that other agents can also do write? I mean, I underseetand that public means that you're allowed visibility onto the metadata graph that the public has read access to but does this also allow you to add new delegations using public authority?
  - That doesn't seem possible the way I'm thinking about the graph but this blocks certain usecase
- If I'm given write access to a document by being given write access to a drawer that contains the document
  - How can my transitive access be used to add the document to another drawer? How does that work on the graph? Keyhive does support adding new delegations edges that don't mention the old verticies that enabled minting such delegation in the first place?
