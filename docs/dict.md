# Dictionary

Here lies a roughly ordered description of the various concepts in daybook.
Currently intended for contributors.

## Repo

In daybook, all of our documents go in a single repo.
We can clone our repo on other devices to get access to our docs.
Changes can be made on any replicas of the same repo, whether it is to add a new document or to modify an existing one.
Daybook ensures then that these changes are synced to other replicas whenever a working connection between devices.

## Docs

Documents are a the main units of information in a repo and have a unique ids.

### Facets

Documents are mainly made up of facets which are JSON objects describing the different pieces of the document.
Facets are stored in a map with unordered keys that have a format of `facet.tag/key-id`.
The facet tag indicates expected schema of the value of under that key.
The key-id allows multiple facets of the same kind in the doc and is an untyped string.
Using a convention for the id, like the plain default "main", allows for convergence when creating facets on different devices.
For uniqueness, uuids can be used.

Some examples of facets:
```js
{
  "org.example.daybook.title/main": "hello world",
  "org.example.daybook.path/main": "/hello.txt",
  // all docs in the document drawer dmeta facet
  "org.example.daybook.dmeta/main": {
    id: "<the id that the drawer knows it by>"
    createdAt: "timestamp",
    updatedAt: "timestamp",
    // more metadata
  }
}
```

### Automerge

Documents are stored using the Automerge data structure, a JSON based CRDT implementation.
CRDTs are a family of data structures allowing concurrent edits acoss devices that can then be resolved to a final, merged state in a repeatable and unsupervised manner.
The design of Automerge requires the full history of the document is kept which can be a feature or a burden depending on the usecase. 

#### Heads

In automerge, instead of line diffs as seen in git, we have operations describing changes to JSON objects.
These operations are bunched up together into transactions or changes as they're called.
A change can be taught of as a single git commit with a hash used to refer to it, the change hash.
But unlike git, automerge avoids ambiguity merge conflicts at the JSON layer.
All changes concurrently resolve to the same outcome for all replicas.
This allows us to avoid the need of creating merge commits to refer to a state of the document at a point in time.
We instead use the set of the concurrent hashes as a commit reference.
In most cases, a point in time for a doc only has a single hash in the set but under concurrent changes, we get a set so we default to that.

### Branches

When we apply changes to a doc, we send them to the peers asap and have them all resolve to the same state.
But in some cases, we need to delay sending changes to others and keep working on them locally.
Branches allow us to create a fork from a doc at some point and work on it.
If satisfied, we can merge it back to the `main` branch.
If not, it can be discarded.

Note that branches by convention have path based names.
Any branches in the `/tmp` path will never leave that device.
All other branches are replicated.
<!-- TODO: test branch deletions + drawer doc sync behavior -->
<!-- TODO: branches in urls -->

### Drawer

The drawer is where we keep track of documents and their branches.
It maintains this information in an automerge document that is replicated to all peers.
It's also the gatekeeper for all docs weather it's reads or writes.
We can read or update multiple facets at once from a single doc.

When changing facets, we send in the full JSON value of the facet to the drawer.
The drawer then creates the minimal set of operations needed to update the existing facet into the document.
This is done in a single transaction to roll them into a single automerge change hash.
Multiple facets can be updated at once and if so, will be put into a single transaction.

### URLs

    db+facet://self/org.example.daybook.title/main?at=hash1|hash2

URLs can be used for intra or inter-doc facet references.
As a special convention, `self` can be used instead of the document id to indicates that the reference is to the same doc containing the facet making holding the URL.

We use change hash sets to refer to commit of the facet we're referring to.
These can be put in the URL query params or be put in another field.

If the heads set is empty, it's assumed that the referred to facet exists at the same change hash of the facet holding the reference. 
I.e. part of the same transaction.
This means that when updating a facet, if it previously had an empty hash set as a reference, unless the other facet is also being updated in that change, we must shift to a proper hash set reference.
Changes that violate self references will be rejected.

### Blobs

Complementary to the facets, blobs are used to store large, static byte arrays like images and videos.
We use the blob facet to manage references to this.

```js
{
  "org.example.daybook.blob/main": {
    mime: "image/png",
    lengthOctets: 1024,
    digest: "<hash>"
    inlineBase64: "small blobs are stored inline as base64 strings but not all blobs",
    urls: [
      "db+blob://<digest>"
    ]
  },
  "org.example.daybook.imagemetadata/main": {
    facetRef: "db+facet://self/org.example.daybook.blob/main"
    refHeads: []
  }
}
```
<!-- TODO: oof, forgot base64 support for inline -->

Blobs, similar to docs, are also synced to all devices and replicas of a repo.

## Plugs

A plug is a unit of features for daybook.
A plug can contain things like:
- Facet definitions
- Wasm based workflow routines for code
- Commands for routines that can be run on command
- Processors for routines that run in response to changes
- Local states for device specific sqlite databases

All incoming facets must be defined by a plug first or will be rejected.
More details can be found at the [plug manifest definitions](../src/daybook_core/plugs/manifest.rs).

### Routines

<!-- TODO: describe capabilites -->
<!-- TODO: show example -->
