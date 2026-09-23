# Docs

Documents are the main units of information of the system and have a unique ids.

## Why

As a p2p system, we need to have a well bound and scoped data records that are easy to transfer across nodes and sync them under changes.
CRDTs are a great way to ensure that large records are efficiently synced under multi-writers, hard partitions and other p2p hardships.

Additionally, we want the documents to be self describing as much as possible to enable flexbility and indepenedent underestanding and evolution. 
There might be a central authority available to adjuidicate schemas and meaning.
This requires us to have stringent laws around keeping schema changes backward compatible since there's no way of rolling out breaking changes atomically to all nodes. 

## How

### Facets

Documents are made up of facets which are JSON objects describing the different pieces of the document.
Facets are stored in a map with unordered keys that have a format of `facet.tag/key-id`.

The facet tag indicates expected schema of the value under that key.
Tags are desigined to be reverse domain name notation similar to AT proto NSIDs.
<!--TODO: specify design of how NSIDs are locked in-->

The `key-id` allows multiple facets of the same `tag` in the doc.
It is an untyped string.
Using a convention for the id, like the plain default "main", allows for convergence when creating facets on different devices.
For uniqueness, uuids can be used.
The `key-id` can also contain fwd slashes and be a path like construct itself.

Key-ids are expected to be valid utf-8 strings.
To put non-utf-8 values, the escaping format from Go string literals is used.
<!--TODO: elaborate how non-utf-8 escaping is used-->

To clarify:
- Facet: a real data record inside of a document
- Facet schema: the shape description of the facet in JSON schema
- Facet key: a key of a real facet inside of a document
- Facet tag: the schema identification section of a facet key
- Facekt key-id: the document unique id of a facet in a document

Some examples of facets:
```js
{
  "org.example.daybook.title/main": "hello world",
  "org.example.daybook.path/hi/hello.txt": {},
}
```

#### Facet schemas

Facet shapes are defined by JSON Schema that is attached to their tag.

Breaking changes to facet schemas are disallowed.
If a breaking change is required, use a new facet tag instead.

To make facet schemas more evolvable, the following advisories should be considered during design:
- Use open enums to allow adding more variants
- Use open unions 

### Automerge

Documents are stored using the Automerge data structure, a JSON based CRDT implementation.
CRDTs are a family of data structures allowing concurrent edits across devices that can then be resolved to a final, merged state in a repeatable and unsupervised manner.
The design of Automerge requires the full history of the document is kept which can be a feature or a burden depending on the usecase. 

Note that the automerge CRDT is actually called a document but to avoid confusion, it'll be called the CRDT on this document.

#### Heads

In automerge, instead of line diffs as seen in git, we have operations describing changes to JSON objects.
These operations are bunched up together into transactions or *changes* as they're called.
A change can be thought of as a single git commit with a hash used to refer to it, the change hash.
Unlike git, Automerge ensures replicas converge deterministically while concurrent conflicting values may still coexist and remain accessible through conflict handling.
These rules ensure changes concurrently resolve to the same outcome for all replicas.

This allows us to avoid the need of creating merge commits to refer to a resolved state of the CRDT at a conflicted point in time.
We instead use the set of the concurrent hashes as a commit reference (the heads frontier) which logically represents a deterministically merged state anyway.
The next change will refer to the current frontier heads as its parents and produce heads that has a single change hash.
I.e. in most cases, a point in time for a doc only has a single hash in the set but under concurrent changes, we get a set.

### Branches

When we apply changes to a doc, we send them to the peers asap and have them all resolve to the same state.
But in some cases, we need to delay sending changes to others and keep working on them locally.
Branches allow us to create a fork from a doc at some point and work on it.
If satisfied, we can merge it back to the `main` branch.
If not, it can be discarded.

Each branch is stored as a separate Automerge CRDT, sharing the same genesis change, which makes them different versions of the same logical document.

Note that branches by convention have path based names.
By design, branches in the `/tmp` path will never leave that device.
All other branches are replicated.
<!-- TODO: test branch deletions + drawer doc sync behavior -->
<!-- TODO: branches in urls -->

### Dmeta

Every

### Blobs

Complementary to the facets, blobs are used to store large, static byte arrays like images and videos.
We use the blob facet to manage references to this.

```js
{
  "org.example.daybook.blob/main": {
    mime: "image/png",
    lengthOctets: 1024,
    digest: "<hash>",
    inlineBase64: "small blobs are stored inline as base64 strings but not all blobs",
    urls: [
      "db+blob://<digest>"
    ],
  },
  "org.example.daybook.imagemetadata/main": {
    facetRef: "db+facet://self/org.example.daybook.blob/main",
    refHeads: [],
  }
}
```
<!-- TODO: oof, forgot base64 support for inline -->

## factlist
