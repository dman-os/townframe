# Docs

Documents are the main units of information of the system and have a unique ids.

## Why

As a p2p system, we need to have a well bound and scoped data records that are easy to transfer across nodes and sync them under changes.
CRDTs are a great way to ensure that large mutable records are efficiently synced under multi-writers, hard partitions and other p2p hardships.

Additionally, we want the documents to be self describing as much as possible to enable flexbility and indepenedent underestanding and evolution. 
There might be a central authority available to adjuidicate schemas and meaning.
This requires us to have stringent laws around keeping schema changes backward compatible since there's no way of rolling out breaking changes atomically to all nodes. 

## How

### Identity

Documents are identified and authorized through [Keyhive](./keyhive.md) and correspond to document principals in the keyhive authorit model.
All keyhive principals and thus documents are identified by 32 byte ED25519 public keys.
This means that a document id is globally unique.

### Facets

Documents are made up of facets which are JSON values describing the different pieces of the document.
Facets are stored in a map with unordered keys that have a format of `facet.tag/key-id`.

The facet tag indicates expected schema of the value under that key.
Facet schemas are AT Proto lexicons with specialization and the tag corresponds to the lexicon NSID.

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
- Facet schema: the shape description of the facet 
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

### DocumentRepo

The documents repo provides the primary interface for reading and writing documents.
It provides a key value shaped interface where documents and their facets can be read or writeen by their ids.
It doesn't provide means of editing specific fields of facets and avoids overfitting to hte CRDT model of documents.

Incoming writes are first diffed the existing data value and changes are written.
Facet writes are generally expected to reference the existin heads they last read at so that the documents repo can create a Change atop those heads.
This prevents TOCTOU issues and ensuring the automerge concurrent write semantics come into play in the case of a remote write that hasn't been observed has come in.

Additionally, the DocumentRepo ensures that incoming writes adhere to the expected schema of the tag.
This is not just about the incoming facet but also the resulting facet after the write.
Specifically, if there's a unobserved write and if the incoming facet creates a concurrent write at a previous heads, the resulting shape of the facet under concurrent merges is also evaluated before accepting writes.

### Schemas

Facet shapes are primarily defined by AT Proto [lexicons](https://atproto.com/specs/lexicon), a JSON schema relative used by the AT protocol network
Lexicons are identified by their [NSID](https://atproto.com/specs/nsid)(Namespaced Identifiers), DNS rooted identifiers that are represented in reverse fully qualified host name notation.

In addition to Lexicons, facet schemas carry an optional JSON schema that is used to define the following extra information not representable in Lexicons.
- Automerge representation
- Typed maps
- Float string format
- Uint string format
- Counter integer format

Any other JSON schema feature usage is forbidden.

All valid record lexicons are valid facet schemas.
Asides from the evolvability available to lexicon per its spec, breaking schemas is disallowed and this extends to JSON schema extensions as well.
This means the follwing are disallowed:
- Changing the Automerge representation annotation
- Change typed map leaf types

If a breaking change is required, use a new facet tag instead.

#### Data model

The facet data model is a layering of a subset of the AT proto datamodel to the Automerge data model.
As the canonical storage format, the primary data model of documents is the Automerge data model. Automerge has the following types:
- Scalars
  - Null
  - Int64
  - Uint64
  - Float64 (IEEE 754)
  - Boolean
  - Int64 counters
  - Int64 timestamps in milliseconds since unix epoch
  - Utf-8 strings
  - Byte strings
- Composites
  - Maps with string keys
  - Lists
  - Text/Collaborative strings

For more information, check out the offical automerge [reference](https://automerge.org/docs/reference/documents/).

The wire and working representation of the data is ATProto Lexicon records which are themselves represented using JSON or CBOR and consist of the [ATProto data model](https://atproto.com/specs/data-model).
Automerge is a JSON CRDT so any ATProto record can be stored in facets.
So yes, while techincally, the full ATProto data model is available to program with, the following items currently have no meaning in facets:
- `cid-link`s or `$link` objects

Facets are stored the CRDT using their JSON based Automerge representation with the following clarifications:
- Incoming `$bytes` objects are stored using byte string scalars in automerge (and not base64).
- String fields with `datetime` format and found to be in ISO 8601 format will be stored as Automerge timestamps.
  - This won't apply to other incoming string datetime formats to ensure roundtripping produces byte accurate CIDs for ATProto native data.

Additionally, documents writes allow non ATProto supported datatypes like which have special rules.
The big one is floats which are representable in Automerge but not in Lexicons.
Documents can contain floats and writing IEEE 754 floats is allowed.
The adjustment is that all floats will be returned from the DocumentRepo in stringfied decimal format.

Similarly, the ATProto data model only supports integers in i64 format.
If one needes the uint64 range above i64:MAX, stringified representation is the only avail avenue.
Non-negative JSON numbers are accepted as input but Uint marked fields are stringifed on return.

<!--TODO: specify expectations around float/u64 stringification-->

The whole document design takes heed of the robustness principle: "be conservative in what you do, be liberal in what you accept from others".
So while the DocumentRepo will return stringified floats always, it'll accept IEEE 754 float representations.

#### Authoring schemas

The standard lexicon authoring [guidlines and rules](https://atproto.com/specs/lexicon#lexicon-publication-and-resolution) apply.
Additionally, you might want to:
- Use byte strings to encode an large array of floats to avoid parsing string parsing costs on reads

Please explore the atproto spec for more information.

##### Evolvability

To make facet schemas more evolvable, the following advisories should be considered during design:
- Use open enums to allow adding more variants
- Use open unions 

JSON scheman and thus lexicons allow unknown field by default and this should be preferred to allow forward compatibility.
Ensure that any interemediate decoding of the facets retain fields for any data that's expected to come back to the DocumentRepo lest the lack of the field reconcile into removal of the unkown field from the facet.
This expectation also extends to unions, unkown unions shouldn't trip and and be preserved by any codec that expects to bring back data to the DocumentRepo.

### Drawers

Sharing documents is done through their membership in [drawers](./drawer.md).
Sharing a single document can easily be done through self-group drawers without incurring extra metadata costs.

### Branches

When we apply changes to a doc, we send them to the peers asap and have them all resolve to the same state.
But in some cases, we need to delay sending changes to others and keep working on them locally.
Branches allow us to create a fork from a doc at some point and work on it.
When satisfied, it's changes can be added back to `/main` in a merge operation.

Each branch is stored as a separate document under a new id in a Automerge CRDT.
The following facets are used to track branch relationships in docs:

```js
{
  // the key-id of the facet indicates the branch id
  "org.example.daybook.branch/main": {},
  // the key-id on this one is just the facet convention
  "org.example.daybook.branches/main": {
    byPath: {
      // under concurrent writes, we might have multiple branches for the same path
      "/my/branch": ["zAyPkUKgPRFaLje8LNjK3CHCQEytgbhN2Xt35pLKpFHck"]
    },
    byId: {
      "zAyPkUKgPRFaLje8LNjK3CHCQEytgbhN2Xt35pLKpFHck": {
        path: "/my/branch",
        branchedFrom: {
          branchPath: "/main",
          heads: ["ztXt35kUKgPLgbpFpLe8A"]
        }
      }
    }
  }
}
{
  // this one branched from /main
  // note that the facet is under key-id /main
  // and that shouldn't be confused as the branch path
  "org.example.daybook.branch/my/branch": {
    // we track the origin
    from: {
      branchPath: "/main",
      heads: ["ztXt35kUKgPLgbpFpLe8A"]
    }
  }
}
```

Note that branches by convention have path based names.
By design, branches in the `/tmp` path will never leave that device.
All other branches are replication candidates.

If intending to share a single document and all it's branches, a single group drawer should be preferred as a unit of sharing.
Non replicated document branches are excluded from such drawrers will not show up in the `branches` map of their origin document.

### Conflict model

As documents are stored using CRDTs, conflicts don't naturally end up blocking progress like in git.
Automerge resolves any concurrent writes using it's [merge rules](https://automerge.org/docs/reference/under-the-hood/merge-rules/) ensuring every node resolves to the same value deterministically.
This value might still break semantics and schemas though and must be dealt with.

Facets should be designed to allow conflicting writes to resolve gracefully where possible.
Automerge does allow querying concurrently [overwritten writes](https://automerge.org/docs/reference/documents/conflicts/) but that API is intentionally not provided for now by the DocumentRepo.
Instead, it's recommended to use the follwing tricks to allow good multi-writer stories.

When creating new facets, prefer determinstic key-ids for facets if there's the possiblity that the same process runnning on another node might try to create the same facet.
Every node writing to `/main` means that they will merge as expected.
Facets need multiple instnaces on a doc? Use ids deterministically derived from the process that produces it.

An illustrating example for this: a processor that creates a derived facet from another.
Another node might run the same processor and in most cases, it doesn't make sense to have multiple derivations from the same source facet.
Determinsitc ids like using the key-id of the source facets avoids that.
Note that this concern also extends to facet internal maps that expects concurrent writers.

An alternative approach to solve concurrent writes on the same map key is to use competing sets.
Arrays that are cleared and then pushed into during writes.
Under concurrent writes, the array will have two values under automerge merge rules.
This is useful in instances where resolving concurrent creations needs to be manual.

While Automerge happily resolves any concurrent writes under its rules, the results don't always make semantic case.
Since Lexicons allow specifying extra validation rules care must be taken around schema design to ensure that
For example, a maximum constraint on a counter is difficult to ensure under concurrent writes.
DocumentRepo prevents writes that would break the schema and thus, relying on such brittle schema requires caution and try-catch around every write.

It's also possible to have concurrent writes break under schema drift.
For example, it's recommended that any facet that intends to be evolvable to allow unkown keys on maps.
When new field is later added, if any facet was using that key to store unsanctioned and now incompatible values, this will result in a schema break.
Concretely, new incoming writes might merge in incompatible fashion preventing more writes possibly.
This is only avoidable by avoiding using unsanctioned keys.
The recommendation to allow unkown keys in schemas is intended to improve evolvability and not for storing unsanctioned data.


Branches can be merged back to other branches that have the same origin doc.
This introduces another hazard that must be avoided when designing facets: mutations done in the branch will be concurrently applied to the origin doc after merge.
This means that fields that reflect branch speciifc information like the document id must be carefully designed lest after the merge, they conflict and break semantics.

<!-- TODO: branches in urls -->
### Rust authorship

### URLs

<!--

 - Does absent heads mean live/current, while supplied heads pin a snapshot? What does [] mean?
 - Which branch does a reference target?
 - What does self mean when a facet is copied into another document?
 - Does a reference pin the whole document frontier, selecting the facet within it?
 - What happens when the target is unavailable, unauthorized, or deleted?
 - Is a reference only an address, or does it imply retention/access? Those shouldn’t be conflated.


-->

### Actors

### Dmeta

Every document carries document specific metadata in a dmeta facet that is mantained by the DocumentRepo. The details are presented in the following snippet:

```js
{
  "org.example.daybook.dmeta/main": {
    createdAt: "2026-10-13T14:30:15Z",
    updatedAt: ["2026-10-13T14:30:15Z"],
    actors: { 
      // pubkey of the actor used to sign facet signatures
      "zAUKgPRFak3CHCQEeLjyP8LNj35pLKpFHcKytgbhN2Xtk": {
        // url identifying the actor
        actorUrl: "db+actor://<nodeid>/<path>/<to>/<subsystem>", 
        // delegation from authority authorizing that the actor
        delegation: { /* TODO */}, 
        createdAt: "2026-10-13T14:30:15Z",
      }
    },
    facetUuids: {
      // concurrent creations could produce the same facet
      // so we assign UUIDs to identify them
      "aa113c83-7eaa-48ad-9f6a-b27edefaf7df": {
        key: "org.example.daybook.branch/main",
        createdAt: "2026-10-13T14:30:15Z",
      },
      "a025415c-461e-4e43-b5d3-4baf0935f7a4": {
        key: "org.example.daybook.branch/main",
        createdAt: "2026-10-13T14:31:42Z",
      },
    },
    facets: {
      // a current competing set, not an audit log: a write replaces the entries it
      // observed at its base heads, and concurrent writers can leave several
      "org.example.daybook.branch/main": {
        updatedAt: ["2026-10-13T14:30:15Z"]
        // removed facets are tombstoned, optional
        deletedAt: ["2026-10-13T14:30:15Z"]
        // every facet write produces a signature over the following, reconstructed
        // from the change that introduced it rather than stored beside it:
        // - document id
        // - actor pubkey
        // - facet key
        // - the smallest alias visible in the writing transaction
        // - whether it was a write or a removal
        // - the Blake3-256 CID of the value in its native storage form, read back
        //   from the transaction after the write
        // - the signature ids visible on the facet at the parent heads
        signatures: [
          { 
            actorPublicKey: "zAUKgPRFak3CHCQEeLjyP8LNj35pLKpFHcKytgbhN2Xtk",
            signature: "zCHCQEyAyPpLKpFHckkUKgPRN2je8LXt3FtgbaLNjK3h5",
            // set only for a signature copied onto a branch at its creation, where the
            // origin's history is not available to reconstruct the signed bytes from.
            // the message is the exact bytes the actor signed; the value is present only
            // when the copied value is not the signed one. both are the protocol bytes
            // object, and neither is present on a signature authored on this document.
            inherited: {
              originDoc: "zpLKpFHN2je8LXtEyAyCHCQckkUKgPRP3FtgbaLNjK3h5",
              message: { "$bytes": "..." },
              value: { "$bytes": "..." }
            }
          }
        ]
      }
    }
  }
}
```

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
