# Docs

Status: human written; up to date.

Documents are the main units of information of the system and have a unique ids.

## Why

As a p2p system, we need to have a well bound and scoped data records that are easy to transfer across nodes and sync them under changes.
CRDTs are a great way to ensure that large mutable records are efficiently synced under multi-writers, hard partitions and other p2p hardships.

Additionally, we want the documents to be self describing as much as possible to enable flexbility and indepenedent underestanding and evolution. 
There is no central authority available to adjuidicate schemas and meaning.
This requires us to have stringent laws around keeping schema changes backward compatible since there's no way of rolling out breaking changes atomically to all nodes. 

## How

### Identity

Documents are identified and authorized through [Keyhive](./keyhive.md) and correspond to document principals in the keyhive authority model.
All keyhive principals and thus documents are identified by 32 byte ED25519 public keys.
This means that a document id is globally unique.

### Datums

Documents are made up of datums which are JSON values describing the different aspects of the document.
Datums are stored in a map with unordered keys that have a format of `key.tag/key-slot`.

The datum tag indicates expected schema of the value under that key.
Datum schemas are AT Proto lexicons (with extra sauce) and the tag corresponds to the lexicon NSID.

The `key-slot` allows multiple datums of the same `tag` in the doc.
It is an untyped string.
Using a convention for the id, like the plain default `main`, allows for convergence when creating datums concurrent on different nodes.
For uniqueness, uuids can be used.
The `key-slot` can also contain fwd slashes and be a path like construct itself.

Key-slots are expected to be valid utf-8 strings.
To put non-utf-8 values, the escaping format from Go string literals is used.
<!--TODO: elaborate how non-utf-8 escaping is used-->

To clarify:
- **Datum**: a real data record inside of a document
- **Datum schema**: the shape description of the datum 
- **Datum key**: a key of a real datum inside of a document
- **Datum tag**: the schema identification section of a datum key
- **Datum key-slot**: the document and tag unique key of a datum in a document

Some examples of datums:
```js
{
  "org.example.daybook.title/main": "hello world",
  "org.example.daybook.path/hi/hello.txt": {},
}
```

### Automerge

Documents are stored using the Automerge data structure, a JSON based CRDT implementation.
CRDTs are a family of data structures allowing concurrent edits across devices that can then be resolved to a final, merged state in a determinstic and unsupervised manner.
The design of Automerge requires the full history of the document is kept which can be a feature or a burden depending on the usecase. 

Note that the automerge CRDT is actually called a document but to avoid confusion, it'll be called the CRDT on this document.

#### Heads

In automerge, instead of line diffs as seen in git, we have operations describing changes to JSON objects.
These operations are bunched up together into transactions or *changes* as they're called.
A change can be thought of as a single git commit and like git, we have a graph of these changes with parent references indicating what a change builds on top of.
Like git again, a content addressing hash is used to refer to changes, the change hash.
Unlike git, Automerge uses a set of rules to ensure conflicting values resolve deterministically on all replicas.
Essentially, you have an append only set of changes and replicas that have the same set read the same values from the CRDT.

This allows us to sidestep the need to produce new Changes to manually resolve merge conflicts, no merge commits.
This raises a non-obvious question, without a merge commit, how do we refer to a resolved state of the CRDT that contains both histories?
Unlike Git, a point in time for the CRDT is not represented by a single hash but a set of hashes.
Under concurrent history, the set of the hashes serve as a commit reference which logically represents a deterministically merged state anyway.
We name this set *heads* and serves as our point in time or frontier reference.

If we produce a new change on a CRDT that has concurrent history, the next change will refer to the current frontier heads as its parents and produce heads that has a single change hash.
I.e. in most circumstances, a point in time for a doc only has a single hash in the heads.
Only under concurrent changes do we get heads with more than one hash.

### DocService

The doc service provides the primary interface for reading and writing documents.
It provides a key value shaped interface where documents and their datums can be read or writen by their ids.
It doesn't provide means of editing specific fields of datums and avoids overfitting to the CRDT model.

Incoming writes produce the full datum and the datum heads to write at. 
The datum are then diffed against the value at the supplied heads, and changes are written atop those heads.
This preserves Automerge's concurrent-write semantics when an unobserved remote write has arrived.

<!--
// FIXME: forgery should be it's own section
-->

Additionally, the DocService ensures that incoming writes adhere to the expected schema of the tag.
This is not just about the incoming datum but also the resulting datum after the write.
Specifically, if there's a unobserved write and if the incoming datum creates a concurrent write at a previous heads, the resulting shape of the datum under concurrent merges is also evaluated before accepting writes.

### Schemas

Datum shapes are primarily defined by AT Proto [lexicons](https://atproto.com/specs/lexicon), a JSON schema relative used by the AT protocol network
Lexicons are identified by their [NSID](https://atproto.com/specs/nsid)(Namespaced Identifiers), DNS rooted identifiers that are represented in reverse fully qualified host name notation.

In addition to Lexicons, datum schemas carry an optional JSON schema that is used to define the following extra information not representable in Lexicons.
- Automerge representation
- Typed maps
- Float string format
- Uint string format
- Counter integer format

Any other JSON schema feature usage is not accepted.

All valid record lexicons are valid datum schemas.
Asides from the evolvability available to lexicon per its spec, breaking schemas is disallowed and this extends to JSON schema extensions as well.
This means the following are disallowed:
- Changing the Automerge representation annotation
- Change typed map leaf types

If a breaking change is required, use a new datum tag instead.

#### Data model

The datum data model is a layering of a subset of the AT proto datamodel to the Automerge data model.
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

The wire and working representation of the data is ATProto Lexicon records which are themselves represented using JSON and consist of the [ATProto data model](https://atproto.com/specs/data-model).
Datums are then stored in the CRDT using their JSON based Automerge representation with the following clarifications:
- Incoming `$bytes` objects are stored using byte string scalars in automerge (and not base64).
- String fields with `datetime` format and found to be in ISO 8601 format will be stored as Automerge timestamps.
  - This won't apply to other incoming string datetime formats to ensure roundtripping produces byte accurate CIDs for ATProto native data.

And yes, this means that any ATProto record can be stored in datums.

Note that while techincally  the full ATProto data model is available to program with, the following items currently have no meaning in datums:
- `cid-link`s or `$link` objects

Additionally, datum writes allow non ATProto supported datatypes under have special rules.
The big one is floats which are representable in Automerge but not in Lexicons.
Documents can contain floats and writing IEEE 754 floats is allowed.
The adjustment is that all floats will be returned from the DocService in stringfied decimal format.

Similarly, the ATProto data model only supports integers in i64 format.
If one needes the uint64 range above i64:MAX, stringified representation is the only avail avenue.
Non-negative JSON numbers are accepted as input but Uint marked fields are stringifed on return.

<!--TODO: specify expectations around float/u64 stringification-->

The whole document design takes heed of the robustness principle: "be conservative in what you do, be liberal in what you accept from others".
So while the DocService will return stringified floats always, it'll accept IEEE 754 float representations.

#### Authoring schemas

The standard lexicon authoring [guidlines and rules](https://atproto.com/specs/lexicon#lexicon-publication-and-resolution) apply.
Additionally, you might want to:
- Use byte strings to encode an large array of floats to avoid parsing string parsing costs on reads

Please explore the atproto spec for more information.

##### Evolvability

To make datum schemas more evolvable, the following advisories should be considered during design:
- Use open enums to allow adding more variants
- Use open unions 

JSON scheman and Lexicons have open objects by default.
I.e. they allow unknown fields. by default
This should generally be preferred to allow forward compatibility.

On open object, it's critical to ensure that any interemediate decoding of the datums retain these additional fields for any data that's expected to come back to the DocService
If a write has a missing field, the DocService will treat it as a field removal otherwise.
This expectation also extends to unions: unknown variants shouldn be preserved by any codec that expects to bring back data to the DocService.

### Drawers

Sharing documents is done through their membership in [drawers](./drawer.md).
Sharing a single document can easily be done through self-group drawers without incurring extra metadata costs.

### Branches

When we apply changes to a doc, we send them to the peers asap and have them all resolve to the same state.
But in some cases, we need to delay sending changes to others and keep working on them locally.
Branches allow us to create a fork from a doc at some point and work on it.
When satisfied, it's changes can be added back to `/main` in a merge operation.

Each branch is stored as a separate document under a new id in a Automerge CRDT.
The following datums are used to track branch relationships in docs:

```js
{
  // the key-slot of the datum indicates the doc id
  "org.example.daybook.branch/zMainDocId": {
    path: "/main"
  },
  // branches will have their own branch datum and won't remove their
  // parent. a parent also carries the branch datum for it's children
  // if it must track them. this means that both datums shown here exist
  // on both parent and child branches
  "org.example.daybook.branch/zBranchDocId": {
    path: "/my/branch",
    // this pins which parent we branched from
    from: {
      // self because ofc, we'll be carrying which parent we branched from
      datumRef: "db+datum://self/org.example.daybook.branch/zMainDocId",
      refHeads: ["zHead1"],
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
Automerge resolves any concurrent writes using its [merge rules](https://automerge.org/docs/reference/under-the-hood/merge-rules/) ensuring every node resolves to the same value deterministically.
This value might still break semantics and schemas though and must be dealt with.

Datums should be designed to allow conflicting writes to resolve gracefully where possible.
Automerge does allow querying concurrently [overwritten writes](https://automerge.org/docs/reference/documents/conflicts/) but that API is intentionally not provided for now by the DocService.
Instead, it's recommended to use the following tricks to allow good multi-writer stories.

When creating new datums, prefer determinstic key-slots for datums if there's the possiblity that a similar process runnning on another node might try to create the same datum.
Every node writing to `/main` means that they will merge as expected.
Datums need multiple instances on a doc?
Use ids deterministically derived from the process that produces it.

An illustrating example for this: a processor that creates a derived datum from another.
Another node might run the same processor and in most cases, it doesn't make sense to have multiple derivations from the same source datum.
Determinsitc ids like using the key-slot of the source datums avoids that.
Note that this concern also extends to any datum internal maps that expects concurrent writers.

An alternative approach to solve concurrent writes on the same map key is to use competing or sibling sets.
Arrays that are first cleared and then pushed into during any writes.
Under concurrent writes, the array will have two values under automerge merge rules.
This is useful in instances where resolving concurrent writes needs to be manual.

While Automerge happily resolves any concurrent writes under its rules, the results don't always make semantic case.
Since Lexicons allow specifying extra validation rules care must be taken around schema design to ensure that concurrent writes don't produce semantically breaking values.
While the DocService prevents writes that produce invalid datums, including after merging with any concurrent changes, this doesn't protect against the case where both writes are done without awereness of the other.
For example, a maximum value constraint on a counter can lead to a value above the constraint if concurrent writes produce increments that are under the limit individually but over the limit when combined.
This mean datums relying on such brittle schemas can't be relied to be valid upon read.

Right now, an invalid datum also means that it's going to be invalid until a write comes in that brings it back into the right shape.
This means that legitimate datum writes will be blocked if their writes merged into the broken datum still result in an invalid will be blocked if their writes merged into the invalid datum still result in an invalid datum.
<!--FIXME: find a good solution for this footgun-->

It's also possible to have concurrent writes break under schema drift.
For example, it's recommended that any datum that intends to be evolvable to allow unkown keys on maps.
When new field is later added, if any datum was using that key to store unsanctioned and now incompatible values, this will result in an invalid datum.
This is only avoidable by avoiding using unsanctioned keys.
The recommendation to allow unkown keys in schemas is intended to improve evolvability and not for storing unsanctioned data.

Branches can be merged back to other branches that have the same origin doc.
This introduces another hazard that must be avoided when designing datums: mutations done in the branch will be concurrently applied to the origin doc after merge.
This means that fields that reflect branch speciifc information like the document id must be carefully designed lest after the merge, they conflict and break semantics.

<!-- TODO: branches in urls -->
### Rust authorship

### URLs

<!--

 - Does absent heads mean live/current, while supplied heads pin a snapshot? What does [] mean?
 - Which branch does a reference target?
 - What does self mean when a datum is copied into another document?
 - Does a reference pin the whole document frontier, selecting the datum within it?
 - What happens when the target is unavailable, unauthorized, or deleted?
 - Is a reference only an address, or does it imply retention/access? Those shouldn’t be conflated.


-->

### Actors

An actor's signing identity is its public key. Dmeta's `actorUuids` binds a UUIDv7 to that key and an exact `actorUrl`; the same key can have multiple UUID bindings. URLs describe applications, processes, or subsystems but confer no authority, and a signature does not prove that a particular plug binary executed. UUID timestamps are metadata, not trusted chronology.

The optional `delegation` bytes contain a node-signed **attribution certificate**, despite the field's name. It certifies the actor UUID, public key, and exact URL, not current permissions. The verifier supplies an externally authenticated expected issuer; a certificate's own `iss` cannot establish whom to trust. The certificate is independent of live AuthorityRepo state and carries no grants, redelegation, audience, `iat`, or expiration. A missing certificate or expected-issuer proof leaves attribution pending.

The certificate is CWT tag 61 around COSE_Sign1 tag 18, with empty unprotected headers and protected `alg: Ed25519 (-19)`, `crit: [16]`, content type `application/cbor`, and `typ: application/vnd.daybook.actor-attribution.v1+cwt`. That media type is private and unregistered. Claims are issuer (`1`), actor public key (`2`), raw UUIDv7 bytes (`-65537`), exact actor URL (`-65538`), and purpose `daybook.actor-attribution` (`-65539`). Issuer and subject are canonical multibase text of raw Ed25519 keys. Verification strictly checks the signature over the received protected/payload bytes and rejects duplicate or unsupported fields; [ADR 020 §5](../adrs/020-documents-repo-and-datum-writes.md#5-actors-attribution-and-dmeta) gives the full profile and bounds.

Node admission through BigRepo/Keyhive, actor attribution, datum signatures, and host-managed local capabilities are separate checks. Host private-key custody is not part of the portable claim format. An AuthorityRepo service owning runtime/WIT routine capabilities for documents, datums, commands, and SQLite is a design direction, **not a PR3 runtime migration** or the meaning of `delegation`.

### Dmeta

Every document carries an internal provenance datum maintained by DocumentsRepo.
This is an illustrative JSON shape, not a signed test vector; angle-bracketed bytes are placeholders:

```json
{
  "org.example.daybook.dmeta/main": {
    // creation time of the doc too
    "createdAt": "2026-10-13T14:30:15Z",
    "updatedAt": ["2026-10-13T14:30:15Z"],
    "actors": {
      // doc local uuidv7 of the actor
      "019e5d38-0000-7000-8000-000000000001": {
        // pubkey of the actor used to sign datum signatures
        "pubkey": "zAUKgPRFak3CHCQEeLjyP8LNj35pLKpFHcKytgbhN2Xtk",
        // url identifying the actor
        "actorUrl": "db+actor://<nodeid>/<path>/<to>/<subsystem>",
        // COSE CWT claims bytes that contain the delegations from
        // the authority authorizing the actor
        // the signature
        // { issuer: "zPubkey", pubkey: "zActorPubkey", actorUrl: "db+actor//xxxx" }
        "delegation": { "$bytes": "<base64 certificate>" }
      }
    },
    // concurrent creations could produce the same datum
    // so we assign UUIDv7s to identify them
    "datumUuids": {
      // if there are multiple uuids for the same datum key
      // the lowest one lexicographically wins
      "019e5d38-0000-7000-8000-000000000002": "org.example.daybook.branch/main",
      "019e5d38-0000-7000-8000-000000000003": "org.example.daybook.branch/main"
    },
    "docUuids": {
      "019e5d38-0000-7000-8000-000000000004": "zDoc1",
    },
    "datums": {
      // every field in this is a competing set to resolve concurrent writes
      "org.example.daybook.branch/main": {
        "updatedAt": ["2026-10-13T14:30:15Z"],
        // tombstone for removed facets, empty by default
        "deletedAt": [],
        "signatures": [
          {
            // actor uuid of the author of the signature
            "actorUuid": "019e5d38-0000-7000-8000-000000000001",
            "docUuid": "019e5d38-0000-7000-8000-000000000001",
            // signature of the conceptual value
            // UTF8("daybook.datum-claim.v1") || 0x00 || CBOR([originDocument, datumKey, chosenUuidBytes, actorUuidBytes, (set|remove), valueCIDOrNull, sortedPredecessorSignatures])
            // - set|remove is physically represented as 0|1
            // - valueCIDOrNull is null on removal
            // Note that the valueCID is not the incoming datum write value
            // but the value that ended up in the CRDT after reconcilliation
            // with the existing value
            "signature": { "$bytes": "<base64 signature>" }
            //  when a document is branched into
            // a fresh CRDT that doesn't share the history for compaction
            "inherited": {
              "originDoc": "zpLKpFHN2je8LXtEyAyCHCQckkUKgPRP3FtgbaLNjK3h5",
              "message": { "$bytes": "<base64 complete signed message>" },
              "value": { "$bytes": "<base64 canonical native signed value>" }
            }
          }
        ]
      }
    }
  }
}
```

The value CID is CIDv1 raw/BLAKE3-256 over tagged canonical automerge native CBOR with the following rules:
- Automerge native scalars remain distinct (`Text` versus `Str`, `Counter` versus `Int`)
- Maps have unique keys ordered by encoded bytes
- Float is binary64, all NaNs normalize to `0x7ff8000000000000`, infinities survive, and signed zeros remain distinct. 
- Only plain text is supported; marks, blocks, tables, and unknown native kinds are unsupported rather than forged.

Native is preferred over the JSON representation of the datum because JSON representation doesn't encode the physical bytes that Automerge stores which are schema dependent.
Using the native encoding allows us to validate signatures without the schema at hand.

#### Anti-forgery

The signature and actors metadata in dmeta is used as an attribution and anti-forgery mechanism.
Since automerge and big_repo allow any node with write access to a document to modify whatever they want in the CRDT, the signatures provide a means of provenance for authorship.

The default DocService read path rejects reads if the signature is not valid.
The write path rejects writes on top of forged heads.
Forgery is a per datum state, a document can still have working.
Missing verification evidence does not authorize an authenticated read or ordinary write.

The signed-claim protocol is portable and designed to be independent of Automerge.
If layering over another substrate, it mainly affects how verification works.
Right now, the original `sortedPredecessorSignatures` value is recovored by hydrating the dmeta at the change prior to the new dmeta write.
Reconstructing an ordinary signature's message from its introduction change is only a storage optimization.

The `inherited` field is used to support validation after a CRDT history-free export or compaction.
It must retain the entire message, including parents and chosen UUID, not rebuild those from the current dmeta view.

### Blobs

<!--
Complementary to the datums, blobs are used to store large, static byte arrays like images and videos.
We use the blob datum to manage references to this.

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
    datumRef: "db+datum://self/org.example.daybook.blob/main",
    refHeads: [],
  }
}
```
-->

## factlist
