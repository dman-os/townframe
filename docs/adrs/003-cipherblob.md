# ADR: Encrypted Blob Mirrors and Key Storage

**Status:** Proposed

## Context

Daybook blobs are immutable, content-addressed byte sequences. The canonical identity of a blob is its plaintext content digest.

Blob synchronization, encryption, and key management are separate concerns:

* Authorized Daybook peers should normally synchronize plaintext blobs directly.
* A user's existing filesystem may remain the canonical blob store. Importing a photo library must not require creating encrypted copies of every file.
* Untrusted storage peers such as relays must be able to retain blobs without receiving plaintext or encryption keys.
* The same plaintext blob may have multiple encrypted representations for different storage or sharing domains.
* Blob references should remain stable when encrypted representations or keys rotate.

Encryption is therefore a **representation concern**, not an intrinsic property of a Blob.

## Decision

### 1. Blob remains the canonical application object

The existing Blob facet remains the canonical description of content:

```json
{
  "org.example.daybook.blob/main": {
    "mime": "image/png",
    "lengthOctets": 123456,
    "digest": "<plaintext-content-digest>",
    "urls": [
      "db+blob:///<plaintext-content-digest>"
    ]
  }
}
```

Its digest identifies the plaintext content.

Application facets SHOULD normally reference the Blob facet rather than a particular physical representation. This keeps MIME information, length, content identity, and available resolution methods centralized.

A Blob may have:

* a directly available plaintext representation;
* zero or more encrypted representations;
* other future mirrors or resolution methods.

### 2. Encrypted representations are lazy mirrors

Daybook does not encrypt every blob when it enters the repo.

Normal peer-to-peer replication may use the plaintext content digest directly:

```text
Blob P
   ↓
plaintext blob P
   ↓
authorized P2P synchronization
```

An encrypted representation is created only when required, such as when a blob is selected for retention by an untrusted relay.

```text
Blob P
   │
   ├── plaintext representation P
   │
   └── cipherBlob X
          ↓
       ciphertext representation C
```

A `cipherBlob` is therefore analogous to an encrypted mirror of the content.

### 3. cipherBlob facet

A cipherBlob facet describes exactly one encrypted physical representation:

```json
{
  "org.example.daybook.cipherBlob/relay": {
    "representation": {
      "digest": "<ciphertext-representation-digest>",
      "lengthOctets": 125337
    },

    "contentEncoding": "aes128gcm",

    "keyRef": "db+facet:///<doc-id|self>/org.example.daybook.jwk/relay",
    "keyRefHeads": ["<change-hash>", "..."],

    "encodingParameters": {
      "recordSize": 65536,
      "padding": "record"
    }
  }
}
```

The cipherBlob intentionally does **not** contain:

* the plaintext content digest;
* MIME type;
* filename;
* application metadata;
* relay sponsorship information.

Those belong to other layers.

`representation.digest` identifies the exact encrypted bytes stored by iroh-blobs, a relay, CDN, or another physical blob backend.

`representation.digest` and the plaintext content digest are multihash-encoded self-describing digests per ADR 001 (BLAKE3 today, algorithm-agile). `representation.digest` is computed over the complete RFC 8188 encoded body (header + records), and `representation.lengthOctets` is the length of that same body including padding and authentication tags. When a representation is served through iroh-blobs, the multihash decodes to the raw BLAKE3 digest iroh-blobs addresses internally; this is an impl detail of the transport, not a facet-level constraint.

`contentEncoding` is the algorithm pivot: its value is an HTTP content-coding token (e.g. `aes128gcm`), and the schema of `encodingParameters` is defined by `contentEncoding`. A codec therefore reads the two as a pair: a token it does not implement is rejected outright rather than interpreted against another scheme's parameter shape, and the parameters it does read are validated against what the wire format can express (the record size is a four-octet header field and must leave room for a record payload). A future encryption scheme is a new `contentEncoding` value with its own `encodingParameters` shape; existing `aes128gcm` cipherBlobs remain decryptable without migration. Daybook deliberately does not reify RFC 8188's binary header as a standalone opaque field, because that would bake one scheme's wire format into the abstraction and hurt agility.

For `aes128gcm`, `encodingParameters` carries the per-scheme reproduction inputs that are not derivable from elsewhere: `recordSize` (the RFC 8188 `rs`) and `padding` (the deterministic padding policy, see §17). The RFC 8188 `salt` header field is deliberately absent from the facet: it is derived at encryption and serving time as a pure function of the referenced JWK secret, the plaintext content digest, and the framing the facet itself carries (`recordSize`, `padding`; see §9). `keyRef` is a facet reference in the ordinary URL form (docs/dict.md, "URLs"): `self` names a facet in the same document, and any other first path segment is the id of another document. `keyRefHeads` is the change-hash set that reference was resolved at, and it is load-bearing rather than decorative. A JWK facet is mutable: rotation writes a new key into it, and §15 requires existing representations to stay decryptable. Pinning the heads is what makes that true - an existing cipherBlob keeps naming the JWK *state* it was encrypted under while a new cipherBlob names the new state. The empty-heads convention (meaning "the same change hash as the facet holding the reference") cannot express a cross-document reference at all, so a `keyRef` must pin its heads - and the implementation refuses an empty-heads `keyRef` outright (§19 places the JWK and cipherBlob in different documents, so the convention has no legitimate user). Declared to the drawer as a facet reference of kind `urlStringSplit`, a change that pointed `keyRef` at the wrong facet tag, or dropped or contradicted the heads, is rejected rather than stored.

The RFC 8188 `keyid` is always empty (see §8) and therefore omitted from the facet.

### 4. Blob → cipherBlob resolution

The Blob records encrypted mirrors through its `urls` field.

Conceptually:

```text
Blob(P)
  urls:
    - direct plaintext resolution
    - resolution through cipherBlob X
    - resolution through cipherBlob Y
```

The exact URI syntax is deferred to the blob-reference ADR, but it will preserve the rule that the plaintext content digest is authoritative while additional information selects a resolution method.

Conceptually:

```text
db+blob:///<P>?via=<cipherBlob X>
```

Resolving this means:

```text
expected content digest P
        ↓
resolve cipherBlob X
        ↓
obtain representation digest C
        ↓
fetch C
        ↓
resolve keyRef
        ↓
decrypt
        ↓
verify plaintext digest == P
```

The final plaintext digest verification binds the resolution result to the requested blob identity.

### 5. Generic key storage uses JWK

Key material is stored independently from cipherBlob metadata.

Daybook defines a generic JWK facet whose value is an RFC 7517 JSON Web Key.

For blob encryption:

```json
{
  "org.example.daybook.jwk/relay": {
    "kty": "oct",
    "k": "<base64url-secret>"
  }
}
```

The facet value SHOULD remain a valid JWK rather than wrapping it inside a Daybook-specific key structure. The schema requires `kty` and carries every other member verbatim, so a key type Daybook never interprets still round-trips unchanged. Requiring `kty` is not cosmetic: facets are also read by untagged deserialization in some paths, where a variant accepting a bare JSON value would match any payload at all and answer for facets it has no relation to.

For RFC 8188 `aes128gcm`, the JWK secret is used as the Input Keying Material (IKM). Daybook derives the representation salt from `(IKM, plaintext content digest)` (see §9); the AEAD content-encryption key and nonce base are then derived by RFC 8188's own HKDF-SHA256 from `(salt, IKM)`, so Daybook performs no additional key schedule beyond that derivation. Because the salt depends on both the secret and the plaintext digest, reusing one JWK across many cipherBlobs yields a different content-encryption key per representation and does not correlate their ciphertexts — with no per-representation random state to create, store, or coordinate across devices.

cipherBlob's `keyRef` is a general URI. It does not require the key to live in a Daybook facet.

Today:

```text
db+facet:///.../org.example.daybook.jwk/relay
```

Future mechanisms may use another key-storage system without changing the cipherBlob abstraction.

The abstraction is therefore:

```text
cipherBlob
   ├── physical representation
   ├── encoding
   └── keyRef → keying material
```

### 6. Keyhive protects JWK facets

For the normal Daybook case, the JWK and cipherBlob facets live inside a Keyhive-protected Automerge document.

Keyhive remains the access-control and dynamic key-distribution mechanism:

```text
Keyhive ACL / DCGKA
        ↓
Automerge document
        ↓
JWK facet
        ↓
blob encryption key
```

Granting a principal read access to the document grants access to the contained blob keys.

Recovery similarly occurs through Keyhive:

```text
recovery authority
    ↓
delegate new repo agent
    ↓
Keyhive rotates/distributes document access
    ↓
new agent reads JWK
```

No per-blob recovery recipients or additional HPKE layer are required.

### 7. Multiple facets and access domains

A document may contain multiple cipherBlob/JWK facet pairs, differentiated using facet key IDs.

The initial implementation may default to one blob encryption domain per document, but the schema does not require one Automerge document per blob.

Grouping multiple keys into one Keyhive document is therefore a future optimization rather than a change to the encryption model.

Different privacy or sharing domains SHOULD normally use different encrypted representations.

For plaintext `P`:

```text
private relay domain:
    key K1
    cipherBlob X
    representation C1

shared project domain:
    key K2
    cipherBlob Y
    representation C2
```

Typically:

```text
K1 != K2
C1 != C2
```

This prevents the representation digest itself from trivially revealing that both domains contain identical plaintext.

### 8. RFC 8188 encrypted representation

The initial encrypted representation format is RFC 8188 `aes128gcm`.

`contentEncoding` in the cipherBlob facet names this scheme and is the algorithm-agility pivot; a future content-encoding is selected by a different `contentEncoding` token without changing the cipherBlob abstraction.

It provides:

* streaming encryption and decryption;
* authenticated fixed-size records;
* seek/range processing at record boundaries;
* per-representation salt derived from `(key, plaintext digest)`, see §9;
* authenticated record ordering;
* padding support.

Daybook leaves the RFC 8188 `keyid` empty. Embedding a Daybook key or facet identifier into ciphertext would unnecessarily disclose stable metadata to relays.

The key is instead resolved through the private cipherBlob `keyRef`.

### 9. Representation creation, derived salt, reproducible generation

Creating a new cipherBlob selects fresh random keying material and fixed encoding parameters. The RFC 8188 salt is not selected: it is derived per representation as

```text
salt = BLAKE3-derive-key("daybook.cipherblob.salt.v1", K || P)[..16]
```

where `K` is the referenced JWK secret and `P` the plaintext content digest.

The reason is GCM safety. RFC 8188 derives per-message nonces from the salt, and a `(CEK, nonce)` pair reused over different plaintexts is catastrophic, not an inconvenient collision. Under a shared key, a salt collision could only arise from persisting the same salt for two representations, and no distributed write path in Daybook can enforce binding uniqueness. Deriving the salt from both inputs makes the collision unrepresentable: two different plaintexts under the same key cannot share a salt, on any device, without any coordination.

Determinism for serving is preserved: the derivation is a pure function of `(K, P)`, so re-encrypting a given plaintext with a given key reproduces byte-identical ciphertext (see §10, §12).

Because the key is mixed into the derivation, independent encryption domains that mint independent keys still produce uncorrelated ciphertext for identical plaintext (contrast with convergent encryption, below).

An explicit non-goal: rotating only the salt is impossible by design. Rotation means fresh keying material (§15). Salt-only rotation exists in systems where derived keys cross a root-key boundary and persist (KMS-style DEKs); Daybook's derived content-encryption keys never persist, so there is no artifact whose leak a salt rotation would mitigate.

After creation, the representation MUST be reproducible:

```text
Encrypt(
    plaintext,
    same key,
    same record size,
    same padding policy
)
    =
same ciphertext bytes
```

The salt is absent from this list because it is a function of the first two inputs. The reproducible inputs are exactly the values persisted in `encodingParameters` (`recordSize`, `padding`) together with the referenced JWK; reproducing a representation is a pure function of the plaintext P and the referenced key. The facet contents alone are deliberately insufficient, since the cipherBlob does not contain the plaintext digest.

The equivalence's inputs are also load-bearing for nonce safety, not just reproducibility, which is why the salt derivation binds the framing. Two representations of the same plaintext under the same JWK with different framings (a different `rs`, or a different padding policy) produce different record plaintexts - where the final delimiter lands, and what zero-padding follows it, both move. RFC 8188 derives every record's `(CEK, nonce)` from the salt alone (§2.2/§2.3), so a shared salt across framings would encrypt differing record contents under the identical key stream. The derivation therefore mixes the framing into the salt:

```text
salt = BLAKE3-derive("daybook.cipherblob.salt.v2",
    key ‖ P-hash ‖ BE32(rs) ‖ padding-domain-octet)
```

with the canonical input encoding fixed byte-exactly:

* the raw 32-octet JWK secret;
* the raw 32-octet BLAKE3 digest of the plaintext - the multihash payload of `representation.digest`, not its multihash-framed spelling;
* `rs` as a 4-octet big-endian value, the exact width the RFC 8188 header field carries (a value that does not fit is rejected at `encodingParameters` parse time, §3), so no framing can be derived that the header could not name;
* one octet identifying the padding policy (`Minimal = 1`, `Record = 2`; the values are frozen, and a future policy takes a fresh value rather than reshuffling existing ones, so derivations under pre-existing policies keep reproducing).

The `.v2` context bump is the derivation's version pin: the v1 derivation mixed only the key and the plaintext digest, under-bound w.r.t. framing. No ciphertext persists across the bump (nothing reads a stored salt - §3 takes it from each representation's own header), so the bump re-digests only future re-encryptions. Deliberately absent from the input list: the record sequence number, which participates per-record through RFC 8188's own nonce derivation; and the key-id header octets, which this implementation always encodes at length zero and skips when foreign - if key ids ever become load-bearing, they must join this list, and the context bump would move to `.v3`.

This is deliberately different from convergent encryption.

Identical plaintext encrypted independently:

```text
Encrypt(P, K1) = C1
Encrypt(P, K2) = C2
```

but regenerating an existing representation:

```text
Encrypt(P, K1) = C1
```

must always produce the exact same bytes.

### 10. Plaintext-only canonical storage

A device is not required to retain ciphertext locally.

For example:

```text
~/Photos/IMG_1002.jpg
    = canonical plaintext P
```

Daybook may retain only:

* the plaintext file;
* the Blob facet;
* the cipherBlob facet;
* its referenced JWK.

When ciphertext `C` is requested:

```text
plaintext P
    ↓
stream through deterministic representation encoder
    ↓
C
    ↓
network
```

Only bounded buffers are required.

The generated ciphertext MUST hash to the cipherBlob's recorded `representation.digest`.

This allows arbitrary external filesystem trees to remain usable by normal software without maintaining duplicate encrypted files.

### 11. Initial representation creation requires one full pass

A content-addressed ciphertext digest cannot generally be known before producing the ciphertext.

Creating a cipherBlob therefore requires one full streaming pass over the plaintext:

```text
plaintext P
    ↓
RFC 8188 encrypt
    ↓
hash ciphertext
    ↓
discard ciphertext
    ↓
representation digest C
```

This consumes bounded memory and no ciphertext-sized disk space.

When a relay subsequently pulls `C`, the peer may perform another plaintext read and encryption pass to regenerate and serve the representation.

The accepted tradeoff is therefore:

```text
extra sequential reads + encryption CPU
instead of
extra ciphertext-sized local storage
```

### 12. Virtual blob provider

Locally generated but non-retained ciphertext is represented by rebuildable local storage state.

Conceptually:

```text
VirtualBlob {
    representation_digest: C,
    source_content_digest: P,
    cipher_blob_ref: X
}
```

The blob provider behaves approximately as:

```text
request representation C

if stored physical blob C exists:
    serve it

else if C has a virtual representation:
    resolve source plaintext P
    resolve cipherBlob X
    resolve keyRef
    stream-encrypt P
    serve C
```

This mapping is local implementation state, not synchronized semantic state.

It can be reconstructed from materialized Blob and cipherBlob facets.

Integration with iroh-blobs is implemented on a fork. The intended design, in its implemented shape: iroh-blobs serves verified byte ranges using a BLAKE3 bao outboard alongside the data. For a virtual ciphertext, the outboard is computed during the §11 creation pass — the same streaming pass that produces `representation.digest` — and stored durably by the store (`build_outboard`), so serving does not recompute the hash tree. The outboard is inlined in the store's database, or kept as a file for large blobs, mirroring normal blobs. Each virtual entry durably records the name of the provider that serves it (`add_virtual`); the application registers live providers at startup (provider name → a random-access `ReadBytesAt` factory that resolves `representation_digest` to plaintext + cipherBlob + keyRef on demand). Serving reads data from the registered provider and verifies it against the stored outboard; an entry whose provider is not registered (or whose provider has no data for the hash) is served as not found. A SQLite/object-store iroh-blobs backend (for relay-grade durability and replication) is a separate future addition on the same fork.

The cipherBlob codec (RFC 8188 `aes128gcm`), the store flows, and the virtual ciphertext provider live in `daybook_core`'s `blobs` module (`daybook_core::blobs::encrypt`). Key storage follows §6: the JWK facet lives in the Keyhive-protected Automerge document, and encrypted-at-rest protection is inherited from the document layer - no key material is ever persisted in the blob store. Key resolution is a `CipherKeySource` over the document layer: ciphertext → cipherBlob facet (matched by `representation.digest`) → `keyRef` → JWK facet → secret. The store flows carry only `ct:`/`pt:` pair tags; key linkage goes through the facet graph. If blob plumbing is later extracted into a dedicated crate, preserve the dependency inversion by which the host application supplies the post-decrypt plaintext sink.

Receiving (download) is resumable: decrypted plaintext records are appended to a temporary spill file as their ciphertext records verify, and an interrupted attempt leaves the spill plus the decryption header facts on disk. The next attempt re-derives the progress watermark from the spill length (plaintext arrives in whole records until the final one) and requests only the missing ciphertext record suffix as a chunk-ranged fetch - the provider serves any byte window. This holds only for the ledger-backed path (`FsDownloadLedger`): the eager-retention import path (`ensure_local_blob_from_active_peers`) carries no ledger and no spill, so an interrupted eager fetch restarts from scratch rather than resuming. Resumability is therefore a property of a store configuration, not of the protocol.

A resumed download does not reuse the received ciphertext outboard fragments. A range-limited transfer only carries the parent fragments that verify the requested suffix, so the fragment set has gaps and cannot be reassembled by concatenation the way a full transfer's pre-order stream can. Instead, once the plaintext is complete, the receiving node re-generates `C` deterministically (the §11 pass) to install its own virtual outboard. This deliberately trades one extra local sequential read of `P` at resume completion against persisting an outboard scratch file. Warning for future investigation: if a resumed download appears to cost a full extra local read of `P` beyond the plaintext import itself, this re-encryption-for-outboard step is why. If it ever matters (very large blobs, frequent resumes), the alternative is to persist received `(TreeNode, pair)` fragments in the download ledger and scatter-merge them into the outboard via `BaoTree::pre_order_offset` at completion.

### 13. Blob inventories

Blob synchronization operates on physical representations.

Authorized repo peers may use ordinary inventories containing plaintext blob digests:

```text
P1
P2
P3
```

Relay sponsorship uses separate inventories containing encrypted representation digests:

```text
C1
C2
C3
```

A relay therefore only learns the physical representations selected for its retention domain.

Inventories are keyed per encryption domain and derived from the cipherBlob
facets readable in that domain's groups; no component authors a representation
pin that no facet entails. See §19.
#### The blob plane follows the docs plane's stream architecture

The doc plane orders local facts through one primitive: a revisioned store
whose mutations commit atomic revisions and whose readers replay strictly
forward (the group-part worker, `/seds`, the automerge frontier worker, and
the facet-set index are all instances). Its three layers are: a cross-node
routing plane (doc → keyhive-group partitions), a store-owned physical
presence plane (`/seds`: membership recorded exactly where the tree is
saved, a pending want where it is known but not held, removal where the
tree is deleted), and internal event-driven consumers (the automerge
frontier worker).

The blob plane now has the same three layers:

* **Routing plane (already):** the blob-inventory partitions — the
  `pins_part_worker` projects the replicated inventory docs' `BlobPin`
  facets into per-inventory blob-part rows. Blob objects are not keyhive
  principals, so inventory-doc membership is their access mapping; that is
  the blob analog of the group-part mapping.
* **Presence plane:** a local-only scope (`daybook-blobs-presence`) whose
  `/blobs` partition records the blobs this node *holds on disk* —
  membership written where the bytes land (`put`, `put_path_copy`,
  `put_path_reference`, `put_from_store`: the last covers download
  completions), a payload row plus membership in one idempotent sink call.
  Local-only, like the frontier and derived stores: presence is a local
  fact, and advertising it cross-node is the eager-retention design (a
  later, deliberate decision), not a side effect. The one-time boot
  announce replays existing on-disk blobs into the plane; membership is
  idempotent and single-event per store, so restart announcements cost
  nothing.
* **Internal consumers (encryption worker):** the worker follows two
  durable part-revision streams and converts them into the same keyed
  tasks the facet walker uses. `/blobs` arrivals re-arm documents whose
  `Blob` facet names the digest (the facet id *is* the plaintext digest,
  so the association is an id-keyed facet-set query); membership events on
  the eligibility group's part re-arm documents whose branch just became
  eligible. Together with the facet walker, three sources cover every
  ordering: blob before doc, doc before blob, eligibility before or after
  either. The two skip shapes that were previously quiet-and-permanent
  (plaintext not local; document not yet eligible) are each re-armed by one
  of the streams, so no boot-time re-derivation over the corpus remains:
  the boot pass is removed rather than demoted.

It does not need to know:

* their plaintext digests;
* their Blob facets;
* their cipherBlob facets;
* their JWKs;
* whether the bytes are encrypted at all.

### 14. Enabling relay retention for existing blobs

Selecting N existing blobs for encrypted relay retention performs approximately:

```text
for each Blob P:

    1. create fresh JWK K

    2. choose the padding policy

    3. stream plaintext P through encryptor and hash output

    4. obtain ciphertext representation digest C

    5. create cipherBlob X describing C and referencing K

    6. register C as a locally generatable virtual blob

    7. add a cipherBlob resolution URL to Blob P

    8. add C to the relay BlobPin/inventory
```

The relay pin is added only after the representation is locally ready to be served, either physically or virtually.

These operations SHOULD be designed to be idempotent so crashes can leave harmless unreferenced JWK/cipherBlob state that can later be resumed or garbage-collected.

### 15. Rotation semantics

There are three distinct operations.

#### Key relocation

The same encryption key is copied to a different key-storage mechanism:

```text
same C
same key K
old keyRef → new keyRef
```

Only `keyRef` changes.

This is not re-encryption.

#### Representation rotation within the same access domain

**Implemented shape (this PR, keyScope: Document).** A rotation runs as an
`EncryptionTaskKind::Rotate` on the same keyed scheduler and budget as
install/delta tasks, keyed on the same branch identity, so a rotation cannot
race an install or a delta for the same document. The trigger is an explicit
request (`Rt::request_doc_representations_rotation`) carried to the worker's
facet machine through a channel; the machine's budget gates it like every
other task. `rotate_document` reconciles first, then rotates each existing
representation: a fresh `MasterKey` and §16 migration (a fresh key-storage
document per rotation), §11 install of the new ciphertext, then the cipherBlob
facet updated **in place under its unchanged facet key** at fresh heads — that
in-place update is the commit point. The old ciphertext's release is *not*
written by the rotation: it rides the reactive pin diff (§19), so the "wait
for required retention acknowledgement" and "eventually remove" of the safe
relay rotation above are realized as the pin worker's release leaf observing
the facet delta. The rotation task carries the branch's pending delta cursor
through the scheduler and acknowledges it on durable success, so a rotated
delta cannot gate the walker's durable prefix forever. Crash windows: a fault
at entry leaves nothing registered (retry is idempotent and orphan-free); a
fault after §11 install leaves the new pair rooted-but-unreferenced — the
declared window, whose eventual collect is GC-era work on the same fork, and
the retry recovers to the correct end state.
Generate fresh keying material and a fresh encrypted representation:

```text
cipherBlob X:

C1 / K1
   ↓
C2 / K2
```

The cipherBlob facet is updated in place. Salt-only rotation is deliberately impossible (§9); every representation rotation mints fresh keying material.

Because the Blob points to cipherBlob X rather than directly to C1, application references do not change.

A safe relay rotation is:

```text
generate C2
→ make C2 locally servable
→ add C2 to relay inventory
→ wait for required retention acknowledgement
→ update cipherBlob X to C2/K2
→ eventually remove C1 from relay inventory
```

#### New privacy/access domain

When the purpose of re-encryption is to prevent correlation with an existing domain, create a new cipherBlob instead:

```text
Blob P
   ├── cipherBlob X → C1/K1
   └── cipherBlob Y → C2/K2
```

The new domain receives Y.

No predecessor/successor link is created by default because such a link would explicitly reveal the correlation that separate encryption domains are intended to obscure.

### 16. Historical key material

Automerge retains historical document state.

Removing or replacing a JWK facet therefore does not imply cryptographic erasure of old key material.

If stronger isolation is required, Daybook may:

* create a new key-storage document;
* create/update the appropriate cipherBlob;
* migrate current references;
* stop granting access to the old Keyhive document.

Because key-storage location is not blob identity, this migration does not require changing the logical Blob identity.

Actual historical crypto-erasure additionally requires deletion of the storage containing the old Automerge history and cannot revoke keys previously copied by an authorized reader.

### 17. Length hiding

Relay-visible representation length leaks approximate plaintext size.

RFC 8188 padding is used as the first mitigation.

RFC 8188 divides the plaintext into fixed-size **records** of `recordSize` (`rs`) octets of *ciphertext*; each record is an independent AES-128-GCM invocation with its own 16-octet authentication tag and a nonce derived from its sequence number. The plaintext fed to each record is `content ‖ delimiter ‖ zero-padding`, where the delimiter is `0x01` for non-final records and `0x02` for the final record; the final record may be shorter than `rs`. Records are what give the encoding its streaming, seek/range, and truncation-detection properties (see §8). The RFC mandates the delimiter and tag but leaves the *amount* of trailing zero padding to the application; Daybook parameterizes this as the `padding` field of `encodingParameters`.

Initial policies:

```text
minimal
    final record contains only the 0x02 delimiter and no extra zero octets;
    ciphertext length is the minimum the format permits, leaking plaintext
    length to the relay down to record granularity.

record
    final record is zero-padded so the whole encoded body rounds up to an
    exact multiple of rs; the relay observes only a whole number of records
    and cannot see the precise tail length. Costs at most one record of overhead.
```

The v1 default is `record`.

More aggressive schemes, such as powers-of-two or fixed-size storage buckets, are deferred because their storage and transfer overhead can be substantial for arbitrary blobs.

### 18. Plaintext blobs

Encryption remains optional.

A Blob that needs no encrypted representation simply has:

```text
Blob P
    ↓
plaintext representation P
```

No cipherBlob or JWK is required.

cipherBlob therefore never needs a special plaintext mode.

### 19. Encryption and pin maintenance

§14 describes the steps that create a representation. This section fixes who
performs them, what the metadata is keyed by, and the order the steps must
happen in, because none of these are derivable from §14 alone.

#### Domains are named, and the facet key is the name

A facet key names the encryption domain a representation belongs to:

```json
{
  "org.example.daybook.cipherBlob/relay": { ... }
}
```

Daybook declares its domains; it does not discover them. A domain named for a
Keyhive group uses a derived facet key:

```text
grp:<base58(multibase(group_id))>
```

using the same internal base58-multibase convention as the rest of the repo.
Facet-key prefixes other than `grp:` are reserved for domains that are not
Keyhive groups, so a reader can always tell which kind of authority governs a
domain without resolving it.

A domain is therefore not a schema object: it is a facet key in a document that
some group can read. Membership in the group is what grants access to the
cipherBlob metadata, and the document's own access control bounds who may learn
that the domain exists.

The same-access rule bounds what the facet key may encode. A cipherBlob
facet key may embed the sibling `Blob` facet's own id, and a `Blob.urls`
`?via=` entry may embed the facet key, because both travel inside the
content document: any reader who can see them already holds the plaintext
digest from the `Blob` facet itself. The facet *body* never names the
plaintext digest, and the inventory pins a domain reads carry only the
ciphertext digest. A cipherBlob that names a blob facet in another document
has no sibling to lean on and must use a keyid scheme that does not derive
from that document's readership - an open shape, deferred to cross-document
keyRef (Deferred Decisions).
#### JWK and cipherBlob are placed in different documents, deliberately

`keyRef` names the JWK facet that holds the encryption key, addressing a
document explicitly: `self` for one in the same document, or a document id
(docs/dict.md, "URLs"). The two facets do not travel together:

```text
content document                      key document
  Blob facet                            JWK facet
  cipherBlob facet                       (readable by decryptors)
   (readable by anyone who
    may serve the blob)
```

The cipherBlob lives beside the Blob facet, because anyone who may serve a
representation must know which representation to serve. The JWK lives in a key
document granted to the *decryptor* group, which is normally `repo_agents`. A
retention domain can therefore be given the cipherBlob - enough to store and
serve the ciphertext - without ever being given the key.

This is the structural form of the rule that encryption keys are never shared
with mere storers or readers of the ciphertext. It is not enforced by omitting
a field: it is enforced by the facets living in documents with different
readers.

#### `keyScope` chooses how many key documents a domain has

A domain's configuration carries:

```text
keyScope: Domain | Document
```

`Domain` gives each (group, domain) pair one key document for all blobs in that
domain. `Document` gives each (group, domain, content document) pair its own
key document.

The default is `Document`, for two reasons. First, rotation: rotating the key
for one document is a local act, while rotating a domain-wide key re-encrypts
every representation in the library, and nobody wants to rotate an entire photo
archive in one step. Second, and less obviously, equality leaks: RFC 8188
encryption here is deterministic in (key, salt, plaintext) and the salt is
derived from the plaintext digest (§9), so two documents holding identical
plaintext produce identical representation digests *within one domain*. With
`keyScope: Domain` that identity is visible across the whole library; with
`keyScope: Document` it is visible only within one document. Narrowing the
scope narrows the leak.

The facet key stays `grp:<group_id>` under both settings, because `keyRef`
already names a document and therefore already distinguishes the keys.

#### Pins are derived from facets, never authored

The set of representations a peer sponsors is a function of the cipherBlob
facets it can read, in the groups it serves:

```text
declared domain + group docs
        ↓
live cipherBlob facets
        ↓
set of representation digests C
        ↓
BlobPins for that domain
```

No component writes a representation pin that no facet entails, and no
component infers a facet from a pin. A pin that outlives its facet is not a
stale-cache annoyance: it is the mechanism by which a representation is
retained, so it is also the mechanism by which a deleted representation keeps
occupying a relay forever (see the release path below).

The store-level roots are the `ct:`/`pt:` tags written when a pair is
registered (§12). These are ordinary named tags, and named tags are what the
store's garbage collector treats as roots, so they are sufficient to keep both
the representation and the plaintext it is served from alive.

#### Ordering, and the commit point

Cross-document steps cannot share an Automerge transaction, so order is the
only atomicity available, and it has to be chosen so that every prefix of the
sequence is safe to be crashed in:

```text
1. §11 pass over P: compute C, install the virtual entry, register the pair
2. write the JWK facet
3. write the cipherBlob facet              <- C becomes nameable
4. derive and write the pin for C
5. write Blob.urls resolution through the cipherBlob   <- commit point
```

The representation is fully servable before anything points at it, and the
resolution that lets a reader reach C is written last. A crash before step 5
leaves the virtual entry and the pair tags unreferenced but inert: nothing
reads `ct:`/`pt:` tags without a cipherBlob facet naming `representation.digest`
(pins are derived from facets, below), so no reader error can surface from the
gap. There is deliberately no rescanning mechanism - no outbox, no boot-time
orphan sweep. Because the §11 pass is deterministic, the same document's next
encryption attempt re-derives the identical digests, finds the registered pair,
and completes the facet write; the retry IS the collector. The one case no
retry reaches - the document deleted before any retry, leaving a forever-
unreferenced pair - is left for the release path below to collect, and costs
nothing until then: an inert pair tag is dead storage, not a correctness hazard.

A pin is written only after the representation is servable, per §14. That is
the same ordering constraint expressed from the other side: servability is
established at step 1, and step 4 is the first moment that fact may be
published.

#### The release path is deliberate

Because `ct:`/`pt:` are named tags and named tags are GC roots, nothing removes
them incidentally. Releasing a representation is an explicit act:

```text
remove cipherBlob facet
        ↓
derived pin disappears
        ↓
delete ct:/pt: tags
```

The last two steps happen in that order deliberately: the pin row leaves the
inventory diff, its pair is released from the store, and only then is the
removal written. A crash in the gap releases a pair whose pin row still
exists, which serves a not-found until the retry lands the removal - the
inverse order would strand a GC root that nothing will ever claim again.
A domain that skips the release step entirely leaks both the
representation's outboard and its plaintext permanently. Release therefore belongs to the same component that
derives pins, and is reconciled the same way - from facets, towards the store -
rather than being a side effect of deleting a document.
This release is deliberately reactive, with no positive-evidence scan in
front of it: the diff that drives it is computed from pin rows that only
legitimate facet deltas touch - a branch tombstone, or a per-branch
rehydrate at current heads - and a transient unreadability leaves the
desired set untouched (the unmaterialized-heads read errors out and the
keyed scheduler retries rather than writing a shrink). A pin therefore
cannot be removed and re-created by anything except a real facet removal
followed by a real re-add, and because the salt is deterministic both
representations re-derive byte-identically and `register_pair` re-roots
the pair, so even that window is self-healing as long as the re-encryption
run happens before a GC collects the unrooted bytes.

#### One worker per group, using the existing group machinery

Maintenance is per group and reuses the existing worker scope and part worker
templates:

* `WorkerGroupScope::Groups(HashSet<PartId>)` selects exactly the documents
  belonging to at least one served group authority, evaluated against live
  keyhive state when the event is handled - never against group-part
  assignment. A relay's domain can therefore grow and shrink without racing the
  group-part worker.
* The part worker owns its dependency graph: every derived task retains the
  source admission row that caused it, and that row is acknowledged only after
  the derived work has completed successfully.

The encryption worker's two facet kinds are handled as two explicit branches
(Blob facet, cipherBlob facet) rather than behind a schema-level accessor. The
only shared helper answers the narrower question a branch actually needs: does
this facet name a blob that is stored outside the document, whether by urls or
inline.

### 20. Random-access decryption

Records are keyed by sequence number and authenticated independently, so plaintext is addressable rather than sequential: reading a plaintext range decrypts only the records that range overlaps, and memory is bounded by the range instead of by the file.

Framing for such a read comes from the ciphertext's own authenticated header, not from the facet - `rs` is on the wire, so a reader never has to be told it. The plaintext length is the one thing the wire cannot supply: RFC 8188 does not encode it, and under `Padding::Record` the padded tail deliberately hides it (§17). A reader therefore takes the length from its own metadata (the Blob facet's `lengthOctets`) and checks it against the ciphertext in both directions - the body bounds it from below, and the final record's real extent bounds it from above, so a length that is too small fails rather than silently truncating what is read.

This is what makes a large encrypted video playable from a byte range on a device that has the ciphertext: start playing without decrypting, downloading, or buffering the whole file. It is also the exact mirror of §12 - the same framing facts serve ciphertext derived from plaintext and plaintext derived from ciphertext.

## Layering

The resulting model is:

```text
Application metadata
        │
        ▼
Blob facet
  plaintext content identity P
  MIME / logical metadata
  available resolution URLs
        │
        ├──────────────────────→ plaintext representation P
        │
        ▼
cipherBlob
  encrypted representation C
  encoding
  keyRef
        │
        ▼
JWK / another key store
        │
        ▼
key material K
```

Separately:

```text
normal repo inventory
    → plaintext physical representations P

relay BlobPin/inventory
    → opaque physical representations C
```

Neither synchronization nor key storage defines blob identity.

## Rejected Alternatives

### Encrypt every blob at import time

Rejected because it would require users to encrypt entire existing libraries even when no untrusted storage is being used, and could force duplicate plaintext/ciphertext storage.

### Make cipherBlob the canonical Blob

Rejected because encryption is optional and representation-specific.

### Make cipherBlob point back to Blob as the primary resolution path

Rejected because resolving a Blob to its encrypted representation would then require maintaining a reverse index.

The canonical Blob instead advertises available cipherBlob mirrors directly.

### Store ciphertext permanently on every authorized peer

Rejected because external plaintext files should be allowed to remain canonical and directly usable by non-Daybook software.

### Deterministic/convergent encryption from plaintext identity

Rejected because it would allow unrelated storage domains to correlate identical plaintext through identical ciphertext. The salt derivation of §9 stays on the right side of this boundary: the secret is a mandatory derivation input, so domains with independent keys produce uncorrelated ciphertext; only encryption under the *same* key is deterministic.

### Put Daybook key identifiers in RFC 8188 ciphertext

Rejected because relays do not need this information and it would create unnecessary stable correlation metadata.

### Reify the RFC 8188 binary header as an opaque facet field

Rejected because it bakes one scheme's wire format into the cipherBlob abstraction and hurts algorithm agility. The per-scheme reproduction inputs are carried as typed `encodingParameters` whose schema is defined by `contentEncoding`.

## Deferred Decisions

Separate ADRs will define:

* exact `db+blob` URI and resolution-hint syntax;
* whether an in-document `self` `keyRef` should ever be accepted - the
  implementation refuses it (an empty-heads reference means "this document",
  and §19 places the JWK and cipherBlob in different documents because the
  Keyhive protection layers differ, §6, so the in-document form has no
  legitimate user; cross-document referencing with pinned `keyRefHeads` is
  implemented);
* BlobPin/inventory schemas and relay completion receipts;
* virtual encrypted iroh-blobs integration;
* ciphertext caching policy;
* the default RFC 8188 record size for new representations (the codec reads
  whatever an existing representation declares, so this is now only a policy
  choice, not a wire-format commitment);
* stronger padding profiles (powers-of-two, fixed-size buckets);
* multi-key/keyring optimization;
* garbage collection of stale cipherBlobs, JWKs, and representations;
* SQLite/object-store iroh-blobs store backend for relay-grade durability and replication.
