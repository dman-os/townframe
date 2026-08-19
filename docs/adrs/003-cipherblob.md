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
  "org.example.daybook.cipherblob/relay": {
    "representation": {
      "digest": "<ciphertext-representation-digest>",
      "lengthOctets": 125337
    },

    "contentEncoding": "aes128gcm",

    "keyRef": "db+facet:///self/org.example.daybook.jwk/relay",

    "encodingParameters": {
      "salt": "<base64url>",
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

`contentEncoding` is the algorithm pivot: its value is an HTTP content-coding token (e.g. `aes128gcm`), and the schema of `encodingParameters` is defined by `contentEncoding`. A future encryption scheme is a new `contentEncoding` value with its own `encodingParameters` shape; existing `aes128gcm` cipherBlobs remain decryptable without migration. Daybook deliberately does not reify RFC 8188's binary header as a standalone opaque field, because that would bake one scheme's wire format into the abstraction and hurt agility.

For `aes128gcm`, `encodingParameters` carries exactly the per-representation inputs needed to reproduce the ciphertext: `salt` (base64url, 16 octets, the only per-representation entropy), `recordSize` (the RFC 8188 `rs`), and `padding` (the deterministic padding policy, see §17). The RFC 8188 `keyid` is always empty (see §8) and therefore omitted from the facet.

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

The facet value SHOULD remain a valid JWK rather than wrapping it inside a Daybook-specific key structure.

For RFC 8188 `aes128gcm`, the JWK secret is used as the Input Keying Material (IKM): the AEAD content-encryption key is derived by RFC 8188's own HKDF-SHA256 from `(salt, IKM)`, so Daybook performs no additional key derivation. Because each cipherBlob contributes its own random `salt`, reusing one JWK across many cipherBlobs yields a different content-encryption key per representation and does not correlate their ciphertexts.

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
* per-representation random salt;
* authenticated record ordering;
* padding support.

Daybook leaves the RFC 8188 `keyid` empty. Embedding a Daybook key or facet identifier into ciphertext would unnecessarily disclose stable metadata to relays.

The key is instead resolved through the private cipherBlob `keyRef`.

### 9. Random representation creation, reproducible generation

Encryption is randomized when a representation is created.

Creating a new cipherBlob selects:

* fresh random keying material;
* fresh random salt;
* fixed encoding parameters.

Thus unrelated encryption operations over identical plaintext do not produce the same ciphertext.

However, after creation, the representation MUST be reproducible:

```text
Encrypt(
    plaintext,
    same key,
    same salt,
    same record size,
    same padding policy
)
    =
same ciphertext bytes
```

These are exactly the values persisted in `encodingParameters` (`salt`, `recordSize`, `padding`) together with the referenced JWK; reproducing a representation is a pure function of the facet contents and the referenced key.

This is deliberately different from convergent encryption.

Identical plaintext encrypted independently:

```text
Encrypt(P, K1, S1) = C1
Encrypt(P, K2, S2) = C2
```

but regenerating an existing representation:

```text
Encrypt(P, K1, S1) = C1
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

The cipherBlob/JWK facet codecs, the virtual provider, the blob sync backend, and the virtual-ciphertext local store are housed in a `big_blobs` crate that does not depend on `daybook_core`. `daybook_core` depends on `big_blobs` and supplies the post-decrypt local-plaintext sink via a trait defined in `big_blobs` (dependency inversion).

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

    2. choose representation encryption parameters

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

Generate fresh keying material and a fresh encrypted representation:

```text
cipherBlob X:

C1 / K1
   ↓
C2 / K2
```

The cipherBlob facet is updated in place.

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

Rejected because it would allow unrelated storage domains to correlate identical plaintext through identical ciphertext.

### Put Daybook key identifiers in RFC 8188 ciphertext

Rejected because relays do not need this information and it would create unnecessary stable correlation metadata.

### Reify the RFC 8188 binary header as an opaque facet field

Rejected because it bakes one scheme's wire format into the cipherBlob abstraction and hurts algorithm agility. The per-scheme reproduction inputs are carried as typed `encodingParameters` whose schema is defined by `contentEncoding`.

## Deferred Decisions

Separate ADRs will define:

* exact `db+blob` URI and resolution-hint syntax;
* BlobPin/inventory schemas and relay completion receipts;
* virtual encrypted iroh-blobs integration;
* ciphertext caching policy;
* default RFC 8188 record size;
* stronger padding profiles (powers-of-two, fixed-size buckets);
* multi-key/keyring optimization;
* garbage collection of stale cipherBlobs, JWKs, and representations;
* SQLite/object-store iroh-blobs store backend for relay-grade durability and replication.
