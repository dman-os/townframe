# ADR 001: Blob Inventories and Pins Drive Blob Synchronization

**Status:** Accepted.

## Context

Daybook documents refer to blobs (large, immutable byte sequences like images, media, and attachments) by content digest using `db+blob` URLs.

We want blob synchronization to support:

* ordinary peer-to-peer replicas;
* hosted or self-hosted relays;
* end-to-end encrypted blobs where relays never receive decryption keys;
* plaintext/public blobs through the same synchronization machinery;
* future encryption schemes without coupling the blob synchronization layer to encryption;
* content-addressed interoperability with systems outside Daybook;
* completely self-contained application documents where application developers and plugs do not have to perform manual synchronization bookkeeping.

### Separation of Concerns

Blob identity, blob encryption/access control, and blob synchronization are three strictly decoupled concerns:

1. **Logical Identity (Application Layer):** Named by **Content Digest** (the digest of plaintext bytes). Application documents use logical `org.example.daybook.blob` facets describing MIME types, logical digests, and optional inline payloads.
2. **Access Control & Keys (Crypto Layer):** Keyhive-protected Automerge **KEM / Keyring documents** encapsulate decryption keys. Access to keys is governed by Keyhive capabilities (`concap`).
3. **Synchronization & Retention (Transport Layer):** Named by **Representation Digest** (the digest of exact stored/transferred bytes, whether ciphertext or plaintext). Synchronized via iroh-blobs and local Big Sync partitions.

A relay needs to know **which physical blob representations it should retain**, but it does not need to know:

* the logical/semantic identity of a blob;
* its plaintext digest;
* its encryption key;
* its KEM / Keyring document;
* its MIME type;
* whether the representation is encrypted at all.

Big Sync partitions are local derived and idiosyncratic state. Partition membership, event history, transaction IDs, and peer cursors are not themselves shared state. Peers gossip and replay their local partition events, and other peers react by performing local writes and maintaining their own partition stores.

Therefore, we need shared Automerge state from which peers and relays can independently derive compatible blob working sets.

---

## Decision

### 1. Multihash Digest Specification

All cryptographic digests across Daybook's blob architecture—both **Content Digests** (logical/plaintext) and **Representation Digests** (transport/storage)—MUST be encoded as self-describing **Multihashes** (e.g. base58btc or base32 multibase strings).

This ensures:
* Algorithm agility (e.g. BLAKE3, SHA-256) is explicit and self-describing;
* Compatibility across content-addressed subsystems without ad-hoc prefixing;
* Clean representation inside URLs and facet keys.

---

### 2. Blob Pin Facets (`org.example.daybook.blobPin`)

We introduce a dedicated facet tag for declaring physical blob retention: **`org.example.daybook.blobPin`**.

Each pinned representation is represented as an individual facet whose key ID is strictly the **representation multihash**:

```json
{
  "org.example.daybook.blobPin/<representation-multihash-1>": {
    "lengthOctets": 12345
  },
  "org.example.daybook.blobPin/<representation-multihash-2>": {
    "lengthOctets": 67890
  }
}
```

The representation multihash identifies the exact bytes stored and transferred by the blob backend (iroh-blobs):
- For an encrypted blob, this is the multihash of the **ciphertext** representation.
- For a plaintext blob, this is the multihash of the **plaintext** representation.

`lengthOctets` is stored as an integer field in the JSON payload for quota enforcement, progress reporting, admission decisions, and storage planning.

#### Why Bare Multihashes in Facet Keys (No Embedded Length / Tickets)
Iroh tickets (`BlobTicket`) or compound strings bundle transport addresses and lengths into a single string. In Daybook:
1. **CRDT Key Determinism:** The facet key ID is strictly the representation multihash. Storing only the multihash in the key guarantees that concurrent pins for the exact same byte content merge to the exact same map key without parsing ambiguity or string-formatting discrepancies.
2. **Durable vs Ephemeral:** Transport endpoints change over time; connection routing is handled by Keyhive and Big Sync actor transports, not embedded in durable Automerge CRDTs.
3. **Structured Metadata:** Size and future sync parameters are kept in the typed JSON payload (`lengthOctets`), making queries, schema validation (`schemars`), and SQLite indexing clean and robust.

#### Strict Omissions
A `blob_pin` facet MUST NOT contain:
* plaintext/content digests for encrypted blobs;
* KEM or Keyring document IDs;
* application-level blob URLs;
* MIME types;
* an `encrypted` flag;
* stable identifiers correlating different representations of the same logical content.

The pin is strictly transport- and encryption-agnostic.

---

### 3. Blob Inventory Documents & Inline Pins

`blob_pin` facets can be declared in two ways:

1. **Blob Inventory Documents:** Ordinary Daybook documents dedicated to aggregating $N$ `blob_pin` facets. A repository or account initially uses a default inventory document and can shard into multiple inventories if size or operational requirements dictate.
2. **Inline Sponsored Documents:** Public or shared documents (e.g. a shared notebook or public gallery) can include `blob_pin` facets directly alongside their application `blob` and `imagemetadata` facets.

The synchronization projection treats both identically: any document in an authorized sponsorship group containing `blob_pin` facets contributes to the local blob partition.

---

### 4. Authority & Sponsorship Groups

In [`authority.rs`](../../src/daybook_core/authority.rs), repositories declare standard Keyhive authority groups:
- `repo_agents`
- `core_docs`
- `content_docs`
- `default_drawer`
- **`blob_inventories`** (new)

The repository's default Blob Inventory document is placed in the `blob_inventories` group.

#### Relay Sponsorship
A relay or storage peer is granted read/sync access to designated Keyhive groups:
- Granting access to `blob_inventories` allows a relay to retain and synchronize all encrypted representations without gaining read access to private content documents.
- Granting access to `content_docs` allows a relay to synchronize public/shared documents and any inline `blob_pin` facets they contain.

```text
Keyhive Sponsored Group (e.g. blob_inventories)
        ↓
Automerge Documents containing blob_pin facets
        ↓ Materialized locally
Representation Multihashes + Lengths
        ↓
Local Big Sync Blob Partition
        ↓
iroh-blobs transfer backend
```

---

### 5. Automated Inventory Maintenance

Application documents remain self-contained. Application code, UI components, and plugs write standard logical `org.example.daybook.blob` facets and do not interact directly with inventory documents.

A local background indexer worker (in `index.rs`):
1. Observes document changes from the Drawer.
2. Identifies `org.example.daybook.blob` facets on local documents.
3. Resolves the corresponding representation multihash:
   - For plaintext blobs: $H_{rep} = H_{plain}$.
   - For encrypted blobs: $H_{rep} = H_{cipher}$ (produced during blob ingestion/encryption).
4. Automatically upserts the corresponding `org.example.daybook.blobPin/<H_rep>` facet in the local Blob Inventory document.
5. When local references to a blob are removed, deletes the corresponding `blobPin` facet.

---

### 6. Big Sync Partition Projection

The projection engine ([`DocBlobsIndexRepo`](../../src/daybook_core/index/doc_blobs.rs)) subscribes to drawer events for all readable documents in sponsored groups:

```text
blob_pin added for multihash H
    ↓
Local projection adds Blob(H) to local Big Sync partition
    ↓
Local partition event generated

blob_pin removed for multihash H (and no other pin references H)
    ↓
Local projection removes Blob(H) from local Big Sync partition
    ↓
Local partition event generated
```

The projection is:
* **idempotent**;
* **reference-counted** across all documents/branches;
* **fully rebuildable** from current document state;
* **independent** of remote peers' internal partition history or transaction IDs.

---

### 7. Blob Transfer & Garbage Collection

* **Transfer:** Big Sync reconciles partition event logs between peers. When a peer or relay discovers missing representation multihashes, it pulls the bytes via `iroh-blobs` and persists them.
* **Garbage Collection:** Removing all `blob_pin` facets for a multihash removes it from the desired working set. Physical deletion from the local store or relay occurs after an implementation-defined grace period once no active pin requires it.

---

## Encryption Separation & Identity

Encryption is intentionally outside the synchronization loop.

```text
Plaintext Content
      ↓ (Encryption)
Ciphertext Representation ──(Multihash)──► Representation Multihash ──► blob_pin facet ──► Big Sync / Relay
      │
      └─► Key Material / Nonce ──► Keyhive-protected KEM / Keyring Doc (Private)
```

The relay sees only the ciphertext representation multihash and bytes.

### Multi-Domain Re-Encryption
Re-encrypting the same plaintext for different sharing domains produces distinct ciphertext representations ($C_1, C_2$) with different multihashes ($H_1, H_2$). The sync layer treats $H_1$ and $H_2$ as completely unrelated blobs, preventing metadata leakage across access domains.

### Blob Identity and URIs
Application-level references remain content-addressed by **Content Multihash** (plaintext hash):

```text
db+blob:///<content-multihash>
```

Resolution metadata can be attached as optional query hints:

```text
db+blob:///<content-multihash>?kem=<kem-doc-id>
db+blob:///<content-multihash>?keyring=<keyring-doc-id>&slot=<slot-id>
db+blob:///<content-multihash>?size=12345
```

The content multihash is authoritative for identity. Resolution hints assist in locating key material and representation hashes without altering content identity. If hints are absent or outdated, the client queries a local index of known KEM/Keyring documents.

---

## Terminology

* **Blob:** Logical immutable byte content.
* **Content Multihash / Content Digest:** Multihash of logical/plaintext bytes (canonical application identity).
* **Representation:** Concrete byte sequence used for storage and network transfer (ciphertext or plaintext).
* **Representation Multihash / Representation Digest:** Multihash of the concrete representation bytes.
* **Blob Pin (`blobPin`):** An Automerge facet `org.example.daybook.blobPin/<rep-multihash>` requesting retention/replication of a representation.
* **Blob Inventory:** An Automerge document containing $N$ `blobPin` facets.
* **KEM / Keyring Document:** A private, Keyhive-protected Automerge document containing key encapsulation material for decrypting representations.
* **Blob Partition:** Local derived Big Sync partition driving peer-to-peer and relay byte replication.

---

## Consequences

### Positive
* **Decoupled Architecture:** Relays remain simple pull-oriented peers requiring no decryption keys or plaintext awareness.
* **Encryption Agnostic:** Plaintext and encrypted blobs share the exact same synchronization machinery.
* **Zero DX Overhead:** Application code and plugs write standard self-contained documents; inventory maintenance is automatic.
* **Fine-Grained Authority:** `blob_inventories` group allows sponsoring blob storage without exposing content documents.
* **Granular CRDT Concurrency:** Facet-per-blob `blob_pin/<multihash>` avoids merge conflicts when adding/removing pins concurrently.
* **Crypto Agility:** Self-describing multihashes prevent algorithm lock-in.

### Costs & Trade-offs
* Requires a local background worker to maintain `blob_pin` facets from application `blob` facets.
* Inventories reveal representation multihashes and mutation history to authorized relays (traffic/size correlation remains possible).
* Encrypted content maintains two multihashes: content multihash for identity and representation multihash for storage.

---

## Deferred Decisions

Separate ADRs will define:
1. **Streaming Blob Encryption:** Chunked AEAD format (e.g. RFC 8188 / ChaCha20-Poly1305) over iroh-blobs.
2. **KEM and Grouped Keyring Documents:** Document schemas and Keyhive capability integration for key distribution.
3. **URL Resolution Protocol:** Detailed parsing and fallback indexing for `db+blob:///` hints.
4. **Relay Retention Policies:** Explicit acknowledgement watermarks and GC grace periods.
