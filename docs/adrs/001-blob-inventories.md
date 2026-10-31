# ADR 001: Blob Inventories and Pins Drive Blob Synchronization

**Status:** Accepted, amended by [ADR 026](026-drawer-native-blobs.md).

The inventory is now a **drawer meta doc**: a separate document referenced from the
drawer's metadata doc by the fixed slot `org.example.daybook.drawer.metaDoc/blobInventory`,
granted to the drawer's meta group, projected and reached through Cabinet's drawer
projection. Withdrawn: the default per-repository inventory document (§3), the
`blob_inventories` authority group and its relay grant (§4), and inline pins in
sponsored content docs (§3, deferred with public drawers). Big Sync blob partitions use
the drawer's own groups, and each drawer with an inventory has its own (§6). The pin
datum's shape, strict omissions, multihash digests, transfer and GC semantics are
unchanged. See ADR 026 §5 and §7.

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
2. **Access Control & Keys (Crypto Layer):** decryption keys are JWK datums co-located with the cipherBlob datums that use them (ADR 026 §2). Access to keys is therefore access to the content doc, governed by Keyhive and by the drawer the doc is entered in.
3. **Synchronization & Retention (Transport Layer):** Named by **Representation Digest** (the digest of exact stored/transferred bytes, whether ciphertext or plaintext). Synchronized via iroh-blobs and local Big Sync partitions.

A relay needs to know **which physical blob representations it should retain**, but it does not need to know:

* the logical/semantic identity of a blob;
* its plaintext digest;
* its encryption key;
* the key material of the doc it came from;
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
* key datum identities;
* application-level blob URLs;
* MIME types;
* an `encrypted` flag;
* stable identifiers correlating different representations of the same logical content.

The pin is strictly transport- and encryption-agnostic.

---

### 3. Blob Inventory Documents & Inline Pins

**Amended by ADR 026 §5.** An inventory is always a separate document, owned by one
drawer: it is a drawer meta doc, created by a DrawerService op that delegates it to the
drawer's meta group at Admin and then writes the fixed `drawer.metaDoc/blobInventory`
slot on the drawer's metadata doc. There is no default per-repository inventory. A drawer
that needs to shard may hold several inventories under several kinds, and sharding is a
deferred decision as before.

Inline pins in public or shared content documents are **deferred**, together with
plaintext pins for public drawers (ADR 026, "Open and deferred"). A content doc's pins
are maintained in its drawer's inventory instead.

The synchronization projection reads inventories, not content docs: the representation
digests a drawer retains are the union of its own inventory's pins and, for a relay
walking a sponsorship closure, the inventories of the drawers it reaches.

`blob_pin` facets were declared in two ways:

1. **Blob Inventory Documents:** Ordinary Daybook documents dedicated to aggregating $N$ `blob_pin` facets. A repository or account initially uses a default inventory document and can shard into multiple inventories if size or operational requirements dictate.
2. **Inline Sponsored Documents:** Public or shared documents (e.g. a shared notebook or public gallery) can include `blob_pin` facets directly alongside their application `blob` and `imagemetadata` facets.

The synchronization projection treats both identically: any document in an authorized sponsorship group containing `blob_pin` facets contributes to the local blob partition.

---

### 4. Authority & Sponsorship Groups

**Amended by ADR 026 §5 and §8.** The `blob_inventories` group does not exist. Drawers
supply the groups: an inventory is delegated to its drawer's **meta group**, and
inviting a relay at Keyhive `Relay` already grants it Read on that meta group, so relays
read pins without any inventory-specific grant. Relay sponsorship names **one root
sponsorship drawer** per account and grows through child entries
([ADR 005](005-relay-sponsorship-retention-admission.md) §1 as amended), not a flat
`sponsorship_group` of documents.

```text
Drawer meta group (relays hold it at Read)
        ↓
Inventory meta docs carrying blob_pin facets
        ↓ Materialized locally
Representation Multihashes + Lengths
        ↓
Per-drawer Big Sync blob partitions
        ↓
iroh-blobs transfer backend
```

---

### 5. Automated Inventory Maintenance

Application documents remain self-contained. Application code, UI components, and plugs write standard logical `org.example.daybook.blob` facets and do not interact directly with inventory documents.

**Amended by ADR 026 §5 and §9.** Maintenance is per drawer, driven by that drawer's
inventory slot existing, and observes through Cabinet's `PollDatums`
([ADR 025](025-cabinet-datum-predicates-and-polling.md)) with an
`InDrawer { transitive: false }` predicate, so roster joins and leaves arrive on the same
cursor as datum changes. It is delta-driven and never a full-set reconcile: several
writers maintain one inventory, and a writer whose view lags must not delete pins for
facets it has not seen. Removals happen only on an observed cipherBlob removal or an
observed removal of the doc from the drawer.

A local background indexer worker ([`BlobPinWorker`](../../src/daybook_core/blobs/pin_worker.rs)):
1. Observes document changes from the Drawer.
2. Identifies `org.example.daybook.blob` facets on local documents.
3. Resolves the corresponding representation multihash ($H_{rep}$):
   - For plaintext blobs: $H_{rep} = H_{plain}$.
   - For encrypted blobs: $H_{rep} = H_{cipher}$ (produced during blob ingestion/encryption).
   - Valid blob URLs map to one or more representations by their $H_{rep}$.
   - When a blob is re-encrypted with new keys or formats, the obsolete representation pin is removed and the new $H_{rep}$ pin is inserted.
4. Automatically upserts the corresponding `org.example.daybook.blobPin/<H_rep>` facet in the local Blob Inventory document.
5. When local references to a blob are removed, deletes the corresponding `blobPin` facet.

---

### 6. Big Sync Partition Projection

**Amended by ADR 026 §7.** Blob partitions are per drawer, and their access mirrors the
drawer's groups rather than a blob-specific authority group: the ciphertext partition's
access is Keyhive Read on the drawer's inventory doc (the meta group's members, relays
included), and the plaintext partition's access is Keyhive Read on the drawer's docs
group, whose readers are entitled to plaintext and derive it from the drawer's Blob
datums. Both are seeded from live Keyhive state before following the access-delta stream
([ADR 013](013-keyhive-access-delta-stream.md)). Group parts and `WorkerGroupScope` are
not the gate for which documents a worker reads.

The projection engine (`DocBlobsIndexRepo`; link target never landed, superseded per ADR 026 §7) subscribed to drawer events for all readable documents in sponsored groups:

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

**Amended by ADR 026 §1–§3.** There is no separate KEM / Keyring document. The JWK datum
lives beside the cipherBlob datum in the same content doc, because no party can read a
content doc without being entitled to its plaintext: relays hold content docs as
ciphertext at Keyhive `Relay` and learn representation digests from inventories. The
separation still holds where it matters — the relay sees only the representation
multihash and bytes — but it is a property of drawer access levels, not of two documents
with different readers.

```text
Plaintext Content
      ↓ (Encryption)
Ciphertext Representation ──(Multihash)──► Representation Multihash ──► blob_pin facet ──► Big Sync / Relay
      │
      └─► JWK datum, co-located with the cipherBlob datum in the content doc
```

### Multi-Domain Re-Encryption

**Amended by ADR 026 §3.** The default domain is the document, not a Keyhive group, and
there is no `keyScope`: slots are keyed `cipherBlob/<blob datum key>` with their JWK at
`jwk/<same key>`, so one JWK per blob slot. The motivation is unchanged — identical
plaintext in different docs with different audiences must not produce correlatable
ciphertext — and per-doc slots satisfy it, because branches and copies are different
docs. A doc that sits in several drawers has one ciphertext, which is all its relays
could correlate anyway.

Re-encrypting the same plaintext for different sharing domains produces distinct ciphertext representations ($C_1, C_2$) with different multihashes ($H_1, H_2$). The sync layer treats $H_1$ and $H_2$ as completely unrelated blobs, preventing metadata leakage across access domains.

### Blob Identity and URIs

**Amended by ADR 026 §3.** A reader resolves a Blob to its cipherBlob by naming
convention inside the same doc, so the encryptor no longer writes `Blob.urls`. A `?via=`
hint stays legal but is optional, and the `?kem=` / `?keyring=` hints below are withdrawn
with the key documents they named.

Application-level references remain content-addressed by **Content Multihash** (plaintext hash):

```text
db+blob:///<content-multihash>
```

Resolution metadata can be attached as optional query hints:

```text
db+blob:///<content-multihash>?via=<cipherBlob-facet-key>
db+blob:///<content-multihash>?size=12345
```

The content multihash is authoritative for identity. Resolution hints assist in locating
key material and representation hashes without altering content identity. If hints are
absent or outdated, the client resolves the co-located slot by name.

---

## Terminology

* **Blob:** Logical immutable byte content.
* **Content Multihash / Content Digest:** Multihash of logical/plaintext bytes (canonical application identity).
* **Representation:** Concrete byte sequence used for storage and network transfer (ciphertext or plaintext).
* **Representation Multihash / Representation Digest:** Multihash of the concrete representation bytes.
* **Blob Pin (`blobPin`):** An Automerge facet `org.example.daybook.blobPin/<rep-multihash>` requesting retention/replication of a representation.
* **Blob Inventory:** An Automerge document containing $N$ `blobPin` facets.
* **JWK datum:** the key material for one cipherBlob slot, co-located with it in the content doc (ADR 026 §2). Withdrawn: the KEM / Keyring document that used to hold it.
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
2. **Key distribution beyond co-location:** `keyRef` stays a general URI, so a separate key store can return without changing the cipherBlob abstraction (ADR 026 §2).
3. **URL Resolution Protocol:** Detailed parsing and fallback indexing for `db+blob:///` hints.
4. **Relay Retention Policies:** Explicit acknowledgement watermarks and GC grace periods.
