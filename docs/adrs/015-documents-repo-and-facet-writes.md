# ADR 015: Daybook document format and DocumentsRepo

**Status:** Proposed

## 1. Document identity and content

A Daybook doc has a `DocumentId`, a versioned content representation, and a history. Every doc is in at least one drawer and can be entered in several. Official roster inclusion, access grants, and retaining a local copy are separate operations. `DocumentsRepo` creates docs and reads and writes their content; DrawerRepo manages roster entries. Creation always names a target drawer, persists identity and initial content before advertising authority, and also enters the doc in the embedder-supplied local drawers for physically present and locally originated docs.

The first content representation is a map of **facets** in Automerge. A facet key has a tag and an instance name, such as `org.example.note/main`. The tag names the value's type, while the instance distinguishes facets of that type. A document can contain facets with different tags, unknown tags, and no application facets at all. Its identity and representation must remain readable without installing a plug. Other content representations may be defined later without changing document identity.

A branch is another document with its own `DocumentId`. It may record the source document and heads from which it was created; it is not another named history *within* the source document. A branch is not automatically listed in the source document's drawers. Removing a facet leaves a tombstone and history; it is not document deletion. A schema failure, missing bytes, or lost access likewise does not delete a document. Document-level deletion requires a separate core lifecycle operation and is not specified here.

A document can recommend software through a Daybook-defined facet with a fixed, readable schema. A recommendation cannot install a plug or grant execution or write access. Program discovery and plug execution do not belong to the document envelope.

## 2. Facet values and writes

The DocumentsRepo API accepts a complete JSON value for each facet it sets, or an explicit request to remove that facet. It does not accept edits at JSON paths. A write may affect several facets, but unmentioned facets are unchanged. Whole-JSON reconciliation traverses the submitted value but emits only changed operations; it is not wholesale replacement of every stored object. Before committing, the repository validates new local values against the selected facet definitions, applies the declared JSON-to-storage codec, and writes the resulting content and provenance atomically. The value **read from the transaction after the content mutation** is the value committed to the facet signature. This matters when the transaction starts from already merged heads or preserves unfamiliar native values supplied in the round-trip.

Each facet Lexicon describes its canonical JSON value and is identified by its facet tag. Its sidecar JSON Schema carries the supported supplementary constraints and Automerge storage annotations. SchemaRepo supplies the known Lexicon and sidecar bundle to Cabinet; how an embedder discovers or registers bundles is outside this repository contract. A local write to an unknown facet schema is refused. Field-name suffix or prefix hints do not recover a schema and are not part of Cabinet authoring. Conflicting definitions for one tag must be reported, not silently selected by installation order. Incompatible semantic meanings use different tags; cross-version transformation is not part of an ordinary doc write.

A read-modify-write must preserve unfamiliar content in its **native storage representation**, not merely as equivalent JSON. An old reader must not turn a future collaborative Text into Str, reset a Counter, or replace an unchanged nested object's identity. Full JSON round-tripping is the chosen read/write contract. Omission means removal at the supplied base heads, including unknown properties; removing an enclosing object removes its contents. Hidden-field projection is rejected: it loses correspondence between list elements with different hidden contents and cannot naturally represent an unknown variant in a required union field. Rejecting new unknown fields and requiring a special native-copy API are not adopted. List correspondence and changed unknown-value reconciliation still need design; JSON copy/paste is expected, but cannot promise native fidelity where neither a schema, corresponding target, nor usable convention is available.

Ordinary writes must supply the heads they are based on; there is no implicit current-head write or automatic retry loop. `AtCurrent` is the explicit unsafe concurrency choice, not the default; it selects a frontier but does not bypass authenticity checks. Reconciliation must use the selected base view, not silently diff against unseen current state. Concurrent creation cannot be rejected based on an invisible remote creator: any creation-tolerance policy must also define its post-merge behavior, which remains open.

**Default reads and writes reject forged facet state at the selected heads.** A caller cannot use `write facet X at heads Y` to build on a forged X at Y, whether or not it actually called read first. DocumentsRepo must enforce the same base-authenticity requirement before reconciliation or signing; supplying heads is not evidence that the caller verified them. Verification concerns the facet state, including hidden contributions that can affect later merges, not just the JSON winner returned to the caller. Missing or unsupported verification evidence cannot silently become a successful authenticated read or ordinary write. These checks are facet-local: a forged unrelated facet does not invalidate X.

An older verified base remains usable through `AtHeads` even if a newer frontier contains a forgery; neither reads nor writes silently upgrade that base. Optional unsafe inspection and explicit repair are separate interfaces, not default read behavior or implicit fallbacks. A complete JSON submission alone does not select repair: ordinary reconciliation can retain native objects and hidden state from its base. The enforcement belongs to the future DocumentsRepo implementation, not the current value-verification primitive.

The write methods have four concurrency modes:

| Mode | Commit condition |
| --- | --- |
| `IfDocumentUnchanged(heads)` | Commit only if all document heads still match. |
| `IfFacetsUnchanged(expected)` | Commit only if the specified facets still have the versions or absence returned by the earlier read, even if other facets changed. |
| `AtHeads(heads)` | Intentionally write from existing historical document heads, producing a concurrent change if newer heads exist. |
| `AtCurrent` | Write from current heads at commit time. |

Comparison and commit are serialized for the document. A facet version check includes all visible conflicting claims, deletion, and recreation—not only the displayed JSON value. Reads return the facet-version information needed for `IfFacetsUnchanged`, and can read a facet at specified document heads. Unknown heads fail. Supplied historical heads are never silently replaced by current heads. An error from a local write commits neither facet content nor dmeta. A successful receipt means local persistence and reports the resulting heads; it does not assert peer delivery or a particular future conflict winner.

Automerge is the initial concurrency substrate, **not the definition of a signed facet**. At historical heads, a write can create another concurrent version; that is not a new document branch. A later merge may display a value different from either writer's transaction view. Such a computed view must not be attributed to either signature as though that actor signed the later result. A subsequent writer can sign that merged view as a new claim through the ordinary path only after the repository verifies its base; seeing the materialized value through an unsafe read does not satisfy that requirement.

## 3. Node authorization and local capabilities

The verified **node signer** of a change is checked against Keyhive Write authority for the document below Cabinet. The forwarding peer is not necessarily the author. The host also checks local plug/actor policy before allowing a local write and validates that write's facets. A hostile node with document Write authority can bypass its own plug checks; a receiver cannot infer that a remote plug or validator ran.

Local capabilities and portable attribution are separate contracts. An actor's signing identity is its public key; names and paths confer no authority. Issuing or revoking local actor access does **not** mutate documents or invalidate historical attribution. A branch has a different document ID and does not inherit drawer entries merely because it records an origin. The same authorization boundary applies across local IPC: a transport is not proof that a caller controls an asserted actor.

The design direction is the authority service (ADR 017) owning runtime routine capabilities, including document, facet, command, and SQLite handles exposed through WIT. That is **not a PR3 runtime migration** or a selected portable capability-token format. Dmeta's `delegation` field does not carry those live capabilities. Private-key custody is likewise a host boundary, with secrets kept in `secrets_rs` and signing exposed without handing keys to callers; the cryptographic protocol does not depend on that particular custody implementation.

Dmeta records **who claims authorship of a facet**, not the host's current capability policy. A node may attribute an actor to an application or process, but its certificate proves the node made that attribution, not that a particular binary executed. Node change signatures, actor-attribution certificates, actor facet signatures, and local capabilities are distinct artifacts.

## 4. Signed facet claims

A facet key selects a slot in the document. Dmeta retains every UUIDv7 alias assigned to that slot by concurrent creators; external references to any alias remain valid. For a new signature the chosen UUID is the lexicographically smallest alias **visible in the writing transaction**. History reconstruction recovers that same choice at the change introducing the signature; portable evidence carries it in the signed message. A later merge may add a smaller alias without changing what was signed. The facet's content is the materialized value at its key, not a value selected by UUID.

The aliases also retain evidence of **concurrent creation** after later writes hide the original Automerge conflict. Convergent facet keys let two OCR processors target the same slot; distinct creation UUIDs let an index distinguish those independent creations durably. They do not select a different content winner. UUID timestamps are metadata, not trusted chronology, even when a signature authenticates the UUID bytes.

A facet signature authenticates a **substrate-independent logical claim**. Its seven fields are the origin document ID, facet key, chosen facet UUIDv7, actor UUIDv7, Set or Remove operation, CID of the transaction-view native value for Set (null for Remove), and sorted predecessor signature IDs. Predecessors record the signatures visible at the selected base heads. They are signed context, **not a recursive trust gate**: a rejected predecessor does not poison a fresh, valid replacement. A new facet has no predecessors; resurrection can name the visible tombstone. Signing identical content with the same key, actor binding, and claim context produces the same signature. Writing at historical heads uses that context and may produce a concurrent signature.

In ordinary Automerge storage, the repository reconstructs the message from the change that introduced an immutable signature entry rather than storing a duplicate claim object. This is a storage optimization, not a dependency of the protocol on Automerge. `FacetSignature.message` can carry the **complete domain-prefixed signed message**, including chosen UUID and predecessors, for history-free export or compaction. Carrying only the current value CID would not preserve the claim context.

The value CID is CIDv1 with `raw` codec and **BLAKE3-256** multihash (code `0x1e`) over the canonical native bytes. The native form is what the signature covers because the JSON spelling of a stored value depends on its declaration: a float leaves as a decimal string, a counter as a whole number, a timestamp as RFC 3339, bytes as `{ "$bytes": "…" }`. Native `Bytes`, `Counter`, and `Text` remain distinct from other kinds, so verification of a native value needs no facet schema. An absent schema leaves it uninterpreted rather than cryptographically invalid. Portable JSON can preserve verification when a schema or typed witness faithfully reconstructs the native kinds; untyped flattening can lose that information.

The canonical encoding is CBOR of the materialized subtree, one application-private tag per storage kind, **unique map keys sorted by their encoded bytes**, and no object, actor, or operation IDs. Copying a value changes its object identity, not its value CID. `Text` encodes its current plain string and `Counter` its current sum, each still distinct from `Str` and `Int`. The tag number is the private base `0x64626b00` plus a kind index, not an IANA registration: 0 null, 1 bool, 2 int, 3 uint, 4 float, 5 str, 6 bytes, 7 timestamp, 8 counter, 9 text, 10 list, 11 map. Timestamps encode milliseconds since the Unix epoch. Containers have definite lengths and other payloads use the shortest RFC 8949 representation, except that **every float uses binary64**: all NaNs normalize to positive quiet NaN bits `0x7ff8000000000000`, infinities are preserved, and positive and negative zero remain distinct. Changing kind indices or canonicalization rules requires a new protocol version. `encode_native` implements this encoding; a claim holds only the resulting CID.

Only plain Automerge text is covered. Marks, embedded blocks, tables, and unknown scalar representations are unsupported until their encoding is specified. An unsupported representation or traversal resource limit is not evidence of forgery. The value encoding covers a visible materialized value, **not hidden Automerge conflicts or a proof of all operations that produced it**.

V2 signs `UTF8("daybook.facet-claim.v2") || 0x00 || CBOR(fields)` with Ed25519 and strict signature verification. `CBOR(fields)` is a deterministic definite-length array in this order:

1. Origin document ID as canonical UTF-8 text.
2. Facet key as UTF-8 text.
3. Chosen facet UUIDv7 as a 16-byte string.
4. Actor UUIDv7 as a 16-byte string.
5. Operation: `0` Set or `1` Remove.
6. Value CID as binary bytes, or null for Remove.
7. Predecessor IDs as an array of binary CIDs, sorted lexicographically without duplicates.

The signature covers the whole message, not merely the value CID. Its ID is a CIDv1 `raw`/BLAKE3-256 multihash of `UTF8("daybook.facet-claim-id.v2") || 0x00 || actor-public-key-bytes || signed-message-bytes || signature-bytes`. Both domains are v2 because the signed field layout changed. The ID does not refer to its containing Automerge change hash, which would be circular. Actor public keys use canonical base58btc multibase `z…` text in metadata; signatures are native 64-byte `SignatureBytes`, rendered in JSON as `{ "$bytes": "<base64>" }`. Messages and retained native-value witnesses also use the bytes object, not multibase text.

## 5. Actors, attribution, and dmeta

An actor's cryptographic identity is its public key. `actorUuids` assigns a UUIDv7 to a binding of that key and an exact actor URL. The same key may have several UUID bindings, each with its own attribution certificate. The URL is descriptive metadata, not an authority namespace; the certificate authenticates the description without proving a plug binary executed. The Automerge change actor remains the node, while the actor key signs the facet claim.

### Attribution certificate profile

Despite its field name, `ActorMetadata.delegation` holds a **neutral node-signed attribution certificate**, not a grant or redelegation chain. It binds the issuer, actor UUIDv7, actor public key, and exact actor URL. Verification takes an **externally authenticated expected issuer** from the caller and compares it with the certificate's issuer; accepting the certificate's self-declared `iss` as the trust anchor would let anyone mint a replacement identity. The certificate does not make the actor a Keyhive principal, confer transitive issuer trust, or require a live authority-service query.

The profile uses CWT claims inside tagged COSE_Sign1:

```text
61(18([bstr(protected), {}, bstr(claims), bstr(signature)]))
protected = {
  1: -19,
  2: [16],
  3: "application/cbor",
  16: "application/vnd.daybook.actor-attribution.v1+cwt"
}
claims = {
  1: issuer,
  2: actorPublicKey,
  -65537: actorUuidBytes,
  -65538: actorUrl,
  -65539: "daybook.actor-attribution"
}
```

`issuer` and `actorPublicKey` are canonical base58btc multibase text of raw 32-byte Ed25519 public keys, not multicodec-prefixed keys. `actorUuidBytes` is the raw 16-byte RFC-variant UUIDv7; `actorUrl` is exact text with no normalization. The media type above is **private and unregistered**. Protected header 16 is critical, and the protected algorithm is fully specified `Ed25519 (-19)`, not deprecated polymorphic `EdDSA (-8)`. The signature is pure Ed25519 over the CBOR Sig_structure `["Signature1", bstr(protected), h'', bstr(claims)]`, with empty external AAD.

This is a closed profile: no `iat`, audience, expiration, grants, or redelegation fields. The unprotected header map is empty. Unknown extensions, other algorithms or profiles, duplicate map labels, trailing data, malformed bindings, weak or noncanonical keys, and invalid signatures are not accepted. Verification uses the **received protected and payload bytes**, not their re-encoded equivalents. Resource bounds are 4096 certificate bytes, 2048 actor-URL UTF-8 bytes, CBOR depth 4, 64 items, and 16 entries per collection. Unsupported profiles and resource-limit refusals remain distinct from malformed or cryptographically invalid evidence.

The standards define the envelope and signature processing, not Daybook authority: [RFC 8392 §§3–7 and §9.1.1](https://www.rfc-editor.org/rfc/rfc8392.html) defines CWT and private claims; [RFC 9052 §§3.1 and 4.2–4.4](https://www.rfc-editor.org/rfc/rfc9052.html) defines COSE headers and Sign1; [RFC 9596 §§2–3](https://www.rfc-editor.org/rfc/rfc9596.html) defines protected `typ`; [RFC 9864 §2.2](https://www.rfc-editor.org/rfc/rfc9864.html) defines the fully specified Ed25519 algorithm. A capability-delegation format would assert different semantics from this attribution-only certificate.

### Dmeta storage

`org.example.daybook.dmeta/main` is the Daybook-owned provenance facet. Its logical structure is:

```text
Dmeta {
  createdAt: Timestamp
  updatedAt: [Timestamp]
  actorUuids: Uuid7 -> { pubkey, actorUrl, delegation?: Bytes }
  facetUuids: Uuid7 -> FacetKey
  facets: FacetKey -> {
    updatedAt: [Timestamp]
    deletedAt: [Timestamp]
    signatures: [ {
      actorUuid: Uuid7
      signature: SignatureBytes
      message?: Bytes
      inherited?: { originDoc, message: Bytes, value?: Bytes }
    } ]
  }
}
```

The document's own ID is not a field. The enclosing document already knows it, and a field inside the facet would be copied verbatim onto a branch, where it would name the origin rather than the branch. Branch relationships are not fields either: they are weak links recorded by the branch facets (ADR 007), and dmeta signs those facets rather than restating them.

`facetUuids` is the only alias index: each UUIDv7 maps directly to a complete facet key, tag and instance together. A per-facet reverse list would duplicate facts that a merge could pull apart. The repository derives the selected alias from this index. Actor and facet UUID times replace separate per-binding creation-time fields, but do not establish a trusted order of events.

For an ordinary Automerge write, a signature entry carries `actorUuid` and native signature bytes; the remaining claim context is reconstructed at its introduction change. Actor keys and certificates are shared through `actorUuids`. The optional `message` preserves a locally authored claim when that reconstruction will no longer be possible. `message` and `inherited` are alternative evidence carriers, not fields to populate together. A missing certificate, issuer proof, or signed message is missing verification evidence, not proof of forgery.

`signatures`, document and facet `updatedAt`, and facet `deletedAt` are **current competing sets**, not append-only logs. Replacing a set removes only the entries observed at the supplied base heads; concurrent entries survive. Retained signature entries remain immutable, and newly inserted signatures get fresh objects rather than mutating old entries in place. Generic positional list reconciliation must not mix fields from concurrent signatures. Full-JSON reconciliation still traverses the value; preserving unchanged native objects and emitting only changed operations does not mean traversal is free.

`deletedAt` is always present and initialized, empty for an authored non-tombstone. A removal records a deletion candidate; a later Set clears the candidates it observed. A concurrent Remove/Set merge can retain deletion candidates even when a value is visible. Timestamps are unsigned display metadata: neither a nonempty `deletedAt` nor the latest timestamp decides current tombstone status. The signed operation and its match to the current slot state do. Older entries may remain in Automerge history without being duplicated in current dmeta. Granting broader local access creates no entries in untouched documents.

### Branches and portable witnesses

An independent branch does not contain its source document's Automerge history. A copied signature therefore retains an `inherited` witness: origin document, the exact domain-prefixed signed message, and, when the copied value differs from the signed one, the canonical native bytes of the historical signed value. Keep these witnesses even when they do not cover the current branch value. They authenticate only their own claim; an authentic historical value is not a signature on a differing merged value. A signature authored on the branch names the branch as origin and uses its own reconstructible history or optional `message`, not `inherited`.

A carried message allows cryptographic verification without the source's Automerge history. It does not by itself prove node admission on the origin or establish the expected attribution issuer. If required authority evidence is missing, that check remains pending rather than forged. Export or compaction must preserve the complete messages and native-value evidence needed for the claims it retains; **omitting carried evidence is an Automerge-history-specific optimization**. Neither copying values nor preserving messages recreates discarded operations or hidden conflicts.

For example, node N certifies actor binding A, whose key signs facets X and Y in D. The signatures share A's certificate. Branch B carries inherited witnesses naming D. When A replaces X on B, the new claim names B and may name the inherited X signature ID as predecessor context, without making predecessor acceptance a condition of the new claim. If B contains a merged value signed by neither source actor, the inherited claims remain historical evidence and do not cover it. Reconstructing or accepting such a derived merge after compaction is unresolved (§6).

## 6. Verification and presentation

Authenticity is **facet-local**, not whole-document validity or recursive predecessor closure. For an ordinary Automerge signature, reconstruct the claim at the change that introduced its immutable entry: read the transaction-view native value or tombstone, key, chosen alias and actor binding there, and predecessor IDs from its parent heads. Do not substitute today's value or alias choice. For a portable or inherited entry, decode the carried complete message; when reconstruction is also available, the messages must agree. Check actor UUID, key, alias, facet key, and origin context, then strictly verify the signature and the attribution certificate against the caller's authenticated expected issuer. An inherited retained value must match its signed CID, but it is not evidence that a differing current value was signed.

BigRepo and Keyhive separately gate node admission below Cabinet. The current `Dmeta::verify_facet` helper checks claim, attribution, and materialized-value evidence; it does not verify the full facet state at supplied heads, independently replay Keyhive authority, or consult current local actor grants. Its `Accepted` result is not the DocumentsRepo read/write gate required by §2. A write performed now uses current local capabilities even if its explicit base heads are old. Portable witnesses do not remove the caller's obligation to supply any required origin-authority evidence.

The current per-entry value-verification states distinguish authenticity from value coverage:

| State | Meaning |
| --- | --- |
| `Accepted` | Claim and attribution verify, and the signed Set value CID or Remove operation matches the supplied materialized value or removal; this is not full frontier validation. |
| `Pending` | Required message, actor binding, certificate, expected-issuer evidence, or value evidence is missing. Unsupported attribution profiles and resource limits do not prove forgery. |
| `Rejected` | Claim/signature/context or attribution evidence is invalid. A proven invalid check is not repaired by missing evidence elsewhere. |
| `Uncovered` | An authentic claim does not match the current value or removal; its historical authorship remains valid. A slot with no signatures is also uncovered. |

At this value-evidence layer, a slot is `Accepted` if **any** signature accepts its current value or removal, even if other entries are pending or rejected. Without a matching accepted entry, the current fold prefers `Pending` if any entry is pending, then `Rejected` if any is rejected, otherwise `Uncovered`. This does not relabel a rejected entry as authentic, nor prove that hidden state is safe to reuse. Rejected signature entries alone do not poison valid content. A fresh valid replacement can recover the slot without recursive acceptance of predecessor claims; their IDs record context, not a requirement to trust rejected ancestry.

There is no whole-document shutdown for a bad facet signature. Rejected physical history can remain as evidence without becoming application content. Ordinary reads must reject forged facet state, not merely return its displayed winner with a warning. Optional unsafe inspection must stay separate from this path and must not make the inspected heads eligible for an ordinary write. Explicit repair can establish a new signed value without endorsing rejected ancestry, but must not silently retain the rejected content or hidden contributions as live state. Its API and enforcement remain DocumentsRepo work. Local authoring errors remain atomic; received evidence is reported through verification state.

Cryptographic checks on immutable claims can be cached. Current-value coverage must be reevaluated when the slot changes, and missing-evidence verdicts must be reconsidered when evidence arrives; a once-per-entry cache is not sufficient for all verdicts. Missing schemas leave native values uninterpreted. Unsupported native representations and resource limits must remain distinguishable from a bad signature.

### Guarded reads and derived-merge edge cases

The default-path guarantee is settled: an honest actor cannot read or ordinarily write a forged facet at its supplied base heads. The following hidden-conflict scenario illustrates why a materialized-value check alone cannot implement that guarantee; it is **not a counterexample to the guarded contract**:

1. A verified branch holds `x = "safe"`. An unsigned concurrent assignment of `x = "evil"` loses the visible conflict but remains in the facet's CRDT state.
2. Through raw Automerge access or an unsafe read, A sees `"safe"`, changes another field, and signs the resulting visible value while retaining the hidden assignment.
3. B, writing from an older verified frontier that never contained `"evil"`, deletes the observed `"safe"` assignment. Merging the branches can expose `"evil"`.
4. Replaying A's and B's forks can reproduce that result, but checking their signed visible values alone missed A's forged base. **The default path must stop A at its read or base check, before A's ordinary write and signature.** B's older verified base remains legitimate.

Reconstructing the original Automerge forks at verified write points and comparing their merge with the selected facet state is a candidate verification approach. Its inputs must meet the guarded-state requirement, not merely match signed snapshot values. Two legitimate concurrent edits can yield a derived value neither actor directly signed; that alone is not forgery. Verification must establish that derivation without attributing the merged value to either actor as a direct signature. Requiring a new signature merely because a legitimate merge occurred is not selected here.

Value claims also do not identify a unique CRDT operation history. Incrementing an existing counter from zero to one and replacing it with a fresh `Counter(1)` have the same native value CID but can merge differently with a concurrent increment. A verifier must not substitute a reconstructed operation history solely because its materialized value matches the claim. This is a requirement to examine during DocumentsRepo design, not a selection of operation receipts or a change to the portable value-signature format.

The gate's implementation, the derived-merge verification algorithm, and the evidence a history-free or compacted branch must retain remain DocumentsRepo work. Messages and inherited native-value witnesses preserve their signed claims but cannot recreate discarded operations or hidden conflicts. Explicit repair remains distinct from an ordinary write on a verified base. No operation-receipt format, read-receipt format, or new node-attester trust policy is selected by these edge cases.

## 7. Conformance work and measurement

Conformance and performance are separate work. Before treating the wire format as stable, publish cross-implementation byte vectors for native encoding, value CIDs, v2 claim messages and signature IDs, the v1 attribution CWT/COSE profile, and strict Ed25519 signatures. Verify canonical textual IDs and keys, native-kind distinctions, binary64 NaNs/infinities/signed zeros, encoded-key ordering, received-byte COSE verification, issuer isolation, duplicate/trailing-data rejection, and resource bounds.

Repository conformance must cover transaction-view values; writes at old heads; concurrent creators and UUIDv7 aliases; immutable competing signature entries; initialized timestamp sets; concurrent Set/Remove and resurrection; inherited and newly authored branch claims; full-message export and compaction; faithfully typed JSON reconstruction; unknown schemas and unsupported native kinds; pending and invalid evidence; uncovered current values; and recovery by a fresh valid replacement without recursive predecessor acceptance. Inspect the signed Automerge change-label payload and its handoff through Subduction separately from the facet claim protocol.

The following are **future DocumentsRepo regressions**, not claims of implemented enforcement:

| Scenario | Required behavior |
| --- | --- |
| Forged facet X at supplied heads Y, including a hidden unsigned contribution behind an authentic-looking winner | Default read fails; ordinary write fails before content or dmeta mutation, even without a preceding read call. |
| Unsafe inspection of the same forged base | Inspection does not authorize an ordinary write at those heads; any repair is explicit. |
| An older verified base while newer heads contain a forgery | `AtHeads` uses and verifies the requested base, never silently upgrades it; later merged state is checked separately. |
| A forgery in an unrelated facet | Reading or writing authentic X remains possible; no document-wide validity fold. |
| Missing proof, unsupported native representation, or unknown heads | No successful authenticated read or ordinary write by fallback; missing evidence remains distinct from proven forgery. |
| Legitimate concurrent disjoint edits | Verify the derived merge without pretending either input signature directly covers its different value. |
| Hidden-conflict resurrection described in §6 | A is stopped by the guarded base check; a raw-Automerge reproduction is not a successful default-path workflow. |
| Equal signed counter values produced by increment versus replacement | Do not treat equal CIDs as interchangeable CRDT histories when reconstructing a merge. |
| Explicit replacement of forged content | New authentic content can recover the slot without trusting rejected predecessors or retaining their hidden live state. |
| Copy or compaction without source history | Preserve verifiable claims and report missing derivation evidence; do not invent a merge from value witnesses alone. |

Native, attribution, and claim conformance tests exist in the source; their presence does not establish completed repository/runtime integration or these guarded-workflow guarantees. **Benchmarks: NOT RUN.** Dmeta size, hydration, signing and verification overhead for many one-field `blobPin` facets, frequent small updates, and long histories remain to be measured. Whole-value traversal can cost time even when reconciliation emits no operations. Sedimentree object-size caps do not bound cumulative history or verification cost; a resource-limit refusal is not forgery.
