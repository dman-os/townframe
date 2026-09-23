# ADR 015: Daybook document format and DocumentsRepo

**Status:** Proposed

## 1. Document identity and content

A Daybook document has a logical `DocumentId`, a versioned content representation, and a history. Documents can exist without drawers and can be listed in more than one drawer. Listing, access grants, and retaining a local copy are separate operations. `DocumentsRepo` creates documents and reads and writes their content; Cabinet manages drawer listings and mirror policy. Creating a document must persist its identity and initial content before an access grant advertises it to other nodes.

The first content representation is a map of **facets** in Automerge. A facet key has a tag and an instance name, such as `org.example.note/main`. The tag names the value's type, while the instance distinguishes facets of that type. A document can contain facets with different tags, unknown tags, and no application facets at all. Its identity and representation must remain readable without installing a plug. Other content representations may be defined later without changing document identity.

A branch is another document with its own `DocumentId`. It may record the source document and heads from which it was created; it is not another named history *within* the source document. A branch is not automatically listed in the source document's drawers. Removing a facet leaves a tombstone and history; it is not document deletion. A schema failure, missing bytes, or lost access likewise does not delete a document. Document-level deletion requires a separate core lifecycle operation and is not specified here.

A document can recommend software through a Daybook-defined facet with a fixed, readable schema. A recommendation cannot install a plug or grant execution or write access. Program discovery and plug execution do not belong to the document envelope.

## 2. Facet values and writes

The DocumentsRepo API accepts a complete JSON value for each facet it sets, or an explicit request to remove that facet. It does not accept edits at JSON paths. A write may affect several facets, but unmentioned facets are unchanged. Before committing, the repository validates new local values against the selected facet definitions, applies the declared JSON-to-storage codec, and writes the resulting content and provenance atomically. The value **read from the transaction after the content mutation** is the value committed to the facet signature. This matters when the transaction starts from already merged heads or when an open facet preserves properties not present in the caller's JSON.

Each facet schema describes its JSON value. A separate codec declaration describes its Automerge representation: for example, ordinary scalar text, collaborative text, or bytes presented as a schema-selected textual encoding. New definitions must declare this rather than rely on field-name suffixes; existing suffix-encoded data remains readable. A plug may define a permissive JSON schema. No ATProto record schema or global registry is required. Tags are NSID-shaped names, not proofs of domain ownership. Registries may distribute and endorse plugs, but nodes may also introduce them offline. Conflicting definitions for one tag must be reported, not silently selected by installation order. Incompatible semantic meanings use different tags; cross-version lenses are not part of a document write.

The codec must specify how omission works. A closed object may remove properties omitted from a complete replacement. An open object must preserve unknown properties unless the caller explicitly removes them. The repository constructs the value to be signed **after** these rules have run. A read-modify-write through a plug that discards unknown fields must not masquerade as a lossless write. Unknown remote facets are retained and forwarded without installing their plugs; local validation does not retroactively invalidate received history.

The write methods have four concurrency modes:

| Mode | Commit condition |
| --- | --- |
| `IfDocumentUnchanged(heads)` | Commit only if all document heads still match. |
| `IfFacetsUnchanged(expected)` | Commit only if the specified facets still have the versions or absence returned by the earlier read, even if other facets changed. |
| `AtHeads(heads)` | Intentionally write from existing historical document heads, producing a concurrent change if newer heads exist. |
| `AtCurrent` | Write from current heads at commit time. |

Comparison and commit are serialized for the document. A facet version check includes all visible conflicting claims, deletion, and recreation—not only the displayed JSON value. Reads return the facet-version information needed for `IfFacetsUnchanged`, and can read a facet at specified document heads. Unknown heads fail. Supplied historical heads are never silently replaced by current heads. An error from a local write commits neither facet content nor dmeta. A successful receipt means local persistence and reports the resulting heads; it does not assert peer delivery or a particular future conflict winner.

Automerge is the initial concurrency substrate, **not the definition of a signed facet**. At historical heads, a write can create another concurrent version; that is not a new document branch. A later merge may display a value different from either writer's transaction view. Such a computed view must not be attributed to either signature as though that actor signed the later result. A subsequent writer who sees and writes that merged view can sign it as a new claim.

## 3. Node authorization and local capabilities

The verified **node signer** of a change is checked against Keyhive Write authority for the document. The forwarding peer is not necessarily the author. The host also checks local plug/actor policy before allowing a local write and validates that write's facets. A hostile node with document Write authority can bypass its own plug checks; a receiver cannot infer that a remote plug or validator ran.

Plug permissions are external policy. A capability manager may combine a plug-level grant with drawer membership to allow writes to a million documents. Issuing or revoking that grant does **not** mutate those documents. DocumentsRepo obtains a policy decision for the actor, target document, and facet keys at commit time. If several drawers provide paths to the same document, one valid policy path suffices. A branch has a different ID and does not inherit drawer membership merely because it records a source. The same checks apply to local IPC callers: a Unix socket is a transport, not proof that a caller controls an asserted actor. Caller identity and policy storage belong to the local service boundary.

Dmeta records **who claims authorship of a facet**, not the host's current capability policy. A node may attribute an actor to a plug or subsystem, but the signature proves the node made that attribution, not that a particular plug binary executed.

## 4. Signed facet claims

A facet key selects a slot in the document. Dmeta retains every UUID alias assigned to that slot by concurrent creators; external references to any alias remain valid. For a new signature the chosen UUID is the lexicographically smallest alias **visible in the writing transaction**. A verifier reads the state at the change that introduced the signature to recover that same choice; a later merge may add a smaller alias without changing what was signed. The facet's content is the merged value at its key, not a value selected by UUID.

A facet signature authenticates a *logical claim*, but an Automerge document **does not store a claim object**. Its signed bytes are reconstructed from the state at the change introducing the signature. The fields are the origin document ID, facet key, UUID selected at that change, Set or Remove operation, CID of the transaction-view JSON value for Set (null for Remove), and sorted IDs of accepted facet signatures visible at the change's parent heads. A new facet has no predecessors; resurrection follows the visible tombstone. Signing identical content with the same actor at the same facet frontier produces the same signature. Writing at historical heads uses their predecessors and may produce a concurrent signature. The logical claim is independent of Automerge even though this compact storage strategy uses Automerge history to reconstruct it.

V1 canonicalizes the transaction-view JSON with RFC 8785 (JCS), then makes a CIDv1 with `raw` codec and **BLAKE3-256** multihash (code `0x1e`) of those bytes. JCS chooses the canonical JSON bytes; it does **not** mandate a hash algorithm. Finite binary64 floats are supported; NaN and infinities are not JSON. Numbers outside JCS's interoperable range require a schema-declared string or bytes representation rather than silent rounding. A schema-declared byte field has its declared JSON representation before canonicalization. Multibase is an API/export rendering of binary IDs, not signed text.

V1 signs `UTF8("daybook.facet-claim.v1") || 0x00 || CBOR(fields)` with Ed25519. `CBOR(fields)` is an RFC 8949 deterministic encoding of a definite-length array in this order: origin document ID (UTF-8 text), facet key (UTF-8 text), chosen UUID (16-byte string), operation (`0` Set or `1` Remove), value CID (binary byte string or null for Remove), and predecessor IDs (array of binary IDs sorted lexicographically without duplicates). The signature covers the whole message, not merely the value CID. The signature ID is a CIDv1 `raw`/BLAKE3-256 multihash of `UTF8("daybook.facet-claim-id.v1") || 0x00 || actor-public-key-bytes || signed-message-bytes || signature-bytes`. It does not refer to its containing Automerge change hash, which would be circular. The signer key and Ed25519 signature are stored as bytes.

## 5. Actors, delegations, and dmeta

An actor is identified cryptographically by its public key and may also have a URL describing its role. A signer may derive an actor key from a node-held secret using a domain-separated derivation incorporating repository identity, document identity, actor path, and purpose; the derivation is not part of the document verification format. Another key source is allowed. A distinct stored secret per facet is not required. The Automerge change actor is the **node**, not this facet actor; node-signed changes and actor-signed facet claims are correlated by the node-signed delegation. Future multi-node or accompanying-actor claims need versioned delegation forms, not a per-plug Keyhive principal.

To attribute an actor to a node, the document carries a node-signed delegation. The delegation's signed message binds its version, issuer node public key, actor public key, actor URL, and descriptive scope. Scope can describe a repository, document, facet prefix, or subsystem; it restricts **which attribution the proof asserts**, not the actor's current local plug permissions. Key rotation of a stable node ID is not presumed: a verifier uses the actual issuer public key named in the proof. The actor signs facet claims with its own key. A node's signed Automerge change, a node's actor delegation, and an actor's facet claim are three independently checked signatures with different messages.

V1 signs `UTF8("daybook.actor-delegation.v1") || 0x00 || CBOR(delegation)` with the issuer's Ed25519 node key. `CBOR(delegation)` is an RFC 8949 deterministic encoding of a definite-length array of issuer node public-key bytes, actor public-key bytes, actor URL UTF-8 text, and scope. Scope is a two-element array: kind (`repo`, `document`, `facet-prefix`, or `subsystem` as UTF-8 text) and identifier (UTF-8 text). Both URL and scope are claims about actor identity and context, not dynamic write grants. The verifier checks the issuer signature with the named node key and the facet claim with the named actor key; the public-key bytes, not an actor path or plug ID, distinguish signers. This initial form has no expiry or independent actor-key rotation. Changing actor keys requires a new delegation; changing node keys changes the node signing identity.

`org.example.daybook.dmeta/main` is the Daybook-owned provenance facet. Its logical structure is:

```text
Dmeta {
  id: DocumentId
  createdAt: Timestamp
  updatedAt: [Timestamp]
  actors: ActorPublicKey -> { actorUrl, delegation, createdAt }
  facetUuids: Uuid -> FacetKey
  facets: FacetKey -> {
    createdAt: Timestamp
    uuids: [Uuid]
    updatedAt: [Timestamp]
    deletedAt: [Timestamp]
    signatures: [ { actorPublicKey, signature, originDocument?,
                    inheritedMessage?, inheritedValue? } ]
  }
}
```

For an ordinary Automerge write, the signature entry carries the actor reference and signature bytes; the facet key, chosen UUID, operation, value, and predecessor IDs are reconstructed at the change that introduced it. No value CID, duplicate facet key, UUID, or serialized claim is stored with that signature. `originDocument` is omitted when it equals the enclosing document ID. Actor public keys and delegations are shared through `actors`. This makes the common case a signature plus a small actor reference, even for a tiny blob-pin facet.

`signatures` is a **current competing set**, not an append-only audit index. A write replaces the entries it observed at its base heads with its own signature; concurrent writers can leave multiple entries. The Automerge encoding must preserve concurrently written entries rather than lose one through list reconciliation. Older signatures can remain in Automerge history, but are not duplicated in the current dmeta facet. Timestamps help display history, not establish signature identity or authorization. Granting a plug broader access creates no dmeta entries in untouched documents.

An independent branch does not contain its source document's Automerge change history. Merely recording `originDocument` is therefore **not enough** to verify an inherited signature whose UUID, value, or predecessors were reconstructed at a source frontier. On branch creation, copy the signed message as `inheritedMessage` for each retained signature, with its origin ID. If the signed source value differs from the branch's copied current value (for example after a concurrent merge), also retain that source value as `inheritedValue` so its CID can be checked. A signature on an old value must not be presented as a signature on the merged value. Newly authored signatures on the branch omit these witnesses and use branch history for reconstruction. A compacted document or non-Automerge substrate that discards authoring history likewise must carry the signed message and value needed for signatures it claims remain verifiable. **Omitting those witnesses is an Automerge-history-specific storage optimization**, not a universal signature format. Without reconstructible evidence, report provenance as unverifiable rather than inventing its context.

For example, node N delegates actor A and A signs facets X and Y in D. The current signatures share A's delegation and do not duplicate the facet keys or value CIDs. Branch B carries X's and Y's inherited signed messages with origin D. When A edits X on B, the new signature is reconstructed from B's own history and names B as origin; its predecessor is the inherited X signature ID. Compaction can keep current signed messages but cannot recreate proof of discarded Automerge operations or revisions no longer retained.

## 6. Verification and presentation

To verify a received Automerge signature, find the change that first installed that signature entry. At that change's resulting frontier, read its facet value or tombstone, key, and UUID aliases; at the change's parent heads, read the accepted predecessor signatures. Reconstruct the signed message, verify it with the referenced actor key, and verify that key's node-signed delegation at the introduction frontier—not merely in today's dmeta. For an inherited entry, verify the carried signed message and check its value CID against the copied value or its retained `inheritedValue`; never treat it as an actor signature on a different merged value. Separately check the node signer of the containing change against document authority. A bad signature on any facet in a received multi-facet change prevents **all** of that change's effects from being presented as accepted, although its physical bytes may be retained as rejected evidence. A later valid write must be able to repair a facet without re-signing rejected content.

Missing authority or attribution evidence is **pending verification**, not proof of forgery. A proven bad signature or unauthorized node write is **rejected** as application content. Verified history is **accepted**; absence of a local schema means its facet remains uninterpreted, not cryptographically invalid. Resolve acceptance before displaying Automerge's visible conflict winner: an invalid winner is not presented just because Automerge selected it. Descendants of rejected physical history cannot launder its content into the accepted view. Local writes fail atomically with an error; received history has no local caller to return an error to and must be reported through verification state.

Arrival order and client timestamps cannot establish whether a node-signed change preceded a Keyhive revocation. A change whose authority cannot be determined from authenticated evidence remains pending; defining the change-to-authority-frontier proof is a prerequisite for claiming post-revocation verification. Node authorization is distinct from actor attribution, including in offline exports: a facet plus delegation can prove what the actor and node signed, but it does not by itself carry the Keyhive history needed to establish that the node was authorized.

## 7. Conformance work

Before v1 signatures are issued, publish cross-implementation byte test vectors for JCS numeric values, BLAKE3-256 value CIDs and signature IDs, binary claim and delegation arrays, and Ed25519 signatures; verify canonical textual forms for document IDs and facet keys. Inspect the actual signed Automerge change-label payload and its handoff through Subduction. Test an open facet's transaction-view value; writes at old heads; concurrent creators and UUID aliases; concurrent signature replacement; tombstone and resurrection; a branch with inherited and newly authored claims, including an inherited value different from the copied merge result; copied and compacted claims; unknown schemas; missing and forged proofs; invalid conflict winners; and repair after a rejected change. Benchmark dmeta overhead and hydration with many one-field `blobPin` facets as well as frequent small updates. Sedimentree object-size caps do not bound cumulative history or verification cost; a resource-limit refusal must not be reported as forgery.
