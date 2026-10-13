# ADR 015: Daybook document format and DocumentsRepo

**Status:** Proposed

## 1. Document identity and content

A Daybook doc has a `DocumentId`, a versioned content representation, and a history. Every doc is in at least one drawer and can be entered in several. Official roster inclusion, access grants, and retaining a local copy are separate operations. `DocumentsRepo` creates docs and reads and writes their content; DrawerRepo manages roster entries. Creation always names a target drawer, persists identity and initial content before advertising authority, and also enters the doc in the embedder-supplied local drawers for physically present and locally originated docs.

The first content representation is a map of **facets** in Automerge. A facet key has a tag and an instance name, such as `org.example.note/main`. The tag names the value's type, while the instance distinguishes facets of that type. A document can contain facets with different tags, unknown tags, and no application facets at all. Its identity and representation must remain readable without installing a plug. Other content representations may be defined later without changing document identity.

A branch is another document with its own `DocumentId`. It may record the source document and heads from which it was created; it is not another named history *within* the source document. A branch is not automatically listed in the source document's drawers. Removing a facet leaves a tombstone and history; it is not document deletion. A schema failure, missing bytes, or lost access likewise does not delete a document. Document-level deletion requires a separate core lifecycle operation and is not specified here.

A document can recommend software through a Daybook-defined facet with a fixed, readable schema. A recommendation cannot install a plug or grant execution or write access. Program discovery and plug execution do not belong to the document envelope.

## 2. Facet values and writes

The DocumentsRepo API accepts a complete JSON value for each facet it sets, or an explicit request to remove that facet. It does not accept edits at JSON paths. A write may affect several facets, but unmentioned facets are unchanged. Before committing, the repository validates new local values against the selected facet definitions, applies the declared JSON-to-storage codec, and writes the resulting content and provenance atomically. The value **read from the transaction after the content mutation** is the value committed to the facet signature. This matters when the transaction starts from already merged heads or when an open facet preserves properties not present in the caller's JSON.

Each facet Lexicon describes its canonical JSON value and is identified by its facet tag. Its sidecar JSON Schema carries the supported supplementary constraints and Automerge storage annotations. SchemaRepo supplies the known Lexicon and sidecar bundle to Cabinet; how an embedder discovers or registers bundles is outside this repository contract. A local write to an unknown facet schema is refused. Field-name suffix or prefix hints do not recover a schema and are not part of Cabinet authoring. Conflicting definitions for one tag must be reported, not silently selected by installation order. Incompatible semantic meanings use different tags; cross-version transformation is not part of an ordinary doc write.

A read-modify-write must preserve unfamiliar content in its **native storage representation**, not merely as equivalent JSON. An old reader must not turn a future collaborative Text into Str, reset a Counter, or replace an unchanged nested object's identity. Full JSON round-tripping is the chosen read/write contract. Omission means removal at the supplied base heads, including unknown properties; removing an enclosing object removes its contents. Hidden-field projection is rejected: it loses correspondence between list elements with different hidden contents and cannot naturally represent an unknown variant in a required union field. Rejecting new unknown fields and requiring a special native-copy API are not adopted. List correspondence and changed unknown-value reconciliation still need design; JSON copy/paste is expected, but cannot promise native fidelity where neither a schema, corresponding target, nor usable convention is available.

Ordinary writes must supply the heads they are based on; there is no implicit current-head write or automatic retry loop. `AtCurrent` is the explicit unsafe choice, not the default. Reconciliation must use the selected base view, not silently diff against unseen current state. Concurrent creation cannot be rejected based on an invisible remote creator: any creation-tolerance policy must also define its post-merge behavior, which remains open.

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

AuthorityRepo verifies a node-signed capability token for the actor and requested drawer-scoped operation. An actor is its public key; names and paths confer no authority. If several drawers provide paths to the same doc, one sufficient actor grant suffices. Issuing or revoking an actor capability does **not** mutate those docs. A branch has a different doc ID and does not inherit drawer entries merely because it records an origin. The same checks apply across local IPC: a transport is not proof that a caller controls an asserted actor.

Dmeta records **who claims authorship of a facet**, not the host's current capability policy. A node may attribute an actor to an application or process, but the signature proves the node made that attribution, not that a particular binary executed.

## 4. Signed facet claims

A facet key selects a slot in the document. Dmeta retains every UUID alias assigned to that slot by concurrent creators; external references to any alias remain valid. For a new signature the chosen UUID is the lexicographically smallest alias **visible in the writing transaction**. A verifier reads the state at the change that introduced the signature to recover that same choice; a later merge may add a smaller alias without changing what was signed. The facet's content is the merged value at its key, not a value selected by UUID.

The aliases also retain evidence of **concurrent creation** after later writes hide the original Automerge conflict. Convergent facet keys let two OCR processors target the same slot; distinct creation UUIDs let an index distinguish those independent creations durably. They do not select a different content winner.

A facet signature authenticates a *logical claim*, but an Automerge document **does not store a claim object**. Its signed bytes are reconstructed from the state at the change introducing the signature. The fields are the origin document ID, facet key, UUID selected at that change, Set or Remove operation, CID of the transaction-view JSON value for Set (null for Remove), and sorted IDs of accepted facet signatures visible at the change's parent heads. A new facet has no predecessors; resurrection follows the visible tombstone. Signing identical content with the same actor at the same facet frontier produces the same signature. Writing at historical heads uses their predecessors and may produce a concurrent signature. The logical claim is independent of Automerge even though this compact storage strategy uses Automerge history to reconstruct it.

V1 canonicalizes the transaction-view JSON with RFC 8785 (JCS), then makes a CIDv1 with `raw` codec and **BLAKE3-256** multihash (code `0x1e`) of those bytes. JCS chooses the canonical JSON bytes; it does **not** mandate a hash algorithm. Finite binary64 floats are supported; NaN and infinities are not JSON. Numbers outside JCS's interoperable range require a schema-declared string or bytes representation rather than silent rounding. A schema-declared byte field has its declared JSON representation before canonicalization. Multibase is an API/export rendering of binary IDs, not signed text.

V1 signs `UTF8("daybook.facet-claim.v1") || 0x00 || CBOR(fields)` with Ed25519. `CBOR(fields)` is an RFC 8949 deterministic encoding of a definite-length array in this order: origin document ID (UTF-8 text), facet key (UTF-8 text), chosen UUID (16-byte string), operation (`0` Set or `1` Remove), value CID (binary byte string or null for Remove), and predecessor IDs (array of binary IDs sorted lexicographically without duplicates). The signature covers the whole message, not merely the value CID. The signature ID is a CIDv1 `raw`/BLAKE3-256 multihash of `UTF8("daybook.facet-claim-id.v1") || 0x00 || actor-public-key-bytes || signed-message-bytes || signature-bytes`. It does not refer to its containing Automerge change hash, which would be circular. The signer key and Ed25519 signature are stored as bytes.

## 5. Actors, delegations, and dmeta

An actor is identified by its public key. An actor URL or description is metadata, not identity and not an authority namespace. Private signing keys remain in `secrets_rs`; DocumentsRepo and DrawerRepo present a capability and typed claim to AuthorityRepo and receive a signature, never the key. This boundary works both in-process and across an address space.

Capability tokens are small node-signed values that authorize coarse drawer operations. AuthorityRepo boot returns the operator capability. The operator may create independent grants; another actor may explicitly delegate only a narrower child capability and may not graft authority laterally onto a sibling path. There is no implicit path inheritance, TTL, or authority epoch in this version. Capability policy is current local authorization and is separate from Keyhive node authority.

Dmeta attribution is also node-signed, including when an intermediate actor or process requested the delegation. It binds the node to the actor public key and the claim context needed for verification; it does not encode the actor's mutable capability grants or turn the actor into a Keyhive principal. The Automerge change actor remains the node, while the actor signs the facet claim. Node change signatures, node-signed actor attribution, actor facet signatures, and capability tokens are distinct artifacts.

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
    signatures: [ { actorPublicKey, signature, originDoc?,
                    inheritedMessage?, inheritedValue? } ]
  }
}
```

For an ordinary Automerge write, the signature entry carries the actor reference and signature bytes; the facet key, chosen UUID, operation, value, and predecessor IDs are reconstructed at the change that introduced it. No value CID, duplicate facet key, UUID, or serialized claim is stored with that signature. `originDoc` is omitted when it equals the enclosing document ID. Actor public keys and delegations are shared through `actors`. This makes the common case a signature plus a small actor reference, even for a tiny blob-pin facet.

`signatures` is a **current competing set**, not an append-only audit index. A write replaces the entries it observed at its base heads with its own signature; concurrent writers can leave multiple entries. The Automerge encoding must preserve concurrently written entries rather than lose one through list reconciliation. Older signatures can remain in Automerge history, but are not duplicated in the current dmeta facet. Timestamps help display history, not establish signature identity or authorization. Granting a plug broader access creates no dmeta entries in untouched documents.

An independent branch does not contain its source document's Automerge change history. Merely recording `originDoc` is therefore **not enough** to verify an inherited signature whose UUID, value, or predecessors were reconstructed at a source frontier. On branch creation, copy the signed message as `inheritedMessage` for each retained signature, with its origin ID. If the signed source value differs from the branch's copied current value (for example after a concurrent merge), also retain that source value as `inheritedValue` so its CID can be checked. A signature on an old value must not be presented as a signature on the merged value. Newly authored signatures on the branch omit these witnesses and use branch history for reconstruction. A compacted document or non-Automerge substrate that discards authoring history likewise must carry the signed message and value needed for signatures it claims remain verifiable. **Omitting those witnesses is an Automerge-history-specific storage optimization**, not a universal signature format. Without reconstructible evidence, report provenance as unverifiable rather than inventing its context.

For example, node N delegates actor A and A signs facets X and Y in D. The current signatures share A's delegation and do not duplicate the facet keys or value CIDs. Branch B carries X's and Y's inherited signed messages with origin D. When A edits X on B, the new signature is reconstructed from B's own history and names B as origin; its predecessor is the inherited X signature ID. Compaction can keep current signed messages but cannot recreate proof of discarded Automerge operations or revisions no longer retained.

## 6. Verification and presentation

To verify a received Automerge signature, find the change that first installed that signature entry. At that change's resulting frontier, read its facet value or tombstone, key, and UUID aliases; at the change's parent heads, read the accepted predecessor signatures. Reconstruct the signed message, verify it with the referenced actor key, and verify that key's node-signed delegation at the introduction frontier—not merely in today's dmeta. For an inherited entry, verify the carried signed message and check its value CID against the copied value or its retained `inheritedValue`; never treat it as an actor signature on a different merged value. Separately check the node signer of the containing change against document authority. For v1, proven facet-signature forgery closes the whole document to ordinary application reads and writes; its physical history may remain as rejected evidence. We do not remove changes, rewind to a last accepted snapshot, or implement arbitrary partial-facet repair. An explicit repair interface and fresh-object replacement are candidates, not a settled recovery protocol. An authorized writer can already destroy availability through valid destructive writes; this conservative shutdown does not promise protection against that attack.

Missing authority or attribution evidence is **pending verification**, not proof of forgery. A proven bad signature or unauthorized node write is **rejected** as application content. Verified history is **accepted**; absence of a local schema means its facet remains uninterpreted, not cryptographically invalid. Resolve acceptance before displaying Automerge's visible conflict winner: an invalid winner is not presented just because Automerge selected it. Descendants of rejected physical history cannot launder its content into the accepted view. Local writes fail atomically with an error; received history has no local caller to return an error to and must be reported through verification state.

Ordinary DocumentsRepo consumers see accepted content only. Missing verification evidence behaves like missing network data, not proven forgery, and does not trigger document shutdown. A separately marked optimistic-read interface may expose pending content, but ordinary writes must not build on it; the repository must enforce that barrier rather than trust the caller. Temporary branches for optimistic edits and later verified merging are deferred. Physical causal ancestry is not itself semantic dependence: reconciliation and signing must not reuse rejected content or objects merely because those objects exist in physical history.

BigRepo and Keyhive gate node authority below Cabinet. AuthorityRepo does not reconstruct a second historical Keyhive decision for actor capabilities: a write performed now uses the actor's current capability, even when its explicit base heads are old. Actor attribution proves who signed the facet claim and is not a new Keyhive grant. A lower-layer node-authority refusal applies to every actor and may be translated into a Cabinet error without duplicating the authority graph.

## 7. Conformance work

Before v1 signatures are issued, publish cross-implementation byte test vectors for JCS numeric values, BLAKE3-256 value CIDs and signature IDs, binary claim and delegation arrays, and Ed25519 signatures; verify canonical textual forms for document IDs and facet keys. Inspect the actual signed Automerge change-label payload and its handoff through Subduction. Test an open facet's transaction-view value; writes at old heads; concurrent creators and UUID aliases; concurrent signature replacement; tombstone and resurrection; a branch with inherited and newly authored claims, including an inherited value different from the copied merge result; copied and compacted claims; unknown schemas; missing and forged proofs; invalid conflict winners; and repair after a rejected change. Benchmark dmeta overhead and hydration with many one-field `blobPin` facets as well as frequent small updates. Sedimentree object-size caps do not bound cumulative history or verification cost; a resource-limit refusal must not be reported as forgery.
